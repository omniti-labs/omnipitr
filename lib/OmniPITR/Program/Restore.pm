package OmniPITR::Program::Restore;
use strict;
use warnings;

our $VERSION = '0.1.2';
use base qw( OmniPITR::Program );

use Carp;
use OmniPITR::Tools qw( :all );
use English qw( -no_match_vars );
use File::Spec;
use File::Path qw( mkpath rmtree );
use File::Copy;
use Storable;
use Data::Dumper;
use Getopt::Long;
use Cwd;

=head1 run()

Main function, called by actual script in bin/, wraps all work done by
script with the sole exception of reading and validating command line
arguments.

These tasks (reading and validating arguments) are in this module, but
they are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need
further information - simply check doc for the method you have questions
about.

=cut

sub run {
    my $self = shift;

    $SIG{ 'USR1' } = sub {
        $self->{ 'finish' } = 'immediate';
        return;
    };

    while ( 1 ) {
        $self->check_for_trigger_file();
        $self->try_to_restore_and_exit();
        next if $self->{ 'finish' };
        sleep 1;
        $self->do_some_removal();
    }
}

=head1 do_some_removal()

Wraps all work necessary to remove obsolete WAL segments from archive.

Contains actual I<unlink> calls, but all other work (checking
pg_controldata, extracting last REDO segment, getting list of files to
remove, calling pre-removal hook) is delegated to dedicated methods.

=cut

sub do_some_removal {
    my $self = shift;

    return unless $self->{ 'remove-unneeded' };

    if ( $self->{ 'removal-pause-trigger' } && -e $self->{ 'removal-pause-trigger' } ) {
        unless ( $self->{ 'trigger-logged' } ) {
            $self->{ 'trigger-logged' }++;
            $self->log->log( 'Pause trigger exists (%s), NOT removing any old segments.', $self->{ 'removal-pause-trigger' } );
        }
        return;
    }

    my $control_data = $self->get_control_data();
    return unless $control_data;

    my $last_important = $self->get_last_redo_segment( $control_data );
    return unless $last_important;

    my @to_be_removed = $self->get_list_of_segments_to_remove( $last_important );
    return if 0 == scalar @to_be_removed;

    for my $segment_name ( @to_be_removed ) {
        return unless $self->handle_pre_removal_processing( $segment_name );

        my $segment_file_name = File::Spec->catfile( $self->{ 'source' }->{ 'path' }, $segment_name );
        $segment_file_name .= ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };
        my $result = unlink $segment_file_name;
        unless ( 1 == $result ) {
            $self->log->error( 'Error while unlinking %s : %s', $segment_file_name, $OS_ERROR );
            return;
        }
        $self->log->log( 'Segment %s (%s) removed, as it is too old (older than %s)', $segment_name, $segment_file_name, $last_important );
    }
    return;
}

=head1 handle_pre_removal_processing()

Before removing obsolete WAL segment, I<omnipitr-restore> can call
arbitrary program to do whatever is necessary - for example - to send
the WAL segment to backup server.

This is done in here. Each segment is first uncompressed to temporary
directory, and then given program is called.

Temporary directory is always made so that it "looks" like it was called
by archive-command from PostgreSQL, i.e.:

=over

=item * Current directory contains pg_xlog directory

=item * Segment is unpacked

=item * Segment is in pg_xlog directory

=item * Handler program is called with segment name like
'pg_xlog/000000010000000500000073'

=back

=cut

sub handle_pre_removal_processing {
    my $self         = shift;
    my $segment_name = shift;
    return 1 unless $self->{ 'pre-removal-processing' };

    $self->prepare_temp_directory();
    my $xlog_dir  = File::Spec->catfile( $self->{ 'temp-dir' }, 'pg_xlog' );
    my $xlog_file = File::Spec->catfile( $xlog_dir,             $segment_name );
    mkpath( $xlog_dir );

    my $comment = 'Copying segment ' . $segment_name . ' to ' . $xlog_file;
    $self->log->time_start( $comment ) if $self->verbose;
    my $response = $self->copy_segment_to( $segment_name, $xlog_file );
    $self->log->time_finish( $comment ) if $self->verbose;

    if ( $response ) {
        $self->log->error( 'Error while copying segment for pre removal processing for %s : %s', $segment_name, $response );
        return;
    }

    my $previous_dir = getcwd();
    chdir $self->{ 'temp-dir' };

    my $full_command = $self->{ 'pre-removal-processing' } . " pg_xlog/$segment_name";

    $comment = 'Running pre-removal-processing command: ' . $full_command;

    $self->log->time_start( $comment ) if $self->verbose;
    my $result = run_command( $self->{ 'tempdir' }, 'bash', '-c', $full_command );
    $self->log->time_finish( $comment ) if $self->verbose;

    chdir $previous_dir;

    rmtree( $xlog_dir );
    return 1 unless $result->{ 'error_code' };

    $self->log->error( 'Error while calling pre removal processing [%s] : %s', $full_command, $result );

    return;
}

=head1 get_list_of_segments_to_remove()

Scans source directory, and returns names of all files, which are
"older" than last required segment (REDO segment from pg_controldata).

Older - is defined as alphabetically smaller than REDO segment.

Returns at most X files, where X is defined by --remove-at-a-time
command line option.

=cut

sub get_list_of_segments_to_remove {
    my $self           = shift;
    my $last_important = shift;

    my $extension = ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };
    my $dir;

    unless ( opendir( $dir, $self->{ 'source' }->{ 'path' } ) ) {
        $self->log->error( 'Cannot open source directory (%s) for reading: %s', $self->{ 'source' }->{ 'path' }, $OS_ERROR );
        return;
    }
    my @content = readdir $dir;
    closedir $dir;

    my @too_old = ();
    for my $file ( @content ) {
        $file =~ s/\Q$extension\E\z// if $extension;
        next unless $file =~ m{\A[a-fA-F0-9]{24}(?:\.[a-fA-F0-9]{8}\.backup)?\z};
        next unless $file lt $last_important;
        push @too_old, $file;
    }
    return if 0 == scalar @too_old;

    $self->log->log( '%u segments too old, to be removed.', scalar @too_old ) if $self->verbose;

    my @sorted = sort @too_old;
    splice( @sorted, $self->{ 'remove-at-a-time' } ) if $self->{ 'remove-at-a-time' } < scalar @sorted;

    return @sorted;
}

=head1 get_last_redo_segment()

Based on information from pg_controldata, returns name of file that
contains oldest file required in case recovery would have to be
restarted.

This is required to be able to tell which files can be safely removed
from archive.

=cut

sub get_last_redo_segment {
    my $self = shift;
    my $CD   = shift;

    my $segment  = $CD->{ "Latest checkpoint's REDO location" };
    my $timeline = $CD->{ "Latest checkpoint's TimeLineID" };

    my ( $series, $offset ) = split m{/}, $segment;

    $offset =~ s/.{0,6}$//;

    my $segment_filename = sprintf '%08s%08s%08s', $timeline, $series, $offset;

    return $segment_filename;
}

=head1 get_control_data()

Wraps SUPER::get_control_data in such way that it will not die in case of problems.

Reason: errors with parsin pg_controldata cannot cause die from omnipitr-restore, to avoid bringing
PostgreSQL from WAL-slave to Standalone.

=cut

sub get_control_data {
    my $self = shift;

    if ( $self->{ 'pause-removal-till' } ) {
        return if time() < $self->{ 'pause-removal-till' };
        delete $self->{ 'pause-removal-till' };
    }

    my $ret;
    eval { $ret = $self->SUPER::get_control_data(); };
    if (   ( !$EVAL_ERROR )
        && ( $ret ) )
    {
        return $ret;
    }

    $self->{ 'pause-removal-till' } = time() + 5 * 60;
    return;
}

=head1 try_to_restore_and_exit()

Checks if requested wal segment exists, and is ready to be restored (
vide --recovery-delay option).

Handles also situations where there is finish request (both immediate
and smart).

If recovery worked - finished with status 0.

If no file can be returned yet - goes back to main loop in L<run()>
method.

=cut

sub try_to_restore_and_exit {
    my $self = shift;

    if ( $self->{ 'finish' } eq 'immediate' ) {
        $self->log->fatal( 'Got immediate finish request. Dying.' );
    }

    my $wanted_file = File::Spec->catfile( $self->{ 'source' }->{ 'path' }, $self->{ 'segment' } );
    $wanted_file .= ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };

    unless ( -e $wanted_file ) {
        if ( $self->{ 'finish' } ) {
            $self->log->fatal( 'Got finish request. Dying.' );
        }
        if ( $self->{ 'segment' } =~ m{\A[a-fA-f0-9]{8}\.history\z} ) {
            $self->log->log( 'Requested history file (%s) that does not exist. Returning error.', $self->{ 'segment' } );
            exit( 1 );
        }
        if ( $self->{ 'streaming-replication' } ) {
            $self->log->fatal( 'Requested file does not exist, and it is streaming replication environment. Dying.' );
        }
        return;
    }

    if (   ( $self->{ 'recovery-delay' } )
        && ( !$self->{ 'finish' } ) )
    {
        my @file_info  = stat( $wanted_file );
        my $file_mtime = $file_info[ 9 ];
        my $ok_since   = time() - $self->{ 'recovery-delay' };
        if ( $ok_since <= $file_mtime ) {
            if (   ( $self->verbose )
                && ( !$self->{ 'logged_delay' } ) )
            {
                $self->log->log( 'Segment %s found, but it is too fresh (mtime = %u, accepted since %u)', $self->{ 'segment' }, $file_mtime, $ok_since );
                $self->{ 'logged_delay' } = 1;
            }
            return;
        }
    }

    my $full_destination = File::Spec->catfile( $self->{ 'data-dir' }, $self->{ 'segment_destination' } );

    my $comment = 'Copying segment ' . $self->{ 'segment' } . ' to ' . $full_destination;
    $self->log->time_start( $comment ) if $self->verbose;
    my $response = $self->copy_segment_to( $self->{ 'segment' }, $full_destination );
    $self->log->time_finish( $comment ) if $self->verbose;

    if ( $response ) {
        $self->log->fatal( $response );
    }

    $self->log->log( 'Segment %s restored', $self->{ 'segment' } );
    exit( 0 );
}

=head1 copy_segment_to()

Helper function which deals with copying segment from archive to given
destination, handling compression when necessary.

=cut

sub copy_segment_to {
    my $self = shift;
    my ( $segment_name, $destination ) = @_;

    my $wanted_file = File::Spec->catfile( $self->{ 'source' }->{ 'path' }, $segment_name );
    $wanted_file .= ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };

    unless ( $self->{ 'source' }->{ 'compression' } ) {
        if ( copy( $wanted_file, $destination ) ) {
            return;
        }
        return sprintf( 'Copying %s to %s failed: %s', $wanted_file, $destination, $OS_ERROR );
    }

    my $compression = $self->{ 'source' }->{ 'compression' };
    my $command = sprintf '%s --decompress --stdout %s > %s', quotemeta( $self->{ "$compression-path" } ), quotemeta( $wanted_file ), quotemeta( $destination );

    $self->prepare_temp_directory();

    my $response = run_command( $self->{ 'temp-dir' }, 'bash', '-c', $command );

    return sprintf( 'Uncompressing %s to %s failed: %s', $wanted_file, $destination, Dumper( $response ) ) if $response->{ 'error_code' };
    return;
}

=head1 check_for_trigger_file()

Checks existence and possibly content of finish-trigger file, setting
appropriate flags.

=cut

sub check_for_trigger_file {
    my $self = shift;

    return unless $self->{ 'finish-trigger' };
    return unless -e $self->{ 'finish-trigger' };

    if ( open my $fh, '<', $self->{ 'finish-trigger' } ) {
        local $INPUT_RECORD_SEPARATOR = undef;
        my $content = <$fh>;
        close $fh;

        $self->{ 'finish' } = $content =~ m{\ANOW\n?\z} ? 'immediate' : 'smart';

        $self->log->log( 'Finish trigger found, %s mode.', $self->{ 'finish' } );
        return;
    }
    $self->log->fatal( 'Finish trigger (%s) exists, but cannot be open?! : %s', $self->{ 'finish-trigger' }, $OS_ERROR );
}

=head1 read_args()

Function which does all the parsing, and transformation of command line
arguments.

It also verified base facts about passed WAL segment name, but all other
validations, are being done in separate function: L<validate_args()>.

=cut

=head1 read_args()

=cut

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my %args = (
        'bzip2-path'          => 'bzip2',
        'data-dir'            => '.',
        'gzip-path'           => 'gzip',
        'lzma-path'           => 'lzma',
        'pgcontroldata-path'  => 'pg_controldata',
        'error-pgcontroldata' => 'break',
        'remove-at-a-time'    => 3,
        'temp-dir'            => $ENV{ 'TMPDIR' } || '/tmp',
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-restore.pod' )
        unless GetOptions(
        \%args,
        'bzip2-path|bp=s',
        'data-dir|D=s',
        'error-pgcontroldata|ep=s',
        'finish-trigger|f=s',
        'gzip-path|gp=s',
        'log|l=s',
        'lzma-path|lp=s',
        'pgcontroldata-path|pp=s',
        'pid-file=s',
        'pre-removal-processing|h=s',
        'recovery-delay|w=i',
        'removal-pause-trigger|p=s',
        'remove-at-a-time|rt=i',
        'remove-unneeded|r',
        'source|s=s',
        'streaming-replication|sr',
        'temp-dir|t=s',
        'verbose|v',
        );

    croak( '--log was not provided - cannot continue.' ) unless $args{ 'log' };
    $args{ 'log' } =~ tr/^/%/;

    for my $key ( keys %args ) {
        next if $key =~ m{ \A (?: source | log ) \z }x;    # Skip those, not needed in $self
        $self->{ $key } = $args{ $key };
    }

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

    $self->log->fatal( 'Source path not provided!' ) unless $args{ 'source' };

    if ( $args{ 'source' } =~ s/\A(gzip|bzip2|lzma)=// ) {
        $self->{ 'source' }->{ 'compression' } = $1;
    }
    $self->{ 'source' }->{ 'path' } = $args{ 'source' };

    # These could theoretically go into validation, but we need to check if we can get anything to put in segment* keys in $self
    $self->log->fatal( 'WAL segment file name and/or destination have not been given' ) if 2 > scalar @ARGV;
    $self->log->fatal( 'Too many arguments given.' ) if 2 < scalar @ARGV;

    @{ $self }{ qw( segment segment_destination ) } = @ARGV;

    $self->{ 'finish' } = '';

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->verbose;

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for compression programs paths - technically, it could
be validated in here, but benefit would be pretty limited, and code to
do so relatively complex, as compression program path might, but doesn't
have to be actual file path - it might be just program name (without
path), which is the default.

=cut

sub validate_args {
    my $self = shift;

    $self->log->fatal( 'Given data-dir (%s) is not valid', $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' } && -f File::Spec->catfile( $self->{ 'data-dir' }, 'PG_VERSION' );

    $self->log->fatal( 'Given segment name is not valid (%s)', $self->{ 'segment' } ) unless $self->{ 'segment' } =~ m{\A([a-fA-F0-9]{24}(?:\.[a-fA-F0-9]{8}\.backup)?|[a-fA-F0-9]{8}\.history)\z};

    $self->log->fatal( 'Given source (%s) is not a directory', $self->{ 'source' }->{ 'path' } ) unless -d $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Given source (%s) is not readable',    $self->{ 'source' }->{ 'path' } ) unless -r $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Given source (%s) is not writable',    $self->{ 'source' }->{ 'path' } ) unless -w $self->{ 'source' }->{ 'path' };

    $self->log->fatal( 'Invalid error-pgcontroldata: %s.', $self->{ 'error-pgcontroldata' } ) unless $self->{ 'error-pgcontroldata' } =~ m{\A (?: break | ignore | hang ) \z}x;

    return;
}

1;
