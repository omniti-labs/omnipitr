package OmniPITR::Program::Restore;
use strict;
use warnings;

use base qw( OmniPITR::Program );

use Carp;
use OmniPITR::Tools qw( :all );
use English qw( -no_match_vars );
use File::Basename;
use File::Spec;
use File::Path qw( make_path remove_tree );
use File::Copy;
use Storable;
use Getopt::Long;
use Cwd;

=head1 run()

Main function, called by actual script in bin/, wraps all work done by script with the sole exception of reading and validating command line arguments.

These tasks (reading and validating arguments) are in this module, but they are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need further information - simply check doc for the method you have questions about.

=cut

sub run {
    my $self = shift;

    $SIG{ 'USR1' } = sub {
        $self->{ 'finish' } = 'immediate';
        return;
    };

    while ( 1 ) {
        $self->try_to_restore_and_exit();
        sleep 1;
        next if $self->{ 'finish' };
        $self->check_for_trigger_file();
        next if $self->{ 'finish' };
        $self->do_some_removal();
    }
}

sub do_some_removal {
    my $self = shift;

    return unless $self->{ 'remove-unneeded' };

    return if $self->{ 'removal-pause-trigger' } && -e $self->{ 'removal-pause-trigger' };

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

sub handle_pre_removal_processing {
    my $self         = shift;
    my $segment_name = shift;
    return unless $self->{ 'pre-removal-processing' };

    $self->prepare_temp_directory();
    my $xlog_dir  = File::Spec->catfile( $self->{ 'temp-dir' }, 'pg_xlog' );
    my $xlog_file = File::Spec->catfile( $xlog_dir,             $segment_name );
    make_path( $xlog_dir );

    my $response = $self->copy_segment_to( $segment_name, $xlog_file );
    if ( $response ) {
        $self->log->error( 'Error while copying segment for pre removal processing for %s : %s', $segment_name, $response );
        return;
    }

    my $previous_dir = gwtcwd();
    chdir $self->{ 'temp-dir' };
    my $full_command = $self->{ 'pre-removal-processing' } . " pg_xlog/$segment_name";
    my $result = run_command( $self->{ 'tempdir' }, 'bash', '-c', $full_command );
    chdir $previous_dir;

    remove_tree( $xlog_dir );
    return 1 unless $result->{ 'error_code' };

    $self->log->error( 'Error while calling pre removal processing [%s] : %s', $full_command, Dumper( $result ) );

    return;
}

sub get_list_of_segments_to_remove {
    my $self           = shift;
    my $last_important = shift;

    my $extension = ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };
    my $dir;

    unless ( opendir( $dir, $self->{ 'source' } ) ) {
        $self->log->error( 'Cannot open source directory (%s) for reading: %s', $self->{ 'source' }->{ 'path' }, $OS_ERROR );
        return;
    }
    my @content = readdir $dir;
    closedir $dir;

    my @too_old = ();
    for my $file ( @content ) {
        $file =~ s/\Q$extension\E\z//;
        next unless $file =~ m{\A[a-f0-9]{24}\z};
        next unless $file lt $last_important;
        push @too_old, $file;
    }
    return if 0 == scalar @too_old;

    my @sorted = sort @too_old;
    splice( @sorted, $self->{ 'remove-at-a-time' } );

    return @sorted;
}

sub get_last_redo_segment {
    my $self = shift;
    my $CD   = shift;

    my $segment  = $CD->{ "Latest checkpoint's REDO location" };
    my $timeline = $CD->{ "Latest checkpoint's TimeLineID" };

    my ( $series, $offset ) = split m{/}, $segment;

    $offset =~ s/.{6}$//;

    my $segment_filename = sprintf '%08s%08s%08s', $timeline, $series, $offset;

    return $segment_filename;
}

sub get_control_data {
    my $self = shift;

    $self->prepare_temp_directory();

    my $response = run_command( $self->{ 'temp-dir' }, $self->{ 'pgcontroldata-path' }, $self->{ 'data-dir' } );
    if ( $response->{ 'error_code' } ) {
        $self->log->error( 'Error while getting pg_controldata for %s: %s', $self->{ 'data-dir' }, Dumper( $response ) );
        return;
    }

    my $control_data = {};

    my @lines = split( /\s*\n/, $response->{ 'stdout' } );
    for my $line ( @lines ) {
        unless ( $line =~ m{\A([^:]+):\s*(.*)\z} ) {
            $self->log->error( 'Pg_controldata for %s contained unparseable line: [%s]', $self->{ 'data-dir' }, $line );
            $self->exit_with_status( 1 );
        }
        $control_data->{ $1 } = $2;
    }

    unless ( $control_data->{ "Latest checkpoint's REDO location" } ) {
        $self->log->error( 'Pg_controldata for %s did not contain latest checkpoint redo location', $self->{ 'data-dir' } );
        return;
    }
    unless ( $control_data->{ "Latest checkpoint's TimeLineID" } ) {
        $self->log->error( 'Pg_controldata for %s did not contain latest checkpoint timeline ID', $self->{ 'data-dir' } );
        return;
    }

    return $control_data;
}

sub try_to_restore_and_exit {
    my $self = shift;

    if ( $self->{ 'finish' } eq 'immediate' ) {
        $self->log->error( 'Got immediate finish request. Dying.' );
        $self->exit_with_status( 1 );
    }

    my $wanted_file = File::Spec->catfile( $self->{ 'source' }->{ 'path' }, $self->{ 'segment' } );
    $wanted_file .= ext_for_compression( $self->{ 'source' }->{ 'compression' } ) if $self->{ 'source' }->{ 'compression' };

    unless ( -e $wanted_file ) {
        if ( $self->{ 'finish' } ) {
            $self->log->error( 'Got finish request. Dying.' );
            $self->exit_with_status( 1 );
        }
    }

    if (   ( $self->{ 'recovery-delay' } )
        && ( !$self->{ 'finish' } ) )
    {
        my @file_info  = stat( $wanted_file );
        my $file_mtime = $file_info[ 9 ];
        my $ok_since   = time - $self->{ 'recovery-delay' };
        if ( $ok_since > $file_mtime ) {
            if ( $self->{ 'verbose' } ) {
                unless ( $self->{ 'logged_delay' } ) {
                    $self->log->log( 'Segment %s found, but it is too fresh (mtime = %u, accepted since %u)', $self->{ 'segment' }, $file_mtime, $ok_since );
                    $self->{ 'logged_delay' } = 1;
                }
            }
            return;
        }
    }

    my $full_destination = File::Spec->catfile( $self->{ 'data-dir' }, $self->{ 'segment_destination' } );

    my $response = $self->copy_segment_to( $self->{ 'segment' }, $full_destination );

    if ( $response ) {
        $self->log->error( $response );
        $self->exit_with_status( 1 );
    }

    $self->log->log( 'Segment %s restored', $self->{ 'segment' } );
    $self->exit_with_status( 0 );
}

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

=head1 exit_with_status()

Exit function, doing cleanup (remove temp-dir), and exiting with given status.

=cut

sub exit_with_status {
    my $self = shift;
    my $code = shift;

    remove_tree( $self->{ 'temp-dir' } ) if $self->{ 'temp-dir-prepared' };

    exit( $code );
}

=head1 prepare_temp_directory()

Helper function, which builds path for temp directory, and creates it.

Path is generated by using given temp-dir and 'omnipitr-restore' name.

For example, for temp-dir '/tmp', actual, used temp directory would be /tmp/omnipitr-restore/.

=cut

sub prepare_temp_directory {
    my $self = shift;
    return if $self->{ 'temp-dir-prepared' };
    my $full_temp_dir = File::Spec->catfile( $self->{ 'temp-dir' }, basename( $PROGRAM_NAME ) );
    make_path( $full_temp_dir );
    $self->{ 'temp-dir' }          = $full_temp_dir;
    $self->{ 'temp-dir-prepared' } = 1;
    return;
}

=head1 read_args()

Function which does all the parsing, and transformation of command line arguments.

It also verified base facts about passed WAL segment name, but all other validations, are being done in separate function: L<validate_args()>.

=cut

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my %args = (
        'bzip2-path'         => 'bzip2',
        'data-dir'           => '.',
        'gzip-path'          => 'gzip',
        'lzma-path'          => 'lzma',
        'pgcontroldata-path' => 'pg_controldata',
        'remove-at-a-time'   => 3,
        'temp-dir'           => $ENV{ 'TMPDIR' } || '/tmp',
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-archive.pod' )
        unless GetOptions(
        \%args,
        'bzip2-path|bp=s',
        'data-dir|D=s',
        'finish-trigger|f=s',
        'gzip-path|gp=s',
        'log|l=s',
        'lzma-path|lp=s',
        'pgcontroldata-path|pp=s',
        'pid-file=s',
        'pre-removal-processing|h=s',
        'remove-at-a-time|rt=i',
        'recovery-delay|w=i',
        'removal-pause-trigger|p=s',
        'remove-unneeded|r=s',
        'source|s=s',
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

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->{ 'verbose' };

    $self->{ 'finish' } = '';

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for compression programs paths - technically, it could be validated in here, but benefit would be pretty limited, and code to do so relatively complex, as compression program path
might, but doesn't have to be actual file path - it might be just program name (without path), which is the default.

=cut

sub validate_args {
    my $self = shift;

    $self->log->fatal( 'Given data-dir (%s) is not valid', $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' } && -f File::Spec->catfile( $self->{ 'data-dir' }, 'PG_VERSION' );

    $self->log->fatal( 'Given segment name is not valid (%s)', $self->{ 'segment' } ) unless $self->{ 'segment' } =~ m{\A[a-f0-9]{24}\z};

    $self->log->fatal( 'Given source (%s) is not a directory', $self->{ 'source' }->{ 'path' } ) unless -d $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Given source (%s) is not readable',    $self->{ 'source' }->{ 'path' } ) unless -r $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Given source (%s) is not writable',    $self->{ 'source' }->{ 'path' } ) unless -w $self->{ 'source' }->{ 'path' };

    return;
}

1;
