package OmniPITR::Program::Archive;
use strict;
use warnings;

our $VERSION = '0.5.0';
use base qw( OmniPITR::Program );

use Carp;
use English qw( -no_match_vars );
use File::Basename;
use File::Copy;
use File::Path qw( mkpath rmtree );
use File::Spec;
use Getopt::Long qw( :config no_ignore_case );
use OmniPITR::Tools::ParallelSystem;
use OmniPITR::Tools qw( :all );
use Storable;

=head1 run()

Main function, called by actual script in bin/, wraps all work done by script with the sole exception of reading and validating command line arguments.

These tasks (reading and validating arguments) are in this module, but they are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need further information - simply check doc for the method you have questions about.

=cut

sub run {
    my $self = shift;
    $self->read_state();
    $self->prepare_temp_directory( basename( $self->{ 'segment' } ) );
    $self->make_all_necessary_compressions();
    $self->log->time_start( 'Segment delivery' ) if $self->verbose;
    $self->send_to_destinations();
    $self->log->time_finish( 'Segment delivery' ) if $self->verbose;
    $self->cleanup();
    $self->log->log( 'Segment %s successfully sent to all destinations.', $self->{ 'segment' } );
    return;
}

=head1 send_to_destinations()

Does all the actual sending of segments to local and remote destinations.

It keeps it's state to be able to continue in case of error.

Since both local and remote destinations are handled in the same way, there is no point in duplicating the code to 2 methods.

Important notice - this function has to have the ability to choose whether to use temp file (for compressed destinations), or original segment (for uncompressed ones). This is done by this line:

    my $local_file = $dst->{ 'compression' } eq 'none' ? $self->{ 'segment' } : $self->get_temp_filename_for( $dst->{ 'compression' } );

=cut

sub send_to_destinations {
    my $self = shift;

    my $all_ok = 1;

    my $handle_finish = sub {
        my $job = shift;
        $self->log->log( 'Sending %s to %s ended in %.6fs', $job->{ 'local_file' }, $job->{ 'destination_file_path' }, $job->{ 'ended' } - $job->{ 'started' } ) if $self->verbose;
        if ( $job->{ 'status' } ) {
            if ( $job->{ 'is_backup' } ) {
                $self->log->error( "Sending segment %s to backup destination %s generated (ignored) error: %s", $job->{ 'local_file' }, $job->{ 'destination_file_path' }, $job );
            }
            else {
                $self->log->error( "Cannot send segment %s to %s : %s", $job->{ 'local_file' }, $job->{ 'destination_file_path' }, $job );
                $all_ok = 0;
            }
        }
        else {
            $self->{ 'state' }->{ 'sent' }->{ $job->{ 'destination_type' } }->{ $job->{ 'dst_path' } } = 1;
        }
        return;
    };

    my $runner = OmniPITR::Tools::ParallelSystem->new(
        'max_jobs'  => $self->{ 'parallel-jobs' },
        'on_finish' => $handle_finish,
    );

    for my $destination_type ( qw( local remote ) ) {
        next unless my $dst_list = $self->{ 'destination' }->{ $destination_type };
        for my $dst ( @{ $dst_list } ) {
            next if $self->segment_already_sent( $destination_type, $dst );

            my $local_file = $dst->{ 'compression' } eq 'none' ? $self->{ 'segment' } : $self->get_temp_filename_for( $dst->{ 'compression' } );

            my $destination_file_path = $dst->{ 'path' };

            my $is_backup = undef;
            if ( $self->{ 'dst-backup' } ) {
                $is_backup = 1 if $dst->{ 'path' } eq $self->{ 'dst-backup' };
            }

            $destination_file_path =~ s{/*\z}{};
            $destination_file_path .= '/' . basename( $local_file );

            $runner->add_command(
                'command'               => [ $self->{ 'rsync-path' }, $local_file, $destination_file_path ],
                'is_backup'             => $is_backup,
                'local_file'            => $local_file,
                'destination_file_path' => $destination_file_path,
                'destination_type'      => $destination_type,
                'dst_path'              => $dst->{ 'path' },
            );
        }
    }

    $ENV{ 'TMPDIR' } = $self->{ 'temp-dir' };

    $runner->run();

    $self->save_state();

    $self->log->fatal( 'There are fatal errors. Dying.' ) unless $all_ok;

    return;
}

=head1 segment_already_sent()

Simple function, that checks if segment has been already sent to given destination, and if yes - logs such information.

=cut

sub segment_already_sent {
    my $self = shift;
    my ( $type, $dst ) = @_;
    return unless $self->{ 'state' }->{ 'sent' }->{ $type };
    return unless $self->{ 'state' }->{ 'sent' }->{ $type }->{ $dst->{ 'path' } };
    $self->log->log( 'Segment already sent to %s. Skipping.', $dst->{ 'path' } );
    return 1;
}

=head1 cleanup()

Function is called only if segment has been successfully compressed and sent to all destinations.

It basically removes tempdir with compressed copies of segment, and state file for given segment.

=cut

sub cleanup {
    my $self = shift;
    rmtree( $self->{ 'temp-dir' } );
    unlink $self->{ 'state-file' } if $self->{ 'state-file' };
    return;
}

=head1 make_all_necessary_compressions()

Wraps all work required to compress segment to all necessary formats.

Call to actuall compressor has to be done via "bash -c" to be able to easily use run_command() function which has side benefits of getting stdout, stderr, and proper fetching error codes.

Overhead of additional fork+exec for bash should be negligible.

=cut

sub make_all_necessary_compressions {
    my $self = shift;
    $self->get_list_of_all_necessary_compressions();

    for my $compression ( @{ $self->{ 'compressions' } } ) {
        next if 'none' eq $compression;
        next if $self->segment_already_compressed( $compression );

        my $compressed_filename = $self->get_temp_filename_for( $compression );

        my $compressor_binary = $self->{ $compression . '-path' } || $compression;

        my $compression_command = sprintf '%s --stdout %s > %s', $compressor_binary, quotemeta( $self->{ 'segment' } ), quotemeta( $compressed_filename );
        unless ( $self->{ 'not-nice' } ) {
            $compression_command = quotemeta( $self->{ 'nice-path' } ) . ' ' . $compression_command;
        }

        $self->log->time_start( 'Compressing with ' . $compression ) if $self->verbose;
        my $response = run_command( $self->{ 'temp-dir' }, 'bash', '-c', $compression_command );
        $self->log->time_finish( 'Compressing with ' . $compression ) if $self->verbose;

        if ( $response->{ 'error_code' } ) {
            $self->log->fatal( 'Error while compressing with %s : %s', $compression, $response );
        }

        $self->{ 'state' }->{ 'compressed' }->{ $compression } = file_md5sum( $compressed_filename );
        $self->save_state();
    }
    return;
}

=head1 segment_already_compressed()

Helper function which checks if segment has been already compressed.

It uses state file, and checks compressed file md5sum to be sure that the file wasn't damaged between prior run and now.

=cut

sub segment_already_compressed {
    my $self = shift;
    my $type = shift;
    return unless $self->{ 'state' }->{ 'compressed' }->{ $type };
    my $want_md5 = $self->{ 'state' }->{ 'compressed' }->{ $type };

    my $temp_file_name = $self->get_temp_filename_for( $type );
    return unless -e $temp_file_name;

    my $has_md5 = file_md5sum( $temp_file_name );
    if ( $has_md5 eq $want_md5 ) {
        $self->log->log( 'Segment has been already compressed with %s.', $type );
        return 1;
    }

    unlink $temp_file_name;
    $self->log->error( 'Segment already compressed to %s, but with bad MD5 (file: %s, state: %s), recompressing.', $type, $has_md5, $want_md5 );

    return;
}

=head1 get_temp_filename_for()

Helper function to build full (with path) filename for compressed segment, assuming given compression.

=cut

sub get_temp_filename_for {
    my $self = shift;
    my $type = shift;

    return File::Spec->catfile( $self->{ 'temp-dir' }, basename( $self->{ 'segment' } ) . ext_for_compression( $type ) );
}

=head1 read_state()

Helper function to read state from state file.

Name of state file is the same as filename of WAL segment being archived, but it is in state-dir.

=cut

sub read_state {
    my $self = shift;
    $self->{ 'state' } = {};

    return unless $self->{ 'state-dir' };

    $self->{ 'state-file' } = File::Spec->catfile( $self->{ 'state-dir' }, basename( $self->{ 'segment' } ) );
    return unless -f $self->{ 'state-file' };
    $self->{ 'state' } = retrieve( $self->{ 'state-file' } );
    return;
}

=head1 save_state()

Helper function to save state to state-file.

=cut

sub save_state {
    my $self = shift;

    return unless $self->{ 'state-file' };

    store( $self->{ 'state' }, $self->{ 'state-file' } );

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
        'data-dir'      => '.',
        'temp-dir'      => $ENV{ 'TMPDIR' } || '/tmp',
        'gzip-path'     => 'gzip',
        'bzip2-path'    => 'bzip2',
        'lzma-path'     => 'lzma',
        'rsync-path'    => 'rsync',
        'nice-path'     => 'nice',
        'parallel-jobs' => 1,
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-archive.pod' )
        unless GetOptions(
        \%args,
        'bzip2-path|bp=s',
        'data-dir|D=s',
        'dst-backup|db=s',
        'dst-local|dl=s@',
        'dst-remote|dr=s@',
        'force-data-dir|f',
        'gzip-path|gp=s',
        'log|l=s',
        'lzma-path|lp=s',
        'rsync-path|rp=s',
        'pid-file=s',
        'state-dir|s=s',
        'temp-dir|t=s',
        'nice-path|np=s',
        'parallel-jobs|PJ=i',
        'verbose|v',
        'not-nice|nn',
        );

    croak( '--log was not provided - cannot continue.' ) unless $args{ 'log' };
    $args{ 'log' } =~ tr/^/%/;

    for my $key ( qw( data-dir dst-backup temp-dir state-dir pid-file verbose gzip-path bzip2-path lzma-path nice-path force-data-dir rsync-path not-nice parallel-jobs ) ) {
        $self->{ $key } = $args{ $key };
    }

    for my $type ( qw( local remote ) ) {
        my $D = [];
        $self->{ 'destination' }->{ $type } = $D;

        next unless defined $args{ 'dst-' . $type };

        my %temp_for_uniq = ();
        my @items = grep { !$temp_for_uniq{ $_ }++ } @{ $args{ 'dst-' . $type } };

        for my $item ( @items ) {
            my $current = { 'compression' => 'none', };
            if ( $item =~ s/\A(gzip|bzip2|lzma)=// ) {
                $current->{ 'compression' } = $1;
            }
            $current->{ 'path' } = $item;
            push @{ $D }, $current;
        }
    }

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

    # These could theoretically go into validation, but we need to check if we can get anything to {'segment'}
    $self->log->fatal( 'WAL segment file name has not been given' ) if 0 == scalar @ARGV;
    $self->log->fatal( 'More than 1 WAL segment file name has been given' ) if 1 < scalar @ARGV;

    $self->{ 'segment' } = shift @ARGV;

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->verbose;

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for compression programs paths - technically, it could be validated in here, but benefit would be pretty limited, and code to do so relatively complex, as compression program path
might, but doesn't have to be actual file path - it might be just program name (without path), which is the default.

=cut

sub validate_args {
    my $self = shift;

    unless ( $self->{ 'force-data-dir' } ) {
        $self->log->fatal( "Given data-dir (%s) is not valid", $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' } && -f File::Spec->catfile( $self->{ 'data-dir' }, 'PG_VERSION' );
    }

    if ( $self->{ 'dst-backup' } ) {
        if ( $self->{ 'dst-backup' } =~ m{\A(gzip|bzip2|lzma)=} ) {
            $self->log->fatal( 'dst-backup cannot be compressed! [%]', $self->{ 'dst-backup' } );
        }
        unless ( $self->{ 'dst-backup' } =~ m{\A/} ) {
            $self->log->fatal( 'dst-backup has to be absolute path, and it is not: %s', $self->{ 'dst-backup' } );
        }
        if ( -e $self->{ 'dst-backup' } ) {
            push @{ $self->{ 'destination' }->{ 'local' } },
                {
                'compression' => 'none',
                'path'        => $self->{ 'dst-backup' },
                };
        }
    }

    my $dst_count = scalar( @{ $self->{ 'destination' }->{ 'local' } } ) + scalar( @{ $self->{ 'destination' }->{ 'remote' } } );
    $self->log->fatal( "No --dst-* has been provided!" ) if 0 == $dst_count;

    if ( 1 < $dst_count ) {
        $self->log->fatal( "More than 1 --dst-* has been provided, but no --state-dir!" ) if !$self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) does not exist",     $self->{ 'state-dir' } ) unless -e $self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) is not a directory", $self->{ 'state-dir' } ) unless -d $self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) is not writable",    $self->{ 'state-dir' } ) unless -w $self->{ 'state-dir' };
    }

    $self->log->fatal( 'Given segment name is not valid (%s)', $self->{ 'segment' } )
        unless basename( $self->{ 'segment' } ) =~ m{\A(?:[a-fA-F0-9]{24}(?:\.[a-fA-F0-9]{8}\.backup)?|[a-fA-F0-9]{8}\.history)\z};
    my $segment_file_name = $self->{ 'segment' };
    $segment_file_name = File::Spec->catfile( $self->{ 'data-dir' }, $self->{ 'segment' } ) unless $self->{ 'segment' } =~ m{^/};

    $self->log->fatal( 'Given segment (%s) does not exist.',  $segment_file_name ) unless -e $segment_file_name;
    $self->log->fatal( 'Given segment (%s) is not a file.',   $segment_file_name ) unless -f $segment_file_name;
    $self->log->fatal( 'Given segment (%s) is not readable.', $segment_file_name ) unless -r $segment_file_name;

    if ( $self->{ 'segment' } =~ m{\A[a-fA-F0-9]{24}\z} ) {
        my $expected_size = 256**3;
        my $file_size     = ( -s $segment_file_name );
        $self->log->fatal( 'Given segment (%s) has incorrect size (%u vs %u).', $segment_file_name, $file_size, $expected_size ) unless $expected_size == $file_size;
    }
    $self->log->fatal( 'Parallel jobs value not given?!' ) unless defined $self->{ 'parallel-jobs' };
    $self->log->fatal( 'Parallel jobs is not integer (%s)', $self->{ 'parallel-jobs' } ) unless $self->{ 'parallel-jobs' } =~ m{\A\d+\z};
    $self->log->fatal( 'Parallel jobs is not >= 1 (%s)', $self->{ 'parallel-jobs' } ) unless $self->{ 'parallel-jobs' } >= 1;

    $self->{ 'segment' } = $segment_file_name;
    return;
}

1;
