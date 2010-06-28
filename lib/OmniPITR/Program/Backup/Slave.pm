package OmniPITR::Program::Backup::Slave;
use strict;
use warnings;

use base qw( OmniPITR::Program );

use File::Spec;
use File::Basename;
use Cwd;
use English qw( -no_match_vars );
use File::Copy;
use File::Path;
use Getopt::Long;
use Carp;
use POSIX qw( strftime );
use Sys::Hostname;
use OmniPITR::Tools qw( run_command ext_for_compression );

sub run {
    my $self = shift;
    $self->get_list_of_all_necessary_compressions();
    $self->choose_base_local_destinations();

    $self->pause_xlog_removal();

    $self->get_initial_pg_control_data();

    $self->compress_pgdata();

    $self->wait_for_checkpoint_location_change();

    $self->compress_xlogs();

    $self->unpause_xlog_removal();

    $self->deliver_to_all_destinations();
}

=head1 deliver_to_all_destinations()

Simple wrapper to have single point to call to deliver backups to all requested backups.

=cut

sub deliver_to_all_destinations {
    my $self = shift;

    $self->deliver_to_all_local_destinations();

    $self->deliver_to_all_remote_destinations();

    return;
}

=head1 deliver_to_all_local_destinations()

Copies backups to all local destinations which are not also base destinations for their respective compressions.

=cut

sub deliver_to_all_local_destinations {
    my $self = shift;
    return unless $self->{ 'destination' }->{ 'local' };
    for my $dst ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        next if $dst->{ 'path' } eq $self->{ 'base' }->{ $dst->{ 'compression' } };

        my $B = $self->{ 'base' }->{ $dst->{ 'compression' } };

        for my $type ( qw( data xlog ) ) {

            my $filename = $self->get_archive_filename( $type, $dst->{ 'compression' } );
            my $source_filename = File::Spec->catfile( $B, $filename );
            my $destination_filename = File::Spec->catfile( $dst->{ 'path' }, $filename );

            my $time_msg = sprintf 'Copying %s to %s', $source_filename, $destination_filename;
            $self->log->time_start( $time_msg ) if $self->verbose;

            my $rc = copy( $source_filename, $destination_filename );

            $self->log->time_finish( $time_msg ) if $self->verbose;

            unless ( $rc ) {
                $self->log->error( 'Cannot copy %s to %s : %s', $source_filename, $destination_filename, $OS_ERROR );
                $self->{ 'had_errors' } = 1;
            }

        }
    }
    return;
}

=head1 deliver_to_all_remote_destinations()

Delivers backups to remote destinations using rsync program.

=cut

sub deliver_to_all_remote_destinations {
    my $self = shift;
    return unless $self->{ 'destination' }->{ 'remote' };
    for my $dst ( @{ $self->{ 'destination' }->{ 'remote' } } ) {

        my $B = $self->{ 'base' }->{ $dst->{ 'compression' } };

        for my $type ( qw( data xlog ) ) {

            my $filename = $self->get_archive_filename( $type, $dst->{ 'compression' } );
            my $source_filename = File::Spec->catfile( $B, $filename );
            my $destination_filename = $dst->{ 'path' };
            $destination_filename =~ s{/*\z}{/};
            $destination_filename .= $filename;

            my $time_msg = sprintf 'Copying %s to %s', $source_filename, $destination_filename;
            $self->log->time_start( $time_msg ) if $self->verbose;

            my $response = run_command( $self->{ 'temp-dir' }, $self->{ 'rsync-path' }, $source_filename, $destination_filename );

            $self->log->time_finish( $time_msg ) if $self->verbose;

            if ( $response->{ 'error_code' } ) {
                $self->log->error( 'Cannot send archive %s to %s: %s', $source_filename, $destination_filename, $response );
                $self->{ 'had_errors' } = 1;
            }
        }
    }
    return;
}

sub compress_xlogs {
    my $self = shift;

    $self->make_dot_backup_file();
    $self->uncompress_wal_archive_segments();

    $self->log->time_start( 'Compressing xlogs' ) if $self->verbose;
    $self->start_writers( 'xlog' );

    my $source_transform_from = basename( $self->{ 'source' }->{ 'path' } );
    $source_transform_from =~ s{^/*}{};
    $source_transform_from =~ s{/*$}{};

    my $dot_backup_transform_from = $self->{ 'temp-dir' };
    $dot_backup_transform_from =~ s{^/*}{};
    $dot_backup_transform_from =~ s{/*$}{};

    my $transform_to = basename( $self->{ 'data-dir' } ) . '/pg_xlog';
    my $transform_command = sprintf 's#^\(%s\|%s\)#%s#', $source_transform_from, $dot_backup_transform_from, $transform_to;

    $self->tar_and_compress(
        'work_dir'  => dirname( $self->{ 'source' }->{ 'path' } ),
        'tar_dir'   => [ basename( $self->{ 'source' }->{ 'path' } ), File::Spec->catfile( $self->{ 'temp-dir' }, $self->{ 'dot_backup_filename' } ), ],
        'transform' => [ $transform_command ],
    );

    $self->log->time_finish( 'Compressing xlogs' ) if $self->verbose;

    return;
}

sub uncompress_wal_archive_segments {
    my $self = shift;
    return if 'none' eq $self->{ 'source' }->{ 'compression' };

    my $old_source = $self->{ 'source' }->{ 'path' };
    my $new_source = File::Spec->catfile( $self->{ 'temp-dir' }, 'uncompresses_pg_xlogs' );
    $self->{ 'source' }->{ 'path' } = $new_source;

    mkpath( [ $new_source ], 0, oct( "755" ) );

    opendir my $dir, $old_source or $self->log->fatal( 'Cannot open wal-archive (%s) : %s', $old_source, $OS_ERROR );
    my $extension = ext_for_compression( $self->{ 'source' }->{ 'compression' } );
    my @wal_segments = sort grep { -f File::Spec->catfile( $old_source, $_ ) && /\Q$extension\E\z/ } readdir( $dir );
    close $dir;

    $self->log->log( '%s wal segments have to be uncompressed', scalar @wal_segments );

    for my $segment ( @wal_segments ) {
        my $old_file = File::Spec->catfile( $old_source, $segment );
        my $new_file = File::Spec->catfile( $new_source, $segment );
        copy( $old_file, $new_file ) or $self->log->fatal( 'Cannot copy %s to %s: %s', $old_file, $new_file, $OS_ERROR );
        $self->log->log( 'File copied: %s -> %s', $old_file, $new_file );
        my $response = run_command( $self->{ 'temp-dir' }, $self->{ 'nice-path' }, $self->{ $self->{ 'source' }->{ 'compression' } . '-path' }, '-d', $new_file );
        if ( $response->{ 'error_code' } ) {
            $self->log->fatal( 'Error while uncompressing wal segment %s: %s', $new_file, $response );
        }
    }
    return;
}

sub make_dot_backup_file {
    my $self = shift;

    my $redo_location  = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's REDO location" };
    my $final_location = $self->{ 'CONTROL' }->{ 'final' }->{ "Latest checkpoint location" };
    my $timeline       = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's TimeLineID" };
    my $offset         = $redo_location;
    $offset =~ s#.*/##;
    $offset =~ s/^.*?(.{0,6})$/$1/;

    my $output_filename = sprintf '%s.%08s.backup', $self->convert_wal_location_and_timeline_to_filename( $redo_location, $timeline ), $offset;

    my @content_lines = @{ $self->{ 'backup_file_data' } };
    splice( @content_lines, 1, 0, sprintf 'STOP WAL LOCATION: %s (file %s)', $final_location, $self->convert_wal_location_and_timeline_to_filename( $final_location, $timeline ) );
    splice( @content_lines, 4, 0, sprintf 'START TIME: %s', strftime( '%Y-%m-%d %H:%M:%S %Z', localtime time ) );

    my $content = join( "\n", @content_lines ) . "\n";

    my $filename = File::Spec->catfile( $self->{ 'temp-dir' }, $output_filename );
    if ( open my $fh, '>', $filename ) {
        print $fh $content;
        close $fh;
        $self->{ 'dot_backup_filename' } = $output_filename;
        return;
    }
    $self->log->fatal( 'Cannot write .backup file file %s : %s', $output_filename, $OS_ERROR );
}

sub wait_for_checkpoint_location_change {
    my $self     = shift;
    my $pre_wait = $self->get_control_data()->{ 'Latest checkpoint location' };
    $self->log->log( 'Waiting for checkpoint' ) if $self->verbose;
    while ( 1 ) {
        sleep 5;
        $self->{ 'CONTROL' }->{ 'final' } = $self->get_control_data();
        last if $self->{ 'CONTROL' }->{ 'final' }->{ 'Latest checkpoint location' } ne $pre_wait;
    }
    $self->log->log( 'Checkpoint .' ) if $self->verbose;
    return;
}

sub make_backup_label_temp_file {
    my $self = shift;

    my $redo_location = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's REDO location" };
    my $last_location = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint location" };
    my $timeline      = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's TimeLineID" };

    my @content_lines = ();
    push @content_lines, sprintf 'START WAL LOCATION: %s (file %s)', $redo_location, $self->convert_wal_location_and_timeline_to_filename( $redo_location, $timeline );
    push @content_lines, sprintf 'CHECKPOINT LOCATION: %s', $last_location;
    push @content_lines, sprintf 'START TIME: %s', strftime( '%Y-%m-%d %H:%M:%S %Z', localtime time );
    push @content_lines, 'LABEL: OmniPITR_Slave_Hot_Backup';

    $self->{ 'backup_file_data' } = \@content_lines;
    my $content = join( "\n", @content_lines ) . "\n";

    my $filename = File::Spec->catfile( $self->{ 'temp-dir' }, 'backup_label' );
    if ( open my $fh, '>', $filename ) {
        print $fh $content;
        close $fh;
        return;
    }
    $self->log->fatal( 'Cannot write backup_label file %s : %s', $filename, $OS_ERROR );
}

sub convert_wal_location_and_timeline_to_filename {
    my $self = shift;
    my ( $location, $timeline ) = @_;

    my ( $series, $offset ) = split m{/}, $location;

    $offset =~ s/.{0,6}$//;

    my $location_filename = sprintf '%08s%08s%08s', $timeline, $series, $offset;

    return $location_filename;
}

=head1 get_archive_filename()

Helper function, which takes filetype and compression schema to use, and returns generated filename (based on filename-template command line option).

=cut

sub get_archive_filename {
    my $self = shift;
    my ( $type, $compression ) = @_;

    my $ext = $compression eq 'none' ? '' : ext_for_compression( $compression );

    my $filename = $self->{ 'filename-template' };
    $filename =~ s/__FILETYPE__/$type/g;
    $filename =~ s/__CEXT__/$ext/g;

    return $filename;
}

=head1 start_writers()

Starts set of filehandles, which write to file, or to compression program, to create final archives.

Each compression schema gets its own filehandle, and printing data to it, will pass it to file directly or through compression program that has been chosen based on command line arguments.

=cut

sub start_writers {
    my $self      = shift;
    my $data_type = shift;

    my %writers = ();

    COMPRESSION:
    while ( my ( $type, $dst_path ) = each %{ $self->{ 'base' } } ) {
        my $filename = $self->get_archive_filename( $data_type, $type );

        my $full_file_path = File::Spec->catfile( $dst_path, $filename );

        if ( $type eq 'none' ) {
            if ( open my $fh, '>', $full_file_path ) {
                $writers{ $type } = $fh;
                $self->log->log( "Starting \"none\" writer to $full_file_path" ) if $self->verbose;
                next COMPRESSION;
            }
            $self->clean_and_die( 'Cannot write to %s : %s', $full_file_path, $OS_ERROR );
        }

        my @command = map { quotemeta $_ } ( $self->{ 'nice-path' }, $self->{ $type . '-path' }, '--stdout', '-' );
        push @command, ( '>', quotemeta( $full_file_path ) );

        $self->log->log( "Starting \"%s\" writer to %s", $type, $full_file_path ) if $self->verbose;
        if ( open my $fh, '|-', join( ' ', @command ) ) {
            $writers{ $type } = $fh;
            next COMPRESSION;
        }
        $self->clean_and_die( 'Cannot open command. Error: %s, Command: %s', $OS_ERROR, \@command );
    }
    $self->{ 'writers' } = \%writers;
    return;
}

sub compress_pgdata {
    my $self = shift;

    $self->make_backup_label_temp_file();

    $self->log->time_start( 'Compressing $PGDATA' ) if $self->verbose;
    $self->start_writers( 'data' );

    my $transform_from = $self->{ 'temp-dir' };
    $transform_from =~ s{^/*}{};
    $transform_from =~ s{/*$}{};
    my $transform_to = basename( $self->{ 'data-dir' } );
    my $transform_command = sprintf 's#^%s/#%s/#', $transform_from, $transform_to;

    my @excludes = qw( pg_log/* pg_xlog/0* pg_xlog/archive_status/* recovery.conf postmaster.pid );
    for my $dir ( qw( pg_log pg_xlog ) ) {
        push @excludes, $dir if -l File::Spec->catfile( $self->{ 'data-dir' }, $dir );
    }

    $self->tar_and_compress(
        'work_dir'  => dirname( $self->{ 'data-dir' } ),
        'tar_dir'   => [ basename( $self->{ 'data-dir' } ), File::Spec->catfile( $self->{ 'temp-dir' }, 'backup_label' ) ],
        'excludes'  => [ map { sprintf( '%s/%s', basename( $self->{ 'data-dir' } ), $_ ) } @excludes ],
        'transform' => [ $transform_command ],
    );

    $self->log->time_finish( 'Compressing $PGDATA' ) if $self->verbose;
    return;
}

=head1 tar_and_compress()

Worker function which does all of the actual tar, and sending data to compression filehandles.

Takes hash (not hashref) as argument, and uses following keys from it:

=over

=item * tar_dir - arrayref with list of directories to compress

=item * work_dir - what should be current working directory when executing tar

=item * excludes - optional key, that (if exists) is treated as arrayref of shell globs (tar dir) of items to exclude from backup

=item * transform - optional key, that (if exists) is treated as arrayref of values for --transform option for tar

=back

If tar will print anything to STDERR it will be logged. Error status code is ignored, as it is expected that tar will generate errors (due to files modified while archiving).

=cut

sub tar_and_compress {
    my $self = shift;
    my %ARGS = @_;

    $SIG{ 'PIPE' } = sub { $self->clean_and_die( 'Got SIGPIPE while tarring %s for %s', $ARGS{ 'tar_dir' }, $self->{ 'sigpipeinfo' } ); };

    my @compression_command = ( $self->{ 'nice-path' }, $self->{ 'tar-path' }, 'cf', '-' );
    if ( $ARGS{ 'excludes' } ) {
        push @compression_command, map { '--exclude=' . $_ } @{ $ARGS{ 'excludes' } };
    }
    if ( $ARGS{ 'transform' } ) {
        push @compression_command, map { '--transform=' . $_ } @{ $ARGS{ 'transform' } };
    }
    push @compression_command, @{ $ARGS{ 'tar_dir' } };

    my $compression_str = join ' ', map { quotemeta $_ } @compression_command;

    $self->prepare_temp_directory();
    my $tar_stderr_filename = File::Spec->catfile( $self->{ 'temp-dir' }, 'tar.stderr' );
    $compression_str .= ' 2> ' . quotemeta( $tar_stderr_filename );

    my $previous_dir = getcwd;
    chdir $ARGS{ 'work_dir' } if $ARGS{ 'work_dir' };

    my $tar;
    unless ( open $tar, '-|', $compression_str ) {
        $self->clean_and_die( 'Cannot start tar (%s) : %s', $compression_str, $OS_ERROR );
    }

    chdir $previous_dir if $ARGS{ 'work_dir' };

    my $buffer;
    while ( my $len = sysread( $tar, $buffer, 8192 ) ) {
        while ( my ( $type, $fh ) = each %{ $self->{ 'writers' } } ) {
            $self->{ 'sigpipeinfo' } = $type;
            my $written = syswrite( $fh, $buffer, $len );
            next if $written == $len;
            $self->clean_and_die( "Writting %u bytes to filehandle for <%s> compression wrote only %u bytes ?!", $len, $type, $written );
        }
    }
    close $tar;

    for my $fh ( values %{ $self->{ 'writers' } } ) {
        close $fh;
    }

    delete $self->{ 'writers' };

    my $stderr_output;
    my $stderr;
    unless ( open $stderr, '<', $tar_stderr_filename ) {
        $self->log->log( 'Cannot open tar stderr file (%s) for reading: %s', $tar_stderr_filename );
        return;
    }
    {
        local $/;
        $stderr_output = <$stderr>;
    };
    close $stderr;
    return unless $stderr_output;
    $self->log->log( 'Tar (%s) generated these output on stderr:', $compression_str );
    $self->log->log( '==============================================' );
    $self->log->log( '%s', $stderr_output );
    $self->log->log( '==============================================' );
    unlink $tar_stderr_filename;
    return;
}

=head1 get_control_data()

Calls pg_controldata, and parses its output.

Verifies that output contains 2 critical pieces of information:

=over

=item * Latest checkpoint's REDO location

=item * Latest checkpoint's TimeLineID

=back

=cut

sub get_control_data {
    my $self = shift;

    $self->prepare_temp_directory();

    my $response = run_command( $self->{ 'temp-dir' }, $self->{ 'pgcontroldata-path' }, $self->{ 'data-dir' } );
    if ( $response->{ 'error_code' } ) {
        $self->log->fatal( 'Error while getting pg_controldata for %s: %s', $self->{ 'data-dir' }, $response );
    }

    my $control_data = {};

    my @lines = split( /\s*\n/, $response->{ 'stdout' } );
    for my $line ( @lines ) {
        unless ( $line =~ m{\A([^:]+):\s*(.*)\z} ) {
            $self->log->fatal( 'Pg_controldata for %s contained unparseable line: [%s]', $self->{ 'data-dir' }, $line );
        }
        $control_data->{ $1 } = $2;
    }

    unless ( $control_data->{ "Latest checkpoint's REDO location" } ) {
        $self->log->fatal( 'Pg_controldata for %s did not contain latest checkpoint redo location', $self->{ 'data-dir' } );
    }
    unless ( $control_data->{ "Latest checkpoint's TimeLineID" } ) {
        $self->log->fatal( 'Pg_controldata for %s did not contain latest checkpoint timeline ID', $self->{ 'data-dir' } );
    }

    return $control_data;
}

sub get_initial_pg_control_data {
    my $self = shift;

    $self->{ 'CONTROL' }->{ 'initial' } = $self->get_control_data();

    return;
}

sub pause_xlog_removal {
    my $self = shift;

    if ( open my $fh, '>', $self->{ 'removal-pause-trigger' } ) {
        print $fh $PROCESS_ID, "\n";
        close $fh;
        $self->{ 'removal-pause-trigger-created' } = 1;
        return;
    }
    $self->log->fatal(
        'Cannot create/write to removal pause trigger (%s) : %S',
        $self->{ 'removal-pause-trigger' },
        $OS_ERROR
    );
}

sub unpause_xlog_removal {
    my $self = shift;
    unlink( $self->{ 'removal-pause-trigger' } ) if $self->{ 'removal-pause-trigger-created' };
    delete $self->{ 'removal-pause-trigger-created' };
    return;
}

=head1 DESTROY()

Destroctor for object - removes temp directory on program exit.

=cut

sub DESTROY {
    my $self = shift;
    unlink( $self->{ 'removal-pause-trigger' } ) if $self->{ 'removal-pause-trigger-created' };
    rmtree( [ $self->{ 'temp-dir-prepared' } ], 0 ) if $self->{ 'temp-dir-prepared' };
    return;
}

=head1 prepare_temp_directory()

Helper function, which builds path for temp directory, and creates it.

Path is generated by using given temp-dir and 'omnipitr-backup-master' named.

For example, for temp-dir '/tmp' used temp directory would be /tmp/omnipitr-backup-master.

=cut

sub prepare_temp_directory {
    my $self = shift;
    return if $self->{ 'temp-dir-prepared' };
    my $full_temp_dir = File::Spec->catfile( $self->{ 'temp-dir' }, basename( $PROGRAM_NAME ) );
    mkpath( $full_temp_dir );
    $self->{ 'temp-dir' }          = $full_temp_dir;
    $self->{ 'temp-dir-prepared' } = $full_temp_dir;
    return;
}

=head1 choose_base_local_destinations()

Chooses single local destination for every compression schema required by destinations specifications.

In case some compression schema exists only for remote destination, local temp directory is created in --temp-dir location.

=cut

sub choose_base_local_destinations {
    my $self = shift;

    my $base = { map { ( $_ => undef ) } @{ $self->{ 'compressions' } } };
    $self->{ 'base' } = $base;

    for my $dst ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        my $type = $dst->{ 'compression' };
        next if defined $base->{ $type };
        $base->{ $type } = $dst->{ 'path' };
    }

    my @unfilled = grep { !defined $base->{ $_ } } keys %{ $base };

    return if 0 == scalar @unfilled;
    $self->log->log( 'These compression(s) were given only for remote destinations. Usually this is not desired: %s', join( ', ', @unfilled ) );

    $self->prepare_temp_directory();
    for my $type ( @unfilled ) {
        my $tmp_dir = File::Spec->catfile( $self->{ 'temp-dir' }, $type );
        mkpath( $tmp_dir );
        $base->{ $type } = $tmp_dir;
    }

    return;
}

=head1 get_list_of_all_necessary_compressions()

Scans list of destinations, and gathers list of all compressions that have to be made.

This is to be able to compress file only once even when having multiple destinations that require compressed format.

=cut

sub get_list_of_all_necessary_compressions {
    my $self = shift;

    my %compression = ();

    for my $dst_type ( qw( local remote ) ) {
        next unless my $dsts = $self->{ 'destination' }->{ $dst_type };
        for my $destination ( @{ $dsts } ) {
            $compression{ $destination->{ 'compression' } } = 1;
        }
    }
    $self->{ 'compressions' } = [ keys %compression ];
    return;
}

=head1 read_args()

Function which does all the parsing, and transformation of command line arguments.

=cut

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my %args = (
        'temp-dir' => $ENV{ 'TMPDIR' } || '/tmp',
        'gzip-path'          => 'gzip',
        'bzip2-path'         => 'bzip2',
        'lzma-path'          => 'lzma',
        'tar-path'           => 'tar',
        'nice-path'          => 'nice',
        'rsync-path'         => 'rsync',
        'pgcontroldata-path' => 'pg_controldata',
        'filename-template'  => '__HOSTNAME__-__FILETYPE__-^Y-^m-^d.tar__CEXT__',
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-backup-slave.pod' )
        unless GetOptions(
        \%args,
        'data-dir|D=s',
        'source|s=s',
        'dst-local|dl=s@',
        'dst-remote|dr=s@',
        'temp-dir|t=s',
        'log|l=s',
        'filename-template|f=s',
        'removal-pause-trigger|p=s',
        'pid-file',
        'verbose|v',
        'gzip-path|gp=s',
        'bzip2-path|bp=s',
        'lzma-path|lp=s',
        'nice-path|np=s',
        'tar-path|tp=s',
        'rsync-path|rp=s',
        'pgcontroldata-path|pp=s',
        );

    croak( '--log was not provided - cannot continue.' ) unless $args{ 'log' };
    for my $key ( qw( log filename-template ) ) {
        $args{ $key } =~ tr/^/%/;
    }

    for my $key ( grep { !/^dst-(?:local|remote)$/ } keys %args ) {
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

    if ( $args{ 'source' } =~ s/\A(gzip|bzip2|lzma)=// ) {
        $self->{ 'source' } = {
            'compression' => $1,
            'path'        => $args{ 'source' },
        };
    }
    else {
        $self->{ 'source' } = {
            'compression' => 'none',
            'path'        => $args{ 'source' },
        };
    }

    $self->{ 'filename-template' } = strftime( $self->{ 'filename-template' }, localtime time() );
    $self->{ 'filename-template' } =~ s/__HOSTNAME__/hostname()/ge;

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

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

    $self->log->fatal( 'Data-dir was not provided!' ) unless defined $self->{ 'data-dir' };
    $self->log->fatal( 'Provided data-dir (%s) does not exist!',   $self->{ 'data-dir' } ) unless -e $self->{ 'data-dir' };
    $self->log->fatal( 'Provided data-dir (%s) is not directory!', $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' };
    $self->log->fatal( 'Provided data-dir (%s) is not readable!',  $self->{ 'data-dir' } ) unless -r $self->{ 'data-dir' };

    my $dst_count = scalar( @{ $self->{ 'destination' }->{ 'local' } } ) + scalar( @{ $self->{ 'destination' }->{ 'remote' } } );
    $self->log->fatal( "No --dst-* has been provided!" ) if 0 == $dst_count;

    $self->log->fatal( "Filename template does not contain __FILETYPE__ placeholder!" ) unless $self->{ 'filename-template' } =~ /__FILETYPE__/;
    $self->log->fatal( "Filename template cannot contain / or \\ characters!" ) if $self->{ 'filename-template' } =~ m{[/\\]};

    $self->log->fatal( 'Source of WAL files was not provided!' ) unless defined $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Provided source of wal files (%s) does not exist!',   $self->{ 'source' }->{ 'path' } ) unless -e $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Provided source of wal files (%s) is not directory!', $self->{ 'source' }->{ 'path' } ) unless -d $self->{ 'source' }->{ 'path' };
    $self->log->fatal( 'Provided source of wal files (%s) is not readable!',  $self->{ 'source' }->{ 'path' } ) unless -r $self->{ 'source' }->{ 'path' };

    $self->log->fatal( 'Temp-dir was not provided!' ) unless defined $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) does not exist!',   $self->{ 'temp-dir' } ) unless -e $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) is not directory!', $self->{ 'temp-dir' } ) unless -d $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) is not writable!',  $self->{ 'temp-dir' } ) unless -w $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) contains # character!', $self->{ 'temp-dir' } ) if $self->{ 'temp-dir' } =~ /#/;

    $self->log->fatal( 'Removal pause trigger name was not provided!' ) unless defined $self->{ 'removal-pause-trigger' };
    $self->log->fatal( 'Provided removal pause trigger file (%s) already exists!', $self->{ 'removal-pause-trigger' } ) if -e $self->{ 'removal-pause-trigger' };

    $self->log->fatal( 'Directory for provided removal pause trigger (%s) does not exist!',   $self->{ 'removal-pause-trigger' } ) unless -e dirname( $self->{ 'removal-pause-trigger' } );
    $self->log->fatal( 'Directory for provided removal pause trigger (%s) is not directory!', $self->{ 'removal-pause-trigger' } ) unless -d dirname( $self->{ 'removal-pause-trigger' } );
    $self->log->fatal( 'Directory for provided removal pause trigger (%s) is not writable!',  $self->{ 'removal-pause-trigger' } ) unless -w dirname( $self->{ 'removal-pause-trigger' } );

    return unless $self->{ 'destination' }->{ 'local' };

    for my $d ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        my $dir = $d->{ 'path' };
        $self->log->fatal( 'Choosen local destination dir (%s) does not exist. Cannot continue.',   $dir ) unless -e $dir;
        $self->log->fatal( 'Choosen local destination dir (%s) is not directory. Cannot continue.', $dir ) unless -d $dir;
        $self->log->fatal( 'Choosen local destination dir (%s) is not writable. Cannot continue.',  $dir ) unless -w $dir;
    }

    return;
}

1;
