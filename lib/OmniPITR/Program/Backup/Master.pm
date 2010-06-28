package OmniPITR::Program::Backup::Master;
use strict;
use warnings;

use base qw( OmniPITR::Program );

use Carp;
use OmniPITR::Tools qw( :all );
use English qw( -no_match_vars );
use File::Basename;
use Sys::Hostname;
use POSIX qw( strftime );
use File::Spec;
use File::Path qw( mkpath rmtree );
use File::Copy;
use Storable;
use Cwd;
use Getopt::Long qw( :config no_ignore_case );

=head1 run()

Main function wrapping all work.

Starts with getting list of compressions that have to be done, then it chooses where to compress to (important if we have remote-only destination), then it makes actual backup, and delivers to all
destinations.

=cut

sub run {
    my $self = shift;
    $self->get_list_of_all_necessary_compressions();
    $self->choose_base_local_destinations();

    $self->start_pg_backup();
    $self->compress_pgdata();

    $self->stop_pg_backup();
    $self->wait_for_final_xlog_and_remove_dst_backup();
    $self->compress_xlogs();

    $self->deliver_to_all_destinations();

    $self->log->log( 'All done%s.', $self->{ 'had_errors' } ? ' with errors' : '' );
    exit( 1 ) if $self->{ 'had_errors' };

    return;
}

=head1 wait_for_file()

Helper function which waits for file to appear.

It will return only if the file appeared.

Return value is name of file.

=cut

sub wait_for_file {
    my $self = shift;
    my ( $dir, $filename_regexp ) = @_;

    my $max_wait = 3600;    # It's 1 hour. There is no technical need to wait longer.
    for my $i ( 0 .. $max_wait ) {
        $self->log->log( 'Waiting for file matching %s in directory %s', $filename_regexp, $dir ) if 10 == $i;

        opendir( my $dh, $dir ) or $self->clean_and_die( 'Cannot open %s for scanning: %s', $dir, $OS_ERROR );
        my @matching = grep { $_ =~ $filename_regexp } readdir $dh;
        closedir $dh;

        if ( 0 == scalar @matching ) {
            sleep 1;
            next;
        }

        my $reply_filename = shift @matching;
        $self->log->log( 'File %s arrived after %u seconds.', $reply_filename, $i ) if $self->verbose;
        return $reply_filename;
    }

    $self->clean_and_die( 'Waited 1 hour for file matching %s, but it did not appear. Something is wrong. No sense in waiting longer.', $filename_regexp );

    return;
}

=head1 wait_for_final_xlog_and_remove_dst_backup()

In PostgreSQL < 8.4 pg_stop_backup() finishes before .backup "wal segment" is archived.

So we need to wait till it appears in backup xlog destination before we can remove symlink.

=cut

sub wait_for_final_xlog_and_remove_dst_backup {
    my $self = shift;

    my $backup_file = $self->wait_for_file( $self->{ 'xlogs' }, $self->{ 'stop_backup_filename_re' } );

    my $last_file = undef;

    open my $fh, '<', File::Spec->catfile( $self->{ 'xlogs' }, $backup_file ) or $self->clean_and_die( 'Cannot open backup file %s for reading: %s', $backup_file, $OS_ERROR );
    while ( my $line = <$fh> ) {
        next unless $line =~ m{\A STOP \s+ WAL \s+ LOCATION: .* file \s+ ( [0-9A-f]{24} ) }x;
        $last_file = qr{\A$1\z};
        last;
    }
    close $fh;

    $self->clean_and_die( '.backup file (%s) does not contain STOP WAL LOCATION line in recognizable format.', $backup_file ) unless $last_file;

    $self->wait_for_file( $self->{ 'xlogs' }, $last_file );

    unlink( $self->{ 'xlogs' } );
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

=head1 compress_xlogs()

Wrapper function which encapsulates all work required to compress xlog segments that accumulated during backup of data directory.

=cut

sub compress_xlogs {
    my $self = shift;
    $self->log->time_start( 'Compressing xlogs' ) if $self->verbose;
    $self->start_writers( 'xlog' );

    $self->tar_and_compress(
        'work_dir' => $self->{ 'xlogs' } . '.real',
        'tar_dir'  => basename( $self->{ 'data-dir' } ),
    );
    $self->log->time_finish( 'Compressing xlogs' ) if $self->verbose;
    rmtree( $self->{ 'xlogs' } . '.real', 0 );

    return;
}

=head1 compress_pgdata()

Wrapper function which encapsulates all work required to compress data directory.

=cut

sub compress_pgdata {
    my $self = shift;
    $self->log->time_start( 'Compressing $PGDATA' ) if $self->verbose;
    $self->start_writers( 'data' );

    my @excludes = qw( pg_log/* pg_xlog/0* pg_xlog/archive_status/* postmaster.pid );
    for my $dir ( qw( pg_log pg_xlog ) ) {
        push @excludes, $dir if -l File::Spec->catfile( $self->{ 'data-dir' }, $dir );
    }

    $self->tar_and_compress(
        'work_dir' => dirname( $self->{ 'data-dir' } ),
        'tar_dir'  => basename( $self->{ 'data-dir' } ),
        'excludes' => \@excludes,
    );

    $self->log->time_finish( 'Compressing $PGDATA' ) if $self->verbose;
    return;
}

=head1 tar_and_compress()

Worker function which does all of the actual tar, and sending data to compression filehandles.

Takes hash (not hashref) as argument, and uses following keys from it:

=over

=item * tar_dir - which directory to compress

=item * work_dir - what should be current working directory when executing tar

=item * excludes - optional key, that (if exists) is treated as arrayref of shell globs (tar dir) of items to exclude from backup

=back

If tar will print anything to STDERR it will be logged. Error status code is ignored, as it is expected that tar will generate errors (due to files modified while archiving).

=cut

sub tar_and_compress {
    my $self = shift;
    my %ARGS = @_;

    $SIG{ 'PIPE' } = sub { $self->clean_and_die( 'Got SIGPIPE while tarring %s for %s', $ARGS{ 'tar_dir' }, $self->{ 'sigpipeinfo' } ); };

    my @compression_command = ( $self->{ 'nice-path' }, $self->{ 'tar-path' }, 'cf', '-' );
    if ( $ARGS{ 'excludes' } ) {
        push @compression_command, map { sprintf '--exclude=%s/%s', $ARGS{ 'tar_dir' }, $_ } @{ $ARGS{ 'excludes' } };
    }
    push @compression_command, $ARGS{ 'tar_dir' };

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

=head1 stop_pg_backup()

Runs pg_stop_backup() PostgreSQL function, which is crucial in backup process.

This happens after data directory compression, but before compression of xlogs.

This function also removes temporary destination for xlogs (dst-backup for omnipitr-archive).

=cut

sub stop_pg_backup {
    my $self = shift;

    $self->prepare_temp_directory();

    my @command = ( @{ $self->{ 'psql' } }, "SELECT pg_stop_backup()" );

    $self->log->time_start( 'pg_stop_backup()' ) if $self->verbose;
    my $status = run_command( $self->{ 'temp-dir' }, @command );
    $self->log->time_finish( 'pg_stop_backup()' ) if $self->verbose;

    $self->clean_and_die( 'Running pg_stop_backup() failed: %s', $status ) if $status->{ 'error_code' };

    $status->{ 'stdout' } =~ s/\s*\z//;
    $self->log->log( q{pg_stop_backup('omnipitr') returned %s.}, $status->{ 'stdout' } );

    my $subdir = basename( $self->{ 'data-dir' } );

    return;
}

=head1 start_pg_backup()

Executes pg_start_backup() postgresql function, and (before it) creates temporary destination for xlogs (dst-backup for omnipitr-archive).

=cut

sub start_pg_backup {
    my $self = shift;

    my $subdir = basename( $self->{ 'data-dir' } );
    $self->clean_and_die( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . '.real',                 $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . '.real' );
    $self->clean_and_die( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . ".real/$subdir",         $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . ".real/$subdir" );
    $self->clean_and_die( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . ".real/$subdir/pg_xlog" );
    $self->clean_and_die( 'Cannot symlink %s to %s: %s', $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $self->{ 'xlogs' }, $OS_ERROR )
        unless symlink( $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $self->{ 'xlogs' } );

    $self->prepare_temp_directory();

    my @command = ( @{ $self->{ 'psql' } }, "SELECT pg_start_backup('omnipitr')" );

    $self->log->time_start( 'pg_start_backup()' ) if $self->verbose;
    my $status = run_command( $self->{ 'temp-dir' }, @command );
    $self->log->time_finish( 'pg_start_backup()' ) if $self->verbose;

    $self->clean_and_die( 'Running pg_start_backup() failed: %s', $status ) if $status->{ 'error_code' };

    $status->{ 'stdout' } =~ s/\s*\z//;
    $self->log->log( q{pg_start_backup('omnipitr') returned %s.}, $status->{ 'stdout' } );
    $self->clean_and_die( 'Ouput from pg_start_backup is not parseable?!' ) unless $status->{ 'stdout' } =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\z};

    my ( $part_1, $part_2 ) = ( $1, $2 );
    $part_2 =~ s/(.{1,6})\z//;
    my $part_3 = $1;

    my $expected_filename_suffix = sprintf '%08s%08s.%08s.backup', $part_1, $part_2, $part_3;
    my $backup_filename_re = qr{\A[0-9A-F]{8}\Q$expected_filename_suffix\E\z};

    $self->{ 'stop_backup_filename_re' } = $backup_filename_re;
    $self->{ 'pg_start_backup_done' }    = 1;

    return;
}

=head1 clean_and_die()

Helper function called by other parts of code - removes temporary destination for xlogs, and exits program with logging passed message.

=cut

sub clean_and_die {
    my $self          = shift;
    my @msg_with_args = @_;
    rmtree( [ $self->{ 'xlogs' } . '.real', $self->{ 'xlogs' } ], 0, );
    $self->stop_pg_backup() if $self->{ 'pg_start_backup_done' };
    $self->log->fatal( @msg_with_args );
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

=head1 DESTROY()

Destroctor for object - removes temp directory on program exit.

=cut

sub DESTROY {
    my $self = shift;
    return unless $self->{ 'temp-dir-prepared' };
    rmtree( [ $self->{ 'temp-dir-prepared' } ], 0 );
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
        'gzip-path'         => 'gzip',
        'bzip2-path'        => 'bzip2',
        'lzma-path'         => 'lzma',
        'tar-path'          => 'tar',
        'nice-path'         => 'nice',
        'psql-path'         => 'psql',
        'rsync-path'        => 'rsync',
        'database'          => 'postgres',
        'filename-template' => '__HOSTNAME__-__FILETYPE__-^Y-^m-^d.tar__CEXT__',
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-backup-master.pod' )
        unless GetOptions(
        \%args,
        'data-dir|D=s',
        'database|d=s',
        'host|h=s',
        'port|p=i',
        'username|U=s',
        'xlogs|x=s',
        'dst-local|dl=s@',
        'dst-remote|dr=s@',
        'temp-dir|t=s',
        'log|l=s',
        'filename-template|f=s',
        'pid-file',
        'verbose|v',
        'gzip-path|gp=s',
        'bzip2-path|bp=s',
        'lzma-path|lp=s',
        'nice-path|np=s',
        'psql-path|pp=s',
        'tar-path|tp=s',
        'rsync-path|rp=s',
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

    $self->{ 'filename-template' } = strftime( $self->{ 'filename-template' }, localtime time() );
    $self->{ 'filename-template' } =~ s/__HOSTNAME__/hostname()/ge;

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->verbose;

    my @psql = ();
    push @psql, $self->{ 'psql-path' };
    push @psql, '-qAtX';
    push @psql, ( '-U', $self->{ 'username' } ) if $self->{ 'username' };
    push @psql, ( '-d', $self->{ 'database' } ) if $self->{ 'database' };
    push @psql, ( '-h', $self->{ 'host' } )     if $self->{ 'host' };
    push @psql, ( '-p', $self->{ 'port' } )     if $self->{ 'port' };
    push @psql, '-c';
    $self->{ 'psql' } = \@psql;

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

    $self->log->fatal( "Xlogs dir (--xlogs) was not given! Cannot work without it" ) unless defined $self->{ 'xlogs' };
    $self->{ 'xlogs' } =~ s{/+$}{};
    $self->log->fatal( "Xlogs dir (%s) already exists! It shouldn't.",           $self->{ 'xlogs' } ) if -e $self->{ 'xlogs' };
    $self->log->fatal( "Xlogs side dir (%s.real) already exists! It shouldn't.", $self->{ 'xlogs' } ) if -e $self->{ 'xlogs' } . '.real';

    my $xlog_parent = dirname( $self->{ 'xlogs' } );
    $self->log->fatal( 'Xlogs dir (%s) parent (%s) does not exist. Cannot continue.',   $self->{ 'xlogs' }, $xlog_parent ) unless -e $xlog_parent;
    $self->log->fatal( 'Xlogs dir (%s) parent (%s) is not directory. Cannot continue.', $self->{ 'xlogs' }, $xlog_parent ) unless -d $xlog_parent;
    $self->log->fatal( 'Xlogs dir (%s) parent (%s) is not writable. Cannot continue.',  $self->{ 'xlogs' }, $xlog_parent ) unless -w $xlog_parent;

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
