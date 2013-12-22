package OmniPITR::Program::Backup::Master;
use strict;
use warnings;

our $VERSION = '1.3.2';
use base qw( OmniPITR::Program::Backup );

use Carp;
use OmniPITR::Tools qw( run_command );
use English qw( -no_match_vars );
use File::Basename;
use POSIX qw( strftime );
use File::Spec;
use File::Path qw( mkpath rmtree );
use File::Copy;
use Storable;
use Cwd qw(abs_path);
use Getopt::Long qw( :config no_ignore_case );

=head1 make_data_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with content of PGDATA

=cut

sub make_data_archive {
    my $self = shift;
    $self->start_pg_backup();
    $self->compress_pgdata();
    $self->stop_pg_backup();
    return;
}

=head1 make_xlog_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with xlogs required to start PostgreSQL from backup.

=cut

sub make_xlog_archive {
    my $self = shift;
    return if $self->{ 'skip-xlogs' };
    $self->wait_for_final_xlog_and_remove_dst_backup();
    $self->compress_xlogs();
    return;
}

=head1 wait_for_final_xlog_and_remove_dst_backup()

In PostgreSQL < 8.4 pg_stop_backup() finishes before .backup "wal segment"
is archived.

So we need to wait till it appears in backup xlog destination before we can
remove symlink.

=cut

sub wait_for_final_xlog_and_remove_dst_backup {
    my $self = shift;

    my $backup_file = $self->wait_for_file( $self->{ 'xlogs' }, $self->{ 'stop_backup_filename_re' } );

    my $last_file = undef;

    open my $fh, '<', File::Spec->catfile( $self->{ 'xlogs' }, $backup_file ) or $self->log->fatal( 'Cannot open backup file %s for reading: %s', $backup_file, $OS_ERROR );
    while ( my $line = <$fh> ) {
        next unless $line =~ m{\A STOP \s+ WAL \s+ LOCATION: .* file \s+ ( [0-9A-f]{24} ) }x;
        $last_file = qr{\A$1\z};
        last;
    }
    close $fh;

    $self->log->fatal( '.backup file (%s) does not contain STOP WAL LOCATION line in recognizable format.', $backup_file ) unless $last_file;

    $self->wait_for_file( $self->{ 'xlogs' }, $last_file );

    unlink( $self->{ 'xlogs' } );
}

=head1 compress_xlogs()

Wrapper function which encapsulates all work required to compress xlog
segments that accumulated during backup of data directory.

=cut

sub compress_xlogs {
    my $self = shift;
    $self->log->time_start( 'Compressing xlogs' ) if $self->verbose;

    $self->tar_and_compress(
        'work_dir'  => $self->{ 'xlogs' } . '.real',
        'tar_dir'   => [ basename( $self->{ 'data-dir' } ) ],
        'data_type' => 'xlog',
    );
    $self->log->time_finish( 'Compressing xlogs' ) if $self->verbose;
    rmtree( $self->{ 'xlogs' } . '.real', 0 );

    return;
}

=head1 compress_pgdata()

Wrapper function which encapsulates all work required to compress data
directory.

=cut

sub compress_pgdata {
    my $self = shift;
    $self->log->time_start( 'Compressing $PGDATA' ) if $self->verbose;

    my @excludes = qw( pg_log/* pg_xlog/0* pg_xlog/archive_status/* postmaster.pid );
    for my $dir ( qw( pg_log pg_xlog ) ) {
        push @excludes, $dir if -l File::Spec->catfile( $self->{ 'data-dir' }, $dir );
    }

    my ( $tablespaces, $transforms ) = $self->get_tablespaces_and_transforms();
    push @{ $tablespaces }, basename( $self->{ 'data-dir' } );

    $self->tar_and_compress(
        'work_dir'  => dirname( $self->{ 'data-dir' } ),
        'tar_dir'   => $tablespaces,
        'excludes'  => \@excludes,
        'transform' => $transforms,
        'data_type' => 'data',
    );

    $self->log->time_finish( 'Compressing $PGDATA' ) if $self->verbose;
    return;
}

=head1 stop_pg_backup()

Runs pg_stop_backup() PostgreSQL function, which is crucial in backup
process.

This happens after data directory compression, but before compression of
xlogs.

=cut

sub stop_pg_backup {
    my $self = shift;

    my $stop_backup_output = $self->psql( 'SELECT pg_stop_backup()' );

    $stop_backup_output =~ s/\s*\z//;

    $self->log->log( q{pg_stop_backup('omnipitr') returned %s.}, $stop_backup_output );

    delete $self->{ 'pg_start_backup_done' };

    return;
}

=head1 start_pg_backup()

Executes pg_start_backup() postgresql function, and (before it) creates
temporary destination for xlogs (dst-backup for omnipitr-archive).

=cut

sub start_pg_backup {
    my $self = shift;

    unless ( $self->{ 'skip-xlogs' } ) {
        my $subdir = basename( $self->{ 'data-dir' } );

        $self->log->fatal( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . '.real',                 $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . '.real' );
        $self->log->fatal( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . ".real/$subdir",         $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . ".real/$subdir" );
        $self->log->fatal( 'Cannot create directory %s : %s', $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $OS_ERROR ) unless mkdir( $self->{ 'xlogs' } . ".real/$subdir/pg_xlog" );
        $self->log->fatal( 'Cannot symlink %s to %s: %s', $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $self->{ 'xlogs' }, $OS_ERROR )
            unless symlink( $self->{ 'xlogs' } . ".real/$subdir/pg_xlog", $self->{ 'xlogs' } );
    }

    my $start_backup_output = $self->psql( "SELECT w, pg_xlogfile_name(w) from (select pg_start_backup('omnipitr') as w ) as x" );
    $start_backup_output =~ s/\s*\z//;

    $self->log->log( q{pg_start_backup('omnipitr') returned %s.}, $start_backup_output );
    $self->log->fatal( 'Output from pg_start_backup is not parseable?!' ) unless $start_backup_output =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\|([0-9A-F]{24})\z};

    my ( $part_1, $part_2, $min_xlog ) = ( $1, $2, $3 );
    $part_2 =~ s/(.{1,6})\z//;
    my $part_3 = $1;

    my $expected_filename_suffix = sprintf '%08s%08s.%08s.backup', $part_1, $part_2, $part_3;
    my $backup_filename_re = qr{\A[0-9A-F]{8}\Q$expected_filename_suffix\E\z};

    $self->{ 'stop_backup_filename_re' } = $backup_filename_re;
    delete $self->{ 'pg_start_backup_done' };

    $self->{ 'meta' }->{ 'xlog-min' } = $min_xlog;

    return;
}

=head1 DESTROY()

Destructor for object - removes created destination for omnipitr-archive,
and issues pg_stop_backup() to database.

=cut

sub DESTROY {
    my $self = shift;
    rmtree( [ $self->{ 'xlogs' } . '.real', $self->{ 'xlogs' } ], 0, ) if ( !$self->{ 'xlogs' } ) && ( defined $self->{ 'xlogs' } );
    $self->stop_pg_backup() if $self->{ 'pg_start_backup_done' };
    $self->SUPER::DESTROY();
    return;
}

=head1 read_args_specification

Defines which options are legal for this program.

=cut

sub read_args_specification {
    my $self = shift;

    return {
        'bzip2-path'        => { 'type'    => 's',  'aliases' => [ 'bp' ], 'default' => 'bzip2', },
        'data-dir'          => { 'type'    => 's',  'aliases' => [ 'D' ], },
        'database'          => { 'type'    => 's',  'aliases' => [ 'd' ],  'default' => 'postgres', },
        'digest'            => { 'type'    => 's',  'aliases' => [ 'dg' ], },
        'dst-direct'        => { 'type'    => 's@', 'aliases' => [ 'dd' ], },
        'dst-local'         => { 'type'    => 's@', 'aliases' => [ 'dl' ], },
        'dst-remote'        => { 'type'    => 's@', 'aliases' => [ 'dr' ], },
        'dst-pipe'          => { 'type'    => 's@', 'aliases' => [ 'dp' ], },
        'filename-template' => { 'type'    => 's',  'aliases' => [ 'f' ],  'default' => '__HOSTNAME__-__FILETYPE__-^Y-^m-^d.tar__CEXT__', },
        'gzip-path'         => { 'type'    => 's',  'aliases' => [ 'gp' ], 'default' => 'gzip', },
        'host'              => { 'type'    => 's',  'aliases' => [ 'h' ], },
        'log'               => { 'type'    => 's',  'aliases' => [ 'l' ], },
        'lzma-path'         => { 'type'    => 's',  'aliases' => [ 'lp' ], 'default' => 'lzma', },
        'nice-path'         => { 'type'    => 's',  'aliases' => [ 'np' ], 'default' => 'nice', },
        'not-nice'          => { 'aliases' => [ 'nn' ], },
        'parallel-jobs'     => { 'type'    => 'i',  'aliases' => [ 'PJ' ], 'default' => '1', },
        'pid-file'          => { 'type'    => 's', },
        'port'            => { 'type' => 'i', 'aliases' => [ 'p' ], },
        'psql-path'       => { 'type' => 's', 'aliases' => [ 'pp' ], 'default' => 'psql', },
        'remote-cat-path' => { 'type' => 's', 'aliases' => [ 'rcp' ], 'default' => 'cat', },
        'rsync-path'      => { 'type' => 's', 'aliases' => [ 'rp' ], 'default' => 'rsync', },
        'shell-path'      => { 'type' => 's', 'aliases' => [ 'sh' ], 'default' => 'bash', },
        'skip-xlogs' => { 'aliases' => [ 'sx' ], },
        'ssh-path'   => { 'type'    => 's', 'aliases' => [ 'ssh' ], 'default' => 'ssh', },
        'tar-path'   => { 'type'    => 's', 'aliases' => [ 'tp' ], 'default' => 'tar', },
        'tee-path'   => { 'type'    => 's', 'aliases' => [ 'ep' ], 'default' => 'tee', },
        'temp-dir'   => { 'type'    => 's', 'aliases' => [ 't' ], 'default' => $ENV{ 'TMPDIR' } || '/tmp', },
        'username' => { 'type'    => 's', 'aliases' => [ 'U' ], },
        'verbose'  => { 'aliases' => [ 'v' ], },
        'xlogs'    => { 'type'    => 's', 'aliases' => [ 'x' ], },
    };
}

=head1 read_args_normalization

Function called back from OmniPITR::Program::read_args(), with parsed args as hashref.

Is responsible for putting arguments to correct places, initializing logs, and so on.

=cut

sub read_args_normalization {
    my $self = shift;
    my $args = shift;

    $args->{ 'filename-template' } =~ tr/^/%/;

    $self->{ 'digests' } = [];
    if ( defined( $args->{ digest } ) ) {
        $self->{ 'digests' } = [ split( /,/, $args->{ 'digest' } ) ];
        delete $args->{ 'digest' };
    }

    for my $key ( grep { !/^dst-(?:local|remote|direct|pipe)$/ } keys %{ $args } ) {
        $self->{ $key } = $args->{ $key };
    }

    for my $type ( qw( local remote direct pipe ) ) {
        my $D = [];
        $self->{ 'destination' }->{ $type } = $D;

        next unless defined $args->{ 'dst-' . $type };

        my %temp_for_uniq = ();
        my @items = grep { !$temp_for_uniq{ $_ }++ } @{ $args->{ 'dst-' . $type } };

        for my $item ( @items ) {
            my $current = { 'compression' => 'none', };
            if ( $item =~ s/\A(gzip|bzip2|lzma)=// ) {
                $current->{ 'compression' } = $1;
            }
            $current->{ 'path' } = $item;
            push @{ $D }, $current;
        }
    }

    $self->{ 'filename-template' } = strftime( $self->{ 'filename-template' }, localtime $self->{ 'meta' }->{ 'started_at' } );
    $self->{ 'filename-template' } =~ s/__HOSTNAME__/$self->{ 'meta' }->{ 'hostname' }/g;

    $self->log->log( 'Called with parameters: %s', join( ' ', @ARGV ) ) if $self->verbose;

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for compression programs paths - technically, it could be
validated in here, but benefit would be pretty limited, and code to do so
relatively complex, as compression program path might, but doesn't have to
be actual file path - it might be just program name (without path), which is
the default.

=cut

sub validate_args {
    my $self = shift;

    $self->log->fatal( 'Data-dir was not provided!' ) unless defined $self->{ 'data-dir' };
    $self->{ 'data-dir' } =~ s{/+$}{};
    $self->log->fatal( 'Provided data-dir (%s) does not exist!',   $self->{ 'data-dir' } ) unless -e $self->{ 'data-dir' };
    $self->log->fatal( 'Provided data-dir (%s) is not directory!', $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' };
    $self->log->fatal( 'Provided data-dir (%s) is not readable!',  $self->{ 'data-dir' } ) unless -r $self->{ 'data-dir' };

    $self->{ 'data-dir' } = abs_path( $self->{ 'data-dir' } );

    my $dst_count = 0;
    for my $dst_type ( qw( local remote direct pipe ) ) {
        $dst_count += scalar( @{ $self->{ 'destination' }->{ $dst_type } } );
    }
    $self->log->fatal( "No --dst-* has been provided!" ) if 0 == $dst_count;

    $self->log->fatal( "Filename template does not contain __FILETYPE__ placeholder!" ) unless $self->{ 'filename-template' } =~ /__FILETYPE__/;
    $self->log->fatal( "Filename template cannot contain / or \\ characters!" ) if $self->{ 'filename-template' } =~ m{[/\\]};

    unless ( $self->{ 'skip-xlogs' } ) {
        $self->log->fatal( "Xlogs dir (--xlogs) was not given! Cannot work without it" ) unless defined $self->{ 'xlogs' };
        $self->{ 'xlogs' } =~ s{/+$}{};
        $self->log->fatal( "Xlogs dir (%s) already exists! It shouldn't.",           $self->{ 'xlogs' } ) if -e $self->{ 'xlogs' };
        $self->log->fatal( "Xlogs side dir (%s.real) already exists! It shouldn't.", $self->{ 'xlogs' } ) if -e $self->{ 'xlogs' } . '.real';

        my $xlog_parent = dirname( $self->{ 'xlogs' } );
        $self->log->fatal( 'Xlogs dir (%s) parent (%s) does not exist. Cannot continue.',   $self->{ 'xlogs' }, $xlog_parent ) unless -e $xlog_parent;
        $self->log->fatal( 'Xlogs dir (%s) parent (%s) is not directory. Cannot continue.', $self->{ 'xlogs' }, $xlog_parent ) unless -d $xlog_parent;
        $self->log->fatal( 'Xlogs dir (%s) parent (%s) is not writable. Cannot continue.',  $self->{ 'xlogs' }, $xlog_parent ) unless -w $xlog_parent;
    }

    my %bad_digest = ();
    for my $digest_type ( @{ $self->{ 'digests' } } ) {
        eval { my $tmp = Digest->new( $digest_type ); };
        next unless $EVAL_ERROR;
        $self->log->log( 'Bad digest method: %s, problem: %s', $digest_type, $EVAL_ERROR );
        $bad_digest{ $digest_type } = 1;
    }
    $self->{ 'digests' } = [ grep { !$bad_digest{ $_ } } @{ $self->{ 'digests' } } ];

    return unless $self->{ 'destination' }->{ 'local' };

    for my $d ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        my $dir = $d->{ 'path' };
        $self->log->fatal( 'Choosen local destination dir (%s) does not exist. Cannot continue.',   $dir ) unless -e $dir;
        $self->log->fatal( 'Choosen local destination dir (%s) is not directory. Cannot continue.', $dir ) unless -d $dir;
        $self->log->fatal( 'Choosen local destination dir (%s) is not writable. Cannot continue.',  $dir ) unless -w $dir;
    }

    $self->log->fatal( 'Parallel jobs value not given?!' ) unless defined $self->{ 'parallel-jobs' };
    $self->log->fatal( 'Parallel jobs is not integer (%s)', $self->{ 'parallel-jobs' } ) unless $self->{ 'parallel-jobs' } =~ m{\A\d+\z};
    $self->log->fatal( 'Parallel jobs is not >= 1 (%s)',    $self->{ 'parallel-jobs' } ) unless $self->{ 'parallel-jobs' } >= 1;

    return;
}

1;
