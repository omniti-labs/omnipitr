package OmniPITR::Program::Backup::Slave;
use strict;
use warnings;

use base qw( OmniPITR::Program::Backup );

use File::Spec;
use File::Basename;
use English qw( -no_match_vars );
use File::Copy;
use File::Path;
use Getopt::Long;
use Carp;
use POSIX qw( strftime );
use Sys::Hostname;
use OmniPITR::Tools qw( run_command ext_for_compression );

=head1 make_data_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with content of PGDATA

=cut

sub make_data_archive {
    my $self = shift;
    $self->pause_xlog_removal();
    $self->{ 'CONTROL' }->{ 'initial' } = $self->get_control_data();
    $self->compress_pgdata();
    return;
}

=head1 make_xlog_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with xlogs required to start PostgreSQL from backup.

=cut

sub make_xlog_archive {
    my $self = shift;
    $self->wait_for_checkpoint_location_change();
    $self->compress_xlogs();
    $self->unpause_xlog_removal();
    return;
}

=head1 compress_xlogs()

Wrapper function which encapsulates all work required to compress xlog
segments that accumulated during backup of data directory.

=cut

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
        'transform' => $transform_command,
    );

    $self->log->time_finish( 'Compressing xlogs' ) if $self->verbose;

    return;
}

=head1 uncompress_wal_archive_segments()

In case walarchive (--source option) is compressed, L<omnipitr-backup-slave>
needs to uncompress files to temp directory before making archive - so that
the archive will be easier to use.

This work is being done in this function.

=cut

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

=head make_dot_backup_file()

Make I<SEGMENT>.I<OFFSET>.backup file that will be included in xlog archive.

This file contains vital information like start and end position of WAL
reply that is required to get consistent state.

=cut

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

=head1 wait_for_checkpoint_location_change()

Just like the name suggests - this function periodically (every 5 seconds,
hardcoded, as there is not much sense in parametrizing it) checks
pg_controldata of PGDATA, and finishes if value in B<Latest checkpoint
location> will change.

=cut

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

=head1 make_backup_label_temp_file()

Normal hot backup contains file named 'backup_label' in PGDATA archive.

Since this is not normal hot backup - PostgreSQL will not create this file,
and it has to be created separately by I<omnipitr-backup-slave>.

This file is created in temp directory (it is B<not> created in PGDATA), and
is included in tar in such a way that, on uncompressing, it will get to
unarchived PGDATA.

=cut

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

=head1 convert_wal_location_and_timeline_to_filename()

Helper function which converts WAL location and timeline number into
filename that given location will be in.

=cut

sub convert_wal_location_and_timeline_to_filename {
    my $self = shift;
    my ( $location, $timeline ) = @_;

    my ( $series, $offset ) = split m{/}, $location;

    $offset =~ s/.{0,6}$//;

    my $location_filename = sprintf '%08s%08s%08s', $timeline, $series, $offset;

    return $location_filename;
}

=head1 compress_pgdata()

Wrapper function which encapsulates all work required to compress data
directory.

=cut

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
        'transform' => $transform_command,
    );

    $self->log->time_finish( 'Compressing $PGDATA' ) if $self->verbose;
    return;
}

=head1 pause_xlog_removal()

Creates trigger file that will pause removal of old segments by
I<omnipitr-restore>.

=cut

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

=head1 unpause_xlog_removal()

Removed trigger file, effectively unpausing removal of old, obsolete log
segments in I<omnipitr-restore>.

=cut

sub unpause_xlog_removal {
    my $self = shift;
    unlink( $self->{ 'removal-pause-trigger' } );
    delete $self->{ 'removal-pause-trigger-created' };
    return;
}

=head1 DESTROY()

Destructor for object - removes created pause trigger;

=cut

sub DESTROY {
    my $self = shift;
    unlink( $self->{ 'removal-pause-trigger' } ) if $self->{ 'removal-pause-trigger-created' };
    $self->SUPER::DESTROY();
    return;
}

=head1 read_args()

Function which does all the parsing, and transformation of command line
arguments.

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

One exception is for compression programs paths - technically, it could be
validated in here, but benefit would be pretty limited, and code to do so
relatively complex, as compression program path might, but doesn't have to
be actual file path - it might be just program name (without path), which is
the default.

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
