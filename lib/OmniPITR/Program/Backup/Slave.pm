package OmniPITR::Program::Backup::Slave;
use strict;
use warnings;

our $VERSION = '1.2.0';
use base qw( OmniPITR::Program::Backup );

use File::Spec;
use File::Basename;
use English qw( -no_match_vars );
use File::Copy;
use File::Path;
use Getopt::Long qw( :config no_ignore_case );
use Carp;
use POSIX qw( strftime );
use Sys::Hostname;
use Cwd qw(abs_path);
use OmniPITR::Tools qw( run_command ext_for_compression );
use OmniPITR::Tools::ParallelSystem;

=head1 make_data_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with content of PGDATA

=cut

sub make_data_archive {
    my $self = shift;
    $self->pause_xlog_removal();
    $self->make_backup_label_temp_file();
    $self->compress_pgdata();
    $self->finish_pgdata_backup();
    return;
}

=head1 finish_pgdata_backup()

Calls pg_stop_backup on master (if necessary), and waits for xlogs to be
ready

=cut

sub finish_pgdata_backup {
    my $self = shift;
    return unless $self->{ 'call-master' };

    my $stop_backup_output = $self->psql( 'SELECT pg_stop_backup()' );

    $stop_backup_output =~ s/\s*\z//;
    $self->log->log( q{pg_stop_backup() returned %s.}, $stop_backup_output );
    $self->log->fatal( 'Output from pg_stop_backup is not parseable?!' ) unless $stop_backup_output =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\z};

    my $timeline = substr( $self->{ 'wal_range' }->{ 'min' }, 0, 8 );
    my $location_file = $self->convert_wal_location_and_timeline_to_filename( $stop_backup_output, $timeline );
    $self->{ 'wal_range' }->{ 'max' } = $location_file;

    return;

}

=head1 make_xlog_archive()

Wraps all work necessary to make local .tar files (optionally compressed)
with xlogs required to start PostgreSQL from backup.

=cut

sub make_xlog_archive {
    my $self = shift;
    return if $self->{ 'skip-xlogs' };
    $self->wait_for_xlog_archive_to_be_ready();
    $self->compress_xlogs();
    $self->unpause_xlog_removal();
    return;
}

=head1 wait_for_xlog_archive_to_be_ready()

Waits till all necessary xlogs will be in archive, or (in case --call-master
was not given) - for checkpoint on slave.

=cut

sub wait_for_xlog_archive_to_be_ready {
    my $self = shift;
    return $self->wait_for_checkpoint_location_change() unless $self->{ 'call-master' };
    $self->wait_for_file( $self->{ 'source' }->{ 'path' }, $self->{ 'stop_backup_filename_re' } );
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

    my $source_transform_from = basename( $self->{ 'source' }->{ 'path' } );
    $source_transform_from =~ s{^/*}{};
    $source_transform_from =~ s{/*$}{};

    my $dot_backup_transform_from = $self->{ 'temp-dir' };
    $dot_backup_transform_from =~ s{^/*}{};
    $dot_backup_transform_from =~ s{/*$}{};

    my $transform_to = basename( $self->{ 'data-dir' } ) . '/pg_xlog';
    my $transform_command = sprintf 's#^\(%s\|%s\)#%s#', $source_transform_from, $dot_backup_transform_from, $transform_to;

    my @stuff_to_compress = ();
    if ( 'none' eq $self->{ 'source' }->{ 'compression' } ) {
        my $wal_files = $self->_find_interesting_xlogs( $self->{ 'source' }->{ 'path' }, '' );
        my $dir_name = basename( $self->{ 'source' }->{ 'path' } );
        push @stuff_to_compress, map { File::Spec->catfile( $dir_name, $_ ) } @{ $wal_files };
    }
    else {
        push @stuff_to_compress, basename( $self->{ 'source' }->{ 'path' } );
    }
    push @stuff_to_compress, File::Spec->catfile( $self->{ 'temp-dir' }, $self->{ 'dot_backup_filename' } ) if $self->{ 'dot_backup_filename' };

    $self->tar_and_compress(
        'work_dir'  => dirname( $self->{ 'source' }->{ 'path' } ),
        'tar_dir'   => \@stuff_to_compress,
        'transform' => $transform_command,
        'data_type' => 'xlog',
    );

    $self->log->time_finish( 'Compressing xlogs' ) if $self->verbose;

    return;
}

=head1 _find_interesting_xlogs()

Internal function that scans source path, and returns arrayref of filenames (without paths) that are xlogs withing interesting wal_range.

=cut

sub _find_interesting_xlogs {
    my $self = shift;
    my ( $directory, $extension ) = @_;

    opendir my $dir, $directory or $self->log->fatal( 'Cannot open wal-archive (%s) : %s', $directory, $OS_ERROR );
    my @wal_segments = sort grep { -f File::Spec->catfile( $directory, $_ ) && /\Q$extension\E\z/ } readdir( $dir );
    close $dir;

    my @reply = ();
    for my $segment ( @wal_segments ) {
        my $base_segment_name = substr( $segment, 0, 24 );
        next if $base_segment_name lt $self->{ 'wal_range' }->{ 'min' };
        next if $base_segment_name gt $self->{ 'wal_range' }->{ 'max' };
        push @reply, $segment;
    }

    return \@reply;
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
    my $new_source = File::Spec->catfile( $self->{ 'temp-dir' }, 'uncompressed_pg_xlogs' );
    $self->{ 'source' }->{ 'path' } = $new_source;

    mkpath( [ $new_source ], 0, oct( "755" ) );

    my $wal_segments = $self->_find_interesting_xlogs(
        $old_source,
        ext_for_compression( $self->{ 'source' }->{ 'compression' } ),
    );

    $self->log->log( '%s wal segments have to be uncompressed', scalar @{ $wal_segments } );

    my $all_ok        = 1;
    my $handle_finish = sub {
        my $job = shift;
        $self->log->log( 'Uncompressing %s ended in %.6fs', $job->{ 'wal_name' }, $job->{ 'ended' } - $job->{ 'started' } ) if $self->verbose;
        return unless $job->{ 'status' };
        $self->log->error( 'Error while uncompressing wal segment %s: %s', $job->{ 'wal_name' }, $job );
        $all_ok = 0;
        return;
    };

    my $runner = OmniPITR::Tools::ParallelSystem->new(
        'max_jobs'  => $self->{ 'parallel-jobs' },
        'on_finish' => $handle_finish,
    );

    for my $segment ( @{ $wal_segments } ) {

        my $old_file = File::Spec->catfile( $old_source, $segment );
        my $new_file = File::Spec->catfile( $new_source, $segment );
        copy( $old_file, $new_file ) or $self->log->fatal( 'Cannot copy %s to %s: %s', $old_file, $new_file, $OS_ERROR );
        $self->log->log( 'File copied: %s -> %s', $old_file, $new_file );
        my @uncompress = ( $self->{ $self->{ 'source' }->{ 'compression' } . '-path' }, '-d', $new_file );
        unshift @uncompress, $self->{ 'nice-path' } unless $self->{ 'not-nice' };
        $runner->add_command(
            'command'  => \@uncompress,
            'wal_name' => $new_file,
        );
    }
    $runner->run;
    $self->log->fatal( 'Decompressing of some files failed.' ) unless $all_ok;
    return;
}

=head make_dot_backup_file()

Make I<SEGMENT>.I<OFFSET>.backup file that will be included in xlog archive.

This file contains vital information like start and end position of WAL
reply that is required to get consistent state.

=cut

sub make_dot_backup_file {
    my $self = shift;

    return if $self->{ 'call-master' };

    my $redo_location = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's REDO location" };
    my $timeline      = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's TimeLineID" };

    my $final_location = $self->{ 'CONTROL' }->{ 'final' }->{ "Latest checkpoint location" };
    my $final_wal_filename = $self->convert_wal_location_and_timeline_to_filename( $final_location, $timeline );
    if (   ( defined $self->{ 'CONTROL' }->{ 'final' }->{ 'Minimum recovery ending location' } )
        && ( $self->{ 'CONTROL' }->{ 'final' }->{ 'Minimum recovery ending location' } =~ m{\A[a-f0-9]+/[a-f0-9]+\z}i )
        && ( '0/0' ne $self->{ 'CONTROL' }->{ 'final' }->{ 'Minimum recovery ending location' } ) )
    {
        my $minimum_location = $self->{ 'CONTROL' }->{ 'final' }->{ 'Minimum recovery ending location' };
        my $minimum_wal_filename = $self->convert_wal_location_and_timeline_to_filename( $minimum_location, $timeline );
        if ( $minimum_wal_filename gt $final_wal_filename ) {
            $final_location     = $minimum_location;
            $final_wal_filename = $minimum_wal_filename;
        }
    }

    # This is set in here only if we're not calling master. If we do, then the max is set in finish_pgdata_backup()
    $self->{ 'wal_range' }->{ 'max' } = $final_wal_filename;

    my $final_wal_filename_re = qr{\A$final_wal_filename};
    $self->wait_for_file( $self->{ 'source' }->{ 'path' }, $final_wal_filename_re );

    my $offset = $redo_location;
    $offset =~ s#.*/##;
    $offset =~ s/^.*?(.{0,6})$/$1/;

    my $output_filename = sprintf '%s.%08s.backup', $self->convert_wal_location_and_timeline_to_filename( $redo_location, $timeline ), $offset;

    my @content_lines = @{ $self->{ 'backup_file_data' } };
    splice( @content_lines, 1, 0, sprintf 'STOP WAL LOCATION: %s (file %s)', $final_location, $final_wal_filename );
    splice( @content_lines, 4, 0, sprintf 'STOP TIME: %s', strftime( '%Y-%m-%d %H:%M:%S %Z', localtime $self->{ 'meta' }->{ 'started_at' } ) );

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

If --call-master was given, it will run pg_start_backup() on master, and
retrieve generated backup_label file.

=cut

sub make_backup_label_temp_file {
    my $self = shift;

    $self->{ 'CONTROL' }->{ 'initial' } = $self->get_control_data();

    if ( $self->{ 'call-master' } ) {
        $self->get_backup_label_from_master();
    }
    else {
        my $redo_location = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's REDO location" };
        my $last_location = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint location" };
        my $timeline      = $self->{ 'CONTROL' }->{ 'initial' }->{ "Latest checkpoint's TimeLineID" };
        my $location_file = $self->convert_wal_location_and_timeline_to_filename( $redo_location, $timeline );

        $self->{ 'wal_range' }->{ 'min' } = $location_file;
        $self->{ 'meta' }->{ 'xlog-min' } = $location_file;

        my @content_lines = ();
        push @content_lines, sprintf 'START WAL LOCATION: %s (file %s)', $redo_location, $location_file;
        push @content_lines, sprintf 'CHECKPOINT LOCATION: %s', $last_location;
        push @content_lines, sprintf 'START TIME: %s', strftime( '%Y-%m-%d %H:%M:%S %Z', localtime time );
        push @content_lines, 'LABEL: OmniPITR_Slave_Hot_Backup';

        $self->{ 'backup_file_data' } = \@content_lines;
    }
    my $content = join( "\n", @{ $self->{ 'backup_file_data' } } ) . "\n";

    my $filename = File::Spec->catfile( $self->{ 'temp-dir' }, 'backup_label' );
    if ( open my $fh, '>', $filename ) {
        print $fh $content;
        close $fh;
        return;
    }
    $self->log->fatal( 'Cannot write backup_label file %s : %s', $filename, $OS_ERROR );
}

=head1 get_backup_label_from_master()

Wraps logic required to call pg_start_backup(), get response, and
backup_label file content .

=cut

sub get_backup_label_from_master {
    my $self = shift;

    my $start_backup_output = $self->psql( "SELECT w, pg_xlogfile_name(w) from (select pg_start_backup('omnipitr_slave_backup_with_master_callback') as w ) as x" );

    $start_backup_output =~ s/\s*\z//;
    $self->log->log( q{pg_start_backup('omnipitr') returned %s.}, $start_backup_output );
    $self->log->fatal( 'Output from pg_start_backup is not parseable?!' ) unless $start_backup_output =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\|([0-9A-F]{24})\z};

    my ( $part_1, $part_2, $min_xlog ) = ( $1, $2, $3 );
    $part_2 =~ s/(.{1,6})\z//;
    my $part_3 = $1;

    $self->{ 'meta' }->{ 'xlog-min' } = $min_xlog;

    my $expected_filename_suffix = sprintf '%08s%08s.%08s.backup', $part_1, $part_2, $part_3;

    if ( 'none' ne $self->{ 'source' }->{ 'compression' } ) {
        my $extension = ext_for_compression( $self->{ 'source' }->{ 'compression' } );
        $expected_filename_suffix .= $extension;
    }

    my $backup_filename_re = qr{\A[0-9A-F]{8}\Q$expected_filename_suffix\E\z};

    $self->{ 'stop_backup_filename_re' } = $backup_filename_re;

    my $backup_label_content = $self->psql(
        "select pg_read_file( 'backup_label', 0, ( pg_stat_file( 'backup_label' ) ).size )",
    );

    $self->{ 'backup_file_data' } = [ split( /\n/, $backup_label_content ) ];

    my @start_wal_lines = grep { m{\ASTART WAL LOCATION: \S+ \(file (\S+)\)\s*\z} } @{ $self->{ 'backup_file_data' } };
    if ( 1 != scalar @start_wal_lines ) {
        $self->log->fatal( "There is no line with START WAL LOCATION in the .backup file (or there are many), it should't happen" );
    }
    $start_wal_lines[ 0 ] =~ s{\ASTART WAL LOCATION: \S+ \(file (\S+)\)\s*\z}{$1};
    $self->{ 'wal_range' }->{ 'min' } = $start_wal_lines[ 0 ];

    $self->wait_for_checkpoint_from_backup_label();

    return;
}

=head1 wait_for_checkpoint_from_backup_label()

Waits till slave will do checkpoint in at least the same location as master
did when pg_start_backup() was called.

=cut

sub wait_for_checkpoint_from_backup_label {
    my $self = shift;

    my @checkpoint_lines = grep { m{\ACHECKPOINT\s+LOCATION:\s+[a-f0-9]+/[0-9a-f]{1,8}\s*\z}i } @{ $self->{ 'backup_file_data' } };

    $self->log->fatal( 'Cannot get checkpoint lines from: %s', $self->{ 'backup_file_data' } ) if 1 != scalar @checkpoint_lines;

    my ( $major, $minor ) = $checkpoint_lines[ 0 ] =~ m{ \s+ ( [a-f0-9]+ ) / ( [a-f0-9]{1,8} ) \s* \z }xmsi;
    $major = hex $major;
    $minor = hex $minor;

    $self->log->log( 'Waiting for checkpoint (based on backup_label from master) - %s', $checkpoint_lines[ 0 ] ) if $self->verbose;
    while ( 1 ) {
        my $temp = $self->get_control_data();

        my ( $c_major, $c_minor ) = $temp->{ 'Latest checkpoint location' } =~ m{ \A ( [a-f0-9]+ ) / ( [a-f0-9]{1,8} ) \s* \z }xmsi;
        $c_major = hex $c_major;
        $c_minor = hex $c_minor;

        last if $c_major > $major;
        last if ( $c_major == $major ) && ( $c_minor >= $minor );

        sleep 5;
    }
    $self->log->log( 'Checkpoint .' ) if $self->verbose;
    return;
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

    $self->log->time_start( 'Compressing $PGDATA' ) if $self->verbose;

    my $transform_from = $self->{ 'temp-dir' };
    $transform_from =~ s{^/*}{};
    $transform_from =~ s{/*$}{};
    my $transform_to = basename( $self->{ 'data-dir' } );
    my $transform_command = sprintf 's#^%s/#%s/#', $transform_from, $transform_to;

    my @excludes = qw( pg_log/* pg_xlog/0* pg_xlog/archive_status/* recovery.conf postmaster.pid );
    for my $dir ( qw( pg_log pg_xlog ) ) {
        push @excludes, $dir if -l File::Spec->catfile( $self->{ 'data-dir' }, $dir );
    }

    my ( $tablespaces, $transforms ) = $self->get_tablespaces_and_transforms();
    push @{ $tablespaces }, basename( $self->{ 'data-dir' } ), File::Spec->catfile( $self->{ 'temp-dir' }, 'backup_label' );
    push @{ $transforms }, $transform_command;

    $self->tar_and_compress(
        'work_dir'  => dirname( $self->{ 'data-dir' } ),
        'tar_dir'   => $tablespaces,
        'excludes'  => [ map { sprintf( '%s/%s', basename( $self->{ 'data-dir' } ), $_ ) } @excludes ],
        'transform' => $transforms,
        'data_type' => 'data',
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
    return if $self->{ 'skip-xlogs' };
    return unless $self->{ 'removal-pause-trigger' };

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
    return unless $self->{ 'removal-pause-trigger' };
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

=head1 read_args_specification

Defines which options are legal for this program.

=cut

sub read_args_specification {
    my $self = shift;

    return {
        'bzip2-path'  => { 'type'    => 's', 'aliases' => [ 'bp' ], 'default' => 'bzip2', },
        'call-master' => { 'aliases' => [ 'cm' ], },
        'data-dir'          => { 'type' => 's',  'aliases' => [ 'D' ], },
        'database'          => { 'type' => 's',  'aliases' => [ 'd' ], },
        'digest'            => { 'type' => 's',  'aliases' => [ 'dg' ], },
        'dst-direct'        => { 'type' => 's@', 'aliases' => [ 'dd' ], },
        'dst-local'         => { 'type' => 's@', 'aliases' => [ 'dl' ], },
        'dst-remote'        => { 'type' => 's@', 'aliases' => [ 'dr' ], },
        'dst-pipe'          => { 'type' => 's@', 'aliases' => [ 'dp' ], },
        'filename-template' => { 'type' => 's',  'aliases' => [ 'f' ], 'default' => '__HOSTNAME__-__FILETYPE__-^Y-^m-^d.tar__CEXT__', },
        'gzip-path'         => { 'type' => 's',  'aliases' => [ 'gp' ], 'default' => 'gzip', },
        'host'              => { 'type' => 's',  'aliases' => [ 'h' ], },
        'log'               => { 'type' => 's',  'aliases' => [ 'l' ], },
        'lzma-path'         => { 'type' => 's',  'aliases' => [ 'lp' ], 'default' => 'lzma', },
        'nice-path'         => { 'type' => 's',  'aliases' => [ 'np' ], 'default' => 'nice', },
        'not-nice'           => { 'aliases' => [ 'nn' ], },
        'parallel-jobs'      => { 'type'    => 'i', 'aliases' => [ 'PJ' ], 'default' => '1', },
        'pgcontroldata-path' => { 'type'    => 's', 'aliases' => [ 'pp' ], 'default' => 'pg_controldata', },
        'pid-file'           => { 'type'    => 's', },
        'port'                  => { 'type' => 'i', 'aliases' => [ 'P' ], },
        'psql-path'             => { 'type' => 's', 'aliases' => [ 'sp' ], 'default' => 'psql', },
        'remote-cat-path'       => { 'type' => 's', 'aliases' => [ 'rcp' ], 'default' => 'cat', },
        'removal-pause-trigger' => { 'type' => 's', 'aliases' => [ 'p' ], },
        'rsync-path'            => { 'type' => 's', 'aliases' => [ 'rp' ], 'default' => 'rsync', },
        'shell-path'            => { 'type' => 's', 'aliases' => [ 'sh' ], 'default' => 'bash', },
        'source'                => { 'type' => 's', 'aliases' => [ 's' ], },
        'skip-xlogs' => { 'aliases' => [ 'sx' ], },
        'ssh-path'   => { 'type'    => 's', 'aliases' => [ 'ssh' ], 'default' => 'ssh', },
        'tar-path'   => { 'type'    => 's', 'aliases' => [ 'tp' ], 'default' => 'tar', },
        'tee-path'   => { 'type'    => 's', 'aliases' => [ 'ep' ], 'default' => 'tee', },
        'temp-dir'   => { 'type'    => 's', 'aliases' => [ 't' ], 'default' => $ENV{ 'TMPDIR' } || '/tmp', },
        'username' => { 'type'    => 's', 'aliases' => [ 'U' ], },
        'verbose'  => { 'aliases' => [ 'v' ], },
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

    if ( defined $args->{ 'source' } && $args->{ 'source' } =~ s/\A(gzip|bzip2|lzma)=// ) {
        $self->{ 'source' } = {
            'compression' => $1,
            'path'        => $args->{ 'source' },
        };
    }
    else {
        $self->{ 'source' } = {
            'compression' => 'none',
            'path'        => $args->{ 'source' },
        };
    }

    $self->{ 'filename-template' } = strftime( $self->{ 'filename-template' }, localtime $self->{ 'meta' }->{ 'started_at' } );
    $self->{ 'filename-template' } =~ s/__HOSTNAME__/hostname()/ge;

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
        $self->log->fatal( 'Source of WAL files was not provided!' ) unless defined $self->{ 'source' }->{ 'path' };
        $self->log->fatal( 'Provided source of wal files (%s) does not exist!',   $self->{ 'source' }->{ 'path' } ) unless -e $self->{ 'source' }->{ 'path' };
        $self->log->fatal( 'Provided source of wal files (%s) is not directory!', $self->{ 'source' }->{ 'path' } ) unless -d $self->{ 'source' }->{ 'path' };
        $self->log->fatal( 'Provided source of wal files (%s) is not readable!',  $self->{ 'source' }->{ 'path' } ) unless -r $self->{ 'source' }->{ 'path' };

        $self->{ 'source' }->{ 'path' } = abs_path( $self->{ 'source' }->{ 'path' } );
    }

    $self->log->fatal( 'Temp-dir was not provided!' ) unless defined $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) does not exist!',   $self->{ 'temp-dir' } ) unless -e $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) is not directory!', $self->{ 'temp-dir' } ) unless -d $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) is not writable!',  $self->{ 'temp-dir' } ) unless -w $self->{ 'temp-dir' };
    $self->log->fatal( 'Provided temp-dir (%s) contains # character!', $self->{ 'temp-dir' } ) if $self->{ 'temp-dir' } =~ /#/;

    if ( defined $self->{ 'removal-pause-trigger' } ) {
        $self->log->fatal( 'Provided removal pause trigger file (%s) already exists!', $self->{ 'removal-pause-trigger' } ) if -e $self->{ 'removal-pause-trigger' };

        $self->log->fatal( 'Directory for provided removal pause trigger (%s) does not exist!',   $self->{ 'removal-pause-trigger' } ) unless -e dirname( $self->{ 'removal-pause-trigger' } );
        $self->log->fatal( 'Directory for provided removal pause trigger (%s) is not directory!', $self->{ 'removal-pause-trigger' } ) unless -d dirname( $self->{ 'removal-pause-trigger' } );
        $self->log->fatal( 'Directory for provided removal pause trigger (%s) is not writable!',  $self->{ 'removal-pause-trigger' } ) unless -w dirname( $self->{ 'removal-pause-trigger' } );
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
