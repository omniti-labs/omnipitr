package OmniPITR::Program::Backup::Cleanup;
use strict;
use warnings;

our $VERSION = '1.3.3';
use base qw( OmniPITR::Program );

use Carp;
use English qw( -no_match_vars );
use OmniPITR::Tools qw( ext_for_compression );
use File::Spec;
use Time::HiRes qw( usleep );
use POSIX qw( strftime tzset );

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
    $self->{ 'stats' }->{ 'removed_size' }  = 0;
    $self->{ 'stats' }->{ 'removed_count' } = 0;
    $self->log->time_start( 'Backup cleanup' ) if $self->verbose;

    $self->find_and_read_all_meta_files();
    $self->ignore_new_backups();

    $self->log->time_start( 'Delete backups' ) if $self->verbose;
    $self->delete_old_backups();
    $self->log->time_finish( 'Delete backups' ) if $self->verbose;

    $self->log->time_start( 'Delete xlogs' ) if $self->verbose;
    $self->delete_old_xlogs();
    $self->log->time_finish( 'Delete xlogs' ) if $self->verbose;

    $self->log->log(
        '%sRemoved %d files, total: %.1fMB', $self->{ 'dry-run' } ? '(dry-run) ' : '', $self->{ 'stats' }->{ 'removed_count' },
        $self->{ 'stats' }->{ 'removed_size' } / ( 1024 * 1024 )
        )
        if $self->verbose;
    $self->log->time_finish( 'Backup cleanup' ) if $self->verbose;
    return;
}

=head1 delete_old_xlogs()

Function that does the actual removal of old xlogs from wal archive.

It's basing its work on "older_kept" meta file, set by L<ignore_new_backups>.

=cut

sub delete_old_xlogs {
    my $self = shift;
    $self->log->fatal( 'There is no "oldest" backup - can not decide which xlogs to remove' ) unless $self->{ 'oldest_kept' };

    my $min_xlog         = $self->{ 'oldest_kept' }->{ 'min-xlog' };
    my $extension        = $self->{ 'archive' }->{ 'extension' } || '';
    my $filename_matcher = qr{\A[0-9A-Fa-f]{24}(?:\.[a-fA-F0-9]{8}\.backup)?\Q$extension\E\z};

    opendir my $dir, $self->{ 'archive' }->{ 'path' } or $self->log->fatal( 'Cannot open wal archive (%s): %s', $self->{ 'archive' }->{ 'path' }, $OS_ERROR );
    my @xlogs = sort grep { $_ =~ $filename_matcher && $_ lt $min_xlog } readdir $dir;
    closedir $dir;

    if ( 0 == scalar @xlogs ) {
        $self->log->log( 'No xlogs to remove.' ) if $self->verbose();
        return;
    }

    $self->log->log( '%d xlogs to be removed, from %s to %s', scalar @xlogs, $xlogs[ 0 ], $xlogs[ -1 ] ) if $self->verbose;

    $self->remove_file( 'xlog', File::Spec->catfile( $self->{ 'archive' }->{ 'path' }, $_ ) ) for @xlogs;

    return;
}

=head1 delete_old_backups()

Removal of too old backups. Works on list provided by L<ignore_new_backups>.

=cut

sub delete_old_backups {
    my $self = shift;
    if (   ( !defined $self->{ 'delete_backups' } )
        || ( 0 == scalar @{ $self->{ 'delete_backups' } } ) )
    {
        $self->log->log( 'No backups to remove.' ) if $self->verbose();
        return;
    }
    $self->log->log( '%d backups to remove.', scalar @{ $self->{ 'delete_backups' } } ) if $self->verbose;

    for my $backup ( @{ $self->{ 'delete_backups' } } ) {
        my $re = $backup->{ 'backup_file_matcher' };
        $self->log->log( '- Backup files matching %s/%s', $self->{ 'backup-dir' }->{ 'path' }, "$re" ) if $self->verbose;
        my @this_backup_files = sort
            grep { -f }
            map { File::Spec->catfile( $self->{ 'backup-dir' }->{ 'path' }, $_ ) }
            grep { $_ =~ $re } @{ $self->{ 'all_backup_files' } };

        $self->remove_file( 'backup', $_ ) for @this_backup_files;
    }

    return;
}

=head1 remove_file()

Helper function which runs unlink on a file, reporting error when needed.

This function exists, so that "dry-run" check can be in one place.

=cut

sub remove_file {
    my $self = shift;
    my ( $type, $filename ) = @_;

    my ( $size ) = ( stat( $filename ) )[ 7 ];

    if (   ( !$self->{ 'dry-run' } )
        && ( $size > $self->{ 'truncate' } )
        && ( $self->{ 'truncate' } > 0 ) )
    {
        while ( 1 ) {
            my $new_size = $size - $self->{ 'truncate' };
            $self->log->log( 'Truncating %s from %s to %s.', $filename, $size, $new_size );
            truncate $filename, $new_size;
            usleep( $self->{ 'sleep' } );
            $size = ( stat( $filename ) )[ 7 ];
            last if $size < $self->{ 'truncate' };
        }
    }

    if ( $self->{ 'dry-run' } ) {
        $self->log->log( '(dry-run) Removing %s file: %s', $type, $filename );
    }
    elsif ( !unlink $filename ) {
        $self->log->error( "Could not unlink %s: %s", $filename, $OS_ERROR );
        return;
    }
    $self->{ 'stats' }->{ 'removed_size' } += $size;
    $self->{ 'stats' }->{ 'removed_count' }++;
    return;
}

=head1 ignore_new_backups()

Scans meta information loaded, and classifies it into 3 parts:

=over

=item * older than keep-days - stacks meta info in $self->{'delete_backups'} - to be used later by L<delete_old_backups>

=item * single oldest, but within keep-days - puts the meta information in $self->{'oldest_kept'} - to be used later by L<delete_old_xlogs>

=item * newer than "oldest_kept" - ignored - nothing to be done about them.

=back

=cut

sub ignore_new_backups {
    my $self       = shift;
    my $keep_since = time() - $self->{ 'keep-days' } * 86400;    # 1 day = 24 hours * 60 minutes * 60 seconds = 86400 seconds.
    my @remove;
    my $oldest = undef;
    for my $meta ( @{ $self->{ 'meta_files' } } ) {
        my $started = $meta->{ 'started-epoch' };
        if ( $started < $keep_since ) {
            push @remove, $meta;
            next;
        }
        if (   ( !defined $oldest )
            || ( $oldest->{ 'started-epoch' } > $started ) )
        {
            $oldest = $meta;
        }
    }
    $self->{ 'delete_backups' } = \@remove;
    $self->{ 'oldest_kept' }    = $oldest;
    delete $self->{ 'meta_files' };
    return;
}

=head1 find_and_read_all_meta_files()

Scans given backup directory for meta files.

Each file containing "meta" in ints name is read (first 8kB only, as the meta files are small, and there is no point in reading big files that accidentaly have "meta" in name).

From the meta file, it extracts information:

=over

=item * Timezone - what timezone was on the server that made the backup

=item * Hostname - what was the hostname of machine making the backup

=item * Min-Xlog - first xlog required to restore given backup

=item * Started-epoch - when the backup was started - epoch time

=back

Then, it tried to regenerate meta filename using fetched details, to make sure that they make sense (filename-template, compression).

If regenerated filename is the same as real - meta info is stacked. If not - error is logged, but processing continues for next metafile.

Afterwards, all hashes with information from meta files are sorted by Started-epoch, and stored in $self for further processing.

=cut

sub find_and_read_all_meta_files {
    my $self = shift;

    my @meta;

    opendir my $dir, $self->{ 'backup-dir' }->{ 'path' } or $self->log->fatal( 'Cannot read from bachup path (%s) : %s', $self->{ 'backup-dir' }->{ 'path' }, $OS_ERROR );
    my @all_files = readdir $dir;
    closedir $dir;

    $self->{ 'all_backup_files' } = \@all_files;

    FILE:
    for my $meta_file_name ( @all_files ) {
        next unless $meta_file_name =~ /meta/;

        my $meta = {
            'file_name' => $meta_file_name,
        };

        my $meta_file_path = File::Spec->catfile( $self->{ 'backup-dir' }->{ 'path' }, $meta_file_name );
        next unless -f $meta_file_path;
        if ( open my $fh, '<', $meta_file_path ) {
            my $content;
            sysread( $fh, $content, 8192 );

            # There is no point (now) to read more. Generally meta files should be < 100 bytes.
            close $fh;
            my @lines = split /\r?\n/, $content;
            for ( @lines ) {
                if ( /^(Timezone|Hostname|Min-Xlog|Started-epoch):\s+(\S+)\s*\z/ ) {
                    $meta->{ lc $1 } = $2;
                }
            }
            for my $required_key ( qw( timezone hostname min-xlog started-epoch ) ) {
                next FILE unless $meta->{ $required_key };
            }
            push @meta, $meta if $self->verify_regeneration_of_meta_filename( $meta );
        }
    }
    $self->{ 'meta_files' } = [ sort { $a->{ 'started-epoch' } <=> $b->{ 'started-epoch' } } @meta ];
    return;
}

=head1 verify_regeneration_of_meta_filename()

Helped function called by L<find_and_read_all_meta_files>. Actually checks if the filename of meta file can be rebuilt using information from it.

=cut

sub verify_regeneration_of_meta_filename {
    my $self = shift;
    my $meta = shift;

    my $filename_mask = $self->{ 'filename-template' };

    if ( $self->{ 'backup-dir' }->{ 'extension' } ) {
        $filename_mask =~ s/__CEXT__/$self->{'backup-dir'}->{ 'extension' }/;
    }
    else {
        $filename_mask =~ s/__CEXT__//;
    }
    $filename_mask =~ s/__HOSTNAME__/$meta->{ 'hostname' }/g;

    $filename_mask =~ s/\^/%/g;
    $filename_mask = $self->strftime_at_timezone( $filename_mask, $meta->{ 'started-epoch' }, $meta->{ 'timezone' } );

    my $meta_filename_verification = $filename_mask;
    $meta_filename_verification =~ s/__FILETYPE__/meta/g;

    if ( $meta_filename_verification ne $meta->{ 'file_name' } ) {
        $self->log->error( 'When processing meta file (%s): %s, regenerated filename was incorrect: %s. Ignoring metafile.', $meta->{ 'file_name' }, $meta, $meta_filename_verification );
        return;
    }

    $filename_mask = join( '', map { $_ eq '__FILETYPE__' ? '.*' : quotemeta( $_ ) } split( /(__FILETYPE__)/, $filename_mask ) );
    $meta->{ 'backup_file_matcher' } = qr{\A$filename_mask\z};
    return 1;
}

=head1 strftime_at_timezone()

Returns time formatted with given format (compatible with normal strftime) at a given time zone, like:

    strftime_at_timezone( '%Y-%m-%d %H:%M:%S %Z', 1386976730, 'America/Los_Angeles' )   # '2013-12-13 15:18:50 PST'
    strftime_at_timezone( '%Y-%m-%d %H:%M:%S %Z', 1386976730, 'UTC' )                   # '2013-12-13 23:18:50 UTC'
    strftime_at_timezone( '%Y-%m-%d %H:%M:%S %Z', 1386976730, 'EST' )                   # '2013-12-13 18:18:50 EST'

=cut

sub strftime_at_timezone {
    my $self = shift;
    my ( $format, $time, $timezone ) = @_;

    # Store previous TZ
    my $delete_tz   = 1;
    my $previous_tz = '';
    if ( exists $ENV{ 'TZ' } ) {
        $delete_tz   = 0;
        $previous_tz = $ENV{ 'TZ' };
    }

    # Set new TZ
    $ENV{ 'TZ' } = $timezone;
    tzset();

    my $output = strftime( $format, localtime $time );

    # Restore previous TZ
    if ( $delete_tz ) {
        delete $ENV{ 'TZ' };
    }
    else {
        $ENV{ 'TZ' } = $previous_tz;
    }
    tzset();

    return $output;
}

=head1 read_args_specification

Defines which options are legal for this program.

=cut

sub read_args_specification {
    my $self = shift;

    return {
        'log'        => { 'type'    => 's', 'aliases' => [ 'l' ] },
        'verbose'    => { 'aliases' => [ 'v' ] },
        'archive'    => { 'type'    => 's', 'aliases' => [ 'a' ], },
        'backup-dir' => { 'type'    => 's', 'aliases' => [ 'b' ], },
        'keep-days'         => { 'type'    => 'i', 'aliases' => [ 'k' ], 'default' => 7, },
        'filename-template' => { 'type'    => 's', 'aliases' => [ 'f' ], 'default' => '__HOSTNAME__-__FILETYPE__-^Y-^m-^d.tar__CEXT__', },
        'truncate'          => { 'type'    => 'i', 'aliases' => [ 't' ], 'default' => 0, },
        'sleep'             => { 'type'    => 'i', 'aliases' => [ 's' ], 'default' => 500, },
        'dry-run'           => { 'aliases' => [ 'd' ] },
    };
}

=head1 read_args_normalization

Function called back from OmniPITR::Program::read_args(), with parsed args as hashref.

Is responsible for putting arguments to correct places, initializing logs, and so on.

=cut

sub read_args_normalization {
    my $self = shift;
    my $args = shift;

    for my $key ( keys %{ $args } ) {
        next if $key =~ m{ \A (?: archive | log | backup-dir ) \z }x;    # Skip those, not needed in $self
        $self->{ $key } = $args->{ $key };
    }

    # sleep is in miliseconds, but we need microseconds for Time::HiRes::usleep
    $self->{ 'sleep' } *= 1000;

    $self->log->fatal( 'Archive path not provided!' ) unless $args->{ 'archive' };

    if ( $args->{ 'archive' } =~ s/\A(gzip|bzip2|lzma|lz4|xz)=// ) {
        $self->{ 'archive' }->{ 'compression' } = $1;
        $self->{ 'archive' }->{ 'extension' }   = ext_for_compression( $1 );
    }
    $self->{ 'archive' }->{ 'path' } = $args->{ 'archive' };

    $self->log->fatal( 'Backup path not provided!' ) unless $args->{ 'backup-dir' };

    if ( $args->{ 'backup-dir' } =~ s/\A(gzip|bzip2|lzma|lz4|xz)=// ) {
        $self->{ 'backup-dir' }->{ 'compression' } = $1;
        $self->{ 'backup-dir' }->{ 'extension' }   = ext_for_compression( $1 );
    }
    $self->{ 'backup-dir' }->{ 'path' } = $args->{ 'backup-dir' };

    $self->log->log( 'Called with parameters: %s', join( ' ', @ARGV ) ) if $self->verbose;

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
    $self->log->fatal( 'keep-days have to be at least 1! (%s given)', $self->{ 'keep-days' } ) if 1 > $self->{ 'keep-days' };
    return;
}

1;
