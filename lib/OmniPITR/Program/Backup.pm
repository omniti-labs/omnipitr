package OmniPITR::Program::Backup;
use strict;
use warnings;

our $VERSION = '0.2.1';
use base qw( OmniPITR::Program );

use File::Spec;
use File::Path qw( mkpath rmtree );
use File::Copy;
use English qw( -no_match_vars );
use OmniPITR::Tools qw( ext_for_compression run_command );
use Data::Dumper;
use Cwd;
use Digest;
use Config;

=head1 run()

Main function wrapping all work.

Starts with getting list of compressions that have to be done, then it
chooses where to compress to (important if we have remote-only destination),
then it makes actual backup, and delivers to all destinations.

=cut

sub run {
    my $self = shift;
    $self->get_list_of_all_necessary_compressions();
    $self->choose_base_local_destinations();

    $self->log->time_start( 'Making data archive' ) if $self->verbose;
    $self->make_data_archive();
    $self->log->time_finish( 'Making data archive' ) if $self->verbose;

    $self->log->time_start( 'Making xlog archive' ) if $self->verbose;
    $self->make_xlog_archive();
    $self->log->time_finish( 'Making xlog archive' ) if $self->verbose;

    $self->deliver_to_all_destinations();

    $self->log->log( 'All done.' );
    return;
}

=head1 make_xlog_archive()

Just a stub method, that has to be overriden in subclasses.

=cut

sub make_xlog_archive {
    my $self = shift;
    croak( "make_xlog_archive() method in OmniPITR::Program::Backup was not overridden!" );
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

        opendir( my $dh, $dir ) or $self->log->fatal( 'Cannot open %s for scanning: %s', $dir, $OS_ERROR );
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

    $self->log->fatal( 'Waited 1 hour for file matching %s, but it did not appear. Something is wrong. No sense in waiting longer.', $filename_regexp );

    return;
}

=head1 choose_base_local_destinations()

Chooses single local destination for every compression schema required by
destinations specifications.

In case some compression schema exists only for remote destination, local
temp directory is created in --temp-dir location.

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

=head1 get_tablespaces_and_transforms()

Helper function.  Takes no arguments.  Uses pg_tblspc directory and returns
a listref of the physical locations for tar to include in the backup as well
as a listref of the transform regexs that it will need to apply in order for
those directories to get untarred to the correct location from pg_tblspc's
perspective.

=cut

sub get_tablespaces_and_transforms {
    my $self = shift;

    # Identify any tablespaces and get those
    my $tablespace_dir = File::Spec->catfile( $self->{ 'data-dir' }, "pg_tblspc" );
    my ( %tablespaces, @transform_regexs );

    my $ts = $self->get_tablespaces();
    if ( defined $ts ) {

        # At this point pgfiles contains a list of the destinations.  Some of THOSE might be links however and need
        # to be identified since we need to pass the actual location bottom location to tar
        %tablespaces = map { $_->{ 'pg_visible' } => $_->{ 'real_path' } } values %{ $ts };

        # Populate the regexes to put these directories under tablespaces with transforms so that the actual physical location
        # is transformed into the 1-level deep link that the pg_tblspc files are pointing at.  We substr becase tar strips leading /
        push @transform_regexs, map { "s,^" . substr( $tablespaces{ $_ }, 1 ) . ",tablespaces$_," } keys %tablespaces;
    }
    $self->log->log( "Including tablespaces: " . ( join ", ", ( keys %tablespaces ) ) . "\n" ) if $self->verbose && keys %tablespaces;

    return ( [ values %tablespaces ], \@transform_regexs );
}

=head1 get_archive_filename()

Helper function, which takes filetype and compression schema to use, and
returns generated filename (based on filename-template command line option).

=cut

sub get_archive_filename {
    my $self = shift;
    my ( $type, $compression ) = @_;

    $compression = 'none' unless defined $compression;

    my $ext = $compression eq 'none' ? '' : ext_for_compression( $compression );

    my $filename = $self->{ 'filename-template' };
    $filename =~ s/__FILETYPE__/$type/g;
    $filename =~ s/__CEXT__/$ext/g;

    return $filename;
}

=head1 tar_and_compress()

Worker function which does all of the actual tar, and sending data to
compression filehandles (should be opened before).

Takes hash (not hashref) as argument, and uses following keys from it:

=over

=item * tar_dir - arrayref with list of directories to compress

=item * work_dir - what should be current working directory when executing
tar

=item * excludes - optional key, that (if exists) is treated as arrayref of
shell globs (tar dir) of items to exclude from backup

=item * transform - optional key, that (if exists) is treated as value for
--transform option for tar

=back

If tar will print anything to STDERR it will be logged. Error status code is
ignored, as it is expected that tar will generate errors (due to files
modified while archiving).

Requires following keys in $self:

=over

=item * nice-path

=item * tar-path

=back

=cut

sub tar_and_compress {
    my $self = shift;
    my %ARGS = @_;

    my $tar = $self->_tar_command( %ARGS );

    $self->log->log( 'Actual command to make tarballs: %s', $tar ) if $self->verbose;

    my @full_command = ();
    push @full_command, 'exec', quotemeta( $self->{ 'shell-path' } );
    push @full_command, '-c',   quotemeta( $tar );
    push @full_command, '>',    quotemeta( $self->temp_file( 'full_tar.stdout' ) );
    push @full_command, '2>',   quotemeta( $self->temp_file( 'full_tar.stderr' ) );

    my $previous_dir = getcwd;
    chdir $ARGS{ 'work_dir' } if $ARGS{ 'work_dir' };
    my $retval = system( join( ' ', @full_command ) );
    chdir $previous_dir if $ARGS{ 'work_dir' };

    my @files = (
        'tar stderr'          => $self->temp_file( 'tar.stderr' ),
        'full command stdout' => $self->temp_file( 'full_tar.stdout' ),
        'full command stderr' => $self->temp_file( 'full_tar.stderr' ),
    );

    while ( my $desc = shift @files ) {
        my $filename = shift @files;
        next unless -s $filename;
        my $fh;
        unless ( open $fh, '<', $filename ) {
            $self->log->log( 'Cannot open %s file (%s) for reading: %s', $desc, $filename, $OS_ERROR );
            next;
        }
        my $data;
        {
            local $/;
            $data = <$fh>;
        };
        close $fh;
        $self->log->log( '%s:', $desc );
        $self->log->log( '==============================================' );
        $self->log->log( '%s', $data );
        $self->log->log( '==============================================' );
    }
    return;
}

=head1 deliver_to_all_destinations()

Simple wrapper to have single point to call to deliver backups to all
requested backups.

=cut

sub deliver_to_all_destinations {
    my $self = shift;

    $self->deliver_to_all_local_destinations();

    $self->deliver_to_all_remote_destinations();

    return;
}

=head1 deliver_to_all_local_destinations()

Copies backups to all local destinations which are not also base
destinations for their respective compressions.

=cut

sub deliver_to_all_local_destinations {
    my $self = shift;
    return unless $self->{ 'destination' }->{ 'local' };
    for my $dst ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        next if $dst->{ 'path' } eq $self->{ 'base' }->{ $dst->{ 'compression' } };

        my $B = $self->{ 'base' }->{ $dst->{ 'compression' } };

        for my $type ( ( @{ $self->{ 'digests' } }, qw( data xlog ) ) ) {

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

        for my $type ( ( @{ $self->{ 'digests' } }, qw( data xlog ) ) ) {

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

=head1 _tar_command()

Helper function which returns string to be passed to system() to run tar of given directory

=cut

sub _tar_command {
    my $self = shift;
    my %ARGS = @_;

    my @tar_command = ( $self->{ 'tar-path' }, 'cf', '-' );
    unshift @tar_command, $self->{ 'nice-path' } unless $self->{ 'not-nice' };
    unshift @tar_command, 'exec';

    if ( $ARGS{ 'excludes' } ) {
        push @tar_command, map { '--exclude=' . $_ } @{ $ARGS{ 'excludes' } };
    }

    if ( $ARGS{ 'transform' } ) {
        if ( ref $ARGS{ 'transform' } ) {
            push @tar_command, map { '--transform=' . $_ } @{ $ARGS{ 'transform' } };
        }
        else {
            push @tar_command, '--transform=' . $ARGS{ 'transform' };
        }
    }

    push @tar_command, @{ $ARGS{ 'tar_dir' } };

    my $tar_str = join ' ', map { quotemeta $_ } @tar_command;

    my $tar_stderr_filename = $self->temp_file( 'tar.stderr' );

    $tar_str .= " 2> " . quotemeta( $tar_stderr_filename );

    my @writers = $self->_compression_commands( $ARGS{ 'data_type' } );

    if ( 1 == scalar @writers ) {
        $tar_str .= ( $writers[ 0 ]->{ 'command' } ? ' | ' : ' > ' ) . $writers[ 0 ]->{ 'str' };
    }
    else {
        my $last = pop @writers;
        $tar_str .= ' | exec ' . quotemeta( $self->{ 'tee-path' } );
        for ( @writers ) {
            $tar_str .= " " . ( $_->{ 'command' } ? ">( " . $_->{ 'str' } . " )" : $_->{ 'str' } );
        }
        $tar_str .= ( $last->{ 'command' } ? ' | ' : ' > ' ) . $last->{ 'str' };
    }

    return $tar_str;
}

=head1 _compression_commands()

Helper function which returns array. Each element of the array is string to be passed to system() that will cause it to compress data from stdin to appropriate output file.

=cut

sub _compression_commands {
    my $self      = shift;
    my $data_type = shift;
    my @reply     = ();
    my $nice      = $self->{ 'not-nice' } ? "" : quotemeta( $self->{ 'nice-path' } ) . " ";

    while ( my ( $compression_type, $dst_path ) = each %{ $self->{ 'base' } } ) {
        my $output = $self->get_archive_filename( $data_type, $compression_type );
        my $output_path = quotemeta( File::Spec->catfile( $dst_path, $output ) );

        my $reply_part = {};

        if ( 'none' eq $compression_type ) {
            $reply_part->{ 'str' } = $output_path;
        }
        else {
            $reply_part->{ 'str' }     = 'exec ' . $nice . quotemeta( $self->{ $compression_type . '-path' } ) . ' --stdout -';
            $reply_part->{ 'command' } = 1;
        }

        if ( 0 < scalar @{ $self->{ 'digests' } } ) {
            my @digest_strs = ();
            for my $d ( @{ $self->{ 'digests' } } ) {
                my $digest_filename = File::Spec->catfile( $dst_path, $self->get_archive_filename( $d, $compression_type ) );
                my @digest = ( $Config{ 'perlpath' }, '-MDigest', '-le', q{binmode STDIN;print Digest->new("} . $d . q{")->addfile(\*STDIN)->hexdigest().' *'.$ARGV[0]}, $output );
                my $digest_str = 'exec ' . $nice . join( ' ', map { quotemeta $_ } @digest );
                $digest_str .= ( $data_type eq 'data' ? ' > ' : ' >> ' ) . quotemeta( $digest_filename );
                push @digest_strs, $digest_str;
            }

            if ( 'none' eq $compression_type ) {
                push @reply, $reply_part;
                push @reply, map { { 'command' => 1, 'str' => $_ } } @digest_strs;
            }
            else {
                $reply_part->{ 'str' } .= ' | exec ' . $self->{ 'tee-path' };
                $reply_part->{ 'str' } .= " >( $_ )" for @digest_strs;
                $reply_part->{ 'str' } .= ' > ' . $output_path;
                push @reply, $reply_part;
            }
        }
        else {
            $reply_part->{ 'str' } .= ' > ' . $output_path unless 'none' eq $compression_type;
            push @reply, $reply_part;
        }
    }

    return @reply;
}

1;
