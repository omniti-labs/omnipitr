package OmniPITR::Program::Backup;
use strict;
use warnings;

use base qw( OmniPITR::Program );

use File::Spec;
use File::Path qw( mkpath rmtree );
use English qw( -no_match_vars );
use OmniPITR::Tools qw( ext_for_compression run_command );
use Cwd;

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

=head1 start_writers()

Starts set of filehandles, which write to file, or to compression program,
to create final archives.

Each compression schema gets its own filehandle, and printing data to it,
will pass it to file directly or through compression program that has been
chosen based on command line arguments.

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
            $self->log->fatal( 'Cannot write to %s : %s', $full_file_path, $OS_ERROR );
        }

        my @command = map { quotemeta $_ } ( $self->{ $type . '-path' }, '--stdout', '-' );
        unshift @command, quotemeta( $self->{ 'nice-path' } ) unless $self->{ 'not-nice' };
        push @command, ( '>', quotemeta( $full_file_path ) );

        $self->log->log( "Starting \"%s\" writer to %s", $type, $full_file_path ) if $self->verbose;
        if ( open my $fh, '|-', join( ' ', @command ) ) {
            $writers{ $type } = $fh;
            next COMPRESSION;
        }
        $self->log->fatal( 'Cannot open command. Error: %s, Command: %s', $OS_ERROR, \@command );
    }
    $self->{ 'writers' } = \%writers;
    return;
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

    local $SIG{ 'PIPE' } = sub { $self->log->fatal( 'Got SIGPIPE while tarring %s for %s', $ARGS{ 'tar_dir' }, $self->{ 'sigpipeinfo' } ); };

    my @compression_command = ( $self->{ 'tar-path' }, 'cf', '-' );
    unshift @compression_command, $self->{ 'nice-path' } unless $self->{ 'not-nice' };

    if ( $ARGS{ 'excludes' } ) {
        push @compression_command, map { '--exclude=' . $_ } @{ $ARGS{ 'excludes' } };
    }

    if ( $ARGS{ 'transform' } ) {
        push @compression_command, '--transform=' . $ARGS{ 'transform' };
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
        $self->log->fatal( 'Cannot start tar (%s) : %s', $compression_str, $OS_ERROR );
    }

    chdir $previous_dir if $ARGS{ 'work_dir' };

    my $buffer;
    while ( my $len = sysread( $tar, $buffer, 8192 ) ) {
        while ( my ( $type, $fh ) = each %{ $self->{ 'writers' } } ) {
            $self->{ 'sigpipeinfo' } = $type;
            my $written = syswrite( $fh, $buffer, $len );
            next if $written == $len;
            $self->log->fatal( "Writting %u bytes to filehandle for <%s> compression wrote only %u bytes ?!", $len, $type, $written );
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

1;
