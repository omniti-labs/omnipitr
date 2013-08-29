package OmniPITR::Program::Backup;
use strict;
use warnings;

our $VERSION = '1.2.0';
use base qw( OmniPITR::Program );

use Config;
use Cwd qw(abs_path getcwd);
use Data::Dumper;
use Digest;
use English qw( -no_match_vars );
use File::Copy;
use File::Path qw( mkpath rmtree );
use File::Spec;
use File::Basename;
use OmniPITR::Tools::CommandPiper;
use OmniPITR::Tools::ParallelSystem;
use OmniPITR::Tools qw( ext_for_compression run_command );

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

    $self->deliver_to_all_remote_destinations();

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

There can be additional destinations for given compression, if we have direct destinations.

=cut

sub choose_base_local_destinations {
    my $self = shift;

    my $base = { map { ( $_ => undef ) } @{ $self->{ 'compressions' } } };
    $self->{ 'base' } = $base;

    for my $dst ( @{ $self->{ 'destination' }->{ 'local' } } ) {
        my $type = $dst->{ 'compression' };
        push @{ $base->{ $type } }, { 'type' => 'local', 'path' => $dst->{ 'path' } };
    }

    $self->prepare_temp_directory();

    for my $dst ( @{ $self->{ 'destination' }->{ 'remote' } } ) {
        my $type = $dst->{ 'compression' };
        next if defined $base->{ $type };
        my $tmp_dir = File::Spec->catfile( $self->{ 'temp-dir' }, $type );
        mkpath( $tmp_dir );
        $base->{ $type } = [ { 'type' => 'local', 'path' => $tmp_dir } ];
    }

    for my $dst ( @{ $self->{ 'destination' }->{ 'direct' } } ) {
        my $type = $dst->{ 'compression' };
        push @{ $base->{ $type } }, { 'type' => 'direct', 'path' => $dst->{ 'path' } };
    }

    for my $dst ( @{ $self->{ 'destination' }->{ 'pipe' } } ) {
        my $type = $dst->{ 'compression' };
        push @{ $base->{ $type } }, { 'type' => 'pipe', 'path' => $dst->{ 'path' } };
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

    $self->log->log( 'Script to make tarballs:%s%s', "\n", $tar ) if $self->verbose;

    my $script_file = $self->temp_file( 'tar.script' );
    open my $scr_fh, '>', $script_file or croak( "Cannot write to $script_file?!" );
    print $scr_fh $tar;
    close $scr_fh;

    my @full_command = ();
    push @full_command, quotemeta( $self->{ 'shell-path' } );
    push @full_command, quotemeta( $script_file );
    push @full_command, '>', quotemeta( $self->temp_file( 'full_tar.stdout' ) );
    push @full_command, '2>', quotemeta( $self->temp_file( 'full_tar.stderr' ) );

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

=head1 deliver_to_all_remote_destinations()

Delivers backups to remote destinations using rsync program.

=cut

sub deliver_to_all_remote_destinations {
    my $self = shift;
    return unless $self->{ 'destination' }->{ 'remote' };

    my $handle_finish = sub {
        my $job = shift;
        $self->log->log( 'Copying %s to %s ended in %.6fs', $job->{ 'source_filename' }, $job->{ 'destination_filename' }, $job->{ 'ended' } - $job->{ 'started' } ) if $self->verbose;
        return unless $job->{ 'status' };
        $self->log->error( 'Cannot send archive %s to %s: %s', $job->{ 'source_filename' }, $job->{ 'destination_filename' }, $job );
        $self->{ 'had_errors' } = 1;
        return;
    };

    my $runner = OmniPITR::Tools::ParallelSystem->new(
        'max_jobs'  => $self->{ 'parallel-jobs' },
        'on_finish' => $handle_finish,
    );

    for my $dst ( @{ $self->{ 'destination' }->{ 'remote' } } ) {

        my $B = $self->{ 'base' }->{ $dst->{ 'compression' } }->[ 0 ]->{ 'path' };

        for my $type ( ( @{ $self->{ 'digests' } }, qw( data xlog ) ) ) {

            my $filename = $self->get_archive_filename( $type, $dst->{ 'compression' } );
            my $source_filename = File::Spec->catfile( $B, $filename );
            my $destination_filename = $dst->{ 'path' };
            $destination_filename =~ s{/*\z}{/};
            $destination_filename .= $filename;

            $runner->add_command(
                'command'              => [ $self->{ 'rsync-path' }, $source_filename, $destination_filename ],
                'source_filename'      => $source_filename,
                'destination_filename' => $destination_filename,
            );
        }
    }

    $ENV{ 'TMPDIR' } = $self->{ 'temp-dir' };

    $self->log->time_start( 'Delivering to all remote destinations' ) if $self->verbose;
    $runner->run();
    $self->log->time_finish( 'Delivering to all remote destinations' ) if $self->verbose;

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

    if ( 200 > scalar @{ $ARGS{ 'tar_dir' } } ) {
        push @tar_command, @{ $ARGS{ 'tar_dir' } };
    }
    else {
        my $tar_list_file = $self->temp_file( 'tar.file.list' );
        open my $fh, '>', $tar_list_file or $self->log->fatal( 'Cannot write to temporary file?!' );
        print $fh "$_\n" for @{ $ARGS{ 'tar_dir' } };
        close $fh;
        push @tar_command, '--files-from=' . $tar_list_file;
    }

    my $tar = OmniPITR::Tools::CommandPiper->new( @tar_command );

    $tar->add_stderr_file( $self->temp_file( 'tar.stderr' ) );

    $self->_add_tar_consummers( $tar, $ARGS{ 'data_type' } );

    return $tar->command();
}

=head1 _add_tar_consummers()

Helper function which returns array. Each element of the array is string to be passed to system() that will cause it to compress data from stdin to appropriate output file.

=cut

sub _add_tar_consummers {
    my $self      = shift;
    my $tar       = shift;
    my $data_type = shift;

    for my $compression_type ( keys %{ $self->{ 'base' } } ) {
        my $compressed = $tar;

        if ( $compression_type ne 'none' ) {
            my @cmd = ( $self->{ $compression_type . '-path' }, '--stdout', '-' );
            unshift @cmd, $self->{ 'nice-path' } unless $self->{ 'not-nice' };
            $compressed = $tar->add_stdout_program( @cmd );
        }

        my $tarball_filename = $self->get_archive_filename( $data_type, $compression_type );

        my %digesters = ();
        for my $d ( @{ $self->{ 'digests' } } ) {
            my @digest = ( File::Spec->catfile( abs_path( $FindBin::Bin ), 'omnipitr-checksum' ), '-d', $d, '-f', $tarball_filename );
            unshift @digest, $self->{ 'nice-path' } unless $self->{ 'not-nice' };
            $digesters{ $d } = $compressed->add_stdout_program( @digest );
            $digesters{ $d }->set_write_mode( 'append' );
        }

        for my $destination ( @{ $self->{ 'base' }->{ $compression_type } } ) {
            if ( $destination->{ 'type' } eq 'local' ) {
                $compressed->add_stdout_file( File::Spec->catfile( $destination->{ 'path' }, $tarball_filename ) );
                while ( my ( $digest_type, $digester ) = each %digesters ) {
                    $digester->add_stdout_file( File::Spec->catfile( $destination->{ 'path' }, $self->get_archive_filename( $digest_type, $compression_type ) ) );
                }
                next;
            }

            if ( $destination->{ 'type' } eq 'pipe' ) {
                $compressed->add_stdout_program( $destination->{ 'path' }, $tarball_filename );
                while ( my ( $digest_type, $digester ) = each %digesters ) {
                    $digester->add_stdout_program( $destination->{ 'path' }, $self->get_archive_filename( $digest_type, $compression_type ) );
                }
                next;
            }

            # it's not local, nor pipe, so it has to be remote now
            $compressed->add_stdout_program( $self->_get_remote_writer_command( $destination->{ 'path' }, $tarball_filename ) );
            while ( my ( $digest_type, $digester ) = each %digesters ) {
                $digester->add_stdout_program(
                    $self->_get_remote_writer_command(
                        $destination->{ 'path' },
                        $self->get_archive_filename( $digest_type, $compression_type ),
                        $data_type eq 'data' ? 'overwrite' : 'append',
                    )
                );
            }
        }
    }
    return;
}

=head1 _get_remote_writer_command()

Helper function returning command line that should be added to deliver data to direct destination

=cut

sub _get_remote_writer_command {
    my $self             = shift;
    my $remote_path      = shift;
    my $tarball_filename = shift;
    my $mode             = shift || 'overwrite';

    $self->log->fatal( 'Given destination path is not parseable?! [%s]', $remote_path ) unless $remote_path =~ m{\A([^:]+):(.*)\z};
    my $user_host = $1;
    $remote_path = $2;
    $remote_path =~ s{/*$}{/$tarball_filename};

    my @remote_command = ();
    if ( substr( $self->{ 'remote-cat-path' }, 0, 1 ) eq '!' ) {
        push @remote_command, quotemeta( substr( $self->{ 'remote-cat-path' }, 1 ) );
    }
    else {
        push @remote_command, quotemeta( $self->{ 'remote-cat-path' } );
        push @remote_command, '-';
        push @remote_command, $mode eq 'overwrite' ? '>' : '>>';
    }
    push @remote_command, quotemeta( $remote_path );

    my $remote_sh = join ' ', @remote_command;
    return ( $self->{ 'ssh-path' }, $user_host, $remote_sh );
}
1;
