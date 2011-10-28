package OmniPITR::Program::Backup;
use strict;
use warnings;

our $VERSION = '0.1.3';
use base qw( OmniPITR::Program );

use File::Spec;
use File::Path qw( mkpath rmtree );
use File::Copy;
use English qw( -no_match_vars );
use OmniPITR::Tools qw( ext_for_compression run_command );
use Cwd;
use IPC::Open2;
use Digest;
use Fcntl qw(F_GETFL F_SETFL O_NONBLOCK);

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

=head1 psql()

Runs given query via psql (assuming options stored in $self->{'psql'}).

In case of errors, it raises fatal error.

Otherwise returns stdout of the psql.

=cut

sub psql {
    my $self = shift;

    my $query = shift;

    $self->prepare_temp_directory();

    my @command = ( @{ $self->{ 'psql' } }, $query );

    $self->log->time_start( $query ) if $self->verbose;
    my $status = run_command( $self->{ 'temp-dir' }, @command );
    $self->log->time_finish( $query ) if $self->verbose;

    $self->log->fatal( 'Running [%s] via psql failed: %s', $query, $status ) if $status->{ 'error_code' };

    return $status->{ 'stdout' };
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

    while ( my ( $compression_type, $dst_path ) = each %{ $self->{ 'base' } } ) {
        my $filename = $self->get_archive_filename( $data_type, $compression_type );

        $self->{ 'writers' }->{ $data_type }->{ $compression_type } = { 'filename' => $filename };

        my $full_file_path = File::Spec->catfile( $dst_path, $filename );

        if ( open my $fh, '>', $full_file_path ) {
            $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'final' } = $fh;
            $self->log->log( "Starting writer to $full_file_path" ) if $self->verbose;
        }
        else {
            $self->log->fatal( 'Cannot write to %s : %s', $full_file_path, $OS_ERROR );
        }

        if ( $compression_type ne 'none' ) {
            my @command = map { quotemeta $_ } ( $self->{ $compression_type . '-path' }, '--stdout', '-' );
            unshift @command, quotemeta( $self->{ 'nice-path' } ) unless $self->{ 'not-nice' };

            $self->log->log( "Starting \"%s\" writer", $compression_type ) if $self->verbose;

            open2( my $in_fh, my $out_fh, join( ' ', @command ) );

            # We need to catch an exception here
            my $flags = fcntl( $in_fh, F_GETFL, 0 );
            $flags = fcntl( $in_fh, F_SETFL, $flags | O_NONBLOCK );
            $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'compression_in' }  = $in_fh;
            $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'compression_out' } = $out_fh;
        }

        for ( my $i = 0; $i < scalar( @{ $self->{ 'digests' } } ); $i++ ) {
            my $digest = $self->{ 'digests' }->[$i];
            if ( defined( $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'digest_obj' }->{ $digest } ) ) {
                splice( @{ $self->{ 'digests' } }, $i--, 1 );
            }
            else {
                eval {
                    $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'digest_obj' }->{ $digest } = Digest->new( $digest );
                };
                if ($@) {
                    $self->log->log( 'Cannot use digest method %s', $digest );
                    delete( $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'digest_obj' }->{ $digest } );
                    splice( @{ $self->{ 'digests' } }, $i--, 1 );
                }
                else {
                    my $digest_filename = $self->get_archive_filename( $digest, $compression_type );
                    my $full_digest_path = File::Spec->catfile( $dst_path, $digest_filename );

                    if ( open( my $fh, '>>', $full_digest_path ) ) {
                        $self->{ 'writers' }->{ $data_type }->{ $compression_type }->{ 'digest_fh' }->{ $digest } = $fh;
                        $self->log->log( "Starting writer to $full_digest_path" ) if $self->verbose;
                    }
                    else {
                        $self->log->fatal( 'Cannot write to %s : %s', $full_file_path, $OS_ERROR );
                    }
                }
            }
        }
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
    if ( -e $tablespace_dir ) {
        my @pgfiles;
        opendir( my $dh, $tablespace_dir ) or $self->log->fatal( "Unable to open tablespace directory $tablespace_dir" );

        # Push onto our list the locations that are pointed to by the pg_tblspc symlinks
        foreach my $filename ( readdir $dh ) {
            next if $filename !~ /^\d+$/;    # Filename should be all numeric
            my $full_name = File::Spec->catfile( $tablespace_dir, $filename );
            next if !-l $full_name;          # It should be a symbolic link
            my $link = readlink $full_name;
            push @pgfiles, $link if $link;    # If it's a valid link, put it onto the list
        }
        closedir $dh;

        # At this point pgfiles contains a list of the destinations.  Some of THOSE might be links however and need
        # to be identified since we need to pass the actual location bottom location to tar
        %tablespaces = map { $_ => Cwd::abs_path( $_ ) } @pgfiles;

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

    local $SIG{ 'PIPE' } = sub { $self->log->fatal( 'Got SIGPIPE while tarring %s for %s', $ARGS{ 'tar_dir' }, $self->{ 'sigpipeinfo' } ); };

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

    push @tar_command, @{ $ARGS{ 'tar_dir' } };

    my $tar_str = join ' ', map { quotemeta $_ } @tar_command;

    $self->prepare_temp_directory();

    my $tar_stderr_filename = File::Spec->catfile( $self->{ 'temp-dir' }, 'tar.stderr' );
    $tar_str .= ' 2> ' . quotemeta( $tar_stderr_filename );

    my $previous_dir = getcwd;
    chdir $ARGS{ 'work_dir' } if $ARGS{ 'work_dir' };

    my $tar;
    unless ( open $tar, '-|', $tar_str ) {
        $self->log->fatal( 'Cannot start tar (%s) : %s', $tar_str, $OS_ERROR );
    }

    chdir $previous_dir if $ARGS{ 'work_dir' };

    my $len = 0;
    do {
        $len = sysread( $tar, my $buffer, 8192 );
        while ( my ( $compression_type, $fhs ) = each %{ $self->{ 'writers' }->{ $ARGS{ 'data_type' } } } ) {
            $self->{ 'sigpipeinfo' } = $compression_type;

            if ( ( $compression_type eq 'none' ) and ( $len > 0 ) ) {
                my $written = syswrite( $fhs->{ 'final' }, $buffer, $len );

                if ( $written != $len ) {
                    $self->log->fatal( "Writting %u bytes to filehandle for %s wrote only %u bytes ?!", $len, $fhs->{ 'filename' }, $written );
                }

                foreach my $digester ( values %{ $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_obj' } } ) {
                    $digester->add( $buffer );
                }
            }
            elsif ( ( $compression_type eq 'none' ) and ( $len == 0 ) ) {
                close( $fhs->{ 'final' } );

                while ( my ( $digest_type, $digester ) = each %{ $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_obj' } } ) {
                    my $digest = $digester->hexdigest();
                    $self->log->log( "File: %s Method: %s Digest: %s", $fhs->{ 'filename' }, $digest_type, $digest );
                    my $fh = $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_fh' }->{ $digest_type };
                    print $fh $digest . " *" . $fhs->{ 'filename' } . "\n";
                    close( $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_fh' }->{ $digest_type } );
                }
            }
            else {
                if ( $len > 0 ) {
                    my $written = syswrite( $fhs->{ 'compression_out' }, $buffer, $len );

                    if ( $written != $len ) {
                        $self->log->fatal( "Writting %u bytes to filehandle for <%s> compression wrote only %u bytes ?!", $len, $compression_type, $written );
                    }
                }
                else {
                    close( $fhs->{ 'compression_out' } );
                    my $flags = fcntl( $fhs->{ 'compression_in' }, F_GETFL, 0 );
                    $flags = fcntl( $fhs->{ 'compression_in' }, F_SETFL, $flags & ~O_NONBLOCK );
                }

                while ( my $comp_len = sysread( $fhs->{ 'compression_in' }, my $comp_buffer, 8192 ) ) {
                    foreach my $digester ( values %{ $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_obj' } } ) {
                        $digester->add( $comp_buffer );
                    }

                    my $final_written = syswrite( $fhs->{ 'final' }, $comp_buffer, $comp_len );

                    if ( $final_written != $comp_len ) {
                        $self->log->fatal( "Writting %u bytes to filehandle for %s wrote only %u bytes ?!", $comp_len, $fhs->{ 'filename' }, $final_written );
                    }
                }

                if ( $len == 0 ) {
                    close( $fhs->{ 'compression_in' } );
                    close( $fhs->{ 'final' } );

                    while ( my ( $digest_type, $digester ) = each %{ $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_obj' } } ) {
                        my $digest = $digester->hexdigest();
                        $self->log->log( "File: %s Method: %s Digest: %s", $fhs->{ 'filename' }, $digest_type, $digest );
                        my $fh = $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_fh' }->{ $digest_type };
                        print $fh $digest . " *" . $fhs->{ 'filename' } . "\n";
                        close( $self->{ 'writers' }->{ $ARGS{ 'data_type' } }->{ $compression_type }->{ 'digest_fh' }->{ $digest_type } );
                    }
                }
            }
        }
    } until ( $len == 0 );

    close $tar;

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
    $self->log->log( 'Tar (%s) generated these output on stderr:', $tar_str );
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

        for my $type ( ( @{ $self->{digests} }, qw( data xlog ) ) ) {

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

        for my $type ( ( @{ $self->{digests} }, qw( data xlog ) ) ) {

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
