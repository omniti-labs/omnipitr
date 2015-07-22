package OmniPITR::Program::Synch;
use strict;
use warnings;

our $VERSION = '1.3.3';
use base qw( OmniPITR::Program );

use Carp qw( croak );
use English qw( -no_match_vars );
use Getopt::Long qw( :config no_ignore_case );
use IO::Select;
use POSIX ":sys_wait_h";

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

    $self->get_list_of_directories_to_process();

    $self->prepare_commands_to_run();

    $self->get_user_confirmation();

    $self->run_pg_start_backup();

    $self->run_transfers();

    $self->run_pg_stop_backup();

    if ( $self->{ 'errors_in_workers' } ) {
        $self->log->log( 'Work is done, but there were problems in transfer worker process(es): %s', join( ', ', @{ $self->{ 'errors_in_workers' } } ) );
    }
    else {
        $self->log->log( 'All done.' );
    }
    exit;
}

=head1 run_transfers

Runs alls the transfer commands, watching for errors.

=cut

sub run_transfers {
    my $self = shift;
    my $s    = IO::Select->new();

    my %pid_for_fh = ();
    my %dead_pids  = ();

    my $previous_sig_chld = $SIG{ 'CHLD' };
    $SIG{ 'CHLD' } = sub {
        my $child;
        while ( ( $child = waitpid( -1, WNOHANG ) ) > 0 ) {
            $dead_pids{ $child } = $CHILD_ERROR;
        }
        return;
    };

    for my $c ( @{ $self->{ 'commands' } } ) {
        my $real_command = sprintf '%s -c %s 2>&1', $self->{ 'shell-path' }, quotemeta( $c->{ 'cmd' } );
        my $kid_pid = open( my $fh, '-|', $real_command );
        $s->add( $fh );
        $pid_for_fh{ "$fh" } = $kid_pid;
        $self->log->log( 'Started [%s] with pid: %d', $c->{ 'label' }, $kid_pid );
        $self->log->log( 'Full command: %s', $c->{ 'cmd' } ) if $self->verbose;
    }

    while ( 1 ) {
        last if 0 == $s->count();
        my @ready = $s->can_read( 10 );
        for my $fh ( @ready ) {
            my $pid = $pid_for_fh{ "$fh" };

            my $buffer;
            my $len = sysread( $fh, $buffer, 8192 );

            if ( 0 == $len ) {
                next unless defined $dead_pids{ $pid };
                my $error_no       = $dead_pids{ $pid };
                my $exit_code      = $error_no >> 8;
                my $killing_signal = $error_no & 127;
                if ( $exit_code ) {

                    # It's not 0 so there was some problem
                    push @{ $self->{ 'errors_in_workers' } }, $pid;
                }
                $self->log->log( 'Job %d finished with status %d%s.', $pid, $exit_code, $killing_signal ? "(Killed by signal $killing_signal)" : "" );
                delete $dead_pids{ $pid };
                delete $pid_for_fh{ "$fh" };
                $s->remove( $fh );
                next;
            }
            $buffer =~ s{^}{$pid > }gm;    # Prepend the line(s) with pid number
            $self->log->log( $buffer );
        }
    }

    $SIG{ 'CHLD' } = $previous_sig_chld;
    return;
}

=head1 run_pg_start_backup

Calls pg_start_backup() function in database, and checks if return is sane.

=cut

sub run_pg_start_backup {
    my $self = shift;

    $self->log->log( 'Calling pg_start_backup()' ) if $self->verbose();

    my $start_backup_output = $self->psql( "SELECT pg_start_backup('omnipitr-synch')" );
    $start_backup_output =~ s/\s*\z//;
    $self->log->log( q{pg_start_backup('omnipitr') returned %s.}, $start_backup_output );
    $self->log->fatal( 'Output from pg_start_backup is not parseable?!' ) unless $start_backup_output =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\z};

    return;
}

=head1 run_pg_stop_backup

Calls pg_stop_backup() function in database, and checks if return is sane.

=cut

sub run_pg_stop_backup {
    my $self = shift;

    $self->log->log( 'Calling pg_stop_backup()' ) if $self->verbose();

    my $stop_backup_output = $self->psql( "SELECT pg_stop_backup()" );
    $stop_backup_output =~ s/\s*\z//;
    $self->log->log( 'pg_stop_backup() returned %s.', $stop_backup_output );
    $self->log->fatal( 'Output from pg_stop_backup is not parseable?!' ) unless $stop_backup_output =~ m{\A([0-9A-F]+)/([0-9A-F]{1,8})\z};

    return;
}

=head1 prepare_commands_to_run

Helper function, which, for every source generates command that has to be
run to transfer it to destinations.

Generated commands are stored in $self->{'commands'} arrayref, as hashref,
with keys:

=over

=item * cmd - command to run (via shell)

=item * label - short description what this command does

=back

Internally there is no logic in this function - it just redirects call to
prepare_commands_to_run_via_tar or prepare_commands_to_run_via_rsync
depending on existence of --rsync switch

=cut

sub prepare_commands_to_run {
    my $self = shift;

    if ( $self->{ 'rsync' } ) {
        $self->prepare_commands_to_run_via_rsync();
    }
    else {
        $self->prepare_commands_to_run_via_tar();
    }
    return;
}

=head1 prepare_commands_to_run_via_tar

Creates command list (vide prepare_commands_to_run method) when
I<omnipitr-synch> was called without --rsync option.

=cut

sub prepare_commands_to_run_via_tar {
    my $self     = shift;
    my @commands = ();
    for my $t ( @{ $self->{ 'transfers' } } ) {
        my @tar_cmd = ();
        push @tar_cmd, 'cd', quotemeta( $t->{ 'source' } ), ';';
        push @tar_cmd, quotemeta( $self->{ 'tar-path' } );
        push @tar_cmd, 'cf', '-';
        push @tar_cmd, '--exclude=postmaster.pid';
        push @tar_cmd, '--exclude=pg_xlog/0*';
        push @tar_cmd, '--exclude=pg_xlog/archive_status/*';
        push @tar_cmd, '--exclude=pg_log';
        push @tar_cmd, '.';

        my $tar_cmd = join ' ', @tar_cmd;
        my $compress_cmd = '';
        if ( $self->{ 'compress' } ) {
            $compress_cmd = sprintf ' | %s --stdout -', quotemeta( $self->{ 'compress' } );
        }
        my @deliveries = ();
        for my $o ( @{ $t->{ 'outputs' } } ) {

            my @rm_cmd = ( quotemeta( $self->{ 'remote-rm-path' } ), '-rf', quotemeta( $o->{ 'path' } ) . '/*' );
            my $rm = join ' ', @rm_cmd;

            my @remote_tar_cmd = ( quotemeta( $self->{ 'remote-tar-path' } ), 'xf', '-', '-C', quotemeta( $o->{ 'path' } ) );
            my $remote_tar = join ' ', @remote_tar_cmd;

            my $decompress = '';
            if ( $self->{ 'compress' } ) {
                $decompress = quotemeta( $self->{ 'remote-compressor-path' } ) . ' -d --stdout - | ';
            }

            my $remote_command = sprintf '%s; %s%s', $rm, $decompress, $remote_tar;
            my $ssh_command = sprintf '%s %s%s %s', $self->{ 'ssh-path' }, $o->{ 'user' } ? $o->{ 'user' } . '@' : '', $o->{ 'host' }, quotemeta( $remote_command );
            push @deliveries, $ssh_command;
        }

        my $cmd = {};
        $cmd->{ 'label' } = "Sending " . $t->{ 'source' };
        $cmd->{ 'cmd' }   = $tar_cmd . $compress_cmd . ' | ';
        if ( 1 == scalar @deliveries ) {
            $cmd->{ 'cmd' } .= $deliveries[ 0 ];
        }
        else {
            $cmd->{ 'cmd' } .= $self->{ 'tee-path' };
            my $final = pop @deliveries;
            for my $d ( @deliveries ) {
                $cmd->{ 'cmd' } .= " >( $d )";
            }
            $cmd->{ 'cmd' } .= " > >( $final )";
        }
        push @commands, $cmd;
    }
    $self->{ 'commands' } = \@commands;
    return;
}

=head1 prepare_commands_to_run_via_rsync

Creates command list (vide prepare_commands_to_run method) when
I<omnipitr-synch> was called with --rsync option.

=cut

sub prepare_commands_to_run_via_rsync {
    my $self     = shift;
    my @commands = ();
    for my $t ( @{ $self->{ 'transfers' } } ) {
        my $src = $t->{ 'source' };
        $src =~ s{/*\z}{/};    # add slash
        for my $o ( @{ $t->{ 'outputs' } } ) {

            # We need to add one more layer of quoting for output paths in rsync mode, due to how rsync works.
            my $use_path = $self->{ 'rsync' } ? quotemeta( $o->{ 'path' } ) : $o->{ 'path' };
            my $dst_path = sprintf '%s%s:%s', ( $o->{ 'user' } ? $o->{ 'user' } . '@' : '' ), $o->{ 'host' }, $use_path;
            $dst_path =~ s{/*\z}{/};

            my @cmd = ();
            push @cmd, $self->{ 'rsync-path' };
            push @cmd, '-a';
            push @cmd, '-e', $self->{ 'ssh-path' };
            push @cmd, '-z' if $self->{ 'compress' };
            push @cmd, '--rsync-path=' . $self->{ 'remote-rsync-path' } if $self->{ 'remote-rsync-path' };
            push @cmd, '--delete';
            push @cmd, '--delete-excluded';
            push @cmd, '--exclude=/postmaster.pid';
            push @cmd, '--exclude=/pg_xlog/0*';
            push @cmd, '--exclude=/pg_xlog/archive_status/*';
            push @cmd, '--exclude=/pg_log';
            push @cmd, $src;
            push @cmd, $dst_path;
            my $full_cmd = join ' ', map { quotemeta } @cmd;
            push @commands,
                {
                'cmd'   => $full_cmd,
                'label' => "Rsyncing $src to $dst_path",
                };
        }
    }
    $self->{ 'commands' } = \@commands;
    return;
}

=head1 get_user_confirmation

This program should be run manually because it's potentially destructive.

Unless user specified -a option, we should print what is what, and get final confirmation.

=cut

sub get_user_confirmation {
    my $self = shift;

    print STDERR "Prepared transfers:\n";

    for my $t ( @{ $self->{ 'transfers' } } ) {
        printf STDERR "\nSource directory: %s\n  Outputs:\n", $t->{ 'source' };
        for my $o ( @{ $t->{ 'outputs' } } ) {
            printf STDERR "  - directory %s on host %s%s\n", $o->{ 'path' }, $o->{ 'host' }, $o->{ 'user' } ? ", logged as " . $o->{ 'user' } : "";
        }
    }

    print STDERR "All data in output directories will be overwritten\n";

    return if $self->{ 'automatic' };

    print STDERR "\nAre you sure you want to continue? (enter: YES): ";
    my $input = <STDIN>;
    return if $input =~ m{\AYES\r?\n\z};
    $self->log->fatal( 'User aborted.' );
}

=head1 get_list_of_directories_to_process

Gets (if it was not passed) data dir plus paths to all used tablespaces,
applies mapping, and stores it all as hashref in $self->{'transfers'}.

=cut

sub get_list_of_directories_to_process {
    my $self = shift;

    unless ( $self->{ 'data-dir' } ) {
        $self->{ 'data-dir' } = $self->psql( 'show data_directory' );
        $self->{ 'data-dir' } =~ s/\r?\n\z//;
    }
    $self->{ 'tablespaces' } = $self->get_tablespaces();
    $self->apply_output_mapping();

    my @transfers = ();

    my $t = {
        'source'  => $self->{ 'data-dir' },
        'outputs' => []
    };
    for my $o ( @{ $self->{ 'output' } } ) {
        push @{ $t->{ 'outputs' } }, { %{ $o } };    # clone of $o
    }
    push @transfers, $t;

    for my $ts ( values %{ $self->{ 'tablespaces' } } ) {
        $ts->{ 'pg_visible' } =~ s{/*$}{};
        $ts->{ 'real_path' } =~ s{/*$}{};
        my $t = {
            'source'  => $ts->{ 'real_path' },
            'outputs' => []
        };
        for my $o ( @{ $self->{ 'output' } } ) {
            my $to = { %{ $o } };                    # clone of $o;
            $to->{ 'path' } = $ts->{ 'mapped' } || $ts->{ 'pg_visible' };
            push @{ $t->{ 'outputs' } }, $to;
        }
        push @transfers, $t;
    }
    $self->{ 'transfers' } = \@transfers;
    return;
}

=head1 apply_output_mapping

For all tablespaces, apply mapping rules (given by -m option), and store the result under key 'mapped'

=cut

sub apply_output_mapping {
    my $self = shift;
    return unless $self->{ 'map' };
    for my $t ( values %{ $self->{ 'tablespaces' } } ) {
        my $path = $t->{ 'pg_visible' };
        for my $m ( @{ $self->{ 'map' } } ) {
            my $re  = $m->{ 're' };
            my $new = $m->{ 'new' };
            $path =~ s{$re}{$new};
        }
        $t->{ 'mapped' } = $path;
    }
    return;
}

=head1 read_args_specification

Defines which options are legal for this program.

=cut

sub read_args_specification {
    my $self = shift;

    return {
        'automatic' => { 'aliases' => [ 'a' ], },
        'compress'  => { 'type'    => 's', 'aliases' => [ 'c' ], },
        'data-dir'  => { 'type'    => 's', 'aliases' => [ 'D' ], },
        'database'  => { 'type'    => 's', 'aliases' => [ 'd' ], 'default' => 'postgres', },
        'host'   => { 'type' => 's',  'aliases' => [ 'h' ], },
        'log'    => { 'type' => 's',  'aliases' => [ 'l' ], 'default' => '-', },
        'map'    => { 'type' => 's@', 'aliases' => [ 'm' ], },
        'output' => { 'type' => 's@', 'aliases' => [ 'o' ], },
        'pid-file'  => { 'type' => 's', },
        'port'      => { 'type' => 'i', 'aliases' => [ 'p' ], },
        'psql-path' => { 'type' => 's', 'aliases' => [ 'pp' ], 'default' => 'psql', },
        'remote-compressor-path' => { 'type' => 's', 'aliases' => [ 'rcp' ], },
        'remote-rm-path'         => { 'type' => 's', 'aliases' => [ 'rrp' ], 'default' => 'rm', },
        'remote-rsync-path'      => { 'type' => 's', 'aliases' => [ 'rsp' ], 'default' => 'rsync', },
        'remote-tar-path'        => { 'type' => 's', 'aliases' => [ 'rtp' ], 'default' => 'tar', },
        'rsync-path'             => { 'type' => 's', 'aliases' => [ 'rp' ], 'default' => 'rsync', },
        'rsync'      => { 'aliases' => [ 'r' ], },
        'shell-path' => { 'type'    => 's', 'aliases' => [ 'sh' ], 'default' => 'bash', },
        'ssh-path'   => { 'type'    => 's', 'aliases' => [ 'sp' ], 'default' => 'ssh', },
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

    $args->{ 'remote-compressor-path' } = $args->{ 'compress' } if $args->{ 'compress' } && !$args->{ 'remote-compressor-path' };

    for my $key ( keys %{ $args } ) {
        next if $key =~ m{ \A log \z }x;    # Skip those, not needed in $self
        $self->{ $key } = $args->{ $key };
    }

    $self->log->log( 'Called with parameters: %s', join( ' ', @ARGV ) ) if $self->verbose;

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for programs paths - technically, it could be validated in
here, but benefit would be pretty limited, and code to do so relatively
complex, as program path might, but doesn't have to be actual file path - it
might be just program name (without path), which is the default.

=cut

sub validate_args {
    my $self = shift;

    if ( $self->{ 'data-dir' } ) {
        $self->log->fatal( 'Given data-dir (%s) is not valid', $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' } && -f File::Spec->catfile( $self->{ 'data-dir' }, 'PG_VERSION' );
    }

    $self->log->fatal( 'No output arguments provided!' ) unless $self->{ 'output' };

    my @parsed_outputs = ();
    for my $item ( @{ $self->{ 'output' } } ) {
        my $o = { 'definition' => $item, };
        if ( $item =~ m{ \A ([^@]+) @ ([^:]+) : (/.*) \z }x ) {
            $o->{ 'user' } = $1;
            $o->{ 'host' } = $2;
            $o->{ 'path' } = $3;
        }
        elsif ( $item =~ m{ \A ([^:]+) : (/.*) \z }x ) {
            $o->{ 'host' } = $1;
            $o->{ 'path' } = $2;
        }
        else {
            $self->log->fatal( 'Unparseable output: [%s]', $item );
        }
        $o->{ 'path' } =~ s{/*$}{};
        push @parsed_outputs, $o;
    }
    $self->{ 'output' } = \@parsed_outputs;

    if ( $self->{ 'map' } ) {
        my @maps = ();
        for my $m ( @{ $self->{ 'map' } } ) {
            $self->log->fatal( 'Unparseable map: [%s]', $m ) unless $m =~ m{\A([^:]*):(.*)\z};
            my ( $prefix, $sub ) = ( $1, $2 );
            push @maps,
                {
                're'  => qr{\A\Q$prefix\E},
                'new' => $sub,
                };
        }
        $self->{ 'map' } = \@maps;
    }

    return;
}

1;
