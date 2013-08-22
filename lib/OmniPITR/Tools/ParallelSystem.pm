package OmniPITR::Tools::ParallelSystem;
use strict;
use warnings;
use Carp qw( croak );
use File::Temp qw( tempfile );
use Time::HiRes;
use POSIX qw( :sys_wait_h );
use English qw( -no_match_vars );

=head1 NAME

OmniPITR::Tools::ParallelSystem - Class for running multiple shell commands in parallel.

=cut

our $VERSION = '1.1.0';

=head1 SYNOPSIS

General usage is:

    my $run = OmniPITR::Tools::ParallelSystem( 'max_jobs' => 2, ... );
    $run->add_command( 'command' => [ 'ls', '-l', '/' ] );
    $run->add_command( 'command' => [ 'ls', '-l', '/root' ] );
    $run->run();
    my $results = $run->results();

=head1 DESCRIPTION

ParallelSystem strives to make the task of running in parallel simple, and effective.

It lets you define any number of commands, set max number of concurrent workers, set startup/finish hooks, and run the whole thing.

Final ->run() is blocking, and it (temporarily) sets CHLD signal handler to its own code, but it is reset to original value afterwards.

=head1 INTERNALS

=head2 new()

Object constructor. Takes one obligatory argument, and two optional:

=over

=item * max_jobs - obligatory integer, >= 1, defines how many workers to run at a time

=item * on_start - coderef (anonymous sub for example) that will be called, every time new worker is spawned. There will be one argument to the code, and it will be job descriptor. More information
about job descriptors in docs for L<add_command()> method.

=item * on_finish - coderef (anonymous sub for example) that will be called, every time worker finishes. There will be one argument to the code, and it will be job descriptor. More information
about job descriptors in docs for L<add_command()> method.

=back

If there are problems with arguments (max_jobs not given, or bad, or hooks given, but not CODE refs - exception will be raised using Carp::croak().

Arguments are passed as hash - both hash and hashref are accepted, so you can both:

  my $run = OmniPITR::Tools::ParallelSystem->new(
    'max_jobs' => 2,
    'on_finish' => sub { call_logging( shift ) },
  );

and

  my $run = OmniPITR::Tools::ParallelSystem->new(
    {
      'max_jobs' => 2,
      'on_finish' => sub { call_logging( shift ) },
    }
  );

=cut

sub new {
    my $class = shift;
    my $args  = ref( $_[ 0 ] ) ? $_[ 0 ] : { @ARG };
    my $self  = { 'commands' => [], };
    croak( 'max_jobs not provided' )   unless defined $args->{ 'max_jobs' };
    croak( 'max_jobs is not integer' ) unless $args->{ 'max_jobs' } =~ m{\A\d+\z};
    croak( 'max_jobs is not >= 1!' )   unless $args->{ 'max_jobs' } >= 1;
    $self->{ 'max_jobs' } = $args->{ 'max_jobs' };
    for my $hook ( qw( on_start on_finish ) ) {
        next unless defined $args->{ $hook };
        croak( "Hook for $hook provided, but is not a code?!" ) unless 'CODE' eq ref( $args->{ $hook } );
        $self->{ $hook } = $args->{ $hook };
    }
    return bless $self, $class;
}

=head2 results()

Returns arrayref with all job descriptors (check L<add_command()> method docs for details), after all the jobs have been ran.

=cut

sub results {
    return shift->{ 'results' };
}

=head2 add_command()

Adds new command to queue of things to be run.

Given argument (both hash and hashref are accepted) is treated as job descriptor.

To make the whole thing run, the only key needed is "command" - which should be arrayref of command and its arguments. The command cannot require any parsing - it will be passed directly to exec()
syscall.

There can be more keys in the job descriptor, and all of them will be stored, and passed back in on_start/on_finish hooks, and will be present in ->results() data.

But, there will be several keys added by OmniPITR::Tools::ParallelSystem itself:

=over

=item * Available in all 3 places: on_start and on_finish hooks, and final ->results():

=over

=item * started - exact time when the worker has started. Time is as epoch time with microsecond precision.

=item * pid - pid of worker process - it will be available in 

=item * stderr - name of temporary file that contains stderr output (in on_start hook), or stderr output from command (in other cases)

=item * stdout - name of temporary file that contains stdout output (in on_start hook), or stdout output from command (in other cases)

=back

=item * Additional information available in all 2 places: on_finish hooks and final ->results():

=over

=item * ended - exact time when the worker has ended - it will be available in on_finish hook, and in results. Time is as epoch time with microsecond precision.

=item * status - numerical status of worker exit. Rules for understanding the value are in perldoc perlvar - as "CHILD_ERROR" - a.k.a. $?

=back

=back

If application provides more keys to add_command, all of them will be preserverd, and passed back to app in hook calls, and in results output.

=cut

sub add_command {
    my $self = shift;
    my $args = ref( $_[ 0 ] ) ? $_[ 0 ] : { @ARG };
    croak( "No 'command' in given args to add_command?!" ) unless defined $args->{ 'command' };
    push @{ $self->{ 'commands' } }, $args;
    return;
}

=head2 add_commands()

Simple wrapper to simplify adding multiple commands.

Input should be array (or arrayref) of hashrefs, each hashref should be valid job descriptor, as described in L<add_command()> docs.

=cut

sub add_commands {
    my $self = shift;
    my $args = 'ARRAY' eq ref( $_[ 0 ] ) ? $_[ 0 ] : \@ARG;
    $self->add_command( $_ ) for @{ $args };
    return;
}

=head2 run()

Main loop responsible of running commands, and handling end of workers.

=cut

sub run {
    my $self = shift;

    $self->{ 'previous_chld_handler' } = $SIG{ 'CHLD' };

    $SIG{ 'CHLD' } = sub {
        local ( $OS_ERROR, $CHILD_ERROR );
        my $pid;
        while ( ( $pid = waitpid( -1, WNOHANG ) ) > 0 ) {
            $self->{ 'finished_workers' }->{ $pid } = {
                'ended'  => Time::HiRes::time(),
                'status' => $CHILD_ERROR,
            };
        }
    };

    $self->{ 'workers' }          = {};
    $self->{ 'finished_workers' } = {};

    while ( 1 ) {
        last if ( 0 == scalar keys %{ $self->{ 'workers' } } ) and ( 0 == scalar keys %{ $self->{ 'finished_workers' } } ) and ( 0 == scalar @{ $self->{ 'commands' } } );
        next if $self->start_new_worker();
        next if $self->handle_finished_workers();
        sleep 1;    # this will be cancelled by signal, so the sleep time doesn't matter much.
    }

    # The no warnings/use warnings "dance" is a workaround for stupid warnings in perl 5.8
    no warnings;
    $SIG{ 'CHLD' } = $self->{ 'previous_chld_handler' };
    use warnings;

    # The no warnings/use warnings "dance" is a workaround for stupid warnings in perl 5.8

    delete $self->{ 'previous_chld_handler' };
    return;
}

=head2 start_new_worker()

Internal method that does actual starting of new worker (if it can be started, and if there is actual work for it to do).

Calls on_start hook if needed.

=cut

sub start_new_worker {
    my $self = shift;
    return if scalar( keys %{ $self->{ 'workers' } } ) >= $self->{ 'max_jobs' };
    return if 0 == scalar @{ $self->{ 'commands' } };

    my $new_command = shift @{ $self->{ 'commands' } };

    my ( $stdout_fh, $stdout_filename ) = tempfile();
    my ( $stderr_fh, $stderr_filename ) = tempfile();

    $new_command->{ 'stdout' }  = $stdout_filename;
    $new_command->{ 'stderr' }  = $stderr_filename;
    $new_command->{ 'started' } = Time::HiRes::time();

    my $child_pid = fork();

    if ( $child_pid ) {

        # it's master
        $new_command->{ 'pid' } = $child_pid;
        $self->{ 'workers' }->{ $child_pid } = $new_command;
        close $stdout_fh;
        close $stderr_fh;
        if ( $self->{ 'on_start' } ) {
            $self->{ 'on_start' }->( $new_command );
        }
        return 1;
    }

    # worker

    open( STDOUT, '>&', $stdout_fh );
    open( STDERR, '>&', $stderr_fh );
    if ( $new_command->{'destination_type'} eq 'pipe' ) {
        open my $fh, '<', $new_command->{'local_file'} or die 'Cannot read from: ' . $new_command->{'local_file'} . ': ' . $OS_ERROR;
        open( STDIN, '<&', $fh );
    }
    exec( @{ $new_command->{ 'command' } } );
}

=head2 handle_finished_workers()

Internal method which does necessary work when worker finishes. Reads stdout/stderr files, unlinks temp files, calls on_finish hook.

=cut

sub handle_finished_workers {
    my $self = shift;
    return if 0 == scalar keys %{ $self->{ 'finished_workers' } };

    my @pids = keys %{ $self->{ 'finished_workers' } };

    for my $pid ( @pids ) {
        my $data = delete $self->{ 'finished_workers' }->{ $pid };

        # sanity check - this shouldn't ever happen.
        next unless $self->{ 'workers' }->{ $pid };

        my $full_data = delete $self->{ 'workers' }->{ $pid };
        $full_data->{ 'ended' }  = $data->{ 'ended' };
        $full_data->{ 'status' } = $data->{ 'status' };

        for my $file_type ( qw( stdout stderr ) ) {
            my $filename = $full_data->{ $file_type };
            if ( open my $fh, '<', $filename ) {
                local $/;
                $full_data->{ $file_type } = <$fh>;
                close $fh;
                unlink $filename;
            }
        }
        push @{ $self->{ 'results' } }, $full_data;

        if ( $self->{ 'on_finish' } ) {
            $self->{ 'on_finish' }->( $full_data );
        }
    }
    return 1;
}

1;
