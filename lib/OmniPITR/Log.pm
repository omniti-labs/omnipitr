package OmniPITR::Log;
use strict;
use warnings;
use English qw( -no_match_vars );
use Carp;
use File::Basename;
use File::Path;
use Data::Dumper;
use POSIX qw(strftime floor);
use IO::File;

our $VERSION = '1.2.0';

BEGIN {
    eval { use Time::HiRes qw( time ); };
}

=head1 new()

Constructor for logger class.

Takes one argument: template (using %*, strftime variables).

This argument can also be reference to File Handle to force log output to given stream. This can be used for example like:

    my $logger = OmniPITR::Log->new( \*STDOUT );

=cut

sub new {
    my $class = shift;
    my ( $filename_template ) = @_;
    croak( 'Logfile name template was not provided!' ) unless $filename_template;

    my $self = bless {}, $class;

    if ( ref $filename_template ) {

        # It's forced filehandle
        $self->{ 'forced_fh' } = $filename_template;
    }
    else {
        $self->{ 'template' } = $filename_template;
    }
    $self->{ 'program' }        = basename( $PROGRAM_NAME );
    $self->{ 'current_log_ts' } = 0;
    $self->{ 'current_log_fn' } = '';

    return $self;
}

=head1 _log()

Internal function, shouldn't be called from client code.

Gets loglevel (assumed to be string), format, and list of values to
fill-in in the format, using standard sprintf semantics.

Each line (even in multiline log messages) is prefixed with
metainformation (timestamp, pid, program name).

In case reference is passed as one of args - it is dumped using
Data::Dumper.

Thanks to this this:

    $object->_log('loglevel', '%s', $object);

Will print dump of $object and not stuff like 'HASH(0xdeadbeef)'.

For client-code open methods check L<log()>, L<error()> and L<fatal()>.

=cut

sub _log {
    my $self = shift;
    my ( $level, $format, @args ) = @_;

    my $log_line_prefix = $self->_get_log_line_prefix();
    my $fh              = $self->_get_log_fh();

    @args = map { ref $_ ? Dumper( $_ ) : $_ } @args;

    my $message = sprintf $format, @args;
    $message =~ s/\s*\z//;

    for my $line ( split /\r?\n/, $message ) {
        printf $fh '%s : %s : %s%s', $log_line_prefix, $level, $line, "\n";
    }

    $fh->flush();
    $fh->sync();

    return;
}

=head1 log()

Client code facing method, which calls internal L<_log()> method, giving
'LOG' as loglevel, and passing rest of arguments without modifications.

Example:

    $logger->log( 'i = %u', $i );

=cut

sub log {
    my $self = shift;
    return $self->_log( 'LOG', @_ );
}

=head1 error()

Client code facing method, which calls internal L<_log()> method, giving
'ERROR' as loglevel, and passing rest of arguments without
modifications.

Example:

    $logger->error( 'File creation failed: %s', $OS_ERROR );

=cut

sub error {
    my $self = shift;
    return $self->_log( 'ERROR', @_ );
}

=head1 fatal()

Client code facing method, which calls internal L<_log()> method, giving
'FATAL' as loglevel, and passing rest of arguments without
modifications.

Additionally, after logging the message, it exits main program, setting
error status 1.

Example:

    $logger->fatal( 'Called from user with uid = %u, and not 0!', $user_uid );

=cut

sub fatal {
    my $self = shift;
    $self->_log( 'FATAL', @_ );
    exit( 1 );
}

=head1 time_start()

Starts timer.

Should be used together with time_finish, for example like this:

    $logger->time_start( 'zipping' );
    $zip->run();
    $logger->time_finish( 'zipping' );

Arguments to time_start and time_finish should be the same to allow
matching of events.

=cut

sub time_start {
    my $self    = shift;
    my $comment = shift;
    $self->{ 'timers' }->{ $comment } = time();
    return;
}

=head1 time_finish()

Finished calculation of time for given block of code.

Calling:

    $logger->time_finish( 'Compressing with gzip' );

Will log line like this:

    2010-04-09 00:08:35.148118 +0200 : 19713 : omnipitr-archive : LOG : Timer [Compressing with gzip] took: 0.351s

Assuming related time_start() was called 0.351s earlier.

=cut

sub time_finish {
    my $self    = shift;
    my $comment = shift;
    my $start   = delete $self->{ 'timers' }->{ $comment };
    $self->log( 'Timer [%s] took: %.3fs', $comment, time() - ( $start || 0 ) );
    return;
}

=head1 _get_log_line_prefix()

Internal method generating line prefix, which is prepended to every
logged line of text.

Prefix contains ( " : " separated ):

=over

=item * Timestamp, with microsecond precision

=item * Process ID (PID) of logging program

=item * Name of program that logged the message

=back

=cut

sub _get_log_line_prefix {
    my $self         = shift;
    my $time         = time();
    my $date_time    = strftime( '%Y-%m-%d %H:%M:%S', localtime $time );
    my $microseconds = ( $time * 1_000_000 ) % 1_000_000;
    my $time_zone    = strftime( '%z', localtime $time );

    my $time_stamp = sprintf "%s.%06u %s", $date_time, $microseconds, $time_zone;
    return sprintf "%s : %u : %s", $time_stamp, $PROCESS_ID, $self->{ 'program' };
}

=head1 _get_log_fh()

Internal method handling logic to close and open logfiles when
necessary, based of given logfile template, current time, and when
previous logline was logged.

At any given moment only 1 filehandle will be opened, and it will be
closed, and reopened, when time changes in such way that it would
require another filename.

=cut

sub _get_log_fh {
    my $self = shift;

    return $self->{ 'forced_fh' } if $self->{ 'forced_fh' };

    my $time = floor( time() );
    return $self->{ 'log_fh' } if $self->{ 'current_log_ts' } == $time;

    $self->{ 'current_log_ts' } = $time;
    my $filename = strftime( $self->{ 'template' }, localtime $time );
    return $self->{ 'log_fh' } if $self->{ 'current_log_fn' } eq $filename;

    $self->{ 'current_log_fn' } = $filename;
    close delete $self->{ 'log_fh' } if exists $self->{ 'log_fh' };

    my $dirname = dirname $filename;
    mkpath( $dirname ) unless -e $dirname;
    open my $fh, '>>', $filename or croak( "Cannot open $filename for writing: $OS_ERROR" );

    $self->{ 'log_fh' } = $fh;
    return $fh;
}

1;
