package OmniPITR::Log;
use strict;
use warnings;
use English qw( -no_match_vars );
use Carp;
use File::Basename;
use File::Path;
use POSIX qw(strftime floor);
use IO::File;

BEGIN {
    eval { use Time::HiRes qw( time ); };
}

sub new {
    my $class                 = shift;
    my ( $filename_template ) = @_;
    croak( 'Logfile name template was not provided!' ) unless $filename_template;

    my $self                  = bless {}, $class;

    $self->{ 'template' }       = $filename_template;
    $self->{ 'program' }        = basename( $PROGRAM_NAME );
    $self->{ 'current_log_ts' } = 0;
    $self->{ 'current_log_fn' } = '';

    return $self;
}

sub _log {
    my $self = shift;
    my ( $level, $format, @args ) = @_;

    my $log_line_prefix = $self->get_log_line_prefix();
    my $fh              = $self->get_log_fh();

    my $message = sprintf $format, @args;
    $message =~ s/\s*\z//;

    for my $line ( split /\r?\n/, $message ) {
        printf $fh '%s : %s : %s%s', $log_line_prefix, $level, $line, "\n";
    }

    $fh->flush();
    $fh->sync();

    return;
}

sub log {
    my $self = shift;
    return $self->_log( 'LOG', @_ );
}

sub error {
    my $self = shift;
    return $self->_log( 'ERROR', @_ );
}

sub fatal {
    my $self = shift;
    $self->_log( 'FATAL', @_ );
    exit(1);
}

sub get_log_line_prefix {
    my $self         = shift;
    my $time         = time();
    my $date_time    = strftime( '%Y-%m-%d %H:%M:%S', localtime $time );
    my $microseconds = ( $time * 1_000_000 ) % 1_000_000;
    my $time_zone    = strftime( '%z', localtime $time );

    my $time_stamp = sprintf "%s.%06u %s", $date_time, $microseconds, $time_zone;
    return sprintf "%s : %u : %s", $time_stamp, $PROCESS_ID, $self->{ 'program' };
}

sub get_log_fh {
    my $self = shift;

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
