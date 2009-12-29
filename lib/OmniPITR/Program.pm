package OmniPITR::Program;
use strict;
use warnings;
use OmniPITR::Log;
use English qw( -no_match_vars );
use Carp;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->read_args();
    $self->validate_args();
    $self->check_pidfile();
    return $self;
}

sub run {
    my $self = shift;
    croak("run() method in OmniPITR::Program was not overridden!");
}

sub check_pidfile {
    my $self = shift;
    return unless defined $self->{'pid-file'};

    my $pidfile = $self->{'pid-file'};

    if ( -e $pidfile ) {
        open( my $fh, '<', $pidfile ) or $self->log->fatal( 'Pidfile (%s) exists, but cannot be opened: %s', $pidfile, $OS_ERROR );
        my $old_pid = <$fh>;
        close $fh;
        $self->log->fatal('Pidfile (%s) exists, but contains unexpected value: {{%s}}', $pidfile, $old_pid) unless $old_pid =~ s/\A(\d{1,5})\s*\z/$1/;
        $self->log->fatal('Previous copy of %s seems to be still running. PID: %d', $PROGRAM_NAME, $old_pid) if kill(0, $old_pid );
    }
    open my $fh, '>', $pidfile or $self->log->fatal( 'Cannot open pidfile (%s) for writing: %s', $pidfile, $OS_ERROR);
    print $fh $PROCESS_ID;
    close $fh;
    return;
}

# Shortcuts
sub verbose { return shift->{'verbose'}; }
sub log { return shift->{'log'}; }

1;


