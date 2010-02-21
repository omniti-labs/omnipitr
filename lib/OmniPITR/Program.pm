package OmniPITR::Program;
use strict;
use warnings;
use OmniPITR::Log;
use English qw( -no_match_vars );
use Proc::Pidfile;
use Carp;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->read_args();
    $self->validate_args();
    $self->{'pid-file'} = Proc::Pidfile->new( 'pidfile' => $self->{ 'pid-file' } ) if $self->{'pid-file'};

    return $self;
}

sub run {
    my $self = shift;
    croak("run() method in OmniPITR::Program was not overridden!");
}

# Shortcuts
sub verbose { return shift->{'verbose'}; }
sub log { return shift->{'log'}; }

1;


