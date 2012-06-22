package OmniPITR::Program::Monitor::Check::Dump_State;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';
use base qw( OmniPITR::Program::Monitor::Check );

sub run_check {
    my $self  = shift;
    my $state = shift;
    $self->log->log( 'State = %s', $state );
    return;
}

1;
