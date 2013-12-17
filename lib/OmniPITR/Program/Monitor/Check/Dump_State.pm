package OmniPITR::Program::Monitor::Check::Dump_State;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.3.1';
use base qw( OmniPITR::Program::Monitor::Check );

use Data::Dumper;

sub run_check {
    my $self  = shift;
    my $state = shift;
    my $d     = Data::Dumper->new( [ $state ], [ 'state' ] );
    $d->Sortkeys( 1 );
    $d->Indent( 1 );
    $d->{ 'xpad' } = '    ';
    print $d->Dump();
    return;
}

1;
