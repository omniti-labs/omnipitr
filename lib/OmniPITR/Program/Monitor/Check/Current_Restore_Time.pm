package OmniPITR::Program::Monitor::Check::Current_Restore_Time;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.0.0';
use base qw( OmniPITR::Program::Monitor::Check );

use Time::HiRes qw( time );

sub run_check {
    my $self  = shift;
    my $state = shift;

    my $S = $state->{ 'Restore' };
    for my $T ( values %{ $S } ) {
        for my $X ( values %{ $T } ) {
            next if defined $X->[ 1 ];
            printf '%f%s', time() - $X->[ 0 ], "\n";
            return;
        }
    }

    print "0\n";
    return;
}

1;
