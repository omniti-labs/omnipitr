package OmniPITR::Program::Monitor::Check::Last_Restore_Age;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';
use base qw( OmniPITR::Program::Monitor::Check );

use Time::HiRes qw( time );

sub run_check {
    my $self  = shift;
    my $state = shift;

    my $last_restore = undef;

    my $S = $state->{ 'Restore' };
    for my $T ( values %{ $S } ) {
        for my $X ( values %{ $T } ) {
            next unless defined $X->[ 1 ];
            if (   ( !defined $last_restore )
                || ( $last_restore < $X->[ 1 ] ) )
            {
                $last_restore = $X->[ 1 ];
            }
        }
    }
    if ( defined $last_restore ) {
        printf '%f%s', time() - $last_restore, "\n";
    } else {
        print "0\n";
    }
    return;
}

1;
