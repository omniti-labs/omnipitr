package OmniPITR::Program::Monitor::Check::Last_Archive_Age;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.3.0';
use base qw( OmniPITR::Program::Monitor::Check );

use Time::HiRes qw( time );

sub run_check {
    my $self  = shift;
    my $state = shift;

    my $last_archive = undef;

    my $S = $state->{ 'Archive' };
    for my $T ( values %{ $S } ) {
        for my $X ( values %{ $T } ) {
            next unless defined $X->[ 1 ];
            if (   ( !defined $last_archive )
                || ( $last_archive < $X->[ 1 ] ) )
            {
                $last_archive = $X->[ 1 ];
            }
        }
    }

    if ( defined $last_archive ) {
        printf '%f%s', time() - $last_archive, "\n";
    }
    else {
        print "0\n";
    }
    return;
}

1;
