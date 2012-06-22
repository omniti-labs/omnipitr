package OmniPITR::Program::Monitor::Check::Last_Archive_Age;

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

    printf '%f%s', time() - $last_archive, "\n";
    return;
}

1;
