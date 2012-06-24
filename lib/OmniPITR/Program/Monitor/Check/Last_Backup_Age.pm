package OmniPITR::Program::Monitor::Check::Last_Backup_Age;

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

    my $last_backup = undef;

    for my $backup_type ( qw( Backup_Master Backup_Slave ) ) {
        next unless $state->{ $backup_type };
        my $S = $state->{ $backup_type };
        for my $backup ( reverse @{ $S } ) {
            next unless $backup->[ 2 ];
            if ( ( !defined $last_backup ) || ( $last_backup < $backup->[ 2 ] ) ) {
                $last_backup = $backup->[ 2 ];
            }
            last;
        }
    }

    if ( defined $last_backup ) {
        printf '%f%s', time() - $last_backup, "\n";
    }
    else {
        print "0\n";
    }
    return;
}

1;
