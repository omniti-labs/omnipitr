package OmniPITR::Program::Monitor::Parser::Backup;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '2.0.0';
use base qw( OmniPITR::Program::Monitor::Parser );

=head1 Backup_Slave/Backup_Master *base* state data structure

Logs parsing for Backup Slave and Backup Master are virtually the same, so the logic has been moved to shared parent - Backup.pm.

But the data is stored separately in state->{'Backup_Slave'} and state->{'Backup_Master'} depending on where it came from.

In all examples below, I write state->{'Backup*'}, and it relates to both places in state.

Within state->{'Backup*'} data is kept using following structure:

    state->{'Backup*'}->[ n ] = { DATA }

Where

=over

=item * n is just a number, irrelevant. The only important fact is that higher n means that backup started later.

=item * DATA - data about archiving this segment

=back

DATA is arrayref which contains:

=over

=item * [0] - epoch of when omnipitr-backup was called

=item * [1] - process number for omnipitr-backup-* program (pid)

=item * [2] - epoch when backup was fully done.

=back

=cut

sub handle_line {
    my $self = shift;
    my $D    = shift;
    my $S    = $self->state();

    if ( $D->{ 'line' } =~ m{\ALOG : Called with parameters: } ) {
        push @{ $S }, [ $D->{ 'epoch' }, $D->{ 'pid' } ];
        return;
    }

    if ( $D->{ 'line' } =~ m{\ALOG : All done\.\s*\z} ) {
        for my $backup ( @{ $S } ) {
            next if $D->{ 'pid' } != $backup->[ 1 ];
            next if defined $backup->[ 2 ];
            $backup->[ 2 ] = $D->{ 'epoch' };
        }
        return;
    }

    return;
}

sub empty_state {
    return [];
}

1;
