package OmniPITR::Program::Monitor::Parser::Archive;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.2.0';
use base qw( OmniPITR::Program::Monitor::Parser );

=head1 Parser/Archvie state data structure

Within state->{'Archive'} data is kept using following structure:

    state->{'Archive'}->{ Timeline }->{ Offset } = [ DATA ]

Where

=over

=item * Timeline - leading-zero-trimmed timeline of wal segment

=item * Offset - offset of wal segment

=item * DATA - data about archiving this segment

=back

For example, data about segment 0000000100008930000000E0 will be in

    state->{Archive}->{1}->{8930E0}

and for 000000010000012300000005 in

    state->{Archive}->{1}->{12305}

Please note additional 0 before 5 in last example - it's due to fact that we keep always 2 last characters from wal segment name.

DATA is arrayref which contains:

=over

=item * [0] - epoch of when omnipitr-archive was called, for the first time, for given wal segment

=item * [1] - epoch of when omnipitr-archive last time delivered the segment

=back

=cut

sub handle_line {
    my $self = shift;
    my $D    = shift;
    my $S    = $self->state();

    if ( $D->{ 'line' } =~ m{\ALOG : Called with parameters: .* pg_xlog/([a-f0-9]{24})\z}i ) {
        my ( $timeline, $xlog_offset ) = $self->split_xlog_filename( $1 );
        $S->{ $timeline }->{ $xlog_offset }->[ 0 ] ||= $D->{ 'epoch' };
        return;
    }

    if ( $D->{ 'line' } =~ m{\ALOG : Segment .*/([a-f0-9]{24}) successfully sent to all destinations\.\z}i ) {
        my ( $timeline, $xlog_offset ) = $self->split_xlog_filename( $1 );
        $S->{ $timeline }->{ $xlog_offset }->[ 1 ] = $D->{ 'epoch' };
        return;
    }

    return;
}

sub clean_state {
    my $self = shift;
    my $S    = $self->state();

    my $cutoff = time() - 7 * 24 * 60 * 60;    # week ago.

    my @timelines = keys %{ $S };

    for my $t ( @timelines ) {
        my @offsets = keys %{ $S->{ $t } };
        for my $o ( @offsets ) {
            next unless $S->{ $t }->{ $o }->[ 1 ];
            next if $S->{ $t }->{ $o }->[ 1 ] >= $cutoff;
            delete $S->{ $t }->{ $o };
        }
        delete $S->{ $t } if 0 == scalar keys %{ $S->{ $t } };
    }

    return;
}

1;
