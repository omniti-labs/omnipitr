package OmniPITR::Program::Monitor::Parser::Archive;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';
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

1;
