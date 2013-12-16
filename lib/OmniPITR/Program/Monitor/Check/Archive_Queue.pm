package OmniPITR::Program::Monitor::Check::Archive_Queue;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.3.0';
use base qw( OmniPITR::Program::Monitor::Check );

use Data::Dumper;

sub run_check {
    my $self  = shift;
    my $state = shift;

    my $x = $self->psql( 'select pg_xlogfile_name(pg_current_xlog_location())' );
    $x =~ s/\s*\z//;
    my ( $timeline, $current_xlog ) = $self->split_xlog_filename( $x );

    $current_xlog--;    # Decrease because if we are currently in xlog 12, then the last archived can be at most 11.

    my $last_archive       = undef;
    my $last_archived_xlog = undef;

    my $S = $state->{ 'Archive' };
    while ( my ( $xlog, $X ) = each %{ $S->{ $timeline } } ) {
        next unless defined $X->[ 1 ];
        if (   ( !defined $last_archive )
            || ( $last_archive < $X->[ 1 ] ) )
        {
            $last_archive       = $X->[ 1 ];
            $last_archived_xlog = $xlog;
        }
    }
    $last_archived_xlog =~ s/(..)\z//;
    my $lower = hex( $1 );
    my $upper = hex( $last_archived_xlog );

    print $current_xlog - ( 255 * $upper + $lower ), "\n";

    return;
}

=head1 split_xlog_filename()

Splits given xlog filename (24 hex characters) into a pair of timeline and xlog offset.

Timeline is trimmed of leading 0s, and xlog offset to converted to decimal.

=cut

sub split_xlog_filename {
    my $self      = shift;
    my $xlog_name = shift;

    my ( $timeline, @elements ) = unpack( '(A8)3', $xlog_name );
    $timeline =~ s/^0*//;

    $elements[ 0 ] =~ s/^0*//;
    $elements[ 1 ] =~ s/^0{6}//;
    my $upper = hex( $elements[ 0 ] );
    my $lower = hex( $elements[ 1 ] );
    return ( $timeline, $upper * 255 + $lower );
}

1;
