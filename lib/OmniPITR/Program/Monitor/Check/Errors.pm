package OmniPITR::Program::Monitor::Check::Errors;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.3.3';
use base qw( OmniPITR::Program::Monitor::Check );

use Getopt::Long qw( :config no_ignore_case );
use Time::Local;
use Data::Dumper;
use File::Spec;

sub run_check {
    my $self  = shift;
    my $state = shift;
    $self->load_from_timestamp() if $self->{ 'state-based-from' };
    my @all_errors;
    for my $type ( qw( ERROR FATAL ) ) {
        next unless $state->{ 'errors' }->{ $type };
        push @all_errors, @{ $state->{ 'errors' }->{ $type } };
    }
    return if 0 == scalar @all_errors;
    my @sorted_to_print = sort { $a->{ 'epoch' } <=> $b->{ 'epoch' } } grep { $_->{ 'epoch' } > $self->{ 'from' } } @all_errors;
    return if 0 == scalar @sorted_to_print;
    for my $line ( @sorted_to_print ) {
        printf '%s : %s : %s%s', @{ $line }{ qw( timestamp pid line ) }, "\n";
    }
    my $final_ts = $sorted_to_print[ -1 ]->{ 'epoch' };
    $self->save_from_timestamp( $final_ts ) if $self->{ 'state-based-from' };
    return;
}

sub get_args {
    my $self = shift;
    my $from = undef;
    GetOptions( 'from=s' => \$from );
    if ( !defined $from ) {
        $self->{ 'state-based-from' } = 1;
    }
    elsif ( my @elements = $from =~ m{\A(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\z} ) {
        $elements[ 1 ]--;    # Time::Local expects months in range 0-11, and not 1-12.
        my $epoch;
        eval { $epoch = timelocal( reverse @elements ); };
        $self->log->fatal( 'Given date (%s) is not valid', $from ) if $EVAL_ERROR;
        $self->{ 'from' } = $epoch;
    }
    elsif ( $from =~ m{\A\d+\z} ) {
        $self->{ 'from' } = time() - $from;
    }
    else {
        $self->log->fatal( 'Bad format of given --from, should be YYYY-MM-DD HH:MI:SS, or just an integer' );
    }
    return;
}

sub load_from_timestamp {
    my $self = shift;
    if ( open my $fh, '<', File::Spec->catfile( $self->{ 'state-dir' }, 'from-timestamp' ) ) {
        my $timestamp = <$fh>;
        close $fh;
        chomp $timestamp;
        $self->{ 'from' } = $timestamp if $timestamp =~ m{\A\d+(?:\.\d+)?\z};
    }
    $self->{ 'from' } ||= 0;
    return;
}

sub save_from_timestamp {
    my $self     = shift;
    my $final_ts = shift;
    if ( open my $fh, '>', File::Spec->catfile( $self->{ 'state-dir' }, 'from-timestamp' ) ) {
        printf $fh '%.6f', $final_ts;
        close $fh;
    }
    return;
}

1;
