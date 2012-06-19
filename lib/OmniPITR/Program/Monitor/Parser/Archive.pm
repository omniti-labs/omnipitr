package OmniPITR::Program::Monitor::Parser::Archive;

use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';
use base qw( OmniPITR::Program::Monitor::Parser );

sub handle_line {
    my $self = shift;
    my $D = shift;

    if ( $D->{'line'} =~ m{\ALOG : Called with parameters: .* pg_xlog/([a-f0-9]{24})\z}i ) {
        my $xlog_name = $1;
        push @{ $self->{'state'}->{'parsed'}->{'Archive'}->{'xlog_archive_called'}->{$xlog_name} }, { 'epoch' => $D->{'epoch'}, 'timestamp' => $D->{'timestamp'} };
    }

    $self->log->log('state = %s', $self->{'state'});

    exit;
}

1;
