package OmniPITR::Program::Monitor::Parser;
use strict;
use warnings;
use Carp;
use Data::Dumper;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';

=head1 NAME

OmniPITR::Program::Monitor::Parser - base for omnipitr-monitor parsers

=head1 SYNOPSIS

    package OmniPITR::Program::Monitor::Parser::Whatever;
    use base qw( OmniPITR::Program::Monitor::Parser );
    sub handle_line { ... }

=head1 DESCRIPTION

This is base class for parsers of lines from particular omnipitr programs.

=head1 CONTROL FLOW

When omnipitr-monitor creates parser object, it doesn't pass any arguments (yet).

Afterwards, it calls ->setup() function, passing (as hash):

=over

=item * state - hashref with current state - all modifications will be stored by omnipitr-monitor

=item * log - log object

=back

For each line from given program, ->handle_line() method will be called, with single argument, being hashref with keys:

=over

=item * timestamp - timestamp, as it was written in the log line

=item * epoch - same timestamp, but converted to epoch format

=item * line - data logged by actual program, with all prefixes removed

=item * pid - process id of the process that logged given line

=back

=head1 METHODS

=head2 new()

Object constructor. No logic in here. Just makes simple hashref based object.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $class =~ s/.*:://;
    $self->{ 'class' } = $class;
    return $self;
}

=head2 setup()

Sets check for work - receives state-dir and log object from omnipitr-monitor.

=cut

sub setup {
    my $self = shift;
    my %args = @_;
    for my $key ( qw( log state ) ) {
        croak( "$key not given in call to ->setup()." ) unless defined $args{ $key };
        $self->{ $key } = $args{ $key };
    }
    $self->{ 'state' }->{ 'parsed' }->{ $self->{ 'class' } } = {} unless defined $self->{ 'state' }->{ 'parsed' }->{ $self->{ 'class' } };
    return;
}

=head1 log()

Shortcut to make code a bit nicer.

Returns logger object.

=cut

sub log { return shift->{ 'log' }; }

=head1 state()

Helper function, accessor, to state hash. Or, to be exact, to subhash in state that relates to current parser.

Has 1, or two arguments. In case of one argument - returns value, from state, for given key.

If it has two arguments, then - if 2nd argument is undef - it removes the key from state, and returns.

If the 2nd argument is defined, it sets value for given key to given value, and returns it.

=cut

sub state {
    my $self = shift;
    my $S    = $self->{ 'state' }->{ $self->{ 'class' } };

    my $key = shift;
    if ( 0 == scalar @_ ) {
        return $S->{ $key };
    }
    my $value = shift;
    if ( defined $value ) {
        return $S->{ $key } = $value;
    }
    delete $S->{ $key };
    return;
}

1;
