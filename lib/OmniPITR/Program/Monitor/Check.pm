package OmniPITR::Program::Monitor::Check;
use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '1.2.0';

=head1 NAME

OmniPITR::Program::Monitor::Check - base for omnipitr-monitor checks

=head1 SYNOPSIS

    package OmniPITR::Program::Monitor::Check::Whatever;
    use base qw( OmniPITR::Program::Monitor::Check );
    sub setup { ... }
    sub get_args { ... }
    sub run_check { ... }

=head1 DESCRIPTION

This is base class that we expect all check classes inherit from.

While not technically requirement, it might make writing check classes simpler.

=head1 CONTROL FLOW

When omnipitr-monitor creates check object, it doesn't pass any arguments (yet).

Afterwards, it calls ->setup() function, passing (as hash):

=over

=item * state-dir - directory where check can store it's own data, in subdirectory named like last element of check package name

=item * log - log object

=item * psql - coderef which will run given query via psql, and return whole output as scalar

=back

Afterwards, omnipitr-monitor will run "get_args" method (if it's defined), to get all necessary options from command line - options specifically for this check.

Finally run_check() method will be called, with one argument - being full copy of omnipitr-monitor internal state.

=head1 METHODS

=head2 new()

Object constructor. No logic in here. Just makes simple hashref based object.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}

=head2 setup()

Sets check for work - receives state-dir and log object from omnipitr-monitor.

=cut

sub setup {
    my $self = shift;
    my %args = @_;
    for my $key ( qw( log state-dir psql ) ) {
        croak( "$key not given in call to ->setup()." ) unless defined $args{ $key };
        $self->{ $key } = $args{ $key };
    }
    return;
}

=head2 get_args()

This method should be overriden in check class if the check has some options get from command line.

=cut

sub get_args {
    my $self = shift;
    return;
}

=head1 log()

Shortcut to make code a bit nicer.

Returns logger object.

=cut

sub log { return shift->{ 'log' }; }

=head1 psql()

Runs given query via psql.

=cut

sub psql {
    my $self = shift;
    return $self->{ 'psql' }->( @_ );
}

1;
