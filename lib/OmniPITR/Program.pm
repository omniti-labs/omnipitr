package OmniPITR::Program;
use strict;
use warnings;
use OmniPITR::Log;
use English qw( -no_match_vars );
use Proc::Pidfile;
use Carp;

=head1 new()

Object contstructor.

Since all OmniPITR programs are based on object, and they start with
doing the same things (namely reading and validating command line
arguments) - this is wrapped in here, to avoid code duplication.

Constructor also handles pid file creation, in case it was requested.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->read_args();
    $self->validate_args();
    $self->{ 'pid-file' } = Proc::Pidfile->new( 'pidfile' => $self->{ 'pid-file' } ) if $self->{ 'pid-file' };

    return $self;
}

=head1 run()

Just a stub method, that has to be overriden in subclasses.

=cut

sub run {
    my $self = shift;
    croak( "run() method in OmniPITR::Program was not overridden!" );
}

=head1 verbose()

Shortcut to make code a bit nicer.

Returns values of (command line given) verbose switch.

=cut

sub verbose { return shift->{ 'verbose' }; }

=head1 log()

Shortcut to make code a bit nicer.

Returns logger object.

=cut

sub log { return shift->{ 'log' }; }

1;

