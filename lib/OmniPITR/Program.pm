package OmniPITR::Program;
use strict;
use warnings;
use OmniPITR::Log;
use English qw( -no_match_vars );
use OmniPITR::Pidfile;
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
    $self->check_debug();
    $self->read_args();
    $self->validate_args();
    $self->{ 'pid-file' } = OmniPITR::Pidfile->new( 'pidfile' => $self->{ 'pid-file' } ) if $self->{ 'pid-file' };

    return $self;
}

=head1 check_debug()

Internal method providing --debug option handling to every omnipitr program.

If *first* argument to omnipitr program it will print to stderr all arguments, and environment variables.

=cut

sub check_debug {
    my $self = shift;
    return unless '--debug' eq $ARGV[ 0 ];

    warn "DEBUG INFORMATION:\n";
    for my $key ( sort keys %ENV ) {
        warn sprintf( "ENV: '%s' => '%s'\n", $key, $ENV{ $key } );
    }
    warn "Command line arguments: [" . join( "] , [", @ARGV ) . "]\n";
    shift @ARGV;

    return;
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

