package OmniPITR::Archive;
use strict;
use warnings;
use Carp;
use OmniPITR::Log;
use Getopt::Long;
use Data::Dumper;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->read_args();
    $self->validate_args();
    return $self;
}

sub run {
    my $self = shift;
    print "YAY, it worked!\n";
}

sub read_args {
    my $self = shift;

    my %args = ();
    croak('Error while reading command line arguments. Please check documentation in doc/omnipitr-archive.pod')
        unless GetOptions(
            \%args,
            'data-dir|D=s',
            'dst-local|dl=s@',
            'dst-remote|dr=s@',
            'temp-dir|t=s',
            'log|l=s',
            'state-dir|s=s',
            'pid-file=s',
            'verbose|v',
            'help|?',
        );
    croak('--log was not provided - cannot continue.') unless $args{'log'};

    # We do it here so it will actually work for reporing problems in validation
    $self->{'log'} = OmniPITR::Log->new( $args{'log'} );
    
}

1;

