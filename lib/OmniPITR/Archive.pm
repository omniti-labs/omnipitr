package OmniPITR::Archive;
use strict;
use warnings;
use base qw( OmniPITR::Program );
use Carp;
use File::Spec;
use Getopt::Long;

sub run {
    my $self = shift;
    print "YAY, it worked!\n";
    print Dumper($self);
}

sub read_args {
    my $self = shift;

    my %args = (
        'data-dir' => '.',
    );
    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-archive.pod' )
        unless GetOptions(
        \%args,
        'data-dir|D=s',
        'dst-local|dl=s@',
        'dst-remote|dr=s@',
        'temp-dir|t=s',
        'log|l=s',
        'state-dir|s=s',
        'pid-file=s',
        'verbose|v'
        );
    croak( '--log was not provided - cannot continue.' ) unless $args{ 'log' };

    for my $key ( qw( data-dir temp-dir state-dir pid-file verbose ) ) {
        $self->{ $key } = $args{ $key };
    }

    for my $type ( qw( local remote ) ) {
        my $D = [];
        $self->{ 'destination' }->{ $type } = $D;

        next unless defined $args{ 'dst-' . $type };

        my %temp_for_uniq = ();
        my @items = grep { !$temp_for_uniq{ $_ }++ } @{ $args{ 'dst-' . $type } };

        for my $item ( @items ) {
            my $current = { 'compression' => 'none', };
            if ( $item =~ s/\A(gzip|bzip2|lzma)\%// ) {
                $current->{ 'compression' } = $1;
            }
            $current->{ 'path' } = $item;
            push @{ $D }, $current;
        }
    }

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log' }          = OmniPITR::Log->new( $args{ 'log' } );
    $self->{ 'log_template' } = OmniPITR::Log->new( $args{ 'log_template' } );

    return;
}

sub validate_args {
    my $self = shift;

    $self->log->fatal( "Given data-dir (%s) is not valid", $self->{ 'data-dir' } ) unless -d $self->{ 'data-dir' } && -f File::Spec->catfile( $self->{ 'data-dir' }, 'PG_VERSION' );

    my $dst_count = scalar( @{ $self->{ 'destination' }->{ 'local' } } ) + scalar( @{ $self->{ 'destination' }->{ 'remote' } } );
    $self->log->fatal( "No --dst-* has been provided!" ) if 0 == $dst_count;

    if ( 1 < $dst_count ) {
        $self->log->fatal( "More than 1 --dst-* has been provided, but no --state-dir!" ) if !$self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) does not exist",     $self->{ 'state-dir' } ) unless -e $self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) is not a directory", $self->{ 'state-dir' } ) unless -d $self->{ 'state-dir' };
        $self->log->fatal( "Given --state-dir (%s) is not writable",    $self->{ 'state-dir' } ) unless -w $self->{ 'state-dir' };
    }

    return;
}

1;
