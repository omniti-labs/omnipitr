package OmniPITR::Program::Checksum;
use strict;
use warnings;

our $VERSION = '2.0.0';
use base qw( OmniPITR::Program );

use Carp;
use File::Spec;
use Digest;
use File::Basename;
use English qw( -no_match_vars );
use Getopt::Long qw( :config no_ignore_case );

=head1 run()

Main function, called by actual script in bin/, wraps all work done by script with the sole exception of reading and validating command line arguments.

These tasks (reading and validating arguments) are in this module, but they are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need further information - simply check doc for the method you have questions about.

=cut

sub run {
    my $self = shift;
    $self->{ 'digest' }->addfile( \*STDIN );
    printf "%s *%s\n", $self->{ 'digest' }->hexdigest, $self->{ 'filename' };
    return;
}

=head1 read_args

Function which does all the parsing of command line argument.

=cut

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my $config = {};

    my $status = GetOptions( $config, qw( digest|d=s filename|f=s help|h|? version|V list|l ) );
    if ( !$status ) {
        $self->show_help_and_die();
    }

    $self->show_help_and_die()      if $config->{ 'help' };
    $self->show_available_digests() if $config->{ 'list' };

    if ( $config->{ 'version' } ) {

        # The $self->VERSION below returns value of $VERSION variable in class of $self.
        printf '%s ver. %s%s', basename( $PROGRAM_NAME ), $self->VERSION, "\n";
        exit;
    }

    $self->{ 'digest' }   = $config->{ 'digest' }   || 'MD5';
    $self->{ 'filename' } = $config->{ 'filename' } || '<stdin>';

    # Restore original @ARGV
    @ARGV = @argv_copy;

}

sub show_available_digests {
    my $self = shift;

    my %found = ();
    for my $path ( @INC ) {
        my $digest_dir = File::Spec->catdir( $path, 'Digest' );
        next unless -d $digest_dir;
        opendir my $dir, $digest_dir or next;
        my @content = readdir $dir;
        closedir $dir;

        for my $item ( @content ) {
            my $full_path = File::Spec->catfile( $digest_dir, $item );
            next unless -f $full_path;
            next unless $item =~ m{\A(.*[A-Z].*)\.pm\z};
            my $module = $1;
            $found{ $module } = 1;
        }
    }
    print "Available digests:\n";
    printf "- %s\n", $_ for sort keys %found;
    exit( 0 );
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

=cut

sub validate_args {
    my $self = shift;

    eval {
        my $digest = Digest->new( $self->{ 'digest' } );
        $self->{ 'digest' } = $digest;
    };
    if ( $EVAL_ERROR ) {
        printf STDERR "Cannot initialize digester %s. Try %s --list\n", $self->{ 'digest' }, $PROGRAM_NAME;
        exit 1;
    }
    return;
}

1;
