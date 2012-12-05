package OmniPITR::Program::Cleanup;
use strict;
use warnings;

our $VERSION = '1.0.0';
use base qw( OmniPITR::Program );

use Carp;
use OmniPITR::Tools qw( :all );
use English qw( -no_match_vars );
use File::Spec;
use Getopt::Long qw( :config no_ignore_case );
use Cwd;

=head1 run()

Main function, called by actual script in bin/, wraps all work done by
script with the sole exception of reading and validating command line
arguments.

These tasks (reading and validating arguments) are in this module, but
they are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need
further information - simply check doc for the method you have questions
about.

=cut

sub run {
    my $self = shift;

    if ( $self->{ 'removal-pause-trigger' } && -e $self->{ 'removal-pause-trigger' } ) {
        $self->log->log( 'Pause trigger exists (%s), NOT removing any old segments.', $self->{ 'removal-pause-trigger' } );
        return;
    }

    my @to_be_removed = $self->get_list_of_segments_to_remove();

    return if 0 == scalar @to_be_removed;

    my $count = unlink map { File::Spec->catfile( $self->{ 'archive' }->{ 'path' }, $_ ) } @to_be_removed;
    if ( $self->{ 'verbose' } ) {
        if ( $count == scalar @to_be_removed ) {
            $self->log->log( 'Segment %s removed.', $_ ) for @to_be_removed;
        }
        else {
            $self->log->log( 'Segment %s removed.', $_ ) for grep { !-e File::Spec->catfile( $self->{ 'archive' }->{ 'path' }, $_ ) } @to_be_removed;
        }
    }
    $self->log->log( '%d segments removed.', $count );

    return;
}

=head1 get_list_of_segments_to_remove()

Scans archive directory, and returns names of all files, which are
"older" than last required segment (given as argument on command line)

Older - is defined as alphabetically smaller than required segment.

=cut

sub get_list_of_segments_to_remove {
    my $self           = shift;
    my $last_important = $self->{ 'segment' };

    my $extension = undef;
    $extension = ext_for_compression( $self->{ 'archive' }->{ 'compression' } ) if $self->{ 'archive' }->{ 'compression' };
    my $dir;

    unless ( opendir( $dir, $self->{ 'archive' }->{ 'path' } ) ) {
        $self->log->fatal( 'Cannot open archive directory (%s) for reading: %s', $self->{ 'archive' }->{ 'path' }, $OS_ERROR );
    }
    my @content = readdir $dir;
    closedir $dir;

    my @too_old = ();
    for my $file ( @content ) {
        my $copy = $file;
        $file =~ s/\Q$extension\E\z// if $extension;
        next unless $file =~ m{\A[a-fA-F0-9]{24}(?:\.[a-fA-F0-9]{8}\.backup)?\z};
        next unless $file lt $last_important;
        push @too_old, $copy;
    }
    if ( 0 == scalar @too_old ) {
        $self->log->log( 'No files to be removed.' ) if $self->verbose;
        return;
    }

    my @sorted = sort @too_old;

    $self->log->log( '%u segments too old, to be removed.', scalar @too_old ) if $self->verbose;
    $self->log->log( 'First segment to be removed: %s. Last one: %s', $too_old[ 0 ], $too_old[ -1 ] ) if $self->verbose;

    return @sorted;
}

=head1 read_args()

Function which does all the parsing, and transformation of command line
arguments.

It also verified base facts about passed WAL segment name, but all other
validations, are being done in separate function: L<validate_args()>.

=cut

=head1 read_args()

=cut

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my %args = ();

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-restore.pod' )
        unless GetOptions(
        \%args,
        'log|l=s',
        'pid-file=s',
        'archive|a=s',
        'verbose|v',
        'removal-pause-trigger|p=s',
        );

    croak( '--log was not provided - cannot continue.' ) unless $args{ 'log' };
    $args{ 'log' } =~ tr/^/%/;

    for my $key ( keys %args ) {
        next if $key =~ m{ \A (?: archive | log ) \z }x;    # Skip those, not needed in $self
        $self->{ $key } = $args{ $key };
    }

    # We do it here so it will actually work for reporing problems in validation
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

    $self->log->fatal( 'Archive path not provided!' ) unless $args{ 'archive' };

    if ( $args{ 'archive' } =~ s/\A(gzip|bzip2|lzma)=// ) {
        $self->{ 'archive' }->{ 'compression' } = $1;
    }
    $self->{ 'archive' }->{ 'path' } = $args{ 'archive' };

    # These could theoretically go into validation, but we need to check if we can get anything to put in segment key in $self
    $self->log->fatal( 'WAL segment name has not been given' ) if 1 > scalar @ARGV;
    $self->log->fatal( 'Too many arguments given.' ) if 1 < scalar @ARGV;

    $self->{ 'segment' } = shift @ARGV;

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->verbose;

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

One exception is for compression programs paths - technically, it could
be validated in here, but benefit would be pretty limited, and code to
do so relatively complex, as compression program path might, but doesn't
have to be actual file path - it might be just program name (without
path), which is the default.

=cut

sub validate_args {
    my $self = shift;

    $self->log->fatal( 'Given segment name is not valid (%s)', $self->{ 'segment' } ) unless $self->{ 'segment' } =~ m{\A([a-fA-F0-9]{24}(?:\.[a-fA-F0-9]{8}\.backup)?|[a-fA-F0-9]{8}\.history)\z};

    $self->log->fatal( 'Given archive (%s) is not a directory', $self->{ 'archive' }->{ 'path' } ) unless -d $self->{ 'archive' }->{ 'path' };
    $self->log->fatal( 'Given archive (%s) is not readable',    $self->{ 'archive' }->{ 'path' } ) unless -r $self->{ 'archive' }->{ 'path' };
    $self->log->fatal( 'Given archive (%s) is not writable',    $self->{ 'archive' }->{ 'path' } ) unless -w $self->{ 'archive' }->{ 'path' };

    return;
}

1;
