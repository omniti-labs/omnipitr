package OmniPITR::Archive;
use strict;
use warnings;
use base qw( OmniPITR::Program );
use Carp;
use English qw( -no_match_vars );
use File::Basename;
use File::Spec;
use File::Path qw( make_path );
use File::Copy;
use Storable;
use Getopt::Long;
use Data::Dumper;
use Digest::MD5;

sub run {
    my $self = shift;
    $self->read_state();
    $self->prepare_temp_directory();
    $self->copy_segment_to_temp_dir();
}

sub copy_segment_to_temp_dir {
    my $self = shift;
    return if $self->segment_already_copied();
    my $new_file = $self->get_temp_filename_for( 'none' );
    unless ( copy( $self->{'segment'}, $new_file ) ) {
        $self->log->fatal('Cannot copy %s to %s : %s', $self->{'segment'}, $new_file, $OS_ERROR );
    }
    my $has_md5 = $self->md5sum( $new_file );
    $self->{'state'}->{'compressed'}->{'none'} = $has_md5;
    $self->save_state();
    return;
}

sub segment_already_copied {
    my $self = shift;
    return unless $self->{ 'state' }->{ 'compressed' }->{ 'none' };
    my $want_md5 = $self->{ 'state' }->{ 'compressed' }->{ 'none' };

    my $temp_file_name = $self->get_temp_filename_for( 'none' );
    return unless -e $temp_file_name;

    my $has_md5 = $self->md5sum( $temp_file_name );
    if ( $has_md5 eq $want_md5 ) {
        $self->log->log('Segment has been already copied to temp location.');
        return 1;
    }

    unlink $temp_file_name;
    $self->log->error( 'Segment already copied, but with bad MD5 ?!' );

    return;
}

sub get_temp_filename_for {
    my $self = shift;
    my $type = shift;

    return File::Spec->catfile( $self->{ 'temp-dir' }, $type );
}

sub md5sum {
    my $self     = shift;
    my $filename = shift;

    my $ctx = Digest::MD5->new;

    open my $fh, '<', $filename or $self->log->fatal( 'Cannot open file for md5summing %s : %s', $filename, $OS_ERROR );
    $ctx->addfile( $fh );
    my $md5 = $ctx->hexdigest();
    close $fh;

    return $md5;
}

sub prepare_temp_directory {
    my $self = shift;
    my $full_temp_dir = File::Spec->catfile( $self->{ 'temp-dir' }, basename( $PROGRAM_NAME ), basename( $self->{ 'segment' } ) );
    make_path( $full_temp_dir );
    $self->{ 'temp-dir' } = $full_temp_dir;
    return;
}

sub read_state {
    my $self = shift;
    $self->{ 'state' } = {};

    return unless $self->{ 'state-dir' };

    $self->{ 'state-file' } = File::Spec->catfile( $self->{ 'state-dir' }, basename( $self->{ 'segment' } ) );
    return unless -f $self->{ 'state-file' };
    $self->{ 'state' } = retrieve( $self->{ 'state-file' } );
    return;
}

sub save_state {
    my $self = shift;

    return unless $self->{ 'state-file' };

    store( $self->{ 'state' }, $self->{ 'state-file' } );

    return;
}

sub read_args {
    my $self = shift;

    my @argv_copy = @ARGV;

    my %args = (
        'data-dir' => '.',
        'temp-dir' => $ENV{ 'TMPDIR' } || '/tmp',
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
    $self->{ 'log_template' } = $args{ 'log' };
    $self->{ 'log' }          = OmniPITR::Log->new( $self->{ 'log_template' } );

    # These could theoretically go into validation, but we need to check if we can get anything to {'segment'}
    $self->log->fatal( 'WAL segment file name has not been given' ) if 0 == scalar @ARGV;
    $self->log->fatal( 'More than 1 WAL segment file name has been given' ) if 1 < scalar @ARGV;

    $self->{ 'segment' } = shift @ARGV;

    $self->log->log( 'Called with parameters: %s', join( ' ', @argv_copy ) ) if $self->{ 'verbose' };

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

    $self->log->fatal( 'Given segment name is not valid (%s)', $self->{ 'segment' } ) unless basename( $self->{ 'segment' } ) =~ m{\A[a-f0-9]{24}\z};
    my $segment_file_name = $self->{ 'segment' };
    $segment_file_name = File::Spec->catfile( $self->{ 'data-dir' }, $self->{ 'segment' } ) unless $self->{ 'segment' } =~ m{^/};

    $self->log->fatal( 'Given segment (%s) does not exist.',  $segment_file_name ) unless -e $segment_file_name;
    $self->log->fatal( 'Given segment (%s) is not a file.',   $segment_file_name ) unless -f $segment_file_name;
    $self->log->fatal( 'Given segment (%s) is not readable.', $segment_file_name ) unless -r $segment_file_name;

    my $expected_size = 256**3;
    my $file_size     = ( -s $segment_file_name );
    $self->log->fatal( 'Given segment (%s) has incorrect size (%u vs %u).', $segment_file_name, $file_size, $expected_size ) unless $expected_size == $file_size;

    $self->{ 'segment' } = $segment_file_name;
    return;
}

1;
