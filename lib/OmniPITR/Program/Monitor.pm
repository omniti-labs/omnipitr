package OmniPITR::Program::Monitor;
use strict;
use warnings;

our $VERSION = '1.0.0';
use base qw( OmniPITR::Program );

use Carp;
use English qw( -no_match_vars );
use Getopt::Long qw( :config no_ignore_case pass_through );
use Storable qw( fd_retrieve store_fd );
use File::Spec;
use Fcntl qw( :flock :seek );
use POSIX qw( strftime );
use Time::Local;

=head1 run()

Main function, called by actual script in bin/, wraps all work done by
script with the sole exception of reading and validating command line
arguments.

These tasks (reading and validating arguments) are in this module, but they
are called from L<OmniPITR::Program::new()>

Name of called method should be self explanatory, and if you need further
information - simply check doc for the method you have questions about.

=cut

sub run {
    my $self = shift;
    $self->load_state();
    if ( $self->read_logs() ) {
        $self->clean_old_state();
        $self->save_state();
    }

    my $check_state_dir = File::Spec->catfile( $self->{ 'state-dir' }, 'Check-' . $self->{ 'check' } );
    if ( !-d $check_state_dir ) {
        $self->log->fatal( 'Cannot create state dir for check (%s) : %s', $check_state_dir, $OS_ERROR ) unless mkdir $check_state_dir;
    }

    my $O = $self->{ 'check_object' };
    $O->setup(
        'state-dir' => $check_state_dir,
        'log'       => $self->{ 'log' },
        'psql'      => sub { return $self->psql( @_ ) },
    );

    $O->get_args();
    $O->run_check( $self->{ 'state' } );
    return;
}

=head1 read_logs()

Wraps all work related to finding actual log files, reading and parsing
them, and extracting information to "state".

=cut

sub read_logs {
    my $self = shift;

    $self->get_list_of_log_files();

    # SHORTCUT
    my $F = $self->state( 'files' );

    # SHORTCUT

    my @sorted_files = sort { $F->{ $a }->{ 'start_epoch' } <=> $F->{ $b }->{ 'start_epoch' } } @{ $self->{ 'log_files' } };

    my $any_changes = undef;

    for my $filename ( @sorted_files ) {

        # Shortcut
        my $D = $F->{ $filename };

        # Shortcut

        my $size = ( stat( $filename ) )[ 7 ];
        next if ( $D->{ 'seek' } ) && ( $D->{ 'seek' } >= $size );

        my $i = 0;
        open my $fh, '<', $filename or $self->log->fatal( 'Cannot open %s to read: %s', $filename, $OS_ERROR );
        seek( $fh, $D->{ 'seek' }, SEEK_SET ) if defined $D->{ 'seek' };
        while ( my $line = <$fh> ) {

            # We might read file that is being written to, so we should disregard any line that is partially written.
            last unless $line =~ s{\r?\n\z}{};

            $self->parse_line( $line );

            $D->{ 'seek' } = tell( $fh );
            $i++;
        }
        close $fh;
        $self->log->log( 'Read %d lines from %s', $i, $filename ) if $self->{ 'verbose' };
        $any_changes = 1 if $i;
    }

    return $any_changes;
}

=head1 parse_line()

Given line from logs, parses it to atoms, and stores important information to state.

=cut

sub parse_line {
    my $self = shift;
    my $line = shift;

    my $epoch = $self->extract_epoch( $line );
    $self->log->fatal( 'Cannot parse line: %s', $line ) unless $line =~ s/\A(.{26}) \+\d+ : (\d+) : omnipitr-(\S+) : //;
    my $timestamp    = $1;
    my $pid          = $2;
    my $program_name = $3;

    my $data = {
        'epoch'     => $epoch,
        'timestamp' => $timestamp,
        'pid'       => $pid,
        'line'      => $line,
    };

    if ( $line =~ m{^(ERROR|FATAL) : } ) {
        push @{ $self->{ 'state' }->{ 'errors' }->{ $1 } }, $data;
    }

    my $P = $self->{ 'parser' }->{ $program_name };
    if ( !defined $P ) {
        my $ignore;
        ( $P, $ignore ) = $self->load_dynamic_object( 'OmniPITR::Program::Monitor::Parser', $program_name );
        if ( defined $P ) {
            $self->{ 'parser' }->{ $program_name } = $P;
            $P->setup(
                'state' => $self->{ 'state' },
                'log'   => $self->{ 'log' },
            );
        }
        else {
            $self->{ 'parser' }->{ $program_name } = '';
        }
    }

    $P->handle_line( $data ) if ref $P;

    return;
}

=head1 clean_old_state()

Calls ->clean_state() on all parser objects (that were used in current iteration).

This is to remove from state old data, that is of no use currently.

=cut

sub clean_old_state {
    my $self = shift;

    for my $P ( values %{ $self->{ 'parser' } } ) {
        next unless ref $P;
        next unless $P->can( 'clean_state' );
        $P->clean_state();
    }

    my $cutoff = time() - 30 * 24 * 60 * 60;    # month ago
    for my $type ( qw( ERROR FATAL ) ) {
        next unless defined $self->{ 'state' }->{ 'errors' }->{ $type };
        $self->{ 'state' }->{ 'errors' }->{ $type } = [ grep { $_->{ 'epoch' } >= $cutoff } @{ $self->{ 'state' }->{ 'errors' }->{ $type } } ];
    }

    return;
}

=head1 get_list_of_log_files()

Scans given log paths, and collects list of files that are log files.

List of all log files is stored in $self->{'log_files'}, being arrayref.

=cut

sub get_list_of_log_files {
    my $self = shift;

    # SHORTCUT
    my $F = $self->state( 'files' );
    $F = $self->state( 'files', {} ) unless defined $F;

    # SHORTCUT

    $self->{ 'log_files' } = [];
    my @scan_for_timestamps = ();
    my %exists_file         = ();

    for my $template ( @{ $self->{ 'log-paths' } } ) {
        my $glob = $template;
        $glob =~ s/\%./*/g;
        for my $filename ( glob( $glob ) ) {
            $exists_file{ $filename } = 1;
            if ( $F->{ $filename } ) {
                push @{ $self->{ 'log_files' } }, $filename;
            }
            else {
                push @scan_for_timestamps, [ $filename, $template ];
            }
        }
    }

    for my $filename ( keys %{ $F } ) {
        delete $F->{ $filename } if !$exists_file{ $filename };
    }

    for my $file ( @scan_for_timestamps ) {
        my ( $filename, $template ) = @{ $file };

        my $fh;
        next unless open $fh, '<', $filename;

        my $data;
        my $length = sysread( $fh, $data, 27 );
        close $fh;

        next if 27 != $length;

        my $epoch = $self->extract_epoch( $data );
        next unless defined $epoch;

        my $reconstructed_filename = strftime( $template, localtime( $epoch ) );
        next unless $reconstructed_filename eq $filename;

        $F->{ $filename }->{ 'start_epoch' } = $epoch;
        push @{ $self->{ 'log_files' } }, $filename;
    }
    return;
}

=head1 extract_epoch()

Given line from logs, it returns epoch value of leading timestamp.

If the line cannot be parsed, or the value is not sensible time - undef is returned.

Returned epoch can (and usually will) contain fractional part - subsecond data with precision of up to microsecond (0.000001s).

=cut

sub extract_epoch {
    my $self = shift;
    my $line = shift;
    return if 27 > length $line;
    return unless my @elements = $line =~ m{\A(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(\.\d{6}) };
    my $subsecond = pop @elements;
    $elements[ 1 ]--;    # Time::Local expects months in range 0-11, and not 1-12.
    my $epoch;
    eval { $epoch = timelocal( reverse @elements ); };
    return if $EVAL_ERROR;
    return $epoch + $subsecond;
}

=head1 state()

Helper function, accessor, to state hash.

Has 1, or two arguments. In case of one argument - returns value, from state, for given key.

If it has two arguments, then - if 2nd argument is undef - it removes the key from state, and returns.

If the 2nd argument is defined, it sets value for given key to given value, and returns it.

=cut

sub state {
    my $self = shift;
    my $key  = shift;
    if ( 0 == scalar @_ ) {
        return $self->{ 'state' }->{ $key };
    }
    my $value = shift;
    if ( defined $value ) {
        return $self->{ 'state' }->{ $key } = $value;
    }
    delete $self->{ 'state' }->{ $key };
    return;
}

=head1 load_state()

Handler reading of state file.

It is important to note that it uses locking, so it will not conflict with
state writing from another run of omnipitr-monitor.

=cut

sub load_state {
    my $self = shift;
    $self->{ 'state' } = {};

    $self->{ 'state-file' } = File::Spec->catfile( $self->{ 'state-dir' }, 'omnipitr-monitor.state' );
    return unless -f $self->{ 'state-file' };

    open my $fh, '<', $self->{ 'state-file' }
        or $self->log->fatal( "Cannot open state file (%s) for reading: %s", $self->{ 'state-file' }, $OS_ERROR );

    # Make sure the file is not written to, at the moment.
    flock( $fh, LOCK_SH );

    $self->{ 'state' } = fd_retrieve( $fh );
    close $fh;

    return;
}

=head1 save_state()

Saves state in safe way (with proper locking).

=cut

sub save_state {
    my $self = shift;

    $self->{ 'state-file' } = File::Spec->catfile( $self->{ 'state-dir' }, 'omnipitr-monitor.state' );

    my $fh;
    if ( -f $self->{ 'state-file' } ) {
        open $fh, '+<', $self->{ 'state-file' }
            or $self->log->fatal( "Cannot open state file (%s) for writing: %s", $self->{ 'state-file' }, $OS_ERROR );
    }
    else {
        open $fh, '>', $self->{ 'state-file' }
            or $self->log->fatal( "Cannot open state file (%s) for writing: %s", $self->{ 'state-file' }, $OS_ERROR );
    }

    # Make sure the file is not written to, at the moment.
    flock( $fh, LOCK_EX );

    store_fd( $self->{ 'state' }, $fh );

    # In case current state was smaller than previously written
    truncate( $fh, tell( $fh ) );

    close $fh;

    return;
}

=head1 read_args()

Function which handles reading of base arguments ( i.e. without options specific to checks ).

=cut

sub read_args {
    my $self = shift;

    my %args = (
        'temp-dir' => $ENV{ 'TMPDIR' } || '/tmp',
        'psql-path' => 'psql',
    );

    croak( 'Error while reading command line arguments. Please check documentation in doc/omnipitr-archive.pod' )
        unless GetOptions(
        \%args,
        'log|l=s@',
        'check|c=s',
        'state-dir|s=s',
        'verbose|v',
        'database|d=s',
        'host|h=s',
        'port|p=i',
        'username|U=s',
        'temp-dir|t=s',
        'psql-path|pp=s',
        );

    for my $key ( qw( check state-dir verbose database host port username temp-dir psql-path ) ) {
        next unless defined $args{ $key };
        $self->{ $key } = $args{ $key };
    }

    $self->{ 'log-paths' } = $args{ 'log' } if defined $args{ 'log' };

    $self->{ 'log' } = OmniPITR::Log->new( \*STDOUT );

    return;
}

=head1 validate_args()

Does all necessary validation of given command line arguments.

=cut

sub validate_args {
    my $self = shift;

    $self->log->fatal( '--state-dir has not been provided!' ) unless defined $self->{ 'state-dir' };
    $self->log->fatal( "Given --state-dir (%s) does not exist",     $self->{ 'state-dir' } ) unless -e $self->{ 'state-dir' };
    $self->log->fatal( "Given --state-dir (%s) is not a directory", $self->{ 'state-dir' } ) unless -d $self->{ 'state-dir' };
    $self->log->fatal( "Given --state-dir (%s) is not writable",    $self->{ 'state-dir' } ) unless -w $self->{ 'state-dir' };

    $self->log->fatal( '--check has not been provided!' ) unless defined $self->{ 'check' };
    $self->log->fatal( 'Invalid value for --check: %s', $self->{ 'check' } )
        unless $self->{ 'check' } =~ m{ \A [a-zA-Z0-9]+ (?: [_-][a-zA-Z0-9]+ )* \z }x;

    $self->log->fatal( '--log has not been provided!' ) unless defined $self->{ 'log-paths' };

    for my $path ( @{ $self->{ 'log-paths' } } ) {
        $path =~ s/\^/\%/g;
    }

    ( $self->{ 'check_object' }, $self->{ 'check' } ) = $self->load_dynamic_object( 'OmniPITR::Program::Monitor::Check', $self->{ 'check' } );
    $self->log->fatal( 'Check code cannot be loaded.' ) unless $self->{ 'check_object' };

    return;
}

=head1 load_dynamic_object()

Loads class which name is based on arguments.

If loading will succeed, creates new object of this class and returns it.

If it fails - ends program with logged message.

=cut

sub load_dynamic_object {
    my $self         = shift;
    my $prefix       = shift;
    my $dynamic_part = shift;

    $prefix =~ s/:+\z//;

    $dynamic_part =~ s/[^a-zA-Z0-9]/_/g;
    $dynamic_part =~ s/([a-zA-Z0-9]+)/\u\L$1/g;

    my $full_class_name = $prefix . '::' . $dynamic_part;

    my $class_filename = $full_class_name . '.pm';
    $class_filename =~ s{::}{/}g;

    my $object;

    eval {
        require $class_filename;
        $object = $full_class_name->new();
    };
    if ( $EVAL_ERROR ) {
        $self->log->error( 'Cannot load class %s: %s', $full_class_name, $EVAL_ERROR ) if $self->{ 'verbose' };
        return ( undef, undef );
    }

    return $object, $dynamic_part;
}

1;
