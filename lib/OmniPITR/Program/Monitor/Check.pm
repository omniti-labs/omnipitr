package OmniPITR::Program::Monitor::Check;
use strict;
use warnings;
use Carp;
use English qw( -no_match_vars );

our $VERSION = '0.7.0';

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
    for my $key ( qw( log state-dir ) ) {
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

sub log { return shift->{'log'}; }

=head1 psql()

Runs given query via psql - assumes there is $self->{ 'psql-path' }.

Uses also:

=over

=item * username

=item * database

=item * port

=item * host

=item

optional keys from $self.

On first run it will cache psql call arguments, so if you'd change them on
subsequent calls, you have to delete $self->{'psql'}.

In case of errors, it raises fatal error.

Otherwise returns stdout of the psql.

=cut

sub psql {
    my $self = shift;
    my $query = shift;

    unless ( $self->{'psql'} ) {
        my @psql = ();
        push @psql, $self->{ 'psql-path' };
        push @psql, '-qAtX';
        push @psql, ( '-U', $self->{ 'username' } ) if $self->{ 'username' };
        push @psql, ( '-d', $self->{ 'database' } ) if $self->{ 'database' };
        push @psql, ( '-h', $self->{ 'host' } )     if $self->{ 'host' };
        push @psql, ( '-p', $self->{ 'port' } )     if $self->{ 'port' };
        push @psql, '-c';
        $self->{'psql'} = \@psql;
    }

    $self->prepare_temp_directory();

    my @command = ( @{ $self->{ 'psql' } }, $query );

    $self->log->time_start( $query ) if $self->verbose;
    my $status = run_command( $self->{ 'temp-dir' }, @command );
    $self->log->time_finish( $query ) if $self->verbose;

    $self->log->fatal( 'Running [%s] via psql failed: %s', $query, $status ) if $status->{ 'error_code' };

    return $status->{ 'stdout' };
}

=head1 find_tablespaces()

Helper function.  Takes no arguments.  Uses pg_tblspc directory and returns
a hashref of the physical locations of tablespaces.
Keys in the hashref are tablespace OIDs (link names in pg_tblspc). Values
are hashrefs with two keys:

=over

=item * pg_visible - what is the path to tablespace that PostgreSQL sees

=item * real_path - what is the real absolute path to tablespace directory

=back

The two can be different in case tablespace got moved and symlinked back to
original location, or if tablespace path itself contains symlinks.

=cut

sub get_tablespaces {
    my $self = shift;

    # Identify any tablespaces and get those
    my $tablespace_dir = File::Spec->catfile( $self->{ 'data-dir' }, "pg_tblspc" );
    my %tablespaces;

    return unless -e $tablespace_dir;

    my @pgfiles;
    opendir( my $dh, $tablespace_dir ) or $self->log->fatal( "Unable to open tablespace directory $tablespace_dir" );

    # Push onto our list the locations that are pointed to by the pg_tblspc symlinks
    foreach my $filename ( readdir $dh ) {
        next if $filename !~ /^\d+$/;    # Filename should be all numeric
        my $full_name = File::Spec->catfile( $tablespace_dir, $filename );
        next if !-l $full_name;          # It should be a symbolic link
        my $pg_visible = readlink $full_name;
        my $real_path = Cwd::abs_path( $full_name );
        $tablespaces{ $filename } = {
            'pg_visible' => $pg_visible,
            'real_path' => $real_path,
        };
    }
    closedir $dh;

    return \%tablespaces;
}

1;
