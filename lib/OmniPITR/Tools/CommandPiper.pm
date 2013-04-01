package OmniPITR::Tools::CommandPiper;
use strict;
use warnings;
use English qw( -no_match_vars );

=head1 NAME

OmniPITR::Tools::CommandPiper - Class for building complex pipe-based shell commands

=cut

our $VERSION = '1.1.0';

=head1 SYNOPSIS

General usage is:

    my $run = OmniPITR::Tools::CommandPiper->new( 'ls', '-l' );
    $run->add_stdout_file( '/tmp/z.txt' );
    $run->add_stdout_file( '/tmp/y.txt' );
    my $checksummer = $run->add_stdout_program( 'md5sum', '-' );
    $checksummer->add_stdout_file( '/tmp/checksum.txt' );

    system( $run->command() );

    Will run:

    ls -l | tee /tmp/z.txt /tmp/y.txt | md5sum - > /tmp/checksum.txt

=head1 DESCRIPTION

It is important to note that to make the final shell command work, it should be run within bash (or other shell that accepts constructions like:

    program > >( other program )

And that you have tee program in path.

=cut

=head1 new()

Object contstructor.

Given options are treated as program that is run to generate stdout.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->{ 'tee' }             = 'tee';
    $self->{ 'write_mode' }      = '>';
    $self->{ 'cmd' }             = [ @_ ];
    $self->{ 'stdout_files' }    = [];
    $self->{ 'stdout_programs' } = [];
    $self->{ 'stderr_files' }    = [];
    $self->{ 'stderr_programs' } = [];
    return $self;
}

=head1 set_write_mode()

Sets whether writes of data should overwrite, or append (> vs. >>)

Accepted values:

=over

=item * overwrite

=item * append

=back

Any other would switch back to default, which is overwrite.

=cut

sub set_write_mode {
    my $self = shift;
    my $want = shift;
    $self->{ 'write_mode' } = '>';
    $self->{ 'write_mode' } = '>>' if $want eq 'append';
    return;
}

=head1 set_tee_path()

Sets path to tee program, when using tee is required.

    $program->set_tee_path( '/opt/gnu/bin/tee' );

Value of tee path will be automaticaly copied to all newly created stdout and stderr programs.

=cut

sub set_tee_path {
    my $self = shift;
    $self->{ 'tee' } = shift;
    return;
}

=head1 add_stdout_file()

Adds another file destination for stdout from current program.

=cut

sub add_stdout_file {
    my $self   = shift;
    my $stdout = shift;
    push @{ $self->{ 'stdout_files' } }, $stdout;
    return;
}

=head1 add_stdout_program()

Add another program that should receive stdout from current program, as its (the new program) stdin.

=cut

sub add_stdout_program {
    my $self = shift;
    push @{ $self->{ 'stdout_programs' } }, $self->new_subprogram( @_ );
    return $self->{ 'stdout_programs' }->[ -1 ];
}

=head1 add_stderr_file()

Add another program that should receive stdout from current program, as its (the new program) stdin.

=cut

sub add_stderr_file {
    my $self   = shift;
    my $stderr = shift;
    push @{ $self->{ 'stderr_files' } }, $stderr;
    return;
}

=head1 add_stderr_program()

Add another program that should receive stderr from current program, as its (the new program) stdin.

=cut

sub add_stderr_program {
    my $self = shift;
    push @{ $self->{ 'stderr_programs' } }, $self->new_subprogram( @_ );
    return $self->{ 'stderr_programs' }->[ -1 ];
}

=head1 new_subprogram()

Helper function to create sub-programs, inheriting settings

=cut

sub new_subprogram {
    my $self        = shift;
    my $sub_program = OmniPITR::Tools::CommandPiper->new( @_ );
    for my $key ( qw( tee write_mode ) ) {
        $sub_program->{ $key } = $self->{ $key };
    }
    return $sub_program;
}

=head1 command()

Returns stringified command that should be ran via "system" that does all the described redirections.

=cut

sub command {
    my $self = shift;

    my $program = join( ' ', map { quotemeta $_ } @{ $self->{ 'cmd' } } );

    my $stderr_redirection = $self->stderr();
    my $stdout_redirection = $self->stdout();

    $program .= ' ' . $stderr_redirection if $stderr_redirection;
    $program .= ' ' . $stdout_redirection if $stdout_redirection;

    return $program;
}

=head1 stdout()

Internal function returning whole stdout redirection for current program, or undef if there are no stdout consummers.

=cut

sub stdout {
    my $self         = shift;
    my @stdout_parts = ();
    push @stdout_parts, map { [ 'PATH', $_ ] } @{ $self->{ 'stdout_files' } };
    push @stdout_parts, map { [ 'CMD',  $_->command() ] } @{ $self->{ 'stdout_programs' } };
    return if 0 == scalar @stdout_parts;

    my $ready = $self->tee_maker( @stdout_parts );
    return sprintf( '%s %s', $self->{ 'write_mode' }, $ready->[ 1 ] ) if 'PATH' eq $ready->[ 0 ];
    return sprintf( '| %s', $ready->[ 1 ] );
}

=head1 stderr()

Internal function returning whole stderr redirection for current program, or undef if there are no stderr consummers.

=cut

sub stderr {
    my $self         = shift;
    my @stderr_parts = ();
    push @stderr_parts, map { [ 'PATH', $_ ] } @{ $self->{ 'stderr_files' } };
    push @stderr_parts, map { [ 'CMD',  $_->command() ] } @{ $self->{ 'stderr_programs' } };
    return if 0 == scalar @stderr_parts;

    my $ready = $self->tee_maker( @stderr_parts );
    return sprintf( '2%s %s', $self->{ 'write_mode' }, $ready->[ 1 ] ) if 'PATH' eq $ready->[ 0 ];
    return sprintf( '2> >( %s )', $ready->[ 1 ] );
}

=head1 tee_maker()

Receives array of arrayrefs. Each element is array with two values:

=over

=item 1. type of element "PATH" - path to file, "CMD" - command to run

=item 2. path to file, or command line of program to run.

=back

Returns single item of [ "PATH", "/path/to/file" ] or [ "CMD", "command to run" ] that will deliver data to all receivers.

=cut

sub tee_maker {
    my $self  = shift;
    my @parts = @_;
    if ( 1 == scalar @parts ) {
        $parts[ 0 ]->[ 1 ] = quotemeta( $parts[ 0 ]->[ 1 ] ) if 'PATH' eq $parts[ 0 ]->[ 0 ];
        return $parts[ 0 ];
    }

    my $last     = pop @parts;
    my @tee_args = ();
    push @tee_args, '-a' if $self->{ 'write_mode' } eq '>>';
    for my $p ( @parts ) {
        if ( 'PATH' eq $p->[ 0 ] ) {
            push @tee_args, quotemeta $p->[ 1 ];
            next;
        }
        push @tee_args, sprintf( '>( %s )', $p->[ 1 ] );
    }
    my $tee_invocation = sprintf '%s %s', quotemeta( $self->{ 'tee' } ), join( ' ', @tee_args );
    if ( 'PATH' eq $last->[ 0 ] ) {
        $tee_invocation .= sprintf ' %s %s', $self->{ 'write_mode' }, quotemeta( $last->[ 1 ] );
    }
    else {
        $tee_invocation .= ' > >( ' . $last->[ 1 ] . ' ) ';
    }
    return [ 'CMD', $tee_invocation ];
}

1;

