package OmniPITR::Tools::CommandPiper;
use strict;
use warnings;
use English qw( -no_match_vars );
use Data::Dumper;
use File::Spec;
use File::Temp qw( tempdir );

=head1 NAME

OmniPITR::Tools::CommandPiper - Class for building complex pipe-based shell commands

=cut

our $VERSION = '1.1.0';

our $fifo_dir = tempdir( 'CommandPiper-' . $$ . '-XXXXXX', 'CLEANUP' => 1, 'TMPDIR' => 1 );

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
    $self->{ 'fifos' }           = [];
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
    $self->{ 'write_mode' } = $want eq 'append' ? '>>' : '>';
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

    my @fifos = $self->get_all_fifos( 0 );

    my $fifo_preamble = 'mkfifo ' . join( " ", map { quotemeta( $_->[ 0 ] ) } @fifos ) . "\n";
    for my $fifo ( @fifos ) {
        $fifo_preamble .= sprintf "%s &\n", $fifo->[ 1 ]->get_command_with_stdin( $fifo->[ 0 ] );
    }
    my $fifo_cleanup = 'rm ' . join( " ", map { quotemeta( $_->[ 0 ] ) } @fifos ) . "\n";

    return $fifo_preamble . $self->get_command_with_stdin() . "\nwait\n" . $fifo_cleanup;
}

sub base_program {
    my $self = shift;
    return join( ' ', map { quotemeta $_ } @{ $self->{ 'cmd' } } );
}

sub get_command_with_stdin {
    my $self = shift;
    my $fifo = shift;

    my $command = $self->base_program();
    $command .= ' < ' . quotemeta( $fifo ) if defined $fifo;

    if ( 0 < scalar @{ $self->{ 'stderr_files' } } ) {
        croak( "This should never happen. Too many stderr files?!" ) if 1 < scalar @{ $self->{ 'stderr_files' } };
        $command .= sprintf ' 2%s %s', $self->{ 'write_mode' }, quotemeta( $self->{ 'stderr_files' }->[ 0 ] );
    }

    return $command if 0 == scalar @{ $self->{ 'stdout_files' } };

    if ( 1 == scalar @{ $self->{ 'stdout_files' } } ) {
        return sprintf( '%s %s %s', $command, $self->{ 'write_mode' }, quotemeta( $self->{ 'stdout_files' }->[ 0 ] ) );
    }
    my $final_file = pop @{ $self->{ 'stdout_files' } };
    my $tee        = sprintf '%s%s %s %s %s',
        quotemeta( $self->{ 'tee' } ),
        $self->{ 'write_mode' } eq '>' ? '' : ' -a',
        join( ' ', map { quotemeta( $_ ) } @{ $self->{ 'stdout_files' } } ),
        $self->{ 'write_mode' },
        quotemeta( $final_file );
    return "$command | $tee";
}

sub get_all_fifos {
    my $self    = shift;
    my $fifo_id = shift;

    my @fifos = ();
    for my $sub ( @{ $self->{ 'stdout_programs' } }, @{ $self->{ 'stderr_programs' } } ) {
        push @fifos, $sub->get_all_fifos( $fifo_id + scalar @fifos );
    }
    while ( my $sub = shift @{ $self->{ 'stdout_programs' } } ) {
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $sub ];
        push @{ $self->{ 'stdout_files' } }, $fifo_name;
    }
    while ( my $sub = shift @{ $self->{ 'stderr_programs' } } ) {
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $sub ];
        push @{ $self->{ 'stderr_files' } }, $fifo_name;
    }
    if ( 1 < scalar @{ $self->{ 'stderr_files' } } ) {
        my $final_stderr_file = pop @{ $self->{ 'stderr_files' } };
        my $stderr_tee = $self->new_subprogram( $self->{ 'tee' }, @{ $self->{ 'stderr_files' } } );
        $stderr_tee->add_stdout_file( $final_stderr_file );
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $stderr_tee ];
        $self->{ 'stderr_files' } = [ $fifo_name ];
    }
    return @fifos;
}

sub get_fifo_name {
    my $self = shift;
    my $id   = shift;
    return File::Spec->catfile( $fifo_dir, "fifo-" . $id );
}

1;
