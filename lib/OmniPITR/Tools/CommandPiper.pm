package OmniPITR::Program::Tools::CommandPiper;
use strict;
use warnings;
use English qw( -no_match_vars );

=head1 NAME

OmniPITR::Program::Tools::CommandPiper - Class for building complex pipe-based shell commands

=head1 VERSION

Version 0.4.0

=cut

our $VERSION = '0.4.0';

=head1 SYNOPSIS

General usage is:

    my $run = OmniPITR::Program::Tools::CommandPiper->new( 'ls', '-l' );
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
    $self->{ 'cmd' }             = [ @ARGV ];
    $self->{ 'stdout_files' }    = [];
    $self->{ 'stdout_programs' } = [];
    $self->{ 'stderr_files' }    = [];
    $self->{ 'stderr_programs' } = [];
    return $self;
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
    my $self        = shift;
    my $sub_program = OmniPITR::Program::Tools::CommandPiper->new( @ARGV );
    push @{ $self->{ 'stdout_programs' } }, $sub_program;
    return $sub_program;
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
    my $self        = shift;
    my $sub_program = OmniPITR::Program::Tools::CommandPiper->new( @ARGV );
    push @{ $self->{ 'stderr_programs' } }, $sub_program;
    return $sub_program;
}

=head1 command()

Returns stringified command that should be ran via "system" that does all the described redirections.

=cut

sub command {
    my $self = shift;
}

1;

