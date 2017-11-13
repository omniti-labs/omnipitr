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

our $VERSION = '2.0.0';

=head1 SYNOPSIS

General usage is:

    my $run = OmniPITR::Tools::CommandPiper->new( 'ls', '-l' );
    $run->add_stdout_file( '/tmp/z.txt' );
    $run->add_stdout_file( '/tmp/y.txt' );
    my $checksummer = $run->add_stdout_program( 'md5sum', '-' );
    $checksummer->add_stdout_file( '/tmp/checksum.txt' );

    system( $run->command() );

Will run:

    mkfifo /tmp/CommandPiper-26195-oCZ7Sw/fifo-0
    md5sum - < /tmp/CommandPiper-26195-oCZ7Sw/fifo-0 > /tmp/checksum.txt &
    ls -l | tee /tmp/z.txt /tmp/y.txt > /tmp/CommandPiper-26195-oCZ7Sw/fifo-0
    wait
    rm /tmp/CommandPiper-26195-oCZ7Sw/fifo-0

While it might look like overkill for something that could be achieved by:

    ls -l | tee /tmp/z.txt /tmp/y.txt | md5sum - > /tmp/checksum.txt

the way it works is beneficial for cases with multiple different redirections.

For example - it works great for taking single backup, compressing it with
two different tools, saving it to multiple places, including delivering it
via ssh tunnel to file on remote server. All while taking checksums, and
also delivering them to said locations.

=head1 DESCRIPTION

It is important to note that to make the final shell command (script) work, it should be run within shell that has access to:

=over

=item * mkfifo

=item * rm

=item * tee

=back

commands. These are standard on all known to me Unix-alike systems, so it
shouldn't be a problem.

=head1 MODULE VARIABLES

=head2 $fifo_dir

Temporary directory used to store all the fifos. It takes virtually no disk
space, so it can be created anywhere.

Thanks to L<File::Temp> logic, the directory will be removed when the
program will end.

=cut

our $fifo_dir = tempdir( 'CommandPiper-' . $$ . '-XXXXXX', 'CLEANUP' => 1, 'TMPDIR' => 1 );

=head1 METHODS

=head2 new()

Object constructor.

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

=head2 set_write_mode()

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

=head2 set_tee_path()

Sets path to tee program, when using tee is required.

    $program->set_tee_path( '/opt/gnu/bin/tee' );

Value of tee path will be automatically copied to all newly created stdout and stderr programs.

=cut

sub set_tee_path {
    my $self = shift;
    $self->{ 'tee' } = shift;
    return;
}

=head2 add_stdout_file()

Adds another file destination for stdout from current program.

=cut

sub add_stdout_file {
    my $self   = shift;
    my $stdout = shift;
    push @{ $self->{ 'stdout_files' } }, $stdout;
    return;
}

=head2 add_stdout_program()

Add another program that should receive stdout from current program, as its (the new program) stdin.

=cut

sub add_stdout_program {
    my $self = shift;
    push @{ $self->{ 'stdout_programs' } }, $self->new_subprogram( @_ );
    return $self->{ 'stdout_programs' }->[ -1 ];
}

=head2 add_stderr_file()

Add another program that should receive stdout from current program, as its (the new program) stdin.

=cut

sub add_stderr_file {
    my $self   = shift;
    my $stderr = shift;
    push @{ $self->{ 'stderr_files' } }, $stderr;
    return;
}

=head2 add_stderr_program()

Add another program that should receive stderr from current program, as its (the new program) stdin.

=cut

sub add_stderr_program {
    my $self = shift;
    push @{ $self->{ 'stderr_programs' } }, $self->new_subprogram( @_ );
    return $self->{ 'stderr_programs' }->[ -1 ];
}

=head2 new_subprogram()

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

=head2 command()

Returns stringified command that should be ran via "system" that does all the described redirections.

Alternatively, the command can be written to text file, and run with

    bash /name/of/the/file

=cut

sub command {
    my $self = shift;

    # Get list of all fifos that are necessary to create, so we can run "mkfifo" on it.
    my @fifos = $self->get_all_fifos( 0 );

    my $fifo_preamble = scalar( @fifos ) ? 'mkfifo ' . join( " ", map { quotemeta( $_->[ 0 ] ) } @fifos ) . "\n" : '';

    # This loop actually writes (well, appends to the $fifo_preamble variable) fifo'ed commands, like:
    #     md5sum - < /tmp/CommandPiper-26195-oCZ7Sw/fifo-0 > /tmp/checksum.txt &
    for my $fifo ( @fifos ) {
        $fifo_preamble .= sprintf "%s &\n", $fifo->[ 1 ]->get_command_with_stdin( $fifo->[ 0 ] );
    }

    # we need to remove the fifos afterwards.
    my $fifo_cleanup = scalar( @fifos ) ? 'rm ' . join( " ", map { quotemeta( $_->[ 0 ] ) } @fifos ) . "\n" : '';

    return $fifo_preamble . $self->get_command_with_stdin() . "\nwait\n" . $fifo_cleanup;
}

=head2 base_program()

Helper functions which returns current program, with its arguments, properly
escaped, so that it can be included in shell script/command.

=cut

sub base_program {
    my $self = shift;
    return join( ' ', map { quotemeta $_ } @{ $self->{ 'cmd' } } );
}

=head2 get_command_with_stdin()

This is the most important part of the code.

In here, there is single line generated that runs current program adding all
necessary stdout and stderr redirections.

Optional $fifo argument is treated as place where current program should
read it's stdin from. If it's absent, it will read stdin from normal STDIN,
but if it is there, generated command will contain:

    ... < $fifo

for stdin redirection.

This redirection is for fifo-reading commands.

=cut

sub get_command_with_stdin {
    my $self = shift;
    my $fifo = shift;

    # start is always the command itself
    my $command = $self->base_program();

    # Now, we add stdin redirection, if it's needed
    $command .= ' < ' . quotemeta( $fifo ) if defined $fifo;

    # If these are stderr files, add stderr redirection "2> ..."
    if ( 0 < scalar @{ $self->{ 'stderr_files' } } ) {

        # Due to how stderr redirection works, we can't really handle more
        # than one stderr file. This is handled in L<get_all_fifos>, which,
        # changes multiple stderr files, into single stderr file, which is
        # fifo to "tee" which outputs to all the files.
        # The croak() is just a sanity check.
        croak( "This should never happen. Too many stderr files?!" ) if 1 < scalar @{ $self->{ 'stderr_files' } };

        # Actually add stderr redirect.
        $command .= sprintf ' 2%s %s', $self->{ 'write_mode' }, quotemeta( $self->{ 'stderr_files' }->[ 0 ] );
    }

    # If there are no files to capture stdout - we're done - no sense in
    # walking through further logic
    return $command if 0 == scalar @{ $self->{ 'stdout_files' } };

    # If there is just one stdout file, then just add redirect like
    # /some/command > file
    # or >> file, in case of appending.
    if ( 1 == scalar @{ $self->{ 'stdout_files' } } ) {
        return sprintf( '%s %s %s', $command, $self->{ 'write_mode' }, quotemeta( $self->{ 'stdout_files' }->[ 0 ] ) );
    }

    # If there are many stdout files, then we need tee.
    # This needs to take the final file off the list, so we can:
    # ... | tee file1 file2 > file3
    # as opposed to:
    # ... | tee file1 file2 file3
    # since the latter would also output the content to normal STDOUT.
    my $final_file = pop @{ $self->{ 'stdout_files' } };

    # The tee run itself - basically "tee file1 file2 ... file(N-1) > fileN"
    my $tee = sprintf '%s%s %s %s %s',
        quotemeta( $self->{ 'tee' } ),
        $self->{ 'write_mode' } eq '>' ? '' : ' -a',
        join( ' ', map { quotemeta( $_ ) } @{ $self->{ 'stdout_files' } } ),
        $self->{ 'write_mode' },
        quotemeta( $final_file );

    return "$command | $tee";
}

=head2 get_all_fifos()

To generate output script we need first to generate all fifos.

Since the command itself is built from tree-like data structure, we need to
parse it depth-first, and find all cases where fifo is needed, and add it to
list of fifos to be created.
Actual lines to generate "mkfifo .." and "... < fifo" commands are in
L<command()> method.

All stdout and stderr programs (i.e. programs that should receive given
command stdout or stderr) are turned into files (fifos), so after running
get_all_fifos, no command in the whole tree should have any
"stdout_programs" or "stderr_programs". Instead, they will have more
"std*_files", and fifos will be created.

While processing all the commands down the tree, this method also checks if
given command doesn't have multiple stderr_files.

Normally you can output stdout to multiple files with:

    /some/command | tee file1 file2 > file3

but there is no easy (and readable) way to do it with stderr.

So, if there are many stderr files, new fifo is created which does:

    tee file1 file2 > file3

and then current command is changed to have only this single fifo as it's
stderr_file.

=cut

sub get_all_fifos {
    my $self = shift;

    # The id itself is irrelevant, but we need to keep names of generated
    # fifos unique.
    # So they are stored in temp directory ( $fifo_dir, module variable ),
    # and named as: "fifo-n", where n is simply monotonically increasing
    # integer.
    # $fifo_id is simply information how many fifos have been already
    # created across whole command tree.
    my $fifo_id = shift;

    # Will contain information about all fifos in current command and all of
    # its subcommands (stdout/stderr programs)
    my @fifos = ();

    # Recursively call get_all_fifos() for all stdout_programs and
    # stderr_programs, to fill the @fifo in correct order (depth first).
    for my $sub ( @{ $self->{ 'stdout_programs' } }, @{ $self->{ 'stderr_programs' } } ) {
        push @fifos, $sub->get_all_fifos( $fifo_id + scalar @fifos );
    }

    # For every stdout program, make new fifo, attach this program to
    # created fifo, and push fifo as stdout_file.
    # Entry in stdout_programs gets removed.
    while ( my $sub = shift @{ $self->{ 'stdout_programs' } } ) {
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $sub ];
        push @{ $self->{ 'stdout_files' } }, $fifo_name;
    }

    # Same logic as above, but this time working on stderr.
    # This should probably be moved to single loop, but the inner part of
    # the while() {} is so small that I don't really care at the moment.
    while ( my $sub = shift @{ $self->{ 'stderr_programs' } } ) {
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $sub ];
        push @{ $self->{ 'stderr_files' } }, $fifo_name;
    }

    # As described in the method documentation - when there are many
    # stderr_files, we need to add fifo with tee to multiply the stream.
    # This is done here.
    if ( 1 < scalar @{ $self->{ 'stderr_files' } } ) {

        # tee should get all, but last one, arguments, as the last one will
        # be provided by ">" or ">>" redirect.
        my $final_stderr_file = pop @{ $self->{ 'stderr_files' } };

        # We're creating subprogram for the tee and all (but last one) files
        my $stderr_tee = $self->new_subprogram( $self->{ 'tee' }, @{ $self->{ 'stderr_files' } } );

        # The last file gets attached to tee as stdout file, so in final
        # script is will be added as "> file" or ">> file"
        $stderr_tee->add_stdout_file( $final_stderr_file );

        # Generate fifo for the tee command
        my $fifo_name = $self->get_fifo_name( $fifo_id + scalar @fifos );
        push @fifos, [ $fifo_name, $stderr_tee ];

        # Change stderr_files so that there will be just one of them,
        # pointing to fifo for tee.
        $self->{ 'stderr_files' } = [ $fifo_name ];
    }

    # At the moment @fifos contains information about all fifos that are
    # needed by subprograms and by current program too.
    return @fifos;
}

=head2 get_fifo_name()

Each fifo needs unique name. Method for generation is simple - we're using
predefined $fifo_dir, in which the fifo will be named "fifo-$id".

This is very simple, but I wanted to keep it separately so that it could be
changed easily in future.

=cut

sub get_fifo_name {
    my $self = shift;
    my $id   = shift;
    return File::Spec->catfile( $fifo_dir, "fifo-" . $id );
}

1;
