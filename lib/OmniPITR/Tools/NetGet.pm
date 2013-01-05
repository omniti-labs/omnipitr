package OmniPITR::Tools::NetGet;
use strict;
use warnings;
use OmniPITR::Tools qw( run_command );
use English qw( -no_match_vars );
use Data::Dumper;
use Carp;
use base qw( Exporter );

=head1 NAME

OmniPITR::Tools::NetGet - Module for getting files over HTTP

=cut

our $VERSION = '1.0.0';

our @EXPORT_OK = qw( download );
our %EXPORT_TAGS = ( 'all' => \@EXPORT_OK );

our $getter = undef;

=head1 SYNOPSIS

General usage is:

    OmniPITR::Tools::NetGet->download( 'http://some/url', 'local.file' );

=head1 DESCRIPTION

This module is used for remote file access from OmniPITR. It's purpose is to abstract out whether HTTP transfer is done using LWP module, or one of supported shell tools (curl, wget).

In all of the cases, interface is the same: single download() call, with two parameters.

=cut

=head1 download()

Downloads given url, and saves response (without headers) in given file path.

Parameters:

=over

=item 1. url

=item 2. local file name (with path)

=back

=cut

sub download {
    my ( $url, $local_filename ) = @_;
    _pick_download_method();
    return $getter->( $url, $local_filename );
}

=head1 _pick_download_method()

Helper function, which sets module-variable $getter to coderef for function
that actually does download.

=cut

sub _pick_download_method {
    return if defined $getter;

    eval {
        require LWP::UserAgent;
        import LWP::UserAgent;
    };
    if ( !$EVAL_ERROR ) {
        $getter = \&_download_via_lwp;
        return;
    }

    my $temp_dir = $ENV{ 'TMPDIR' } || '/tmp';

    my $wget = run_command( $temp_dir, 'wget', '--version' );
    if ( !$wget->{ 'error_code' } ) {
        $getter = \&_download_via_wget;
        return;
    }

    my $curl = run_command( $temp_dir, 'curl', '--version' );
    if ( !$curl->{ 'error_code' } ) {
        $getter = \&_download_via_curl;
        return;
    }
    croak( 'There is none of: LWP::UserAgent perl module, wget program nor curl program available?!' );
}

=head1 _download_via_lwp

Actual downloading code, using LWP::UserAgent module.

=cut

sub _download_via_lwp {
    my ( $url, $filename ) = @_;

    open my $fh, '>', $filename or croak( "Cannot write to $filename: $OS_ERROR" );
    binmode $fh;
    my $agent = LWP::UserAgent->new();

    my $response = $agent->get( $url );
    my $code     = $response->code;
    croak( "Getting $url failed with HTTP/$code\n" ) if 200 != $code;

    print $fh $response->decoded_content( 'charset' => 'none' );
    close $fh;
    return;
}

=head1 _download_via_wget

Actual downloading code, using wget program

=cut

sub _download_via_wget {
    my ( $url, $filename ) = @_;

    my $temp_dir = $ENV{ 'TMPDIR' } || '/tmp';

    my $rc = run_command( $temp_dir, 'wget', '-O', $filename, $url );
    croak( "Getting $url failed with " . $rc->{ 'stderr' } ) if $rc->{ 'error_code' };

    return;
}

=head1 _download_via_curl

Actual downloading code, using curl program

=cut

sub _download_via_curl {
    my ( $url, $filename ) = @_;

    open my $fh, '>', $filename or croak( "Cannot write to $filename: $OS_ERROR" );
    binmode $fh;
    my $temp_dir = $ENV{ 'TMPDIR' } || '/tmp';

    my $rc = run_command( $temp_dir, 'curl', '--silent', '--show-error', '--fail', '--location', $url );
    croak( "Getting $url failed with " . $rc->{ 'stderr' } ) if $rc->{ 'error_code' };

    print $fh $rc->{ 'stdout' };
    close $fh;
    return;
}

1;
