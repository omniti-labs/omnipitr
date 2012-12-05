#!/usr/bin/perl
use strict;
use warnings;
use File::Spec;
use Test::More;
use English qw(-no_match_vars);

eval { require Test::PerlTidy; import Test::PerlTidy; };

if ( $EVAL_ERROR ) {
    my $msg = 'Test::Tidy required to criticise code';
    plan( skip_all => $msg );
}

my $rcfile = File::Spec->catfile( 't', 'perltidyrc' );
run_tests(
    perltidyrc => $rcfile,
    exclude    => [ qr{\.t$}, qr{^blib/}, qr{^lib/Pg/SQL/Parser/SQL\.pm$}, qr{^lib/Pg/SQL/Parser/Lexer/Keywords\.pm$}, qr{^t/(?:07-parser|06-lexer)-data/.*\.pl$} ],
);
