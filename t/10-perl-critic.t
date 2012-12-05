#!/usr/bin/perl
use strict;
use warnings;
use File::Spec;
use Test::More;
use English qw(-no_match_vars);
use File::Find;

eval { require Test::Perl::Critic; };

if ( $EVAL_ERROR ) {
    my $msg = 'Test::Perl::Critic required to criticise code';
    plan( skip_all => $msg );
}

my $rcfile = File::Spec->catfile( 't', 'perlcriticrc' );
Test::Perl::Critic->import( -profile => $rcfile );

my @files = ();
find(
    sub {
        return unless -f;
        return unless /\.pm\z/;
        return if $File::Find::name eq 'lib/Pg/SQL/Parser/SQL.pm';
        push @files, $File::Find::name;
    },
    'lib/'
);

plan tests => scalar @files;
critic_ok( $_ ) for @files;
