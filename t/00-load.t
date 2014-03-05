#!perl

use Test::More 0.96;
use Test::Exception;
use Test::Deep;

BEGIN {
    use_ok('ElasticSearchX::UniqueKey') || print "Bail out!";
}

diag "";
diag(
    "Testing ElasticSearchX::UniqueKey $ElasticSearchX::UniqueKey::VERSION, Perl $], $^X"
);

done_testing;
