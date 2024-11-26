# Copyright (c) 2023-2024, PostgreSQL Global Development Group

# Tests for checking that edb_stat_statements contents are preserved
# across restarts.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'edb_stat_statements'");
$node->append_conf('postgresql.conf',
	"edb_stat_statements.track_unrecognized = true");
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION edb_stat_statements');

$node->safe_psql('postgres', 'CREATE TABLE t1 (a int)');
$node->safe_psql('postgres', 'SELECT a FROM t1');

is( $node->safe_psql(
		'postgres',
		"SELECT query FROM edb_stat_statements WHERE query NOT LIKE '%edb_stat_statements%' ORDER BY query"
	),
	"CREATE TABLE t1 (a int)\nSELECT a FROM t1",
	'edb_stat_statements populated');

$node->restart;

is( $node->safe_psql(
		'postgres',
		"SELECT query FROM edb_stat_statements WHERE query NOT LIKE '%edb_stat_statements%' ORDER BY query"
	),
	"CREATE TABLE t1 (a int)\nSELECT a FROM t1",
	'edb_stat_statements data kept across restart');

$node->append_conf('postgresql.conf', "edb_stat_statements.save = false");
$node->reload;

$node->restart;

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM edb_stat_statements WHERE query NOT LIKE '%edb_stat_statements%'"
	),
	'0',
	'edb_stat_statements data not kept across restart with .save=false');

$node->stop;

done_testing();
