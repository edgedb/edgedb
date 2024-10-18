-- test old extension version entry points

CREATE EXTENSION edb_stat_statements WITH VERSION '1.0';

SELECT pg_get_functiondef('edb_stat_statements_info'::regproc);

SELECT pg_get_functiondef('edb_stat_statements_reset'::regproc);

SELECT edb_stat_statements_reset() IS NOT NULL AS t;
\d edb_stat_statements
SELECT count(*) > 0 AS has_data FROM edb_stat_statements;

DROP EXTENSION edb_stat_statements;
