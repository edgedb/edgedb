-- test old extension version entry points

CREATE EXTENSION pg_stat_statements WITH VERSION '1.0';

SELECT pg_get_functiondef('pg_stat_statements_info'::regproc);
SELECT pg_get_functiondef('pg_stat_statements_reset'::regproc);
SELECT pg_stat_statements_reset() IS NOT NULL AS t;
\d pg_stat_statements
SELECT count(*) > 0 AS has_data FROM pg_stat_statements;

DROP EXTENSION pg_stat_statements;
