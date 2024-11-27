-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION edb_stat_statements" to load this file. \quit

-- Register functions.
CREATE FUNCTION edb_stat_statements_reset(IN userid Oid DEFAULT 0,
    IN dbids Oid[] DEFAULT '{}',
    IN queryid bigint DEFAULT 0,
    IN minmax_only boolean DEFAULT false
)
RETURNS timestamp with time zone
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION edb_stat_queryid(IN id uuid)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION edb_stat_statements(IN showtext boolean,
    OUT userid oid,
    OUT dbid oid,
    OUT toplevel bool,
    OUT queryid bigint,
    OUT query text,
    OUT extras jsonb,
    OUT tag text,
    OUT id uuid,
    OUT stmt_type int2,
    OUT plans int8,
    OUT total_plan_time float8,
    OUT min_plan_time float8,
    OUT max_plan_time float8,
    OUT mean_plan_time float8,
    OUT stddev_plan_time float8,
    OUT calls int8,
    OUT total_exec_time float8,
    OUT min_exec_time float8,
    OUT max_exec_time float8,
    OUT mean_exec_time float8,
    OUT stddev_exec_time float8,
    OUT rows int8,
    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_dirtied int8,
    OUT shared_blks_written int8,
    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_dirtied int8,
    OUT local_blks_written int8,
    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT shared_blk_read_time float8,
    OUT shared_blk_write_time float8,
    OUT local_blk_read_time float8,
    OUT local_blk_write_time float8,
    OUT temp_blk_read_time float8,
    OUT temp_blk_write_time float8,
    OUT wal_records int8,
    OUT wal_fpi int8,
    OUT wal_bytes numeric,
    OUT jit_functions int8,
    OUT jit_generation_time float8,
    OUT jit_inlining_count int8,
    OUT jit_inlining_time float8,
    OUT jit_optimization_count int8,
    OUT jit_optimization_time float8,
    OUT jit_emission_count int8,
    OUT jit_emission_time float8,
    OUT jit_deform_count int8,
    OUT jit_deform_time float8,
    OUT parallel_workers_to_launch int8,
    OUT parallel_workers_launched int8,
    OUT stats_since timestamp with time zone,
    OUT minmax_stats_since timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION edb_stat_statements_info(
    OUT dealloc bigint,
    OUT stats_reset timestamp with time zone
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

-- Register views on the functions for ease of use.
CREATE VIEW edb_stat_statements AS
  SELECT * FROM edb_stat_statements(true);

GRANT SELECT ON edb_stat_statements TO PUBLIC;

CREATE VIEW edb_stat_statements_info AS
  SELECT * FROM edb_stat_statements_info();

GRANT SELECT ON edb_stat_statements_info TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION edb_stat_statements_reset(Oid, Oid[], bigint, boolean) FROM PUBLIC;
