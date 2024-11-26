--
-- Statement level tracking
--

SET edb_stat_statements.track_utility = TRUE;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;

-- DO block - top-level tracking.
CREATE TABLE stats_track_tab (x int);
SET edb_stat_statements.track = 'dev';
DELETE FROM stats_track_tab;
DO $$
BEGIN
  DELETE FROM stats_track_tab;
END;
$$ LANGUAGE plpgsql;
SELECT toplevel, calls, query FROM edb_stat_statements
  WHERE query LIKE '%DELETE%' ORDER BY query COLLATE "C", toplevel;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;

-- DO block - all-level tracking.
SET edb_stat_statements.track = 'dev-nested';
DELETE FROM stats_track_tab;
DO $$
BEGIN
  DELETE FROM stats_track_tab;
END; $$;
DO LANGUAGE plpgsql $$
BEGIN
  -- this is a SELECT
  PERFORM 'hello world'::TEXT;
END; $$;
SELECT toplevel, calls, query FROM edb_stat_statements
  ORDER BY query COLLATE "C", toplevel;

-- Procedure with multiple utility statements.
CREATE OR REPLACE PROCEDURE proc_with_utility_stmt()
LANGUAGE SQL
AS $$
  SHOW edb_stat_statements.track;
  show edb_stat_statements.track;
  SHOW edb_stat_statements.track_utility;
$$;
SET edb_stat_statements.track_utility = TRUE;
-- all-level tracking.
SET edb_stat_statements.track = 'dev-nested';
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
CALL proc_with_utility_stmt();
SELECT toplevel, calls, query FROM edb_stat_statements
  ORDER BY query COLLATE "C", toplevel;
-- top-level tracking.
SET edb_stat_statements.track = 'dev';
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
CALL proc_with_utility_stmt();
SELECT toplevel, calls, query FROM edb_stat_statements
  ORDER BY query COLLATE "C", toplevel;

-- DO block - top-level tracking without utility.
SET edb_stat_statements.track = 'dev';
SET edb_stat_statements.track_utility = FALSE;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
DELETE FROM stats_track_tab;
DO $$
BEGIN
  DELETE FROM stats_track_tab;
END; $$;
DO LANGUAGE plpgsql $$
BEGIN
  -- this is a SELECT
  PERFORM 'hello world'::TEXT;
END; $$;
SELECT toplevel, calls, query FROM edb_stat_statements
  ORDER BY query COLLATE "C", toplevel;

-- DO block - all-level tracking without utility.
SET edb_stat_statements.track = 'dev-nested';
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
DELETE FROM stats_track_tab;
DO $$
BEGIN
  DELETE FROM stats_track_tab;
END; $$;
DO LANGUAGE plpgsql $$
BEGIN
  -- this is a SELECT
  PERFORM 'hello world'::TEXT;
END; $$;
SELECT toplevel, calls, query FROM edb_stat_statements
  ORDER BY query COLLATE "C", toplevel;

-- PL/pgSQL function - top-level tracking.
SET edb_stat_statements.track = 'dev';
SET edb_stat_statements.track_utility = FALSE;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
CREATE FUNCTION PLUS_TWO(i INTEGER) RETURNS INTEGER AS $$
DECLARE
  r INTEGER;
BEGIN
  SELECT (i + 1 + 1.0)::INTEGER INTO r;
  RETURN r;
END; $$ LANGUAGE plpgsql;

SELECT PLUS_TWO(3);
SELECT PLUS_TWO(7);

-- SQL function --- use LIMIT to keep it from being inlined
CREATE FUNCTION PLUS_ONE(i INTEGER) RETURNS INTEGER AS
$$ SELECT (i + 1.0)::INTEGER LIMIT 1 $$ LANGUAGE SQL;

SELECT PLUS_ONE(8);
SELECT PLUS_ONE(10);

SELECT calls, rows, query FROM edb_stat_statements ORDER BY query COLLATE "C";

-- immutable SQL function --- can be executed at plan time
CREATE FUNCTION PLUS_THREE(i INTEGER) RETURNS INTEGER AS
$$ SELECT i + 3 LIMIT 1 $$ IMMUTABLE LANGUAGE SQL;

SELECT PLUS_THREE(8);
SELECT PLUS_THREE(10);

SELECT toplevel, calls, rows, query FROM edb_stat_statements ORDER BY query COLLATE "C";

-- PL/pgSQL function - all-level tracking.
SET edb_stat_statements.track = 'dev-nested';
SELECT edb_stat_statements_reset() IS NOT NULL AS t;

-- we drop and recreate the functions to avoid any caching funnies
DROP FUNCTION PLUS_ONE(INTEGER);
DROP FUNCTION PLUS_TWO(INTEGER);
DROP FUNCTION PLUS_THREE(INTEGER);

-- PL/pgSQL function
CREATE FUNCTION PLUS_TWO(i INTEGER) RETURNS INTEGER AS $$
DECLARE
  r INTEGER;
BEGIN
  SELECT (i + 1 + 1.0)::INTEGER INTO r;
  RETURN r;
END; $$ LANGUAGE plpgsql;

SELECT PLUS_TWO(-1);
SELECT PLUS_TWO(2);

-- SQL function --- use LIMIT to keep it from being inlined
CREATE FUNCTION PLUS_ONE(i INTEGER) RETURNS INTEGER AS
$$ SELECT (i + 1.0)::INTEGER LIMIT 1 $$ LANGUAGE SQL;

SELECT PLUS_ONE(3);
SELECT PLUS_ONE(1);

SELECT calls, rows, query FROM edb_stat_statements ORDER BY query COLLATE "C";

-- immutable SQL function --- can be executed at plan time
CREATE FUNCTION PLUS_THREE(i INTEGER) RETURNS INTEGER AS
$$ SELECT i + 3 LIMIT 1 $$ IMMUTABLE LANGUAGE SQL;

SELECT PLUS_THREE(8);
SELECT PLUS_THREE(10);

SELECT toplevel, calls, rows, query FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- edb_stat_statements.track = none
--
SET edb_stat_statements.track = 'none';
SELECT edb_stat_statements_reset() IS NOT NULL AS t;

SELECT 1 AS "one";
SELECT 1 + 1 AS "two";

SELECT calls, rows, query FROM edb_stat_statements ORDER BY query COLLATE "C";
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
