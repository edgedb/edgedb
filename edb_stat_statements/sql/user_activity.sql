--
-- Track user activity and reset them
--

SET edb_stat_statements.track_utility = TRUE;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
CREATE ROLE regress_stats_user1;
CREATE ROLE regress_stats_user2;

SET ROLE regress_stats_user1;

SELECT 1 AS "ONE";
SELECT 1+1 AS "TWO";

RESET ROLE;
SET ROLE regress_stats_user2;

SELECT 1 AS "ONE";
SELECT 1+1 AS "TWO";

RESET ROLE;
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- Don't reset anything if any of the parameter is NULL
--
SELECT edb_stat_statements_reset(NULL) IS NOT NULL AS t;
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- remove query ('SELECT $1+$2 AS "TWO"') executed by regress_stats_user2
-- in the current_database
--
SELECT edb_stat_statements_reset(
	(SELECT r.oid FROM pg_roles AS r WHERE r.rolname = 'regress_stats_user2'),
	ARRAY(SELECT d.oid FROM pg_database As d where datname = current_database()),
	(SELECT s.queryid FROM edb_stat_statements AS s
				WHERE s.query = 'SELECT $1+$2 AS "TWO"' LIMIT 1))
	IS NOT NULL AS t;
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- remove query ('SELECT $1 AS "ONE"') executed by two users
--
SELECT edb_stat_statements_reset(0,'{}',s.queryid) IS NOT NULL AS t
	FROM edb_stat_statements AS s WHERE s.query = 'SELECT $1 AS "ONE"';
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- remove query of a user (regress_stats_user1)
--
SELECT edb_stat_statements_reset(r.oid) IS NOT NULL AS t
		FROM pg_roles AS r WHERE r.rolname = 'regress_stats_user1';
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- reset all
--
SELECT edb_stat_statements_reset(0,'{}',0) IS NOT NULL AS t;
SELECT query, calls, rows FROM edb_stat_statements ORDER BY query COLLATE "C";

--
-- cleanup
--
DROP ROLE regress_stats_user1;
DROP ROLE regress_stats_user2;
SELECT edb_stat_statements_reset() IS NOT NULL AS t;
