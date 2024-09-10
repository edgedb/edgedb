-- Via http://web.archive.org/web/20240216072136/https://www.highgo.ca/2020/01/10/how-to-create-test-and-debug-an-extension-written-in-c-for-postgresql/

--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION get_sum" to load this file. \quit

CREATE OR REPLACE FUNCTION get_sum(int, int) RETURNS int
AS '$libdir/get_sum'
LANGUAGE C IMMUTABLE STRICT;
