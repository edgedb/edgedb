SELECT * FROM my_table
SELECT col1 FROM my_table WHERE my_attribute LIKE 'condition' AND other = 5.6 AND extra > 5
SELECT * FROM table_one JOIN table_two USING (common)
WITH fake_table AS (SELECT SUM(countable) AS total FROM inner_table GROUP BY groupable) SELECT * FROM fake_table
SELECT * FROM (SELECT something FROM dataset) AS other
SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t (num, letter)
SELECT a, CASE WHEN a=1 THEN 'one' WHEN a=2 THEN 'two' ELSE 'other' END FROM test
SELECT CASE a.value WHEN 0 THEN '1' ELSE '2' END FROM sometable a
SELECT * FROM table_one UNION select * FROM table_two
SELECT * FROM my_table WHERE ST_Intersects(geo1, geo2)
SELECT 'accbf276-705b-11e7-b8e4-0242ac120002'::UUID
SELECT * FROM my_table ORDER BY field DESC NULLS FIRST
SELECT * FROM my_table ORDER BY field USING @>
SELECT salary, sum(salary) OVER () FROM empsalary
SELECT salary, sum(salary) OVER (ORDER BY salary) FROM empsalary
SELECT salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary
SELECT m.* FROM mytable m FOR UPDATE
SELECT m.* FROM mytable m FOR SHARE of m nowait
SELECT m.* FROM mytable m WHERE m.foo IS NULL
SELECT m.* FROM mytable m WHERE m.foo IS NOT NULL
SELECT m.* FROM mytable m WHERE m.foo IS TRUE
SELECT m.name AS mname, pname FROM manufacturers m, LATERAL get_product_names(m.id) pname
SELECT * FROM unnest(ARRAY['a','b','c','d','e','f']) WITH ORDINALITY
SELECT * FROM my_table WHERE (a, b) in (('a', 'b'), ('c', 'd'))
INSERT INTO my_table(id, name) VALUES(1, 'some')
INSERT INTO my_table(id, name) SELECT 1, 'some'
INSERT INTO my_table(id) VALUES (5) RETURNING id, "date"
UPDATE my_table SET the_value = DEFAULT
UPDATE tictactoe SET board[1:3][1:3] = '{{,,},{,,},{,,}}' WHERE game = 1
UPDATE accounts SET (contact_first_name, contact_last_name) = (SELECT first_name, last_name FROM salesmen WHERE salesmen.id = accounts.sales_id)
INSERT INTO my_table(id) VALUES(1); SELECT * FROM my_table
UPDATE my_table SET id = 5; DELETE FROM my_table
SELECT * FRO my_table
INSERT INTO my_table
SELECT a, CASE WHEN a=1 THEN 'one' WHEN a=2 THEN  ELSE 'other' END FROM test
SELECT * FROM table_one, table_two
SELECT * FROM table_one, public.table_one
WITH fake_table AS (SELECT * FROM inner_table) SELECT * FROM fake_table
UPDATE dataset SET a = 5 WHERE id IN (SELECT * from table_one) OR age IN (select * from table_two)
UPDATE dataset SET a = 5 FROM extra WHERE b = c
SELECT * FROM table_one JOIN table_two USING (common_1) JOIN table_three USING (common_2)
INSERT INTO table_one(id, name) SELECT * from table_two
WITH fake as (SELECT * FROM inner_table) INSERT INTO dataset SELECT * FROM fake
DELETE FROM dataset USING table_one WHERE x = y OR x IN (SELECT * from table_two)
select * FROM table_one UNION select * FROM table_two
SELECT * FROM my_table WHERE (a, b) in ('a', 'b')
SELECT * FROM my_table WHERE (a, b) in (('a', 'b'), ('c', 'd'))
SELECT (SELECT * FROM table_one)
SELECT my_func((select * from table_one))
SELECT 1
SELECT 2
SELECT ?
SELECT $1
SELECT 1; SELECT a FROM b
SELECT COUNT(DISTINCT id), * FROM targets WHERE something IS NOT NULL AND elsewhere::interval < now()
INSERT INTO test (a, b) VALUES (?, ?)
INSERT INTO test (b, a) VALUES (?, ?)
INSERT INTO test (a, b) VALUES (ARRAY[?, ?, ?, ?], ?::timestamptz), (ARRAY[?, ?, ?, ?], ?::timestamptz), (?, ?::timestamptz)
SELECT b AS x, a AS y FROM z
SELECT * FROM x WHERE y = ?
SELECT * FROM x WHERE y = ANY ($1)
SELECT * FROM x WHERE y IN (?)
SELECT * FROM x WHERE y IN (?, ?, ?)
SELECT * FROM x WHERE y IN ( ?::uuid )
SELECT * FROM x WHERE y IN ( ?::uuid, ?::uuid, ?::uuid )
PREPARE a123 AS SELECT a
EXECUTE a123
DEALLOCATE a123
DEALLOCATE ALL
EXPLAIN ANALYZE SELECT a
WITH a AS (SELECT * FROM x WHERE x.y = ? AND x.z = 1) SELECT * FROM a
CREATE TABLE types (a float(2), b float(49), c NUMERIC(2, 3), d character(4), e char(5), f varchar(6), g character varying(7))
CREATE VIEW view_a (a, b) AS WITH RECURSIVE view_a (a, b) AS (SELECT * FROM a(1)) SELECT "a", "b" FROM "view_a"
VACUUM FULL my_table
SELECT * FROM x AS a, y AS b
SELECT * FROM y AS a, x AS b
SELECT x AS a, y AS b FROM x
SELECT y AS a, x AS b FROM x
SELECT x, y FROM z
SELECT y, x FROM z
INSERT INTO films (code, title, did) VALUES ('UA502', 'Bananas', 105), ('T_601', 'Yojimbo', DEFAULT)
INSERT INTO films (code, title, did) VALUES (?, ?, ?)
SELECT * FROM a
SELECT * FROM a AS b
UPDATE users SET one_thing = $1, second_thing = $2 WHERE users.id = ?
UPDATE users SET something_else = $1 WHERE users.id = ?
UPDATE users SET something_else = (SELECT a FROM x WHERE uid = users.id LIMIT 1) WHERE users.id = ?
SAVEPOINT some_id
RELEASE some_id
PREPARE TRANSACTION 'some_id'
START TRANSACTION READ WRITE
DECLARE cursor_123 CURSOR FOR SELECT * FROM test WHERE id = 123
FETCH 1000 FROM cursor_123
CLOSE cursor_123
-- nothing
CREATE FOREIGN TABLE ft1 () SERVER no_server
UPDATE x SET a = 1, b = 2, c = 3
UPDATE x SET z = now()
CREATE TEMPORARY TABLE my_temp_table (test_id integer NOT NULL) ON COMMIT DROP
CREATE TEMPORARY TABLE my_temp_table AS SELECT 1
SELECT INTERVAL (0) $2
SELECT INTERVAL (2) $2
SELECT * FROM t WHERE t.a IN (1, 2) AND t.b = 3
SELECT * FROM t WHERE t.b = 3 AND t.a IN (1, 2)
SELECT * FROM t WHERE a && '[1,2]'
SELECT * FROM t WHERE a && '[1,2]'::int4range
SELECT * FROM t_20210301_x
SELECT * FROM t_20210302_x
SELECT * FROM t_20210302_y
SELECT * FROM t_1
SELECT * FROM t_2
