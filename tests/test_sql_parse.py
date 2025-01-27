#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from edb.pgsql import codegen, parser
from edb.testbase import lang as tb
from edb.tools import test


class TestSQLParse(tb.BaseDocTest):

    def run_test(self, *, source, spec, expected):
        def inline(text):
            lines = (line.strip() for line in text.split('\n'))
            return ' '.join((line for line in lines if len(line) > 0))

        def normalize(s):
            return s.replace("  ", " ").replace("( ", "(").replace(" )", ")")

        source = normalize(inline(source))

        can_omit_expected = False
        if expected:
            expected = normalize(inline(expected))
            can_omit_expected = source == expected
        else:
            expected = source

        ast = parser.parse(source, propagate_spans=True)
        sql_stmts = [
            codegen.generate_source(stmt, pretty=False) for stmt in ast
        ]
        sql = normalize("; ".join(sql_stmts))

        self.assertEqual(expected, sql)

        if can_omit_expected:
            raise BaseException(
                'Warning: test''s `source` is same as `expected`. '
                'You can omit `expected`.'
            )

    def test_sql_parse_select_00(self):
        """
        SELECT * FROM my_table
        """

    def test_sql_parse_select_01(self):
        """
        SELECT col1 FROM my_table WHERE
        my_attribute LIKE 'condition' AND other = 5.6 AND extra > 5
% OK %
        SELECT col1 FROM my_table WHERE
        (((my_attribute LIKE 'condition') AND
        (other = 5.6)) AND (extra > 5))
        """

    def test_sql_parse_select_02(self):
        """
        SELECT * FROM table_one JOIN table_two USING (common)
        """

    def test_sql_parse_select_03(self):
        """
        WITH fake_table AS (
            SELECT SUM(countable) AS total FROM inner_table
            GROUP BY groupable
        ) SELECT * FROM fake_table
% OK %
        WITH fake_table AS ((
            SELECT sum(countable) AS total FROM inner_table
            GROUP BY groupable
        )) SELECT * FROM fake_table
        """

    def test_sql_parse_select_04(self):
        """
        SELECT * FROM (SELECT something FROM dataset) AS other
        """

    def test_sql_parse_select_05(self):
        """
        SELECT a, CASE WHEN a=1 THEN 'one' WHEN a=2
        THEN 'two' ELSE 'other' END FROM test
% OK %
        SELECT a, (CASE WHEN (a = 1) THEN 'one' WHEN (a = 2)
        THEN 'two' ELSE 'other' END) FROM test
        """

    def test_sql_parse_select_06(self):
        """
        SELECT CASE a.value WHEN 0 THEN '1' ELSE '2' END
        FROM sometable a
% OK %
        SELECT (CASE a.value WHEN 0 THEN '1' ELSE '2' END)
        FROM sometable AS a
        """

    def test_sql_parse_select_07(self):
        """
        SELECT * FROM table_one UNION select * FROM table_two
% OK %
        (SELECT * FROM table_one) UNION (SELECT * FROM table_two)
        """

    def test_sql_parse_select_08(self):
        """
        SELECT * FROM my_table WHERE ST_Intersects(geo1, geo2)
% OK %
        SELECT * FROM my_table WHERE st_intersects(geo1, geo2)
        """

    def test_sql_parse_select_09(self):
        """
        SELECT 'accbf276-705b-11e7-b8e4-0242ac120002'::UUID
% OK %
        SELECT ('accbf276-705b-11e7-b8e4-0242ac120002')::uuid
        """

    def test_sql_parse_select_10(self):
        """
        SELECT * FROM my_table ORDER BY field DESC NULLS FIRST
        """

    def test_sql_parse_select_11(self):
        """
        SELECT * FROM my_table ORDER BY field
% OK %
        SELECT * FROM my_table ORDER BY field ASC NULLS LAST
        """

    def test_sql_parse_select_12(self):
        """
        SELECT salary, sum(salary) OVER () FROM empsalary
        """

    def test_sql_parse_select_13(self):
        """
        SELECT salary, sum(salary)
        OVER (ORDER BY salary) FROM empsalary
% OK %
        SELECT salary, sum(salary)
        OVER (ORDER BY salary ASC NULLS LAST) FROM empsalary
        """

    def test_sql_parse_select_14(self):
        """
        SELECT salary, avg(salary)
        OVER (PARTITION BY depname) FROM empsalary
        """

    def test_sql_parse_select_15(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS NULL
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS NULL)
        """

    def test_sql_parse_select_16(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS NOT NULL
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS NOT NULL)
        """

    def test_sql_parse_select_17(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS TRUE
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS TRUE)
        """

    def test_sql_parse_select_18(self):
        """
        SELECT m.name AS mname, pname FROM manufacturers m,
        LATERAL get_product_names(m.id) pname
% OK %
        SELECT m.name AS mname, pname FROM manufacturers AS m,
        LATERAL get_product_names(m.id) AS pname
        """

    def test_sql_parse_select_19(self):
        """
        SELECT * FROM unnest(ARRAY['a','b','c','d','e','f'])
% OK %
        SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])
        """

    def test_sql_parse_select_20(self):
        """
        SELECT * FROM my_table
        WHERE (a, b) in (('a', 'b'), ('c', 'd'))
% OK %
        SELECT * FROM my_table
        WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))
        """

    @test.xerror('bad FRO keyword')
    def test_sql_parse_select_21(self):
        """
        SELECT * FRO my_table
        """

    @test.xerror('missing expression after THEN')
    def test_sql_parse_select_22(self):
        """
        SELECT a, CASE WHEN a=1 THEN 'one'
        WHEN a=2 THEN ELSE 'other' END FROM test
        """

    def test_sql_parse_select_23(self):
        """
        SELECT * FROM table_one, table_two
        """

    def test_sql_parse_select_24(self):
        """
        SELECT * FROM table_one, public.table_one
        """

    def test_sql_parse_select_25(self):
        """
        WITH fake_table AS (SELECT * FROM inner_table)
        SELECT * FROM fake_table
% OK %
        WITH fake_table AS ((SELECT * FROM inner_table))
        SELECT * FROM fake_table
        """

    def test_sql_parse_select_26(self):
        """
        SELECT * FROM table_one JOIN table_two USING (common_1)
        JOIN table_three USING (common_2)
        """

    def test_sql_parse_select_27(self):
        """
        select * FROM table_one UNION select * FROM table_two
% OK %
        (SELECT * FROM table_one) UNION (SELECT * FROM table_two)
        """

    def test_sql_parse_select_28(self):
        """
        SELECT * FROM my_table WHERE (a, b) in ('a', 'b')
% OK %
        SELECT * FROM my_table WHERE ((a, b) IN ('a', 'b'))
        """

    def test_sql_parse_select_29(self):
        """
        SELECT * FROM my_table
        WHERE (a, b) in (('a', 'b'), ('c', 'd'))
% OK %
        SELECT * FROM my_table
        WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))
        """

    def test_sql_parse_select_30(self):
        """
        SELECT (SELECT * FROM table_one)
% OK %
        SELECT ((SELECT * FROM table_one))
        """

    def test_sql_parse_select_31(self):
        """
        SELECT my_func((select * from table_one))
% OK %
        SELECT my_func(((SELECT * FROM table_one)))
        """

    def test_sql_parse_select_32(self):
        """
        SELECT 1
        """

    def test_sql_parse_select_33(self):
        """
        SELECT 2
        """

    def test_sql_parse_select_34(self):
        """
        SELECT $1
        """

    def test_sql_parse_select_35(self):
        """
        SELECT 1; SELECT a FROM b
        """

    def test_sql_parse_select_36(self):
        """
        SELECT COUNT(DISTINCT id), * FROM targets
        WHERE something IS NOT NULL
        AND elsewhere::interval < now()
% OK %
        SELECT count(DISTINCT id), * FROM targets
        WHERE ((something IS NOT NULL)
        AND ((elsewhere)::pg_catalog.interval < now()))
        """

    def test_sql_parse_select_37(self):
        """
        SELECT b AS x, a AS y FROM z
        """

    def test_sql_parse_select_38(self):
        """
        WITH a AS (SELECT * FROM x WHERE x.y = $1 AND x.z = 1)
        SELECT * FROM a
% OK %
        WITH a AS ((SELECT * FROM x WHERE ((x.y = $1) AND (x.z = 1))))
        SELECT * FROM a
        """

    def test_sql_parse_select_39(self):
        """
        SELECT * FROM x WHERE y IN ($1)
% OK %
        SELECT * FROM x WHERE (y IN ($1))
        """

    def test_sql_parse_select_40(self):
        """
        SELECT * FROM x WHERE y IN ($1, $2, $3)
% OK %
        SELECT * FROM x WHERE (y IN ($1, $2, $3))
        """

    def test_sql_parse_select_41(self):
        """
        SELECT * FROM x WHERE y IN ( $1::uuid )
% OK %
        SELECT * FROM x WHERE (y IN (($1)::uuid))
        """

    def test_sql_parse_select_42(self):
        """
        SELECT * FROM x
        WHERE y IN ( $1::uuid, $2::uuid, $3::uuid )
% OK %
        SELECT * FROM x
        WHERE (y IN (($1)::uuid, ($2)::uuid, ($3)::uuid))
        """

    def test_sql_parse_select_43(self):
        """
        SELECT * FROM x AS a, y AS b
        """

    def test_sql_parse_select_44(self):
        """
        SELECT * FROM y AS a, x AS b
        """

    def test_sql_parse_select_45(self):
        """
        SELECT x AS a, y AS b FROM x
        """

    def test_sql_parse_select_46(self):
        """
        SELECT x, y FROM z
        """

    def test_sql_parse_select_47(self):
        """
        SELECT y, x FROM z
        """

    def test_sql_parse_select_48(self):
        """
        SELECT * FROM a
        """

    def test_sql_parse_select_49(self):
        """
        SELECT * FROM a AS b
        """

    def test_sql_parse_select_50(self):
        """
        -- nothing
% OK %
        """

        # TODO: is this ok? What is `(0)`?
    def test_sql_parse_select_51(self):
        """
        SELECT INTERVAL (0) $2
% OK %
        SELECT ($2)::pg_catalog.interval
        """

    def test_sql_parse_select_52(self):
        """
        SELECT INTERVAL (2) $2
% OK %
        SELECT ($2)::pg_catalog.interval
        """

    def test_sql_parse_select_53(self):
        """
        SELECT * FROM t WHERE t.a IN (1, 2) AND t.b = 3
% OK %
        SELECT * FROM t WHERE ((t.a IN (1, 2)) AND (t.b = 3))
        """

    def test_sql_parse_select_54(self):
        """
        SELECT * FROM t WHERE t.b = 3 AND t.a IN (1, 2)
% OK %
        SELECT * FROM t WHERE ((t.b = 3) AND (t.a IN (1, 2)))
        """

    def test_sql_parse_select_55(self):
        """
        SELECT * FROM t WHERE a && '[1,2]'
% OK %
        SELECT * FROM t WHERE (a && '[1,2]')
        """

    def test_sql_parse_select_56(self):
        """
        SELECT * FROM t WHERE a && '[1,2]'::int4range
% OK %
        SELECT * FROM t WHERE (a && ('[1,2]')::int4range)
        """

    def test_sql_parse_select_57(self):
        """
        SELECT * FROM t_20210301_x
        """

    def test_sql_parse_select_58(self):
        """
        SELECT (1.2 * 3.4)
        """

    def test_sql_parse_select_59(self):
        """
        SELECT TRUE; SELECT FALSE
        """

    def test_sql_parse_select_60(self):
        """
        SELECT -1; SELECT 0; SELECT 1
        """

    def test_sql_parse_select_61(self):
        """
        SELECT a[1:3], b.x
% OK %
        SELECT (a)[1:3], b.x
        """

    def test_sql_parse_insert_00(self):
        """
        INSERT INTO my_table (id, name) VALUES (1, 'some')
        """

    def test_sql_parse_insert_01(self):
        """
        INSERT INTO my_table (id, name) SELECT 1, 'some'
% OK %
        INSERT INTO my_table (id, name) ((SELECT 1, 'some'))
        """

    def test_sql_parse_insert_02(self):
        """
        INSERT INTO my_table (id) VALUES (5) RETURNING id, date
        """

    def test_sql_parse_insert_03(self):
        """
        INSERT INTO my_table (id) VALUES (5) RETURNING id, "date"
% OK %
        INSERT INTO my_table (id) VALUES (5) RETURNING id, date
        """

    def test_sql_parse_insert_04(self):
        """
        INSERT INTO my_table (id) VALUES(1); SELECT * FROM my_table
% OK %
        INSERT INTO my_table (id) VALUES (1); SELECT * FROM my_table
        """

    @test.xerror('missing VALUES or SELECT')
    def test_sql_parse_insert_05(self):
        """
        INSERT INTO my_table
        """

    def test_sql_parse_insert_06(self):
        """
        INSERT INTO table_one (id, name) SELECT * from table_two
% OK %
        INSERT INTO table_one (id, name) ((SELECT * FROM table_two))
        """

    def test_sql_parse_insert_07(self):
        """
        WITH fake as (SELECT * FROM inner_table)
        INSERT INTO dataset SELECT * FROM fake
% OK %
        WITH fake AS ((SELECT * FROM inner_table))
        INSERT INTO dataset ((SELECT * FROM fake))
        """

    def test_sql_parse_insert_08(self):
        """
        INSERT INTO test (a, b) VALUES
        (ARRAY[$1, $1, $2, $3], $4::timestamptz),
        (ARRAY[$1, $1, $2, $3], $4::timestamptz),
        ($5, $6::timestamptz)
% OK %
        INSERT INTO test (a, b) VALUES
        (ARRAY[$1, $1, $2, $3], ($4)::timestamptz),
        (ARRAY[$1, $1, $2, $3], ($4)::timestamptz),
        ($5, ($6)::timestamptz)
        """

    def test_sql_parse_insert_09(self):
        """
        INSERT INTO films (code, title, did) VALUES
        ('UA502', 'Bananas', 105), ('T_601', 'Yojimbo', DEFAULT)
        """

    def test_sql_parse_insert_10(self):
        """
        INSERT INTO films (code, title, did) VALUES ($1, $2, $3)
        """

    def test_sql_parse_insert_11(self):
        """
        INSERT INTO films DEFAULT VALUES
        ON CONFLICT DO UPDATE
        SET (a, b) = ('a', 'b'), c = 'c', (d, e) = ('d', 'e')
        """

    def test_sql_parse_insert_12(self):
        """
        INSERT INTO foo DEFAULT VALUES
        RETURNING a[1:3] AS a, b.x AS b
% OK %
        INSERT INTO foo DEFAULT VALUES
        RETURNING (a)[1:3] AS a, b.x AS b
        """

    def test_sql_parse_update_00(self):
        """
        UPDATE my_table SET the_value = DEFAULT
        """

    def test_sql_parse_update_01(self):
        """
        UPDATE tictactoe SET board[1:3][1:3] = '{{,,},{,,},{,,}}'
        WHERE game = 1
% OK %
        UPDATE tictactoe SET board[1:3][1:3] = '{{,,},{,,},{,,}}'
        WHERE (game = 1)
        """

    def test_sql_parse_update_02(self):
        """
        UPDATE accounts SET
        (contact_first_name, contact_last_name) =
        (SELECT first_name, last_name
        FROM salesmen WHERE salesmen.id = accounts.sales_id)
% OK %
        UPDATE accounts SET
        (contact_first_name, contact_last_name) =
        ((SELECT first_name, last_name
        FROM salesmen WHERE (salesmen.id = accounts.sales_id)))
        """

    def test_sql_parse_update_03(self):
        """
        UPDATE my_table SET id = 5; DELETE FROM my_table
        """

    def test_sql_parse_update_04(self):
        """
        UPDATE dataset SET a = 5
        WHERE id IN (SELECT * from table_one)
        OR age IN (select * from table_two)
% OK %
        UPDATE dataset SET a = 5
        WHERE (id = ANY ((SELECT * FROM table_one))
        OR age = ANY ((SELECT * FROM table_two)))
        """

    def test_sql_parse_update_05(self):
        """
        UPDATE dataset SET a = 5 FROM extra WHERE b = c
% OK %
        UPDATE dataset SET a = 5 FROM extra WHERE (b = c)
        """

    def test_sql_parse_update_06(self):
        """
        UPDATE users SET one_thing = $1, second_thing = $2
        WHERE users.id = $1
% OK %
        UPDATE users SET one_thing = $1, second_thing = $2
        WHERE (users.id = $1)
        """

    def test_sql_parse_update_07(self):
        """
        UPDATE users SET something_else = $1 WHERE users.id = $1
% OK %
        UPDATE users SET something_else = $1 WHERE (users.id = $1)
        """

    def test_sql_parse_update_08(self):
        """
        UPDATE users SET something_else =
        (SELECT a FROM x WHERE uid = users.id LIMIT 1)
        WHERE users.id = $1
% OK %
        UPDATE users SET something_else =
        ((SELECT a FROM x WHERE (uid = users.id) LIMIT 1))
        WHERE (users.id = $1)
        """

    def test_sql_parse_update_09(self):
        """
        UPDATE x SET a = 1, b = 2, c = 3
        """

    def test_sql_parse_update_10(self):
        """
        UPDATE x SET z = now()
        """

    def test_sql_parse_update_11(self):
        """
        UPDATE x SET (a, b) = ('a', 'b'), c = 'c', (d, e) = ('d', 'e')
        """

    @test.xerror('unsupported')
    def test_sql_parse_update_12(self):
        """
        UPDATE tictactoe SET
        (board[1:3][1:3], finished) = ('{{,,},{,,},{,,}}', FALSE)
        """

    def test_sql_parse_update_13(self):
        """
        UPDATE tictactoe SET a = a RETURNING *
        """

    def test_sql_parse_delete(self):
        """
        DELETE FROM dataset USING table_one
        WHERE x = y OR x IN (SELECT * from table_two)
% OK %
        DELETE FROM dataset USING table_one
        WHERE ((x = y) OR x = ANY ((SELECT * FROM table_two)))
        """

    def test_sql_parse_transaction_00(self):
        """
        BEGIN
        """

    def test_sql_parse_transaction_01(self):
        """
        BEGIN TRANSACTION
% OK %
        BEGIN
        """

    def test_sql_parse_transaction_02(self):
        """
        BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE
        """

    def test_sql_parse_transaction_03(self):
        """
        START TRANSACTION
        """

    def test_sql_parse_transaction_04(self):
        """
        START TRANSACTION ISOLATION LEVEL REPEATABLE READ
        """

    def test_sql_parse_transaction_05(self):
        """
        START TRANSACTION ISOLATION LEVEL READ COMMITTED
        """

    def test_sql_parse_transaction_06(self):
        """
        START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
        """

    def test_sql_parse_transaction_07(self):
        """
        START TRANSACTION READ WRITE
        """

    def test_sql_parse_transaction_08(self):
        """
        START TRANSACTION READ ONLY
        """

    def test_sql_parse_transaction_09(self):
        """
        START TRANSACTION NOT DEFERRABLE
        """

    def test_sql_parse_transaction_10(self):
        """
        START TRANSACTION DEFERRABLE
        """

    def test_sql_parse_transaction_11(self):
        """
        COMMIT
        """

    def test_sql_parse_transaction_12(self):
        """
        COMMIT TRANSACTION
% OK %
        COMMIT
        """

    def test_sql_parse_transaction_13(self):
        """
        COMMIT WORK
% OK %
        COMMIT
        """

    def test_sql_parse_transaction_14(self):
        """
        COMMIT AND NO CHAIN
% OK %
        COMMIT
        """

    def test_sql_parse_transaction_15(self):
        """
        COMMIT AND CHAIN
        """

    def test_sql_parse_transaction_16(self):
        """
        ROLLBACK
        """

    def test_sql_parse_transaction_17(self):
        """
        ROLLBACK TRANSACTION
% OK %
        ROLLBACK
        """

    def test_sql_parse_transaction_18(self):
        """
        ROLLBACK WORK
% OK %
        ROLLBACK
        """

    def test_sql_parse_transaction_19(self):
        """
        ROLLBACK AND NO CHAIN
% OK %
        ROLLBACK
        """

    def test_sql_parse_transaction_20(self):
        """
        ROLLBACK AND CHAIN
        """

    def test_sql_parse_transaction_21(self):
        """
        SAVEPOINT some_id
        """

    def test_sql_parse_transaction_22(self):
        """
        RELEASE some_id
        """

    def test_sql_parse_transaction_23(self):
        """
        ROLLBACK TO SAVEPOINT savepoint_name
        """

    def test_sql_parse_transaction_24(self):
        """
        PREPARE TRANSACTION 'transaction_id'
        """

    def test_sql_parse_transaction_25(self):
        """
        COMMIT PREPARED 'transaction_id'
        """

    def test_sql_parse_transaction_26(self):
        """
        ROLLBACK PREPARED 'transaction_id'
        """

    def test_sql_parse_transaction_27(self):
        """
        SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE
        """

    def test_sql_parse_transaction_28(self):
        """
        SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE
        """

    def test_sql_parse_query_00(self):
        """
        SELECT * FROM
        (VALUES (1, 'one'), (2, 'two')) AS t(num, letter)
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_01(self):
        """
        SELECT * FROM my_table ORDER BY field ASC NULLS LAST USING @>
        """

    def test_sql_parse_query_02(self):
        """
        SELECT m.* FROM mytable AS m FOR UPDATE
        """

    @test.xfail("unsupported")
    def test_sql_parse_query_03(self):
        """
        SELECT m.* FROM mytable m FOR SHARE of m nowait
        """

    def test_sql_parse_query_04(self):
        """
        SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_06(self):
        """
        SELECT ?
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_07(self):
        """
        SELECT * FROM x WHERE y = ?
        """

    def test_sql_parse_query_08(self):
        """
        SELECT * FROM x WHERE y = ANY ($1)
        """

    def test_sql_parse_query_09(self):
        """
        PREPARE fooplan (int, text, bool, numeric) AS (SELECT $1, $2, $3, $4)
% OK %
        PREPARE fooplan(pg_catalog.int4, text, bool, pg_catalog.numeric) AS (
            SELECT $1, $2, $3, $4
        )
        """

    def test_sql_parse_query_10(self):
        """
        EXECUTE fooplan(1, 'Hunter Valley', 't', 200.00)
        """

    def test_sql_parse_query_11(self):
        """
        DEALLOCATE a123
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_12(self):
        """
        DEALLOCATE ALL
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_13(self):
        """
        EXPLAIN ANALYZE SELECT a
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_14(self):
        """
        VACUUM FULL my_table
        """

    def test_sql_parse_query_15(self):
        """
        SELECT (pg_column_size(ROW()))::text
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_19(self):
        """
        DECLARE cursor_123 CURSOR FOR
        SELECT * FROM test WHERE id = 123
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_20(self):
        """
        FETCH 1000 FROM cursor_123
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_21(self):
        """
        CLOSE cursor_123
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_22(self):
        """
        CREATE VIEW view_a (a, b) AS WITH RECURSIVE view_a (a, b) AS
        (SELECT * FROM a(1)) SELECT "a", "b" FROM "view_a"
        """

    @test.xerror("unsupported")
    def test_sql_parse_query_23(self):
        """
        CREATE FOREIGN TABLE ft1 () SERVER no_server
        """

    def test_sql_parse_query_24(self):
        """
        CREATE TEMPORARY TABLE my_temp_table (
            test_id integer NOT NULL
        ) ON COMMIT DROP
% OK %
        CREATE TEMPORARY TABLE my_temp_table (
            test_id pg_catalog.int4 NOT NULL
        ) ON COMMIT DROP
        """

    def test_sql_parse_query_25(self):
        """
        CREATE TEMPORARY TABLE my_temp_table AS (SELECT 1) WITH NO DATA
        """

    def test_sql_parse_query_26(self):
        """
        CREATE TABLE types (
        a float(2), b float(49),
        c NUMERIC(2, 3), d character(4), e char(5),
        f varchar(6), g character varying(7))
% OK %
        CREATE TABLE types (
            a pg_catalog.float4,
            b pg_catalog.float8,
            c pg_catalog.numeric,
            d pg_catalog.bpchar,
            e pg_catalog.bpchar,
            f pg_catalog.varchar,
            g pg_catalog.varchar
        )
        """

    def test_sql_parse_query_27(self):
        """
        SET LOCAL search_path TO 'my_schema', 'public'
        """

    def test_sql_parse_query_28(self):
        """
        SET SESSION datestyle TO postgres, dmy
% OK %
        SET datestyle TO 'postgres', 'dmy'
        """
        # SESSION is the default
        # args are strings actually

    def test_sql_parse_query_29(self):
        """
        SHOW search_path
        """

    def test_sql_parse_query_30(self):
        """
        SHOW TIME ZONE
% OK %
        SHOW timezone
        """
        # `SHOW TIME ZONE`` is a pg alias for `SHOW timezone`

    def test_sql_parse_query_31(self):
        """
        SELECT ('asxasx')[2][3:4]
        """

    def test_sql_parse_query_32(self):
        """
        SELECT ((blah(4))[0])[2][3:4][2][5:5]
        """

    def test_sql_parse_query_33(self):
        """
        SELECT a <= ANY (ARRAY[1, 2, 3])
        """

    def test_sql_parse_query_34(self):
        """
        SELECT a <= ALL (ARRAY[1, 2, 3])
        """

    def test_sql_parse_query_35(self):
        """
        SELECT a <= some(array[1, 2, 3])
% OK %
        SELECT a <= ANY (ARRAY[1, 2, 3])
        """

    def test_sql_parse_query_36(self):
        """
        SELECT a NOT IN (1, 2, 3)
% OK %
        SELECT (a NOT IN (1, 2, 3))
        """

    def test_sql_parse_query_37(self):
        """
        SELECT a NOT LIKE 'a%'
% OK %
        SELECT (a NOT LIKE 'a%')
        """

    def test_sql_parse_query_38(self):
        """
        SELECT a NOT ILIKE 'a%'
% OK %
        SELECT (a NOT ILIKE 'a%')
        """

    def test_sql_parse_query_39(self):
        """
        SELECT a ILIKE 'a%'
% OK %
        SELECT (a ILIKE 'a%')
        """

    def test_sql_parse_query_40(self):
        """
        WITH RECURSIVE t(n) AS (((
            VALUES (1)
        ) UNION ALL (
            SELECT (n + 1) FROM t WHERE (n < 100)
        )))
        SELECT sum(n) FROM t
        """

    def test_sql_parse_query_41(self):
        """
        SELECT 1 FROM t WHERE ia.attnum > 0 AND NOT ia.attisdropped
% OK %
        SELECT 1 FROM t WHERE ((ia.attnum > 0) AND (NOT ia.attisdropped))
        """

    def test_sql_parse_query_42(self):
        """
        SELECT ($1)::oid[]
        """

    def test_sql_parse_query_43(self):
        """
        SELECT ($1)::oid[5]
        """

    def test_sql_parse_query_44(self):
        """
        SELECT ($1)::oid[5][6]
        """

    def test_sql_parse_query_45(self):
        """
        SET LOCAL search_path TO DEFAULT
        """

    def test_sql_parse_query_46(self):
        """
        SET SESSION search_path TO DEFAULT
% OK %
        SET search_path TO DEFAULT
        """

    def test_sql_parse_query_47(self):
        """
        RESET search_path
% OK %
        SET search_path TO DEFAULT
        """

    def test_sql_parse_query_48(self):
        """
        RESET ALL
        """

    def test_sql_parse_query_49(self):
        """
        SELECT nullif(a, 3) FROM b
        """

    def test_sql_parse_query_50(self):
        """
        SELECT 'a'::char, 'a'::"char"
% OK %
        SELECT ('a')::pg_catalog.bpchar, ('a')::pg_catalog.char
        """

    def test_sql_parse_query_51(self):
        """
        SELECT ARRAY ((SELECT c FROM a)) FROM b
        """

    def test_sql_parse_query_52(self):
        """
        SELECT * FROM b WHERE (c ILIKE 'blah%' COLLATE collation_name)
        """

    def test_sql_parse_query_53(self):
        """
        SELECT GREATEST(x, y, 0), LEAST(x, y, 100) FROM b
        """

    def test_sql_parse_query_54(self):
        """
        SELECT (x IS DISTINCT FROM y) FROM b
        """

    def test_sql_parse_query_55(self):
        """
        SELECT (x IS NOT DISTINCT FROM y) FROM b
        """

    def test_sql_parse_lock_01(self):
        '''
        LOCK TABLE films IN ACCESS SHARE MODE
        '''

    def test_sql_parse_lock_02(self):
        '''
        LOCK TABLE films IN ACCESS SHARE MODE NOWAIT
        '''

    def test_sql_parse_lock_03(self):
        '''
        LOCK TABLE ONLY (films) IN ACCESS SHARE MODE
        '''

    def test_sql_parse_lock_04(self):
        '''
        LOCK TABLE ONLY (films) IN ACCESS SHARE MODE NOWAIT
        '''

    def test_sql_parse_lock_05(self):
        '''
        LOCK TABLE films IN ROW SHARE MODE
        '''

    def test_sql_parse_lock_06(self):
        '''
        LOCK TABLE films IN ROW EXCLUSIVE MODE
        '''

    def test_sql_parse_lock_07(self):
        '''
        LOCK TABLE films IN SHARE UPDATE EXCLUSIVE MODE
        '''

    def test_sql_parse_lock_08(self):
        '''
        LOCK TABLE films IN SHARE MODE
        '''

    def test_sql_parse_lock_09(self):
        '''
        LOCK TABLE films IN SHARE ROW EXCLUSIVE MODE
        '''

    def test_sql_parse_lock_10(self):
        '''
        LOCK TABLE films IN EXCLUSIVE MODE
        '''

    def test_sql_parse_lock_11(self):
        '''
        LOCK TABLE films IN ACCESS EXCLUSIVE MODE
        '''

    # The transaction_* settings are always on transaction level

    def test_sql_parse_transaction_29(self):
        """
        SET SESSION transaction_isolation = serializable
% OK %
        SET LOCAL transaction_isolation TO 'serializable'
        """

    def test_sql_parse_transaction_30(self):
        """
        RESET transaction_deferrable
% OK %
        SET LOCAL transaction_deferrable TO DEFAULT
        """

    def test_sql_parse_transaction_31(self):
        """
        SET transaction_read_only TO DEFAULT
% OK %
        SET LOCAL transaction_read_only TO DEFAULT
        """

    def test_sql_parse_copy_01(self):
        """
        COPY "Movie" TO STDOUT (
            FORMAT CSV,
            FREEZE,
            DELIMITER '|',
            NULL 'this is a null',
            HEADER FALSE,
            QUOTE '''',
            ESCAPE 'e',
            FORCE_QUOTE (title, year_release),
            FORCE_NOT_NULL (title),
            FORCE_NULL (year_release),
            ENCODING 'UTF-8'
        )
        """

    def test_sql_parse_copy_02(self):
        """
        COPY ((SELECT * FROM "Movie")) TO STDOUT
        """

    def test_sql_parse_copy_03(self):
        """
        COPY "Movie" (title, release_year) FROM STDIN WHERE (id > 100)
        """

    def test_sql_parse_copy_04(self):
        """
        COPY country TO STDOUT (DELIMITER '|')
        """

    def test_sql_parse_copy_05(self):
        """
        COPY country FROM '/usr1/proj/bray/sql/country_data'
        """

    def test_sql_parse_copy_06(self):
        """
        COPY country TO PROGRAM 'gzip > /usr1/proj/bray/sql/country_data.gz'
        """

    def test_sql_parse_table(self):
        """
        TABLE hello_world
% OK %
        SELECT * FROM hello_world
        """

    def test_sql_parse_select_locking_00(self):
        """
        SELECT id FROM a FOR UPDATE
        """

    def test_sql_parse_select_locking_01(self):
        """
        SELECT id FROM a FOR NO KEY UPDATE
        """

    def test_sql_parse_select_locking_02(self):
        """
        SELECT id FROM a FOR SHARE
        """

    def test_sql_parse_select_locking_03(self):
        """
        SELECT id FROM a FOR KEY SHARE
        """

    def test_sql_parse_select_locking_04(self):
        """
        SELECT id FROM a FOR UPDATE NOWAIT
        """

    def test_sql_parse_select_locking_05(self):
        """
        SELECT id FROM a FOR UPDATE SKIP LOCKED
        """

    def test_sql_parse_select_locking_06(self):
        """
        SELECT id FROM a FOR UPDATE OF b
        """
