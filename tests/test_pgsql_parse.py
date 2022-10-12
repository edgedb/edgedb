#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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


import textwrap
import unittest

from edb.testbase import lang as tb
from edb.tools import test

from edb.pgsql import parser
from edb.pgsql import codegen


def parse_and_gen(sql: str) -> str:
    ast = parser.parse(sql)
    sql_stmts = [codegen.generate_source(stmt, pretty=False) for stmt in ast]
    sql = "; ".join(sql_stmts)
    return sql.replace("  ", " ").replace("( (", "((").replace(") )", "))")
class TestEdgeQLSelect(tb.BaseDocTest):

    def run_test(self, *, source, spec, expected):
        def inline(text):
            lines = (l.strip() for l in text.split('\n'))
            return ' '.join((l for l in lines if len(l) > 0))

        def normalize(s):
            return s.replace("  ", " ").replace("( ", "(").replace(" )", ")")

        source = normalize(inline(source))

        can_omit_expected = False
        if expected:
            expected = normalize(inline(expected))
            can_omit_expected = source == expected
        else:
            expected = source

        ast = parser.parse(source)
        sql_stmts = [codegen.generate_source(stmt, pretty=False) for stmt in ast]
        sql = normalize("; ".join(sql_stmts))

        self.assertEqual(expected, sql)

        if can_omit_expected:
            raise BaseException(
                'Warning: test''s `source` is same as `expected`. '
                'You can omit `expected`.'
            )

    def test_pgsql_parse_select_00(self):
        """
        SELECT * FROM my_table
        """

    def test_pgsql_parse_select_01(self):
        """
        SELECT col1 FROM my_table WHERE
        my_attribute LIKE 'condition' AND other = 5.6 AND extra > 5
% OK %
        SELECT col1 FROM my_table WHERE
        (((my_attribute LIKE 'condition') AND
        (other = 5.6)) AND (extra > 5))
        """

    def test_pgsql_parse_select_02(self):
        """
        SELECT * FROM table_one JOIN table_two USING (common)
        """

    def test_pgsql_parse_select_03(self):
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

    def test_pgsql_parse_select_04(self):
        """
        SELECT * FROM (SELECT something FROM dataset) AS other
        """

    def test_pgsql_parse_select_05(self):
        """
        SELECT a, CASE WHEN a=1 THEN 'one' WHEN a=2
        THEN 'two' ELSE 'other' END FROM test
% OK %
        SELECT a, (CASE WHEN (a = 1) THEN 'one' WHEN (a = 2)
        THEN 'two' ELSE 'other' END) FROM test
        """

    def test_pgsql_parse_select_06(self):
        """
        SELECT CASE a.value WHEN 0 THEN '1' ELSE '2' END
        FROM sometable a
% OK %
        SELECT (CASE a.value WHEN 0 THEN '1' ELSE '2' END)
        FROM sometable AS a
        """

    def test_pgsql_parse_select_07(self):
        """
        SELECT * FROM table_one UNION select * FROM table_two
% OK %
        SELECT * FROM table_one UNION (SELECT * FROM table_two)
        """

    def test_pgsql_parse_select_08(self):
        """
        SELECT * FROM my_table WHERE ST_Intersects(geo1, geo2)
% OK %
        SELECT * FROM my_table WHERE st_intersects(geo1, geo2)
        """

    def test_pgsql_parse_select_09(self):
        """
        SELECT 'accbf276-705b-11e7-b8e4-0242ac120002'::UUID
% OK %
        SELECT ('accbf276-705b-11e7-b8e4-0242ac120002')::uuid
        """

    def test_pgsql_parse_select_10(self):
        """
        SELECT * FROM my_table ORDER BY field DESC NULLS FIRST
        """

    def test_pgsql_parse_select_11(self):
        """
        SELECT * FROM my_table ORDER BY field
% OK %
        SELECT * FROM my_table ORDER BY field ASC NULLS LAST
        """

    def test_pgsql_parse_select_12(self):
        """
        SELECT salary, sum(salary) OVER () FROM empsalary
        """

    def test_pgsql_parse_select_13(self):
        """
        SELECT salary, sum(salary)
        OVER (ORDER BY salary) FROM empsalary
% OK %
        SELECT salary, sum(salary)
        OVER (ORDER BY salary ASC NULLS LAST) FROM empsalary
        """

    def test_pgsql_parse_select_14(self):
        """
        SELECT salary, avg(salary)
        OVER (PARTITION BY depname) FROM empsalary
        """

    def test_pgsql_parse_select_15(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS NULL
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS NULL)
        """

    def test_pgsql_parse_select_16(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS NOT NULL
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS NOT NULL)
        """

    def test_pgsql_parse_select_17(self):
        """
        SELECT m.* FROM mytable m WHERE m.foo IS TRUE
% OK %
        SELECT m.* FROM mytable AS m WHERE (m.foo IS TRUE)
        """


    def test_pgsql_parse_select_18(self):
        """
        SELECT m.name AS mname, pname FROM manufacturers m,
        LATERAL get_product_names(m.id) pname
% OK %
        SELECT m.name AS mname, pname FROM manufacturers AS m,
        LATERAL get_product_names(m.id) AS pname
        """

    def test_pgsql_parse_select_19(self):
        """
        SELECT * FROM unnest(ARRAY['a','b','c','d','e','f'])
% OK %
        SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])
        """

    def test_pgsql_parse_select_20(self):
        """
        SELECT * FROM my_table
        WHERE (a, b) in (('a', 'b'), ('c', 'd'))
% OK %
        SELECT * FROM my_table
        WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))
        """

    @test.xerror('bad FRO keyword')
    def test_pgsql_parse_select_21(self):
        """
        SELECT * FRO my_table
        """

    @test.xerror('missing expression after THEN')
    def test_pgsql_parse_select_22(self):
        """
        SELECT a, CASE WHEN a=1 THEN 'one'
        WHEN a=2 THEN ELSE 'other' END FROM test
        """


    def test_pgsql_parse_select_23(self):
        """
        SELECT * FROM table_one, table_two
        """

    def test_pgsql_parse_select_24(self):
        """
        SELECT * FROM table_one, public.table_one
% OK %
        SELECT * FROM table_one, table_one
        """

    def test_pgsql_parse_select_25(self):
        """
        WITH fake_table AS (SELECT * FROM inner_table)
        SELECT * FROM fake_table
% OK %
        WITH fake_table AS ((SELECT * FROM inner_table))
        SELECT * FROM fake_table
        """

    def test_pgsql_parse_select_26(self):
        """
        SELECT * FROM table_one JOIN table_two USING (common_1)
        JOIN table_three USING (common_2)
        """

    def test_pgsql_parse_select_27(self):
        """
        select * FROM table_one UNION select * FROM table_two
% OK %
        SELECT * FROM table_one UNION (SELECT * FROM table_two)
        """

    def test_pgsql_parse_select_28(self):
        """
        SELECT * FROM my_table WHERE (a, b) in ('a', 'b')
% OK %
        SELECT * FROM my_table WHERE ((a, b) IN ('a', 'b'))
        """

    def test_pgsql_parse_select_29(self):
        """
        SELECT * FROM my_table
        WHERE (a, b) in (('a', 'b'), ('c', 'd'))
% OK %
        SELECT * FROM my_table
        WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))
        """

    def test_pgsql_parse_select_30(self):
        """
        SELECT (SELECT * FROM table_one)
% OK %
        SELECT ((SELECT * FROM table_one))
        """

    def test_pgsql_parse_select_31(self):
        """
        SELECT my_func((select * from table_one))
% OK %
        SELECT my_func(((SELECT * FROM table_one)))
        """

    def test_pgsql_parse_select_32(self):
        """
        SELECT 1
        """

    def test_pgsql_parse_select_33(self):
        """
        SELECT 2
        """

    def test_pgsql_parse_select_34(self):
        """
        SELECT $1
        """

    def test_pgsql_parse_select_35(self):
        """
        SELECT 1; SELECT a FROM b
        """

    def test_pgsql_parse_select_36(self):
        """
        SELECT COUNT(DISTINCT id), * FROM targets
        WHERE something IS NOT NULL
        AND elsewhere::interval < now()
% OK %
        SELECT count(DISTINCT id), * FROM targets
        WHERE ((something IS NOT NULL)
        AND ((elsewhere)::pg_catalog.interval < now()))
        """

    def test_pgsql_parse_select_37(self):
        """
        SELECT b AS x, a AS y FROM z
        """

    def test_pgsql_parse_select_38(self):
        """
        WITH a AS (SELECT * FROM x WHERE x.y = $1 AND x.z = 1)
        SELECT * FROM a
% OK %
        WITH a AS ((SELECT * FROM x WHERE ((x.y = $1) AND (x.z = 1))))
        SELECT * FROM a
        """

    def test_pgsql_parse_select_39(self):
        """
        SELECT * FROM x WHERE y IN ($1)
% OK %
        SELECT * FROM x WHERE (y IN ($1))
        """

    def test_pgsql_parse_select_40(self):
        """
        SELECT * FROM x WHERE y IN ($1, $2, $3)
% OK %
        SELECT * FROM x WHERE (y IN ($1, $2, $3))
        """

    def test_pgsql_parse_select_41(self):
        """
        SELECT * FROM x WHERE y IN ( $1::uuid )
% OK %
        SELECT * FROM x WHERE (y IN (($1)::uuid))
        """

    def test_pgsql_parse_select_42(self):
        """
        SELECT * FROM x
        WHERE y IN ( $1::uuid, $2::uuid, $3::uuid )
% OK %
        SELECT * FROM x
        WHERE (y IN (($1)::uuid, ($2)::uuid, ($3)::uuid))
        """

    def test_pgsql_parse_select_43(self):
        """
        SELECT * FROM x AS a, y AS b
        """

    def test_pgsql_parse_select_44(self):
        """
        SELECT * FROM y AS a, x AS b
        """

    def test_pgsql_parse_select_45(self):
        """
        SELECT x AS a, y AS b FROM x
        """

    def test_pgsql_parse_select_46(self):
        """
        SELECT x, y FROM z
        """

    def test_pgsql_parse_select_47(self):
        """
        SELECT y, x FROM z
        """


    def test_pgsql_parse_select_48(self):
        """
        SELECT * FROM a
        """

    def test_pgsql_parse_select_49(self):
        """
        SELECT * FROM a AS b
        """

    def test_pgsql_parse_select_50(self):
        """
        -- nothing
% OK %
        """

        # TODO: is this ok? What is `(0)`?
    def test_pgsql_parse_select_51(self):
        """
        SELECT INTERVAL (0) $2
% OK %
        SELECT ($2)::pg_catalog.interval
        """

    def test_pgsql_parse_select_52(self):
        """
        SELECT INTERVAL (2) $2
% OK %
        SELECT ($2)::pg_catalog.interval
        """


    def test_pgsql_parse_select_53(self):
        """
        SELECT * FROM t WHERE t.a IN (1, 2) AND t.b = 3
% OK %
        SELECT * FROM t WHERE ((t.a IN (1, 2)) AND (t.b = 3))
        """

    def test_pgsql_parse_select_54(self):
        """
        SELECT * FROM t WHERE t.b = 3 AND t.a IN (1, 2)
% OK %
        SELECT * FROM t WHERE ((t.b = 3) AND (t.a IN (1, 2)))
        """

    def test_pgsql_parse_select_55(self):
        """
        SELECT * FROM t WHERE a && '[1,2]'
% OK %
        SELECT * FROM t WHERE (a && '[1,2]')
        """

    def test_pgsql_parse_select_56(self):
        """
        SELECT * FROM t WHERE a && '[1,2]'::int4range
% OK %
        SELECT * FROM t WHERE (a && ('[1,2]')::int4range)
        """

    def test_pgsql_parse_select_57(self):
        """
        SELECT * FROM t_20210301_x
        """


    @unittest.skip("unsupported SQL INSERT statement")
    def test_pgsql_parse_insert(self):
        self.assertEqual(
            parse_and_gen("INSERT INTO my_table(id, name) VALUES(1, 'some')"),
            "INSERT INTO my_table(id, name) VALUES(1, 'some')",
        )
        self.assertEqual(
            parse_and_gen("INSERT INTO my_table(id, name) SELECT 1, 'some'"),
            "INSERT INTO my_table(id, name) SELECT 1, 'some'",
        )
        self.assertEqual(
            parse_and_gen(
                'INSERT INTO my_table(id) VALUES (5) RETURNING id, "date"'
            ),
            'INSERT INTO my_table(id) VALUES (5) RETURNING id, "date"',
        )
        self.assertEqual(
            parse_and_gen(
                "INSERT INTO my_table(id) VALUES(1); SELECT * FROM my_table"
            ),
            "INSERT INTO my_table(id) VALUES(1); SELECT * FROM my_table",
        )
        self.assertEqual(
            parse_and_gen("INSERT INTO my_table"), "INSERT INTO my_table"
        )
        self.assertEqual(
            parse_and_gen(
                "INSERT INTO table_one(id, name) SELECT * from table_two"
            ),
            "INSERT INTO table_one(id, name) SELECT * from table_two",
        )
        self.assertEqual(
            parse_and_gen(
                "WITH fake as (SELECT * FROM inner_table) "
                "INSERT INTO dataset SELECT * FROM fake"
            ),
            "WITH fake as (SELECT * FROM inner_table) "
            "INSERT INTO dataset SELECT * FROM fake",
        )
        self.assertEqual(
            parse_and_gen("INSERT INTO test (a, b) VALUES (?, ?)"),
            "INSERT INTO test (a, b) VALUES (?, ?)",
        )
        self.assertEqual(
            parse_and_gen("INSERT INTO test (b, a) VALUES (?, ?)"),
            "INSERT INTO test (b, a) VALUES (?, ?)",
        )
        self.assertEqual(
            parse_and_gen(
                "INSERT INTO test (a, b) VALUES "
                "(ARRAY[?, ?, ?, ?], ?::timestamptz), "
                "(ARRAY[?, ?, ?, ?], ?::timestamptz), "
                "(?, ?::timestamptz)"
            ),
            "INSERT INTO test (a, b) VALUES "
            "(ARRAY[?, ?, ?, ?], ?::timestamptz), "
            "(ARRAY[?, ?, ?, ?], ?::timestamptz), "
            "(?, ?::timestamptz)",
        )
        self.assertEqual(
            parse_and_gen(
                "INSERT INTO films (code, title, did) VALUES "
                "('UA502', 'Bananas', 105), ('T_601', 'Yojimbo', DEFAULT)"
            ),
            "INSERT INTO films (code, title, did) VALUES "
            "('UA502', 'Bananas', 105), ('T_601', 'Yojimbo', DEFAULT)",
        )
        self.assertEqual(
            parse_and_gen(
                "INSERT INTO films (code, title, did) VALUES (?, ?, ?)"
            ),
            "INSERT INTO films (code, title, did) VALUES (?, ?, ?)",
        )

    @test.skip("unsupported SQL UPDATE")
    def test_pgsql_parse_update(self):
        self.assertEqual(
            parse_and_gen("UPDATE my_table SET the_value = DEFAULT"),
            "UPDATE my_table SET the_value = DEFAULT",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE tictactoe SET board[1:3][1:3] = '{{,,},{,,},{,,}}' "
                "WHERE game = 1"
            ),
            "UPDATE tictactoe SET board[1:3][1:3] = '{{,,},{,,},{,,}}' "
            "WHERE game = 1",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE accounts SET "
                "(contact_first_name, contact_last_name) = "
                "(SELECT first_name, last_name "
                "FROM salesmen WHERE salesmen.id = accounts.sales_id)"
            ),
            "UPDATE accounts SET "
            "(contact_first_name, contact_last_name) = "
            "(SELECT first_name, last_name "
            "FROM salesmen WHERE salesmen.id = accounts.sales_id)",
        )
        self.assertEqual(
            parse_and_gen("UPDATE my_table SET id = 5; DELETE FROM my_table"),
            "UPDATE my_table SET id = 5; DELETE FROM my_table",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE dataset SET a = 5 WHERE id IN "
                "(SELECT * from table_one) OR age IN (select * from table_two)"
            ),
            "UPDATE dataset SET a = 5 WHERE id IN "
            "(SELECT * from table_one) OR age IN (select * from table_two)",
        )
        self.assertEqual(
            parse_and_gen("UPDATE dataset SET a = 5 FROM extra WHERE b = c"),
            "UPDATE dataset SET a = 5 FROM extra WHERE b = c",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE users SET one_thing = $1, second_thing = $2 "
                "WHERE users.id = ?"
            ),
            "UPDATE users SET one_thing = $1, second_thing = $2 "
            "WHERE users.id = ?",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE users SET something_else = $1 WHERE users.id = ?"
            ),
            "UPDATE users SET something_else = $1 WHERE users.id = ?",
        )
        self.assertEqual(
            parse_and_gen(
                "UPDATE users SET something_else = "
                "(SELECT a FROM x WHERE uid = users.id LIMIT 1) "
                "WHERE users.id = ?"
            ),
            "UPDATE users SET something_else = "
            "(SELECT a FROM x WHERE uid = users.id LIMIT 1) "
            "WHERE users.id = ?",
        )
        self.assertEqual(
            parse_and_gen("UPDATE x SET a = 1, b = 2, c = 3"),
            "UPDATE x SET a = 1, b = 2, c = 3",
        )
        self.assertEqual(
            parse_and_gen("UPDATE x SET z = now()"), "UPDATE x SET z = now()"
        )

    @test.skip("unsupported SQL constructs")
    def test_pgsql_parse_unsupported(self):
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM "
                "(VALUES (1, 'one'), (2, 'two')) AS t (num, letter)"
            ),
            "SELECT * FROM "
            "(VALUES (1, 'one'), (2, 'two')) AS t (num, letter)",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM my_table ORDER BY field"),
            "SELECT * FROM my_table ORDER BY field ASC NULLS LAST USING @>",
        )
        self.assertEqual(
            parse_and_gen("SELECT m.* FROM mytable m FOR UPDATE"),
            "SELECT m.* FROM mytable AS m FOR UPDATE",
        )
        self.assertEqual(
            parse_and_gen("SELECT m.* FROM mytable m FOR SHARE of m nowait"),
            "SELECT m.* FROM mytable m FOR SHARE of m nowait",
        )

        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM unnest(ARRAY['a','b','c','d','e','f']) "
                "WITH ORDINALITY"
            ),
            "SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])",
        )

        self.assertEqual(
            parse_and_gen(
                "DELETE FROM dataset USING table_one "
                "WHERE x = y OR x IN (SELECT * from table_two)"
            ),
            "DELETE FROM dataset USING table_one "
            "WHERE x = y OR x IN (SELECT * from table_two)",
        )

        self.assertEqual(
            parse_and_gen("SELECT ?"), "SELECT ?")
        self.assertEqual(
            parse_and_gen("SELECT * FROM x WHERE y = ?"),
            "SELECT * FROM x WHERE y = ?",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM x WHERE y = ANY ($1)"),
            "SELECT * FROM x WHERE y = ANY ($1)",
        )
        self.assertEqual(
            parse_and_gen("PREPARE a123 AS SELECT a"),
            "PREPARE a123 AS SELECT a",
        )
        self.assertEqual(
            parse_and_gen("EXECUTE a123"), "EXECUTE a123")
        self.assertEqual(
            parse_and_gen("DEALLOCATE a123"), "DEALLOCATE a123")
        self.assertEqual(
            parse_and_gen("DEALLOCATE ALL"), "DEALLOCATE ALL")
        self.assertEqual(
            parse_and_gen("EXPLAIN ANALYZE SELECT a"),
            "EXPLAIN ANALYZE SELECT a",
        )
        self.assertEqual(
            parse_and_gen("VACUUM FULL my_table"), "VACUUM FULL my_table"
        )
        self.assertEqual(
            parse_and_gen("SAVEPOINT some_id"), "SAVEPOINT some_id"
        )
        self.assertEqual(
            parse_and_gen("RELEASE some_id"), "RELEASE some_id")
        self.assertEqual(
            parse_and_gen("PREPARE TRANSACTION 'some_id'"),
            "PREPARE TRANSACTION 'some_id'",
        )
        self.assertEqual(
            parse_and_gen("START TRANSACTION READ WRITE"),
            "START TRANSACTION READ WRITE",
        )
        self.assertEqual(
            parse_and_gen(
                "DECLARE cursor_123 CURSOR FOR "
                "SELECT * FROM test WHERE id = 123"
            ),
            "DECLARE cursor_123 CURSOR FOR "
            "SELECT * FROM test WHERE id = 123",
        )
        self.assertEqual(
            parse_and_gen("FETCH 1000 FROM cursor_123"),
            "FETCH 1000 FROM cursor_123",
        )
        self.assertEqual(
            parse_and_gen("CLOSE cursor_123"), "CLOSE cursor_123")

        self.assertEqual(
            parse_and_gen(
                "CREATE TABLE types ("
                "a float(2), b float(49), "
                "c NUMERIC(2, 3), d character(4), e char(5), "
                "f varchar(6), g character varying(7))"
            ),
            "CREATE TABLE types ("
            "a float(2), b float(49), "
            "c NUMERIC(2, 3), d character(4), e char(5), "
            "f varchar(6), g character varying(7))",
        )
        self.assertEqual(
            parse_and_gen(
                "CREATE VIEW view_a (a, b) AS WITH RECURSIVE view_a (a, b) AS"
                ' (SELECT * FROM a(1)) SELECT "a", "b" FROM "view_a"'
            ),
            "CREATE VIEW view_a (a, b) AS WITH RECURSIVE view_a (a, b) AS"
            ' (SELECT * FROM a(1)) SELECT "a", "b" FROM "view_a"',
        )
        self.assertEqual(
            parse_and_gen("CREATE FOREIGN TABLE ft1 () SERVER no_server"),
            "CREATE FOREIGN TABLE ft1 () SERVER no_server",
        )
        self.assertEqual(
            parse_and_gen(
                "CREATE TEMPORARY TABLE my_temp_table "
                "(test_id integer NOT NULL) ON COMMIT DROP"
            ),
            "CREATE TEMPORARY TABLE my_temp_table "
            "(test_id integer NOT NULL) ON COMMIT DROP",
        )
        self.assertEqual(
            parse_and_gen("CREATE TEMPORARY TABLE my_temp_table AS SELECT 1"),
            "CREATE TEMPORARY TABLE my_temp_table AS SELECT 1",
        )
