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


import unittest
from edb.testbase import server as tb

from edb.pgsql import parser
from edb.pgsql import codegen


def parse_and_gen(sql: str) -> str:
    ast = parser.parse(sql)
    sql_stmts = [codegen.generate_source(stmt, pretty=False) for stmt in ast]
    sql = "; ".join(sql_stmts)
    return sql.replace("  ", " ").replace("( (", "((").replace(") )", "))")


class TestEdgeQLSelect(tb.TestCase):
    def test_pgsql_parse_select(self):
        self.assertEqual(
            parse_and_gen("SELECT * FROM my_table"), "SELECT * FROM my_table"
        )
        self.assertEqual(
            parse_and_gen(
                """
                SELECT col1 FROM my_table WHERE
                my_attribute LIKE 'condition' AND other = 5.6 AND extra > 5
                """
            ),
            "SELECT col1 FROM my_table WHERE "
            "(((my_attribute LIKE 'condition') AND "
            "(other = 5.6)) AND (extra > 5))",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM table_one JOIN table_two USING (common)"
            ),
            "SELECT * FROM table_one JOIN table_two USING (common)",
        )
        self.assertEqual(
            parse_and_gen(
                """
                WITH fake_table AS (
                    SELECT SUM(countable) AS total FROM inner_table
                    GROUP BY groupable
                ) SELECT * FROM fake_table
                """
            ),
            "WITH fake_table AS ((SELECT sum(countable) AS total "
            "FROM inner_table GROUP BY groupable )) "
            "SELECT * FROM fake_table",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM (SELECT something FROM dataset) AS other"
            ),
            "SELECT * FROM (SELECT something FROM dataset ) AS other",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT a, CASE WHEN a=1 THEN 'one' WHEN a=2 "
                "THEN 'two' ELSE 'other' END FROM test"
            ),
            "SELECT a, (CASE WHEN (a = 1) THEN 'one' WHEN (a = 2) "
            "THEN 'two' ELSE 'other' END) FROM test",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT CASE a.value WHEN 0 THEN '1' ELSE '2' END "
                "FROM sometable a"
            ),
            "SELECT (CASE a.value WHEN 0 THEN '1' ELSE '2' END) "
            "FROM sometable AS a",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM table_one UNION select * FROM table_two"
            ),
            "SELECT * FROM table_one UNION (SELECT * FROM table_two )",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM my_table WHERE ST_Intersects(geo1, geo2)"
            ),
            "SELECT * FROM my_table WHERE st_intersects(geo1, geo2)",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT 'accbf276-705b-11e7-b8e4-0242ac120002'::UUID"
            ),
            "SELECT ('accbf276-705b-11e7-b8e4-0242ac120002')::uuid",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM my_table ORDER BY field DESC NULLS FIRST"
            ),
            "SELECT * FROM my_table ORDER BY field DESC NULLS FIRST",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM my_table ORDER BY field"),
            "SELECT * FROM my_table ORDER BY field ASC NULLS LAST",
        )
        self.assertEqual(
            parse_and_gen("SELECT salary, sum(salary) OVER () FROM empsalary"),
            "SELECT salary, sum(salary) OVER () FROM empsalary",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT salary, sum(salary) "
                "OVER (ORDER BY salary) FROM empsalary"
            ),
            "SELECT salary, sum(salary) "
            "OVER ( ORDER BY salary ASC NULLS LAST) FROM empsalary",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT salary, avg(salary) "
                "OVER (PARTITION BY depname) FROM empsalary"
            ),
            "SELECT salary, avg(salary) "
            "OVER (PARTITION BY depname) FROM empsalary",
        )
        self.assertEqual(
            parse_and_gen("SELECT m.* FROM mytable m WHERE m.foo IS NULL"),
            "SELECT m.* FROM mytable AS m WHERE (m.foo IS NULL)",
        )
        self.assertEqual(
            parse_and_gen("SELECT m.* FROM mytable m WHERE m.foo IS NOT NULL"),
            "SELECT m.* FROM mytable AS m WHERE (m.foo IS NOT NULL)",
        )
        self.assertEqual(
            parse_and_gen("SELECT m.* FROM mytable m WHERE m.foo IS TRUE"),
            "SELECT m.* FROM mytable AS m WHERE (m.foo IS TRUE)",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT m.name AS mname, pname FROM manufacturers m, "
                "LATERAL get_product_names(m.id) pname"
            ),
            "SELECT m.name AS mname, pname FROM manufacturers AS m, "
            "LATERAL get_product_names(m.id) AS pname",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM unnest(ARRAY['a','b','c','d','e','f'])"
            ),
            "SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM my_table "
                "WHERE (a, b) in (('a', 'b'), ('c', 'd'))"
            ),
            "SELECT * FROM my_table "
            "WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))",
        )
        with self.assertRaises(BaseException):
            parse_and_gen("SELECT * FRO my_table")

        with self.assertRaises(BaseException):
            parse_and_gen(
                "SELECT a, CASE WHEN a=1 THEN 'one' "
                "WHEN a=2 THEN ELSE 'other' END FROM test"
            )

        self.assertEqual(
            parse_and_gen("SELECT * FROM table_one, table_two"),
            "SELECT * FROM table_one, table_two",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM table_one, public.table_one"),
            "SELECT * FROM table_one, table_one",
        )
        self.assertEqual(
            parse_and_gen(
                "WITH fake_table AS (SELECT * FROM inner_table) "
                "SELECT * FROM fake_table"
            ),
            "WITH fake_table AS ((SELECT * FROM inner_table )) "
            "SELECT * FROM fake_table",
        )

        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM table_one JOIN table_two USING (common_1) "
                "JOIN table_three USING (common_2)"
            ),
            "SELECT * FROM table_one JOIN table_two USING (common_1) "
            "JOIN table_three USING (common_2)",
        )
        self.assertEqual(
            parse_and_gen(
                "select * FROM table_one UNION select * FROM table_two"
            ),
            "SELECT * FROM table_one UNION (SELECT * FROM table_two )",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM my_table WHERE (a, b) in ('a', 'b')"),
            "SELECT * FROM my_table WHERE ((a, b) IN ('a', 'b'))",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM my_table "
                "WHERE (a, b) in (('a', 'b'), ('c', 'd'))"
            ),
            "SELECT * FROM my_table "
            "WHERE ((a, b) IN (('a', 'b'), ('c', 'd')))",
        )
        self.assertEqual(
            parse_and_gen("SELECT (SELECT * FROM table_one)"),
            "SELECT ((SELECT * FROM table_one ))",
        )
        self.assertEqual(
            parse_and_gen("SELECT my_func((select * from table_one))"),
            "SELECT my_func(((SELECT * FROM table_one )))",
        )
        self.assertEqual(parse_and_gen("SELECT 1"), "SELECT 1")
        self.assertEqual(parse_and_gen("SELECT 2"), "SELECT 2")
        self.assertEqual(parse_and_gen("SELECT $1"), "SELECT $1")
        self.assertEqual(
            parse_and_gen("SELECT 1; SELECT a FROM b"),
            "SELECT 1; SELECT a FROM b",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT COUNT(DISTINCT id), * FROM targets "
                "WHERE something IS NOT NULL "
                "AND elsewhere::interval < now()"
            ),
            "SELECT count(DISTINCT id), * FROM targets "
            "WHERE ((something IS NOT NULL) "
            "AND ((elsewhere)::pg_catalog.interval < now()))",
        )

        self.assertEqual(
            parse_and_gen("SELECT b AS x, a AS y FROM z"),
            "SELECT b AS x, a AS y FROM z",
        )
        self.assertEqual(
            parse_and_gen(
                "WITH a AS (SELECT * FROM x WHERE x.y = $1 AND x.z = 1) "
                "SELECT * FROM a"
            ),
            "WITH a AS ((SELECT * FROM x WHERE ((x.y = $1) AND (x.z = 1)))) "
            "SELECT * FROM a",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM x WHERE y IN ($1)"),
            "SELECT * FROM x WHERE (y IN ($1))",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM x WHERE y IN ($1, $2, $3)"),
            "SELECT * FROM x WHERE (y IN ($1, $2, $3))",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM x WHERE y IN ( $1::uuid )"),
            "SELECT * FROM x WHERE (y IN (($1)::uuid))",
        )
        self.assertEqual(
            parse_and_gen(
                "SELECT * FROM x "
                "WHERE y IN ( $1::uuid, $2::uuid, $3::uuid )"
            ),
            "SELECT * FROM x "
            "WHERE (y IN (($1)::uuid, ($2)::uuid, ($3)::uuid))",
        )

        self.assertEqual(
            parse_and_gen("SELECT * FROM x AS a, y AS b"),
            "SELECT * FROM x AS a, y AS b",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM y AS a, x AS b"),
            "SELECT * FROM y AS a, x AS b",
        )
        self.assertEqual(
            parse_and_gen("SELECT x AS a, y AS b FROM x"),
            "SELECT x AS a, y AS b FROM x",
        )
        self.assertEqual(
            parse_and_gen("SELECT y AS a, x AS b FROM x"),
            "SELECT y AS a, x AS b FROM x",
        )
        self.assertEqual(
            parse_and_gen("SELECT x, y FROM z"), "SELECT x, y FROM z"
        )
        self.assertEqual(
            parse_and_gen("SELECT y, x FROM z"), "SELECT y, x FROM z"
        )

        self.assertEqual(parse_and_gen("SELECT * FROM a"), "SELECT * FROM a")
        self.assertEqual(
            parse_and_gen("SELECT * FROM a AS b"), "SELECT * FROM a AS b"
        )
        self.assertEqual(parse_and_gen("-- nothing"), "")

        # TODO: is this ok? What is `(0)`?
        self.assertEqual(
            parse_and_gen("SELECT INTERVAL (0) $2"),
            "SELECT ($2)::pg_catalog.interval",
        )
        self.assertEqual(
            parse_and_gen("SELECT INTERVAL (2) $2"),
            "SELECT ($2)::pg_catalog.interval",
        )

        self.assertEqual(
            parse_and_gen("SELECT * FROM t WHERE t.a IN (1, 2) AND t.b = 3"),
            "SELECT * FROM t WHERE ((t.a IN (1, 2)) AND (t.b = 3))",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM t WHERE t.b = 3 AND t.a IN (1, 2)"),
            "SELECT * FROM t WHERE ((t.b = 3) AND (t.a IN (1, 2)))",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM t WHERE a && '[1,2]'"),
            "SELECT * FROM t WHERE (a && '[1,2]')",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM t WHERE a && '[1,2]'::int4range"),
            "SELECT * FROM t WHERE (a && ('[1,2]')::int4range)",
        )
        self.assertEqual(
            parse_and_gen("SELECT * FROM t_20210301_x"),
            "SELECT * FROM t_20210301_x",
        )

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

    @unittest.skip("unsupported SQL UPDATE")
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

    @unittest.skip("unsupported SQL constructs")
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

        self.assertEqual(parse_and_gen("SELECT ?"), "SELECT ?")
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
        self.assertEqual(parse_and_gen("EXECUTE a123"), "EXECUTE a123")
        self.assertEqual(parse_and_gen("DEALLOCATE a123"), "DEALLOCATE a123")
        self.assertEqual(parse_and_gen("DEALLOCATE ALL"), "DEALLOCATE ALL")
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
        self.assertEqual(parse_and_gen("RELEASE some_id"), "RELEASE some_id")
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
        self.assertEqual(parse_and_gen("CLOSE cursor_123"), "CLOSE cursor_123")

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
