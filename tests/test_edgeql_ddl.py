##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.client import exceptions as client_errors
from edgedb.server import _testbase as tb


class TestDeltas(tb.DDLTestCase):

    async def test_edgeql_ddl01(self):
        await self.con.execute("""
            CREATE LINK test::test_link;
        """)

    async def test_edgeql_ddl02(self):
        await self.con.execute("""
            CREATE LINK test::test_concept_link {
                CREATE LINK PROPERTY test::test_link_prop TO std::int;
            };

            CREATE CONCEPT test::TestConcept {
                CREATE LINK test::test_concept_link TO std::str {
                    CREATE LINK PROPERTY test::test_link_prop TO std::int {
                        SET title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl03(self):
        await self.con.execute("""
            CREATE LINK test::test_concept_link_prop {
                CREATE LINK PROPERTY test::link_prop1 TO std::str;
            };
        """)

    async def test_edgeql_ddl04(self):
        await self.con.execute("""
            CREATE CONCEPT test::A;
            CREATE CONCEPT test::B INHERITING test::A;

            CREATE CONCEPT test::Object1 {
                CREATE REQUIRED LINK test::a TO test::A;
            };

            CREATE CONCEPT test::Object2 {
                CREATE LINK test::a TO test::B;
            };

            CREATE CONCEPT test::Object_12
                INHERITING (test::Object1, test::Object2);
        """)

    async def test_edgeql_ddl05(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create an aggregate function'):

            await self.con.execute("""
                CREATE FUNCTION test::my_lower(std::str) RETURNING std::str
                    FROM SQL FUNCTION 'lower';

                CREATE AGGREGATE test::my_lower(std::any)
                    RETURNING std::str
                    INITIAL VALUE ''
                    FROM SQL AGGREGATE 'count';
            """)

        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create a function'):

            await self.con.execute("""
                CREATE AGGREGATE test::my_lower2(std::any)
                    RETURNING std::str
                    INITIAL VALUE ''
                    FROM SQL AGGREGATE 'count';

                CREATE FUNCTION test::my_lower2(std::str) RETURNING std::str
                    FROM SQL FUNCTION 'lower';
            """)

    async def test_edgeql_ddl06(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.con.execute(f"""
            CREATE FUNCTION test::my_sql_func1()
                RETURNING std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;

            CREATE FUNCTION test::my_sql_func2($foo: std::str)
                RETURNING std::str
                FROM SQL $$
                    SELECT $1::text
                $$;

            CREATE FUNCTION test::my_sql_func3(std::str)
                RETURNING std::str
                FROM SQL $$
                    SELECT $1::text
                $$;

            CREATE FUNCTION test::my_sql_func4(*std::str)
                RETURNING std::str
                FROM SQL $$
                    SELECT array_to_string($1, '-')
                $$;

            CREATE FUNCTION test::{long_func_name}()
                RETURNING std::str
                FROM SQL $$
                    SELECT '{long_func_name}'::text
                $$;

            CREATE FUNCTION test::my_sql_func6(std::str='a' + 'b')
                RETURNING std::str
                FROM SQL $$
                    SELECT $1 || 'c'
                $$;

            CREATE FUNCTION test::my_sql_func7(array<std::int>)
                RETURNING std::int
                FROM SQL $$
                    SELECT sum(s)::bigint FROM UNNEST($1) AS s
                $$;
        """)

        await self.assert_query_result(fr"""
            SELECT test::my_sql_func1();
            SELECT test::my_sql_func2('foo');
            SELECT test::my_sql_func3('bar');
            SELECT test::my_sql_func4('fizz', 'buzz');
            SELECT test::{long_func_name}();
            SELECT test::my_sql_func6();
            SELECT test::my_sql_func6('xy');
            SELECT test::my_sql_func7([1, 2, 3, 10]);
        """, [
            ['spam'],
            ['foo'],
            ['bar'],
            ['fizz-buzz'],
            [long_func_name],
            ['abc'],
            ['xyc'],
            [16],
        ])

    async def test_edgeql_ddl07(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'could not.*broken_sql.*not constant'):
            await self.con.execute(f"""
                CREATE FUNCTION test::broken_sql_func1(
                    std::int=(SELECT schema::Concept))
                RETURNING std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;
            """)

    async def test_edgeql_ddl08(self):
        await self.con.execute(f"""
            CREATE FUNCTION test::my_edgeql_func1()
                RETURNING std::str
                FROM EdgeQL $$
                    SELECT 'sp' + 'am'
                $$;

            CREATE FUNCTION test::my_edgeql_func2(std::str)
                RETURNING schema::Concept
                FROM EdgeQL $$
                    SELECT
                        schema::Concept
                    FILTER schema::Concept.name = $1
                $$;

            CREATE FUNCTION test::my_edgeql_func3(std::int)
                RETURNING std::int
                FROM EdgeQL $$
                    SELECT $1 + 10
                $$;

            CREATE FUNCTION test::my_edgeql_func4(std::int)
                RETURNING array<std::int>
                FROM EdgeQL $$
                    SELECT [$1, 1, 2, 3]
                $$;
        """)

        await self.assert_query_result(r"""
            SELECT test::my_edgeql_func1();
            SELECT test::my_edgeql_func2('schema::PrimaryClass').name;
            SELECT test::my_edgeql_func3(1);
            SELECT test::my_edgeql_func4(42);
        """, [
            ['spam'],
            ['schema::PrimaryClass'],
            [11],
            [[42, 1, 2, 3]]
        ])

    async def test_edgeql_ddl09(self):
        await self.con.execute("""
            CREATE FUNCTION test::attr_func_1() RETURNING std::str {
                SET description := 'hello';
                FROM EdgeQL "SELECT '1'";
            };
        """)

        await self.assert_query_result(r"""
            SELECT schema::Function {
                attributes: {
                    @value
                } FILTER .name = 'stdattrs::description'
            } FILTER .name = 'test::attr_func_1';
        """, [
            [{
                'attributes': [{
                    '@value': 'hello'
                }]
            }],
        ])

    async def test_edgeql_ddl10(self):
        await self.con.execute("""
            CREATE FUNCTION test::int_func_1() RETURNING std::int {
                FROM EdgeQL "SELECT 1";
            };
        """)

        await self.assert_query_result(r"""
            SELECT test::int_func_1();
        """, [
            [1],
        ])
