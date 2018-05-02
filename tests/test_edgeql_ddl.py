##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest  # NOQA

from edgedb.client import exceptions as client_errors
from edgedb.server import _testbase as tb


class TestEdgeQLDDL(tb.DDLTestCase):

    async def test_edgeql_ddl_01(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_link;
        """)

    async def test_edgeql_ddl_02(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_object_link {
                CREATE PROPERTY test::test_link_prop -> std::int64;
            };

            CREATE TYPE test::TestObjectType {
                CREATE LINK test::test_object_link -> std::Object {
                    CREATE PROPERTY test::test_link_prop -> std::int64 {
                        SET title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl_03(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_object_link_prop {
                CREATE PROPERTY test::link_prop1 -> std::str;
            };
        """)

    async def test_edgeql_ddl_04(self):
        await self.con.execute("""
            CREATE TYPE test::A;
            CREATE TYPE test::B EXTENDING test::A;

            CREATE TYPE test::Object1 {
                CREATE REQUIRED LINK test::a -> test::A;
            };

            CREATE TYPE test::Object2 {
                CREATE LINK test::a -> test::B;
            };

            CREATE TYPE test::Object_12
                EXTENDING (test::Object1, test::Object2);
        """)

    async def test_edgeql_ddl_05(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create a function'):

            await self.con.execute("""
                CREATE FUNCTION test::my_lower(std::str) -> std::str
                    FROM SQL FUNCTION 'lower';

                CREATE FUNCTION test::my_lower(SET OF std::str)
                    -> std::str {
                    INITIAL VALUE '';
                    FROM SQL FUNCTION 'count';
                };
            """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(std::str);
        """)

        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create a function'):

            await self.con.execute("""
                CREATE FUNCTION test::my_lower(SET OF std::any)
                    -> std::str {
                    INITIAL VALUE '';
                    FROM SQL FUNCTION 'count';
                };

                CREATE FUNCTION test::my_lower(std::any) -> std::str
                    FROM SQL FUNCTION 'lower';
            """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(std::any);
        """)

    async def test_edgeql_ddl_06(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.con.execute(f"""
            CREATE FUNCTION test::my_sql_func1()
                -> std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;

            CREATE FUNCTION test::my_sql_func2($foo: std::str)
                -> std::str
                FROM SQL $$
                    SELECT $1::text
                $$;

            CREATE FUNCTION test::my_sql_func3(std::str)
                -> std::str
                FROM SQL $$
                    SELECT $1::text
                $$;

            CREATE FUNCTION test::my_sql_func4(VARIADIC std::str)
                -> std::str
                FROM SQL $$
                    SELECT array_to_string($1, '-')
                $$;

            CREATE FUNCTION test::{long_func_name}()
                -> std::str
                FROM SQL $$
                    SELECT '{long_func_name}'::text
                $$;

            CREATE FUNCTION test::my_sql_func6(std::str='a' + 'b')
                -> std::str
                FROM SQL $$
                    SELECT $1 || 'c'
                $$;

            CREATE FUNCTION test::my_sql_func7(array<std::int64>)
                -> std::int64
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

        await self.con.execute(f"""
            DROP FUNCTION test::my_sql_func1();
            DROP FUNCTION test::my_sql_func2($foo: std::str);
            DROP FUNCTION test::my_sql_func3(std::str);
            DROP FUNCTION test::my_sql_func4(VARIADIC std::str);
            DROP FUNCTION test::{long_func_name}();
            DROP FUNCTION test::my_sql_func6(std::str='a' + 'b');
            DROP FUNCTION test::my_sql_func7(array<std::int64>);
        """)

    async def test_edgeql_ddl_07(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'could not.*broken_sql.*not constant'):
            await self.con.execute(f"""
                CREATE FUNCTION test::broken_sql_func1(
                    std::int64=(SELECT schema::ObjectType))
                -> std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;
            """)

    async def test_edgeql_ddl_08(self):
        await self.con.execute(f"""
            CREATE FUNCTION test::my_edgeql_func1()
                -> std::str
                FROM EdgeQL $$
                    SELECT 'sp' + 'am'
                $$;

            CREATE FUNCTION test::my_edgeql_func2(std::str)
                -> schema::ObjectType
                FROM EdgeQL $$
                    SELECT
                        schema::ObjectType
                    FILTER schema::ObjectType.name = $1
                $$;

            CREATE FUNCTION test::my_edgeql_func3(std::int64)
                -> std::int64
                FROM EdgeQL $$
                    SELECT $1 + 10
                $$;

            CREATE FUNCTION test::my_edgeql_func4(std::int64)
                -> array<std::int64>
                FROM EdgeQL $$
                    SELECT [$1, 1, 2, 3]
                $$;
        """)

        await self.assert_query_result(r"""
            SELECT test::my_edgeql_func1();
            SELECT test::my_edgeql_func2('schema::Object').name;
            SELECT test::my_edgeql_func3(1);
            SELECT test::my_edgeql_func4(42);
        """, [
            ['spam'],
            ['schema::Object'],
            [11],
            [[42, 1, 2, 3]]
        ])

        await self.con.execute(f"""
            DROP FUNCTION test::my_edgeql_func1();
            DROP FUNCTION test::my_edgeql_func2(std::str);
            DROP FUNCTION test::my_edgeql_func3(std::int64);
            DROP FUNCTION test::my_edgeql_func4(std::int64);
        """)

    async def test_edgeql_ddl_09(self):
        await self.con.execute("""
            CREATE FUNCTION test::attr_func_1() -> std::str {
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

        await self.con.execute("""
            DROP FUNCTION test::attr_func_1();
        """)

    async def test_edgeql_ddl_10(self):
        await self.con.execute("""
            CREATE FUNCTION test::int_func_1() -> std::int64 {
                FROM EdgeQL "SELECT 1";
            };
        """)

        await self.assert_query_result(r"""
            SELECT test::int_func_1();
        """, [
            [1],
        ])

    async def test_edgeql_ddl_11(self):
        await self.con.execute("""
            CREATE TYPE test::TestContainerLinkObjectType {
                CREATE PROPERTY test::test_array_link -> array<std::str>;
                CREATE PROPERTY test::test_array_link_2 -> array<std::str[10]>;
                CREATE PROPERTY test::test_map_link ->
                    map<std::str, std::int64>;
            };
        """)

    async def test_edgeql_ddl_12(self):
        with self.assertRaisesRegex(
                client_errors.EdgeQLError,
                r'Unknown token.*__subject__'):
            await self.con.execute(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY test::foo -> std::str {
                        CREATE CONSTRAINT expression
                            ON (`__subject__` = 'foo');
                    };
                };
            """)

    async def test_edgeql_ddl_13(self):
        with self.assertRaisesRegex(
                client_errors.EdgeQLError,
                'reference to a non-existent schema item: self'):
            await self.con.execute(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY test::foo -> std::str {
                        CREATE CONSTRAINT expression ON (`self` = 'foo');
                    };
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_14(self):
        await self.con.execute("""
            CREATE TYPE test::TestSelfLink1 {
                CREATE PROPERTY test::foo1 -> std::str;
                CREATE PROPERTY test::bar1 -> std::str {
                    SET default := __source__.foo1;
                };
            };
        """)

        await self.assert_query_result(r"""
            INSERT test::TestSelfLink1 {
                foo1 := 'Victor'
            };

            WITH MODULE test
            SELECT TestSelfLink1 {
                foo1,
                bar1,
            };
        """, [
            [1],
            [{'foo1': 'Victor', 'bar1': 'Victor'}]
        ])

    @unittest.expectedFailure
    async def test_edgeql_ddl_15(self):
        await self.con.execute(r"""
            CREATE TYPE test::TestSelfLink2 {
                CREATE PROPERTY test::foo2 -> std::str;
                CREATE PROPERTY test::bar2 -> std::str {
                    # NOTE: this is a set of all TestSelfLink2.foo2
                    SET default := test::TestSelfLink2.foo2;
                    SET cardinality := '1*';
                };
            };
        """)

        await self.assert_query_result(r"""
            INSERT test::TestSelfLink2 {
                foo2 := 'Victor'
            };
            INSERT test::TestSelfLink2 {
                foo2 := 'Elvis'
            };

            WITH MODULE test
            SELECT TestSelfLink2 {
                foo2,
                bar2,
            } FILTER TestSelfLink2.foo2 = 'Victor';
        """, [
            [1],
            [1],
            [{'foo2': 'Victor', 'bar2': {'Victor', 'Elvis'}}]
        ])

    @unittest.expectedFailure
    async def test_edgeql_ddl_16(self):
        # XXX: not sure what the error would say exactly, but
        # cardinality should be an issue here
        with self.assertRaisesRegex(client_errors.EdgeQLError):
            await self.con.execute(r"""
                CREATE TYPE test::TestSelfLink3 {
                    CREATE PROPERTY test::foo3 -> std::str;
                    CREATE PROPERTY test::bar3 -> std::str {
                        # NOTE: this is a set of all TestSelfLink2.foo3
                        SET default := test::TestSelfLink2.foo3;
                    };
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_17(self):
        await self.con.execute("""
            CREATE TYPE test::TestSelfLink4 {
                CREATE PROPERTY test::__typename4 -> std::str {
                    SET default := __source__.__type__.name;
                };
            };
        """)

        await self.assert_query_result(r"""
            INSERT test::TestSelfLink4;

            WITH MODULE test
            SELECT TestSelfLink4 {
                __typename4,
            };
        """, [
            [1],
            [{'__typename4': 'test::TestSelfLink4'}]
        ])
