#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import unittest  # NOQA

from edb.client import exceptions as client_errors
from edb.server import _testbase as tb


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
                                    'cannot create a function'):

            await self.con.execute("""
                CREATE FUNCTION test::my_lower(s: std::str) -> std::str
                    FROM SQL FUNCTION 'lower';

                CREATE FUNCTION test::my_lower(s: SET OF std::str)
                    -> std::str {
                    INITIAL VALUE '';
                    FROM SQL FUNCTION 'count';
                };
            """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: std::str);
        """)

        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'cannot create a function'):

            await self.con.execute("""
                CREATE FUNCTION test::my_lower(s: SET OF std::any)
                    -> std::str {
                    INITIAL VALUE '';
                    FROM SQL FUNCTION 'count';
                };

                CREATE FUNCTION test::my_lower(s: std::any) -> std::str
                    FROM SQL FUNCTION 'lower';
            """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: std::any);
        """)

    async def test_edgeql_ddl_06(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.con.execute(f"""
            CREATE FUNCTION test::my_sql_func1()
                -> std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;

            CREATE FUNCTION test::my_sql_func2(foo: std::str)
                -> std::str
                FROM SQL $$
                    SELECT "foo"::text
                $$;

            CREATE FUNCTION test::my_sql_func4(VARIADIC s: std::str)
                -> std::str
                FROM SQL $$
                    SELECT array_to_string(s, '-')
                $$;

            CREATE FUNCTION test::{long_func_name}()
                -> std::str
                FROM SQL $$
                    SELECT '{long_func_name}'::text
                $$;

            CREATE FUNCTION test::my_sql_func6(a: std::str='a' + 'b')
                -> std::str
                FROM SQL $$
                    SELECT $1 || 'c'
                $$;

            CREATE FUNCTION test::my_sql_func7(s: array<std::int64>)
                -> std::int64
                FROM SQL $$
                    SELECT sum(s)::bigint FROM UNNEST($1) AS s
                $$;
        """)

        await self.assert_query_result(fr"""
            SELECT test::my_sql_func1();
            SELECT test::my_sql_func2('foo');
            SELECT test::my_sql_func4('fizz', 'buzz');
            SELECT test::{long_func_name}();
            SELECT test::my_sql_func6();
            SELECT test::my_sql_func6('xy');
            SELECT test::my_sql_func7([1, 2, 3, 10]);
        """, [
            ['spam'],
            ['foo'],
            ['fizz-buzz'],
            [long_func_name],
            ['abc'],
            ['xyc'],
            [16],
        ])

        await self.con.execute(f"""
            DROP FUNCTION test::my_sql_func1();
            DROP FUNCTION test::my_sql_func2(foo: std::str);
            DROP FUNCTION test::my_sql_func4(VARIADIC s: std::str);
            DROP FUNCTION test::{long_func_name}();
            DROP FUNCTION test::my_sql_func6(a: std::str='a' + 'b');
            DROP FUNCTION test::my_sql_func7(s: array<std::int64>);
        """)

    async def test_edgeql_ddl_07(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    r'invalid default value'):
            await self.con.execute(f"""
                CREATE FUNCTION test::broken_sql_func1(
                    a: std::int64=(SELECT schema::ObjectType))
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

            CREATE FUNCTION test::my_edgeql_func2(s: std::str)
                -> schema::ObjectType
                FROM EdgeQL $$
                    SELECT
                        schema::ObjectType
                    FILTER schema::ObjectType.name = $s
                $$;

            CREATE FUNCTION test::my_edgeql_func3(s: std::int64)
                -> std::int64
                FROM EdgeQL $$
                    SELECT $s + 10
                $$;

            CREATE FUNCTION test::my_edgeql_func4(i: std::int64)
                -> array<std::int64>
                FROM EdgeQL $$
                    SELECT [$i, 1, 2, 3]
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
            DROP FUNCTION test::my_edgeql_func2(s: std::str);
            DROP FUNCTION test::my_edgeql_func3(s: std::int64);
            DROP FUNCTION test::my_edgeql_func4(i: std::int64);
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
        await self.con.execute(r"""
            CREATE TYPE test::TestContainerLinkObjectType {
                CREATE PROPERTY test::test_array_link -> array<std::str>;
                # FIXME: for now dimention specs on the array are
                # disabled pending a syntax change
                # CREATE PROPERTY test::test_array_link_2 ->
                #     array<std::str[10]>;
            };
        """)

    async def test_edgeql_ddl_12(self):
        with self.assertRaisesRegex(
                client_errors.EdgeQLError,
                r"Unexpected '`__subject__`'"):
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
                foo2 := 'Alice'
            };
            INSERT test::TestSelfLink2 {
                foo2 := 'Bob'
            };
            INSERT test::TestSelfLink2 {
                foo2 := 'Carol'
            };

            WITH MODULE test
            SELECT TestSelfLink2 {
                foo2,
                bar2,
            } ORDER BY TestSelfLink2.foo2;
        """, [
            [1],
            [1],
            [1],
            [
                {'bar2': {}, 'foo2': 'Alice'},
                {'bar2': {'Alice'}, 'foo2': 'Bob'},
                {'bar2': {'Alice', 'Bob'}, 'foo2': 'Carol'}
            ],
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
                        # NOTE: this is a set of all TestSelfLink3.foo3
                        SET default := test::TestSelfLink3.foo3;
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

    async def test_edgeql_ddl_18(self):
        await self.con.execute("""
            CREATE MODULE foo;
            CREATE MODULE bar;

            SET MODULE foo, b := MODULE bar;

            CREATE SCALAR TYPE foo_t EXTENDING int64 {
                CREATE CONSTRAINT expression ON (__subject__ > 0);
            };

            CREATE SCALAR TYPE b::bar_t EXTENDING int64;

            CREATE TYPE Obj {
                CREATE PROPERTY foo -> foo_t;
                CREATE PROPERTY bar -> b::bar_t;
            };

            CREATE TYPE b::Obj2 {
                CREATE LINK obj -> Obj;
            };
        """)

        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT ScalarType {
                name,
                constraints: {
                    name
                }
            }
            FILTER .name LIKE '%bar%' OR .name LIKE '%foo%'
            ORDER BY .name;
        """, [
            [
                {'name': 'bar::bar_t', 'constraints': []},
                {'name': 'foo::foo_t', 'constraints': [
                    {'name': 'std::expression'}
                ]},
            ]
        ])

    async def test_edgeql_ddl_19(self):
        await self.con.execute("""
            SET MODULE test;

            CREATE TYPE ActualType {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            CREATE VIEW View1 := ActualType {
                bar := 9
            };

            CREATE VIEW View2 := ActualType {
                connected := (SELECT View1 ORDER BY View1.foo)
            };
        """)

        await self.assert_query_result(r"""
            SET MODULE test;

            INSERT ActualType {
                foo := 'obj1'
            };
            INSERT ActualType {
                foo := 'obj2'
            };

            SELECT View2 {
                foo,
                connected: {
                    foo,
                    bar
                }
            }
            ORDER BY View2.foo;
        """, [
            None,
            [1],
            [1],

            [
                {
                    'foo': 'obj1',
                    'connected': [{
                        'foo': 'obj1',
                        'bar': 9,
                    }, {
                        'foo': 'obj2',
                        'bar': 9,
                    }],
                },
                {
                    'foo': 'obj2',
                    'connected': [{
                        'foo': 'obj1',
                        'bar': 9,
                    }, {
                        'foo': 'obj2',
                        'bar': 9,
                    }],
                }
            ]
        ])

    async def test_edgeql_ddl_20(self):
        with self.assertRaisesRegex(
                client_errors.EdgeQLError,
                r'cannot create a function.+any.+cannot '
                r'have a non-empty default'):
            await self.con.execute(r"""
                CREATE FUNCTION test::my_agg(s: any = [1]) -> array<any>
                    FROM SQL FUNCTION "my_agg";
            """)

    async def test_edgeql_ddl_21(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'unqualified name and no default module set'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr array;
            """)

    async def test_edgeql_ddl_22(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'unqualified name and no default module set'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr tuple;
            """)

    async def test_edgeql_ddl_23(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr
                    array<int64, int64, int64>;
            """)

    async def test_edgeql_ddl_24(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'nested arrays are not supported'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr array<array<int64>>;
            """)

    async def test_edgeql_ddl_25(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'mixing named and unnamed tuple declaration is not '
                r'supported'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr
                    tuple<int64, foo:int64>;
            """)

    async def test_edgeql_ddl_26(self):
        with self.assertRaisesRegex(
                client_errors.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.con.execute(r"""
                CREATE ABSTRACT ATTRIBUTE test::bad_attr array<>;
            """)

    async def test_edgeql_ddl_27(self):
        with self.assertRaisesRegex(
                client_errors.EdgeQLError,
                r'invalid declaration.*unexpected type of the default'):

            await self.con.execute("""
                CREATE FUNCTION test::ddlf_1(s: std::str = 1) -> std::str
                    FROM EdgeQL $$ SELECT "1" $$;
            """)

    async def test_edgeql_ddl_28(self):
        try:
            await self.con.execute("""
                CREATE FUNCTION test::ddlf_2(
                    NAMED ONLY a: int64,
                    NAMED ONLY b: int64
                ) -> std::str
                    FROM EdgeQL $$ SELECT "1" $$;
            """)

            with self.assertRaisesRegex(
                    client_errors.EdgeQLError,
                    r'already defined'):

                await self.con.execute("""
                    CREATE FUNCTION test::ddlf_2(
                        NAMED ONLY b: int64,
                        NAMED ONLY a: int64 = 1
                    ) -> std::str
                        FROM EdgeQL $$ SELECT "1" $$;
                """)

            await self.con.execute("""
                CREATE FUNCTION test::ddlf_2(
                    NAMED ONLY b: str,
                    NAMED ONLY a: int64
                ) -> std::str
                    FROM EdgeQL $$ SELECT "2" $$;
            """)

            await self.assert_query_result(r'''
                SELECT test::ddlf_2(a:=1, b:=1);
                SELECT test::ddlf_2(a:=1, b:='a');
            ''', [
                ['1'],
                ['2'],
            ])

        finally:
            await self.con.execute("""
                DROP FUNCTION test::ddlf_2(
                    NAMED ONLY a: int64,
                    NAMED ONLY b: int64
                );

                DROP FUNCTION test::ddlf_2(
                    NAMED ONLY b: str,
                    NAMED ONLY a: int64
                );
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_29(self):
        try:
            await self.con.execute('START TRANSACTION')

            with self.assertRaises(client_errors.EdgeQLError):
                await self.con.execute("""
                    CREATE FUNCTION test::ddlf_2(
                        a: int64
                    ) -> int64
                        FROM EdgeQL $$ SELECT sum({$a}) $$;
                """)
        finally:
            await self.con.execute('ROLLBACK')
