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

import edgedb

from edb.server import _testbase as tb


class TestEdgeQLDDL(tb.DDLTestCase):

    async def test_edgeql_ddl_01(self):
        await self.query("""
            CREATE ABSTRACT LINK test::test_link;
        """)

    async def test_edgeql_ddl_02(self):
        await self.query("""
            CREATE ABSTRACT LINK test::test_object_link {
                CREATE PROPERTY test::test_link_prop -> std::int64;
            };

            CREATE TYPE test::TestObjectType {
                CREATE LINK test::test_object_link -> std::Object {
                    CREATE PROPERTY test::test_link_prop -> std::int64 {
                        SET ATTRIBUTE title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl_03(self):
        await self.query("""
            CREATE ABSTRACT LINK test::test_object_link_prop {
                CREATE PROPERTY test::link_prop1 -> std::str;
            };
        """)

    async def test_edgeql_ddl_04(self):
        await self.query("""
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

    async def test_edgeql_ddl_type_05(self):
        await self.query("""
            CREATE TYPE test::A5;
            CREATE TYPE test::Object5 {
                CREATE REQUIRED LINK test::a -> test::A5;
                CREATE REQUIRED PROPERTY test::b -> str;
            };
        """)

        await self.assert_query_result("""
            SELECT schema::ObjectType {
                links: {
                    name,
                    required,
                }
                FILTER .name = 'test::a'
                ORDER BY .name,

                properties: {
                    name,
                    required,
                }
                FILTER .name = 'test::b'
                ORDER BY .name
            }
            FILTER .name = 'test::Object5';
        """, [
            [{
                'links': [{
                    'name': 'test::a',
                    'required': True,
                }],

                'properties': [{
                    'name': 'test::b',
                    'required': True,
                }],
            }],
        ])

        await self.query("""
            ALTER TYPE test::Object5 {
                ALTER LINK test::a DROP REQUIRED;
            };

            ALTER TYPE test::Object5 {
                ALTER PROPERTY test::b DROP REQUIRED;
            };
        """)

        await self.assert_query_result("""
            SELECT schema::ObjectType {
                links: {
                    name,
                    required,
                }
                FILTER .name = 'test::a'
                ORDER BY .name,

                properties: {
                    name,
                    required,
                }
                FILTER .name = 'test::b'
                ORDER BY .name
            }
            FILTER .name = 'test::Object5';
        """, [
            [{
                'links': [{
                    'name': 'test::a',
                    'required': False,
                }],

                'properties': [{
                    'name': 'test::b',
                    'required': False,
                }],
            }],
        ])

    async def test_edgeql_ddl_type_06(self):
        await self.query("""
            CREATE TYPE test::A6;
            CREATE TYPE test::Object6 {
                CREATE SINGLE LINK test::a -> test::A6;
                CREATE SINGLE PROPERTY test::b -> str;
            };
        """)

        await self.assert_query_result("""
            SELECT schema::ObjectType {
                links: {
                    name,
                    cardinality,
                }
                FILTER .name = 'test::a'
                ORDER BY .name,

                properties: {
                    name,
                    cardinality,
                }
                FILTER .name = 'test::b'
                ORDER BY .name
            }
            FILTER .name = 'test::Object6';
        """, [
            [{
                'links': [{
                    'name': 'test::a',
                    'cardinality': 'ONE',
                }],

                'properties': [{
                    'name': 'test::b',
                    'cardinality': 'ONE',
                }],
            }],
        ])

        await self.query("""
            ALTER TYPE test::Object6 {
                ALTER LINK test::a SET MULTI;
            };

            ALTER TYPE test::Object6 {
                ALTER PROPERTY test::b SET MULTI;
            };
        """)

        await self.assert_query_result("""
            SELECT schema::ObjectType {
                links: {
                    name,
                    cardinality,
                }
                FILTER .name = 'test::a'
                ORDER BY .name,

                properties: {
                    name,
                    cardinality,
                }
                FILTER .name = 'test::b'
                ORDER BY .name
            }
            FILTER .name = 'test::Object6';
        """, [
            [{
                'links': [{
                    'name': 'test::a',
                    'cardinality': 'MANY',
                }],

                'properties': [{
                    'name': 'test::b',
                    'cardinality': 'MANY',
                }],
            }],
        ])

    async def test_edgeql_ddl_05(self):
        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            await self.query("""
                CREATE FUNCTION test::my_lower(s: std::str) -> std::str
                    FROM SQL FUNCTION 'lower';

                CREATE FUNCTION test::my_lower(s: SET OF std::str)
                    -> std::str {
                    SET initial_value := '';
                    FROM SQL FUNCTION 'count';
                };
            """)

        await self.query("""
            DROP FUNCTION test::my_lower(s: std::str);
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            await self.query("""
                CREATE FUNCTION test::my_lower(s: SET OF anytype)
                    -> std::str {
                    FROM SQL FUNCTION 'count';
                    SET initial_value := '';
                };

                CREATE FUNCTION test::my_lower(s: anytype) -> std::str
                    FROM SQL FUNCTION 'lower';
            """)

        await self.query("""
            DROP FUNCTION test::my_lower(s: anytype);
        """)

    async def test_edgeql_ddl_06(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.query(f"""
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

            CREATE FUNCTION test::my_sql_func6(a: std::str='a' ++ 'b')
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

        await self.query(f"""
            DROP FUNCTION test::my_sql_func1();
            DROP FUNCTION test::my_sql_func2(foo: std::str);
            DROP FUNCTION test::my_sql_func4(VARIADIC s: std::str);
            DROP FUNCTION test::{long_func_name}();
            DROP FUNCTION test::my_sql_func6(a: std::str='a' ++ 'b');
            DROP FUNCTION test::my_sql_func7(s: array<std::int64>);
        """)

    async def test_edgeql_ddl_07(self):
        with self.assertRaisesRegex(edgedb.InvalidFunctionDefinitionError,
                                    r'invalid default value'):
            await self.query(f"""
                CREATE FUNCTION test::broken_sql_func1(
                    a: std::int64=(SELECT schema::ObjectType))
                -> std::str
                FROM SQL $$
                    SELECT 'spam'::text
                $$;
            """)

    async def test_edgeql_ddl_08(self):
        await self.query(f"""
            CREATE FUNCTION test::my_edgeql_func1()
                -> std::str
                FROM EdgeQL $$
                    SELECT 'sp' ++ 'am'
                $$;

            CREATE FUNCTION test::my_edgeql_func2(s: std::str)
                -> schema::ObjectType
                FROM EdgeQL $$
                    SELECT
                        schema::ObjectType
                    FILTER schema::ObjectType.name = s
                $$;

            CREATE FUNCTION test::my_edgeql_func3(s: std::int64)
                -> std::int64
                FROM EdgeQL $$
                    SELECT s + 10
                $$;

            CREATE FUNCTION test::my_edgeql_func4(i: std::int64)
                -> array<std::int64>
                FROM EdgeQL $$
                    SELECT [i, 1, 2, 3]
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

        await self.query(f"""
            DROP FUNCTION test::my_edgeql_func1();
            DROP FUNCTION test::my_edgeql_func2(s: std::str);
            DROP FUNCTION test::my_edgeql_func3(s: std::int64);
            DROP FUNCTION test::my_edgeql_func4(i: std::int64);
        """)

    async def test_edgeql_ddl_09(self):
        await self.query("""
            CREATE FUNCTION test::attr_func_1() -> std::str {
                SET ATTRIBUTE description := 'hello';
                FROM EdgeQL "SELECT '1'";
            };
        """)

        await self.assert_query_result(r"""
            SELECT schema::Function {
                attributes: {
                    @value
                } FILTER .name = 'std::description'
            } FILTER .name = 'test::attr_func_1';
        """, [
            [{
                'attributes': [{
                    '@value': 'hello'
                }]
            }],
        ])

        await self.query("""
            DROP FUNCTION test::attr_func_1();
        """)

    async def test_edgeql_ddl_10(self):
        await self.query("""
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
        await self.query(r"""
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
                edgedb.EdgeQLSyntaxError,
                r"Unexpected '`__subject__`'"):
            await self.query(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY test::foo -> std::str {
                        CREATE CONSTRAINT expression
                            ON (`__subject__` = 'foo');
                    };
                };
            """)

    async def test_edgeql_ddl_13(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'reference to a non-existent schema item: self'):
            await self.query(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY test::foo -> std::str {
                        CREATE CONSTRAINT expression ON (`self` = 'foo');
                    };
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_14(self):
        await self.query("""
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
        await self.query(r"""
            CREATE TYPE test::TestSelfLink2 {
                CREATE PROPERTY test::foo2 -> std::str;
                CREATE MULTI PROPERTY test::bar2 -> std::str {
                    # NOTE: this is a set of all TestSelfLink2.foo2
                    SET default := test::TestSelfLink2.foo2;
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
        with self.assertRaisesRegex(edgedb.QueryError):
            await self.query(r"""
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
        await self.query("""
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
        await self.query("""
            CREATE MODULE foo;
            CREATE MODULE bar;

            SET MODULE foo, ALIAS b AS MODULE bar;

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
        await self.query("""
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

    async def test_edgeql_ddl_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r'unqualified name and no default module set'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> array;
                };
            """)

    async def test_edgeql_ddl_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r'unqualified name and no default module set'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> tuple;
                };
            """)

    async def test_edgeql_ddl_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> array<int64, int64, int64>;
                };
            """)

    async def test_edgeql_ddl_bad_04(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'nested arrays are not supported'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> array<array<int64>>;
                };
            """)

    async def test_edgeql_ddl_bad_05(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r'mixing named and unnamed tuple declaration is not '
                r'supported'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> tuple<int64, foo:int64>;
                };
            """)

    async def test_edgeql_ddl_bad_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.query(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::bar -> array<>;
                };
            """)

    async def test_edgeql_ddl_link_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            await self.query("""
                CREATE ABSTRACT LINK test::f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789;
            """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            await self.query("""
                CREATE TYPE test::Foo {
                    CREATE LINK test::f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789 -> test::Foo;
                };
            """)

    async def test_edgeql_ddl_prop_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            await self.query("""
                CREATE ABSTRACT PROPERTY test::f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789;
            """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            await self.query("""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY test::f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789 -> std::str;
                };
            """)

    async def test_edgeql_ddl_function_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::my_agg.*function:.+anytype.+cannot '
                r'have a non-empty default'):
            await self.query(r"""
                CREATE FUNCTION test::my_agg(
                        s: anytype = [1]) -> array<anytype>
                    FROM SQL FUNCTION "my_agg";
            """)

    async def test_edgeql_ddl_function_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'invalid declaration.*unexpected type of the default'):

            await self.query("""
                CREATE FUNCTION test::ddlf_1(s: std::str = 1) -> std::str
                    FROM EdgeQL $$ SELECT "1" $$;
            """)

    async def test_edgeql_ddl_28(self):
        await self.query("""
            CREATE FUNCTION test::ddlf_2(
                NAMED ONLY a: int64,
                NAMED ONLY b: int64
            ) -> std::str
                FROM EdgeQL $$ SELECT "1" $$;
        """)

        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'already defined'):

            await self.query("""
                CREATE FUNCTION test::ddlf_2(
                    NAMED ONLY b: int64,
                    NAMED ONLY a: int64 = 1
                ) -> std::str
                    FROM EdgeQL $$ SELECT "1" $$;
            """)

        await self.query("""
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

    async def test_edgeql_ddl_31(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter `sum` is not callable'):

            await self.query('''
                CREATE FUNCTION test::ddlf_4(
                    sum: int64
                ) -> int64
                    FROM EdgeQL $$
                        SELECT <int64>sum(sum)
                    $$;
            ''')

    async def test_edgeql_ddl_32(self):
        await self.query(r'''
            CREATE FUNCTION test::ddlf_5_1() -> str
                FROM EdgeQL $$
                    SELECT '\u0062'
                $$;

            CREATE FUNCTION test::ddlf_5_2() -> str
                FROM EdgeQL $$
                    SELECT r'\u0062'
                $$;

            CREATE FUNCTION test::ddlf_5_3() -> str
                FROM EdgeQL $$
                    SELECT $a$\u0062$a$
                $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::ddlf_5_1();
                SELECT test::ddlf_5_2();
                SELECT test::ddlf_5_3();
            ''', [
                ['b'],
                [r'\u0062'],
                [r'\u0062'],
            ])
        finally:
            await self.query("""
                DROP FUNCTION test::ddlf_5_1();
                DROP FUNCTION test::ddlf_5_2();
                DROP FUNCTION test::ddlf_5_3();
            """)

    async def test_edgeql_ddl_33(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'cannot create.*test::ddlf_6\(a: std::int64\).*'
                r'function with the same signature is already defined'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_6(a: int64) -> int64
                    FROM EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_6(a: int64) -> float64
                    FROM EdgeQL $$ SELECT 11 $$;
            ''')

    async def test_edgeql_ddl_34(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'cannot create.*test::ddlf_7\(a: SET OF std::int64\).*'
                r'SET OF parameters in user-defined EdgeQL functions are '
                r'not supported'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_7(a: SET OF int64) -> int64
                    FROM EdgeQL $$ SELECT 11 $$;
            ''')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.query("""
                DROP FUNCTION test::ddlf_7(a: SET OF int64);
            """)

    async def test_edgeql_ddl_35(self):
        await self.query(r'''
            CREATE FUNCTION test::ddlf_8(
                    a: int64, NAMED ONLY f: int64) -> int64
                FROM EdgeQL $$ SELECT 11 $$;

            CREATE FUNCTION test::ddlf_8(
                    a: int32, NAMED ONLY f: str) -> int64
                FROM EdgeQL $$ SELECT 12 $$;
        ''')

        try:
            await self.assert_query_result(r'''
                SELECT test::ddlf_8(<int64>10, f := 11);
                SELECT test::ddlf_8(<int32>10, f := '11');
            ''', [
                [11],
                [12],
            ])
        finally:
            await self.query("""
                DROP FUNCTION test::ddlf_8(a: int64, NAMED ONLY f: int64);
                DROP FUNCTION test::ddlf_8(a: int32, NAMED ONLY f: str);
            """)

    async def test_edgeql_ddl_36(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_9.*NAMED ONLY h:.*'
                r'different named only parameters'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_9(
                        a: int64, NAMED ONLY f: int64) -> int64
                    FROM EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_9(
                        a: int32, NAMED ONLY h: str) -> int64
                    FROM EdgeQL $$ SELECT 12 $$;
            ''')

    async def test_edgeql_ddl_37(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create the polymorphic.*test::ddlf_10.*'
                r'function with different return type'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_10(
                        a: anytype, b: int64) -> OPTIONAL int64
                    FROM EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_10(a: anytype, b: float64) -> str
                    FROM EdgeQL $$ SELECT '12' $$;
            ''')

    async def test_edgeql_ddl_38(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_11.*'
                r'overloading "FROM SQL FUNCTION"'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_11(str: std::str) -> int64
                    FROM SQL FUNCTION 'whatever';

                CREATE FUNCTION test::ddlf_11(str: std::int64) -> int64
                    FROM SQL FUNCTION 'whatever2';
            ''')

        await self.query("""
            DROP FUNCTION test::ddlf_11(str: std::str);
        """)

    async def test_edgeql_ddl_39(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_12.*'
                r'function returns a generic type but has no '
                r'generic parameters'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_12(str: std::str) -> anytype
                    FROM EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_40(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r'reference to a non-existent schema item: std::anytype'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_13(f: std::anytype) -> int64
                    FROM EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_41(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'functions can only contain one statement'):

            await self.query(r'''
                CREATE FUNCTION test::ddlf_14(f: int64) -> int64
                    FROM EdgeQL $$ SELECT 1; SELECT f; $$;
            ''')

    async def test_edgeql_ddl_module_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'spam' is already present in the schema"):

            await self.query('''\
                CREATE MODULE spam;
                CREATE MODULE spam;
            ''')

    async def test_edgeql_ddl_operator_01(self):
        await self.query('''
            CREATE INFIX OPERATOR test::`+`
                (left: int64, right: int64) -> int64
                FROM SQL OPERATOR r'+';
        ''')

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT Operator {
                name,
                params: {
                    name,
                    type: {
                        name
                    },
                    typemod
                } ORDER BY .name,
                operator_kind,
                return_typemod
            }
            FILTER
                .name = 'test::+';
        ''', [
            [{
                'name': 'test::+',
                'params': [
                    {
                        'name': 'left',
                        'type': {
                            'name': 'std::int64'
                        },
                        'typemod': 'SINGLETON'
                    },
                    {
                        'name': 'right',
                        'type': {
                            'name': 'std::int64'
                        },
                        'typemod': 'SINGLETON'}
                ],
                'operator_kind': 'INFIX',
                'return_typemod': 'SINGLETON'
            }]
        ])

        await self.query('''
            ALTER INFIX OPERATOR test::`+`
                (left: int64, right: int64)
                SET ATTRIBUTE description := 'my plus';
        ''')

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT Operator {
                name,
            }
            FILTER
                .name = 'test::+'
                AND .attributes.name = 'std::description'
                AND .attributes@value = 'my plus';
        ''', [
            [{
                'name': 'test::+',
            }]
        ])

        await self.query("""
            DROP INFIX OPERATOR test::`+` (left: int64, right: int64);
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT Operator {
                name,
                params: {
                    name,
                    type: {
                        name
                    },
                    typemod
                },
                operator_kind,
                return_typemod
            }
            FILTER
                .name = 'test::+';
        ''', [
            []
        ])

    async def test_edgeql_ddl_operator_02(self):
        try:
            await self.query('''
                CREATE POSTFIX OPERATOR test::`!`
                    (operand: int64) -> int64
                    FROM SQL OPERATOR r'!';

                CREATE PREFIX OPERATOR test::`!`
                    (operand: int64) -> int64
                    FROM SQL OPERATOR r'!!';
            ''')

            await self.assert_query_result('''
                WITH MODULE schema
                SELECT Operator {
                    name,
                    operator_kind,
                }
                FILTER
                    .name = 'test::!'
                ORDER BY
                    .operator_kind;
            ''', [
                [{
                    'name': 'test::!',
                    'operator_kind': 'POSTFIX',
                }, {
                    'name': 'test::!',
                    'operator_kind': 'PREFIX',
                }]
            ])

        finally:
            await self.query('''
                DROP POSTFIX OPERATOR test::`!`
                    (operand: int64);

                DROP PREFIX OPERATOR test::`!`
                    (operand: int64);
            ''')

    async def test_edgeql_ddl_cast_01(self):
        await self.query('''
            CREATE SCALAR TYPE test::type_a EXTENDING std::str;
            CREATE SCALAR TYPE test::type_b EXTENDING std::int64;
            CREATE SCALAR TYPE test::type_c EXTENDING std::datetime;

            CREATE CAST FROM test::type_a TO test::type_b {
                FROM SQL CAST;
                ALLOW IMPLICIT;
            };

            CREATE CAST FROM test::type_a TO test::type_c {
                FROM SQL CAST;
                ALLOW ASSIGNMENT;
            };
        ''')

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT Cast {
                from_type: {name},
                to_type: {name},
                allow_implicit,
                allow_assignment,
            }
            FILTER
                .from_type.name LIKE 'test::%'
            ORDER BY
                .allow_implicit;
        ''', [
            [{
                'from_type': {'name': 'test::type_a'},
                'to_type': {'name': 'test::type_c'},
                'allow_implicit': False,
                'allow_assignment': True,
            }, {
                'from_type': {'name': 'test::type_a'},
                'to_type': {'name': 'test::type_b'},
                'allow_implicit': True,
                'allow_assignment': False,
            }]
        ])

        await self.query("""
            DROP CAST FROM test::type_a TO test::type_b;
            DROP CAST FROM test::type_a TO test::type_c;
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT Cast {
                from_type: {name},
                to_type: {name},
                allow_implicit,
                allow_assignment,
            }
            FILTER
                .from_type.name LIKE 'test::%'
            ORDER BY
                .allow_implicit;
        ''', [
            []
        ])

    async def test_edgeql_ddl_property_computable_01(self):
        await self.query('''\
            CREATE TYPE test::CompProp;
            ALTER TYPE test::CompProp {
                CREATE PROPERTY test::prop := 'I am a computable';
            };
            INSERT test::CompProp;
        ''')

        await self.assert_query_result('''
            SELECT test::CompProp {
                prop
            };

            WITH MODULE schema
            SELECT ObjectType {
                properties: {
                    name,
                    target: {
                        name
                    }
                } FILTER .name = 'test::prop'
            }
            FILTER
                .name = 'test::CompProp';

        ''', [
            [{
                'prop': 'I am a computable',
            }],
            [{
                'properties': [{
                    'name': 'test::prop',
                    'target': {
                        'name': 'std::str'
                    }
                }]
            }]
        ])

    async def test_edgeql_ddl_property_computable_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property target, expected.*, got 'std::Object'"):
            await self.query('''\
                CREATE TYPE test::CompPropBad;
                ALTER TYPE test::CompPropBad {
                    CREATE PROPERTY test::prop := (SELECT std::Object LIMIT 1);
                };
            ''')

    async def test_edgeql_ddl_attribute_01(self):
        await self.query("""
            CREATE ABSTRACT ATTRIBUTE test::attr1;

            CREATE SCALAR TYPE test::TestAttrType1 EXTENDING std::str {
                SET ATTRIBUTE test::attr1 := 'aaaa';
            };
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType {
                attributes: {
                    name,
                    @value,
                }
            }
            FILTER
                .name = 'test::TestAttrType1';

        ''', [
            [{"attributes": [{"name": "test::attr1", "@value": "aaaa"}]}]
        ])

        await self.query("""
            CREATE MIGRATION test::mig1 TO eschema $$
            abstract attribute attr2

            scalar type TestAttrType1 extending std::str:
                attribute attr2 := 'aaaa'
            $$;

            COMMIT MIGRATION test::mig1;
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType {
                attributes: {
                    name,
                    @value,
                }
            }
            FILTER
                .name = 'test::TestAttrType1';

        ''', [
            [{"attributes": [{"name": "test::attr2", "@value": "aaaa"}]}]
        ])

    async def test_edgeql_ddl_attribute_02(self):
        await self.query("""
            CREATE ABSTRACT ATTRIBUTE test::attr1;

            CREATE TYPE test::TestAttrType2 {
                SET ATTRIBUTE test::attr1 := 'aaaa';
            };
        """)

        await self.query("""
            CREATE MIGRATION test::mig1 TO eschema $$
            abstract attribute attr2

            type TestAttrType2:
                attribute attr2 := 'aaaa'
            $$;

            COMMIT MIGRATION test::mig1;
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ObjectType {
                attributes: {
                    name,
                    @value,
                } FILTER .name = 'test::attr2'
            }
            FILTER
                .name = 'test::TestAttrType2';

        ''', [
            [{"attributes": [{"name": "test::attr2", "@value": "aaaa"}]}]
        ])

    async def test_edgeql_ddl_attribute_03(self):
        await self.query("""
            CREATE ABSTRACT ATTRIBUTE test::noninh;
            CREATE ABSTRACT INHERITABLE ATTRIBUTE test::inh;

            CREATE TYPE test::TestAttr1 {
                SET ATTRIBUTE test::noninh := 'no inherit';
                SET ATTRIBUTE test::inh := 'inherit me';
            };

            CREATE TYPE test::TestAttr2 EXTENDING test::TestAttr1;
        """)

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ObjectType {
                attributes: {
                    name,
                    inheritable,
                    @value,
                } ORDER BY .name
            }
            FILTER
                .name LIKE 'test::TestAttr%'
            ORDER BY
                .name;

        ''', [
            [{
                "attributes": [{
                    "name": "test::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }, {
                    "name": "test::noninh",
                    "@value": "no inherit",
                }]
            }, {
                "attributes": [{
                    "name": "test::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }]
            }]
        ])

    async def test_edgeql_ddl_anytype_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property target"):

            await self.query("""
                CREATE ABSTRACT LINK test::test_object_link_prop {
                    CREATE PROPERTY test::link_prop1 -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid link target"):

            await self.query("""
                CREATE TYPE test::AnyObject2 {
                    CREATE LINK test::a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property target"):

            await self.query("""
                CREATE TYPE test::AnyObject3 {
                    CREATE PROPERTY test::a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property target"):

            await self.query("""
                CREATE TYPE test::AnyObject4 {
                    CREATE PROPERTY test::a -> anyscalar;
                };
            """)

    async def test_edgeql_ddl_anytype_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property target"):

            await self.query("""
                CREATE TYPE test::AnyObject5 {
                    CREATE PROPERTY test::a -> anyint;
                };
            """)

    async def test_edgeql_ddl_anytype_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'anytype' cannot be a parent type"):

            await self.query("""
                CREATE TYPE test::AnyObject6 EXTENDING anytype {
                    CREATE REQUIRED LINK test::a -> test::AnyObject6;
                    CREATE REQUIRED PROPERTY test::b -> str;
                };
            """)

    async def test_edgeql_ddl_extending_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"Could not find consistent MRO for test::Merged1"):

            await self.query(r"""
                CREATE TYPE test::ExtA1;
                CREATE TYPE test::ExtB1;
                # create two types with incompatible linearized bases
                CREATE TYPE test::ExtC1 EXTENDING (test::ExtA1, test::ExtB1);
                CREATE TYPE test::ExtD1 EXTENDING (test::ExtB1, test::ExtA1);
                # extending from both of these incompatible types
                CREATE TYPE test::Merged1 EXTENDING (test::ExtC1, test::ExtD1);
            """)

    async def test_edgeql_ddl_extending_02(self):
        await self.query(r"""
            CREATE TYPE test::ExtA2;
            # Create two types with a different position of Object
            # in the bases. This doesn't impact the linearized
            # bases because Object is already implicitly included
            # as the first element of the base types.
            CREATE TYPE test::ExtC2 EXTENDING (test::ExtA2, Object);
            CREATE TYPE test::ExtD2 EXTENDING (Object, test::ExtA2);
            # extending from both of these types
            CREATE TYPE test::Merged2 EXTENDING (test::ExtC2, test::ExtD2);
        """)

    async def test_edgeql_ddl_extending_03(self):
        # Check that the mro is recomputed properly on rebase.
        await self.query(r"""
            CREATE TYPE test::ExtA3;
            CREATE TYPE test::ExtB3 EXTENDING test::ExtA3;
            CREATE TYPE test::ExtC3 EXTENDING test::ExtB3;
        """)

        await self.assert_query_result("""
            SELECT (SELECT schema::ObjectType
                    FILTER .name = 'test::ExtC3').mro.name;
        """, [
            {'std::Object', 'test::ExtA3', 'test::ExtB3'}
        ])

        await self.query(r"""
            ALTER TYPE test::ExtB3 DROP EXTENDING test::ExtA3;
        """)

        await self.assert_query_result("""
            SELECT (SELECT schema::ObjectType
                    FILTER .name = 'test::ExtC3').mro.name;
        """, [
            {'std::Object', 'test::ExtB3'}
        ])
