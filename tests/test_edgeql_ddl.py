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


import unittest

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLDDL(tb.DDLTestCase):

    async def test_edgeql_ddl_01(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_link;
        """)

    async def test_edgeql_ddl_02(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_object_link {
                CREATE PROPERTY test_link_prop -> std::int64;
            };

            CREATE TYPE test::TestObjectType {
                CREATE LINK test_object_link -> std::Object {
                    CREATE PROPERTY test_link_prop -> std::int64 {
                        SET ANNOTATION title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl_03(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::test_object_link_prop {
                CREATE PROPERTY link_prop1 -> std::str;
            };
        """)

    async def test_edgeql_ddl_04(self):
        await self.con.execute("""
            CREATE TYPE test::A;
            CREATE TYPE test::B EXTENDING test::A;

            CREATE TYPE test::Object1 {
                CREATE REQUIRED LINK a -> test::A;
            };

            CREATE TYPE test::Object2 {
                CREATE LINK a -> test::B;
            };

            CREATE TYPE test::Object_12
                EXTENDING test::Object1, test::Object2;
        """)

    async def test_edgeql_ddl_type_05(self):
        await self.con.execute("""
            CREATE TYPE test::A5;
            CREATE TYPE test::Object5 {
                CREATE REQUIRED LINK a -> test::A5;
                CREATE REQUIRED PROPERTY b -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    links: {
                        name,
                        required,
                    }
                    FILTER .name = 'a'
                    ORDER BY .name,

                    properties: {
                        name,
                        required,
                    }
                    FILTER .name = 'b'
                    ORDER BY .name
                }
                FILTER .name = 'test::Object5';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'required': True,
                }],

                'properties': [{
                    'name': 'b',
                    'required': True,
                }],
            }],
        )

        await self.con.execute("""
            ALTER TYPE test::Object5 {
                ALTER LINK a DROP REQUIRED;
            };

            ALTER TYPE test::Object5 {
                ALTER PROPERTY b DROP REQUIRED;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    links: {
                        name,
                        required,
                    }
                    FILTER .name = 'a'
                    ORDER BY .name,

                    properties: {
                        name,
                        required,
                    }
                    FILTER .name = 'b'
                    ORDER BY .name
                }
                FILTER .name = 'test::Object5';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'required': False,
                }],

                'properties': [{
                    'name': 'b',
                    'required': False,
                }],
            }],
        )

    async def test_edgeql_ddl_type_06(self):
        await self.con.execute("""
            CREATE TYPE test::A6 {
                CREATE PROPERTY name -> str;
            };

            CREATE TYPE test::Object6 {
                CREATE SINGLE LINK a -> test::A6;
                CREATE SINGLE PROPERTY b -> str;
            };

            INSERT test::A6 { name := 'a6' };
            INSERT test::Object6 {
                a := (SELECT test::A6 LIMIT 1),
                b := 'foo'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    links: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'a'
                    ORDER BY .name,

                    properties: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'b'
                    ORDER BY .name
                }
                FILTER .name = 'test::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'ONE',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'ONE',
                }],
            }],
        )

        await self.assert_query_result(
            r"""
            SELECT test::Object6 {
                a: {name},
                b,
            }
            """,
            [{
                'a': {'name': 'a6'},
                'b': 'foo',
            }]
        )

        await self.con.execute("""
            ALTER TYPE test::Object6 {
                ALTER LINK a SET MULTI;
            };

            ALTER TYPE test::Object6 {
                ALTER PROPERTY b SET MULTI;
            };
        """)

        await self.assert_query_result(
            """
                SELECT schema::ObjectType {
                    links: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'a'
                    ORDER BY .name,

                    properties: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'b'
                    ORDER BY .name
                }
                FILTER .name = 'test::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'MANY',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'MANY',
                }],
            }],
        )

        # Check that the data has been migrated correctly.
        await self.assert_query_result(
            r"""
            SELECT test::Object6 {
                a: {name},
                b,
            }
            """,
            [{
                'a': [{'name': 'a6'}],
                'b': ['foo'],
            }]
        )

        # Change it back.
        await self.con.execute("""
            ALTER TYPE test::Object6 {
                ALTER LINK a SET SINGLE;
            };

            ALTER TYPE test::Object6 {
                ALTER PROPERTY b SET SINGLE;
            };
        """)

        await self.assert_query_result(
            """
                SELECT schema::ObjectType {
                    links: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'a'
                    ORDER BY .name,

                    properties: {
                        name,
                        cardinality,
                    }
                    FILTER .name = 'b'
                    ORDER BY .name
                }
                FILTER .name = 'test::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'ONE',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'ONE',
                }],
            }],
        )

        # Check that the data has been migrated correctly.
        await self.assert_query_result(
            r"""
            SELECT test::Object6 {
                a: {name},
                b,
            }
            """,
            [{
                'a': {'name': 'a6'},
                'b': 'foo',
            }]
        )

    async def test_edgeql_ddl_05(self):
        await self.con.execute("""
            CREATE FUNCTION test::my_lower(s: std::str) -> std::str
                FROM SQL FUNCTION 'lower';
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION test::my_lower(s: SET OF std::str)
                        -> std::str {
                        SET initial_value := '';
                        FROM SQL FUNCTION 'count';
                    };
                """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: std::str);
        """)

        await self.con.execute("""
            CREATE FUNCTION test::my_lower(s: SET OF anytype)
                -> std::str {
                FROM SQL FUNCTION 'count';
                SET initial_value := '';
            };
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION test::my_lower(s: anytype) -> std::str
                        FROM SQL FUNCTION 'lower';
                """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: anytype);
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

        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func1();
            """,
            ['spam'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func2('foo');
            """,
            ['foo'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func4('fizz', 'buzz');
            """,
            ['fizz-buzz'],
        )
        await self.assert_query_result(
            fr"""
                SELECT test::{long_func_name}();
            """,
            [long_func_name],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func6();
            """,
            ['abc'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func6('xy');
            """,
            ['xyc'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_sql_func7([1, 2, 3, 10]);
            """,
            [16],
        )

        await self.con.execute(f"""
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

        await self.assert_query_result(
            r"""
                SELECT test::my_edgeql_func1();
            """,
            ['spam'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_edgeql_func2('schema::Object').name;
            """,
            ['schema::Object'],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_edgeql_func3(1);
            """,
            [11],
        )
        await self.assert_query_result(
            r"""
                SELECT test::my_edgeql_func4(42);
            """,
            [[42, 1, 2, 3]]
        )

        await self.con.execute(f"""
            DROP FUNCTION test::my_edgeql_func1();
            DROP FUNCTION test::my_edgeql_func2(s: std::str);
            DROP FUNCTION test::my_edgeql_func3(s: std::int64);
            DROP FUNCTION test::my_edgeql_func4(i: std::int64);
        """)

    async def test_edgeql_ddl_09(self):
        await self.con.execute("""
            CREATE FUNCTION test::attr_func_1() -> std::str {
                SET ANNOTATION description := 'hello';
                FROM EdgeQL "SELECT '1'";
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Function {
                    annotations: {
                        @value
                    } FILTER .name = 'std::description'
                } FILTER .name = 'test::attr_func_1';
            """,
            [{
                'annotations': [{
                    '@value': 'hello'
                }]
            }],
        )

        await self.con.execute("""
            DROP FUNCTION test::attr_func_1();
        """)

    async def test_edgeql_ddl_10(self):
        await self.con.execute("""
            CREATE FUNCTION test::int_func_1() -> std::int64 {
                FROM EdgeQL "SELECT 1";
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::int_func_1();
            """,
            [{}],
        )

    async def test_edgeql_ddl_11(self):
        await self.con.execute(r"""
            CREATE TYPE test::TestContainerLinkObjectType {
                CREATE PROPERTY test_array_link -> array<std::str>;
                # FIXME: for now dimention specs on the array are
                # disabled pending a syntax change
                # CREATE PROPERTY test_array_link_2 ->
                #     array<std::str[10]>;
            };
        """)

    async def test_edgeql_ddl_12(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r"Unexpected '`__subject__`'"):
            await self.con.execute(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT expression
                            ON (`__subject__` = 'foo');
                    };
                };
            """)

    async def test_edgeql_ddl_13(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'reference to a non-existent schema item: self'):
            await self.con.execute(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT expression ON (`self` = 'foo');
                    };
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_14(self):
        await self.con.execute("""
            CREATE TYPE test::TestSelfLink1 {
                CREATE PROPERTY foo1 -> std::str;
                CREATE PROPERTY bar1 -> std::str {
                    SET default := __source__.foo1;
                };
            };
        """)

        await self.con.execute(r"""
            INSERT test::TestSelfLink1 {
                foo1 := 'Victor'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT TestSelfLink1 {
                    foo1,
                    bar1,
                };
            """,
            [{'foo1': 'Victor', 'bar1': 'Victor'}]
        )

    async def test_edgeql_ddl_15(self):
        await self.con.execute(r"""
            CREATE TYPE test::TestSelfLink2 {
                CREATE PROPERTY foo2 -> std::str;
                CREATE MULTI PROPERTY bar2 -> std::str {
                    # NOTE: this is a set of all TestSelfLink2.foo2
                    SET default := test::TestSelfLink2.foo2;
                };
            };

            INSERT test::TestSelfLink2 {
                foo2 := 'Alice'
            };
            INSERT test::TestSelfLink2 {
                foo2 := 'Bob'
            };
            INSERT test::TestSelfLink2 {
                foo2 := 'Carol'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT TestSelfLink2 {
                    foo2,
                    bar2,
                } ORDER BY TestSelfLink2.foo2;
            """,
            [
                {'bar2': {}, 'foo2': 'Alice'},
                {'bar2': {'Alice'}, 'foo2': 'Bob'},
                {'bar2': {'Alice', 'Bob'}, 'foo2': 'Carol'}
            ],
        )

    @unittest.expectedFailure
    async def test_edgeql_ddl_16(self):
        # XXX: not sure what the error would say exactly, but
        # cardinality should be an issue here
        with self.assertRaisesRegex(edgedb.QueryError):
            await self.con.execute(r"""
                CREATE TYPE test::TestSelfLink3 {
                    CREATE PROPERTY foo3 -> std::str;
                    CREATE PROPERTY bar3 -> std::str {
                        # NOTE: this is a set of all TestSelfLink3.foo3
                        SET default := test::TestSelfLink3.foo3;
                    };
                };
            """)

    @unittest.expectedFailure
    async def test_edgeql_ddl_17(self):
        await self.con.execute("""
            CREATE TYPE test::TestSelfLink4 {
                CREATE PROPERTY __typename4 -> std::str {
                    SET default := __source__.__type__.name;
                };
            };
        """)

        await self.con.execute(r"""
            INSERT test::TestSelfLink4;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT TestSelfLink4 {
                    __typename4,
                };
            """,
            [{'__typename4': 'test::TestSelfLink4'}]
        )

    async def test_edgeql_ddl_18(self):
        await self.con.execute("""
            CREATE MODULE foo;
            CREATE MODULE bar;

            SET MODULE foo;
            SET ALIAS b AS MODULE bar;

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

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ScalarType {
                    name,
                    constraints: {
                        name
                    }
                }
                FILTER .name LIKE '%bar%' OR .name LIKE '%foo%'
                ORDER BY .name;
            """,
            [
                {'name': 'bar::bar_t', 'constraints': []},
                {'name': 'foo::foo_t', 'constraints': [
                    {'name': 'std::expression'}
                ]},
            ]
        )

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

            SET MODULE test;

            INSERT ActualType {
                foo := 'obj1'
            };
            INSERT ActualType {
                foo := 'obj2'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT View2 {
                    foo,
                    connected: {
                        foo,
                        bar
                    }
                }
                ORDER BY View2.foo;
            """,
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
        )

    async def test_edgeql_ddl_20(self):
        await self.con.execute("""
            SET MODULE test;

            CREATE TYPE A20 {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            CREATE TYPE B20 {
                CREATE LINK l -> A20;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    links: {
                        name,
                        bases: {
                            name
                        }
                    } FILTER .name = 'l'
                }
                FILTER .name = 'test::B20'
            """,
            [
                {
                    'links': [{
                        'name': 'l',
                        'bases': [{
                            'name': 'std::link',
                        }],
                    }],
                },
            ]
        )

        await self.con.execute("""
            SET MODULE test;

            CREATE ABSTRACT LINK l20;

            ALTER TYPE B20 {
                ALTER LINK l EXTENDING l20;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    links: {
                        name,
                        bases: {
                            name
                        }
                    } FILTER .name = 'l'
                }
                FILTER .name = 'test::B20'
            """,
            [
                {
                    'links': [{
                        'name': 'l',
                        'bases': [{
                            'name': 'test::l20',
                        }],
                    }],
                },
            ]
        )

        await self.con.execute("""
            SET MODULE test;

            ALTER TYPE B20 {
                ALTER LINK l DROP EXTENDING l20;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    links: {
                        name,
                        bases: {
                            name
                        }
                    } FILTER .name = 'l'
                }
                FILTER .name = 'test::B20'
            """,
            [
                {
                    'links': [{
                        'name': 'l',
                        'bases': [{
                            'name': 'std::link',
                        }],
                    }],
                },
            ]
        )

    async def test_edgeql_ddl_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"reference to a non-existant schema name 'array'"):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> array;
                };
            """)

    async def test_edgeql_ddl_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"reference to a non-existant schema name 'tuple'"):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> tuple;
                };
            """)

    async def test_edgeql_ddl_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> array<int64, int64, int64>;
                };
            """)

    async def test_edgeql_ddl_bad_04(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'nested arrays are not supported'):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> array<array<int64>>;
                };
            """)

    async def test_edgeql_ddl_bad_05(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r'mixing named and unnamed tuple declaration is not '
                r'supported'):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> tuple<int64, foo:int64>;
                };
            """)

    async def test_edgeql_ddl_bad_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> array<>;
                };
            """)

    async def test_edgeql_ddl_link_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT LINK test::f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789;
                """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE test::Foo {
                        CREATE LINK f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789 -> test::Foo;
                    };
                """)

    async def test_edgeql_ddl_link_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                f'unexpected fully-qualified name'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE test::Foo {
                        CREATE LINK foo::bar -> test::Foo;
                    };
                """)

    async def test_edgeql_ddl_link_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstact link"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT LINK test::bar {
                        SET default := Object;
                    };
                """)

    async def test_edgeql_ddl_prop_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT PROPERTY test::f123456789_123456789_\
23456789_123456789_123456789_123456789_123456789_123456789;
                """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'link or property name length exceeds the maximum'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE test::Foo {
                        CREATE PROPERTY f123456789_123456789_123456789_\
123456789_123456789_123456789_123456789_123456789 -> std::str;
                    };
                """)

    async def test_edgeql_ddl_property_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                f'unexpected fully-qualified name'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE test::Foo {
                        CREATE PROPERTY foo::bar -> test::Foo;
                    };
                """)

    async def test_edgeql_ddl_property_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstact property"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT PROPERTY test::bar {
                        SET default := 'bad';
                    };
                """)

    async def test_edgeql_ddl_function_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::my_agg.*function:.+anytype.+cannot '
                r'have a non-empty default'):
            await self.con.execute(r"""
                CREATE FUNCTION test::my_agg(
                        s: anytype = [1]) -> array<anytype>
                    FROM SQL FUNCTION "my_agg";
            """)

    async def test_edgeql_ddl_function_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'invalid declaration.*unexpected type of the default'):

            await self.con.execute("""
                CREATE FUNCTION test::ddlf_1(s: std::str = 1) -> std::str
                    FROM EdgeQL $$ SELECT "1" $$;
            """)

    async def test_edgeql_ddl_28(self):
        await self.con.execute("""
            CREATE FUNCTION test::ddlf_2(
                NAMED ONLY a: int64,
                NAMED ONLY b: int64
            ) -> std::str
                FROM EdgeQL $$ SELECT "1" $$;
        """)

        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'already defined'):

            async with self.con.transaction():
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

        await self.assert_query_result(
            r'''
                SELECT test::ddlf_2(a:=1, b:=1);
            ''',
            ['1'],
        )
        await self.assert_query_result(
            r'''
                SELECT test::ddlf_2(a:=1, b:='a');
            ''',
            ['2'],
        )

    async def test_edgeql_ddl_31(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter `sum` is not callable'):

            await self.con.execute('''
                CREATE FUNCTION test::ddlf_4(
                    sum: int64
                ) -> int64
                    FROM EdgeQL $$
                        SELECT <int64>sum(sum)
                    $$;
            ''')

    async def test_edgeql_ddl_32(self):
        await self.con.execute(r'''
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
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_5_1();
                ''',
                ['b'],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_5_2();
                ''',
                [r'\u0062'],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_5_3();
                ''',
                [r'\u0062'],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION test::ddlf_5_1();
                DROP FUNCTION test::ddlf_5_2();
                DROP FUNCTION test::ddlf_5_3();
            """)

    async def test_edgeql_ddl_33(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'cannot create.*test::ddlf_6\(a: std::int64\).*'
                r'function with the same signature is already defined'):

            await self.con.execute(r'''
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

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION test::ddlf_7(a: SET OF int64) -> int64
                        FROM EdgeQL $$ SELECT 11 $$;
                ''')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.execute("""
                DROP FUNCTION test::ddlf_7(a: SET OF int64);
            """)

    async def test_edgeql_ddl_35(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_8(
                    a: int64, NAMED ONLY f: int64) -> int64
                FROM EdgeQL $$ SELECT 11 $$;

            CREATE FUNCTION test::ddlf_8(
                    a: int32, NAMED ONLY f: str) -> int64
                FROM EdgeQL $$ SELECT 12 $$;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_8(<int64>10, f := 11);
                ''',
                [11],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_8(<int32>10, f := '11');
                ''',
                [12],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION test::ddlf_8(a: int64, NAMED ONLY f: int64);
                DROP FUNCTION test::ddlf_8(a: int32, NAMED ONLY f: str);
            """)

    async def test_edgeql_ddl_36(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_9.*NAMED ONLY h:.*'
                r'different named only parameters'):

            await self.con.execute(r'''
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

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_10(
                        a: anytype, b: int64) -> OPTIONAL int64
                    FROM EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_10(a: anytype, b: float64) -> str
                    FROM EdgeQL $$ SELECT '12' $$;
            ''')

    async def test_edgeql_ddl_38(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_11(str: std::str) -> int64
                FROM SQL FUNCTION 'whatever';
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_11.*'
                r'overloading "FROM SQL FUNCTION"'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION test::ddlf_11(str: std::int64) -> int64
                        FROM SQL FUNCTION 'whatever2';
                ''')

        await self.con.execute("""
            DROP FUNCTION test::ddlf_11(str: std::str);
        """)

    async def test_edgeql_ddl_39(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_12.*'
                r'function returns a generic type but has no '
                r'generic parameters'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_12(str: std::str) -> anytype
                    FROM EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_40(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r'reference to a non-existent schema item: std::anytype'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_13(f: std::anytype) -> int64
                    FROM EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_41(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'functions can only contain one statement'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_14(f: int64) -> int64
                    FROM EdgeQL $$ SELECT 1; SELECT f; $$;
            ''')

    async def test_edgeql_ddl_module_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'spam' is already present in the schema"):

            await self.con.execute('''\
                CREATE MODULE spam;
                CREATE MODULE spam;
            ''')

    async def test_edgeql_ddl_operator_01(self):
        await self.con.execute('''
            CREATE INFIX OPERATOR test::`+++`
                (left: int64, right: int64) -> int64
                FROM SQL OPERATOR r'+';
        ''')

        await self.assert_query_result(
            r'''
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
                    .name = 'test::+++';
            ''',
            [{
                'name': 'test::+++',
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
        )

        await self.con.execute('''
            ALTER INFIX OPERATOR test::`+++`
                (left: int64, right: int64)
                SET ANNOTATION description := 'my plus';
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Operator {
                    name,
                }
                FILTER
                    .name = 'test::+++'
                    AND .annotations.name = 'std::description'
                    AND .annotations@value = 'my plus';
            ''',
            [{
                'name': 'test::+++',
            }]
        )

        await self.con.execute("""
            DROP INFIX OPERATOR test::`+++` (left: int64, right: int64);
        """)

        await self.assert_query_result(
            r'''
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
                    .name = 'test::+++';
            ''',
            []
        )

    async def test_edgeql_ddl_operator_02(self):
        try:
            await self.con.execute('''
                CREATE POSTFIX OPERATOR test::`!`
                    (operand: int64) -> int64
                    FROM SQL OPERATOR r'!';

                CREATE PREFIX OPERATOR test::`!`
                    (operand: int64) -> int64
                    FROM SQL OPERATOR r'!!';
            ''')

            await self.assert_query_result(
                r'''
                    WITH MODULE schema
                    SELECT Operator {
                        name,
                        operator_kind,
                    }
                    FILTER
                        .name = 'test::!'
                    ORDER BY
                        .operator_kind;
                ''',
                [
                    {
                        'name': 'test::!',
                        'operator_kind': 'POSTFIX',
                    },
                    {
                        'name': 'test::!',
                        'operator_kind': 'PREFIX',
                    }
                ]
            )

        finally:
            await self.con.execute('''
                DROP POSTFIX OPERATOR test::`!`
                    (operand: int64);

                DROP PREFIX OPERATOR test::`!`
                    (operand: int64);
            ''')

    async def test_edgeql_ddl_operator_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the `test::NOT\(\)` operator: '
                r'an operator must have operands'):
            await self.con.execute('''
                CREATE PREFIX OPERATOR test::`NOT`() -> bool
                    FROM SQL EXPRESSION;
            ''')

    async def test_edgeql_ddl_operator_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`test::=\(l: std::array<anytype>, r: std::str\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: str) -> std::bool {
                    FROM SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`test::=\(l: std::array<anytype>, r: anytuple\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: anytuple) -> std::bool {
                    FROM SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the non-recursive '
                r'`std::=\(l: std::array<std::int64>, '
                r'r: std::array<std::int64>\)` operator: '
                r'overloading a recursive operator '
                r'`array<anytype> = array<anytype>` with a non-recursive one '
                r'is not allowed'):
            # attempt to overload a recursive `=` from std with a
            # non-recursive version
            await self.con.execute('''
                CREATE INFIX OPERATOR
                std::`=` (l: array<int64>, r: array<int64>) -> std::bool {
                    FROM SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the recursive '
                r'`test::=\(l: std::array<std::int64>, '
                r'r: std::array<std::int64>\)` operator: '
                r'overloading a non-recursive operator '
                r'`array<anytype> = array<anytype>` with a recursive one '
                r'is not allowed'):
            # create 2 operators in test: non-recursive first, then a
            # recursive one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: array<anytype>) -> std::bool {
                    FROM SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                test::`=` (l: array<int64>, r: array<int64>) -> std::bool {
                    FROM SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_cast_01(self):
        await self.con.execute('''
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

        await self.assert_query_result(
            r'''
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
            ''',
            [
                {
                    'from_type': {'name': 'test::type_a'},
                    'to_type': {'name': 'test::type_c'},
                    'allow_implicit': False,
                    'allow_assignment': True,
                },
                {
                    'from_type': {'name': 'test::type_a'},
                    'to_type': {'name': 'test::type_b'},
                    'allow_implicit': True,
                    'allow_assignment': False,
                }
            ]
        )

        await self.con.execute("""
            DROP CAST FROM test::type_a TO test::type_b;
            DROP CAST FROM test::type_a TO test::type_c;
        """)

        await self.assert_query_result(
            r'''
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
            ''',
            []
        )

    async def test_edgeql_ddl_property_computable_01(self):
        await self.con.execute('''\
            CREATE TYPE test::CompProp;
            ALTER TYPE test::CompProp {
                CREATE PROPERTY prop := 'I am a computable';
            };
            INSERT test::CompProp;
        ''')

        await self.assert_query_result(
            r'''
                SELECT test::CompProp {
                    prop
                };
            ''',
            [{
                'prop': 'I am a computable',
            }],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    properties: {
                        name,
                        target: {
                            name
                        }
                    } FILTER .name = 'prop'
                }
                FILTER
                    .name = 'test::CompProp';
            ''',
            [{
                'properties': [{
                    'name': 'prop',
                    'target': {
                        'name': 'std::str'
                    }
                }]
            }]
        )

    async def test_edgeql_ddl_property_computable_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type: expected.*, got 'std::Object'"):
            await self.con.execute('''\
                CREATE TYPE test::CompPropBad;
                ALTER TYPE test::CompPropBad {
                    CREATE PROPERTY prop := (SELECT std::Object LIMIT 1);
                };
            ''')

    async def test_edgeql_ddl_annotation_01(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::attr1;

            CREATE SCALAR TYPE test::TestAttrType1 EXTENDING std::str {
                SET ANNOTATION test::attr1 := 'aaaa';
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    annotations: {
                        name,
                        @value,
                    }
                }
                FILTER
                    .name = 'test::TestAttrType1';
            ''',
            [{"annotations": [{"name": "test::attr1", "@value": "aaaa"}]}]
        )

        await self.con.execute("""
            CREATE MIGRATION test::mig1 TO {
                abstract annotation attr2;

                scalar type TestAttrType1 extending std::str {
                    annotation attr2 := 'aaaa';
                };
            };

            COMMIT MIGRATION test::mig1;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    annotations: {
                        name,
                        @value,
                    }
                }
                FILTER
                    .name = 'test::TestAttrType1';
            ''',
            [{"annotations": [{"name": "test::attr2", "@value": "aaaa"}]}]
        )

    async def test_edgeql_ddl_annotation_02(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::attr1;

            CREATE TYPE test::TestAttrType2 {
                SET ANNOTATION test::attr1 := 'aaaa';
            };
        """)

        await self.con.execute("""
            CREATE MIGRATION test::mig1 TO {
                abstract annotation attr2;

                type TestAttrType2 {
                    annotation attr2 := 'aaaa';
                };
            };

            COMMIT MIGRATION test::mig1;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name = 'test::attr2'
                }
                FILTER
                    .name = 'test::TestAttrType2';
            ''',
            [{"annotations": [{"name": "test::attr2", "@value": "aaaa"}]}]
        )

    async def test_edgeql_ddl_annotation_03(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::noninh;
            CREATE ABSTRACT INHERITABLE ANNOTATION test::inh;

            CREATE TYPE test::TestAttr1 {
                SET ANNOTATION test::noninh := 'no inherit';
                SET ANNOTATION test::inh := 'inherit me';
            };

            CREATE TYPE test::TestAttr2 EXTENDING test::TestAttr1;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        inheritable,
                        @value,
                    }
                    FILTER .name LIKE 'test::%'
                    ORDER BY .name
                }
                FILTER
                    .name LIKE 'test::TestAttr%'
                ORDER BY
                    .name;
            ''',
            [{
                "annotations": [{
                    "name": "test::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }, {
                    "name": "test::noninh",
                    "@value": "no inherit",
                }]
            }, {
                "annotations": [{
                    "name": "test::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }]
            }]
        )

    async def test_edgeql_ddl_anytype_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE ABSTRACT LINK test::test_object_link_prop {
                    CREATE PROPERTY link_prop1 -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid link target"):

            await self.con.execute("""
                CREATE TYPE test::AnyObject2 {
                    CREATE LINK a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE test::AnyObject3 {
                    CREATE PROPERTY a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE test::AnyObject4 {
                    CREATE PROPERTY a -> anyscalar;
                };
            """)

    async def test_edgeql_ddl_anytype_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE test::AnyObject5 {
                    CREATE PROPERTY a -> anyint;
                };
            """)

    async def test_edgeql_ddl_anytype_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'anytype' cannot be a parent type"):

            await self.con.execute("""
                CREATE TYPE test::AnyObject6 EXTENDING anytype {
                    CREATE REQUIRED LINK a -> test::AnyObject6;
                    CREATE REQUIRED PROPERTY b -> str;
                };
            """)

    async def test_edgeql_ddl_extending_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"Could not find consistent ancestor order for "
                r"object type 'test::Merged1'"):

            await self.con.execute(r"""
                CREATE TYPE test::ExtA1;
                CREATE TYPE test::ExtB1;
                # create two types with incompatible linearized bases
                CREATE TYPE test::ExtC1 EXTENDING test::ExtA1, test::ExtB1;
                CREATE TYPE test::ExtD1 EXTENDING test::ExtB1, test::ExtA1;
                # extending from both of these incompatible types
                CREATE TYPE test::Merged1 EXTENDING test::ExtC1, test::ExtD1;
            """)

    async def test_edgeql_ddl_extending_02(self):
        await self.con.execute(r"""
            CREATE TYPE test::ExtA2;
            # Create two types with a different position of Object
            # in the bases. This doesn't impact the linearized
            # bases because Object is already implicitly included
            # as the first element of the base types.
            CREATE TYPE test::ExtC2 EXTENDING test::ExtA2, Object;
            CREATE TYPE test::ExtD2 EXTENDING Object, test::ExtA2;
            # extending from both of these types
            CREATE TYPE test::Merged2 EXTENDING test::ExtC2, test::ExtD2;
        """)

    async def test_edgeql_ddl_extending_03(self):
        # Check that ancestors are recomputed properly on rebase.
        await self.con.execute(r"""
            CREATE TYPE test::ExtA3;
            CREATE TYPE test::ExtB3 EXTENDING test::ExtA3;
            CREATE TYPE test::ExtC3 EXTENDING test::ExtB3;
        """)

        await self.assert_query_result(
            r"""
                SELECT (SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC3').ancestors.name;
            """,
            {'std::Object', 'test::ExtA3', 'test::ExtB3'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::ExtB3 DROP EXTENDING test::ExtA3;
        """)

        await self.assert_query_result(
            r"""
                SELECT (SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC3').ancestors.name;
            """,
            {'std::Object', 'test::ExtB3'}
        )

    @test.xfail('''
        This fails due to incomplete cleanup when DROP TYPE is executed,
        so the final DROP MODULE fails to execute since there are
        still objects within it.
    ''')
    async def test_edgeql_ddl_modules_01(self):
        try:
            await self.con.execute(r"""
                CREATE MODULE test_other;

                CREATE TYPE test::ModuleTest01 {
                    CREATE PROPERTY clash -> str;
                };

                CREATE TYPE test_other::ModuleTest01 {
                    CREATE LINK clash -> Object;
                };
            """)

            await self.con.execute("""
                DROP TYPE test_other::ModuleTest01;
            """)

        finally:
            await self.con.execute("""
                DROP MODULE test_other;
            """)

    async def test_edgeql_ddl_modules_02(self):
        await self.con.execute(r"""
            CREATE MODULE test_other;

            CREATE ABSTRACT TYPE test_other::Named {
                CREATE REQUIRED PROPERTY name -> str;
            };

            CREATE ABSTRACT TYPE test_other::UniquelyNamed
                EXTENDING test_other::Named
            {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE DELEGATED CONSTRAINT exclusive;
                }
            };

            CREATE TYPE test::Priority EXTENDING test_other::Named;

            CREATE TYPE test::Status
                EXTENDING test_other::UniquelyNamed;

            INSERT test::Priority {name := 'one'};
            INSERT test::Priority {name := 'two'};
            INSERT test::Status {name := 'open'};
            INSERT test::Status {name := 'closed'};
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test_other
                SELECT Named.name;
            """,
            {
                'one', 'two', 'open', 'closed',
            }
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test_other
                SELECT UniquelyNamed.name;
            """,
            {
                'open', 'closed',
            }
        )

        await self.con.execute("""
            DROP TYPE test::Status;
            DROP TYPE test::Priority;
            DROP TYPE test_other::UniquelyNamed;
            DROP TYPE test_other::Named;
            DROP MODULE test_other;
        """)

    @test.xfail('''
        Currently declarative.py doesn't have the up-to-date module list
        at the time it tries interpreting the migration.

        InvalidReferenceError: reference to a non-existent schema
        item: test_other::UniquelyNamed
    ''')
    async def test_edgeql_ddl_modules_03(self):
        await self.con.execute(r"""
            CREATE MODULE test_other;

            CREATE ABSTRACT TYPE test_other::Named {
                CREATE REQUIRED PROPERTY name -> str;
            };

            CREATE ABSTRACT TYPE test_other::UniquelyNamed
                EXTENDING test_other::Named
            {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE DELEGATED CONSTRAINT exclusive;
                }
            };
        """)

        try:
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE MIGRATION test::d1 TO {
                        type Status extending test_other::UniquelyNamed;
                    };
                    COMMIT MIGRATION test::d1;
                """)

            await self.con.execute("""
                DROP TYPE test::Status;
            """)
        finally:
            await self.con.execute("""
                DROP TYPE test_other::UniquelyNamed;
                DROP TYPE test_other::Named;
                DROP MODULE test_other;
            """)

    async def test_edgeql_ddl_role_01(self):
        await self.con.execute(r"""
            CREATE ROLE foo_01;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    allow_login,
                    is_superuser,
                    password,
                } FILTER .name = 'foo_01'
            """,
            [{
                'name': 'foo_01',
                'allow_login': False,
                'is_superuser': False,
                'password': None,
            }]
        )

    async def test_edgeql_ddl_role_02(self):
        await self.con.execute(r"""
            CREATE ROLE foo2 {
                SET allow_login := true;
                SET is_superuser := true;
                SET password := 'secret';
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    allow_login,
                    is_superuser,
                } FILTER .name = 'foo2'
            """,
            [{
                'name': 'foo2',
                'allow_login': True,
                'is_superuser': True,
            }]
        )

        role = await self.con.fetchone('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNotNone(role.password)

        await self.con.execute(r"""
            ALTER ROLE foo2 {
                SET password := {}
            };
        """)

        role = await self.con.fetchone('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNone(role.password)

    async def test_edgeql_ddl_role_03(self):
        await self.con.execute(r"""
            CREATE ROLE foo3 {
                SET allow_login := true;
                SET is_superuser := true;
                SET password := 'secret';
            };
        """)

        await self.con.execute(r"""
            CREATE ROLE foo4 EXTENDING foo3;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    allow_login,
                    is_superuser,
                    password,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo4'
            """,
            [{
                'name': 'foo4',
                'allow_login': False,
                'is_superuser': False,
                'password': None,
                'member_of': [{
                    'name': 'foo3'
                }]
            }]
        )

        await self.con.execute(r"""
            ALTER ROLE foo4 DROP EXTENDING foo3;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo4'
            """,
            [{
                'name': 'foo4',
                'member_of': [],
            }]
        )

        await self.con.execute(r"""
            ALTER ROLE foo4 EXTENDING foo3;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo4'
            """,
            [{
                'name': 'foo4',
                'member_of': [{
                    'name': 'foo3',
                }],
            }]
        )

    async def test_edgeql_ddl_rename_01(self):
        await self.con.execute(r"""
            CREATE TYPE test::RenameObj01 {
                CREATE PROPERTY name -> str;
            };

            INSERT test::RenameObj01 {name := 'rename 01'};

            ALTER TYPE test::RenameObj01 {
                RENAME TO test::NewNameObj01;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::NewNameObj01.name;
            ''',
            ['rename 01']
        )

    async def test_edgeql_ddl_rename_02(self):
        await self.con.execute(r"""
            CREATE TYPE test::RenameObj02 {
                CREATE PROPERTY name -> str;
            };

            INSERT test::RenameObj02 {name := 'rename 02'};

            ALTER TYPE test::RenameObj02 {
                ALTER PROPERTY name {
                    RENAME TO new_name_02;
                };
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::RenameObj02.new_name_02;
            ''',
            ['rename 02']
        )

    async def test_edgeql_ddl_rename_03(self):
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE test::RenameObj03 {
                CREATE PROPERTY name -> str;
            };

            INSERT RenameObj03 {name := 'rename 03'};

            ALTER TYPE RenameObj03 {
                ALTER PROPERTY name {
                    RENAME TO new_name_03;
                };
            };

            RESET MODULE;
        """)

        await self.assert_query_result(
            r'''
                SELECT test::RenameObj03.new_name_03;
            ''',
            ['rename 03']
        )

    @test.xfail('''
        The error is:
        column "test::new_prop_04" of relation "..." does not exist
    ''')
    async def test_edgeql_ddl_rename_04(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::rename_link_04 {
                CREATE PROPERTY rename_prop_04 -> std::int64;
            };

            CREATE TYPE test::LinkedObj04;
            CREATE TYPE test::RenameObj04 {
                CREATE MULTI LINK rename_link_04 -> test::LinkedObj04;
            };

            INSERT test::LinkedObj04;
            INSERT test::RenameObj04 {
                rename_link_04 := test::LinkedObj04 {@rename_prop_04 := 123}
            };

            ALTER ABSTRACT LINK test::rename_link_04 {
                ALTER PROPERTY rename_prop_04 {
                    RENAME TO new_prop_04;
                };
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::RenameObj04.rename_link_04@new_prop_04;
            ''',
            [123]
        )

    @test.xfail('''
        The error is:
        relation "edgedb_1e143af8-1929-...-311a41c76d4d" does not exist
    ''')
    async def test_edgeql_ddl_rename_05(self):
        await self.con.execute(r"""
            CREATE VIEW test::RenameView05 := (
                SELECT Object {
                    view_computable := 'rename view 05'
                }
            );

            ALTER VIEW test::RenameView05 {
                RENAME TO test::NewView05;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::NewView05.view_computable LIMIT 1;
            ''',
            ['rename view 05']
        )

    async def test_edgeql_ddl_inheritance_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE test::InhTest01 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE test::InhTest01_child EXTENDING test::InhTest01;
        """)

        await self.con.execute("""
            ALTER TYPE test::InhTest01 {
                DROP PROPERTY testp;
            }
        """)

    @test.xfail('''
        The error is: reference to a non-existent schema item

        A proper error should be something along the following lines:
        "Cannot drop property inherited property 'testp' from InhTest01_child"
    ''')
    async def test_edgeql_ddl_inheritance_alter_02(self):
        await self.con.execute(r"""
            CREATE TYPE test::InhTest01 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE test::InhTest01_child EXTENDING test::InhTest01;
        """)

        await self.con.execute("""
            ALTER TYPE test::InhTest01_child {
                DROP PROPERTY testp;
            }
        """)

    @test.xfail('''
        The error is:

        SchemaError: cannot derive <Link ...>(
            std::std|link@@std|
            Virtual_71a37e2cf26c98e5dad84176a6ffa910@test|Owner
        )
        from itself
    ''')
    async def test_edgeql_ddl_inheritance_alter_03(self):
        await self.con.execute(r"""
            CREATE TYPE test::Owner;

            CREATE TYPE test::Stuff1 {
                # same link name, but NOT related via explicit inheritance
                CREATE LINK owner -> test::Owner
            };

            CREATE TYPE test::Stuff2 {
                # same link name, but NOT related via explicit inheritance
                CREATE LINK owner -> test::Owner
            };
        """)

        await self.assert_query_result("""
            SELECT test::Owner.<owner;
        """, [{}])

    @test.xfail('''
        The error is:

        InvalidReferenceError: reference to a non-existent schema
        item: test::std|min_value@@test|test|con_test@@test|ConTest01

        Altering the constraint instead of dropping it also produces
        the same error.

        The current docs for DDL have a *very* similar example to this
        one, but which works fine. The difference is in the
        CREATE/ALTER sequence of the PROPERTY and CONSTRAINT.
    ''')
    async def test_edgeql_ddl_constraint_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE test::ConTest01 {
                CREATE PROPERTY con_test -> int64;
            };

            ALTER TYPE test::ConTest01
                ALTER PROPERTY con_test
                    CREATE CONSTRAINT min_value(0);
        """)

        await self.con.execute("""
            ALTER TYPE test::ConTest01
                ALTER PROPERTY con_test
                    DROP CONSTRAINT min_value;
        """)

        await self.assert_query_result("""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                properties: {
                    name,
                    constraints: { name }
                } FILTER .name = 'con_test'
            }
            FILTER .name = 'test::ConTest01';
        """, [[
            {
                'name': 'test::ConTest01',
                'properties': [{
                    'name': 'con_test',
                    'constraints': {},
                }]
            }
        ]])

    async def test_edgeql_ddl_tuple_properties(self):
        await self.con.execute(r"""
            CREATE TYPE test::TupProp01 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
                CREATE PROPERTY p2 -> tuple<foo: int64, bar: str>;
                CREATE PROPERTY p3 -> tuple<foo: int64,
                                            bar: tuple<json, json>>;
            };

            CREATE TYPE test::TupProp02 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
                CREATE PROPERTY p2 -> tuple<json, json>;
            };
        """)

        # Drop identical p1 properties from both objects,
        # to check positive refcount.
        await self.con.execute(r"""
            ALTER TYPE test::TupProp01 {
                DROP PROPERTY p1;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE test::TupProp02 {
                DROP PROPERTY p1;
            };
        """)

        # Re-create the property to check that the associated
        # composite type was actually removed.
        await self.con.execute(r"""
            ALTER TYPE test::TupProp02 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
            };
        """)

        # Now, drop the property that has a nested tuple that
        # is referred to directly by another property.
        await self.con.execute(r"""
            ALTER TYPE test::TupProp01 {
                DROP PROPERTY p3;
            };
        """)

        # Drop the last user.
        await self.con.execute(r"""
            ALTER TYPE test::TupProp02 {
                DROP PROPERTY p2;
            };
        """)

        # Re-create to assure cleanup.
        await self.con.execute(r"""
            ALTER TYPE test::TupProp02 {
                CREATE PROPERTY p3 -> tuple<json, json>;
                CREATE PROPERTY p4 -> tuple<a: json, b: json>;
            };
        """)

        await self.con.execute('DECLARE SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                'expected a scalar type, or a scalar collection'):

            await self.con.execute(r"""
                ALTER TYPE test::TupProp02 {
                    CREATE PROPERTY p4 -> tuple<test::TupProp02>;
                };
            """)

        # Recover.
        await self.con.execute('ROLLBACK TO SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'arrays of tuples are not supported'):

            await self.con.execute(r"""
                ALTER TYPE test::TupProp02 {
                    CREATE PROPERTY p4 -> array<tuple<int64>>;
                };
            """)

    async def test_edgeql_ddl_enum_01(self):
        await self.con.execute('''
            CREATE SCALAR TYPE test::my_enum EXTENDING enum<'foo', 'bar'>;
        ''')

        await self.assert_query_result(
            r"""
                SELECT schema::ScalarType {
                    enum_values,
                }
                FILTER .name = 'test::my_enum';
            """,
            [{
                'enum_values': ['foo', 'bar'],
            }],
        )

        await self.con.execute('''
            CREATE TYPE test::EnumHost {
                CREATE PROPERTY foo -> test::my_enum;
            }
        ''')

        await self.con.execute('DECLARE SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'enumeration must be the only supertype specified'):
            await self.con.execute('''
                CREATE SCALAR TYPE test::my_enum_2
                    EXTENDING enum<'foo', 'bar'>,
                    std::int32;
            ''')

        await self.con.execute('ROLLBACK TO SAVEPOINT t0;')

        await self.con.execute('''
            CREATE SCALAR TYPE test::my_enum_2
                EXTENDING enum<'foo', 'bar'>;
        ''')

        await self.con.execute('DECLARE SAVEPOINT t1;')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'altering enum composition is not supported'):
            await self.con.execute('''
                ALTER SCALAR TYPE test::my_enum_2
                    EXTENDING enum<'foo', 'bar', 'baz'>;
            ''')

        # Recover.
        await self.con.execute('ROLLBACK TO SAVEPOINT t1;')

        await self.con.execute('DECLARE SAVEPOINT t2;')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'constraints cannot be defined on an enumerated type'):
            await self.con.execute('''
                CREATE SCALAR TYPE test::my_enum_3
                    EXTENDING enum<'foo', 'bar', 'baz'> {
                    CREATE CONSTRAINT expression ON (__subject__ = 'bar')
                };
            ''')

        # Recover.
        await self.con.execute('ROLLBACK TO SAVEPOINT t2;')

        await self.con.execute('''
            ALTER SCALAR TYPE test::my_enum_2
                RENAME TO test::my_enum_3;
        ''')

        await self.con.execute('''
            DROP SCALAR TYPE test::my_enum_3;
        ''')
