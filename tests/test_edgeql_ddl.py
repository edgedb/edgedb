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

import decimal
import uuid

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
                        CREATE ANNOTATION title := 'Test Property';
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

    async def test_edgeql_ddl_11(self):
        await self.con.execute(r"""
            CREATE TYPE test::TestContainerLinkObjectType {
                CREATE PROPERTY test_array_link -> array<std::str>;
                # FIXME: for now dimension specs on the array are
                # disabled pending a syntax change
                # CREATE PROPERTY test_array_link_2 ->
                #     array<std::str[10]>;
            };
        """)

    async def test_edgeql_ddl_12(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r"backtick-quoted names surrounded by double underscores "
                r"are forbidden"):
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
                "object type or alias 'default::self' does not exist"):
            await self.con.execute(r"""
                CREATE TYPE test::TestBadContainerLinkObjectType {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT expression ON (`self` = 'foo');
                    };
                };
            """)

    async def test_edgeql_ddl_14(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                f'__source__ cannot be used in this expression'):
            await self.con.execute("""
                CREATE TYPE test::TestSelfLink1 {
                    CREATE PROPERTY foo1 -> std::str;
                    CREATE PROPERTY bar1 -> std::str {
                        SET default := __source__.foo1;
                    };
                };
            """)

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

    async def test_edgeql_ddl_16(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'possibly more than one element'):
            await self.con.execute(r"""
                CREATE TYPE test::TestSelfLink3 {
                    CREATE PROPERTY foo3 -> std::str;
                    CREATE PROPERTY bar3 -> std::str {
                        # NOTE: this is a set of all TestSelfLink3.foo3
                        SET default := test::TestSelfLink3.foo3;
                    };
                };
            """)

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
                        name,
                        subjectexpr,
                    }
                }
                FILTER .name LIKE '%bar%' OR .name LIKE '%foo%'
                ORDER BY .name;
            """,
            [
                {'name': 'bar::bar_t', 'constraints': []},
                {'name': 'foo::foo_t', 'constraints': [
                    {
                        'name': 'std::expression',
                        'subjectexpr': '(__subject__ > 0)',
                    },
                ]},
            ]
        )

    async def test_edgeql_ddl_19(self):
        await self.con.execute("""
            SET MODULE test;

            CREATE TYPE ActualType {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            CREATE ALIAS Alias1 := ActualType {
                bar := 9
            };

            CREATE ALIAS Alias2 := ActualType {
                connected := (SELECT Alias1 ORDER BY Alias1.foo)
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
                SELECT Alias2 {
                    foo,
                    connected: {
                        foo,
                        bar
                    }
                }
                ORDER BY Alias2.foo;
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

    async def test_edgeql_ddl_23(self):
        # Test that an unqualifed reverse link expression
        # as an alias pointer target is handled correctly and
        # manifests as std::BaseObject.
        await self.con.execute("""
            SET MODULE test;

            CREATE TYPE User;
            CREATE TYPE Award {
                CREATE LINK user -> User;
            };

            CREATE ALIAS Alias1 := (SELECT User {
                awards := .<user
            });
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::Alias1')
                SELECT
                    C.pointers { target: { name } }
                FILTER
                    C.pointers.name = 'awards'
            """,
            [
                {
                    'target': {
                        'name': 'std::BaseObject'
                    }
                },
            ],
        )

    async def test_edgeql_ddl_24(self):
        # Test transition of property from inherited to owned.
        await self.con.execute("""
            SET MODULE test;
            CREATE TYPE Desc;
            CREATE TYPE Named {
                CREATE PROPERTY name -> str;
                CREATE LINK desc -> Desc;
            };
            CREATE TYPE User EXTENDING Named;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::User')
                SELECT
                    C {
                        pointers: { @is_owned }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@is_owned': False,
                    }, {
                        '@is_owned': False,
                    }],
                },
            ],
        )

        await self.con.execute("""
            ALTER TYPE User {
                ALTER PROPERTY name SET OWNED;
                ALTER LINK desc SET OWNED;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::User')
                SELECT
                    C {
                        pointers: { @is_owned }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@is_owned': True,
                    }, {
                        '@is_owned': True,
                    }],
                },
            ],
        )

        await self.con.execute("""
            ALTER TYPE User {
                ALTER PROPERTY name {
                    SET REQUIRED;
                    CREATE CONSTRAINT exclusive;
                };

                ALTER LINK desc {
                    SET REQUIRED;
                    CREATE CONSTRAINT exclusive;
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::User')
                SELECT
                    C {
                        pointers: {
                            @is_owned,
                            required,
                            constraints: {
                                name,
                            }
                        }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@is_owned': True,
                        'required': True,
                        'constraints': [{
                            'name': 'std::exclusive',
                        }],
                    }, {
                        '@is_owned': True,
                        'required': True,
                        'constraints': [{
                            'name': 'std::exclusive',
                        }],
                    }],
                },
            ],
        )

        # and drop it again
        await self.con.execute("""
            ALTER TYPE User {
                ALTER PROPERTY name DROP OWNED;
                ALTER LINK desc DROP OWNED;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::User')
                SELECT
                    C {
                        pointers: {
                            @is_owned,
                            required,
                            constraints: {
                                name,
                            }
                        }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@is_owned': False,
                        'required': False,
                        'constraints': [],
                    }, {
                        '@is_owned': False,
                        'required': False,
                        'constraints': [],
                    }],
                },
            ],
        )

    async def test_edgeql_ddl_25(self):
        with self.assertRaisesRegex(
            edgedb.InvalidDefinitionError,
            "cannot drop owned property 'name'.*not inherited",
        ):
            await self.con.execute("""
                SET MODULE test;
                CREATE TYPE Named {
                    CREATE PROPERTY name -> str;
                };
                ALTER TYPE Named ALTER PROPERTY name DROP OWNED;
            """)

    async def test_edgeql_ddl_26(self):
        await self.con.execute("""
            SET MODULE test;
            CREATE TYPE Target;
            CREATE TYPE Source {
                CREATE LINK target -> Source;
            };
            CREATE TYPE Child EXTENDING Source {
                ALTER LINK target {
                    SET REQUIRED;
                    CREATE PROPERTY foo -> str;
                }
            };
            CREATE TYPE Grandchild EXTENDING Child {
                ALTER LINK target {
                    ALTER PROPERTY foo {
                        CREATE CONSTRAINT exclusive;
                    }
                }
            };
        """)

        await self.con.execute("""
            SET MODULE test;
            ALTER TYPE Child ALTER LINK target DROP OWNED;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::Child')
                SELECT
                    C {
                        links: {
                            @is_owned,
                            required,
                            properties: {
                                name,
                            }
                        }
                        FILTER .name = 'target'
                    };
            """,
            [
                {
                    'links': [{
                        '@is_owned': False,
                        'required': False,
                        'properties': [],
                    }],
                },
            ],
        )

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::Grandchild')
                SELECT
                    C {
                        links: {
                            @is_owned,
                            required,
                            properties: {
                                name,
                                @is_owned,
                                constraints: {
                                    name,
                                }
                            } FILTER .name = 'foo'
                        }
                        FILTER .name = 'target'
                    };
            """,
            [
                {
                    'links': [{
                        '@is_owned': True,
                        'required': True,
                        'properties': [{
                            'name': 'foo',
                            '@is_owned': True,
                            'constraints': [{
                                'name': 'std::exclusive',
                            }]
                        }],
                    }],
                },
            ],
        )

    async def test_edgeql_ddl_27(self):
        await self.con.execute("""
            SET MODULE test;
            CREATE TYPE Base {
                CREATE PROPERTY foo -> str;
            };
            CREATE TYPE Derived EXTENDING Base {
                ALTER PROPERTY foo SET REQUIRED;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::Derived')
                SELECT
                    C {
                        properties: {
                            @is_owned,
                            required,
                            inherited_fields,
                        }
                        FILTER .name = 'foo'
                    };
            """,
            [
                {
                    'properties': [{
                        '@is_owned': True,
                        'required': True,
                        'inherited_fields': {
                            'cardinality',
                            'readonly',
                            'target',
                        },
                    }],
                },
            ],
        )

        await self.con.execute("""
            SET MODULE test;
            ALTER TYPE Base DROP PROPERTY foo;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'test::Derived')
                SELECT
                    C {
                        properties: {
                            @is_owned,
                            required,
                            inherited_fields,
                        }
                        FILTER .name = 'foo'
                    };
            """,
            [
                {
                    'properties': [{
                        '@is_owned': True,
                        'required': True,
                        'inherited_fields': [],
                    }],
                },
            ],
        )

    async def test_edgeql_ddl_28(self):
        # Test that identifiers that are SQL keywords get quoted.
        # Issue 1667
        await self.con.execute("""
            CREATE TYPE test::Foo {
                CREATE PROPERTY left -> str;
                CREATE PROPERTY smallint -> str;
                CREATE PROPERTY natural -> str;
                CREATE PROPERTY null -> str;
                CREATE PROPERTY `like` -> str;
                CREATE PROPERTY `create` -> str;
                CREATE PROPERTY `link` -> str;
            };
        """)

    async def test_edgeql_ddl_default_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'default expression is of invalid type: std::int64, '
                'expected std::str'):
            await self.con.execute(r"""
                CREATE TYPE test::TestDefault01 {
                    CREATE PROPERTY def01 -> str {
                        # int64 doesn't have an assignment cast into str
                        SET default := 42;
                    };
                };
            """)

    async def test_edgeql_ddl_default_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'default expression is of invalid type: std::int64, '
                'expected std::str'):
            await self.con.execute(r"""
                CREATE TYPE test::TestDefault02 {
                    CREATE PROPERTY def02 -> str {
                        SET default := '42';
                    };
                };

                ALTER TYPE test::TestDefault02 {
                    ALTER PROPERTY def02 SET default := 42;
                };
            """)

    async def test_edgeql_ddl_default_03(self):
        # Test INSERT as default link expression
        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultInsert03;

            CREATE TYPE test::TestDefault03 {
                CREATE LINK def03 -> test::TestDefaultInsert03 {
                    SET default := (INSERT test::TestDefaultInsert03);
                };
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    count(test::TestDefault03),
                    count(test::TestDefaultInsert03)
                );
            """,
            [[0, 0]],
        )

        await self.assert_query_result(
            r"""
                SELECT test::TestDefault03 {
                    def03
                };
            """,
            [],
        )

        # `assert_query_result` is used instead of `execute` to
        # highlight the issue #1721
        await self.assert_query_result(
            r"""INSERT test::TestDefault03;""",
            [{'id': uuid.UUID}]
        )

        await self.assert_query_result(
            r"""
                SELECT (
                    count(test::TestDefault03),
                    count(test::TestDefaultInsert03)
                );
            """,
            [[1, 1]],
        )

        await self.assert_query_result(
            r"""
                SELECT test::TestDefault03 {
                    def03
                };
            """,
            [{
                'def03': {
                    'id': uuid.UUID
                }
            }],
        )

    async def test_edgeql_ddl_default_04(self):
        # Test UPDATE as default link expression
        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultUpdate04 {
                CREATE PROPERTY val -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };

            CREATE TYPE test::TestDefault04 {
                CREATE LINK def04 -> test::TestDefaultUpdate04 {
                    SET default := (
                        UPDATE test::TestDefaultUpdate04
                        FILTER .val = 'def04'
                        SET {
                            val := .val ++ '!'
                        }
                    );
                };
            };

            INSERT test::TestDefaultUpdate04 {
                val := 'notdef04'
            };
            INSERT test::TestDefaultUpdate04 {
                val := 'def04'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::TestDefaultUpdate04.val;
            """,
            {'def04', 'notdef04'},
        )

        await self.assert_query_result(r"""
            SELECT {
                (INSERT test::TestDefault04),
                (INSERT test::TestDefault04)
            };
        """, [{'id': uuid.UUID}, {'id': uuid.UUID}])

        await self.assert_query_result(
            r"""
                SELECT test::TestDefaultUpdate04.val;
            """,
            {'def04!', 'notdef04'},
        )

        await self.assert_query_result(
            r"""
                SELECT test::TestDefault04 {
                    def04: {
                        val
                    }
                } ORDER BY .def04.val EMPTY FIRST;
            """,
            [{
                'def04': None
            }, {
                'def04': {
                    'val': 'def04!'
                }
            }],
        )

    async def test_edgeql_ddl_default_05(self):
        # Test DELETE as default property expression
        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultDelete05 {
                CREATE PROPERTY val -> str;
            };

            CREATE TYPE test::TestDefault05 {
                CREATE PROPERTY def05 -> str {
                    SET default := (SELECT (
                        DELETE test::TestDefaultDelete05
                        FILTER .val = 'def05'
                        LIMIT 1
                    ).val);
                };
            };

            INSERT test::TestDefaultDelete05 {
                val := 'notdef05'
            };
            INSERT test::TestDefaultDelete05 {
                val := 'def05'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::TestDefaultDelete05.val;
            """,
            {'def05', 'notdef05'},
        )

        await self.con.execute(r"""
            INSERT test::TestDefault05;
            INSERT test::TestDefault05;
        """)

        await self.assert_query_result(
            r"""
                SELECT test::TestDefaultDelete05.val;
            """,
            {'notdef05'},
        )

        await self.assert_query_result(
            r"""
                SELECT test::TestDefault05 {
                    def05
                } ORDER BY .def05 EMPTY FIRST;
            """,
            [{
                'def05': None
            }, {
                'def05': 'def05'
            }],
        )

    async def test_edgeql_ddl_default_06(self):
        # Test DELETE as default link expression
        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultDelete06 {
                CREATE PROPERTY val -> str;
            };

            CREATE TYPE test::TestDefault06 {
                CREATE REQUIRED LINK def06 -> test::TestDefaultDelete06 {
                    SET default := (
                        DELETE test::TestDefaultDelete06
                        FILTER .val = 'def06'
                        LIMIT 1
                    );
                };
            };

            INSERT test::TestDefaultDelete06 {
                val := 'notdef06'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::TestDefaultDelete06.val;
            """,
            {'notdef06'},
        )

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'missing value for required.*test::TestDefault06.def06'):
            await self.con.execute(r"""
                INSERT test::TestDefault06;
            """)

    async def test_edgeql_ddl_default_circular(self):
        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultCircular {
                CREATE PROPERTY def01 -> int64 {
                    SET default := (SELECT count(test::TestDefaultCircular));
                };
            };
        """)

    async def test_edgeql_ddl_property_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE test::Foo {
                CREATE PROPERTY bar -> float32;
            };
        """)

        await self.con.execute(r"""
            CREATE TYPE test::TestDefaultCircular {
                CREATE PROPERTY def01 -> int64 {
                    SET default := (SELECT count(test::TestDefaultCircular));
                };
            };
        """)

    async def test_edgeql_ddl_link_target_bad_01(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B;

            CREATE TYPE Base0 {
                CREATE LINK foo -> A;
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> B;
            };
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                "cannot redefine link 'foo' of object type 'test::Derived' "
                "as object type 'test::B'"):
            await self.con.execute('''
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_link_target_bad_02(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B;
            CREATE TYPE C;

            CREATE TYPE Base0 {
                CREATE LINK foo -> A | B;
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> C;
            };
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                "cannot redefine link 'foo' of object type 'test::Derived' "
                "as object type 'test::C'"):
            await self.con.execute('''
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_link_target_merge_01(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B EXTENDING A;

            CREATE TYPE Base0 {
                CREATE LINK foo -> B;
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> A;
            };
            CREATE TYPE Derived EXTENDING Base0, Base1;
        ''')

    async def test_edgeql_ddl_link_target_merge_02(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B;
            CREATE TYPE C;

            CREATE TYPE Base0 {
                CREATE LINK foo -> A;
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> A | B;
            };
            CREATE TYPE Derived EXTENDING Base0, Base1;
        ''')

    async def test_edgeql_ddl_link_target_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE test::GrandParent01 {
                CREATE PROPERTY foo -> int64;
            };

            CREATE TYPE test::Parent01 EXTENDING test::GrandParent01;
            CREATE TYPE test::Parent02 EXTENDING test::GrandParent01;

            CREATE TYPE test::Child EXTENDING test::Parent01, test::Parent02;

            ALTER TYPE test::GrandParent01 {
                ALTER PROPERTY foo SET TYPE int16;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name IN {'test::Child', 'test::Parent01'})
                SELECT
                    C.pointers { target: { name } }
                FILTER
                    C.pointers.name = 'foo'
            """,
            [
                {
                    'target': {
                        'name': 'std::int16'
                    }
                },
                {
                    'target': {
                        'name': 'std::int16'
                    }
                },
            ],
        )

    async def test_edgeql_ddl_link_target_alter_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot change the target type of inherited property 'foo'"):
            await self.con.execute("""
                CREATE TYPE test::Parent01 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE test::Parent02 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE test::Child
                    EXTENDING test::Parent01, test::Parent02;

                ALTER TYPE test::Parent02 {
                    ALTER PROPERTY foo SET TYPE int16;
                };
            """)

    async def test_edgeql_ddl_link_target_alter_03(self):
        await self.con.execute("""
            CREATE TYPE test::Foo {
                CREATE PROPERTY bar -> int64;
            };

            CREATE TYPE test::Bar {
                CREATE MULTI PROPERTY foo -> int64 {
                    SET default := (SELECT test::Foo.bar);
                }
            };

            ALTER TYPE test::Foo ALTER PROPERTY bar SET TYPE int32;
        """)

    async def test_edgeql_ddl_link_target_alter_04(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B;

            CREATE TYPE Base0 {
                CREATE LINK foo -> A | B;
            };

            CREATE TYPE Derived EXTENDING Base0 {
                ALTER LINK foo SET TYPE B;
            }
        ''')

    async def test_edgeql_ddl_link_target_alter_05(self):
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE A;
            CREATE TYPE B EXTENDING A;

            CREATE TYPE Base0 {
                CREATE LINK foo -> B;
            };

            CREATE TYPE Base1;

            CREATE TYPE Derived EXTENDING Base0, Base1;

            ALTER TYPE Base1 CREATE LINK foo -> A;
        ''')

    async def test_edgeql_ddl_link_property_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"link properties cannot be required"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_01 {
                    CREATE LINK test_linkprop_link_01 -> std::Object {
                        CREATE REQUIRED PROPERTY test_link_prop_01
                            -> std::int64;
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"multi properties aren't supported for links"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_02 {
                    CREATE LINK test_linkprop_link_02 -> std::Object {
                        CREATE MULTI PROPERTY test_link_prop_02 -> std::int64;
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"link properties cannot be required"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_03 {
                    CREATE LINK test_linkprop_link_03 -> std::Object;
                };

                ALTER TYPE test::TestLinkPropType_03 {
                    ALTER LINK test_linkprop_link_03 {
                        CREATE REQUIRED PROPERTY test_link_prop_03
                            -> std::int64;
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"multi properties aren't supported for links"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_04 {
                    CREATE LINK test_linkprop_link_04 -> std::Object;
                };

                ALTER TYPE test::TestLinkPropType_04 {
                    ALTER LINK test_linkprop_link_04 {
                        CREATE MULTI PROPERTY test_link_prop_04 -> std::int64;
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"link properties cannot be required"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_05 {
                    CREATE LINK test_linkprop_link_05 -> std::Object {
                        CREATE PROPERTY test_link_prop_05 -> std::int64;
                    };
                };

                ALTER TYPE test::TestLinkPropType_05 {
                    ALTER LINK test_linkprop_link_05 {
                        ALTER PROPERTY test_link_prop_05 {
                            SET REQUIRED;
                        };
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"multi properties aren't supported for links"):
            await self.con.execute("""
                CREATE TYPE test::TestLinkPropType_06 {
                    CREATE LINK test_linkprop_link_06 -> std::Object {
                        CREATE MULTI PROPERTY test_link_prop_06 -> std::int64;
                    };
                };

                ALTER TYPE test::TestLinkPropType_06 {
                    ALTER LINK test_linkprop_link_06 {
                        ALTER PROPERTY test_link_prop_06 {
                            SET MULTI;
                        };
                    };
                };
            """)

    async def test_edgeql_ddl_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'default::array' does not exist"):
            await self.con.execute(r"""
                CREATE TYPE test::Foo {
                    CREATE PROPERTY bar -> array;
                };
            """)

    async def test_edgeql_ddl_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'default::tuple' does not exist"):
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

    async def test_edgeql_ddl_bad_07(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in computable 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;

                    CREATE TYPE test::Bar {
                        CREATE LINK foo := (INSERT test::Foo);
                    };
                """)

    async def test_edgeql_ddl_bad_08(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in computable 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;

                    CREATE TYPE test::Bar {
                        CREATE LINK foo := (
                            WITH x := (INSERT test::Foo)
                            SELECT x
                        );
                    };
                """)

    async def test_edgeql_ddl_bad_09(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in computable 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;

                    CREATE TYPE test::Bar {
                        CREATE PROPERTY foo := (INSERT test::Foo).id;
                    };
                """)

    async def test_edgeql_ddl_bad_10(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;
                    CREATE TYPE test::Bar;

                    CREATE ALIAS test::Baz := test::Bar {
                        foo := (INSERT test::Foo)
                    };
                """)

    async def test_edgeql_ddl_bad_11(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;
                    CREATE TYPE test::Bar;

                    CREATE ALIAS test::Baz := test::Bar {
                        foo := (INSERT test::Foo).id
                    };
                """)

    async def test_edgeql_ddl_bad_12(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;
                    CREATE TYPE test::Bar {
                        CREATE LINK foo -> test::Foo;
                    };

                    CREATE ALIAS test::Baz := test::Bar {
                        foo: {
                            fuz := (INSERT test::Foo)
                        }
                    };
                """)

    async def test_edgeql_ddl_bad_13(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"invalid mutation in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE test::Foo;
                    CREATE TYPE test::Bar {
                        CREATE LINK foo -> test::Foo;
                    };

                    CREATE ALIAS test::Baz := (
                        WITH x := (INSERT test::Foo)
                        SELECT test::Bar {
                            foo: {
                                fuz := x
                            }
                        }
                    );
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
                f"'default' is not a valid field for an abstract link"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT LINK test::bar {
                        SET default := Object;
                    };
                """)

    async def test_edgeql_ddl_property_bad_01(self):
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
                f"'default' is not a valid field for an abstract property"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT PROPERTY test::bar {
                        SET default := 'bad';
                    };
                """)

    async def test_edgeql_ddl_function_01(self):
        await self.con.execute("""
            CREATE FUNCTION test::my_lower(s: std::str) -> std::str
                USING SQL FUNCTION 'lower';
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION test::my_lower(s: SET OF std::str)
                        -> std::str {
                        SET initial_value := '';
                        USING SQL FUNCTION 'count';
                    };
                """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: std::str);
        """)

        await self.con.execute("""
            CREATE FUNCTION test::my_lower(s: SET OF anytype)
                -> std::str {
                USING SQL FUNCTION 'count';
                SET initial_value := '';
            };
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*test::my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION test::my_lower(s: anytype) -> std::str
                        USING SQL FUNCTION 'lower';
                """)

        await self.con.execute("""
            DROP FUNCTION test::my_lower(s: anytype);
        """)

    async def test_edgeql_ddl_function_02(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.con.execute(f"""
            CREATE FUNCTION test::my_sql_func1()
                -> std::str
                USING SQL $$
                    SELECT 'spam'::text
                $$;

            CREATE FUNCTION test::my_sql_func2(foo: std::str)
                -> std::str
                USING SQL $$
                    SELECT "foo"::text
                $$;

            CREATE FUNCTION test::my_sql_func4(VARIADIC s: std::str)
                -> std::str
                USING SQL $$
                    SELECT array_to_string(s, '-')
                $$;

            CREATE FUNCTION test::{long_func_name}()
                -> std::str
                USING SQL $$
                    SELECT '{long_func_name}'::text
                $$;

            CREATE FUNCTION test::my_sql_func6(a: std::str='a' ++ 'b')
                -> std::str
                USING SQL $$
                    SELECT $1 || 'c'
                $$;

            CREATE FUNCTION test::my_sql_func7(s: array<std::int64>)
                -> std::int64
                USING SQL $$
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

    async def test_edgeql_ddl_function_03(self):
        with self.assertRaisesRegex(edgedb.InvalidFunctionDefinitionError,
                                    r'invalid default value'):
            await self.con.execute(f"""
                CREATE FUNCTION test::broken_sql_func1(
                    a: std::int64=(SELECT schema::ObjectType))
                -> std::str
                USING SQL $$
                    SELECT 'spam'::text
                $$;
            """)

    async def test_edgeql_ddl_function_04(self):
        await self.con.execute(f"""
            CREATE FUNCTION test::my_edgeql_func1()
                -> std::str
                USING EdgeQL $$
                    SELECT 'sp' ++ 'am'
                $$;

            CREATE FUNCTION test::my_edgeql_func2(s: std::str)
                -> schema::ObjectType
                USING EdgeQL $$
                    SELECT
                        schema::ObjectType
                    FILTER schema::ObjectType.name = s
                    LIMIT 1
                $$;

            CREATE FUNCTION test::my_edgeql_func3(s: std::int64)
                -> std::int64
                USING EdgeQL $$
                    SELECT s + 10
                $$;

            CREATE FUNCTION test::my_edgeql_func4(i: std::int64)
                -> array<std::int64>
                USING EdgeQL $$
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
                SELECT (SELECT test::my_edgeql_func2('schema::Object')).name;
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

    async def test_edgeql_ddl_function_05(self):
        await self.con.execute("""
            CREATE FUNCTION test::attr_func_1() -> std::str {
                CREATE ANNOTATION description := 'hello';
                USING EdgeQL "SELECT '1'";
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

    async def test_edgeql_ddl_function_06(self):
        await self.con.execute("""
            CREATE FUNCTION test::int_func_1() -> std::int64 {
                USING EdgeQL "SELECT 1";
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::int_func_1();
            """,
            [{}],
        )

    async def test_edgeql_ddl_function_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::my_agg.*function:.+anytype.+cannot '
                r'have a non-empty default'):
            await self.con.execute(r"""
                CREATE FUNCTION test::my_agg(
                        s: anytype = [1]) -> array<anytype>
                    USING SQL FUNCTION "my_agg";
            """)

    async def test_edgeql_ddl_function_08(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'invalid declaration.*unexpected type of the default'):

            await self.con.execute("""
                CREATE FUNCTION test::ddlf_08(s: std::str = 1) -> std::str
                    USING EdgeQL $$ SELECT "1" $$;
            """)

    async def test_edgeql_ddl_function_09(self):
        await self.con.execute("""
            CREATE FUNCTION test::ddlf_09(
                NAMED ONLY a: int64,
                NAMED ONLY b: int64
            ) -> std::str
                USING EdgeQL $$ SELECT "1" $$;
        """)

        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'already defined'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION test::ddlf_09(
                        NAMED ONLY b: int64,
                        NAMED ONLY a: int64 = 1
                    ) -> std::str
                        USING EdgeQL $$ SELECT "1" $$;
                """)

        await self.con.execute("""
            CREATE FUNCTION test::ddlf_09(
                NAMED ONLY b: str,
                NAMED ONLY a: int64
            ) -> std::str
                USING EdgeQL $$ SELECT "2" $$;
        """)

        await self.assert_query_result(
            r'''
                SELECT test::ddlf_09(a:=1, b:=1);
            ''',
            ['1'],
        )
        await self.assert_query_result(
            r'''
                SELECT test::ddlf_09(a:=1, b:='a');
            ''',
            ['2'],
        )

    async def test_edgeql_ddl_function_10(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter `sum` is not callable',
                _line=6, _col=39):

            await self.con.execute('''
                CREATE FUNCTION test::ddlf_10(
                    sum: int64
                ) -> int64
                    USING (
                        SELECT <int64>sum(sum)
                    );
            ''')

    async def test_edgeql_ddl_function_11(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_11_1() -> str
                USING EdgeQL $$
                    SELECT '\u0062'
                $$;

            CREATE FUNCTION test::ddlf_11_2() -> str
                USING EdgeQL $$
                    SELECT r'\u0062'
                $$;

            CREATE FUNCTION test::ddlf_11_3() -> str
                USING EdgeQL $$
                    SELECT $a$\u0062$a$
                $$;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_11_1();
                ''',
                ['b'],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_11_2();
                ''',
                [r'\u0062'],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_11_3();
                ''',
                [r'\u0062'],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION test::ddlf_11_1();
                DROP FUNCTION test::ddlf_11_2();
                DROP FUNCTION test::ddlf_11_3();
            """)

    async def test_edgeql_ddl_function_12(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'cannot create.*test::ddlf_12\(a: std::int64\).*'
                r'function with the same signature is already defined'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_12(a: int64) -> int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_12(a: int64) -> float64
                    USING EdgeQL $$ SELECT 11 $$;
            ''')

    async def test_edgeql_ddl_function_13(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'cannot create.*test::ddlf_13\(a: SET OF std::int64\).*'
                r'SET OF parameters in user-defined EdgeQL functions are '
                r'not supported'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION test::ddlf_13(a: SET OF int64) -> int64
                        USING EdgeQL $$ SELECT 11 $$;
                ''')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.execute("""
                DROP FUNCTION test::ddlf_13(a: SET OF int64);
            """)

    async def test_edgeql_ddl_function_14(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_14(
                    a: int64, NAMED ONLY f: int64) -> int64
                USING EdgeQL $$ SELECT 11 $$;

            CREATE FUNCTION test::ddlf_14(
                    a: int32, NAMED ONLY f: str) -> int64
                USING EdgeQL $$ SELECT 12 $$;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_14(<int64>10, f := 11);
                ''',
                [11],
            )
            await self.assert_query_result(
                r'''
                    SELECT test::ddlf_14(<int32>10, f := '11');
                ''',
                [12],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION test::ddlf_14(a: int64, NAMED ONLY f: int64);
                DROP FUNCTION test::ddlf_14(a: int32, NAMED ONLY f: str);
            """)

    async def test_edgeql_ddl_function_15(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_15.*NAMED ONLY h:.*'
                r'different named only parameters'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_15(
                        a: int64, NAMED ONLY f: int64) -> int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_15(
                        a: int32, NAMED ONLY h: str) -> int64
                    USING EdgeQL $$ SELECT 12 $$;
            ''')

    async def test_edgeql_ddl_function_16(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create the polymorphic.*test::ddlf_16.*'
                r'function with different return type'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_16(
                        a: anytype, b: int64) -> OPTIONAL int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION test::ddlf_16(a: anytype, b: float64) -> str
                    USING EdgeQL $$ SELECT '12' $$;
            ''')

    async def test_edgeql_ddl_function_17(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_17(str: std::str) -> int64
                USING SQL FUNCTION 'whatever';
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_17.*'
                r'overloading "USING SQL FUNCTION"'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION test::ddlf_17(str: std::int64) -> int64
                        USING SQL FUNCTION 'whatever2';
                ''')

        await self.con.execute("""
            DROP FUNCTION test::ddlf_17(str: std::str);
        """)

    async def test_edgeql_ddl_function_18(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_18.*'
                r'function returns a generic type but has no '
                r'generic parameters'):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_18(str: std::str) -> anytype
                    USING EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_function_19(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'std::anytype' does not exist"):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_19(f: std::anytype) -> int64
                    USING EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_function_20(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r"Unexpected ';'"):

            await self.con.execute(r'''
                CREATE FUNCTION test::ddlf_20(f: int64) -> int64
                    USING EdgeQL $$ SELECT 1; SELECT f; $$;
            ''')

    async def test_edgeql_ddl_function_21(self):
        await self.con.execute(r'''
            CREATE FUNCTION test::ddlf_21(str: str) -> int64
                USING EdgeQL $$ SELECT 1$$;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::ddlf_21.*'
                r'overloading.*different `session_only` flag'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION test::ddlf_21(str: int64) -> int64 {
                        SET session_only := true;
                        USING EdgeQL $$ SELECT 1$$;
                    };
                ''')

        await self.con.execute("""
            DROP FUNCTION test::ddlf_21(str: std::str);
        """)

    async def test_edgeql_ddl_function_22(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return type mismatch.*scalar type 'std::int64'"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION test::broken_edgeql_func22(
                    a: std::str) -> std::int64
                USING EdgeQL $$
                    SELECT a
                $$;
            """)

    async def test_edgeql_ddl_function_23(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return type mismatch.*scalar type 'std::int64'"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION test::broken_edgeql_func23(
                    a: std::str) -> std::int64
                USING EdgeQL $$
                    SELECT [a]
                $$;
            """)

    async def test_edgeql_ddl_function_24(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return type mismatch.*scalar type 'std::str'"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION test::broken_edgeql_func24(
                    a: std::str) -> std::str
                USING EdgeQL $$
                    SELECT [a]
                $$;
            """)

    async def test_edgeql_ddl_function_25(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return cardinality mismatch"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION test::broken_edgeql_func25(
                    a: std::str) -> std::str
                USING EdgeQL $$
                    SELECT {a, a}
                $$;
            """)

    async def test_edgeql_ddl_function_26(self):
        await self.con.execute(r"""
            CREATE ABSTRACT ANNOTATION foo26;

            CREATE FUNCTION test::edgeql_func26(a: std::str) -> std::str {
                USING EdgeQL $$
                    SELECT a ++ 'aaa'
                $$;
                SET volatility := 'VOLATILE';
            };

            ALTER FUNCTION test::edgeql_func26(a: std::str) {
                CREATE ANNOTATION foo26 := 'aaaa';
            };

            ALTER FUNCTION test::edgeql_func26(a: std::str) {
                SET volatility := 'IMMUTABLE';
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::edgeql_func26('b')
            ''',
            [
                'baaa'
            ],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Function {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                    vol := <str>.volatility,
                }
                FILTER
                    .name = 'test::edgeql_func26';
            ''',
            [
                {
                    'name': 'test::edgeql_func26',
                    'annotations': [
                        {
                            'name': 'default::foo26',
                            '@value': 'aaaa',
                        },
                    ],
                    'vol': 'IMMUTABLE',
                },
            ]
        )

        await self.con.execute(r"""
            ALTER FUNCTION test::edgeql_func26(a: std::str) {
                DROP ANNOTATION foo26;
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Function {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                }
                FILTER
                    .name = 'test::edgeql_func26';
            ''',
            [
                {
                    'name': 'test::edgeql_func26',
                    'annotations': [],
                },
            ]
        )

        await self.con.execute(r"""
            ALTER FUNCTION test::edgeql_func26(a: std::str) {
                USING (
                    SELECT a ++ 'bbb'
                )
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::edgeql_func26('b')
            ''',
            [
                'bbbb'
            ],
        )

        await self.con.execute(r"""
            ALTER FUNCTION test::edgeql_func26(a: std::str) {
                USING EdgeQL $$
                    SELECT a ++ 'zzz'
                $$
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::edgeql_func26('b')
            ''',
            [
                'bzzz'
            ],
        )

    async def test_edgeql_ddl_function_27(self):
        # This test checks constants, but we have to do DDLs to test them
        # with constant extraction disabled
        await self.con.execute('''
            CREATE FUNCTION test::constant_int() -> std::int64 {
                USING (SELECT 1_024);
            };
            CREATE FUNCTION test::constant_bigint() -> std::bigint {
                USING (SELECT 1_024n);
            };
            CREATE FUNCTION test::constant_float() -> std::float64 {
                USING (SELECT 1_024.1_250);
            };
            CREATE FUNCTION test::constant_decimal() -> std::decimal {
                USING (SELECT 1_024.1_024n);
            };
        ''')
        try:
            await self.assert_query_result(
                r'''
                    SELECT (
                        int := test::constant_int(),
                        bigint := test::constant_bigint(),
                        float := test::constant_float(),
                        decimal := test::constant_decimal(),
                    )
                ''',
                [{
                    "int": 1024,
                    "bigint": 1024,
                    "float": 1024.125,
                    "decimal": 1024.1024,
                }],
                [{
                    "int": 1024,
                    "bigint": 1024,
                    "float": 1024.125,
                    "decimal": decimal.Decimal('1024.1024'),
                }],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION test::constant_int();
                DROP FUNCTION test::constant_float();
                DROP FUNCTION test::constant_bigint();
                DROP FUNCTION test::constant_decimal();
            """)

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
            {
                SET commutator := 'test::+++';
                USING SQL OPERATOR r'+';
            };
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
                CREATE ANNOTATION description := 'my plus';
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
                    USING SQL OPERATOR r'!';

                CREATE PREFIX OPERATOR test::`!`
                    (operand: int64) -> int64
                    USING SQL OPERATOR r'!!';
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
                    USING SQL EXPRESSION;
            ''')

    async def test_edgeql_ddl_operator_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`test::=\(l: array<anytype>, r: std::str\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: str) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`test::=\(l: array<anytype>, r: anytuple\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: anytuple) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the non-recursive '
                r'`std::=\(l: array<std::int64>, '
                r'r: array<std::int64>\)` operator: '
                r'overloading a recursive operator '
                r'`array<anytype> = array<anytype>` with a non-recursive one '
                r'is not allowed'):
            # attempt to overload a recursive `=` from std with a
            # non-recursive version
            await self.con.execute('''
                CREATE INFIX OPERATOR
                std::`=` (l: array<int64>, r: array<int64>) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the recursive '
                r'`test::=\(l: array<std::int64>, '
                r'r: array<std::int64>\)` operator: '
                r'overloading a non-recursive operator '
                r'`array<anytype> = array<anytype>` with a recursive one '
                r'is not allowed'):
            # create 2 operators in test: non-recursive first, then a
            # recursive one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`=` (l: array<anytype>, r: array<anytype>) -> std::bool {
                    USING SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                test::`=` (l: array<int64>, r: array<int64>) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_08(self):
        try:
            await self.con.execute('''
                CREATE ABSTRACT INFIX OPERATOR test::`>`
                    (left: anytype, right: anytype) -> bool;
            ''')

            await self.assert_query_result(
                r'''
                    WITH MODULE schema
                    SELECT Operator {
                        name,
                        is_abstract,
                    }
                    FILTER
                        .name = 'test::>'
                ''',
                [
                    {
                        'name': 'test::>',
                        'is_abstract': True,
                    },
                ]
            )

        finally:
            await self.con.execute('''
                DROP INFIX OPERATOR test::`>`
                    (left: anytype, right: anytype);
            ''')

    async def test_edgeql_ddl_operator_09(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'unexpected USING clause in abstract operator definition'):
            await self.con.execute('''
                CREATE ABSTRACT INFIX OPERATOR
                test::`=` (l: array<anytype>, r: array<anytype>) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_10(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateOperatorDefinitionError,
                r'cannot create the '
                r'`test::IN\(l: std::int64, r: std::int64\)` operator: '
                r'there exists a derivative operator of the same name'):
            # create 2 operators in test: derivative first, then a
            # non-derivative one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`IN` (l: std::float64, r: std::float64) -> std::bool {
                    USING SQL EXPRESSION;
                    SET derivative_of := 'std::=';
                };

                CREATE INFIX OPERATOR
                test::`IN` (l: std::int64, r: std::int64) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_11(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateOperatorDefinitionError,
                r'cannot create '
                r'`test::IN\(l: std::int64, r: std::int64\)` as a '
                r'derivative operator: there already exists an operator '
                r'of the same name'):
            # create 2 operators in test: non-derivative first, then a
            # derivative one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                test::`IN` (l: std::float64, r: std::float64) -> std::bool {
                    USING SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                test::`IN` (l: std::int64, r: std::int64) -> std::bool {
                    USING SQL EXPRESSION;
                    SET derivative_of := 'std::=';
                };
            ''')

    async def test_edgeql_ddl_cast_01(self):
        await self.con.execute('''
            CREATE SCALAR TYPE test::type_a EXTENDING std::str;
            CREATE SCALAR TYPE test::type_b EXTENDING std::int64;
            CREATE SCALAR TYPE test::type_c EXTENDING std::datetime;

            CREATE CAST FROM test::type_a TO test::type_b {
                USING SQL CAST;
                ALLOW IMPLICIT;
            };

            CREATE CAST FROM test::type_a TO test::type_c {
                USING SQL CAST;
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

    async def test_edgeql_ddl_property_computable_circular(self):
        await self.con.execute('''\
            CREATE TYPE test::CompPropCircular {
                CREATE PROPERTY prop := (SELECT count(test::CompPropCircular))
            };
        ''')

    async def test_edgeql_ddl_property_computable_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type: expected.* got .* 'std::Object'"):
            await self.con.execute('''\
                CREATE TYPE test::CompPropBad;
                ALTER TYPE test::CompPropBad {
                    CREATE PROPERTY prop := (SELECT std::Object LIMIT 1);
                };
            ''')

    async def test_edgeql_ddl_link_computable_circular_01(self):
        await self.con.execute('''\
            CREATE TYPE test::CompLinkCircular {
                CREATE LINK l := (SELECT test::CompLinkCircular LIMIT 1)
            };
        ''')

    async def test_edgeql_ddl_link_target_circular_01(self):
        # Circular target as part of a union.
        await self.con.execute('''\
            CREATE TYPE test::LinkCircularA;
            CREATE TYPE test::LinkCircularB {
                CREATE LINK l -> test::LinkCircularA
                                 | test::LinkCircularB;
            };
        ''')

    async def test_edgeql_ddl_annotation_01(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::attr1;

            CREATE SCALAR TYPE test::TestAttrType1 EXTENDING std::str {
                CREATE ANNOTATION test::attr1 := 'aaaa';
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

        await self.migrate("""
            abstract annotation attr2;

            scalar type TestAttrType1 extending std::str {
                annotation attr2 := 'aaaa';
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
            [{"annotations": [{"name": "test::attr2", "@value": "aaaa"}]}]
        )

    async def test_edgeql_ddl_annotation_02(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::attr1;

            CREATE TYPE test::TestAttrType2 {
                CREATE ANNOTATION test::attr1 := 'aaaa';
            };
        """)

        await self.migrate("""
            abstract annotation attr2;

            type TestAttrType2 {
                annotation attr2 := 'aaaa';
            };
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
                CREATE ANNOTATION test::noninh := 'no inherit';
                CREATE ANNOTATION test::inh := 'inherit me';
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

    async def test_edgeql_ddl_annotation_04(self):
        await self.con.execute('''
            CREATE TYPE test::BaseAnno4;
            CREATE TYPE test::DerivedAnno4 EXTENDING test::BaseAnno4;
            CREATE ABSTRACT ANNOTATION test::noninh_anno;
            CREATE ABSTRACT INHERITABLE ANNOTATION test::inh_anno;
            ALTER TYPE test::BaseAnno4
                CREATE ANNOTATION test::noninh_anno := '1';
            ALTER TYPE test::BaseAnno4
                CREATE ANNOTATION test::inh_anno := '2';
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        inheritable,
                        @value,
                    }
                    FILTER .name LIKE 'test::%_anno'
                    ORDER BY .name
                }
                FILTER
                    .name = 'test::DerivedAnno4'
                ORDER BY
                    .name;
            ''',
            [{
                "annotations": [{
                    "name": "test::inh_anno",
                    "inheritable": True,
                    "@value": "2",
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_05(self):
        await self.con.execute(r'''
            CREATE TYPE test::BaseAnno05 {
                CREATE PROPERTY name -> str;
                CREATE INDEX ON (.name) {
                    CREATE ANNOTATION title := 'name index'
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno05';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "name index",
                    }]
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_06(self):
        await self.con.execute(r'''
            CREATE TYPE test::BaseAnno06 {
                CREATE PROPERTY name -> str;
                CREATE INDEX ON (.name);
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE test::BaseAnno06 {
                ALTER INDEX ON (.name) {
                    CREATE ANNOTATION title := 'name index'
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno06';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "name index",
                    }]
                }]
            }]
        )

        await self.con.execute(r'''
            ALTER TYPE test::BaseAnno06 {
                ALTER INDEX ON (.name) {
                    DROP ANNOTATION title;
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno06';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": []
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_07(self):
        # Create index annotation using DDL, then drop annotation using SDL.
        await self.con.execute(r'''
            CREATE TYPE test::BaseAnno07 {
                CREATE PROPERTY name -> str;
                CREATE INDEX ON (.name) {
                    CREATE ANNOTATION title := 'name index'
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno07';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "name index",
                    }]
                }]
            }]
        )

        await self.migrate(r'''
            type BaseAnno07 {
                property name -> str;
                index ON (.name);
            }
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno07';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": []
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_08(self):
        # Create index using DDL, then add annotation to it using SDL.
        await self.con.execute(r'''
            CREATE TYPE test::BaseAnno08 {
                CREATE PROPERTY name -> str;
                CREATE INDEX ON (.name);
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno08';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": []
                }]
            }]
        )

        await self.migrate(r'''
            type BaseAnno08 {
                property name -> str;
                index ON (.name) {
                    annotation title := 'name index';
                }
            }
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    indexes: {
                        expr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::BaseAnno08';
            ''',
            [{
                "indexes": [{
                    "expr": ".name",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "name index",
                    }]
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_09(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::anno09;

            CREATE TYPE test::TestTypeAnno09 {
                CREATE ANNOTATION test::anno09 := 'A';
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name = 'test::anno09'
                }
                FILTER
                    .name = 'test::TestTypeAnno09';
            ''',
            [{"annotations": [{"name": "test::anno09", "@value": "A"}]}]
        )

        # Alter the annotation.
        await self.con.execute("""
            ALTER TYPE test::TestTypeAnno09 {
                ALTER ANNOTATION test::anno09 := 'B';
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name = 'test::anno09'
                }
                FILTER
                    .name = 'test::TestTypeAnno09';
            ''',
            [{"annotations": [{"name": "test::anno09", "@value": "B"}]}]
        )

    async def test_edgeql_ddl_annotation_10(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::anno10;
            CREATE ABSTRACT INHERITABLE ANNOTATION test::anno10_inh;

            CREATE TYPE test::TestTypeAnno10
            {
                CREATE ANNOTATION test::anno10 := 'A';
                CREATE ANNOTATION test::anno10_inh := 'A';
            };

            CREATE TYPE test::TestSubTypeAnno10
                    EXTENDING test::TestTypeAnno10
            {
                CREATE ANNOTATION test::anno10 := 'B';
                ALTER ANNOTATION test::anno10_inh := 'B';
            }
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    }
                    FILTER .name LIKE 'test::anno10%'
                    ORDER BY .name
                }
                FILTER
                    .name LIKE 'test::%Anno10'
                ORDER BY
                    .name
            ''',
            [
                {
                    "annotations": [
                        {"name": "test::anno10", "@value": "B"},
                        {"name": "test::anno10_inh", "@value": "B"},
                    ]
                },
                {
                    "annotations": [
                        {"name": "test::anno10", "@value": "A"},
                        {"name": "test::anno10_inh", "@value": "A"},
                    ]
                },
            ]
        )

        # Drop the non-inherited annotation from subtype.
        await self.con.execute("""
            ALTER TYPE test::TestSubTypeAnno10 {
                DROP ANNOTATION test::anno10;
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name LIKE 'test::anno10%'
                }
                FILTER
                    .name = 'test::TestSubTypeAnno10';
            ''',
            [{"annotations": [{"name": "test::anno10_inh", "@value": "B"}]}]
        )

        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "cannot drop inherited annotation 'test::anno10_inh'",
        ):
            await self.con.execute("""
                ALTER TYPE test::TestSubTypeAnno10 {
                    DROP ANNOTATION test::anno10_inh;
                };
            """)

    async def test_edgeql_ddl_annotation_11(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::anno11;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'test::anno11%';
            ''',
            [{"name": "test::anno11"}]
        )

        await self.con.execute("""
            ALTER ABSTRACT ANNOTATION test::anno11
                RENAME TO test::anno11_new_name;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'test::anno11%';
            ''',
            [{"name": "test::anno11_new_name"}]
        )

        await self.con.execute("""
            DROP ABSTRACT ANNOTATION test::anno11_new_name;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'test::anno11%';
            ''',
            []
        )

    async def test_edgeql_ddl_annotation_12(self):
        with self.assertRaisesRegex(
            edgedb.UnknownModuleError,
            "module 'bogus' is not in this schema",
        ):
            await self.con.execute("""
                CREATE ABSTRACT ANNOTATION bogus::anno12;
            """)

    @test.xfail('''
        UnknownModuleError not raised.
    ''')
    async def test_edgeql_ddl_annotation_13(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION test::anno13;
        """)

        with self.assertRaisesRegex(
            edgedb.UnknownModuleError,
            "module 'bogus' is not in this schema",
        ):
            await self.con.execute("""
                ALTER ABSTRACT ANNOTATION test::anno13 RENAME TO bogus::anno13;
            """)

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
                SELECT schema::ObjectType {
                    ancestors: {
                        name
                    } ORDER BY @index
                }
                FILTER .name = 'test::ExtC3'
            """,
            [{
                'ancestors': [{
                    'name': 'test::ExtB3',
                }, {
                    'name': 'test::ExtA3',
                }, {
                    'name': 'std::Object',
                }, {
                    'name': 'std::BaseObject',
                }],
            }]
        )

        await self.con.execute(r"""
            ALTER TYPE test::ExtB3 DROP EXTENDING test::ExtA3;
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    ancestors: {
                        name
                    } ORDER BY @index
                }
                FILTER .name = 'test::ExtC3'
            """,
            [{
                'ancestors': [{
                    'name': 'test::ExtB3',
                }, {
                    'name': 'std::Object',
                }, {
                    'name': 'std::BaseObject',
                }],
            }]
        )

    async def test_edgeql_ddl_extending_04(self):
        # Check that descendants are recomputed properly on rebase.
        await self.con.execute(r"""
            CREATE TYPE test::ExtA4 {
                CREATE PROPERTY a -> int64;
            };

            CREATE ABSTRACT INHERITABLE ANNOTATION a_anno;

            CREATE TYPE test::ExtB4 {
                CREATE PROPERTY a -> int64 {
                    CREATE ANNOTATION a_anno := 'anno';
                };

                CREATE PROPERTY b -> str;
            };

            CREATE TYPE test::Ext4Child EXTENDING test::ExtA4;
            CREATE TYPE test::Ext4GrandChild EXTENDING test::Ext4Child;
            CREATE TYPE test::Ext4GrandGrandChild
                EXTENDING test::Ext4GrandChild;
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    SELECT schema::ObjectType
                    FILTER .name = 'test::Ext4Child'
                ).properties.name;
            """,
            {'id', 'a'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::Ext4Child EXTENDING test::ExtB4;
        """)

        for name in {'Ext4Child', 'Ext4GrandChild', 'Ext4GrandGrandChild'}:
            await self.assert_query_result(
                f"""
                    SELECT (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::{name}'
                    ).properties.name;
                """,
                {'id', 'a', 'b'}
            )

        await self.assert_query_result(
            r"""
                WITH
                    ggc := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::Ext4GrandGrandChild'
                    )
                SELECT
                    (SELECT ggc.properties FILTER .name = 'a')
                        .annotations@value;
            """,
            {'anno'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::Ext4Child DROP EXTENDING test::ExtB4;
        """)

        for name in {'Ext4Child', 'Ext4GrandChild', 'Ext4GrandGrandChild'}:
            await self.assert_query_result(
                f"""
                    SELECT (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::{name}'
                    ).properties.name;
                """,
                {'id', 'a'}
            )

        await self.assert_query_result(
            r"""
                WITH
                    ggc := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::Ext4GrandGrandChild'
                    )
                SELECT
                    (SELECT ggc.properties FILTER .name = 'a')
                        .annotations@value;
            """,
            {}
        )

    async def test_edgeql_ddl_extending_05(self):
        # Check that field alters are propagated.
        await self.con.execute(r"""
            CREATE TYPE test::ExtA5 {
                CREATE PROPERTY a -> int64 {
                    SET default := 1;
                };
            };

            CREATE TYPE test::ExtB5 {
                CREATE PROPERTY a -> int64 {
                    SET default := 2;
                };
            };

            CREATE TYPE test::ExtC5 EXTENDING test::ExtB5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'2'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::ExtC5 EXTENDING test::ExtA5 FIRST;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'1'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::ExtC5 DROP EXTENDING test::ExtA5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'2'}
        )

        await self.con.execute(r"""
            ALTER TYPE test::ExtC5 ALTER PROPERTY a SET REQUIRED;
            ALTER TYPE test::ExtC5 DROP EXTENDING test::ExtA5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'test::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {}
        )

    async def test_edgeql_ddl_modules_01(self):
        try:
            await self.con.execute(r"""
                CREATE MODULE test_other;

                CREATE TYPE test::ModuleTest01 {
                    CREATE PROPERTY clash -> str;
                };

                CREATE TYPE test_other::Target;
                CREATE TYPE test_other::ModuleTest01 {
                    CREATE LINK clash -> test_other::Target;
                };
            """)

            await self.con.execute("""
                DROP TYPE test_other::ModuleTest01;
                DROP TYPE test_other::Target;
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
                ALTER PROPERTY name {
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
                ALTER PROPERTY name {
                    CREATE DELEGATED CONSTRAINT exclusive;
                }
            };
        """)

        try:
            async with self.con.transaction():
                await self.con.execute(r"""
                    START MIGRATION TO {
                        type test::Status extending test_other::UniquelyNamed;
                    };
                    POPULATE MIGRATION;
                    COMMIT MIGRATION;
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
                    is_superuser,
                    password,
                } FILTER .name = 'foo_01'
            """,
            [{
                'name': 'foo_01',
                'is_superuser': False,
                'password': None,
            }]
        )

    async def test_edgeql_ddl_role_02(self):
        await self.con.execute(r"""
            CREATE SUPERUSER ROLE foo2 {
                SET password := 'secret';
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    is_superuser,
                } FILTER .name = 'foo2'
            """,
            [{
                'name': 'foo2',
                'is_superuser': True,
            }]
        )

        role = await self.con.query_one('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNotNone(role.password)

        await self.con.execute(r"""
            ALTER ROLE foo2 {
                SET password := {}
            };
        """)

        role = await self.con.query_one('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNone(role.password)

    async def test_edgeql_ddl_role_03(self):
        await self.con.execute(r"""
            CREATE SUPERUSER ROLE foo3 {
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
                    is_superuser,
                    password,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo4'
            """,
            [{
                'name': 'foo4',
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

    async def test_edgeql_ddl_rename_04(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test::rename_link_04 {
                CREATE PROPERTY rename_prop_04 -> std::int64;
            };

            CREATE TYPE test::LinkedObj04;
            CREATE TYPE test::RenameObj04 {
                CREATE MULTI LINK rename_link_04 EXTENDING test::rename_link_04
                    -> test::LinkedObj04;
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

    async def test_edgeql_ddl_rename_05(self):
        await self.con.execute("""
            CREATE TYPE test::GrandParent01 {
                CREATE PROPERTY foo -> int64;
            };

            CREATE TYPE test::Parent01 EXTENDING test::GrandParent01;
            CREATE TYPE test::Parent02 EXTENDING test::GrandParent01;

            CREATE TYPE test::Child EXTENDING test::Parent01, test::Parent02;

            ALTER TYPE test::GrandParent01 {
                ALTER PROPERTY foo RENAME TO renamed;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::Child.renamed;
            ''',
            []
        )

    async def test_edgeql_ddl_rename_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot rename inherited property 'foo'"):
            await self.con.execute("""
                CREATE TYPE test::Parent01 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE test::Parent02 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE test::Child
                    EXTENDING test::Parent01, test::Parent02;

                ALTER TYPE test::Parent02 {
                    ALTER PROPERTY foo RENAME TO renamed;
                };
            """)

    async def test_edgeql_ddl_rename_07(self):
        await self.con.execute("""
            CREATE TYPE test::Foo;

            CREATE TYPE test::Bar {
                CREATE MULTI LINK foo -> test::Foo {
                    SET default := (SELECT test::Foo);
                }
            };

            ALTER TYPE test::Foo RENAME TO test::FooRenamed;
        """)

    @test.xfail('''
        Fails with the below on "CREATE ALIAS Alias2":

        edb.errors.InvalidReferenceError: schema item
        'test::__test|Alias1@@w~1__user2' does not exist
    ''')
    async def test_edgeql_ddl_alias_01(self):
        # Issue #1184
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
            };

            CREATE TYPE Award {
                CREATE LINK user -> User;
            };

            CREATE ALIAS Alias1 := Award {
                user2 := (SELECT .user {name2 := .name ++ '!'})
            };

            CREATE ALIAS Alias2 := Alias1;
        """)

        # TODO: Add an actual INSERT/SELECT test.

    @test.xfail('''
        Fails with the below on "CREATE ALIAS Alias2":

        edgedb.errors.SchemaError: ObjectType 'test::test|User@@view~1'
        is already present in the schema <Schema gen:4121 at 0x109f222d0>
    ''')
    async def test_edgeql_ddl_alias_02(self):
        # Issue #1184
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
            };

            CREATE TYPE Award {
                CREATE REQUIRED PROPERTY name -> str;
            };

            CREATE ALIAS Alias1 := Award {
                a_user := (SELECT User { name } LIMIT 1)
            };

            CREATE ALIAS Alias2 := Alias1;
        """)

        # TODO: Add an actual INSERT/SELECT test.

    async def test_edgeql_ddl_alias_03(self):
        await self.con.execute(r"""
            CREATE ALIAS test::RenameAlias03 := (
                SELECT BaseObject {
                    alias_computable := 'rename alias 03'
                }
            );

            ALTER ALIAS test::RenameAlias03 {
                RENAME TO test::NewAlias03;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::NewAlias03.alias_computable LIMIT 1;
            ''',
            ['rename alias 03']
        )

    async def test_edgeql_ddl_alias_04(self):
        await self.con.execute(r"""
            CREATE ALIAS test::DupAlias04_1 := BaseObject {
                foo := 'hello world 04'
            };

            # create an identical alias with a different name
            CREATE ALIAS test::DupAlias04_2 := BaseObject {
                foo := 'hello world 04'
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT test::DupAlias04_1.foo LIMIT 1;
            ''',
            ['hello world 04']
        )

        await self.assert_query_result(
            r'''
                SELECT test::DupAlias04_2.foo LIMIT 1;
            ''',
            ['hello world 04']
        )

    async def test_edgeql_ddl_alias_05(self):
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE BaseType05 {
                CREATE PROPERTY name -> str;
            };

            CREATE ALIAS BT05Alias1 := BaseType05 {
                a := .name ++ '_more'
            };

            # alias of an alias
            CREATE ALIAS BT05Alias2 := BT05Alias1 {
                b := .a ++ '_stuff'
            };

            INSERT BaseType05 {name := 'bt05'};
        """)

        await self.assert_query_result(
            r'''
                SELECT BT05Alias1 {name, a};
            ''',
            [{
                'name': 'bt05',
                'a': 'bt05_more',
            }]
        )

        await self.assert_query_result(
            r'''
                SELECT BT05Alias2 {name, a, b};
            ''',
            [{
                'name': 'bt05',
                'a': 'bt05_more',
                'b': 'bt05_more_stuff',
            }]
        )

    @test.xfail('''
        See test_edgeql_ddl_alias_08 first.

        Fails to create "BT06Alias3":

        edgedb.errors.SchemaError: ObjectType
        'test::test|BT06Alias1@@w~1' is already present in the
        schema <Schema gen:3431 at 0x7fe1a7b69880>
    ''')
    async def test_edgeql_ddl_alias_06(self):
        # Issue #1184
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE BaseType06 {
                CREATE PROPERTY name -> str;
            };

            CREATE ALIAS BT06Alias1 := BaseType06 {
                a := .name ++ '_a'
            };

            CREATE ALIAS BT06Alias2 := BT06Alias1 {
                b := .a ++ '_b'
            };

            CREATE ALIAS BT06Alias3 := BaseType06 {
                b := BT06Alias1
            };
        """)

        # TODO: Add an actual INSERT/SELECT test.

    async def test_edgeql_ddl_alias_07(self):
        # Issue #1187
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "illegal self-reference in definition of "
                "'test::IllegalAlias07'"):

            await self.con.execute(r"""
                WITH MODULE test
                CREATE ALIAS IllegalAlias07 := Object {a := IllegalAlias07};
            """)

    @test.xfail('''
        Fails to create "BT06Alias3":

        edgedb.errors.SchemaError: ObjectType
        'test::test|BT06Alias1@@w~1' is already present in the
        schema <Schema gen:3431 at 0x7fe1a7b69880>

        This is actually causing the same issue as test_edgeql_ddl_alias_06,
        but it means that the implicit aliases don't get cleaned up.

        This means that this should be fixed first before it gets
        masked by the fix to the other bug.
    ''')
    async def test_edgeql_ddl_alias_08(self):
        # Issue #1184
        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE BaseType06 {
                CREATE PROPERTY name -> str;
            };

            CREATE ALIAS BT06Alias1 := BaseType06 {
                a := .name ++ '_a'
            };

            CREATE ALIAS BT06Alias2 := BT06Alias1 {
                b := .a ++ '_b'
            };

            # drop the freshly created alias
            DROP ALIAS BT06Alias2;

            # re-create the alias that was just dropped
            CREATE ALIAS BT06Alias2 := BT06Alias1 {
                b := .a ++ '_b'
            };
        """)

        # TODO: Add an actual INSERT/SELECT test.

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

    async def test_edgeql_ddl_inheritance_alter_02(self):
        await self.con.execute(r"""
            CREATE TYPE test::InhTest01 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE test::InhTest01_child EXTENDING test::InhTest01;
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                "cannot drop inherited property 'testp'"):

            await self.con.execute("""
                ALTER TYPE test::InhTest01_child {
                    DROP PROPERTY testp;
                }
            """)

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
        """, [])

    async def test_edgeql_ddl_inheritance_alter_04(self):
        await self.con.execute(r"""
            CREATE TYPE test::InhTest04 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE test::InhTest04_child EXTENDING test::InhTest04;
        """)

        await self.con.execute(r"""
            ALTER TYPE test::InhTest04_child {
                ALTER PROPERTY testp {
                    SET default := 42;
                };
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    properties: {
                        name,
                        default,
                    }
                    FILTER .name = 'testp',
                }
                FILTER .name = 'test::InhTest04_child';
            """,
            [{
                'properties': [{
                    'name': 'testp',
                    'default': '42',
                }],
            }],
        )

    async def test_edgeql_ddl_constraint_01(self):
        # Test that the inherited constraint doesn't end up with some
        # bad name like 'default::std::exclusive'.
        await self.con.execute(r"""
            CREATE ABSTRACT TYPE test::BaseTypeCon01;
            CREATE TYPE test::TypeCon01 EXTENDING test::BaseTypeCon01;
            ALTER TYPE test::BaseTypeCon01
                CREATE SINGLE PROPERTY name -> std::str;
            # make sure that we can create a constraint in the base
            # type now
            ALTER TYPE test::BaseTypeCon01
                ALTER PROPERTY name
                    CREATE DELEGATED CONSTRAINT exclusive;
        """)

        await self.assert_query_result("""
            WITH MODULE schema
            SELECT ObjectType {
                name,
                properties: {
                    name,
                    constraints: {
                        name,
                        delegated,
                    }
                } FILTER .name = 'name'
            }
            FILTER .name LIKE 'test::%TypeCon01'
            ORDER BY .name;
        """, [
            {
                'name': 'test::BaseTypeCon01',
                'properties': [{
                    'name': 'name',
                    'constraints': [{
                        'name': 'std::exclusive',
                        'delegated': True,
                    }],
                }]
            },
            {
                'name': 'test::TypeCon01',
                'properties': [{
                    'name': 'name',
                    'constraints': [{
                        'name': 'std::exclusive',
                        'delegated': False,
                    }],
                }]
            }
        ])

    async def test_edgeql_ddl_constraint_02(self):
        # Regression test for #1441.
        with self.assertRaisesRegex(
            edgedb.InvalidConstraintDefinitionError,
            "must define parameters"
        ):
            async with self._run_and_rollback():
                await self.con.execute('''
                    CREATE ABSTRACT CONSTRAINT aaa EXTENDING max_len_value;

                    CREATE SCALAR TYPE foo EXTENDING str {
                        CREATE CONSTRAINT aaa(10);
                    };
                ''')

    async def test_edgeql_ddl_constraint_03(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE test::TypeCon03 {
                CREATE PROPERTY name -> str {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT test::TypeCon03 {name := 'OK'};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon03;
                """)

    @test.xfail('''
        EXISTS constraint violation not raised for MULTI property.
    ''')
    async def test_edgeql_ddl_constraint_04(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE test::TypeCon04 {
                CREATE MULTI PROPERTY name -> str {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT test::TypeCon04 {name := 'OK'};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon04 {name := {}};
                """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon04;
                """)

    async def test_edgeql_ddl_constraint_05(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE test::Child05;
            CREATE TYPE test::TypeCon05 {
                CREATE LINK child -> test::Child05 {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT test::Child05;
            INSERT test::TypeCon05 {child := (SELECT test::Child05 LIMIT 1)};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid child'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon05;
                """)

    @test.xfail('''
        EXISTS constraint violation not raised for MULTI links.
    ''')
    async def test_edgeql_ddl_constraint_06(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE test::Child06;
            CREATE TYPE test::TypeCon06 {
                CREATE MULTI LINK children -> test::Child06 {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT test::Child06;
            INSERT test::TypeCon06 {children := test::Child06};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid children'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon06;
                """)

    async def test_edgeql_ddl_constraint_07(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE test::Child07;
            CREATE TYPE test::TypeCon07 {
                CREATE LINK child -> test::Child07 {
                    CREATE PROPERTY index -> int64;
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__@index)
                }
            };
        """)

        await self.con.execute("""
            INSERT test::Child07;
            INSERT test::TypeCon07 {
                child := (SELECT test::Child07 LIMIT 1){@index := 0}
            };
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid child'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::TypeCon07 {
                        child := (SELECT test::Child07 LIMIT 1)
                    };
                """)

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
                    DROP CONSTRAINT min_value(0);
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
        """, [
            {
                'name': 'test::ConTest01',
                'properties': [{
                    'name': 'con_test',
                    'constraints': {},
                }]
            }
        ])

    async def test_edgeql_ddl_constraint_alter_02(self):
        # Create constraint, then add and drop annotation for it. This
        # is similar to `test_edgeql_ddl_annotation_06`.
        await self.con.execute(r'''
            CREATE SCALAR TYPE test::contest2_t EXTENDING int64 {
                CREATE CONSTRAINT expression ON (__subject__ > 0);
            };
        ''')

        await self.con.execute(r'''
            ALTER SCALAR TYPE test::contest2_t {
                ALTER CONSTRAINT expression ON (__subject__ > 0) {
                    CREATE ANNOTATION title := 'my constraint 2'
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest2_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "my constraint 2",
                    }]
                }]
            }]
        )

        await self.con.execute(r'''
            ALTER SCALAR TYPE test::contest2_t {
                ALTER CONSTRAINT expression ON (__subject__ > 0) {
                    DROP ANNOTATION title;
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest2_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": []
                }]
            }]
        )

    async def test_edgeql_ddl_constraint_alter_03(self):
        # Create constraint annotation using DDL, then drop annotation
        # using SDL. This is similar to `test_edgeql_ddl_annotation_07`.
        await self.con.execute(r'''
            CREATE SCALAR TYPE test::contest3_t EXTENDING int64 {
                CREATE CONSTRAINT expression ON (__subject__ > 0) {
                    CREATE ANNOTATION title := 'my constraint 3';
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest3_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "my constraint 3",
                    }]
                }]
            }]
        )

        await self.migrate(r'''
            scalar type contest3_t extending int64 {
                constraint expression on (__subject__ > 0);
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest3_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": []
                }]
            }]
        )

    async def test_edgeql_ddl_constraint_alter_04(self):
        # Create constraints using DDL, then add annotation to it
        # using SDL. This tests how "on expr" is handled. This is
        # similar to `test_edgeql_ddl_annotation_08`.
        await self.con.execute(r'''
            CREATE SCALAR TYPE test::contest4_t EXTENDING int64 {
                CREATE CONSTRAINT expression ON (__subject__ > 0);
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest4_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": []
                }]
            }]
        )

        await self.migrate(r'''
            scalar type contest4_t extending int64 {
                constraint expression on (__subject__ > 0) {
                    annotation title := 'my constraint 5';
                }
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ScalarType {
                    constraints: {
                        subjectexpr,
                        annotations: {
                            name,
                            @value,
                        }
                    }
                }
                FILTER
                    .name = 'test::contest4_t';
            ''',
            [{
                "constraints": [{
                    "subjectexpr": "(__subject__ > 0)",
                    "annotations": [{
                        "name": "std::title",
                        "@value": "my constraint 5",
                    }]
                }]
            }]
        )

    async def test_edgeql_ddl_drop_inherited_link(self):
        await self.con.execute(r"""
            CREATE TYPE test::Target;
            CREATE TYPE test::Parent {
                CREATE LINK dil_foo -> test::Target;
            };

            CREATE TYPE test::Child EXTENDING test::Parent;
            CREATE TYPE test::GrandChild EXTENDING test::Child;
       """)

        await self.con.execute("""
            ALTER TYPE test::Parent DROP LINK dil_foo;
        """)

    async def test_edgeql_ddl_drop_01(self):
        # Check that constraints defined on scalars being dropped are
        # dropped.
        await self.con.execute("""
            CREATE SCALAR TYPE test::a1 EXTENDING std::str;

            ALTER SCALAR TYPE test::a1 {
                CREATE CONSTRAINT std::one_of('a', 'b') {
                    CREATE ANNOTATION description :=
                        'test_delta_drop_01_constraint';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Constraint {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_01_constraint';
            """,
            [
                {
                    'name': 'std::one_of',
                }
            ],
        )

        await self.con.execute("""
            DROP SCALAR TYPE test::a1;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Constraint {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_01_constraint';
            """,
            []
        )

    async def test_edgeql_ddl_drop_02(self):
        # Check that links defined on types being dropped are
        # dropped.
        await self.con.execute("""
            CREATE TYPE test::C1 {
                CREATE PROPERTY l1 -> std::str {
                    CREATE ANNOTATION description := 'test_delta_drop_02_link';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Property {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_02_link';
            """,
            [
                {
                    'name': 'l1',
                }
            ],
        )

        await self.con.execute("""
            DROP TYPE test::C1;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Property {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_02_link';
            """,
            []
        )

    async def test_edgeql_ddl_drop_refuse_01(self):
        # Check that the schema refuses to drop objects with live references
        await self.con.execute("""
            CREATE TYPE test::DropA;
            CREATE ABSTRACT ANNOTATION test::dropattr;
            CREATE ABSTRACT LINK test::l1_parent;
            CREATE TYPE test::DropB {
                CREATE LINK l1 EXTENDING test::l1_parent -> test::DropA {
                    CREATE ANNOTATION test::dropattr := 'foo';
                };
            };
            CREATE SCALAR TYPE test::dropint EXTENDING int64;
            CREATE FUNCTION test::dropfunc(a: test::dropint) -> int64
                USING EdgeQL $$ SELECT a $$;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop object type.*test::DropA.*other objects'):
            await self.con.execute('DROP TYPE test::DropA')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract anno.*test::dropattr.*other objects'):
            await self.con.execute('DROP ABSTRACT ANNOTATION test::dropattr')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract link.*test::l1_parent.*other objects'):
            await self.con.execute('DROP ABSTRACT LINK test::l1_parent')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop.*dropint.*other objects'):
            await self.con.execute('DROP SCALAR TYPE test::dropint')

    async def test_edgeql_ddl_unicode_01(self):
        await self.con.execute(r"""
            # setup delta
            START MIGRATION TO {
                module test {
                    type Пример {
                        required property номер -> int16;
                    };
                };
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
            SET MODULE test;

            INSERT Пример {
                номер := 987
            };
            INSERT Пример {
                номер := 456
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    Пример {
                        номер
                    }
                ORDER BY
                    Пример.номер;
            """,
            [{'номер': 456}, {'номер': 987}]
        )

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

        await self.con.execute(r"""
            ALTER TYPE test::TupProp02 {
                CREATE PROPERTY p5 -> array<tuple<int64>>;
            };
        """)

        await self.con.execute('DECLARE SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                'expected a scalar type, or a scalar collection'):

            await self.con.execute(r"""
                ALTER TYPE test::TupProp02 {
                    CREATE PROPERTY p6 -> tuple<test::TupProp02>;
                };
            """)

        # Recover.
        await self.con.execute('ROLLBACK TO SAVEPOINT t0;')

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
                'constraints cannot be defined on enumerated type.*'):
            await self.con.execute('''
                CREATE SCALAR TYPE test::my_enum_3
                    EXTENDING enum<'foo', 'bar', 'baz'> {
                    CREATE CONSTRAINT expression ON (EXISTS(__subject__))
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

    async def test_edgeql_ddl_explicit_id(self):
        await self.con.execute('''
            CREATE TYPE test::ExID {
                SET id := <uuid>'00000000-0000-0000-0000-0000feedbeef'
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    id
                }
                FILTER .name = 'test::ExID';
            """,
            [{
                'id': '00000000-0000-0000-0000-0000feedbeef',
            }],
        )

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'cannot alter object id'):
            await self.con.execute('''
                ALTER TYPE test::ExID {
                    SET id := <uuid>'00000000-0000-0000-0000-0000feedbeef'
                }
            ''')

    async def test_edgeql_ddl_quoting_01(self):
        await self.con.execute("""
            CREATE TYPE test::`U S``E R` {
                CREATE PROPERTY `n ame` -> str;
            };
        """)

        await self.con.execute("""
            INSERT test::`U S``E R` {
                `n ame` := 'quoting_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::`U S``E R` {
                    __type__: {
                        name
                    },
                    `n ame`
                };
            """,
            [{
                '__type__': {'name': 'test::U S`E R'},
                'n ame': 'quoting_01'
            }],
        )

        await self.con.execute("""
            DROP TYPE test::`U S``E R`;
        """)

    async def test_edgeql_ddl_link_overload_01(self):
        await self.con.execute("""
            CREATE TYPE T;
            CREATE TYPE A {
                CREATE MULTI LINK t -> T;
            };
            CREATE TYPE B EXTENDING A;
            INSERT T;
            INSERT B {
                t := T
            };
            ALTER TYPE B ALTER LINK t CREATE ANNOTATION title := 'overloaded';
            UPDATE B SET { t := T };
        """)

        await self.assert_query_result(
            r"""
            SELECT A { ct := count(.t) };
            """,
            [{
                'ct': 1,
            }]
        )

    async def test_edgeql_ddl_readonly_01(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'foo' of "
                "object type 'test::Derived': it is defined as True in "
                "property 'foo' of object type 'test::Derived' and as "
                "False in property 'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE PROPERTY foo -> str;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER PROPERTY foo {
                        SET readonly := True;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_02(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "property 'foo' of object type 'test::Derived' and as "
                "True in property 'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE PROPERTY foo -> str {
                        SET readonly := True;
                    };
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER PROPERTY foo {
                        SET readonly := False;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_03(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "property 'foo' of object type 'test::Base0' and as "
                "True in property 'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base0 {
                    CREATE PROPERTY foo -> str;
                };
                CREATE TYPE Base1 {
                    CREATE PROPERTY foo -> str {
                        SET readonly := True;
                    };
                };
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_readonly_04(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE Base0 {
                CREATE PROPERTY foo -> str;
            };
            CREATE TYPE Base1 {
                CREATE PROPERTY foo -> str;
            };
            CREATE TYPE Derived EXTENDING Base0, Base1;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "property 'foo' of object type 'test::Base0' and as "
                "True in property 'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                ALTER TYPE Base1 {
                    ALTER PROPERTY foo {
                        SET readonly := True;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_05(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of link 'foo' of "
                "object type 'test::Derived': it is defined as True in "
                "link 'foo' of object type 'test::Derived' and as "
                "False in link 'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        SET readonly := True;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_06(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of link 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "link 'foo' of object type 'test::Derived' and as "
                "True in link 'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE LINK foo -> Object {
                        SET readonly := True;
                    };
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        SET readonly := False;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_07(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of link 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "link 'foo' of object type 'test::Base0' and as "
                "True in link 'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base0 {
                    CREATE LINK foo -> Object;
                };
                CREATE TYPE Base1 {
                    CREATE LINK foo -> Object {
                        SET readonly := True;
                    };
                };
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_readonly_08(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE Base0 {
                CREATE LINK foo -> Object;
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> Object;
            };
            CREATE TYPE Derived EXTENDING Base0, Base1;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of link 'foo' of "
                "object type 'test::Derived': it is defined as False in "
                "link 'foo' of object type 'test::Base0' and as "
                "True in link 'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                ALTER TYPE Base1 {
                    ALTER LINK foo {
                        SET readonly := True;
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_09(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'bar' of "
                "link 'foo' of object type 'test::Derived': it is defined "
                "as True in property 'bar' of link 'foo' of object type "
                "'test::Derived' and as False in property 'bar' of link "
                "'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE LINK foo -> Object {
                        CREATE PROPERTY bar -> str;
                    };
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        ALTER PROPERTY bar {
                            SET readonly := True;
                        }
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_10(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'bar' of "
                "link 'foo' of object type 'test::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'test::Derived' and as True in property 'bar' of link "
                "'foo' of object type 'test::Base'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE LINK foo -> Object {
                        CREATE PROPERTY bar -> str {
                            SET readonly := True;
                        };
                    };
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        ALTER PROPERTY bar {
                            SET readonly := False;
                        }
                    };
                };
            ''')

    async def test_edgeql_ddl_readonly_11(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'bar' of "
                "link 'foo' of object type 'test::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'test::Base0' and as True in property 'bar' of link "
                "'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base0 {
                    CREATE LINK foo -> Object {
                        CREATE PROPERTY bar -> str;
                    };
                };
                CREATE TYPE Base1 {
                    CREATE LINK foo -> Object {
                        CREATE PROPERTY bar -> str {
                            SET readonly := True;
                        };
                    };
                };
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_readonly_12(self):
        # Test that read-only flag must be consistent in the
        # inheritance hierarchy.
        await self.con.execute('''
            SET MODULE test;

            CREATE TYPE Base0 {
                CREATE LINK foo -> Object {
                    CREATE PROPERTY bar -> str;
                };
            };
            CREATE TYPE Base1 {
                CREATE LINK foo -> Object {
                    CREATE PROPERTY bar -> str;
                };
            };
            CREATE TYPE Derived EXTENDING Base0, Base1;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot redefine the readonly flag of property 'bar' of "
                "link 'foo' of object type 'test::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'test::Base0' and as True in property 'bar' of link "
                "'foo' of object type 'test::Base1'."):
            await self.con.execute('''
                ALTER TYPE Base1 {
                    ALTER LINK foo {
                        ALTER PROPERTY bar {
                            SET readonly := True;
                        };
                    };
                };
            ''')

    async def test_edgeql_ddl_required_01(self):
        # Test that required qualifier cannot be dropped if it was not
        # actually set on the particular property.
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "cannot make.*optional",
        ):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE REQUIRED PROPERTY foo -> str;
                };
                CREATE TYPE Derived EXTENDING Base;
                ALTER TYPE Derived {
                    ALTER PROPERTY foo {
                        DROP REQUIRED;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_02(self):
        # Test that required qualifier cannot be dropped if it was not
        # actually set on the particular property.
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "cannot make.*optional",
        ):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE REQUIRED PROPERTY foo -> str;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER PROPERTY foo {
                        DROP REQUIRED;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_03(self):
        # Test that required qualifier cannot be dropped if it was not
        # actually set on the particular link.
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "cannot make.*optional",
        ):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base;
                ALTER TYPE Derived {
                    ALTER LINK foo {
                        DROP REQUIRED;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_04(self):
        # Test that required qualifier cannot be dropped if it was not
        # actually set on the particular link.
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "cannot make.*optional",
        ):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        DROP REQUIRED;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_05(self):
        # Test that required qualifier cannot be dropped if it was not
        # actually set on the particular link.
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "cannot make.*optional",
        ):
            await self.con.execute('''
                SET MODULE test;

                CREATE TYPE Base {
                    CREATE OPTIONAL LINK foo -> Object;
                };
                CREATE TYPE Base2 {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base, Base2 {
                    ALTER LINK foo {
                        DROP REQUIRED;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_08(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE Base {
                CREATE REQUIRED PROPERTY foo -> str;
            };
            CREATE TYPE Derived EXTENDING Base {
                ALTER PROPERTY foo {
                    # overloading the property to be required
                    # regardless of the ancestors
                    SET REQUIRED;
                };
            };
        """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Base.foo'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base;
                """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Derived.foo'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Base {
                ALTER PROPERTY foo {
                    DROP REQUIRED;
                };
            };
        """)

        await self.con.execute("""
            INSERT Base;
        """)
        await self.assert_query_result(
            r'''
                SELECT count(Base);
            ''',
            [1],
        )

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Derived.foo'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Derived {
                ALTER PROPERTY foo {
                    DROP REQUIRED;
                };
            };
        """)

        await self.con.execute("""
            INSERT Derived;
        """)
        await self.assert_query_result(
            r'''
                SELECT count(Derived);
            ''',
            [1],
        )

    async def test_edgeql_ddl_required_09(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            SET MODULE test;

            CREATE TYPE Base {
                CREATE OPTIONAL PROPERTY foo -> str;
            };
            CREATE TYPE Derived EXTENDING Base {
                ALTER PROPERTY foo {
                    # overloading the property to be required
                    # regardless of the ancestors
                    SET REQUIRED;
                };
            };
        """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Derived.foo'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            INSERT Base;
        """)
        await self.assert_query_result(
            r'''
                SELECT count(Base);
            ''',
            [1],
        )

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Derived.foo'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Derived {
                ALTER PROPERTY foo {
                    DROP REQUIRED;
                };
            };
        """)

        await self.con.execute("""
            INSERT Derived;
        """)
        await self.assert_query_result(
            r'''
                SELECT count(Derived);
            ''',
            [1],
        )

    @test.xfail('''
        MissingRequiredError not raised
    ''')
    async def test_edgeql_ddl_required_10(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            CREATE TYPE test::Base {
                CREATE REQUIRED MULTI PROPERTY name -> str;
            };
        """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Base.name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::Base;
                """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Base.name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::Base {name := {}};
                """)

    @test.xfail('''
        MissingRequiredError not raised
    ''')
    async def test_edgeql_ddl_required_11(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            CREATE TYPE test::Child;
            CREATE TYPE test::Base {
                CREATE REQUIRED MULTI LINK children -> test::Child;
            };
        """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Base.children'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::Base;
                """)

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r'Base.children'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT test::Base {children := {}};
                """)

    async def test_edgeql_ddl_prop_alias(self):
        await self.con.execute("""
            CREATE TYPE Named {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY canonical_name := .name;
            };
        """)

    async def test_edgeql_ddl_index_01(self):
        with self.assertRaisesRegex(
            edgedb.ResultCardinalityMismatchError,
            r"possibly more than one element returned by the index expression"
        ):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE MULTI PROPERTY a -> int64;
                    CREATE INDEX ON (.a);
                }
            """)

    async def test_edgeql_ddl_index_02(self):
        with self.assertRaisesRegex(
            edgedb.ResultCardinalityMismatchError,
            r"possibly more than one element returned by the index expression"
        ):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY a -> int64;
                    CREATE PROPERTY b -> int64;
                    CREATE INDEX ON ({.a, .b});
                }
            """)

    async def test_edgeql_ddl_index_03(self):
        with self.assertRaisesRegex(
            edgedb.ResultCardinalityMismatchError,
            r"possibly more than one element returned by the index expression"
        ):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY a -> int64;
                    CREATE PROPERTY b -> int64;
                    CREATE INDEX ON (array_unpack([.a, .b]));
                }
            """)

    async def test_edgeql_ddl_errors_01(self):
        await self.con.execute('''
            WITH MODULE test
            CREATE TYPE Err1 {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            WITH MODULE test
            ALTER TYPE Err1
            CREATE REQUIRED LINK bar -> Err1;
        ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'b' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER PROPERTY b
                    CREATE CONSTRAINT std::regexp(r'b');
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'b' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 DROP PROPERTY b
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'test::a' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    DROP CONSTRAINT a;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'test::a' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    ALTER CONSTRAINT a ON (foo > 0) {
                        CREATE ANNOTATION title := 'test'
                    }
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    ALTER ANNOTATION title := 'aaa'
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    DROP ANNOTATION title;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    ALTER ANNOTATION title := 'aaa'
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    DROP ANNOTATION title
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                edgedb.errors.InvalidReferenceError,
                r"index on \(.foo\) does not exist on"
                r" object type 'test::Err1'",
            ):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    DROP INDEX ON (.foo)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                edgedb.errors.InvalidReferenceError,
                r"index on \(.zz\) does not exist on object type 'test::Err1'",
            ):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    DROP INDEX ON (.zz)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'test::Err1' has no link or property 'zz'"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    CREATE INDEX ON (.zz)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'test::Err1' has no link or property 'zz'"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    CREATE INDEX ON ((.foo, .zz))
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'test::blah' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    CREATE TYPE Err1 EXTENDING blah {
                        CREATE PROPERTY foo -> str;
                    };
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'test::blah' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    CREATE TYPE Err2 EXTENDING test::blah {
                        CREATE PROPERTY foo -> str;
                    };
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'b' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER LINK b
                    CREATE CONSTRAINT std::regexp(r'b');
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'b' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 DROP LINK b;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER LINK bar
                    DROP ANNOTATION title;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'std::min_value' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1 ALTER LINK bar
                    DROP CONSTRAINT min_value(0);
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'spam' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err1
                    ALTER LINK bar
                    DROP PROPERTY spam;
                ''')

    @test.xfail('''
        The test currently fails with "property 'spam' does not exist",
        but it should fail with "link 'foo' does not exist", as
        `ALTER LINK foo` is the preceeding invalid command.
    ''')
    async def test_edgeql_ddl_errors_02(self):
        await self.con.execute('''
            WITH MODULE test
            CREATE TYPE Err2 {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            WITH MODULE test
            ALTER TYPE Err2
            CREATE REQUIRED LINK bar -> Err2;
        ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'foo' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER TYPE Err2
                    ALTER LINK foo
                    DROP PROPERTY spam;
                ''')

    async def test_edgeql_ddl_errors_03(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "function 'test::foo___1' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    ALTER FUNCTION foo___1(a: int64)
                    SET volatility := 'STABLE';
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "function 'test::foo___1' does not exist"):
                await self.con.execute('''
                    WITH MODULE test
                    DROP FUNCTION foo___1(a: int64);
                ''')

    async def test_edgeql_ddl_create_migration_01(self):
        await self.con.execute(f'''
            CREATE MIGRATION
            {{
                CREATE TYPE Type1 {{
                    CREATE PROPERTY field1 -> str;
                }};
            }};
        ''')

        await self.assert_query_result(
            '''
            SELECT schema::ObjectType {
                name
            } FILTER .name = 'default::Type1'
            ''',
            [{
                'name': 'default::Type1',
            }]
        )
