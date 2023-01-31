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
import os
import re
import textwrap
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLDDL(tb.DDLTestCase):

    async def test_edgeql_ddl_04(self):
        await self.con.execute("""
            CREATE TYPE A;
            CREATE TYPE B EXTENDING A;

            CREATE TYPE Object1 {
                CREATE REQUIRED LINK a -> A;
            };

            CREATE TYPE Object2 {
                CREATE LINK a -> B;
            };

            CREATE TYPE Object_12
                EXTENDING Object1, Object2;
        """)

    async def test_edgeql_ddl_type_05(self):
        await self.con.execute("""
            CREATE TYPE A5;
            CREATE TYPE Object5 {
                CREATE REQUIRED LINK a -> A5;
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
                FILTER .name = 'default::Object5';
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
            ALTER TYPE Object5 {
                ALTER LINK a SET OPTIONAL;
            };

            ALTER TYPE Object5 {
                ALTER PROPERTY b SET OPTIONAL;
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
                FILTER .name = 'default::Object5';
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
            CREATE TYPE A6 {
                CREATE PROPERTY name -> str;
            };

            CREATE TYPE Object6 {
                CREATE SINGLE LINK a -> A6;
                CREATE SINGLE PROPERTY b -> str;
            };

            INSERT A6 { name := 'a6' };
            INSERT Object6 {
                a := (SELECT A6 LIMIT 1),
                b := 'foo'
            };
            INSERT Object6;
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
                FILTER .name = 'default::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'One',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'One',
                }],
            }],
        )

        await self.assert_query_result(
            r"""
            SELECT Object6 {
                a: {name},
                b,
            } FILTER EXISTS .a
            """,
            [{
                'a': {'name': 'a6'},
                'b': 'foo',
            }]
        )

        await self.con.execute("""
            ALTER TYPE Object6 {
                ALTER LINK a SET MULTI;
            };

            ALTER TYPE Object6 {
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
                FILTER .name = 'default::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'Many',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'Many',
                }],
            }],
        )

        # Check that the data has been migrated correctly.
        await self.assert_query_result(
            r"""
            SELECT Object6 {
                a: {name},
                b,
            } FILTER EXISTS .a
            """,
            [{
                'a': [{'name': 'a6'}],
                'b': ['foo'],
            }]
        )

        # Change it back.
        await self.con.execute("""
            ALTER TYPE Object6 {
                ALTER LINK a SET SINGLE USING (SELECT .a LIMIT 1);
            };

            ALTER TYPE Object6 {
                ALTER PROPERTY b SET SINGLE USING (SELECT .b LIMIT 1);
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
                FILTER .name = 'default::Object6';
            """,
            [{
                'links': [{
                    'name': 'a',
                    'cardinality': 'One',
                }],

                'properties': [{
                    'name': 'b',
                    'cardinality': 'One',
                }],
            }],
        )

        # Check that the data has been migrated correctly.
        await self.assert_query_result(
            r"""
            SELECT Object6 {
                a: {name},
                b,
            } FILTER EXISTS .a
            """,
            [{
                'a': {'name': 'a6'},
                'b': 'foo',
            }]
        )

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_ddl_rename_type_and_add_01(self):
        await self.con.execute("""

            CREATE TYPE Foo {
                CREATE PROPERTY x -> str;
            };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                DROP PROPERTY x;
                RENAME TO Bar;
                CREATE PROPERTY a -> str;
                CREATE LINK b -> Object;
                CREATE CONSTRAINT expression ON (true);
                CREATE ANNOTATION description := 'hello';
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT schema::ObjectType {
                links: {name} ORDER BY .name,
                properties: {name} ORDER BY .name,
                constraints: {name},
                annotations: {name}
            }
            FILTER .name = 'default::Bar';
            """,
            [
                {
                    "annotations": [{"name": "std::description"}],
                    "constraints": [{"name": "std::expression"}],
                    "links": [{"name": "__type__"}, {"name": "b"}],
                    "properties": [{"name": "a"}, {"name": "id"}],
                }
            ],
        )

        await self.con.execute("""
            ALTER TYPE Bar {
                DROP PROPERTY a;
                DROP link b;
                DROP CONSTRAINT expression ON (true);
                DROP ANNOTATION description;
            };
        """)

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_ddl_rename_type_and_add_02(self):
        await self.con.execute("""

            CREATE TYPE Foo;
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE PROPERTY a -> str;
                CREATE LINK b -> Object;
                CREATE CONSTRAINT expression ON (true);
                CREATE ANNOTATION description := 'hello';
                RENAME TO Bar;
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT schema::ObjectType {
                links: {name} ORDER BY .name,
                properties: {name} ORDER BY .name,
                constraints: {name},
                annotations: {name}
            }
            FILTER .name = 'default::Bar';
            """,
            [
                {
                    "annotations": [{"name": "std::description"}],
                    "constraints": [{"name": "std::expression"}],
                    "links": [{"name": "__type__"}, {"name": "b"}],
                    "properties": [{"name": "a"}, {"name": "id"}],
                }
            ],
        )

        await self.con.execute("""
            ALTER TYPE Bar {
                DROP PROPERTY a;
                DROP link b;
                DROP CONSTRAINT expression ON (true);
                DROP ANNOTATION description;
            };
        """)

    async def test_edgeql_ddl_rename_type_and_drop_01(self):
        await self.con.execute("""

            CREATE TYPE Foo {
                CREATE PROPERTY a -> str;
                CREATE LINK b -> Object;
                CREATE CONSTRAINT expression ON (true);
                CREATE ANNOTATION description := 'hello';
            };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                RENAME TO Bar;
                DROP PROPERTY a;
                DROP link b;
                DROP CONSTRAINT expression ON (true);
                DROP ANNOTATION description;
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT schema::ObjectType {
                links: {name} ORDER BY .name,
                properties: {name} ORDER BY .name,
                constraints: {name},
                annotations: {name}
            }
            FILTER .name = 'default::Bar';
            """,
            [
                {
                    "annotations": [],
                    "constraints": [],
                    "links": [{"name": "__type__"}],
                    "properties": [{"name": "id"}],
                }
            ],
        )

        await self.con.execute("""
            DROP TYPE Bar;
        """)

    async def test_edgeql_ddl_rename_type_and_drop_02(self):
        await self.con.execute("""

            CREATE TYPE Foo {
                CREATE PROPERTY a -> str;
                CREATE LINK b -> Object;
                CREATE CONSTRAINT expression ON (true);
                CREATE ANNOTATION description := 'hello';
            };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                DROP PROPERTY a;
                DROP link b;
                DROP CONSTRAINT expression ON (true);
                DROP ANNOTATION description;
                RENAME TO Bar;
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT schema::ObjectType {
                links: {name} ORDER BY .name,
                properties: {name} ORDER BY .name,
                constraints: {name},
                annotations: {name}
            }
            FILTER .name = 'default::Bar';
            """,
            [
                {
                    "annotations": [],
                    "constraints": [],
                    "links": [{"name": "__type__"}],
                    "properties": [{"name": "id"}],
                }
            ],
        )
        await self.con.execute("""
            DROP TYPE Bar;
        """)

    async def test_edgeql_ddl_rename_type_and_prop_01(self):
        await self.con.execute(r"""

            CREATE TYPE Note {
                CREATE PROPERTY note -> str;
                CREATE LINK friend -> Object;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Note {
                RENAME TO Remark;
                ALTER PROPERTY note RENAME TO remark;
                ALTER LINK friend RENAME TO enemy;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Remark {
                DROP PROPERTY remark;
                DROP LINK enemy;
            };
        """)

    async def test_edgeql_ddl_11(self):
        await self.con.execute(r"""
            CREATE TYPE TestContainerLinkObjectType {
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
                CREATE TYPE TestBadContainerLinkObjectType {
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
                CREATE TYPE TestBadContainerLinkObjectType {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT expression ON (`self` = 'foo');
                    };
                };
            """)

    async def test_edgeql_ddl_14(self):
        await self.con.execute("""
            CREATE TYPE TestSelfLink1 {
                CREATE PROPERTY foo1 -> std::str;
                CREATE PROPERTY bar1 -> std::str {
                    SET default := __source__.foo1;
                };
            };
        """)

    async def test_edgeql_ddl_15(self):
        await self.con.execute(r"""
            CREATE TYPE TestSelfLink2 {
                CREATE PROPERTY foo2 -> std::str;
                CREATE MULTI PROPERTY bar2 -> std::str {
                    # NOTE: this is a set of all TestSelfLink2.foo2
                    SET default := TestSelfLink2.foo2;
                };
            };

            INSERT TestSelfLink2 {
                foo2 := 'Alice'
            };
            INSERT TestSelfLink2 {
                foo2 := 'Bob'
            };
            INSERT TestSelfLink2 {
                foo2 := 'Carol'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT TestSelfLink2 {
                    foo2,
                    bar2,
                } ORDER BY TestSelfLink2.foo2;
            """,
            [
                {'bar2': [], 'foo2': 'Alice'},
                {'bar2': {'Alice'}, 'foo2': 'Bob'},
                {'bar2': {'Alice', 'Bob'}, 'foo2': 'Carol'}
            ],
        )

    async def test_edgeql_ddl_16(self):
        await self.con.execute(r"""
            CREATE TYPE TestSelfLink3 {
                CREATE PROPERTY foo3 -> std::str;
                CREATE PROPERTY bar3 -> std::str {
                    SET default := TestSelfLink3.foo3;
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
                CREATE PROPERTY foo -> foo_t {
                    SET default := <foo::foo_t>20;
                };
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

        await self.con.execute("""
            ALTER SCALAR TYPE foo::foo_t RENAME TO foo::baz_t;
        """)

        await self.con.execute("""
            ALTER SCALAR TYPE foo::baz_t RENAME TO bar::quux_t;
        """)

        await self.con.execute("""
            DROP TYPE bar::Obj2;
            DROP TYPE foo::Obj;
            DROP SCALAR TYPE bar::quux_t;
        """)

    async def test_edgeql_ddl_19(self):
        await self.con.execute("""

            CREATE TYPE ActualType {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            CREATE ALIAS Alias1 := ActualType {
                bar := 9
            };

            CREATE ALIAS Alias2 := ActualType {
                connected := (SELECT Alias1 ORDER BY Alias1.foo)
            };


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
                FILTER .name = 'default::B20'
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
                FILTER .name = 'default::B20'
            """,
            [
                {
                    'links': [{
                        'name': 'l',
                        'bases': [{
                            'name': 'default::l20',
                        }],
                    }],
                },
            ]
        )

        await self.con.execute("""

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
                FILTER .name = 'default::B20'
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
                          FILTER .name = 'default::Alias1')
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
                          FILTER .name = 'default::User')
                SELECT
                    C {
                        pointers: { @owned }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@owned': False,
                    }, {
                        '@owned': False,
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
                          FILTER .name = 'default::User')
                SELECT
                    C {
                        pointers: { @owned }
                        FILTER .name IN {'name', 'desc'}
                    };
            """,
            [
                {
                    'pointers': [{
                        '@owned': True,
                    }, {
                        '@owned': True,
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
                          FILTER .name = 'default::User')
                SELECT
                    C {
                        pointers: {
                            @owned,
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
                        '@owned': True,
                        'required': True,
                        'constraints': [{
                            'name': 'std::exclusive',
                        }],
                    }, {
                        '@owned': True,
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
                          FILTER .name = 'default::User')
                SELECT
                    C {
                        pointers: {
                            @owned,
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
                        '@owned': False,
                        'required': False,
                        'constraints': [],
                    }, {
                        '@owned': False,
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
                CREATE TYPE Named {
                    CREATE PROPERTY name -> str;
                };
                ALTER TYPE Named ALTER PROPERTY name DROP OWNED;
            """)

    async def test_edgeql_ddl_26(self):
        await self.con.execute("""
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
            ALTER TYPE Child ALTER LINK target DROP OWNED;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'default::Child')
                SELECT
                    C {
                        links: {
                            @owned,
                            required,
                            properties: {
                                name,
                            } ORDER BY .name
                        }
                        FILTER .name = 'target'
                    };
            """,
            [
                {
                    'links': [{
                        '@owned': False,
                        'required': False,
                        'properties': [{"name": "source"}, {"name": "target"}],
                    }],
                },
            ],
        )

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'default::Grandchild')
                SELECT
                    C {
                        links: {
                            @owned,
                            required,
                            properties: {
                                name,
                                @owned,
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
                        '@owned': True,
                        'required': True,
                        'properties': [{
                            'name': 'foo',
                            '@owned': True,
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
                          FILTER .name = 'default::Derived')
                SELECT
                    C {
                        properties: {
                            @owned,
                            required,
                            inherited_fields,
                        }
                        FILTER .name = 'foo'
                    };
            """,
            [
                {
                    'properties': [{
                        '@owned': True,
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
            ALTER TYPE Base DROP PROPERTY foo;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'default::Derived')
                SELECT
                    C {
                        properties: {
                            @owned,
                            required,
                            inherited_fields,
                        }
                        FILTER .name = 'foo'
                    };
            """,
            [
                {
                    'properties': [{
                        '@owned': True,
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
            CREATE TYPE Foo {
                CREATE PROPERTY left -> str;
                CREATE PROPERTY smallint -> str;
                CREATE PROPERTY natural -> str;
                CREATE PROPERTY null -> str;
                CREATE PROPERTY `like` -> str;
                CREATE PROPERTY `create` -> str;
                CREATE PROPERTY `link` -> str;
            };
        """)

    async def test_edgeql_ddl_sequence_01(self):
        await self.con.execute("""
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY index -> std::int64;
            };
        """)

        await self.con.execute("""
            CREATE SCALAR TYPE ctr EXTENDING std::sequence;
            ALTER TYPE Foo {
                ALTER PROPERTY index {
                    SET TYPE ctr;
                };
            };
        """)

        await self.con.execute("""
            INSERT Foo;
        """)

    async def test_edgeql_ddl_abstract_link_01(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test_link;
        """)

    async def test_edgeql_ddl_abstract_link_02(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test_object_link {
                CREATE PROPERTY test_link_prop -> std::int64;
            };

            CREATE TYPE TestObjectType {
                CREATE LINK test_object_link -> std::Object {
                    CREATE PROPERTY test_link_prop -> std::int64 {
                        CREATE ANNOTATION title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl_abstract_link_03(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK test_object_link_prop {
                CREATE PROPERTY link_prop1 -> std::str;
            };
        """)

    async def test_edgeql_ddl_abstract_link_04(self):
        await self.con.execute("""

            CREATE ABSTRACT LINK test_object_link {
                CREATE PROPERTY test_link_prop -> int64;
                CREATE PROPERTY computed_prop := @test_link_prop * 2;
            };

            CREATE TYPE Target;
            CREATE TYPE TestObjectType {
                CREATE LINK test_object_link EXTENDING test_object_link
                   -> Target;
            };

            INSERT TestObjectType {
                test_object_link := (INSERT Target { @test_link_prop := 42 })
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT TestObjectType {
                    test_object_link: { @test_link_prop, @computed_prop },
                };
            """,
            [{"test_object_link":
              {"@computed_prop": 84, "@test_link_prop": 42}}]
        )

    async def test_edgeql_ddl_drop_extending_01(self):
        await self.con.execute("""

            CREATE TYPE Parent {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Child EXTENDING Parent;
        """)

        await self.con.execute("""
            ALTER TYPE Child DROP EXTENDING Parent;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "object type 'default::Child' has no link or property 'name'",
        ):
            await self.con.execute("""
                SELECT Child.name
            """)

        # Should be able to drop parent
        await self.con.execute("""
            DROP TYPE Parent;
        """)

    async def test_edgeql_ddl_drop_extending_02(self):
        await self.con.execute("""

            CREATE TYPE Parent {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Child EXTENDING Parent {
                ALTER PROPERTY name {
                    SET OWNED;
                    ALTER CONSTRAINT exclusive SET OWNED;
                };
            };
        """)

        await self.con.execute("""
            ALTER TYPE Child DROP EXTENDING Parent;
        """)

        # The constraint shouldn't be linked anymore
        await self.con.execute("""
            INSERT Child { name := "foo" };
            INSERT Parent { name := "foo" };
        """)
        await self.con.execute("""
            INSERT Parent { name := "bar" };
            INSERT Child { name := "bar" };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'name violates exclusivity constraint',
        ):
            await self.con.execute("""
                INSERT Parent { name := "bar" };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'name violates exclusivity constraint',
        ):
            await self.con.execute("""
                INSERT Child { name := "bar" };
            """)

        # Should be able to drop parent
        await self.con.execute("""
            DROP TYPE Parent;
        """)

    async def test_edgeql_ddl_drop_extending_03(self):
        await self.con.execute("""

            CREATE TYPE Parent {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Child EXTENDING Parent {
                ALTER PROPERTY name {
                    SET OWNED;
                    ALTER CONSTRAINT exclusive SET OWNED;
                };
            };
            CREATE TYPE Grandchild EXTENDING Child;
        """)

        await self.con.execute("""
            ALTER TYPE Child DROP EXTENDING Parent;
        """)

        # Should be able to drop parent
        await self.con.execute("""
            DROP TYPE Parent;
        """)

    async def test_edgeql_ddl_drop_extending_04(self):
        await self.con.execute("""

            CREATE TYPE Parent {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Child EXTENDING Parent {
                ALTER PROPERTY name {
                    SET OWNED;
                    ALTER CONSTRAINT exclusive SET OWNED;
                };
            };
            CREATE TYPE Grandchild EXTENDING Child {
                ALTER PROPERTY name {
                    SET OWNED;
                    ALTER CONSTRAINT exclusive SET OWNED;
                };
            };
        """)

        await self.con.execute("""
            ALTER TYPE Grandchild DROP EXTENDING Child;
        """)

        # Should be able to drop parent
        await self.con.execute("""
            DROP TYPE Child;
            DROP TYPE Parent;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'name violates exclusivity constraint',
        ):
            await self.con.execute("""
                INSERT Grandchild { name := "bar" };
                INSERT Grandchild { name := "bar" };
            """)

    async def test_edgeql_ddl_drop_extending_05(self):
        await self.con.execute("""

            CREATE TYPE Parent {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Child EXTENDING Parent {
                ALTER PROPERTY name {
                    SET OWNED;
                };
            };
        """)

        await self.con.execute("""
            ALTER TYPE Child DROP EXTENDING Parent;
        """)

        # The constraint on Child should be dropped
        await self.con.execute("""
            INSERT Child { name := "foo" };
            INSERT Child { name := "foo" };
        """)

    async def test_edgeql_ddl_drop_extending_06(self):
        await self.con.execute("""

            CREATE ABSTRACT TYPE Named {
                CREATE OPTIONAL SINGLE PROPERTY name -> str;
            };
            CREATE TYPE Foo EXTENDING Named;
        """)

        await self.con.execute("""
            INSERT Foo { name := "Phil Emarg" };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                ALTER PROPERTY name {
                    SET OWNED;
                };
                DROP EXTENDING Named;
            };
            DROP TYPE Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            ["Phil Emarg"],
        )

    async def test_edgeql_ddl_drop_extending_07(self):
        await self.con.execute("""

            CREATE ABSTRACT TYPE Named {
                CREATE PROPERTY name -> str;
            };
            CREATE ABSTRACT TYPE Noted {
                CREATE PROPERTY note -> str;
            };
            CREATE TYPE Foo EXTENDING Named {
                CREATE PROPERTY note -> str;
            };
        """)

        await self.con.execute("""
            INSERT Foo { name := "Phil Emarg", note := "foo" };
        """)

        # swap parent from Named to Noted, and drop ownership of note
        await self.con.execute("""
            ALTER TYPE Foo {
                ALTER PROPERTY name {
                    SET OWNED;
                };
                DROP EXTENDING Named;
                EXTENDING Noted LAST;
                ALTER PROPERTY note {
                    DROP OWNED;
                };
            };
            DROP TYPE Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo { note, name };
            """,
            [{"name": "Phil Emarg", "note": "foo"}],
        )

        await self.con.execute("""
            ALTER TYPE Foo {
                DROP EXTENDING Noted;
            };
        """)

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "has no link or property 'note'"):
            await self.con.execute(r"""
                SELECT Foo.note;
            """)

    async def test_edgeql_ddl_drop_extending_08(self):
        await self.con.execute("""

            CREATE ABSTRACT TYPE Named {
                CREATE OPTIONAL SINGLE PROPERTY name -> str;
            };
            CREATE ABSTRACT TYPE Named2 {
                CREATE OPTIONAL SINGLE PROPERTY name -> str;
            };
            CREATE TYPE Foo EXTENDING Named;
        """)

        await self.con.execute("""
            INSERT Foo { name := "Phil Emarg" };
        """)

        # swap parent from Named to Named2; this should preserve the name prop
        await self.con.execute("""
            ALTER TYPE Foo {
                DROP EXTENDING Named;
                EXTENDING Named2 LAST;
            };
            DROP TYPE Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            ["Phil Emarg"],
        )

    async def test_edgeql_ddl_add_extending_01(self):
        await self.con.execute("""

            CREATE TYPE Thing;

            CREATE TYPE Foo {
                CREATE LINK item -> Object {
                    CREATE PROPERTY foo -> str;
                };
            };

            INSERT Foo { item := (INSERT Thing { @foo := "test" }) };
        """)

        await self.con.execute("""
            CREATE TYPE Base {
                CREATE OPTIONAL SINGLE LINK item -> Object {
                    CREATE OPTIONAL SINGLE PROPERTY foo -> str;
                };
            };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                EXTENDING Base LAST;
                ALTER LINK item {
                    ALTER PROPERTY foo {
                        DROP OWNED;
                    };
                    DROP OWNED;
                };
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo { item: {@foo} };
            """,
            [{"item": {"@foo": "test"}}],
        )

    async def test_edgeql_ddl_default_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'default expression is of invalid type: std::int64, '
                'expected std::str'):
            await self.con.execute(r"""
                CREATE TYPE TestDefault01 {
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
                CREATE TYPE TestDefault02 {
                    CREATE PROPERTY def02 -> str {
                        SET default := '42';
                    };
                };

                ALTER TYPE TestDefault02 {
                    ALTER PROPERTY def02 SET default := 42;
                };
            """)

    async def test_edgeql_ddl_default_03(self):
        # Test INSERT as default link expression
        await self.con.execute(r"""
            CREATE TYPE TestDefaultInsert03;

            CREATE TYPE TestDefault03 {
                CREATE LINK def03 -> TestDefaultInsert03 {
                    SET default := (INSERT TestDefaultInsert03);
                };
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    count(TestDefault03),
                    count(TestDefaultInsert03)
                );
            """,
            [[0, 0]],
        )

        await self.assert_query_result(
            r"""
                SELECT TestDefault03 {
                    def03
                };
            """,
            [],
        )

        # `assert_query_result` is used instead of `execute` to
        # highlight the issue #1721
        await self.assert_query_result(
            r"""INSERT TestDefault03;""",
            [{'id': uuid.UUID}]
        )

        await self.assert_query_result(
            r"""
                SELECT (
                    count(TestDefault03),
                    count(TestDefaultInsert03)
                );
            """,
            [[1, 1]],
        )

        await self.assert_query_result(
            r"""
                SELECT TestDefault03 {
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
            CREATE TYPE TestDefaultUpdate04 {
                CREATE PROPERTY val -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };

            CREATE TYPE TestDefault04 {
                CREATE LINK def04 -> TestDefaultUpdate04 {
                    SET default := (
                        UPDATE TestDefaultUpdate04
                        FILTER .val = 'def04'
                        SET {
                            val := .val ++ '!'
                        }
                    );
                };
            };

            INSERT TestDefaultUpdate04 {
                val := 'notdef04'
            };
            INSERT TestDefaultUpdate04 {
                val := 'def04'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT TestDefaultUpdate04.val;
            """,
            {'def04', 'notdef04'},
        )

        await self.assert_query_result(r"""
            SELECT {
                (INSERT TestDefault04),
                (INSERT TestDefault04)
            };
        """, [{'id': uuid.UUID}, {'id': uuid.UUID}])

        await self.assert_query_result(
            r"""
                SELECT TestDefaultUpdate04.val;
            """,
            {'def04!', 'notdef04'},
        )

        await self.assert_query_result(
            r"""
                SELECT TestDefault04 {
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
            CREATE TYPE TestDefaultDelete05 {
                CREATE PROPERTY val -> str;
            };

            CREATE TYPE TestDefault05 {
                CREATE PROPERTY def05 -> str {
                    SET default := (SELECT (
                        DELETE TestDefaultDelete05
                        FILTER .val = 'def05'
                        LIMIT 1
                    ).val);
                };
            };

            INSERT TestDefaultDelete05 {
                val := 'notdef05'
            };
            INSERT TestDefaultDelete05 {
                val := 'def05'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT TestDefaultDelete05.val;
            """,
            {'def05', 'notdef05'},
        )

        await self.con.execute(r"""
            INSERT TestDefault05;
            INSERT TestDefault05;
        """)

        await self.assert_query_result(
            r"""
                SELECT TestDefaultDelete05.val;
            """,
            {'notdef05'},
        )

        await self.assert_query_result(
            r"""
                SELECT TestDefault05 {
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
            CREATE TYPE TestDefaultDelete06 {
                CREATE PROPERTY val -> str;
            };

            CREATE TYPE TestDefault06 {
                CREATE REQUIRED LINK def06 -> TestDefaultDelete06 {
                    SET default := (
                        DELETE TestDefaultDelete06
                        FILTER .val = 'def06'
                        LIMIT 1
                    );
                };
            };

            INSERT TestDefaultDelete06 {
                val := 'notdef06'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT TestDefaultDelete06.val;
            """,
            {'notdef06'},
        )

        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r"missing value for required link 'def06'"):
            await self.con.execute(r"""
                INSERT TestDefault06;
            """)

    async def test_edgeql_ddl_default_07(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;

            alter type Foo {
                create required property name -> str {
                    set default := 'something'
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            {'something'},
        )

    async def test_edgeql_ddl_default_08(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;

            alter type Foo {
                create required multi property name -> str {
                    set default := 'something'
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            {'something'},
        )

    async def test_edgeql_ddl_default_09(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
        """)

        with self.assertRaisesRegex(
            edgedb.UnsupportedFeatureError,
            "default value for property 'x' of link 'asdf' of object type "
            "'default::Bar' is too complicated; "
            "link property defaults must not depend on database contents"
        ):
            await self.con.execute('''
                create type Bar {
                    create link asdf -> Foo {
                        create property x -> int64 {
                            set default := count(Object)
                        }
                    }
                };
            ''')

    async def test_edgeql_ddl_default_10(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'default expression is of invalid type: array<std::int32>, '
                'expected array<std::int16>'):
            await self.con.execute(r"""
                CREATE TYPE X {
                    CREATE PROPERTY y -> array<int16> {
                        SET default := <array<int32>>[]
                    };
                };
            """)

    async def test_edgeql_ddl_default_11(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'default expression is of invalid type: array<std::int32>, '
                'expected array<std::int16>'):
            await self.con.execute(r"""
                CREATE GLOBAL y -> array<int16> {
                    SET default := <array<int32>>[]
                };
            """)

    async def test_edgeql_ddl_default_circular(self):
        await self.con.execute(r"""
            CREATE TYPE TestDefaultCircular {
                CREATE PROPERTY def01 -> int64 {
                    SET default := (SELECT count(TestDefaultCircular));
                };
            };
        """)

    async def test_edgeql_ddl_default_id(self):
        # Overriding id's default with another uuid generate function is legit
        await self.con.execute(r"""
            create type A {
                alter property id {
                    set default := std::uuid_generate_v4()
                }
            };
        """)

        await self.con.execute(r"""
            create type B {
                alter property id {
                    set default := (select std::uuid_generate_v4())
                }
            };
        """)

        # But overriding it with other things is not
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            "invalid default value for 'id' property",
        ):
            await self.con.execute(r"""
                create type C {
                    alter property id {
                        set default :=
                          <uuid>"00000000-0000-0000-0000-000000000000"
                    }
                };
            """)

    async def test_edgeql_ddl_property_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY bar -> float32;
            };
        """)

        await self.con.execute(r"""
            CREATE TYPE TestDefaultCircular {
                CREATE PROPERTY def01 -> int64 {
                    SET default := (SELECT count(TestDefaultCircular));
                };
            };
        """)

    async def test_edgeql_ddl_link_target_bad_01(self):
        await self.con.execute('''

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
            "inherited link 'foo' of object type 'default::Derived' has a "
            "type conflict"
        ):
            await self.con.execute('''
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_link_target_bad_02(self):
        await self.con.execute('''

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
            "inherited link 'foo' of object type 'default::Derived' "
            "has a type conflict"
        ):
            await self.con.execute('''
                CREATE TYPE Derived EXTENDING Base0, Base1;
            ''')

    async def test_edgeql_ddl_link_target_bad_03(self):
        await self.con.execute('''
            CREATE TYPE A;
            CREATE TYPE Foo {
                CREATE LINK a -> A;
                CREATE PROPERTY b -> str;
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                "cannot RESET TYPE of link 'a' of object type 'default::Foo' "
                "because it is not inherited"):
            await self.con.execute('''
                ALTER TYPE Foo ALTER LINK a RESET TYPE;
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                "cannot RESET TYPE of property 'b' of object type "
                "'default::Foo' because it is not inherited"):
            await self.con.execute('''
                ALTER TYPE Foo ALTER PROPERTY b RESET TYPE;
            ''')

    async def test_edgeql_ddl_link_target_bad_04(self):
        await self.con.execute('''
            CREATE TYPE Foo;
            CREATE TYPE Bar;
        ''')

        with self.assertRaisesRegex(
            edgedb.UnsupportedFeatureError,
            "unsupported type intersection in schema"
        ):
            await self.con.execute('''
                CREATE TYPE Spam {
                    CREATE MULTI LINK foobar := Foo[IS Bar]
                };
            ''')

    async def test_edgeql_ddl_link_target_merge_01(self):
        await self.con.execute('''

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
            CREATE TYPE GrandParent01 {
                CREATE PROPERTY foo -> int64;
            };

            CREATE TYPE Parent01 EXTENDING GrandParent01;
            CREATE TYPE Parent02 EXTENDING GrandParent01;

            CREATE TYPE Child EXTENDING Parent01, Parent02;

            ALTER TYPE GrandParent01 {
                ALTER PROPERTY foo SET TYPE int16;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name IN
                          {'default::Child', 'default::Parent01'})
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
            edgedb.SchemaError,
            "inherited property 'foo' of object type 'default::Child'"
            " has a type conflict",
        ):
            await self.con.execute("""
                CREATE TYPE Parent01 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE Parent02 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE Child
                    EXTENDING Parent01, Parent02;

                ALTER TYPE Parent02 {
                    ALTER PROPERTY foo SET TYPE int16;
                };
            """)

    async def test_edgeql_ddl_link_target_alter_03(self):
        await self.con.execute("""
            CREATE TYPE Foo {
                CREATE PROPERTY bar -> int64;
            };

            CREATE TYPE Bar {
                CREATE MULTI PROPERTY foo -> int64 {
                    SET default := (SELECT Foo.bar);
                }
            };

            ALTER TYPE Foo ALTER PROPERTY bar SET TYPE int32;
        """)

    async def test_edgeql_ddl_link_target_alter_04(self):
        await self.con.execute('''

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

            CREATE TYPE A;
            CREATE TYPE B EXTENDING A;

            CREATE TYPE Base0 {
                CREATE LINK foo -> B;
            };

            CREATE TYPE Base1;

            CREATE TYPE Derived EXTENDING Base0, Base1;

            ALTER TYPE Base1 CREATE LINK foo -> A;
        ''')

    async def test_edgeql_ddl_link_target_alter_06(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> int64;
                CREATE PROPERTY bar := .foo + .foo;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo SET TYPE int16;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C := (SELECT schema::ObjectType
                          FILTER .name = 'default::Foo')
                SELECT
                    C.pointers { target: { name } }
                FILTER
                    C.pointers.name = 'bar'
            """,
            [
                {
                    'target': {
                        'name': 'std::int16'
                    }
                },
            ],
        )

    async def test_edgeql_ddl_prop_target_alter_array_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> array<int32>;
            };

            ALTER TYPE Foo {
                ALTER PROPERTY foo SET TYPE array<float64>;
            };

            ALTER TYPE Foo {
                ALTER PROPERTY foo {
                    SET TYPE array<int32> USING (<array<int32>>.foo);
                };
            };
        """)

    async def test_edgeql_ddl_prop_target_subtype_01(self):
        await self.con.execute(r"""
            CREATE SCALAR TYPE mystr EXTENDING std::str {
                CREATE CONSTRAINT std::max_len_value(5)
            };

            CREATE TYPE Foo {
                CREATE PROPERTY a -> std::str;
            };

            CREATE TYPE Bar EXTENDING Foo {
                ALTER PROPERTY a SET TYPE mystr;
            };
        """)

        await self.con.execute('INSERT Foo { a := "123456" }')

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'must be no longer than 5 characters'
        ):
            await self.con.execute('INSERT Bar { a := "123456" }')

        await self.con.execute("""
            ALTER TYPE Bar ALTER PROPERTY a RESET TYPE;
        """)

        await self.con.execute('INSERT Bar { a := "123456" }')

    async def test_edgeql_ddl_ptr_set_type_using_01(self):
        await self.con.execute(r"""

            CREATE SCALAR TYPE mystr EXTENDING str;

            CREATE TYPE Bar {
                CREATE PROPERTY name -> str;
            };

            CREATE TYPE SubBar EXTENDING Bar;

            CREATE TYPE Foo {
                CREATE PROPERTY p -> str {
                    CREATE CONSTRAINT exclusive;
                };
                CREATE CONSTRAINT exclusive ON (.p);
                CREATE REQUIRED PROPERTY r_p -> str;
                CREATE MULTI PROPERTY m_p -> str;
                CREATE REQUIRED MULTI PROPERTY rm_p -> str;

                CREATE LINK l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
                CREATE REQUIRED LINK r_l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
                CREATE MULTI LINK m_l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
                CREATE REQUIRED MULTI LINK rm_l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
            };

            INSERT Bar {name := 'bar1'};
            INSERT SubBar {name := 'bar2'};

            WITH
                bar := (SELECT Bar FILTER .name = 'bar1' LIMIT 1),
                bars := (SELECT Bar),
            INSERT Foo {
                p := '1',
                r_p := '10',
                m_p := {'1', '2'},
                rm_p := {'10', '20'},

                l := bar { @lp := '1' },
                r_l := bar { @lp := '10' },
                m_l := (
                    FOR bar IN {enumerate(bars)}
                    UNION (SELECT bar.1 { @lp := <str>(bar.0 + 1) })
                ),
                rm_l := (
                    FOR bar IN {enumerate(bars)}
                    UNION (SELECT bar.1 { @lp := <str>((bar.0 + 1) * 10) })
                )
            };

            WITH
                bar := (SELECT Bar FILTER .name = 'bar2' LIMIT 1),
                bars := (SELECT Bar),
            INSERT Foo {
                p := '3',
                r_p := '30',
                m_p := {'3', '4'},
                rm_p := {'30', '40'},

                l := bar { @lp := '3' },
                r_l := bar { @lp := '30' },
                m_l := (
                    FOR bar IN {enumerate(bars)}
                    UNION (SELECT bar.1 { @lp := <str>(bar.0 + 3) })
                ),
                rm_l := (
                    FOR bar IN {enumerate(bars)}
                    UNION (SELECT bar.1 { @lp := <str>((bar.0 + 3) * 10) })
                )
            };
        """)

        # A normal cast of a property.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET TYPE int64 USING (<int64>.p)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': 1},
                    {'p': 3},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET TYPE int64 USING (<int64>.m_p)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_p } ORDER BY .p',
                [
                    {'m_p': {1, 2}},
                    {'m_p': {3, 4}},
                ],
            )

        # Cast to an already-compatible type, but with an explicit expression.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET TYPE mystr USING (.p ++ '!')
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': '1!'},
                    {'p': '3!'},
                ],
            )

        # Cast to the _same_ type, but with an explicit expression.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET TYPE str USING (.p ++ '!')
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': '1!'},
                    {'p': '3!'},
                ],
            )

        # A reference to another property of the same host type.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET TYPE int64 USING (<int64>.r_p)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': 10},
                    {'p': 30},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET TYPE int64 USING (<int64>.m_p + <int64>.r_p)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_p } ORDER BY .p',
                [
                    {'m_p': {11, 12}},
                    {'m_p': {33, 34}},
                ],
            )

        async with self._run_and_rollback():
            # This had trouble if there was a SELECT, at one point.
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET TYPE int64 USING (SELECT <int64>.m_p + <int64>.r_p)
                }
            """)

        # Conversion expression that reduces cardinality...
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET TYPE int64 USING (<int64>{})
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': None},
                    {'p': None},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET TYPE int64 USING (
                        <int64>{} IF <int64>.m_p % 2 = 0 ELSE <int64>.m_p
                    )
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_p } ORDER BY .p',
                [
                    {'m_p': {1}},
                    {'m_p': {3}},
                ],
            )

        # ... should fail if empty set is produced and the property is required
        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'r_p'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY r_p {
                    SET TYPE int64 USING (<int64>{})
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'rm_p'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY rm_p {
                    SET TYPE int64 USING (
                        <int64>{} IF True ELSE <int64>.rm_p
                    )
                }
            """)

        # Straightforward link cast.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l {
                    SET TYPE SubBar USING (.l[IS SubBar])
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { l: {name} } ORDER BY .p',
                [
                    {'l': None},
                    {'l': {'name': 'bar2'}},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK m_l {
                    SET TYPE SubBar USING (.m_l[IS SubBar])
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_l: {name} } ORDER BY .p',
                [
                    {'m_l': [{'name': 'bar2'}]},
                    {'m_l': [{'name': 'bar2'}]},
                ],
            )

        # Use a more elaborate expression for the tranform.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l {
                    SET TYPE SubBar USING (SELECT .m_l[IS SubBar] LIMIT 1)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { l: {name, @lp} } ORDER BY .p',
                [
                    {'l': {'name': 'bar2', '@lp': '1'}},
                    {'l': {'name': 'bar2', '@lp': '3'}},
                ],
            )

        # Check that minimum cardinality constraint is enforced on links too...
        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'r_l'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK r_l {
                    SET TYPE SubBar USING (.r_l[IS SubBar])
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'rm_l'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK rm_l {
                    SET TYPE SubBar USING (SELECT SubBar FILTER False LIMIT 1)
                }
            """)

        # Test link property transforms now.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l ALTER PROPERTY lp {
                    SET TYPE int64 USING (<int64>@lp)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { l: { @lp } } ORDER BY .p',
                [
                    {'l': {'@lp': 1}},
                    {'l': {'@lp': 3}},
                ],
            )

        async with self._run_and_rollback():
            # Also once had SELECT trouble
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l ALTER PROPERTY lp {
                    SET TYPE int64 USING (SELECT <int64>@lp)
                }
            """)

    async def test_edgeql_ddl_ptr_set_type_using_02(self):
        await self.con.execute(r"""

            CREATE ABSTRACT TYPE Parent {
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE Child EXTENDING Parent;
            INSERT Child { name := "10" };
        """)

        await self.con.execute(r"""
            ALTER TYPE Parent {
                ALTER PROPERTY name {
                    SET TYPE int64 USING (<int64>.name)
                }
            }
        """)

        await self.assert_query_result(
            'SELECT Child { name }',
            [
                {'name': 10},
            ]
        )

    async def test_edgeql_ddl_ptr_set_type_using_03(self):
        # check that defaults don't break things
        await self.con.execute(r"""
            create type Foo {
                create property x -> str {
                    set default := '';
                };
                create multi property y -> str {
                    set default := '';
                };
            };
        """)

        await self.con.execute(r"""
            alter type Foo {
                alter property x {
                    set default := 0;
                    set type int64 using (<int64>.x);
                };
                alter property y {
                    set default := 0;
                    set type int64 using (<int64>.y);
                };
            };
        """)

        await self.con.execute(r"""
            create type Bar {
                create link l -> Foo {
                    create property x -> str {
                        set default := '';
                    }
                }
            };
        """)

        await self.con.execute(r"""
            alter type Bar {
                alter link l {
                    alter property x {
                        set default := 0;
                        set type int64 using (<int64>@x);
                    }
                }
            };
        """)

    async def test_edgeql_ddl_ptr_set_type_using_04(self):
        # check that defaults don't break things
        await self.con.execute(r"""
            create scalar type X extending sequence;
            create type Foo {
                create property x -> X;
            };
        """)

        await self.con.execute(r"""
            alter type Foo {
                alter property x {
                    set type array<str> using ([<str>.x]);
                }
            };
        """)

        await self.con.execute(r"""
            create type Bar {
                create property x -> int64;
            };
        """)
        await self.con.execute(r"""
            alter type Bar {
                alter property x {
                    set type X using (<X>.x);
                }
            };
        """)

        await self.con.execute(r"""
            create type Baz {
                create multi property x -> int64;
            };
        """)
        await self.con.execute(r"""
            alter type Baz {
                alter property x {
                    set type X using (<X>.x);
                }
            };
        """)

    async def test_edgeql_ddl_ptr_set_type_validation(self):
        await self.con.execute(r"""

            CREATE TYPE Bar;
            CREATE TYPE Spam;
            CREATE TYPE Egg;
            CREATE TYPE Foo {
                CREATE PROPERTY p -> str;
                CREATE LINK l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"property 'p' of object type 'default::Foo' cannot be cast"
            r" automatically from scalar type 'std::str' to scalar"
            r" type 'std::int64'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p SET TYPE int64;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"result of USING clause for the alteration of"
            r" property 'p' of object type 'default::Foo' cannot be cast"
            r" automatically from scalar type 'std::float64' to scalar"
            r" type 'std::int64'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p
                    SET TYPE int64 USING (<float64>.p)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"possibly more than one element returned by the USING clause for"
            r" the alteration of property 'p' of object type 'default::Foo',"
            r" while a singleton is expected"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p SET TYPE int64 USING ({1, 2})
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"link 'l' of object type 'default::Foo' cannot be cast"
            r" automatically from object type 'default::Bar' to object"
            r" type 'default::Spam'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l SET TYPE Spam;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"result of USING clause for the alteration of"
            r" link 'l' of object type 'default::Foo' cannot be cast"
            r" automatically from object type 'default::Bar & default::Egg'"
            r" to object type 'default::Spam'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l SET TYPE Spam USING (.l[IS Egg])
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"possibly more than one element returned by the USING clause for"
            r" the alteration of link 'l' of object type 'default::Foo', while"
            r" a singleton is expected"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l SET TYPE Spam USING (SELECT Spam)
            """)

    async def test_edgeql_ddl_ptr_set_cardinality_01(self):
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE MULTI PROPERTY bar -> str;
            };
            INSERT Foo { bar := "foo" };
            ALTER TYPE Foo { ALTER PROPERTY bar {
                SET SINGLE USING (assert_single(.bar))
            } };
        ''')

        await self.assert_query_result(
            r"""
                SELECT Foo { bar }
            """,
            [
                {'bar': "foo"}
            ]
        )

        # Make sure the delete triggers get cleaned up
        await self.assert_query_result(
            r"""
                DELETE Foo
            """,
            [
                {}
            ]
        )

    async def test_edgeql_ddl_ptr_set_cardinality_validation(self):
        await self.con.execute(r"""
            CREATE TYPE Bar;
            CREATE TYPE Egg;
            CREATE TYPE Foo {
                CREATE MULTI PROPERTY p -> str;
                CREATE MULTI LINK l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"cannot automatically convert property 'p' of object type"
            r" 'default::Foo' to 'single' cardinality"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p SET SINGLE;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"result of USING clause for the alteration of"
            r" property 'p' of object type 'default::Foo' cannot be cast"
            r" automatically from scalar type 'std::float64' to scalar"
            r" type 'std::int64'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p
                    SET TYPE int64 USING (<float64>.p)
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"possibly more than one element returned by the USING clause for"
            r" the alteration of property 'p' of object type 'default::Foo',"
            r" while a singleton is expected"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p SET SINGLE USING ({1, 2})
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"cannot automatically convert link 'l' of object type"
            r" 'default::Foo' to 'single' cardinality"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l SET SINGLE;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"result of USING clause for the alteration of"
            r" link 'l' of object type 'default::Foo' cannot be cast"
            r" automatically from object type 'default::Egg'"
            r" to object type 'default::Bar'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l
                    SET SINGLE USING (SELECT Egg LIMIT 1);
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"possibly more than one element returned by the USING clause for"
            r" the alteration of link 'l' of object type 'default::Foo', while"
            r" a singleton is expected"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l SET SINGLE USING (SELECT Bar)
            """)

    async def test_edgeql_ddl_ptr_set_required_01(self):
        await self.con.execute(r"""

            CREATE TYPE Bar {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT std::exclusive;
                }
            };

            CREATE TYPE Foo {
                CREATE PROPERTY p -> str;
                CREATE PROPERTY p2 -> str;
                CREATE MULTI PROPERTY m_p -> str;

                CREATE LINK l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
                CREATE MULTI LINK m_l -> Bar {
                    CREATE PROPERTY lp -> str;
                };
            };

            INSERT Bar {name := 'bar1'};
            INSERT Bar {name := 'bar2'};

            WITH
                bar := (SELECT Bar FILTER .name = 'bar1' LIMIT 1),
                bars := (SELECT Bar),
            INSERT Foo {
                p := '1',
                p2 := '1',
                m_p := {'1', '2'},

                l := bar { @lp := '1' },
                m_l := (
                    FOR bar IN {enumerate(bars)}
                    UNION (SELECT bar.1 { @lp := <str>(bar.0 + 1) })
                ),
            };

            INSERT Foo {
                p2 := '3',
            };
        """)

        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET REQUIRED USING ('3')
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': '1'},
                    {'p': '3'},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET REQUIRED USING ('3')
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_p } ORDER BY .p',
                [
                    {'m_p': {'1', '2'}},
                    {'m_p': {'3'}},
                ],
            )

        # A reference to another property of the same host type.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET REQUIRED USING (.p2)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { p } ORDER BY .p',
                [
                    {'p': '1'},
                    {'p': '3'},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET REQUIRED USING (.p2)
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { m_p } ORDER BY .p',
                [
                    {'m_p': {'1', '2'}},
                    {'m_p': {'3'}},
                ],
            )

        # ... should fail if empty set is produced by the USING clause
        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'p'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY p {
                    SET REQUIRED USING (<str>{})
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'm_p'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY m_p {
                    SET REQUIRED USING (
                        <str>{} IF True ELSE .p2
                    )
                }
            """)

        # And now see about the links.
        async with self._run_and_rollback():
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l {
                    SET REQUIRED USING (SELECT Bar FILTER .name = 'bar2')
                }
            """)

            await self.assert_query_result(
                'SELECT Foo { l: {name} } ORDER BY .p EMPTY LAST',
                [
                    {'l': {'name': 'bar1'}},
                    {'l': {'name': 'bar2'}},
                ],
            )

            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK m_l {
                    SET REQUIRED USING (SELECT Bar FILTER .name = 'bar2')
                }
            """)

            await self.assert_query_result(
                '''
                SELECT Foo { m_l: {name} ORDER BY .name }
                ORDER BY .p EMPTY LAST
                ''',
                [
                    {'m_l': [{'name': 'bar1'}, {'name': 'bar2'}]},
                    {'m_l': [{'name': 'bar2'}]},
                ],
            )

        # Check that minimum cardinality constraint is enforced on links too...
        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'l'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK l {
                    SET REQUIRED USING (SELECT Bar FILTER false LIMIT 1)
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'm_l'"
            r" of object type 'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK m_l {
                    SET REQUIRED USING (SELECT Bar FILTER false LIMIT 1)
                }
            """)

    async def test_edgeql_ddl_link_property_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyDefinitionError,
                r"link properties cannot be required"):
            await self.con.execute("""
                CREATE TYPE TestLinkPropType_01 {
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
                CREATE TYPE TestLinkPropType_02 {
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
                CREATE TYPE TestLinkPropType_03 {
                    CREATE LINK test_linkprop_link_03 -> std::Object;
                };

                ALTER TYPE TestLinkPropType_03 {
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
                CREATE TYPE TestLinkPropType_04 {
                    CREATE LINK test_linkprop_link_04 -> std::Object;
                };

                ALTER TYPE TestLinkPropType_04 {
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
                CREATE TYPE TestLinkPropType_05 {
                    CREATE LINK test_linkprop_link_05 -> std::Object {
                        CREATE PROPERTY test_link_prop_05 -> std::int64;
                    };
                };

                ALTER TYPE TestLinkPropType_05 {
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
                CREATE TYPE TestLinkPropType_06 {
                    CREATE LINK test_linkprop_link_06 -> std::Object {
                        CREATE MULTI PROPERTY test_link_prop_06 -> std::int64;
                    };
                };

                ALTER TYPE TestLinkPropType_06 {
                    ALTER LINK test_linkprop_link_06 {
                        ALTER PROPERTY test_link_prop_06 {
                            SET MULTI;
                        };
                    };
                };
            """)

    async def test_edgeql_ddl_link_property_07(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK link_with_value {
                CREATE SINGLE PROPERTY value -> int64;
                CREATE INDEX on (__subject__@value);
                CREATE INDEX on ((__subject__@source, __subject__@value));
                CREATE INDEX on ((__subject__@target, __subject__@value));
                # FIXME: this is broken
                # CREATE INDEX on ((__subject__, __subject__@value));
            };

            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK l1 EXTENDING link_with_value -> Tgt;
            };
        """)

    async def test_edgeql_ddl_link_property_08(self):
        await self.con.execute("""
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK l2 -> Tgt {
                    CREATE SINGLE PROPERTY value -> int64;
                    CREATE INDEX on (__subject__@value);
                    CREATE INDEX on ((__subject__@target, __subject__@value));
                };

            };

            ALTER TYPE Foo {
                ALTER LINK l2 {
                    ALTER INDEX on ((__subject__@target, __subject__@value)) {
                        CREATE ANNOTATION description := "woo";
                    };
                    CREATE INDEX on ((__subject__@source, __subject__@value));
                    DROP INDEX on (__subject__@value);
                }
            };
        """)

    async def test_edgeql_ddl_link_property_09(self):
        await self.con.execute("""
            create type T;
            create type S {
                create multi link x -> T {
                    create property id -> str;
                    create index on (__subject__@id);
                }
            };
            insert T;
            insert S { x := (select T { @id := "lol" }) };
        """)

        await self.assert_query_result(
            r"""
                select S { x: {id, @id} }
            """,
            [{'x': [{'id': str, '@id': "lol"}]}],
            # The python bindings seem to misbehave when there is
            # linkprop and a regular prop with the same name
            json_only=True,
        )

    async def test_edgeql_ddl_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'default::array' does not exist"):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar -> array;
                };
            """)

    async def test_edgeql_ddl_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'default::tuple' does not exist"):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar -> tuple;
                };
            """)

    async def test_edgeql_ddl_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'unexpected number of subtypes, expecting 1'):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar -> array<int64, int64, int64>;
                };
            """)

    async def test_edgeql_ddl_bad_04(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'nested arrays are not supported'):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar -> array<array<int64>>;
                };
            """)

    async def test_edgeql_ddl_bad_05(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r'mixing named and unnamed subtype declarations is not '
                r'supported'):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar -> tuple<int64, foo:int64>;
                };
            """)

    async def test_edgeql_ddl_bad_07(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in computed link 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;

                    CREATE TYPE Bar {
                        CREATE LINK foo := (INSERT Foo);
                    };
                """)

    async def test_edgeql_ddl_bad_08(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in computed link 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;

                    CREATE TYPE Bar {
                        CREATE LINK foo := (
                            WITH x := (INSERT Foo)
                            SELECT x
                        );
                    };
                """)

    async def test_edgeql_ddl_bad_09(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in computed property 'foo'"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;

                    CREATE TYPE Bar {
                        CREATE PROPERTY foo := (INSERT Foo).id;
                    };
                """)

    async def test_edgeql_ddl_bad_10(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;
                    CREATE TYPE Bar;

                    CREATE ALIAS Baz := Bar {
                        foo := (INSERT Foo)
                    };
                """)

    async def test_edgeql_ddl_bad_11(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;
                    CREATE TYPE Bar;

                    CREATE ALIAS Baz := Bar {
                        foo := (INSERT Foo).id
                    };
                """)

    async def test_edgeql_ddl_bad_12(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;
                    CREATE TYPE Bar {
                        CREATE LINK foo -> Foo;
                    };

                    CREATE ALIAS Baz := Bar {
                        foo: {
                            fuz := (INSERT Foo)
                        }
                    };
                """)

    async def test_edgeql_ddl_bad_13(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r"mutations are invalid in alias definition"):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE TYPE Foo;
                    CREATE TYPE Bar {
                        CREATE LINK foo -> Foo;
                    };

                    CREATE ALIAS Baz := (
                        WITH x := (INSERT Foo)
                        SELECT Bar {
                            foo: {
                                fuz := x
                            }
                        }
                    );
                """)

    async def test_edgeql_ddl_link_long_01(self):
        link_name = (
            'f123456789_123456789_123456789_123456789'
            '_123456789_123456789_123456789_123456789'
        )
        await self.con.execute(f"""
            CREATE ABSTRACT LINK {link_name};
        """)

        await self.con.execute(f"""
            CREATE TYPE Foo {{
                CREATE LINK {link_name} -> Foo;
            }};
        """)

        await self.con.query(f"SELECT Foo.{link_name}")

    async def test_edgeql_ddl_link_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                f'unexpected fully-qualified name'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE Foo {
                        CREATE LINK foo::bar -> Foo;
                    };
                """)

    async def test_edgeql_ddl_link_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstract link"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT LINK bar {
                        SET default := Object;
                    };
                """)

    async def test_edgeql_ddl_link_bad_04(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstract link"):
            async with self.con.transaction():
                await self.migrate("""
                    abstract link bar {
                        default := Object;
                    };
                """)

    async def test_edgeql_ddl_property_long_01(self):
        prop_name = (
            'f123456789_123456789_123456789_123456789'
            '_123456789_123456789_123456789_123456789'
        )
        await self.con.execute(f"""
            CREATE ABSTRACT PROPERTY {prop_name}
        """)

        await self.con.execute(f"""
            CREATE TYPE Foo {{
                CREATE PROPERTY {prop_name} -> std::str;
            }};
        """)

        await self.con.query(f"SELECT Foo.{prop_name}")

    async def test_edgeql_ddl_property_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                f'unexpected fully-qualified name'):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE TYPE Foo {
                        CREATE PROPERTY foo::bar -> Foo;
                    };
                """)

    async def test_edgeql_ddl_property_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstract property"):
            async with self.con.transaction():
                await self.con.execute("""
                    CREATE ABSTRACT PROPERTY bar {
                        SET default := 'bad';
                    };
                """)

    async def test_edgeql_ddl_property_bad_04(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f"'default' is not a valid field for an abstract property"):
            async with self.con.transaction():
                await self.migrate("""
                    abstract property currency_fallback {
                        default := 'EUR';
                    };
                """)

    async def test_edgeql_ddl_function_01(self):
        await self.con.execute("""
            CREATE FUNCTION my_lower(s: std::str) -> std::str
                USING SQL FUNCTION 'lower';
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION my_lower(s: SET OF std::str)
                        -> std::str {
                        SET initial_value := '';
                        USING SQL FUNCTION 'count';
                    };
                """)

        await self.con.execute("""
            DROP FUNCTION my_lower(s: std::str);
        """)

        await self.con.execute("""
            CREATE FUNCTION my_lower(s: SET OF anytype)
                -> std::str {
                USING SQL FUNCTION 'count';
                SET initial_value := '';
            };
        """)

        with self.assertRaisesRegex(edgedb.DuplicateFunctionDefinitionError,
                                    r'cannot create.*my_lower.*func'):

            async with self.con.transaction():
                await self.con.execute("""
                    CREATE FUNCTION my_lower(s: anytype) -> std::str
                        USING SQL FUNCTION 'lower';
                """)

        await self.con.execute("""
            DROP FUNCTION my_lower(s: anytype);
        """)

    async def test_edgeql_ddl_function_02(self):
        long_func_name = 'my_sql_func5_' + 'abc' * 50

        await self.con.execute(f"""
            CREATE FUNCTION my_sql_func1()
                -> std::str
                USING SQL $$
                    SELECT 'spam'::text
                $$;

            CREATE FUNCTION my_sql_func2(foo: std::str)
                -> std::str
                USING SQL $$
                    SELECT "foo"::text
                $$;

            CREATE FUNCTION my_sql_func4(VARIADIC s: std::str)
                -> std::str
                USING SQL $$
                    SELECT array_to_string(s, '-')
                $$;

            CREATE FUNCTION {long_func_name}()
                -> std::str
                USING SQL $$
                    SELECT '{long_func_name}'::text
                $$;

            CREATE FUNCTION my_sql_func6(a: std::str='a' ++ 'b')
                -> std::str
                USING SQL $$
                    SELECT $1 || 'c'
                $$;

            CREATE FUNCTION my_sql_func7(s: array<std::int64>)
                -> std::int64
                USING SQL $$
                    SELECT sum(s)::bigint FROM UNNEST($1) AS s
                $$;
        """)

        await self.assert_query_result(
            r"""
                SELECT my_sql_func1();
            """,
            ['spam'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_sql_func2('foo');
            """,
            ['foo'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_sql_func4('fizz', 'buzz');
            """,
            ['fizz-buzz'],
        )
        await self.assert_query_result(
            fr"""
                SELECT {long_func_name}();
            """,
            [long_func_name],
        )
        await self.assert_query_result(
            r"""
                SELECT my_sql_func6();
            """,
            ['abc'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_sql_func6('xy');
            """,
            ['xyc'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_sql_func7([1, 2, 3, 10]);
            """,
            [16],
        )

        await self.con.execute(f"""
            DROP FUNCTION my_sql_func1();
            DROP FUNCTION my_sql_func2(foo: std::str);
            DROP FUNCTION my_sql_func4(VARIADIC s: std::str);
            DROP FUNCTION {long_func_name}();
            DROP FUNCTION my_sql_func6(a: std::str='a' ++ 'b');
            DROP FUNCTION my_sql_func7(s: array<std::int64>);
        """)

    async def test_edgeql_ddl_function_03(self):
        with self.assertRaisesRegex(edgedb.InvalidFunctionDefinitionError,
                                    r'invalid default value'):
            await self.con.execute(f"""
                CREATE FUNCTION broken_sql_func1(
                    a: std::int64=(SELECT schema::ObjectType))
                -> std::str
                USING SQL $$
                    SELECT 'spam'::text
                $$;
            """)

    async def test_edgeql_ddl_function_04(self):
        await self.con.execute(f"""
            CREATE FUNCTION my_edgeql_func1()
                -> std::str
                USING EdgeQL $$
                    SELECT 'sp' ++ 'am'
                $$;

            CREATE FUNCTION my_edgeql_func2(s: std::str)
                -> OPTIONAL schema::ObjectType
                USING EdgeQL $$
                    SELECT
                        schema::ObjectType
                    FILTER schema::ObjectType.name = s
                    LIMIT 1
                $$;

            CREATE FUNCTION my_edgeql_func3(s: std::int64)
                -> std::int64
                USING EdgeQL $$
                    SELECT s + 10
                $$;

            CREATE FUNCTION my_edgeql_func4(i: std::int64)
                -> array<std::int64>
                USING EdgeQL $$
                    SELECT [i, 1, 2, 3]
                $$;
        """)

        await self.assert_query_result(
            r"""
                SELECT my_edgeql_func1();
            """,
            ['spam'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_edgeql_func2('schema::Object').name;
            """,
            ['schema::Object'],
        )
        await self.assert_query_result(
            r"""
                SELECT (SELECT my_edgeql_func2('schema::Object')).name;
            """,
            ['schema::Object'],
        )
        await self.assert_query_result(
            r"""
                SELECT my_edgeql_func3(1);
            """,
            [11],
        )
        await self.assert_query_result(
            r"""
                SELECT my_edgeql_func4(42);
            """,
            [[42, 1, 2, 3]]
        )

        await self.con.execute(f"""
            DROP FUNCTION my_edgeql_func1();
            DROP FUNCTION my_edgeql_func2(s: std::str);
            DROP FUNCTION my_edgeql_func3(s: std::int64);
            DROP FUNCTION my_edgeql_func4(i: std::int64);
        """)

    async def test_edgeql_ddl_function_05(self):
        await self.con.execute("""
            CREATE FUNCTION attr_func_1() -> std::str {
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
                } FILTER .name = 'default::attr_func_1';
            """,
            [{
                'annotations': [{
                    '@value': 'hello'
                }]
            }],
        )

        await self.con.execute("""
            DROP FUNCTION attr_func_1();
        """)

    async def test_edgeql_ddl_function_06(self):
        await self.con.execute("""
            CREATE FUNCTION int_func_1() -> std::int64 {
                USING EdgeQL "SELECT 1";
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT int_func_1();
            """,
            [1],
        )

    async def test_edgeql_ddl_function_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*my_agg.*function:.+anytype.+cannot '
                r'have a non-empty default'):
            await self.con.execute(r"""
                CREATE FUNCTION my_agg(
                        s: anytype = [1]) -> array<anytype>
                    USING SQL FUNCTION "my_agg";
            """)

    async def test_edgeql_ddl_function_08(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'invalid declaration.*unexpected type of the default'):

            await self.con.execute("""
                CREATE FUNCTION ddlf_08(s: std::str = 1) -> std::str
                    USING EdgeQL $$ SELECT "1" $$;
            """)

    async def test_edgeql_ddl_function_09(self):
        await self.con.execute("""
            CREATE FUNCTION ddlf_09(
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
                    CREATE FUNCTION ddlf_09(
                        NAMED ONLY b: int64,
                        NAMED ONLY a: int64 = 1
                    ) -> std::str
                        USING EdgeQL $$ SELECT "1" $$;
                """)

        await self.con.execute("""
            CREATE FUNCTION ddlf_09(
                NAMED ONLY b: str,
                NAMED ONLY a: int64
            ) -> std::str
                USING EdgeQL $$ SELECT "2" $$;
        """)

        await self.assert_query_result(
            r'''
                SELECT ddlf_09(a:=1, b:=1);
            ''',
            ['1'],
        )
        await self.assert_query_result(
            r'''
                SELECT ddlf_09(a:=1, b:='a');
            ''',
            ['2'],
        )

    async def test_edgeql_ddl_function_10(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'parameter `sum` is not callable',
                _line=6, _col=39):

            await self.con.execute('''
                CREATE FUNCTION ddlf_10(
                    sum: int64
                ) -> int64
                    USING (
                        SELECT <int64>sum(sum)
                    );
            ''')

    async def test_edgeql_ddl_function_11(self):
        await self.con.execute(r'''
            CREATE FUNCTION ddlf_11_1() -> str
                USING EdgeQL $$
                    SELECT '\u0062'
                $$;

            CREATE FUNCTION ddlf_11_2() -> str
                USING EdgeQL $$
                    SELECT r'\u0062'
                $$;

            CREATE FUNCTION ddlf_11_3() -> str
                USING EdgeQL $$
                    SELECT $a$\u0062$a$
                $$;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT ddlf_11_1();
                ''',
                ['b'],
            )
            await self.assert_query_result(
                r'''
                    SELECT ddlf_11_2();
                ''',
                [r'\u0062'],
            )
            await self.assert_query_result(
                r'''
                    SELECT ddlf_11_3();
                ''',
                [r'\u0062'],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION ddlf_11_1();
                DROP FUNCTION ddlf_11_2();
                DROP FUNCTION ddlf_11_3();
            """)

    async def test_edgeql_ddl_function_12(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateFunctionDefinitionError,
                r'cannot create.*ddlf_12\(a: std::int64\).*'
                r'function with the same signature is already defined'):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_12(a: int64) -> int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION ddlf_12(a: int64) -> float64
                    USING EdgeQL $$ SELECT 11 $$;
            ''')

    async def test_edgeql_ddl_function_13(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'cannot create.*ddlf_13\(a: SET OF std::int64\).*'
                r'SET OF parameters in user-defined EdgeQL functions are '
                r'not supported'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION ddlf_13(a: SET OF int64) -> int64
                        USING EdgeQL $$ SELECT 11 $$;
                ''')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.execute("""
                DROP FUNCTION ddlf_13(a: SET OF int64);
            """)

    async def test_edgeql_ddl_function_14(self):
        await self.con.execute(r'''
            CREATE FUNCTION ddlf_14(
                    a: int64, NAMED ONLY f: int64) -> int64
                USING EdgeQL $$ SELECT 11 $$;

            CREATE FUNCTION ddlf_14(
                    a: int32, NAMED ONLY f: str) -> int64
                USING EdgeQL $$ SELECT 12 $$;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT ddlf_14(<int64>10, f := 11);
                ''',
                [11],
            )
            await self.assert_query_result(
                r'''
                    SELECT ddlf_14(<int32>10, f := '11');
                ''',
                [12],
            )
        finally:
            await self.con.execute("""
                DROP FUNCTION ddlf_14(a: int64, NAMED ONLY f: int64);
                DROP FUNCTION ddlf_14(a: int32, NAMED ONLY f: str);
            """)

    async def test_edgeql_ddl_function_15(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*ddlf_15.*NAMED ONLY h:.*'
                r'different named only parameters'):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_15(
                        a: int64, NAMED ONLY f: int64) -> int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION ddlf_15(
                        a: int32, NAMED ONLY h: str) -> int64
                    USING EdgeQL $$ SELECT 12 $$;
            ''')

    async def test_edgeql_ddl_function_16(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create the polymorphic.*ddlf_16.*'
                r'function with different return type'):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_16(
                        a: anytype, b: int64) -> OPTIONAL int64
                    USING EdgeQL $$ SELECT 11 $$;

                CREATE FUNCTION ddlf_16(a: anytype, b: float64) -> str
                    USING EdgeQL $$ SELECT '12' $$;
            ''')

    async def test_edgeql_ddl_function_17(self):
        await self.con.execute(r'''
            CREATE FUNCTION ddlf_17(str: std::str) -> int32
                USING SQL FUNCTION 'char_length';
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*ddlf_17.*'
                r'overloading "USING SQL FUNCTION"'):

            async with self.con.transaction():
                await self.con.execute(r'''
                    CREATE FUNCTION ddlf_17(str: std::int64) -> int32
                        USING SQL FUNCTION 'whatever2';
                ''')

        await self.con.execute("""
            DROP FUNCTION ddlf_17(str: std::str);
        """)

    async def test_edgeql_ddl_function_18(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*ddlf_18.*'
                r'function returns a generic type but has no '
                r'generic parameters'):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_18(str: std::str) -> anytype
                    USING EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_function_19(self):
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"type 'std::anytype' does not exist"):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_19(f: std::anytype) -> int64
                    USING EdgeQL $$ SELECT 1 $$;
            ''')

    async def test_edgeql_ddl_function_20(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                r"Unexpected ';'"):

            await self.con.execute(r'''
                CREATE FUNCTION ddlf_20(f: int64) -> int64
                    USING EdgeQL $$ SELECT 1; SELECT f; $$;
            ''')

    async def test_edgeql_ddl_function_22(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return type mismatch.*scalar type 'std::int64'"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION broken_edgeql_func22(
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
                CREATE FUNCTION broken_edgeql_func23(
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
                CREATE FUNCTION broken_edgeql_func24(
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
                CREATE FUNCTION broken_edgeql_func25(
                    a: std::str) -> std::str
                USING EdgeQL $$
                    SELECT {a, a}
                $$;
            """)

    async def test_edgeql_ddl_function_26(self):
        await self.con.execute(r"""
            CREATE ABSTRACT ANNOTATION foo26;

            CREATE FUNCTION edgeql_func26(a: std::str) -> std::str {
                USING EdgeQL $$
                    SELECT a ++ 'aaa'
                $$;
                # volatility must be case insensitive
                SET volatility := 'Volatile';
            };

            ALTER FUNCTION edgeql_func26(a: std::str) {
                CREATE ANNOTATION foo26 := 'aaaa';
            };

            ALTER FUNCTION edgeql_func26(a: std::str) {
                # volatility must be case insensitive
                SET volatility := 'immutable';
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT edgeql_func26('b')
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
                    .name = 'default::edgeql_func26';
            ''',
            [
                {
                    'name': 'default::edgeql_func26',
                    'annotations': [
                        {
                            'name': 'default::foo26',
                            '@value': 'aaaa',
                        },
                    ],
                    'vol': 'Immutable',
                },
            ]
        )

        await self.con.execute(r"""
            ALTER FUNCTION edgeql_func26(a: std::str) {
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
                    .name = 'default::edgeql_func26';
            ''',
            [
                {
                    'name': 'default::edgeql_func26',
                    'annotations': [],
                },
            ]
        )

        await self.con.execute(r"""
            ALTER FUNCTION edgeql_func26(a: std::str) {
                USING (
                    SELECT a ++ 'bbb'
                )
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT edgeql_func26('b')
            ''',
            [
                'bbbb'
            ],
        )

        await self.con.execute(r"""
            ALTER FUNCTION edgeql_func26(a: std::str) {
                USING EdgeQL $$
                    SELECT a ++ 'zzz'
                $$
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT edgeql_func26('b')
            ''',
            [
                'bzzz'
            ],
        )

    async def test_edgeql_ddl_function_27(self):
        # This test checks constants, but we have to do DDLs to test them
        # with constant extraction disabled
        await self.con.execute('''
            CREATE FUNCTION constant_int() -> std::int64 {
                USING (SELECT 1_024);
            };
            CREATE FUNCTION constant_bigint() -> std::bigint {
                USING (SELECT 1_024n);
            };
            CREATE FUNCTION constant_float() -> std::float64 {
                USING (SELECT 1_024.1_250);
            };
            CREATE FUNCTION constant_decimal() -> std::decimal {
                USING (SELECT 1_024.1_024n);
            };
        ''')
        try:
            await self.assert_query_result(
                r'''
                    SELECT (
                        int := constant_int(),
                        bigint := constant_bigint(),
                        float := constant_float(),
                        decimal := constant_decimal(),
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
                DROP FUNCTION constant_int();
                DROP FUNCTION constant_float();
                DROP FUNCTION constant_bigint();
                DROP FUNCTION constant_decimal();
            """)

    async def test_edgeql_ddl_function_28(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'default::foo' already exists"):

            await self.con.execute('''\
                CREATE TYPE foo;
                CREATE FUNCTION foo() -> str USING ('a');
            ''')

    async def test_edgeql_ddl_function_29(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'default::foo\(\)' already exists"):

            await self.con.execute('''\
                CREATE FUNCTION foo() -> str USING ('a');
                CREATE TYPE foo;
            ''')

    async def test_edgeql_ddl_function_30(self):
        with self.assertRaisesRegex(
            edgedb.InternalServerError,
            r'declared to return SQL type "int8", but the underlying '
            r'SQL function returns "integer"'
        ):
            await self.con.execute(r'''
                CREATE FUNCTION ddlf_30(str: std::str) -> int64
                    USING SQL FUNCTION 'char_length';
            ''')

    async def test_edgeql_ddl_function_31(self):
        await self.con.execute(r'''
            CREATE FUNCTION foo() -> str USING ('a');
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r"return type mismatch"):
            await self.con.execute(r'''
                ALTER FUNCTION foo() USING (1);
            ''')

    async def test_edgeql_ddl_function_32(self):
        await self.con.execute(r'''
            CREATE TYPE Foo;
            CREATE TYPE Bar;
            INSERT Foo;
            INSERT Bar;
        ''')

        # All these overloads are OK.
        await self.con.execute(r"""
            CREATE FUNCTION func32_ok(obj: Foo, a: int64) -> str
                USING ('Foo int64');
            CREATE FUNCTION func32_ok(obj: Bar, a: int64) -> str
                USING ('Bar int64');
            CREATE FUNCTION func32_ok(s: str, a: int64) -> str
                USING ('str int64');
            CREATE FUNCTION func32_ok(s: str, a: Foo) -> str
                USING ('str Foo');
            CREATE FUNCTION func32_ok(s: str, a: Bar) -> str
                USING ('str Bar');
            CREATE FUNCTION func32_ok(s: str, a: str, b: str) -> str
                USING ('str str str');
        """)

        await self.assert_query_result(
            r"""
                WITH
                    Foo := assert_single(Foo),
                    Bar := assert_single(Bar),
                SELECT {
                    Foo_int64 := func32_ok(Foo, 1),
                    Bar_int64 := func32_ok(Bar, 1),
                    str_int64 := func32_ok("a", 1),
                    str_Foo := func32_ok("a", Foo),
                    str_Bar := func32_ok("a", Bar),
                    str_str_str := func32_ok("a", "b", "c"),
                }
            """,
            [{
                "Foo_int64": "Foo int64",
                "Bar_int64": "Bar int64",
                "str_int64": "str int64",
                "str_Foo": "str Foo",
                "str_Bar": "str Bar",
                "str_str_str": "str str str",
            }]
        )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot create the .* function: overloading an object "
            r"type-receiving function with differences in the remaining "
            r"parameters is not supported",
        ):
            await self.con.execute(r"""
                CREATE FUNCTION func32_a(obj: Foo, a: int32) -> str
                    USING ('foo');
                CREATE FUNCTION func32_a(obj: Bar, a: int64) -> str
                    USING ('bar');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot create the .* function: overloading an object "
            r"type-receiving function with differences in the remaining "
            r"parameters is not supported",
        ):
            await self.con.execute(r"""
                CREATE FUNCTION func32_a(obj: Foo, obj2: Bar) -> str
                    USING ('foo');
                CREATE FUNCTION func32_a(obj: Bar, obj2: Foo) -> str
                    USING ('bar');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot create the .* function: overloading an object "
            r"type-receiving function with differences in the remaining "
            r"parameters is not supported",
        ):
            await self.con.execute(r"""
                CREATE FUNCTION func32_a(obj: Foo, a: int32, b: int64) -> str
                    USING ('foo');
                CREATE FUNCTION func32_a(obj: Bar, a: int32) -> str
                    USING ('bar');
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot create the .* function: overloading an object "
            r"type-receiving function with differences in the names "
            r"of parameters is not supported",
        ):
            await self.con.execute(r"""
                CREATE FUNCTION func32_a(obj: Foo, a: int32) -> str
                    USING ('foo');
                CREATE FUNCTION func32_a(obj: Bar, b: int32) -> str
                    USING ('bar');
            """)

    async def test_edgeql_ddl_function_33(self):
        await self.con.execute(r"""
            CREATE TYPE Parent;
            CREATE TYPE Foo EXTENDING Parent;
            CREATE TYPE Bar EXTENDING Parent;
            INSERT Foo;
            INSERT Bar;
            CREATE FUNCTION func33(obj: Parent) -> str USING ('parent');
            CREATE FUNCTION func33(obj: Foo) -> str USING ('foo');
            CREATE FUNCTION func33(obj: Bar) -> str USING ('bar');
        """)

        await self.assert_query_result(
            r"""
                SELECT {
                    foo := assert_single(func33(Foo)),
                    bar := assert_single(func33(Bar)),
                }
            """,
            [{
                "foo": "foo",
                "bar": "bar",
            }],
        )

        await self.con.execute(r'''
            CREATE TYPE Baz EXTENDING Parent;
            INSERT Baz;
        ''')

        # Baz haven't got a specific overload, so it defaults to
        # Parent's implementation.
        await self.assert_query_result(
            r"""
                SELECT {
                    baz := assert_single(func33(Baz)),
                }
            """,
            [{
                "baz": "parent",
            }],
        )

        await self.con.execute(r"""
            CREATE FUNCTION func33(obj: Baz) -> str USING ('baz');
        """)

        # There is a specific impl now.
        await self.assert_query_result(
            r"""
                SELECT {
                    baz := assert_single(func33(Baz)),
                }
            """,
            [{
                "baz": "baz",
            }],
        )

        await self.con.execute(r"""
            DROP FUNCTION func33(obj: Baz);
        """)

        # Now there isn't.
        await self.assert_query_result(
            r"""
                SELECT {
                    foo := assert_single(func33(Foo)),
                    bar := assert_single(func33(Bar)),
                    baz := assert_single(func33(Baz)),
                }
            """,
            [{
                "foo": "foo",
                "bar": "bar",
                "baz": "parent",
            }],
        )

        await self.con.execute(r'''
            CREATE TYPE PriorityParent;
            ALTER TYPE Baz EXTENDING PriorityParent FIRST;
            CREATE FUNCTION func33(obj: PriorityParent) -> str
                USING ('priority parent');
        ''')

        # Now there is a new parent earlier in ancestor resolution order.
        await self.assert_query_result(
            r"""
                SELECT {
                    foo := assert_single(func33(Foo)),
                    bar := assert_single(func33(Bar)),
                    baz := assert_single(func33(Baz)),
                }
            """,
            [{
                "foo": "foo",
                "bar": "bar",
                "baz": "priority parent",
            }],
        )

        # Test that named-only stuff works just as well.
        await self.con.execute(
            r"""
                CREATE FUNCTION func33_no(NAMED ONLY obj: Parent) -> str
                    USING ('parent');
                CREATE FUNCTION func33_no(NAMED ONLY obj: Foo) -> str
                    USING ('foo');
                CREATE FUNCTION func33_no(NAMED ONLY obj: Bar) -> str
                    USING ('bar');
            """,
        )

        await self.assert_query_result(
            r"""
                SELECT {
                    foo := assert_single(func33_no(obj := Foo)),
                    bar := assert_single(func33_no(obj := Bar)),
                    baz := assert_single(func33_no(obj := Baz)),
                }
            """,
            [{
                "foo": "foo",
                "bar": "bar",
                "baz": "parent",
            }],
        )

    async def test_edgeql_ddl_function_34(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return cardinality mismatch"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION broken_edgeql_func25(
                    a: std::int64) -> std::int64
                USING EdgeQL $$
                    SELECT a FILTER a > 0
                $$;
            """)

    async def test_edgeql_ddl_function_35(self):
        with self.assertRaisesRegex(
            edgedb.InvalidFunctionDefinitionError,
            r"return cardinality mismatch"
        ):
            await self.con.execute(r"""
                CREATE FUNCTION broken_edgeql_func35(
                    a: optional std::int64) -> std::int64
                USING EdgeQL $$
                    SELECT a
                $$;
            """)

    async def test_edgeql_ddl_function_rename_01(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            }
        """)

        await self.assert_query_result(
            """SELECT foo("a")""",
            ["a"],
        )

        await self.con.execute("""
            ALTER FUNCTION foo(s: str)
            RENAME TO bar;
        """)

        await self.assert_query_result(
            """SELECT bar("a")""",
            ["a"],
        )

        await self.con.execute("""
            DROP FUNCTION bar(s: str)
        """)

    async def test_edgeql_ddl_function_rename_02(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            };

            CREATE FUNCTION bar(s: int64) -> str {
                USING (SELECT <str>s)
            };
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"can not rename function to 'default::foo' because "
                r"a function with the same name already exists"):
            await self.con.execute("""
                ALTER FUNCTION bar(s: int64)
                RENAME TO foo;
            """)

    async def test_edgeql_ddl_function_rename_03(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            };

            CREATE FUNCTION foo(s: int64) -> str {
                USING (SELECT <str>s)
            };
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"renaming an overloaded function is not allowed"):
            await self.con.execute("""
                ALTER FUNCTION foo(s: int64)
                RENAME TO bar;
            """)

    async def test_edgeql_ddl_function_rename_04(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            };
            CREATE MODULE foo;
        """)

        await self.assert_query_result(
            """SELECT foo("a")""",
            ["a"],
        )

        await self.con.execute("""
            ALTER FUNCTION foo(s: str)
            RENAME TO foo::bar;
        """)

        await self.assert_query_result(
            """SELECT foo::bar("a")""",
            ["a"],
        )

        await self.con.execute("""
            DROP FUNCTION foo::bar(s: str)
        """)

    async def test_edgeql_ddl_function_rename_05(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            };
            CREATE FUNCTION call(s: str) -> str {
                USING (SELECT foo(s))
            };
        """)

        await self.con.execute("""
            ALTER FUNCTION foo(s: str) RENAME TO bar;
        """)

        await self.assert_query_result(
            """SELECT call("a")""",
            ["a"],
        )

    async def test_edgeql_ddl_function_rename_06(self):
        await self.con.execute("""
            CREATE FUNCTION foo(s: str) -> str {
                USING (SELECT s)
            };
            CREATE FUNCTION call(s: str) -> str {
                USING (SELECT foo(s))
            };
        """)

        await self.con.execute("""
            CREATE MODULE foo;
            ALTER FUNCTION foo(s: str) RENAME TO foo::foo;
        """)

        await self.assert_query_result(
            """SELECT call("a")""",
            ["a"],
        )

    async def test_edgeql_ddl_function_volatility_01(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> int64 {
                USING (SELECT 1)
            }
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function { volatility }
            FILTER .name = 'default::foo';
            ''',
            [{
                "volatility": "Immutable",
            }]
        )

        await self.assert_query_result(
            '''SELECT (foo(), {1,2})''',
            [[1, 1], [1, 2]]
        )

    async def test_edgeql_ddl_function_volatility_02(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> int64 {
                USING (SELECT <int64>random())
            }
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {volatility}
            FILTER .name = 'default::foo';
            ''',
            [{
                "volatility": "Volatile",
            }]
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"can not take cross product of volatile operation"):
            await self.con.query(
                '''SELECT (foo(), {1,2})'''
            )

    async def test_edgeql_ddl_function_volatility_03(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> int64 {
                USING (SELECT 1);
                SET volatility := "volatile";
            }
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {volatility}
            FILTER .name = 'default::foo';
            ''',
            [{
                "volatility": "Volatile",
            }]
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"can not take cross product of volatile operation"):
            await self.con.query(
                '''SELECT (foo(), {1,2})'''
            )

    async def test_edgeql_ddl_function_volatility_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r"(?s)volatility mismatch in function declared as stable"):
            await self.con.execute('''
                CREATE FUNCTION foo() -> int64 {
                    USING (SELECT <int64>random());
                    SET volatility := "stable";
                }
            ''')

    async def test_edgeql_ddl_function_volatility_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r"(?s)volatility mismatch in function declared as immutable"):
            await self.con.execute('''
                CREATE FUNCTION foo() -> int64 {
                    USING (SELECT count(Object));
                    SET volatility := "immutable";
                }
            ''')

    async def test_edgeql_ddl_function_volatility_06(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> float64 {
                USING (1);
            };
            CREATE FUNCTION bar() -> float64 {
                USING (foo());
            };
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {name, volatility}
            FILTER .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {"name": "default::bar", "volatility": "Immutable"},
                {"name": "default::foo", "volatility": "Immutable"},
            ]
        )

        await self.con.execute('''
            ALTER FUNCTION foo() SET volatility := "stable";
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {name, volatility, computed_fields}
            FILTER .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {"name": "default::bar", "volatility": "Stable",
                 "computed_fields": ["volatility"]},
                {"name": "default::foo", "volatility": "Stable",
                 "computed_fields": []},
            ]
        )

        await self.con.execute('''
            ALTER FUNCTION foo() {
                RESET volatility;
            }
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {name, volatility, computed_fields}
            FILTER .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {"name": "default::bar", "volatility": "Immutable",
                 "computed_fields": ["volatility"]},
                {"name": "default::foo", "volatility": "Immutable",
                 "computed_fields": ["volatility"]},
            ]
        )

        await self.con.execute('''
            ALTER FUNCTION foo() {
                RESET volatility;
                USING (random());
            }
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {name, volatility}
            FILTER .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {"name": "default::bar", "volatility": "Volatile"},
                {"name": "default::foo", "volatility": "Volatile"},
            ]
        )

    async def test_edgeql_ddl_function_volatility_07(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> float64 {
                USING (1);
            };
            CREATE FUNCTION bar() -> float64 {
                USING (foo());
            };
            CREATE FUNCTION baz() -> float64 {
                USING (bar());
            };
        ''')

        # Test that the alter propagates multiple times
        await self.con.execute('''
            ALTER FUNCTION foo() SET volatility := "stable";
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function {name, volatility}
            FILTER .name LIKE 'default::%'
            ORDER BY .name;
            ''',
            [
                {"name": "default::bar", "volatility": "Stable"},
                {"name": "default::baz", "volatility": "Stable"},
                {"name": "default::foo", "volatility": "Stable"},
            ]
        )

    async def test_edgeql_ddl_function_volatility_08(self):
        await self.con.execute('''
            CREATE FUNCTION foo() -> float64 {
                USING (1);
            };
            CREATE FUNCTION bar() -> float64 {
                SET volatility := "stable";
                USING (foo());
            };
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"cannot alter function 'default::foo\(\)' because this affects "
            r".*function 'default::bar\(\)'",

        ):
            await self.con.execute('''
                ALTER FUNCTION foo() SET volatility := "volatile";
            ''')

    async def test_edgeql_ddl_function_volatility_09(self):
        await self.con.execute('''
            CREATE TYPE FuncVol { CREATE REQUIRED PROPERTY i -> int64 };
            CREATE FUNCTION obj_func(obj: FuncVol) -> int64 {
                USING (obj.i)
            };
            CREATE FUNCTION obj_func_tuple(
                obj: tuple<array<FuncVol>>
            ) -> SET OF int64 {
                USING (array_unpack(obj.0).i)
            };
            CREATE FUNCTION obj_func_tuple_not_referring(
                arg: tuple<array<FuncVol>, int64>
            ) -> int64 {
                USING (arg.1)
            };
            CREATE FUNCTION obj_func_const(obj: FuncVol) -> int64 {
                USING (1)
            };
        ''')

        await self.assert_query_result(
            r'''
            SELECT schema::Function { name, volatility }
            FILTER .name LIKE 'default::obj_func%'
            ORDER BY .name;
            ''',
            [{
                "name": "default::obj_func",
                "volatility": "Stable",
            }, {
                "name": "default::obj_func_const",
                "volatility": "Immutable",
            }, {
                "name": "default::obj_func_tuple",
                "volatility": "Stable",
            }, {
                "name": "default::obj_func_tuple_not_referring",
                "volatility": "Immutable",
            }]
        )

    async def test_edgeql_ddl_function_fallback_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*foo\(a: anytype\).*'
                r'only one generic fallback per polymorphic function '
                r'is allowed'):
            await self.con.execute(r'''
                CREATE FUNCTION foo(a: int64) -> str {
                    USING (SELECT 'foo' ++ <str>(a + 1));
                };
                CREATE FUNCTION foo(a: bytes) -> str {
                    USING (SELECT 'foobytes' ++ <str>len(a));
                };
                CREATE FUNCTION foo(a: array<anytype>) -> str {
                    SET fallback := True;
                    USING (SELECT 'fooarray' ++ <str>len(a));
                };
                CREATE FUNCTION foo(a: anytype) -> str {
                    SET fallback := True;
                    USING (SELECT 'foo' ++ <str>a);
                };
            ''')

    async def test_edgeql_ddl_function_fallback_02(self):
        await self.con.execute(r'''
            CREATE FUNCTION foo(a: int64) -> str {
                USING (SELECT 'foo' ++ <str>(a + 1));
            };
            CREATE FUNCTION foo(a: bytes) -> str {
                USING (SELECT 'foobytes' ++ <str>len(a));
            };
            CREATE FUNCTION foo(a: array<anytype>) -> str {
                USING (SELECT 'fooarray' ++ <str>len(a));
            };
            CREATE FUNCTION foo(a: anytype) -> str {
                USING (SELECT 'foo' ++ <str>a);
            };
        ''')
        await self.con.execute(r'''
            ALTER FUNCTION foo(a: array<anytype>) {
                SET fallback := true;
            };
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot alter.*foo\(a: anytype\).*'
                r'only one generic fallback per polymorphic function '
                r'is allowed'):
            await self.con.execute(r'''
                ALTER FUNCTION foo(a: anytype) {
                    SET fallback := true;
                };
            ''')

    async def test_edgeql_ddl_module_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'spam' already exists"):

            await self.con.execute('''\
                CREATE MODULE spam;
                CREATE MODULE spam;
            ''')

    async def test_edgeql_ddl_module_02(self):
        await self.con.execute('''\
            CREATE MODULE spam IF NOT EXISTS;
            CREATE MODULE spam IF NOT EXISTS;

            # Just to validate that the module was indeed created,
            # make something inside it.
            CREATE TYPE spam::Test;
        ''')

    async def test_edgeql_ddl_module_03(self):
        await self.assert_query_result(
            r'''
            select _test::abs(-1)
            ''',
            [1]
        )
        await self.con.execute('''\
            CREATE MODULE _test
        ''')
        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "'_test::abs' does not exist"):
            await self.con.execute('''\
            select _test::abs(-1)
            ''')

    async def test_edgeql_ddl_module_04(self):
        async with self.assertRaisesRegexTx(
                edgedb.UnknownModuleError,
                "module 'foo' is not in this schema"):
            await self.con.execute('''\
                CREATE MODULE foo::bar;
            ''')

        await self.con.execute('''\
            CREATE MODULE foo;
            CREATE MODULE foo::bar;
            CREATE TYPE foo::Foo;
            CREATE TYPE foo::bar::Baz;
        ''')

        await self.assert_query_result(
            r'''
            select foo::bar::Baz
            ''',
            []
        )
        await self.assert_query_result(
            r'''
            with module foo::bar
            select Baz
            ''',
            []
        )
        await self.con.execute('''\
            SET MODULE foo::bar;
        ''')
        await self.assert_query_result(
            r'''
            select foo::bar::Baz
            ''',
            []
        )
        await self.assert_query_result(
            r'''
            select Baz
            ''',
            []
        )

        await self.con.execute('''\
            SET MODULE foo;
        ''')
        # We *don't* support relative references of submodules
        async with self.assertRaisesRegexTx(
                edgedb.InvalidReferenceError,
                "'bar::Baz' does not exist"):
            await self.con.execute('''\
                SELECT bar::Baz
            ''')
        await self.assert_query_result(
            r'''
            select Foo
            ''',
            []
        )
        await self.con.execute('''\
            RESET MODULE;
        ''')

        # We *don't* support relative references of submodules
        async with self.assertRaisesRegexTx(
                edgedb.InvalidReferenceError,
                "'bar::Baz' does not exist"):
            await self.con.execute('''\
                WITH MODULE foo
                SELECT bar::Baz
            ''')

        await self.assert_query_result(
            r'''
            with m as module foo::bar
            select m::Baz
            ''',
            []
        )

        await self.assert_query_result(
            r'''
            with m as module foo
            select m::bar::Baz
            ''',
            []
        )

    async def test_edgeql_ddl_module_05(self):
        await self.con.execute('''\
            CREATE MODULE foo;
            CREATE MODULE foo::bar;
            SET MODULE foo::bar;
            CREATE TYPE Baz;
        ''')

        await self.assert_query_result(
            r'''
            select foo::bar::Baz
            ''',
            []
        )

    async def test_edgeql_ddl_operator_01(self):
        await self.con.execute('''
            CREATE INFIX OPERATOR `+++`
                (left: int64, right: int64) -> int64
            {
                SET commutator := 'default::+++';
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
                    .name = 'default::+++';
            ''',
            [{
                'name': 'default::+++',
                'params': [
                    {
                        'name': 'left',
                        'type': {
                            'name': 'std::int64'
                        },
                        'typemod': 'SingletonType'
                    },
                    {
                        'name': 'right',
                        'type': {
                            'name': 'std::int64'
                        },
                        'typemod': 'SingletonType'}
                ],
                'operator_kind': 'Infix',
                'return_typemod': 'SingletonType'
            }]
        )

        await self.con.execute('''
            ALTER INFIX OPERATOR `+++`
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
                    .name = 'default::+++'
                    AND .annotations.name = 'std::description'
                    AND .annotations@value = 'my plus';
            ''',
            [{
                'name': 'default::+++',
            }]
        )

        await self.con.execute("""
            DROP INFIX OPERATOR `+++` (left: int64, right: int64);
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
                    .name = 'default::+++';
            ''',
            []
        )

    async def test_edgeql_ddl_operator_02(self):
        try:
            await self.con.execute('''
                CREATE PREFIX OPERATOR `!`
                    (operand: int64) -> int64
                {
                    USING SQL OPERATOR r'+';
                };

                CREATE INFIX OPERATOR `!`
                    (l: int64, r: int64) -> int64
                {
                    SET commutator := 'default::!';
                    USING SQL OPERATOR r'+';
                };
            ''')

            await self.assert_query_result(
                r'''
                    WITH MODULE schema
                    SELECT Operator {
                        name,
                        operator_kind,
                    }
                    FILTER
                        .name = 'default::!'
                    ORDER BY
                        .operator_kind;
                ''',
                [
                    {
                        'name': 'default::!',
                        'operator_kind': 'Infix',
                    },
                    {
                        'name': 'default::!',
                        'operator_kind': 'Prefix',
                    }
                ]
            )

        finally:
            await self.con.execute('''
                DROP INFIX OPERATOR `!`
                    (l: int64, r: int64);

                DROP PREFIX OPERATOR `!`
                    (operand: int64);
            ''')

    async def test_edgeql_ddl_operator_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the `default::NOT\(\)` operator: '
                r'an operator must have operands'):
            await self.con.execute('''
                CREATE PREFIX OPERATOR `NOT`() -> bool
                    USING SQL EXPRESSION;
            ''')

    async def test_edgeql_ddl_operator_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`default::=\(l: array<anytype>, r: std::str\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `=` (l: array<anytype>, r: str) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the '
                r'`default::=\(l: array<anytype>, r: anytuple\)` operator: '
                r'operands of a recursive operator must either be '
                r'all arrays or all tuples'):
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `=` (l: array<anytype>, r: anytuple) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the non-recursive '
                r'`default::=\(l: array<std::int64>, '
                r'r: array<std::int64>\)` operator: '
                r'overloading a recursive operator '
                r'`array<anytype> = array<anytype>` with a non-recursive one '
                r'is not allowed'):
            # attempt to overload a recursive `=` from std with a
            # non-recursive version
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `=` (l: array<anytype>, r: array<anytype>) -> std::bool {
                    SET recursive := true;
                    USING SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                `=` (l: array<int64>, r: array<int64>) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_07(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'cannot create the recursive '
                r'`default::=\(l: array<std::int64>, '
                r'r: array<std::int64>\)` operator: '
                r'overloading a non-recursive operator '
                r'`array<anytype> = array<anytype>` with a recursive one '
                r'is not allowed'):
            # create 2 operators in test: non-recursive first, then a
            # recursive one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `=` (l: array<anytype>, r: array<anytype>)
                    -> std::bool {
                    USING SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                `=` (l: array<int64>, r: array<int64>) -> std::bool {
                    USING SQL EXPRESSION;
                    SET recursive := true;
                };
            ''')

    async def test_edgeql_ddl_operator_08(self):
        try:
            await self.con.execute('''
                CREATE ABSTRACT INFIX OPERATOR `>`
                    (left: anytype, right: anytype) -> bool;
            ''')

            await self.assert_query_result(
                r'''
                    WITH MODULE schema
                    SELECT Operator {
                        name,
                        abstract,
                    }
                    FILTER
                        .name = 'default::>'
                ''',
                [
                    {
                        'name': 'default::>',
                        'abstract': True,
                    },
                ]
            )

        finally:
            await self.con.execute('''
                DROP INFIX OPERATOR `>`
                    (left: anytype, right: anytype);
            ''')

    async def test_edgeql_ddl_operator_09(self):
        with self.assertRaisesRegex(
                edgedb.InvalidOperatorDefinitionError,
                r'unexpected USING clause in abstract operator definition'):
            await self.con.execute('''
                CREATE ABSTRACT INFIX OPERATOR
                `=` (l: array<anytype>, r: array<anytype>) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_10(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateOperatorDefinitionError,
                r'cannot create the '
                r'`default::IN\(l: std::int64, r: std::int64\)` operator: '
                r'there exists a derivative operator of the same name'):
            # create 2 operators in test: derivative first, then a
            # non-derivative one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `IN` (l: std::float64, r: std::float64) -> std::bool {
                    USING SQL EXPRESSION;
                    SET derivative_of := 'std::=';
                };

                CREATE INFIX OPERATOR
                `IN` (l: std::int64, r: std::int64) -> std::bool {
                    USING SQL EXPRESSION;
                };
            ''')

    async def test_edgeql_ddl_operator_11(self):
        with self.assertRaisesRegex(
                edgedb.DuplicateOperatorDefinitionError,
                r'cannot create '
                r'`default::IN\(l: std::int64, r: std::int64\)` as a '
                r'derivative operator: there already exists an operator '
                r'of the same name'):
            # create 2 operators in test: non-derivative first, then a
            # derivative one
            await self.con.execute('''
                CREATE INFIX OPERATOR
                `IN` (l: std::float64, r: std::float64) -> std::bool {
                    USING SQL EXPRESSION;
                };

                CREATE INFIX OPERATOR
                `IN` (l: std::int64, r: std::int64) -> std::bool {
                    USING SQL EXPRESSION;
                    SET derivative_of := 'std::=';
                };
            ''')

    async def test_edgeql_ddl_operator_12(self):
        with self.assertRaisesRegex(
            edgedb.InternalServerError,
            r'operator "! std::int64" is declared to return SQL type "int8", '
            r'but the underlying SQL function returns "numeric"',
        ):
            await self.con.execute('''
                CREATE PREFIX OPERATOR
                `!` (l: std::int64) -> std::int64 {
                    USING SQL FUNCTION 'factorial';
                };
            ''')

    async def test_edgeql_ddl_scalar_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'may not have more than one concrete base type'):
            await self.con.execute('''
                CREATE SCALAR TYPE myint EXTENDING std::int64, std::str;
            ''')

    async def test_edgeql_ddl_scalar_02(self):
        await self.con.execute('''
            CREATE ABSTRACT SCALAR TYPE a EXTENDING std::int64;
            CREATE ABSTRACT SCALAR TYPE b EXTENDING std::str;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'may not have more than one concrete base type'):
            await self.con.execute('''
                CREATE SCALAR TYPE myint EXTENDING a, b;
            ''')

    async def test_edgeql_ddl_scalar_03(self):
        await self.con.execute('''
            CREATE ABSTRACT SCALAR TYPE a EXTENDING std::int64;
            CREATE ABSTRACT SCALAR TYPE b EXTENDING std::str;
            CREATE SCALAR TYPE myint EXTENDING a;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'scalar type may not have more than one concrete base type'):
            await self.con.execute('''
                ALTER SCALAR TYPE myint EXTENDING b;
            ''')

    async def test_edgeql_ddl_scalar_04(self):
        await self.con.execute('''
            CREATE ABSTRACT SCALAR TYPE a;
            CREATE SCALAR TYPE myint EXTENDING int64, a;
        ''')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'scalar type may not have more than one concrete base type'):
            await self.con.execute('''
                ALTER SCALAR TYPE a EXTENDING str;
            ''')

    async def test_edgeql_ddl_scalar_05(self):
        await self.con.execute('''
            CREATE ABSTRACT SCALAR TYPE a EXTENDING std::int64;
            CREATE ABSTRACT SCALAR TYPE b EXTENDING std::int64;
            CREATE SCALAR TYPE myint EXTENDING a, b;
        ''')

    async def test_edgeql_ddl_scalar_06(self):
        await self.con.execute('''
            CREATE SCALAR TYPE myint EXTENDING int64;
            CREATE SCALAR TYPE myint2 EXTENDING myint;
        ''')

    async def test_edgeql_ddl_scalar_07(self):
        await self.con.execute('''
            CREATE SCALAR TYPE a EXTENDING std::str;
            CREATE SCALAR TYPE b EXTENDING std::str;
        ''')

        # I think we want to prohibit this kind of diamond pattern
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'may not have more than one concrete base type'):
            await self.con.execute('''
                CREATE SCALAR TYPE myint EXTENDING a, b;
            ''')

    async def test_edgeql_ddl_scalar_08(self):
        await self.con.execute('''

            CREATE SCALAR TYPE myint EXTENDING int64;
            CREATE TYPE Bar {
                CREATE PROPERTY b1 -> tuple<myint, tuple<myint>>;
                CREATE PROPERTY b2 -> tuple<myint, tuple<myint>>;
                CREATE MULTI PROPERTY b3 -> tuple<z: myint, y: array<myint>>;
            };
            CREATE TYPE Foo {
                CREATE PROPERTY a1 -> array<myint>;
                CREATE PROPERTY a2 -> tuple<array<myint>>;
                CREATE PROPERTY a3 -> array<tuple<array<myint>>>;
                CREATE PROPERTY a4 -> tuple<myint, str>;
                CREATE PROPERTY a5 -> tuple<myint, myint>;
                CREATE PROPERTY a6 -> tuple<myint, tuple<myint>>;
                CREATE PROPERTY a6b -> tuple<myint, tuple<myint>>;
                CREATE LINK l -> Bar {
                    CREATE PROPERTY l1 -> tuple<str, myint>;
                    CREATE PROPERTY l2 -> tuple<myint, tuple<myint>>;
                };
            };
        ''')

        count_query = "SELECT count(schema::CollectionType);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute('''
            ALTER SCALAR TYPE myint CREATE CONSTRAINT std::one_of(1, 2);
        ''')

        self.assertEqual(await self.con.query_single(count_query), orig_count)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                'myint must be one of'):
            await self.con.execute('''
                INSERT Foo { a4 := (10, "oops") };
            ''')

        await self.con.execute('''
            INSERT Foo { a3 := [([2],)] };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                'myint must be one of:'):
            await self.con.execute('''
                ALTER SCALAR TYPE myint DROP CONSTRAINT std::one_of(1, 2);
                ALTER SCALAR TYPE myint CREATE CONSTRAINT std::one_of(1);
            ''')

    async def test_edgeql_ddl_scalar_09(self):
        # We need to support CREATE FINAL SCALAR for enums because it
        # is written out into old migrations.
        await self.con.execute('''
            CREATE FINAL SCALAR TYPE my_enum EXTENDING enum<'foo', 'bar'>;
        ''')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'FINAL is not supported'):
            await self.con.execute('''
                CREATE FINAL SCALAR TYPE myint EXTENDING std::int64;
            ''')

    async def test_edgeql_ddl_scalar_10(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'scalar type must have a concrete base type'):
            await self.con.execute('''
                create scalar type Foo;
            ''')

    async def test_edgeql_ddl_scalar_11(self):
        await self.con.execute('''
            create scalar type Foo extending str;
        ''')
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'scalar type must have a concrete base type'):
            await self.con.execute('''
                alter scalar type Foo drop extending str;
            ''')

    async def test_edgeql_ddl_scalar_12(self):
        await self.con.execute('''
            create scalar type Foo extending str;
        ''')
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r'cannot change concrete base of a scalar type'):
            await self.con.execute('''
                alter scalar type Foo {
                    drop extending str;
                    extending int64 LAST;
                };
            ''')

    async def test_edgeql_ddl_scalar_13(self):
        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                r'scalar type may not have a collection base type'):
            await self.con.execute('''
                create scalar type Foo extending array<str>;
            ''')

        await self.con.execute('''
            create scalar type Foo extending str;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                r'scalar type may not have a collection base type'):
            await self.con.execute('''
                alter scalar type Foo {
                    drop extending str; extending array<str> last;
                };
            ''')

    async def test_edgeql_ddl_cast_01(self):
        await self.con.execute('''
            CREATE SCALAR TYPE type_a EXTENDING std::str;
            CREATE SCALAR TYPE type_b EXTENDING std::int64;
            CREATE SCALAR TYPE type_c EXTENDING std::datetime;

            CREATE CAST FROM type_a TO type_b {
                USING SQL CAST;
                ALLOW IMPLICIT;
            };

            CREATE CAST FROM type_a TO type_c {
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
                    .from_type.name LIKE 'default::%'
                ORDER BY
                    .allow_implicit;
            ''',
            [
                {
                    'from_type': {'name': 'default::type_a'},
                    'to_type': {'name': 'default::type_c'},
                    'allow_implicit': False,
                    'allow_assignment': True,
                },
                {
                    'from_type': {'name': 'default::type_a'},
                    'to_type': {'name': 'default::type_b'},
                    'allow_implicit': True,
                    'allow_assignment': False,
                }
            ]
        )

        await self.con.execute("""
            DROP CAST FROM type_a TO type_b;
            DROP CAST FROM type_a TO type_c;
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
                    .from_type.name LIKE 'default::%'
                ORDER BY
                    .allow_implicit;
            ''',
            []
        )

    async def test_edgeql_ddl_policies_01(self):
        await self.con.execute(r"""
            create required global filtering -> bool { set default := false };
            create global cur -> str;

            create type User {
                create required property name -> str;
                create access policy all_on allow all using (true);
                create access policy filtering
                    when (global filtering)
                    deny select, delete using (.name ?!= global cur);
            };
            create type Bot extending User;
        """)

        await self.assert_query_result(
            '''
                select schema::AccessPolicy {
                    name, condition, expr, action, access_kinds,
                    sname := .subject.name, root := not exists .bases }
                filter .sname like 'default::%'
            ''',
            tb.bag([
                {
                    "access_kinds": {
                        "Select", "UpdateRead", "UpdateWrite", "Delete",
                        "Insert"
                    },
                    "action": "Allow",
                    "condition": None,
                    "expr": "true",
                    "name": "all_on",
                    "sname": "default::User",
                    "root": True,
                },
                {
                    "access_kinds": {"Select", "Delete"},
                    "action": "Deny",
                    "condition": "global default::filtering",
                    "expr": "(.name ?!= global default::cur)",
                    "name": "filtering",
                    "sname": "default::User",
                    "root": True,
                },
                {
                    "access_kinds": {
                        "Select", "UpdateRead", "UpdateWrite", "Delete",
                        "Insert"
                    },
                    "action": "Allow",
                    "condition": None,
                    "expr": "true",
                    "name": "all_on",
                    "sname": "default::Bot",
                    "root": False,
                },
                {
                    "access_kinds": {"Select", "Delete"},
                    "action": "Deny",
                    "condition": "global default::filtering",
                    "expr": "(.name ?!= global default::cur)",
                    "name": "filtering",
                    "sname": "default::Bot",
                    "root": False,
                },
            ])
        )

        await self.con.execute(r"""
            alter type User {
                alter access policy filtering {
                    reset when;
                    deny select;
                    using (false);
                }
            };
        """)

        await self.assert_query_result(
            '''
                select schema::AccessPolicy {
                    name, condition, expr, action, access_kinds,
                    sname := .subject.name, root := not exists .bases }
                filter .sname = 'default::User' and .name = 'filtering'
            ''',
            [
                {
                    "access_kinds": {"Select"},
                    "action": "Deny",
                    "condition": None,
                    "expr": "false",
                    "name": "filtering",
                    "sname": "default::User",
                    "root": True,
                },
            ]
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"cannot alter the definition of inherited access policy",
        ):
            await self.con.execute('''
                alter type Bot alter access policy filtering allow all;
            ''')

    async def test_edgeql_ddl_policies_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"when expression.* is of invalid type",
        ):
            await self.con.execute("""
                create type X {
                    create access policy test
                        when (1)
                        allow all using (true);
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"using expression.* is of invalid type",
        ):
            await self.con.execute("""
                create type X {
                    create access policy test
                        allow all using (1);
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly an empty set returned",
        ):
            await self.con.execute("""
                create type X {
                    create property x -> str;
                    create access policy test
                        allow all using (.x not like '%redacted%');
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly more than one element returned",
        ):
            await self.con.execute("""
                create type X {
                    create access policy test
                        allow all using ({true, false});
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"has a volatile using expression",
        ):
            await self.con.execute("""
                create type X {
                    create access policy test
                        allow all using (random() < 0.5);
                };
            """)

    async def test_edgeql_ddl_policies_03(self):
        # Ideally we will make this actually work instead of rejecting it!
        await self.con.execute("""
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE MULTI LINK tgt -> Tgt {
                    CREATE PROPERTY foo -> str;
                };
                CREATE ACCESS POLICY asdf
                    ALLOW ALL USING (all(.tgt@foo LIKE '%!'));
            };
        """)
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"may not refer to link properties with default values"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK tgt ALTER property foo
                  SET default := "!!!";
            """)

    async def test_edgeql_ddl_policies_04(self):
        await self.con.execute("""
            create global current_user -> uuid;

            create type User {
                create access policy ins allow insert;
                create access policy sel allow select
                  using (.id ?= global current_user);
            };
            create type User2 extending User;

            create type Obj {
                create optional multi link user -> User;
            };
        """)

        await self.con.execute("""
            alter type Obj {
                alter link user set required using (select User limit 1);
            };
        """)
        await self.con.execute("""
            alter type Obj {
                alter link user set single using (select User limit 1);
            };
        """)
        await self.con.execute("""
            alter type Obj {
                alter link user set type User2 using (select User2 limit 1);
            };
        """)

    # A big collection of tests to make sure that functions get
    # updated when access policies change
    async def test_edgeql_ddl_func_policies_01(self):
        await self.con.execute(r"""
            create type X;
            insert X;
            create function get_x() -> set of uuid using (X.id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_02(self):
        await self.con.execute(r"""
            create type Y;
            create type X extending Y;
            insert X;
            create function get_x() -> set of uuid using (X.id);
            alter type Y {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_03(self):
        await self.con.execute(r"""
            create type X;
            create type W extending X;
            insert W;
            create function get_x() -> set of uuid using (X.id);
            alter type W {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_04(self):
        await self.con.execute(r"""
            create type Y;
            create type X;
            create type W extending X, Y;
            insert W;
            create function get_x() -> set of uuid using (X.id);
            alter type Y {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_05(self):
        # links
        await self.con.execute(r"""
            create type X;
            create type T { create link x -> X };
            insert T { x := (insert X) };
            create function get_x() -> set of uuid using (T.x.id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_06(self):
        # links
        await self.con.execute(r"""
            create type T;
            create type X { create link t -> T };

            insert X { t := (insert T) };
            create function get_x() -> set of uuid using (T.<t[IS X].id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_07(self):
        # links
        await self.con.execute(r"""
            create type T;
            create type X { create link t -> T };

            insert X { t := (insert T) };
            create function get_x() -> set of uuid using (T.<t.id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_08(self):
        # links
        await self.con.execute(r"""
            create type X;
            create type Y;
            create type T { create link x -> X | Y };
            insert T { x := (insert X) };
            create function get_x() -> set of uuid using (T.x.id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_09(self):
        await self.con.execute(r"""
            create type X;
            insert X;
            create alias Y := X;
            create function get_x() -> set of uuid using (Y.id);
            alter type X {
                create access policy test allow select using (false) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

    async def test_edgeql_ddl_func_policies_10(self):
        # Make sure we succesfully update multiple functions to thread
        # through globals when an access policy is created.
        await self.con.execute(r"""
            create type X;
            insert X;
            create function get_xi() -> set of uuid using (X.id);
            create function get_x() -> set of uuid using (get_xi());
            create required global en -> bool { set default := false };
            alter type X {
                create access policy test allow select using (global en) };
        """)

        await self.assert_query_result(
            'select get_x()',
            [],
        )

        await self.con.execute('''
            set global en := true;
        ''')

        await self.assert_query_result(
            'select get_x()',
            [str],
        )

    async def test_edgeql_ddl_global_01(self):
        INTRO_Q = '''
            select schema::Global {
                required, typ := .target.name, default }
            filter .name = 'default::foo';
        '''

        await self.con.execute(r"""
            create global foo -> str;
        """)

        await self.assert_query_result(
            INTRO_Q,
            [{
                "required": False,
                "typ": "std::str",
                "default": None,
            }]
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"required globals must have a default",
        ):
            await self.con.execute("""
                alter global foo set required;
            """)

        await self.con.execute(r"""
            drop global foo;
            create required global foo -> str { set default := "" };
        """)

        await self.assert_query_result(
            INTRO_Q,
            [{
                "required": True,
                "typ": "std::str",
                "default": "''",
            }]
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"default expression is of invalid type",
        ):
            await self.con.execute("""
                alter global foo set type array<uuid> reset to default;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"required globals must have a default",
        ):
            await self.con.execute("""
                alter global foo reset default;
            """)

        await self.con.execute("""
            alter global foo set optional;
            alter global foo reset default;
            alter global foo set type array<int64> reset to default;
        """)

        await self.assert_query_result(
            INTRO_Q,
            [{
                "required": False,
                "typ": "array<std::int64>",
                "default": None,
            }]
        )

    async def test_edgeql_ddl_global_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "non-computed globals may not be multi",
        ):
            await self.con.execute("""
                create multi global foo -> str;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly more than one element returned",
        ):
            await self.con.execute("""
                create global foo -> str {
                    set default := {"foo", "bar"}
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly no elements returned",
        ):
            await self.con.execute("""
                create required global foo -> str {
                    set default := (select "foo" filter false)
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"non-computed globals may not have have object type",
        ):
            await self.con.execute("""
                create global foo -> Object;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"non-computed globals may not have have object type",
        ):
            await self.con.execute("""
                create global foo -> array<Object>;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"has a volatile default expression, which is not allowed",
        ):
            await self.con.execute("""
                create global foo -> float64 { set default := random(); };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "computed globals may not have default values",
        ):
            await self.con.execute("""
                create global test {
                    using ('abc');
                    set default := 'def';
                }
            """)

    async def test_edgeql_ddl_global_03(self):
        await self.con.execute("""
            create global foo -> str;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"global variables cannot be referenced from constraint"
        ):
            await self.con.execute("""
                create type X {
                    create property foo -> str {
                        create constraint expression on (
                            __subject__ != global foo)
                    }
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"global variables cannot be referenced from index"
        ):
            await self.con.execute("""
                create type X {
                    create index on (global foo);
                }
            """)

        await self.con.execute("""
            create type X;
        """)

        # We can't set a default that uses a global when creating a new
        # pointer, since it would need to run *now* and populate the data
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"globals may not be used when converting/populating data "
            r"in migrations"
        ):
            await self.con.execute("""
                alter type X {
                    create property foo -> str {
                        set default := (global foo);
                    }
                };
            """)

        await self.con.execute("""
            set global foo := "test"
        """)
        # But we *can* do it when creating a brand new type
        await self.con.execute("""
            create type Y {
                create property foo -> str {
                    set default := (global foo);
                }
            };
        """)
        await self.con.query("""
            insert Y;
        """)
        await self.assert_query_result(
            r'''
                select Y.foo
            ''',
            ['test']
        )

        # And when adding a default to an existing column
        await self.con.execute("""
            alter type X {
                create property foo -> str;
            };
            alter type X {
                alter property foo {
                    set default := (global foo);
                }
            };
        """)
        await self.con.query("""
            insert X;
        """)
        await self.assert_query_result(
            r'''
                select X.foo
            ''',
            ['test']
        )

    async def test_edgeql_ddl_global_04(self):
        # mostly the same as _03 but with functions
        await self.con.execute("""
            create global foo -> str;
            create function gfoo() -> optional str using (global foo)
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"functions that reference global variables cannot be called "
            r"from constraint"
        ):
            await self.con.execute("""
                create type X {
                    create property foo -> str {
                        create constraint expression on (
                            __subject__ != gfoo())
                    }
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"functions that reference global variables cannot be called "
            r"from index"
        ):
            await self.con.execute("""
                create type X {
                    create index on (gfoo());
                }
            """)

        await self.con.execute("""
            create type X;
        """)

        # We can't set a default that uses a global when creating a new
        # pointer, since it would need to run *now* and populate the data
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"functions that reference globals may not be used when "
            r"converting/populating data in migrations"
        ):
            await self.con.execute("""
                alter type X {
                    create property foo -> str {
                        set default := (gfoo());
                    }
                };
            """)

    async def test_edgeql_ddl_global_05(self):
        await self.con.execute("""
            create global foo -> str;
            create function gfoo() -> optional str using ("test");
            create function gbar() -> optional str using (gfoo());
        """)
        await self.con.execute("""
            set global foo := "!!"
        """)
        # test that when we alter a function definition, functions
        # that depend on it get updated
        await self.con.execute("""
            alter function gfoo() using (global foo)
        """)
        await self.assert_query_result(
            r'''select gbar()''',
            ["!!"],
        )

    async def test_edgeql_ddl_global_06(self):
        INTRO_Q = '''
            select schema::Global {
                required, cardinality, typ := .target.name,
                req_comp := contains(.computed_fields, 'required'),
                card_comp := contains(.computed_fields, 'cardinality'),
                computed := exists .expr,
            }
            filter .name = 'default::foo';
        '''
        await self.con.execute('''
            create global foo := 10;
        ''')

        await self.assert_query_result(
            INTRO_Q,
            [{
                "computed": True,
                "card_comp": True,
                "req_comp": True,
                "required": True,
                "cardinality": "One",
                "typ": "default::foo",
            }]
        )

        await self.con.execute('''
            drop global foo;
            create optional multi global foo := 10;
        ''')

        await self.assert_query_result(
            INTRO_Q,
            [{
                "computed": True,
                "card_comp": False,
                "req_comp": False,
                "required": False,
                "cardinality": "Many",
                "typ": "default::foo",
            }]
        )

        await self.con.execute('''
            alter global foo reset optionality;
        ''')

        await self.assert_query_result(
            INTRO_Q,
            [{
                "computed": True,
                "card_comp": False,
                "req_comp": True,
                "required": True,
                "cardinality": "Many",
                "typ": "default::foo",
            }]
        )

        await self.con.execute('''
            alter global foo {
                reset cardinality;
                reset expression;
                set type str reset to default;
            };
        ''')

        await self.assert_query_result(
            INTRO_Q,
            [{
                "computed": False,
                "card_comp": False,
                "req_comp": False,
                "required": False,
                "cardinality": "One",
                "typ": "std::str",
            }]
        )

    async def test_edgeql_ddl_global_07(self):
        await self.con.execute('''
            create global foo := <str>Object.id
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly an empty set returned",
        ):
            await self.con.execute("""
                alter global foo set required;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"possibly more than one element returned",
        ):
            await self.con.execute("""
                alter global foo set single;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot specify a type and an expression for a global",
        ):
            await self.con.execute("""
                alter global foo set type str reset to default;
            """)

    async def test_edgeql_ddl_global_08(self):
        await self.con.execute('''
            create global foo -> str;
            set global foo := "test";
        ''')
        await self.con.execute('''
            alter global foo set type int64 reset to default;
        ''')
        await self.assert_query_result(
            r'''select global foo''',
            []
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"SET TYPE on global must explicitly reset the global's value"
        ):
            await self.con.execute("""
                alter global foo set type str;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"USING casts for SET TYPE on globals are not supported"
        ):
            await self.con.execute("""
                alter global foo set type str using ('lol');
            """)

    async def test_edgeql_ddl_property_computable_01(self):
        await self.con.execute('''\
            CREATE TYPE CompProp;
            ALTER TYPE CompProp {
                CREATE PROPERTY prop := 'I am a computable';
            };
            INSERT CompProp;
        ''')

        await self.assert_query_result(
            r'''
                SELECT CompProp {
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
                    .name = 'default::CompProp';
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

    async def test_edgeql_ddl_property_computable_02(self):
        await self.con.execute('''\
            CREATE TYPE CompProp {
                CREATE PROPERTY prop := 'I am a computable';
            };
            INSERT CompProp;
        ''')

        await self.assert_query_result(
            r'''
                SELECT CompProp {
                    prop
                };
            ''',
            [{
                'prop': 'I am a computable',
            }],
        )

        await self.con.execute('''\
            ALTER TYPE CompProp {
                ALTER PROPERTY prop {
                    RESET EXPRESSION;
                };
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT CompProp {
                    prop
                };
            ''',
            [{
                'prop': None,
            }],
        )

    async def test_edgeql_ddl_property_computable_03(self):
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE PROPERTY bar -> str;
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Foo { ALTER PROPERTY bar { USING (1) } };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Foo { ALTER PROPERTY bar { USING ("1") } };
        ''')

    async def test_edgeql_ddl_property_computable_circular(self):
        await self.con.execute('''\
            CREATE TYPE CompPropCircular {
                CREATE PROPERTY prop := (SELECT count(CompPropCircular))
            };
        ''')

    async def test_edgeql_ddl_property_computable_add_dep(self):
        # Make sure things don't get messed up when we add new dependencies
        await self.con.execute('''
            create type A {
                create property foo := "!";
                create property bar -> str;
            };
            alter type A alter property foo using (.bar);
            create type B extending A;
        ''')

    async def test_edgeql_ddl_property_computable_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type: expected.* got .* 'std::Object'"):
            await self.con.execute('''\
                CREATE TYPE CompPropBad;
                ALTER TYPE CompPropBad {
                    CREATE PROPERTY prop := (SELECT std::Object LIMIT 1);
                };
            ''')

    async def test_edgeql_ddl_link_computable_01(self):
        await self.con.execute('''\
            CREATE TYPE LinkTarget;
            CREATE TYPE CompLink {
                CREATE MULTI LINK l := LinkTarget;
            };

            INSERT LinkTarget;
            INSERT CompLink;
        ''')
        await self.assert_query_result(
            r'''
                SELECT CompLink {
                    l: {
                        id
                    }
                };
            ''',
            [{
                'l': [{
                    'id': uuid.UUID
                }],
            }],
        )

        await self.con.execute('''\
            ALTER TYPE CompLink {
                ALTER LINK l {
                    RESET EXPRESSION;
                };
            };
        ''')
        await self.assert_query_result(
            r'''
                SELECT CompLink {
                    l: {
                        id
                    }
                };
            ''',
            [{
                'l': [],
            }],
        )

    async def test_edgeql_ddl_link_computable_02(self):
        await self.con.execute('''
            CREATE TYPE LinkTarget;
        ''')

        # TODO We want to actually support this, but until then we should
        # have a decent error.
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"including a shape on schema-defined computed links "
            r"is not yet supported"
        ):
            await self.con.execute("""
                CREATE TYPE X { CREATE LINK x := LinkTarget { z := 1 } };
            """)

    async def test_edgeql_ddl_link_computable_circular_01(self):
        await self.con.execute('''\
            CREATE TYPE CompLinkCircular {
                CREATE LINK l := (SELECT CompLinkCircular LIMIT 1)
            };
        ''')

    async def test_edgeql_ddl_link_target_circular_01(self):
        # Circular target as part of a union.
        await self.con.execute('''\
            CREATE TYPE LinkCircularA;
            CREATE TYPE LinkCircularB {
                CREATE LINK l -> LinkCircularA
                                 | LinkCircularB;
            };
        ''')

    async def test_edgeql_ddl_annotation_01(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION attr1;

            CREATE SCALAR TYPE TestAttrType1 EXTENDING std::str {
                CREATE ANNOTATION attr1 := 'aaaa';
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
                    .name = 'default::TestAttrType1';
            ''',
            [{"annotations": [{"name": "default::attr1", "@value": "aaaa"}]}]
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
                    .name = 'default::TestAttrType1';
            ''',
            [{"annotations": [{"name": "default::attr2", "@value": "aaaa"}]}]
        )

    async def test_edgeql_ddl_annotation_02(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION attr1;

            CREATE TYPE TestAttrType2 {
                CREATE ANNOTATION attr1 := 'aaaa';
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
                    } FILTER .name = 'default::attr2'
                }
                FILTER
                    .name = 'default::TestAttrType2';
            ''',
            [{"annotations": [{"name": "default::attr2", "@value": "aaaa"}]}]
        )

    async def test_edgeql_ddl_annotation_03(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION noninh;
            CREATE ABSTRACT INHERITABLE ANNOTATION inh;

            CREATE TYPE TestAttr1 {
                CREATE ANNOTATION noninh := 'no inherit';
                CREATE ANNOTATION inh := 'inherit me';
            };

            CREATE TYPE TestAttr2 EXTENDING TestAttr1;
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
                    FILTER .name LIKE 'default::%'
                    ORDER BY .name
                }
                FILTER
                    .name LIKE 'default::TestAttr%'
                ORDER BY
                    .name;
            ''',
            [{
                "annotations": [{
                    "name": "default::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }, {
                    "name": "default::noninh",
                    "@value": "no inherit",
                }]
            }, {
                "annotations": [{
                    "name": "default::inh",
                    "inheritable": True,
                    "@value": "inherit me",
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_04(self):
        await self.con.execute('''
            CREATE TYPE BaseAnno4;
            CREATE TYPE DerivedAnno4 EXTENDING BaseAnno4;
            CREATE ABSTRACT ANNOTATION noninh_anno;
            CREATE ABSTRACT INHERITABLE ANNOTATION inh_anno;
            ALTER TYPE BaseAnno4
                CREATE ANNOTATION noninh_anno := '1';
            ALTER TYPE BaseAnno4
                CREATE ANNOTATION inh_anno := '2';
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
                    FILTER .name LIKE 'default::%_anno'
                    ORDER BY .name
                }
                FILTER
                    .name = 'default::DerivedAnno4'
                ORDER BY
                    .name;
            ''',
            [{
                "annotations": [{
                    "name": "default::inh_anno",
                    "inheritable": True,
                    "@value": "2",
                }]
            }]
        )

    async def test_edgeql_ddl_annotation_05(self):
        await self.con.execute(r'''
            CREATE TYPE BaseAnno05 {
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
                    .name = 'default::BaseAnno05';
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
            CREATE TYPE BaseAnno06 {
                CREATE PROPERTY name -> str;
                CREATE INDEX ON (.name);
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE BaseAnno06 {
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
                    .name = 'default::BaseAnno06';
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
            ALTER TYPE BaseAnno06 {
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
                    .name = 'default::BaseAnno06';
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
            CREATE TYPE BaseAnno07 {
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
                    .name = 'default::BaseAnno07';
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
                    .name = 'default::BaseAnno07';
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
            CREATE TYPE BaseAnno08 {
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
                    .name = 'default::BaseAnno08';
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
                    .name = 'default::BaseAnno08';
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
            CREATE ABSTRACT ANNOTATION anno09;

            CREATE TYPE TestTypeAnno09 {
                CREATE ANNOTATION anno09 := 'A';
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name = 'default::anno09'
                }
                FILTER
                    .name = 'default::TestTypeAnno09';
            ''',
            [{"annotations": [{"name": "default::anno09", "@value": "A"}]}]
        )

        # Alter the annotation.
        await self.con.execute("""
            ALTER TYPE TestTypeAnno09 {
                ALTER ANNOTATION anno09 := 'B';
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name = 'default::anno09'
                }
                FILTER
                    .name = 'default::TestTypeAnno09';
            ''',
            [{"annotations": [{"name": "default::anno09", "@value": "B"}]}]
        )

    async def test_edgeql_ddl_annotation_10(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION anno10;
            CREATE ABSTRACT INHERITABLE ANNOTATION anno10_inh;

            CREATE TYPE TestTypeAnno10
            {
                CREATE ANNOTATION anno10 := 'A';
                CREATE ANNOTATION anno10_inh := 'A';
            };

            CREATE TYPE TestSubTypeAnno10
                    EXTENDING TestTypeAnno10
            {
                CREATE ANNOTATION anno10 := 'B';
                ALTER ANNOTATION anno10_inh := 'B';
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
                    FILTER .name LIKE 'default::anno10%'
                    ORDER BY .name
                }
                FILTER
                    .name LIKE 'default::%Anno10'
                ORDER BY
                    .name
            ''',
            [
                {
                    "annotations": [
                        {"name": "default::anno10", "@value": "B"},
                        {"name": "default::anno10_inh", "@value": "B"},
                    ]
                },
                {
                    "annotations": [
                        {"name": "default::anno10", "@value": "A"},
                        {"name": "default::anno10_inh", "@value": "A"},
                    ]
                },
            ]
        )

        # Drop the non-inherited annotation from subtype.
        await self.con.execute("""
            ALTER TYPE TestSubTypeAnno10 {
                DROP ANNOTATION anno10;
            };
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT ObjectType {
                    annotations: {
                        name,
                        @value,
                    } FILTER .name LIKE 'default::anno10%'
                }
                FILTER
                    .name = 'default::TestSubTypeAnno10';
            ''',
            [{"annotations": [{"name": "default::anno10_inh", "@value": "B"}]}]
        )

        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "cannot drop inherited annotation 'default::anno10_inh'",
        ):
            await self.con.execute("""
                ALTER TYPE TestSubTypeAnno10 {
                    DROP ANNOTATION anno10_inh;
                };
            """)

    async def test_edgeql_ddl_annotation_11(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION anno11;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'default::anno11%';
            ''',
            [{"name": "default::anno11"}]
        )

        await self.con.execute("""
            ALTER ABSTRACT ANNOTATION anno11
                RENAME TO anno11_new_name;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'default::anno11%';
            ''',
            [{"name": "default::anno11_new_name"}]
        )

        await self.con.execute("""
            CREATE MODULE foo;

            ALTER ABSTRACT ANNOTATION anno11_new_name
                RENAME TO foo::anno11_new_name;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'foo::anno11%';
            ''',
            [{"name": "foo::anno11_new_name"}]
        )

        await self.con.execute("""
            DROP ABSTRACT ANNOTATION foo::anno11_new_name;
        """)

        await self.assert_query_result(
            r'''
                WITH MODULE schema
                SELECT Annotation {
                    name,
                }
                FILTER
                    .name LIKE 'foo::anno11%';
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

    async def test_edgeql_ddl_annotation_13(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION anno13;
        """)

        with self.assertRaisesRegex(
            edgedb.UnknownModuleError,
            "module 'bogus' is not in this schema",
        ):
            await self.con.execute("""
                ALTER ABSTRACT ANNOTATION anno13 RENAME TO bogus::anno13;
            """)

    async def test_edgeql_ddl_annotation_14(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION anno;
            CREATE TYPE Foo {
                CREATE ANNOTATION anno := "test";
            };
        """)

        await self.con.execute("""
            ALTER ABSTRACT ANNOTATION anno
                RENAME TO anno_new_name;
        """)

        await self.assert_query_result(
            "DESCRIBE MODULE default as sdl",
            ["""
abstract annotation default::anno_new_name;
type default::Foo {
    annotation default::anno_new_name := 'test';
};
            """.strip()]
        )

        await self.con.execute("""
            DROP TYPE Foo;
        """)

    async def test_edgeql_ddl_annotation_15(self):
        await self.con.execute("""
            CREATE ABSTRACT INHERITABLE ANNOTATION anno;
            CREATE TYPE Foo {
                CREATE PROPERTY prop -> str {
                    CREATE ANNOTATION anno := "parent";
                };
            };
            CREATE TYPE Bar EXTENDING Foo {
                ALTER PROPERTY prop {
                    ALTER ANNOTATION anno := "child";
                }
            };
        """)

        qry = '''
            WITH MODULE schema
            SELECT Property {
                obj := .source.name,
                annotations: {name, @value, @owned}
                ORDER BY .name
            }
            FILTER
                .name = 'prop'
            ORDER BY
                (.obj, .name);
        '''

        await self.assert_query_result(
            qry,
            [
                {
                    "annotations": [
                        {"@value": "child", "@owned": True,
                         "name": "default::anno"}
                    ],
                    "obj": "default::Bar"
                },
                {
                    "annotations": [
                        {"@value": "parent", "@owned": True,
                         "name": "default::anno"}
                    ],
                    "obj": "default::Foo"
                }
            ]
        )

        await self.con.execute("""
            ALTER TYPE Bar {
                ALTER PROPERTY prop {
                    ALTER ANNOTATION anno DROP OWNED;
                }
            };
        """)

        await self.assert_query_result(
            qry,
            [
                {
                    "annotations": [
                        {"@value": "parent", "@owned": False,
                         "name": "default::anno"}
                    ],
                    "obj": "default::Bar"
                },
                {
                    "annotations": [
                        {"@value": "parent", "@owned": True,
                         "name": "default::anno"}
                    ],
                    "obj": "default::Foo"
                }
            ]
        )

    async def test_edgeql_ddl_annotation_16(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION attr1;
        """)
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"annotation values must be 'std::str', "
                r"got scalar type 'std::int64'"):
            await self.con.execute("""
                CREATE SCALAR TYPE TestAttrType1 EXTENDING std::str {
                    CREATE ANNOTATION attr1 := 10;
                };
            """)

    async def test_edgeql_ddl_annotation_17(self):
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION attr1;
            CREATE SCALAR TYPE TestAttrType1 EXTENDING std::str {
                CREATE ANNOTATION attr1 := '10';
            };
        """)
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"annotation values must be 'std::str', "
                r"got scalar type 'std::int64'"):
            await self.con.execute("""
                ALTER SCALAR TYPE TestAttrType1 {
                    ALTER ANNOTATION attr1 := 10;
                };
            """)

    async def test_edgeql_ddl_annotation_18(self):
        # An annotation on an annotation!
        await self.con.execute("""
            CREATE ABSTRACT ANNOTATION ann {
                CREATE ANNOTATION description := "foo";
            };
        """)

        qry = '''
            WITH MODULE schema
            SELECT Annotation {
                annotations: {name, @value}
            }
            FILTER .name = 'default::ann'
        '''

        await self.assert_query_result(
            qry,
            [{"annotations": [{"@value": "foo", "name": "std::description"}]}]
        )

        await self.con.execute("""
            ALTER ABSTRACT ANNOTATION ann {
                ALTER ANNOTATION description := "bar";
            };
        """)

        await self.assert_query_result(
            qry,
            [{"annotations": [{"@value": "bar", "name": "std::description"}]}]
        )

        await self.con.execute("""
            ALTER ABSTRACT ANNOTATION ann {
                DROP ANNOTATION description;
            };
        """)

        await self.assert_query_result(
            qry,
            [{"annotations": []}]
        )

    async def test_edgeql_ddl_anytype_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE ABSTRACT LINK test_object_link_prop {
                    CREATE PROPERTY link_prop1 -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid link target"):

            await self.con.execute("""
                CREATE TYPE AnyObject2 {
                    CREATE LINK a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE AnyObject3 {
                    CREATE PROPERTY a -> anytype;
                };
            """)

    async def test_edgeql_ddl_anytype_04(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE AnyObject4 {
                    CREATE PROPERTY a -> anyscalar;
                };
            """)

    async def test_edgeql_ddl_anytype_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid property type"):

            await self.con.execute("""
                CREATE TYPE AnyObject5 {
                    CREATE PROPERTY a -> anyint;
                };
            """)

    async def test_edgeql_ddl_anytype_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"'anytype' cannot be a parent type"):

            await self.con.execute("""
                CREATE TYPE AnyObject6 EXTENDING anytype {
                    CREATE REQUIRED LINK a -> AnyObject6;
                    CREATE REQUIRED PROPERTY b -> str;
                };
            """)

    async def test_edgeql_ddl_extending_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError,
                r"Could not find consistent ancestor order for "
                r"object type 'default::Merged1'"):

            await self.con.execute(r"""
                CREATE TYPE ExtA1;
                CREATE TYPE ExtB1;
                # create two types with incompatible linearized bases
                CREATE TYPE ExtC1 EXTENDING ExtA1, ExtB1;
                CREATE TYPE ExtD1 EXTENDING ExtB1, ExtA1;
                # extending from both of these incompatible types
                CREATE TYPE Merged1 EXTENDING ExtC1, ExtD1;
            """)

    async def test_edgeql_ddl_extending_02(self):
        await self.con.execute(r"""
            CREATE TYPE ExtA2;
            # Create two types with a different position of Object
            # in the bases. This doesn't impact the linearized
            # bases because Object is already implicitly included
            # as the first element of the base types.
            CREATE TYPE ExtC2 EXTENDING ExtA2, Object;
            CREATE TYPE ExtD2 EXTENDING Object, ExtA2;
            # extending from both of these types
            CREATE TYPE Merged2 EXTENDING ExtC2, ExtD2;
        """)

    async def test_edgeql_ddl_extending_03(self):
        # Check that ancestors are recomputed properly on rebase.
        await self.con.execute(r"""
            CREATE TYPE ExtA3;
            CREATE TYPE ExtB3 EXTENDING ExtA3;
            CREATE TYPE ExtC3 EXTENDING ExtB3;
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    ancestors: {
                        name
                    } ORDER BY @index
                }
                FILTER .name = 'default::ExtC3'
            """,
            [{
                'ancestors': [{
                    'name': 'default::ExtB3',
                }, {
                    'name': 'default::ExtA3',
                }, {
                    'name': 'std::Object',
                }, {
                    'name': 'std::BaseObject',
                }],
            }]
        )

        await self.con.execute(r"""
            ALTER TYPE ExtB3 DROP EXTENDING ExtA3;
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    ancestors: {
                        name
                    } ORDER BY @index
                }
                FILTER .name = 'default::ExtC3'
            """,
            [{
                'ancestors': [{
                    'name': 'default::ExtB3',
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
            CREATE TYPE ExtA4 {
                CREATE PROPERTY a -> int64;
            };

            CREATE ABSTRACT INHERITABLE ANNOTATION a_anno;

            CREATE TYPE ExtB4 {
                CREATE PROPERTY a -> int64 {
                    CREATE ANNOTATION a_anno := 'anno';
                };

                CREATE PROPERTY b -> str;
            };

            CREATE TYPE Ext4Child EXTENDING ExtA4;
            CREATE TYPE Ext4GrandChild EXTENDING Ext4Child;
            CREATE TYPE Ext4GrandGrandChild
                EXTENDING Ext4GrandChild;
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    SELECT schema::ObjectType
                    FILTER .name = 'default::Ext4Child'
                ).properties.name;
            """,
            {'id', 'a'}
        )

        await self.con.execute(r"""
            ALTER TYPE Ext4Child EXTENDING ExtB4;
        """)

        for name in {'Ext4Child', 'Ext4GrandChild', 'Ext4GrandGrandChild'}:
            await self.assert_query_result(
                f"""
                    SELECT (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::{name}'
                    ).properties.name;
                """,
                {'id', 'a', 'b'}
            )

        await self.assert_query_result(
            r"""
                WITH
                    ggc := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::Ext4GrandGrandChild'
                    )
                SELECT
                    (SELECT ggc.properties FILTER .name = 'a')
                        .annotations@value;
            """,
            {'anno'}
        )

        await self.con.execute(r"""
            ALTER TYPE Ext4Child DROP EXTENDING ExtB4;
        """)

        for name in {'Ext4Child', 'Ext4GrandChild', 'Ext4GrandGrandChild'}:
            await self.assert_query_result(
                f"""
                    SELECT (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::{name}'
                    ).properties.name;
                """,
                {'id', 'a'}
            )

        await self.assert_query_result(
            r"""
                WITH
                    ggc := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::Ext4GrandGrandChild'
                    )
                SELECT
                    (SELECT ggc.properties FILTER .name = 'a')
                        .annotations@value;
            """,
            []
        )

    @test.xfail('''
        Default value ought to get reset back to non-existent, since it
        was inherited? (Or actually maybe not, since the prop is owned
        by then?)
    ''')
    async def test_edgeql_ddl_extending_05(self):
        # Check that field alters are propagated.
        await self.con.execute(r"""
            CREATE TYPE ExtA5 {
                CREATE PROPERTY a -> int64 {
                    SET default := 1;
                };
            };

            CREATE TYPE ExtB5 {
                CREATE PROPERTY a -> int64 {
                    SET default := 2;
                };
            };

            CREATE TYPE ExtC5 EXTENDING ExtB5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'2'}
        )

        await self.con.execute(r"""
            ALTER TYPE ExtC5 EXTENDING ExtA5 FIRST;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'1'}
        )

        await self.con.execute(r"""
            ALTER TYPE ExtC5 DROP EXTENDING ExtA5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            {'2'}
        )

        await self.con.execute(r"""
            ALTER TYPE ExtC5 ALTER PROPERTY a SET REQUIRED;
            ALTER TYPE ExtC5 DROP EXTENDING ExtB5;
        """)

        await self.assert_query_result(
            r"""
                WITH
                    C5 := (
                        SELECT schema::ObjectType
                        FILTER .name = 'default::ExtC5'
                    )
                SELECT
                    (SELECT C5.properties FILTER .name = 'a')
                        .default;
            """,
            []
        )

    async def test_edgeql_ddl_extending_06(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"'std::FreeObject' cannot be a parent type",
        ):
            await self.con.execute("""
                CREATE TYPE SomeObject6 EXTENDING FreeObject;
            """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            r"'std::FreeObject' cannot be a parent type",
        ):
            await self.con.execute("""
                CREATE TYPE SomeObject6;
                ALTER TYPE SomeObject6 EXTENDING FreeObject;
            """)

    async def test_edgeql_ddl_modules_01(self):
        try:
            await self.con.execute(r"""
                CREATE MODULE test_other;

                CREATE TYPE ModuleTest01 {
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

            CREATE TYPE Priority EXTENDING test_other::Named;

            CREATE TYPE Status
                EXTENDING test_other::UniquelyNamed;

            INSERT Priority {name := 'one'};
            INSERT Priority {name := 'two'};
            INSERT Status {name := 'open'};
            INSERT Status {name := 'closed'};
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
            DROP TYPE Status;
            DROP TYPE Priority;
            DROP TYPE test_other::UniquelyNamed;
            DROP TYPE test_other::Named;
            DROP MODULE test_other;
        """)

    @test.xerror('''
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
                        type Status extending test_other::UniquelyNamed;
                    };
                    POPULATE MIGRATION;
                    COMMIT MIGRATION;
                """)

            await self.con.execute("""
                DROP TYPE Status;
            """)
        finally:
            await self.con.execute("""
                DROP TYPE test_other::UniquelyNamed;
                DROP TYPE test_other::Named;
                DROP MODULE test_other;
            """)

    async def test_edgeql_ddl_modules_04(self):
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

            CREATE ABSTRACT ANNOTATION whatever;

            CREATE TYPE test_other::Foo;
            CREATE TYPE test_other::Bar {
                CREATE LINK foo -> test_other::Foo;
                CREATE ANNOTATION whatever := "huh";
            };
            ALTER TYPE test_other::Foo {
                CREATE LINK bar -> test_other::Bar;
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            "cannot drop module 'test_other' because it is not empty",
        ):
            await self.con.execute(r"""
                DROP MODULE test_other;
            """)

    async def test_edgeql_ddl_extension_package_01(self):
        await self.con.execute(r"""
            CREATE EXTENSION PACKAGE foo_01 VERSION '1.0' {
                CREATE MODULE foo_ext;;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::ExtensionPackage {
                    name,
                    script,
                    ver := (.version.major, .version.minor),
                }
                FILTER .name LIKE 'foo_%'
                ORDER BY .name
            """,
            [{
                'name': 'foo_01',
                'script': (
                    "CREATE MODULE foo_ext;;"
                ),
                'ver': [1, 0],
            }]
        )

        await self.con.execute(r"""
            CREATE EXTENSION PACKAGE foo_01 VERSION '2.0-beta.1' {
                SELECT 1/0;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::ExtensionPackage {
                    name,
                    script,
                    ver := (.version.major, .version.minor, .version.stage),
                }
                FILTER .name LIKE 'foo_%'
                ORDER BY .name THEN .version
            """,
            [{
                'name': 'foo_01',
                'script': (
                    "CREATE MODULE foo_ext;;"
                ),
                'ver': [1, 0, 'final'],
            }, {
                'name': 'foo_01',
                'script': (
                    "SELECT 1/0;"
                ),
                'ver': [2, 0, 'beta'],
            }]
        )

        await self.con.execute(r"""
            DROP EXTENSION PACKAGE foo_01 VERSION '1.0';
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::ExtensionPackage {
                    name,
                    script,
                }
                FILTER .name LIKE 'foo_%'
                ORDER BY .name
            """,
            [{
                'name': 'foo_01',
                'script': (
                    "SELECT 1/0;"
                ),
            }]
        )

    async def test_edgeql_ddl_extension_01(self):
        await self.con.execute(r"""
            CREATE EXTENSION PACKAGE MyExtension VERSION '1.0';
            CREATE EXTENSION PACKAGE MyExtension VERSION '2.0';
        """)

        await self.con.execute(r"""
            CREATE EXTENSION MyExtension;
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Extension {
                    name,
                    package: {
                        ver := (.version.major, .version.minor)
                    }
                }
                FILTER .name = 'MyExtension'
            """,
            [{
                'name': 'MyExtension',
                'package': {
                    'ver': [2, 0],
                }
            }]
        )

        await self.con.execute(r"""
            DROP EXTENSION MyExtension;
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Extension {
                    name,
                    package: {
                        ver := (.version.major, .version.minor)
                    }
                }
                FILTER .name = 'MyExtension'
            """,
            [],
        )

        await self.con.execute(r"""
            CREATE EXTENSION MyExtension VERSION '1.0';
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Extension {
                    name,
                    package: {
                        ver := (.version.major, .version.minor)
                    }
                }
                FILTER .name = 'MyExtension'
            """,
            [{
                'name': 'MyExtension',
                'package': {
                    'ver': [1, 0],
                }
            }]
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            "extension 'MyExtension' already exists",
        ):
            await self.con.execute(r"""
                CREATE EXTENSION MyExtension VERSION '2.0';
            """)

        await self.con.execute(r"""
            DROP EXTENSION MyExtension;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            "cannot create extension 'MyExtension': extension"
            " package 'MyExtension' version '3.0' does not exist",
        ):
            await self.con.execute(r"""
                CREATE EXTENSION MyExtension VERSION '3.0';
            """)

    async def test_edgeql_ddl_role_01(self):
        if not self.has_create_role:
            self.skipTest("create role is not supported by the backend")

        await self.con.execute(r"""
            CREATE ROLE foo_01;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    superuser,
                    password,
                } FILTER .name = 'foo_01'
            """,
            [{
                'name': 'foo_01',
                'superuser': False,
                'password': None,
            }]
        )

    async def test_edgeql_ddl_role_02(self):
        if not self.has_create_role:
            self.skipTest("create role is not supported by the backend")

        await self.con.execute(r"""
            CREATE SUPERUSER ROLE foo2 {
                SET password := 'secret';
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    superuser,
                } FILTER .name = 'foo2'
            """,
            [{
                'name': 'foo2',
                'superuser': True,
            }]
        )

        role = await self.con.query_single('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNotNone(role.password)

        await self.con.execute(r"""
            ALTER ROLE foo2 {
                SET password := {}
            };
        """)

        role = await self.con.query_single('''
            SELECT sys::Role { password }
            FILTER .name = 'foo2'
        ''')

        self.assertIsNone(role.password)

    async def test_edgeql_ddl_role_03(self):
        if not self.has_create_role:
            self.skipTest("create role is not supported by the backend")

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
                    superuser,
                    password,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo4'
            """,
            [{
                'name': 'foo4',
                'superuser': False,
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

    async def test_edgeql_ddl_role_04(self):
        if not self.has_create_role:
            self.skipTest("create role is not supported by the backend")

        await self.con.execute(r"""
            CREATE SUPERUSER ROLE foo5 IF NOT EXISTS {
                SET password := 'secret';
            };
            CREATE SUPERUSER ROLE foo5 IF NOT EXISTS {
                SET password := 'secret';
            };
            CREATE SUPERUSER ROLE foo5 IF NOT EXISTS {
                SET password := 'secret';
            };
            CREATE ROLE foo6 EXTENDING foo5 IF NOT EXISTS;
            CREATE ROLE foo6 EXTENDING foo5 IF NOT EXISTS;
            CREATE ROLE foo6 EXTENDING foo5 IF NOT EXISTS;
        """)

        await self.assert_query_result(
            r"""
                SELECT sys::Role {
                    name,
                    superuser,
                    password,
                    member_of: {
                        name
                    },
                } FILTER .name = 'foo6'
            """,
            [{
                'name': 'foo6',
                'superuser': False,
                'password': None,
                'member_of': [{
                    'name': 'foo5'
                }]
            }]
        )

    async def test_edgeql_ddl_role_05(self):
        if self.has_create_role:
            self.skipTest("create role is supported by the backend")
        con = await self.connect()
        try:
            await con.execute("""
                ALTER ROLE edgedb SET password := 'test_role_05'
            """)
            if self.has_create_database:
                await con.execute("""CREATE DATABASE test_role_05""")
        finally:
            await con.aclose()
        args = {'password': "test_role_05"}
        if self.has_create_database:
            args['database'] = "test_role_05"
        con = await self.connect(**args)
        try:
            await con.execute("""
                ALTER ROLE edgedb SET password := 'test'
            """)
        finally:
            await con.aclose()
        con = await self.connect()
        try:
            if self.has_create_database:
                await tb.drop_db(con, 'test_role_05')
        finally:
            await con.aclose()

    async def test_edgeql_ddl_describe_roles(self):
        if not self.has_create_role:
            self.skipTest("create role is not supported by the backend")

        await self.con.execute("""
            CREATE SUPERUSER ROLE base1;
            CREATE SUPERUSER ROLE `base 2`;
            CREATE SUPERUSER ROLE child1 EXTENDING base1;
            CREATE SUPERUSER ROLE child2 EXTENDING `base 2`;
            CREATE SUPERUSER ROLE child3 EXTENDING base1, child2 {
                SET password := 'test'
            };
        """)
        roles = next(iter(await self.con.query("DESCRIBE ROLES")))
        base1 = roles.index('CREATE SUPERUSER ROLE `base1`;')
        base2 = roles.index('CREATE SUPERUSER ROLE `base 2`;')
        child1 = roles.index('CREATE SUPERUSER ROLE `child1`')
        child2 = roles.index('CREATE SUPERUSER ROLE `child2`')
        child3 = roles.index('CREATE SUPERUSER ROLE `child3`')
        self.assertGreater(child1, base1, roles)
        self.assertGreater(child2, base2, roles)
        self.assertGreater(child3, child2, roles)
        self.assertGreater(child3, base1, roles)
        self.assertIn("SET password_hash := 'SCRAM-SHA-256$4096:", roles)

    async def test_edgeql_ddl_describe_schema(self):
        # This is ensuring that describing std does not cause errors.
        # The test validates only a small sample of entries, though.

        result = await self.con.query_single("""
            DESCRIBE MODULE std
        """)
        # This is essentially a syntax test from this point on.
        re_filter = re.compile(r'[\s]+|(#.*?(\n|$))|(,(?=\s*[})]))')
        result_stripped = re_filter.sub('', result).lower()

        for expected in [
                '''
                CREATE SCALAR TYPE std::float32 EXTENDING std::anyfloat;
                ''',
                '''
                CREATE FUNCTION
                std::str_lower(s: std::str) -> std::str
                {
                    SET volatility := 'Immutable';
                    CREATE ANNOTATION std::description :=
                        'Return a lowercase copy of the input *string*.';
                    USING SQL FUNCTION 'lower';
                };
                ''',
                '''
                CREATE INFIX OPERATOR
                std::`AND`(a: std::bool, b: std::bool) -> std::bool {
                    SET volatility := 'Immutable';
                    CREATE ANNOTATION std::description :=
                        'Logical conjunction.';
                    USING SQL EXPRESSION;
                };
                ''',
                '''
                CREATE ABSTRACT INFIX OPERATOR
                std::`>=`(l: anytype, r: anytype) -> std::bool;
                ''',
                '''
                CREATE CAST FROM std::str TO std::bool {
                    SET volatility := 'Immutable';
                    USING SQL FUNCTION 'edgedb.str_to_bool';
                };
                ''',
                '''
                CREATE CAST FROM std::int64 TO std::int16 {
                    SET volatility := 'Immutable';
                    USING SQL CAST;
                    ALLOW ASSIGNMENT;
                };
                ''']:
            expected_stripped = re_filter.sub('', expected).lower()
            self.assertTrue(
                expected_stripped in result_stripped,
                f'`DESCRIBE MODULE std` is missing the following: "{expected}"'
            )

    async def test_edgeql_ddl_rename_01(self):
        await self.con.execute(r"""
            CREATE TYPE RenameObj01 {
                CREATE PROPERTY name -> str;
            };

            INSERT RenameObj01 {name := 'rename 01'};

            ALTER TYPE RenameObj01 {
                RENAME TO NewNameObj01;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT NewNameObj01.name;
            ''',
            ['rename 01']
        )

    async def test_edgeql_ddl_rename_02(self):
        await self.con.execute(r"""
            CREATE TYPE RenameObj02 {
                CREATE PROPERTY name -> str;
            };

            INSERT RenameObj02 {name := 'rename 02'};

            ALTER TYPE RenameObj02 {
                ALTER PROPERTY name {
                    RENAME TO new_name_02;
                };
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT RenameObj02.new_name_02;
            ''',
            ['rename 02']
        )

    async def test_edgeql_ddl_rename_03(self):
        await self.con.execute(r"""

            CREATE TYPE RenameObj03 {
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
                SELECT RenameObj03.new_name_03;
            ''',
            ['rename 03']
        )

    async def test_edgeql_ddl_rename_04(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK rename_link_04 {
                CREATE PROPERTY rename_prop_04 -> std::int64;
            };

            CREATE TYPE LinkedObj04;
            CREATE TYPE RenameObj04 {
                CREATE MULTI LINK rename_link_04 EXTENDING rename_link_04
                    -> LinkedObj04;
            };

            INSERT LinkedObj04;
            INSERT RenameObj04 {
                rename_link_04 := LinkedObj04 {@rename_prop_04 := 123}
            };

            ALTER ABSTRACT LINK rename_link_04 {
                ALTER PROPERTY rename_prop_04 {
                    RENAME TO new_prop_04;
                };
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT RenameObj04.rename_link_04@new_prop_04;
            ''',
            [123]
        )

    async def test_edgeql_ddl_rename_05(self):
        await self.con.execute("""
            CREATE TYPE GrandParent01 {
                CREATE PROPERTY foo -> int64;
            };

            CREATE TYPE Parent01 EXTENDING GrandParent01;
            CREATE TYPE Parent02 EXTENDING GrandParent01;

            CREATE TYPE Child EXTENDING Parent01, Parent02;

            ALTER TYPE GrandParent01 {
                ALTER PROPERTY foo RENAME TO renamed;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT Child.renamed;
            ''',
            []
        )

    async def test_edgeql_ddl_rename_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "cannot rename inherited property 'foo'"):
            await self.con.execute("""
                CREATE TYPE Parent01 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE Parent02 {
                    CREATE PROPERTY foo -> int64;
                };

                CREATE TYPE Child
                    EXTENDING Parent01, Parent02;

                ALTER TYPE Parent02 {
                    ALTER PROPERTY foo RENAME TO renamed;
                };
            """)

    async def test_edgeql_ddl_rename_07(self):
        await self.con.execute("""
            CREATE TYPE Foo;

            CREATE TYPE Bar {
                CREATE MULTI LINK foo -> Foo {
                    SET default := (SELECT Foo);
                }
            };

            ALTER TYPE Foo RENAME TO FooRenamed;
        """)

    async def test_edgeql_ddl_rename_abs_ptr_01(self):
        await self.con.execute("""
            CREATE ABSTRACT LINK abs_link {
                CREATE PROPERTY prop -> std::int64;
            };

            CREATE TYPE LinkedObj;
            CREATE TYPE RenameObj {
                CREATE MULTI LINK link EXTENDING abs_link
                    -> LinkedObj;
            };

            INSERT LinkedObj;
            INSERT RenameObj {
                link := LinkedObj {@prop := 123}
            };
        """)

        await self.con.execute("""
            ALTER ABSTRACT LINK abs_link
            RENAME TO new_abs_link;
        """)

        await self.assert_query_result(
            r'''
                SELECT RenameObj.link@prop;
            ''',
            [123]
        )

        # Check we can create a new type that uses it
        await self.con.execute("""
            CREATE TYPE RenameObj2 {
                CREATE MULTI LINK link EXTENDING new_abs_link
                    -> LinkedObj;
            };
        """)

        # Check we can create a new link with the same name
        await self.con.execute("""
            CREATE ABSTRACT LINK abs_link {
                CREATE PROPERTY prop -> std::int64;
            };
        """)

        await self.con.execute("""
            CREATE MODULE foo;

            ALTER ABSTRACT LINK new_abs_link
            RENAME TO foo::new_abs_link2;
        """)

        await self.con.execute("""
            ALTER TYPE RenameObj DROP LINK link;
            ALTER TYPE RenameObj2 DROP LINK link;
            DROP ABSTRACT LINK foo::new_abs_link2;
        """)

    async def test_edgeql_ddl_rename_abs_ptr_02(self):
        await self.con.execute("""
            CREATE ABSTRACT PROPERTY abs_prop {
                CREATE ANNOTATION title := "lol";
            };

            CREATE TYPE RenameObj {
                CREATE PROPERTY prop EXTENDING abs_prop -> str;
            };
        """)

        await self.con.execute("""
            ALTER ABSTRACT PROPERTY abs_prop
            RENAME TO new_abs_prop;
        """)

        # Check we can create a new type that uses it
        await self.con.execute("""
            CREATE TYPE RenameObj2 {
                CREATE PROPERTY prop EXTENDING new_abs_prop -> str;
            };
        """)

        # Check we can create a new prop with the same name
        await self.con.execute("""
            CREATE ABSTRACT PROPERTY abs_prop {
                CREATE ANNOTATION title := "lol";
            };
        """)

        await self.con.execute("""
            CREATE MODULE foo;

            ALTER ABSTRACT PROPERTY new_abs_prop
            RENAME TO foo::new_abs_prop2;
        """)

        await self.con.execute("""
            ALTER TYPE RenameObj DROP PROPERTY prop;
            ALTER TYPE RenameObj2 DROP PROPERTY prop;
            DROP ABSTRACT PROPERTY foo::new_abs_prop2;
        """)

    async def test_edgeql_ddl_rename_annotated_01(self):
        await self.con.execute("""
            CREATE TYPE RenameObj {
                CREATE PROPERTY prop -> str {
                   CREATE ANNOTATION title := "lol";
                }
            };
        """)

        await self.con.execute("""
            ALTER TYPE RenameObj {
                ALTER PROPERTY prop RENAME TO prop2;
            };
        """)

    async def test_edgeql_ddl_delete_abs_link_01(self):
        # test deleting a trivial abstract link
        await self.con.execute("""
            CREATE ABSTRACT LINK abs_link;
        """)

        await self.con.execute("""
            DROP ABSTRACT LINK abs_link;
        """)

    async def test_edgeql_ddl_alias_01(self):
        # Issue #1184
        await self.con.execute(r"""

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

            INSERT Award { user := (INSERT User { name := 'Corvo' }) };
        """)

        await self.assert_query_result(
            r'''
                SELECT Alias1 {
                    user2: {
                        name2
                    }
                }
            ''',
            [{
                'user2': {
                    'name2': 'Corvo!',
                },
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT Alias2 {
                    user2: {
                        name2
                    }
                }
            ''',
            [{
                'user2': {
                    'name2': 'Corvo!',
                },
            }],
        )

    async def test_edgeql_ddl_alias_02(self):
        # Issue #1184
        await self.con.execute(r"""

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

            INSERT User { name := 'Corvo' };
            INSERT Award { name := 'Rune' };
        """)

        await self.assert_query_result(
            r'''
                SELECT Alias1 {
                    a_user: {
                        name
                    }
                }
            ''',
            [{
                'a_user': {
                    'name': 'Corvo',
                },
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT Alias2 {
                    a_user: {
                        name
                    }
                }
            ''',
            [{
                'a_user': {
                    'name': 'Corvo',
                },
            }],
        )

    async def test_edgeql_ddl_alias_03(self):
        await self.con.execute(r"""
            CREATE ALIAS RenameAlias03 := (
                SELECT BaseObject {
                    alias_computable := 'rename alias 03'
                }
            );

            ALTER ALIAS RenameAlias03 {
                RENAME TO NewAlias03;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT NewAlias03.alias_computable LIMIT 1;
            ''',
            ['rename alias 03']
        )

        await self.con.execute(r"""
            CREATE MODULE foo;

            ALTER ALIAS NewAlias03 {
                RENAME TO foo::NewAlias03;
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT foo::NewAlias03.alias_computable LIMIT 1;
            ''',
            ['rename alias 03']
        )

        await self.con.execute(r"""
            DROP ALIAS foo::NewAlias03;
        """)

    async def test_edgeql_ddl_alias_04(self):
        await self.con.execute(r"""
            CREATE ALIAS DupAlias04_1 := BaseObject {
                foo := 'hello world 04'
            };

            # create an identical alias with a different name
            CREATE ALIAS DupAlias04_2 := BaseObject {
                foo := 'hello world 04'
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT DupAlias04_1.foo LIMIT 1;
            ''',
            ['hello world 04']
        )

        await self.assert_query_result(
            r'''
                SELECT DupAlias04_2.foo LIMIT 1;
            ''',
            ['hello world 04']
        )

    async def test_edgeql_ddl_alias_05(self):
        await self.con.execute(r"""

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

    async def test_edgeql_ddl_alias_06(self):
        # Issue #1184
        await self.con.execute(r"""

            CREATE TYPE BaseType06 {
                CREATE PROPERTY name -> str;
            };

            INSERT BaseType06 {
                name := 'bt06',
            };

            INSERT BaseType06 {
                name := 'bt06_1',
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

        await self.assert_query_result(
            r'''
                SELECT BT06Alias1 {name, a} FILTER .name = 'bt06';
            ''',
            [{
                'name': 'bt06',
                'a': 'bt06_a',
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT BT06Alias2 {name, a, b} FILTER .name = 'bt06';
            ''',
            [{
                'name': 'bt06',
                'a': 'bt06_a',
                'b': 'bt06_a_b',
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT BT06Alias3 {
                    name,
                    b: {name, a} ORDER BY .name
                }
                FILTER .name = 'bt06';
            ''',
            [{
                'name': 'bt06',
                'b': [{
                    'name': 'bt06',
                    'a': 'bt06_a',
                }, {
                    'name': 'bt06_1',
                    'a': 'bt06_1_a',
                }],
            }],
        )

    async def test_edgeql_ddl_alias_07(self):
        # Issue #1187
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "illegal self-reference in definition of "
                "'default::IllegalAlias07'"):

            await self.con.execute(r"""
                CREATE ALIAS IllegalAlias07 := Object {a := IllegalAlias07};
            """)

    async def test_edgeql_ddl_alias_08(self):
        # Issue #1184
        await self.con.execute(r"""

            CREATE TYPE BaseType08 {
                CREATE PROPERTY name -> str;
            };

            INSERT BaseType08 {
                name := 'bt08',
            };

            CREATE ALIAS BT08Alias1 := BaseType08 {
                a := .name ++ '_a'
            };

            CREATE ALIAS BT08Alias2 := BT08Alias1 {
                b := .a ++ '_b'
            };

            # drop the freshly created alias
            DROP ALIAS BT08Alias2;

            # re-create the alias that was just dropped
            CREATE ALIAS BT08Alias2 := BT08Alias1 {
                b := .a ++ '_bb'
            };
        """)

        await self.assert_query_result(
            r'''
                SELECT BT08Alias1 {name, a} FILTER .name = 'bt08';
            ''',
            [{
                'name': 'bt08',
                'a': 'bt08_a',
            }],
        )

        await self.assert_query_result(
            r'''
                SELECT BT08Alias2 {name, a, b} FILTER .name = 'bt08';
            ''',
            [{
                'name': 'bt08',
                'a': 'bt08_a',
                'b': 'bt08_a_bb',
            }],
        )

    async def test_edgeql_ddl_alias_09(self):
        await self.con.execute(r"""
            CREATE ALIAS CreateAlias09 := (
                SELECT BaseObject {
                    alias_computable := 'rename alias 03'
                }
            );
        """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidLinkTargetError,
            "invalid link type: 'default::CreateAlias09' is an"
            " expression alias, not a proper object type",
        ):
            await self.con.execute(r"""
                CREATE TYPE AliasType09 {
                    CREATE OPTIONAL SINGLE LINK a -> CreateAlias09;
                }
            """)

    async def test_edgeql_ddl_alias_10(self):
        await self.con.execute(r"""
            create type Foo;
            create type Bar;
            create alias X := Foo { bar := Bar { z := 1 } };
            alter alias X using (Bar);
        """)

    async def test_edgeql_ddl_inheritance_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE InhTest01 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE InhTest01_child EXTENDING InhTest01;
        """)

        await self.con.execute("""
            ALTER TYPE InhTest01 {
                DROP PROPERTY testp;
            }
        """)

    async def test_edgeql_ddl_inheritance_alter_02(self):
        await self.con.execute(r"""
            CREATE TYPE InhTest01 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE InhTest01_child EXTENDING InhTest01;
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                "cannot drop inherited property 'testp'"):

            await self.con.execute("""
                ALTER TYPE InhTest01_child {
                    DROP PROPERTY testp;
                }
            """)

    async def test_edgeql_ddl_inheritance_alter_03(self):
        await self.con.execute(r"""
            CREATE TYPE Owner;

            CREATE TYPE Stuff1 {
                # same link name, but NOT related via explicit inheritance
                CREATE LINK owner -> Owner
            };

            CREATE TYPE Stuff2 {
                # same link name, but NOT related via explicit inheritance
                CREATE LINK owner -> Owner
            };
        """)

        await self.assert_query_result("""
            SELECT Owner.<owner;
        """, [])

    async def test_edgeql_ddl_inheritance_alter_04(self):
        await self.con.execute(r"""
            CREATE TYPE InhTest04 {
                CREATE PROPERTY testp -> int64;
            };

            CREATE TYPE InhTest04_child EXTENDING InhTest04;
        """)

        await self.con.execute(r"""
            ALTER TYPE InhTest04_child {
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
                FILTER .name = 'default::InhTest04_child';
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
            CREATE ABSTRACT TYPE BaseTypeCon01;
            CREATE TYPE TypeCon01 EXTENDING BaseTypeCon01;
            ALTER TYPE BaseTypeCon01
                CREATE SINGLE PROPERTY name -> std::str;
            # make sure that we can create a constraint in the base
            # type now
            ALTER TYPE BaseTypeCon01
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
            FILTER .name LIKE 'default::%TypeCon01'
            ORDER BY .name;
        """, [
            {
                'name': 'default::BaseTypeCon01',
                'properties': [{
                    'name': 'name',
                    'constraints': [{
                        'name': 'std::exclusive',
                        'delegated': True,
                    }],
                }]
            },
            {
                'name': 'default::TypeCon01',
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
            CREATE TYPE TypeCon03 {
                CREATE PROPERTY name -> str {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT TypeCon03 {name := 'OK'};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon03;
                """)

    @test.xerror('''
        Reports an schema error. Maybe that is exactly what we want?
    ''')
    async def test_edgeql_ddl_constraint_04(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE TypeCon04 {
                CREATE MULTI PROPERTY name -> str {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT TypeCon04 {name := 'OK'};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon04 {name := {}};
                """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid name'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon04;
                """)

    async def test_edgeql_ddl_constraint_05(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE Child05;
            CREATE TYPE TypeCon05 {
                CREATE LINK child -> Child05 {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT Child05;
            INSERT TypeCon05 {child := (SELECT Child05 LIMIT 1)};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid child'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon05;
                """)

    @test.xerror('''
        Reports an schema error. Maybe that is exactly what we want?
    ''')
    async def test_edgeql_ddl_constraint_06(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE Child06;
            CREATE TYPE TypeCon06 {
                CREATE MULTI LINK children -> Child06 {
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__)
                }
            };
        """)

        await self.con.execute("""
            INSERT Child06;
            INSERT TypeCon06 {children := Child06};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid children'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon06;
                """)

    async def test_edgeql_ddl_constraint_07(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE Child07;
            CREATE TYPE TypeCon07 {
                CREATE LINK child -> Child07 {
                    CREATE PROPERTY index -> int64;
                    # emulating "required"
                    CREATE CONSTRAINT expression ON (EXISTS __subject__@index)
                }
            };
        """)

        await self.con.execute("""
            INSERT Child07;
            INSERT TypeCon07 {
                child := (SELECT Child07 LIMIT 1){@index := 0}
            };
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid child'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT TypeCon07 {
                        child := (SELECT Child07 LIMIT 1)
                    };
                """)

    async def test_edgeql_ddl_constraint_08(self):
        # Test non-delegated object constraints on abstract types
        await self.con.execute(r"""
            CREATE TYPE Base {
                CREATE PROPERTY x -> str {
                    CREATE CONSTRAINT exclusive;
                }
            };
            CREATE TYPE Foo EXTENDING Base;
            CREATE TYPE Bar EXTENDING Base;

            INSERT Foo { x := "a" };
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'violates exclusivity constraint'):
            await self.con.execute(r"""
                INSERT Foo { x := "a" };
            """)

    async def test_edgeql_ddl_constraint_09(self):
        await self.con.execute(r"""

            CREATE ABSTRACT TYPE Text {
                CREATE REQUIRED SINGLE PROPERTY body -> str {
                    CREATE CONSTRAINT max_len_value(10000);
                };
            };
            CREATE TYPE Comment EXTENDING Text;
        """)

        await self.con.execute("""
            ALTER TYPE Text
                ALTER PROPERTY body
                    DROP CONSTRAINT max_len_value(10000);
        """)

    async def test_edgeql_ddl_constraint_10(self):
        await self.con.execute(r"""

            CREATE ABSTRACT TYPE Text {
                CREATE REQUIRED SINGLE PROPERTY body -> str {
                    CREATE CONSTRAINT max_len_value(10000);
                };
            };
            CREATE TYPE Comment EXTENDING Text;
        """)

        await self.con.execute("""
            ALTER TYPE Text
                DROP PROPERTY body;
        """)

    async def test_edgeql_ddl_constraint_11(self):
        await self.con.execute(r"""

            CREATE ABSTRACT TYPE Text {
                CREATE REQUIRED SINGLE PROPERTY body -> str {
                    CREATE CONSTRAINT max_value(10000)
                        ON (len(__subject__));
                };
            };
            CREATE TYPE Comment EXTENDING Text;
            CREATE TYPE Troll EXTENDING Comment;
        """)

        await self.con.execute("""
            ALTER TYPE Text
                ALTER PROPERTY body
                    DROP CONSTRAINT max_value(10000)
                        ON (len(__subject__));
        """)

    async def test_edgeql_ddl_constraint_12(self):
        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"constraint 'std::max_len_value' of property 'firstname' "
                r"of object type 'default::Base' already exists"):
            await self.con.execute(r"""
                CREATE TYPE Base {
                    CREATE PROPERTY firstname -> str {
                        CREATE CONSTRAINT max_len_value(10);
                        CREATE CONSTRAINT max_len_value(10);
                    }
                }
            """)

    async def test_edgeql_ddl_constraint_13(self):
        await self.con.execute(r"""
            CREATE ABSTRACT CONSTRAINT Lol {
                USING ((__subject__ < 10));
            };
            CREATE TYPE Foo {
                CREATE PROPERTY x -> int64 {
                    CREATE CONSTRAINT Lol;
                };
            };
            CREATE TYPE Bar EXTENDING Foo;
        """)

        await self.con.execute(r"""
            ALTER ABSTRACT CONSTRAINT Lol RENAME TO Lolol;
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo DROP PROPERTY x;
        """)

    async def test_edgeql_ddl_constraint_14(self):
        # Test for #1727. Usage of EXISTS in constraints.
        await self.con.execute(r"""
            CREATE TYPE Foo;
            CREATE TYPE Bar {
                CREATE MULTI LINK children -> Foo {
                    CREATE PROPERTY lprop -> str {
                        CREATE CONSTRAINT expression ON (EXISTS __subject__)
                    }
                }
            };
        """)

        await self.con.execute("""
            INSERT Foo;
            INSERT Bar {children := (SELECT Foo {@lprop := "test"})};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid lprop'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Bar { children := Foo };
                """)

    async def test_edgeql_ddl_constraint_15(self):
        # Test for #1727. Usage of ?!= in constraints.
        await self.con.execute(r"""
            CREATE TYPE Foo;
            CREATE TYPE Bar {
                CREATE MULTI LINK children -> Foo {
                    CREATE PROPERTY lprop -> str {
                        CREATE CONSTRAINT expression ON (
                            __subject__ ?!= <str>{})
                    }
                }
            };
        """)

        await self.con.execute("""
            INSERT Foo;
            INSERT Bar {children := (SELECT Foo {@lprop := "test"})};
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid lprop'):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Bar { children := Foo };
                """)

    async def test_edgeql_ddl_constraint_16(self):
        await self.con.execute(r"""
            create type Foo {
                create property x -> tuple<x: str, y: str> {
                    create constraint exclusive;
                }
             };
        """)

        await self.con.execute("""
            INSERT Foo { x := ('1', '2') };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'x violates exclusivity constraint'):
            await self.con.execute("""
                INSERT Foo { x := ('1', '2') };
            """)

    async def test_edgeql_ddl_constraint_17(self):
        await self.con.execute(r"""
            create type Post {
                create link original -> Post;
                create constraint expression ON ((.original != __subject__));
            };
        """)

        await self.con.execute("""
            insert Post;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'invalid Post'):
            await self.con.execute("""
                update Post set { original := Post };
            """)

    async def test_edgeql_ddl_constraint_18(self):
        await self.con.execute(r"""
            create type Foo {
                create property flag -> bool;
                create property except -> bool;
                create constraint expression on (.flag) except (.except);
             };
        """)

        # Check all combinations of expression outcome and except value
        for flag in [True, False, None]:
            for ex in [True, False, None]:
                op = self.con.query(
                    """
                    INSERT Foo {
                        flag := <optional bool>$flag,
                        except := <optional bool>$ex,
                    }
                    """,
                    flag=flag,
                    ex=ex,
                )

                # The constraint fails if it is specifically false
                # (not empty) and except is not true.
                fail = flag is False and ex is not True
                if fail:
                    async with self.assertRaisesRegexTx(
                            edgedb.ConstraintViolationError,
                            r'invalid Foo'):
                        await op
                else:
                    await op

    @test.xerror('only object constraints may use EXCEPT')
    async def test_edgeql_ddl_constraint_19(self):
        # This is pretty marginal, but make sure we can distinguish
        # on and except in name creation;
        await self.con.execute(r"""
            create abstract constraint always_ok extending constraint {
                using (true)
            };
        """)

        await self.con.execute(r"""
            create type ExceptTest {
                create property b -> bool;
                create property e -> bool;
                create link l -> Object {
                    create constraint always_ok except (.e);
                    create constraint always_ok on (.e);
                };
            };
        """)

    async def test_edgeql_ddl_constraint_20(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidConstraintDefinitionError,
                r'constraints cannot contain paths with more than one hop'):
            await self.con.execute("""
                create type Foo {
                    create constraint expression on (false)
                        except (.__type__.name = 'default::Bar') ;
                };
            """)

    async def test_edgeql_ddl_constraint_21(self):
        # We plan on rejecting this, but for now do the thing closest
        # to right.
        await self.con.execute(r"""
            create type A {
                create property x -> str;
                create constraint exclusive on (A.x);
            };
            create type B extending A;
            insert A { x := "!" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'violates exclusivity constraint'):
            await self.con.execute("""
                insert B { x := "!" }
            """)

    async def test_edgeql_ddl_constraint_22(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidConstraintDefinitionError,
                r'expected to return a bool value, got collection'):
            await self.con.execute("""
                create type X {
                    create property y -> str {
                        create constraint expression on (<array<int32>>[]);
                    }
                };
            """)

    async def test_edgeql_ddl_constraint_23(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidConstraintDefinitionError,
                r"constraints on object types must have an 'on' clause"):
            await self.con.execute("""
                create type X {
                    create constraint exclusive;
                };
            """)

    async def test_edgeql_ddl_constraint_24(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidConstraintDefinitionError,
                r"constraint expressions must be immutable"):
            await self.con.execute("""
                create type X {
                    create constraint exclusive on (random());
                };
            """)

    async def test_edgeql_ddl_constraint_25(self):
        await self.con.execute("""
            create scalar type Status extending enum<open, closed>;
            create type Order {
                create required property status -> Status;
            }
        """)
        await self.con.execute("""
            alter type Order {
                create constraint exclusive on ((Status.open = .status));
            };
        """)
        await self.con.execute("""
            alter type Order {
                create constraint exclusive on ((<Status>'open' = .status));
            };
        """)
        await self.con.execute("""
            alter type Order {
                create index on ((Status.open = .status));
            };
        """)
        await self.con.execute("""
            alter type Order {
                create index on ((<Status>'open' = .status));
            };
        """)

    async def test_edgeql_ddl_constraint_check_01a(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str;
            };
            create type Bar extending Foo;

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter property foo {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_01b(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str {create constraint exclusive;};
            };
            create type Bar {
                create property foo -> str {create constraint exclusive;};
            };

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Bar extending Foo;
            """)

    async def test_edgeql_ddl_constraint_check_02a(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str;
            };
            create type Bar extending Foo;

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo {
                    create constraint exclusive on (.foo);
                };
            """)

    async def test_edgeql_ddl_constraint_check_02b(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str;
                create constraint exclusive on (.foo);
            };
            create type Bar {
                CREATE PROPERTY foo -> str;
                create constraint exclusive on (.foo);
            };

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'violates exclusivity constraint'):
            await self.con.execute("""
                alter type Bar extending Foo;
            """)

    async def test_edgeql_ddl_constraint_check_03a(self):
        await self.con.execute(r"""
            create type Foo {
                create multi property foo -> str;
            };
            create type Bar extending Foo;

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter property foo {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_03b(self):
        await self.con.execute(r"""
            create type Foo {
                create multi property foo -> str {create constraint exclusive;}
            };
            create type Bar {
                create multi property foo -> str {create constraint exclusive;}
            };

            insert Foo { foo := "x" };
            insert Bar { foo := "x" };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Bar extending Foo;
            """)

    async def test_edgeql_ddl_constraint_check_04(self):
        await self.con.execute(r"""
            create type Tgt;
            create type Foo {
                create link foo -> Tgt
            };
            create type Bar extending Foo;

            insert Tgt;
            insert Foo { foo := assert_single(Tgt) };
            insert Bar { foo := assert_single(Tgt) };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter link foo {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_05(self):
        await self.con.execute(r"""
            create type Tgt;
            create type Foo {
                create multi link foo -> Tgt { create property x -> str; }
            };
            create type Bar extending Foo;

            insert Tgt;
            insert Foo { foo := assert_single(Tgt) };
            insert Bar { foo := assert_single(Tgt) };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter link foo {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_06(self):
        await self.con.execute(r"""
            create type Tgt;
            create type Foo {
                create link foo -> Tgt { create property x -> str; }
            };
            create type Bar extending Foo;

            insert Tgt;
            insert Foo { foo := assert_single(Tgt { @x := "foo" }) };
            insert Bar { foo := assert_single(Tgt { @x := "foo" }) };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'x violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter link foo alter property x {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_07(self):
        await self.con.execute(r"""
            create type Tgt;
            create type Foo {
                create link foo -> Tgt { create property x -> str; }
            };
            create type Bar extending Foo;

            insert Tgt;
            insert Foo { foo := assert_single(Tgt { @x := "foo" }) };
            insert Bar { foo := assert_single(Tgt { @x := "foo" }) };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'foo violates exclusivity constraint'):
            await self.con.execute("""
                alter type Foo alter link foo  {
                    create constraint exclusive on (__subject__@x)
                };
            """)

    async def test_edgeql_ddl_constraint_check_08(self):
        await self.con.execute(r"""
            create type Tgt;
            create abstract link Lnk { create property x -> str; };
            create type Foo {
                create link foo extending Lnk -> Tgt;
            };
            create type Bar extending Foo;

            insert Tgt;
            insert Foo { foo := assert_single(Tgt { @x := "foo" }) };
            insert Bar { foo := assert_single(Tgt { @x := "foo" }) };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r'x violates exclusivity constraint'):
            await self.con.execute("""
                alter abstract link Lnk alter property x {
                    create constraint exclusive
                };
            """)

    async def test_edgeql_ddl_constraint_check_09(self):
        # Test a diamond pattern with a delegated constraint

        await self.con.execute(r"""
            CREATE ABSTRACT TYPE R {
                CREATE REQUIRED PROPERTY name -> std::str {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE S EXTENDING R;
            CREATE TYPE T EXTENDING R;
            CREATE TYPE V EXTENDING S, T;

            INSERT S { name := "S" };
            INSERT T { name := "T" };
            INSERT V { name := "V" };
        """)

        for t1, t2 in ["SV", "TV", "VT", "VS"]:
            with self.annotate(tables=(t1, t2)):
                async with self.assertRaisesRegexTx(
                        edgedb.ConstraintViolationError,
                        r'violates exclusivity constraint'):
                    await self.con.execute(f"""
                        insert {t1} {{ name := "{t2}" }}
                    """)

                async with self.assertRaisesRegexTx(
                        edgedb.ConstraintViolationError,
                        r'violates exclusivity constraint'):
                    await self.con.execute(f"""
                        select {{
                            (insert {t1} {{ name := "!" }}),
                            (insert {t2} {{ name := "!" }}),
                        }}
                    """)

        await self.con.execute(r"""
            ALTER TYPE default::R {
                DROP PROPERTY name;
            };
        """)

    async def test_edgeql_ddl_constraint_check_10(self):
        # Test a half-delegated twice inherited constraint pattern

        await self.con.execute(r"""
            CREATE ABSTRACT TYPE R {
                CREATE REQUIRED PROPERTY name -> std::str {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE S EXTENDING R;
            CREATE TYPE T {
                CREATE REQUIRED PROPERTY name -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE V EXTENDING S, T;

            INSERT S { name := "S" };
            INSERT T { name := "T" };
            INSERT V { name := "V" };
        """)

        for t1, t2 in ["SV", "TV", "VT", "VS"]:
            with self.annotate(tables=(t1, t2)):
                async with self.assertRaisesRegexTx(
                        edgedb.ConstraintViolationError,
                        r'violates exclusivity constraint'):
                    await self.con.execute(f"""
                        insert {t1} {{ name := "{t2}" }}
                    """)

                async with self.assertRaisesRegexTx(
                        edgedb.ConstraintViolationError,
                        r'violates exclusivity constraint'):
                    await self.con.execute(f"""
                        select {{
                            (insert {t1} {{ name := "!" }}),
                            (insert {t2} {{ name := "!" }}),
                        }}
                    """)

    async def test_edgeql_ddl_constraint_alter_01(self):
        await self.con.execute(r"""
            CREATE TYPE ConTest01 {
                CREATE PROPERTY con_test -> int64;
            };

            ALTER TYPE ConTest01
                ALTER PROPERTY con_test
                    CREATE CONSTRAINT min_value(0);
        """)

        await self.con.execute("""
            ALTER TYPE ConTest01
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
            FILTER .name = 'default::ConTest01';
        """, [
            {
                'name': 'default::ConTest01',
                'properties': [{
                    'name': 'con_test',
                    'constraints': [],
                }]
            }
        ])

    async def test_edgeql_ddl_constraint_alter_02(self):
        # Create constraint, then add and drop annotation for it. This
        # is similar to `test_edgeql_ddl_annotation_06`.
        await self.con.execute(r'''
            CREATE SCALAR TYPE contest2_t EXTENDING int64 {
                CREATE CONSTRAINT expression ON (__subject__ > 0);
            };
        ''')

        await self.con.execute(r'''
            ALTER SCALAR TYPE contest2_t {
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
                    .name = 'default::contest2_t';
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
            ALTER SCALAR TYPE contest2_t {
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
                    .name = 'default::contest2_t';
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
            CREATE SCALAR TYPE contest3_t EXTENDING int64 {
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
                    .name = 'default::contest3_t';
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
                    .name = 'default::contest3_t';
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
            CREATE SCALAR TYPE contest4_t EXTENDING int64 {
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
                    .name = 'default::contest4_t';
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
                    .name = 'default::contest4_t';
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

    async def test_edgeql_ddl_constraint_alter_05(self):
        await self.con.execute(r"""
            CREATE TYPE Base {
                CREATE PROPERTY firstname -> str {
                    CREATE CONSTRAINT max_len_value(10);
                }
            }
        """)

        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"constraint 'std::max_len_value' of property 'firstname' "
                r"of object type 'default::Base' already exists"):
            await self.con.execute(r"""
                ALTER TYPE Base {
                    ALTER PROPERTY firstname {
                        CREATE CONSTRAINT max_len_value(10);
                    }
                }
            """)

    async def test_edgeql_ddl_constraint_alter_06(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str {create constraint exclusive;};
            };
            create type Bar extending Foo;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Bar { foo := "x" }; insert Foo { foo := "x" };
            """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Foo { foo := "x" }; insert Bar { foo := "x" };
            """)

    async def test_edgeql_ddl_constraint_alter_07(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str;
            };
            create type Bar extending Foo;
            alter type Foo alter property foo {
                create constraint exclusive
            };
        """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Bar { foo := "x" }; insert Foo { foo := "x" };
            """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Foo { foo := "x" }; insert Bar { foo := "x" };
            """)

    async def test_edgeql_ddl_constraint_alter_08(self):
        await self.con.execute(r"""
            create type Foo {
                create property foo -> str {create constraint exclusive;};
            };
            create type Bar {
                create property foo -> str {create constraint exclusive;};
            };
            alter type Bar extending Foo;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Bar { foo := "x" }; insert Foo { foo := "x" };
            """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(r"""
                insert Foo { foo := "x" }; insert Bar { foo := "x" };
            """)

    async def test_edgeql_ddl_constraint_alter_09(self):
        await self.con.execute(r"""
            CREATE ABSTRACT TYPE default::T;
            CREATE ABSTRACT TYPE default::Sub1 EXTENDING default::T;
            CREATE TYPE default::Sub2 EXTENDING default::Sub1, default::T;
            ALTER TYPE default::T {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };
        """)

    async def test_edgeql_ddl_drop_inherited_link(self):
        await self.con.execute(r"""
            CREATE TYPE Target;
            CREATE TYPE Parent {
                CREATE LINK dil_foo -> Target;
            };

            CREATE TYPE Child EXTENDING Parent;
            CREATE TYPE GrandChild EXTENDING Child;
       """)

        await self.con.execute("""
            ALTER TYPE Parent DROP LINK dil_foo;
        """)

    async def test_edgeql_ddl_drop_01(self):
        # Check that constraints defined on scalars being dropped are
        # dropped.
        await self.con.execute("""
            CREATE SCALAR TYPE a1 EXTENDING std::str;

            ALTER SCALAR TYPE a1 {
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
            DROP SCALAR TYPE a1;
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
            CREATE TYPE C1 {
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
            DROP TYPE C1;
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

    async def test_edgeql_ddl_drop_03(self):
        await self.con.execute("""
            CREATE TYPE Foo {
                CREATE REQUIRED SINGLE PROPERTY name -> std::str;
            };
        """)
        await self.con.execute("""
            CREATE TYPE Bar {
                CREATE OPTIONAL SINGLE LINK lol -> Foo {
                    CREATE PROPERTY note -> str;
                };
            };
        """)

        await self.con.execute("""
            DROP TYPE Bar;
        """)

    async def test_edgeql_ddl_drop_refuse_01(self):
        # Check that the schema refuses to drop objects with live references
        await self.con.execute("""
            CREATE TYPE DropA;
            CREATE ABSTRACT ANNOTATION dropattr;
            CREATE ABSTRACT LINK l1_parent;
            CREATE TYPE DropB {
                CREATE LINK l1 EXTENDING l1_parent -> DropA {
                    CREATE ANNOTATION dropattr := 'foo';
                };
            };
            CREATE SCALAR TYPE dropint EXTENDING int64;
            CREATE FUNCTION dropfunc(a: dropint) -> int64
                USING EdgeQL $$ SELECT a $$;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop object type.*DropA.*other objects'):
            await self.con.execute('DROP TYPE DropA')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract anno.*dropattr.*other objects'):
            await self.con.execute('DROP ABSTRACT ANNOTATION dropattr')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract link.*l1_parent.*other objects'):
            await self.con.execute('DROP ABSTRACT LINK l1_parent')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop.*dropint.*other objects'):
            await self.con.execute('DROP SCALAR TYPE dropint')

    async def test_edgeql_ddl_unicode_01(self):
        await self.con.execute(r"""
            # setup delta
            START MIGRATION TO {
                module default {
                    type Пример {
                        required property номер -> int16;
                    };
                };
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;

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
            CREATE TYPE TupProp01 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
                CREATE PROPERTY p2 -> tuple<foo: int64, bar: str>;
                CREATE PROPERTY p3 -> tuple<foo: int64,
                                            bar: tuple<json, json>>;
            };

            CREATE TYPE TupProp02 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
                CREATE PROPERTY p2 -> tuple<json, json>;
            };
        """)

        # Drop identical p1 properties from both objects,
        # to check positive refcount.
        await self.con.execute(r"""
            ALTER TYPE TupProp01 {
                DROP PROPERTY p1;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE TupProp02 {
                DROP PROPERTY p1;
            };
        """)

        # Re-create the property to check that the associated
        # composite type was actually removed.
        await self.con.execute(r"""
            ALTER TYPE TupProp02 {
                CREATE PROPERTY p1 -> tuple<int64, str>;
            };
        """)

        # Now, drop the property that has a nested tuple that
        # is referred to directly by another property.
        await self.con.execute(r"""
            ALTER TYPE TupProp01 {
                DROP PROPERTY p3;
            };
        """)

        # Drop the last user.
        await self.con.execute(r"""
            ALTER TYPE TupProp02 {
                DROP PROPERTY p2;
            };
        """)

        # Re-create to assure cleanup.
        await self.con.execute(r"""
            ALTER TYPE TupProp02 {
                CREATE PROPERTY p3 -> tuple<json, json>;
                CREATE PROPERTY p4 -> tuple<a: json, b: json>;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE TupProp02 {
                CREATE PROPERTY p5 -> array<tuple<int64>>;
            };
        """)

        await self.con.query('DECLARE SAVEPOINT t0')

        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                'expected a scalar type, or a scalar collection'):

            await self.con.execute(r"""
                ALTER TYPE TupProp02 {
                    CREATE PROPERTY p6 -> tuple<TupProp02>;
                };
            """)

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

    async def test_edgeql_ddl_enum_01(self):
        await self.con.execute('''
            CREATE SCALAR TYPE my_enum EXTENDING enum<'foo', 'bar'>;
        ''')

        await self.assert_query_result(
            r"""
                SELECT schema::ScalarType {
                    enum_values,
                }
                FILTER .name = 'default::my_enum';
            """,
            [{
                'enum_values': ['foo', 'bar'],
            }],
        )

        await self.con.execute('''
            CREATE TYPE EnumHost {
                CREATE PROPERTY foo -> my_enum;
            }
        ''')

        await self.con.query('DECLARE SAVEPOINT t0')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'enumeration must be the only supertype specified'):
            await self.con.execute('''
                CREATE SCALAR TYPE my_enum_2
                    EXTENDING enum<'foo', 'bar'>,
                    std::int32;
            ''')

        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        await self.con.execute('''
            CREATE SCALAR TYPE my_enum_2
                EXTENDING enum<'foo', 'bar'>;
        ''')

        await self.con.query('DECLARE SAVEPOINT t1')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'constraints cannot be defined on enumerated type.*'):
            await self.con.execute('''
                CREATE SCALAR TYPE my_enum_3
                    EXTENDING enum<'foo', 'bar', 'baz'> {
                    CREATE CONSTRAINT expression ON (EXISTS(__subject__))
                };
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t1;')

        await self.con.execute('''
            ALTER SCALAR TYPE my_enum_2
                RENAME TO my_enum_3;
        ''')

        await self.con.execute('''
            CREATE MODULE foo;
            ALTER SCALAR TYPE my_enum_3
                RENAME TO foo::my_enum_4;
        ''')

        await self.con.execute('''
            DROP SCALAR TYPE foo::my_enum_4;
        ''')

    async def test_edgeql_ddl_enum_02(self):
        await self.con.execute('''
            CREATE SCALAR TYPE my_enum EXTENDING enum<'foo', 'bar'>;
        ''')

        await self.con.execute('''
            CREATE TYPE Obj {
                CREATE PROPERTY e -> my_enum {
                    SET default := <my_enum>'foo';
                }
            }
        ''')

        await self.con.execute('''
            CREATE MODULE foo;
            ALTER SCALAR TYPE my_enum
                RENAME TO foo::my_enum_2;
        ''')

        await self.con.execute('''
            DROP TYPE Obj;
            DROP SCALAR TYPE foo::my_enum_2;
        ''')

    async def test_edgeql_ddl_enum_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'enums cannot contain duplicate values'):
            await self.con.execute('''
                CREATE SCALAR TYPE Color
                    EXTENDING enum<Red, Green, Blue, Red>;
            ''')

    async def test_edgeql_ddl_enum_04(self):
        await self.con.execute('''
            CREATE SCALAR TYPE Color
                EXTENDING enum<Red, Green, Blue>;
        ''')

        await self.con.query('DECLARE SAVEPOINT t0')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'cannot DROP EXTENDING enum'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color
                    DROP EXTENDING enum<Red, Green, Blue>;
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'enumeration must be the only supertype specified'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color EXTENDING str FIRST;
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'cannot add another enum as supertype, '
                'use EXTENDING without position qualification'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color
                    EXTENDING enum<Bad> LAST;
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'cannot set more than one enum as supertype'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color
                    EXTENDING enum<Bad>, enum<AlsoBad>;
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.SchemaError,
                'enums cannot contain duplicate values'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color
                    EXTENDING enum<Red, Green, Blue, Red>;
            ''')

        # Recover.
        await self.con.query('ROLLBACK TO SAVEPOINT t0;')

        await self.con.execute(r'''
            ALTER SCALAR TYPE Color
                EXTENDING enum<Red, Green, Blue, Magic>;
        ''')
        # Commit the changes and start a new transaction for more testing.
        await self.con.query("COMMIT")
        await self.con.query("START TRANSACTION")
        await self.assert_query_result(
            r"""
                SELECT <Color>'Magic' >
                    <Color>'Red';
            """,
            [True],
        )

        await self.con.execute('''
            DROP SCALAR TYPE Color;
        ''')
        await self.con.query("COMMIT")

    async def test_edgeql_ddl_enum_05(self):
        await self.con.execute('''
            CREATE SCALAR TYPE Color
                EXTENDING enum<Red, Green, Blue>;

             CREATE FUNCTION asdf(x: Color) -> str USING (
                 <str>(x));
             CREATE FUNCTION asdf2() -> str USING (
                 asdf(<Color>'Red'));

             CREATE TYPE Entry {
                 CREATE PROPERTY num -> int64;
                 CREATE PROPERTY color -> Color;
                 CREATE PROPERTY colors -> array<Color>;
                 CREATE CONSTRAINT expression ON (
                     <str>.num != asdf2()
                 );
                 CREATE INDEX ON (asdf(.color));
                 CREATE PROPERTY lol -> str {
                     SET default := asdf2();
                 }
             };
             INSERT Entry { num := 1, color := "Red" };
             INSERT Entry {
                 num := 2, color := "Green", colors := ["Red", "Green"] };
        ''')

        await self.con.execute('''
            ALTER SCALAR TYPE Color
                EXTENDING enum<Red, Green>;
        ''')

        await self.con.execute('''
            ALTER SCALAR TYPE Color
                EXTENDING enum<Green, Red>;
        ''')

        await self.assert_query_result(
            r"""
                SELECT Entry { num, color } ORDER BY .color;
            """,
            [
                {'num': 2, 'color': 'Green'},
                {'num': 1, 'color': 'Red'},
            ],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                'invalid input value for enum'):
            await self.con.execute('''
                ALTER SCALAR TYPE Color
                    EXTENDING enum<Green>;
            ''')

    async def test_edgeql_ddl_explicit_id(self):
        await self.con.execute('''
            CREATE TYPE ExID {
                SET id := <uuid>'00000000-0000-0000-0000-0000feedbeef'
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT schema::ObjectType {
                    id
                }
                FILTER .name = 'default::ExID';
            """,
            [{
                'id': '00000000-0000-0000-0000-0000feedbeef',
            }],
        )

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                'cannot alter object id'):
            await self.con.execute('''
                ALTER TYPE ExID {
                    SET id := <uuid>'00000000-0000-0000-0000-0000feedbeef'
                }
            ''')

    async def test_edgeql_ddl_quoting_01(self):
        await self.con.execute("""
            CREATE TYPE `U S``E R` {
                CREATE PROPERTY `n ame` -> str;
            };
        """)

        await self.con.execute("""
            INSERT `U S``E R` {
                `n ame` := 'quoting_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT `U S``E R` {
                    __type__: {
                        name
                    },
                    `n ame`
                };
            """,
            [{
                '__type__': {'name': 'default::U S`E R'},
                'n ame': 'quoting_01'
            }],
        )

        await self.con.execute("""
            DROP TYPE `U S``E R`;
        """)

    async def test_edgeql_ddl_prop_overload_01(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the computed property 'val' "
                "of object type 'default::UniqueName_2' to overload "
                "an existing property"):
            await self.con.execute("""
                CREATE TYPE UniqueName {
                    CREATE PROPERTY val -> str;
                };
                CREATE TYPE UniqueName_2 EXTENDING UniqueName {
                    ALTER PROPERTY val {
                        USING ('bad');
                    };
                };
            """)

    async def test_edgeql_ddl_prop_overload_02(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the computed property 'val' "
                "of object type 'default::UniqueName_2' to overload "
                "an existing property"):
            await self.con.execute("""
                CREATE TYPE UniqueName {
                    CREATE PROPERTY val := 'bad';
                };
                CREATE TYPE UniqueName_2 EXTENDING UniqueName {
                    ALTER PROPERTY val {
                        CREATE CONSTRAINT exclusive;
                    };
                };
            """)

    async def test_edgeql_ddl_prop_overload_03(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the property 'val' of object "
                "type 'default::UniqueName_3' to extend both a computed "
                "and a non-computed property"):
            await self.con.execute("""
                CREATE TYPE UniqueName {
                    CREATE PROPERTY val := 'ok';
                };
                CREATE TYPE UniqueName_2 {
                    CREATE PROPERTY val -> str;
                };
                CREATE TYPE UniqueName_3 EXTENDING UniqueName, UniqueName_2;
            """)

    async def test_edgeql_ddl_prop_overload_04(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the property 'val' of object "
                "type 'default::UniqueName_3' to extend more than one "
                "computed property"):
            await self.con.execute("""
                CREATE TYPE UniqueName {
                    CREATE PROPERTY val := 'ok';
                };
                CREATE TYPE UniqueName_2 {
                    CREATE PROPERTY val := 'ok';
                };
                CREATE TYPE UniqueName_3 EXTENDING UniqueName, UniqueName_2;
            """)

    async def test_edgeql_ddl_prop_overload_05(self):
        await self.con.execute("""
            CREATE TYPE UniqueName {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE UniqueName_2 {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE UniqueName_3 EXTENDING UniqueName, UniqueName_2;
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the property 'val' of object "
                "type 'default::UniqueName_3' to extend both a computed "
                "and a non-computed property"):
            await self.con.execute("""
                ALTER TYPE UniqueName {
                    ALTER PROPERTY val {
                        USING ('bad');
                    };
                };
            """)

    async def test_edgeql_ddl_prop_overload_06(self):
        await self.con.execute("""
            CREATE TYPE UniqueName {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE UniqueName_2 {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE UniqueName_3 {
                CREATE PROPERTY val := 'ok';
            };
            CREATE TYPE UniqueName_4 EXTENDING UniqueName, UniqueName_2;
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the property 'val' of object "
                "type 'default::UniqueName_4' to extend both a computed "
                "and a non-computed property"):
            await self.con.execute("""
                ALTER TYPE UniqueName_4 EXTENDING UniqueName_3;
            """)

    async def test_edgeql_ddl_prop_overload_07(self):
        await self.con.execute("""
            CREATE TYPE UniqueName {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE UniqueName_2 {
                CREATE PROPERTY val := 'ok';
            };
            CREATE TYPE UniqueName_3;
            CREATE TYPE UniqueName_4 EXTENDING UniqueName, UniqueName_3;
        """)

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "it is illegal for the property 'val' of object "
                "type 'default::UniqueName_4' to extend both a computed "
                "and a non-computed property"):
            await self.con.execute("""
                ALTER TYPE UniqueName_3 EXTENDING UniqueName_2;
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
                "object type 'default::Derived': it is defined as True in "
                "property 'foo' of object type 'default::Derived' and as "
                "False in property 'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "property 'foo' of object type 'default::Derived' and as "
                "True in property 'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "property 'foo' of object type 'default::Base0' and as "
                "True in property 'foo' of object type 'default::Base1'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "property 'foo' of object type 'default::Base0' and as "
                "True in property 'foo' of object type 'default::Base1'."):
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
                "object type 'default::Derived': it is defined as True in "
                "link 'foo' of object type 'default::Derived' and as "
                "False in link 'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "link 'foo' of object type 'default::Derived' and as "
                "True in link 'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "link 'foo' of object type 'default::Base0' and as "
                "True in link 'foo' of object type 'default::Base1'."):
            await self.con.execute('''

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
                "object type 'default::Derived': it is defined as False in "
                "link 'foo' of object type 'default::Base0' and as "
                "True in link 'foo' of object type 'default::Base1'."):
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
                "link 'foo' of object type 'default::Derived': it is defined "
                "as True in property 'bar' of link 'foo' of object type "
                "'default::Derived' and as False in property 'bar' of link "
                "'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "link 'foo' of object type 'default::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'default::Derived' and as True in property 'bar' of link "
                "'foo' of object type 'default::Base'."):
            await self.con.execute('''

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
                "link 'foo' of object type 'default::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'default::Base0' and as True in property 'bar' of link "
                "'foo' of object type 'default::Base1'."):
            await self.con.execute('''

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
                "link 'foo' of object type 'default::Derived': it is defined "
                "as False in property 'bar' of link 'foo' of object type "
                "'default::Base0' and as True in property 'bar' of link "
                "'foo' of object type 'default::Base1'."):
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

                CREATE TYPE Base {
                    CREATE REQUIRED PROPERTY foo -> str;
                };
                CREATE TYPE Derived EXTENDING Base;
                ALTER TYPE Derived {
                    ALTER PROPERTY foo {
                        SET OPTIONAL;
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

                CREATE TYPE Base {
                    CREATE REQUIRED PROPERTY foo -> str;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER PROPERTY foo {
                        SET OPTIONAL;
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

                CREATE TYPE Base {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base;
                ALTER TYPE Derived {
                    ALTER LINK foo {
                        SET OPTIONAL;
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

                CREATE TYPE Base {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base {
                    ALTER LINK foo {
                        SET OPTIONAL;
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

                CREATE TYPE Base {
                    CREATE OPTIONAL LINK foo -> Object;
                };
                CREATE TYPE Base2 {
                    CREATE REQUIRED LINK foo -> Object;
                };
                CREATE TYPE Derived EXTENDING Base, Base2 {
                    ALTER LINK foo {
                        SET OPTIONAL;
                    };
                };
            ''')

    async def test_edgeql_ddl_required_08(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""

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
            f"missing value for required property"
            r" 'foo' of object type 'default::Base'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base;
                """)

        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            f"missing value for required property"
            r" 'foo' of object type 'default::Derived'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Base {
                ALTER PROPERTY foo {
                    SET OPTIONAL;
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
            f"missing value for required property"
            r" 'foo' of object type 'default::Derived'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Derived {
                ALTER PROPERTY foo {
                    SET OPTIONAL;
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
            f"missing value for required property"
            r" 'foo' of object type 'default::Derived'",
        ):
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
            f"missing value for required property"
            r" 'foo' of object type 'default::Derived'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Derived;
                """)

        await self.con.execute("""
            ALTER TYPE Derived {
                ALTER PROPERTY foo {
                    SET OPTIONAL;
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

    async def test_edgeql_ddl_required_10(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            CREATE TYPE Base {
                CREATE REQUIRED MULTI PROPERTY name -> str;
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'name'"
            r" of object type 'default::Base'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base;
                """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'name'"
            r" of object type 'default::Base'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base {name := {}};
                """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'name'"
            r" of object type 'default::Base'",
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    WITH names := {'A', 'B'}
                    INSERT Base {
                        name := (SELECT names FILTER names = 'C'),
                    };
                """)

    async def test_edgeql_ddl_required_11(self):
        # Test normal that required qualifier behavior.

        await self.con.execute(r"""
            CREATE TYPE Child;
            CREATE TYPE Base {
                CREATE REQUIRED MULTI LINK children -> Child;
            };
        """)

        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required link 'children'"
            r" of object type 'default::Base'"
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base;
                """)

        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required link 'children'"
            r" of object type 'default::Base'"
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base {children := {}};
                """)

        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required link 'children'"
            r" of object type 'default::Base'"
        ):
            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Base {
                        children := (SELECT Child FILTER false)
                    };
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
            r"possibly more than one element returned by the index expression",
            _line=4, _col=34
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
            r"possibly more than one element returned by the index expression",
            _line=5, _col=34
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
            r"possibly more than one element returned by the index expression",
            _line=5, _col=34
        ):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY a -> int64;
                    CREATE PROPERTY b -> int64;
                    CREATE INDEX ON (array_unpack([.a, .b]));
                }
            """)

    async def test_edgeql_ddl_index_04(self):
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            r"index expressions must be immutable"
        ):
            await self.con.execute(r"""
                create function f(s: str) -> str {
                    set volatility := "stable";
                    using (s)
                };

                create type Bar {
                    create property x -> str;
                    create index on (f(.x));
                };
            """)

    async def test_edgeql_ddl_index_05(self):
        await self.con.execute(r"""
            create type Artist {
                create property oid -> bigint;
                create index on (<str>.oid)
            };
        """)

    async def test_edgeql_ddl_index_06(self):
        # Unfortunately we don't really have a way to test that this
        # actually works, but I looked at the SQL DDL.
        await self.con.execute(r"""
            create type Foo {
                create property name -> str;
                create property exclude -> bool;
                create index on (.name) except (.exclude);
            };
        """)

        # But we can at least make sure it made it into the schema
        await self.assert_query_result(
            '''
            SELECT schema::ObjectType {
                indexes: {expr, except_expr}
            } FILTER .name = 'default::Foo'
            ''',
            [{"indexes": [{"except_expr": ".exclude", "expr": ".name"}]}]
        )

    async def test_edgeql_ddl_errors_01(self):
        await self.con.execute('''
            CREATE TYPE Err1 {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            ALTER TYPE Err1
            CREATE REQUIRED LINK bar -> Err1;
        ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'b' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER PROPERTY b
                    CREATE CONSTRAINT std::regexp(r'b');
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'b' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 DROP PROPERTY b
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'default::a' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    DROP CONSTRAINT a;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'default::a' does not exist"):
                await self.con.execute('''
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
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    ALTER ANNOTATION title := 'aaa'
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER PROPERTY foo
                    DROP ANNOTATION title;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1
                    ALTER ANNOTATION title := 'aaa'
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1
                    DROP ANNOTATION title
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                edgedb.errors.InvalidReferenceError,
                r"index on \(.foo\) does not exist on"
                r" object type 'default::Err1'",
            ):
                await self.con.execute('''
                    ALTER TYPE Err1
                    DROP INDEX ON (.foo)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                edgedb.errors.InvalidReferenceError,
                r"index on \(.zz\) does not exist on object type "
                r"'default::Err1'",
            ):
                await self.con.execute('''
                    ALTER TYPE Err1
                    DROP INDEX ON (.zz)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'default::Err1' has no link or "
                    "property 'zz'"):
                await self.con.execute('''
                    ALTER TYPE Err1
                    CREATE INDEX ON (.zz)
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'default::Err1' has no link or "
                    "property 'zz'"):
                await self.con.execute('''
                    ALTER TYPE Err1
                    CREATE INDEX ON ((.foo, .zz))
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'default::blah' does not exist"):
                await self.con.execute('''
                    CREATE TYPE Err1 EXTENDING blah {
                        CREATE PROPERTY foo -> str;
                    };
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "object type 'default::blah' does not exist"):
                await self.con.execute('''
                    CREATE TYPE Err2 EXTENDING blah {
                        CREATE PROPERTY foo -> str;
                    };
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'b' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER LINK b
                    CREATE CONSTRAINT std::regexp(r'b');
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'b' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 DROP LINK b;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "annotation 'std::title' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER LINK bar
                    DROP ANNOTATION title;
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "constraint 'std::min_value' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1 ALTER LINK bar
                    DROP CONSTRAINT min_value(0);
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "property 'spam' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err1
                    ALTER LINK bar
                    DROP PROPERTY spam;
                ''')

    @test.xfail('''
        The test currently fails with the ugly
        "'default::__|foo@default|Err2' exists, but is a property, not a link"
        but it should fail with "link 'foo' does not exist", as
        `ALTER LINK foo` is the preceeding invalid command.
    ''')
    async def test_edgeql_ddl_errors_02(self):
        await self.con.execute('''
            CREATE TYPE Err2 {
                CREATE REQUIRED PROPERTY foo -> str;
            };

            ALTER TYPE Err2
            CREATE REQUIRED LINK bar -> Err2;
        ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "link 'foo' does not exist"):
                await self.con.execute('''
                    ALTER TYPE Err2
                    ALTER LINK foo
                    DROP PROPERTY spam;
                ''')

    async def test_edgeql_ddl_errors_03(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "function 'default::foo___1' does not exist"):
                await self.con.execute('''
                    ALTER FUNCTION foo___1(a: int64)
                    SET volatility := 'Stable';
                ''')

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.errors.InvalidReferenceError,
                    "function 'default::foo___1' does not exist"):
                await self.con.execute('''
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

    async def test_edgeql_ddl_create_migration_02(self):
        await self.con.execute('''
CREATE MIGRATION m1kmv2mcizpj2twxlxxerkgngr2fkto7wnjd6uig3aa3x67dykvspq
    ONTO initial
{
  CREATE GLOBAL default::foo -> std::bool;
  CREATE TYPE default::Foo {
      CREATE ACCESS POLICY foo
          ALLOW ALL USING ((GLOBAL default::foo ?? true));
  };
};
        ''')

        await self.con.execute('''
CREATE MIGRATION m14i24uhm6przo3bpl2lqndphuomfrtq3qdjaqdg6fza7h6m7tlbra
    ONTO m1kmv2mcizpj2twxlxxerkgngr2fkto7wnjd6uig3aa3x67dykvspq
{
  CREATE TYPE default::X;

  INSERT Foo;
};
        ''')

    async def test_edgeql_ddl_create_migration_03(self):
        await self.con.execute('''
            CREATE MIGRATION
            {
                SET message := "migration2";
                SET generated_by := schema::MigrationGeneratedBy.DevMode;
                CREATE TYPE Type2 {
                    CREATE PROPERTY field2 -> int32;
                };
            };
        ''')

        await self.assert_query_result(
            '''
            SELECT schema::Migration { generated_by }
            FILTER .message = "migration2"
            ''',
            [{'generated_by': 'DevMode'}]
        )

        await self.con.execute(f'''
            CREATE TYPE Type3
        ''')

        await self.assert_query_result(
            '''
            SELECT schema::Migration { generated_by }
            FILTER .script like "%Type3%"
            ''',
            [{'generated_by': 'DDLStatement'}]
        )

    async def test_edgeql_ddl_naked_backlink_in_computable(self):
        await self.con.execute('''
            CREATE TYPE User {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Post {
                CREATE LINK author -> User;
            };
            CREATE TYPE Video {
                CREATE LINK author -> User;
            };
            ALTER TYPE User {
                CREATE MULTI LINK authored := .<author;
            };
            INSERT User { name := 'Lars' };
            INSERT Post { author := (SELECT User FILTER .name = 'Lars') };
            INSERT Video { author := (SELECT User FILTER .name = 'Lars') };
        ''')

        await self.assert_query_result(
            '''
            WITH
                User := (SELECT schema::ObjectType
                         FILTER .name = 'default::User')
            SELECT
                User.pointers {
                    target: {
                        name
                    }
                }
            FILTER
                .name = 'authored'
            ''',
            [{
                'target': {
                    'name': 'std::BaseObject',
                }
            }]
        )

        await self.assert_query_result(
            '''
            SELECT _ := User.authored.__type__.name
            ORDER BY _
            ''',
            ['default::Post', 'default::Video']
        )

    async def test_edgeql_ddl_change_module_01(self):
        await self.con.execute("""
            CREATE MODULE foo;

            CREATE TYPE Note {
                CREATE PROPERTY note -> str;
            };
            ALTER TYPE Note RENAME TO foo::Note;
            DROP TYPE foo::Note;
        """)

    async def test_edgeql_ddl_change_module_02(self):
        await self.con.execute("""
            CREATE MODULE foo;

            CREATE TYPE Parent {
                CREATE PROPERTY note -> str;
            };
            CREATE TYPE Sub EXTENDING Parent;
            ALTER TYPE Parent RENAME TO foo::Parent;
            DROP TYPE Sub;
            DROP TYPE foo::Parent;
        """)

    async def test_edgeql_ddl_change_module_03(self):
        await self.con.execute("""
            CREATE MODULE foo;

            CREATE TYPE Note {
                CREATE PROPERTY note -> str {
                    CREATE CONSTRAINT exclusive;
                }
            };
            ALTER TYPE Note RENAME TO foo::Note;
            DROP TYPE foo::Note;
        """)

    async def test_edgeql_ddl_change_module_04(self):
        await self.con.execute("""
            CREATE MODULE foo;

            CREATE TYPE Tag;

            CREATE TYPE Note {
                CREATE SINGLE LINK tags -> Tag {
                    ON TARGET DELETE DELETE SOURCE;
                }
            };

            INSERT Note { tags := (INSERT Tag) };
        """)

        await self.con.execute("""
            ALTER TYPE Tag RENAME TO foo::Tag;
            DELETE foo::Tag FILTER true;
        """)

        await self.assert_query_result(
            """SELECT Note;""",
            [],
        )

        await self.con.execute("""
            ALTER TYPE Note RENAME TO foo::Note;
            DROP TYPE foo::Note;
            DROP TYPE foo::Tag;
        """)

    async def _simple_rename_ref_test(
        self,
        ddl,
        cleanup=None,
        *,
        rename_type,
        rename_prop,
        rename_module,
        type_extra=0,
        prop_extra=0,
        type_refs=1,
        prop_refs=1,
    ):
        """Driver for simple rename tests for objects with expr references.

        Supports renaming a type, a property, or both. By default,
        each is expected to be named once in the referencing object.

        """
        await self.con.execute(f"""
            CREATE TYPE Note {{
                CREATE PROPERTY note -> str;
            }};

            {ddl.lstrip()}
        """)

        type_rename = "RENAME TO Remark;" if rename_type else ""
        prop_rename = (
            "ALTER PROPERTY note RENAME TO remark;" if rename_prop else "")

        await self.con.execute(f"""
            ALTER TYPE Note {{
                {type_rename.lstrip()}
                {prop_rename.lstrip()}
            }}
        """)
        if rename_module:
            await self.con.execute(f"""
            CREATE MODULE foo;
            ALTER TYPE Note RENAME TO foo::Note;
            """)

        else:
            res = await self.con.query_single("""
                DESCRIBE MODULE default
            """)

            total_type = 1 + type_refs
            num_type_orig = 0 if rename_type else total_type
            self.assertEqual(res.count("Note"), num_type_orig + type_extra)
            self.assertEqual(res.count("Remark"), total_type - num_type_orig)
            total_prop = 1 + prop_refs
            num_prop_orig = 0 if rename_prop else total_prop
            self.assertEqual(res.count("note"), num_prop_orig + type_extra)
            self.assertEqual(res.count("remark"), total_prop - num_prop_orig)

        if cleanup:
            if rename_prop:
                cleanup = cleanup.replace("note", "remark")
            if rename_type:
                cleanup = cleanup.replace("Note", "Remark")
            if rename_module:
                cleanup = cleanup.replace("default", "foo")
            await self.con.execute(f"""
                {cleanup.lstrip()}
            """)

    async def _simple_rename_ref_tests(self, ddl, cleanup=None, **kwargs):
        """Do the three interesting invocations of _simple_rename_ref_test"""
        async with self._run_and_rollback():
            await self._simple_rename_ref_test(
                ddl, cleanup,
                rename_type=False, rename_prop=True, rename_module=False,
                **kwargs)
        async with self._run_and_rollback():
            await self._simple_rename_ref_test(
                ddl, cleanup,
                rename_type=True, rename_prop=False, rename_module=False,
                **kwargs)
        async with self._run_and_rollback():
            await self._simple_rename_ref_test(
                ddl, cleanup,
                rename_type=True, rename_prop=True, rename_module=False,
                **kwargs)
        async with self._run_and_rollback():
            await self._simple_rename_ref_test(
                ddl, cleanup,
                rename_type=False, rename_prop=False, rename_module=True,
                **kwargs)

    async def test_edgeql_ddl_rename_ref_function_01(self):
        await self._simple_rename_ref_tests(
            """
            CREATE FUNCTION foo(x: Note) -> OPTIONAL str {
                USING (SELECT ('Note note ' ++ x.note ++
                               (SELECT Note.note LIMIT 1)))
            }
            """,
            """DROP FUNCTION foo(x: default::Note);""",
            type_extra=1,
            prop_extra=1,
            type_refs=2,
            prop_refs=2,
        )

    async def test_edgeql_ddl_rename_ref_function_02(self):
        # Test renaming two types that appear as function arguments at
        # the same time.
        await self.con.execute("""
            CREATE TYPE Note {
                CREATE PROPERTY note -> str;
            };

            CREATE TYPE Name {
                CREATE PROPERTY name -> str;
            };

            CREATE FUNCTION foo(x: Note, y: Name) -> OPTIONAL str {
                USING (SELECT (x.note ++ " " ++ y.name))
            };
        """)

        await self.con.execute("""
            INSERT Note { note := "hello" }
        """)
        await self.con.execute("""
            INSERT Name { name := "world" }
        """)

        await self.con.execute("""
            CREATE MIGRATION {
                ALTER TYPE Note RENAME TO Remark;
                ALTER TYPE Name RENAME TO Handle;
            }
            """)

        res = await self.con.query_single("""
            DESCRIBE MODULE default
        """)

        self.assertEqual(res.count("Note"), 0)
        self.assertEqual(res.count("Name"), 0)
        self.assertEqual(res.count("Remark"), 2)
        self.assertEqual(res.count("Handle"), 2)

        await self.assert_query_result(
            '''
                SELECT foo(Remark, Handle);
            ''',
            ['hello world'],
        )

        await self.con.execute("""
            DROP FUNCTION foo(x: Remark, y: Handle);
        """)

    async def test_edgeql_ddl_rename_ref_function_03(self):
        await self._simple_rename_ref_tests(
            """
            CREATE FUNCTION foo(x: str) -> OPTIONAL Note {
                USING (SELECT Note FILTER .note = x LIMIT 1)
            }
            """,
            """DROP FUNCTION foo(x: str);""",
            type_refs=2,
        )

    async def test_edgeql_ddl_rename_ref_function_04(self):
        await self._simple_rename_ref_tests(
            """
            CREATE FUNCTION foo(x: str) -> OPTIONAL Note {
                USING (SELECT Note FILTER .note = x LIMIT 1)
            }
            """,
            """SELECT foo("test");""",
            type_refs=2,
        )

    async def test_edgeql_ddl_rename_ref_function_05(self):
        await self._simple_rename_ref_tests(
            """
            CREATE FUNCTION foo(x: array<Note>) -> str {
                USING ('x')
            }
            """,
            """DROP FUNCTION foo(x: array<default::Note>);""",
            prop_refs=0,
        )

    async def test_edgeql_ddl_rename_ref_default_01(self):
        await self._simple_rename_ref_tests(
            """
            CREATE TYPE Object2 {
                CREATE REQUIRED PROPERTY x -> str {
                    SET default := (SELECT Note.note LIMIT 1)
                }
            };
            """,
            """ALTER TYPE Object2 DROP PROPERTY x;""",
        )

    async def test_edgeql_ddl_rename_ref_constraint_01(self):
        await self.con.execute("""
            CREATE TYPE Note {
                CREATE PROPERTY name -> str;
                CREATE PROPERTY note -> str;
                CREATE CONSTRAINT exclusive ON (
                    (__subject__.name, __subject__.note));
            };
        """)

        await self.con.execute("""
            ALTER TYPE Note {
                ALTER PROPERTY note {
                    RENAME TO remark;
                };
                ALTER PROPERTY name {
                    RENAME TO callsign;
                };
            }
        """)

        res = await self.con.query_single("""
            DESCRIBE MODULE default
        """)

        self.assertEqual(res.count("note"), 0)
        self.assertEqual(res.count("remark"), 2)
        self.assertEqual(res.count("name"), 0)
        self.assertEqual(res.count("callsign"), 2)

        await self.con.execute("""
            ALTER TYPE Note
            DROP CONSTRAINT exclusive ON ((
                (__subject__.callsign, __subject__.remark)));
        """)

    async def test_edgeql_ddl_rename_ref_index_01(self):
        await self._simple_rename_ref_tests(
            """ALTER TYPE Note CREATE INDEX ON (.note);""",
            """ALTER TYPE default::Note DROP INDEX ON (.note);""",
            type_refs=0,
        )

    async def test_edgeql_ddl_rename_ref_default_02(self):
        await self._simple_rename_ref_tests("""
            CREATE TYPE Uses {
                CREATE REQUIRED PROPERTY x -> str {
                    SET default := (SELECT Note.note LIMIT 1)
                }
            };

            CREATE TYPE Uses2 {
                CREATE REQUIRED PROPERTY x -> str {
                    SET default := (SELECT Note.note LIMIT 1)
                }
            };
        """, prop_refs=2, type_refs=2)

    async def test_edgeql_ddl_rename_ref_computable_01(self):
        await self._simple_rename_ref_tests(
            """
            ALTER TYPE Note {
                CREATE PROPERTY x := .note ++ "!";
            };
            """,
            """ALTER TYPE default::Note DROP PROPERTY x;""",
            type_refs=0,
        )

    async def test_edgeql_ddl_rename_ref_computable_02(self):
        await self._simple_rename_ref_tests(
            """
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> str;
                CREATE MULTI LINK x := (
                    SELECT Note FILTER Note.note = Foo.foo);
            };
            """,
            """ALTER TYPE Foo DROP LINK x;""",
            type_refs=2,
        )

    async def test_edgeql_ddl_rename_ref_type_alias_01(self):
        await self._simple_rename_ref_tests(
            """CREATE ALIAS Alias := Note;""",
            """DROP ALIAS Alias;""",
            prop_refs=0,
        )

    async def test_edgeql_ddl_rename_ref_expr_alias_01(self):
        await self._simple_rename_ref_tests(
            """CREATE ALIAS Alias := (SELECT Note.note);""",
            """DROP ALIAS Alias;""",
        )

    async def test_edgeql_ddl_rename_ref_shape_alias_01(self):
        await self._simple_rename_ref_tests(
            """CREATE ALIAS Alias := Note { command := .note ++ "!" };""",
            """DROP ALIAS Alias;""",
        )

    async def test_edgeql_ddl_drop_multi_prop_01(self):
        await self.con.execute(r"""

            CREATE TYPE Test {
                CREATE MULTI PROPERTY x -> str;
                CREATE MULTI PROPERTY y := {1, 2, 3};
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Test DROP PROPERTY x;
        """)

        await self.con.execute(r"""
            ALTER TYPE Test DROP PROPERTY y;
        """)

        await self.con.execute(r"""
            INSERT Test;
            DELETE Test;
        """)

    async def test_edgeql_ddl_collection_cleanup_01(self):
        count_query = "SELECT count(schema::Array);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;
            CREATE SCALAR TYPE c extending str;

            CREATE TYPE TestArrays {
                CREATE PROPERTY x -> array<a>;
                CREATE PROPERTY y -> array<b>;
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            ALTER TYPE TestArrays {
                DROP PROPERTY x;
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 1,
        )

        await self.con.execute(r"""
            ALTER TYPE TestArrays {
                ALTER PROPERTY y {
                    SET TYPE array<c> USING (
                        <array<c>><array<str>>.y);
                }
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 1,
        )

        await self.con.execute(r"""
            DROP TYPE TestArrays;
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)

    async def test_edgeql_ddl_collection_cleanup_01b(self):
        count_query = "SELECT count(schema::Array);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;
            CREATE SCALAR TYPE c extending str;

            CREATE TYPE TestArrays {
                CREATE PROPERTY x -> array<a>;
                CREATE PROPERTY y -> array<b>;
                CREATE PROPERTY z -> array<b>;
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            ALTER TYPE TestArrays {
                DROP PROPERTY x;
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 1,
        )

        await self.con.execute(r"""
            ALTER TYPE TestArrays {
                ALTER PROPERTY y {
                    SET TYPE array<c> USING (
                        <array<c>><array<str>>.y);
                }
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            DROP TYPE TestArrays;
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)

    async def test_edgeql_ddl_collection_cleanup_02(self):
        count_query = "SELECT count(schema::CollectionType);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;
            CREATE SCALAR TYPE c extending str;

            CREATE TYPE TestArrays {
                CREATE PROPERTY x -> array<tuple<a, b>>;
            };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            DROP TYPE TestArrays;
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)

    async def test_edgeql_ddl_collection_cleanup_03(self):
        count_query = "SELECT count(schema::CollectionType);"
        orig_count = await self.con.query_single(count_query)
        elem_count_query = "SELECT count(schema::TupleElement);"
        orig_elem_count = await self.con.query_single(elem_count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;
            CREATE SCALAR TYPE c extending str;

            CREATE FUNCTION foo(x: array<a>, z: tuple<b, c>,
                                y: array<tuple<b, c>>)
                 -> array<b> USING (SELECT [<b>""]);
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 4,
        )

        await self.con.execute(r"""
            DROP FUNCTION foo(
                x: array<a>, z: tuple<b, c>, y: array<tuple<b, c>>);
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)
        self.assertEqual(
            await self.con.query_single(elem_count_query), orig_elem_count)

    async def test_edgeql_ddl_collection_cleanup_04(self):
        count_query = "SELECT count(schema::CollectionType);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;
            CREATE SCALAR TYPE c extending str;

            CREATE TYPE Foo {
                CREATE PROPERTY a -> a;
                CREATE PROPERTY b -> b;
                CREATE PROPERTY c -> c;
            };

            CREATE ALIAS Bar := Foo { thing := (.a, .b) };
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 1,
        )

        await self.con.execute(r"""
            ALTER ALIAS Bar USING (Foo { thing := (.a, .b, .c) });
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 1,
        )

        await self.con.execute(r"""
            ALTER ALIAS Bar USING (Foo { thing := (.a, (.b, .c)) });
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            ALTER ALIAS Bar USING (Foo { thing := ((.a, .b), .c) });
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            ALTER ALIAS Bar USING (Foo { thing := ((.a, .b), .c, "foo") });
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        # Make a change that doesn't change the types
        await self.con.execute(r"""
            ALTER ALIAS Bar USING (Foo { thing := ((.a, .b), .c, "bar") });
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            DROP ALIAS Bar;
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)

    async def test_edgeql_ddl_collection_cleanup_05(self):
        count_query = "SELECT count(schema::CollectionType);"
        orig_count = await self.con.query_single(count_query)

        await self.con.execute(r"""

            CREATE SCALAR TYPE a extending str;
            CREATE SCALAR TYPE b extending str;

            CREATE ALIAS Bar := (<a>"", <b>"");
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            ALTER ALIAS Bar USING ((<b>"", <a>""));
        """)

        self.assertEqual(
            await self.con.query_single(count_query),
            orig_count + 2,
        )

        await self.con.execute(r"""
            DROP ALIAS Bar;
        """)

        self.assertEqual(await self.con.query_single(count_query), orig_count)

    async def test_edgeql_ddl_drop_field_01(self):
        await self.con.execute(r"""

            CREATE FUNCTION foo() -> str USING ("test");

            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY a -> str {
                    SET default := foo();
                }
            };
        """)

        await self.con.execute(r"""
            INSERT Foo;
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY a {
                    RESET default;
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property"
            r" 'a' of object type 'default::Foo'",
        ):
            await self.con.execute(r"""
                INSERT Foo;
            """)

        await self.con.execute(r"""
            DROP FUNCTION foo();
        """)

    async def test_edgeql_ddl_drop_field_02(self):
        await self.con.execute(r"""

            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY a -> str {
                    CREATE CONSTRAINT exclusive {
                        SET errmessage := "whoops";
                    }
                }
            };
        """)

        await self.con.execute(r"""
            INSERT Foo { a := "x" };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'whoops',
        ):
            await self.con.execute(r"""
                INSERT Foo { a := "x" };
            """)

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY a {
                    ALTER CONSTRAINT exclusive {
                        RESET errmessage;
                    }
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'a violates exclusivity constraint',
        ):
            await self.con.execute(r"""
                INSERT Foo { a := "x" };
            """)

    async def test_edgeql_ddl_drop_field_03(self):
        await self.con.execute(r"""

            CREATE ABSTRACT CONSTRAINT bogus {
                USING (false);
                SET errmessage := "never!";
            };

            CREATE TYPE Foo {
                CREATE CONSTRAINT bogus on (true);
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'never!',
        ):
            await self.con.execute(r"""
                INSERT Foo;
            """)

        await self.con.execute(r"""
            ALTER ABSTRACT CONSTRAINT bogus
            RESET errmessage;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'invalid Foo',
        ):
            await self.con.execute(r"""
                INSERT Foo;
            """)

    async def test_edgeql_ddl_bad_field_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "'ha' is not a valid field",
        ):
            await self.con.execute(r"""
                CREATE TYPE Lol {SET ha := "crash"};
            """)

    async def test_edgeql_ddl_bad_field_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "'ha' is not a valid field",
        ):
            await self.con.execute(r"""
                START MIGRATION TO {
                    type default::Lol {
                        ha := "crash"
                    }
                }
            """)

    async def test_edgeql_ddl_adjust_computed_01(self):
        await self.con.execute(r"""

            CREATE TYPE Foo {
                CREATE PROPERTY foo := {1, 2, 3};
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo SET MULTI;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo RESET CARDINALITY;
            };
        """)

    async def test_edgeql_ddl_adjust_computed_02(self):
        await self.con.execute(r"""

            CREATE TYPE Foo {
                CREATE PROPERTY foo := 1;
            };
        """)

        await self.con.execute(r"""
            INSERT Foo;
        """)

        await self.assert_query_result(
            "SELECT Foo { foo }",
            [{"foo": 1}],
        )

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo SET MULTI;
            };
        """)

        await self.assert_query_result(
            "SELECT Foo { foo }",
            [{"foo": [1]}],
        )

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo RESET CARDINALITY;
            };
        """)

        await self.assert_query_result(
            "SELECT Foo { foo }",
            [{"foo": 1}],
        )

    async def test_edgeql_ddl_adjust_computed_03(self):
        await self.con.execute(r"""

            CREATE TYPE Foo {
                CREATE PROPERTY foo := 1;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Pointer {
                    required,
                    has_required := contains(.computed_fields, "required")
                } FILTER .name = "foo"
            """,
            [{"required": True, "has_required": True}]
        )

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo SET OPTIONAL;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Pointer {
                    required,
                    has_required := contains(.computed_fields, "required")
                } FILTER .name = "foo"
            """,
            [{"required": False, "has_required": False}]
        )

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo RESET OPTIONALITY;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Pointer {
                    required,
                    has_required := contains(.computed_fields, "required")
                } FILTER .name = "foo"
            """,
            [{"required": True, "has_required": True}]
        )

        await self.con.execute(r"""
            ALTER TYPE Foo {
                ALTER PROPERTY foo SET REQUIRED;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT schema::Pointer {
                    required,
                    has_required := contains(.computed_fields, "required")
                } FILTER .name = "foo"
            """,
            [{"required": True, "has_required": False}]
        )

    async def test_edgeql_ddl_adjust_computed_04(self):
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY bar -> str;
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Foo { ALTER PROPERTY bar { USING ("1") } };
        ''')

        # Should work
        await self.con.execute(r'''
            INSERT Foo;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required property"):
            # Should fail because there is missing data
            # TODO: ask for fill_expr?
            await self.con.execute(r'''
                ALTER TYPE Foo { ALTER PROPERTY bar RESET EXPRESSION };
            ''')

        # Delete the data, then try
        await self.con.execute(r'''
            DELETE Foo;
            ALTER TYPE Foo { ALTER PROPERTY bar RESET EXPRESSION };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required property"):
            await self.con.execute(r'''
                INSERT Foo;
            ''')

    async def test_edgeql_ddl_adjust_computed_05(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE REQUIRED LINK bar -> Tgt;
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Foo { ALTER LINK bar {
                USING (assert_exists((SELECT Tgt LIMIT 1)))
            } };
        ''')

        # Should work
        await self.con.execute(r'''
            INSERT Foo;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required link"):
            # Should fail because there is missing data
            await self.con.execute(r'''
                ALTER TYPE Foo { ALTER LINK bar RESET EXPRESSION };
            ''')

        # Delete the data, then try
        await self.con.execute(r'''
            DELETE Foo;
            ALTER TYPE Foo { ALTER LINK bar RESET EXPRESSION };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required link"):
            await self.con.execute(r'''
                INSERT Foo;
            ''')

    async def test_edgeql_ddl_adjust_computed_06(self):
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE REQUIRED MULTI LINK bar -> Tgt;
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Foo { ALTER LINK bar {
                USING (assert_exists((SELECT Tgt)))
            } };
        ''')

        # Should work
        await self.con.execute(r'''
            INSERT Foo;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required link"):
            # Should fail because there is missing data
            await self.con.execute(r'''
                ALTER TYPE Foo { ALTER LINK bar RESET EXPRESSION };
            ''')

        # Delete the data, then try
        await self.con.execute(r'''
            DELETE Foo;
            ALTER TYPE Foo { ALTER LINK bar RESET EXPRESSION };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.MissingRequiredError,
                r"missing value for required link"):
            await self.con.execute(r'''
                INSERT Foo;
            ''')

    async def test_edgeql_ddl_adjust_computed_07(self):
        # Switching a property to computed and back should lose its data
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE PROPERTY bar -> str;
            };
            INSERT Foo { bar := "hello" };
            ALTER TYPE Foo { ALTER PROPERTY bar { USING ("world") } };
            ALTER TYPE Foo { ALTER PROPERTY bar RESET expression };
        ''')

        await self.assert_query_result(
            r"""
                SELECT Foo { bar }
            """,
            [
                {'bar': None}
            ]
        )

    async def test_edgeql_ddl_adjust_computed_08(self):
        # Switching a property to computed and back should lose its data
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE MULTI PROPERTY bar -> str;
            };
            INSERT Foo { bar := {"foo", "bar"} };
            ALTER TYPE Foo { ALTER PROPERTY bar { USING ({"a", "b"}) } };
            ALTER TYPE Foo { ALTER PROPERTY bar RESET expression };
        ''')

        await self.assert_query_result(
            r"""
                SELECT Foo { bar }
            """,
            [
                {'bar': []}
            ]
        )

    async def test_edgeql_ddl_adjust_computed_09(self):
        # Switching a link to computed and back should lose its data
        await self.con.execute(r'''
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE MULTI LINK bar -> Tgt;
            };
            INSERT Foo { bar := (INSERT Tgt) };
            ALTER TYPE Foo { ALTER LINK bar { USING (Tgt) } };
            ALTER TYPE Foo { ALTER LINK bar RESET expression };
        ''')

        await self.assert_query_result(
            r"""
                SELECT Foo { bar }
            """,
            [
                {'bar': []}
            ]
        )

    async def test_edgeql_ddl_adjust_computed_10(self):
        # Make sure everything gets cleaned up in this transition
        await self.con.execute(r'''
            CREATE TYPE Foo {
                CREATE MULTI PROPERTY bar -> str;
            };
            INSERT Foo { bar := {"foo", "bar"} };
            ALTER TYPE Foo { ALTER PROPERTY bar { USING ({"a", "b"}) } };
        ''')

        await self.assert_query_result(
            r"""
                DELETE Foo
            """,
            [
                {}
            ]
        )

    async def test_edgeql_ddl_adjust_computed_11(self):
        await self.con.execute(r'''
            CREATE TYPE default::Foo;
            CREATE TYPE default::Bar {
                CREATE LINK foos := (default::Foo);
            };
        ''')

        # it's annoying that we need the using on the RESET CARDINALITY;
        # maybe we should be able to know it isn't needed
        await self.con.execute(r'''
            ALTER TYPE default::Bar {
                ALTER LINK foos {
                    RESET EXPRESSION;
                    RESET CARDINALITY using (<Foo>{});
                    RESET OPTIONALITY;
                    SET TYPE default::Foo;
                };
            }
        ''')

    async def test_edgeql_ddl_captured_as_migration_01(self):

        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo := 1;
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    MODULE schema,
                    LM := (
                        SELECT Migration
                        FILTER NOT EXISTS(.<parents[IS Migration])
                    )
                    SELECT LM {
                        script
                    }
            """,
            [{
                'script': textwrap.dedent(
                    '''\
                    CREATE TYPE Foo {
                        CREATE PROPERTY foo := (1);
                    };'''
                )
            }]
        )

    async def test_edgeql_ddl_link_policy_01(self):
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo { CREATE MULTI LINK tgt -> Tgt; };
            CREATE TYPE Bar EXTENDING Foo;
        """)

        await self.con.execute(r"""
            INSERT Bar { tgt := (INSERT Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt;
            """)

    async def test_edgeql_ddl_link_policy_02(self):
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Base { CREATE MULTI LINK tgt -> Tgt; };
            CREATE TYPE Foo;
            ALTER TYPE Foo EXTENDING Base;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
        """)

        await self.con.execute(r"""
            DELETE Foo;
        """)

        await self.con.execute(r"""
            DELETE Tgt;
        """)

    async def test_edgeql_ddl_link_policy_03(self):
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Base;
            CREATE TYPE Foo EXTENDING Base { CREATE MULTI LINK tgt -> Tgt; };
            ALTER TYPE Base CREATE MULTI LINK foo -> Tgt;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                WITH D := Foo,
                SELECT {(DELETE D.tgt), (DELETE D)};
            """)

        await self.con.execute(r"""
            WITH D := Foo,
            SELECT {(DELETE D), (DELETE D.tgt)};
        """)

    async def test_edgeql_ddl_link_policy_04(self):
        # Make sure that a newly created subtype gets the appropriate
        # target link policies
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo { CREATE MULTI LINK tgt -> Tgt; };
            CREATE TYPE Tgt2 EXTENDING Tgt;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt2) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt2;
            """)

    async def test_edgeql_ddl_link_policy_05(self):
        # Make sure that a subtype with newly added bases gets the appropriate
        # target link policies
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo { CREATE MULTI LINK tgt -> Tgt; };
            CREATE TYPE Tgt2;
            ALTER TYPE Tgt2 EXTENDING Tgt;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt2) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt2;
            """)

        await self.con.execute(r"""
            DELETE Foo;
            ALTER TYPE Tgt2 DROP EXTENDING Tgt;
            DROP TYPE Foo;
        """)

        # Make sure that if we drop the base type, everything works right still
        await self.con.execute("""
            DELETE Tgt2;
        """)

    async def test_edgeql_ddl_link_policy_06(self):
        # Make sure that links coming into base types don't
        # interfere with link policies
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Tgt2 EXTENDING Tgt;
            CREATE TYPE Foo { CREATE MULTI LINK tgt -> Tgt2; };
            CREATE TYPE Bar { CREATE MULTI LINK tgt -> Tgt; };
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt2) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt2;
            """)

    async def test_edgeql_ddl_link_policy_07(self):
        # Make sure that swapping between deferred and not works
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE MULTI LINK tgt -> Tgt;
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo ALTER LINK tgt ON TARGET DELETE DEFERRED RESTRICT;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
        """)

        await self.con.execute("""
            DELETE Tgt;
            DELETE Foo;
        """)

    async def test_edgeql_ddl_link_policy_08(self):
        # Make sure that swapping between deferred and not works
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt;
            };
            ALTER TYPE Foo ALTER LINK tgt SET MULTI;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt;
            """)

        await self.con.execute("""
            DELETE Foo;
            DELETE Tgt;
        """)

    async def test_edgeql_ddl_link_policy_09(self):
        # Make sure that it still works after we rebase a link
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt;
            };
            CREATE TYPE Bar EXTENDING Foo {
                ALTER LINK tgt SET OWNED;
            };
            ALTER TYPE Bar DROP EXTENDING Foo;
        """)

        await self.con.execute(r"""
            INSERT Bar { tgt := (INSERT Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                DELETE Tgt;
            """)

        await self.con.execute("""
            DELETE Bar;
            DELETE Tgt;
        """)

    async def test_edgeql_ddl_link_policy_10(self):
        # Make sure we NULL out the pointer on the delete, which will
        # trigger the constraint
        await self.con.execute(r"""

            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt {
                    ON TARGET DELETE ALLOW;
                };
                CREATE CONSTRAINT expression on (EXISTS .tgt);

            };
            CREATE TYPE Bar EXTENDING Foo;
        """)

        await self.con.execute(r"""
            INSERT Bar { tgt := (INSERT Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'invalid Bar',
        ):
            await self.con.execute("""
                DELETE Tgt;
            """)

        await self.con.execute("""
            DELETE Bar;
            DELETE Tgt;
        """)

    async def test_edgeql_ddl_link_policy_11(self):
        await self.con.execute(r"""

            CREATE TYPE Tgt { CREATE PROPERTY name -> str };
            CREATE TYPE Foo {
                CREATE REQUIRED MULTI LINK tgt -> Tgt {
                    ON TARGET DELETE ALLOW;
                };
            };
            CREATE TYPE Bar EXTENDING Foo;
        """)

        await self.con.execute(r"""
            INSERT Bar { tgt := {(INSERT Tgt { name := "foo" }),
                                 (INSERT Tgt { name := "bar" })} };
            INSERT Bar { tgt := (INSERT Tgt { name := "foo" }) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required link 'tgt'",
        ):
            await self.con.execute("""
                DELETE Tgt FILTER .name = "foo";
            """)

        await self.con.execute("""
            DELETE Tgt FILTER .name = "bar";
            DELETE Bar;
            DELETE Tgt;
        """)

    async def test_edgeql_ddl_link_policy_12(self):
        await self.con.execute("""
            create type Tgt;
            create type Foo {
                create link tgt -> Tgt {
                    on target delete allow;
                }
            };
            create type Bar extending Foo {
                alter link tgt {
                    on target delete restrict;
                }
            };
        """)

        # Make sure we can still delete on Foo
        await self.con.execute("""
            insert Foo { tgt := (insert Tgt) };
            delete Tgt;
        """)

        await self.con.execute("""
             insert Bar { tgt := (insert Tgt) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'prohibited by link target policy',
        ):
            await self.con.execute("""
                delete Tgt;
            """)

        await self.con.execute("""
            alter type Bar {
                alter link tgt {
                    reset on target delete;
                }
            };
        """)

        await self.con.execute("""
            delete Tgt
        """)

        await self.assert_query_result(
            r"""
                select schema::Link {name, on_target_delete, source: {name}}
                filter .name = 'tgt';

            """,
            tb.bag([
                {
                    "name": "tgt",
                    "on_target_delete": "Allow",
                    "source": {"name": "default::Foo"}
                },
                {
                    "name": "tgt",
                    "on_target_delete": "Allow",
                    "source": {"name": "default::Bar"}
                }
            ]),
        )

    async def test_edgeql_ddl_link_policy_13(self):
        # Make sure that swapping between delete target and not works
        await self.con.execute(r"""
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt;
            };
            ALTER TYPE Foo ALTER LINK tgt ON SOURCE DELETE DELETE TARGET;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
            DELETE Foo;
        """)

        await self.assert_query_result(
            'select Tgt',
            [],
        )

        await self.con.execute(r"""
            ALTER TYPE Foo ALTER LINK tgt ON SOURCE DELETE ALLOW;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
            DELETE Foo;
        """)

        await self.assert_query_result(
            'select Tgt',
            [{}],
        )

    async def test_edgeql_ddl_link_policy_14(self):
        # Make sure that it works when changing cardinality
        await self.con.execute(r"""
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt {
                    ON SOURCE DELETE DELETE TARGET;
                }
            };
            ALTER TYPE Foo ALTER LINK tgt SET MULTI;
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := (INSERT Tgt) };
        """)

        await self.con.execute("""
            DELETE Foo;
        """)
        await self.assert_query_result(
            'select Tgt',
            [],
        )

    async def test_edgeql_ddl_link_policy_15(self):
        # Make sure that it works when changing cardinality
        await self.con.execute(r"""
            CREATE TYPE Tgt;
            CREATE TYPE Foo {
                CREATE LINK tgt -> Tgt {
                    ON SOURCE DELETE DELETE TARGET;
                }
            };
            CREATE TYPE Bar EXTENDING Foo;
        """)

        await self.con.execute(r"""
            INSERT Bar { tgt := (INSERT Tgt) };
        """)

        await self.con.execute("""
            DELETE Foo;
        """)
        await self.assert_query_result(
            'select Tgt',
            [],
        )

    async def test_edgeql_ddl_link_policy_16(self):
        # Make sure that it works when changing cardinality
        await self.con.execute(r"""
            CREATE TYPE Tgt;
            CREATE TYPE Tgt2 EXTENDING Tgt;
            CREATE TYPE Tgt3;
            CREATE TYPE Foo {
                CREATE MULTI LINK tgt -> Tgt | Tgt3 {
                    ON SOURCE DELETE DELETE TARGET;
                }
            };
        """)

        await self.con.execute(r"""
            INSERT Foo { tgt := {(INSERT Tgt), (INSERT Tgt2), (INSERT Tgt3)} };
        """)

        await self.con.execute("""
            DELETE Foo;
        """)
        await self.assert_query_result(
            'select Tgt UNION Tgt3',
            [],
        )

    async def test_edgeql_ddl_link_policy_implicit_01(self):
        await self.con.execute("""
            create type T;
            create type X {
                create link foo -> schema::ObjectType;
            };
        """)
        await self.con.execute("""
            drop type T;
        """)

    async def test_edgeql_ddl_dupe_link_storage_01(self):
        await self.con.execute(r"""

            CREATE TYPE Foo {
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE Bar {
                CREATE PROPERTY name -> str;
                CREATE LINK foo -> Foo;
                CREATE PROPERTY x -> int64;
            };
            CREATE TYPE Baz {
                CREATE PROPERTY name -> str;
                CREATE MULTI LINK foo -> Foo;
                CREATE MULTI PROPERTY x -> int64
            };
            INSERT Foo { name := "foo" };
            INSERT Bar { name := "bar", foo := (SELECT Foo LIMIT 1), x := 1 };
            INSERT Baz { name := "baz", foo := (SELECT Foo), x := {2, 3} };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo {bars := .<foo[IS Bar] {name}};
            """,
            [{"bars": [{"name": "bar"}]}],
        )

        await self.assert_query_result(
            r"""
                SELECT (Bar UNION Baz).foo { name };
            """,
            [{"name": "foo"}]
        )

        await self.assert_query_result(
            r"""
                WITH W := (Bar UNION Baz)
                SELECT _ := (W { name }, W.foo) ORDER BY _.0.name;
            """,
            [
                [{"name": "bar"}, {}], [{"name": "baz"}, {}]
            ],
        )

        await self.con.execute(r"""
            WITH W := (Bar UNION Baz), SELECT (W, W.foo.id);
        """)

    async def test_edgeql_ddl_no_volatile_computable_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "volatile functions are not permitted in schema-defined "
            "computed expressions",
        ):
            await self.con.execute("""
                CREATE TYPE Foo {
                    CREATE PROPERTY foo := random();
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "volatile functions are not permitted in schema-defined "
            "computed expressions",
        ):
            await self.con.execute("""
                CREATE TYPE Foo {
                    CREATE PROPERTY foo := (SELECT {
                        asdf := random()
                    }).asdf
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "volatile functions are not permitted in schema-defined "
            "computed expressions",
        ):
            await self.con.execute("""
                CREATE TYPE Noob {
                    CREATE MULTI LINK friends -> Noob;
                    CREATE LINK best_friends := (
                        SELECT .friends FILTER random() > 0.5
                    );
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "volatile functions are not permitted in schema-defined "
            "computed expressions",
        ):
            await self.con.execute("""
                CREATE TYPE Noob {
                    CREATE LINK noob -> Noob {
                        CREATE PROPERTY foo := random();
                    }
                }
            """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "volatile functions are not permitted in schema-defined "
            "computed expressions",
        ):
            await self.con.execute("""
                CREATE ALIAS Asdf := Object { foo := random() };
            """)

    async def test_edgeql_ddl_new_required_pointer_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required property 'name' of object type "
            "'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo CREATE REQUIRED PROPERTY name -> str;
            """)

    async def test_edgeql_ddl_new_required_pointer_02(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY num -> int64;
            };
            INSERT Foo { num := 20 };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE PROPERTY name -> str {
                    SET REQUIRED USING (<str>.num ++ "!")
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {name, num}''',
            [{'name': '20!', 'num': 20}]
        )

    async def test_edgeql_ddl_new_required_pointer_03(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY num -> int64;
            };
            INSERT Foo { num := 20 };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE MULTI PROPERTY name -> str {
                    SET REQUIRED USING (<str>.num ++ "!")
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {name, num}''',
            [{'name': ['20!'], 'num': 20}]
        )

    async def test_edgeql_ddl_new_required_pointer_04(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY num -> int64;
            };
            CREATE TYPE Bar {
                CREATE PROPERTY code -> int64 {
                    CREATE CONSTRAINT exclusive;
                }
            };
            INSERT Foo { num := 20 };
            INSERT Bar { code := 40 };
            INSERT Foo { num := 30 };
            INSERT Bar { code := 60 };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE LINK partner -> Bar {
                    SET REQUIRED USING (SELECT Bar FILTER Bar.code = 2*Foo.num)
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {num, partner: {code}} ORDER BY .num''',
            [
                {'num': 20, 'partner': {'code': 40}},
                {'num': 30, 'partner': {'code': 60}},
            ]
        )

    async def test_edgeql_ddl_new_required_pointer_05(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY num -> int64;
            };
            CREATE TYPE Bar {
                CREATE PROPERTY code -> int64 {
                    CREATE CONSTRAINT exclusive;
                }
            };
            INSERT Foo { num := 20 };
            INSERT Bar { code := 40 };
            INSERT Foo { num := 30 };
            INSERT Bar { code := 60 };
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE MULTI LINK partner -> Bar {
                    SET REQUIRED USING (SELECT Bar FILTER Bar.code = 2*Foo.num)
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {num, partner: {code}} ORDER BY .num''',
            [
                {'num': 20, 'partner': [{'code': 40}]},
                {'num': 30, 'partner': [{'code': 60}]},
            ]
        )

    async def test_edgeql_ddl_new_required_pointer_06(self):
        await self.con.execute(r"""
            CREATE ABSTRACT TYPE Bar  {
                CREATE PROPERTY num -> int64;
            };
            CREATE TYPE Foo EXTENDING Bar;
            INSERT Foo { num := 20 };
        """)

        await self.con.execute("""
            ALTER TYPE Bar {
                CREATE PROPERTY name -> str {
                    SET REQUIRED USING (<str>.num ++ "!")
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {name, num}''',
            [{'name': '20!', 'num': 20}]
        )

    async def test_edgeql_ddl_new_required_pointer_07(self):
        await self.con.execute(r"""
            CREATE ABSTRACT TYPE Bar  {
                CREATE PROPERTY num -> int64;
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE Foo EXTENDING Bar;
            INSERT Foo { num := 20 };
        """)

        await self.con.execute("""
            ALTER TYPE Bar {
                ALTER PROPERTY name {
                    SET REQUIRED USING (<str>.num ++ "!")
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {name, num}''',
            [{'name': '20!', 'num': 20}]
        )

    async def test_edgeql_ddl_new_required_pointer_08(self):
        await self.con.execute(r"""
            CREATE TYPE Bar  {
                CREATE PROPERTY num -> int64;
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE Foo EXTENDING Bar;
            INSERT Bar { num := 10 };
            INSERT Foo { num := 20 };
        """)

        await self.con.execute("""
            ALTER TYPE Bar {
                ALTER PROPERTY name {
                    SET REQUIRED USING (<str>.num ++ "!")
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Bar {name, num} ORDER BY .num''',
            [
                {'name': '10!', 'num': 10},
                {'name': '20!', 'num': 20},
            ]
        )

    async def test_edgeql_ddl_new_required_pointer_09(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;
        """)

        await self.con.execute("""
            ALTER TYPE Foo {
                CREATE MULTI PROPERTY name -> str {
                    SET REQUIRED USING ({"hello", "world"})
                }
            }
        """)

        await self.assert_query_result(
            r'''SELECT Foo {name}''',
            [{'name': {'hello', 'world'}}]
        )

    async def test_edgeql_ddl_new_required_multi_pointer_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required property 'name' of object type "
            "'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo CREATE REQUIRED MULTI PROPERTY name -> str;
            """)

    async def test_edgeql_ddl_new_required_multi_pointer_02(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            INSERT Foo;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required link 'link' of object type "
            "'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo CREATE REQUIRED MULTI LINK link -> Object;
            """)

    async def test_edgeql_ddl_new_required_multi_pointer_03(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE MULTI PROPERTY name -> str;
            };
            INSERT Foo;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required property 'name' of object type "
            "'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER PROPERTY name SET REQUIRED;
            """)

    async def test_edgeql_ddl_new_required_multi_pointer_04(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE MULTI LINK link -> Object;
            };
            INSERT Foo;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required link 'link' of object type "
            "'default::Foo'"
        ):
            await self.con.execute("""
                ALTER TYPE Foo ALTER LINK link SET REQUIRED;
            """)

    async def test_edgeql_ddl_link_union_delete_01(self):
        await self.con.execute(r"""
            CREATE TYPE default::M;
            CREATE ABSTRACT TYPE default::Base {
                CREATE LINK l -> default::M;
            };
            CREATE TYPE default::A EXTENDING default::Base;
            CREATE TYPE default::B EXTENDING default::Base;
            CREATE TYPE default::L {
                CREATE LINK l -> (default::B | default::A);
            };
            CREATE TYPE ForceRedo {
                CREATE LINK l -> default::M;
            };
        """)
        await self.con.execute(r"""
            insert M;
        """)
        await self.con.execute(r"""
            delete M;
        """)

    async def test_edgeql_ddl_alter_union_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            CREATE TYPE Bar;
        """)

        await self.con.execute(r"""
            CREATE TYPE Ref {
                CREATE LINK fubar -> Foo | Bar;
            }
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo CREATE PROPERTY x -> str;
            ALTER TYPE Bar CREATE PROPERTY x -> str;
        """)

        await self.assert_query_result(
            r'''SELECT Ref.fubar.x''',
            [],
        )

    async def test_edgeql_ddl_alter_union_02(self):
        await self.con.execute(r"""
            CREATE TYPE Foo { CREATE PROPERTY x -> str; };
            CREATE TYPE Bar { CREATE PROPERTY x -> str; };
            CREATE TYPE Baz { CREATE PROPERTY x -> str; };
        """)

        await self.con.execute(r"""
            CREATE TYPE Ref {
                CREATE LINK everything -> Foo | Bar | Baz;
                CREATE LINK fubar -> Foo | Bar;
                CREATE LINK barbaz -> Bar | Baz;
            }
        """)

        await self.con.execute(r"""
            ALTER TYPE Baz DROP PROPERTY x;
        """)

        await self.assert_query_result(
            r'''SELECT Ref.fubar.x''',
            [],
        )

        await self.con.execute(r"""
            ALTER TYPE Baz CREATE PROPERTY x -> str;
        """)

        await self.assert_query_result(
            r'''SELECT Ref.everything.x''',
            [],
        )

    async def test_edgeql_ddl_alter_union_03(self):
        await self.con.execute(r"""
            CREATE TYPE Parent;
            CREATE TYPE Child EXTENDING Parent {
                CREATE PROPERTY prop -> str;
            };
            CREATE TYPE Foo {CREATE LINK y -> Child};
            CREATE TYPE Bar {CREATE LINK y -> Child};
        """)

        await self.con.execute(r"""
            CREATE TYPE Ref {
                CREATE LINK fubar -> Foo | Bar;
            }
        """)

        await self.con.execute(r"""
            ALTER TYPE Foo ALTER LINK y SET TYPE Parent;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "object type 'default::Parent' has no link or property 'prop'",
        ):
            await self.assert_query_result(
                r'''SELECT Ref.fubar.y.prop''',
                [],
            )

    async def test_edgeql_ddl_extending_scalar_wrongly(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "'str' exists, but is a scalar type, not an object type",
            _line=1, _col=29
        ):
            await self.con.execute(
                r'''CREATE TYPE MyStr EXTENDING str;''',
            )

    async def test_edgeql_ddl_required_computed_01(self):
        await self.con.execute(r'''
            CREATE TYPE Profile;
            CREATE TYPE User {
                CREATE REQUIRED SINGLE LINK profile -> Profile;
            };
        ''')

        await self.con.execute(r'''
            ALTER TYPE Profile {
                CREATE REQUIRED LINK user := (std::assert_exists((SELECT
                    .<profile[IS User]
                )));
            };
            ALTER TYPE Profile {
                ALTER LINK user SET OPTIONAL;
            };
        ''')

    async def test_edgeql_ddl_required_computed_02(self):
        await self.con.execute(r'''
            CREATE TYPE Foo;
            ALTER TYPE Foo {
                CREATE PROPERTY z := {1, 2};
            };
            ALTER TYPE Foo {
                ALTER PROPERTY z SET OPTIONAL;
            };
        ''')

    async def test_edgeql_ddl_recursive_func(self):
        await self.con.execute(r'''
            CREATE TYPE SomeThing {
                CREATE LINK child -> SomeThing;
            }
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "function 'get_all_children_ordered' does not exist"
        ):
            await self.con.execute(r'''
                CREATE FUNCTION get_all_children_ordered(parent: SomeThing)
                -> SET OF SomeThing Using (
                    SELECT SomeThing UNION get_all_children_ordered(parent))
            ''')

        await self.con.execute(r'''
            CREATE FUNCTION get_all_children_ordered(parent: SomeThing)
            -> SET OF SomeThing Using (
                SELECT SomeThing
            )
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"function 'default::get_all_children_ordered"
            r"\(parent: default::SomeThing\)' is defined recursively"
        ):
            await self.con.execute(r'''
                ALTER FUNCTION get_all_children_ordered(parent: SomeThing)
                USING (
                    SELECT parent.child
                        UNION get_all_children_ordered(parent)
                );
            ''')

    async def test_edgeql_ddl_duplicates_01(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
        """)

        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"object type 'default::Foo' already exists"):
            await self.con.execute(r"""
                CREATE TYPE Foo;
            """)

    async def test_edgeql_ddl_duplicates_02(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> str;
            }
        """)

        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"property 'foo' of "
                r"object type 'default::Foo' already exists"):
            await self.con.execute(r"""
                ALTER TYPE Foo {
                    CREATE PROPERTY foo -> str;
                }
            """)

    async def test_edgeql_ddl_duplicates_03(self):
        await self.con.execute(r"""
            CREATE TYPE Foo;
            CREATE TYPE Bar;
        """)

        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"object type 'default::Foo' already exists"):
            await self.con.execute(r"""
                ALTER TYPE Bar RENAME TO Foo;
            """)

    async def test_edgeql_ddl_duplicates_04(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> str;
                CREATE PROPERTY bar -> str;
            }
        """)

        with self.assertRaisesRegex(
                edgedb.errors.SchemaError,
                r"property 'foo' of "
                r"object type 'default::Foo' already exists"):
            await self.con.execute(r"""
                ALTER TYPE Foo {
                    ALTER PROPERTY bar RENAME TO foo;
                }
            """)

    async def test_edgeql_ddl_alias_in_computable_01(self):
        await self.con.execute(r"""
            CREATE ALIAS Alias := {0, 1, 2, 3};
        """)

        # We don't want to prohibit this forever, but we need to for now.
        async with self.assertRaisesRegexTx(
                edgedb.errors.UnsupportedFeatureError,
                r"referring to alias 'default::Alias' from computed property"):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar := Alias;
                };
            """)

        async with self.assertRaisesRegexTx(
                edgedb.errors.UnsupportedFeatureError,
                r"referring to alias 'default::Alias' from computed property"):
            await self.con.execute(r"""
                CREATE TYPE Foo {
                    CREATE PROPERTY bar := {Alias, Alias};
                };
            """)

    async def test_edgeql_ddl_linkprop_partial_paths(self):
        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE LINK x -> Object {
                    CREATE PROPERTY z -> str;
                    CREATE CONSTRAINT expression ON (@z != "lol");
                    CREATE INDEX ON (@z);
                    CREATE PROPERTY y := @z ++ "!";
                };
            };
        """)

    async def test_edgeql_ddl_drop_parent_multi_link(self):
        await self.con.execute(r"""
            CREATE TYPE C;
            CREATE TYPE D {
                CREATE MULTI LINK multi_link -> C;
            };
            CREATE TYPE E EXTENDING D;
            INSERT C;
        """)

        await self.con.execute(r"""
            ALTER TYPE D {
                DROP LINK multi_link;
            };
        """)

        await self.con.execute(r"""
            DELETE C;
        """)

    async def test_edgeql_ddl_drop_multi_parent_multi_link(self):
        await self.con.execute(r"""
            CREATE TYPE C;
            INSERT C;
            CREATE TYPE D {
                CREATE MULTI LINK multi_link -> C;
            };
            CREATE TYPE E {
                CREATE MULTI LINK multi_link -> C;
            };
            CREATE TYPE F EXTENDING D, E;
        """)

        await self.con.execute(r"""
            ALTER TYPE D {
                DROP LINK multi_link;
            };
        """)

        await self.con.execute(r"""
            DELETE C;
        """)

    async def test_edgeql_ddl_drop_incoming_link(self):
        await self.con.execute(r"""
            create type Foo;
            create type Bar { create link foo -> Foo; };
            alter type Bar { drop link foo; };
            insert Foo;
            delete Foo;
        """)

    async def test_edgeql_ddl_switch_link_to_computed(self):
        await self.con.execute(r"""
            create type Identity;
            create type User {
                create required property name -> str {
                    create constraint exclusive;
                };
                create multi link identities -> Identity {
                    create constraint exclusive;
                };
            };
            alter type Identity {
                create link user -> User {
                    on target delete delete source;
                };
            };
        """)

        await self.con.execute(r"""
            alter type User {
                alter link identities {
                    drop constraint exclusive;
                };
                alter link identities {
                    using (.<user[IS Identity]);
                };
            };
        """)

        await self.con.execute(r"""
            insert Identity { user := (insert User { name := 'foo' }) }
        """)
        await self.con.execute(r"""
            delete User filter true
        """)

    async def test_edgeql_ddl_switch_link_target(self):
        await self.con.execute(r"""
            create type Foo;
            create type Bar;
            create type Ptr { create link p -> Foo; };
            alter type Ptr { alter link p set type Bar using (<Bar>{}); };
            insert Ptr { p := (insert Bar) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "prohibited by link target policy",
        ):
            await self.con.execute("""
                delete Bar;
            """)

        await self.con.execute(r"""
            drop type Ptr;
        """)
        await self.con.execute(r"""
            insert Foo;
            delete Foo;
        """)

    async def test_edgeql_ddl_set_abs_linkprop_type(self):
        await self.con.execute(r"""
            CREATE ABSTRACT LINK orderable {
                CREATE PROPERTY orderVal -> str {
                    CREATE DELEGATED CONSTRAINT exclusive;
                };
            };
            CREATE ABSTRACT TYPE Entity;
            CREATE TYPE Video EXTENDING Entity;
            CREATE TYPE Topic EXTENDING Entity {
                CREATE MULTI LINK videos EXTENDING orderable -> Video;
            };
        """)

        await self.con.execute(r"""
            ALTER ABSTRACT LINK orderable {
                ALTER PROPERTY orderVal {
                    SET TYPE decimal using (<decimal>@orderVal);
                };
            };
        """)

    async def test_edgeql_ddl_set_multi_with_children_01(self):
        await self.con.execute(r"""
            create type Person { create link lover -> Person; };
            create type NPC extending Person;
            alter type Person { alter link lover { set multi; }; };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_multi_with_children_02(self):
        await self.con.execute(r"""
            create abstract type Person { create link lover -> Person; };
            create type NPC extending Person;
            alter type Person { alter link lover { set multi; }; };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_multi_with_children_03(self):
        await self.con.execute(r"""
            create type Person { create property foo -> str; };
            create type NPC extending Person;
            alter type Person { alter property foo { set multi; }; };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_multi_with_children_04(self):
        await self.con.execute(r"""
            create abstract type Person { create property foo -> str; };
            create type NPC extending Person;
            alter type Person { alter property foo { set multi; }; };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_single_with_children_01(self):
        await self.con.execute(r"""
            create abstract type Person { create multi link foo -> Person; };
            create type NPC extending Person;
            alter type Person alter link foo {
                set single USING (SELECT .foo LIMIT 1);
            };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_single_with_children_02(self):
        await self.con.execute(r"""
            create abstract type Person { create multi property foo -> str; };
            create type NPC extending Person;
            alter type Person alter property foo {
                set single USING (SELECT .foo LIMIT 1);
            };
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_drop_multi_child_01(self):
        await self.con.execute(r"""
            create abstract type Person { create multi property foo -> str; };
            create type NPC extending Person;
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_drop_multi_child_02(self):
        await self.con.execute(r"""
            create abstract type Person { create multi link foo -> Person; };
            create type NPC extending Person;
        """)

        await self.con.execute(r"""
            drop type NPC;
            drop type Person;
        """)

    async def test_edgeql_ddl_set_abstract_bogus_01(self):
        await self.con.execute(r"""
            create type Foo;
            insert Foo;
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"may not make non-empty object type 'default::Foo' abstract"):
            await self.con.execute(r"""
                alter type Foo set abstract;
            """)

    async def test_edgeql_no_type_intro_in_default(self):
        await self.con.execute(r"""
            create scalar type Foo extending sequence;
            create type Project {
                create required property number -> Foo {
                    set default := sequence_next(introspect Foo);
                }
            };
        """)

        await self.con.execute(r"""
            insert Project;
            insert Project;
        """)

    async def test_edgeql_ddl_no_shapes_in_using(self):
        await self.con.execute(r"""
            create type Foo;
            create type Bar extending Foo;
            create type Baz {
                create multi link foo -> Foo;
            };
        """)

        q = 'select Bar { x := "oops" } limit 1'
        alters = [
            f'set required using ({q})',
            f'set single using ({q})',
            f'set default := ({q})',
            f'set type Bar using ({q})',
        ]

        for alter in alters:
            async with self.assertRaisesRegexTx(
                    (edgedb.SchemaError, edgedb.SchemaDefinitionError),
                    r"may not include a shape"):
                await self.con.execute(fr"""
                    alter type Baz {{
                        alter link foo {{
                            {alter}
                        }}
                     }};
                """)

    async def test_edgeql_ddl_uuid_array_01(self):
        await self.con.execute(r"""
            create type Foo {
                create property uuid_array_prop -> array<uuid>
            }
        """)

        await self.assert_query_result(
            r"""
                select schema::Property {target: {name}}
                filter .name = 'uuid_array_prop';
            """,
            [{'target': {'name': 'array<std::uuid>'}}]
        )

    async def test_edgeql_ddl_computed_and_alias(self):
        await self.con.execute(r"""
            create type Tgt;
            create type X { create link foo -> Tgt };
            create alias Y := X { foo: {id} };
            alter type X { create link bar := .foo };
        """)

    async def test_edgeql_ddl_rebase_views_01(self):
        await self.con.execute(r"""
            CREATE TYPE default::Foo {
                CREATE PROPERTY x -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE default::Bar EXTENDING default::Foo;
            CREATE TYPE default::Baz EXTENDING default::Foo;
        """)

        await self.con.execute(r"""
            CREATE TYPE default::Foo2 EXTENDING default::Foo;
            ALTER TYPE default::Bar {
                DROP EXTENDING default::Foo;
                EXTENDING default::Foo2 LAST;
            };

            INSERT Bar;
        """)

        # should still be in the view
        await self.assert_query_result(
            'select Foo',
            [{}],
        )

        await self.assert_query_result(
            'select Object',
            [{}],
        )

    async def test_edgeql_ddl_rebase_views_02(self):
        await self.con.execute(r"""
            CREATE TYPE default::Foo {
                CREATE PROPERTY x -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE default::Bar EXTENDING default::Foo;
            CREATE TYPE default::Baz EXTENDING default::Foo;
        """)

        await self.con.execute(r"""
            CREATE TYPE default::Foo2 {
                CREATE PROPERTY x -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };
            ALTER TYPE default::Bar {
                DROP EXTENDING default::Foo;
                EXTENDING default::Foo2 LAST;
            };

            INSERT Bar;
        """)

        # should *not* still be in the view
        await self.assert_query_result(
            'select Foo',
            [],
        )

        await self.assert_query_result(
            'select Object',
            [{}],
        )

    async def test_edgeql_ddl_alias_and_create_set_required(self):
        await self.con.execute(r"""
            create type T;
            create alias A := T;
            alter type T {
                create required property bar -> str {
                    set required using ('!')
                }
            };
        """)


class TestConsecutiveMigrations(tb.DDLTestCase):
    TRANSACTION_ISOLATION = False

    async def test_edgeql_ddl_consecutive_create_migration_01(self):
        # A regression test for https://github.com/edgedb/edgedb/issues/2085.
        await self.con.execute('''
        CREATE MIGRATION m1dpxyvsejl6b2tqe5nzpy6wpk5zzjhm7gwky7jn5vmnqrqoujxn6q
            ONTO initial
        {
            CREATE TYPE default::A;
        };
        ''')
        await self.con.query('''
        CREATE MIGRATION m1xuduby4e6u2sraygw352y553ltcj4cyz4dijuwlbqqq34ap43yca
            ONTO m1dpxyvsejl6b2tqe5nzpy6wpk5zzjhm7gwky7jn5vmnqrqoujxn6q
        {
            CREATE TYPE default::B;
        };
        ''')
