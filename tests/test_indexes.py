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


import edgedb

from edb.testbase import server as tb


class TestIndexes(tb.DDLTestCase):

    async def test_index_01(self):
        await self.migrate(r"""
            type Person {
                property first_name -> str;
                property last_name -> str;

                index on ((.first_name, .last_name));
            };

            type Person2 extending Person;
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    schema::ObjectType {
                        indexes: {
                            expr
                        }
                    }
                FILTER schema::ObjectType.name = 'default::Person';
            """,
            [{
                'indexes': [{
                    'expr': '(.first_name, .last_name)',
                }]
            }],
        )

        await self.con.execute(r"""
            INSERT Person {
                first_name := 'Elon',
                last_name := 'Musk'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    Person {
                        first_name
                    }
                FILTER
                    Person.first_name = 'Elon' AND Person.last_name = 'Musk';
            """,
            [{
                'first_name': 'Elon'
            }]
        )

        await self.con.execute(
            """
                ALTER TYPE Person
                DROP INDEX ON ((.first_name, .last_name));
            """
        )

        await self.assert_query_result(
            r"""
                SELECT
                    schema::ObjectType {
                        indexes: {
                            expr
                        }
                    }
                FILTER schema::ObjectType.name = 'default::Person';
            """,
            [{
                'indexes': []
            }],
        )

    async def test_index_02(self):
        await self.con.execute(r"""
            # setup delta
            CREATE TYPE User {
                CREATE PROPERTY title -> str;
                CREATE INDEX ON (.title);
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    schema::ObjectType {
                        indexes: {
                            expr
                        }
                    }
                FILTER .name = 'default::User';
            """,
            [{
                'indexes': [{
                    'expr': '.title'
                }]
            }],
        )

        # simply test that the type can be dropped
        await self.con.execute(r"""
            DROP TYPE User;
        """)

    async def test_index_03(self):
        await self.con.execute(r"""
            CREATE TYPE User {
                CREATE PROPERTY name -> str;
                CREATE PROPERTY title -> str;
                CREATE INDEX ON (.title);
            }
        """)

        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            r"index on \(.name\) does not exist on object type "
            r"'default::User'",
        ):
            await self.con.execute("""
                ALTER TYPE User DROP INDEX ON (.name)
            """)

    async def test_index_04(self):
        await self.con.execute(r"""
            CREATE TYPE User {
                CREATE PROPERTY title -> str;
                CREATE INDEX ON (.title ?? "N/A");
            }
        """)

    async def test_index_05(self):
        await self.con.execute(
            """
            CREATE TYPE ObjIndex1 {
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE ObjIndex2 {
                CREATE MULTI PROPERTY first_name -> str;
                CREATE PROPERTY last_name -> str;
                CREATE LINK foo -> ObjIndex1 {
                    CREATE PROPERTY p -> str;
                };

                CREATE INDEX ON (__subject__.last_name);
            };
            """
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "cannot use SET OF operator 'std::EXISTS' "
            "in an index expression",
        ):
            await self.con.execute(
                """
                ALTER TYPE ObjIndex2 {
                    CREATE INDEX on (EXISTS .first_name);
                };
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "cannot use SET OF function 'std::count' "
            "in an index expression",
        ):
            await self.con.execute(
                """
                ALTER TYPE ObjIndex2 {
                    CREATE PROPERTY first_name_count
                        := count(.first_name);
                    CREATE INDEX ON (.first_name_count);
                };
                """
            )

        await self.con.execute(
            """
            ALTER TYPE ObjIndex2 {
                CREATE PROPERTY last_name_len := len(.last_name);
                CREATE INDEX ON (.last_name_len);
            };
            """
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "index expressions must be immutable",
        ):
            await self.con.execute(
                """
                ALTER TYPE ObjIndex2 {
                    CREATE INDEX ON (.foo@p);
                };
                """
            )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "index expressions must be immutable",
        ):
            await self.con.execute(
                """
                ALTER TYPE ObjIndex2 {
                    CREATE INDEX ON (.foo.name);
                };
                """
            )

    async def test_index_06(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            r"index of object type 'default::Foo' already exists"
        ):
            await self.con.execute(r"""
                create type Foo{
                    create property val -> str;
                    create index on (.val);
                    create index on (.val);
                };
            """)

    async def test_index_07(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            r"index.+fts::index"
            r".+of object type 'default::Foo' already exists",
        ):
            await self.con.execute(
                r"""
                create type Foo{
                    create property val -> str;
                    create index fts::index on (
                        fts::with_options(.val, language := fts::Language.eng));
                    create index fts::index on (
                        fts::with_options(.val, language := fts::Language.eng));
                    create index fts::index on (
                        fts::with_options(.val, language := fts::Language.eng));
                };
                """
            )

    async def test_index_08(self):
        await self.con.execute(
            r"""
            # setup delta
            create type ObjIndex3 {
                create property name -> str;
                create index fts::index on (
                    fts::with_options(.name, language := fts::Language.eng)
                );
            };
            """
        )

        await self.assert_query_result(
            r"""
                select
                    schema::ObjectType {
                        indexes: {
                            name,
                            kwargs,
                            expr,
                            abstract,
                        }
                    }
                filter .name = 'default::ObjIndex3';
            """,
            [
                {
                    'indexes': [
                        {
                            'name': 'fts::index',
                            'kwargs': [],
                            'expr': (
                                'std::fts::with_options(.name, '
                                'language := std::fts::Language.eng)'
                            ),
                            'abstract': False,
                        }
                    ]
                }
            ],
        )

    async def test_index_09(self):
        await self.con.execute(
            r"""
            # setup delta
            create abstract index MyIndex extending fts::index;

            create type ObjIndex4 {
                create property name -> str;
                create index MyIndex on (
                    fts::with_options(.name, language := fts::Language.eng)
                );
            };
            """
        )

        await self.assert_query_result(
            r"""
                select
                    schema::ObjectType {
                        indexes: {
                            name,
                            kwargs,
                            expr,
                            abstract,
                        }
                    }
                filter .name = 'default::ObjIndex4';
            """,
            [
                {
                    'indexes': [
                        {
                            'name': 'default::MyIndex',
                            'kwargs': [],
                            'expr': (
                                'std::fts::with_options(.name, '
                                'language := std::fts::Language.eng)'
                            ),
                            'abstract': False,
                        }
                    ]
                }
            ],
        )

        await self.assert_query_result(
            r"""
                select
                    schema::Index {
                        name,
                        kwargs,
                        abstract,
                        ancestors[is schema::Index]: {
                            name,
                            params: {
                                name,
                                type_name := .type.name,
                                default,
                            },
                            abstract,
                        },
                    }
                filter .name = 'default::MyIndex' and .abstract = true;
            """,
            [
                {
                    'name': 'default::MyIndex',
                    'kwargs': [],
                    'abstract': True,
                    'ancestors': [
                        {
                            'name': 'std::fts::index',
                            'params': [],
                            'abstract': True,
                        }
                    ],
                }
            ],
        )

    async def test_index_10(self):
        with self.assertRaisesRegex(
            edgedb.SchemaDefinitionError,
            r"possibly more than one element returned",
        ):
            await self.con.execute(
                r"""
                create type Foo {
                    create property val -> str;

                    create index on (Foo.val);
                };
                """
            )

    async def test_index_11(self):
        # indexes should not be rebased, but should be dropped and recreated

        await self.con.execute(
            '''
            create type Hello {
                create required property world: str;
                create index pg::btree on (.world);
            };
            '''
        )
        await self.migrate(
            r"""
            type Hello {
                property world: str;
                index pg::hash on (.world);
            };
            """
        )
        await self.migrate(
            r"""
            type Hello {
                property world: str;
                index pg::btree on (.world);
            };
            """
        )
