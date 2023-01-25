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
            "cannot use aggregate functions or operators in an "
            "index expression",
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
            "cannot use aggregate functions or operators in an "
            "index expression",
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
            r"index.+fts::textsearch\(language:='english'\)"
            r".+of object type 'default::Foo' already exists"
        ):
            await self.con.execute(r"""
                create type Foo{
                    create property val -> str;
                    create index fts::textsearch(language := 'english')
                        on (.val);
                    create index fts::textsearch(language := 'spanish')
                        on (.val);
                    create index fts::textsearch(language := 'english')
                        on (.val);
                };
            """)
