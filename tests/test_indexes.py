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
        await self.con.execute(r"""
            # setup delta
            CREATE MIGRATION d1 TO {
                module test {
                    type Person {
                        property first_name -> str;
                        property last_name -> str;

                        index on ((.first_name, .last_name));
                    };

                    type Person2 extending Person;
                };
            };

            COMMIT MIGRATION d1;
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    schema::ObjectType {
                        indexes: {
                            expr
                        }
                    }
                FILTER schema::ObjectType.name = 'test::Person';
            """,
            [{
                'indexes': [{
                    'expr': '(.first_name, .last_name)',
                }]
            }],
        )

        await self.con.execute(r"""
            INSERT test::Person {
                first_name := 'Elon',
                last_name := 'Musk'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                ALTER TYPE test::Person
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
                FILTER schema::ObjectType.name = 'test::Person';
            """,
            [{
                'indexes': []
            }],
        )

    async def test_index_02(self):
        await self.con.execute(r"""
            # setup delta
            CREATE TYPE test::User {
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
                FILTER .name = 'test::User';
            """,
            [{
                'indexes': [{
                    'expr': '.title'
                }]
            }],
        )

        # simply test that the type can be dropped
        await self.con.execute(r"""
            DROP TYPE test::User;
        """)

    async def test_index_03(self):
        await self.con.execute(r"""
            CREATE TYPE test::User {
                CREATE PROPERTY name -> str;
                CREATE PROPERTY title -> str;
                CREATE INDEX ON (.title);
            }
        """)

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "index '.name' does not exist on object type 'test::User'"):
            await self.con.execute("""
                ALTER TYPE test::User DROP INDEX ON (.name)
            """)
