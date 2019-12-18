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


class TestSession(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION mig TO {
            module default {
                type User {
                    required property name -> str;
                    property name_len := len(User.name);
                };

                type Flag {
                    required property value -> bool;
                };
            };
            module foo {
                type Entity {
                    required property name -> str;
                };
            }
            module fuz {
                type Entity {
                    required property name -> str;
                };
            };
        };

        COMMIT MIGRATION mig;

        # Needed for validating that "ALTER TYPE User" DDL commands work.
        CREATE ALIAS UserOneToOneAlias := (SELECT User);

        WITH MODULE default INSERT User {name := 'user'};
        WITH MODULE foo INSERT Entity {name := 'entity'};
        WITH MODULE fuz INSERT Entity {name := 'fuzentity'};
    """

    async def test_session_default_module_01(self):
        await self.assert_query_result(
            """
                SELECT User {name};
            """,
            [{
                'name': 'user'
            }]
        )

    async def test_session_set_command_01(self):
        await self.con.execute('SET MODULE foo;')

        await self.assert_query_result(
            """
                SELECT Entity {name};
            """,
            [{
                'name': 'entity'
            }]
        )

    async def test_session_set_command_02(self):
        await self.con.fetchall('SET MODULE foo;')
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "object type or alias 'User' does not exist"):
            await self.con.fetchall('SELECT User {name};')

    async def test_session_set_command_03(self):
        await self.con.execute(
            'SET MODULE foo; SET ALIAS bar AS MODULE default;')
        await self.assert_query_result(
            """SELECT (Entity.name, bar::User.name);""",
            [['entity', 'user']]
        )

    async def test_session_set_command_05(self):
        await self.con.execute(
            'SET MODULE default; SET ALIAS bar AS MODULE foo;')
        # Check that local WITH overrides the session level setting.
        await self.assert_query_result(
            """
                WITH MODULE foo, bar AS MODULE default
                SELECT (Entity.name, bar::User.name);
            """,
            [['entity', 'user']]
        )

    async def test_session_set_command_06(self):
        # Check that nested WITH blocks work correctly, with and
        # without DETACHED.
        await self.assert_query_result(
            """
                WITH MODULE foo
                SELECT (
                    Entity.name,
                    fuz::Entity.name
                );
            """,
            [['entity', 'fuzentity']],
        )

        await self.assert_query_result(
            """
                WITH MODULE foo
                SELECT (
                    Entity.name,
                    (WITH MODULE fuz SELECT Entity.name)
                );
            """,
            [['entity', 'fuzentity']],
        )

        await self.assert_query_result(
            """
                WITH MODULE foo
                SELECT (
                    Entity.name,
                    (WITH MODULE fuz SELECT DETACHED Entity.name)
                );
            """,
            [['entity', 'fuzentity']],
        )

    async def test_session_ddl_default_module_01(self):
        await self.con.execute('RESET ALIAS *')

        tx = self.con.transaction()
        await tx.start()

        try:
            await self.con.execute('''
                ALTER TYPE User {
                    # Test that UserOneToOneAlias doesn't block the alter.
                    DROP PROPERTY name_len;
                }
            ''')

            await self.con.execute('''
                ALTER TYPE User {
                    CREATE PROPERTY aaa := len(
                        'yes' IF __source__ IS User ELSE 'no');
                    CREATE PROPERTY name_upper := str_upper(.name);
                }
            ''')

            await self.assert_query_result(
                """
                    SELECT User {name, aaa, name_upper};
                """,
                [{
                    'name': 'user',
                    'aaa': 3,
                    'name_upper': 'USER',
                }]
            )
        finally:
            await tx.rollback()

    async def test_session_only_function_01(self):
        # Check that a session only function can be called in a
        # regular query.
        await self.assert_query_result(
            r"""
                SELECT sys::sleep(0);
            """,
            [True],
        )

    async def test_session_only_function_02(self):
        # Check that a session only function can be called in an
        # INSERT and UPDATE.
        async with self.con.transaction():
            await self.con.execute(r"""
                INSERT Flag {
                    value := sys::sleep(0)
                };
            """)

            await self.assert_query_result(
                r"""
                    SELECT Flag { value };
                """,
                [{
                    'value': True,
                }],
            )

            await self.con.execute(r"""
                UPDATE Flag
                SET {
                    value := NOT sys::sleep(0)
                };
            """)

            await self.assert_query_result(
                r"""
                    SELECT Flag { value };
                """,
                [{
                    'value': False,
                }],
            )

    async def test_session_only_function_03(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'sys::sleep\(\) cannot be '
                                    r'called in a non-session context'):
            await self.con.execute(r"""
                CREATE ALIAS BadAlias := User {
                    sess := sys::sleep(0)
                };
            """)

    async def test_session_only_function_04(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'sys::sleep\(\) cannot be '
                                    r'called in a non-session context'):
            await self.con.execute(r"""
                CREATE TYPE BadType {
                    CREATE PROPERTY bad -> bool {
                        SET default := sys::sleep(0)
                    }
                };
            """)

    async def test_session_only_function_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'sys::sleep\(\) cannot be '
                                    r'called in a non-session context'):
            await self.con.execute(r"""
                CREATE FUNCTION bad() -> bool
                    USING EdgeQL $$ SELECT sys::sleep(0) $$;
            """)

    async def test_session_only_function_06(self):
        async with self.con.transaction():
            await self.con.execute(r"""
                CREATE FUNCTION ok() -> bool {
                    SET session_only := true;
                    USING EdgeQL $$ SELECT sys::sleep(0) $$;
                };
            """)

            await self.assert_query_result(
                r"""
                    SELECT ok();
                """,
                [True],
            )

    async def test_session_only_function_07(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'sys::sleep\(\) cannot be '
                                    r'called in a non-session context'):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE MIGRATION bad TO {
                        alias test::BadAlias := Object {
                            sess := sys::sleep(0)
                        }
                    };
                    COMMIT MIGRATION bad;
                """)

    async def test_session_only_function_08(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'sys::sleep\(\) cannot be '
                                    r'called in a non-session context'):
            async with self.con.transaction():
                await self.con.execute(r"""
                    CREATE MIGRATION bad TO {
                        type test::BadType {
                            property bad -> bool {
                                default := sys::sleep(0)
                            }
                        }
                    };
                    COMMIT MIGRATION bad;
                """)
