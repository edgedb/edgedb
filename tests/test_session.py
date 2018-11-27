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

from edb.server import _testbase as tb


class TestSession(tb.QueryTestCase):
    SETUP = """
        CREATE MODULE foo;
        CREATE MODULE fuz;

        CREATE MIGRATION default::m TO eschema $$
            type User:
                required property name -> str
        $$;

        COMMIT MIGRATION default::m;

        WITH MODULE default INSERT User {name := 'user'};

        CREATE MIGRATION foo::m TO eschema $$
            type Entity:
                required property name -> str
        $$;

        COMMIT MIGRATION foo::m;

        CREATE MIGRATION fuz::m TO eschema $$
            type Entity:
                required property name -> str
        $$;

        COMMIT MIGRATION fuz::m;

        WITH MODULE foo INSERT Entity {name := 'entity'};
        WITH MODULE fuz INSERT Entity {name := 'fuzentity'};
    """

    async def test_session_default_module_01(self):
        await self.assert_query_result("""
            SELECT User {name};
        """, [[{
            'name': 'user'
        }]])

    async def test_session_set_command_01(self):
        await self.assert_query_result("""
            SET MODULE foo;

            SELECT Entity {name};
        """, [

            None,

            [{
                'name': 'entity'
            }]
        ])

    async def test_session_set_command_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'reference to a non-existent schema item: User'):
            await self.assert_query_result("""
                SET MODULE foo;

                SELECT User {name};
            """, [

                None,

                [{
                    'name': 'user'
                }]
            ])

    async def test_session_set_command_03(self):
        await self.assert_query_result("""
            SET MODULE foo, ALIAS bar AS MODULE default;

            SELECT (Entity.name, bar::User.name);
        """, [

            None,

            [['entity', 'user']]
        ])

    async def test_session_set_command_05(self):
        # Check that local WITH overrides the session level setting.
        await self.assert_query_result("""
            SET MODULE default, ALIAS bar AS MODULE foo;

            WITH MODULE foo, bar AS MODULE default
            SELECT (Entity.name, bar::User.name);
        """, [

            None,

            [['entity', 'user']]
        ])

    async def test_session_set_command_06(self):
        # Check that nested WITH blocks work correctly, with and
        # without DETACHED.
        await self.assert_query_result("""
            WITH MODULE foo
                SELECT (
                    Entity.name,
                    fuz::Entity.name
                );

            WITH MODULE foo
                SELECT (
                    Entity.name,
                    (WITH MODULE fuz SELECT Entity.name)
                );

            WITH MODULE foo
                SELECT (
                    Entity.name,
                    (WITH MODULE fuz SELECT DETACHED Entity.name)
                );
        """, [
            [['entity', 'fuzentity']],
            [['entity', 'fuzentity']],
            [['entity', 'fuzentity']],
        ])
