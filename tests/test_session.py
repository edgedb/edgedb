##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.client import exceptions as err
from edgedb.server import _testbase as tb


class TestSession(tb.QueryTestCase):
    SETUP = """
        CREATE MODULE foo;

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

        WITH MODULE foo INSERT Entity {name := 'entity'};
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
                err.EdgeQLError,
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
            SET MODULE foo, bar := MODULE default;

            SELECT (Entity.name, bar::User.name);
        """, [

            None,

            [['entity', 'user']]
        ])

    async def test_session_set_command_04(self):
        with self.assertRaisesRegex(
                err.EdgeQLError,
                'expression aliases in SET are not supported yet'):
            await self.assert_query_result("""
                SET foo := 1 + 1;
            """, [
                None,
            ])

    async def test_session_set_command_05(self):
        # Check that local WITH overrides the session level setting.
        await self.assert_query_result("""
            SET MODULE default, bar := MODULE foo;

            WITH MODULE foo, bar := MODULE default
            SELECT (Entity.name, bar::User.name);
        """, [

            None,

            [['entity', 'user']]
        ])
