#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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


import os.path

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLGlobals(tb.QueryTestCase):
    '''Tests for globals.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'cards_setup.edgeql'),
        '''
            create global cur_user -> str;
            create required global def_cur_user -> str {
                set default := 'Alice'
            };
            create global cur_card -> str {
                set default := 'Dragon'
            };

            create alias CurUser := (
                select User filter .name = global cur_user);

        '''
    ]

    async def test_edgeql_globals_errors_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"possibly an empty set returned",
        ):
            await self.con.execute('''
                set global def_cur_user := {};
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"possibly more than one element returned",
        ):
            await self.con.execute('''
                set global def_cur_user := {"foo", "bar"};
            ''')

    async def test_edgeql_globals_01(self):
        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''
                select global cur_user
            ''',
            ['Bob'],
        )

    @test.xfail("We can't execute multiple set globals in a block yet")
    async def test_edgeql_globals_02(self):
        await self.con.execute('''
            set global cur_user := "Bob";
            set global def_cur_user := "Dave";
            select 1;
        ''')

        await self.assert_query_result(
            r'''
                select (global cur_user, global def_cur_user)
            ''',
            [['Bob', 'Dave']],
        )

    async def test_edgeql_globals_03(self):
        # test the behaviors of an optional with a default
        await self.assert_query_result(
            r'''select global cur_card''',
            ['Dragon']
        )

        await self.con.execute('''
            set global cur_card := 'foo'
        ''')

        await self.assert_query_result(
            r'''select global cur_card''',
            ['foo']
        )

        # setting it to {} actually sets it to {}
        await self.con.execute('''
            set global cur_card := {}
        ''')

        await self.assert_query_result(
            r'''select global cur_card''',
            []
        )

        # and RESET puts it back to the default
        await self.con.execute('''
            reset global cur_card
        ''')
        await self.assert_query_result(
            r'''select global cur_card''',
            ['Dragon']
        )

    async def test_edgeql_globals_04(self):
        await self.assert_query_result(
            r'''select CurUser { name }''',
            []
        )

        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''select CurUser { name }''',
            [
                {'name': 'Bob'}
            ]
        )
