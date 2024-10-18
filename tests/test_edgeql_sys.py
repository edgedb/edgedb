#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


class TestEdgeQLSys(tb.QueryTestCase):
    SETUP = '''
        create type TestEdgeQLSys {
            create property bar -> str;
        };
    '''

    async def test_edgeql_sys_locks(self):
        lock_key = tb.gen_lock_key()

        async with self.assertRaisesRegexTx(
            edgedb.InternalServerError,
            "lock key cannot be negative",
        ):
            await self.con.execute('select sys::_advisory_lock(-1)')

        async with self.assertRaisesRegexTx(
            edgedb.InternalServerError,
            "lock key cannot be negative",
        ):
            await self.con.execute('select sys::_advisory_unlock(-1)')

        self.assertEqual(
            await self.con.query(
                'select sys::_advisory_unlock(<int64>$0)',
                lock_key),
            [False])

        await self.con.query(
            'select sys::_advisory_lock(<int64>$0)',
            lock_key)

        self.assertEqual(
            await self.con.query(
                'select sys::_advisory_unlock(<int64>$0)',
                lock_key),
            [True])
        self.assertEqual(
            await self.con.query(
                'select sys::_advisory_unlock(<int64>$0)',
                lock_key),
            [False])

    async def test_edgeql_sys_query_stats(self):
        stats_query = '''
            with stats := (
                select
                    sys::QueryStats
                filter
                    .query like '%TestEdgeQLSys%'
                    and .query not like '%sys::%'
            )
            select sum(stats.calls)
        '''
        calls = await self.con.query_single(stats_query)

        self.assertEqual(await self.con.query('select TestEdgeQLSys'), [])
        self.assertEqual(await self.con.query_single(stats_query), calls + 1)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError, 'does not exist'
        ):
            await self.con.query('select TestEdgeQLSys_NoSuchType')
        self.assertEqual(await self.con.query_single(stats_query), calls + 1)

        self.assertIsNone(
            await self.con.query_single(
                "select sys::reset_query_stats(branch_name := 'non_exdb')"
            )
        )
        self.assertEqual(await self.con.query_single(stats_query), calls + 1)

        self.assertIsNotNone(
            await self.con.query('select sys::reset_query_stats()')
        )
        self.assertEqual(await self.con.query_single(stats_query), 0)
