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

from edb.pgsql import common

from edb.testbase import server as tb


class TestQueryStatsMixin:
    stats_magic_word: str = NotImplemented
    stats_type: str = NotImplemented
    counter: int = 0

    async def _query_for_stats(self):
        raise NotImplementedError

    async def _configure_track(self, option: str):
        raise NotImplementedError

    async def _bad_query_for_stats(self):
        raise NotImplementedError

    def _before_test_sys_query_stats(self):
        if self.backend_dsn:
            self.skipTest(
                "can't run query stats test when extension isn't present"
            )

    async def _test_sys_query_stats(self):
        stats_query = f'''
            with stats := (
                select
                    sys::QueryStats
                filter
                    .query like '%{self.stats_magic_word}%'
                    and .query not like '%sys::%'
                    and .query_type = <sys::QueryType>$0
            )
            select sum(stats.calls)
        '''

        # Take the initial tracking number of executions
        calls = await self.con.query_single(stats_query, self.stats_type)

        # Execute the query one more time
        await self._query_for_stats()
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            calls + 1,
        )

        # Bad queries are not tracked
        await self._bad_query_for_stats()
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            calls + 1,
        )

        # sys::reset_query_stats() branch filter works correctly
        self.assertIsNone(
            await self.con.query_single(
                "select sys::reset_query_stats(branch_name := 'non_exdb')"
            )
        )
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            calls + 1,
        )

        # sys::reset_query_stats() works correctly
        self.assertIsNotNone(
            await self.con.query('select sys::reset_query_stats()')
        )
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            0,
        )

        # Turn off cfg::Config.track_query_stats, verify tracking is stopped
        await self._configure_track('None')
        await self._query_for_stats()
        await self._query_for_stats()
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            0,
        )

        # Turn cfg::Config.track_query_stats back on again
        if self.stats_type == 'SQL':
            # FIXME: don't return after fixing #8147
            return
        await self._configure_track('All')
        await self._query_for_stats()
        self.assertEqual(
            await self.con.query_single(stats_query, self.stats_type),
            1,
        )


class TestEdgeQLSys(tb.QueryTestCase, TestQueryStatsMixin):
    stats_magic_word = 'TestEdgeQLSys'
    stats_type = 'EdgeQL'

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

    async def _query_for_stats(self):
        self.counter += 1
        self.assertEqual(
            await self.con.query(
                f'select ('
                    f'{self.stats_magic_word}{self.counter} := {self.counter})'
            ),
            [(self.counter,)],
        )

    async def _configure_track(self, option: str):
        await self.con.query(f'''
            configure session set track_query_stats :=
                <cfg::QueryStatsOption>{common.quote_literal(option)};
        ''')

    async def _bad_query_for_stats(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError, 'does not exist'
        ):
            await self.con.query(f'select {self.stats_magic_word}_NoSuchType')

    async def test_edgeql_sys_query_stats(self):
        self._before_test_sys_query_stats()
        async with tb.start_edgedb_server() as sd:
            old_con = self.con
            self.con = await sd.connect()
            try:
                await self._test_sys_query_stats()
            finally:
                await self.con.aclose()
                self.con = old_con


class TestSQLSys(tb.SQLQueryTestCase, TestQueryStatsMixin):
    stats_magic_word = 'TestSQLSys'
    stats_type = 'SQL'

    TRANSACTION_ISOLATION = False

    async def _query_for_stats(self):
        self.counter += 1
        ident = common.quote_ident(self.stats_magic_word + str(self.counter))
        self.assertEqual(
            await self.squery_values(
                f"select {self.counter} as {ident}"
            ),
            [[self.counter]],
        )

    async def _configure_track(self, option: str):
        # XXX: we should probably translate the config name in the compiler,
        # so that we can use the frontend name (track_query_stats) here instead
        # FIXME: drop lower() after fixing #8147
        await self.scon.execute(f'''
            set "edb_stat_statements.track" TO '{option.lower()}';
        ''')

    async def _bad_query_for_stats(self):
        import asyncpg

        with self.assertRaisesRegex(
            asyncpg.UndefinedColumnError, "does not exist"
        ):
            await self.squery_values(
                f'select {self.stats_magic_word}_NoSuchType'
            )

    async def test_sql_sys_query_stats(self):
        self._before_test_sys_query_stats()
        async with tb.start_edgedb_server() as sd:
            old_cons = self.con, self.scon
            self.con = await sd.connect()
            self.scon = await sd.connect_pg()
            try:
                await self._test_sys_query_stats()
            finally:
                await self.scon.close()
                await self.con.aclose()
                self.con, self.scon = old_cons
