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

import asyncio

import edgedb

from edb.common import taskgroup as tg
from edb.testbase import server as tb
from edb.tools import test


class TestServerProto(tb.QueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    async def test_server_proto_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetch('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetch('select syntax error')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetch('select 1;'),
                    edgedb.Set((1,)))

    async def test_server_proto_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            for _ in range(10):
                await self.con.execute('select 1; select 2;'),

    async def test_server_proto_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetch('select 1;'),
                    edgedb.Set((1,)))

    async def test_server_proto_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('select 1 / 0;')

            for _ in range(10):
                await self.con.execute('select 1;')

    async def test_server_proto_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    await self.con.fetch(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetch(query, i)

    async def test_server_proto_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetch(f'select 10 // {i};')

    async def test_server_set_reset_alias_01(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.fetch('SELECT foo::min({1}) + bar::min({0})'),
            [1])

        self.assertEqual(
            await self.con.fetch('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET ALIAS bar;
        ''')

        self.assertEqual(
            await self.con.fetch('SELECT foo::min({1})'),
            [1])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent function: bar::min'):
            await self.con.fetch('SELECT bar::min({1})')

        await self.con.execute('''
            RESET ALIAS *;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent function: foo::min'):
            await self.con.fetch('SELECT foo::min({3})')

        self.assertEqual(
            await self.con.fetch('SELECT min({4})'),
            [4])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent schema item: Tmp'):
            await self.con.fetch('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            ''')

    async def test_server_set_reset_alias_02(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.fetch('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET MODULE;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent schema item: Tmp'):
            await self.con.fetch('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            ''')

    async def test_server_proto_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetch(
                    'select ()'),
                edgedb.Set([()]))

            self.assertEqual(
                await self.con.fetch(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                await self.con.fetch(
                    'select <array<int64>>[]'),
                edgedb.Set([[]]))

            self.assertEqual(
                await self.con.fetch(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.fetch('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

    async def test_server_proto_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.fetch(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            await self.con.fetch(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_server_proto_args_01(self):
        self.assertEqual(
            await self.con.fetch(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_args_02(self):
        self.assertEqual(
            await self.con.fetch(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_json_cast_01(self):
        self.assertEqual(
            await self.con.fetch('''
                select <json>(
                    select schema::Type{name} filter .name = 'std::bool'
                )
            '''),
            edgedb.Set(('{"name": "std::bool"}',))
        )

    async def test_server_proto_wait_cancel_01(self):
        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.cluster.connect(user='edgedb',
                                          database=self.con.dbname)

        await self.con.fetch('select sys::advisory_lock(<int64>$0)', lock_key)

        try:
            async with tg.TaskGroup() as g:

                async def exec_to_fail():
                    with self.assertRaises(ConnectionAbortedError):
                        await con2.fetch(
                            'select sys::advisory_lock(<int64>$0)', lock_key)

                g.create_task(exec_to_fail())

                await asyncio.sleep(0.1)
                await con2.close()

        finally:
            self.assertEqual(
                await self.con.fetch(
                    'select sys::advisory_unlock(<int64>$0)', lock_key),
                [True])

    async def test_server_proto_tx_savepoint_01(self):
        typename = 'Savepoint_01'
        query = f'SELECT test::{typename}.prop1'
        con = self.con

        await con.execute(f'''
            START TRANSACTION;

            DECLARE SAVEPOINT t1;

            CREATE TYPE test::{typename} {{
                CREATE REQUIRED PROPERTY prop1 -> std::str;
            }};

            DECLARE SAVEPOINT t1;
        ''')

        try:
            await con.execute(f'''
                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};

                DECLARE SAVEPOINT t1;
            ''')

            await con.execute(f'''
                INSERT test::{typename} {{
                    prop1 := 'bbb'
                }};

                DECLARE SAVEPOINT t2;
            ''')

            await con.execute(f'''
                INSERT test::{typename} {{
                    prop1 := 'ccc'
                }};

                DECLARE SAVEPOINT t1;
            ''')

            await con.execute(f'''
                INSERT test::{typename} {{
                    prop1 := 'ddd'
                }};

                DECLARE SAVEPOINT t3;
            ''')

            self.assertEqual(
                await con.fetch(query),
                edgedb.Set(('aaa', 'bbb', 'ccc', 'ddd')))

            for _ in range(10):
                await con.execute('ROLLBACK TO SAVEPOINT t1')

                self.assertEqual(
                    await con.fetch(query),
                    edgedb.Set(('aaa', 'bbb', 'ccc')))

            await con.execute('RELEASE SAVEPOINT t1')
            self.assertEqual(
                await con.fetch(query),
                edgedb.Set(('aaa', 'bbb', 'ccc')))

            for _ in range(5):
                await con.execute('ROLLBACK TO SAVEPOINT t1')
                self.assertEqual(
                    await con.fetch(query),
                    edgedb.Set(('aaa',)))

            await con.execute('RELEASE SAVEPOINT t1')
            await con.execute('RELEASE SAVEPOINT t1')
            await con.execute('ROLLBACK TO SAVEPOINT t1')

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError, 'non-existent.*Savepoint'):
                await con.fetch(query)

        finally:
            await con.execute('ROLLBACK')

    async def test_server_proto_tx_savepoint_02(self):
        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.execute('''
                DECLARE SAVEPOINT t1;
            ''')

        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.fetch('''
                DECLARE SAVEPOINT t1;
            ''')

    async def test_server_proto_tx_savepoint_03(self):
        # test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.fetch('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('''
                    RELEASE SAVEPOINT t1;
                ''')

            await self.con.fetch('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.fetch('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('''
                    RELEASE SAVEPOINT t1;
                ''')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_04(self):
        # test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('SELECT 1;')

            await self.con.fetch('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetch('SELECT 1;')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            self.assertEqual(
                await self.con.fetch('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_05(self):
        # test that SIMPLE QUERY plays nice with savepoints

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.execute('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('''
                    RELEASE SAVEPOINT t1;
                ''')

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.execute('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('''
                    RELEASE SAVEPOINT t1;
                ''')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_savepoint_06(self):
        # test that SIMPLE QUERY plays nice with savepoints

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            await self.con.execute('SELECT 1;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            await self.con.execute('SELECT 1;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_savepoint_07(self):
        con = self.con

        await con.execute(f'''
            START TRANSACTION;

            DECLARE SAVEPOINT t1;

            SET ALIAS t1 AS MODULE std;
            SET ALIAS t1 AS MODULE std;

            DECLARE SAVEPOINT t2;

            SET ALIAS t2 AS MODULE std;
        ''')

        try:

            for _ in range(5):
                self.assertEqual(
                    await con.fetch('SELECT t1::min({1}) + t2::min({2})'),
                    [3])

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t2;
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con.fetch('SELECT t1::min({1}) + std::min({100})'),
                    [101])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    'non-existent function: t2::min'):
                await con.fetch('SELECT t1::min({1}) + t2::min({2})')

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t1;
            ''')

            self.assertEqual(
                await con.fetch('SELECT std::min({100})'),
                [100])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    'non-existent function: t1::min'):
                await con.fetch('SELECT t1::min({1})')

        finally:
            await con.execute('ROLLBACK')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent function: t1::min'):
            await con.fetch('SELECT t1::min({1})')

    async def test_server_proto_tx_savepoint_08(self):
        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.execute('''
                START TRANSACTION;

                DECLARE SAVEPOINT t1;

                SET ALIAS t1 AS MODULE std;

                SELECT 1 / 0;
            ''')

        await con.execute('ROLLBACK;')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                'non-existent function: t1::min'):
            await con.fetch('SELECT t1::min({1})')

    async def test_server_proto_tx_savepoint_09(self):
        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.execute('''
                START TRANSACTION;

                DECLARE SAVEPOINT t1;

                SET ALIAS t1 AS MODULE std;

                SELECT 1 / 0;
            ''')

        try:
            await con.execute('''
                ROLLBACK TO SAVEPOINT t1;
                SET ALIAS t2 AS MODULE std;
            ''')

            self.assertEqual(
                await con.fetch('SELECT t2::min({2})'),
                [2])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    'non-existent function: t1::min'):
                await con.fetch('SELECT t1::min({1})')

        finally:
            await con.execute('ROLLBACK')

    async def test_server_proto_tx_01(self):
        await self.con.execute('''
            START TRANSACTION;
        ''')

        try:
            await self.con.execute('SELECT 1;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

        await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_02(self):
        # Test Parse/Execute with ROLLBACK; use new connection
        # to make sure that Opportunistic Execute isn't used.

        con2 = await self.cluster.connect(user='edgedb',
                                          database=self.con.dbname)

        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await con2.execute('''
                    START TRANSACTION;
                    SELECT 1;
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    "current transaction is aborted"):
                await con2.fetch('SELECT 1;')

            await con2.fetch('ROLLBACK')

            self.assertEqual(
                await con2.fetch('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.execute('''
                    DECLARE SAVEPOINT t1;
                ''')
        finally:
            await con2.close()

    async def test_server_proto_tx_03(self):
        # Test Opportunistic Execute with ROLLBACK; use new connection
        # to make sure that "ROLLBACK" is cached.

        con2 = await self.cluster.connect(user='edgedb',
                                          database=self.con.dbname)

        try:
            for _ in range(5):
                await con2.fetch('START TRANSACTION')
                await con2.fetch('ROLLBACK')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await con2.execute('''
                    START TRANSACTION;
                    SELECT 1;
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    "current transaction is aborted"):
                await con2.fetch('SELECT 1;')

            await con2.fetch('ROLLBACK')

            self.assertEqual(
                await con2.fetch('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.execute('''
                    DECLARE SAVEPOINT t1;
                ''')
        finally:
            await con2.close()

    async def test_server_proto_tx_04(self):
        await self.con.execute('''
            START TRANSACTION;
        ''')

        try:
            with self.assertRaisesRegex(
                    edgedb.TransactionError, 'already in transaction'):

                await self.con.execute('''
                    START TRANSACTION;
                ''')
        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

    async def test_server_proto_tx_05(self):
        # test that caching of compiled queries doesn't interfere
        # with transactions

        query = 'SELECT "test_server_proto_tx_04"'

        for _ in range(5):
            self.assertEqual(
                await self.con.fetch(query),
                ['test_server_proto_tx_04'])

        await self.con.execute('''
            START TRANSACTION;
        ''')

        for i in range(5):
            self.assertEqual(
                await self.con.fetch(query),
                ['test_server_proto_tx_04'])

            self.assertEqual(
                await self.con.fetch('SELECT <int64>$0', i),
                [i])

        await self.con.execute('''
            ROLLBACK;
        ''')

    async def test_server_proto_tx_06(self):
        # test that caching of compiled queries in other connections
        # doesn't interfere with transactions

        query = 'SELECT 1'

        con2 = await self.cluster.connect(user='edgedb',
                                          database=self.con.dbname)
        try:
            for _ in range(5):
                self.assertEqual(
                    await self.con.fetch(query),
                    [1])
        finally:
            await con2.close()

        await self.con.execute('''
            START TRANSACTION;
        ''')

        try:
            for i in range(5):
                self.assertEqual(
                    await self.con.fetch(query),
                    [1])
        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

    async def test_server_proto_tx_07(self):
        try:
            await self.con.execute('''
                START TRANSACTION ISOLATION SERIALIZABLE, READ ONLY,
                    DEFERRABLE;
            ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'read-only transaction'):

                await self.con.execute('''
                    INSERT test::Tmp {
                        tmp := 'aaa'
                    };
                ''')
        finally:
            await self.con.execute(f'''
                ROLLBACK;
            ''')

        self.assertEqual(
            await self.con.fetch('SELECT 42'),
            [42])

    async def test_server_proto_tx_08(self):
        initq = '''
            INSERT test::Tmp {
                tmp := 'test_server_proto_tx_07'
            };

            START TRANSACTION;
            SELECT 1 / 0;
        '''

        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute(initq)
        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

        self.assertEqual(
            await self.con.fetch('''
                SELECT test::Tmp { tmp }
                FILTER .tmp = 'test_server_proto_tx_07'
            '''),
            [])

    async def test_server_proto_tx_09(self):
        initq = '''
            INSERT test::Tmp {
                tmp := 'test_server_proto_tx_08'
            };

            START TRANSACTION;
            SELECT 1 / 0;

            COMMIT;

            INSERT test::Tmp {
                tmp := 'test_server_proto_tx_08'
            };
        '''

        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute(initq)
        finally:
            await self.con.execute('''
                ROLLBACK;

                INSERT test::Tmp {
                    tmp := 'test_server_proto_tx_08'
                };
            ''')

        self.assertEqual(
            await self.con.fetch('''
                SELECT count(
                    test::Tmp { tmp }
                    FILTER .tmp = 'test_server_proto_tx_08'
                )
            '''),
            [1])

        await self.con.execute('''
            DELETE (SELECT test::Tmp);
        ''')


class TestServerProtoDDL(tb.NonIsolatedDDLTestCase):

    async def test_server_proto_query_cache_invalidate_01(self):
        typename = 'CacheInv_01'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};

                INSERT test::{typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set([123]))

        finally:
            await con2.close()

    async def test_server_proto_query_cache_invalidate_02(self):
        typename = 'CacheInv_02'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.fetch(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};
            ''')

            await con2.fetch(f'''
                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['aaa']))

            await con2.fetch(f'''
                DELETE (SELECT test::{typename});
            ''')

            await con2.fetch(f'''
                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};
            ''')

            await con2.fetch(f'''
                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};
            ''')

            await con2.fetch(f'''
                INSERT test::{typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set([123]))

        finally:
            await con2.close()

    async def test_server_proto_query_cache_invalidate_03(self):
        typename = 'CacheInv_03'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> array<std::str>;
                }};

                INSERT test::{typename} {{
                    prop1 := ['a', 'aa']
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set([['a', 'aa']]))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> array<std::int64>;
                }};

                INSERT test::{typename} {{
                    prop1 := [1, 23]
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set([[1, 23]]))

        finally:
            await con2.close()

    async def test_server_proto_query_cache_invalidate_04(self):
        typename = 'CacheInv_04'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED MULTI PROPERTY prop1 -> std::str;
                }};

                INSERT test::{typename} {{
                    prop1 := {{'bbb', 'ccc'}}
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['bbb', 'ccc']))

        finally:
            await con2.close()

    @test.xfail('''
        The error is:
        reference to a non-existent schema item: test::prop1
    ''')
    async def test_server_proto_query_cache_invalidate_05(self):
        typename = 'CacheInv_05'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY test::prop1 -> std::str;
                }};

                CREATE TYPE test::Other{typename} {{
                    CREATE REQUIRED PROPERTY test::prop2 -> std::str;
                }};

                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};

                INSERT test::Other{typename} {{
                    prop2 := 'bbb'
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP PROPERTY test::prop1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED LINK test::prop1 -> test::Other{typename};
                }};

                INSERT test::{typename} {{
                    prop1 := (SELECT test::Other{typename} LIMIT 1)
                }};
            ''')

            other = await con1.fetch(f'SELECT test::Other{typename}')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    other)

        finally:
            await con2.close()

    async def test_server_proto_query_cache_invalidate_06(self):
        typename = 'CacheInv_06'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::Foo{typename};

                CREATE TYPE test::Bar{typename};

                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED LINK link1 -> test::Foo{typename};
                }};

                INSERT test::Foo{typename};
                INSERT test::Bar{typename};

                INSERT test::{typename} {{
                    link1 := (SELECT test::Foo{typename} LIMIT 1)
                }};
            ''')

            foo = await con1.fetch(f'SELECT test::Foo{typename}')
            bar = await con1.fetch(f'SELECT test::Bar{typename}')

            query = f'SELECT test::{typename}.link1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    foo)

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP LINK link1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED LINK link1 -> test::Bar{typename};
                }};

                INSERT test::{typename} {{
                    link1 := (SELECT test::Bar{typename} LIMIT 1)
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    bar)

        finally:
            await con2.close()

    @test.xfail('''
        The error is:
        reference to a non-existent schema item:
        3c6145d4-192f-11e9-83b3-edb414a0f9bf in schema
        <Schema gen:4594 at 0x7f5dbfd68198>
    ''')
    async def test_server_proto_query_cache_invalidate_07(self):
        typename = 'CacheInv_07'

        con1 = self.con
        con2 = await self.cluster.connect(user='edgedb', database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::Foo{typename};

                CREATE ABSTRACT LINK test::link1 {{
                    CREATE PROPERTY prop1 -> std::str;
                }};

                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED LINK test::link1 -> test::Foo{typename};
                }};

                INSERT test::Foo{typename};

                INSERT test::{typename} {{
                    link1 := (
                        SELECT test::Foo{typename} {{@prop1 := 'aaa'}}
                    )
                }};
            ''')

            query = f'SELECT test::{typename}.link1@prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER ABSTRACT LINK test::link1 {{
                    DROP PROPERTY prop1;
                }};

                ALTER ABSTRACT LINK test::link1 {{
                    CREATE PROPERTY prop1 -> std::int64;
                }};

                INSERT test::{typename} {{
                    link1 := (
                        SELECT test::Foo{typename} {{@prop1 := 123}} LIMIT 1
                    )
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetch(query),
                    edgedb.Set([123]))

        finally:
            await con2.close()

    @test.xfail("concurrent DDL isn't yet supported")
    async def test_server_proto_query_cache_invalidate_08(self):
        typename_prefix = 'CacheInvMulti_'
        ntasks = 5

        async with tg.TaskGroup() as g:
            cons_tasks = [
                g.create_task(
                    self.cluster.connect(user='edgedb',
                                         database=self.con.dbname))
                for _ in range(ntasks)
            ]

        cons = [c.result() for c in cons_tasks]

        try:
            async with tg.TaskGroup() as g:
                for i, con in enumerate(cons):
                    g.create_task(con.execute(f'''
                        CREATE TYPE test::{typename_prefix}{i} {{
                            CREATE REQUIRED PROPERTY prop1 -> std::int64;
                        }};

                        INSERT test::{typename_prefix}{i} {{
                            prop1 := {i}
                        }};
                    '''))

            for i, con in enumerate(cons):
                ret = await con.fetch(
                    f'SELECT test::{typename_prefix}{i}.prop1')
                self.assertEqual(ret, i)

        finally:
            async with tg.TaskGroup() as g:
                for con in cons:
                    g.create_task(con.close())
