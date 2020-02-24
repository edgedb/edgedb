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
import json
import uuid
import subprocess
import sys
import tempfile
import unittest

import edgedb

from edb.common import devmode
from edb.common import taskgroup as tg
from edb.server import main as server_main
from edb.testbase import server as tb
from edb.tools import test


class TestServerProto(tb.QueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };

        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };

        CREATE SCALAR TYPE test::RGB
            EXTENDING enum<'RED', 'BLUE', 'GREEN'>;

        # Used by is_testmode_on() to ensure that config modifications
        # persist correctly when set inside and outside of (potentially
        # failing) transaction blocks.
        CONFIGURE SESSION SET __internal_testmode := true;
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    async def is_testmode_on(self):
        # The idea is that if __internal_testmode value config is lost
        # (no longer "true") then this script fails.
        try:
            await self.con.execute('''
                CREATE FUNCTION test::testconf() -> bool
                    USING SQL $$ SELECT true; $$;
                DROP FUNCTION test::testconf();
            ''')
        except edgedb.InvalidFunctionDefinitionError:
            return False

        return await self.con.fetchone('''
            SELECT cfg::Config.__internal_testmode LIMIT 1
        ''')

    async def test_server_proto_parse_redirect_data_01(self):
        # This is a regression fuzz test for ReadBuffer.redirect_messages().
        # The bug was related to 'D' messages that were filling the entire
        # receive buffer (8192 bytes) precisely.
        for power in range(10, 20):
            base = 2 ** power
            for i in range(base - 100, base + 100):
                v = await self.con.fetchone(
                    'select str_repeat(".", <int64>$i)', i=i)
                self.assertEqual(len(v), i)

    async def test_server_proto_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetchall('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetchall('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetchall('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetchall_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetchall('select 1;'),
                    edgedb.Set((1,)))

            self.assertTrue(await self.is_testmode_on())

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
                await self.con.fetchall('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetchall('select 1 / 0;')
            self.assertEqual(self.con._get_last_status(), None)

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetchall('select 1;'),
                    edgedb.Set((1,)))
                self.assertEqual(self.con._get_last_status(), 'SELECT')

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
                    await self.con.fetchall(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetchall(query, i)

    async def test_server_proto_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetchall(f'select 10 // {i};')

    async def test_server_proto_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            await self.con.execute(f'select <int64>$0')
        self.assertEqual(
            await self.con.fetchall('SELECT "HELLO"'),
            ["HELLO"])

    async def test_server_proto_fetch_single_command_01(self):
        r = await self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'CREATE')

        r = await self.con.fetchall('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'DROP')

        r = await self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.fetchall('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.fetchall_json('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetchall_json('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

    async def test_server_proto_fetch_single_command_02(self):
        r = await self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

        r = await self.con.fetchall('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.fetchall_json('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetchall_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetchall_json('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, '[]')

    async def test_server_proto_fetch_single_command_03(self):
        qs = [
            'START TRANSACTION',
            'DECLARE SAVEPOINT t0',
            'ROLLBACK TO SAVEPOINT t0',
            'RELEASE SAVEPOINT t0',
            'ROLLBACK',
            'START TRANSACTION',
            'COMMIT',
        ]

        for _ in range(3):
            for q in qs:
                r = await self.con.fetchall(q)
                self.assertEqual(r, [])

            for q in qs:
                r = await self.con.fetchall_json(q)
                self.assertEqual(r, '[]')

        for q in qs:
            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone\(\).*'
                    r'not return'):
                await self.con.fetchone(q)

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone_json\(\).*'
                    r'not return'):
                await self.con.fetchone_json(q)

    async def test_server_proto_fetch_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchall('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchone('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchall_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    async def test_server_proto_set_reset_alias_01(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.fetchall('SELECT foo::min({1}) + bar::min({0})'),
            [1])

        self.assertEqual(
            await self.con.fetchall('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET ALIAS bar;
        ''')

        self.assertEqual(
            await self.con.fetchall('SELECT foo::min({1})'),
            [1])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 'bar::min' does not exist"):
            await self.con.fetchall('SELECT bar::min({1})')

        await self.con.fetchall('''
            RESET ALIAS *;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 'foo::min' does not exist"):
            await self.con.fetchall('SELECT foo::min({3})')

        self.assertEqual(
            await self.con.fetchall('SELECT min({4})'),
            [4])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "object type or alias 'Tmp' does not exist"):
            await self.con.fetchall('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            ''')

    async def test_server_proto_set_reset_alias_02(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.fetchall('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET MODULE;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "object type or alias 'Tmp' does not exist"):
            await self.con.fetchall('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            ''')

    async def test_server_proto_set_reset_alias_03(self):
        with self.assertRaisesRegex(
                edgedb.UnknownModuleError, "module 'blahhhh' does not exist"):
            await self.con.execute('''
                SET ALIAS foo AS MODULE blahhhh;
            ''')

        with self.assertRaisesRegex(
                edgedb.UnknownModuleError, "module 'blahhhh' does not exist"):
            await self.con.execute('''
                SET MODULE blahhhh;
            ''')

        # Test error recovery now
        await self.con.execute('''
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.fetchall('''
                SELECT count(
                    Tmp FILTER Tmp.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

    async def test_server_proto_set_reset_alias_04(self):
        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                "unrecognized configuration parameter 'blahhhhhh'"):

            await self.con.execute('''
                SET ALIAS foo AS MODULE std;
                CONFIGURE SESSION SET blahhhhhh := 123;
            ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 'foo::min' does not exist"):
            await self.con.fetchall('SELECT foo::min({3})')

    async def test_server_proto_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetchone(
                    'select ()'),
                ())

            self.assertEqual(
                await self.con.fetchall(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            async with self.con.transaction():
                self.assertEqual(
                    await self.con.fetchone(
                        'select <array<int64>>[]'),
                    [])

            self.assertEqual(
                await self.con.fetchall(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.fetchall('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'fetchone\(\) as it returns a multiset'):
                await self.con.fetchone('SELECT {1, 2}')

            with self.assertRaisesRegex(edgedb.NoDataError, r'\bfetchone\('):
                await self.con.fetchone('SELECT <int64>{}')

    async def test_server_proto_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.fetchall(
                r'''select [b"\x00a", b"b", b'', b'\na', b'=A0']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na', b'=A0']]))

        self.assertEqual(
            await self.con.fetchall(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_server_proto_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetchall_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.con.fetchall_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.con.fetchall_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    await self.con.fetchone_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(await self.con.fetchall_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                await self.con.fetchone_json('SELECT <int64>{}')

        self.assertEqual(self.con._get_last_status(), 'SELECT')

    async def test_server_proto_basic_datatypes_04(self):
        # A regression test for enum typedescs being improperly
        # serialized and screwing up client's decoder.
        d = await self.con.fetchone('''
            WITH MODULE test
            SELECT (<RGB>"RED", <RGB>"GREEN", [1], [<RGB>"GREEN"], [2])
        ''')
        self.assertEqual(d[2], [1])

    async def test_server_proto_basic_datatypes_05(self):
        # A regression test to ensure that typedesc IDs are different
        # for shapes with equal fields names bit of different kinds
        # (e.g. in this test it's "@foo" vs "foo"; before fixing the
        # bug the results of second query were with "@foo" key, not "foo")

        for _ in range(5):
            await self.assert_query_result(
                r"""
                    WITH MODULE schema
                    SELECT ObjectType {
                        name,
                        properties: {
                            name,
                            @foo := 1
                        } ORDER BY .name LIMIT 1,
                    }
                    FILTER .name = 'test::Tmp';
                """,
                [{
                    'name': 'test::Tmp',
                    'properties': [{
                        'name': 'id',
                        '@foo': 1
                    }],
                }]
            )

        for _ in range(5):
            await self.assert_query_result(
                r"""
                    WITH MODULE schema
                    SELECT ObjectType {
                        name,
                        properties: {
                            name,
                            foo := 1
                        } ORDER BY .name LIMIT 1,
                    }
                    FILTER .name = 'test::Tmp';
                """,
                [{
                    'name': 'test::Tmp',
                    'properties': [{
                        'name': 'id',
                        'foo': 1
                    }],
                }]
            )

    async def test_server_proto_basic_datatypes_06(self):
        # Test that field names are taken into account when
        # typedesc id is computed.
        for _ in range(5):
            await self.assert_query_result(
                r"""
                    WITH MODULE schema
                    SELECT ObjectType {
                        name,
                        properties: {
                            name,
                            foo1 := 1
                        } ORDER BY .name LIMIT 1,
                    }
                    FILTER .name = 'test::Tmp';
                """,
                [{
                    'name': 'test::Tmp',
                    'properties': [{
                        'name': 'id',
                        'foo1': 1
                    }],
                }]
            )

        for _ in range(5):
            await self.assert_query_result(
                r"""
                    WITH MODULE schema
                    SELECT ObjectType {
                        name,
                        properties: {
                            name,
                            foo2 := 1
                        } ORDER BY .name LIMIT 1,
                    }
                    FILTER .name = 'test::Tmp';
                """,
                [{
                    'name': 'test::Tmp',
                    'properties': [{
                        'name': 'id',
                        'foo2': 1
                    }],
                }]
            )

    async def test_server_proto_args_01(self):
        self.assertEqual(
            await self.con.fetchall(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_args_02(self):
        self.assertEqual(
            await self.con.fetchall(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.con.fetchall('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.con.fetchall('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.con.fetchall('select <int64>$0 + <int64>$bar;')

    async def test_server_proto_args_04(self):
        self.assertEqual(
            await self.con.fetchall_json(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            '["aaabbb"]')

    async def test_server_proto_args_05(self):
        self.assertEqual(
            await self.con.fetchall_json(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            '["aaabbb"]')

    async def test_server_proto_args_06(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetchone(
                    'select <int64>$你好 + 10',
                    你好=32),
                42)

    async def test_server_proto_args_07(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'missing a type cast.*parameter'):
            await self.con.fetchone(
                'select schema::Object {name} filter .id=$id', id='asd')

    async def test_server_proto_wait_cancel_01(self):
        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.connect(database=self.con.dbname)

        await self.con.fetchall(
            'select sys::advisory_lock(<int64>$0)', lock_key)

        try:
            async with tg.TaskGroup() as g:

                async def exec_to_fail():
                    with self.assertRaises(ConnectionAbortedError):
                        await con2.fetchall(
                            'select sys::advisory_lock(<int64>$0)', lock_key)

                g.create_task(exec_to_fail())

                await asyncio.sleep(0.1)
                await con2.aclose()

        finally:
            self.assertEqual(
                await self.con.fetchall(
                    'select sys::advisory_unlock(<int64>$0)', lock_key),
                [True])

    async def test_server_proto_log_message_01(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.con.add_log_listener(on_log)
        try:
            await self.con.fetchall(
                'configure system set __internal_restart := true;')
            await asyncio.sleep(0.01)  # allow the loop to call the callback
        finally:
            self.con.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')

    async def test_server_proto_tx_savepoint_01(self):
        # Basic test that SAVEPOINTS actually work; test with DML.

        typename = 'Savepoint_01'
        query = f'SELECT test::{typename}.prop1'
        con = self.con

        # __internal_testmode should be ON
        self.assertTrue(await self.is_testmode_on())

        await con.execute(f'''
            START TRANSACTION;

            CONFIGURE SESSION SET __internal_testmode := false;

            DECLARE SAVEPOINT t1;

            CREATE TYPE test::{typename} {{
                CREATE REQUIRED PROPERTY prop1 -> std::str;
            }};

            DECLARE SAVEPOINT t1;
        ''')

        self.assertEqual(self.con._get_last_status(), 'DECLARE SAVEPOINT')

        # Make sure that __internal_testmode was indeed updated.
        self.assertFalse(await self.is_testmode_on())
        # is_testmode_on call caused an error; rollback
        await con.execute('ROLLBACK TO SAVEPOINT t1')

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
                await con.fetchall(query),
                edgedb.Set(('aaa', 'bbb', 'ccc', 'ddd')))

            for _ in range(10):
                await con.execute('ROLLBACK TO SAVEPOINT t1')

                self.assertEqual(
                    await con.fetchall(query),
                    edgedb.Set(('aaa', 'bbb', 'ccc')))

            await con.execute('RELEASE SAVEPOINT t1')
            self.assertEqual(
                await con.fetchall(query),
                edgedb.Set(('aaa', 'bbb', 'ccc')))

            for _ in range(5):
                await con.execute('ROLLBACK TO SAVEPOINT t1')
                self.assertEqual(
                    await con.fetchall(query),
                    edgedb.Set(('aaa',)))

            await con.execute('RELEASE SAVEPOINT t1')
            await con.execute('RELEASE SAVEPOINT t1')
            await con.execute('ROLLBACK TO SAVEPOINT t1')

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    ".*Savepoint.*does not exist"):
                await con.fetchall(query)

        finally:
            await con.execute('ROLLBACK')

        # __internal_testmode should be ON, just as when the test method
        # was called.
        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_savepoint_02(self):
        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.execute('''
                DECLARE SAVEPOINT t1;
            ''')

        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.fetchall('''
                DECLARE SAVEPOINT t1;
            ''')

    async def test_server_proto_tx_savepoint_03(self):
        # Test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints.

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.fetchall('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchall('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchone('''
                    RELEASE SAVEPOINT t1;
                ''')

            await self.con.fetchall('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.fetchall('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchall('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchall('''
                    RELEASE SAVEPOINT t1;
                ''')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_04(self):
        # Test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints.

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT t0;
        ''')

        try:
            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetchall('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchall('SELECT 1;')

            await self.con.fetchall('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetchone('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.fetchall('SELECT 1;')

        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

            self.assertEqual(
                await self.con.fetchall('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_05(self):
        # Test RELEASE SAVEPOINT

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
        # Test that SIMPLE QUERY can combine START TRANSACTION
        # and DECLARE SAVEPOINT; test basic TransactionError
        # reflection.

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

        self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

        try:

            for _ in range(5):
                self.assertEqual(
                    await con.fetchall('SELECT t1::min({1}) + t2::min({2})'),
                    [3])

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t2;
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con.fetchall(
                        'SELECT t1::min({1}) + std::min({100})'),
                    [101])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't2::min' does not exist"):
                await con.fetchall('SELECT t1::min({1}) + t2::min({2})')

            await self.con.execute('''
                ROLLBACK TO SAVEPOINT t1;
            ''')

            self.assertEqual(
                await con.fetchall('SELECT std::min({100})'),
                [100])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't1::min' does not exist"):
                await con.fetchall('SELECT t1::min({1})')

        finally:
            await con.execute('ROLLBACK')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 't1::min' does not exist"):
            await con.fetchall('SELECT t1::min({1})')

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
        self.assertEqual(self.con._get_last_status(), 'ROLLBACK')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 't1::min' does not exist"):
            await con.fetchone('SELECT t1::min({1})')

    async def test_server_proto_tx_savepoint_09(self):
        # Test basic SET ALIAS tracking in transactions/savepoints;
        # test also that ROLLBACK TO SAVEPOINT can be safely combined
        # with other commands in the same SIMPLE QUERY.

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
            self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

            self.assertEqual(
                await con.fetchall('SELECT t2::min({2})'),
                [2])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't1::min' does not exist"):
                await con.fetchall('SELECT t1::min({1})')

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

        con2 = await self.connect(database=self.con.dbname)

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
                await con2.fetchall('SELECT 1;')

            await con2.fetchall('ROLLBACK')

            self.assertEqual(
                await con2.fetchall('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.execute('''
                    DECLARE SAVEPOINT t1;
                ''')
        finally:
            await con2.aclose()

    async def test_server_proto_tx_03(self):
        # Test Opportunistic Execute with ROLLBACK; use new connection
        # to make sure that "ROLLBACK" is cached.

        con2 = await self.connect(database=self.con.dbname)

        try:
            for _ in range(5):
                await con2.fetchall('START TRANSACTION')
                await con2.fetchall('ROLLBACK')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await con2.execute('''
                    START TRANSACTION;
                    SELECT 1;
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    "current transaction is aborted"):
                await con2.fetchall('SELECT 1;')

            await con2.fetchall('ROLLBACK')

            self.assertEqual(
                await con2.fetchall('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.execute('''
                    DECLARE SAVEPOINT t1;
                ''')
        finally:
            await con2.aclose()

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
        # Test that caching of compiled queries doesn't interfere
        # with transactions.

        query = 'SELECT "test_server_proto_tx_04"'

        for _ in range(5):
            self.assertEqual(
                await self.con.fetchall(query),
                ['test_server_proto_tx_04'])

        await self.con.execute('''
            START TRANSACTION;
        ''')

        for i in range(5):
            self.assertEqual(
                await self.con.fetchall(query),
                ['test_server_proto_tx_04'])

            self.assertEqual(
                await self.con.fetchall('SELECT <int64>$0', i),
                [i])

        await self.con.execute('''
            ROLLBACK;
        ''')

    async def test_server_proto_tx_06(self):
        # Test that caching of compiled queries in other connections
        # doesn't interfere with transactions.

        query = 'SELECT 1'

        con2 = await self.connect(database=self.con.dbname)
        try:
            for _ in range(5):
                self.assertEqual(
                    await self.con.fetchall(query),
                    [1])
        finally:
            await con2.aclose()

        await self.con.execute('''
            START TRANSACTION;
        ''')

        try:
            for _ in range(5):
                self.assertEqual(
                    await self.con.fetchall(query),
                    [1])
        finally:
            await self.con.execute('''
                ROLLBACK;
            ''')

    async def test_server_proto_tx_07(self):
        # Test that START TRANSACTION reflects its modes.

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
            await self.con.fetchall('SELECT 42'),
            [42])

    async def test_server_proto_tx_08(self):
        # Test that the topmost INSERT is executed in
        # the same transaction with "SELECT 1 / 0"

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
            await self.con.fetchall('''
                SELECT test::Tmp { tmp }
                FILTER .tmp = 'test_server_proto_tx_07'
            '''),
            [])

    async def test_server_proto_tx_09(self):
        # Test that the topmost INSERT is executed in
        # the same transaction with "SELECT 1 / 0"; test
        # that the later COMMIT is ignored.

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
            await self.con.fetchall('''
                SELECT count(
                    test::Tmp { tmp }
                    FILTER .tmp = 'test_server_proto_tx_08'
                )
            '''),
            [1])

        await self.con.execute('''
            DELETE (SELECT test::Tmp);
        ''')

        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_10(self):
        # Basic test that ROLLBACK works on SET ALIAS changes.

        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute('''
                START TRANSACTION;
                DECLARE SAVEPOINT c0;
                SET ALIAS f1 AS MODULE std;
                DECLARE SAVEPOINT c1;
                CONFIGURE SESSION SET __internal_testmode := false;
                COMMIT;

                SET ALIAS f2 AS MODULE std;

                START TRANSACTION;
                DECLARE SAVEPOINT a0;
                SET ALIAS f3 AS MODULE std;
                DECLARE SAVEPOINT a1;
                SELECT 1 / 0;
                COMMIT;

                START TRANSACTION;
                SET ALIAS f4 AS MODULE std;
                COMMIT;
            ''')

        await self.con.fetchall('ROLLBACK')

        self.assertFalse(await self.is_testmode_on())

        self.assertEqual(
            await self.con.fetchall('SELECT f1::min({1})'),
            [1])

        for n in ['f2', 'f3', 'f4']:
            with self.assertRaises(edgedb.errors.InvalidReferenceError):
                async with self.con.transaction():
                    await self.con.fetchall(f'SELECT {n}::min({{1}})')

        await self.con.fetchall(
            'CONFIGURE SESSION SET __internal_testmode := true')
        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_11(self):
        # Test that SET ALIAS (and therefore CONFIGURE SESSION SET etc)
        # tracked by the server behaves exactly like DML tracked by Postgres
        # when applied around savepoints.

        async def test_funcs(*, count, working, not_working):
            for ns in working:
                self.assertEqual(
                    await self.con.fetchall(f'SELECT {ns}::min({{1}})'),
                    [1])

            await self.con.execute('DECLARE SAVEPOINT _;')
            for ns in not_working:
                with self.assertRaises(edgedb.errors.InvalidReferenceError):
                    try:
                        await self.con.fetchall(f'SELECT {ns}::min({{1}})')
                    finally:
                        await self.con.execute('ROLLBACK TO SAVEPOINT _;')
            await self.con.execute('RELEASE SAVEPOINT _;')

            actual_count = await self.con.fetchone(
                '''SELECT count(
                    test::Tmp11
                    FILTER test::Tmp11.tmp = "test_server_proto_tx_11")
                ''')
            self.assertEqual(actual_count, count)

        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute('''
                CREATE TYPE test::Tmp11 {
                    CREATE REQUIRED PROPERTY tmp -> std::str;
                };

                START TRANSACTION;
                    DECLARE SAVEPOINT c0;
                        SET ALIAS f1 AS MODULE std;
                        INSERT test::Tmp11 {
                            tmp := 'test_server_proto_tx_11'
                        };
                    DECLARE SAVEPOINT c1;
                COMMIT;

                SET ALIAS f2 AS MODULE std;
                INSERT test::Tmp11 {
                    tmp := 'test_server_proto_tx_11'
                };

                START TRANSACTION;
                    DECLARE SAVEPOINT a0;
                        SET ALIAS f3 AS MODULE std;
                        INSERT test::Tmp11 {
                            tmp := 'test_server_proto_tx_11'
                        };
                    DECLARE SAVEPOINT a1;
                        SET ALIAS f4 AS MODULE std;
                        INSERT test::Tmp11 {
                            tmp := 'test_server_proto_tx_11'
                        };
                        SELECT 1 / 0;
                COMMIT;

                START TRANSACTION;  # this never executes
                    SET ALIAS f5 AS MODULE std;
                    INSERT test::Tmp11 {
                        tmp := 'test_server_proto_tx_11'
                    };
                COMMIT;
            ''')

        await self.con.fetchall('ROLLBACK TO SAVEPOINT a1')
        await test_funcs(
            count=3,
            working=['f1', 'f2', 'f3'], not_working=['f4', 'f5'])

        await self.con.fetchall('ROLLBACK TO SAVEPOINT a0')
        await test_funcs(
            count=2,
            working=['f1', 'f2'], not_working=['f3', 'f4', 'f5'])

        await self.con.execute('''
            ROLLBACK;
            START TRANSACTION;
        ''')

        await test_funcs(
            count=1,
            working=['f1'], not_working=['f2', 'f3', 'f4', 'f5'])
        await self.con.execute('''
            COMMIT;
        ''')

        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_12(self):
        # Test that savepoint's state isn't corrupted by repeated
        # rolling back to it and stacking changes on top.

        await self.con.execute('''
            START TRANSACTION;
            DECLARE SAVEPOINT c0;
            SET ALIAS z1 AS MODULE std;
            DECLARE SAVEPOINT c1;
        ''')

        for _ in range(3):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SET ALIAS z2 AS MODULE std;
                    SELECT 1 / 0;
                ''')
            await self.con.fetchall('ROLLBACK TO SAVEPOINT c1')

            await self.con.fetchall('''
                SET ALIAS z3 AS MODULE std;
            ''')
            await self.con.fetchall('ROLLBACK TO SAVEPOINT c1')

        self.assertEqual(
            await self.con.fetchall('SELECT z1::min({1})'),
            [1])

        await self.con.fetchall('DECLARE SAVEPOINT _;')
        for ns in ['z2', 'z3']:
            with self.assertRaises(edgedb.errors.InvalidReferenceError):
                try:
                    await self.con.fetchall(f'SELECT {ns}::min({{1}})')
                finally:
                    await self.con.fetchall('ROLLBACK TO SAVEPOINT _;')
        await self.con.fetchall('RELEASE SAVEPOINT _;')

        self.assertEqual(
            await self.con.fetchall('SELECT z1::min({1})'),
            [1])

        await self.con.fetchall('ROLLBACK')

    async def test_server_proto_tx_13(self):
        # Test COMMIT abort

        async def test_funcs(*, working, not_working):
            for ns in working:
                self.assertEqual(
                    await self.con.fetchall(f'SELECT {ns}::min({{1}})'),
                    [1])

            for ns in not_working:
                with self.assertRaises(edgedb.errors.InvalidReferenceError):
                    await self.con.fetchall(f'SELECT {ns}::min({{1}})')

        for exec_meth in (self.con.execute, self.con.fetchall):
            self.assertTrue(await self.is_testmode_on())

            try:
                await self.con.execute('''
                    CREATE TYPE test::Tmp_tx_13 {
                        CREATE PROPERTY tmp_tx_13_1 -> int64;
                    };

                    ALTER TYPE test::Tmp_tx_13 {
                        CREATE LINK tmp_tx_13_2 -> test::Tmp_tx_13 {
                            ON TARGET DELETE DEFERRED RESTRICT;
                        };
                    };

                    INSERT test::Tmp_tx_13 {
                        tmp_tx_13_1 := 1
                    };

                    INSERT test::Tmp_tx_13 {
                        tmp_tx_13_1 := 2,
                        tmp_tx_13_2 := DETACHED (
                            SELECT test::Tmp_tx_13
                            FILTER test::Tmp_tx_13.tmp_tx_13_1 = 1
                            LIMIT 1
                        )
                    };

                    SET ALIAS f1 AS MODULE std;
                ''')

                await self.con.execute('''
                    SET ALIAS f2 AS MODULE std;
                    CONFIGURE SESSION SET __internal_testmode := false;

                    START TRANSACTION;

                    SET ALIAS f3 AS MODULE std;

                    DELETE (SELECT test::Tmp_tx_13
                            FILTER test::Tmp_tx_13.tmp_tx_13_1 = 1);

                    SET ALIAS f4 AS MODULE std;
                ''')

                self.assertFalse(
                    await self.con.fetchone('''
                        SELECT cfg::Config.__internal_testmode LIMIT 1
                    ''')
                )

                with self.assertRaises(edgedb.ConstraintViolationError):
                    await exec_meth('COMMIT')

                await test_funcs(working=['f1'],
                                 not_working=['f2', 'f3', 'f4'])

            finally:
                await self.con.execute('''
                    DROP TYPE test::Tmp_tx_13;
                ''')

        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_14(self):
        await self.con.execute('''
            ROLLBACK;
            ROLLBACK;
            ROLLBACK;
        ''')

        self.assertEqual(
            await self.con.fetchone('SELECT 1;'),
            1)

        await self.con.execute('''
            START TRANSACTION;
            ROLLBACK;
            ROLLBACK;
            ROLLBACK;
        ''')

        self.assertEqual(
            await self.con.fetchone('SELECT 1;'),
            1)

        await self.con.execute('''
            START TRANSACTION;
        ''')

        await self.con.execute('''
            ROLLBACK;
        ''')
        await self.con.execute('''
            ROLLBACK;
        ''')

        self.assertEqual(
            await self.con.fetchone('SELECT 1;'),
            1)

    async def test_server_proto_tx_15(self):
        commands = [
            '''
            CREATE MIGRATION ttt TO {
                module default {
                    type User {
                        required property login -> str {
                            constraint exclusive;
                        };
                    };
                };
            };
            ''',
            '''GET MIGRATION ttt;''',
            '''COMMIT MIGRATION ttt;''',
        ]

        for command in commands:
            with self.annotate(command=command):
                with self.assertRaisesRegex(
                        edgedb.QueryError,
                        'must be executed in a transaction'):
                    await self.con.execute(command)

        self.assertEqual(
            await self.con.fetchone('SELECT 1111;'),
            1111)

    async def test_server_proto_tx_16(self):
        try:
            for isol in ['', 'SERIALIZABLE', 'REPEATABLE READ']:
                stmt = 'START TRANSACTION'

                if isol:
                    stmt += f' ISOLATION {isol}'
                    expected = isol
                else:
                    expected = 'REPEATABLE READ'

                await self.con.execute(stmt)
                result = await self.con.fetchone(
                    'SELECT sys::get_transaction_isolation()')
                # Check that it's an enum and that the value is as
                # expected without explicitly listing all the possible
                # enum values for this.
                self.assertIsInstance(result, edgedb.EnumValue)
                self.assertEqual(str(result), expected)
                await self.con.execute('ROLLBACK')
        finally:
            await self.con.execute('ROLLBACK')

    async def test_server_proto_tx_17(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)

        tx1 = con1.transaction(isolation='serializable')
        tx2 = con2.transaction(isolation='serializable')
        await tx1.start()
        await tx2.start()

        try:
            async def worker(con, tx, n):
                await con.fetchone(f'''
                    WITH MODULE test
                    SELECT count(TransactionTest FILTER .name LIKE 'tx_17_{n}')
                ''')

                n2 = 1 if n == 2 else 2

                await con.fetchall(f'''
                    WITH MODULE test
                    INSERT TransactionTest {{
                        name := 'tx_17_{n2}'
                    }}
                ''')

            await asyncio.gather(
                worker(con1, tx1, 1), worker(con2, tx2, 2)
            )

            await tx1.commit()

            with self.assertRaises(edgedb.TransactionSerializationError):
                await tx2.commit()

        finally:
            if tx1.is_active():
                await tx1.rollback()
            if tx2.is_active():
                await tx2.rollback()
            await con2.aclose()

    async def test_server_proto_tx_18(self):
        # The schema altered within the transaction should be visible
        # to the error handler in order to correctly map the
        # ConstraintViolationError.
        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    'upper_str is not in upper case'):
            async with self.con.transaction():
                await self.con.execute(r"""
                    SET MODULE test;

                    CREATE ABSTRACT CONSTRAINT uppercase {
                        CREATE ANNOTATION title := "Upper case constraint";
                        USING (str_upper(__subject__) = __subject__);
                        SET errmessage := "{__subject__} is not in upper case";
                    };

                    CREATE SCALAR TYPE upper_str EXTENDING str {
                        CREATE CONSTRAINT uppercase
                    };

                    SELECT <test::upper_str>'123_hello';
                """)

    async def test_server_proto_tx_19(self):
        # A regression test ensuring that optimistic execute supports
        # custom scalar types; unfortunately no way to test that if
        # the Python driver is implemented correctly.  Still, this
        # test might catch regressions in the python driver or
        # detect new edge cases in the server implementation.

        # Not using a transaction here because optimistic execute isn't
        # enabled in transactions with DDL.

        # Note: don't change this test.  If need be, copy/paste it and
        # add new stuff to it.

        typename = f'test_{uuid.uuid4().hex}'

        await self.con.execute(f'''
            CREATE SCALAR TYPE {typename} EXTENDING int64;
        ''')

        for _ in range(10):
            result = await self.con.fetchone(f'''
                SELECT <{typename}>100000
            ''')
            self.assertEqual(result, 100000)

            result = await self.con.fetchone('''
                SELECT "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ''')
            self.assertEqual(
                result, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")


class TestServerProtoMigration(tb.QueryTestCase):

    ISOLATED_METHODS = False

    async def test_server_proto_mig_01(self):
        # Replicating the "test_edgeql_tutorial" test that might
        # disappear at some point. That test was the only one that
        # uncovered a regression in how server schema state is
        # handled, so we need to keep some form of it.

        typename = f'test_{uuid.uuid4().hex}'

        await self.con.execute(f'''
            START TRANSACTION;
            CREATE MIGRATION def TO {{
                module default {{
                    type {typename} {{
                        required property foo -> str;
                    }}
                }}
            }};
            COMMIT MIGRATION def;
            COMMIT;

            INSERT {typename} {{
                foo := '123'
            }};
        ''')

        await self.assert_query_result(
            f'SELECT {typename}.foo',
            ['123']
        )


class TestServerProtoDdlPropagation(tb.QueryTestCase):

    ISOLATED_METHODS = False

    @unittest.skipUnless(devmode.is_in_dev_mode(),
                         'the test requires devmode')
    async def test_server_proto_ddlprop_01(self):
        conargs = self.get_connect_args()

        settings = self.con.get_settings()
        pgaddr = settings.get('pgaddr')
        if pgaddr is None:
            raise RuntimeError('test requires devmode')
        pgaddr = json.loads(pgaddr)
        pgdsn = (
            f'postgres:///?user={pgaddr["user"]}&port={pgaddr["port"]}'
            f'&host={pgaddr["host"]}'
        )

        await self.con.execute('''
            CREATE TYPE Test {
                CREATE PROPERTY foo -> int16;
            };

            INSERT Test { foo := 123 };
        ''')

        self.assertEqual(
            await self.con.fetchone('SELECT Test.foo LIMIT 1'),
            123
        )

        with tempfile.TemporaryDirectory() as tmp:
            other_port = server_main.PortType.find_available_port()
            cmd = [
                sys.executable, '-m', 'edb.server.main',
                '--postgres-dsn', pgdsn,
                '--runstate-dir', tmp,
                '--port', str(other_port),
            ]

            # Note: for debug comment "stderr=subprocess.PIPE".
            proc: asyncio.Process = await asyncio.create_subprocess_exec(
                *cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )

            try:
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        con2 = await edgedb.async_connect(
                            host=tmp,
                            port=other_port,
                            user=conargs.get('user'),
                            password=conargs.get('password'),
                            database=self.get_database_name(),
                            admin=True,
                        )
                    except (ConnectionError, edgedb.ClientConnectionError):
                        if attempt >= 100:
                            raise
                        await asyncio.sleep(0.1)
                        continue
                    else:
                        break

                self.assertEqual(
                    await con2.fetchone('SELECT Test.foo LIMIT 1'),
                    123
                )

                await self.con.execute('''
                    CREATE TYPE Test2 {
                        CREATE PROPERTY foo -> str;
                    };

                    INSERT Test2 { foo := 'text' };
                ''')

                self.assertEqual(
                    await self.con.fetchone('SELECT Test2.foo LIMIT 1'),
                    'text'
                )

                self.assertEqual(
                    await con2.fetchone('SELECT Test2.foo LIMIT 1'),
                    'text'
                )

                await con2.aclose()
            finally:
                if proc.returncode is None:
                    proc.terminate()
                    await proc.wait()


class TestServerProtoDDL(tb.NonIsolatedDDLTestCase):

    async def test_server_proto_query_cache_invalidate_01(self):
        typename = 'CacheInv_01'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
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
                    await con1.fetchall(query),
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
                    await con1.fetchall(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_02(self):
        typename = 'CacheInv_02'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.fetchall(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};
            ''')

            await con2.fetchall(f'''
                INSERT test::{typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT test::{typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetchall(query),
                    edgedb.Set(['aaa']))

            await con2.fetchall(f'''
                DELETE (SELECT test::{typename});
            ''')

            await con2.fetchall(f'''
                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};
            ''')

            await con2.fetchall(f'''
                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};
            ''')

            await con2.fetchall(f'''
                INSERT test::{typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetchall(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_03(self):
        typename = 'CacheInv_03'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
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
                    await con1.fetchall(query),
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
                    await con1.fetchall(query),
                    edgedb.Set([[1, 23]]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_04(self):
        typename = 'CacheInv_04'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
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
                    await con1.fetchall(query),
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
                    await con1.fetchall(query),
                    edgedb.Set(['bbb', 'ccc']))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_05(self):
        typename = 'CacheInv_05'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                CREATE TYPE test::Other{typename} {{
                    CREATE REQUIRED PROPERTY prop2 -> std::str;
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
                    await con1.fetchall(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT test::{typename});

                ALTER TYPE test::{typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE test::{typename} {{
                    CREATE REQUIRED LINK prop1 -> test::Other{typename};
                }};

                INSERT test::{typename} {{
                    prop1 := (SELECT test::Other{typename} LIMIT 1)
                }};
            ''')

            other = await con1.fetchall(f'SELECT test::Other{typename}')

            for _ in range(5):
                self.assertEqual(
                    await con1.fetchall(query),
                    other)

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_06(self):
        typename = 'CacheInv_06'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
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

            foo = await con1.fetchall(f'SELECT test::Foo{typename}')
            bar = await con1.fetchall(f'SELECT test::Bar{typename}')

            query = f'SELECT test::{typename}.link1'

            for _ in range(5):
                self.assertEqual(
                    await con1.fetchall(query),
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
                    await con1.fetchall(query),
                    bar)

        finally:
            await con2.aclose()

    @test.xfail('''
        The error is:
        reference to a non-existent schema item
        3c6145d4-192f-11e9-83b3-edb414a0f9bf in schema
        <Schema gen:4594 at 0x7f5dbfd68198>
    ''')
    async def test_server_proto_query_cache_invalidate_07(self):
        typename = 'CacheInv_07'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE test::Foo{typename};

                CREATE ABSTRACT LINK test::link1 {{
                    CREATE PROPERTY prop1 -> std::str;
                }};

                CREATE TYPE test::{typename} {{
                    CREATE REQUIRED LINK link1 -> test::Foo{typename};
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
                    await con1.fetchall(query),
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
                    await con1.fetchall(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    @test.xfail("concurrent DDL isn't yet supported")
    async def test_server_proto_query_cache_invalidate_08(self):
        typename_prefix = 'CacheInvMulti_'
        ntasks = 5

        async with tg.TaskGroup() as g:
            cons_tasks = [
                g.create_task(self.connect(database=self.con.dbname))
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
                ret = await con.fetchall(
                    f'SELECT test::{typename_prefix}{i}.prop1')
                self.assertEqual(ret, i)

        finally:
            async with tg.TaskGroup() as g:
                for con in cons:
                    g.create_task(con.aclose())

    async def test_server_proto_query_cache_invalidate_09(self):
        typename = 'CacheInv_09'

        await self.con.execute('START TRANSACTION')
        try:
            await self.con.execute(f'''
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
                    await self.con.fetchall(query),
                    edgedb.Set(['aaa']))

            await self.con.execute(f'''
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
                    await self.con.fetchall(query),
                    edgedb.Set([123]))

        finally:
            await self.con.execute('ROLLBACK')

    async def test_server_proto_backend_tid_propagation_01(self):
        async with self._run_and_rollback():
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_01 EXTENDING str;
            ''')

            result = await self.con.fetchone('''
                SELECT (<array<tid_prop_01>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')

    async def test_server_proto_backend_tid_propagation_02(self):
        try:
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_02 EXTENDING str;
            ''')

            result = await self.con.fetchone('''
                SELECT (<array<tid_prop_02>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')
        finally:
            await self.con.execute('''
                DROP SCALAR TYPE tid_prop_02;
            ''')

    async def test_server_proto_fetch_limit_01(self):
        try:
            await self.con.execute('''
                CREATE TYPE test::FL_A {
                    CREATE PROPERTY n -> int64;
                };
                CREATE TYPE test::FL_B {
                    CREATE PROPERTY n -> int64;
                    CREATE MULTI LINK a -> test::FL_A;
                };

                WITH MODULE test
                FOR i IN {1, 2, 3, 4, 5}
                UNION (
                    INSERT FL_A {
                        n := i
                    }
                );

                WITH MODULE test
                FOR i IN {1, 2, 3, 4, 5}
                UNION (
                    INSERT FL_B {
                        n := i,
                        a := FL_A,
                    }
                );
            ''')

            result = await self.con._fetchall(
                r"""
                    WITH MODULE test
                    SELECT FL_B {
                        a ORDER BY .n,
                        a_arr := array_agg(.a)
                    } ORDER BY .n
                """,
                __limit__=2
            )

            self.assertEqual(len(result), 2)
            self.assertEqual(len(result[0].a), 2)
            self.assertEqual(len(result[0].a_arr), 2)

            # Check that things are not cached improperly.
            result = await self.con._fetchall(
                r"""
                    WITH MODULE test
                    SELECT FL_B {
                        a ORDER BY .n,
                        a_arr := array_agg(.a)
                    } ORDER BY .n
                """,
                __limit__=3
            )

            self.assertEqual(len(result), 3)
            self.assertEqual(len(result[0].a), 3)
            self.assertEqual(len(result[0].a_arr), 3)

            # Check that explicit LIMIT is not overridden
            result = await self.con._fetchall(
                r"""
                    WITH MODULE test
                    SELECT FL_B {
                        a ORDER BY .n LIMIT 3,
                        a_arr := array_agg((SELECT .a LIMIT 3)),
                        a_count := count(.a),
                    }
                    ORDER BY .n
                    LIMIT 3
                """,
                __limit__=2
            )

            self.assertEqual(len(result), 3)
            self.assertEqual(len(result[0].a), 3)
            self.assertEqual(len(result[0].a_arr), 3)
            self.assertEqual(result[0].a_count, 5)

            # Check that implicit limit does not break inline aliases.
            result = await self.con._fetchall(
                r"""
                    WITH a := {11, 12, 13}
                    SELECT _ := {9, 1, 13}
                    FILTER _ IN a;
                """,
                __limit__=1
            )

            self.assertEqual(result, edgedb.Set([13]))

        finally:
            await self.con.execute('''
                DROP TYPE test::FL_B;
                DROP TYPE test::FL_A;
            ''')

    async def test_server_proto_fetch_limit_02(self):
        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'SELECT {1, 2, 3}',
                __limit__=-2,
            )

    async def test_server_proto_fetch_limit_03(self):
        await self.con._fetchall(
            'SELECT {1, 2, 3}',
            __limit__=1,
        )

        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'SELECT {1, 2, 3}',
                __limit__=-2,
            )

    async def test_fetch_elements(self):
        result = await self.con._fetchall_json_elements('''
            SELECT {"test1", "test2"}
        ''')
        self.assertEqual(result, ['"test1"', '"test2"'])
