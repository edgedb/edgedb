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
import decimal
import http
import json
import time
import uuid
import unittest
import tempfile
import shutil

import edgedb

from edb.common import devmode
from edb.common import asyncutil
from edb.testbase import server as tb
from edb.server.compiler import enums
from edb.tools import test


class TestServerProto(tb.QueryTestCase):

    TRANSACTION_ISOLATION = False

    SETUP = '''
        CREATE TYPE Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };

        CREATE MODULE test;
        CREATE TYPE test::Tmp2 {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };

        CREATE TYPE TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };

        CREATE SCALAR TYPE RGB
            EXTENDING enum<'RED', 'BLUE', 'GREEN'>;

        CREATE GLOBAL glob -> int64;

        # Used by is_testmode_on() to ensure that config modifications
        # persist correctly when set inside and outside of (potentially
        # failing) transaction blocks.
        CONFIGURE SESSION SET __internal_testmode := true;

    '''

    TEARDOWN = '''
        DROP TYPE Tmp;
    '''

    def setUp(self):
        super().setUp()
        # Reset cached codecs for every test. That ensures that
        # tests cannot interfere with each other when the connection
        # is reused.
        self.con._clear_codecs_cache()

    async def is_testmode_on(self):
        # The idea is that if __internal_testmode value config is lost
        # (no longer "true") then this script fails.
        try:
            await self.con.execute('''
                CREATE FUNCTION testconf() -> bool
                    USING SQL $$ SELECT true; $$;
                DROP FUNCTION testconf();
            ''')
        except edgedb.InvalidFunctionDefinitionError:
            return False

        return await self.con.query_single('''
            SELECT cfg::Config.__internal_testmode LIMIT 1
        ''')

    async def test_server_proto_parse_redirect_data_01(self):
        # This is a regression fuzz test for ReadBuffer.redirect_messages().
        # The bug was related to 'D' messages that were filling the entire
        # receive buffer (8192 bytes) precisely.
        for power in range(10, 20):
            base = 2 ** power
            for i in range(base - 100, base + 100):
                v = await self.con.query_single(
                    'select str_repeat(".", <int64>$i)', i=i)
                self.assertEqual(len(v), i)

    async def test_server_proto_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.query('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.query('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        r"Missing '\)'"):
                await self.con.query('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        r"Missing '\)'"):
                await self.con.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.con.query('select 1;'),
                    edgedb.Set((1,)))

            self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            for _ in range(10):
                await self.con.execute('select 1; select 2;')

    async def test_server_proto_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.query('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.query('select 1 / 0;')
            self.assertEqual(self.con._get_last_status(), None)

            for _ in range(10):
                self.assertEqual(
                    await self.con.query('select 1;'),
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
                    await self.con.query(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.query(query, i)

    async def test_server_proto_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.query(f'select 10 // {i};')

    async def test_server_proto_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryArgumentError,
                                    "missed {'0'}"):
            await self.con.execute(f'select <int64>$0')
        self.assertEqual(
            await self.con.query('SELECT "HELLO"'),
            ["HELLO"])

    async def test_server_proto_fetch_single_command_01(self):
        r = await self.con.query('''
            CREATE TYPE server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'CREATE TYPE')

        r = await self.con.query('''
            DROP TYPE server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'DROP TYPE')

        r = await self.con.query('''
            CREATE TYPE server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.query('''
            DROP TYPE server_fetch_single_command_01;
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.query_json('''
            CREATE TYPE server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.query_json('''
            DROP TYPE server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

    async def test_server_proto_fetch_single_command_02(self):
        r = await self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])
        self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

        r = await self.con.query('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(len(r), 0)

        r = await self.con.query_json('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.query_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.query_json('''
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
                r = await self.con.query(q)
                self.assertEqual(r, [])

            for q in qs:
                r = await self.con.query_json(q)
                self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'it does not return any data'):
            await self.con.query_required_single('START TRANSACTION')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'it does not return any data'):
            await self.con.query_required_single_json('START TRANSACTION')

    async def test_server_proto_query_script_01(self):
        self.assertEqual(
            await self.con.query('''
                SET MODULE test;
                SELECT 1;
            '''),
            [1],
        )

        self.assertEqual(
            await self.con.query_json('''
                SET MODULE test;
                SELECT 1;
            '''),
            '[1]',
        )

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'it does not return any data'):
            await self.con.query_required_single('''
                SELECT 1;
                SET MODULE test;
            ''')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'it does not return any data'):
            await self.con.query_required_single_json('''
                SELECT 1;
                SET MODULE test;
            ''')

    async def test_server_proto_set_reset_alias_01(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.query('SELECT foo::min({1}) + bar::min({0})'),
            [1])

        self.assertEqual(
            await self.con.query('''
                SELECT count(
                    Tmp2 FILTER Tmp2.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET ALIAS bar;
        ''')

        self.assertEqual(
            await self.con.query('SELECT foo::min({1})'),
            [1])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 'bar::min' does not exist"):
            await self.con.query('SELECT bar::min({1})')

        await self.con.query('''
            RESET ALIAS *;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 'foo::min' does not exist"):
            await self.con.query('SELECT foo::min({3})')

        self.assertEqual(
            await self.con.query('SELECT min({4})'),
            [4])

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "object type or alias 'default::Tmp2' does not exist"):
            await self.con.query('''
                SELECT count(
                    Tmp2 FILTER Tmp2.tmp = "test_server_set_reset_alias_01");
            ''')

    async def test_server_proto_set_reset_alias_02(self):
        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
            SET ALIAS bar AS MODULE std;
            SET MODULE test;
        ''')

        self.assertEqual(
            await self.con.query('''
                SELECT count(
                    Tmp2 FILTER Tmp2.tmp = "test_server_set_reset_alias_01");
            '''),
            [0])

        await self.con.execute('''
            RESET MODULE;
        ''')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "object type or alias 'default::Tmp2' does not exist"):
            await self.con.query('''
                SELECT count(
                    Tmp2 FILTER Tmp2.tmp = "test_server_set_reset_alias_01");
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
            SET MODULE default;
        ''')

        self.assertEqual(
            await self.con.query('''
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
            await self.con.query('SELECT foo::min({3})')

    async def test_server_proto_set_reset_alias_05(self):
        # A regression test.
        # The "DECLARE SAVEPOINT a1; ROLLBACK TO SAVEPOINT a1;" commands
        # used to propagate the 'foo -> std' alias to the connection state
        # which the failed to correctly revert it back on ROLLBACK.

        await self.con.query('START TRANSACTION')

        await self.con.execute('''
            SET ALIAS foo AS MODULE std;
        ''')
        await self.con.query('DECLARE SAVEPOINT a1')
        await self.con.query('ROLLBACK TO SAVEPOINT a1')

        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute('''
                SELECT 1/0;
            ''')

        await self.con.query('ROLLBACK')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.execute('''
                SELECT foo::len('aaa')
            ''')

    async def test_server_proto_set_reset_alias_06(self):
        await self.con.query('START TRANSACTION')

        await self.con.execute('''
            SET MODULE test;
        ''')
        await self.con.query('select Tmp2')
        await self.con.query('ROLLBACK')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.query('select Tmp2')

    async def test_server_proto_set_reset_alias_07(self):
        await self.con.query('START TRANSACTION')

        await self.con.execute('''
            SET MODULE test;
        ''')
        await self.con.query('select Tmp2')

        await self.con.execute('''
            SET MODULE default;
        ''')

        with self.assertRaises(edgedb.InvalidReferenceError):
            await self.con.query('select Tmp2')

        await self.con.query('ROLLBACK')

    async def test_server_proto_set_global_01(self):
        await self.con.query('set global glob := 0')
        self.assertEqual(await self.con.query_single('select global glob'), 0)

        await self.con.query('START TRANSACTION')
        self.assertEqual(await self.con.query_single('select global glob'), 0)
        await self.con.query('set global glob := 1')
        self.assertEqual(await self.con.query_single('select global glob'), 1)

        await self.con.query('DECLARE SAVEPOINT a1')
        await self.con.query('set global glob := 2')
        self.assertEqual(await self.con.query_single('select global glob'), 2)
        await self.con.query('ROLLBACK TO SAVEPOINT a1')

        self.assertEqual(await self.con.query_single('select global glob'), 1)

        await self.con.query('ROLLBACK')

        self.assertEqual(await self.con.query_single('select global glob'), 0)

        await self.con.query('reset global glob')

    async def test_server_proto_set_global_02(self):
        await self.con.execute('START TRANSACTION')
        await self.con.execute('set global glob := 1')
        await self.con.execute('COMMIT')
        self.assertEqual(await self.con.query_single('select global glob'), 1)

    async def test_server_proto_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                await self.con.query(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            async with self.con.transaction():
                self.assertEqual(
                    await self.con.query_single(
                        'select <array<int64>>[]'),
                    [])

            self.assertEqual(
                await self.con.query(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.query('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'query_single\(\) as it may return more than one element'
            ):
                await self.con.query_single('SELECT {1, 2}')

            await self.con.query_single('SELECT <int64>{}')

            with self.assertRaisesRegex(
                edgedb.NoDataError,
                r'returned no data',
            ):
                await self.con.query_required_single('SELECT <int64>{}')

    async def test_server_proto_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.query(
                r'''select [b"\x00a", b"b", b'', b'\na', b'=A0']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na', b'=A0']]))

        self.assertEqual(
            await self.con.query(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_server_proto_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.con.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.con.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    await self.con.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    await self.con.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    await self.con.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    await self.con.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(await self.con.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                await self.con.query_required_single_json('SELECT <int64>{}')

        self.assertEqual(self.con._get_last_status(), 'SELECT')

    async def test_server_proto_basic_datatypes_04(self):
        # A regression test for enum typedescs being improperly
        # serialized and screwing up client's decoder.
        d = await self.con.query_single('''
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
                    FILTER .name = 'default::Tmp';
                """,
                [{
                    'name': 'default::Tmp',
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
                    FILTER .name = 'default::Tmp';
                """,
                [{
                    'name': 'default::Tmp',
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
                    FILTER .name = 'default::Tmp';
                """,
                [{
                    'name': 'default::Tmp',
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
                    FILTER .name = 'default::Tmp';
                """,
                [{
                    'name': 'default::Tmp',
                    'properties': [{
                        'name': 'id',
                        'foo2': 1
                    }],
                }]
            )

    async def test_server_proto_args_01(self):
        self.assertEqual(
            await self.con.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_args_02(self):
        self.assertEqual(
            await self.con.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_server_proto_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.con.query('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.con.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.con.query('select <int64>$0 + <int64>$bar;')

    async def test_server_proto_args_04(self):
        self.assertEqual(
            await self.con.query_json(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            '["aaabbb"]')

    async def test_server_proto_args_05(self):
        self.assertEqual(
            await self.con.query_json(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            '["aaabbb"]')

    async def test_server_proto_args_06(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.query_single(
                    'select <int64>$你好 + 10',
                    你好=32),
                42)

    async def test_server_proto_args_07(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'missing a type cast.*parameter'):
            await self.con.query_single(
                'select schema::Object {name} filter .id=$id', id='asd')

    async def test_server_proto_args_07_1(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    "cannot apply a shape to the parameter"):
            await self.con.query_single(
                'select schema::Object filter .id=<uuid>$id {name}', id='asd')

    async def test_server_proto_args_08(self):
        async with self._run_and_rollback():
            await self.con.execute(
                '''
                CREATE TYPE str;
                CREATE TYPE int64;
                CREATE TYPE float64;
                CREATE TYPE decimal;
                CREATE TYPE bigint;
                '''
            )

            self.assertEqual(
                await self.con.query_single('select ("1", 1, 1.1, 1.1n, 1n)'),
                ('1', 1, 1.1, decimal.Decimal('1.1'), 1)
            )

    async def test_server_proto_args_09(self):
        async with self._run_and_rollback():
            self.assertEqual(
                await self.con.query_single(
                    'WITH std AS MODULE math SELECT ("1", 1, 1.1, 1.1n, 1n)'
                ),
                ('1', 1, 1.1, decimal.Decimal('1.1'), 1)
            )

    async def test_server_proto_args_10(self):
        self.assertEqual(
            await self.con.query(
                '''
                    select 1;
                    select '!' ++ <str>$arg;
                ''',
                arg='?'
            ),
            edgedb.Set(('!?',)))

    async def test_server_proto_args_11(self):
        async with self._run_and_rollback():
            self.assertEqual(
                await self.con.query(
                    '''
                        insert Tmp { tmp := <str>$0 };
                        select Tmp.tmp ++ <str>$1;
                    ''',
                    "?", "!"),
                edgedb.Set(["?!"]),
            )

        async with self._run_and_rollback():
            self.assertEqual(
                await self.con.query(
                    '''
                        insert Tmp { tmp := <str>$foo };
                        select Tmp.tmp ++ <str>$bar;
                    ''',
                    foo="?", bar="!"),
                edgedb.Set(["?!"]),
            )

    async def test_server_proto_wait_cancel_01(self):
        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.connect(database=self.con.dbname)

        await self.con.query('START TRANSACTION')
        await self.con.query(
            'select sys::_advisory_lock(<int64>$0)', lock_key)

        try:
            async with asyncio.TaskGroup() as g:

                async def exec_to_fail():
                    with self.assertRaises(edgedb.ClientConnectionClosedError):
                        await con2.query(
                            'select sys::_advisory_lock(<int64>$0)', lock_key)

                g.create_task(exec_to_fail())

                await asyncio.sleep(0.1)
                con2.terminate()

            # Give the server some time to actually close the con2 connection.
            await asyncio.sleep(2)

        finally:
            k = await self.con.query(
                'select sys::_advisory_unlock(<int64>$0)', lock_key)
            await self.con.query('ROLLBACK')
            self.assertEqual(k, [True])

    async def test_server_proto_log_message_01(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.con.add_log_listener(on_log)
        try:
            await self.con.query(
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
        query = f'SELECT {typename}.prop1'
        con = self.con

        # __internal_testmode should be ON
        self.assertTrue(await self.is_testmode_on())

        await con.query('START TRANSACTION')
        await con.execute(f'''
            CONFIGURE SESSION SET __internal_testmode := false;
        ''')
        await con.query('DECLARE SAVEPOINT t1')
        await con.execute(f'''
            CREATE TYPE {typename} {{
                CREATE REQUIRED PROPERTY prop1 -> std::str;
            }};
        ''')
        await con.query('DECLARE SAVEPOINT t1')

        self.assertEqual(self.con._get_last_status(), 'DECLARE SAVEPOINT')

        # Make sure that __internal_testmode was indeed updated.
        self.assertFalse(await self.is_testmode_on())
        # is_testmode_on call caused an error; rollback
        await con.query('ROLLBACK TO SAVEPOINT t1')

        try:
            await con.execute(f'''
                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')
            await self.con.query('DECLARE SAVEPOINT t1')

            await con.execute(f'''
                INSERT {typename} {{
                    prop1 := 'bbb'
                }};
            ''')

            await self.con.query('DECLARE SAVEPOINT t2')

            await con.execute(f'''
                INSERT {typename} {{
                    prop1 := 'ccc'
                }};
            ''')

            await self.con.query('DECLARE SAVEPOINT t1')

            await con.execute(f'''
                INSERT {typename} {{
                    prop1 := 'ddd'
                }};
            ''')

            await self.con.query('DECLARE SAVEPOINT t3')

            self.assertEqual(
                await con.query(query),
                edgedb.Set(('aaa', 'bbb', 'ccc', 'ddd')))

            for _ in range(10):
                await con.query('ROLLBACK TO SAVEPOINT t1')

                self.assertEqual(
                    await con.query(query),
                    edgedb.Set(('aaa', 'bbb', 'ccc')))

            await con.query('RELEASE SAVEPOINT t1')
            self.assertEqual(
                await con.query(query),
                edgedb.Set(('aaa', 'bbb', 'ccc')))

            for _ in range(5):
                await con.query('ROLLBACK TO SAVEPOINT t1')
                self.assertEqual(
                    await con.query(query),
                    edgedb.Set(('aaa',)))

            await con.query('RELEASE SAVEPOINT t1')
            await con.query('RELEASE SAVEPOINT t1')
            await con.query('ROLLBACK TO SAVEPOINT t1')

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    ".*Savepoint.*does not exist"):
                await con.query(query)

        finally:
            await con.query('ROLLBACK')

        # __internal_testmode should be ON, just as when the test method
        # was called.
        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_savepoint_02(self):
        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.query('DECLARE SAVEPOINT t1')

        with self.assertRaisesRegex(
                edgedb.TransactionError, 'savepoints can only be used in tra'):
            await self.con.query('DECLARE SAVEPOINT t1')

    async def test_server_proto_tx_savepoint_03(self):
        # Test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints.

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT t0')

        try:
            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.query('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query_single('''
                    RELEASE SAVEPOINT t1;
                ''')

            await self.con.query('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.query('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('''
                    RELEASE SAVEPOINT t1;
                ''')

        finally:
            await self.con.query('ROLLBACK')

            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_04(self):
        # Test that PARSE/EXECUTE/OPPORTUNISTIC-EXECUTE play nice
        # with savepoints.

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT t0')

        try:
            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.query('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('SELECT 1;')

            await self.con.query('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.query_single('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('SELECT 1;')

        finally:
            await self.con.query('ROLLBACK')

            self.assertEqual(
                await self.con.query('SELECT 1;'),
                [1])

    async def test_server_proto_tx_savepoint_05(self):
        # Test RELEASE SAVEPOINT

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT t0')

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

            await self.con.query('''
                ROLLBACK TO SAVEPOINT t0;
            ''')

            await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "there is no 't1' savepoint"):
                await self.con.query('''
                    RELEASE SAVEPOINT t1;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.query('''
                    RELEASE SAVEPOINT t1;
                ''')

        finally:
            await self.con.query('ROLLBACK')

            await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_savepoint_06(self):
        # Test that SIMPLE QUERY can combine START TRANSACTION
        # and DECLARE SAVEPOINT; test basic TransactionError
        # reflection.

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT t0')

        try:
            await self.con.execute('SELECT 1;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            await self.con.query('''
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
            await self.con.query('ROLLBACK')

            await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_savepoint_07(self):
        con = self.con

        await con.query('START TRANSACTION')
        await con.query('DECLARE SAVEPOINT t1')
        await con.execute(f'''
            SET ALIAS t1 AS MODULE std;
            SET ALIAS t1 AS MODULE std;
        ''')
        await con.query('DECLARE SAVEPOINT t2')
        await con.execute(f'''
            SET ALIAS t2 AS MODULE std;
        ''')

        self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

        try:

            for _ in range(5):
                self.assertEqual(
                    await con.query('SELECT t1::min({1}) + t2::min({2})'),
                    [3])

            await self.con.query('ROLLBACK TO SAVEPOINT t2')

            for _ in range(5):
                self.assertEqual(
                    await con.query(
                        'SELECT t1::min({1}) + std::min({100})'),
                    [101])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't2::min' does not exist"):
                await con.query('SELECT t1::min({1}) + t2::min({2})')

            await self.con.query('''
                ROLLBACK TO SAVEPOINT t1;
            ''')

            self.assertEqual(
                await con.query('SELECT std::min({100})'),
                [100])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't1::min' does not exist"):
                await con.query('SELECT t1::min({1})')

        finally:
            await con.query('ROLLBACK')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 't1::min' does not exist"):
            await con.query('SELECT t1::min({1})')

    async def test_server_proto_tx_savepoint_08(self):
        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.query('START TRANSACTION')
            await con.query('DECLARE SAVEPOINT t1')
            await con.query('SET ALIAS t1 AS MODULE std')
            await con.query('SELECT 1 / 0')

        await con.query('ROLLBACK')
        self.assertEqual(self.con._get_last_status(), 'ROLLBACK TRANSACTION')

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                "function 't1::min' does not exist"):
            await con.query_single('SELECT t1::min({1})')

    async def test_server_proto_tx_savepoint_09(self):
        # Test basic SET ALIAS tracking in transactions/savepoints;
        # test also that ROLLBACK TO SAVEPOINT can be safely combined
        # with other commands in the same SIMPLE QUERY.

        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.query('START TRANSACTION')
            await con.query('DECLARE SAVEPOINT t1')
            await con.query('SET ALIAS t1 AS MODULE std')
            await con.query('SELECT 1 / 0')

        try:
            await con.query('ROLLBACK TO SAVEPOINT t1')
            await con.query('SET ALIAS t2 AS MODULE std')
            self.assertEqual(self.con._get_last_status(), 'SET ALIAS')

            self.assertEqual(
                await con.query('SELECT t2::min({2})'),
                [2])

            with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError,
                    "function 't1::min' does not exist"):
                await con.query('SELECT t1::min({1})')

        finally:
            await con.query('ROLLBACK')

    async def test_server_proto_tx_savepoint_10(self):
        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.query('START TRANSACTION')
            await con.query('DECLARE SAVEPOINT t1')
            await con.query('DECLARE SAVEPOINT t2')
            await con.query('SELECT 1/0')

        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await con.query('ROLLBACK TO SAVEPOINT t2')
                await self.con.query('SELECT 1/0')

            await con.query('''
                ROLLBACK TO SAVEPOINT t1;
            ''')

            self.assertEqual(
                await con.query('SELECT 42+1+1+1'),
                [45])
        finally:
            await con.query('ROLLBACK')

    async def test_server_proto_tx_savepoint_11(self):
        con = self.con

        with self.assertRaises(edgedb.DivisionByZeroError):
            await con.query('START TRANSACTION')
            await con.query('DECLARE SAVEPOINT t1')
            await con.query('DECLARE SAVEPOINT t2')
            await con.query('SELECT 1/0')

        try:
            await con.query('ROLLBACK TO SAVEPOINT t2')

            self.assertEqual(
                await con.query_single('SELECT 42+1+1+1+1'),
                46)
        finally:
            await con.query('ROLLBACK')

    async def test_server_proto_tx_savepoint_12(self):
        con = self.con

        await con.query('START TRANSACTION')
        await con.query('DECLARE SAVEPOINT p1')
        await con.query('DECLARE SAVEPOINT p2')
        await con.query('ROLLBACK TO SAVEPOINT p1')

        try:
            with self.assertRaises(edgedb.TransactionError):
                await con.query('ROLLBACK TO SAVEPOINT p2')
        finally:
            await con.query('ROLLBACK')

    async def test_server_proto_tx_savepoint_13(self):
        con = self.con

        await con.query('START TRANSACTION')
        await con.query('DECLARE SAVEPOINT p1')
        await con.query('DECLARE SAVEPOINT p2')
        await con.query('RELEASE SAVEPOINT p1')

        try:
            with self.assertRaises(edgedb.TransactionError):
                await con.query('ROLLBACK TO SAVEPOINT p2')
        finally:
            await con.query('ROLLBACK')

    async def test_server_proto_tx_01(self):
        await self.con.query('START TRANSACTION')

        try:
            await self.con.execute('SELECT 1;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError, "current transaction is aborted"):
                await self.con.execute('SELECT 1;')

            # Test that syntax errors are not papered over by
            # a TransactionError.
            with self.assertRaisesRegex(
                    edgedb.EdgeQLSyntaxError, "Unexpected 'ROLLBA'"):
                await self.con.execute('ROLLBA;')

        finally:
            await self.con.query('ROLLBACK')

        await self.con.execute('SELECT 1;')

    async def test_server_proto_tx_02(self):
        # Test Parse/Execute with ROLLBACK; use new connection
        # to make sure that Opportunistic Execute isn't used.

        con2 = await self.connect(database=self.con.dbname)

        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await con2.query('START TRANSACTION')
                await con2.execute('''
                    SELECT 1;
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    "current transaction is aborted"):
                await con2.query('SELECT 1;')

            await con2.query('ROLLBACK')

            self.assertEqual(
                await con2.query('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.query('DECLARE SAVEPOINT t1')
        finally:
            await con2.aclose()

    async def test_server_proto_tx_03(self):
        # Test Opportunistic Execute with ROLLBACK; use new connection
        # to make sure that "ROLLBACK" is cached.

        con2 = await self.connect(database=self.con.dbname)

        try:
            for _ in range(5):
                await con2.query('START TRANSACTION')
                await con2.query('ROLLBACK')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await con2.query('START TRANSACTION')
                await con2.execute('''
                    SELECT 1;
                    SELECT 1 / 0;
                ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    "current transaction is aborted"):
                await con2.query('SELECT 1;')

            await con2.query('ROLLBACK')

            self.assertEqual(
                await con2.query('SELECT 1;'),
                [1])

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'savepoints can only be used in tra'):
                await con2.query('DECLARE SAVEPOINT t1')
        finally:
            await con2.aclose()

    async def test_server_proto_tx_04(self):
        await self.con.query('START TRANSACTION')

        try:
            with self.assertRaisesRegex(
                    edgedb.TransactionError, 'already in transaction'):

                await self.con.query('START TRANSACTION')
        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_tx_05(self):
        # Test that caching of compiled queries doesn't interfere
        # with transactions.

        query = 'SELECT "test_server_proto_tx_04"'

        for _ in range(5):
            self.assertEqual(
                await self.con.query(query),
                ['test_server_proto_tx_04'])

        await self.con.query('START TRANSACTION')

        for i in range(5):
            self.assertEqual(
                await self.con.query(query),
                ['test_server_proto_tx_04'])

            self.assertEqual(
                await self.con.query('SELECT <int64>$0', i),
                [i])

        await self.con.query('ROLLBACK')

    async def test_server_proto_tx_06(self):
        # Test that caching of compiled queries in other connections
        # doesn't interfere with transactions.

        query = 'SELECT 1'

        con2 = await self.connect(database=self.con.dbname)
        try:
            for _ in range(5):
                self.assertEqual(
                    await self.con.query(query),
                    [1])
        finally:
            await con2.aclose()

        await self.con.query('START TRANSACTION')

        try:
            for _ in range(5):
                self.assertEqual(
                    await self.con.query(query),
                    [1])
        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_tx_07(self):
        # Test that START TRANSACTION reflects its modes.

        try:
            await self.con.query('''
                START TRANSACTION ISOLATION SERIALIZABLE, READ ONLY,
                    DEFERRABLE;
            ''')

            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'read-only transaction'):

                await self.con.query('''
                    INSERT Tmp {
                        tmp := 'aaa'
                    };
                ''')
        finally:
            await self.con.query(f'''
                ROLLBACK;
            ''')

        self.assertEqual(
            await self.con.query('SELECT 42'),
            [42])

    async def test_server_proto_tx_08(self):
        try:
            await self.con.query('''
                START TRANSACTION ISOLATION REPEATABLE READ, READ ONLY;
            ''')

            self.assertEqual(
                await self.con.query(
                    'select <str>sys::get_transaction_isolation();',
                ),
                ["RepeatableRead"],
            )
        finally:
            await self.con.query(f'''
                ROLLBACK;
            ''')

    async def test_server_proto_tx_09(self):
        try:
            with self.assertRaisesRegex(
                    edgedb.TransactionError,
                    'only supported in read-only transactions'):
                await self.con.query('''
                    START TRANSACTION ISOLATION REPEATABLE READ;
                ''')
        finally:
            await self.con.query(f'''
                ROLLBACK;
            ''')

    async def test_server_proto_tx_10(self):
        # Basic test that ROLLBACK works on SET ALIAS changes.

        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.query('START TRANSACTION')
            await self.con.query('DECLARE SAVEPOINT c0')
            await self.con.query('SET ALIAS f1 AS MODULE std')
            await self.con.query('DECLARE SAVEPOINT c1')
            await self.con.query('''
                CONFIGURE SESSION SET __internal_testmode := false
            ''')
            await self.con.query('COMMIT')

            await self.con.query('START TRANSACTION')
            await self.con.query('SET ALIAS f2 AS MODULE std')

            await self.con.query('DECLARE SAVEPOINT a0')
            await self.con.query('SET ALIAS f3 AS MODULE std')
            await self.con.query('DECLARE SAVEPOINT a1')
            await self.con.query('SELECT 1 / 0')
            await self.con.query('COMMIT')

            await self.con.query('START TRANSACTION')
            await self.con.query('SET ALIAS f4 AS MODULE std')
            await self.con.query('COMMIT')

        await self.con.query('ROLLBACK')

        self.assertFalse(await self.is_testmode_on())

        self.assertEqual(
            await self.con.query('SELECT f1::min({1})'),
            [1])

        for n in ['f2', 'f3', 'f4']:
            with self.assertRaises(edgedb.errors.InvalidReferenceError):
                async with self.con.transaction():
                    await self.con.query(f'SELECT {n}::min({{1}})')

        await self.con.query(
            'CONFIGURE SESSION SET __internal_testmode := true')
        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_11(self):
        # Test that SET ALIAS (and therefore CONFIGURE SESSION SET etc)
        # tracked by the server behaves exactly like DML tracked by Postgres
        # when applied around savepoints.

        async def test_funcs(*, count, working, not_working):
            for ns in working:
                self.assertEqual(
                    await self.con.query(f'SELECT {ns}::min({{1}})'),
                    [1])

            await self.con.query('DECLARE SAVEPOINT _')
            for ns in not_working:
                with self.assertRaises(edgedb.errors.InvalidReferenceError):
                    try:
                        await self.con.query(f'SELECT {ns}::min({{1}})')
                    finally:
                        await self.con.query('ROLLBACK TO SAVEPOINT _;')
            await self.con.query('RELEASE SAVEPOINT _')

            actual_count = await self.con.query_single(
                '''SELECT count(
                    Tmp11
                    FILTER Tmp11.tmp = "test_server_proto_tx_11")
                ''')
            self.assertEqual(actual_count, count)

        await self.con.execute('''
            CREATE TYPE Tmp11 {
                CREATE REQUIRED PROPERTY tmp -> std::str;
            };
        ''')

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT c0')
        await self.con.query('SET ALIAS f1 AS MODULE std')
        await self.con.execute('''
            INSERT Tmp11 {
                tmp := 'test_server_proto_tx_11'
            };
        ''')
        await self.con.query('DECLARE SAVEPOINT c1')
        await self.con.query('COMMIT')

        await self.con.query('START TRANSACTION')
        await self.con.query('SET ALIAS f2 AS MODULE std')
        await self.con.execute('''
            INSERT Tmp11 {
                tmp := 'test_server_proto_tx_11'
            };
        ''')

        await self.con.query('DECLARE SAVEPOINT a0')
        await self.con.query('SET ALIAS f3 AS MODULE std')
        await self.con.execute('''
            INSERT Tmp11 {
                tmp := 'test_server_proto_tx_11'
            };
        ''')

        await self.con.query('DECLARE SAVEPOINT a1')
        await self.con.query('SET ALIAS f4 AS MODULE std')
        await self.con.execute('''
            INSERT Tmp11 {
                tmp := 'test_server_proto_tx_11'
            };
        ''')
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.query('SELECT 1 / 0')

        await self.con.query('ROLLBACK TO SAVEPOINT a1')
        await test_funcs(
            count=3,
            working=['f1', 'f2', 'f3'], not_working=['f4', 'f5'])

        await self.con.query('ROLLBACK TO SAVEPOINT a0')
        await test_funcs(
            count=2,
            working=['f1', 'f2'], not_working=['f3', 'f4', 'f5'])

        await self.con.query('ROLLBACK')
        await self.con.query('START TRANSACTION')

        await test_funcs(
            count=1,
            working=['f1'], not_working=['f2', 'f3', 'f4', 'f5'])
        await self.con.query('COMMIT')

    async def test_server_proto_tx_12(self):
        # Test that savepoint's state isn't corrupted by repeated
        # rolling back to it and stacking changes on top.

        await self.con.query('START TRANSACTION')
        await self.con.query('DECLARE SAVEPOINT c0')
        await self.con.query('SET ALIAS z1 AS MODULE std')
        await self.con.query('DECLARE SAVEPOINT c1')

        for _ in range(3):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('''
                    SET ALIAS z2 AS MODULE std;
                    SELECT 1 / 0;
                ''')
            await self.con.query('ROLLBACK TO SAVEPOINT c1')

            await self.con.query('''
                SET ALIAS z3 AS MODULE std;
            ''')
            await self.con.query('ROLLBACK TO SAVEPOINT c1')

        self.assertEqual(
            await self.con.query('SELECT z1::min({1})'),
            [1])

        await self.con.query('DECLARE SAVEPOINT _;')
        for ns in ['z2', 'z3']:
            with self.assertRaises(edgedb.errors.InvalidReferenceError):
                try:
                    await self.con.query(f'SELECT {ns}::min({{1}})')
                finally:
                    await self.con.query('ROLLBACK TO SAVEPOINT _;')
        await self.con.query('RELEASE SAVEPOINT _;')

        self.assertEqual(
            await self.con.query('SELECT z1::min({1})'),
            [1])

        await self.con.query('ROLLBACK')

    async def test_server_proto_tx_13(self):
        # Test COMMIT abort

        async def test_funcs(*, working, not_working):
            for ns in working:
                self.assertEqual(
                    await self.con.query(f'SELECT {ns}::min({{1}})'),
                    [1])

            for ns in not_working:
                with self.assertRaises(edgedb.errors.InvalidReferenceError):
                    await self.con.query(f'SELECT {ns}::min({{1}})')

        self.assertTrue(await self.is_testmode_on())

        try:
            await self.con.execute('''
                CREATE TYPE Tmp_tx_13 {
                    CREATE PROPERTY tmp_tx_13_1 -> int64;
                };

                ALTER TYPE Tmp_tx_13 {
                    CREATE LINK tmp_tx_13_2 -> Tmp_tx_13 {
                        ON TARGET DELETE DEFERRED RESTRICT;
                    };
                };

                INSERT Tmp_tx_13 {
                    tmp_tx_13_1 := 1
                };

                INSERT Tmp_tx_13 {
                    tmp_tx_13_1 := 2,
                    tmp_tx_13_2 := DETACHED (
                        SELECT Tmp_tx_13
                        FILTER Tmp_tx_13.tmp_tx_13_1 = 1
                        LIMIT 1
                    )
                };

                SET ALIAS f1 AS MODULE std;
            ''')

            await self.con.query('START TRANSACTION')
            await self.con.execute('''
                SET ALIAS f2 AS MODULE std;
                CONFIGURE SESSION SET __internal_testmode := false;
            ''')
            await self.con.query('SET ALIAS f3 AS MODULE std')
            await self.con.execute('''
                DELETE (SELECT Tmp_tx_13
                        FILTER Tmp_tx_13.tmp_tx_13_1 = 1);
                SET ALIAS f4 AS MODULE std;
            ''')

            self.assertFalse(
                await self.con.query_single('''
                    SELECT cfg::Config.__internal_testmode LIMIT 1
                ''')
            )

            with self.assertRaises(edgedb.ConstraintViolationError):
                await self.con.query('COMMIT')

            await test_funcs(working=['f1'],
                             not_working=['f2', 'f3', 'f4'])

        finally:
            await self.con.execute('''
                DROP TYPE Tmp_tx_13;
            ''')

        self.assertTrue(await self.is_testmode_on())

    async def test_server_proto_tx_14(self):
        await self.con.query('ROLLBACK')
        await self.con.query('ROLLBACK')
        await self.con.query('ROLLBACK')

        self.assertEqual(
            await self.con.query_single('SELECT 1;'),
            1)

        await self.con.query('START TRANSACTION')
        await self.con.query('ROLLBACK')
        await self.con.query('ROLLBACK')
        await self.con.query('ROLLBACK')

        self.assertEqual(
            await self.con.query_single('SELECT 1;'),
            1)

        await self.con.query('START TRANSACTION')

        await self.con.query('ROLLBACK')
        await self.con.query('ROLLBACK')

        self.assertEqual(
            await self.con.query_single('SELECT 1;'),
            1)

    @test.xfail("... we currently always use serializable")
    async def test_server_proto_tx_16(self):
        try:
            for isol, expected in [
                ('', 'RepeatableRead'),
                ('SERIALIZABLE', 'Serializable'),
                ('REPEATABLE READ', 'RepeatableRead')
            ]:
                stmt = 'START TRANSACTION'

                if isol:
                    stmt += f' ISOLATION {isol}'

                await self.con.query(stmt)
                result = await self.con.query_single(
                    'SELECT sys::get_transaction_isolation()')
                # Check that it's an enum and that the value is as
                # expected without explicitly listing all the possible
                # enum values for this.
                self.assertIsInstance(result, edgedb.EnumValue)
                self.assertEqual(str(result), expected)
                await self.con.query('ROLLBACK')
        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_tx_17(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)

        tx1 = con1.transaction()
        tx2 = con2.transaction()
        await tx1.start()
        await tx2.start()

        try:
            async def worker(con, tx, n):
                await con.query_single(f'''
                    SELECT count(TransactionTest FILTER .name LIKE 'tx_17_{n}')
                ''')

                n2 = 1 if n == 2 else 2

                await con.query(f'''
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

                    CREATE ABSTRACT CONSTRAINT uppercase {
                        CREATE ANNOTATION title := "Upper case constraint";
                        USING (str_upper(__subject__) = __subject__);
                        SET errmessage := "{__subject__} is not in upper case";
                    };

                    CREATE SCALAR TYPE upper_str EXTENDING str {
                        CREATE CONSTRAINT uppercase
                    };

                    SELECT <upper_str>'123_hello';
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
            result = await self.con.query_single(f'''
                SELECT <{typename}>100000
            ''')
            self.assertEqual(result, 100000)

            result = await self.con.query_single('''
                SELECT "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ''')
            self.assertEqual(
                result, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

    async def test_server_proto_tx_20(self):
        await self.con.query('START TRANSACTION')
        try:
            with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot execute CREATE DATABASE in a transaction'
            ):
                await self.con.execute('CREATE DATABASE t1;')
        finally:
            await self.con.query('ROLLBACK')

        with self.assertRaisesRegex(
            edgedb.QueryError,
            'cannot execute CREATE DATABASE with other commands'
        ):
            await self.con.execute('''
                SELECT 1;
                CREATE DATABASE t1;
            ''')

    async def test_server_proto_tx_21(self):
        # Test that the Postgres connection state is restored
        # in a separate implicit transaction from where a
        # START TRANSACTION is called.
        #
        # If state is restored in the same implicit transaction
        # (between SYNCs) with starting a new DEFERRABLE transaction
        # Postgres would complain that the transaction has already
        # been started and it's too late to change it to DEFERRABLE.
        #
        # This test starts a separate cluster with a limited backend
        # connections pool size, sets some session config aiming to
        # set it on all backend connections in the pool, and then tries
        # to run a transaction on a brand new EdgeDB connection.
        # When that connection gets a PG connection from the backend pool,
        # it must restore the state to "default". If it fails to inject
        # a "SYNC" between state reset and "START TRANSACTION" this
        # test would fail.

        async with tb.start_edgedb_server(max_allowed_connections=4) as sd:
            for _ in range(8):
                con1 = await sd.connect()
                try:
                    await con1.query('SET ALIAS foo AS MODULE default')
                finally:
                    await con1.aclose()

            con2 = await sd.connect()
            try:
                await con2.query('START TRANSACTION READ ONLY, DEFERRABLE')
            finally:
                await con2.aclose()

            con2 = await sd.connect()
            try:
                await con2.execute('START TRANSACTION READ ONLY, DEFERRABLE')
            finally:
                await con2.aclose()

    async def test_server_proto_tx_22(self):
        await self.con.query('START TRANSACTION')
        try:
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('SELECT 1/0')
            # The commit
            with self.assertRaises(edgedb.TransactionError):
                await self.con.query('COMMIT')
        finally:
            await self.con.query('ROLLBACK')

        self.assertEqual(await self.con.query_single('SELECT 42'), 42)


class TestServerProtoMigration(tb.QueryTestCase):

    TRANSACTION_ISOLATION = False

    async def test_server_proto_mig_01(self):
        # Replicating the "test_edgeql_tutorial" test that might
        # disappear at some point. That test was the only one that
        # uncovered a regression in how server schema state is
        # handled, so we need to keep some form of it.

        typename = f'test_{uuid.uuid4().hex}'

        await self.con.execute(f'''
            START MIGRATION TO {{
                module default {{
                    type {typename} {{
                        required property foo -> str;
                    }}
                }}
            }};
            POPULATE MIGRATION;
            COMMIT MIGRATION;

            INSERT {typename} {{
                foo := '123'
            }};
        ''')

        await self.assert_query_result(
            f'SELECT {typename}.foo',
            ['123']
        )


class TestServerProtoDdlPropagation(tb.QueryTestCase):

    TRANSACTION_ISOLATION = False

    def setUp(self):
        super().setUp()
        self.runstate_dir = tempfile.mkdtemp()

    def tearDown(self):
        try:
            shutil.rmtree(self.runstate_dir)
        finally:
            super().tearDown()

    def get_adjacent_server_args(self):
        server_args = {}
        if self.backend_dsn:
            server_args['backend_dsn'] = self.backend_dsn
        else:
            server_args['adjacent_to'] = self.con
        server_args['runstate_dir'] = self.runstate_dir
        server_args['net_worker_mode'] = 'disabled'
        return server_args

    @unittest.skipUnless(devmode.is_in_dev_mode(),
                         'the test requires devmode')
    async def test_server_proto_ddlprop_01(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        conargs = self.get_connect_args()

        await self.con.execute('''
            CREATE TYPE Test {
                CREATE PROPERTY foo -> int16;
            };

            INSERT Test { foo := 123 };
        ''')

        self.assertEqual(
            await self.con.query_single('SELECT Test.foo LIMIT 1'),
            123
        )

        server_args = self.get_adjacent_server_args()
        async with tb.start_edgedb_server(**server_args) as sd:

            con2 = await sd.connect(
                user=conargs.get('user'),
                password=conargs.get('password'),
                database=self.get_database_name(),
            )

            try:
                self.assertEqual(
                    await con2.query_single('SELECT Test.foo LIMIT 1'),
                    123
                )

                await self.con.execute('''
                    CREATE TYPE Test2 {
                        CREATE PROPERTY foo -> str;
                    };

                    INSERT Test2 { foo := 'text' };
                ''')

                self.assertEqual(
                    await self.con.query_single('SELECT Test2.foo LIMIT 1'),
                    'text'
                )

                # Give some time for the other server to re-introspect the
                # schema: the first attempt of querying Test2 might fail.
                # We'll give it generous 30 seconds to accomodate slow CI.
                async for tr in self.try_until_succeeds(
                    ignore=edgedb.InvalidReferenceError, timeout=30,
                ):
                    async with tr:
                        self.assertEqual(
                            await con2.query_single(
                                'SELECT Test2.foo LIMIT 1',
                            ),
                            'text',
                        )

            finally:
                await con2.aclose()

            # Other tests mutate global DDL, hence try a few times.
            async for tr in self.try_until_succeeds(
                ignore=edgedb.TransactionSerializationError
            ):
                async with tr:
                    await self.con.execute('''
                        CREATE SUPERUSER ROLE ddlprop01 {
                            SET password := 'aaaa';
                        }
                    ''')

            # Give some time for the other server to receive the
            # updated roles notification and re-fetch them.
            # We'll give it generous 5 seconds to accomodate slow CI.
            async for tr in self.try_until_succeeds(
                ignore=edgedb.AuthenticationError
            ):
                async with tr:
                    con3 = await sd.connect(
                        user='ddlprop01',
                        password='aaaa',
                        database=self.get_database_name(),
                    )

            try:
                self.assertEqual(
                    await con3.query_single('SELECT 42'),
                    42
                )
            finally:
                await con3.aclose()

                # Other tests mutate global DDL, hence try a few times.
                async for tr in self.try_until_succeeds(
                    ignore=edgedb.TransactionSerializationError
                ):
                    async with tr:
                        await self.con.execute('''
                            DROP ROLE ddlprop01;
                        ''')

    @unittest.skipUnless(devmode.is_in_dev_mode(),
                         'the test requires devmode')
    async def test_server_adjacent_database_propagation(self):
        if not self.has_create_database:
            self.skipTest('create database is not supported by the backend')

        conargs = self.get_connect_args()

        server_args = self.get_adjacent_server_args()
        async with tb.start_edgedb_server(**server_args) as sd:

            # Run twice to make sure there is no lingering accessibility state
            for _ in range(2):
                await self.con.execute('''
                    CREATE DATABASE test_db_prop;
                ''')

                # Make sure the adjacent server picks up on the new db
                async for tr in self.try_until_succeeds(
                    ignore=edgedb.UnknownDatabaseError,
                    timeout=30,
                ):
                    async with tr:
                        con2 = await sd.connect(
                            user=conargs.get('user'),
                            password=conargs.get('password'),
                            database="test_db_prop",
                        )

                        await con2.query("select 1")
                        await con2.aclose()

                await tb.drop_db(self.con, 'test_db_prop')

                # Wait until the drop message hit the adjacent server, or the
                # next create may make the db inaccessible, see also #5081
                with self.assertRaises(edgedb.UnknownDatabaseError):
                    while True:
                        async for tr in self.try_until_succeeds(
                            ignore=edgedb.AccessError,
                            timeout=30,
                        ):
                            async with tr:
                                con2 = await sd.connect(
                                    user=conargs.get('user'),
                                    password=conargs.get('password'),
                                    database="test_db_prop",
                                )
                                await con2.aclose()

            # Now, recreate the DB and try the other way around
            con2 = await sd.connect(
                user=conargs.get('user'),
                password=conargs.get('password'),
                database=self.get_database_name(),
            )

            await con2.execute('''
                CREATE DATABASE test_db_prop;
            ''')

            # Accessible from the adjacent server
            con3 = await sd.connect(
                user=conargs.get('user'),
                password=conargs.get('password'),
                database="test_db_prop",
            )
            await con3.query("select 1")
            await con3.aclose()

            async for tr in self.try_until_succeeds(
                ignore=edgedb.UnknownDatabaseError,
                timeout=30,
            ):
                async with tr:
                    con1 = await self.connect(database="test_db_prop")
                    await con1.query("select 1")
                    await con1.aclose()

            await tb.drop_db(con2, 'test_db_prop')

            await con2.aclose()

    @unittest.skipUnless(devmode.is_in_dev_mode(),
                         'the test requires devmode')
    async def test_server_adjacent_extension_propagation(self):
        headers = {
            'Authorization': self.make_auth_header(),
        }

        server_args = self.get_adjacent_server_args()
        async with tb.start_edgedb_server(**server_args) as sd:

            await self.con.execute("CREATE EXTENSION notebook;")

            # First, ensure that the local server is aware of the new ext.
            async for tr in self.try_until_succeeds(
                ignore=self.failureException,
            ):
                async with tr:
                    with self.http_con(server=self) as http_con:
                        response, _, status = self.http_con_json_request(
                            http_con,
                            path="notebook",
                            body={"queries": ["SELECT 1"]},
                            headers=headers,
                        )

                        self.assertEqual(
                            status, http.HTTPStatus.OK, f"fuck: {response} {_}")
                        self.assert_data_shape(
                            response,
                            {
                                'kind': 'results',
                                'results': [
                                    {
                                        'kind': 'data',
                                    },
                                ],
                            },
                        )

            # Make sure the adjacent server picks up on the new extension
            async for tr in self.try_until_succeeds(
                ignore=self.failureException,
            ):
                async with tr:
                    with self.http_con(server=sd) as http_con:
                        response, _, status = self.http_con_json_request(
                            http_con,
                            path="notebook",
                            body={"queries": ["SELECT 1"]},
                            headers=headers,
                        )

                        self.assertEqual(status, http.HTTPStatus.OK)
                        self.assert_data_shape(
                            response,
                            {
                                'kind': 'results',
                                'results': [
                                    {
                                        'kind': 'data',
                                    },
                                ],
                            },
                        )

            # Now drop the extension.
            await self.con.execute("DROP EXTENSION notebook;")

            # First, ensure that the local server is aware of the new ext.
            async for tr in self.try_until_succeeds(
                ignore=self.failureException,
            ):
                async with tr:
                    with self.http_con(server=self) as http_con:
                        response, _, status = self.http_con_json_request(
                            http_con,
                            path="notebook",
                            body={"queries": ["SELECT 1"]},
                            headers=headers,
                        )

                        self.assertEqual(status, http.HTTPStatus.NOT_FOUND)

            # Make sure the adjacent server picks up on the new extension
            async for tr in self.try_until_succeeds(
                ignore=self.failureException,
            ):
                async with tr:
                    with self.http_con(server=sd) as http_con:
                        response, _, status = self.http_con_json_request(
                            http_con,
                            path="notebook",
                            body={"queries": ["SELECT 1"]},
                            headers=headers,
                        )

                        self.assertEqual(status, http.HTTPStatus.NOT_FOUND)


class TestServerProtoDDL(tb.DDLTestCase):

    TRANSACTION_ISOLATION = False

    async def test_server_proto_create_db_01(self):
        if not self.has_create_database:
            self.skipTest('create database is not supported by the backend')

        db = 'test_server_proto_create_db_01'

        con1 = self.con

        cleanup = False
        try:
            for _ in range(3):
                await con1.execute(f'''
                    CREATE DATABASE {db};
                ''')
                cleanup = True

                con2 = await self.connect(database=db)
                try:
                    self.assertEqual(
                        await con2.query_single('SELECT 1'),
                        1
                    )
                finally:
                    await con2.aclose()

                await tb.drop_db(con1, db)
                cleanup = False
        finally:
            if cleanup:
                await tb.drop_db(con1, db)

    async def test_server_proto_query_cache_invalidate_01(self):
        typename = 'CacheInv_01'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};

                INSERT {typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_02(self):
        typename = 'CacheInv_02'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.query(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};
            ''')

            await con2.query(f'''
                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['aaa']))

            await con2.query(f'''
                DELETE (SELECT {typename});
            ''')

            await con2.query(f'''
                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};
            ''')

            await con2.query(f'''
                ALTER TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};
            ''')

            await con2.query(f'''
                INSERT {typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_03(self):
        typename = 'CacheInv_03'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> array<std::str>;
                }};

                INSERT {typename} {{
                    prop1 := ['a', 'aa']
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set([['a', 'aa']]))

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> array<std::int64>;
                }};

                INSERT {typename} {{
                    prop1 := [1, 23]
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set([[1, 23]]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_04(self):
        typename = 'CacheInv_04'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED MULTI PROPERTY prop1 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := {{'bbb', 'ccc'}}
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['bbb', 'ccc']))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_05(self):
        typename = 'CacheInv_05'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                CREATE TYPE Other{typename} {{
                    CREATE REQUIRED PROPERTY prop2 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := 'aaa'
                }};

                INSERT Other{typename} {{
                    prop2 := 'bbb'
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED LINK prop1 -> Other{typename};
                }};

                INSERT {typename} {{
                    prop1 := (SELECT Other{typename} LIMIT 1)
                }};
            ''')

            other = await con1.query(f'SELECT Other{typename}')

            for _ in range(5):
                self.assertEqual(
                    [x.id for x in await con1.query(query)],
                    [x.id for x in other],
                )

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_06(self):
        typename = 'CacheInv_06'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE Foo{typename};

                CREATE TYPE Bar{typename};

                CREATE TYPE {typename} {{
                    CREATE REQUIRED LINK link1 -> Foo{typename};
                }};

                INSERT Foo{typename};
                INSERT Bar{typename};

                INSERT {typename} {{
                    link1 := (SELECT Foo{typename} LIMIT 1)
                }};
            ''')

            foo = await con1.query(f'SELECT Foo{typename}')
            bar = await con1.query(f'SELECT Bar{typename}')

            query = f'SELECT {typename}.link1'

            for _ in range(5):
                self.assertEqual(
                    [x.id for x in await con1.query(query)],
                    [x.id for x in foo],
                )

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP LINK link1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED LINK link1 -> Bar{typename};
                }};

                INSERT {typename} {{
                    link1 := (SELECT Bar{typename} LIMIT 1)
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    [x.id for x in await con1.query(query)],
                    [x.id for x in bar],
                )

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_07(self):
        typename = 'CacheInv_07'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE Foo{typename};

                CREATE ABSTRACT LINK link1 {{
                    CREATE PROPERTY prop1 -> std::str;
                }};

                CREATE TYPE {typename} {{
                    CREATE REQUIRED LINK link1 EXTENDING link1
                        -> Foo{typename};
                }};

                INSERT Foo{typename};

                INSERT {typename} {{
                    link1 := (
                        SELECT assert_single(Foo{typename}) {{@prop1 := 'aaa'}}
                    )
                }};
            ''')

            query = f'SELECT {typename}.link1@prop1'

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set(['aaa']))

            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER ABSTRACT LINK link1 {{
                    DROP PROPERTY prop1;
                }};

                ALTER ABSTRACT LINK link1 {{
                    CREATE PROPERTY prop1 -> std::int64;
                }};

                INSERT {typename} {{
                    link1 := (
                        (SELECT Foo{typename} LIMIT 1)
                        {{@prop1 := 123}}
                    )
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await con1.query(query),
                    edgedb.Set([123]))

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_09(self):
        typename = 'CacheInv_09'

        await self.con.query('START TRANSACTION')
        try:
            await self.con.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            query = f'SELECT {typename}.prop1'

            for _ in range(5):
                self.assertEqual(
                    await self.con.query(query),
                    edgedb.Set(['aaa']))

            await self.con.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};

                INSERT {typename} {{
                    prop1 := 123
                }};
            ''')

            for _ in range(5):
                self.assertEqual(
                    await self.con.query(query),
                    edgedb.Set([123]))

        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_query_cache_invalidate_10(self):
        typename = 'CacheInv_10'

        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute(f'''
                CREATE TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::str;
                }};

                INSERT {typename} {{
                    prop1 := 'aaa'
                }};
            ''')

            sleep = 3
            query = (
                f"# EDGEDB_TEST_COMPILER_SLEEP = {sleep}\n"
                f"SELECT {typename}.prop1"
            )
            task = self.loop.create_task(con1.query(query))

            start = time.monotonic()
            await con2.execute(f'''
                DELETE (SELECT {typename});

                ALTER TYPE {typename} {{
                    DROP PROPERTY prop1;
                }};

                ALTER TYPE {typename} {{
                    CREATE REQUIRED PROPERTY prop1 -> std::int64;
                }};

                INSERT {typename} {{
                    prop1 := 123
                }};
            ''')
            if time.monotonic() - start > sleep:
                self.skipTest("The host is too slow for this test.")
                # If this happens too much, consider increasing the sleep time

            # ISE is NOT the right expected result - a proper EdgeDB error is.
            # FIXME in https://github.com/edgedb/edgedb/issues/6820
            with self.assertRaisesRegex(
                edgedb.errors.InternalServerError,
                "column .* does not exist",
            ):
                await task

            self.assertEqual(
                await con1.query(query),
                edgedb.Set([123]),
            )

        finally:
            await con2.aclose()

    async def test_server_proto_query_cache_invalidate_11(self):
        typename = 'CacheInv_11'

        await self.con.execute(f"""
            CREATE TYPE {typename} {{
                CREATE PROPERTY prop1 -> std::int64;
            }};
            INSERT {typename} {{ prop1 := 42 }};
        """)

        class Rollback(Exception):
            pass

        # Cache SELECT 123 so that we don't lock the _query_cache table later
        await self.con.query('SELECT 123')

        with self.assertRaises(Rollback):
            async with self.con.transaction():
                # make sure the transaction is started
                await self.con.query('SELECT 123')

                # DDL in another connection
                con2 = await self.connect(database=self.con.dbname)
                try:
                    await con2.execute(f"""
                        ALTER TYPE {typename} {{
                            DROP PROPERTY prop1;
                        }};
                    """)
                finally:
                    await con2.aclose()

                # This compiles fine with the schema in the transaction, and
                # the compile result is cached with an outdated version.
                # However, the execution fails because the column is already
                # dropped outside the transaction.
                # FIXME: ISE is not an ideal error as for user experience here
                with self.assertRaisesRegex(
                    edgedb.InternalServerError, "column.*does not exist"
                ):
                    await self.con.query(f'SELECT {typename}.prop1')

                raise Rollback

        # Should recompile with latest schema, instead of reusing a wrong cache
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "has no link or property 'prop1'"
        ):
            await self.con.query(f'SELECT {typename}.prop1')

    async def test_server_proto_backend_tid_propagation_01(self):
        async with self._run_and_rollback():
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_01 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                select 1;
                SELECT (<array<tid_prop_01>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')

    async def test_server_proto_backend_tid_propagation_02(self):
        try:
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_02 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_02>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')
        finally:
            # Retry for https://github.com/edgedb/edgedb/issues/7553
            async for tr in self.try_until_succeeds(
                ignore_regexp="cannot drop type .* "
                              "because other objects depend on it",
            ):
                async with tr:
                    await self.con.execute('''
                        DROP SCALAR TYPE tid_prop_02;
                    ''')

    async def test_server_proto_backend_tid_propagation_03(self):
        try:
            await self.con.execute('''
                START MIGRATION TO {
                    module default {
                        scalar type tid_prop_03 extending str;
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_03>>$input)[1]
            ''', input=['A', 'B'])

            self.assertEqual(result, 'B')

        finally:
            # Retry for https://github.com/edgedb/edgedb/issues/7553
            async for tr in self.try_until_succeeds(
                ignore_regexp="cannot drop type .* "
                              "because other objects depend on it",
            ):
                async with tr:
                    await self.con.execute('''
                        DROP SCALAR TYPE tid_prop_03;
                    ''')

    async def test_server_proto_backend_tid_propagation_04(self):
        try:
            await self.con.query('START TRANSACTION;')
            await self.con.execute(f'''
                CREATE SCALAR TYPE tid_prop_04 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_04>>$input)[1]
            ''', input=['A', 'B'])

            self.assertEqual(result, 'B')

        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_backend_tid_propagation_05(self):
        try:
            await self.con.query('START TRANSACTION')
            await self.con.query('DECLARE SAVEPOINT s1')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_051 EXTENDING str;
            ''')
            await self.con.query('ROLLBACK TO SAVEPOINT s1')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_051 EXTENDING str;
                CREATE SCALAR TYPE tid_prop_052 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_052>>$input)[1]
            ''', input=['A', 'C'])

            self.assertEqual(result, 'C')

        finally:
            await self.con.query('ROLLBACK')

    async def test_server_proto_backend_tid_propagation_06(self):
        async with self._run_and_rollback():
            await self.con.query('''
                CREATE SCALAR TYPE tid_prop_06 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_06>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')

    async def test_server_proto_backend_tid_propagation_07(self):
        try:
            await self.con.query('''
                CREATE SCALAR TYPE tid_prop_07 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_07>>$input)[1]
            ''', input=['a', 'b'])

            self.assertEqual(result, 'b')
        finally:
            # Retry for https://github.com/edgedb/edgedb/issues/7553
            async for tr in self.try_until_succeeds(
                ignore_regexp="cannot drop type .* "
                              "because other objects depend on it",
            ):
                async with tr:
                    await self.con.execute('''
                        DROP SCALAR TYPE tid_prop_07;
                    ''')

    async def test_server_proto_backend_tid_propagation_08(self):
        try:
            await self.con.query('START TRANSACTION')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_081 EXTENDING str;
            ''')
            await self.con.query('COMMIT')
            await self.con.query('START TRANSACTION')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_082 EXTENDING str;
            ''')

            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_083 EXTENDING str;
            ''')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_081>>$input)[0]
            ''', input=['A', 'C'])
            self.assertEqual(result, 'A')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_082>>$input)[1]
            ''', input=['A', 'C'])
            self.assertEqual(result, 'C')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_083>>$input)[1]
            ''', input=['A', 'Z'])
            self.assertEqual(result, 'Z')

        finally:
            await self.con.query('ROLLBACK')
            # Retry for https://github.com/edgedb/edgedb/issues/7553
            async for tr in self.try_until_succeeds(
                ignore_regexp="cannot drop type .* "
                              "because other objects depend on it",
            ):
                async with tr:
                    await self.con.execute('''
                        DROP SCALAR TYPE tid_prop_081;
                    ''')

    async def test_server_proto_backend_tid_propagation_09(self):
        try:
            await self.con.query('START TRANSACTION')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_091 EXTENDING str;
            ''')
            await self.con.query('COMMIT')
            await self.con.query('START TRANSACTION')
            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_092 EXTENDING str;
            ''')

            await self.con.execute('''
                CREATE SCALAR TYPE tid_prop_093 EXTENDING str;
            ''')

            await self.con.query('COMMIT')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_091>>$input)[0]
            ''', input=['A', 'C'])
            self.assertEqual(result, 'A')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_092>>$input)[1]
            ''', input=['A', 'C'])
            self.assertEqual(result, 'C')

            result = await self.con.query_single('''
                SELECT (<array<tid_prop_093>>$input)[1]
            ''', input=['A', 'Z'])
            self.assertEqual(result, 'Z')

        finally:
            # Retry for https://github.com/edgedb/edgedb/issues/7553
            async for tr in self.try_until_succeeds(
                ignore_regexp="cannot drop type .* "
                              "because other objects depend on it",
            ):
                async with tr:
                    await self.con.execute('''
                        DROP SCALAR TYPE tid_prop_091;
                        DROP SCALAR TYPE tid_prop_092;
                        DROP SCALAR TYPE tid_prop_093;
                    ''')

    async def test_server_proto_fetch_limit_01(self):
        try:
            await self.con.execute('''
                CREATE TYPE FL_A {
                    CREATE PROPERTY n -> int64;
                };
                CREATE TYPE FL_B {
                    CREATE PROPERTY n -> int64;
                    CREATE MULTI LINK a -> FL_A;
                };

                FOR i IN {1, 2, 3, 4, 5}
                UNION (
                    INSERT FL_A {
                        n := i
                    }
                );

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
                    SELECT FL_B {
                        id,
                        __type__,
                        a,
                    } ORDER BY .n
                """,
                __limit__=2
            )

            self.assertEqual(len(result), 2)
            self.assertEqual(len(result[0].a), 2)

            result = await self.con._fetchall(
                r"""
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
                    SELECT FL_B {
                        a ORDER BY .n LIMIT 3,
                        a_arr := array_agg((SELECT .a LIMIT 3)),
                        a_count := count(.a),
                        a_comp := (SELECT .a LIMIT 3),
                    }
                    ORDER BY .n
                    LIMIT 3
                """,
                __limit__=4
            )

            self.assertEqual(len(result), 3)
            self.assertEqual(len(result[0].a), 3)
            self.assertEqual(len(result[0].a_arr), 3)
            self.assertEqual(len(result[0].a_comp), 3)
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

            # Check that things cast to JSON don't get limited.
            result = await self.con._fetchall(
                r"""
                    WITH a := {11, 12, 13}
                    SELECT <json>array_agg(a);
                """,
                __limit__=1
            )

            self.assertEqual(result, edgedb.Set(['[11, 12, 13]']))

            # Check that non-array_agg calls don't get limited.
            result = await self.con._fetchall(
                r"""
                    WITH a := {11, 12, 13}
                    SELECT max(a);
                """,
                __limit__=1
            )

            self.assertEqual(result, edgedb.Set([13]))

        finally:
            await self.con.execute('''
                DROP TYPE FL_B;
                DROP TYPE FL_A;
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

    async def test_query_single_script(self):
        # query single should work even if earlier parts of the script
        # are multisets
        result = await self.con.query_single('''
            select {1, 2};
            select 1;
        ''')
        self.assertEqual(result, 1)


class TestServerProtoConcurrentDDL(tb.DDLTestCase):

    TRANSACTION_ISOLATION = False

    async def test_server_proto_concurrent_ddl(self):
        typename_prefix = 'ConcurrentDDL'
        ntasks = 5

        async with asyncio.TaskGroup() as g:
            cons_tasks = [
                g.create_task(self.connect(database=self.con.dbname))
                for _ in range(ntasks)
            ]

        cons = [c.result() for c in cons_tasks]

        try:
            async with asyncio.TaskGroup() as g:
                for i, con in enumerate(cons):
                    # deferred_shield ensures that none of the
                    # operations get cancelled, which allows us to
                    # aclose them all cleanly.
                    # Use _fetchall, because it doesn't retry
                    g.create_task(asyncutil.deferred_shield(con._fetchall(f'''
                        CREATE TYPE {typename_prefix}{i} {{
                            CREATE REQUIRED PROPERTY prop1 -> std::int64;
                        }};

                        INSERT {typename_prefix}{i} {{
                            prop1 := {i}
                        }};
                    ''')))
        except ExceptionGroup as e:
            self.assertIn(
                edgedb.TransactionSerializationError,
                [type(e) for e in e.exceptions],
            )
        else:
            self.fail("TransactionSerializationError not raised")
        finally:
            async with asyncio.TaskGroup() as g:
                for con in cons:
                    g.create_task(con.aclose())


class TestServerProtoConcurrentGlobalDDL(tb.DDLTestCase):

    TRANSACTION_ISOLATION = False

    async def test_server_proto_concurrent_global_ddl(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        ntasks = 5

        async with asyncio.TaskGroup() as g:
            cons_tasks = [
                g.create_task(self.connect(database=self.con.dbname))
                for _ in range(ntasks)
            ]

        cons = [c.result() for c in cons_tasks]

        try:
            async with asyncio.TaskGroup() as g:
                for i, con in enumerate(cons):
                    # deferred_shield ensures that none of the
                    # operations get cancelled, which allows us to
                    # aclose them all cleanly.
                    # Use _fetchall, because it doesn't retry
                    g.create_task(asyncutil.deferred_shield(con._fetchall(f'''
                        CREATE SUPERUSER ROLE concurrent_{i}
                    ''')))
        except ExceptionGroup as e:
            self.assertIn(
                edgedb.TransactionSerializationError,
                [type(e) for e in e.exceptions],
            )
        else:
            self.fail("TransactionSerializationError not raised")
        finally:
            async with asyncio.TaskGroup() as g:
                for con in cons:
                    g.create_task(con.aclose())


class TestServerCapabilities(tb.QueryTestCase):

    TRANSACTION_ISOLATION = False

    SETUP = '''
        CREATE TYPE Modify {
            CREATE REQUIRED PROPERTY prop1 -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE Modify;
    '''

    async def test_server_capabilities_01(self):
        await self.con._fetchall(
            'SELECT {1, 2, 3}',
        )
        self.assertEqual(
            self.con._get_last_capabilities(),
            enums.Capability(0),
        )

        # selects are always allowed
        await self.con._fetchall(
            'SELECT {1, 2, 3}',
            __allow_capabilities__=0,
        )
        self.assertEqual(
            self.con._get_last_capabilities(),
            enums.Capability(0),
        )

        # as well as describes
        await self.con._fetchall(
            'DESCRIBE OBJECT cfg::Config',
            __allow_capabilities__=0,
        )
        self.assertEqual(
            self.con._get_last_capabilities(),
            enums.Capability(0),
        )

    async def test_server_capabilities_02(self):
        await self.con._fetchall(
            'INSERT Modify { prop1 := "xx" }',
        )
        self.assertEqual(
            self.con._get_last_capabilities(),
            enums.Capability.MODIFICATIONS,
        )
        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'INSERT Modify { prop1 := "xx" }',
                __allow_capabilities__=0,
            )
        await self.con._fetchall(
            'INSERT Modify { prop1 := "xx" }',
            __allow_capabilities__=enums.Capability.MODIFICATIONS,
        )

    async def test_server_capabilities_03(self):
        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'CREATE TYPE Type1',
                __allow_capabilities__=0,
            )
        try:
            await self.con._fetchall(
                'CREATE TYPE Type1',
                __allow_capabilities__=enums.Capability.DDL,
            )
            self.assertEqual(
                self.con._get_last_capabilities(),
                enums.Capability.DDL,
            )
        finally:
            await self.con._fetchall(
                'DROP TYPE Type1',
            )
            self.assertEqual(
                self.con._get_last_capabilities(),
                enums.Capability.DDL,
            )

    async def test_server_capabilities_04(self):
        caps = enums.Capability.ALL & ~enums.Capability.SESSION_CONFIG
        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'CONFIGURE SESSION SET singleprop := "42"',
                __allow_capabilities__=caps,
            )

    async def test_server_capabilities_05(self):
        caps = enums.Capability.ALL & ~enums.Capability.PERSISTENT_CONFIG
        with self.assertRaises(edgedb.ProtocolError):
            await self.con._fetchall(
                'CONFIGURE INSTANCE SET singleprop := "42"',
                __allow_capabilities__=caps,
            )

    async def test_server_capabilities_06(self):
        caps = enums.Capability.ALL & ~enums.Capability.TRANSACTION
        with self.assertRaisesRegex(
            edgedb.DisabledCapabilityError, "disabled by the client"
        ):
            await self.con._fetchall(
                'START MIGRATION TO {}',
                __allow_capabilities__=caps,
            )

    async def test_server_capabilities_07(self):
        caps = enums.Capability.ALL & ~enums.Capability.TRANSACTION
        await self.con._fetchall(
            'START MIGRATION TO {};'
            'POPULATE MIGRATION;'
            'ABORT MIGRATION;',
            __allow_capabilities__=caps,
        )
