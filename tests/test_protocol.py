#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
import contextlib
import struct

import edgedb

from edb.server import args as srv_args
from edb.server import compiler
from edb import protocol
from edb.protocol.protocol import Connection
from edb.testbase import server as tb
from edb.testbase import connection as tconn
from edb.testbase.protocol.test import ProtocolTestCase


def pack_i32s(*args):
    return struct.pack("!" + "i" * len(args), *args)


class TestProtocol(ProtocolTestCase):

    async def _execute(
        self,
        command_text: str,
        sync: bool = True,
        data: bool = False,
        sql: bool = False,
        cc: protocol.CommandComplete | None = None,
        con: Connection | None = None,
        input_language: protocol.InputLanguage = protocol.InputLanguage.EDGEQL,
    ) -> None:
        exec_args = dict(
            annotations=[],
            allowed_capabilities=protocol.Capability.ALL,
            compilation_flags=protocol.CompilationFlag(0),
            implicit_limit=0,
            command_text=command_text,
            input_language=(
                protocol.InputLanguage.SQL
                if sql else protocol.InputLanguage.EDGEQL
            ),
            output_format=protocol.OutputFormat.NONE,
            expected_cardinality=protocol.Cardinality.MANY,
            input_typedesc_id=b'\0' * 16,
            output_typedesc_id=b'\0' * 16,
            state_typedesc_id=b'\0' * 16,
            arguments=b'',
            state_data=b'',
        )
        if cc is not None:
            exec_args['state_typedesc_id'] = cc.state_typedesc_id
            exec_args['state_data'] = cc.state_data
        if data:
            exec_args['output_format'] = protocol.OutputFormat.BINARY

        args: tuple[protocol.ClientMessage, ...] = (
            protocol.Execute(**exec_args),
        )
        if sync:
            args += (protocol.Sync(),)
        if con is None:
            con = self.con
        await con.send(*args)

    async def test_proto_execute_01(self):
        # Test that Execute returns ErrorResponse immediately.

        await self.con.connect()

        await self._execute('SELECT 1/0; SELECT 42')
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='division by zero'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

        # Test that the protocol has recovered.
        await self._execute('SELECT 1')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_execute_02(self):
        # Test ReadyForCommand.transaction_state

        await self.con.connect()

        await self._execute('START TRANSACTION')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='START TRANSACTION'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_TRANSACTION,
        )

        await self._execute('SELECT 1/0')
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='division by zero'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_FAILED_TRANSACTION,
        )

        # Test that the protocol is still in a failed transaction
        await self._execute('SELECT 1/0')
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='current transaction is aborted'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_FAILED_TRANSACTION,
        )

        # Test recovery
        await self._execute('ROLLBACK')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='ROLLBACK'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_execute_03(self):
        # Test that OutputFormat.NONE returns no data

        await self.con.connect()

        await self._execute('SELECT 1', data=True)

        await self.con.recv_match(
            protocol.CommandDataDescription,
            result_cardinality=compiler.Cardinality.ONE,
        )
        await self.con.recv_match(
            protocol.Data,
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

        await self._execute('SELECT 1')

        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_execute_04(self):
        # Same as test_proto_execute_03 but for SQL

        await self.con.connect()

        await self._execute('SELECT 1', data=True, sql=True)

        await self.con.recv_match(
            protocol.CommandDataDescription,
            result_cardinality=compiler.Cardinality.MANY,
        )
        await self.con.recv_match(
            protocol.Data,
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

        await self._execute('SELECT 1', sql=True)

        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_flush_01(self):

        await self.con.connect()

        await self.con.send(
            protocol.Parse(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=protocol.OutputFormat.BINARY,
                expected_cardinality=compiler.Cardinality.AT_MOST_ONE,
                command_text='SEL ECT 1',
                state_typedesc_id=b'\0' * 16,
                state_data=b'',
            )
        )
        # Should come through even without an explicit 'flush'
        await self.con.recv_match(
            protocol.ErrorResponse,
            message="Unexpected 'SEL'"
        )

        # Recover the protocol state from the error
        self.assertEqual(
            await self.con.sync(),
            protocol.TransactionState.NOT_IN_TRANSACTION)

        await self.con.send(
            protocol.Parse(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=protocol.OutputFormat.BINARY,
                expected_cardinality=compiler.Cardinality.AT_MOST_ONE,
                command_text='SELECT 1',
                state_typedesc_id=b'\0' * 16,
                state_data=b'',
            ),
            protocol.Flush()
        )
        await self.con.recv_match(
            protocol.CommandDataDescription,
            result_cardinality=compiler.Cardinality.ONE,
        )

        # Test that Flush has completed successfully -- the
        # command should be executed and no exception should
        # be received.
        # While at it, rogue ROLLBACK should be allowed.
        await self._execute('ROLLBACK')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='ROLLBACK'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_args_mismatch(self):
        await self.con.connect()

        await self._execute('SELECT <str>$0', sync=False)
        # Should come through even without an explicit 'flush'
        await self.con.recv_match(protocol.CommandDataDescription)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message=(
                r"specified parameter type\(s\) do not match the parameter "
                r"types inferred from specified command\(s\)"
            )
        )

        # Recover the protocol state from the error
        self.assertEqual(
            await self.con.sync(),
            protocol.TransactionState.NOT_IN_TRANSACTION)

    async def test_proto_state(self):
        await self.con.connect()
        try:
            await self._test_proto_state()
        finally:
            await self.con.execute('DROP GLOBAL state_desc_1')
            await self.con.execute('DROP GLOBAL state_desc_2')

    async def _test_proto_state(self):
        # Create initial state schema
        await self._execute('CREATE GLOBAL state_desc_1 -> int32')
        sdd1 = await self.con.recv_match(protocol.StateDataDescription)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertNotEqual(sdd1.typedesc_id, b'\0' * 16)

        # Check setting the state
        await self._execute('SET GLOBAL state_desc_1 := 11')
        cc1 = await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd1.typedesc_id,
            state_data=b'\0\0\0\x01\0\0\0\x03\0\0\0\x10\0\0\0\x01'
                       b'\0\0\0\0\0\0\0\x04\0\0\0\x0b'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # Verify the state is set
        await self._execute('SELECT GLOBAL state_desc_1', data=True, cc=cc1)
        await self.con.recv_match(protocol.CommandDataDescription)
        d1 = await self.con.recv_match(protocol.Data)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertEqual(d1.data[0].data[-1], 11)

        # Entering a transaction with cc1 - which will be stored by the server
        await self._execute('START TRANSACTION', cc=cc1)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # Create 2nd global while the 1st is set
        await self._execute('CREATE GLOBAL state_desc_2 -> int32', cc=cc1)
        sdd2 = await self.con.recv_match(protocol.StateDataDescription)
        cc2_1 = await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd2.typedesc_id,
            state_data=cc1.state_data,
        )
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertNotEqual(sdd2.typedesc_id, b'\0' * 16)

        # Verify we could also set the 2nd global
        await self._execute('SET GLOBAL state_desc_2 := 22', cc=cc2_1)
        cc2_2 = await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd2.typedesc_id,
            state_data=b'\0\0\0\x01\0\0\0\x03\0\0\0\x1c\0\0\0\x02'
                       b'\0\0\0\x00\0\0\0\x04\0\0\0\x0b'
                       b'\0\0\0\x01\0\0\0\x04\0\0\0\x16'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # The 1st global's value is still available
        await self._execute('SELECT GLOBAL state_desc_1', data=True, cc=cc2_2)
        await self.con.recv_match(protocol.CommandDataDescription)
        d1_2 = await self.con.recv_match(protocol.Data)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertEqual(d1_2.data[0].data[-1], 11)

        # Query with an outdated state cc1, expect SDD2 + StateMismatchError
        await self._execute('SELECT GLOBAL state_desc_2', data=True, cc=cc1)
        await self.con.recv_match(
            protocol.StateDataDescription,
            typedesc_id=sdd2.typedesc_id,
        )
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='Cannot decode state: type mismatch',
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # Rollback the transaction, should reset to SDD1 and CC1
        await self._execute('ROLLBACK', cc=cc2_2)
        await self.con.recv_match(
            protocol.StateDataDescription,
            typedesc_id=sdd1.typedesc_id,
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd1.typedesc_id,
            state_data=cc1.state_data,
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # New transaction
        await self._execute('START TRANSACTION', cc=cc1)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # Create the same 2nd global again
        await self._execute('CREATE GLOBAL state_desc_2 -> int32', cc=cc1)
        await self.con.recv_match(
            protocol.StateDataDescription,
            typedesc_id=sdd2.typedesc_id,
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd2.typedesc_id,
            state_data=cc1.state_data,
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # Commit the transaction with CC2_2, nothing would change
        await self._execute('COMMIT', cc=cc2_2)
        await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=b'\0' * 16,
            state_data=b'',
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # The 2nd global's value is still available
        await self._execute('SELECT GLOBAL state_desc_2', data=True, cc=cc2_2)
        await self.con.recv_match(protocol.CommandDataDescription)
        d2 = await self.con.recv_match(protocol.Data)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertEqual(d2.data[0].data[-1], 22)

    async def test_proto_state_desc_in_script(self):
        await self.con.connect()

        await self._execute(
            'CREATE GLOBAL state_desc_in_script -> int32;'
            'SELECT GLOBAL state_desc_in_script;'
        )
        await self.con.recv_match(protocol.StateDataDescription)
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )
        await self.con.execute('DROP GLOBAL state_desc_in_script')

    async def test_proto_desc_id_cardinality(self):
        await self.con.connect()

        await self._execute(
            'CREATE TYPE CardTest { CREATE PROPERTY prop -> int32; }'
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            status='CREATE TYPE'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        try:
            await self._test_proto_desc_id_cardinality()
        finally:
            await self.con.execute('DROP TYPE CardTest')

    async def _test_proto_desc_id_cardinality(self):

        await self._execute('SELECT CardTest { prop }', data=True)
        cdd1 = await self.con.recv_match(protocol.CommandDataDescription)
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        await self._execute('''
            ALTER TYPE CardTest {
                ALTER PROPERTY prop {
                    SET DEFAULT := 42;
                    SET REQUIRED;
                };
            }
        ''')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='ALTER TYPE'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        await self._execute('SELECT CardTest { prop }', data=True)
        cdd2 = await self.con.recv_match(protocol.CommandDataDescription)
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        self.assertNotEqual(cdd1.output_typedesc_id, cdd2.output_typedesc_id)

    async def _parse(self, query, output_format=protocol.OutputFormat.BINARY):
        await self.con.send(
            protocol.Parse(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=output_format,
                expected_cardinality=compiler.Cardinality.MANY,
                command_text=query,
                state_typedesc_id=b'\0' * 16,
                state_data=b'',
            ),
            protocol.Flush()
        )

    async def test_proto_parse_cardinality(self):
        await self.con.connect()

        await self._parse("SELECT 42")
        await self.con.recv_match(
            protocol.CommandDataDescription,
            result_cardinality=compiler.Cardinality.ONE,
        )

        await self._parse("SELECT {1,2,3}")
        await self.con.recv_match(
            protocol.CommandDataDescription,
            result_cardinality=compiler.Cardinality.AT_LEAST_ONE,
        )

        await self._execute('CREATE TYPE ParseCardTest')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='CREATE TYPE'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        try:
            await self._parse("SELECT ParseCardTest")
            await self.con.recv_match(
                protocol.CommandDataDescription,
                result_cardinality=compiler.Cardinality.MANY,
            )

            await self._parse("SELECT ParseCardTest LIMIT 1")
            await self.con.recv_match(
                protocol.CommandDataDescription,
                result_cardinality=compiler.Cardinality.AT_MOST_ONE,
            )
        finally:
            await self.con.execute("DROP TYPE ParseCardTest")

    async def test_proto_state_concurrent_alter(self):
        con2 = await protocol.protocol.new_connection(
            **self.get_connect_args(database=self.get_database_name())
        )
        try:
            await self.con.connect()
            await con2.connect()

            # Create initial state schema
            await self._execute('CREATE GLOBAL state_desc_3 -> int32')
            sdd1 = await self.con.recv_match(protocol.StateDataDescription)
            await self.con.recv_match(protocol.CommandComplete)
            await self.con.recv_match(protocol.ReadyForCommand)
            self.assertNotEqual(sdd1.typedesc_id, b'\0' * 16)

            # Check setting the state
            await self._execute('SET GLOBAL state_desc_3 := 11')
            cc1 = await self.con.recv_match(
                protocol.CommandComplete,
                state_typedesc_id=sdd1.typedesc_id,
            )
            await self.con.recv_match(protocol.ReadyForCommand)

            # Verify the state is set
            await self._execute(
                'SELECT GLOBAL state_desc_3', data=True, cc=cc1)
            await self.con.recv_match(protocol.CommandDataDescription)
            d1 = await self.con.recv_match(protocol.Data)
            await self.con.recv_match(protocol.CommandComplete)
            await self.con.recv_match(protocol.ReadyForCommand)
            self.assertEqual(d1.data[0].data[-1], 11)

            # Alter the global type in another connection
            await self._execute(
                'ALTER GLOBAL state_desc_3 SET TYPE str RESET TO DEFAULT',
                con=con2,
            )
            sdd2 = await con2.recv_match(protocol.StateDataDescription)
            await con2.recv_match(protocol.CommandComplete)
            await con2.recv_match(protocol.ReadyForCommand)
            self.assertNotEqual(sdd1.typedesc_id, sdd2.typedesc_id)

            # The same query in the first connection should now fail
            await self._execute(
                'SELECT GLOBAL state_desc_3', data=True, cc=cc1)
            sdd3 = await self.con.recv_match(protocol.StateDataDescription)
            await self.con.recv_match(
                protocol.ErrorResponse,
                message='Cannot decode state: type mismatch',
            )
            await self.con.recv_match(protocol.ReadyForCommand)
            self.assertEqual(sdd2.typedesc_id, sdd3.typedesc_id)

        finally:
            await con2.aclose()
            await self.con.execute("DROP GLOBAL state_desc_3")

    async def _parse_execute(self, query, args):
        output_format = protocol.OutputFormat.BINARY
        await self._parse(query, output_format=output_format)
        res = await self.con.recv()

        await self.con.send(
            protocol.Execute(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                command_text=query,
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=output_format,
                expected_cardinality=protocol.Cardinality.MANY,
                input_typedesc_id=res.input_typedesc_id,
                output_typedesc_id=res.output_typedesc_id,
                state_typedesc_id=b'\0' * 16,
                arguments=args,
                state_data=b'',
            ),
            protocol.Sync(),
        )

    async def test_proto_execute_bad_array_01(self):
        q = "SELECT <array<int32>>$0"

        array = pack_i32s(
            1,  # dims
            0,  # flags
            0,  # reserved
            3,  # num elems
            1,  # bound,

            4,  # el 1 length
            1337,  # el 1
            -1,  # NULL!
            4,  # el 2 length
            10000,
        )

        args = pack_i32s(
            1,   # num args
            0,   # reserved
            len(array),   # len
        ) + array

        await self.con.connect()
        await self._parse_execute(q, args)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='invalid NULL'
        )

    async def test_proto_execute_bad_array_02(self):
        q = "SELECT <array<int32>>$0"

        array = pack_i32s(
            1,  # dims
            0,  # flags
            0,  # reserved
            2,  # num elems
            4,  # bound,

            4,  # el 1 length
            1337,  # el 1
            4,  # el 2 length
            10000,
        )

        args = pack_i32s(
            1,   # num args
            0,   # reserved
            len(array),   # len
        ) + array

        await self.con.connect()
        await self._parse_execute(q, args)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='unsupported array bound'
        )

    async def test_proto_execute_bad_array_03(self):
        q = "SELECT <array<int32>>$0"

        array = pack_i32s(
            2,  # dims
            0,  # flags
            0,  # reserved
            2,  # num elems
            1,  # bound,

            4,  # el 1 length
            1337,  # el 1
            4,  # el 2 length
            10000,
        )

        args = pack_i32s(
            1,   # num args
            0,   # reserved
            len(array),   # len
        ) + array

        await self.con.connect()
        await self._parse_execute(q, args)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='unsupported array dimensions'
        )

    async def test_proto_global_bad_array(self):
        await self.con.connect()

        # Use a transaction to avoid interfering with tests that care about
        # the details of the state
        await self._execute('START TRANSACTION')
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # Create a global
        await self._execute('CREATE GLOBAL array_glob -> array<str>')
        sdd1 = await self.con.recv_match(protocol.StateDataDescription)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        self.assertNotEqual(sdd1.typedesc_id, b'\0' * 16)

        # Set an array in the state
        await self._execute('SET GLOBAL array_glob := ["AAAA", "", "CCCC"]')
        cc1 = await self.con.recv_match(
            protocol.CommandComplete,
            state_typedesc_id=sdd1.typedesc_id,
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        # Blow away the empty second string from the encoding and
        # replace it with NULL
        cc1.state_data = cc1.state_data.replace(
            b'AAAA' + b'\x00' * 4, b'AAAA' + b'\xff' * 4
        )

        # Verify the state is set
        await self._execute('SELECT 1', data=True, cc=cc1)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='invalid NULL'
        )

    async def test_proto_parse_execute_transaction_id(self):
        await self.con.connect()
        await self._parse_execute("start transaction", b"")
        await self.con.recv_match(
            protocol.CommandComplete,
            status='START TRANSACTION'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_TRANSACTION,
        )
        await self._parse_execute("commit", b"")
        await self.con.recv_match(
            protocol.CommandComplete,
            status='COMMIT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_state_change_in_tx(self):
        await self.con.connect()

        # Fixture
        await self._execute('CREATE MODULE TestStateChangeInTx')
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        await self._execute('CREATE TYPE TestStateChangeInTx::StateChangeInTx')
        await self.con.recv_match(
            protocol.CommandComplete,
            status='CREATE TYPE'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

        try:
            await self._test_proto_state_change_in_tx()
        finally:
            await self.con.execute('ROLLBACK')
            await self.con.execute(
                'DROP TYPE TestStateChangeInTx::StateChangeInTx')
            await self.con.execute('DROP MODULE TestStateChangeInTx')

    async def _test_proto_state_change_in_tx(self):
        # Collect states
        await self._execute('''
            SET MODULE TestStateChangeInTx
        ''')
        cc = await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        await self._execute('''
            CONFIGURE SESSION SET allow_user_specified_id := true
        ''', cc=cc)
        cc_true = await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)
        await self._execute('''
            CONFIGURE SESSION SET allow_user_specified_id := false
        ''')
        cc_false = await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # Start a transaction that doesn't allow_user_specified_id
        await self._execute('START TRANSACTION', cc=cc_false)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # But insert with session that does allow_user_specified_id
        await self._execute('''
            INSERT StateChangeInTx {
                id := <uuid>'a768e9d5-d908-4072-b370-865b450216ff'
            };
        ''', cc=cc_true)
        await self.con.recv_match(
            protocol.CommandComplete,
            status='INSERT'
        )
        await self.con.recv_match(protocol.ReadyForCommand)

    async def test_proto_discard_prepared_statement_in_script(self):
        await self.con.connect()

        try:
            await self._test_proto_discard_prepared_statement_in_script()
        finally:
            await self.con.execute("drop type DiscardStmtInScript")

    async def _test_proto_discard_prepared_statement_in_script(self):
        # Context: we don't want to jump around cache function calls
        await self._execute(
            "configure session set query_cache_mode"
            " := <cfg::QueryCacheMode>'InMemory'"
        )
        state = await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # First, run a query that is known to use a prepared statement
        await self._execute("select 42", cc=state)
        await self.con.recv_match(protocol.CommandComplete)
        await self.con.recv_match(protocol.ReadyForCommand)

        # Then, bump dbver by modifying the schema to invalidate the statement
        await self.con.execute("create type DiscardStmtInScript")

        # Now here comes the key. We execute a script that is meant to fail at
        # the first command. The second command, `select 42` again, is never
        # executed living in an aborted transaction, but we didn't know that
        # before executing the first command; we sent messages of both commands
        # altogether. Because dbver is bumped, we need to rebuild the prepared
        # statement for `select 42`, involving a `CLOSE` message followed by a
        # `PARSE` message. The problem was, `CLOSE` was placed *after* the
        # `EXECUTE` message of the first command so `CLOSE` was simply skipped
        # because the first command failed; but we still removed the prepared
        # statement from the in-memory registry of PGConnection, leading to an
        # inconsistency of memory state.
        await self._execute('select 1/0; select 42', cc=state)
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='division by zero'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

        # With such inconsistency, the next `select 42` was failing trying to
        # create its prepared statement, because the memory registry showed we
        # didn't have a prepared statement for `select 42`, but it was actually
        # not closed in the PG session due the issue mentioned above.
        await self._execute("select 42", cc=state)
        try:
            await self.con.recv_match(protocol.CommandComplete)
        finally:
            await self.con.recv_match(protocol.ReadyForCommand)


class TestServerCancellation(tb.TestCase):
    @contextlib.asynccontextmanager
    async def _fixture(self):
        # Prepare the test data
        con1 = con2 = None
        async with tb.start_edgedb_server(max_allowed_connections=4) as sd:
            conn_args = sd.get_connect_args()
            try:
                con1 = await sd.connect_test_protocol()
                con2 = await sd.connect()
                await con2.execute(
                    'CREATE TYPE tclcq { CREATE PROPERTY p -> str }'
                )
                await con2.execute("INSERT tclcq { p := 'initial' }")
                yield con1, con2, conn_args
            finally:
                for con in [con1, con2]:
                    if con is not None:
                        await con.aclose()

    async def test_proto_connection_lost_cancel_query(self):
        async with self._fixture() as (con1, con2, conn_args):
            # Use an implicit transaction in the nested connection: lock
            # the row with an UPDATE, and then hold the transaction for 10
            # seconds, which is long enough for the upcoming cancellation
            await con1.send(
                protocol.Execute(
                    annotations=[],
                    allowed_capabilities=protocol.Capability.ALL,
                    compilation_flags=protocol.CompilationFlag(0),
                    implicit_limit=0,
                    command_text="""\
                    UPDATE tclcq SET { p := 'inner' };
                    SELECT sys::_sleep(10);
                    """,
                    input_language=protocol.InputLanguage.EDGEQL,
                    output_format=protocol.OutputFormat.NONE,
                    expected_cardinality=protocol.Cardinality.MANY,
                    input_typedesc_id=b'\0' * 16,
                    output_typedesc_id=b'\0' * 16,
                    state_typedesc_id=b'\0' * 16,
                    arguments=b'',
                    state_data=b'',
                ),
                protocol.Sync(),
            )

            # Take up all free backend connections
            other_conns = []
            for _ in range(2):
                con = await tconn.async_connect_test_client(**conn_args)
                other_conns.append(con)
                self.loop.create_task(
                    con.execute("SELECT sys::_sleep(60)")
                ).add_done_callback(lambda f: f.exception())
            await asyncio.sleep(0.1)

            try:
                # Close the nested connection without waiting for the result;
                # the server is supposed to cancel the pending query.
                self.loop.call_later(0.5, self.loop.create_task, con1.aclose())

                # In the outer connection, let's wait until the lock is
                # released by either an expected cancellation, or an unexpected
                # commit after 10 seconds.
                tx = con2.transaction()
                await asyncio.wait_for(tx.start(), 2)
                try:
                    await con2.execute("UPDATE tclcq SET { p := 'lock' }")
                except edgedb.TransactionSerializationError:
                    # In case the nested transaction succeeded, we'll meet an
                    # concurrent update error here, which can be safely ignored
                    pass
                finally:
                    await tx.rollback()

                # Let's check what's in the row - if the cancellation didn't
                # happen, the test will fail with value "inner".
                val = await con2.query_single('SELECT tclcq.p LIMIT 1')
                self.assertEqual(val, 'initial')
            finally:
                for con in other_conns:
                    con.terminate()
                for con in other_conns:
                    await con.aclose()

    async def test_proto_gh3170_connection_lost_error(self):
        async with tb.start_edgedb_server(
            security=srv_args.ServerSecurityMode.InsecureDevMode,
        ) as sd:
            self.assertNotIn(
                'edgedb_server_background_errors_total'
                '{source="release_pgcon"}',
                sd.fetch_metrics(),
            )
            con = await sd.connect_test_protocol()
            try:
                await con.send(
                    protocol.Execute(
                        annotations=[],
                        allowed_capabilities=protocol.Capability.ALL,
                        compilation_flags=protocol.CompilationFlag(0),
                        implicit_limit=0,
                        command_text='START TRANSACTION',
                        input_language=protocol.InputLanguage.EDGEQL,
                        output_format=protocol.OutputFormat.NONE,
                        expected_cardinality=protocol.Cardinality.MANY,
                        input_typedesc_id=b'\0' * 16,
                        output_typedesc_id=b'\0' * 16,
                        state_typedesc_id=b'\0' * 16,
                        arguments=b'',
                        state_data=b'',
                    ),
                    protocol.Sync(),
                )
                await con.recv_match(
                    protocol.CommandComplete,
                    status='START TRANSACTION'
                )
                await con.recv_match(
                    protocol.ReadyForCommand,
                    transaction_state=protocol.TransactionState.IN_TRANSACTION,
                )
                await con.aclose()
                self.assertNotIn(
                    'edgedb_server_background_errors_total'
                    '{source="release_pgcon"}',
                    sd.fetch_metrics(),
                )
            except Exception:
                await con.aclose()
                raise
