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
import edgedb

from edb.server import args as srv_args
from edb.server import compiler
from edb import protocol
from edb.testbase import server as tb
from edb.testbase import connection as tconn
from edb.testbase.protocol.test import ProtocolTestCase


class TestProtocol(ProtocolTestCase):

    async def _execute(self, command_text, sync=True, data=False, cc=None):
        exec_args = dict(
            annotations=[],
            allowed_capabilities=protocol.Capability.ALL,
            compilation_flags=protocol.CompilationFlag(0),
            implicit_limit=0,
            command_text=command_text,
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

        args = (protocol.Execute(**exec_args),)
        if sync:
            args += (protocol.Sync(),)
        await self.con.send(*args)

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

    async def test_proto_flush_01(self):

        await self.con.connect()

        await self.con.send(
            protocol.Parse(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
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
            result_cardinality=compiler.Cardinality.AT_MOST_ONE,
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
