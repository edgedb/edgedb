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
import edgedb

from edb.testbase import protocol
from edb.testbase.protocol.test import ProtocolTestCase


class TestProtocol(ProtocolTestCase):

    async def test_proto_executescript_01(self):
        # Test that ExecuteScript returns ErrorResponse immediately.

        await self.con.connect()

        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='SELECT 1/0'
            )
        )
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='division by zero'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

        # Test that the protocol has recovered.

        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='SELECT 1'
            )
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_executescript_02(self):
        # Test ReadyForCommand.transaction_state

        await self.con.connect()

        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='START TRANSACTION; SELECT 1/0'
            )
        )
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='division by zero'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_FAILED_TRANSACTION,
        )

        # Test that the protocol is still in a failed transaction

        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='SELECT 1/0'
            )
        )
        await self.con.recv_match(
            protocol.ErrorResponse,
            message='current transaction is aborted'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.IN_FAILED_TRANSACTION,
        )

        # Test recovery

        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='ROLLBACK'
            )
        )
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
            protocol.Prepare(
                headers=[],
                io_format=protocol.IOFormat.BINARY,
                expected_cardinality=protocol.Cardinality.ONE,
                statement_name=b'',
                command='SEL ECT 1',
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

        # This Prepare should be handled alright
        await self.con.send(
            protocol.Prepare(
                headers=[],
                io_format=protocol.IOFormat.BINARY,
                expected_cardinality=protocol.Cardinality.ONE,
                statement_name=b'',
                command='SELECT 1',
            ),
            protocol.Flush()
        )
        await self.con.recv_match(
            protocol.PrepareComplete,
            cardinality=protocol.Cardinality.ONE,
        )

        # Test that Flush has completed successfully -- the
        # command should be executed and no exception should
        # be received.
        # While at it, rogue ROLLBACK should be allowed.
        await self.con.send(
            protocol.ExecuteScript(
                headers=[],
                script='ROLLBACK'
            )
        )
        await self.con.recv_match(
            protocol.CommandComplete,
            status='ROLLBACK'
        )
        await self.con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_proto_connection_lost_cancel_query(self):
        # Prepare the test data
        con2 = await edgedb.async_connect(**self.get_connect_args())
        try:
            await con2.execute(
                'CREATE TYPE tclcq { CREATE PROPERTY p -> str }'
            )
            try:
                await con2.execute("INSERT tclcq { p := 'initial' }")

                # Ready the nested connection
                await self.con.connect()

                # Use an implicit transaction in the nested connection: lock
                # the row with an UPDATE, and then hold the transaction for 10
                # seconds, which is long enough for the upcoming cancellation
                await self.con.send(
                    protocol.ExecuteScript(
                        headers=[],
                        script="""\
                        UPDATE tclcq SET { p := 'inner' };
                        SELECT sys::_sleep(10);
                        """,
                    )
                )

                # Sanity check - we shouldn't get anything here
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.con.recv_match(
                            protocol.CommandComplete,
                            status='UPDATE'
                        ),
                        0.1,
                    )

                # Close the nested connection without waiting for the result;
                # the server is supposed to cancel the pending query.
                await self.con.aclose()

                # In the outer connection, let's wait until the lock is
                # released by either an expected cancellation, or an unexpected
                # commit after 10 seconds.
                tx = con2.raw_transaction()
                await tx.start()
                try:
                    await tx.execute("UPDATE tclcq SET { p := 'lock' }")
                except edgedb.TransactionSerializationError:
                    # In case the nested transaction succeeded, we'll meet an
                    # concurrent update error here, which can be safely ignored
                    pass
                finally:
                    await tx.rollback()

                # Let's check what's in the row - if the cancellation didn't
                # happen, the test will fail with value "inner".
                val = await con2.query_one('SELECT tclcq.p LIMIT 1')
                self.assertEqual(val, 'initial')

            finally:
                # Clean up
                await con2.execute(
                    "DROP TYPE tclcq"
                )
        finally:
            await con2.aclose()
