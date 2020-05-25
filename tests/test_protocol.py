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


from edb.testbase import protocol


class TestProtocol(protocol.ProtocolTestCase):

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
