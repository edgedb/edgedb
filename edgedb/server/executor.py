#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import asyncpg

from edgedb.lang.ir import ast as irast
from edgedb.lang.schema import delta as s_delta
from edgedb.lang.schema import deltas as s_deltas
from edgedb.lang.common import exceptions
from edgedb.server import query as edgedb_query

from . import planner


async def execute_plan(plan, protocol):
    backend = protocol.backend

    if isinstance(plan, s_deltas.DeltaCommand):
        return await backend.run_delta_command(plan)

    elif isinstance(plan, s_delta.Command):
        return await backend.run_ddl_command(plan)

    elif isinstance(plan, planner.TransactionStatement):
        if plan.op == 'start':
            protocol.transactions.append(backend.connection.transaction())
            await protocol.transactions[-1].start()

        elif plan.op == 'commit':
            if not protocol.transactions:
                raise exceptions.NoActiveTransactionError(
                    'there is no transaction in progress')
            transaction = protocol.transactions.pop()
            await transaction.commit()

        elif plan.op == 'rollback':
            if not protocol.transactions:
                raise exceptions.NoActiveTransactionError(
                    'there is no transaction in progress')
            transaction = protocol.transactions.pop()
            await transaction.rollback()
            await backend.invalidate_schema_cache()
            await backend.getschema()

        else:
            raise exceptions.InternalError(
                'unexpected transaction statement: {!r}'.format(plan))

    elif isinstance(plan, edgedb_query.Query):
        try:
            ps = await backend.connection.prepare(plan.text)
            return [r[0] for r in await ps.fetch()]

        except asyncpg.PostgresError as e:
            _error = await backend.translate_pg_error(plan, e)
            if _error is not None:
                raise _error from e
            else:
                raise

    elif isinstance(plan, irast.SessionStateCmd):
        # SET command

        await backend.exec_session_state_cmd(plan)

    else:
        raise exceptions.InternalError('unexpected plan: {!r}'.format(plan))
