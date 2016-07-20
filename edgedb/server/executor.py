##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncpg

from edgedb.lang.schema import delta as s_delta

from edgedb.server import query as edgedb_query
from edgedb.server import exceptions as edgedb_exc


async def execute_plan(plan, backend):
    if isinstance(plan, s_delta.Command):
        return await backend.run_delta_command(plan)

    elif isinstance(plan, edgedb_query.Query):
        try:
            ps = await backend.connection.prepare(plan.text)
            return [dict(r.items()) for r in await ps.fetch()]

        except asyncpg.PostgresError as e:
            _error = _translate_pg_error(plan, backend, e)
            if _error is not None:
                raise _error from e
            else:
                raise

    else:
        raise RuntimeError('unexpected plan: {!r}'.format(plan))


def _translate_pg_error(query, backend, error):
    exc = None

    if isinstance(error, asyncpg.NotNullViolationError):
        exc = edgedb_exc.MissingRequiredPointerError(error.message)

    return exc
