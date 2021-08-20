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


import http
import json

import immutables as immu

from edb import errors
from edb import edgeql

from edb.common import debug
from edb.common import markup

from edb.schema import schema as s_schema

from edb.server import compiler
from edb.server.compiler import IoFormat
from edb.server.compiler import enums
from edb.server import defines as edbdef


ALLOWED_CAPABILITIES = (
    enums.Capability.MODIFICATIONS
)


async def handle_request(
    request,
    response,
    path_parts,
    server,
):
    try:
        if path_parts == ['status', 'ready'] and request.method == b'GET':
            await handle_status_request(request, response, server)
        else:
            response.body = b'Unknown path'
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True

        return
    except errors.BackendUnavailableError as ex:
        _response_error(
            response, http.HTTPStatus.SERVICE_UNAVAILABLE, str(ex), type(ex)
        )
    except errors.EdgeDBError as ex:
        if debug.flags.server:
            markup.dump(ex)
        _response_error(
            response, http.HTTPStatus.INTERNAL_SERVER_ERROR, str(ex), type(ex)
        )
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        # XXX Fix this when LSP "location" objects are implemented
        ex_type = errors.InternalServerError

        _response_error(
            response, http.HTTPStatus.INTERNAL_SERVER_ERROR, str(ex), ex_type
        )


def _response_error(response, status, message, ex_type):
    err_dct = {
        'message': message,
        'type': str(ex_type.__name__),
        'code': ex_type.get_code(),
    }

    response.body = json.dumps({'error': err_dct}).encode()
    response.status = status
    response.close_connection = True


async def handle_status_request(
    request,
    response,
    server,
):
    result = await execute(server, "SELECT 'OK'", {})
    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'
    response.body = result
    return


async def compile(server, query):
    compiler_pool = server.get_compiler_pool()

    units, _ = await compiler_pool.compile(
        edbdef.EDGEDB_SYSTEM_DB,
        s_schema.FlatSchema(),  # user schema
        server.get_global_schema(),
        immu.Map(),             # reflection cache
        immu.Map(),             # database config
        server.get_compilation_system_config(),
        edgeql.Source.from_string(query),
        None,           # modaliases
        None,           # session config
        IoFormat.JSON_ELEMENTS,  # json mode
        False,          # expected cardinality is MANY
        0,              # no implicit limit
        False,          # no inlining of type IDs
        False,          # no inlining of type names
        compiler.CompileStatementMode.SINGLE,
        edbdef.CURRENT_PROTOCOL,  # protocol_version
        True,           # inline_objectids
        True,           # json parameters
    )
    return units[0]


async def execute(server, query, variables):
    query_unit = await compile(server, query)
    if query_unit.capabilities & ~ALLOWED_CAPABILITIES:
        raise query_unit.capabilities.make_error(
            ALLOWED_CAPABILITIES,
            errors.UnsupportedCapabilityError,
        )

    args = []
    if query_unit.in_type_args:
        for param in query_unit.in_type_args:
            if variables is None or param.name not in variables:
                raise errors.QueryError(
                    f'no value for the ${param.name} query parameter')
            else:
                value = variables[param.name]
                if value is None and param.required:
                    raise errors.QueryError(
                        f'parameter ${param.name} is required')
                args.append(value)

    pgcon = await server.acquire_pgcon(edbdef.EDGEDB_SYSTEM_DB)
    try:
        data = await pgcon.parse_execute_json(
            query_unit.sql[0],
            query_unit.sql_hash,
            1,
            True,
            args,
        )
    finally:
        server.release_pgcon(edbdef.EDGEDB_SYSTEM_DB, pgcon)

    if data is None:
        raise errors.InternalServerError(
            f'no data received for a JSON query {query_unit.sql[0]!r}')

    return data
