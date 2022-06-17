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


import base64
import http
import json
import urllib.parse

import immutables

from edb import errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server import compiler
from edb.server import defines as edbdef
from edb.server.compiler import OutputFormat
from edb.server.compiler import enums

include "./consts.pxi"

cdef tuple CURRENT_PROTOCOL = edbdef.CURRENT_PROTOCOL

ALLOWED_CAPABILITIES = (
    enums.Capability.MODIFICATIONS |
    enums.Capability.DDL
)


cdef handle_error(
    object request,
    object response,
    error
):
    if debug.flags.server:
        markup.dump(error)

    er_type = type(error)
    if not issubclass(er_type, errors.EdgeDBError):
        er_type = errors.InternalServerError

    response.body = json.dumps({
        'kind': 'error',
        'error': {
            'message': str(error),
            'type': er_type.__name__,
        }
    }).encode()
    response.status = http.HTTPStatus.BAD_REQUEST
    response.close_connection = True


async def handle_request(
    object request,
    object response,
    object db,
    list args,
    object server,
):
    response.content_type = b'application/json'

    if args == ['status'] and request.method == b'GET':
        try:
            await heartbeat_check(db, server)
        except Exception as ex:
            return handle_error(request, response, ex)
        else:
            response.status = http.HTTPStatus.OK
            response.body = b'{"kind": "status", "status": "OK"}'
            return

    if args != []:
        ex = Exception(f'Unknown path')
        return handle_error(request, response, ex)

    queries = None

    try:
        if request.method == b'POST':
            body = json.loads(request.body)
            if not isinstance(body, dict):
                raise TypeError(
                    'the body of the request must be a JSON object')
            queries = body.get('queries')

        else:
            raise TypeError('expected a POST request')

        if not queries:
            raise TypeError(
                'invalid notebook request: "queries" is missing')

    except Exception as ex:
        return handle_error(request, response, ex)

    response.status = http.HTTPStatus.OK
    try:
        result = await execute(db, server, queries)
    except Exception as ex:
        return handle_error(request, response, ex)
    else:
        response.custom_headers['EdgeDB-Protocol-Version'] = \
            f'{CURRENT_PROTOCOL[0]}.{CURRENT_PROTOCOL[1]}'
        response.body = b'{"kind": "results", "results":' + result + b'}'


async def heartbeat_check(db, server):
    pgcon = await server.acquire_pgcon(db.name)
    try:
        await pgcon.simple_query(b"SELECT 'OK';", True)
    finally:
        server.release_pgcon(db.name, pgcon)


async def compile(db, server, list queries):
    compiler_pool = server.get_compiler_pool()
    return await compiler_pool.compile_notebook(
        db.name,
        db.user_schema,
        server.get_global_schema(),
        db.reflection_cache,
        db.db_config,
        server.get_compilation_system_config(),
        queries,
        CURRENT_PROTOCOL,
        50,  # implicit limit
    )


async def execute(db, server, queries: list):
    dbver = db.dbver
    units = await compile(db, server, queries)
    result = []

    pgcon = await server.acquire_pgcon(db.name)
    try:
        await pgcon.simple_query(b'START TRANSACTION;', True)

        for is_error, unit_or_error in units:
            if is_error:
                result.append({
                    'kind': 'error',
                    'error': unit_or_error,
                })
            else:
                query_unit = unit_or_error
                if query_unit.capabilities & ~ALLOWED_CAPABILITIES:
                    raise query_unit.capabilities.make_error(
                        ALLOWED_CAPABILITIES,
                        errors.UnsupportedCapabilityError,
                    )
                try:
                    if query_unit.in_type_args:
                        raise errors.QueryError(
                            'cannot use query parameters in tutorial')

                    data = await pgcon.parse_execute_notebook(
                        query_unit.sql[0], dbver)
                except Exception as ex:
                    if debug.flags.server:
                        markup.dump(ex)

                    # TODO: copy proper error reporting from edgecon
                    if not issubclass(type(ex), errors.EdgeDBError):
                        ex_type = 'Error'
                    else:
                        ex_type = type(ex).__name__

                    result.append({
                        'kind': 'error',
                        'error': [ex_type, str(ex), {}],
                    })

                    break
                else:
                    result.append({
                        'kind': 'data',
                        'data': (
                            base64.b64encode(
                                query_unit.out_type_id).decode(),
                            base64.b64encode(
                                query_unit.out_type_data).decode(),
                            base64.b64encode(
                                data).decode(),
                            base64.b64encode(
                                query_unit.status).decode(),
                        ),
                    })

    finally:
        try:
            await pgcon.simple_query(b'ROLLBACK;', True)
        finally:
            server.release_pgcon(db.name, pgcon)

    return json.dumps(result).encode()
