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
from edb.server.compiler import dbstate
from edb.server.compiler import enums
from edb.server.protocol import execute as p_execute
from edb.server.dbview cimport dbview
from edb.server.protocol cimport frontend

from edb.server.pgproto.pgproto cimport (
    WriteBuffer,
)


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
    object tenant,
):
    response.content_type = b'application/json'

    if args == ['status'] and request.method == b'GET':
        try:
            await heartbeat_check(db, tenant)
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
        result = await execute(db, tenant, queries)
    except Exception as ex:
        return handle_error(request, response, ex)
    else:
        response.custom_headers['EdgeDB-Protocol-Version'] = \
            f'{CURRENT_PROTOCOL[0]}.{CURRENT_PROTOCOL[1]}'
        response.body = b'{"kind": "results", "results":' + result + b'}'


async def heartbeat_check(db, tenant):
    pgcon = await tenant.acquire_pgcon(db.name)
    try:
        await pgcon.sql_execute(b"SELECT 'OK';")
    finally:
        tenant.release_pgcon(db.name, pgcon)


cdef class NotebookConnection(frontend.AbstractFrontendConnection):
    def __cinit__(self):
        self.buf = WriteBuffer.new()

    cdef write(self, WriteBuffer data):
        self.buf.write_bytes(bytes(data))

    cdef bytes _get_data(self):
        return bytes(self.buf)

    cdef flush(self):
        pass


async def execute(db, tenant, queries: list):
    dbv: dbview.DatabaseConnectionView = await tenant.new_dbview(
        dbname=db.name,
        query_cache=False,
        protocol_version=edbdef.CURRENT_PROTOCOL,
    )
    compiler_pool = tenant.server.get_compiler_pool()
    units = await compiler_pool.compile_notebook(
        dbv.dbname,
        dbv.get_user_schema_pickled(),
        dbv.get_global_schema_pickled(),
        dbv.reflection_cache,
        dbv.get_database_config(),
        dbv.get_compilation_system_config(),
        queries,
        CURRENT_PROTOCOL,
        50,  # implicit limit
        client_id=tenant.client_id,
    )
    result = []
    bind_data = None
    pgcon = await tenant.acquire_pgcon(db.name)
    try:
        await pgcon.sql_execute(b'START TRANSACTION;')

        for is_error, unit_or_error in units:
            if is_error:
                result.append({
                    'kind': 'error',
                    'error': unit_or_error,
                })
            else:
                query_unit = unit_or_error
                query_unit_group = dbstate.QueryUnitGroup()
                query_unit_group.append(query_unit)

                dbv.check_capabilities(
                    query_unit.capabilities,
                    ALLOWED_CAPABILITIES,
                    errors.UnsupportedCapabilityError,
                    "disallowed in notebook",
                )
                try:
                    if query_unit.in_type_args:
                        raise errors.QueryError(
                            'cannot use query parameters in tutorial')

                    fe_conn = NotebookConnection()

                    dbv.start_implicit(query_unit)

                    compiled = dbview.CompiledQuery(
                        query_unit_group=query_unit_group)
                    await p_execute.execute(
                        pgcon, dbv, compiled, b'', fe_conn=fe_conn,
                        skip_start=True,
                    )

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
                                fe_conn._get_data()).decode(),
                            base64.b64encode(
                                query_unit.status).decode(),
                        ),
                    })

    finally:
        try:
            await pgcon.sql_execute(b'ROLLBACK;')
        finally:
            tenant.release_pgcon(db.name, pgcon)

    return json.dumps(result).encode()
