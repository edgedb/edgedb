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


import decimal
import http
import json
import urllib.parse

import immutables

from edb import errors
from edb import edgeql
from edb.server import defines as edbdef
from edb.server.protocol import execute

from edb.common import debug
from edb.common import markup

from edb.edgeql import qltypes

from edb.server import compiler
from edb.server import config
from edb.server.compiler import enums
from edb.server.dbview cimport dbview
from edb.server.pgproto.pgproto cimport WriteBuffer


async def handle_request(
    object request,
    object response,
    object db,
    list args,
    object server,
):
    if args != []:
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return

    variables = None
    globals_ = None
    query = None

    try:
        if request.method == b'POST':
            if request.content_type and b'json' in request.content_type:
                body = json.loads(request.body)
                if not isinstance(body, dict):
                    raise TypeError(
                        'the body of the request must be a JSON object')
                query = body.get('query')
                variables = body.get('variables')
                globals_ = body.get('globals')
            else:
                raise TypeError(
                    'unable to interpret EdgeQL POST request')

        elif request.method == b'GET':
            if request.url.query:
                url_query = request.url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)

                query = qs.get('query')
                if query is not None:
                    query = query[0]

                variables = qs.get('variables')
                if variables is not None:
                    try:
                        variables = json.loads(variables[0])
                    except Exception:
                        raise TypeError(
                            '"variables" must be a JSON object')

                globals_ = qs.get('globals')
                if globals_ is not None:
                    try:
                        globals_ = json.loads(globals_[0])
                    except Exception:
                        raise TypeError(
                            '"globals" must be a JSON object')

        else:
            raise TypeError('expected a GET or a POST request')

        if not query:
            raise TypeError('invalid EdgeQL request: query is missing')

        if variables is not None and not isinstance(variables, dict):
            raise TypeError('"variables" must be a JSON object')

        if globals_ is not None and not isinstance(globals_, dict):
            raise TypeError('"globals" must be a JSON object')

    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        response.body = str(ex).encode()
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
        return

    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'
    try:
        result = await _execute(db, server, query, variables, globals_)
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        ex_type = type(ex)
        if not issubclass(ex_type, errors.EdgeDBError):
            # XXX Fix this when LSP "location" objects are implemented
            ex_type = errors.InternalServerError

        err_dct = {
            'message': str(ex),
            'type': str(ex_type.__name__),
            'code': ex_type.get_code(),
        }

        response.body = json.dumps({'error': err_dct}).encode()
    else:
        response.body = b'{"data":' + result + b'}'


async def _execute(db, server, query, variables, globals_):
    query_cache_enabled = not (
        debug.flags.disable_qcache or debug.flags.edgeql_compile)

    dbv = await server.new_dbview(
        dbname=db.name,
        query_cache=query_cache_enabled,
        protocol_version=edbdef.CURRENT_PROTOCOL,
    )

    if globals_:
        dbv.set_globals(immutables.Map({
            "__::__edb_json_globals__": config.SettingValue(
                name="__::__edb_json_globals__",
                value=_encode_json_value(globals_),
                source='global',
                scope=qltypes.ConfigScope.GLOBAL,
            )
        }))

    query_req = dbview.QueryRequestInfo(
        edgeql.Source.from_string(query),
        protocol_version=edbdef.CURRENT_PROTOCOL,
        input_format=enums.InputFormat.JSON,
        output_format=enums.OutputFormat.JSON,
        allow_capabilities=enums.Capability.MODIFICATIONS,
    )

    compiled = await dbv.parse(query_req)
    qug = compiled.query_unit_group

    args = []
    if qug.in_type_args:
        for param in qug.in_type_args:
            if variables is None or param.name not in variables:
                raise errors.QueryError(
                    f'no value for the ${param.name} query parameter')
            else:
                value = variables[param.name]
                if value is None and param.required:
                    raise errors.QueryError(
                        f'parameter ${param.name} is required')
                args.append(value)

    bind_args = _encode_args(args)

    pgcon = await server.acquire_pgcon(db.name)
    try:
        if len(qug) > 1:
            data = await execute.execute_script(
                pgcon,
                dbv,
                compiled,
                bind_args,
                fe_conn=None,
            )
        else:
            data = await execute.execute(
                pgcon,
                dbv,
                compiled,
                bind_args,
                fe_conn=None,
            )
    finally:
        server.release_pgcon(db.name, pgcon)

    if not data or len(data) > 1 or len(data[0]) != 1:
        raise errors.InternalServerError(
            f'received incorrect response data for a JSON query')

    return data[0][0]


cdef bytes _encode_json_value(object val):
    if isinstance(val, decimal.Decimal):
        jarg = str(val)
    else:
        jarg = json.dumps(val)

    return b'\x01' + jarg.encode('utf-8')


cdef bytes _encode_args(list args):
    cdef:
        WriteBuffer out_buf = WriteBuffer.new()

    if args:
        out_buf.write_int32(len(args))
        for arg in args:
            out_buf.write_int32(0)  # reserved
            if arg is None:
                out_buf.write_int32(-1)
            else:
                jval = _encode_json_value(arg)
                out_buf.write_int32(len(jval))
                out_buf.write_bytes(jval)

    return bytes(out_buf)
