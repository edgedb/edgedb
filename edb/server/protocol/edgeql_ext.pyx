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
import urllib.parse

import immutables

from edb import errors
from edb import edgeql
from edb.server import defines as edbdef
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server import compiler
from edb.server.compiler import IoFormat
from edb.server.compiler import enums


ALLOWED_CAPABILITIES = (
    enums.Capability.MODIFICATIONS
)


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

        else:
            raise TypeError('expected a GET or a POST request')

        if not query:
            raise TypeError('invalid EdgeQL request: query is missing')

        if variables is not None and not isinstance(variables, dict):
            raise TypeError('"variables" must be a JSON object')

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
        result = await execute(db, server, query.encode(), variables)
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


async def compile(db, server, bytes query):
    compiler_pool = server.get_compiler_pool()

    units, _ = await compiler_pool.compile(
        db.name,
        db.user_schema,
        server.get_global_schema(),
        db.reflection_cache,
        db.db_config,
        server.get_compilation_system_config(),
        edgeql.Source.from_string(query.decode('utf-8')),
        None,           # modaliases
        None,           # session config
        IoFormat.JSON,  # json mode
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


async def execute(db, server, bytes query, variables):
    dbver = db.dbver
    query_cache = server._http_query_cache

    cache_key = ('edgeql_http', query, dbver)
    use_prep_stmt = False

    query_unit: compiler.QueryUnit = query_cache.get(
        cache_key, None)

    if query_unit is None:
        query_unit = await compile(db, server, query)
        if query_unit.capabilities & ~ALLOWED_CAPABILITIES:
            raise query_unit.capabilities.make_error(
                ALLOWED_CAPABILITIES,
                errors.UnsupportedCapabilityError,
            )
        query_cache[cache_key] = query_unit
    else:
        # This is at least the second time this query is used.
        use_prep_stmt = True

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

    pgcon = await server.acquire_pgcon(db.name)
    try:
        data = await pgcon.parse_execute_json(
            query_unit.sql[0], query_unit.sql_hash, dbver,
            use_prep_stmt, args)
    finally:
        server.release_pgcon(db.name, pgcon)

    if data is None:
        raise errors.InternalServerError(
            f'no data received for a JSON query {query_unit.sql[0]!r}')

    return data
