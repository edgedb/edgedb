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


import json
import urllib.parse

from edb import errors
from edb.graphql import errors as gql_errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server.http import http
from edb.server.http cimport http

from . import explore
from . import compiler


cdef class Protocol(http.HttpProtocol):

    def __init__(self, loop, server, query_cache):
        http.HttpProtocol.__init__(self, loop)
        self.server = server
        self.query_cache = query_cache

    async def handle_request(self, http.HttpRequest request,
                             http.HttpResponse response):
        url_path = request.url.path.strip(b'/')

        if url_path == b'explore' and request.method == b'GET':
            response.body = explore.EXPLORE_HTML
            response.content_type = b'text/html'
            return

        if url_path != b'':
            response.body = f'Unknown path: /{url_path.decode()!r}'.encode()
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True
            return

        operation_name = None
        variables = None

        try:
            if request.method == b'POST':
                if request.content_type and b'json' in request.content_type:
                    body = json.loads(request.body)
                    if not isinstance(body, dict):
                        raise TypeError(
                            'the body of the request must be a JSON object')
                    query = body.get('query')
                    operation_name = body.get('operationName')
                    variables = body.get('variables')
                elif request.content_type == 'application/graphql':
                    query = request.body.decode('utf-8')
                else:
                    raise TypeError(
                        'unable to interpret GraphQL POST request')

            elif request.method == b'GET':
                url_query = request.url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)

                query = qs.get('query')
                if query is not None:
                    query = query[0]

                operation_name = qs.get('operationName')
                if operation_name is not None:
                    operation_name = operation_name[0]

                variables = qs.get('variables')
                if variables is not None:
                    variables = json.loads(variables[0])

            else:
                raise TypeError('expected a GET or a POST request')

            if not query:
                raise TypeError('invalid GraphQL request: query is missing')

            if (operation_name is not None and
                    not isinstance(operation_name, str)):
                raise TypeError('operationName must be a string')

            if variables is not None and not isinstance(variables, dict):
                raise TypeError('variables must be a JSON object')

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
            result = await self.execute(query, operation_name, variables)
        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            ex_type = type(ex)
            if issubclass(ex_type, (gql_errors.GraphQLError,
                                    pgerrors.BackendError)):
                # XXX Fix this when LSP "location" objects are implemented
                ex_type = errors.QueryError

            err_dct = {
                'message': f'{ex_type.__name__}: {ex}',
            }

            if (isinstance(ex, errors.EdgeDBError) and
                    hasattr(ex, 'line') and
                    hasattr(ex, 'col')):
                err_dct['locations'] = [{'line': ex.line, 'column': ex.col}]

            response.body = json.dumps({'errors': [err_dct]}).encode()
        else:
            response.body = b'{"data":' + result + b'}'

    async def compile(self, dbver, query, operation_name, variables):
        compiler = await self.server.compilers.get()
        try:
            return await compiler.call(
                'compile_graphql',
                dbver,
                query,
                operation_name,
                variables)
        finally:
            self.server.compilers.put_nowait(compiler)

    async def execute(self, query, operation_name, variables):
        dbver = self.server.get_dbver()
        cache_key = (query, dbver)

        compiled: compiler.CompiledQuery = self.query_cache.get(
            cache_key, None)

        if compiled is None:
            compiled = await self.compile(
                dbver, query, operation_name, variables)
            self.query_cache[cache_key] = compiled
            op: compiler.CompiledOperation = compiled.get_op(operation_name)
        else:
            op: compiler.CompiledOperation = compiled.get_op(operation_name)
            if op.cache_deps_vars:
                compiled = await self.compile(
                    dbver, query, operation_name, variables)
                op = compiled.get_op(operation_name)

        args = []
        if op.sql_args:
            for name in op.sql_args:
                if variables is None or name not in variables:
                    default = op.variables.get(name)
                    if default is None:
                        raise errors.QueryError(
                            f'no value for the {name!r} variable')
                    args.append(default)
                else:
                    args.append(variables[name])

        pgcon = await self.server.pgcons.get()
        try:
            data = await pgcon.parse_execute_json(op.sql, args)
        finally:
            self.server.pgcons.put_nowait(pgcon)

        return data
