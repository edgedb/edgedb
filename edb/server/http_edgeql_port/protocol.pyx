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

import immutables

from edb import errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server import compiler
from edb.server.http import http
from edb.server.http cimport http


cdef class Protocol(http.HttpProtocol):

    def __init__(self, loop, server, query_cache):
        http.HttpProtocol.__init__(self, loop)
        self.server = server
        self.query_cache = query_cache

    async def handle_request(self, http.HttpRequest request,
                             http.HttpResponse response):
        url_path = request.url.path.strip(b'/')

        if url_path != b'':
            response.body = f'Unknown path: /{url_path.decode()!r}'.encode()
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
            result = await self.execute(query.encode(), variables)
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

    async def compile(self, dbver, bytes query):
        comp = await self.server.compilers.get()
        try:
            units = await comp.call(
                'compile_eql',
                dbver,
                query,
                None,  # modaliases
                None,  # session config
                True,  # json mode
                False, # expected cardinality is MANY
                0,     # no implicit limit
                compiler.CompileStatementMode.SINGLE,
                compiler.Capability.QUERY,
                True,  # json parameters
            )
            return units[0]
        finally:
            self.server.compilers.put_nowait(comp)

    async def execute(self, bytes query, variables):
        dbver = self.server.get_dbver()
        cache_key = (query, dbver)
        use_prep_stmt = False

        query_unit: compiler.QueryUnit = self.query_cache.get(
            cache_key, None)

        if query_unit is None:
            query_unit = await self.compile(dbver, query)
            self.query_cache[cache_key] = query_unit
        else:
            # This is at least the second time this query is used.
            use_prep_stmt = True

        args = []
        if query_unit.in_type_args:
            for name in query_unit.in_type_args:
                if variables is None or name not in variables:
                    raise errors.QueryError(
                        f'no value for the ${name} query parameter')
                else:
                    args.append(variables[name])

        pgcon = await self.server.pgcons.get()
        try:
            data = await pgcon.parse_execute_json(
                query_unit.sql[0], query_unit.sql_hash, query_unit.dbver,
                use_prep_stmt, args)
        finally:
            self.server.pgcons.put_nowait(pgcon)

        if data is None:
            raise errors.InternalServerError(
                f'no data received for a JSON query {query_unit.sql[0]!r}')

        return data
