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
import json
import urllib.parse

import immutables

from edb import errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from edb.server import compiler
from edb.server.compiler import IoFormat
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
        response.content_type = b'application/json'

        if url_path != b'':
            response.body = json.dumps({
                'kind': 'error',
                'error': {
                    'message': f'Unknown path: /{url_path.decode()!r}',
                    'type': 'ServerError',
                }
            }).encode()
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True
            return

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
            if debug.flags.server:
                markup.dump(ex)

            response.body = json.dumps({
                'kind': 'error',
                'error':{
                    'message': str(ex),
                    'type': str(type(ex).__name__),
                }
            }).encode()
            response.status = http.HTTPStatus.BAD_REQUEST
            response.close_connection = True
            return

        response.status = http.HTTPStatus.OK
        try:
            result = await self.execute(queries)
        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            ex_type = type(ex)
            if not issubclass(ex_type, errors.EdgeDBError):
                ex_type = errors.InternalServerError

            response.body = json.dumps({
                'kind': 'error',
                'error':{
                    'message': str(ex),
                    'type': str(ex_type.__name__),
                }
            }).encode()
        else:
            response.body = b'{"kind": "results", "results":' + result + b'}'

    async def compile(self, dbver, list queries):
        comp = await self.server.compilers.get()
        try:
            return await comp.call(
                'compile_notebook',
                dbver,
                [q.encode() for q in queries],
                0,  # implicit limit
            )
        finally:
            self.server.compilers.put_nowait(comp)

    async def execute(self, queries: list):
        dbver = self.server.get_dbver()

        units = await self.compile(dbver, queries)
        result = []

        pgcon = await self.server.pgcons.get()
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

                    data = await pgcon.parse_execute_notebook(
                        query_unit.sql[0], query_unit.dbver)

                    result.append({
                        'kind': 'data',
                        'data': (
                            base64.b64encode(query_unit.out_type_id).decode(),
                            base64.b64encode(query_unit.out_type_data).decode(),
                            base64.b64encode(data).decode(),
                        ),
                    })

        finally:
            await pgcon.simple_query(b'ROLLBACK;', True)

            self.server.pgcons.put_nowait(pgcon)

        return json.dumps(result).encode()
