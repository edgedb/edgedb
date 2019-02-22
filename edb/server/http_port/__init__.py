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


from __future__ import annotations

import asyncio
import json
import typing
import urllib.parse

import httptools

from edb import errors
from edb.graphql import errors as gql_errors

from edb.common import debug
from edb.common import markup
from edb.common import taskgroup
from edb.server import baseport
from edb.server.pgcon import errors as pgerrors


class HttpRequest(typing.NamedTuple):

    url: bytes
    content_type: str
    method: str
    body: typing.Optional[bytes]
    version: float


class HttpResponse:
    __slots__ = ('_protocol', '_request', '_headers_sent')

    def __init__(self, protocol, request: HttpRequest):
        self._protocol = protocol
        self._request = request
        self._headers_sent = False

    def write(self, data, *,
              status=200,
              content_type='application/json'):

        self._protocol._transport.write(b''.join([
            f'HTTP/{self._request.version} {status} OK\r\n'.encode('ascii'),
            f'Content-Type: {content_type}\r\n'.encode('ascii'),
            f'Content-Length: {len(data)}\r\n'.encode('ascii'),
            b'\r\n',
            data
        ]))


class HttpProtocol(asyncio.Protocol):

    def __init__(self, *, server=None):
        self._loop = server.get_loop()
        self._server = server
        self._transport = None

        self._req_parser = None
        self._req_url = None
        self._req_content_type = None
        self._req_body = None

    def on_url(self, url: bytes):
        self._req_url = url

    def on_header(self, name: bytes, value: bytes):
        name = name.lower()
        if name == b'content-type':
            self._req_content_type = value.decode('ascii')

    def on_body(self, body: bytes):
        self._req_body = body

    def on_message_complete(self):
        req = HttpRequest(
            url=self._req_url,
            content_type=self._req_content_type,
            method=self._req_parser.get_method().decode('ascii').upper(),
            version=self._req_parser.get_http_version(),
            body=self._req_body,
        )

        self._loop.create_task(
            self.handle(
                req,
                HttpResponse(self, req)))

        self._req_parser = None

    ####

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        pass

    def data_received(self, data):
        if self._req_parser is None:
            self._req_parser = httptools.HttpRequestParser(self)
        self._req_parser.feed_data(data)

    async def handle(self, request, response):
        try:
            operation_name = None
            variables = None
            if request.method == 'POST':
                if request.content_type and 'json' in request.content_type:
                    body = json.loads(request.body)
                    query = body['query']
                    operation_name = body.get('operationName')
                    if operation_name:
                        assert isinstance(operation_name, str)
                    variables = body.get('variables')
                    if variables:
                        assert isinstance(variables, dict)
                elif request.content_type == 'application/graphql':
                    query = body.decode('utf-8')
                else:
                    raise RuntimeError(
                        'unable to interpret GraphQL POST request')
            elif request.method == 'GET':
                url = httptools.parse_url(request.url)
                url_query = url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)
                query = qs.get('query')[0]
                operation_name = qs.get('operationName')
                if operation_name is not None:
                    operation_name = operation_name[0]
                variables = qs.get('variables')
                if variables is not None:
                    variables = json.loads(variables[0])
                if not query:
                    raise RuntimeError(
                        'unable to interpret GraphQL GET request')
            else:
                if not query:
                    raise RuntimeError(
                        f'GraphQL over HTTP does not support '
                        '{request.method} requests')
        except Exception as ex:
            response.write(f'{type(ex).__name__}: {ex}'.encode(),
                           status=400,
                           content_type='text/plain')
            if debug.flags.server:
                markup.dump(ex)
            return

        try:
            compiler = await self._server._compilers.get()
            try:
                qu = await compiler.call(
                    'compile_graphql',
                    self._server._dbindex.get_dbver(self._server.database),
                    query,
                    operation_name,
                    variables)
            finally:
                self._server._compilers.put_nowait(compiler)

            if operation_name is None:
                if len(qu) == 1:
                    operation_name = next(iter(qu))
                else:
                    raise errors.QueryError(
                        'must provide operation name if query contains '
                        'multiple operations')

            try:
                qu = qu[operation_name]
            except KeyError:
                raise errors.QueryError(
                    f'unknown operation named "{operation_name}"')

            sql = qu['sql']
            argmap = qu['args']

            args = []
            if argmap:
                for name in argmap:
                    if variables is None or name not in variables:
                        default = qu['variables_desc'].get(name)
                        if default is None:
                            raise errors.QueryError(
                                f'no value for the {name!r} variable')
                        args.append(default)
                    else:
                        args.append(variables[name])

            pgcon = await self._server._pgcons.get()
            try:
                data = await pgcon.parse_execute_json(sql, args)
            finally:
                self._server._pgcons.put_nowait(pgcon)

            response.write(b'{"data":' + data + b'}')
        except Exception as ex:
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

            response.write(json.dumps({'errors': [err_dct]}).encode())

            if debug.flags.server:
                markup.dump(ex)


class HttpPort(baseport.Port):

    def __init__(self, nethost: str, netport: int,
                 database: str,
                 user: str,
                 concurrency: int,
                 protocol: str,
                 **kwargs):
        super().__init__(**kwargs)

        if protocol != 'http+graphql':
            raise RuntimeError(f'unknown protocol {protocol!r}')
        if concurrency <= 0 or concurrency > 500:
            raise RuntimeError(
                f'concurrency must be greater than 0 and less than 500')

        self._compilers = asyncio.LifoQueue()
        self._pgcons = asyncio.LifoQueue()
        self._compilers_list = []
        self._pgcons_list = []

        self._nethost = nethost
        self._netport = netport

        self.database = database
        self.user = user
        self.concurrency = concurrency

        self._serving = False
        self._servers = []

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        dbver = self._dbindex.get_dbver(self.database)

        compilers = []
        pgcons = []

        async with taskgroup.TaskGroup() as g:
            for _ in range(self.concurrency):
                compilers.append(
                    g.create_task(self.get_server().new_compiler(
                        self.database, dbver)))
                pgcons.append(
                    g.create_task(self.get_server().new_pgcon(self.database)))

        for com_task in compilers:
            self._compilers.put_nowait(com_task.result())
            self._compilers_list.append(com_task.result())

        for con_task in pgcons:
            self._pgcons.put_nowait(con_task.result())
            self._pgcons_list.append(con_task.result())

        srv = await self._loop.create_server(
            lambda: HttpProtocol(server=self),
            host=self._nethost, port=self._netport)

        self._servers.append(srv)

    async def stop(self):
        self._serving = False

        try:
            async with taskgroup.TaskGroup() as g:
                for srv in self._servers:
                    srv.close()
                    g.create_task(srv.wait_closed())
        finally:
            async with taskgroup.TaskGroup() as g:
                for compiler in self._compilers_list:
                    g.create_task(compiler.close())

            for pgcon in self._pgcons_list:
                pgcon.terminate()
