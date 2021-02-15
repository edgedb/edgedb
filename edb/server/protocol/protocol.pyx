#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


include "./consts.pxi"


import collections
import http
import urllib.parse

import httptools

from edb.common import debug
from edb.common import markup

from edb.graphql import extension as graphql_ext

from edb.server.protocol import binary

from . import edgeql_ext
from . import notebook_ext


HTTPStatus = http.HTTPStatus


cdef class HttpRequest:
    pass


cdef class HttpResponse:

    def __cinit__(self):
        self.status = HTTPStatus.OK
        self.content_type = b'text/plain'
        self.body = b''
        self.close_connection = False


cdef class HttpProtocol:

    def __init__(self, server, *, external_auth: bool=False):
        self.loop = server.get_loop()
        self.server = server
        self.transport = None
        self.external_auth = external_auth

        self.parser = None
        self.current_request = None
        self.in_response = False
        self.unprocessed = None
        self.first_data_call = True

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None
        self.unprocessed = None

    def data_received(self, data):
        if self.first_data_call:
            self.first_data_call = False

            if data[0:2] == b'V\x00':
                # This is, most likely, our binary protocol,
                # as its first message kind is `V`.
                #
                # Switch protocols now (for compatibility).
                binproto = binary.EdgeConnection(
                    self.server, self.external_auth)
                self.transport.set_protocol(binproto)
                binproto.connection_made(self.transport)
                binproto.data_received(data)
                return
            else:
                # HTTP.
                self.parser = httptools.HttpRequestParser(self)
                self.current_request = HttpRequest()

        try:
            self.parser.feed_data(data)
        except Exception as ex:
            self.unhandled_exception(ex)

    def on_url(self, url: bytes):
        self.current_request.url = httptools.parse_url(url)

    def on_header(self, name: bytes, value: bytes):
        name = name.lower()
        if name == b'content-type':
            self.current_request.content_type = value

    def on_body(self, body: bytes):
        self.current_request.body = body

    def on_message_complete(self):
        self.transport.pause_reading()

        req = self.current_request
        self.current_request = HttpRequest()

        req.version = self.parser.get_http_version().encode()
        req.should_keep_alive = self.parser.should_keep_alive()
        req.method = self.parser.get_method().upper()

        if self.in_response:
            # pipelining support
            if self.unprocessed is None:
                self.unprocessed = collections.deque()
            self.unprocessed.append(req)
        else:
            self.in_response = True
            self.loop.create_task(self._handle_request(req))

        self.server._http_last_minute_requests += 1

    cdef close(self):
        self.transport.close()
        self.transport = None
        self.unprocessed = None

    cdef unhandled_exception(self, ex):
        if debug.flags.server:
            markup.dump(ex)

        self._write(
            b'1.0',
            b'400 Bad Request',
            b'text/plain',
            f'{type(ex).__name__}: {ex}'.encode(),
            True)

        self.close()

    cdef resume(self):
        if self.transport is None:
            return

        if self.unprocessed:
            req = self.unprocessed.popleft()
            self.loop.create_task(self._handle_request(req))
        else:
            self.transport.resume_reading()

    cdef _write(self, bytes req_version, bytes resp_status,
                bytes content_type, bytes body, bint close_connection):
        if self.transport is None:
            return
        data = [
            b'HTTP/', req_version, b' ', resp_status, b'\r\n',
            b'Content-Type: ', content_type, b'\r\n',
            b'Content-Length: ', f'{len(body)}'.encode(), b'\r\n',
        ]

        if debug.flags.http_inject_cors:
            data.append(b'Access-Control-Allow-Origin: *\r\n')

        if close_connection:
            data.append(b'Connection: close\r\n')
        data.append(b'\r\n')
        if body:
            data.append(body)
        self.transport.write(b''.join(data))

    cdef write(self, HttpRequest request, HttpResponse response):
        assert type(response.status) is HTTPStatus
        self._write(
            request.version,
            f'{response.status.value} {response.status.phrase}'.encode(),
            response.content_type,
            response.body,
            response.close_connection)

    async def _handle_request(self, HttpRequest request):
        cdef:
            HttpResponse response = HttpResponse()

        if self.transport is None:
            return

        try:
            await self.handle_request(request, response)
        except Exception as ex:
            self.unhandled_exception(ex)
            return

        self.write(request, response)
        self.in_response = False

        if response.close_connection or not request.should_keep_alive:
            self.close()
        else:
            self.resume()

    async def handle_request(self, HttpRequest request, HttpResponse response):
        path = urllib.parse.unquote(request.url.path.decode('ascii'))
        path = path.strip('/')
        path_parts = path.split('/')

        if len(path_parts) >= 3:
            root, dbname, extname, *args = path_parts
            db = self.server.maybe_get_db(dbname=dbname)
            if extname == 'edgeql':
                extname = 'edgeql_http'

            if db is not None and root == 'db' and extname in db.extensions:
                if extname == 'graphql':
                    await graphql_ext.handle_request(
                        request, response, db, args, self.server
                    )
                    return
                elif extname == 'notebook':
                    await notebook_ext.handle_request(
                        request, response, db, args, self.server
                    )
                    return
                elif extname == 'edgeql_http':
                    await edgeql_ext.handle_request(
                        request, response, db, args, self.server
                    )
                    return

        response.body = f'Unknown path: {path!r}'.encode()
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return
