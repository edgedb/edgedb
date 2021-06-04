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


import asyncio
import collections
import http
import ssl
import urllib.parse

import httptools

from edb import errors
from edb.common import debug
from edb.common import markup

from edb.graphql import extension as graphql_ext

from edb.server.protocol import binary

from . import edgeql_ext
from . import notebook_ext
from . import system_api


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
        self.response_hsts = False

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None
        self.unprocessed = None

    def eof_received(self):
        pass

    def data_received(self, data):
        if self.first_data_call:
            self.first_data_call = False

            is_ssl = True
            is_binary = False
            try:
                outgoing = ssl.MemoryBIO()
                incoming = ssl.MemoryBIO()
                incoming.write(data)
                sslobj = self.server._sslctx.wrap_bio(
                    incoming, outgoing, server_side=True
                )
                sslobj.do_handshake()
            except ssl.SSLWantReadError:
                pass
            except Exception:
                is_ssl = False
                if not self.server._tls_compat:
                    if data[0:2] == b'V\x00':
                        # This is, most likely, our binary protocol,
                        # as its first message kind is `V`.
                        self.loop.create_task(self._return_binary_error())
                    else:
                        self.response_hsts = True

            if is_ssl:
                self.loop.create_task(self._forward_first_data(data))
                self.loop.create_task(self._start_tls())
                return

            if data[0:2] == b'V\x00':
                # This is, most likely, our binary protocol,
                # as its first message kind is `V`.
                #
                # Switch protocols now (for compatibility).
                self._switch_to_binary_protocol(data)
                return
            else:
                # HTTP.
                self._init_http_parser()

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
        elif name == b'host':
            self.current_request.host = value

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
            self.server.create_task(self._handle_request(req))

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
            self.server.create_task(self._handle_request(req))
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

    def _switch_to_binary_protocol(self, data=None):
        binproto = binary.EdgeConnection(self.server, self.external_auth)
        self.transport.set_protocol(binproto)
        binproto.connection_made(self.transport)
        if data:
            binproto.data_received(data)

    def _init_http_parser(self):
        self.parser = httptools.HttpRequestParser(self)
        self.current_request = HttpRequest()

    async def _forward_first_data(self, data):
        # As we stole the "first data", we need to manually send it back to
        # the SSLProtocol
        transport = self.transport  # The TCP transport
        for i in range(3):
            await asyncio.sleep(0)
            ssl_proto = self.transport.get_protocol()
            if ssl_proto is not self:
                break
        else:
            raise RuntimeError("start_tls() hasn't run in 3 loop iterations")

        await asyncio.sleep(0)
        data_len = len(data)
        buf = ssl_proto.get_buffer(data_len)
        buf[:data_len] = data
        ssl_proto.buffer_updated(data_len)

    async def _start_tls(self):
        self.transport = await self.loop.start_tls(
            self.transport, self, self.server._sslctx, server_side=True
        )
        sslobj = self.transport.get_extra_info('ssl_object')
        if sslobj.selected_alpn_protocol() == 'edgedb-binary':
            self._switch_to_binary_protocol()
        else:
            self._init_http_parser()

    async def _return_binary_error(self):
        binary_proto = self.transport.get_protocol()
        await binary_proto.write_error(errors.BinaryProtocolError(
            'TLS Required',
            details='The server requires Transport Layer Security (TLS)',
            hint='Upgrade the client to a newer version that supports TLS'
        ))
        binary_proto.close()

    async def _handle_request(self, HttpRequest request):
        cdef:
            HttpResponse response = HttpResponse()

        if self.transport is None:
            return

        if self.response_hsts:
            if request.host:
                path = request.url.path.lstrip(b'/')
                loc = b'https://' + request.host + b'/' + path
                self.transport.write(
                    b'HTTP/1.1 301 Moved Permanently\r\n'
                    b'Strict-Transport-Security: max-age=31536000\r\n'
                    b'Location: ' + loc + b'\r\n'
                    b'\r\n'
                )
            else:
                msg = b'Request is missing header: Host\r\n'
                self.transport.write(
                    b'HTTP/1.1 400 Bad Request\r\n'
                    b'Content-Length: ' + str(len(msg)).encode() + b'\r\n'
                    b'\r\n' + msg
                )

            self.close()
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

        # Check if this a request to a registered extension
        if len(path_parts) >= 3 and path_parts[0] == 'db':
            root, dbname, extname, *args = path_parts
            db = self.server.maybe_get_db(dbname=dbname)
            if extname == 'edgeql':
                extname = 'edgeql_http'

            if db is not None and extname in db.extensions:
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

        elif path_parts and path_parts[0] == 'server':
            # System API request
            await system_api.handle_request(
                request,
                response,
                path_parts[1:],
                self.server,
            )
            return

        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True

        return
