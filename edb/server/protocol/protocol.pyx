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
import re
import ssl
import urllib.parse

import httptools

from edb import errors
from edb.common import debug
from edb.common import markup

from edb.graphql import extension as graphql_ext

from edb.server import args as srvargs
from edb.server.protocol cimport binary
from edb.server.protocol import binary
from edb.server import defines as edbdef
# Without an explicit cimport of `pgproto.debug`, we
# can't cimport `protocol.binary` for some reason.
from edb.server.pgproto.debug cimport PG_DEBUG

from . import edgeql_ext
from . import metrics
from . import server_info
from . import notebook_ext
from . import system_api
from . import ui_ext


HTTPStatus = http.HTTPStatus

PROTO_MIME = (
    f'application/x.edgedb.'
    f'v_{edbdef.CURRENT_PROTOCOL[0]}_{edbdef.CURRENT_PROTOCOL[1]}'
    f'.binary'
).encode()

PROTO_MIME_RE = re.compile(br'application/x\.edgedb\.v_(\d+)_(\d+)\.binary')


cdef class HttpRequest:

    def __cinit__(self):
        self.body = b''
        self.authorization = b''
        self.content_type = b''


cdef class HttpResponse:

    def __cinit__(self):
        self.status = HTTPStatus.OK
        self.content_type = b'text/plain'
        self.custom_headers = {}
        self.body = b''
        self.close_connection = False


cdef class HttpProtocol:

    def __init__(
        self,
        server,
        sslctx,
        *,
        external_auth: bool=False,
        binary_endpoint_security = None,
        http_endpoint_security = None,
    ):
        self.loop = server.get_loop()
        self.server = server
        self.transport = None
        self.external_auth = external_auth
        self.sslctx = sslctx

        self.parser = None
        self.current_request = None
        self.in_response = False
        self.unprocessed = None
        self.first_data_call = True

        self.binary_endpoint_security = binary_endpoint_security
        self.http_endpoint_security = http_endpoint_security
        self.respond_hsts = False  # redirect non-TLS HTTP clients to TLS URL

        self.is_tls = False

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None
        self.unprocessed = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def eof_received(self):
        pass

    def data_received(self, data):
        if self.first_data_call:
            self.first_data_call = False

            # Detect if the client is speaking TLS in the "first" data using
            # the SSL library. This is not the official handshake as we only
            # need to know "is_tls"; the first data is used again for the true
            # handshake if is_tls = True. This is for further responding a nice
            # error message to non-TLS clients.
            is_tls = True
            try:
                outgoing = ssl.MemoryBIO()
                incoming = ssl.MemoryBIO()
                incoming.write(data)
                sslobj = self.sslctx.wrap_bio(
                    incoming, outgoing, server_side=True
                )
                sslobj.do_handshake()
            except ssl.SSLWantReadError:
                pass
            except ssl.SSLError:
                is_tls = False

            self.is_tls = is_tls

            if is_tls:
                # Most clients should arrive here to continue with TLS
                self.transport.pause_reading()
                self.server.create_task(
                    self._forward_first_data(data), interruptable=True
                )
                self.server.create_task(self._start_tls(), interruptable=True)
                return

            # In case when we're talking to a non-TLS client, keep using the
            # legacy magic byte check to choose the HTTP or binary protocol.
            if data[0:2] == b'V\x00':
                # This is, most likely, our binary protocol,
                # as its first message kind is `V`.
                #
                # Switch protocols now (for compatibility).
                if (
                    self.binary_endpoint_security
                    is srvargs.ServerEndpointSecurityMode.Optional
                ):
                    self._switch_to_binary_protocol(data)
                else:
                    self._return_binary_error(
                        self._switch_to_binary_protocol()
                    )
                return
            else:
                # HTTP.
                self._init_http_parser()
                self.respond_hsts = (
                    self.http_endpoint_security
                    is srvargs.ServerEndpointSecurityMode.Tls
                )

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
        elif name == b'accept':
            if self.current_request.accept:
                self.current_request.accept += b',' + value
            else:
                self.current_request.accept = value
        elif name == b'authorization':
            self.current_request.authorization = value
        elif name.startswith(b'x-edgedb-'):
            if self.current_request.params is None:
                self.current_request.params = {}
            param = name[len(b'x-edgedb-'):]
            self.current_request.params[param] = value

    def on_body(self, body: bytes):
        self.current_request.body += body

    def on_message_begin(self):
        self.current_request = HttpRequest()

    def on_message_complete(self):
        self.transport.pause_reading()

        req = self.current_request

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
            self.server.create_task(
                self._handle_request(req), interruptable=False
            )

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
            {},
            f'{type(ex).__name__}: {ex}'.encode(),
            True)

        self.close()

    cdef resume(self):
        if self.transport is None:
            return

        if self.unprocessed:
            req = self.unprocessed.popleft()
            self.server.create_task(
                self._handle_request(req), interruptable=False
            )
        else:
            self.transport.resume_reading()

    cdef _write(self, bytes req_version, bytes resp_status,
                bytes content_type, dict custom_headers, bytes body,
                bint close_connection):
        if self.transport is None:
            return
        data = [
            b'HTTP/', req_version, b' ', resp_status, b'\r\n',
            b'Content-Type: ', content_type, b'\r\n',
            b'Content-Length: ', f'{len(body)}'.encode(), b'\r\n',
        ]

        for key, value in custom_headers.items():
            data.append(f'{key}: {value}\r\n'.encode())

        if debug.flags.http_inject_cors:
            data.append(b'Access-Control-Allow-Origin: *\r\n')
            data.append(b'Access-Control-Allow-Headers: Content-Type\r\n')
            if custom_headers:
                data.append(b'Access-Control-Expose-Headers: ' + \
                    ', '.join(custom_headers.keys()).encode() + b'\r\n')

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
            response.custom_headers,
            response.body,
            response.close_connection)

    def _switch_to_binary_protocol(self, data=None):
        binproto = binary.new_edge_connection(
            self.server,
            external_auth=self.external_auth,
        )
        self.transport.set_protocol(binproto)
        binproto.connection_made(self.transport)
        if data:
            binproto.data_received(data)
        return binproto

    def _init_http_parser(self):
        self.parser = httptools.HttpRequestParser(self)
        self.current_request = HttpRequest()

    async def _forward_first_data(self, data):
        # As we stole the "first data", we need to manually send it back to
        # the SSLProtocol. The hack here is highly-coupled with uvloop impl.
        transport = self.transport  # The TCP transport
        for i in range(3):
            await asyncio.sleep(0)
            ssl_proto = self.transport.get_protocol()
            if ssl_proto is not self:
                break
        else:
            raise RuntimeError("start_tls() hasn't run in 3 loop iterations")

        # Delay for one more iteration to make sure the first data is fed after
        # SSLProtocol.connection_made() is called.
        await asyncio.sleep(0)

        data_len = len(data)
        buf = ssl_proto.get_buffer(data_len)
        buf[:data_len] = data
        ssl_proto.buffer_updated(data_len)

    async def _start_tls(self):
        self.transport = await self.loop.start_tls(
            self.transport, self, self.sslctx, server_side=True
        )
        sslobj = self.transport.get_extra_info('ssl_object')
        if sslobj.selected_alpn_protocol() == 'edgedb-binary':
            self._switch_to_binary_protocol()
        else:
            # It's either HTTP as the negotiated protocol, or the negotiation
            # failed and we have no idea what ALPN the client has set. Here we
            # just start talking in HTTP, and let the client bindings decide if
            # this is an error based on the ALPN result.
            self._init_http_parser()

    cdef _return_binary_error(self, binary.EdgeConnection proto):
        proto.write_error(errors.BinaryProtocolError(
            'TLS Required',
            details='The server requires Transport Layer Security (TLS)',
            hint='Upgrade the client to a newer version that supports TLS'
        ))
        proto.close()

    async def _handle_request(self, HttpRequest request):
        cdef:
            HttpResponse response = HttpResponse()

        if self.transport is None:
            return

        if self.respond_hsts:
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
                msg = b'Request is missing a header: Host\r\n'
                self.transport.write(
                    b'HTTP/1.1 400 Bad Request\r\n'
                    b'Content-Length: ' + str(len(msg)).encode() + b'\r\n'
                    b'\r\n' + msg
                )

            self.close()
            return

        if self.is_tls:
            if (
                self.http_endpoint_security
                is srvargs.ServerEndpointSecurityMode.Optional
            ):
                response.custom_headers['Strict-Transport-Security'] = \
                    'max-age=0'
            elif (
                self.http_endpoint_security
                is srvargs.ServerEndpointSecurityMode.Tls
            ):
                response.custom_headers['Strict-Transport-Security'] = \
                    'max-age=31536000'
            else:
                raise AssertionError(
                    f"unexpected http_endpoint_security "
                    f"value: {self.http_endpoint_security}"
                )

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
        path_parts_len = len(path_parts)
        route = path_parts[0]

        if route == 'db':
            if path_parts_len < 2:
                return self._not_found(request, response)

            dbname = path_parts[1]
            db = self.server.maybe_get_db(dbname=dbname)
            if db is None:
                return self._not_found(request, response)

            extname = path_parts[2] if path_parts_len > 2 else None

            if (
                # Binary proto tunnelled through HTTP
                (extname is None and request.method == b'POST')
                # Legacy admin UI "extension" path for same
                or (
                    extname == 'admin_binary_http'
                    and self.server.is_admin_ui_enabled()
                )
            ):
                if not request.content_type:
                    return self._bad_request(
                        request,
                        response,
                        message="missing or malformed Content-Type header",
                    )

                ver_m = PROTO_MIME_RE.match(request.content_type)
                if not ver_m:
                    return self._bad_request(
                        request,
                        response,
                        message="missing or malformed Content-Type header",
                    )

                proto_ver = (
                    int(ver_m.group(1).decode()),
                    int(ver_m.group(2).decode()),
                )

                params = request.params
                if params is None:
                    conn_params = {}
                else:
                    conn_params = {
                        n.decode("utf-8"): v.decode("utf-8")
                        for n, v in request.params.items()
                    }

                conn_params["database"] = dbname

                response.body = await binary.eval_buffer(
                    self.server,
                    database=dbname,
                    data=self.current_request.body,
                    conn_params=conn_params,
                    protocol_version=proto_ver,
                    auth_data=self.current_request.authorization,
                    transport=srvargs.ServerConnTransport.HTTP,
                )
                response.status = http.HTTPStatus.OK
                response.content_type = PROTO_MIME
                response.close_connection = True

            else:
                # Check if this is a request to a registered extension
                if extname == 'edgeql':
                    extname = 'edgeql_http'

                if extname not in db.extensions:
                    return self._not_found(request, response)

                args = path_parts[3:]

                if extname == 'graphql':
                    await graphql_ext.handle_request(
                        request, response, db, args, self.server
                    )
                elif extname == 'notebook':
                    await notebook_ext.handle_request(
                        request, response, db, args, self.server
                    )
                elif extname == 'edgeql_http':
                    await edgeql_ext.handle_request(
                        request, response, db, args, self.server
                    )

        elif route == 'server':
            # System API request
            await system_api.handle_request(
                request,
                response,
                path_parts[1:],
                self.server,
            )
        elif path_parts == ['metrics'] and request.method == b'GET':
            # Quoting the Open Metrics spec:
            #    Implementers MUST expose metrics in the OpenMetrics
            #    text format in response to a simple HTTP GET request
            #    to a documented URL for a given process or device.
            #    This endpoint SHOULD be called "/metrics".
            await metrics.handle_request(
                request,
                response,
            )
        elif (path_parts == ['server-info'] and
            request.method == b'GET' and
            (self.server.in_dev_mode() or self.server.in_test_mode())
        ):
            await server_info.handle_request(
                request,
                response,
                self.server,
            )
        elif path_parts[0] == 'ui' and self.server.is_admin_ui_enabled():
            await ui_ext.handle_request(
                request,
                response,
                path_parts[1:],
                self.server,
            )
        else:
            return self._not_found(request, response)

    cdef _not_found(self, HttpRequest request, HttpResponse response):
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True

    cdef _bad_request(
        self,
        HttpRequest request,
        HttpResponse response,
        str message,
    ):
        response.body = message.encode("utf-8")
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
