
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
import http.cookies
import re
import ssl
import time
import urllib.parse

import httptools

from edb import errors
from edb.common import debug
from edb.common import markup
from edb.common.log import current_tenant

from edb.graphql import extension as graphql_ext

from edb.server import args as srvargs
from edb.server import config, metrics as srv_metrics
from edb.server import tenant as edbtenant
from edb.server.protocol cimport binary
from edb.server.protocol import binary
from edb.server.protocol import pg_ext
from edb.server import defines as edbdef
from edb.server.dbview cimport dbview
# Without an explicit cimport of `pgproto.debug`, we
# can't cimport `protocol.binary` for some reason.
from edb.server.pgproto.debug cimport PG_DEBUG

from . import auth
from . cimport auth_helpers
from . import edgeql_ext
from . import metrics
from . import server_info
from . import notebook_ext
from . import system_api
from . import ui_ext
from . import auth_ext
from . import ai_ext


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
        self.forwarded = {}
        self.cookies = http.cookies.SimpleCookie()


cdef class HttpResponse:

    def __cinit__(self):
        self.status = HTTPStatus.OK
        self.content_type = b'text/plain'
        self.custom_headers = {}
        self.body = b''
        self.close_connection = False
        self.sent = False


cdef class HttpProtocol:

    def __init__(
        self,
        server,
        sslctx,
        sslctx_pgext,
        *,
        external_auth: bool=False,
        binary_endpoint_security = None,
        http_endpoint_security = None,
    ):
        self.loop = server.get_loop()
        self.server = server
        self.tenant = None
        self.transport = None
        self.external_auth = external_auth
        self.sslctx = sslctx
        self.sslctx_pgext = sslctx_pgext

        self.parser = None
        self.current_request = None
        self.in_response = False
        self.unprocessed = None
        self.first_data_call = True

        self.binary_endpoint_security = binary_endpoint_security
        self.http_endpoint_security = http_endpoint_security
        self.respond_hsts = False  # redirect non-TLS HTTP clients to TLS URL

        self.is_tls = False
        self.is_tenant_host = False

    def connection_made(self, transport):
        self.connection_made_at = time.monotonic()
        self.transport = transport

    def connection_lost(self, exc):
        srv_metrics.client_connection_duration.observe(
            time.monotonic() - self.connection_made_at,
            self.get_tenant_label(),
            "http",
        )
        self.transport = None
        self.unprocessed = None

    def get_tenant_label(self):
        if self.tenant is None:
            return "unknown"
        else:
            return self.tenant.get_instance_name()

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
                self.loop.create_task(self._forward_first_data(data))
                self.loop.create_task(self._start_tls())
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
            elif data[0:1] == b'\x00':
                # Postgres protocol, assuming the 1st message is less than 16MB
                pg_ext_conn = pg_ext.new_pg_connection(
                    self.server,
                    self.sslctx_pgext,
                    self.binary_endpoint_security,
                    connection_made_at=self.connection_made_at,
                )
                self.transport.set_protocol(pg_ext_conn)
                pg_ext_conn.connection_made(self.transport)
                pg_ext_conn.data_received(data)
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
            self.unhandled_exception(b'400 Bad Request', ex)

    def on_url(self, url: bytes):
        self.current_request.url = httptools.parse_url(url)

    def on_header(self, name: bytes, value: bytes):
        name = name.lower()
        if name == b'content-type':
            self.current_request.content_type = value
        elif name == b'host':
            self.current_request.host = value
        elif name == b'origin':
            self.current_request.origin = value
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
        elif name.startswith(b'x-gel-'):
            if self.current_request.params is None:
                self.current_request.params = {}
            param = name[len(b'x-gel-'):]
            self.current_request.params[param] = value
        elif name.startswith(b'x-forwarded-'):
            if self.current_request.forwarded is None:
                self.current_request.forwarded = {}
            forwarded_key = name[len(b'x-forwarded-'):]
            self.current_request.forwarded[forwarded_key] = value
        elif name == b'cookie':
            self.current_request.cookies.load(value.decode('ascii'))

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
            self._schedule_handle_request(req)

        self.server._http_last_minute_requests += 1

    cdef inline _schedule_handle_request(self, request):
        if self.tenant is None:
            self.loop.create_task(self._handle_request(request))
        elif self.tenant.is_accepting_connections():
            self.tenant.create_task(
                self._handle_request(request), interruptable=False
            )
        else:
            self._close_with_error(
                b'503 Service Unavailable',
                b'The server is closing.',
            )

    cpdef close(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None
        self.unprocessed = None

    cdef unhandled_exception(self, bytes status, ex):
        if debug.flags.server:
            markup.dump(ex)

        self._close_with_error(
            status,
            f'{type(ex).__name__}: {ex}'.encode(),
        )

    cdef inline _close_with_error(self, bytes status, bytes message):
        self._write(
            b'1.0',
            status,
            b'text/plain',
            {},
            message,
            True)

        self.close()

    cdef resume(self):
        if self.transport is None:
            return

        if self.unprocessed:
            req = self.unprocessed.popleft()
            self._schedule_handle_request(req)
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
        ]
        if content_type != b"text/event-stream":
            data.extend(
                (b'Content-Length: ', f'{len(body)}'.encode(), b'\r\n'),
            )

        for key, value in custom_headers.items():
            data.append(f'{key}: {value}\r\n'.encode())

        if close_connection:
            data.append(b'Connection: close\r\n')
        data.append(b'\r\n')
        if body:
            data.append(body)
        self.transport.write(b''.join(data))

    cpdef write(self, HttpRequest request, HttpResponse response):
        assert type(response.status) is HTTPStatus
        self._write(
            request.version,
            f'{response.status.value} {response.status.phrase}'.encode(),
            response.content_type,
            response.custom_headers,
            response.body,
            response.close_connection or not request.should_keep_alive)
        response.sent = True

    def write_raw(self, bytes data):
        self.transport.write(data)

    def _switch_to_binary_protocol(self, data=None):
        binproto = binary.new_edge_connection(
            self.server,
            self.tenant,
            external_auth=self.external_auth,
            connection_made_at=self.connection_made_at,
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
        tenant = self.server.retrieve_tenant(sslobj)
        if tenant is edbtenant.host_tenant:
            tenant = None
            self.is_tenant_host = True
        self.tenant = tenant
        if self.tenant is not None:
            current_tenant.set(self.get_tenant_label())
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
        except errors.AvailabilityError as ex:
            self._close_with_error(
                b"503 Service Unavailable",
                f'{type(ex).__name__}: {ex}'.encode(),
            )
            return
        except Exception as ex:
            self.unhandled_exception(b"500 Internal Server Error", ex)
            return

        if not response.sent:
            self.write(request, response)
        self.in_response = False

        if response.close_connection or not request.should_keep_alive:
            self.close()
        else:
            self.resume()

    def check_readiness(self):
        if self.tenant.is_blocked():
            readiness_reason = self.tenant.get_readiness_reason()
            msg = "the server is not accepting requests"
            if readiness_reason:
                msg = f"{msg}: {readiness_reason}"
            raise errors.ServerBlockedError(msg)
        elif not self.tenant.is_online():
            readiness_reason = self.tenant.get_readiness_reason()
            msg = "the server is going offline"
            if readiness_reason:
                msg = f"{msg}: {readiness_reason}"
            raise errors.ServerOfflineError(msg)

    async def handle_request(self, HttpRequest request, HttpResponse response):
        request_url = get_request_url(request, self.is_tls)
        path = request_url.path.decode('ascii')
        path = path.strip('/')
        path_parts = path.split('/')
        path_parts_len = len(path_parts)
        route = path_parts[0]

        if self.tenant is None and route in ['db', 'auth', 'branch']:
            self.tenant = self.server.get_default_tenant()
            self.check_readiness()
            if self.tenant.is_accepting_connections():
                return await self.tenant.create_task(
                    self.handle_request(request, response),
                    interruptable=False,
                )
            else:
                return self._close_with_error(
                    b'503 Service Unavailable',
                    b'The server is closing.',
                )

        if route in ['db', 'branch']:
            if path_parts_len < 2:
                return self._not_found(request, response)

            dbname = urllib.parse.unquote(path_parts[1])
            dbname = self.tenant.resolve_branch_name(
                database=dbname if route == 'db' else None,
                branch=dbname if route == 'branch' else None,
            )
            extname = path_parts[2] if path_parts_len > 2 else None

            # Binary proto tunnelled through HTTP
            if extname is None:
                if await self._handle_cors(
                    request, response,
                    dbname=dbname,
                    allow_methods=['POST'],
                    allow_headers=[
                        'Authorization', 'X-EdgeDB-User', 'X-Gel-User'
                    ],
                ):
                    return

                if request.method == b'POST':
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

                    if proto_ver < edbdef.MIN_PROTOCOL:
                        return self._bad_request(
                            request,
                            response,
                            message="requested protocol version is too old and "
                                "no longer supported",
                        )
                    if proto_ver > edbdef.CURRENT_PROTOCOL:
                        proto_ver = edbdef.CURRENT_PROTOCOL

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
                        self.tenant,
                        database=dbname,
                        data=self.current_request.body,
                        conn_params=conn_params,
                        protocol_version=proto_ver,
                        auth_data=self.current_request.authorization,
                        transport=srvargs.ServerConnTransport.HTTP,
                        tcp_transport=self.transport,
                    )
                    response.status = http.HTTPStatus.OK
                    response.content_type = (
                        f'application/x.edgedb.v_'
                        f'{proto_ver[0]}_{proto_ver[1]}.binary'
                    ).encode()
                    response.close_connection = True

            else:
                if await self._handle_cors(
                    request, response,
                    dbname=dbname,
                    allow_methods=['GET', 'POST'],
                    allow_headers=[
                        'Authorization', 'X-EdgeDB-User', 'X-Gel-User'
                    ],
                    expose_headers=(
                        ['EdgeDB-Protocol-Version', 'Gel-Protocol-Version']
                        if extname == 'notebook'
                        else ['WWW-Authenticate'] if extname != 'auth'
                        else None
                    ),
                    allow_credentials=True
                ):
                    return

                # Check if this is a request to a registered extension
                if extname == 'edgeql':
                    extname = 'edgeql_http'
                if extname == 'ext':
                    if path_parts_len < 4:
                        return self._not_found(request, response)
                    extname = path_parts[3]
                    args = path_parts[4:]
                else:
                    args = path_parts[3:]

                if extname != 'auth':
                    if not await self._check_http_auth(
                        request, response, dbname
                    ):
                        return

                db = self.tenant.maybe_get_db(dbname=dbname)
                if db is None:
                    return self._not_found(request, response)

                if extname not in db.extensions:
                    return self._not_found(request, response)

                if extname == 'graphql':
                    await graphql_ext.handle_request(
                        request, response, db, args, self.tenant
                    )
                elif extname == 'notebook':
                    await notebook_ext.handle_request(
                        request, response, db, args, self.tenant
                    )
                elif extname == 'edgeql_http':
                    await edgeql_ext.handle_request(
                        request, response, db, args, self.tenant
                    )
                elif extname == 'ai':
                    await ai_ext.handle_request(
                        self, request, response, db, args, self.tenant
                    )
                elif extname == 'auth':
                    netloc = (
                        f"{request_url.host.decode()}:{request_url.port}"
                            if request_url.port
                            else request_url.host.decode()
                    )
                    ext_base_path = f"{request_url.schema.decode()}://" \
                                    f"{netloc}/{route}/" \
                                    f"{urllib.parse.quote(dbname)}/ext/auth"
                    handler = auth_ext.http.Router(
                        db=db,
                        base_path=ext_base_path,
                        tenant=self.tenant,
                    )
                    await handler.handle_request(request, response, args)
                    if args:
                        if args[0] == 'ui':
                            if not (len(args) > 1 and args[1] == "_static"):
                                srv_metrics.auth_ui_renders.inc(
                                    1.0, self.get_tenant_label()
                                )
                        else:
                            srv_metrics.auth_api_calls.inc(
                                1.0, self.get_tenant_label()
                            )
                else:
                    return self._not_found(request, response)

        elif route == 'auth':
            if await self._handle_cors(
                request, response,
                allow_methods=['GET'],
                allow_headers=['Authorization'],
                expose_headers=['WWW-Authenticate', 'Authentication-Info']
            ):
                return

            # Authentication request
            await auth.handle_request(
                request,
                response,
                path_parts[1:],
                self.tenant,
            )
        elif route == 'server':
            if not await self._authenticate_for_default_conn_transport(
                request,
                response,
                srvargs.ServerConnTransport.HTTP_HEALTH,
            ):
                return

            # System API request
            await system_api.handle_request(
                request,
                response,
                path_parts[1:],
                self.server,
                self.tenant,
                self.is_tenant_host,
            )
        elif path_parts == ['metrics'] and request.method == b'GET':
            if not await self._authenticate_for_default_conn_transport(
                request,
                response,
                srvargs.ServerConnTransport.HTTP_METRICS,
            ):
                return

            # Quoting the Open Metrics spec:
            #    Implementers MUST expose metrics in the OpenMetrics
            #    text format in response to a simple HTTP GET request
            #    to a documented URL for a given process or device.
            #    This endpoint SHOULD be called "/metrics".
            await metrics.handle_request(
                request,
                response,
                self.tenant,
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
        elif path_parts[0] == 'ui':
            if not self.server.is_admin_ui_enabled():
                return self._not_found(
                    request,
                    response,
                    "Admin UI is not enabled on this EdgeDB instance. "
                    "Run the server with --admin-ui=enabled "
                    "(or EDGEDB_SERVER_ADMIN_UI=enabled) to enable."
                )
            else:
                await ui_ext.handle_request(
                    request,
                    response,
                    path_parts[1:],
                    self.server,
                )
        else:
            return self._not_found(request, response)

    cdef _not_found(
        self,
        HttpRequest request,
        HttpResponse response,
        str message = "Unknown path",
    ):
        response.body = message.encode("utf-8")
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

    async def _handle_cors(
        self,
        HttpRequest request,
        HttpResponse response,
        *,
        str dbname = None,
        list allow_methods = None,
        list allow_headers = [],
        list expose_headers = None,
        bint allow_credentials = False
    ):
        db = self.tenant.maybe_get_db(dbname=dbname) if dbname else None

        config = None
        if db is not None:
            if db.db_config is None:
                await db.introspection()

            config = db.db_config.get('cors_allow_origins')
        if config is None:
            config = self.tenant.get_sys_config().get('cors_allow_origins')

        allowed_origins = config.value if config else None

        if allowed_origins is None:
            return False

        origin = request.origin.decode() if request.origin else None
        origin_allowed = origin is not None and (
            origin in allowed_origins or '*' in allowed_origins)

        if origin_allowed:
            response.custom_headers['Access-Control-Allow-Origin'] = origin
            if expose_headers is not None:
                response.custom_headers['Access-Control-Expose-Headers'] = (
                    ', '.join(expose_headers))

        if request.method == b'OPTIONS':
            response.status = http.HTTPStatus.NO_CONTENT
            if origin_allowed:
                if allow_methods is not None:
                    response.custom_headers['Access-Control-Allow-Methods'] = (
                        ', '.join(allow_methods))
                response.custom_headers['Access-Control-Allow-Headers'] = (
                    ', '.join(['Content-Type'] + allow_headers))
                if allow_credentials:
                    response.custom_headers['Access-Control-Allow-Credentials'] = (
                        'true')

            return True

        return False

    cdef _unauthorized(
        self,
        HttpRequest request,
        HttpResponse response,
        str message,
    ):
        response.body = message.encode("utf-8")
        response.status = http.HTTPStatus.UNAUTHORIZED
        response.close_connection = True

    async def _check_http_auth(
        self,
        HttpRequest request,
        HttpResponse response,
        str dbname,
    ):
        dbindex: dbview.DatabaseIndex = self.tenant._dbindex

        scheme = None
        try:
            # Extract the username from the relevant request headers
            scheme, auth_payload = auth_helpers.extract_token_from_auth_data(
                request.authorization)
            username, opt_password = auth_helpers.extract_http_user(
                scheme, auth_payload, request.params)
            username = self.tenant.resolve_user_name(username)

            # Fetch the configured auth methods
            authmethods = await self.tenant.get_auth_methods(
                username, srvargs.ServerConnTransport.SIMPLE_HTTP)

            auth_errors = {}

            for authmethod in authmethods:
                authmethod_name = authmethod._tspec.name.split('::')[1]
                try:
                    # If the auth method and the provided auth information
                    # match, try to resolve the authentication.
                    if authmethod_name == 'JWT' and scheme == 'bearer':
                        auth_helpers.auth_jwt(
                            self.tenant, auth_payload, username, dbname)
                    elif authmethod_name == 'Password' and scheme == 'basic':
                        auth_helpers.auth_basic(
                            self.tenant, username, opt_password)
                    elif authmethod_name == 'Trust':
                        pass
                    elif authmethod_name == 'SCRAM':
                        raise errors.AuthenticationError(
                            'authentication failed: '
                            'SCRAM authentication required but not '
                            'supported for HTTP'
                        )
                    elif authmethod_name == 'mTLS':
                        if (
                            self.http_endpoint_security
                            is srvargs.ServerEndpointSecurityMode.Tls
                            or self.is_tls
                        ):
                            auth_helpers.auth_mtls_with_user(
                                self.transport, username)
                    else:
                        raise errors.AuthenticationError(
                            'authentication failed: wrong method used')

                except errors.AuthenticationError as e:
                    auth_errors[authmethod_name] = e

                else:
                    break

            if len(auth_errors) == len(authmethods):
                if len(auth_errors) > 1:
                    desc = "; ".join(
                        f"{k}: {e.args[0]}" for k, e in auth_errors.items())
                    raise errors.AuthenticationError(
                        f"all authentication methods failed: {desc}")
                else:
                    raise next(iter(auth_errors.values()))

        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            self._unauthorized(request, response, str(ex))

            # If no scheme was specified, add a WWW-Authenticate header
            if scheme == '':
                response.custom_headers['WWW-Authenticate'] = (
                    'Basic realm="edgedb", Bearer'
                )

            return False

        return True

    async def _authenticate_for_default_conn_transport(
        self,
        HttpRequest request,
        HttpResponse response,
        transport: srvargs.ServerConnTransport,
    ):
        try:
            auth_methods = self.server.get_default_auth_methods(transport)
            auth_errors = {}

            for auth_method in auth_methods:
                authmethod_name = auth_method._tspec.name.split('::')[1]
                try:
                    # If the auth method and the provided auth information
                    # match, try to resolve the authentication.
                    if authmethod_name == 'Trust':
                        pass
                    elif authmethod_name == 'mTLS':
                        if (
                            self.http_endpoint_security
                            is srvargs.ServerEndpointSecurityMode.Tls
                            or self.is_tls
                        ):
                            auth_helpers.auth_mtls(self.transport)
                    else:
                        raise errors.AuthenticationError(
                            'authentication failed: wrong method used')
                except errors.AuthenticationError as e:
                    auth_errors[authmethod_name] = e
                else:
                    break

            if len(auth_errors) == len(auth_methods):
                if len(auth_errors) > 1:
                    desc = "; ".join(
                        f"{k}: {e.args[0]}" for k, e in auth_errors.items())
                    raise errors.AuthenticationError(
                        f"all authentication methods failed: {desc}")
                else:
                    raise next(iter(auth_errors.values()))

        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)

            self._unauthorized(request, response, str(ex))

            return False

        return True

def get_request_url(request, is_tls):
    request_url = request.url
    default_schema = b"https" if is_tls else b"http"
    if all(
        getattr(request_url, attr) is None
        for attr in ('schema', 'host', 'port')
    ):
        forwarded = request.forwarded if hasattr(request, 'forwarded') else {}
        schema = forwarded.get(b'proto', default_schema).decode()
        host_header = forwarded.get(b'host', request.host).decode()

        host, _, port = host_header.partition(':')
        path = request_url.path.decode()
        new_url = f"{schema}://"\
                  f"{host}{port and ':' + port}"\
                  f"{path}"
        request_url = httptools.parse_url(new_url.encode())

    return request_url
