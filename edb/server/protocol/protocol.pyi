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

import asyncio
import http
import http.cookies
import httptools
import ssl

from edb.server import args as srvargs
from edb.server import server

class HttpRequest:
    url: httptools.URL
    version: bytes
    should_keep_alive: bool
    content_type: bytes
    method: bytes
    accept: bytes
    body: bytes
    host: bytes
    origin: bytes
    authorization: bytes
    params: dict[bytes, bytes]
    forwarded: dict[bytes, bytes]
    cookies: http.cookies.SimpleCookie

class HttpResponse:
    status: http.HTTPStatus
    close_connection: bool
    content_type: bytes
    custom_headers: dict[str, str]
    body: bytes
    sent: bool

class HttpProtocol(asyncio.Protocol):
    def __init__(
        self,
        server: server.BaseServer,
        sslctx: ssl.SSLContext,
        sslctx_pgext: ssl.SSLContext,
        *,
        external_auth: bool = False,
        binary_endpoint_security: srvargs.ServerEndpointSecurityMode,
        http_endpoint_security: srvargs.ServerEndpointSecurityMode,
    ) -> None:
        ...

    def write_raw(self, data: bytes) -> None:
        ...

    def write(self, request: HttpRequest, response: HttpResponse) -> None:
        ...

    def close(self) -> None:
        ...
