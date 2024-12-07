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
from typing import Type, TYPE_CHECKING
import http
import json

from edb import errors

from edb.common import debug
from edb.common import markup

from edb.server import compiler
from edb.server import defines as edbdef

from . import execute

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant, server as edbserver
    from edb.server.protocol import protocol


async def handle_request(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    path_parts: list[str],
    server: edbserver.BaseServer,
    tenant: edbtenant.Tenant,
    is_tenant_host: bool,
) -> None:
    try:
        if tenant is None:
            try:
                tenant = server.get_default_tenant()
            except Exception:
                # Multi-tenant server doesn't have default tenant
                pass
        if tenant is None and not is_tenant_host:
            _response(
                response,
                http.HTTPStatus.NOT_FOUND,
                b'"No such tenant configured"',
                True,
            )
        elif path_parts == ['status', 'ready'] and request.method == b'GET':
            if tenant is None:
                await handle_compiler_query(server, response)
            else:
                await tenant.create_task(
                    handle_readiness_query(request, response, tenant),
                    interruptable=False,
                )
        elif path_parts == ['status', 'alive'] and request.method == b'GET':
            if tenant is None:
                await handle_compiler_query(server, response)
            else:
                await tenant.create_task(
                    handle_liveness_query(request, response, tenant),
                    interruptable=False,
                )
        else:
            _response(
                response,
                http.HTTPStatus.NOT_FOUND,
                b'"Unknown path"',
                True,
            )
    except errors.BackendUnavailableError as ex:
        _response_error(
            response, http.HTTPStatus.SERVICE_UNAVAILABLE, str(ex), type(ex)
        )
    except errors.EdgeDBError as ex:
        if debug.flags.server:
            markup.dump(ex)
        _response_error(
            response, http.HTTPStatus.INTERNAL_SERVER_ERROR, str(ex), type(ex)
        )
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        # XXX Fix this when LSP "location" objects are implemented
        ex_type = errors.InternalServerError

        _response_error(
            response, http.HTTPStatus.INTERNAL_SERVER_ERROR, str(ex), ex_type
        )


def _response_error(
    response: protocol.HttpResponse,
    status: http.HTTPStatus,
    message: str,
    ex_type: Type[errors.EdgeDBError],
) -> None:
    err_dct = {
        'message': message,
        'type': str(ex_type.__name__),
        'code': ex_type.get_code(),
    }
    _response(response, status, json.dumps({'error': err_dct}).encode(), True)


def _response(
    response: protocol.HttpResponse,
    status: http.HTTPStatus,
    message: bytes,
    close_connection: bool,
) -> None:
    response.body = message
    response.status = status
    response.content_type = b'application/json'
    response.close_connection = close_connection


def _response_ok(response: protocol.HttpResponse, message: bytes) -> None:
    _response(response, http.HTTPStatus.OK, message, False)


async def _ping(tenant: edbtenant.Tenant) -> bytes:
    if tenant.get_backend_runtime_params().has_create_database:
        dbname = edbdef.EDGEDB_SYSTEM_DB
    else:
        dbname = tenant.default_database

    return await execute.parse_execute_json(
        tenant.get_db(dbname=dbname),
        query="SELECT 'OK'",
        output_format=compiler.OutputFormat.JSON_ELEMENTS,
        # Disable query cache because we need to ensure that the compiled
        # pool is healthy.
        query_cache_enabled=False,
        cached_globally=True,
        use_metrics=False,
        query_tag='gel/system',
    )


async def handle_compiler_query(
    server: edbserver.BaseServer,
    response: protocol.HttpResponse,
) -> None:
    try:
        # This is just testing if the RPC to the compiler is healthy
        await server.get_compiler_pool().make_compilation_config_serializer()
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)
        _response_error(
            response,
            http.HTTPStatus.INTERNAL_SERVER_ERROR,
            str(ex),
            errors.InternalServerError,
        )
    else:
        _response_ok(response, b'"OK"')


async def handle_liveness_query(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    tenant: edbtenant.Tenant,
) -> None:
    _response_ok(response, await _ping(tenant))


async def handle_readiness_query(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    tenant: edbtenant.Tenant,
) -> None:
    if not tenant.is_ready():
        _response_error(
            response,
            http.HTTPStatus.SERVICE_UNAVAILABLE,
            "this server is not ready to accept connections",
            errors.AccessError,
        )
    else:
        _response_ok(response, await _ping(tenant))
