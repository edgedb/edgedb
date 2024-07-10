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


from __future__ import annotations
from typing import Type, TYPE_CHECKING
import http

from edb import errors
from edb.server import metrics
from edb.server import server

from edb.common import debug
from edb.common import markup

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant
    from edb.server.protocol import protocol


async def handle_request(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    tenant: edbtenant.Tenant,
) -> None:
    try:
        if tenant is None or isinstance(tenant.server, server.Server):
            output = metrics.registry.generate()
        else:
            output = metrics.registry.generate(
                tenant=tenant.get_instance_name()
            )
        response.status = http.HTTPStatus.OK
        response.content_type = b'text/plain; version=0.0.4; charset=utf-8'
        response.body = output.encode()
        response.close_connection = True

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
    response.body = (
        f'Unexpected error in /metrics.\n\n'
        f'{ex_type.__name__}: {message}'
    ).encode()
    response.status = status
    response.close_connection = True
