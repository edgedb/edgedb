#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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

from . import scram

if TYPE_CHECKING:
    from edb.server import tenant as edbtenant
    from edb.server.protocol import protocol


async def handle_request(
    request: protocol.HttpRequest,
    response: protocol.HttpResponse,
    path_parts: list[str],
    tenant: edbtenant.Tenant,
) -> None:
    try:
        if path_parts == ["token"]:
            if not request.authorization:
                response.status = http.HTTPStatus.UNAUTHORIZED
                response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
                return

            scheme, _, auth_str = request.authorization.decode(
                "ascii"
            ).partition(" ")

            if scheme.lower().startswith("scram"):
                scram.handle_request(scheme, auth_str, response, tenant)
            else:
                response.body = b"Unsupported authentication scheme"
                response.status = http.HTTPStatus.UNAUTHORIZED
                response.custom_headers["WWW-Authenticate"] = "SCRAM-SHA-256"
                response.close_connection = True
        else:
            response.body = b"Unknown path"
            response.status = http.HTTPStatus.NOT_FOUND
            response.close_connection = True
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
        "message": message,
        "type": str(ex_type.__name__),
        "code": ex_type.get_code(),
    }

    response.body = json.dumps({"error": err_dct}).encode()
    response.status = status
    response.close_connection = True
