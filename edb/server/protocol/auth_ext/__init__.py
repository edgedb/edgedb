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

import http
import json
import urllib.parse

from edb import errors
from edb.common import debug
from edb.common import markup

from . import providers


async def handle_request(
    request, response, db, extension_base_path: str, args: list[str]
):
    try:
        # Set up routing to the appropriate handler
        if args[0] == "authorize":
            provider_name = _get_search_param(
                request.url.query.decode("ascii"), "provider"
            )
            if provider_name is None:
                raise errors.BackendError(
                    "No provider specified in URL search parameters."
                )

            redirect_uri = f"{extension_base_path}/callback"

            provider = providers.make(db.db_config, provider_name)
            return await provider.authorize(
                response=response,
                redirect_uri=redirect_uri,
                issuer=extension_base_path,
            )
        elif args[0] == "callback":
            raise errors.BackendError("Callback not implemented yet.")
        else:
            raise errors.BackendError("Unknown OAuth endpoint.")
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        # XXX Fix this when LSP "location" objects are implemented
        ex_type = errors.InternalServerError

        _fail_with_error(
            response=response,
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            message=str(ex),
            ex_type=ex_type,
        )


def _fail_with_error(*, response, status, message, ex_type):
    err_dct = {
        "message": message,
        "type": str(ex_type.__name__),
        "code": ex_type.get_code(),
    }

    response.body = json.dumps({"error": err_dct}).encode()
    response.status = status
    response.close_connection = True


def _get_search_param(query: str, key: str) -> str | None:
    params = urllib.parse.parse_qs(query).get(key)
    return params[0] if params else None
