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


import dataclasses
import http
import json

import immutables

from edb import errors

from edb.ir import statypes
from edb.server.config import types as configtypes

from edb.common import debug
from edb.common import markup


class ImmutableEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (set, frozenset)):
            return list(obj)
        if isinstance(obj, immutables.Map):
            return dict(obj.items())
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if isinstance(obj, statypes.Duration):
            return obj.to_iso8601()
        if isinstance(obj, statypes.ConfigMemory):
            return obj.to_str()
        if isinstance(obj, configtypes.CompositeConfigType):
            return obj.to_json_value()
        return super().default(obj)


async def handle_request(
    request,
    response,
    server,
):
    try:
        output = ImmutableEncoder().encode(server.get_debug_info())
        response.status = http.HTTPStatus.OK
        response.content_type = b'application/json'
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


def _response_error(response, status, message, ex_type):
    response.body = (
        f'Unexpected error in /server-info.\n\n'
        f'{ex_type.__name__}: {message}'
    ).encode()
    response.status = status
    response.close_connection = True
