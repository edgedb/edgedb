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


import datetime
import http
import json
import urllib.parse
import base64

from typing import *
from jwcrypto import jwk, jwt

from edb import errors as edb_errors
from edb.common import debug
from edb.common import markup

from . import oauth
from . import errors
from . import util


class Router:
    def __init__(self, *, db: Any, base_path: str, test_mode: bool):
        self.db = db
        self.base_path = base_path
        self.test_mode = test_mode

    async def handle_request(
        self, request: Any, response: Any, args: list[str]
    ):
        test_url = (
            request.params[b'oauth-test-server'].decode()
            if (
                self.test_mode
                and request.params
                and b'oauth-test-server' in request.params
            )
            else None
        )

        try:
            match args:
                case ("authorize",):
                    # TODO: this is ambiguous whether it's a name or ID which
                    # is useful now, but we'll need to pivot to ID sooner than
                    # later and then rename all of this to provider_id
                    provider = _get_search_param(
                        request.url.query.decode("ascii"), "provider"
                    )
                    client = oauth.Client(
                        db=self.db, provider=provider, base_url=test_url
                    )
                    authorize_url = client.get_authorize_url(
                        redirect_uri=self._get_callback_url(),
                        state=self._make_state_claims(provider),
                    )
                    response.status = http.HTTPStatus.FOUND
                    response.custom_headers["Location"] = authorize_url
                    response.close_connection = True

                case ("callback",):
                    query = request.url.query.decode("ascii")
                    state = _get_search_param(query, "state")
                    code = _get_search_param(query, "code")
                    provider = self._get_from_claims(state, "provider")
                    redirect_to = self._get_from_claims(state, "redirect_to")
                    client = oauth.Client(
                        db=self.db,
                        provider=provider,
                        base_url=test_url,
                    )
                    await client.handle_callback(code)
                    response.status = http.HTTPStatus.FOUND
                    response.custom_headers["Location"] = redirect_to
                    response.close_connection = True

                case _:
                    raise errors.NotFound("Unknown OAuth endpoint")

        except errors.NotFound as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.NOT_FOUND,
                message=str(ex),
                ex_type=edb_errors.ProtocolError,
            )

        except errors.InvalidData as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.BAD_REQUEST,
                message=str(ex),
                ex_type=edb_errors.ProtocolError,
            )

        except errors.MissingConfiguration as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(ex),
                ex_type=edb_errors.ProtocolError,
            )

        except Exception as ex:
            if debug.flags.server:
                markup.dump(ex)
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=str(ex),
                ex_type=type(ex),
            )

    def _get_callback_url(self) -> str:
        return f"{self.base_path}/callback"

    def _get_auth_signing_key(self) -> jwk.JWK:
        auth_signing_key = util.get_config(
            self.db.db_config, "xxx_auth_signing_key"
        )
        key_bytes = base64.b64encode(auth_signing_key.encode())

        return jwk.JWK(kty="oct", k=key_bytes.decode())

    def _make_state_claims(self, provider: str) -> str:
        signing_key = self._get_auth_signing_key()
        expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)

        state_claims = {
            "iss": self.base_path,
            "provider": provider,
            "exp": expires_at.astimezone().timestamp(),
        }
        state_token = jwt.JWT(
            header={"alg": "HS256"},
            claims=state_claims,
        )
        state_token.make_signed_token(signing_key)
        return state_token.serialize()

    def _get_from_claims(self, state: str, key: str) -> str:
        signing_key = self._get_auth_signing_key()
        try:
            state_token = jwt.JWT(key=signing_key, jwt=state)
        except Exception:
            raise errors.InvalidData("Invalid state token")
        state_claims: dict[str, str] = json.loads(state_token.claims)
        value = state_claims.get(key)
        if value is None:
            raise errors.InvalidData("Invalid state token")
        return value


def _fail_with_error(
    *,
    response: Any,
    status: http.HTTPStatus,
    message: str,
    ex_type: Any,
):
    err_dct = {
        "message": message,
        "type": str(ex_type.__name__),
        "code": ex_type.get_code(),
    }

    response.body = json.dumps({"error": err_dct}).encode()
    response.status = status
    response.close_connection = True


def _maybe_get_search_param(query: str, key: str) -> str | None:
    params = urllib.parse.parse_qs(query).get(key)
    return params[0] if params else None


def _get_search_param(query: str, key: str) -> str:
    val = _maybe_get_search_param(query, key)
    if val is None:
        raise errors.InvalidData(f"Missing query parameter: {key}")
    return val
