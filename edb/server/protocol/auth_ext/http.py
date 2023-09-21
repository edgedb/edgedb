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
from edb.ir import statypes

from . import oauth, local, errors, util


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
                    provider_id = _get_search_param(
                        request.url.query.decode("ascii"), "provider"
                    )
                    oauth_client = oauth.Client(
                        db=self.db, provider_id=provider_id, base_url=test_url
                    )
                    authorize_url = await oauth_client.get_authorize_url(
                        redirect_uri=self._get_callback_url(),
                        state=self._make_state_claims(provider_id),
                    )
                    response.status = http.HTTPStatus.FOUND
                    response.custom_headers["Location"] = authorize_url

                case ("callback",):
                    query = request.url.query.decode("ascii")
                    state = _get_search_param(query, "state")
                    try:
                        code = _get_search_param(query, "code")
                    except (
                        errors.InvalidData,
                        errors.MisconfiguredProvider,
                    ) as ex:
                        error_description = None
                        if isinstance(ex, errors.MisconfiguredProvider):
                            error = "misconfigured_provider"
                            error_description = ex.description
                        else:
                            error = _get_search_param(query, "error")
                            error_description = _maybe_get_search_param(
                                query, "error_description"
                            )
                        redirect_to = self._get_from_claims(
                            state, "redirect_to"
                        )
                        response.status = http.HTTPStatus.FOUND
                        params = {
                            "error": error,
                        }
                        if error_description is not None:
                            params["error_description"] = error_description
                        response.custom_headers[
                            "Location"
                        ] = f"{redirect_to}?{urllib.parse.urlencode(params)}"
                        return

                    provider_id = self._get_from_claims(state, "provider")
                    redirect_to = self._get_from_claims(state, "redirect_to")
                    oauth_client = oauth.Client(
                        db=self.db,
                        provider_id=provider_id,
                        base_url=test_url,
                    )
                    identity = await oauth_client.handle_callback(code)
                    session_token = self._make_session_token(identity.id)
                    response.status = http.HTTPStatus.FOUND
                    response.custom_headers["Location"] = redirect_to
                    response.custom_headers["Set-Cookie"] = (
                        f"edgedb-session={session_token}; "
                        f"HttpOnly; Secure; SameSite=Strict"
                    )

                case ("register",):
                    content_type = request.content_type
                    match content_type:
                        case b"application/x-www-form-urlencoded":
                            data = {
                                k: v[0]
                                for k, v in urllib.parse.parse_qs(
                                    request.body.decode('ascii')
                                ).items()
                            }
                        case b"application/json":
                            data = json.loads(request.body)
                        case _:
                            raise errors.InvalidData(
                                f"Unsupported Content-Type: {content_type}"
                            )

                    register_provider_id = data.get("provider")
                    if register_provider_id is None:
                        raise errors.InvalidData(
                            'Missing "provider" in register request'
                        )

                    local_client = local.Client(
                        db=self.db, provider_id=register_provider_id
                    )
                    try:
                        identity = await local_client.register(data)
                        session_token = self._make_session_token(identity.id)
                        response.custom_headers["Set-Cookie"] = (
                            f"edgedb-session={session_token}; "
                            f"HttpOnly; Secure; SameSite=Strict"
                        )
                        if data.get("redirect_to") is not None:
                            response.status = http.HTTPStatus.FOUND
                            redirect_params = urllib.parse.urlencode(
                                {
                                    "identity_id": identity.id,
                                    "auth_token": session_token,
                                }
                            )
                            redirect_url = (
                                f"{data['redirect_to']}?{redirect_params}"
                            )
                            response.custom_headers["Location"] = redirect_url
                        else:
                            response.status = http.HTTPStatus.CREATED
                            response.content_type = b"application/json"
                            response.body = json.dumps(
                                {
                                    "identity_id": identity.id,
                                    "auth_token": session_token,
                                }
                            ).encode()
                    except Exception as ex:
                        redirect_on_failure = data.get(
                            "redirect_on_failure", data.get("redirect_to")
                        )
                        if redirect_on_failure is not None:
                            response.status = http.HTTPStatus.FOUND
                            redirect_params = urllib.parse.urlencode(
                                {
                                    "error": str(ex),
                                }
                            )
                            redirect_url = (
                                f"{redirect_on_failure}?{redirect_params}"
                            )
                            response.custom_headers["Location"] = redirect_url
                        else:
                            raise ex

                case ("authenticate",):
                    content_type = request.content_type
                    match content_type:
                        case b"application/x-www-form-urlencoded":
                            data = {
                                k: v[0]
                                for k, v in urllib.parse.parse_qs(
                                    request.body.decode('ascii')
                                ).items()
                            }
                        case b"application/json":
                            data = json.loads(request.body)
                        case _:
                            raise errors.InvalidData(
                                f"Unsupported Content-Type: {content_type}"
                            )

                    authenticate_provider_id = data.get("provider")
                    if authenticate_provider_id is None:
                        raise errors.InvalidData(
                            'Missing "provider" in register request'
                        )

                    local_client = local.Client(
                        db=self.db, provider_id=authenticate_provider_id
                    )
                    try:
                        identity = await local_client.authenticate(data)

                        session_token = self._make_session_token(identity.id)
                        response.custom_headers["Set-Cookie"] = (
                            f"edgedb-session={session_token}; "
                            f"HttpOnly; Secure; SameSite=Strict"
                        )
                        if data.get("redirect_to") is not None:
                            response.status = http.HTTPStatus.FOUND
                            redirect_params = urllib.parse.urlencode(
                                {
                                    "identity_id": identity.id,
                                    "auth_token": session_token,
                                }
                            )
                            redirect_url = (
                                f"{data['redirect_to']}?{redirect_params}"
                            )
                            response.custom_headers["Location"] = redirect_url
                        else:
                            response.status = http.HTTPStatus.OK
                            response.content_type = b"application/json"
                            response.body = json.dumps(
                                {
                                    "identity_id": identity.id,
                                    "auth_token": session_token,
                                }
                            ).encode()
                    except Exception as ex:
                        redirect_on_failure = data.get(
                            "redirect_on_failure", data.get("redirect_to")
                        )
                        if redirect_on_failure is not None:
                            response.status = http.HTTPStatus.FOUND
                            redirect_params = urllib.parse.urlencode(
                                {
                                    "error": str(ex),
                                }
                            )
                            redirect_url = (
                                f"{redirect_on_failure}?{redirect_params}"
                            )
                            response.custom_headers["Location"] = redirect_url
                        else:
                            raise ex

                case _:
                    raise errors.NotFound("Unknown auth endpoint")

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

        except errors.NoIdentityFound:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.FORBIDDEN,
                message="No identity found",
                ex_type=edb_errors.ProtocolError,
            )

        except errors.UserAlreadyRegistered as ex:
            _fail_with_error(
                response=response,
                status=http.HTTPStatus.CONFLICT,
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
            self.db.db_config, "ext::auth::AuthConfig::auth_signing_key"
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

    def _make_session_token(self, identity_id: str) -> str:
        signing_key = self._get_auth_signing_key()
        auth_expiration_time = util.get_config(
            self.db.db_config,
            "ext::auth::AuthConfig::token_time_to_live",
            statypes.Duration,
        )
        expires_in = auth_expiration_time.to_timedelta()
        expires_at = datetime.datetime.utcnow() + expires_in

        claims: dict[str, Any] = {
            "iss": self.base_path,
            "sub": identity_id,
        }
        if expires_in.total_seconds() != 0:
            claims["exp"] = expires_at.astimezone().timestamp()
        session_token = jwt.JWT(
            header={"alg": "HS256"},
            claims=claims,
        )
        session_token.make_signed_token(signing_key)
        return session_token.serialize()

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


def _maybe_get_search_param(query: str, key: str) -> str | None:
    params = urllib.parse.parse_qs(query).get(key)
    return params[0] if params else None


def _get_search_param(query: str, key: str) -> str:
    val = _maybe_get_search_param(query, key)
    if val is None:
        raise errors.InvalidData(f"Missing query parameter: {key}")
    return val
