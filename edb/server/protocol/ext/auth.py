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
import httpx
import datetime
import base64
import immutables
import httptools

from typing import Any, Type, TypeVar, TYPE_CHECKING
from jwcrypto import jwt, jwk

from edb import errors
from edb.common import debug
from edb.common import markup
from edb.server.config import ops


# TODO get this from edb.server.config.ops directly?
SettingsMap = immutables.Map[str, ops.SettingValue]


# Base class for OAuth 2 Providers
class BaseProvider:
    def __init__(self, name: str, client_id: str, client_secret: str):
        self.name = name
        self.client_id = client_id
        self.client_secret = client_secret

    def get_code_url(self) -> str:
        raise NotImplementedError

    async def exchange_code(self, code: str) -> str:
        raise NotImplementedError

    async def fetch_user_info(self, token: str) -> dict:
        raise NotImplementedError


# OAuth 2 Client HTTP Endpoints


def redirect_to_auth_provider(
    response,
    redirect_uri: str,
    iss: str,
    provider_name: str,
    db_config: SettingsMap,
):
    client_id, client_secret = _get_client_credentials(provider_name, db_config)
    provider = _get_provider(
        name=provider_name,
        client_id=client_id,
        client_secret=client_secret,
    )

    signing_key = _get_auth_signing_key(db_config)
    state_token = _make_signed_token(
        iss=iss, provider=provider_name, key=signing_key
    )

    auth_url = provider.get_code_url(
        state=state_token.serialize(), redirect_uri=redirect_uri
    )

    response.status = http.HTTPStatus.FOUND
    response.custom_headers["Location"] = auth_url
    response.close_connection = True


async def handle_auth_callback(request, response):
    return


async def handle_request(request, response, db, args):
    try:
        # Set up routing to the appropriate handler
        if args[0] == "authorize":
            provider_name: str | None = _get_search_param(
                request.url.query.decode("ascii"), "provider"
            )
            if provider_name is None:
                raise errors.InternalServerError("No provider specified")

            redirect_uri = f"{_get_extension_path(request.url)}/callback"

            return redirect_to_auth_provider(
                response=response,
                redirect_uri=redirect_uri,
                iss=request.host.decode(),
                provider_name=provider_name,
                db_config=db.db_config,
            )
        elif args[0] == "callback":
            await handle_auth_callback(request, response)
        else:
            raise errors.BackendError
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        # XXX Fix this when LSP "location" objects are implemented
        ex_type = errors.InternalServerError

        _response_error(
            response, http.HTTPStatus.INTERNAL_SERVER_ERROR, str(ex), ex_type
        )


def _response_error(response, status, message, ex_type):
    err_dct = {
        "message": message,
        "type": str(ex_type.__name__),
        "code": ex_type.get_code(),
    }

    response.body = json.dumps({"error": err_dct}).encode()
    response.status = status
    response.close_connection = True


# OAuth 2 Provider for GitHub
class GitHubProvider(BaseProvider):
    def __init__(self, *args, **kwargs):
        super().__init__("github", *args, **kwargs)

    def get_code_url(
        self, state: str, redirect_uri: str, scope: str = "read:user"
    ) -> str:
        params = {
            "client_id": self.client_id,
            "scope": scope,
            "state": state,
            "redirect_uri": redirect_uri,
        }
        encoded = urllib.parse.urlencode(params)
        return f"https://github.com/login/oauth/authorize?{encoded}"

    async def exchange_access_token(self, code, state):
        # Check state value
        # TODO: Look up state value from FlowState object
        # flow_state = await db.get_flow_state(state, "github")
        # if flow_state is None:
        #    raise errors.UnauthorizedError("invalid state value")
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'state': state,
            "redirect_uri": "http://localhost:8080/auth/callback",  # TODO: Get correct db-namespaced URL
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }

        headers = {'Content-Type': 'application/json'}

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://github.com/login/oauth/access_token",
                json=data,
                headers=headers,
            )
            token = resp.json()['access_token']

            return token


providers = {
    "github": GitHubProvider,
}


def _get_provider(name, client_id, client_secret):
    provider_class = providers.get(name)
    if provider_class is None:
        raise errors.InternalServerError(f"unknown provider: {name}")
    return provider_class(client_id, client_secret)


def _get_auth_signing_key(db_config: SettingsMap):
    auth_signing_key = _maybe_get_config(db_config, "xxx_auth_signing_key")
    if auth_signing_key is None:
        raise errors.InternalServerError(
            "No auth signing key configured: Please set `cfg::Config.xxx_auth_signing_key`"
        )
    key_bytes = base64.b64encode(auth_signing_key.encode())

    return jwk.JWK(kty="oct", k=key_bytes.decode())


def _get_client_credentials(
    provider_name: str, db_config: SettingsMap
) -> tuple[str, str]:
    client_id = _maybe_get_config(db_config, f"xxx_{provider_name}_client_id")
    client_secret = _maybe_get_config(
        db_config, f"xxx_{provider_name}_client_secret"
    )
    if client_id is None or client_secret is None:
        raise errors.InternalServerError(
            f"No client credentials configured for provider `{provider_name}`: "
            f"Please set `cfg::Config.{provider_name}_client_id` and "
            f"`cfg::Config.{provider_name}_client_secret`"
        )
    return (client_id, client_secret)


T = TypeVar("T")


def _maybe_get_config(
    db_config: SettingsMap, key: str, expected_type: Type[T] = str
) -> T | None:
    value = db_config.get(key, (None, None, None, None))[1]

    if value is None:
        return None

    if not isinstance(value, expected_type):
        raise TypeError(
            f"Config value `{key}` must be {expected_type.__name__}, got {type(value).__name__}"
        )

    return value


def _make_signed_token(iss: str, provider: str, key: jwk.JWK) -> jwt.JWT:
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)

    state_claims = {
        "iss": iss,
        "provider": provider,
        "exp": expires_at.astimezone().timestamp(),
    }
    state_token = jwt.JWT(
        header={"alg": "HS256"},
        claims=state_claims,
    )
    state_token.make_signed_token(key)
    return state_token


def _get_search_param(query: str, key: str) -> str | None:
    return urllib.parse.parse_qs(query).get(key, [None])[0]


def _get_extension_path(url: httptools.parser.url_parser.URL) -> str:
    path_parts = url.path.decode().split('/')

    path_seg_parts = ["ext", "auth"]

    try:
        index = path_parts.index(path_seg_parts[0])
        if path_parts[index : index + len(path_seg_parts)] == path_seg_parts:
            index += len(path_seg_parts) - 1
    except ValueError:
        print(f"ext/auth not found in path")
    else:
        path_parts = path_parts[: index + 1]

    new_path = '/'.join(path_parts)

    netloc = (
        f"{url.host.decode()}:{url.port}" if url.port else url.host.decode()
    )
    return f"{url.schema.decode()}://{netloc}{new_path}"
