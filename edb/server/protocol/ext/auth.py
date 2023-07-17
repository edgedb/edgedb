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

from jwcrypto import jwt, jwk
from edb import errors
from edb.common import debug
from edb.common import markup
from edb.server.server import Server
from edb.server.protocol import execute


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


async def redirect_to_auth_provider(request, response, secretkey: jwk.JWK):
    provider_name = request.query.get("provider")
    provider = _get_provider(
        provider_name,
        "", # TODO: Get client_id from server config
        "", # TODO: Get client_secret from server config
    )
    redirect_url = request.query.get("referrer")
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
    state_claims = {
        "iss": request.headers.get("host"),
        "provider": provider_name,
        "referrer": redirect_url,
        "exp": expires_at.astimezone().timestamp(),
    }
    state_token = jwt.JWT(
        key=secretkey,
        algs=["HS256"],
        claims=state_claims,
    )
    state_token.make_signed_token(secretkey)
    auth_url = provider.get_code_url(state=state_token.serialize())
    response.status = http.HTTPStatus.FOUND
    response.headers["Location"] = auth_url
    response.close_connection = True


async def handle_auth_callback(request, response):
    return


async def handle_request(request, response, db, args, server: Server):
    try:
        # Set up routing to the appropriate handler
        if args[0] == "authorize":
            signing_key = await _get_auth_signing_key(db)
            await redirect_to_auth_provider(request, response, signing_key)
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
    def __init__(self, client_id, client_secret):
        super().__init__("github", client_id, client_secret)

    async def get_code_url(self, state: str, scope: str = "read:user") -> str:
        params = {
            "client_id": self.client_id,
            "scope": "read:user",
            "state": state,
            "redirect_uri": "http://localhost:8080/auth/callback",  # TODO: Get correct db-namespaced URL
        }
        return f"https://github.com/login/oauth/authorize?{urllib.parse.urlencode(params)}"

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

async def _get_auth_signing_key(db):
    key_json = await execute.parse_execute_json(
        db,
        "SELECT cfg::Config.xxx_auth_signing_key"
    )
    if key_json is None:
        raise errors.InternalServerError("no JWS key configured")
    auth_signing_key = json.loads(key_json)
    key_bytes = base64.urlsafe_b64encode(auth_signing_key).rstrip(b'=')
    return jwk.JWK(kty="oct", k=key_bytes)