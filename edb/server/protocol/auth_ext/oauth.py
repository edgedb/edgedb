#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
import datetime
import base64
import immutables

from typing import Any, Type, TypeVar, TYPE_CHECKING
from jwcrypto import jwt, jwk

from edb import errors
from edb.server.config import ops

from . import github


# TODO get this from edb.server.config.ops directly?
SettingsMap = immutables.Map[str, ops.SettingValue]


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


providers = {
    "github": github.GitHubProvider,
}


def _get_provider(name, client_id, client_secret):
    provider_class = providers.get(name)
    if provider_class is None:
        raise errors.BackendError(f"Unknown provider: {name}")
    return provider_class(client_id, client_secret)


def _get_auth_signing_key(db_config: SettingsMap):
    auth_signing_key = _maybe_get_config(db_config, "xxx_auth_signing_key")
    if auth_signing_key is None:
        raise errors.BackendError(
            "No auth signing key configured: "
            "Please set `cfg::Config.xxx_auth_signing_key`"
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
        raise errors.BackendError(
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
            f"Config value `{key}` must be {expected_type.__name__}, got "
            f"{type(value).__name__}"
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
