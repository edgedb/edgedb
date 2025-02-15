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

import uuid
import urllib.parse
import enum
import logging

from typing import Any, Callable
from datetime import datetime

from . import data, errors
from edb.server.http import HttpClient
from edb.server import auth as jwt_auth
from edb.server.protocol.auth_ext import util as auth_util
from edb.server import metrics

logger = logging.getLogger("edb.server.ext.auth")


class BaseProvider:
    def __init__(
        self,
        name: str,
        issuer_url: str,
        client_id: str,
        client_secret: str,
        *,
        additional_scope: str | None,
        http_factory: Callable[..., HttpClient],
    ):
        self.name = name
        self.issuer_url = issuer_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.http_factory = http_factory
        self.additional_scope = additional_scope

    async def get_code_url(
        self, state: str, redirect_uri: str, additional_scope: str
    ) -> str:
        raise NotImplementedError

    async def exchange_code(
        self, code: str, redirect_uri: str
    ) -> data.OAuthAccessTokenResponse:
        raise NotImplementedError

    async def fetch_user_info(
        self, token_response: data.OAuthAccessTokenResponse
    ) -> data.UserInfo:
        raise NotImplementedError

    def _maybe_isoformat_to_timestamp(self, value: str | None) -> float | None:
        return datetime.fromisoformat(value).timestamp() if value else None


class ContentType(enum.StrEnum):
    JSON = "application/json"
    FORM_ENCODED = "application/x-www-form-urlencoded"


class OpenIDConnectProvider(BaseProvider):
    def __init__(
        self,
        name: str,
        issuer_url: str,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(name, issuer_url, *args, **kwargs)

    async def get_code_url(
        self, state: str, redirect_uri: str, additional_scope: str
    ) -> str:
        oidc_config = await self._get_oidc_config()
        params = {
            "client_id": self.client_id,
            "scope": f"openid profile email {additional_scope}",
            "state": state,
            "redirect_uri": redirect_uri,
            "nonce": str(uuid.uuid4()),
            "response_type": "code",
        }
        encoded = urllib.parse.urlencode(params)
        return f"{oidc_config.authorization_endpoint}?{encoded}"

    async def exchange_code(
        self, code: str, redirect_uri: str
    ) -> data.OpenIDConnectAccessTokenResponse:
        oidc_config = await self._get_oidc_config()

        token_endpoint = urllib.parse.urlparse(oidc_config.token_endpoint)
        async with self.http_factory(
            base_url=f"{token_endpoint.scheme}://{token_endpoint.netloc}"
        ) as client:
            request_body = {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": redirect_uri,
            }
            headers = {"Accept": ContentType.JSON.value}
            resp = await client.post(
                token_endpoint.path,
                data=request_body,
                headers=headers,
            )
            if resp.status_code >= 400:
                raise errors.OAuthProviderFailure(
                    f"Failed to exchange code: {resp.text}"
                )
            content_type = resp.headers.get('Content-Type')
            if content_type.startswith(str(ContentType.JSON)):
                response_body = resp.json()
            else:
                response_body = {
                    k: v[0] if len(v) == 1 else v
                    for k, v in urllib.parse.parse_qs(resp.text).items()
                }

            return data.OpenIDConnectAccessTokenResponse(**response_body)

    async def fetch_user_info(
        self, token_response: data.OAuthAccessTokenResponse
    ) -> data.UserInfo:
        if not isinstance(
            token_response, data.OpenIDConnectAccessTokenResponse
        ):
            raise TypeError(
                "token_response must be of type "
                "OpenIDConnectAccessTokenResponse"
            )
        id_token = token_response.id_token

        # Retrieve JWK Set, potentially from the cache
        oidc_config = await self._get_oidc_config()
        try:
            async def fetcher(url: str) -> jwt_auth.JWKSet:
                jwks_uri = urllib.parse.urlparse(url)
                async with self.http_factory(
                    base_url=f"{jwks_uri.scheme}://{jwks_uri.netloc}"
                ) as client:
                    r = await client.get(jwks_uri.path, cache=True)
                    jwk_set = jwt_auth.JWKSet()
                    jwk_set.load_json(r.text)
                    jwk_set.default_validation_context.allow(
                        "aud", [self.client_id]
                    )
                    jwk_set.default_validation_context.require_expiry()
                    metrics.auth_provider_jwkset_fetch_success.inc(
                        1.0, self.name
                    )
                    return jwk_set

            jwk_set = await auth_util.get_remote_jwtset(
                oidc_config.jwks_uri, fetcher
            )
        except Exception as e:
            metrics.auth_provider_jwkset_fetch_errors.inc(1.0, self.name)
            logger.exception(
                f"Failed to fetch JWK Set from provider {oidc_config.jwks_uri}"
            )
            raise errors.MisconfiguredProvider(
                f"Failed to fetch JWK Set from provider {oidc_config.jwks_uri}"
            ) from e

        # Load the token as a JWT object and verify it directly. This will
        # validate the audience and expiry.
        try:
            payload = jwk_set.validate(id_token)
        except Exception as e:
            metrics.auth_provider_token_validation_errors.inc(1.0, self.name)
            raise errors.MisconfiguredProvider(
                "Failed to parse ID token with provider keyset"
            ) from e

        metrics.auth_provider_token_validation_success.inc(1.0, self.name)

        return data.UserInfo(
            sub=str(payload["sub"]),
            name=payload.get("name"),
            email=payload.get("email"),
            picture=payload.get("picture"),
            source_id_token=id_token,
        )

    async def _get_oidc_config(self) -> data.OpenIDConfig:
        client = self.http_factory(base_url=self.issuer_url)
        response = await client.get(
            '/.well-known/openid-configuration',
            cache=True
        )
        config = response.json()
        return data.OpenIDConfig(**config)
