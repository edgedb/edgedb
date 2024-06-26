#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


from typing import Any
import urllib.parse
import functools

from . import base, data, errors


class DiscordProvider(base.BaseProvider):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__("discord", "https://discord.com", *args, **kwargs)
        self.auth_domain = self.issuer_url
        self.api_domain = f"{self.issuer_url}/api/v10"
        self.auth_client = functools.partial(
            self.http_factory, base_url=self.auth_domain
        )
        self.api_client = functools.partial(
            self.http_factory, base_url=self.api_domain
        )

    async def get_code_url(
        self, state: str, redirect_uri: str, additional_scope: str
    ) -> str:
        params = {
            "client_id": self.client_id,
            "scope": f"email identify {additional_scope}",
            "state": state,
            "redirect_uri": redirect_uri,
            "response_type": "code",
        }
        encoded = urllib.parse.urlencode(params)
        return f"{self.auth_domain}/oauth2/authorize?{encoded}"

    async def exchange_code(
        self, code: str, redirect_uri: str
    ) -> data.OAuthAccessTokenResponse:
        async with self.auth_client() as client:
            resp = await client.post(
                "/api/oauth2/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": redirect_uri,
                },
                headers={
                    "accept": "application/json",
                },
            )
            if resp.status_code >= 400:
                raise errors.OAuthProviderFailure(
                    f"Failed to exchange code: {resp.text}"
                )
            json = resp.json()

            return data.OAuthAccessTokenResponse(**json)

    async def fetch_user_info(
        self, token_response: data.OAuthAccessTokenResponse
    ) -> data.UserInfo:
        async with self.api_client() as client:
            resp = await client.get(
                "/users/@me",
                headers={
                    "Authorization": f"Bearer {token_response.access_token}",
                    "Accept": "application/json",
                    "Cache-Control": "no-store",
                },
            )
            payload = resp.json()
            return data.UserInfo(
                sub=str(payload["id"]),
                preferred_username=payload.get("username"),
                name=payload.get("global_name"),
                email=payload.get("email"),
                picture=payload.get("avatar"),
            )
