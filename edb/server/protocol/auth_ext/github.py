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


import urllib.parse
import functools

from . import base
from . import data


class GitHubProvider(base.BaseProvider):
    def __init__(self, *args, **kwargs):
        super().__init__("github", *args, **kwargs)
        self.auth_domain = "https://github.com"
        self.api_domain = "https://api.github.com"
        self.auth_client = functools.partial(
            self.http_factory, base_url=self.auth_domain
        )
        self.api_client = functools.partial(
            self.http_factory, base_url=self.api_domain
        )

    def get_code_url(self, state: str, redirect_uri: str) -> str:
        params = {
            "client_id": self.client_id,
            "scope": "read:user user:email",
            "state": state,
            "redirect_uri": redirect_uri,
        }
        encoded = urllib.parse.urlencode(params)
        return f"{self.auth_domain}/login/oauth/authorize?{encoded}"

    async def exchange_code(self, code: str) -> str:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        async with self.auth_client() as client:
            resp = await client.post(
                "/login/oauth/access_token",
                json=data,
            )
            token = resp.json()["access_token"]

            return token

    async def fetch_user_info(self, token: str) -> data.UserInfo:
        async with self.api_client() as client:
            resp = await client.get(
                "/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            payload = resp.json()
            return data.UserInfo(
                sub=payload["id"],
                preferred_username=payload.get("login"),
                name=payload.get("name"),
                email=payload.get("email"),
                picture=payload.get("avatar_url"),
                updated_at=self._maybe_isoformat_to_timestamp(
                    payload.get("updated_at")
                ),
            )

    async def fetch_emails(self, token: str) -> list[data.Email]:
        async with self.api_client() as client:
            resp = await client.get(
                "/user/emails",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            payload = resp.json()

            return [
                data.Email(
                    address=d["email"],
                    is_verified=d["verified"],
                    is_primary=d["primary"],
                )
                for d in payload
            ]
