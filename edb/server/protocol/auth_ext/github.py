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


import httpx
import urllib.parse

from . import base
from . import data


class GitHubProvider(base.BaseProvider):
    def __init__(self, *args, **kwargs):
        super().__init__("github", *args, **kwargs)

    def get_code_url(self, state: str, redirect_uri: str) -> str:
        params = {
            "client_id": self.client_id,
            "scope": "read:user user:email",
            "state": state,
            "redirect_uri": redirect_uri,
        }
        encoded = urllib.parse.urlencode(params)
        return f"https://github.com/login/oauth/authorize?{encoded}"

    async def exchange_code(self, code: str) -> str:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://github.com/login/oauth/access_token",
                json=data,
            )
            print(f"resp: {resp.text!r}")
            token = resp.json()["access_token"]

            return token

    async def get_user_info(self, token: str) -> data.UserInfo:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.github.com/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            data = resp.json()

            return data.UserInfo(
                sub=data["id"],
                preferred_username=data.get("login"),
                name=data.get("name"),
                email=data.get("email"),
                picture=data.get("avatar_url"),
                updated_at=self._maybe_isoformat_to_timestamp(
                    data.get("updated_at")
                ),
            )

    async def fetch_emails(self, token: str) -> list[data.Email]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.github.com/user/emails",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            data = resp.json()

            return [
                data.Email(
                    email=d["email"],
                    verified=d["verified"],
                    primary=d["primary"],
                )
                for d in data
            ]
