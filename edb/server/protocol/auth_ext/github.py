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


class GitHubProvider(base.BaseOAuthProvider):
    def __init__(self, *args, **kwargs):
        super().__init__("github", *args[1:], **kwargs)

    def _get_code_url(self, state: str, redirect_uri: str) -> str:
        params = {
            "client_id": self.client_id,
            "scope": "read:user",
            "state": state,
            "redirect_uri": redirect_uri,
        }
        encoded = urllib.parse.urlencode(params)
        return f"https://github.com/login/oauth/authorize?{encoded}"

    async def _exchange_access_token(
        self, code: str, state: str, redirect_uri: str
    ):
        # TODO: Check state value
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "state": state,
            "redirect_uri": redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        headers = {"Content-Type": "application/json"}

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://github.com/login/oauth/access_token",
                json=data,
                headers=headers,
            )
            token = resp.json()["access_token"]

            return token
