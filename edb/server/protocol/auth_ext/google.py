#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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
import uuid
import json

from jwcrypto import jwt, jwk
from . import base, data, errors


class GoogleProvider(base.BaseProvider):
    def __init__(self, *args, **kwargs):
        super().__init__(
            "google", "https://accounts.google.com", *args, **kwargs
        )
        self.auth_domain = "https://oauth2.googleapis.com/"
        self.api_domain = "https://www.googleapis.com"
        self.auth_client = functools.partial(
            self.http_factory, base_url=self.auth_domain
        )
        self.api_client = functools.partial(
            self.http_factory, base_url=self.api_domain
        )

    def get_code_url(self, state: str, redirect_uri: str) -> str:
        params = {
            "client_id": self.client_id,
            "scope": "openid profile email",
            "state": state,
            "redirect_uri": redirect_uri,
            "nonce": str(uuid.uuid4()),
            "response_type": "code",
        }
        encoded = urllib.parse.urlencode(params)
        return f"{self.issuer}/o/oauth2/v2/auth?{encoded}"

    async def exchange_code(self, code: str) -> str:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        async with self.auth_client() as client:
            resp = await client.post(
                "/token",
                json=data,
            )
            token = resp.json()["access_token"]
            self.id_token = resp.json()["id_token"]

            return token

    async def fetch_user_info(self, token: str) -> data.UserInfo:
        # Retrieve Google's JWKs
        async with self.api_client() as client:
            r = await client.get('/oauth2/v3/certs')
        keyset = r.json()

        # Load the token as a JWT object and verify it directly
        jwk_set = jwk.JWKSet.from_json(keyset)
        id_token_verified = jwt.JWT(key=jwk_set, jwt=self.id_token)
        payload = json.loads(id_token_verified.claims)
        if payload["iss"] != self.issuer_url:
            raise errors.InvalidData("Invalid value for iss in id_token")
        if payload["aud"] != self.client_id:
            raise errors.InvalidData("Invalid value for aud in id_token")

        return data.UserInfo(
            sub=str(payload["sub"]),
            name=payload.get("name"),
            email=payload.get("email"),
            picture=payload.get("picture"),
        )
