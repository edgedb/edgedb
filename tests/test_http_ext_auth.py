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


import os
import respx
import urllib.parse
import uuid
import json
import base64

import edgedb

from jwcrypto import jwt, jwk
from edb.common import markup
from edb.testbase import http as tb


class TestHttpExtAuth(tb.ExtAuthTestCase):
    client_id = uuid.uuid4()

    SETUP = [
        f"""CONFIGURE CURRENT DATABASE SET xxx_auth_signing_key := <str>'{"a" * 32}';""",
        f"""CONFIGURE CURRENT DATABASE SET xxx_github_client_secret := <str>'{"b" * 32}';""",
        f"""CONFIGURE CURRENT DATABASE SET xxx_github_client_id := <str>'{client_id}';""",
    ]

    async def test_http_auth_ext_github_authorize_01(self):
        with self.http_con() as http_con:
            client_id = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_github_client_id);"""
            )
            auth_signing_key = await self.con.query_single(
                """SELECT assert_single(cfg::Config.xxx_auth_signing_key);"""
            )

            data, headers, status = self.http_con_request(
                http_con, {"provider": "github"}, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "github.com")
            self.assertEqual(url.path, "/login/oauth/authorize")
            self.assertEqual(qs.get("scope"), ["read:user"])

            state = qs.get("state")
            assert state is not None

            key_bytes = base64.b64encode(auth_signing_key.encode())
            key = jwk.JWK(k=key_bytes.decode(), kty="oct")
            signed_token = jwt.JWT(key=key, algs=["HS256"], jwt=state[0])
            claims = json.loads(signed_token.claims)
            self.assertEqual(claims.get("provider"), "github")

            self.assertEqual(
                qs.get("redirect_uri"), [f"{self.http_addr}/callback"]
            )
            self.assertEqual(qs.get("client_id"), [str(client_id)])
