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

import edgedb

from edb.common import markup
from edb.testbase import http as tb

class TestHttpExtAuth(tb.ExtAuthTestCase):
    async def test_http_ext_auth_hello_01(self):
        with self.http_con() as http_con:
            signing_key = "a" * 32
            client_id = uuid.uuid4()

            await self.con.execute(
                """
                CONFIGURE SESSION SET xxx_auth_signing_key := <str>'{0}';
                """.format(signing_key)
            )

            data, headers, status = self.http_con_request(
                http_con, { "provider": "github" }, path="authorize"
            )

            self.assertEqual(status, 302)

            location = headers.get("location")
            assert location is not None
            url = urllib.parse.urlparse(location)
            qs = urllib.parse.parse_qs(url.query, keep_blank_values=True)
            markup.dump({ "url": url, "qs": qs })
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.hostname, "github.com")
            self.assertEqual(url.path, "/login/oauth/authorize")
            self.assertEqual(qs.get("scope"), ["read:user"])
            self.assertIsNotNone(qs.get("state"))

            self.assertEqual(qs.get("redirect_uri"), [f"{self.http_addr}/auth/callback"])
            self.assertEqual(qs.get("client_id"), [client_id])