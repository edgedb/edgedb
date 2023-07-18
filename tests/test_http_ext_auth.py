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

import edgedb

from edb.common import markup
from edb.testbase import http as tb

class TestHttpExtAuth(tb.ExtAuthTestCase):
    @respx.mock
    async def test_http_ext_auth_hello_01(self):
        with self.http_con() as http_con:
            await self.con.execute(
                """
                CONFIGURE SESSION SET xxx_auth_signing_key := <str>'{0}';
                """.format("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            )

            respx.get("https://github.com/login/oauth/authorize").respond(
                status_code=200,
                content=b'Hello world',
            )

            data, headers, status = self.http_con_request(
                http_con, { "provider": "github" }, path='authorize'
            )

            markup.dump({ "data": data, "headers": headers })
            self.assertEqual(status, 302)
            location = headers.get("location")
            self.assertIn("github.com/login/oauth/authorize", location)
