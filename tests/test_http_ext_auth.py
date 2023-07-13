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

import edgedb

from edb.testbase import http as tb


class TestHttpExtAuth(tb.ExtAuthTestCase):
    def test_http_ext_auth_hello_01(self):
        with self.http_con() as con:
            data, headers, status = self.http_con_request(
                con, {}, path='')

            self.assertEqual(status, 200)
            self.assertIn(b'Hello world', data)
