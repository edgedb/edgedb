#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

from edb.testbase import server

from edb.protocol import protocol  # type: ignore
from edb.protocol.protocol import Connection


class ProtocolTestCase(server.DatabaseTestCase):

    PARALLELISM_GRANULARITY = 'database'
    BASE_TEST_CLASS = True

    con: Connection

    def setUp(self):
        self.con = self.loop.run_until_complete(
            protocol.new_connection(
                **self.get_connect_args(database=self.get_database_name())
            )
        )

    def tearDown(self):
        try:
            self.loop.run_until_complete(
                self.con.aclose()
            )
        finally:
            self.con = None
