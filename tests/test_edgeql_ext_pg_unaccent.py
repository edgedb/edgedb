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

import os

from edb.testbase import server as tb


class TestEdgeQLExtPgUnaccent(tb.QueryTestCase):
    EXTENSIONS = ['pg_unaccent']

    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas', 'pg_unaccent.esdl'
    )
    SETUP = '''
        insert Post { title := 'Resumè' };
        insert Post { body := 'Resumè' };
    '''

    BACKEND_SUPERUSER = True

    async def test_edgeql_ext_pg_unaccent_01(self):
        await self.assert_query_result(
            """
            WITH
                module ext::pg_unaccent,
                a := {'Pešec prečka cestišče.', 'Železnica', 'Hôtel'}
            SELECT unaccent(a)
            """,
            ['Pesec precka cestisce.', 'Zeleznica', 'Hotel'],
        )

    async def test_edgeql_ext_pg_unaccent_02(self):
        await self.assert_query_result(
            """
            select fts::search(
                Post, 'resuming', language := 'eng'
            ).object { title, body }
            """,
            [{'title': None, 'body': 'Resumè'}],
        )

        await self.assert_query_result(
            """
            select fts::search(
                Post, 'Resumè', language := 'eng'
            ).object { title, body }
            """,
            [{'title': 'Resumè', 'body': None}],
        )
