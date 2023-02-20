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


import os.path

import edgedb

from edb.testbase import server as tb
# from edb.tools import test


class TestRewrites(tb.QueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'movies.esdl')
    SETUP = []

    async def test_edgeql_rewrites_01(self):
        await self.con.execute('''
            alter type Movie {
              alter property title {
                create rewrite insert using (__subject__.title ++ ' (new)');
                set default := 'untitled';
              };

              create property title_specified: bool {
                create rewrite insert using (__specified__.title);
              };
            };
        ''')

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie')
        await self.assert_query_result(
            'select Movie { title, title_specified }',
            [
                { "title": "Whiplash (new)", "title_specified": True },
                { "title": "untitled (new)", "title_specified": False }
            ]
        )

