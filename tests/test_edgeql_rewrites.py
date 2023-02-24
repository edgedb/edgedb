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

# import edgedb

from edb.testbase import server as tb

# from edb.tools import test


class TestRewrites(tb.QueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')
    SETUP = []

    async def test_edgeql_rewrites_01(self):
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using ('inserted');
                create rewrite update using ('updated');
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "inserted"}, {"title": "inserted"}],
        )

        await self.con.execute('update Movie { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "updated"}, {"title": "updated"}],
        )

    async def test_edgeql_rewrites_02(self):
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using (__subject__.title);
                create rewrite update using (__subject__.title);
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash"}, {"title": None}],
        )


    async def test_edgeql_rewrites_03(self):
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using (__subject__.title ++ ' (new)');
                set default := 'untitled';
              };

              create property title_specified: bool {
                create rewrite insert using (__specified__.title);
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie')
        await self.assert_query_result(
            'select Movie { title, title_specified }',
            [
                {"title": "Whiplash (new)", "title_specified": True},
                {"title": "untitled (new)", "title_specified": False},
            ],
        )

    async def test_edgeql_rewrites_04(self):
        # Rewrites should also be applied to children types.

        await self.con.execute(
            '''
            alter type Content {
              create property updated_at -> str {
                create rewrite update using ('just now');
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title, updated_at }',
            [{"title": "The Godfather", "updated_at": None}],
        )

        await self.con.execute(
            'update Movie set { title:= "The Godfather II" }'
        )
        await self.assert_query_result(
            'select Movie { title, updated_at }',
            [{"title": "The Godfather II", "updated_at": "just now"}],
        )

    async def test_edgeql_rewrites_05(self):
        # Rewrites override parent overrides.

        await self.con.execute(
            '''
            alter type Content {
              alter property title {
                set optional;
                create rewrite update
                    using (__subject__.title ++ ' - content updated');
              };
            };
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - movie updated');
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')
        await self.con.execute('insert Content { title:= "Harry Potter" }')

        await self.con.execute('update Content set { title := .title }')
        await self.assert_query_result(
            'select Content { title } order by .title',
            [
                {"title": "Harry Potter - content updated"},
                {"title": "The Godfather - movie updated"},
            ],
        )

    async def test_edgeql_rewrites_06(self):
        await self.con.execute(
            '''
            alter type Content {
                alter property title set optional;
            };
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - updated');
              };
            };
        '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')

        # if pointer is specified, __subject__ should refer to that value
        await self.con.execute('update Movie set { title:= "Whiplash" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated"}],
        )

        # if pointer is not specified, __subject__ should refer to existing
        # values
        await self.con.execute('update Movie set { release_year := 2000 }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated - updated"}],
        )
