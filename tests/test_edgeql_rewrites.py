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

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')
    SETUP = []

    async def test_edgeql_rewrites_01(self):
        # basic overriding of properties
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

        # title is required, but we don't specify it
        await self.con.execute('insert Movie { }')

        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "inserted"}, {"title": "inserted"}],
        )

        await self.con.execute('update Movie set { title:= "The Godfather" }')
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
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash"}],
        )

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError, r"missing value for required property"
        ):
            await self.con.execute('insert Movie { }')

        # if title is specified
        # __subject__.title refers to the new value
        await self.con.execute('update Movie set { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather"}],
        )

        # if title is not specified
        # __subject__.title refers to the existing value
        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather"}],
        )

    async def test_edgeql_rewrites_03(self):
        # interaction with default
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using (__subject__.title ++ ' (new)');
                set default := 'untitled';
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash (new)"}, {"title": "untitled (new)"}],
        )

    async def test_edgeql_rewrites_04(self):
        # __specified__

        await self.con.execute(
            '''
            alter type Content alter property title set optional;
            alter type Movie {
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
                {"title_specified": True},
                {"title_specified": False},
            ],
        )

    async def test_edgeql_rewrites_05(self):
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

    async def test_edgeql_rewrites_06(self):
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

    async def test_edgeql_rewrites_06b(self):
        # Rewrites need to obey the covariant check
        await self.con.execute('insert Movie { title:= "Whiplash" }')

        await self.con.execute(
            '''
            create type Collection {
                create property name -> str;
                create link elements -> Content;
            };
            create type Library extending Collection {
                alter link elements set type Book;
            };


            alter type Collection {
              alter link elements {
                create rewrite update
                    using ((select Content filter .title = 'Whiplash' limit 1));
              };
            };
            '''
        )

        await self.con.execute('insert Library')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { elements := {} }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { elements := {} }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { name := "x" }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { name := "x" }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { }')

    async def test_edgeql_rewrites_07(self):
        await self.con.execute(
            '''
            alter type Content alter property title set optional;
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - updated');
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')

        await self.con.execute('update Movie set { title:= "Whiplash" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated"}],
        )

        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated - updated"}],
        )

    async def test_edgeql_rewrites_08(self):
        # __old__
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - ' ++ __old__.title);
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('update Movie set { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather - Whiplash"}],
        )

        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather - Whiplash - The Godfather - Whiplash"}],
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__old__ cannot be used in this expression"
        ):
            await self.con.execute(
                '''
                alter type Movie {
                  alter property title {
                    create rewrite insert using (__old__.title);
                  };
                };
                '''
            )

    async def test_edgeql_rewrites_09(self):
        # a common use case
        await self.con.execute(
            '''
            alter type Content {
              create property title_updated -> str {
                set default := 'never';
                create rewrite update using (
                    'just now'
                    if __specified__.title
                    else __old__.title_updated
                );
              };
            };
            '''
        )

        await self.con.execute('insert Content { title := "Harry Potter" }')
        await self.con.execute('insert Movie { title := "Whiplash" }')
        await self.con.execute(
            '''
            insert Movie {
                title := "The Godfather", title_updated := "a long time ago"
            }
            '''
        )

        await self.assert_query_result(
            'select Movie { title, title_updated } order by .title',
            [
                {"title": "The Godfather", "title_updated": "a long time ago"},
                {"title": "Whiplash", "title_updated": "never"},
            ],
        )

        await self.con.execute(
            '''
            update Content filter .title = "Whiplash" set { title := "Up" }
            '''
        )

        await self.assert_query_result(
            'select Movie { title, title_updated } order by .title',
            [
                {"title": "The Godfather", "title_updated": "a long time ago"},
                {"title": "Up", "title_updated": "just now"},
            ],
        )
