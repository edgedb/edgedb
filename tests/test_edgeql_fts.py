#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


class TestEdgeQLFTSQuery(tb.QueryTestCase):
    '''Tests for fts::search.

    This is intended to test the FTS query language as well as result scoring.
    '''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'fts0.esdl')

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'fts_setup0.edgeql'
    )

    async def test_edgeql_fts_search_01(self):
        # At least one of "drink" or "poison" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink poison',
                language := 'eng'
            ).object {
                number,
                text := .text[0:10],
            };
            ''',
            tb.bag(
                [
                    {
                        "number": 15,
                        "text": "There seem",
                    },
                    {
                        "number": 16,
                        "text": "It was all",
                    },
                    {
                        "number": 17,
                        "text": "However, t",
                    },
                ]
            ),
        )

    async def test_edgeql_fts_search_02(self):
        # At least one of "drink" or "me" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink me',
                language := 'eng'
            ).object {
                number,
                text := .text[0:10],
            };
            ''',
            tb.bag(
                [
                    {
                        "number": 15,
                        "text": "There seem",
                    },
                    {
                        "number": 16,
                        "text": "It was all",
                    },
                ]
            ),
        )

    async def test_edgeql_fts_search_03(self):
        # Both "drink" and "poison" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink AND poison',
                language := 'eng'
            ).object {
                number,
                text := .text[0:10],
            };
            ''',
            [{"number": 16, "text": "It was all"}],
        )

        # Same sematics as above
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink "poison"',
                language := 'eng'
            ).object {
                number,
                text := .text[0:10],
            };
            ''',
            [{"number": 16, "text": "It was all"}],
        )

    async def test_edgeql_fts_search_04(self):
        # Search for top 3 best matches for words "white", "rabbit", "gloves",
        # "watch".
        await self.assert_query_result(
            r'''
            with x := (
                select fts::search(
                    Paragraph,
                    'white rabbit gloves watch',
                    language := 'eng'
                )
                order by .score desc
                limit 3
            )
            select x.object {
                number,
                rank := x.score,
            };
            ''',
            [
                {
                    # "hl":
                    # "<b>White</b> <b>Rabbit</b> returning, splendidly "
                    # "dressed, with a pair of <b>white</b> kid <b>gloves</b> "
                    # "in one hand",
                    "number": 8,
                    "rank": 0.6037054,
                },
                {
                    # "hl":
                    # "<b>Rabbit</b>’s little <b>white</b> kid <b>gloves</b> "
                    # "while she was talking. “How _can_ I have done",
                    "number": 14,
                    "rank": 0.4559453,
                },
                {
                    # "hl":
                    # "<b>Rabbit</b> actually _took a <b>watch</b> out of its "
                    # "waistcoat-pocket_, and looked at it, and then",
                    "number": 3,
                    "rank": 0.40634018,
                },
            ],
        )

    async def test_edgeql_fts_search_05(self):
        # Search for top 3 best matches for either one of phrases "golden key"
        # or "white rabbit".
        await self.assert_query_result(
            r'''
            with x := (
                select fts::search(
                    Paragraph,
                    '"golden key" OR "white rabbit"',
                    language := 'eng'
                )
                order by .score desc
                limit 3
            )
            select x.object {
                number,
                rank := x.score,
            };
            ''',
            [
                {
                    # "hl":
                    # "<b>White</b> <b>Rabbit</b> returning, splendidly "
                    # "dressed, with a pair of <b>white</b> kid gloves in one "
                    # "hand",
                    "number": 8,
                    "rank": 0.41372818,
                },
                {
                    # "hl":
                    # "<b>golden</b> <b>key</b>, and Alice’s first thought "
                    # "was that it might belong to one of the doors",
                    "number": 13,
                    "rank": 0.39684132,
                },
                {
                    # "hl":
                    # "<b>White</b> <b>Rabbit</b> was still in sight, "
                    # "hurrying down it. There was not a moment to be lost",
                    "number": 11,
                    "rank": 0.341959,
                },
            ],
        )

    async def test_edgeql_fts_search_06(self):
        # Search for top 3 best matches for "drink" and "poison". However,
        # there's only one paragraph (number 16 as we know from earlier tests)
        # that actually contains both of those words.
        await self.assert_query_result(
            r'''
            with x := (
                select fts::search(
                    Paragraph,
                    'drink AND poison',
                    language := 'eng'
                )
                order by .score desc
                limit 3
            )
            select x.object {
                number,
                ch := .chapter.number,
                rank := x.score,
            };
            ''',
            [
                {
                    "ch": 1,
                    "number": 16,
                    "rank": 0.8530858,
                }
            ],
        )

    async def test_edgeql_fts_language_01(self):
        await self.con.execute(
            '''
            create scalar type MyLangs
                extending enum<English, PigLatin, Esperanto>;

            create type Doc1 {
                create required property x -> str;

                create index fts::index on (
                    fts::with_options(.x, language := MyLangs.English)
                );
            };
            create type Doc2 {
                create required property x -> str;
            };
            '''
        )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "languages `esperanto`, `piglatin` not supported",
        ):
            # In this case, language is not a constant, so we fallback to all
            # possible values of the enum. This then fails because some of them
            # are not supported by postgres.
            await self.con.execute(
                """
                alter type Doc2 create index fts::index on (
                  fts::with_options(.x, language :=
                    MyLangs.English if .x = 'blah' else MyLangs.PigLatin
                  )
                );
                """
            )

    async def test_edgeql_fts_ddl_01(self):
        await self.con.execute(
            '''
            create type Doc {create required property text: str};

            alter type Doc {
                create index fts::index on (
                    fts::with_options(.text, language := fts::Language.eng)
                )
            };

            alter type Doc {
                drop index fts::index on (
                    fts::with_options(.text, language := fts::Language.eng)
                )
            };

            alter type Doc {
                create index fts::index on (
                    fts::with_options(.text, language := fts::Language.eng)
                )
            };
            '''
        )


class TestEdgeQLFTSFeatures(tb.QueryTestCase):
    '''Tests for FTS features.

    This is intended to test the various FTS schema features.
    '''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'fts1.esdl')

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'fts_setup1.edgeql'
    )

    async def test_edgeql_fts_inheritance_01(self):
        # Test the fts search on a bunch of types that inherit from one
        # another.
        await self.assert_query_result(
            r'''
            select fts::search(
                Text,
                'rabbit run around the world',
                language := 'eng'
            ).object {
                text,
                type := .__type__.name,
            }
            ''',
            tb.bag(
                [
                    {'text': 'hello world', 'type': 'default::Text'},
                    {
                        'text': 'running and jumping fox',
                        'type': 'default::Text',
                    },
                    {
                        'text': 'the fox chases the rabbit',
                        'type': 'default::FancyQuotedText',
                    },
                    {
                        'text': 'the rabbit is fast',
                        'type': 'default::FancyQuotedText',
                    },
                    {'text': 'the world is big', 'type': 'default::QuotedText'},
                ]
            ),
        )

    async def test_edgeql_fts_inheritance_02(self):
        # Test the fts search on a bunch of types that inherit from one
        # another.
        await self.assert_query_result(
            r'''
            select fts::search(
                Text,
                'foxy world',
                language := 'eng'
            ).object {
                text,
                type := .__type__.name,
            }
            ''',
            tb.bag(
                [
                    {'text': 'hello world', 'type': 'default::Text'},
                    {
                        'text': 'elaborate and foxy',
                        'type': 'default::FancyText',
                    },
                    {'text': 'the world is big', 'type': 'default::QuotedText'},
                ]
            ),
        )

    async def test_edgeql_fts_inheritance_03(self):
        # Test the fts search on a bunch of types that inherit from one
        # another.
        await self.assert_query_result(
            r'''
            select fts::search(
                FancyText,
                'fancy chase',
                language := 'eng'
            ).object {
                text,
                type := .__type__.name,
            }
            ''',
            tb.bag(
                [
                    {'text': 'fancy hello', 'type': 'default::FancyText'},
                    {
                        'text': 'the fox chases the rabbit',
                        'type': 'default::FancyQuotedText',
                    },
                ]
            ),
        )

    async def test_edgeql_fts_inheritance_04(self):
        # Test the fts search on a bunch of types that inherit from one
        # another.
        await self.assert_query_result(
            r'''
            select fts::search(
                FancyQuotedText,
                'fancy chase',
                language := 'eng'
            ).object {
                text,
                type := .__type__.name,
            }
            ''',
            [
                {
                    'text': 'the fox chases the rabbit',
                    'type': 'default::FancyQuotedText',
                },
            ],
        )

    async def test_edgeql_fts_multifield_01(self):
        # Test the fts search on a several fields.
        await self.assert_query_result(
            r'''
            select fts::search(
                Post,
                'angry',
                language := 'eng'
            ).object {
                title,
                body,
            }
            ''',
            tb.bag(
                [
                    {
                        'title': 'angry reply',
                        'body': "No! Wrong! It's blue!",
                    },
                    {
                        'title': 'random stuff',
                        'body': 'angry giraffes',
                    },
                ]
            ),
        )

        await self.assert_query_result(
            r'''
            select fts::search(
                Post,
                'reply',
                language := 'eng'
            ).object {
                title,
                body,
            }
            ''',
            tb.bag(
                [
                    {
                        'title': 'angry reply',
                        'body': "No! Wrong! It's blue!",
                    },
                    {
                        'title': 'helpful reply',
                        'body': "That's Rayleigh scattering for you",
                    },
                ]
            ),
        )

        await self.assert_query_result(
            r'''
            select fts::search(
                Post,
                'sky',
                language := 'eng'
            ).object {
                title,
                body,
            }
            ''',
            [
                {
                    'title': 'first post',
                    'body': 'The sky is so red.',
                },
            ],
        )

    async def test_edgeql_fts_computed_01(self):
        # Test the fts search on a computed property.
        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                'red',
                language := 'eng'
            ).object {
                text,
            }
            ''',
            tb.bag(
                [
                    {'text': 'Item #1: red umbrella'},
                    {'text': 'Item #2: red and white candy cane'},
                ]
            ),
        )

        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                '2 or 3',
                language := 'eng'
            ).object {
                text,
            }
            ''',
            tb.bag(
                [
                    {'text': 'Item #2: red and white candy cane'},
                    {'text': 'Item #3: fancy pants'},
                ]
            ),
        )

        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                'item AND fancy',
                language := 'eng'
            ).object {
                text,
            }
            ''',
            [
                {'text': 'Item #3: fancy pants'},
            ],
        )

    async def test_edgeql_fts_complex_object(self):
        # object is a subquery
        await self.assert_query_result(
            r'''
            select fts::search(
                (select Description limit 10),
                'red',
                language := 'eng'
            ).object {
                text,
            }
            ''',
            tb.bag(
                [
                    {'text': 'Item #1: red umbrella'},
                    {'text': 'Item #2: red and white candy cane'},
                ]
            ),
        )

        # object is a subquery
        await self.assert_query_result(
            r'''
            select fts::search(
                (select Description filter .text like 'Item%'),
                'red',
                language := 'eng'
            ).object { text }
            ''',
            tb.bag(
                [
                    {'text': 'Item #1: red umbrella'},
                    {'text': 'Item #2: red and white candy cane'},
                ]
            ),
        )

        # object is a union
        await self.assert_query_result(
            r'''
            select fts::search(
                {
                    (select Description filter .text like 'Item%'),
                    Post,
                },
                'red',
                language := 'eng'
            ).object { __type__: { name }}
            ''',
            tb.bag(
                [
                    {'__type__': {'name': 'default::Post'}},
                    {'__type__': {'name': 'default::Description'}},
                    {'__type__': {'name': 'default::Description'}},
                ]
            ),
        )

        # object is an empty set
        await self.assert_query_result(
            r'''
            select fts::search(
                (select Description filter false),
                'red',
                language := 'eng'
            ).object { text }
            ''',
            [],
        )

    async def test_edgeql_fts_complex_query(self):
        # query is a subquery
        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                (select FancyText filter .style = 0 limit 1).text[0:5],
                language := 'eng'
            ).object { text }
            ''',
            tb.bag(
                [
                    {'text': 'Item #3: fancy pants'},
                ]
            ),
        )

        # query is an empty set
        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                <optional str>$0,
                language := 'eng'
            ).object { text }
            ''',
            [],
            variables=(None,),
        )

    async def test_edgeql_fts_complex_lang(self):
        # lang is an expression
        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                'pants',
                language := 'I can speak english fluently'[12:19]
            ).object { text }
            ''',
            tb.bag(
                [
                    {'text': 'Item #3: fancy pants'},
                ]
            ),
        )

        # query is an empty set, default to english
        await self.assert_query_result(
            r'''
            select fts::search(
                Description,
                'pants',
                language := <optional str>$0
            ).object { text }
            ''',
            tb.bag(
                [
                    {'text': 'Item #3: fancy pants'},
                ]
            ),
            variables=(None,),
        )

    async def test_edgeql_fts_weights(self):
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry',
                language := 'eng',
                weights := [0.0, 0.0, 0.0, 1.0]
            )
            select res.object.title
            order by res.score desc
            ''',
            ["angry reply", "random stuff"],
        )
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry',
                language := 'eng',
                weights := [0.0, 0.0, 1.0, 0.0]
            )
            select res.object.title
            order by res.score desc
            ''',
            ["random stuff", "angry reply"],
        )

    async def test_edgeql_fts_updating(self):
        # test that adding an fts index, existing objects are also indexed

        await self.con.execute(
            '''
            create type Doc1 {
                create required property x -> str;
            };
            insert Doc1 { x := 'hello world' };
            alter type Doc1 {
                create index fts::index on (
                    fts::with_options(.x, language := fts::Language.eng)
                );
            };
            '''
        )

        await self.assert_query_result(
            r'''
            select fts::search(Doc1, 'world', language := 'eng').object.x;
            ''',
            ['hello world'],
        )

    async def test_edgeql_fts_empty_fields(self):
        # test that adding an fts index, existing objects are also indexed

        await self.assert_query_result(
            r'''
            select fts::search(
                Post, 'no body', language := 'eng'
            ).object { title, body};
            ''',
            [{"title": "no body", "body": None}],
        )
