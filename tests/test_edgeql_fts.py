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
    '''Tests for std::fts::search.

    This is intended to test the FTS query language, result scoring as well as
    various FTS schema features.
    '''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'fts.esdl')

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas', 'fts_setup.edgeql'
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
                score := x.score,
            };
            ''',
            [
                {
                    # "hl":
                    # "<b>White</b> <b>Rabbit</b> returning, splendidly "
                    # "dressed, with a pair of <b>white</b> kid <b>gloves</b> "
                    # "in one hand",
                    "number": 8,
                    "score": 0.6037054,
                },
                {
                    # "hl":
                    # "<b>Rabbit</b>’s little <b>white</b> kid <b>gloves</b> "
                    # "while she was talking. “How _can_ I have done",
                    "number": 14,
                    "score": 0.4559453,
                },
                {
                    # "hl":
                    # "<b>Rabbit</b> actually _took a <b>watch</b> out of its "
                    # "waistcoat-pocket_, and looked at it, and then",
                    "number": 3,
                    "score": 0.40634018,
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

    async def test_edgeql_fts_inheritance_01(self):
        # Test the fts search on a bunch of types that inherit from one
        # another.
        await self.assert_query_result(
            r'''
            select fts::search(
                Text,
                'rabbit runs around the world',
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

    async def test_edgeql_fts_inheritance_05(self):
        # Empty index is inherited from Ordered, so it returns no results.
        await self.assert_query_result(
            r'''
            select Sentence.text;
            ''',
            ['This will not be indexed'],
        )

        await self.assert_query_result(
            r'''
            select fts::search(
                Sentence,
                'indexed',
                language := 'eng'
            ).object.text
            ''',
            [],
        )

    async def test_edgeql_fts_inheritance_06(self):
        # Search Ordered things (includes Chapter, Paragraph, Sentence). Only
        # Chapter and Paragraph actually have non-empty index.
        await self.assert_query_result(
            r'''
            select fts::search(
                Ordered,
                'pool indexed',
                language := 'eng'
            ).object.__type__.name
            ''',
            # We only care to validate which types got indexed
            {'default::Chapter', 'default::Paragraph'},
        )

        # Same results regardless of whether it is a union or a common
        # ancestor
        await self.assert_query_result(
            r'''
            with
                a := (
                    select fts::search(
                        Ordered,
                        'pool indexed',
                        language := 'eng'
                    ).object
                ),
                b := (
                    select fts::search(
                        {Chapter, Paragraph, Sentence},
                        'pool indexed',
                        language := 'eng'
                    ).object
                )
            select all(a in b)
            ''',
            {True},
        )

    async def test_edgeql_fts_inheritance_07(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r'std::fts::search\(\) requires an std::fts::index index',
        ):
            await self.con.execute(
                '''
                select count((
                    select fts::search(
                        Object, 'hello', language := 'eng'
                    )
                ))
                '''
            )

    async def test_edgeql_fts_inheritance_08(self):
        await self.assert_query_result(
            r'''
            select fts::search(
                Text,
                'big AND important',
                language := 'eng'
            ).object {
                text,
                [is TitledText].title,
            }
            ''',
            # We only care to validate which types got indexed
            [{
                'title': 'big',
                'text': 'important',
            }],
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

    async def test_edgeql_fts_complex_query_01(self):
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

    async def test_edgeql_fts_complex_query_02(self):
        # Use a property to provide the search query
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                Post.note,
                language := 'eng',
                weights := [0.5, 1.0],
            )
            select res.object {
                title,
                body,
                note,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    "title": "angry reply",
                    "body": "No! Wrong! It's blue!",
                    "note": "blue reply",
                    "score": 0.4559453,
                },
                {
                    "title": "random stuff",
                    "body": "angry giraffes",
                    "note": "random angry stuff",
                    "score": 0.40528473,
                },
                {
                    "title": "no body",
                    "body": None,
                    "note": "no body",
                    "score": 0.30396354,
                },
            ],
        )

    async def test_edgeql_fts_lang_01(self):
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

    async def test_edgeql_fts_lang_02(self):
        # Establish baseline with same weights for all languages
        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'eng',
                weights := [1, 1, 1],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            # English "pane" apparently doesn't clash with other languages
            [
                {
                    "eng": "The window pane is clear",
                    "fra": "La vitre est claire",
                    "ita": "Il vetro della finestra è chiaro",
                    "score": 0.6079271,
                }
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'fra',
                weights := [1, 1, 1],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            # In French and Italian "pane" get's hits in both languages
            # regardless of the search language (probably due to similar
            # stemming).
            [
                {
                    "eng": "This delicious bread",
                    "fra": "Ce délicieux pain",
                    "ita": "Questo pane buonissimo",
                    "score": 0.6079271,
                },
                {
                    "eng": "The breaded pork is nice",
                    "fra": "Le porc pané est bon",
                    "ita": "Il maiale impanato è buono",
                    "score": 0.6079271,
                },
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'ita',
                weights := [1, 1, 1],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            # In French and Italian "pane" get's hits in both languages
            # regardless of the search language (probably due to similar
            # stemming).
            [
                {
                    "eng": "This delicious bread",
                    "fra": "Ce délicieux pain",
                    "ita": "Questo pane buonissimo",
                    "score": 0.6079271,
                },
                {
                    "eng": "The breaded pork is nice",
                    "fra": "Le porc pané est bon",
                    "ita": "Il maiale impanato è buono",
                    "score": 0.6079271,
                },
            ]
        )

    async def test_edgeql_fts_lang_03(self):
        # Use weight categories to get only results in a given language.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'eng',
                weights := [1, 0, 0],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            # English "pane" apparently doesn't clash with other languages
            [
                {
                    "eng": "The window pane is clear",
                    "fra": "La vitre est claire",
                    "ita": "Il vetro della finestra è chiaro",
                    "score": 0.6079271,
                }
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'fra',
                weights := [0, 1, 0],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    "eng": "The breaded pork is nice",
                    "fra": "Le porc pané est bon",
                    "ita": "Il maiale impanato è buono",
                    "score": 0.6079271,
                },
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'ita',
                weights := [0, 0, 1],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    "eng": "This delicious bread",
                    "fra": "Ce délicieux pain",
                    "ita": "Questo pane buonissimo",
                    "score": 0.6079271,
                },
            ]
        )

    async def test_edgeql_fts_lang_04(self):
        # Use weight categories to get only results in a given language.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                MultiLang,
                'pane',
                language := 'eng',
                weights := [1, 0, 0],
            )
            select res.object {
                eng,
                fra,
                ita,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            # English "pane" apparently doesn't clash with other languages
            [
                {
                    "eng": "The window pane is clear",
                    "fra": "La vitre est claire",
                    "ita": "Il vetro della finestra è chiaro",
                    "score": 0.6079271,
                }
            ]
        )

    async def test_edgeql_fts_lang_05(self):
        # Use a property to define the language.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                DynamicLang,
                'clear pane',
                language := <str>DynamicLang.lang,
            )
            select res.object {
                text,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {"text": "The window pane is clear", "score": 0.6079271},
                {"text": "Questo pane buonissimo", "score": 0.30396354},
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                DynamicLang,
                'pain gain',
                language := <str>DynamicLang.lang,
            )
            select res.object {
                text,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {"text": "No pain no gain", "score": 0.6079271},
                {"text": "Ce délicieux pain", "score": 0.30396354},
            ]
        )

    async def test_edgeql_fts_lang_06(self):
        # A single property has mixed language.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                TouristVocab,
                'pane',
                language := 'eng',
                weights := [1, 0],
            )
            select res.object.text
            filter res.score > 0
            order by res.score desc
            ''',
            [
                "The window pane is clear -- "
                "Il vetro della finestra è chiaro"
            ]
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                TouristVocab,
                'pane',
                language := 'ita',
                weights := [0, 1],
            )
            select res.object.text
            filter res.score > 0
            order by res.score desc
            ''',
            [
                "This delicious bread -- "
                "Questo pane buonissimo"
            ]
        )

    async def test_edgeql_fts_weights_01(self):
        # Use weights to search only one of the properties.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry',
                language := 'eng',
                weights := [1.0, 0.0]
            )
            select res.object.title
            filter res.score > 0
            order by res.score desc
            ''',
            ["angry reply"],
        )
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry',
                language := 'eng',
                weights := [0.0, 1.0]
            )
            select res.object.title
            filter res.score > 0
            order by res.score desc
            ''',
            ["random stuff"],
        )

    async def test_edgeql_fts_weights_02(self):
        # Use weights to change search priority: default weights
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry replying giraffes',
                language := 'eng',
            )
            select res.object {
                title,
                body,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    'title': 'angry reply',
                    'body': 'No! Wrong! It\'s blue!',
                    'score': 0.40528473
                },
                {
                    'title': 'helpful reply',
                    'body': 'That\'s Rayleigh scattering for you',
                    'score': 0.20264237
                },
                {
                    'title': 'random stuff',
                    'body': 'angry giraffes',
                    'score': 0.20264237
                },
            ],
        )

    async def test_edgeql_fts_weights_03(self):
        # Use weights to change search priority: incomplete weights
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry replying giraffes',
                language := 'eng',
                # weights get padded with 0, so B category is not searched
                weights := [1.0],
            )
            select res.object {
                title,
                body,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    'title': 'angry reply',
                    'body': 'No! Wrong! It\'s blue!',
                    'score': 0.40528473,
                },
                {
                    'title': 'helpful reply',
                    'body': 'That\'s Rayleigh scattering for you',
                    'score': 0.20264237,
                },
            ],
        )

    async def test_edgeql_fts_weights_04(self):
        # Use weights to change search priority: custom weights
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'angry replying giraffes',
                language := 'eng',
                weights := [0.8, 1.0],
            )
            select res.object {
                title,
                body,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    'title': 'random stuff',
                    'body': 'angry giraffes',
                    'score': 0.40528473,
                },
                {
                    'title': 'angry reply',
                    'body': 'No! Wrong! It\'s blue!',
                    'score': 0.32422778,
                },
                {
                    'title': 'helpful reply',
                    'body': 'That\'s Rayleigh scattering for you',
                    'score': 0.16211389,
                },
            ],
        )

    async def test_edgeql_fts_weights_05(self):
        # Use weights to change search priority: weights from property
        await self.assert_query_result(
            r'''
            with res := fts::search(
                Post,
                'random angry replying giraffes',
                language := 'eng',
                # Note that if the weight property is {}, the default weights
                # will be used.
                weights := [Post.weight_a, 0.7],
            )
            select res.object {
                title,
                body,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    'title': 'angry reply',
                    'body': 'No! Wrong! It\'s blue!',
                    'score': 0.30396354,
                },
                {
                    'title': 'random stuff',
                    'body': 'angry giraffes',
                    'score': 0.22797266,
                },
            ],
        )

    async def test_edgeql_fts_weights_06(self):
        # Use weights to change search priority: custom weights across
        # multiple types.
        await self.assert_query_result(
            r'''
            with res := fts::search(
                {Post, Description},
                'red white blue post',
                language := 'eng',
                weights := [0.5, 1.0, 0.7],
            )
            select res.object {
                [is Post].title,
                [is Post].body,
                [is Description].text,
                score := res.score
            }
            filter res.score > 0
            order by res.score desc
            ''',
            [
                {
                    "title": "first post",
                    "body": "The sky is so red.",
                    "text": None,
                    "score": 0.22797266,
                },
                {
                    "title": None,
                    "body": None,
                    "text": "Item #2: red and white candy cane",
                    "score": 0.21277449,
                },
                {
                    "title": "angry reply",
                    "body": "No! Wrong! It's blue!",
                    "text": None,
                    "score": 0.15198177,
                },
                {
                    "title": None,
                    "body": None,
                    "text": "Item #1: red umbrella",
                    "score": 0.10638724,
                },
            ],
        )

    async def test_edgeql_fts_empty_fields(self):
        await self.assert_query_result(
            r'''
            select fts::search(
                Post, 'no body', language := 'eng'
            ).object { title, body};
            ''',
            [{"title": "no body", "body": None}],
        )

    async def test_edgeql_fts_links(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "fts::index cannot be declared on links",
        ):
            await self.con.execute(
                '''
                create type Doc1 {
                    create link x -> schema::ObjectType {
                        create property y -> str;

                        create index fts::index on (
                            fts::with_options(@y, language := fts::Language.eng)
                        );
                    }
                };
                '''
            )
