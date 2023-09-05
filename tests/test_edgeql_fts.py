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

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'fts0.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'fts_setup0.edgeql')

    async def test_edgeql_fts_search_01(self):
        # At least one of "drink" or "poison" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink poison',
                language := 'English'
            ).object {
                number,
                text,
            };
            ''',
            tb.bag([{
                "number": 15,
                "text":
                    "There seemed to be no use in waiting by the little "
                    "door, so she went back to the table, half hoping she "
                    "might find another key on it, or at any rate a book of "
                    "rules for shutting people up like telescopes: this "
                    "time she found a little bottle on it, (“which "
                    "certainly was not here before,” said Alice,) and round "
                    "the neck of the bottle was a paper label, with the "
                    "words “DRINK ME,” beautifully printed on it in large "
                    "letters."
            }, {
                "number": 16,
                "text":
                    "It was all very well to say “Drink me,” but the wise "
                    "little Alice was not going to do _that_ in a hurry. "
                    "“No, I’ll look first,” she said, “and see whether it’s "
                    "marked ‘_poison_’ or not”; for she had read several "
                    "nice little histories about children who had got "
                    "burnt, and eaten up by wild beasts and other "
                    "unpleasant things, all because they _would_ not "
                    "remember the simple rules their friends had taught "
                    "them: such as, that a red-hot poker will burn you if "
                    "you hold it too long; and that if you cut your finger "
                    "_very_ deeply with a knife, it usually bleeds; and she "
                    "had never forgotten that, if you drink much from a "
                    "bottle marked “poison,” it is almost certain to "
                    "disagree with you, sooner or later."
            }, {
                "number": 17,
                "text":
                    "However, this bottle was _not_ marked “poison,” so "
                    "Alice ventured to taste it, and finding it very nice, "
                    "(it had, in fact, a sort of mixed flavour of "
                    "cherry-tart, custard, pine-apple, roast turkey, "
                    "toffee, and hot buttered toast,) she very soon "
                    "finished it off."
            }])
        )

    async def test_edgeql_fts_search_02(self):
        # At least one of "drink" or "me" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink me',
                language := 'English'
            ).object {
                number,
                text,
            };
            ''',
            tb.bag([{
                "number": 15,
                "text":
                    "There seemed to be no use in waiting by the little "
                    "door, so she went back to the table, half hoping she "
                    "might find another key on it, or at any rate a book of "
                    "rules for shutting people up like telescopes: this "
                    "time she found a little bottle on it, (“which "
                    "certainly was not here before,” said Alice,) and round "
                    "the neck of the bottle was a paper label, with the "
                    "words “DRINK ME,” beautifully printed on it in large "
                    "letters."
            }, {
                "number": 16,
                "text":
                    "It was all very well to say “Drink me,” but the wise "
                    "little Alice was not going to do _that_ in a hurry. "
                    "“No, I’ll look first,” she said, “and see whether it’s "
                    "marked ‘_poison_’ or not”; for she had read several "
                    "nice little histories about children who had got "
                    "burnt, and eaten up by wild beasts and other "
                    "unpleasant things, all because they _would_ not "
                    "remember the simple rules their friends had taught "
                    "them: such as, that a red-hot poker will burn you if "
                    "you hold it too long; and that if you cut your finger "
                    "_very_ deeply with a knife, it usually bleeds; and she "
                    "had never forgotten that, if you drink much from a "
                    "bottle marked “poison,” it is almost certain to "
                    "disagree with you, sooner or later."
            }])
        )

    async def test_edgeql_fts_search_03(self):
        # Both "drink" and "poison" should appear in text.
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink AND poison',
                language := 'English'
            ).object {
                number,
                text,
            };
            ''',
            [{
                "number": 16,
                "text":
                    "It was all very well to say “Drink me,” but the wise "
                    "little Alice was not going to do _that_ in a hurry. "
                    "“No, I’ll look first,” she said, “and see whether it’s "
                    "marked ‘_poison_’ or not”; for she had read several "
                    "nice little histories about children who had got "
                    "burnt, and eaten up by wild beasts and other "
                    "unpleasant things, all because they _would_ not "
                    "remember the simple rules their friends had taught "
                    "them: such as, that a red-hot poker will burn you if "
                    "you hold it too long; and that if you cut your finger "
                    "_very_ deeply with a knife, it usually bleeds; and she "
                    "had never forgotten that, if you drink much from a "
                    "bottle marked “poison,” it is almost certain to "
                    "disagree with you, sooner or later."
            }]
        )

        # Same sematics as above
        await self.assert_query_result(
            r'''
            select fts::search(
                Paragraph,
                'drink "poison"',
                language := 'English'
            ).object {
                number,
                text,
            };
            ''',
            [{
                "number": 16,
                "text":
                    "It was all very well to say “Drink me,” but the wise "
                    "little Alice was not going to do _that_ in a hurry. "
                    "“No, I’ll look first,” she said, “and see whether it’s "
                    "marked ‘_poison_’ or not”; for she had read several "
                    "nice little histories about children who had got "
                    "burnt, and eaten up by wild beasts and other "
                    "unpleasant things, all because they _would_ not "
                    "remember the simple rules their friends had taught "
                    "them: such as, that a red-hot poker will burn you if "
                    "you hold it too long; and that if you cut your finger "
                    "_very_ deeply with a knife, it usually bleeds; and she "
                    "had never forgotten that, if you drink much from a "
                    "bottle marked “poison,” it is almost certain to "
                    "disagree with you, sooner or later."
            }]
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
                    language := 'English'
                )
                order by .score desc
                limit 3
            )
            select x.object {
                number,
                rank := x.score,
            };
            ''',
            [{
                # "hl":
                #     "<b>White</b> <b>Rabbit</b> returning, splendidly "
                #     "dressed, with a pair of <b>white</b> kid <b>gloves</b> "
                #     "in one hand",
                "number": 8,
                "rank": 0.6037054,
            }, {
                # "hl":
                #     "<b>Rabbit</b>’s little <b>white</b> kid <b>gloves</b> "
                #     "while she was talking. “How _can_ I have done",
                "number": 14,
                "rank": 0.4559453,
            }, {
                # "hl":
                #     "<b>Rabbit</b> actually _took a <b>watch</b> out of its "
                #     "waistcoat-pocket_, and looked at it, and then",
                "number": 3,
                "rank": 0.40634018,
            }]
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
                    language := 'English'
                )
                order by .score desc
                limit 3
            )
            select x.object {
                number,
                rank := x.score,
            };
            ''',
            [{
                # "hl":
                #     "<b>White</b> <b>Rabbit</b> returning, splendidly "
                #     "dressed, with a pair of <b>white</b> kid gloves in one "
                #     "hand",
                "number": 8,
                "rank": 0.41372818,
            }, {
                # "hl":
                #     "<b>golden</b> <b>key</b>, and Alice’s first thought "
                #     "was that it might belong to one of the doors",
                "number": 13,
                "rank": 0.39684132,
            }, {
                # "hl":
                #     "<b>White</b> <b>Rabbit</b> was still in sight, "
                #     "hurrying down it. There was not a moment to be lost",
                "number": 11,
                "rank": 0.341959,
            }]
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
                    language := 'English'
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
            [{
                "ch": 1,
                "number": 16,
                "rank": 0.8530858,
            }]
        )

    async def test_edgeql_fts_language_01(self):
        await self.con.execute(
            '''
            type MyLangs extends Enum<English, PigLatin, Esperanto>;

            create type Doc1 {
                create required property x -> str;

                create index fts::textsearch on (
                    fts::with_language(.x, MyLangs.English)
                );
            }
            create type Doc2 {
                create required property x -> str;
            }
            '''
        )

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "`piglatin`, `esperanto`",
        ):
            # In this case, language is not a constant, so we fallback to all
            # possible values of the enum. This then fails because some of them
            # are not supported by postgres.
            await self.con.execute("""
                alter type Doc2 create index fts::textsearch on (
                    fts::with_language(.x,
                        MyLangs.English if .x == 'blah' else MyLangs.PigLatin
                    )
                );
            """)
