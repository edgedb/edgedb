#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

from edb.testbase import server as tb

import edgedb


class TestEdgeQLFTSSchema(tb.DDLTestCase):
    '''Tests for fts schema mutations.'''

    async def test_edgeql_fts_schema_language_01(self):
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

    async def test_edgeql_fts_schema_language_02(self):
        # test that adding an fts index, existing objects are also indexed
        await self.con.execute(
            '''
            create type Doc {
                create required property x -> str;
            };
            insert Doc { x := 'hello world' };
            alter type Doc {
                create index fts::index on (
                    fts::with_options(.x, language := fts::Language.eng)
                );
            };
            '''
        )

        await self.assert_query_result(
            r'''
            select fts::search(Doc, 'world', language := 'eng').object.x;
            ''',
            ['hello world'],
        )

    async def test_edgeql_fts_schema_language_03(self):
        # Test adding an index to existing schema
        await self.con.execute(
            r"""
            start migration to {
                module default {
                    type Text {
                        required text0: str;
                        required text1: str;
                        required text2: str;
                        required text3: str;
                        required text4: str;
                        required text5: str;
                        required text6: str;
                    }
                }
            };
            populate migration;
            commit migration;
            """
        )

        await self.con.execute(
            '''
            alter type default::Text {
              create index fts::index on ((
                fts::with_options(.text0, language := fts::Language.eng),
                fts::with_options(.text1, language := fts::Language.eng),
                fts::with_options(.text2, language := fts::Language.eng),
                fts::with_options(.text3, language := fts::Language.eng),
                fts::with_options(.text4, language := fts::Language.eng),
                fts::with_options(.text5, language := fts::Language.eng),
                fts::with_options(.text6, language := fts::Language.eng),
              ));
            }
            '''
        )

        # The search is actually valid now, even though there's nothing
        # there.
        await self.assert_query_result(
            r'''
            select fts::search(
                Text,
                'something',
                language := 'eng'
            )
            ''',
            [],
        )

    async def test_edgeql_fts_schema_language_04(self):
        await self.con.execute(
            '''
            create type Text {
                create required property text: str;
                create index fts::index on (
                    fts::with_options(
                        .text,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A
                    )
                );
            };

            insert Text {text := 'Questo pane buonissimo'};
            '''
        )

        # No result in Italian because it's indexed in English
        await self.assert_query_result(
            r'''
            select fts::search(Text, 'pane', language := 'ita').object.text;
            ''',
            [],
        )
        await self.assert_query_result(
            r'''
            select fts::search(Text, 'pane', language := 'eng').object.text;
            ''',
            ['Questo pane buonissimo'],
        )

        # Change the language and try again
        await self.con.execute(
            '''
            alter type Text {
                drop index fts::index on (
                    fts::with_options(
                        .text,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A
                    )
                );
            };

            alter type Text {
                create index fts::index on (
                    fts::with_options(
                        .text,
                        language := fts::Language.ita,
                        weight_category := fts::Weight.A
                    )
                );
            };

            administer reindex(Text);
            '''
        )

        await self.assert_query_result(
            r'''
            select fts::search(Text, 'pane', language := 'ita').object.text;
            ''',
            ['Questo pane buonissimo'],
        )
        await self.assert_query_result(
            r'''
            select fts::search(Text, 'pane', language := 'eng').object.text;
            ''',
            [],
        )

    async def test_edgeql_fts_schema_language_05(self):
        # test that adding/removing an fts index via inheritance, existing
        # objects are also indexed accordingly
        await self.con.execute(
            r'''
            create type Doc {
                create required property x -> str;
            };
            insert Doc { x := 'hello world' };

            # move the property to a Base type
            start migration to {
                module default {
                    abstract type Base {
                        required property x -> str;
                    };
                    type Doc extending Base;
                }
            };
            populate migration;
            commit migration;

            # Add an index on the Base type
            alter type Base {
                create index fts::index on (
                    fts::with_options(.x, language := fts::Language.eng)
                );
            };
            '''
        )

        await self.assert_query_result(
            r'''
            select fts::search(Doc, 'world', language := 'eng').object.x;
            ''',
            ['hello world'],
        )

        # Remove the index
        await self.con.execute(
            r'''
            alter type Base {
                drop index fts::index on (
                    fts::with_options(.x, language := fts::Language.eng)
                );
            };
            '''
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r'std::fts::search\(\) requires an std::fts::index index',
        ):
            await self.con.execute(
                r'''
                select fts::search(Doc, 'world', language := 'eng').object.x;
                '''
            )

    async def test_edgeql_fts_schema_weight_01(self):
        # Add and then change the weights on a couple of properties.
        await self.con.execute(
            r'''
            create type Doc {
                create required property a -> str;
                create required property b -> str;
            };
            insert Doc {
                a := 'hello world',
                b := 'running fox',
            };
            insert Doc {
                a := 'goodbye everyone',
                b := 'sleepy fox',
            };

            alter type Doc {
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                ));
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng'
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "hello world", "b": "running fox"},
                {"a": "goodbye everyone", "b": "sleepy fox"},
            ],
        )

        # Give props distinc categories.
        await self.con.execute(
            r'''
            alter type Doc {
                drop index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                ));
            };

            alter type Doc {
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.B,
                    ),
                ));
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng',
                weights := [0.1, 1],
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "goodbye everyone", "b": "sleepy fox"},
                {"a": "hello world", "b": "running fox"},
            ],
        )

    async def test_edgeql_fts_schema_weight_02(self):
        # Change inherited weights
        await self.con.execute(
            r'''
            create abstract type Base {
                create required property a -> str;
                create required property b -> str;
            };
            create type Doc extending Base;
            insert Doc {
                a := 'hello world',
                b := 'running fox',
            };
            insert Doc {
                a := 'goodbye everyone',
                b := 'sleepy fox',
            };

            alter type Base {
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                ));
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng'
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "hello world", "b": "running fox"},
                {"a": "goodbye everyone", "b": "sleepy fox"},
            ],
        )

        # Give props distinc categories.
        await self.con.execute(
            r'''
            alter type Base {
                drop index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                ));
            };

            alter type Base {
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.B,
                    ),
                ));
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng',
                weights := [0.1, 1],
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "goodbye everyone", "b": "sleepy fox"},
                {"a": "hello world", "b": "running fox"},
            ],
        )

    async def test_edgeql_fts_schema_weight_03(self):
        # Override inherited weights
        await self.con.execute(
            r'''
            create abstract type Base {
                create required property a -> str;
                create required property b -> str;
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                ));
            };
            create type Doc extending Base;
            insert Doc {
                a := 'hello world',
                b := 'running fox',
            };
            insert Doc {
                a := 'goodbye everyone',
                b := 'sleepy fox',
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng',
                weights := [0.1, 1],
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "hello world", "b": "running fox"},
                {"a": "goodbye everyone", "b": "sleepy fox"},
            ],
        )

        # Override Doc to give props distinct categories.
        await self.con.execute(
            r'''
            alter type Doc {
                create index fts::index on ((
                    fts::with_options(
                        .a,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.A,
                    ),
                    fts::with_options(
                        .b,
                        language := fts::Language.eng,
                        weight_category := fts::Weight.B,
                    ),
                ));
            };
            '''
        )

        await self.assert_query_result(
            r'''
            with res := fts::search(
                Doc, 'hello world sleepy fox', language := 'eng',
                weights := [0.1, 1],
            )
            select res.object {
                a,
                b,
            } order by res.score desc;
            ''',
            [
                {"a": "goodbye everyone", "b": "sleepy fox"},
                {"a": "hello world", "b": "running fox"},
            ],
        )

    async def test_edgeql_fts_schema_fiddly_args_01(self):
        await self.con.execute(
            r'''
            create type Doc {
                create required property x -> str;
                create index fts::index on (
                    fts::with_options(
                        .x,
                        language := <fts::Language>('en'++'g'),
                        weight_category := (select fts::Weight.B),
                    )
                );
            };
            '''
        )

    async def test_edgeql_fts_schema_fiddly_args_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "std::fts::search weight_category must be a constant",
        ):
            await self.con.execute(
                r'''
                create type Doc {
                    create required property x -> str;
                    create index fts::index on (
                        fts::with_options(
                            .x,
                            language := fts::Language.eng,
                            weight_category := <fts::Weight>("AB"[0]),
                         )
                    );
                };
                '''
            )
