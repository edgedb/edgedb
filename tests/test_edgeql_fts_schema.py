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
    async def test_edgeql_fts_schema_inheritance_01(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        type Ordered {
                            required num: int64;
                            index fts::index on (());
                        }

                        type Text extending Ordered {
                            required text: str;
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {num := 0, text := 'hello world'}
                '''
            )

            # Empty index returns no results.
            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'eng'
                    ).object.num
                ''',
                [],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'eng'
                    ).object.num
                ''',
                [],
            )

    async def test_edgeql_fts_schema_inheritance_02(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        type Ordered {
                            required num: int64;
                            index fts::index on (());
                        }

                        type Text extending Ordered {
                            required text: str;
                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {num := 0, text := 'hello world'}
                '''
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'eng'
                    ).object.num
                ''',
                [0],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'eng'
                    ).object.num
                ''',
                [0],
            )

    async def test_edgeql_fts_schema_inheritance_03(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        abstract type Ordered {
                            required num: int64;
                            index fts::index on (());
                        }

                        type Text extending Ordered {
                            required text: str;
                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {num := 0, text := 'hello world'}
                '''
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'eng'
                    ).object.num
                ''',
                [0],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'eng'
                    ).object.num
                ''',
                [0],
            )

    async def test_edgeql_fts_schema_inheritance_04(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        abstract type Text {
                            required text: str;
                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }

                        type FancyText extending Text {
                            required num: int64;
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert FancyText {num := 0, text := 'hello world'}
                '''
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'eng'
                    ).object.text
                ''',
                ['hello world'],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        FancyText, 'hello', language := 'eng'
                    ).object.text
                ''',
                ['hello world'],
            )

    async def test_edgeql_fts_schema_inheritance_05(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        type Text {
                            required text: str;
                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {text := 'hello world'}
                '''
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'eng'
                    ).object.text
                ''',
                ['hello world'],
            )

            async with self.assertRaisesRegexTx(
                edgedb.InvalidReferenceError,
                r'fts::search requires an fts::index index',
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

    async def test_edgeql_fts_schema_inheritance_06(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        type Text {
                            required text: str;
                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }

                        type TitledText extending Text {
                            required title: str;
                            index fts::index on ((
                              fts::with_options(
                                .title,
                                language := fts::Language.eng
                              ),
                              fts::with_options(
                                .text,
                                language := fts::Language.eng
                              )
                            ));
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {text := 'hello world'};
                insert TitledText {
                    title := 'farewell',
                    text := 'goodbye world'
                };
                '''
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'world', language := 'eng'
                    ).object.text
                ''',
                {'hello world', 'goodbye world'},
            )

    async def test_edgeql_fts_schema_union_01(self):
        async with self._run_and_rollback():
            await self.con.execute(
                r"""
                start migration to {
                    module default {
                        abstract type Searchable {
                            index fts::index on (());
                        }

                        type Text extending Searchable {
                            required text: str;

                            index fts::index on (
                                fts::with_options(
                                    .text,
                                    language := fts::Language.eng
                                )
                            );
                        }

                        type TitledText extending Searchable {
                            required title: str;
                            required text: str;
                            index fts::index on ((
                              fts::with_options(
                                .title,
                                language := fts::Language.eng
                              ),
                              fts::with_options(
                                .text,
                                language := fts::Language.eng
                              )
                            ));
                        }
                    }
                };
                populate migration;
                commit migration;
                """
            )

            await self.con.execute(
                '''
                insert Text {text := 'hello world'};
                insert TitledText {
                    title := 'farewell',
                    text := 'goodbye world'
                };
                '''
            )

            await self.assert_query_result(
                '''
                    select count((
                        select fts::search(
                            Searchable, 'world', language := 'eng'
                        )
                    ))
                ''',
                {2},
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        {Text, TitledText}, 'world', language := 'eng'
                    ).object.text
                ''',
                {'hello world', 'goodbye world'},
            )

    async def test_edgeql_fts_schema_with_options(self):
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

        async with self._run_and_rollback():
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

        # async with self._run_and_rollback():
        #     await self.con.execute('''
        #         alter type default::Text {
        #           create index fts::index on (
        #             fts::with_options(
        #              (.text0, .text1, .text2, .text3, .text4, .text5, .text6),
        #               fts::Language.ISO_eng
        #             )
        #           );
        #         }
        #     ''')
