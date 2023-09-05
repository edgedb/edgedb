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

import decimal
import json
import os
import re
import textwrap
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLFTSSchema(tb.DDLTestCase):

    async def test_edgeql_fts_schema_inheritance_01(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        type Ordered {
                            required num: int64;
                        }

                        type Text extending Ordered {
                            required text: str;
                            # no FTS index
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {num := 0, text := 'hello world'}
            ''')

            # Without an index we simply expect no results.
            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'English'
                    ).object.num
                ''',
                [],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'English'
                    ).object.num
                ''',
                [],
            )

    async def test_edgeql_fts_schema_inheritance_02(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        type Ordered {
                            required num: int64;
                        }

                        type Text extending Ordered {
                            required text: str;
                            index fts::textsearch on (.text);
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {num := 0, text := 'hello world'}
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'English'
                    ).object.num
                ''',
                [0],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'English'
                    ).object.num
                ''',
                [0],
            )

    async def test_edgeql_fts_schema_inheritance_03(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        abstract type Ordered {
                            required num: int64;
                        }

                        type Text extending Ordered {
                            required text: str;
                            index fts::textsearch on (.text);
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {num := 0, text := 'hello world'}
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'English'
                    ).object.num
                ''',
                [0],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        Ordered, 'hello', language := 'English'
                    ).object.num
                ''',
                [0],
            )

    async def test_edgeql_fts_schema_inheritance_04(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        abstract type Text {
                            required text: str;
                            index fts::textsearch on (.text);
                        }

                        type FancyText extending Text {
                            required num: int64;
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert FancyText {num := 0, text := 'hello world'}
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'English'
                    ).object.text
                ''',
                ['hello world'],
            )

            await self.assert_query_result(
                '''
                    select fts::search(
                        FancyText, 'hello', language := 'English'
                    ).object.text
                ''',
                ['hello world'],
            )

    async def test_edgeql_fts_schema_inheritance_05(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        type Text {
                            required text: str;
                            index fts::textsearch on (.text);
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {text := 'hello world'}
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'hello', language := 'English'
                    ).object.text
                ''',
                ['hello world'],
            )

            await self.assert_query_result(
                '''
                    select count((
                        select fts::search(
                            Object, 'hello', language := 'English'
                        )
                    ))
                ''',
                [1],
            )

    async def test_edgeql_fts_schema_inheritance_06(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        abstract type Text {
                            required text: str;
                            index fts::textsearch on (.text);
                        }

                        type TitledText extending Text {
                            required title: str;
                            index fts::textsearch on ((.title, .text));
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {text := 'hello world'};
                insert TitledText {
                    title := 'farewell',
                    text := 'goodbye world'
                };
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        Text, 'world', language := 'English'
                    ).object.text
                ''',
                {'hello world', 'goodbye world'},
            )

    async def test_edgeql_fts_schema_union_01(self):
        async with self._run_and_rollback():
            await self.con.execute(r"""
                start migration to {
                    module default {
                        type Text {
                            required text: str;
                            index fts::textsearch on (.text);
                        }

                        type TitledText {
                            required title: str;
                            required text: str;
                            index fts::textsearch on ((.title, .text));
                        }
                    }
                };
                populate migration;
                commit migration;
            """)

            await self.con.execute('''
                insert Text {text := 'hello world'};
                insert TitledText {
                    title := 'farewell',
                    text := 'goodbye world'
                };
            ''')

            await self.assert_query_result(
                '''
                    select fts::search(
                        {Text, TitledText}, 'world', language := 'English'
                    ).object.text
                ''',
                {'hello world', 'goodbye world'},
            )
