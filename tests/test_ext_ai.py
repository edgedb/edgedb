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


import json
import pathlib

from edb.testbase import http as tb


class TestExtAI(tb.BaseHttpExtensionTest):
    EXTENSIONS = ['pgvector', 'ai']
    BACKEND_SUPERUSER = True
    TRANSACTION_ISOLATION = False
    PARALLELISM_GRANULARITY = 'suite'

    SCHEMA = pathlib.Path(__file__).parent / 'schemas' / 'ext_ai.esdl'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mock_server = tb.MockHttpServer()
        cls.mock_server.start()
        base_url = cls.mock_server.get_base_url().rstrip("/")

        cls.mock_server.register_route_handler(
            "POST",
            base_url,
            "/v1/embeddings",
        )(cls.mock_api_embeddings)

        async def _setup():
            await cls.con.execute(
                f"""
                CONFIGURE CURRENT DATABASE
                INSERT ext::ai::CustomProviderConfig {{
                    name := 'custom::test',
                    secret := 'very secret',
                    api_url := '{base_url}/v1',
                    api_style := ext::ai::ProviderAPIStyle.OpenAI,
                }};

                CONFIGURE CURRENT DATABASE
                    SET ext::ai::Config::indexer_naptime := <duration>'100ms';
                """,
            )

            await cls._wait_for_db_config('ext::ai::Config::providers')

        cls.loop.run_until_complete(_setup())

    @classmethod
    def tearDownClass(cls):
        cls.mock_server.stop()

    @classmethod
    def get_setup_script(cls):
        res = super().get_setup_script()

        # HACK: As a debugging cycle hack, when RELOAD is true, we reload the
        # extension package from the file, so we can test without a bootstrap.
        RELOAD = False

        if RELOAD:
            root = pathlib.Path(__file__).parent.parent
            with open(root / 'edb/lib/ext/ai.edgeql') as f:
                contents = f.read()
            to_add = (
                '''
                drop extension package ai version '1.0';
                create extension ai;
            '''
                + contents
            )
            splice = '__internal_testmode := true;'
            res = res.replace(splice, splice + to_add)

        return res

    @classmethod
    def mock_api_embeddings(
        cls,
        handler: tb.MockHttpServerHandler,
    ) -> tb.ResponseType:
        return (
            json.dumps({
                "object": "list",
                "data": [{
                    "object": "embedding",
                    "index": 0,
                    "embedding": [
                        1.0,
                        -2.0,
                        3.0,
                        -4.0,
                        5.0,
                        -6.0,
                        7.0,
                        -8.0,
                        9.0,
                        -10.0,
                    ],
                }],
            }),
            200,
        )

    async def test_ext_ai_indexing_01(self):
        await self.con.execute(
            """
            insert Astronomy {
                content := 'Skies on Mars are red'
            };
            insert Astronomy {
                content := 'Skies on Earth are blue'
            };
            """,
        )

        async for tr in self.try_until_succeeds(
            ignore=(AssertionError,),
            timeout=10.0,
        ):
            async with tr:
                await self.assert_query_result(
                    r'''
                    with
                        result := ext::ai::search(
                            Astronomy, <array<float32>>$qv)
                    select
                        result.object {
                            content,
                            distance := result.distance,
                        }
                    order by
                        result.distance asc empty last
                        then result.object.content
                    ''',
                    [
                        {
                            'content': 'Skies on Earth are blue',
                            'distance': 0,
                        },
                        {
                            'content': 'Skies on Mars are red',
                            'distance': 0,
                        },
                    ],
                    variables={
                        "qv": [
                            1.0,
                            -2.0,
                            3.0,
                            -4.0,
                            5.0,
                            -6.0,
                            7.0,
                            -8.0,
                            9.0,
                            -10.0,
                        ],
                    }
                )

    async def test_ext_ai_indexing_02(self):
        qry = '''
            with
                result := ext::ai::search(
                    Stuff, <array<float32>>$qv)
            select
                result.object {
                    content,
                    content2,
                    distance := result.distance,
                }
            order by
                result.distance asc empty last
                then result.object.content;
        '''
        qv = [1.0, -2.0, 3.0, -4.0, 5.0, -6.0, 7.0, -8.0, 9.0, -10.0]

        await self.assert_query_result(
            """
            insert Stuff {
                content := 'Skies on Mars',
                content2 := ' are red',
            };
            insert Stuff {
                content := 'Skies on Earth',
                content2 := ' are blue',
            };
            """ + qry,
            [],
            variables=dict(qv=qv),
        )

        await self.assert_query_result(
            '''
            select _ := ext::ai::to_context((select Stuff))
            order by _
            ''',
            [
                'Skies on Earth are blue',
                'Skies on Mars are red',
            ],
        )

        async for tr in self.try_until_succeeds(
            ignore=(AssertionError,),
            timeout=10.0,
        ):
            async with tr:
                await self.assert_query_result(
                    qry,
                    [
                        {
                            'content': 'Skies on Earth',
                            'content2': ' are blue',
                            'distance': 0,
                        },
                        {
                            'content': 'Skies on Mars',
                            'content2': ' are red',
                            'distance': 0,
                        },
                    ],
                    variables=dict(qv=qv),
                )

        # updating an object should make it disappear from results.
        # (the read is done in the same tx, so there is no possible
        # race where the worker picks it up before the read)
        await self.assert_query_result(
            """
            update Stuff filter .content like '%Earth'
            set { content2 := ' are often grey' };
            """ + qry,
            [
                {
                    'content': 'Skies on Mars',
                    'content2': ' are red',
                    'distance': 0,
                },
            ],
            variables=dict(qv=qv),
        )

    async def test_ext_ai_indexing_03(self):
        await self.con.execute(
            """
            insert Star {
                content := 'Skies on Mars are red'
            };
            insert Supernova {
                content := 'Skies on Earth are blue'
            };
            """,
        )

        await self.assert_query_result(
            '''
            select _ := ext::ai::to_context((select Star))
            order by _
            ''',
            [
                'Skies on Earth are blue',
                'Skies on Mars are red',
            ],
        )

        async for tr in self.try_until_succeeds(
            ignore=(AssertionError,),
            timeout=10.0,
        ):
            async with tr:
                await self.assert_query_result(
                    r'''
                    with
                        result := ext::ai::search(
                            Star, <array<float32>>$qv)
                    select
                        result.object {
                            content,
                            distance := result.distance,
                        }
                    order by
                        result.distance asc empty last
                        then result.object.content
                    ''',
                    [
                        {
                            'content': 'Skies on Earth are blue',
                            'distance': 0,
                        },
                        {
                            'content': 'Skies on Mars are red',
                            'distance': 0,
                        },
                    ],
                    variables={
                        "qv": [
                            1.0,
                            -2.0,
                            3.0,
                            -4.0,
                            5.0,
                            -6.0,
                            7.0,
                            -8.0,
                            9.0,
                            -10.0,
                        ],
                    }
                )

    async def _assert_index_use(self, query, *args):
        def look(obj):
            if isinstance(obj, dict) and obj.get('plan_type') == "IndexScan":
                return any(
                    prop['title'] == 'index_name'
                    and f'ai::index' in prop['value']
                    for prop in obj.get('properties', [])
                )

            if isinstance(obj, dict):
                return any([look(v) for v in obj.values()])
            elif isinstance(obj, list):
                return any(look(v) for v in obj)
            else:
                return False

        async with self._run_and_rollback():
            await self.con.execute('select _set_seqscan("off");')
            plan = await self.con.query_json(f'analyze {query};', *args)
        if not look(json.loads(plan)):
            raise AssertionError(f'query did not use ext::ai::index index')

    async def test_ext_ai_indexing_04(self):
        qv = [1.0, -2.0, 3.0, -4.0, 5.0, -6.0, 7.0, -8.0, 9.0, -10.0]

        await self._assert_index_use(
            f'''
            with vector := <array<float32>>$0
            select ext::ai::search(Stuff, vector) limit 5;
            ''',
            qv,
        )
        await self._assert_index_use(
            f'''
            with vector := <array<float32>>$0
            select ext::ai::search(Stuff, vector).object limit 5;
            ''',
            qv,
        )
        await self._assert_index_use(
            f'''
            select ext::ai::search(Stuff, <array<float32>>$0) limit 5;
            ''',
            qv,
        )

        await self._assert_index_use(
            f'''
            with vector := <array<float32>><json>$0
            select ext::ai::search(Stuff, vector) limit 5;
            ''',
            json.dumps(qv),
        )
        await self._assert_index_use(
            f'''
            select ext::ai::search(Stuff, <array<float32>><json>$0) limit 5;
            ''',
            json.dumps(qv),
        )
