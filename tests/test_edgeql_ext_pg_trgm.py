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


import os.path

from edb.testbase import server as tb


class TestEdgeQLExtPgTrgm(tb.QueryTestCase):
    EXTENSIONS = ['pg_trgm']
    BACKEND_SUPERUSER = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'pg_trgm.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'pg_trgm_setup.edgeql')

    async def test_edgeql_ext_pg_trgm_similarity(self):
        await self.assert_query_result(
            """
            SELECT
                Gist {
                    p_str,
                    sim := ext::pg_trgm::similarity(.p_str, "qwertyu0988")
                }
            FILTER
                ext::pg_trgm::similar(.p_str, "qwertyu0988")
            ORDER BY
                .sim DESC
                THEN .p_str
            LIMIT
                11
            """,
            [
                {
                    "p_str": "qwertyu0988",
                    "sim": 1.0,
                },
                {
                    "p_str": "qwertyu0980",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0981",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0982",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0983",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0984",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0985",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0986",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0987",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0989",
                    "sim": 0.714286,
                },
                {
                    "p_str": "qwertyu0088",
                    "sim": 0.6,
                },
            ]
        )

        qry = """
            SELECT
                Gist {
                    p_str,
                    sim_dist := ext::pg_trgm::similarity_dist(
                        .p_str, "q0987wertyu0988"
                    )
                }
            ORDER BY
                .sim_dist EMPTY LAST
            LIMIT
                2
        """

        await self.assert_query_result(
            qry,
            [
                {
                    "p_str": "qwertyu0988",
                    "sim_dist": 0.411765,
                },
                {
                    "p_str": "qwertyu0987",
                    "sim_dist": 0.5,
                },
            ]
        )

        await self.assert_index_use(
            qry,
            index_type="ext::pg_trgm::gist",
        )

        qry = """
            SELECT
                Gist2 {
                    p_str,
                    sim_dist := ext::pg_trgm::similarity_dist(
                        .p_str, "q0987wertyu0988"
                    ),
                    p_str_2,
                    sim_dist_2 := ext::pg_trgm::similarity_dist(
                        .p_str_2, "q0987opasdf0988"
                    ),
                }
            ORDER BY
                .sim_dist EMPTY LAST THEN .sim_dist_2 EMPTY LAST
            LIMIT
                2
        """

        await self.assert_query_result(
            qry,
            [
                {
                    "p_str": "qwertyu0988",
                    "sim_dist": 0.411765,
                    "p_str_2": "iopasdf0988",
                    "sim_dist_2": 0.5,
                },
                {
                    "p_str": "qwertyu0987",
                    "sim_dist": 0.5,
                    "p_str_2": "iopasdf0987",
                    "sim_dist_2": 0.57894737,
                },
            ]
        )

        await self.assert_index_use(
            qry,
            index_type="ext::pg_trgm::gist",
        )

    async def test_edgeql_ext_pg_trgm_word_similarity(self):
        await self.assert_query_result(
            """
            SELECT
                Gist {
                    p_str,
                    sim := ext::pg_trgm::word_similarity("Kabankala", .p_str)
                }
            FILTER
                ext::pg_trgm::word_similar("Kabankala", .p_str)
            ORDER BY
                .sim DESC
                THEN .p_str
            """,
            [
                {
                    "p_str": "Kabankala",
                    "sim": 1.0,
                },
                {
                    "p_str": "Kabankalan City Public Plaza",
                    "sim": 0.9,
                },
                {
                    "p_str": "Abankala",
                    "sim": 0.7,
                },
                {
                    "p_str": "Ntombankala School",
                    "sim": 0.6,
                },
            ]
        )

        qry = """
            SELECT
                Gist {
                    p_str,
                    word_sim_dist := ext::pg_trgm::word_similarity_dist(
                        "Kabankala", .p_str
                    )
                }
            ORDER BY
                .word_sim_dist EMPTY LAST
            LIMIT
                7
        """

        await self.assert_query_result(
            qry,
            [
                {
                    "p_str": "Kabankala",
                    "word_sim_dist": 0.0,
                },
                {
                    "p_str": "Kabankalan City Public Plaza",
                    "word_sim_dist": 0.1,
                },
                {
                    "p_str": "Abankala",
                    "word_sim_dist": 0.3,
                },
                {
                    "p_str": "Ntombankala School",
                    "word_sim_dist": 0.4,
                },
                {
                    "p_str": "Kabakala",
                    "word_sim_dist": 0.416667,
                },
                {
                    "p_str": "Nehalla Bankalah Reserved Forest",
                    "word_sim_dist": 0.5,
                },
                {
                    "p_str": "Kabikala",
                    "word_sim_dist": 0.538462,
                },
            ]
        )

        await self.assert_index_use(
            qry,
            index_type="ext::pg_trgm::gist",
        )

        qry = """
            SELECT
                Gist2 {
                    p_str,
                    word_sim_dist := ext::pg_trgm::word_similarity_dist(
                        "Kabankala", .p_str
                    ),
                    p_str_2,
                    word_sim_dist_2 := ext::pg_trgm::word_similarity_dist(
                        "Pub", .p_str_2
                    )
                }
            ORDER BY
                .word_sim_dist EMPTY LAST THEN .word_sim_dist_2 EMPTY LAST
            LIMIT
                2
        """

        await self.assert_query_result(
            qry,
            [
                {
                    "p_str": "Kabankala",
                    "word_sim_dist": 0.0,
                },
                {
                    "p_str": "Kabankalan City Public Plaza",
                    "word_sim_dist": 0.1,
                },
            ]
        )

        await self.assert_index_use(
            qry,
            index_type="ext::pg_trgm::gist",
        )

    async def test_edgeql_ext_pg_trgm_strict_word_similarity(self):
        await self.assert_query_result(
            """
            SELECT
                Gist {
                    p_str,
                    sim := ext::pg_trgm::strict_word_similarity(
                        "Kabankala", .p_str
                    )
                }
            FILTER
                ext::pg_trgm::strict_word_similar("Kabankala", .p_str)
            ORDER BY
                .sim DESC
                THEN .p_str
            """,
            [
                {
                    "p_str": "Kabankala",
                    "sim": 1.0,
                },
                {
                    "p_str": "Kabankalan City Public Plaza",
                    "sim": 0.75,
                },
                {
                    "p_str": "Abankala",
                    "sim": 0.583333,
                },
                {
                    "p_str": "Kabakala",
                    "sim": 0.583333,
                },
            ]
        )

        qry = """
            SELECT
                Gist {
                    p_str,
                    word_sim_dist := ext::pg_trgm::strict_word_similarity_dist(
                        "Alaikallupoddakulam", .p_str
                    )
                }
            ORDER BY
                .word_sim_dist EMPTY LAST
            LIMIT
                7
        """

        await self.assert_query_result(
            qry,
            [
                {
                    "p_str": "Alaikallupoddakulam",
                    "word_sim_dist": 0.0,
                },
                {
                    "p_str": "Alaikallupodda Alankulam",
                    "word_sim_dist": 0.25,
                },
                {
                    "p_str": "Alaikalluppodda Kulam",
                    "word_sim_dist": 0.32,
                },
                {
                    "p_str": "Mulaikallu Kulam",
                    "word_sim_dist": 0.615385,
                },
                {
                    "p_str": "Koraikalapu Kulam",
                    "word_sim_dist": 0.724138,
                },
                {
                    "p_str": "Vaikaliththevakulam",
                    "word_sim_dist": 0.75,
                },
                {
                    "p_str": "Karaivaikal Kulam",
                    "word_sim_dist": 0.766667,
                },
            ]
        )

        await self.assert_index_use(
            qry,
            index_type="ext::pg_trgm::gist",
        )

    async def test_edgeql_ext_pg_trgm_config(self):
        # We are going to fiddle with the similarity_threshold config
        # and make sure it works right.

        sim_query = """
            WITH similar := (
                SELECT
                    Gist {
                        p_str,
                        sim := ext::pg_trgm::similarity(.p_str, "qwertyu0988")
                    }
                FILTER
                    ext::pg_trgm::similar(.p_str, "qwertyu0988")
            ),
            SELECT exists similar and all(similar.sim >= <float32>$sim)
        """

        cfg_query = """
            select cfg::Config.extensions[is ext::pg_trgm::Config]
            .similarity_threshold;
        """

        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.3),
        )
        await self.assert_query_result(
            sim_query,
            [False],
            variables=dict(sim=0.5),
        )
        await self.assert_query_result(
            sim_query,
            [False],
            variables=dict(sim=0.9),
        )

        await self.assert_query_result(
            cfg_query,
            [0.3],
        )

        await self.con.execute('''
            configure session
            set ext::pg_trgm::Config::similarity_threshold := 0.5
        ''')

        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.3),
        )
        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.5),
        )
        await self.assert_query_result(
            sim_query,
            [False],
            variables=dict(sim=0.9),
        )
        await self.assert_query_result(
            cfg_query,
            [0.5],
        )

        await self.con.execute('''
            configure session
            set ext::pg_trgm::Config::similarity_threshold := 0.9
        ''')

        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.3),
        )
        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.5),
        )
        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.9),
        )
        await self.assert_query_result(
            cfg_query,
            [0.9],
        )

        await self.con.execute('''
            configure session
            reset ext::pg_trgm::Config::similarity_threshold
        ''')

        await self.assert_query_result(
            sim_query,
            [True],
            variables=dict(sim=0.3),
        )
        await self.assert_query_result(
            sim_query,
            [False],
            variables=dict(sim=0.5),
        )
        await self.assert_query_result(
            sim_query,
            [False],
            variables=dict(sim=0.9),
        )
        await self.assert_query_result(
            cfg_query,
            [0.3],
        )
