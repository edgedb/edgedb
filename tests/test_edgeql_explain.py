#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

import json
import os.path

from edb.testbase import server as tb
from edb.common import assert_data_shape


class TestEdgeQLExplain(tb.QueryTestCase):
    '''Tests for EXPLAIN'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'issues_setup.edgeql'),
        '''
            alter type User {
                create link owned_issues := .<owner[is Issue]
            };
            # Make the database more populated so that it uses
            # indexes...
            for i in range_unpack(range(1, 1000)) union (
              with u := (insert User { name := <str>i }),
              for j in range_unpack(range(0, 5)) union (
                insert Issue {
                  owner := u,
                  number := <str>(i*100 + j),
                  name := "issue " ++ <str>i ++ "/" ++ <str>j,
                  status := (select Status filter .name = 'Open'),
                  body := "BUG",
                }
            ));
            update User set {
              todo += (select .owned_issues filter <int64>.number % 3 = 0)
            };

            administer statistics_update();
        '''
    ]

    def assert_plan(self, data, shape, message=None):
        assert_data_shape.assert_data_shape(
            data, shape, fail=self.fail, message=message)

    async def explain(self, query, *, execute=True, con=None):
        no_ex = '(execute := False) ' if not execute else ''
        return json.loads(await (con or self.con).query_single(
            f'analyze {no_ex}{query}'
        ))[0]

    async def test_edgeql_explain_simple_01(self):
        res = await self.explain('''
            select User { id, name } filter .name = 'Elvis'
        ''')
        self.assert_plan(res['plan'], {
            'node_type': 'Index Scan',
            'relation_name': 'default::User',
            'contexts': [{'start': 28, 'end': 32, 'buffer_idx': 0}],
        })
        self.assert_plan(res['config_vals'], {
            "allow_user_specified_id": False,
            "apply_access_policies": True
        })

    async def test_edgeql_explain_with_bound_01(self):
        res = await self.explain('''
            with U := User,
            select {
                elvis := (select U filter .name like 'E%'),
                yury := (select U filter .name[0] = 'Y'),
            };
        ''')

        shape = {
            "node_type": "Subquery Scan",
            "plans": tb.bag([
                1,
                2,
                {
                    "node_type": "Result",
                    "output": [
                        "edgedbext.uuid_generate_v4()"
                    ]
                }
            ]),
            "collapsed_plans": tb.bag([
                {
                    "node_type": "Aggregate",
                    "plans": [0],
                    # XXX: If we don't run ANALYZE in the test setup,
                    # we sometimes get this plan using bitmap scans
                    # instead of just the index scan?
                    # "Node Type": "Bitmap Heap Scan",
                    # "Plans": [
                    #     {
                    #         "Node Type": "Bitmap Index Scan",
                    #         "Parent Relationship": "Outer",
                    #         "Index Name": str,
                    #     }
                    # ],
                    "nearest_context_plan": {
                        "node_type": "Index Scan",
                        "parent_relationship": "Outer",
                        "relation_name": "default::User",
                        "contexts": [
                            {
                                "start": 31,
                                "end": 35,
                                "buffer_idx": 0
                            },
                            {
                                "start": 91,
                                "end": 92,
                                "buffer_idx": 0
                            },
                            {
                                "start": 74,
                                "end": 116,
                                "buffer_idx": 0
                            }
                        ],
                        "suggested_display_ctx_idx": 2,
                    }
                },
                {
                    "node_type": "Aggregate",
                    "plans": [0],
                    "nearest_context_plan": {
                        "node_type": "Seq Scan",
                        "relation_name": "default::User",
                        "contexts": [
                            {
                                "start": 31,
                                "end": 35,
                                "buffer_idx": 0
                            },
                            {
                                "start": 150,
                                "end": 151,
                                "buffer_idx": 0
                            },
                            {
                                "start": 134,
                                "end": 174,
                                "buffer_idx": 0
                            }
                        ],
                        "suggested_display_ctx_idx": 2
                    }
                },
            ])
        }
        self.assert_plan(res['plan'], shape)

    async def test_edgeql_explain_multi_link_01(self):
        res = await self.explain('''
            select User { name, todo: {name, number} }
            filter .name = 'Elvis';
        ''')

        shape = {
            "node_type": "Index Scan",
            "index_name": (
                "index of object type 'default::User' on (__subject__.name)"),
            "relation_name": "default::User",
            "plans": [1],
            "collapsed_plans": [
                {
                    "node_type": "Aggregate",
                    "strategy": "Plain",
                    "parent_relationship": "SubPlan",
                    "subplan_name": "SubPlan 1",
                    "plans": [
                        {
                            "node_type": "Nested Loop",
                            "parent_relationship": "Outer",
                            "join_type": "Inner",
                            "inner_unique": True,
                            "plans": [0, 1]
                        }
                    ],
                    "collapsed_plans": [
                        {
                            "node_type": "Index Scan",
                            "parent_relationship": "Inner",
                            "index_name": (
                                "constraint 'std::exclusive' of "
                                "property 'id' of object type '"
                                "default::Issue'"
                            ),
                            "relation_name": "default::Issue",
                            "contexts": [
                                {
                                    "start": 41,
                                    "end": 45,
                                    "buffer_idx": 0
                                }
                            ]
                        }
                    ],
                    "nearest_context_plan": {
                        "node_type": "Index Only Scan",
                        "parent_relationship": "Outer",
                        "index_name": (
                            "default::User.todo forward link index"
                        ),
                        "relation_name": "default::User.todo",
                        "contexts": [
                            {
                                "start": 41,
                                "end": 45,
                                "buffer_idx": 0
                            }
                        ]
                    },
                }
            ],
            "contexts": [
                {
                    "start": 28,
                    "end": 32,
                    "buffer_idx": 0
                }
            ]
        }
        self.assert_plan(res['plan'], shape)

    async def test_edgeql_explain_computed_backlink_01(self):
        res = await self.explain('''
            select User { name, owned_issues: {name, number} }
            filter .name = 'Elvis';
        ''')

        shape = {
            "node_type": "Index Scan",
            "index_name": (
                "index of object type 'default::User' on (__subject__.name)"),
            "relation_name": "default::User",
            "plans": [1],
            "collapsed_plans": [
                {
                    "node_type": "Aggregate",
                    "parent_relationship": "SubPlan",
                    "plans": [
                        {
                            "node_type": "Result",
                            "parent_relationship": "Outer",
                            "plans": [0]
                        }
                    ],
                    "nearest_context_plan": {
                        # I've seen this as both Index Scan and Bitmap
                        # Heap Scan with a subnode Bitmap Index Scan.
                        # Just don't check, it's fine.
                        # "node_type": "Index Scan",
                        "parent_relationship": "Outer",
                        "relation_name": "default::Issue",
                        # We get a stack of contexts back
                        "contexts": [
                            {
                                "start": 0,
                                "end": 7,
                                "buffer_idx": 1
                            },
                            {
                                "start": 0,
                                "end": 26,
                                "buffer_idx": 1
                            },
                            {
                                "start": 41,
                                "end": 53,
                                "buffer_idx": 0
                            }
                        ],
                        "suggested_display_ctx_idx": 2
                    }
                }
            ],
            "contexts": [
                {
                    "start": 28,
                    "end": 32,
                    "buffer_idx": 0
                }
            ]
        }
        self.assert_plan(res['plan'], shape)

        self.assertEqual(len(res['buffers']), 2)
        self.assertEqual(res['buffers'][1][0], ".<owner[is default::Issue]")

    async def test_edgeql_explain_inheritance_01(self):
        res = await self.explain('''
            WITH X := Text, select X;
        ''')

        shape = {
            "node_type": "Result",
            "plans": [
                {
                    "node_type": "Append",
                    "parent_relationship": "Outer",
                    "plans": tb.bag([1, 2, 3])
                }
            ],
            "collapsed_plans": tb.bag([
                {
                    "node_type": "Seq Scan",
                    "parent_relationship": "Member",
                    "relation_name": "default::Issue",
                    "original_relation_name": "default::Text",
                    "contexts": [
                        {
                            "start": 31,
                            "end": 35,
                            "buffer_idx": 0,
                            "text": "Text"
                        },
                        {
                            "start": 44,
                            "end": 45,
                            "buffer_idx": 0,
                            "text": "X"
                        }
                    ]
                },
                {
                    "node_type": "Seq Scan",
                    "parent_relationship": "Member",
                    "relation_name": "default::Comment",
                    "original_relation_name": "default::Text",
                    "contexts": [
                        {
                            "start": 31,
                            "end": 35,
                            "buffer_idx": 0,
                            "text": "Text"
                        },
                        {
                            "start": 44,
                            "end": 45,
                            "buffer_idx": 0,
                            "text": "X"
                        }
                    ]
                },
                {
                    "node_type": "Seq Scan",
                    "parent_relationship": "Member",
                    "relation_name": "default::LogEntry",
                    "original_relation_name": "default::Text",
                    "contexts": [
                        {
                            "start": 31,
                            "end": 35,
                            "buffer_idx": 0,
                            "text": "Text"
                        },
                        {
                            "start": 44,
                            "end": 45,
                            "buffer_idx": 0,
                            "text": "X"
                        }
                    ]
                }
            ])
        }

        self.assert_plan(res['plan'], shape)

    async def test_edgeql_explain_type_intersect_01(self):
        res = await self.explain('''
            select Text {
                body,
                z := [is Issue].name
            };
        ''')

        shape = {
            "node_type": "Result",
            "plans": tb.bag([
                {
                    "node_type": "Append",
                    "plans": tb.bag([2, 3, 4])
                },
                1
            ]),
            "collapsed_plans": tb.bag([
                {
                    "node_type": "Index Scan",
                    "relation_name": "default::Issue",
                    "contexts": [
                        {"text": "[is Issue]"},
                        {"text": "[is Issue].name"},
                        {"text": "z := [is Issue].name"},
                    ],
                    "suggested_display_ctx_idx": 2
                },
                {
                    "relation_name": "default::LogEntry",
                    "original_relation_name": "default::Text",
                    "contexts": [{"text": "Text"}]
                },
                {
                    "relation_name": "default::Issue",
                    "original_relation_name": "default::Text",
                    "contexts": [{"text": "Text"}]
                },
                {
                    "relation_name": "default::Comment",
                    "original_relation_name": "default::Text",
                    "contexts": [{"text": "Text"}]
                }
            ])
        }

        self.assert_plan(res['plan'], shape)

    async def test_edgeql_explain_insert_01(self):
        # Use an ad-hoc connection to avoid TRANSACTION_ISOLATION
        con = await self.connect(database=self.con.dbname)
        try:
            res = await self.explain('''
                insert User { name := 'Fantix' }
            ''', execute=True, con=con)
            self.assert_plan(res['plan'], {
                'node_type': 'Nested Loop',
            })
            self.assertFalse(await con.query('''
                select User { id, name } filter .name = 'Fantix'
            '''))
        finally:
            await con.aclose()

    async def test_edgeql_explain_insert_02(self):
        async with self.con.transaction():
            await self.con.execute('''
                insert User { name := 'Sully' }
            ''')
            res = await self.explain('''
                insert User { name := 'Fantix' }
            ''', execute=True)
            self.assert_plan(res['plan'], {
                'node_type': 'Nested Loop',
            })
            self.assertTrue(await self.con.query('''
                select User { id, name } filter .name = 'Sully'
            '''))
            self.assertFalse(await self.con.query('''
                select User { id, name } filter .name = 'Fantix'
            '''))

        self.assertTrue(await self.con.query('''
            select User { id, name } filter .name = 'Sully'
        '''))
        self.assertFalse(await self.con.query('''
            select User { id, name } filter .name = 'Fantix'
        '''))

    async def test_edgeql_explain_options_01(self):
        res = await self.explain('''
            select User
        ''', execute=False)
        self.assertNotIn('actual_startup_time', res['plan'])

        res = json.loads(await self.con.query_single('''
            analyze (buffers := True) select User
        '''))[0]
        self.assertIn('shared_read_blocks', res['plan'])

        res = json.loads(await self.con.query_single('''
            analyze (buffers := false) select User
        '''))[0]
        self.assertNotIn('shared_read_blocks', res['plan'])

    async def test_edgeql_explain_options_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"unknown ANALYZE argument"
        ):
            await self.con.query_single('''
                analyze (bogus_argument := True) select User
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"incorrect type"
        ):
            await self.con.query_single('''
                analyze (execute := "hell yeah") select User
            ''')
