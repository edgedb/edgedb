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
        ))

    async def test_edgeql_explain_simple_01(self):
        res = await self.explain('''
            select User { id, name } filter .name = 'Elvis'
        ''')
        self.assert_plan(res['fine_grained'], {
            "contexts": [
                {"buffer_idx": 0, "end": 32, "start": 28, "text": "User"}
            ],
            "pipeline": [
                {
                    "actual_loops": 1,
                    "actual_rows": 1,
                    "plan_rows": 1,
                    "plan_type": "IndexScan",
                    "plan_width": 32,
                    "properties": [
                        {
                            "important": False,
                            "title": "schema",
                            "type": "text",
                            "value": "edgedbpub",
                        },
                        {
                            "important": False,
                            "title": "alias",
                            "type": "text",
                            "value": "User~2",
                        },
                        {
                            "important": False,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": False,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": False,
                            "title": "index_name",
                            "type": "index",
                            "value": "index of object type 'default::User' "
                            "on (__subject__.name)",
                        },
                        {
                            "important": False,
                            "title": "index_cond",
                            "type": "expr",
                        },
                    ],
                    "startup_cost": 0.28,
                }
            ],
            "subplans": [],
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
            "contexts": [
                {"buffer_idx": 0, "end": 35, "start": 31, "text": "User"}],
            "pipeline": [
                {
                    "actual_loops": 1,
                    "actual_rows": 1,
                    "plan_rows": 1,
                    "plan_type": "SubqueryScan",
                    "plan_width": 32,
                    "properties": tb.bag([
                        {
                            "important": False,
                            "title": "filter",
                            "type": "expr",
                        },
                    ]),
                    "startup_cost": 278.1,
                    "total_cost": 278.12,
                }
            ],
            "subplans": [
                {
                    "contexts": [
                        {
                            "buffer_idx": 0,
                            "end": 116,
                            "start": 74,
                        }
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
                            "plan_width": 32,
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "InitPlan",
                                },
                                {
                                    "important": False,
                                    "title": "subplan_name",
                                    "type": "text",
                                    "value": "InitPlan 1 (returns " "$0)",
                                },
                                {
                                    "important": False,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": False,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                            "startup_cost": 8.3,
                            "total_cost": 8.31,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "IndexScan",
                            "plan_width": 16,
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "filter",
                                    "type": "expr",
                                },
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                    "value": "User~3",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": False,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": False,
                                    "title": "index_name",
                                    "type": "index",
                                    "value": "index of object type "
                                    "'default::User' on "
                                    "(__subject__.name)",
                                },
                                {
                                    "important": False,
                                    "title": "index_cond",
                                    "type": "expr",
                                },
                            ]),
                            "startup_cost": 0.28,
                            "total_cost": 8.3,
                        },
                    ],
                    "subplans": [],
                },
                {
                    "contexts": [
                        {
                            "buffer_idx": 0,
                            "end": 174,
                            "start": 134,
                        },
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
                            "plan_width": 32,
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "InitPlan",
                                },
                                {
                                    "important": False,
                                    "title": "subplan_name",
                                    "type": "text",
                                    "value": "InitPlan 2 (returns " "$1)",
                                },
                                {
                                    "important": False,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": False,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                            "startup_cost": 269.78,
                            "total_cost": 269.79,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 5,
                            "plan_type": "SeqScan",
                            "plan_width": 16,
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "filter",
                                    "type": "expr",
                                },
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ]),
                            "startup_cost": 0.0,
                            "total_cost": 269.76,
                        },
                    ],
                    "subplans": [],
                },
            ],
        }
        self.assert_plan(res['fine_grained'], shape)

    async def test_edgeql_explain_multi_link_01(self):
        res = await self.explain('''
            select User { name, todo: {name, number} }
            filter .name = 'Elvis';
        ''')

        shape = {
            "contexts": [{
                "buffer_idx": 0,
                "end": 32,
                "start": 28,
                "text": "User",
            }],
            "pipeline": [
                {
                    "actual_loops": 1,
                    "actual_rows": 1,
                    "plan_rows": 1,
                    "plan_type": "IndexScan",
                    "plan_width": 32,
                    "properties": [
                        {
                            "important": False,
                            "title": "schema",
                            "type": "text",
                            "value": "edgedbpub",
                        },
                        {
                            "important": False,
                            "title": "alias",
                            "type": "text",
                            "value": "User~2",
                        },
                        {
                            "important": False,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": False,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": False,
                            "title": "index_name",
                            "type": "index",
                            "value": "index of object type 'default::User' "
                            "on (__subject__.name)",
                        },
                        {
                            "important": False,
                            "title": "index_cond",
                            "type": "expr",
                        },
                    ],
                }
            ],
            "subplans": [
                {
                    "contexts": [{
                        "buffer_idx": 0,
                        "end": 45,
                        "start": 41,
                        "text": "todo"
                    }],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
                            "plan_width": 32,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "SubPlan",
                                },
                                {
                                    "important": False,
                                    "title": "subplan_name",
                                    "type": "text",
                                    "value": "SubPlan 1",
                                },
                                {
                                    "important": False,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": False,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ],
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 2,
                            "plan_type": "NestedLoop",
                            "plan_width": 32,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": False,
                                    "title": "join_type",
                                    "type": "text",
                                    "value": "Inner",
                                },
                            ],
                        },
                    ],
                    "subplans": [
                        {
                            "pipeline": [
                                {
                                    "actual_loops": 1,
                                    "actual_rows": 2,
                                    "plan_rows": 2,
                                    "plan_type": "IndexOnlyScan",
                                    "plan_width": 16,
                                    "properties": [
                                        {
                                            "important": False,
                                            "title": "parent_relationship",
                                            "type": "text",
                                            "value": "Outer",
                                        },
                                        {
                                            "important": False,
                                            "title": "schema",
                                            "type": "text",
                                            "value": "edgedbpub",
                                        },
                                        {
                                            "important": False,
                                            "title": "alias",
                                            "type": "text",
                                            "value": "todo~1",
                                        },
                                        {
                                            "important": False,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                        {
                                            "important": False,
                                            "title": "scan_direction",
                                            "type": "text",
                                            "value": "Forward",
                                        },
                                        {
                                            "important": False,
                                            "title": "index_name",
                                            "type": "index",
                                            "value": "default::User.todo "
                                            "forward "
                                            "link "
                                            "index",
                                        },
                                        {
                                            "important": False,
                                            "title": "index_cond",
                                            "type": "expr",
                                            "value": '("todo~1".source '
                                            "= "
                                            '"User~2".id)',
                                        },
                                        {
                                            "important": False,
                                            "title": "heap_fetches",
                                            "type": "float",
                                            "value": 2,
                                        },
                                    ],
                                }
                            ],
                            "subplans": [],
                        },
                        {
                            "pipeline": [
                                {
                                    "actual_loops": 2,
                                    "actual_rows": 1,
                                    "plan_rows": 1,
                                    "plan_type": "IndexScan",
                                    "plan_width": 32,
                                    "properties": [
                                        {
                                            "important": False,
                                            "title": "parent_relationship",
                                            "type": "text",
                                            "value": "Inner",
                                        },
                                        {
                                            "important": False,
                                            "title": "schema",
                                            "type": "text",
                                            "value": "edgedbpub",
                                        },
                                        {
                                            "important": False,
                                            "title": "alias",
                                            "type": "text",
                                            "value": "Issue~1",
                                        },
                                        {
                                            "important": False,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                        {
                                            "important": False,
                                            "title": "scan_direction",
                                            "type": "text",
                                            "value": "Forward",
                                        },
                                        {
                                            "important": False,
                                            "title": "index_name",
                                            "type": "index",
                                            "value": "constraint "
                                            "'std::exclusive' "
                                            "of "
                                            "property "
                                            "'id' of "
                                            "object "
                                            "type "
                                            "'default::Issue'",
                                        },
                                        {
                                            "important": False,
                                            "title": "index_cond",
                                            "type": "expr",
                                            "value": '("Issue~1".id '
                                            "= "
                                            '"todo~1".target)',
                                        },
                                    ],
                                }
                            ],
                            "subplans": [],
                        },
                    ],
                }
            ],
        }
        self.assert_plan(res['fine_grained'], shape)

    async def test_edgeql_explain_computed_backlink_01(self):
        res = await self.explain('''
            select User { name, owned_issues: {name, number} }
            filter .name = 'Elvis';
        ''')

        shape = {
            "contexts": [{
                "buffer_idx": 0,
                "end": 32,
                "start": 28,
                "text": "User",
            }],
            "pipeline": [
                {
                    "actual_loops": 1,
                    "actual_rows": 1,
                    "plan_rows": 1,
                    "plan_type": "IndexScan",
                    "plan_width": 32,
                    "properties": [
                        {
                            "important": False,
                            "title": "schema",
                            "type": "text",
                            "value": "edgedbpub",
                        },
                        {
                            "important": False,
                            "title": "alias",
                            "type": "text",
                            "value": "User~2",
                        },
                        {
                            "important": False,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": False,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": False,
                            "title": "index_name",
                            "type": "index",
                            "value": "index of object type 'default::User' "
                            "on (__subject__.name)",
                        },
                        {
                            "important": False,
                            "title": "index_cond",
                            "type": "expr",
                        },
                    ],
                    "startup_cost": 0.28,
                    "total_cost": 16.69,
                }
            ],
            "subplans": [
                {
                    "contexts": [
                        {
                            "buffer_idx": 1,
                            "end": 26,
                            "start": 0,
                            "text": ".<owner[is default::Issue]",
                        },
                        {
                            "buffer_idx": 0,
                            "end": 53,
                            "start": 41,
                            "text": "owned_issues",
                        },
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
                            "plan_width": 32,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "SubPlan",
                                },
                                {
                                    "important": False,
                                    "title": "subplan_name",
                                    "type": "text",
                                    "value": "SubPlan 1",
                                },
                                {
                                    "important": False,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": False,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ],
                            "startup_cost": 8.38,
                            "total_cost": 8.39,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 5,
                            "plan_type": "Result",
                            "plan_width": 32,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": False,
                                    "title": "one_time_filter",
                                    "type": "expr",
                                    "value": '("User~2".id = "User~2".id)',
                                },
                            ],
                            "startup_cost": 0.28,
                            "total_cost": 8.37,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 5,
                            "plan_type": "IndexScan",
                            "plan_width": 32,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                    "value": "Issue~1",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": False,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": False,
                                    "title": "index_name",
                                    "type": "index",
                                    "value": "default::Issue.owner index",
                                },
                                {
                                    "important": False,
                                    "title": "index_cond",
                                    "type": "expr",
                                },
                            ],
                            "startup_cost": 0.28,
                            "total_cost": 8.37,
                        },
                    ],
                    "subplans": [],
                }
            ],
        }
        self.assert_plan(res['fine_grained'], shape)

        self.assertEqual(len(res['buffers']), 2)
        self.assertEqual(res['buffers'][1], ".<owner[is default::Issue]")

    async def test_edgeql_explain_inheritance_01(self):
        res = await self.explain('''
            WITH X := Text, select X;
        ''')

        shape = {
            "contexts": [
                {"buffer_idx": 0, "end": 35, "start": 31, "text": "Text"},
                {"buffer_idx": 0, "end": 45, "start": 44, "text": "X"},
            ],
            "pipeline": [
                {
                    "actual_loops": 1,
                    "plan_rows": 5001,
                    "plan_type": "Result",
                    "plan_width": 32,
                    "properties": [],
                    "startup_cost": 0.0,
                },
                {
                    "actual_loops": 1,
                    "actual_rows": 5001,
                    "plan_rows": 5001,
                    "plan_type": "Append",
                    "plan_width": 16,
                    "properties": [
                        {
                            "important": False,
                            "title": "parent_relationship",
                            "type": "text",
                            "value": "Outer",
                        }
                    ],
                    "startup_cost": 0.0,
                },
            ],
            # Order here can be arbitrary, unfortunately.
            # And because of arbitrary order, the plan is a bit different.
            # So se remove many important fields from the check and only check
            # the overall structure
            "subplans": [
                {
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "plan_type": "SeqScan",
                            "plan_width": 16,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Member",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ],
                            "startup_cost": 0.0,
                        }
                    ],
                    "subplans": [],
                },
                {
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "plan_type": "SeqScan",
                            "plan_width": 16,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Member",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ],
                            "startup_cost": 0.0,
                        }
                    ],
                    "subplans": [],
                },
                {
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "plan_type": "SeqScan",
                            "plan_width": 16,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Member",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ],
                            "startup_cost": 0.0,
                        }
                    ],
                    "subplans": [],
                },
            ],
        }

        self.assert_plan(res['fine_grained'], shape)

    async def test_edgeql_explain_type_intersect_01(self):
        res = await self.explain('''
            select Text {
                body,
                z := [is Issue].name
            };
        ''')

        shape = {
            "pipeline": [
                {
                    "actual_loops": 1,
                    "actual_rows": 5001,
                    "plan_rows": 5001,
                    "plan_type": "Result",
                    "plan_width": 32,
                    "properties": [],
                    "startup_cost": 0.0,
                    "total_cost": 41707.33,
                }
            ],
            "subplans": [
                {
                    "contexts": [{
                        "buffer_idx": 0,
                        "end": 32,
                        "start": 28,
                        "text": "Text",
                    }],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 5001,
                            "plan_rows": 5001,
                            "plan_type": "Append",
                            "plan_width": 20,
                            "properties": [
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                }
                            ],
                            "startup_cost": 0.0,
                            "total_cost": 149.02,
                        }
                    ],
                    "subplans": tb.bag([
                        {
                            "pipeline": [
                                {
                                    "actual_loops": 1,
                                    "actual_rows": 4999,
                                    "plan_rows": 4999,
                                    "plan_type": "SeqScan",
                                    "plan_width": 20,
                                    "properties": tb.bag([
                                        {
                                            "important": False,
                                            "title": "parent_relationship",
                                            "type": "text",
                                            "value": "Member",
                                        },
                                        {
                                            "important": False,
                                            "title": "schema",
                                            "type": "text",
                                            "value": "edgedbpub",
                                        },
                                        {
                                            "important": False,
                                            "title": "alias",
                                            "type": "text",
                                            "value": "Issue~1",
                                        },
                                        {
                                            "important": False,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": 0.0,
                                    "total_cost": 121.99,
                                }
                            ],
                            "subplans": [],
                        },
                        {
                            "pipeline": [
                                {
                                    "actual_loops": 1,
                                    "actual_rows": 1,
                                    "plan_rows": 1,
                                    "plan_type": "SeqScan",
                                    "plan_width": 38,
                                    "properties": tb.bag([
                                        {
                                            "important": False,
                                            "title": "parent_relationship",
                                            "type": "text",
                                            "value": "Member",
                                        },
                                        {
                                            "important": False,
                                            "title": "schema",
                                            "type": "text",
                                            "value": "edgedbpub",
                                        },
                                        {
                                            "important": False,
                                            "title": "alias",
                                            "type": "text",
                                            "value": "LogEntry~1",
                                        },
                                        {
                                            "important": False,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": 0.0,
                                    "total_cost": 1.01,
                                }
                            ],
                            "subplans": [],
                        },
                        {
                            "pipeline": [
                                {
                                    "actual_loops": 1,
                                    "actual_rows": 1,
                                    "plan_rows": 1,
                                    "plan_type": "SeqScan",
                                    "plan_width": 45,
                                    "properties": tb.bag([
                                        {
                                            "important": False,
                                            "title": "parent_relationship",
                                            "type": "text",
                                            "value": "Member",
                                        },
                                        {
                                            "important": False,
                                            "title": "schema",
                                            "type": "text",
                                            "value": "edgedbpub",
                                        },
                                        {
                                            "important": False,
                                            "title": "alias",
                                            "type": "text",
                                            "value": "Comment~1",
                                        },
                                        {
                                            "important": False,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": 0.0,
                                    "total_cost": 1.01,
                                }
                            ],
                            "subplans": [],
                        },
                    ]),
                },
                {
                    "contexts": [
                        {
                            "buffer_idx": 0,
                            "end": 93,
                            "start": 73,
                            "text": "z := [is Issue].name",
                        }
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 5001,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "IndexScan",
                            "plan_width": 11,
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "SubPlan",
                                },
                                {
                                    "important": False,
                                    "title": "subplan_name",
                                    "type": "text",
                                    "value": "SubPlan 1",
                                },
                                {
                                    "important": False,
                                    "title": "schema",
                                    "type": "text",
                                    "value": "edgedbpub",
                                },
                                {
                                    "important": False,
                                    "title": "alias",
                                    "type": "text",
                                    "value": "Issue~2",
                                },
                                {
                                    "important": False,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": False,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": False,
                                    "title": "index_name",
                                    "type": "index",
                                    "value": "constraint "
                                    "'std::exclusive' of "
                                    "property 'id' of object "
                                    "type 'default::Issue'",
                                },
                                {
                                    "important": False,
                                    "title": "index_cond",
                                    "type": "expr",
                                },
                            ]),
                            "startup_cost": 0.28,
                            "total_cost": 8.3,
                        }
                    ],
                    "subplans": [],
                },
            ],
        }

        self.assert_plan(res['fine_grained'], shape)

    async def test_edgeql_explain_insert_01(self):
        # Use an ad-hoc connection to avoid TRANSACTION_ISOLATION
        con = await self.connect(database=self.con.dbname)
        try:
            res = await self.explain('''
                insert User { name := 'Fantix' }
            ''', execute=True, con=con)
            self.assert_plan(res['fine_grained'], {
                'pipeline': [{'plan_type': 'NestedLoop'}],
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
            self.assert_plan(res['fine_grained'], {
                'pipeline': [{'plan_type': 'NestedLoop'}],
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
        self.assertNotIn(
            'actual_startup_time',
            res['fine_grained']['pipeline'][0],
        )
        self.assertEqual(
            {'buffers': False, 'execute': False},
            res['arguments'],
        )

        res = json.loads(await self.con.query_single('''
            analyze (buffers := True) select User
        '''))
        self.assertIn('shared_read_blocks', res['fine_grained']['pipeline'][0])
        self.assertEqual({'buffers': True, 'execute': True}, res['arguments'])

        res = json.loads(await self.con.query_single('''
            analyze (buffers := false) select User
        '''))
        self.assertNotIn(
            'shared_read_blocks',
            res['fine_grained']['pipeline'][0],
        )
        self.assertEqual({'buffers': False, 'execute': True}, res['arguments'])

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
