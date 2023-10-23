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

import unittest
import json
import os.path

from edb.testbase import server as tb
from edb.common import assert_data_shape
from edb.schema import name as sn
from edb.server.compiler.explain import pg_tree


class TestEdgeQLExplain(tb.QueryTestCase):
    '''Tests for EXPLAIN.

    This is a good way of testing explain functionality, but also this can be
    used to test indexes.
    '''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'explain.esdl')

    SCHEMA_BUG5758 = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'explain_bug5758.esdl')

    SCHEMA_BUG5791 = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'explain_bug5791.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'explain_setup.edgeql'),
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
                    "properties": tb.bag([
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
                            "important": True,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": True,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": True,
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
                    ]),
                    "startup_cost": float,
                }
            ],
            "subplans": [],
        })
        self.assert_plan(res['config_vals'], {
            "allow_user_specified_id": False,
            "apply_access_policies": True
        })

    async def test_edgeql_explain_introspection_01(self):
        res = await self.explain('select sys::Database')
        self.assertIn(
            ('relation_name', 'pg_database'),
            ((p['title'], p['value'])
             for p in res['fine_grained']['pipeline'][0]['properties']),
        )

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
                    "properties": tb.bag([
                        {
                            "important": False,
                            "title": "filter",
                            "type": "expr",
                        },
                    ]),
                    "startup_cost": float,
                    "total_cost": float,
                }
            ],
            "subplans": [
                {
                    "contexts": [
                        {
                            "buffer_idx": 0,
                            "end": 115,
                            "start": 74,
                        }
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
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
                                    "important": True,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": True,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "IndexScan",
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
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": True,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": True,
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
                            "startup_cost": float,
                            "total_cost": float,
                        },
                    ],
                    "subplans": [],
                },
                {
                    "contexts": [
                        {
                            "buffer_idx": 0,
                            "end": 173,
                            "start": 134,
                        },
                    ],
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 1,
                            "plan_type": "Aggregate",
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
                                    "important": True,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": True,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 1,
                            "plan_rows": 5,
                            "plan_type": "SeqScan",
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
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
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
                    "properties": tb.bag([
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
                            "important": True,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": True,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": True,
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
                    ]),
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
                                    "important": True,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": True,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 2,
                            "plan_type": "NestedLoop",
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                },
                                {
                                    "important": True,
                                    "title": "join_type",
                                    "type": "text",
                                    "value": "Inner",
                                },
                            ]),
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
                                    # This has property `heap_fetches`
                                    # that vary on github an locally.
                                    # So skip checking "properties"
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
                                    "properties": tb.bag([
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
                                            "important": True,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                        {
                                            "important": True,
                                            "title": "scan_direction",
                                            "type": "text",
                                            "value": "Forward",
                                        },
                                        {
                                            "important": True,
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
                                    ]),
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
                    "properties": tb.bag([
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
                            "important": True,
                            "title": "relation_name",
                            "type": "relation",
                        },
                        {
                            "important": True,
                            "title": "scan_direction",
                            "type": "text",
                            "value": "Forward",
                        },
                        {
                            "important": True,
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
                    ]),
                    "startup_cost": float,
                    "total_cost": float,
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
                                    "important": True,
                                    "title": "strategy",
                                    "type": "text",
                                    "value": "Plain",
                                },
                                {
                                    "important": True,
                                    "title": "partial_mode",
                                    "type": "text",
                                    "value": "Simple",
                                },
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 5,
                            "plan_type": "Result",
                            "properties": tb.bag([
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
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
                        },
                        {
                            "actual_loops": 1,
                            "actual_rows": 2,
                            "plan_rows": 5,
                            "plan_type": "IndexScan",
                            "properties": tb.bag([
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
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": True,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": True,
                                    "title": "index_name",
                                    "type": "index",
                                    "value": "Issue.owner index",
                                },
                                {
                                    "important": False,
                                    "title": "index_cond",
                                    "type": "expr",
                                },
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
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
                    "properties": [],
                    "startup_cost": float,
                },
                {
                    "actual_loops": 1,
                    "actual_rows": 5001,
                    "plan_rows": 5001,
                    "plan_type": "Append",
                    "properties": tb.bag([
                        {
                            "important": False,
                            "title": "parent_relationship",
                            "type": "text",
                            "value": "Outer",
                        }
                    ]),
                    "startup_cost": float,
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
                                },
                                {
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ]),
                            "startup_cost": float,
                        }
                    ],
                    "subplans": [],
                },
                {
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "plan_type": "SeqScan",
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
                                },
                                {
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ]),
                            "startup_cost": float,
                        }
                    ],
                    "subplans": [],
                },
                {
                    "pipeline": [
                        {
                            "actual_loops": 1,
                            "plan_type": "SeqScan",
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
                                },
                                {
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                            ]),
                            "startup_cost": float,
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
                    "properties": [],
                    "startup_cost": float,
                    "total_cost": float,
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
                            "properties": tb.bag([
                                {
                                    "important": False,
                                    "title": "parent_relationship",
                                    "type": "text",
                                    "value": "Outer",
                                }
                            ]),
                            "startup_cost": float,
                            "total_cost": float,
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
                                            "important": True,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": float,
                                    "total_cost": float,
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
                                            "important": True,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": float,
                                    "total_cost": float,
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
                                            "important": True,
                                            "title": "relation_name",
                                            "type": "relation",
                                        },
                                    ]),
                                    "startup_cost": float,
                                    "total_cost": float,
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
                                    "important": True,
                                    "title": "relation_name",
                                    "type": "relation",
                                },
                                {
                                    "important": True,
                                    "title": "scan_direction",
                                    "type": "text",
                                    "value": "Forward",
                                },
                                {
                                    "important": True,
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
                            "startup_cost": float,
                            "total_cost": float,
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

    def assert_index_in_plan(self, data, propname, message='Index test'):
        # First we check the plan_type as there are a couple of valid options
        # here: IndexScan and BitmapHeapScan, but they have different
        # substructure to check.
        self.assert_plan(
            data,
            {
                'fine_grained': {
                    'pipeline': [dict()]
                }
            },
            message=message,
        )
        plan_type = data['fine_grained']['pipeline'][0]['plan_type']
        self.assert_plan(
            data['fine_grained'],
            self.get_gist_index_expected_res(
                propname, plan_type, message=message
            ),
            message=message,
        )

    def get_gist_index_expected_res(self, fname, plan_type, message=None):
        if plan_type == 'IndexScan':
            return {
                "pipeline": [
                    {
                        "plan_type": "IndexScan",
                        "properties": tb.bag([
                            {
                                'important': False,
                                'title': 'schema',
                                'type': 'text',
                                'value': 'edgedbpub',
                            },
                            {
                                'important': False,
                                'title': 'alias',
                                'type': 'text',
                            },
                            {
                                'important': True,
                                'title': 'relation_name',
                                'type': 'relation',
                                'value': 'RangeTest',
                            },
                            {
                                'important': True,
                                'title': 'scan_direction',
                                'type': 'text',
                                'value': str,
                            },
                            {
                                'important': True,
                                'title': 'index_name',
                                'type': 'index',
                                'value':
                                    f"index 'pg::gist' of object type "
                                    f"'default::RangeTest' on (.{fname})",
                            },
                            {
                                'important': False,
                                'title': 'index_cond',
                                'type': 'expr',
                            },
                        ]),
                    },
                ],
            }
        elif plan_type == 'BitmapHeapScan':
            return {
                "pipeline": [
                    {
                        "plan_type": "BitmapHeapScan",
                    },
                ],
                "subplans": [
                    {
                        "pipeline": [
                            {
                                "plan_type": "BitmapIndexScan",
                                "properties": tb.bag([
                                    {
                                        'important': False,
                                        'title': 'parent_relationship',
                                        'type': 'text',
                                        'value': 'Outer',
                                    },
                                    {
                                        'important': True,
                                        'title': 'index_name',
                                        'type': 'index',
                                        'value':
                                            f"index 'pg::gist' of object"
                                            f" type 'default::RangeTest'"
                                            f" on (.{fname})",
                                    },
                                    {
                                        'important': False,
                                        'title': 'index_cond',
                                        'type': 'expr',
                                    },
                                ]),
                            },
                        ],
                    },
                ],
            }
        else:
            self.fail(
                f'{message}: "plan_type" expected to be "IndexScan" or '
                f'"BitmapHeapScan", got {plan_type!r}'
            )

    async def test_edgeql_explain_ranges_contains_01(self):
        res = await self.explain('''
            select RangeTest {id, rval}
            filter contains(.rval, 295)
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter contains(.mval, 295)
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter contains(.rdate, <cal::local_date>'2000-01-05')
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter contains(.mdate, <cal::local_date>'2000-01-05')
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_contains_02(self):
        res = await self.explain('''
            select RangeTest {id, rval}
            filter contains(.rval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter contains(.mval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter contains(
                .rdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10')
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter contains(
                .mdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10')
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_contains_03(self):
        res = await self.explain('''
            select RangeTest {id, mval}
            filter contains(
                .mval,
                multirange([
                    range(-299, 297),
                    range(297, 299),
                ])
            )
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter contains(
                .mdate,
                multirange([
                    range(<cal::local_date>'2000-01-05',
                          <cal::local_date>'2000-01-10'),
                    range(<cal::local_date>'2010-01-05',
                          <cal::local_date>'2010-01-10'),
                ])
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_overlaps_01(self):
        # The field is the first arg in `overlaps`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter overlaps(.rval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter overlaps(.mval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter overlaps(
                .rdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter overlaps(
                .mdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_overlaps_02(self):
        # The field is the second arg in `overlaps`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter overlaps(range(295, 299), .rval)
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter overlaps(range(295, 299), .mval)
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter overlaps(
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
                .rdate,
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter overlaps(
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
                .mdate,
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_adjacent_01(self):
        # The field is the first arg in `adjacent`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter adjacent(.rval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter adjacent(.mval, range(295, 299))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter adjacent(
                .rdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter adjacent(
                .mdate,
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_adjacent_02(self):
        # The field is the second arg in `adjacent`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter adjacent(range(295, 299), .rval)
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter adjacent(range(295, 299), .mval)
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter adjacent(
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
                .rdate,
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter adjacent(
                range(<cal::local_date>'2000-01-05',
                      <cal::local_date>'2000-01-10'),
                .mdate,
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_strictly_below_01(self):
        # The field is the first arg in `strictly_below`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter strictly_below(.rval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter strictly_below(.mval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter strictly_below(
                .rdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter strictly_below(
                .mdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_strictly_below_02(self):
        # The field is the second arg in `strictly_below`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter strictly_below(range(-50, 50), .rval)
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter strictly_below(range(-50, 50), .mval)
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter strictly_below(
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
                .rdate,
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter strictly_below(
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
                .mdate,
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_strictly_above_01(self):
        # The field is the first arg in `strictly_above`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter strictly_above(.rval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter strictly_above(.mval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter strictly_above(
                .rdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter strictly_above(
                .mdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_strictly_above_02(self):
        # The field is the second arg in `strictly_above`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter strictly_above(range(-50, 50), .rval)
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter strictly_above(range(-50, 50), .mval)
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter strictly_above(
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
                .rdate,
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter strictly_above(
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
                .mdate,
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_bounded_below_01(self):
        # The field is the first arg in `bounded_below`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter bounded_below(.rval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter bounded_below(.mval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter bounded_below(
                .rdate,
                range(<cal::local_date>'2012-01-05',
                      <cal::local_date>'2015-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter bounded_below(
                .mdate,
                range(<cal::local_date>'2012-01-05',
                      <cal::local_date>'2015-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_ranges_bounded_above_01(self):
        # The field is the first arg in `bounded_above`
        res = await self.explain('''
            select RangeTest {id, rval}
            filter bounded_above(.rval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'rval')

        res = await self.explain('''
            select RangeTest {id, mval}
            filter bounded_above(.mval, range(-50, 50))
        ''')
        self.assert_index_in_plan(res, 'mval')

        res = await self.explain('''
            select RangeTest {id, rdate}
            filter bounded_above(
                .rdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'rdate')

        res = await self.explain('''
            select RangeTest {id, mdate}
            filter bounded_above(
                .mdate,
                range(<cal::local_date>'2005-01-05',
                      <cal::local_date>'2012-02-10'),
            )
        ''')
        self.assert_index_in_plan(res, 'mdate')

    async def test_edgeql_explain_json_contains_01(self):
        res = await self.explain('''
            select JSONTest {id, val}
            filter contains(.val, <json>(b := 123))
        ''')
        res = res['fine_grained']

        if len(res['subplans']) >= 2:
            # Postgres version <16
            res = res['subplans'][1]
        else:
            # Postgres version >=16
            # Response seems to be inlined, so let's remove all but last thing
            # in the pipeline.
            res['pipeline'] = res['pipeline'][-1:]

        self.assert_plan(
            res,
            {
                "pipeline": [
                    {
                        "plan_type": "BitmapHeapScan",
                    },
                ],
                "subplans": [
                    {
                        "pipeline": [
                            {
                                "plan_type": "BitmapIndexScan",
                                "properties": tb.bag([
                                    {
                                        'important': False,
                                        'title': 'parent_relationship',
                                        'type': 'text',
                                        'value': 'Outer',
                                    },
                                    {
                                        'important': True,
                                        'title': 'index_name',
                                        'type': 'index',
                                        'value':
                                            f"index 'pg::gin' of object"
                                            f" type 'default::JSONTest'"
                                            f" on (.val)",
                                    },
                                    {
                                        'important': False,
                                        'title': 'index_cond',
                                        'type': 'expr',
                                    },
                                ]),
                            },
                        ],
                    },
                ],
            },
        )

    async def test_edgeql_explain_bug_5758(self):
        # Issue #5758
        res = await self.explain('''
            with
                module bug5758,
                user := (select User filter .id =
                    <uuid>'17b5649c-58b2-11ee-a739-4706f31ed5ab'),
                track := (select Track filter .id =
                    <uuid>'81958316-58d4-11ee-a739-9b645ff26c66'),
                shouldLike := (user not in track.liked_by)
            select (
                update track
                set {
                    liked_by := assert_distinct(
                        (.liked_by union user) if shouldLike
                        else (select .liked_by filter .id != user.id)
                    )
                }
            );
        ''', execute=False)
        # We use execute := False above because we actually don't have data,
        # but we can target the issue reliably with the "default" plan.
        #
        # The bug is that when coarse plan is generated "main_alias" may not
        # be found in the plan, causing the coarse plan to be None.
        #
        # Part of the problem with this kind of bug is that we can definitely
        # tell that having no coarse plan is cause by an exception, but
        # besides that it's much harder to validate that the actual "fixed"
        # coarse plan is "good".
        self.assertIsNotNone(res['coarse_grained'])

    async def test_edgeql_explain_bug_5791(self):
        # Issue #5758
        res = await self.explain('''
            with
                module bug5791,
                users := (
                    SELECT UserPreference
                    FILTER count(
                        File
                        FILTER
                            .userId = UserPreference.userId
                            AND .isPremium = false
                            AND .status = "PUBLISHED"
                    ) >= 3
                    AND .isHireable = true
                ),
                users_with_recent_files := (
                    SELECT users {
                        totalDownloadCount := sum((
                            SELECT File
                            FILTER
                                .userId = users.userId
                                AND .publishedAt >=
                                    <datetime>"2023-06-24T09:37:21.714Z"
                                AND .isPremium = false
                                AND .status = "PUBLISHED"
                            ).downloadCount
                        ),
                        files := (
                            SELECT File {
                                id,
                                name,
                                bgColor,
                                isSticker,
                                publishedAt,
                                status,
                                workflowId,
                                userTags,
                                userId,
                                lottieSource,
                                jsonSource,
                                imageSource
                            }
                            FILTER
                                .userId = users.userId
                                AND .publishedAt >=
                                    <datetime>"2023-06-24T09:37:21.714Z"
                                AND .isPremium = false
                                AND .status = "PUBLISHED"
                            ORDER BY .downloadCount DESC
                            LIMIT 3
                        )
                    }
                )
                SELECT users_with_recent_files {
                    userId,
                    isHireable,
                    totalDownloadCount,
                    files: {
                        id,
                        name,
                        bgColor,
                        isSticker,
                        publishedAt,
                        status,
                        workflowId,
                        userTags,
                        userId,
                        lottieSource,
                        jsonSource,
                        imageSource
                    }
                }
                ORDER BY .totalDownloadCount DESC
                OFFSET 0
                LIMIT 6
        ''', execute=False)
        # We use execute := False above because we actually don't have data,
        # but we can target the issue reliably with the "default" plan.
        #
        # The bug is that when coarse plan is generated "main_alias" may not
        # be found in the plan, causing the coarse plan to be None.
        #
        # Part of the problem with this kind of bug is that we can definitely
        # tell that having no coarse plan is cause by an exception, but
        # besides that it's much harder to validate that the actual "fixed"
        # coarse plan is "good".
        self.assertIsNotNone(res['coarse_grained'])


class NameTranslation(unittest.TestCase):

    def test_name_default(self):
        raliases = {'default': None}
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('default', 'Type1'), raliases),
            "Type1",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('mod1', 'Type2'), raliases),
            "mod1::Type2",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('m1::m2', 'Type3'), raliases),
            "m1::m2::Type3",
        )

    def test_name_aliases_01(self):
        raliases = {'mod1': None, 'mod2': 'main'}
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('default', 'Type1'), raliases),
            "default::Type1",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('mod1', 'Type2'), raliases),
            "Type2",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('mod2', 'Type3'), raliases),
            "main::Type3",
        )

    def test_name_aliases_nested_01(self):
        raliases = {'mod1': None, 'mod2': 'main', 'mod3::mod4': 'aux'}
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('default', 'Type1'), raliases),
            "default::Type1",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('mod1::mod2', 'Type2'),
                                    raliases),
            # default module is not replaced if there is nesting
            "mod1::mod2::Type2",
        )
        self.assertEqual(
            pg_tree._translate_name(sn.QualName('mod3::mod4::mod5', 'Type3'),
                                    raliases),
            "aux::mod5::Type3",
        )
        self.assertEqual(
            pg_tree._translate_name(
                sn.QualName('mod3::mod4::mod5::mod6', 'Type4'),
                raliases,
            ),
            "aux::mod5::mod6::Type4",
        )
        self.assertEqual(
            pg_tree._translate_name(
                sn.QualName('mod3::mod7', 'Type5'),
                raliases,
            ),
            "mod3::mod7::Type5",
        )
        self.assertEqual(
            pg_tree._translate_name(
                sn.QualName('mod2::mod3::mod4', 'Type6'),
                raliases,
            ),
            "main::mod3::mod4::Type6",
        )
