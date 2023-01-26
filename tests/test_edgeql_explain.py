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


import json
import os.path
import unittest

from edb.testbase import server as tb
from edb.server import pgconnparams
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
        '''
    ]

    @classmethod
    async def _get_raw_sql_connection(cls):
        """Get a raw connection to the underlying SQL server, if possible

        We have to do this miserable hack in order to get access to ANALYZE
        """
        try:
            import asyncpg
        except ImportError:
            raise unittest.SkipTest(
                'explain tests skipped: asyncpg not installed')

        settings = cls.con.get_settings()
        pgaddr = settings.get('pgaddr')
        if pgaddr is None:
            raise unittest.SkipTest('explain tests skipped: not in devmode')
        pgaddr = json.loads(pgaddr)

        # Try to grab a password from the specified DSN, if one is
        # present, since the pgaddr won't have a real one. (The non
        # specified DSN test suite setup doesn't have one, so it is
        # fine.)
        password = None
        spec_dsn = os.environ.get('EDGEDB_TEST_BACKEND_DSN')
        if spec_dsn:
            _, params = pgconnparams.parse_dsn(spec_dsn)
            password = params.password

        pgdsn = (
            f'postgres:///{pgaddr["database"]}?user={pgaddr["user"]}'
            f'&port={pgaddr["port"]}&host={pgaddr["host"]}'
        )
        if password is not None:
            pgdsn += f'&password={password}'

        return await asyncpg.connect(pgdsn)

    @classmethod
    async def _analyze_db(cls):
        # HACK: Run ANALYZE so that test results are more deterministic
        scon = await cls._get_raw_sql_connection()
        try:
            await scon.execute('ANALYZE')
        finally:
            await scon.close()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls._analyze_db())

    def assert_plan(self, data, shape, message=None):
        assert_data_shape.assert_data_shape(
            data, shape, fail=self.fail, message=message)

    async def explain(self, query, *, analyze=False):
        return json.loads(await self.con.query_single(
            f'explain {"analyze " if analyze else ""}{query}'
        ))[0]

    async def test_edgeql_explain_simple_01(self):
        res = await self.explain('''
            select User { id, name } filter .name = 'Elvis'
        ''')
        self.assert_plan(res['Plan'], {
            'Node Type': 'Index Scan',
            'Relation Name': 'default::User',
            'Contexts': [[{'start': 28, 'end': 32, 'buffer_idx': 0}]],
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
            "Node Type": "Subquery Scan",
            "Plans": tb.bag([
                {
                    "Node Type": "Aggregate",
                    "Plans": [
                        {
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

                            "Node Type": "Index Scan",
                            "Parent Relationship": "Outer",
                            "Relation Name": "default::User",
                            "Contexts": [
                                [
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
                                ]
                            ]
                        }
                    ]
                },
                {
                    "Node Type": "Aggregate",
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "default::User",
                            "Contexts": [
                                [
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
                                ]
                            ]
                        }
                    ]
                },
                {
                    "Node Type": "Result",
                    "Output": [
                        "edgedbext.uuid_generate_v4()"
                    ]
                }
            ])
        }
        self.assert_plan(res['Plan'], shape)

    async def test_edgeql_explain_multi_link_01(self):
        res = await self.explain('''
            select User { name, todo: {name, number} }
            filter .name = 'Elvis';
        ''')
        shape = {
            "Node Type": "Index Scan",
            "Index Name": (
                "index 'name_9d90cf37' of object type 'default::User' index"),
            "Relation Name": "default::User",
            "Plans": [
                {
                    "Node Type": "Aggregate",
                    "Strategy": "Plain",
                    "Parent Relationship": "SubPlan",
                    "Subplan Name": "SubPlan 1",
                    "Plans": [
                        {
                            "Node Type": "Nested Loop",
                            "Parent Relationship": "Outer",
                            "Join Type": "Inner",
                            "Inner Unique": True,
                            "Plans": [
                                {
                                    "Node Type": "Index Only Scan",
                                    "Parent Relationship": "Outer",
                                    "Index Name": (
                                        "default::User.todo forward link index"
                                    ),
                                    "Relation Name": "default::User.todo",
                                    "Contexts": [
                                        [
                                            {
                                                "start": 41,
                                                "end": 45,
                                                "buffer_idx": 0
                                            }
                                        ]
                                    ]
                                },
                                {
                                    "Node Type": "Index Scan",
                                    "Parent Relationship": "Inner",
                                    "Index Name": (
                                        "constraint 'std::exclusive' of "
                                        "property 'id' of object type '"
                                        "default::Issue' exclusive constraint "
                                        "index"
                                    ),
                                    "Relation Name": "default::Issue",
                                    "Contexts": [
                                        [
                                            {
                                                "start": 41,
                                                "end": 45,
                                                "buffer_idx": 0
                                            }
                                        ]
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ],
            "Contexts": [
                [
                    {
                        "start": 28,
                        "end": 32,
                        "buffer_idx": 0
                    }
                ]
            ]
        }
        self.assert_plan(res['Plan'], shape)

    async def test_edgeql_explain_computed_backlink_01(self):
        res = await self.explain('''
            select User { name, owned_issues: {name, number} }
            filter .name = 'Elvis';
        ''')

        shape = {
            "Node Type": "Index Scan",
            "Index Name": (
                "index 'name_9d90cf37' of object type 'default::User' index"),
            "Relation Name": "default::User",
            "Plans": [
                {
                    "Node Type": "Aggregate",
                    "Parent Relationship": "SubPlan",
                    "Plans": [
                        {
                            "Node Type": "Result",
                            "Parent Relationship": "Outer",
                            "Plans": [
                                {
                                    "Node Type": "Bitmap Heap Scan",
                                    "Parent Relationship": "Outer",
                                    "Relation Name": "default::Issue",
                                    "Plans": [
                                        {
                                            "Node Type": "Bitmap Index Scan",
                                            "Parent Relationship": "Outer",
                                            "Index Name": (
                                                "default::Issue.owner index"),
                                        }
                                    ],
                                    # We get a stack of contexts back
                                    "Contexts": [
                                        [
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
                                        ]
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ],
            "Contexts": [
                [
                    {
                        "start": 28,
                        "end": 32,
                        "buffer_idx": 0
                    }
                ]
            ]
        }
        self.assert_plan(res['Plan'], shape)

        self.assertEqual(len(res['Buffers']), 2)
        self.assertEqual(res['Buffers'][1][0], ".<owner[is default::Issue]")

    async def test_edgeql_explain_inheritance_01(self):
        res = await self.explain('''
            WITH X := Text, select X;
        ''')

        shape = {
            "Node Type": "Result",
            "Plans": [
                {
                    "Node Type": "Append",
                    "Parent Relationship": "Outer",
                    "Plans": tb.bag([
                        {
                            "Node Type": "Seq Scan",
                            "Parent Relationship": "Member",
                            "Relation Name": "default::Issue",
                            "Original Relation Name": "default::Text",
                            "Contexts": [
                                [
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
                            ]
                        },
                        {
                            "Node Type": "Seq Scan",
                            "Parent Relationship": "Member",
                            "Relation Name": "default::Comment",
                            "Original Relation Name": "default::Text",
                            "Contexts": [
                                [
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
                            ]
                        },
                        {
                            "Node Type": "Seq Scan",
                            "Parent Relationship": "Member",
                            "Relation Name": "default::LogEntry",
                            "Original Relation Name": "default::Text",
                            "Contexts": [
                                [
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
                            ]
                        }
                    ])
                }
            ]
        }

        self.assert_plan(res['Plan'], shape)

    async def test_edgeql_explain_type_intersect_01(self):
        res = await self.explain('''
            select Text {
                body,
                z := [is Issue].name
            };
        ''')

        shape = {
            "Node Type": "Result",
            "Plans": tb.bag([
                {
                    "Node Type": "Append",
                    "Plans": tb.bag([
                        {
                            "Relation Name": "default::LogEntry",
                            "Original Relation Name": "default::Text",
                            "Contexts": [[{"text": "Text"}]]
                        },
                        {
                            "Relation Name": "default::Issue",
                            "Original Relation Name": "default::Text",
                            "Contexts": [[{"text": "Text"}]]
                        },
                        {
                            "Relation Name": "default::Comment",
                            "Original Relation Name": "default::Text",
                            "Contexts": [[{"text": "Text"}]]
                        }
                    ])
                },
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "default::Issue",
                    "Contexts": [
                        [
                            {"text": "[is Issue]"},
                            {"text": "[is Issue].name"},
                            {"text": "z := [is Issue].name"},
                        ]
                    ]
                }
            ])
        }

        self.assert_plan(res['Plan'], shape)
