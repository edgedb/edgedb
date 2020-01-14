#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
from edb.tools import test


class TestTree(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'tree.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'tree_setup.edgeql')

    @test.xfail('''
        This test fails with the following error:

        edgedb.errors.InternalServerError: relation
        "edgedb_e0b78b90-3693-11ea-9161-b92965214344.
        e0a71e4e-3693-11ea-bfb7-1dc5433a3c29" does not exist

        See issue #1097 for details.
    ''')
    async def test_edgeql_tree_delete_01(self):
        await self.con.execute(r"""
            DELETE test::Tree;
        """)
        await self.assert_query_result(
            r"""
                SELECT test::Tree;
            """,
            [],
        )

    @test.xfail('''
        This test fails with the following error:

        edgedb.errors.InternalServerError: relation
        "edgedb_09906334-3695-11ea-81b0-0b9acee64bce.
        0985cd78-3695-11ea-9ff3-3bca199ee0b1" does not exist

        See issue #1097 for details.
    ''')
    async def test_edgeql_tree_delete_02(self):
        await self.con.execute(r"""
            DELETE test::Eert;
        """)
        await self.assert_query_result(
            r"""
                SELECT test::Eert;
            """,
            [],
        )

    @test.xfail('''
        This test fails with the following error:

        edgedb.errors.QueryError: invalid reference to test::Tree:
        self-referencing INSERTs are not allowed

        This test is part of several versions of how nested INSERT
        might function, but not necessarily legal in the end.

        See issue #1080 for details.
    ''')
    async def test_edgeql_tree_insert_01(self):
        # Test nested insert of a tree branch.
        await self.con.execute(r"""
            WITH MODULE test
            INSERT Tree {
                val := 'i2',
                parent := (
                    INSERT Tree {
                        val := 'i1',
                        parent := (
                            INSERT Tree {
                                val := 'i0',
                            }
                        ),
                    }
                ),
            };
        """)
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            },
                        },
                    },
                }
                FILTER .val = 'i0';
            """,
            [{
                'val': 'i0',
                'children': [{
                    'val': 'i1',
                    'children': [{
                        'val': 'i2',
                    }],
                }],
            }],
        )

    @test.xfail('''
        This test fails with the following error:

        edgedb.errors.EdgeQLSyntaxError:
        insert expression must be an object type reference

        This test is part of several versions of how nested INSERT
        might function, but not necessarily legal in the end.

        See issue #1080 for details.
    ''')
    async def test_edgeql_tree_insert_02(self):
        # Test nested insert of a tree branch.
        await self.con.execute(r"""
            WITH MODULE test
            INSERT Eert {
                val := 'i0',
                children := (
                    INSERT DETACHED Eert {
                        val := 'i1',
                        children := (
                            INSERT DETACHED Eert {
                                val := 'i2',
                            }
                        ),
                    }
                ),
            };
        """)
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            },
                        },
                    },
                }
                FILTER .val = 'i0';
            """,
            [{
                'val': 'i0',
                'children': [{
                    'val': 'i1',
                    'children': [{
                        'val': 'i2',
                    }],
                }],
            }],
        )

    @test.xfail('''
        This test fails with the following error:

        edgedb.errors.QueryError:
        cannot insert into expression alias '__derived__::T1'

        This test is part of several versions of how nested INSERT
        might function, but not necessarily legal in the end.

        See issue #1080 for details.

        Unless we come up with aliases for types, this version should
        probably be illegal.
    ''')
    async def test_edgeql_tree_insert_03(self):
        # Test nested insert of a tree branch.
        await self.con.execute(r"""
            WITH
                MODULE test,
                T1 := Tree,
                T2 := Tree,
            INSERT Tree {
                val := 'i2',
                parent := (
                    INSERT T1 {
                        val := 'i1',
                        parent := (
                            INSERT T2 {
                                val := 'i0',
                            }
                        ),
                    }
                ),
            };
        """)
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            },
                        },
                    },
                }
                FILTER .val = 'i0';
            """,
            [{
                'val': 'i0',
                'children': [{
                    'val': 'i1',
                    'children': [{
                        'val': 'i2',
                    }],
                }],
            }],
        )

    async def test_edgeql_tree_select_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            },
                        },
                    },
                }
                FILTER NOT EXISTS .parent
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '00',
                            'children': [{'val': '000', 'children': []}]
                        },
                        {
                            'val': '01',
                            'children': [{'val': '010', 'children': []}]
                        },
                        {
                            'val': '02',
                            'children': []
                        },
                    ],
                },
                {
                    'val': '1',
                    'children': [
                        {'val': '10', 'children': []},
                        {'val': '11', 'children': []},
                        {'val': '12', 'children': []},
                        {'val': '13', 'children': []},
                    ],
                },
            ],
        )

    async def test_edgeql_tree_select_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            },
                        },
                    },
                }
                FILTER NOT EXISTS .parent
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '00',
                            'children': [{'val': '000', 'children': []}]
                        },
                        {
                            'val': '01',
                            'children': [{'val': '010', 'children': []}]
                        },
                        {
                            'val': '02',
                            'children': []
                        },
                    ],
                },
                {
                    'val': '1',
                    'children': [
                        {'val': '10', 'children': []},
                        {'val': '11', 'children': []},
                        {'val': '12', 'children': []},
                        {'val': '13', 'children': []},
                    ],
                },
            ],
        )

    async def test_edgeql_tree_select_03(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree.parent.parent.val;
            """,
            ['0'],
        )

    @test.xfail('''
        The query produces: ['0', '0']

        See issue #1098.
    ''')
    async def test_edgeql_tree_select_04(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert.parent.parent.val;
            """,
            ['0'],
        )

    async def test_edgeql_tree_select_05(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert.<children[IS Eert].<children[IS Eert].val;
            """,
            ['0'],
        )

    async def test_edgeql_tree_select_06(self):
        # 1098
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert.children.children.val;
            """,
            {'000', '010'},
        )

    @test.xfail('''
        This test fails with the following error:

        InternalServerError: missing FROM-clause entry for table
        "Tree_children~5"

        See issue #1098.
    ''')
    async def test_edgeql_tree_select_07(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree.children.children.val;
            """,
            {'000', '010'},
        )

    async def test_edgeql_tree_select_08(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree.<parent[IS Tree].<parent[IS Tree].val;
            """,
            {'000', '010'},
        )

    @test.xfail('''
        This test fails with the following error:

        InternalServerError: missing FROM-clause entry for table
        "default|Tree@@view~1_children~4"

        See issue #1098.
    ''')
    async def test_edgeql_tree_select_09(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Tree {val}
                FILTER
                    .children.children.val = '000'
                ORDER BY .val;
            """,
            [{'val': '0'}],
        )

    async def test_edgeql_tree_select_10(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT Eert {val}
                FILTER
                    .children.children.val = '000'
                ORDER BY .val;
            """,
            [{'val': '0'}],
        )
