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

    async def test_edgeql_tree_delete_01(self):
        await self.con.execute(r"""
            DELETE Tree;
        """)
        await self.assert_query_result(
            r"""
                SELECT Tree;
            """,
            [],
        )

    async def test_edgeql_tree_delete_02(self):
        await self.con.execute(r"""
            DELETE Eert FILTER .val = '0';
        """)
        await self.assert_query_result(
            r"""
                SELECT Eert FILTER .val = '0';
            """,
            [],
        )

    @test.xerror('''
        This test fails with the following error:

        edgedb.errors.QueryError: invalid reference to Tree:
        self-referencing INSERTs are not allowed

        This test is part of several versions of how nested INSERT
        might function, but not necessarily legal in the end.

        See issue #1080 for details.
    ''')
    async def test_edgeql_tree_insert_01(self):
        # Test nested insert of a tree branch.
        await self.con.execute(r"""
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

    @test.xerror('''
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

    @test.xerror('''
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
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
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
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
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
                SELECT Tree.parent.parent.val;
            """,
            ['0'],
        )

    async def test_edgeql_tree_select_04(self):
        await self.assert_query_result(
            r"""
                SELECT Eert.parent.parent.val;
            """,
            ['0'],
        )

    async def test_edgeql_tree_select_05(self):
        await self.assert_query_result(
            r"""
                SELECT Eert.<children[IS Eert].<children[IS Eert].val;
            """,
            ['0'],
        )

    async def test_edgeql_tree_select_06(self):
        # 1098
        await self.assert_query_result(
            r"""
                SELECT Eert.children.children.val;
            """,
            {'000', '010'},
        )

    async def test_edgeql_tree_select_07(self):
        await self.assert_query_result(
            r"""
                SELECT Tree.children.children.val;
            """,
            {'000', '010'},
        )

    async def test_edgeql_tree_select_08(self):
        await self.assert_query_result(
            r"""
                SELECT Tree.<parent[IS Tree].<parent[IS Tree].val;
            """,
            {'000', '010'},
        )

    async def test_edgeql_tree_select_09(self):
        await self.assert_query_result(
            r"""
                SELECT Tree {val}
                FILTER
                    any(.children.children.val = '000')
                ORDER BY .val;
            """,
            [{'val': '0'}],
        )

    async def test_edgeql_tree_select_10(self):
        await self.assert_query_result(
            r"""
                SELECT Eert {val}
                FILTER
                    any(.children.children.val = '000')
                ORDER BY .val;
            """,
            [{'val': '0'}],
        )

    async def test_edgeql_tree_select_11(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := '010',
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER
                    NOT EXISTS .parent
                    AND x IN {
                        .val,
                        .children.val,
                        .children.children.val,
                        .children.children.children.val,
                    };
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
            ],
        )

    async def test_edgeql_tree_select_12(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := '12',
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER
                    NOT EXISTS .parent
                    AND x IN {
                        .val,
                        .children.val,
                        .children.children.val,
                        .children.children.children.val,
                    };
            """,
            [
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

    async def test_edgeql_tree_select_13(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := '010',
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER
                    NOT EXISTS .parent
                    AND x IN {
                        .val,
                        .children.val,
                        .children.children.val,
                        .children.children.children.val,
                    };
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
            ],
        )

    async def test_edgeql_tree_select_14(self):
        await self.assert_query_result(
            r"""
                WITH
                    x := '12',
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER
                    NOT EXISTS .parent
                    AND x IN {
                        .val,
                        .children.val,
                        .children.children.val,
                        .children.children.children.val,
                    };
            """,
            [
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

    async def test_edgeql_tree_update_01(self):
        # Update all the tree nodes to base their val on the children
        # vals.
        await self.con.execute(
            r"""
                UPDATE Tree
                SET {
                    val := array_join(
                        [.val, 'c'] ++ array_agg((
                            SELECT _ := .children.val
                            ORDER BY _
                        )),
                        '_'
                    )
                }
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER NOT EXISTS .parent
                ORDER BY .val;
            """,
            [
                {
                    'val': '0_c_00_01_02',
                    'children': [
                        {
                            'val': '00_c_000',
                            'children': [{'val': '000_c', 'children': []}]
                        },
                        {
                            'val': '01_c_010',
                            'children': [{'val': '010_c', 'children': []}]
                        },
                        {
                            'val': '02_c',
                            'children': []
                        },
                    ],
                },
                {
                    'val': '1_c_10_11_12_13',
                    'children': [
                        {'val': '10_c', 'children': []},
                        {'val': '11_c', 'children': []},
                        {'val': '12_c', 'children': []},
                        {'val': '13_c', 'children': []},
                    ],
                },
            ],
        )

    async def test_edgeql_tree_update_02(self):
        # Update all the tree nodes to base their val on the children
        # vals.
        await self.con.execute(
            r"""
                UPDATE Eert
                SET {
                    val := array_join(
                        [.val, 'c'] ++ array_agg((
                            SELECT _ := .children.val
                            ORDER BY _
                        )),
                        '_'
                    )
                }
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER NOT EXISTS .parent
                ORDER BY .val;
            """,
            [
                {
                    'val': '0_c_00_01_02',
                    'children': [
                        {
                            'val': '00_c_000',
                            'children': [{'val': '000_c', 'children': []}]
                        },
                        {
                            'val': '01_c_010',
                            'children': [{'val': '010_c', 'children': []}]
                        },
                        {
                            'val': '02_c',
                            'children': []
                        },
                    ],
                },
                {
                    'val': '1_c_10_11_12_13',
                    'children': [
                        {'val': '10_c', 'children': []},
                        {'val': '11_c', 'children': []},
                        {'val': '12_c', 'children': []},
                        {'val': '13_c', 'children': []},
                    ],
                },
            ],
        )

    async def test_edgeql_tree_update_03(self):
        # Update all the tree nodes to base their val on the parent
        # val.
        await self.con.execute(
            r"""
                UPDATE Tree
                SET {
                    val := .val ++ '_p' ++ (('_' ++ .parent.val) ?? '')
                };
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Tree {val}
                ORDER BY .val;
            """,
            [
                {'val': '000_p_00'},
                {'val': '00_p_0'},
                {'val': '010_p_01'},
                {'val': '01_p_0'},
                {'val': '02_p_0'},
                {'val': '0_p'},
                {'val': '10_p_1'},
                {'val': '11_p_1'},
                {'val': '12_p_1'},
                {'val': '13_p_1'},
                {'val': '1_p'},
            ]
        )

    async def test_edgeql_tree_update_04(self):
        # Update all the tree nodes to base their val on the parent
        # val.
        await self.con.execute(
            r"""
                UPDATE Eert
                SET {
                    val := .val ++ '_p' ++ (('_' ++ .parent.val) ?? '')
                };
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Eert {val}
                ORDER BY .val;
            """,
            [
                {'val': '000_p_00'},
                {'val': '00_p_0'},
                {'val': '010_p_01'},
                {'val': '01_p_0'},
                {'val': '02_p_0'},
                {'val': '0_p'},
                {'val': '10_p_1'},
                {'val': '11_p_1'},
                {'val': '12_p_1'},
                {'val': '13_p_1'},
                {'val': '1_p'},
            ]
        )

    async def test_edgeql_tree_update_05(self):
        # Swap around a tree node and its first child as an atomic operation.
        #
        # The same basic principle might be used to swap around 2
        # nodes in a linked-list, but we'll use trees since we have
        # them already.
        await self.con.execute(
            r"""
                WITH
                    # start with node '00'
                    T00 := (
                        SELECT Tree
                        FILTER .val = '00'
                    ),
                    # update its first child node ('000')
                    TC := (
                        UPDATE (SELECT T00.children
                        ORDER BY .val
                        LIMIT 1)
                        SET {parent := T00.parent}
                    ),
                UPDATE T00
                SET {parent := TC};
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER .val = '0'
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '000',
                            'children': [{'val': '00', 'children': []}]
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
            ],
        )

    @test.xerror('''
        This test fails with the following error:

        InternalServerError: ON CONFLICT DO UPDATE command cannot
        affect row a second time
    ''')
    async def test_edgeql_tree_update_06(self):
        # Swap around a tree node and its first child as an atomic operation.
        #
        # The same basic principle might be used to swap around 2
        # nodes in a linked-list, but we'll use trees since we have
        # them already.
        #
        # With this particular structure swapping nodes is awkward
        # because children lists for 3 nodes must be updated. The
        # whole point of this exercise it to try and express this as a
        # single command to make it atomic without needing to use
        # transactions.
        await self.con.execute(
            r"""
                WITH
                    # start with node '00'
                    T00 := (
                        SELECT Eert
                        FILTER .val = '00'
                    ),
                    # grab the parent of '00'
                    TP := T00.parent,
                    # update its first child node ('000')
                    TC := (
                        UPDATE (
                            SELECT T00.children
                            ORDER BY .val
                            LIMIT 1
                        )
                        SET {
                            children := {.children, T00}
                        }
                    ),
                    T00_up := (
                        UPDATE T00
                        SET {
                            children := (
                                SELECT _ := T00.children
                                FILTER _ != TC
                            )
                        }
                    ),
                # update the original parent of '00'
                UPDATE TP
                SET {
                    children := (
                        SELECT _ := {.children, T00_up}
                        FILTER _ != T00
                    )
                }
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER .val = '0'
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '000',
                            'children': [{'val': '00', 'children': []}]
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
            ],
        )

    async def test_edgeql_tree_update_07(self):
        # Swap around a tree node and its parent as an atomic operation.
        #
        # The same basic principle might be used to swap around 2
        # nodes in a linked-list, but we'll use trees since we have
        # them already.
        await self.con.execute(
            r"""
                WITH
                    # start with node '000', get its parent
                    TP := (
                        SELECT Tree
                        FILTER .val = '000'
                    ).parent,
                    # move the '000' node
                    T000 := (
                        UPDATE Tree
                        FILTER .val = '000'
                        SET {parent := .parent.parent}
                    )
                UPDATE TP
                SET {parent := T000};
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Tree {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER .val = '0'
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '000',
                            'children': [{'val': '00', 'children': []}]
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
            ],
        )

    async def test_edgeql_tree_update_08(self):
        # Swap around a tree node and its parent as an atomic operation.
        #
        # The same basic principle might be used to swap around 2
        # nodes in a linked-list, but we'll use trees since we have
        # them already.
        #
        # With this particular structure swapping nodes is awkward
        # because children lists for 3 nodes must be updated. The
        # whole point of this exercise it to try and express this as a
        # single command to make it atomic without needing to use
        # transactions.
        await self.con.execute(
            r"""
                WITH
                    # start with node '000'
                    T000 := (
                        SELECT Eert
                        FILTER .val = '000'
                    ),
                    # update the parent of '000'
                    TP := (
                        UPDATE (SELECT T000.parent)
                        SET {
                            children := (
                                SELECT _ := .children
                                FILTER _ != T000
                            )
                        }
                    ),
                    # update the grand-parent of '000'
                    TPP := (
                        UPDATE (SELECT TP.parent)
                        SET {
                            children := (
                                SELECT _ := assert_distinct({.children, T000})
                                FILTER _ != TP
                            )
                        }
                    )
                # update node '000'
                UPDATE (SELECT _ := TPP.children FILTER _ = T000)
                SET {
                    children := assert_distinct({.children, TP})
                };
            """
        )

        await self.assert_query_result(
            r"""
                SELECT Eert {
                    val,
                    children: {
                        val,
                        children: {
                            val,
                            children: {
                                val,
                            } ORDER BY .val,
                        } ORDER BY .val,
                    } ORDER BY .val,
                }
                FILTER .val = '0'
                ORDER BY .val;
            """,
            [
                {
                    'val': '0',
                    'children': [
                        {
                            'val': '000',
                            'children': [{'val': '00', 'children': []}]
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
            ],
        )

    async def test_edgeql_tree_update_09(self):
        # A trivial update that accesses children
        await self.assert_query_result(
            r"""
                select (update Tree filter .val = "00" set { }) {
                    children: {val}
                }
            """,
            [{"children": [{"val": "000"}]}],
        )

    async def test_edgeql_tree_update_10(self):
        # A real update that accesses children
        await self.assert_query_result(
            r"""
               select (
                    update Tree filter .val IN {"0", "00"}
                    set { parent := {} }
               ) {
                   val, children: {val} order by .val
               } order by .val;
            """,
            [
                {"children": [{"val": "01"}, {"val": "02"}], "val": "0"},
                {"children": [{"val": "000"}], "val": "00"}
            ]
        )
