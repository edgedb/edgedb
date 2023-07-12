#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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

from edb import errors

from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import parser as qlparser


class TestEdgeQLIRScopeTree(tb.BaseEdgeQLCompilerTest):
    """Unit tests for scope tree logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                apply_query_rewrites=False,
                modaliases={None: 'default'},
            )
        )

        root = ir.scope_tree
        if len(root.children) != 1:
            self.fail(
                f'Scope tree root is expected to have only one child, got'
                f' {len(root.children)}'
                f' \n{root.pformat()}'
            )

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=3, col=16)
    def test_edgeql_ir_scope_tree_bad_01(self):
        """
        SELECT User.deck
        FILTER User.name
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=3, col=16)
    def test_edgeql_ir_scope_tree_bad_02(self):
        """
        SELECT User.deck
        FILTER User.deck@count
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=2, col=35)
    def test_edgeql_ir_scope_tree_bad_03(self):
        """
        SELECT User.deck { foo := User }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=2, col=40)
    def test_edgeql_ir_scope_tree_bad_04(self):
        """
        UPDATE User.deck SET { name := User.name }
        """

    def test_edgeql_ir_scope_tree_bad_05(self):
        """
        WITH
            U := User {id, r := random()}
        SELECT
            (
                users := array_agg((SELECT U.id ORDER BY U.r LIMIT 10))
            )
        """
        # This one is fine now, since it is a property

    @tb.must_fail(errors.InvalidReferenceError,
                  "cannot reference correlated set 'User' here",
                  line=2, col=45)
    def test_edgeql_ir_scope_tree_bad_06(self):
        """
        UPDATE User SET { avatar := (UPDATE .avatar SET { text := "foo" }) }
        """
