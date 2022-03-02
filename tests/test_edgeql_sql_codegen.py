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

from edb.testbase import lang as tb

from edb.common.ast import visitor as ast_visitor

from edb.edgeql import compiler
from edb.edgeql import parser as qlparser
from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pg_compiler


class TestEdgeQLSQLCodegen(tb.BaseEdgeQLCompilerTest):
    """Tests for specfic details of the generated SQL.

    Tests can be written by inspecting the AST or the generated text.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    def _compile_to_tree(self, source):
        qltree = qlparser.parse(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            ),
        )
        return pg_compiler.compile_ir_to_sql_tree(
            ir,
            output_format=pg_compiler.OutputFormat.NATIVE,
        )

    def _compile(self, source):
        qtree = self._compile_to_tree(source)
        return pg_compiler.run_codegen(qtree)

    def no_self_join_test(self, query, tables):
        # Issue #2567: We generate a pointless self join

        sql = self._compile(query)

        for table in tables:
            # Make sure that table is only selected from *once* in the query
            table_obj = self.schema.get("default::" + table)
            count = sql.count(str(table_obj.id))
            self.assertEqual(
                count,
                1,
                f"{table} referenced more than once: {sql}")

    def test_codegen_no_self_join_single(self):
        self.no_self_join_test("SELECT Issue.status", ["Issue", "Status"])

    def test_codegen_no_self_join_multi(self):
        self.no_self_join_test("SELECT Issue.watchers.name", ["User"])

    def no_optional_test(self, query):
        sql = self._compile(query)

        # One distinguishing characteristic of an optional wrapper is
        # selecting '("m~1" = first_value("m~1") OVER ())'
        self.assertNotIn(
            "OVER ()", sql,
            "optional wrapper generated when it shouldn't be needed"
        )

    def test_codegen_elide_optional_wrapper_01(self):
        self.no_optional_test('''
            select Issue { te := .time_estimate ?? -1 }
        ''')

    def test_codegen_elide_optional_wrapper_02(self):
        self.no_optional_test('''
            SELECT (Issue.name, Issue.time_estimate ?= 60)
        ''')

    def test_codegen_elide_optional_wrapper_03(self):
        self.no_optional_test('''
            SELECT opt_test(0, <str>Issue.time_estimate)
        ''')

    def test_codegen_elide_optional_wrapper_04(self):
        self.no_optional_test('''
            SELECT (Issue, opt_test(0, <str>Issue.time_estimate))
        ''')

    def test_codegen_order_by_not_subquery_01(self):
        sql = self._compile_to_tree('''
            select User order by .name
        ''')
        child = ast_visitor.find_children(
            sql,
            lambda x: isinstance(x, pgast.SelectStmt) and x.sort_clause,
            terminate_early=True
        )

        # Make sure that a simple order by on a property is not compiled
        # as a subquery in the ORDER BY, which pg fails to use an index for.
        self.assertIsInstance(
            child.sort_clause[0].node, pgast.ColumnRef,
            "simple sort clause is not a column ref",
        )

    def test_codegen_order_by_not_subquery_02(self):
        # Same as above but a bit more involved
        sql = self._compile_to_tree('''
            select User { z := .name ++ "!" } order by .z
        ''')
        child = ast_visitor.find_children(
            sql,
            lambda x: isinstance(x, pgast.SelectStmt) and x.sort_clause,
            terminate_early=True
        )

        # Make sure that a simple order by on a property is not compiled
        # as a subquery in the ORDER BY, which pg fails to use an index for.
        self.assertIsInstance(
            child.sort_clause[0].node, pgast.Expr,
            "simple sort clause is not a op expr",
        )
