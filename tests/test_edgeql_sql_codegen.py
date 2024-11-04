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
from edb.pgsql import codegen as pg_codegen


class TestEdgeQLSQLCodegen(tb.BaseEdgeQLCompilerTest):
    """Tests for specfic details of the generated SQL.

    Tests can be written by inspecting the AST or the generated text.
    """

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SCHEMA_cards = os.path.join(os.path.dirname(__file__), 'schemas',
                                'cards.esdl')

    @classmethod
    def get_schema_script(cls):
        script = super().get_schema_script()
        # Setting internal params like is_inlined in the schema
        # doesn't work right so we override the script to add DDL.
        return script + '''
            create function cards::ins_bot(name: str) -> cards::Bot {
                set is_inlined := true;
                using (insert cards::Bot { name := "asdf" });
            };
        '''

    def _compile_to_tree(self, source):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            ),
        )
        sql_res = pg_compiler.compile_ir_to_sql_tree(
            ir,
            output_format=pg_compiler.OutputFormat.NATIVE,
        )
        return sql_res.ast

    def _compile(self, source):
        qtree = self._compile_to_tree(source)
        return pg_codegen.generate_source(qtree, pretty=True)

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

    def test_codegen_elide_optional_wrapper_05(self):
        self.no_optional_test('''
            select Owned { z := .owner.name ?= <optional str>$0 }
        ''')

    def test_codegen_order_by_not_subquery_01(self):
        sql = self._compile_to_tree('''
            select User order by .name
        ''')
        child = ast_visitor.find_children(
            sql,
            pgast.SelectStmt,
            lambda x: bool(x.sort_clause),
            terminate_early=True
        )[0]

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
            pgast.SelectStmt,
            lambda x: bool(x.sort_clause),
            terminate_early=True
        )[0]

        # Make sure that a simple order by on a property is not compiled
        # as a subquery in the ORDER BY, which pg fails to use an index for.
        self.assertIsInstance(
            child.sort_clause[0].node, pgast.Expr,
            "simple sort clause is not a op expr",
        )

    def test_codegen_update_no_conflict_01(self):
        # Should have no conflict check because it has no subtypes
        sql = self._compile('''
            update User set { name := .name ++ '!' }
        ''')

        self.assertNotIn(
            "exclusion_violation", sql,
            "update has unnecessary conflict check"
        )

    SCHEMA_constraints = r'''
        type Foo {
            required property name -> str;
            property foo -> str { constraint exclusive; }
            property bar -> str;
            constraint exclusive on (.bar);
            property baz -> str;
        }
        type Bar extending Foo;
    '''

    def test_codegen_update_no_conflict_02(self):
        # Should have no conflict because baz has no exclusive constraints
        sql = self._compile('''
            update constraints::Foo set { baz := '!' }
        ''')

        self.assertNotIn(
            "exclusion_violation", sql,
            "update has unnecessary conflict check"
        )

    def test_codegen_group_simple_01(self):
        tree = self._compile_to_tree('''
        select (group Issue by .status) {
            name := .key.status.name,
            num := count(.elements),
        } order by .name
        ''')
        child = ast_visitor.find_children(
            tree,
            pgast.SelectStmt,
            lambda x: bool(x.group_clause),
            terminate_early=True
        )[0]
        group_sql = pg_codegen.generate_source(child, pretty=True)

        # We want no array_agg in the group - it should just be able
        # to do a count
        self.assertNotIn(
            "array_agg", group_sql,
            "group has unnecessary array_agg",
        )

        # And we want no uuid generation, which is a huge perf killer
        self.assertNotIn(
            "uuid_generate", group_sql,
            "group has unnecessary uuid_generate",
        )

    def test_codegen_group_simple_02(self):
        tree = self._compile_to_tree('''
        for g in (group Issue by .status)
        select (g.key.status.name, count(g.elements))
        ''')
        child = ast_visitor.find_children(
            tree,
            pgast.SelectStmt,
            lambda x: bool(x.group_clause),
            terminate_early=True
        )[0]
        group_sql = pg_codegen.generate_source(child, pretty=True)

        # We want no array_agg in the group - it should just be able
        # to do a count
        self.assertNotIn(
            "array_agg", group_sql,
            "group has unnecessary array_agg",
        )

        # And we want no uuid generation, which is a huge perf killer
        self.assertNotIn(
            "uuid_generate", group_sql,
            "group has unnecessary uuid_generate",
        )

    def test_codegen_group_binding(self):
        sql = self._compile('''
        with g := (group Issue by .status)
        select g {
            name := .key.status.name,
            num := count(.elements),
        } order by .name
        ''')

        self.assertNotIn(
            "array_agg", sql,
            "group has unnecessary array_agg",
        )

    def test_codegen_in_array_unpack_no_dupe(self):
        sql = self._compile('''
            select 1 in array_unpack(
                <array<int64>><array<str>>to_json('["1"]'))
        ''')

        count = sql.count('["1"]')
        self.assertEqual(
            count,
            1,
            f"argument needlessly duplicated")

    def test_codegen_filtered_link_no_semijoin(self):
        sql = self._compile('''
            select Named {
               [IS User].todo:{name}
            }
       ''')

        self.assertNotIn(
            " IN ", sql,
            "unexpected semi-join",
        )

    def test_codegen_chained_single_no_semijoin(self):
        sql = self._compile('''
            select Issue {
               z := .owner.todo
            }
       ''')

        self.assertNotIn(
            " IN ", sql,
            "unexpected semi-join",
        )

    def test_codegen_unless_conflict_link_no_semijoin(self):
        sql = self._compile('''
          with module cards
          insert User {
              name := "x",
              avatar := (select Card filter .name = 'Dragon')
          }
          unless conflict on (.avatar) else (User)
       ''')

        self.assertNotIn(
            " IN ", sql,
            "unexpected semi-join",
        )

    def test_codegen_order_by_param_compare(self):
        sql = self._compile('''
            select Issue { name }
            order by .name = <str>$0
       ''')
        count = sql.count('SELECT')
        self.assertEqual(
            count,
            1,
            f"ORDER BY subquery not optimized out")

    def test_codegen_tuples_no_extra_serialized(self):
        sql = self._compile('''
            select (select (1, 'foo'))
       ''')

        self.assertNotIn(
            "0_serialized~1", sql,
            "pointless extra query outputs",
        )

    def test_codegen_fts_search_no_score(self):
        sql = self._compile(
            '''
            select fts::search(Issue, 'spiced', language := 'eng').object
            '''
        )

        self.assertNotIn(
            "score_serialized",
            sql,
            "std::fts::search score should not be serialized when not needed",
        )

    def test_codegen_typeid_no_join(self):
        sql = self._compile(
            '''
            select Issue { name, number, tid := .__type__.id }
            '''
        )

        self.assertNotIn(
            "edgedbstd",
            sql,
            "typeid injection shouldn't joining ObjectType table",
        )

    def test_codegen_nested_for_no_uuid(self):
        sql = self._compile(
            '''
            for x in {1,2,3} union (for y in {3,4,5} union (x+y))
            '''
        )

        self.assertNotIn(
            "uuid_generate",
            sql,
            "unnecessary uuid_generate for FOR loop without volatility",
        )

    def test_codegen_linkprop_intersection_01(self):
        # Should have no conflict check because it has no subtypes
        sql = self._compile('''
            with module cards
            select User { deck[is SpecialCard]: { name, @count } }
        ''')

        card_obj = self.schema.get("cards::Card")
        self.assertNotIn(
            str(card_obj.id),
            sql,
            "Card being selected when SpecialCard should suffice"
        )

    def test_codegen_materialized_01(self):
        sql = self._compile('''
            with x := materialized(1 + 2)
            select ({x}, {x})
        ''')

        count = sql.count('+')
        self.assertEqual(
            count,
            1,
            f"addition not materialized")

    def test_codegen_materialized_02(self):
        sql = self._compile('''
            with x := materialized((
              select User { x := (1 + 2) } filter .name = 'Alice'
            ))
            select ({x {x}}, {x {x}})
        ''')

        count = sql.count('+')
        self.assertEqual(
            count,
            1,
            f"addition not materialized")

        count = sql.count('Alice')
        self.assertEqual(
            count,
            1,
            f"filter not materialized")

    def test_codegen_unless_conflict_01(self):
        # Should have no conflict check because it has no subtypes
        sql = self._compile('''
            insert User { name := "test" }
            unless conflict
        ''')

        self.assertIn(
            "ON CONFLICT", sql,
            "insert unless conflict not using ON CONFLICT"
        )

    def test_codegen_unless_conflict_02(self):
        # Should have no conflict check because it has no subtypes
        sql = self._compile('''
            insert User { name := "test" }
            unless conflict on (.name)
            else (User)
        ''')

        self.assertIn(
            "ON CONFLICT", sql,
            "insert unless conflict not using ON CONFLICT"
        )

    SCHEMA_asdf = r'''
        type Tgt;
        type Tgt2;
        type Src {
            name: str { constraint exclusive; }
            tgt: Tgt;
            multi tgts: Tgt2;
        };
    '''

    def test_codegen_unless_conflict_03(self):
        # Should have no conflict check because it has no subtypes
        sql = self._compile('''
        WITH MODULE asdf
        INSERT Src {
            name := 'asdf',
            tgt := (select Tgt limit 1),
            tgts := (insert Tgt2),
        } UNLESS CONFLICT
        ''')

        self.assertIn(
            "ON CONFLICT", sql,
            "insert unless conflict not using ON CONFLICT"
        )

    def test_codegen_inlined_insert_01(self):
        # Test that we don't use an overlay when selecting from a
        # simple function that does an INSERT.
        sql = self._compile('''
            WITH MODULE cards
            select ins_bot("asdf") { id, name }
        ''')

        table_obj = self.schema.get("cards::Bot")
        count = sql.count(str(table_obj.id))
        # The table should only be referenced once, in the INSERT.
        # If we reference it more than that, we're probably selecting it.
        self.assertEqual(
            count,
            1,
            f"Bot selected from and not just inserted: {sql}")

    def test_codegen_inlined_insert_02(self):
        # Test that we don't use an overlay when selecting from a
        # net::http::schedule_request
        sql = self._compile('''
            with
                nh as module std::net::http,
                url := <str>$url,
                request := (
                    nh::schedule_request(
                        url,
                        method := nh::Method.`GET`
                    )
                )
            select request {
                id,
                state,
                failure,
                response,
            }
        ''')

        table_obj = self.schema.get("std::net::http::ScheduledRequest")
        count = sql.count(str(table_obj.id))
        # The table should only be referenced once, in the INSERT.
        # If we reference it more than that, we're probably selecting it.
        self.assertEqual(
            count,
            1,
            f"ScheduledRequest selected from and not just inserted: {sql}")
