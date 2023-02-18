#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import Sequence

from typing import *

from edb import errors

from edb.pgsql import common
from edb.pgsql import ast as pgast
from edb.common.ast import codegen
from edb.common import exceptions
from edb.common import markup


class SQLSourceGeneratorContext(markup.MarkupExceptionContext):
    title = 'SQL Source Generator Context'

    def __init__(
        self,
        node: pgast.Base,
        chunks_generated: Optional[Sequence[str]] = None,
    ):
        self.node = node
        self.chunks_generated = chunks_generated

    @classmethod
    def as_markup(cls: Any, self: Any, *, ctx: Any):  # type: ignore
        me = markup.elements

        body = [
            me.doc.Section(
                title='SQL Tree',
                body=[markup.serialize(self.node, ctx=ctx)],  # type: ignore
            )
        ]

        if self.chunks_generated:
            code = markup.serializer.serialize_code(
                ''.join(self.chunks_generated), lexer='sql'
            )
            body.append(
                me.doc.Section(
                    title='SQL generated so far', body=[code]  # type: ignore
                )
            )

        return me.lang.ExceptionContext(
            title=self.title, body=body  # type: ignore
        )


class SQLSourceGeneratorError(errors.InternalServerError):
    def __init__(
        self,
        msg: str,
        *,
        node: Optional[pgast.Base] = None,
        details: Optional[str] = None,
        hint: Optional[str] = None,
    ) -> None:
        super().__init__(msg, details=details, hint=hint)
        if node is not None:
            ctx = SQLSourceGeneratorContext(node)
            exceptions.add_context(self, ctx)


class SQLSourceGenerator(codegen.SourceGenerator):
    def __init__(self, *args, reordered=False, **kwargs):  # type: ignore
        super().__init__(*args, **kwargs)
        self.param_index: dict[object, int] = {}
        self.reordered = reordered

    @classmethod
    def to_source(  # type: ignore
        cls,
        node: pgast.Base,
        indent_with: str = ' ' * 4,
        add_line_information: bool = False,
        pretty: bool = True,
        reordered: bool = False,
    ) -> str:
        try:
            return super().to_source(
                node,
                indent_with=indent_with,
                reordered=reordered,
                add_line_information=add_line_information,
                pretty=pretty,
            )
        except SQLSourceGeneratorError as e:
            ctx = SQLSourceGeneratorContext(node)
            exceptions.add_context(e, ctx)
            raise

    @classmethod
    def ctes_to_source(cls, ctes: List[pgast.CommonTableExpr]) -> str:
        generator = cls()
        generator.gen_ctes(ctes)
        return ''.join(generator.result)

    def generic_visit(self, node):  # type: ignore
        raise SQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__
        )

    def gen_ctes(self, ctes: List[pgast.CommonTableExpr]) -> None:
        count = len(ctes)
        for i, cte in enumerate(ctes):
            self.new_lines = 1
            if getattr(cte, 'recursive', None):
                self.write('RECURSIVE ')
            self.write(common.quote_ident(cte.name))

            if cte.aliascolnames:
                self.write('(')
                for (index, col_name) in enumerate(cte.aliascolnames):
                    self.write(common.qname(col_name))
                    if index + 1 < len(cte.aliascolnames):
                        self.write(',')
                self.write(')')

            self.write(' AS ')
            if cte.materialized is not None:
                if cte.materialized:
                    self.write('MATERIALIZED ')
                else:
                    self.write('NOT MATERIALIZED ')
            self.indentation += 1
            self.new_lines = 1
            self.write('(')
            self.visit(cte.query)
            self.write(')')
            if i != count - 1:
                self.write(',')
            self.indentation -= 1

        self.new_lines = 1

    def visit__Ref(self, node):  # type: ignore
        self.visit(node.node)

    def visit_Relation(self, node: pgast.Relation) -> None:
        assert node.name
        if node.schemaname is None:
            self.write(common.qname(node.name))
        else:
            self.write(common.qname(node.schemaname, node.name))

    def _visit_values_expr(self, node: pgast.SelectStmt) -> None:
        assert node.values
        self.new_lines = 1
        self.write('(')
        self.write('VALUES')
        self.new_lines = 1
        self.indentation += 1
        self.visit_list(node.values)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_NullRelation(self, node: pgast.NullRelation) -> None:
        self.write('(SELECT ')
        if node.target_list:
            self.visit_list(node.target_list)
        if node.where_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where_clause)
            self.indentation -= 2
        self.write(')')

    def visit_SelectStmt(self, node: pgast.SelectStmt) -> None:
        if node.values:
            self._visit_values_expr(node)
            return

        # This is a very crude detection of whether this SELECT is
        # a top level statement.
        parenthesize = bool(self.result)

        if parenthesize:
            if not self.reordered:
                self.new_lines = 1
            self.write('(')
            if self.reordered:
                self.new_lines = 1
                if not node.op:
                    self.indentation += 1

        if node.ctes:
            self.write('WITH ')
            self.gen_ctes(node.ctes)

        # If reordered is True, we try to put the FROM clause *before* SELECT,
        # like it *ought* to be. We do various hokey things to try to make
        # that look good.
        # Otherwise we emit real SQL.
        def _select() -> None:
            self.write('SELECT')
            if node.distinct_clause:
                self.write(' DISTINCT')
                if len(node.distinct_clause) > 1 or not isinstance(
                    node.distinct_clause[0], pgast.Star
                ):
                    self.write(' ON (')
                    self.visit_list(node.distinct_clause, newlines=False)
                    self.write(')')
            if self.pretty:
                self.write('/*', repr(node), '*/')
            self.new_lines = 1

        if node.op:
            # Upper level set operation node (UNION/INTERSECT)
            self.visit(node.larg)
            self.write(' ' + node.op + ' ')
            if node.all:
                self.write('ALL ')
            self.visit(node.rarg)
        else:
            if not self.reordered:
                _select()
                self.indentation += 2

        if not self.reordered:
            if node.target_list:
                self.visit_list(node.target_list)

            if not node.op:
                self.indentation -= 2

        if node.from_clause:
            if not self.reordered:
                self.indentation += 1
                self.new_lines = 1
            self.write('FROM ')
            if not self.reordered:
                self.new_lines = 1
                self.indentation += 1
            self.visit_list(node.from_clause)
            if self.reordered:
                self.new_lines = 1
            else:
                self.indentation -= 2

        if self.reordered and not node.op:
            _select()

            self.indentation += 1

            if node.target_list:
                self.visit_list(node.target_list)

            # In reordered mode, we don't want to indent the clauses,
            # so we overreduce the indentation at this point and fix
            # it up at the end
            self.indentation -= 2

        if node.where_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where_clause)
            self.indentation -= 2

        if node.group_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('GROUP BY')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.group_clause)
            self.indentation -= 2

        if node.having:
            self.indentation += 1
            self.new_lines = 1
            self.write('HAVING')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.having)
            self.indentation -= 2

        if node.sort_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('ORDER BY')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.sort_clause)
            self.indentation -= 2

        if node.limit_offset:
            self.indentation += 1
            self.new_lines = 1
            self.write('OFFSET ')
            self.visit(node.limit_offset)
            self.indentation -= 1

        if node.limit_count:
            self.indentation += 1
            self.new_lines = 1
            self.write('LIMIT ')
            self.visit(node.limit_count)
            self.indentation -= 1

        if self.reordered and not node.op:
            self.indentation += 1

        if parenthesize:
            self.new_lines = 1
            if self.reordered and not node.op:
                self.indentation -= 1
            self.write(')')

    def visit_InsertStmt(self, node: pgast.InsertStmt) -> None:
        if node.ctes:
            self.write('WITH ')
            self.gen_ctes(node.ctes)

        self.write('INSERT INTO ')
        self.visit(node.relation)
        if node.cols:
            self.new_lines = 1
            self.indentation += 1
            self.write('(')
            self.visit_list(node.cols, newlines=False)
            self.write(')')
            self.indentation -= 1

        self.indentation += 1
        self.new_lines = 1

        if node.select_stmt:
            if (
                isinstance(node.select_stmt, pgast.SelectStmt)
                and node.select_stmt.values
            ):
                self.write('VALUES ')
                self.new_lines = 1
                self.indentation += 1
                self.visit_list(node.select_stmt.values)
                self.indentation -= 1
            else:
                self.write('(')
                self.visit(node.select_stmt)
                self.write(')')

        if node.on_conflict:
            self.new_lines = 1
            self.write('ON CONFLICT')

            if node.on_conflict.infer:
                self.visit(node.on_conflict.infer)

            self.write(' DO ')
            self.write(node.on_conflict.action.upper())

            if node.on_conflict.target_list:
                self.write(' SET')
                self.new_lines = 1
                self.indentation += 1
                self.visit_list(node.on_conflict.target_list)
                self.indentation -= 1

        if node.returning_list:
            self.new_lines = 1
            self.write('RETURNING')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.returning_list)
            self.indentation -= 1

        self.indentation -= 1

    def visit_UpdateStmt(self, node: pgast.UpdateStmt) -> None:
        if node.ctes:
            self.write('WITH ')
            self.gen_ctes(node.ctes)

        self.write('UPDATE ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.relation)
        self.indentation -= 1
        self.new_lines = 1
        self.write('SET')

        self.new_lines = 1
        self.indentation += 1
        self.visit_list(node.targets)
        self.indentation -= 1

        if node.from_clause:
            self.new_lines = 1
            self.write('FROM')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.from_clause)
            self.indentation -= 1

        if node.where_clause:
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where_clause)
            self.new_lines = 1
            self.indentation -= 1

        if node.returning_list:
            self.new_lines = 1
            self.write('RETURNING')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.returning_list)
            self.indentation -= 1

    def visit_DeleteStmt(self, node: pgast.DeleteStmt) -> None:
        if node.ctes:
            self.write('WITH ')
            self.gen_ctes(node.ctes)

        self.write('DELETE FROM ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.relation)
        self.indentation -= 1

        if node.using_clause:
            self.new_lines = 1
            self.write('USING')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.using_clause)
            self.indentation -= 1

        if node.where_clause:
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where_clause)
            self.new_lines = 1
            self.indentation -= 1

        if node.returning_list:
            self.new_lines = 1
            self.write('RETURNING')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.returning_list)
            self.indentation -= 1

    def visit_InferClause(self, node: pgast.InferClause) -> None:
        assert not node.conname or not node.index_elems
        if node.conname:
            self.write(' ON CONSTRAINT ')
            self.write(node.conname)
        if node.index_elems:
            self.write(' (')
            self.visit_list(node.index_elems, newlines=False)
            self.write(')')

    def visit_MultiAssignRef(self, node: pgast.MultiAssignRef) -> None:
        self.write('(')
        self.visit_list(node.columns, newlines=False)
        self.write(') = ')
        self.visit(node.source)

    def visit_LiteralExpr(self, node: pgast.LiteralExpr) -> None:
        self.write(node.expr)

    def visit_ResTarget(self, node: pgast.ResTarget) -> None:
        self.visit(node.val)
        if node.indirection:
            self._visit_indirection_ops(node.indirection)
        if node.name:
            self.write(' AS ' + common.quote_ident(node.name))

    def visit_InsertTarget(self, node: pgast.InsertTarget) -> None:
        self.write(common.quote_ident(node.name))

    def visit_UpdateTarget(self, node: pgast.UpdateTarget) -> None:
        if isinstance(node.name, list):
            self.write('(')
            self.write(', '.join(common.quote_ident(n) for n in node.name))
            self.write(')')
        else:
            self.write(common.quote_ident(node.name))
        if node.indirection:
            self._visit_indirection_ops(node.indirection)
        self.write(' = ')
        self.visit(node.val)

    def visit_Alias(self, node: pgast.Alias) -> None:
        self.write(common.quote_ident(node.aliasname))
        if node.colnames:
            self.write('(')
            self.write(', '.join(common.quote_ident(n) for n in node.colnames))
            self.write(')')

    def visit_Keyword(self, node: pgast.Keyword) -> None:
        self.write(node.name)

    def visit_RelRangeVar(self, node: pgast.RelRangeVar) -> None:
        rel = node.relation

        if not node.include_inherited:
            self.write(' ONLY (')

        if isinstance(rel, (pgast.Relation, pgast.NullRelation)):
            self.visit(rel)
        elif isinstance(rel, pgast.CommonTableExpr):
            self.write(common.quote_ident(rel.name))
        else:
            raise SQLSourceGeneratorError(
                'unexpected relation in RelRangeVar: {!r}'.format(rel)
            )

        if not node.include_inherited:
            self.write(')')

        if node.alias and node.alias.aliasname:
            self.write(' AS ')
            self.visit(node.alias)

    def visit_RangeSubselect(self, node: pgast.RangeSubselect) -> None:
        if node.lateral:
            self.write('LATERAL ')

        self.visit(node.subquery)

        if node.alias and node.alias.aliasname:
            self.write(' AS ')
            self.visit(node.alias)

    def visit_RangeFunction(self, node: pgast.RangeFunction) -> None:
        if node.lateral:
            self.write('LATERAL ')

        if node.is_rowsfrom:
            self.write('ROWS FROM (')

        self.visit_list(node.functions)

        if node.is_rowsfrom:
            self.write(')')

        if node.with_ordinality:
            self.write(' WITH ORDINALITY ')

        if node.alias and node.alias.aliasname:
            self.write(' AS ')
            self.visit(node.alias)

    def visit_ColumnRef(self, node: pgast.ColumnRef) -> None:
        names = node.name
        if isinstance(names[-1], pgast.Star):
            self.write(common.qname(*names))
        else:
            if names == ['VALUE']:
                self.write('VALUE')
            elif names[0] in {'OLD', 'NEW'}:
                assert isinstance(names[0], str)
                self.write(names[0])
                if len(names) > 1:
                    self.write('.')
                    self.write(common.qname(*names[1:]))
            else:
                self.write(common.qname(*names))

    def visit_ExprOutputVar(self, node: pgast.ExprOutputVar) -> None:
        self.visit(node.expr)

    def visit_ColumnDef(self, node: pgast.ColumnDef) -> None:
        self.write(common.quote_ident(node.name))
        if node.typename:
            self.write(' ')
            self.visit(node.typename)

        if node.is_not_null:
            self.write(' NOT NULL')
        if node.default_expr:
            self.write(' DEFAULT ')
            self.visit(node.default_expr)

    def visit_GroupingOperation(self, node: pgast.GroupingOperation) -> None:
        if node.operation:
            self.write(node.operation)
            self.write(' ')
        self.write('(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_JoinExpr(self, node: pgast.JoinExpr) -> None:
        self.visit(node.larg)
        if node.rarg is not None:
            self.new_lines = 1
            if not node.quals and not node.using_clause:
                join_type = 'CROSS'
            else:
                join_type = node.type.upper()
            if join_type == 'INNER':
                self.write('JOIN ')
            else:
                self.write(join_type + ' JOIN ')
            nested_join = (
                isinstance(node.rarg, pgast.JoinExpr)
                and node.rarg.rarg is not None
            )
            if nested_join:
                self.write('(')
                self.new_lines = 1
                self.indentation += 1
            self.visit(node.rarg)
            if nested_join:
                self.indentation -= 1
                self.new_lines = 1
                self.write(')')
            if node.quals is not None:
                if not nested_join:
                    self.indentation += 1
                    self.new_lines = 1
                    self.write('ON ')
                else:
                    self.write(' ON ')
                self.visit(node.quals)
                if not nested_join:
                    self.indentation -= 1
            elif node.using_clause:
                self.write(" USING (")
                self.visit_list(node.using_clause)
                self.write(")")

    def visit_Expr(self, node: pgast.Expr) -> None:
        self.write('(')
        if node.lexpr is not None:
            self.visit(node.lexpr)
            self.write(' ')
        op = str(node.name)
        if '.' not in op:
            op = op.upper()
        self.write(op)
        if node.rexpr is not None:
            self.write(" ")
            self.visit_indented(node.rexpr, indent=op in {"OR", "AND"})
        self.write(")")

    def visit_NullConstant(self, _node: pgast.NullConstant) -> None:
        self.write('NULL')

    def visit_NumericConstant(self, node: pgast.NumericConstant) -> None:
        self.write(node.val)

    def visit_BooleanConstant(self, node: pgast.BooleanConstant) -> None:
        self.write('TRUE' if node.val else 'FALSE')

    def visit_StringConstant(self, node: pgast.StringConstant) -> None:
        self.write(common.quote_literal(node.val))

    def visit_ByteaConstant(self, node: pgast.ByteaConstant) -> None:
        self.write(common.quote_bytea_literal(node.val))

    def visit_ParamRef(self, node: pgast.ParamRef) -> None:
        self.write('$', str(node.number))

    def visit_RowExpr(self, node: pgast.RowExpr) -> None:
        self.write('ROW(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_ImplicitRowExpr(self, node: pgast.ImplicitRowExpr) -> None:
        self.write('(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_ArrayExpr(self, node: pgast.ArrayExpr) -> None:
        self.write('ARRAY[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_ArrayDimension(self, node: pgast.ArrayDimension) -> None:
        self.write('[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_VariadicArgument(self, node: pgast.VariadicArgument) -> None:
        self.write('VARIADIC ')
        self.visit(node.expr)

    def visit_FuncCall(self, node: pgast.FuncCall) -> None:
        self.write(common.qname(*node.name))

        self.write('(')
        if node.agg_star:
            self.write("*")
        elif node.agg_distinct:
            self.write('DISTINCT ')
        self.visit_list(node.args, newlines=False)

        if node.agg_order:
            self.write(' ORDER BY ')
            self.visit_list(node.agg_order, newlines=False)

        self.write(')')

        if node.agg_filter:
            self.write(' FILTER (WHERE ')
            self.visit(node.agg_filter)
            self.write(')')

        if node.over:
            self.write(' OVER (')
            if node.over.partition_clause:
                self.write('PARTITION BY ')
                self.visit_list(node.over.partition_clause, newlines=False)

            if node.over.order_clause:
                self.write(' ORDER BY ')
                self.visit_list(node.over.order_clause, newlines=False)

            # XXX: add support for frame definition

            self.write(')')

        if node.with_ordinality:
            self.write(' WITH ORDINALITY')

        if node.coldeflist:
            self.write(' AS (')
            self.visit_list(node.coldeflist, newlines=False)
            self.write(')')

    def visit_NamedFuncArg(self, node: pgast.NamedFuncArg) -> None:
        self.write(common.quote_ident(node.name), ' => ')
        self.visit(node.val)

    def visit_SubLink(self, node: pgast.SubLink) -> None:
        if node.test_expr:
            self.visit(node.test_expr)

        if node.operator:
            self.write(" " + node.operator + " ")
        self.visit_indented(node.expr, indent=True, nest=True)

    def visit_SortBy(self, node: pgast.SortBy) -> None:
        self.visit(node.node)
        if node.dir:
            direction = 'ASC' if node.dir == pgast.SortAsc else 'DESC'
            self.write(' ' + direction)

            if node.nulls is None:
                if node.dir == pgast.SortDesc:
                    self.write(' NULLS LAST')
                else:
                    self.write(' NULLS FIRST')
            elif node.nulls == pgast.NullsFirst:
                self.write(' NULLS FIRST')
            elif node.nulls == pgast.NullsLast:
                self.write(' NULLS LAST')
            else:
                raise SQLSourceGeneratorError(
                    'unexpected NULLS order: {}'.format(node.nulls)
                )

    def visit_TypeCast(self, node: pgast.TypeCast) -> None:
        # '::' has very high precedence, so parenthesize the expression.
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        self.write('::')
        self.visit(node.type_name)

    def visit_TypeName(self, node: pgast.TypeName) -> None:
        self.write(common.quote_type(node.name))
        if node.array_bounds:
            for array_bound in node.array_bounds:
                self.write('[')
                if array_bound >= 0:
                    self.write(str(array_bound))
                self.write(']')

    def visit_Star(self, _: pgast.Star) -> None:
        self.write('*')

    def visit_CaseExpr(self, node: pgast.CaseExpr) -> None:
        self.write('(CASE ')
        if node.arg:
            self.visit(node.arg)
            self.write(' ')
        for arg in node.args:
            self.visit(arg)
            self.new_lines = 1
        if node.defresult:
            self.write('ELSE ')
            self.visit(node.defresult)
            self.new_lines = 1
        self.write('END)')

    def visit_CaseWhen(self, node: pgast.CaseWhen) -> None:
        self.write('WHEN ')
        self.visit(node.expr)
        self.write(' THEN ')
        self.visit(node.result)

    def visit_NullTest(self, node: pgast.NullTest) -> None:
        self.write('(')
        self.visit(node.arg)
        if node.negated:
            self.write(' IS NOT NULL')
        else:
            self.write(' IS NULL')
        self.write(')')

    def visit_BooleanTest(self, node: pgast.BooleanTest) -> None:
        self.write("(")
        self.visit(node.arg)
        op = " IS"
        if node.negated:
            op += " NOT"
        if node.is_true:
            op += " TRUE"
        else:
            op += " FALSE"
        self.write(op)
        self.write(")")

    def visit_Indirection(self, node: pgast.Indirection) -> None:
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        self._visit_indirection_ops(node.indirection)

    def visit_RecordIndirectionOp(
        self, node: pgast.RecordIndirectionOp
    ) -> None:
        self.write('.')
        self.write(node.name)

    def _visit_indirection_ops(
        self, ops: Sequence[pgast.IndirectionOp]
    ) -> None:
        for op in ops:
            if isinstance(op, (pgast.Star, pgast.ColumnRef)):
                self.write('.')
            self.visit(op)

    def visit_Index(self, node: pgast.Index) -> None:
        self.write('[')
        self.visit(node.idx)
        self.write(']')

    def visit_Slice(self, node: pgast.Slice) -> None:
        self.write('[')
        if node.lidx is not None:
            self.visit(node.lidx)
        self.write(':')
        if node.ridx is not None:
            self.visit(node.ridx)
        self.write(']')

    def visit_CollateClause(self, node: pgast.CollateClause) -> None:
        self.visit(node.arg)
        self.write(f' COLLATE {node.collname}')

    def visit_CoalesceExpr(self, node: pgast.CoalesceExpr) -> None:
        self.write('COALESCE(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_AlterSystem(self, node: pgast.AlterSystem) -> None:
        self.write('ALTER SYSTEM ')
        if node.value is not None:
            self.write('SET ')
            self.write(common.quote_ident(node.name))
            self.write(' = ')
            self.visit(node.value)
        else:
            self.write('RESET ')
            self.write(common.quote_ident(node.name))

    def visit_Set(self, node: pgast.Set) -> None:
        if node.value is not None:
            self.write('SET ')
            self.write(common.quote_ident(node.name))
            self.write(' = ')
            self.visit(node.value)
        else:
            self.write('RESET ')
            self.write(common.quote_ident(node.name))

    def visit_VariableSetStmt(self, node: pgast.VariableSetStmt) -> None:
        self.write("SET ")
        if node.scope == pgast.OptionsScope.TRANSACTION:
            self.write("LOCAL ")
        self.write(node.name)
        self.write(" TO ")
        self.visit(node.args)

    def visit_ArgsList(self, node: pgast.ArgsList) -> None:
        self.visit_list(node.args)

    def visit_VariableResetStmt(self, node: pgast.VariableResetStmt) -> None:
        if node.name is None:
            assert node.scope == pgast.OptionsScope.SESSION
            self.write("RESET ALL")
        else:
            self.write("SET ")
            if node.scope == pgast.OptionsScope.TRANSACTION:
                self.write("LOCAL ")
            self.write(node.name)
            self.write(" TO DEFAULT")

    def visit_SetTransactionStmt(self, node: pgast.SetTransactionStmt) -> None:
        self.write("SET ")
        if node.scope == pgast.OptionsScope.TRANSACTION:
            self.write("TRANSACTION ")
        else:
            self.write("SESSION CHARACTERISTICS AS TRANSACTION ")
        self.visit(node.options)

    def visit_VariableShowStmt(self, node: pgast.VariableShowStmt) -> None:
        self.write("SHOW ")
        self.write(node.name)

    def visit_BeginStmt(self, node: pgast.BeginStmt) -> None:
        self.write("BEGIN")
        if node.options:
            self.visit(node.options)

    def visit_StartStmt(self, node: pgast.StartStmt) -> None:
        self.write("START TRANSACTION")
        if node.options:
            self.visit(node.options)

    def visit_CommitStmt(self, node: pgast.CommitStmt) -> None:
        self.write("COMMIT")
        if node.chain:
            self.write(" AND CHAIN")

    def visit_RollbackStmt(self, node: pgast.RollbackStmt) -> None:
        self.write("ROLLBACK")
        if node.chain:
            self.write(" AND CHAIN")

    def visit_SavepointStmt(self, node: pgast.SavepointStmt) -> None:
        self.write(f"SAVEPOINT {node.savepoint_name}")

    def visit_ReleaseStmt(self, node: pgast.ReleaseStmt) -> None:
        self.write(f"RELEASE {node.savepoint_name}")

    def visit_RollbackToStmt(self, node: pgast.RollbackToStmt) -> None:
        self.write(f"ROLLBACK TO SAVEPOINT {node.savepoint_name}")

    def visit_PrepareTransaction(self, node: pgast.PrepareTransaction) -> None:
        self.write(f"PREPARE TRANSACTION '{node.gid}'")

    def visit_CommitPreparedStmt(self, node: pgast.CommitPreparedStmt) -> None:
        self.write(f"COMMIT PREPARED '{node.gid}'")

    def visit_RollbackPreparedStmt(
        self, node: pgast.RollbackPreparedStmt
    ) -> None:
        self.write(f"ROLLBACK PREPARED '{node.gid}'")

    def visit_TransactionOptions(self, node: pgast.TransactionOptions) -> None:
        for def_name, arg in node.options.items():
            if def_name == "transaction_isolation":
                self.write(" ISOLATION LEVEL ")
                if isinstance(arg, pgast.StringConstant):
                    self.write(arg.val.upper())
            elif def_name == "transaction_read_only":
                if isinstance(arg, pgast.NumericConstant):
                    if arg.val == "1":
                        self.write(" READ ONLY")
                    else:
                        self.write(" READ WRITE")
            elif def_name == "transaction_deferrable":
                if isinstance(arg, pgast.NumericConstant):
                    if arg.val != "1":
                        self.write(" NOT")
                    self.write(" DEFERRABLE")

    def visit_PrepareStmt(self, node: pgast.PrepareStmt) -> None:
        self.write(f"PREPARE {node.name}")
        if node.argtypes:
            self.write(f"(")
            self.visit_list(node.argtypes, newlines=False)
            self.write(f")")
        self.write(f" AS ")
        self.visit(node.query)

    def visit_ExecuteStmt(self, node: pgast.ExecuteStmt) -> None:
        self.write(f"EXECUTE {node.name}")
        if node.params:
            self.write(f"(")
            self.visit_list(node.params, newlines=False)
            self.write(f")")

    def visit_SQLValueFunction(self, node: pgast.SQLValueFunction) -> None:
        from edb.pgsql.ast import SQLValueFunctionOP as op

        names = {
            op.CURRENT_DATE: "current_date",
            op.CURRENT_TIME: "current_time",
            op.CURRENT_TIME_N: "current_time",
            op.CURRENT_TIMESTAMP: "current_timestamp",
            op.CURRENT_TIMESTAMP_N: "current_timestamp",
            op.LOCALTIME: "localtime",
            op.LOCALTIME_N: "localtime",
            op.LOCALTIMESTAMP: "localtimestamp",
            op.LOCALTIMESTAMP_N: "localtimestamp",
            op.CURRENT_ROLE: "current_role",
            op.CURRENT_USER: "current_user",
            op.USER: "user",
            op.SESSION_USER: "session_user",
            op.CURRENT_CATALOG: "current_catalog",
            op.CURRENT_SCHEMA: "current_schema",
        }

        self.write(names[node.op])
        if node.arg:
            self.write("(")
            self.visit(node.arg)
            self.write(")")

    def visit_CreateStmt(self, node: pgast.CreateStmt) -> None:
        self.write('CREATE ')
        if node.relation.is_temporary:
            self.write('TEMPORARY ')
        self.write('TABLE ')
        self.visit_Relation(node.relation)

        if node.table_elements:
            self.write(' (')
            self.visit_list(node.table_elements)
            self.write(')')

        if node.on_commit:
            self.write(' ON COMMIT ')
            self.write(node.on_commit)

    def visit_CreateTableAsStmt(self, node: pgast.CreateTableAsStmt) -> None:
        self.visit(node.into)
        self.write(' AS ')
        self.visit(node.query)

        if node.with_no_data:
            self.write(' WITH NO DATA')

    def visit_MinMaxExpr(self, node: pgast.MinMaxExpr) -> None:
        self.write(node.op)
        self.write('(')
        self.visit_list(node.args)
        self.write(')')

    def visit_LockStmt(self, node: pgast.LockStmt) -> None:
        self.write('LOCK TABLE ')
        self.visit_list(node.relations)
        self.write(' IN ')
        self.write(node.mode)
        self.write(' MODE')
        if node.no_wait:
            self.write(' NOWAIT')


generate_source = SQLSourceGenerator.to_source
