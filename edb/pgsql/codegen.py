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

from typing import Any, Optional, Sequence, List

import abc
import collections
import dataclasses

from edb import errors

from edb.pgsql import common
from edb.pgsql import ast as pgast
from edb.common.ast import codegen
from edb.common import exceptions
from edb.common import markup


def generate(
    node: pgast.Base,
    *,
    indent_with: str = ' ' * 4,
    add_line_information: bool = False,
    pretty: bool = True,
    reordered: bool = False,
    with_source_map: bool = False,
) -> SQLSource:
    # Main entrypoint

    generator = SQLSourceGenerator(
        opts=codegen.Options(
            indent_with=indent_with,
            add_line_information=add_line_information,
            pretty=pretty,
        ),
        reordered=reordered,
        with_source_map=with_source_map,
    )

    try:
        generator.visit(node)
    except RecursionError:
        # Don't try to wrap and add context to a recursion error,
        # since the context might easily be too deeply recursive to
        # process further down the pipe.
        raise
    except GeneratorError as error:
        ctx = GeneratorContext(node, generator.result)
        exceptions.add_context(error, ctx)
        raise
    except Exception as error:
        ctx = GeneratorContext(node, generator.result)
        err = GeneratorError('error while generating SQL source')
        exceptions.add_context(err, ctx)
        raise err from error

    if with_source_map:
        assert generator.source_map

    return SQLSource(
        text=generator.finish(),
        source_map=generator.source_map,
        param_index=generator.param_index,
    )


def generate_source(
    node: pgast.Base,
    *,
    indent_with: str = ' ' * 4,
    add_line_information: bool = False,
    pretty: bool = False,
    reordered: bool = False,
) -> str:
    # Simplified entrypoint

    source = generate(
        node,
        indent_with=indent_with,
        add_line_information=add_line_information,
        pretty=pretty,
        reordered=reordered,
    )
    return source.text


def generate_ctes_source(
    ctes: List[pgast.CommonTableExpr],
) -> str:
    # Alternative simplified entrypoint generating 'WITH a AS (...)' only.

    generator = SQLSourceGenerator(opts=codegen.Options())
    generator.gen_ctes(ctes)

    return generator.finish()


class SourceMap:
    @abc.abstractmethod
    def translate(self, pos: int) -> int:
        ...


@dataclasses.dataclass(kw_only=True)
class BaseSourceMap(SourceMap):
    source_start: int
    output_start: int
    output_end: int | None = None
    children: List[BaseSourceMap] = (
        dataclasses.field(default_factory=list))

    def translate(self, pos: int) -> int:
        bu = None
        for u in self.children:
            if u.output_start >= pos:
                break
            bu = u
        if bu and (bu.output_end is None or bu.output_end > pos):
            return bu.translate(pos)
        return self.source_start


@dataclasses.dataclass
class ChainedSourceMap(SourceMap):
    parts: List[SourceMap] = (
        dataclasses.field(default_factory=list))

    def translate(self, pos: int) -> int:
        for part in self.parts:
            pos = part.translate(pos)
        return pos


@dataclasses.dataclass(frozen=True)
class SQLSource:
    text: str
    param_index: dict[int, list[int]]
    source_map: Optional[SourceMap] = None


class SQLSourceGenerator(codegen.SourceGenerator):
    def __init__(
        self,
        opts: codegen.Options,
        *,
        with_source_map: bool = False,
        reordered: bool = False,
    ):
        super().__init__(
            indent_with=opts.indent_with,
            add_line_information=opts.add_line_information,
            pretty=opts.pretty,
        )
        self.is_toplevel = True
        # params
        self.with_source_map: bool = with_source_map
        self.reordered = reordered

        # state
        self.param_index: collections.defaultdict[int, list[int]] = (
            collections.defaultdict(list))
        self.write_index: int = 0
        self.source_map: Optional[BaseSourceMap] = None

    def write(
        self,
        *x: str,
        delimiter: Optional[str] = None,
    ) -> None:
        self.is_toplevel = False
        start = len(self.result)
        super().write(*x, delimiter=delimiter)
        for new in range(start, len(self.result)):
            self.write_index += len(self.result[new])

    def visit(self, node):  # type: ignore
        if self.with_source_map:
            source_map = BaseSourceMap(
                source_start=node.span.start if node.span else 0,
                output_start=self.write_index,
            )
            old_top = self.source_map
            self.source_map = source_map
        super().visit(node)
        if self.with_source_map:
            assert self.source_map == source_map
            self.source_map.output_end = self.write_index
            if old_top:
                old_top.children.append(self.source_map)
                self.source_map = old_top

    def generic_visit(self, node):  # type: ignore
        raise GeneratorError(
            'No method to generate code for %s' % node.__class__.__name__
        )

    def gen_ctes(self, ctes: List[pgast.CommonTableExpr]) -> None:
        count = len(ctes)
        for i, cte in enumerate(ctes):
            self.new_lines = 1
            if i == 0 and getattr(cte, 'recursive', None):
                self.write('RECURSIVE ')
            self.write(common.quote_ident(cte.name))

            if cte.aliascolnames:
                self.write('(')
                for (index, col_name) in enumerate(cte.aliascolnames):
                    self.write(common.qname(col_name, column=True))
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
        parenthesize = not self.is_toplevel

        if parenthesize:
            if not self.reordered and self.result:
                self.new_lines = 1
            self.write('(')
            if self.reordered:
                self.new_lines = 1
                if not node.op:
                    self.indentation += 1

        if node.ctes:
            self.write('WITH ')
            self.gen_ctes(node.ctes)

        if node.values:
            self.write('VALUES')
            self.new_lines = 1
            self.visit_list(node.values)
            if parenthesize:
                self.new_lines = 1
                if self.reordered and not node.op:
                    self.indentation -= 1
                self.write(')')
            return

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

            # HACK: The LHS of a set operation is *not* top-level, and
            # shouldn't be treated as such. Since we (also hackily)
            # use whether anything has been written do determine
            # whether we are at the top level, write out an empty
            # string to force parenthesization.
            self.is_toplevel = False
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

        if node.having_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('HAVING')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.having_clause)
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

        if node.locking_clause:
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(node.locking_clause, separator=" ")
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
        else:
            self.write('DEFAULT VALUES')

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
        for index, col in enumerate(node.columns):
            if index > 0:
                self.write(', ')
            self.write(common.quote_col(col))
        self.write(') = ')
        self.visit(node.source)

    def visit_LiteralExpr(self, node: pgast.LiteralExpr) -> None:
        self.write(node.expr)

    def visit_ResTarget(self, node: pgast.ResTarget) -> None:
        self.visit(node.val)
        if node.name:
            self.write(' AS ' + common.quote_col(node.name))

    def visit_InsertTarget(self, node: pgast.InsertTarget) -> None:
        self.write(common.quote_col(node.name))

    def visit_UpdateTarget(self, node: pgast.UpdateTarget) -> None:
        if isinstance(node.name, list):
            self.write('(')
            self.write(', '.join(common.quote_col(n) for n in node.name))
            self.write(')')
        else:
            self.write(common.quote_col(node.name))
        if node.indirection:
            self._visit_indirection_ops(node.indirection)
        self.write(' = ')
        self.visit(node.val)

    def visit_Alias(self, node: pgast.Alias) -> None:
        self.write(common.quote_ident(node.aliasname))
        if node.colnames:
            self.write('(')
            self.write(', '.join(common.quote_col(n) for n in node.colnames))
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
            raise GeneratorError(
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
                    self.write(common.qname(*names[1:], column=True))
            else:
                self.write(common.qname(*names, column=True))

    def visit_ExprOutputVar(self, node: pgast.ExprOutputVar) -> None:
        self.visit(node.expr)

    def visit_ColumnDef(self, node: pgast.ColumnDef) -> None:
        self.write(common.quote_col(node.name))
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
        for join in node.joins:
            self.new_lines = 1
            if not join.quals and not join.using_clause:
                join_type = 'CROSS'
            else:
                join_type = join.type.upper()
            if join_type == 'INNER':
                self.write('JOIN ')
            else:
                self.write(join_type + ' JOIN ')
            nested_join = (
                isinstance(join.rarg, pgast.JoinExpr)
                and join.rarg.joins
            )
            if nested_join:
                self.write('(')
                self.new_lines = 1
                self.indentation += 1
            self.visit(join.rarg)
            if nested_join:
                self.indentation -= 1
                self.new_lines = 1
                self.write(')')
            if join.quals is not None:
                if not nested_join:
                    self.indentation += 1
                    self.new_lines = 1
                    self.write('ON ')
                else:
                    self.write(' ON ')
                self.visit(join.quals)
                if not nested_join:
                    self.indentation -= 1
            elif join.using_clause:
                self.write(" USING (")
                self.visit_list(join.using_clause)
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

    def visit_BitStringConstant(self, node: pgast.BitStringConstant) -> None:
        self.write(f"{node.kind}'{node.val}'")

    def visit_ByteaConstant(self, node: pgast.ByteaConstant) -> None:
        self.write(common.quote_bytea_literal(node.val))

    def visit_ParamRef(self, node: pgast.ParamRef) -> None:
        self.write(f'${node.number}')
        self.param_index[node.number].append(len(self.result) - 1)

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
                raise GeneratorError(
                    'unexpected NULLS order: {}'.format(node.nulls)
                )

    def visit_LockingClause(self, node: pgast.LockingClause) -> None:
        self.write("FOR ", str(node.strength))
        if node.locked_rels:
            self.write(" OF ")
            self.visit_list(node.locked_rels)
        if node.wait_policy is not None:
            if kw := str(node.wait_policy):
                self.write(f" {kw}")

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
        self.write(common.qname(node.name))

    def _visit_indirection_ops(
        self, ops: Sequence[pgast.IndirectionOp]
    ) -> None:
        for op in ops:
            if isinstance(op, pgast.Star):
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
        self.write(common.qname(node.name))
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
            self.write(common.qname(node.name))
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
        self.write(common.qname(node.name))

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
        self.write(f"PREPARE {common.quote_ident(node.name)}")
        if node.argtypes:
            self.write(f"(")
            self.visit_list(node.argtypes, newlines=False)
            self.write(f")")
        self.write(f" AS ")
        self.visit(node.query)

    def visit_ExecuteStmt(self, node: pgast.ExecuteStmt) -> None:
        self.write(f"EXECUTE {common.quote_ident(node.name)}")
        if node.params:
            self.write(f"(")
            self.visit_list(node.params, newlines=False)
            self.write(f")")

    def visit_DeallocateStmt(self, node: pgast.DeallocateStmt) -> None:
        self.write(f"DEALLOCATE {common.quote_ident(node.name)}")

    def visit_SQLValueFunction(self, node: pgast.SQLValueFunction) -> None:
        self.write(common.get_sql_value_function_op(node.op))
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

    def visit_CopyStmt(self, node: pgast.CopyStmt) -> None:
        self.write('COPY ')
        if node.query:
            self.write('(')
            self.indentation += 1
            self.new_lines = 1
            self.visit(node.query)
            self.indentation -= 1
            self.write(')')
        elif node.relation:
            self.visit_Relation(node.relation)
            if node.colnames:
                self.write(' (')
                self.write(
                    ', '.join(common.quote_ident(n) for n in node.colnames))
                self.write(')')

        if node.is_from:
            self.write(' FROM ')
        else:
            self.write(' TO ')

        if node.is_program:
            self.write('PROGRAM ')
        if node.filename:
            self.write(common.quote_literal(node.filename))
        else:
            if node.is_from:
                self.write('STDIN')
            else:
                self.write('STDOUT')

        self.visit_CopyOptions(node.options)

        if node.where_clause:
            self.indentation += 1
            self.new_lines = 1
            self.write('WHERE')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.where_clause)
            self.indentation -= 2

    def visit_CopyOptions(self, node: pgast.CopyOptions) -> None:
        ql = common.quote_literal
        qi = common.quote_ident

        opts = []

        if node.format:
            opts.append('FORMAT ' + node.format._name_)
        if node.freeze is not None:
            opts.append('FREEZE' + ('' if node.freeze else ' FALSE'))
        if node.delimiter:
            opts.append('DELIMITER ' + ql(node.delimiter))
        if node.null:
            opts.append('NULL ' + ql(node.null))
        if node.header is not None:
            opts.append('HEADER' + ('' if node.header else ' FALSE'))
        if node.quote:
            opts.append('QUOTE ' + ql(node.quote))
        if node.escape:
            opts.append('ESCAPE ' + ql(node.escape))
        if node.force_quote:
            opts.append(
                'FORCE_QUOTE (' + ', '.join(map(qi, node.force_quote)) + ')'
            )
        if node.force_not_null:
            opts.append(
                'FORCE_NOT_NULL ('
                + ', '.join(map(qi, node.force_not_null))
                + ')'
            )
        if node.force_null:
            opts.append(
                'FORCE_NULL (' + ', '.join(map(qi, node.force_null)) + ')'
            )
        if node.encoding:
            opts.append('ENCODING ' + ql(node.encoding))

        if opts:
            self.write(' (' + ', '.join(opts), ')')


class GeneratorContext(markup.MarkupExceptionContext):
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


class GeneratorError(errors.InternalServerError):
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
            ctx = GeneratorContext(node)
            exceptions.add_context(self, ctx)
