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

from edb import errors

from edb.pgsql import common
from edb.pgsql import ast as pgast
from edb.common.ast import codegen
from edb.common import exceptions
from edb.common import markup


class SQLSourceGeneratorContext(markup.MarkupExceptionContext):
    title = 'SQL Source Generator Context'

    def __init__(self, node, chunks_generated=None):
        self.node = node
        self.chunks_generated = chunks_generated

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        body = [
            me.doc.Section(
                title='SQL Tree', body=[markup.serialize(self.node, ctx=ctx)])
        ]

        if self.chunks_generated:
            code = markup.serializer.serialize_code(
                ''.join(self.chunks_generated), lexer='sql')
            body.append(
                me.doc.Section(title='SQL generated so far', body=[code]))

        return me.lang.ExceptionContext(title=self.title, body=body)


class SQLSourceGeneratorError(errors.InternalServerError):
    def __init__(self, msg, *, node=None, details=None, hint=None):
        super().__init__(msg, details=details, hint=hint)
        if node is not None:
            ctx = SQLSourceGeneratorContext(node)
            exceptions.add_context(self, ctx)


class SQLSourceGenerator(codegen.SourceGenerator):
    def __init__(self, *args, reordered: bool=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.param_index: dict[object, int] = {}
        self.reordered = reordered

    @classmethod
    def to_source(
            cls, node, indent_with=' ' * 4, add_line_information=False,
            pretty=True):
        try:
            return super().to_source(
                node, indent_with=indent_with,
                add_line_information=add_line_information, pretty=pretty)
        except SQLSourceGeneratorError as e:
            ctx = SQLSourceGeneratorContext(node)
            exceptions.add_context(e, ctx)
            raise

    def generic_visit(self, node):
        raise SQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__)

    def gen_ctes(self, ctes):
        self.write('WITH')
        count = len(ctes)
        for i, cte in enumerate(ctes):
            self.new_lines = 1
            if getattr(cte, 'recursive', None):
                self.write('RECURSIVE ')
            self.write(common.quote_ident(cte.name))
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

    def visit__Ref(self, node):
        self.visit(node.node)

    def visit_Relation(self, node):
        if node.schemaname is None:
            self.write(common.qname(node.name))
        else:
            self.write(common.qname(node.schemaname, node.name))

    def _visit_values_expr(self, node):
        self.new_lines = 1
        self.write('(')
        self.write('VALUES')
        self.new_lines = 1
        self.indentation += 1
        self.visit_list(node.values)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_NullRelation(self, node):
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

    def visit_SelectStmt(self, node):
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
                self.indentation += 1

        if node.ctes:
            self.gen_ctes(node.ctes)

        # If reordered is True, we try to put the FROM clause *before* SELECT,
        # like it *ought* to be. We do various hokey things to try to make
        # that look good.
        # Otherwise we emit real SQL.
        def _select() -> None:
            self.write('SELECT')
            if node.distinct_clause:
                self.write(' DISTINCT')
                if (len(node.distinct_clause) > 1 or
                        not isinstance(node.distinct_clause[0], pgast.Star)):
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
            if self.reordered:
                self.indentation -= 1
            self.write(')')

    def visit_InsertStmt(self, node):
        if node.ctes:
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

        if node.select_stmt.values:
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

    def visit_UpdateStmt(self, node):
        if node.ctes:
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

    def visit_DeleteStmt(self, node):
        if node.ctes:
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

    def visit_InferClause(self, node):
        assert not node.conname or not node.index_elems
        if node.conname:
            self.write(' ON CONSTRAINT ')
            self.write(node.conname)
        if node.index_elems:
            self.write(' (')
            self.visit_list(node.index_elems, newlines=False)
            self.write(')')

    def visit_MultiAssignRef(self, node):
        self.write('(')
        self.visit_list(node.columns, newlines=False)
        self.write(') = ')
        self.visit(node.source)

    def visit_LiteralExpr(self, node):
        self.write(node.expr)

    def visit_ResTarget(self, node):
        self.visit(node.val)
        if node.name:
            self.write(' AS ' + common.quote_ident(node.name))

    def visit_UpdateTarget(self, node):
        self.write(common.quote_ident(node.name))
        self.write(' = ')
        self.visit(node.val)

    def visit_Alias(self, node):
        self.write(common.quote_ident(node.aliasname))
        if node.colnames:
            self.write('(')
            self.write(', '.join(common.quote_ident(n) for n in node.colnames))
            self.write(')')

    def visit_Keyword(self, node):
        self.write(node.name)

    def visit_RelRangeVar(self, node):
        rel = node.relation

        if not node.include_inherited:
            self.write(' ONLY (')

        if isinstance(rel, (pgast.Relation, pgast.NullRelation)):
            self.visit(rel)
        elif isinstance(rel, pgast.CommonTableExpr):
            self.write(common.quote_ident(rel.name))
        else:
            raise SQLSourceGeneratorError(
                'unexpected relation in RelRangeVar: {!r}'.format(rel))

        if not node.include_inherited:
            self.write(')')

        if node.alias and node.alias.aliasname:
            self.write(' AS ')
            self.visit(node.alias)

    def visit_RangeSubselect(self, node):
        if node.lateral:
            self.write('LATERAL ')

        self.visit(node.subquery)

        if node.alias and node.alias.aliasname:
            self.write(' AS ')
            self.visit(node.alias)

    def visit_RangeFunction(self, node):
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

    def visit_ColumnRef(self, node):
        names = node.name
        if isinstance(names[-1], pgast.Star):
            self.write(common.qname(*names[:-1]))
            if len(names) > 1:
                self.write('.')
            self.write('*')
        else:
            if names == ['VALUE']:
                self.write(names[0])
            elif names[0] in {'OLD', 'NEW'}:
                self.write(names[0])
                if len(names) > 1:
                    self.write('.')
                    self.write(common.qname(*names[1:]))
            else:
                self.write(common.qname(*names))

    def visit_ColumnDef(self, node):
        self.write(common.quote_ident(node.name))
        if node.typename:
            self.write(' ')
            self.visit(node.typename)

    def visit_GroupingOperation(self, node):
        if node.operation:
            self.write(node.operation)
            self.write(' ')
        self.write('(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_JoinExpr(self, node):
        self.visit(node.larg)
        if node.rarg is not None:
            self.new_lines = 1
            self.write(node.type.upper() + ' JOIN ')
            nested_join = (
                isinstance(node.rarg, pgast.JoinExpr) and
                node.rarg.rarg is not None
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

    def visit_Expr(self, node):
        self.write('(')
        if node.lexpr is not None:
            self.visit(node.lexpr)
            self.write(' ')
        op = str(node.name)
        if '.' not in op:
            op = op.upper()
        self.write(op)
        if op.lower() in {'or', 'and'}:
            self.new_lines = 1
            self.char_indentation += 1
        if node.rexpr is not None:
            self.write(' ')
            self.visit(node.rexpr)
        if op.lower() in {'or', 'and'}:
            self.char_indentation -= 1
        self.write(')')

    def visit_NullConstant(self, node):
        self.write('NULL')

    def visit_NumericConstant(self, node):
        self.write(node.val)

    def visit_BooleanConstant(self, node):
        self.write(node.val)

    def visit_StringConstant(self, node):
        self.write(common.quote_literal(node.val))

    def visit_ByteaConstant(self, node):
        self.write(common.quote_bytea_literal(node.val))

    def visit_ParamRef(self, node):
        self.write('$', str(node.number))

    def visit_NamedParamRef(self, node):
        self.write(common.quote_ident(node.name))

    def visit_RowExpr(self, node):
        self.write('ROW(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_ImplicitRowExpr(self, node):
        self.write('(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_ArrayExpr(self, node):
        self.write('ARRAY[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_VariadicArgument(self, node):
        self.write('VARIADIC ')
        self.visit(node.expr)

    def visit_FuncCall(self, node):
        self.write(common.qname(*node.name))

        self.write('(')
        if node.agg_distinct:
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

    def visit_NamedFuncArg(self, node):
        self.write(common.quote_ident(node.name), ' => ')
        self.visit(node.val)

    def visit_SubLink(self, node):
        if node.type == pgast.SubLinkType.EXISTS:
            self.write('EXISTS')
        elif node.type == pgast.SubLinkType.NOT_EXISTS:
            self.write('NOT EXISTS')
        elif node.type == pgast.SubLinkType.ALL:
            self.write('ALL')
        elif node.type == pgast.SubLinkType.ANY:
            self.write('ANY')
        else:
            raise SQLSourceGeneratorError(
                'unexpected SubLinkType: {!r}'.format(node.type))

        self.write(' (')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_SortBy(self, node):
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
                    'unexpected NULLS order: {}'.format(node.nulls))

    def visit_TypeCast(self, node):
        # '::' has very high precedence, so parenthesize the expression.
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        self.write('::')
        self.visit(node.type_name)

    def visit_TypeName(self, node):
        self.write(common.quote_type(node.name))
        if node.array_bounds:
            for array_bound in node.array_bounds:
                self.write('[')
                if array_bound >= 0:
                    self.write(array_bound)
                self.write(']')

    def visit_Star(self, node):
        self.write('*')

    def visit_CaseExpr(self, node):
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

    def visit_CaseWhen(self, node):
        self.write('WHEN ')
        self.visit(node.expr)
        self.write(' THEN ')
        self.visit(node.result)

    def visit_NullTest(self, node):
        self.write('(')
        self.visit(node.arg)
        if node.negated:
            self.write(' IS NOT NULL')
        else:
            self.write(' IS NULL')
        self.write(')')

    def visit_Indirection(self, node):
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        for indirection in node.indirection:
            if isinstance(indirection, (pgast.Star, pgast.ColumnRef)):
                self.write('.')
            self.visit(indirection)

    def visit_Indices(self, node):
        self.write('[')
        if node.lidx is not None:
            self.visit(node.lidx)
            self.write(':')
        self.visit(node.ridx)
        self.write(']')

    def visit_CollateClause(self, node):
        self.visit(node.arg)
        self.write(' COLLATE ')
        self.visit(node.collname)

    def visit_CoalesceExpr(self, node):
        self.write('COALESCE(')
        self.visit_list(node.args, newlines=False)
        self.write(')')

    def visit_AlterSystem(self, node):
        self.write('ALTER SYSTEM ')
        if node.value is not None:
            self.write('SET ')
            self.write(common.quote_ident(node.name))
            self.write(' = ')
            self.visit(node.value)
        else:
            self.write('RESET ')
            self.write(common.quote_ident(node.name))

    def visit_Set(self, node):
        if node.value is not None:
            self.write('SET ')
            self.write(common.quote_ident(node.name))
            self.write(' = ')
            self.visit(node.value)
        else:
            self.write('RESET ')
            self.write(common.quote_ident(node.name))


generate_source = SQLSourceGenerator.to_source
