##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import numbers
import postgresql.string

from edgedb.server.pgsql import common
from edgedb.server.pgsql import ast as pgast
from edgedb.lang.common.ast import codegen
from edgedb.lang.common import markup
from edgedb.lang.common import exceptions as edgedb_error


class SQLSourceGeneratorContext(markup.MarkupExceptionContext):
    title = 'SQL Source Generator Context'

    def __init__(self, node, chunks_generated=None):
        self.node = node
        self.chunks_generated = chunks_generated

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        body = [me.doc.Section(title='SQL Tree', body=[markup.serialize(self.node, ctx=ctx)])]

        if self.chunks_generated:
            code = markup.serializer.serialize_code(''.join(self.chunks_generated), lexer='sql')
            body.append(me.doc.Section(title='SQL generated so far', body=[code]))

        return me.lang.ExceptionContext(title=self.title, body=body)


class SQLSourceGeneratorError(edgedb_error.EdgeDBError):
    def __init__(self, msg, *, node=None, details=None, hint=None):
        super().__init__(msg, details=details, hint=hint)
        if node is not None:
            ctx = SQLSourceGeneratorContext(node)
            edgedb_error.add_context(self, ctx)


class SQLSourceGenerator(codegen.SourceGenerator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.param_index = {}

    @classmethod
    def to_source(cls, node, indent_with=' '*4, add_line_information=False,
                       pretty=True):
        try:
            return super().to_source(node, indent_with=indent_with,
                                     add_line_information=add_line_information,
                                     pretty=pretty)
        except SQLSourceGeneratorError as e:
            ctx = SQLSourceGeneratorContext(node)
            edgedb_error.add_context(e, ctx)
            raise

    def generic_visit(self, node):
        raise SQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def visit_LiteralExprNode(self, node):
        self.write(node.expr)

    def visit_CTENode(self, node):
        if isinstance(node.alias, str):
            self.write(common.quote_ident(node.alias))
        else:
            self.write(common.quote_ident(node.alias.alias))

    def visit_CTERefNode(self, node):
        if isinstance(node.cte.alias, str):
            self.write(common.quote_ident(node.cte.alias))
        else:
            self.write(common.quote_ident(node.cte.alias.alias))

    def gen_ctes(self, ctes):
        self.write('WITH')
        count = len(ctes)
        for i, cte in enumerate(ctes):
            self.new_lines = 1
            if getattr(cte, 'recursive', None):
                self.write('RECURSIVE ')
            if isinstance(cte.alias, str):
                self.write(common.quote_ident(cte.alias))
            else:
                self.visit(cte.alias)
            self.write(' AS ')
            self.indentation += 1
            if isinstance(cte, pgast.SelectQueryNode):
                self.visit_SelectQueryNode(cte, use_alias=False)
            else:
                self.new_lines = 1
                self.write('(')
                self.visit(cte)
                self.write(')')
            if i != count - 1:
                self.write(',')
            self.indentation -= 1

        self.new_lines = 1

    def visit_TableQueryNode(self, node):
        self.write('(TABLE ')
        if node.schema:
            self.write(common.qname(node.schema, node.name))
        else:
            self.write(common.quote_ident(node.name))
        self.write(')')

    def visit_SelectQueryNode(self, node, use_alias=True):
        self.new_lines = 1

        self.write('(')

        if node.text_override:
            self.write(node.text_override)
        else:
            if node.ctes:
                self.gen_ctes(node.ctes)

            if node.op:
                # Upper level set operation node (UNION/INTERSECT)
                self.visit(node.larg)
                self.write(' ' + node.op + ' ')
                if not node.distinct:
                    self.write('ALL ')
                self.visit(node.rarg)
            else:
                self.write('SELECT')
                if node.distinct is not None:
                    self.write(' DISTINCT')

                self.new_lines = 1
                self.indentation += 2

            if node.targets:
                count = len(node.targets)
                for i, target in enumerate(node.targets):
                    self.new_lines = 1
                    self.visit(target)
                    if i != count -1:
                        self.write(',')

            if not node.op:
                self.indentation -= 2

            if node.fromlist:
                self.indentation += 1
                self.new_lines = 1
                self.write('FROM')
                if node.from_only:
                    self.write(' ONLY')
                self.new_lines = 1
                self.indentation += 1
                count = len(node.fromlist)
                for i, source in enumerate(node.fromlist):
                    self.new_lines = 1
                    if isinstance(source, pgast.DMLNode):
                        self.visit_CTENode(source)
                    else:
                        self.visit(source)
                    if i != count - 1:
                        self.write(',')

                self.indentation -= 2

            if node.where:
                self.indentation += 1
                self.new_lines = 1
                self.write('WHERE')
                self.new_lines = 1
                self.indentation += 1
                self.visit(node.where)
                self.indentation -= 2

            if node.groupby:
                self.indentation += 1
                self.new_lines = 1
                self.write('GROUP BY')
                self.new_lines = 1
                self.indentation += 1
                count = len(node.groupby)
                for i, groupexpr in enumerate(node.groupby):
                    self.new_lines = 1
                    self.visit(groupexpr)
                    if i != count - 1:
                        self.write(',')
                self.indentation -= 2

            if node.having:
                self.indentation += 1
                self.new_lines = 1
                self.write('HAVING')
                self.new_lines = 1
                self.indentation += 1
                self.visit(node.having)
                self.indentation -= 2

            if node.orderby:
                self.indentation += 1
                self.new_lines = 1
                self.write('ORDER BY')
                self.new_lines = 1
                self.indentation += 1
                count = len(node.orderby)
                for i, sortexpr in enumerate(node.orderby):
                    self.new_lines = 1
                    self.visit(sortexpr)
                    if i != count - 1:
                        self.write(',')
                self.indentation -= 2

            if node.offset:
                self.indentation += 1
                self.new_lines = 1
                self.write('OFFSET ')
                self.visit(node.offset)
                self.indentation -= 1

            if node.limit:
                self.indentation += 1
                self.new_lines = 1
                self.write('LIMIT ')
                self.visit(node.limit)
                self.indentation -= 1

        self.new_lines = 1
        self.write(')')

        if node.alias and use_alias:
            self.write(' AS ' + common.quote_ident(node.alias))
            if node.coldef:
                self.write(node.coldef)

    def visit_ExistsNode(self, node):
        self.write('EXISTS (')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_InsertQueryNode(self, node):
        self.write('INSERT INTO ')
        self.visit(node.fromexpr)
        if node.cols:
            self.new_lines = 1
            self.indentation += 1
            self.write('(')
            self.visit_list(node.cols, newlines=False)
            self.write(')')
            self.indentation -= 1

        self.indentation += 1
        self.new_lines = 1

        if node.select.values:
            self.write('VALUES ')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.select.values)
            self.indentation -= 1
        else:
            self.write('(')
            self.visit(node.select)
            self.write(')')

        if node.on_conflict:
            self.new_lines = 1
            self.write('ON CONFLICT')

            if node.on_conflict.infer:
                self.write(' (')
                self.visit_list(node.on_conflict.infer, newlines=False)
                self.write(')')

            self.write(' DO ')
            self.write(node.on_conflict.action.upper())

            if node.on_conflict.targets:
                self.write(' SET')
                self.new_lines = 1
                self.indentation += 1
                self.visit_list(node.on_conflict.targets)
                self.indentation -= 1

        if node.targets:
            self.new_lines = 1
            self.write('RETURNING')
            self.new_lines = 1
            self.indentation += 1

            count = len(node.targets)
            for i, expr in enumerate(node.targets):
                self.new_lines = 1
                self.visit(expr)
                if i != count - 1:
                    self.write(',')
            self.indentation -= 1

        self.indentation -= 1

    def visit_UpdateQueryNode(self, node):
        if node.ctes:
            self.gen_ctes(node.ctes)
        self.write('UPDATE ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.fromexpr)
        self.indentation -= 1
        self.new_lines = 1
        self.write('SET')

        self.indentation += 1
        count = len(node.values)
        for i, expr in enumerate(node.values):
            self.new_lines = 1
            self.visit(expr)
            if i != count - 1:
                self.write(',')
        self.indentation -= 1

        self.new_lines = 1
        self.write('WHERE')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.where)
        self.new_lines = 1
        self.indentation -= 1
        self.write('RETURNING')
        self.new_lines = 1
        self.indentation += 1

        count = len(node.targets)
        for i, expr in enumerate(node.targets):
            self.new_lines = 1
            self.visit(expr)
            if i != count - 1:
                self.write(',')
        self.indentation -= 1

    def visit_DeleteQueryNode(self, node):
        if node.ctes:
            self.gen_ctes(node.ctes)
        self.write('DELETE FROM ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.fromexpr)
        self.indentation -= 1
        if node.using:
            self.new_lines = 1
            self.write('USING')
            self.indentation += 1
            for i, source in enumerate(node.using):
                if i > 0:
                    self.write(',')
                self.new_lines = 1
                if isinstance(source, pgast.DMLNode):
                    self.visit_CTENode(source)
                else:
                    self.visit(source)
            self.indentation -= 1
        self.new_lines = 1
        self.write('WHERE')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.where)
        self.new_lines = 1
        self.indentation -= 1

        if node.targets:
            self.write('RETURNING')
            self.new_lines = 1
            self.indentation += 1

            count = len(node.targets)
            for i, expr in enumerate(node.targets):
                self.new_lines = 1
                self.visit(expr)
                if i != count - 1:
                    self.write(',')
            self.indentation -= 1

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_UpdateExprNode(self, node):
        # Do no call visit_FieldRefNode here, since fields in UPDATE clause
        # must not have table qualifiers
        #
        if isinstance(node.expr, pgast.FieldRefNode):
            self.write(common.qname(str(node.expr.field)))
        else:
            self.visit(node.expr)
        self.write(' = ')
        self.visit(node.value)

    def visit_FieldRefNode(self, node):
        if node.field == '*':
            self.write(common.quote_ident(node.table.alias) + '.' + str(node.field))
        else:
            if node.table:
                if isinstance(node.table.alias, str):
                    alias = node.table.alias
                else:
                    alias = node.table.alias.alias

                if isinstance(node.table, pgast.PseudoRelationNode):
                    self.write(alias + "." + postgresql.string.quote_ident(str(node.field)))
                else:
                    self.write(common.qname(alias, str(node.field)))
            else:
                self.write(postgresql.string.quote_ident_if_needed(str(node.field)))

    def visit_FromExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ')
            if isinstance(node.alias, str):
                self.write(common.quote_ident(node.alias))
            else:
                self.visit(node.alias)

    def visit_FuncAliasNode(self, node):
        self.write(common.quote_ident(node.alias))
        if node.elements:
            self.write(' (')
            count = len(node.elements)
            for i, expr in enumerate(node.elements):
                self.visit(expr)
                if i != count - 1:
                    self.write(', ')
            self.write(')')

    def visit_TableFuncElement(self, node):
        self.write(common.quote_ident(node.name))
        if node.type is not None:
            self.write(' ')
            self.visit(node.type)

    def visit_JoinNode(self, node):
        self.visit(node.left)
        if node.right is not None:
            self.new_lines = 1
            self.write(node.type.upper() + ' JOIN ')
            if isinstance(node.right, pgast.JoinNode) and node.right.right is not None:
                self.write('(')
            self.visit(node.right)
            if isinstance(node.right, pgast.JoinNode) and node.right.right is not None:
                self.write(')')
            if node.condition is not None:
                self.write(' ON ')
                self.visit(node.condition)

    def visit_TableNode(self, node):
        self.write(common.qname(node.schema, node.name))
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        op = str(node.op)
        if '.' not in op:
            op = op.upper()
        self.write(' ' + op + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_UnaryOpNode(self, node):
        self.write('(')
        self.write(str(node.op).upper())
        self.write(' ')
        self.visit(node.operand)
        self.write(')')

    def visit_PredicateNode(self, node):
        self.visit(node.expr)

    def visit_ConstantNode(self, node):
        if node.expr is not None:
            self.visit(node.expr)
        elif node.index is not None:
            self.write('$%d' % (node.index + 1))
            self.param_index.setdefault(node.index, []).append(len(self.result) - 1)
            if node.type is not None:
                self.write('::%s' % node.type)
        else:
            if node.value is None:
                self.write('NULL')
                if node.type is not None:
                    self.write('::{}'.format(node.type))
            elif isinstance(node.value, (bool, numbers.Number)):
                self.write(str(node.value))
            else:
                self.write(postgresql.string.quote_literal(str(node.value)))

    def visit_SequenceNode(self, node):
        self.write('(')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_RowExprNode(self, node):
        self.write('ROW(')
        count = len(node.args)
        for i, e in enumerate(node.args):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')


    def visit_ArrayNode(self, node):
        self.write('ARRAY[')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')
        self.write(']')

    def visit_FunctionCallNode(self, node):
        self.write(node.name)
        if node.args or not node.noparens:
            self.write('(')
        count = len(node.args)
        for i, e in enumerate(node.args):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        if node.agg_sort:
            self.write(' ORDER BY ')
            count = len(node.agg_sort)
            for i, sortexpr in enumerate(node.agg_sort):
                self.visit(sortexpr)
                if i != count - 1:
                    self.write(',')

        if node.args or not node.noparens:
            self.write(')')

        if node.agg_filter:
            self.write(' FILTER (WHERE ')
            self.visit(node.agg_filter)
            self.write(')')

        if node.over:
            self.write(' OVER (')
            if node.over.partition:
                self.write('PARTITION BY ')

                count = len(node.over.partition)
                for i, groupexpr in enumerate(node.over.partition):
                    self.visit(groupexpr)
                    if i != count - 1:
                        self.write(',')

            if node.over.orderby:
                self.write(' ORDER BY ')
                count = len(node.over.orderby)
                for i, sortexpr in enumerate(node.over.orderby):
                    self.visit(sortexpr)
                    if i != count - 1:
                        self.write(',')

            # XXX: add support for frame definition

            self.write(')')

    def visit_SortExprNode(self, node):
        self.visit(node.expr)
        if node.direction:
            direction = 'ASC' if node.direction == pgast.SortAsc else 'DESC'
            self.write(' ' + direction)

            if node.nulls_order is None:
                if node.direction == pgast.SortDesc:
                    self.write(' NULLS LAST')
                else:
                    self.write(' NULLS FIRST')
            elif node.nulls_order == pgast.NullsFirst:
                self.write(' NULLS FIRST')
            elif node.nulls_order == pgast.NullsLast:
                self.write(' NULLS LAST')
            else:
                raise SQLSourceGeneratorError('unexpected NULLS order: {}'.format(node.nulls_order))

    def visit_TypeCastNode(self, node):
        self.visit(node.expr)
        self.write('::')
        self.visit(node.type)

    def visit_TypeNode(self, node):
        self.write(node.name)
        if node.array_bounds:
            for array_bound in node.array_bounds:
                self.write('[')
                if array_bound >= 0:
                    self.write(array_bound)
                self.write(']')

    def visit_StarIndirectionNode(self, node):
        self.write('*')

    def visit_CaseExprNode(self, node):
        self.write('(CASE ')
        for arg in node.args:
            self.visit(arg)
            self.new_lines = 1
        if node.default:
            self.write('ELSE ')
            self.visit(node.default)
            self.new_lines = 1
        self.write('END)')

    def visit_CaseWhenNode(self, node):
        self.write('WHEN ')
        self.visit(node.expr)
        self.write(' THEN ')
        self.visit(node.result)

    def visit_NullTestNode(self, node):
        self.visit(node.expr)
        self.write(' IS NULL ')

    def visit_IndirectionNode(self, node):
        self.write('(')
        self.visit(node.expr)
        self.write(')')
        if isinstance(node.indirection, pgast.StarIndirectionNode):
            self.write('.')
        self.visit(node.indirection)

    def visit_IndexIndirectionNode(self, node):
        self.write('[')
        if node.lower is not None:
            self.visit(node.lower)
            self.write(':')
        self.visit(node.upper)
        self.write(']')

    def visit_CollateClauseNode(self, node):
        self.visit(node.expr)
        self.write(' COLLATE ')
        self.visit(node.collation_name)

    def visit_IdentNode(self, node):
        self.write(common.quote_ident(node.name))
