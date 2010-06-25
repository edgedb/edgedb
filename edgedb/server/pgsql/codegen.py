##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import numbers
import postgresql.string
from semantix.caos.backends.pgsql import common
from . import ast as pgast
from semantix.utils.ast import codegen


class SQLSourceGeneratorError(Exception): pass

class SQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise SQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def visit_CTENode(self, node):
        self.write(common.quote_ident(node.alias))

    def gen_ctes(self, ctes):
        self.write('WITH')
        count = len(ctes)
        for i, cte in enumerate(ctes):
            self.new_lines = 1
            self.write(common.quote_ident(cte.alias))
            self.write(' AS ')
            self.indentation += 2
            alias = cte.alias
            self.visit_SelectQueryNode(cte, use_alias=False)
            if i != count - 1:
                self.write(',')
            self.indentation -= 2

        self.new_lines = 1

    def visit_SelectQueryNode(self, node, use_alias=True):
        self.new_lines = 1

        self.write('(')
        if node.ctes:
            self.gen_ctes(node.ctes)

        self.write('SELECT')
        if node.distinct is not None:
            self.write(' DISTINCT')

        self.new_lines = 1
        self.indentation += 2

        count = len(node.targets)
        for i, target in enumerate(node.targets):
            self.new_lines = 1
            self.visit(target)
            if i != count -1:
                self.write(',')

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

    def visit_UnionNode(self, node):
        self.write('(')
        if node.ctes:
            self.gen_ctes(node.ctes)

        count = len(node.queries)
        for i, query in enumerate(node.queries):
            self.new_lines = 1
            self.visit(query)
            if i != count - 1:
                self.write(' UNION ')
                if not node.distinct:
                    self.write(' ALL ')

        self.write(')')
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_IntersectNode(self, node):
        self.write('(')
        if node.ctes:
            self.gen_ctes(node.ctes)

        count = len(node.queries)
        for i, query in enumerate(node.queries):
            self.new_lines = 1
            self.visit(query)
            if i != count - 1:
                self.write(' INTERSECT ')

        self.write(')')
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_ExistsNode(self, node):
        self.write('EXISTS (')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_UpdateQueryNode(self, node):
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
        self.write('DELETE FROM ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.fromexpr)
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

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_UpdateExprNode(self, node):
        # Do no call visit_FieldRefNode here, since fields in UPDATE clause
        # must not have table qualifiers
        #
        self.write(common.qname(str(node.expr.field)))
        self.write(' = ')
        self.visit(node.value)

    def visit_FieldRefNode(self, node):
        if node.field == '*':
            self.write(common.quote_ident(node.table.alias) + '.' + str(node.field))
        else:
            if node.table:
                self.write(common.qname(node.table.alias, str(node.field)))
            else:
                self.write(common.quote_ident(str(node.field)))

    def visit_FromExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_JoinNode(self, node):
        self.visit(node.left)
        self.new_lines = 1
        self.write(node.type.upper() + ' JOIN ')
        self.visit(node.right)
        self.write(' ON ')
        self.visit(node.condition)

    def visit_TableNode(self, node):
        self.write(common.qname(node.schema, node.name))
        if node.alias:
            self.write(' AS ' + common.quote_ident(node.alias))

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
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
            if node.type is not None:
                self.write('::%s' % node.type)
        else:
            if node.value is None:
                self.write('NULL')
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
        self.write('(')
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
        self.write('(')
        count = len(node.args)
        for i, e in enumerate(node.args):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_SortExprNode(self, node):
        self.visit(node.expr)
        if node.direction:
            direction = 'ASC' if node.direction == pgast.SortAsc else 'DESC'
            self.write(' ' + direction)
            if node.direction == pgast.SortDesc:
                self.write(' NULLS LAST')
            else:
                self.write(' NULLS FIRST')

    def visit_TypeCastNode(self, node):
        self.visit(node.expr)
        self.write('::')
        self.visit(node.type)

    def visit_TypeNode(self, node):
        self.write(node.name)

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
