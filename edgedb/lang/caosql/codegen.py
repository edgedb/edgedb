##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from metamagic.exceptions import MetamagicError
from metamagic.utils.ast import codegen, AST

from . import ast as caosql_ast
from . import quote as caosql_quote


class CaosQLSourceGeneratorError(MetamagicError):
    pass


class CaosQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise CaosQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def _visit_namespaces(self, node):
        if node.namespaces or node.aliases:
            self.write('USING')
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(itertools.chain(node.namespaces, node.aliases))
            self.new_lines = 1
            self.indentation -= 1

    def _visit_cges(self, cges):
        if cges:
            self.new_lines = 1
            self.write('WITH')
            self.new_lines = 1
            self.indentation += 1
            for i, cge in enumerate(cges):
                if i > 0:
                    self.write(',')
                    self.new_lines = 1
                self.visit(cge)
            self.indentation -= 1
            self.new_lines = 1

    def visit_CGENode(self, node):
        self.write(node.alias)
        self.write(' AS (')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1
        self.write(')')

    def visit_UpdateQueryNode(self, node):
        self._visit_namespaces(node)
        self._visit_cges(node.cges)

        self.write('UPDATE')

        self.indentation += 1
        self.new_lines = 1
        self.visit(node.subject)
        self.indentation -= 1
        self.new_lines = 1

        self.write('SET')

        self.indentation += 1
        self.new_lines = 1
        for i, e in enumerate(node.values):
            if i > 0:
                self.write(',')
                self.new_lines = 1
            self.visit(e)
        self.indentation -= 1

        if node.where:
            self.new_lines = 1
            self.write('WHERE')
            self.indentation += 1
            self.new_lines = 1
            self.visit(node.where)
            self.indentation -= 1

        if node.targets:
            self.new_lines = 1
            self.write('RETURNING')
            self.indentation += 1
            self.new_lines = 1
            for i, e in enumerate(node.targets):
                if i > 0:
                    self.write(', ')
                self.visit(e)
            self.indentation -= 1

    def visit_UpdateExprNode(self, node):
        self.visit(node.expr)
        self.write(' = ')
        self.visit(node.value)

    def visit_DeleteQueryNode(self, node):
        self._visit_namespaces(node)
        self._visit_cges(node.cges)
        self.write('DELETE ')
        self.visit(node.subject)
        if node.where:
            self.write(' WHERE ')
            self.visit(node.where)
        if node.targets:
            self.write(' RETURNING ')
            for i, e in enumerate(node.targets):
                if i > 0:
                    self.write(', ')
                self.visit(e)

    def visit_SubqueryNode(self, node):
        self.write('(')
        self.visit(node.expr)
        self.write(')')

    def visit_SelectQueryNode(self, node):
        self._visit_namespaces(node)

        if node.op:
            # Upper level set operation node (UNION/INTERSECT)
            self.write('(')
            self.visit(node.op_larg)
            self.write(')')
            self.new_lines = 1
            self.write(' ' + node.op + ' ')
            self.new_lines = 1
            self.write('(')
            self.visit(node.op_rarg)
            self.write(')')
        else:
            self._visit_cges(node.cges)

            self.write('SELECT')
            self.new_lines = 1
            self.indentation += 1
            for i, e in enumerate(node.targets):
                if i > 0:
                    self.write(',')
                    self.new_lines = 1
                self.visit(e)
            self.new_lines = 1
            self.indentation -= 1
            if node.where:
                self.write('WHERE')
                self.new_lines = 1
                self.indentation += 1
                self.visit(node.where)
                self.new_lines = 1
                self.indentation -= 1
            if node.groupby:
                self.write('GROUP BY')
                self.new_lines = 1
                self.indentation += 1
                for i, e in enumerate(node.groupby):
                    if i > 0:
                        self.write(',')
                        self.new_lines = 1
                    self.visit(e)
                self.new_lines = 1
                self.indentation -= 1

        if node.orderby:
            self.write('ORDER BY')
            self.new_lines = 1
            self.indentation += 1
            for i, e in enumerate(node.orderby):
                if i > 0:
                    self.write(',')
                    self.new_lines = 1
                self.visit(e)
            self.new_lines = 1
            self.indentation -= 1
        if node.offset is not None:
            self.write('OFFSET')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.offset)
            self.indentation -= 1
            self.new_lines = 1
        if node.limit is not None:
            self.write('LIMIT')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.limit)
            self.indentation -= 1
            self.new_lines = 1

    def visit_NamespaceAliasDeclNode(self, node):
        if node.alias:
            self.write(node.alias)
            self.write(' := ')
        self.write('NAMESPACE ')
        self.write(node.namespace)

    def visit_ExpressionAliasDeclNode(self, node):
        self.write(node.alias)
        self.write(' := ')
        self.visit(node.expr)

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ')
            self.write('"')
            self.write(node.alias)
            self.write('"')

    def visit_SortExprNode(self, node):
        self.visit(node.path)
        if node.direction:
            self.write(' ')
            self.write(node.direction)
        if node.nones_order:
            self.write(' NONES ')
            self.write(node.nones_order)

    def visit_ExistsPredicateNode(self, node):
        self.write('EXISTS (')
        self.visit(node.expr)
        self.write(')')

    def visit_UnaryOpNode(self, node):
        self.write(node.op)
        self.write(' ')
        self.visit(node.operand)

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_SequenceNode(self, node):
        self.write('(')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        self.write(')')

    def visit_PathNode(self, node):
        for i, e in enumerate(node.steps):
            if i > 0:
                if not isinstance(e, caosql_ast.LinkPropExprNode):
                    self.write('.')
            self.visit(e)

        if node.pathspec:
            self._visit_pathspec(node.pathspec)

    def _visit_pathspec(self, pathspec):
        if pathspec:
            self.write('[')
            self.indentation += 1
            self.new_lines = 1
            for i, spec in enumerate(pathspec):
                if i > 0:
                    self.write(', ')
                    self.new_lines = 1
                self.visit(spec)
            self.indentation -= 1
            self.new_lines = 1
            self.write(']')

    def visit_PathStepNode(self, node):
        if node.namespace:
            self.write('[%s.%s]' % (node.namespace, node.expr))
        else:
            self.write(node.expr)

    def visit_LinkExprNode(self, node):
        self.visit(node.expr)

    def visit_LinkPropExprNode(self, node):
        self.visit(node.expr)

    def visit_LinkNode(self, node, quote=True):
        if node.type == 'property':
            self.write('@')

        if node.namespace or node.target or node.direction:
            if quote:
                self.write('[')
            if node.direction and node.direction != '>':
                self.write(node.direction)
            if node.namespace:
                self.write('%s.%s' % (node.namespace, node.name))
            else:
                self.write(node.name)
            if node.target and node.type != 'property':
                if node.target.module:
                    self.write('({}.{})'.format(
                        node.target.module, node.target.name))
                else:
                    self.write('({})'.format(node.target.name))
            if quote:
                self.write(']')
        else:
            self.write(node.name)

    def visit_SelectTypeRefNode(self, node):
        self.write('__type__.')
        for i, attr in enumerate(node.attrs):
            if i > 0:
                self.write('.')
            self.visit(attr)

    def visit_SelectPathSpecNode(self, node):
        # PathSpecNode can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.
        if node.where or node.orderby or node.offset or node.limit:
            self.write('(')

        self.visit_LinkNode(node.expr.expr, quote=False)

        if node.where:
            self.write(' WHERE ')
            self.visit(node.where)

        if node.orderby:
            self.write(' ORDER BY ')
            for i, e in enumerate(node.orderby):
                if i > 0:
                    self.write(', ')
                self.visit(e)

        if node.offset:
            self.write(' OFFSET ')
            self.visit(node.offset)

        if node.limit:
            self.write(' LIMIT ')
            self.visit(node.limit)

        if node.where or node.orderby or node.offset or node.limit:
            self.write(')')

        if node.recurse:
            self.write('*')
            self.visit(node.recurse)

        if node.pathspec:
            self._visit_pathspec(node.pathspec)

    def visit_ConstantNode(self, node):
        if node.value is not None:
            try:
                caosql_repr = node.value.__mm_caosql__
            except AttributeError:
                if isinstance(node.value, str):
                    self.write(caosql_quote.quote_literal(node.value))
                elif isinstance(node.value, AST):
                    self.visit(node.value)
                else:
                    self.write(str(node.value))
            else:
                self.write(caosql_repr())

        elif node.index is not None:
            self.write('$')
            if '.' in node.index:
                self.write('[')
            self.write(node.index)
            if '.' in node.index:
                self.write(']')
        else:
            self.write('None')

    def visit_FunctionCallNode(self, node):
        if isinstance(node.func, tuple):
            self.write('::'.join(node.func))
        else:
            self.write(node.func)

        self.write('(')

        for i, arg in enumerate(node.args):
            if i > 0:
                self.write(', ')
            self.visit(arg)

        if node.agg_sort:
            self.write(' ORDER BY ')
            for i, sortexpr in enumerate(node.agg_sort):
                if i > 0:
                    self.write(', ')
                self.visit(sortexpr)

        self.write(')')

        if node.window:
            self.write(' OVER (')

            if node.window.partition:
                self.write('PARTITION BY')

                count = len(node.window.partition)
                for i, groupexpr in enumerate(node.window.partition):
                    self.visit(groupexpr)
                    if i != count - 1:
                        self.write(',')

            if node.window.orderby:
                self.write(' ORDER BY ')
                count = len(node.window.orderby)
                for i, sortexpr in enumerate(node.window.orderby):
                    self.visit(sortexpr)
                    if i != count - 1:
                        self.write(',')

            self.write(')')

    def visit_NamedArgNode(self, node):
        self.write(node.name)
        self.write(' := ')
        self.visit(node.arg)

    def visit_TypeRefNode(self, node):
        self.write('type(')
        self.visit(node.expr)
        self.write(')')

    def visit_TypeCastNode(self, node):
        self.write('CAST(')
        self.visit(node.expr)
        self.write(' AS ')
        if isinstance(node.type, tuple):
            if node.type[0] is list:
                if '.' in node.type[1]:
                    self.write('[')
                self.write(node.type[1])
                if '.' in node.type[1]:
                    self.write(']')
                self.write('[]')
            else:
                raise ValueError('unexpected collection type: {!r}'.format(node.type[0]))
        else:
            if '.' in node.type:
                self.write('[')
            self.write(node.type)
            if '.' in node.type:
                self.write(']')
        self.write(')')

    def visit_IndirectionNode(self, node):
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        for indirection in node.indirection:
            self.visit(indirection)

    def visit_SliceNode(self, node):
        self.write('[')
        if node.start:
            self.visit(node.start)
        self.write(':')
        if node.stop:
            self.visit(node.stop)
        self.write(']')

    def visit_IndexNode(self, node):
        self.write('[')
        self.visit(node.index)
        self.write(']')

    def visit_PrototypeRefNode(self, node):
        self.write('[')
        self.write(node.module)
        self.write('.')
        self.write(node.name)
        self.write(']')

    def visit_NoneTestNode(self, node):
        self.visit(node.expr)
        self.write(' IS None')


generate_source = CaosQLSourceGenerator.to_source
