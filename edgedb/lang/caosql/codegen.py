##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.exceptions import MetamagicError
from metamagic.utils.ast import codegen

from . import ast as caosql_ast


class CaosQLSourceGeneratorError(MetamagicError):
    pass


class CaosQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise CaosQLSourceGeneratorError('No method to generate code for %s' % node.__class__.__name__)

    def visit_SelectQueryNode(self, node):
        self.write('SELECT ')
        for i, e in enumerate(node.targets):
            if i > 0:
                self.write(', ')
            self.visit(e)
        if node.where:
            self.write(' WHERE ')
            self.visit(node.where)
        if node.groupby:
            self.write(' GROUP BY ')
            for i, e in enumerate(node.groupby):
                if i > 0:
                    self.write(', ')
                self.visit(e)
        if node.orderby:
            self.write(' ORDER BY ')
            for i, e in enumerate(node.orderby):
                if i > 0:
                    self.write(', ')
                self.visit(e)
        if node.offset is not None:
            self.write(' OFFSET ')
            self.visit(node.offset)
        if node.limit is not None:
            self.write(' LIMIT ')
            self.visit(node.limit)

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)
        if node.alias:
            self.write(' AS ')
            self.write(node.alias)

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
                if isinstance(e, caosql_ast.LinkPropExprNode):
                    self.write('@')
                else:
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
        if node.namespace or node.target or node.direction:
            if quote:
                self.write('[')
            if node.direction:
                self.write(node.direction)
            self.write('%s.%s' % (node.namespace, node.name))
            if node.target:
                self.write('({}.{})'.format(node.target.module, node.target.name))
            if quote:
                self.write(']')
        else:
            self.write(node.name)

    def visit_SelectPathSpecNode(self, node):
        # PathSpecNode can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.
        self.visit_LinkNode(node.expr.expr, quote=False)

        if node.pathspec:
            self._visit_pathspec(node.pathspec)

    def visit_ConstantNode(self, node):
        if node.value is not None:
            if isinstance(node.value, str):
                self.write("'%s'" % node.value)
            else:
                self.write("%s" % node.value)
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

        self.write(')')

    def visit_TypeRefNode(self, node):
        self.write('type(')
        self.visit(node.expr)
        self.write(')')

    def visit_TypeCastNode(self, node):
        self.write('CAST (')
        self.visit(node.expr)
        self.write(' AS [')
        self.write(node.type)
        self.write('])')
