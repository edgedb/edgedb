##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.ast import codegen, AST

from . import ast as edgeql_ast
from . import quote as edgeql_quote


class EdgeQLSourceGeneratorError(EdgeDBError):
    pass


class EdgeQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise EdgeQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__)

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

    def visit_InsertQueryNode(self, node):
        self._visit_namespaces(node)
        self._visit_cges(node.cges)

        self.write('INSERT')

        self.indentation += 1
        self.new_lines = 1
        self.visit(node.subject)
        self.indentation -= 1
        self.new_lines = 1

        if node.pathspec:
            self.indentation += 1
            self._visit_pathspec(node.pathspec)
            self.indentation -= 1

        if node.targets:
            self.new_lines = 1
            self.write('RETURNING')
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(node.targets)
            self.indentation -= 1

    def visit_UpdateQueryNode(self, node):
        self._visit_namespaces(node)
        self._visit_cges(node.cges)

        self.write('UPDATE')

        self.indentation += 1
        self.new_lines = 1
        self.visit(node.subject)
        self.indentation -= 1
        self.new_lines = 1

        if node.pathspec:
            self.indentation += 1
            self._visit_pathspec(node.pathspec)
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
            self.visit_list(node.targets)
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

        if count == 1:
            self.write(',')

        self.write(')')

    def visit_MappingNode(self, node):
        self.write('{')
        for i, (key, value) in enumerate(node.items):
            if i > 0:
                self.write(', ')
            self.visit(key)
            self.write(': ')
            self.visit(value)

        self.write('}')

    def visit_PathNode(self, node):
        for i, e in enumerate(node.steps):
            if i > 0:
                if not isinstance(e, edgeql_ast.LinkPropExprNode):
                    self.write('.')
            self.visit(e)

        if node.pathspec:
            self._visit_pathspec(node.pathspec)

    def _visit_pathspec(self, pathspec):
        if pathspec:
            self.write('{')
            self.indentation += 1
            self.new_lines = 1
            for i, spec in enumerate(pathspec):
                if i > 0:
                    self.write(', ')
                    self.new_lines = 1
                self.visit(spec)
            self.indentation -= 1
            self.new_lines = 1
            self.write('}')

    def visit_PathStepNode(self, node):
        if node.namespace:
            self.write('{%s::%s}' % (node.namespace, node.expr))
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
                self.write('{')
            if node.direction and node.direction != '>':
                self.write(node.direction)
            if node.namespace:
                self.write('%s::%s' % (node.namespace, node.name))
            else:
                self.write(node.name)
            if node.target and node.type != 'property':
                if node.target.module:
                    self.write('({}::{})'.format(
                        node.target.module, node.target.name))
                else:
                    self.write('({})'.format(node.target.name))
            if quote:
                self.write('}')
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

        if node.compexpr:
            self.write(' := ')
            self.visit(node.compexpr)

    def visit_ConstantNode(self, node):
        if node.value is not None:
            try:
                edgeql_repr = node.value.__mm_edgeql__
            except AttributeError:
                if isinstance(node.value, str):
                    self.write(edgeql_quote.quote_literal(node.value))
                elif isinstance(node.value, AST):
                    self.visit(node.value)
                else:
                    self.write(str(node.value))
            else:
                self.write(edgeql_repr())

        elif node.index is not None:
            self.write('$')
            if '.' in node.index:
                self.write('{')
            self.write(node.index)
            if '.' in node.index:
                self.write('}')
        else:
            self.write('None')

    def visit_DefaultValueNode(self, node):
        self.write('DEFAULT')

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

        if node.agg_filter:
            self.write(' FILTER (WHERE ')
            self.visit(node.agg_filter)
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
        self.visit(node.type)
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
        if node.module or '.' in node.name:
            self.write('{')

        if node.module:
            self.write(node.module)
            self.write('::')

        self.write(node.name)

        if node.module or '.' in node.name:
            self.write('}')

    def visit_NoneTestNode(self, node):
        self.visit(node.expr)
        self.write(' IS None')

    def visit_TypeNameNode(self, node):
        if '.' in node.maintype:
            self.write('{')
        self.write(node.maintype)
        if '.' in node.maintype:
            self.write('}')
        if node.subtype is not None:
            self.write('<')
            self.visit(node.subtype)
            self.write('>')

    # DDL nodes

    def visit_ExpressionTextNode(self, node):
        self.write('(', node.expr, ')')

    def visit_PositionNode(self, node):
        self.write(node.position)
        if node.ref:
            self.write(' ')
            self.visit(node.ref)

    def _ddl_visit_bases(self, node):
        if getattr(node, 'bases', None):
            self.write(' INHERITING ')
            if len(node.bases) > 1:
                self.write('(')
            for i, base in enumerate(node.bases):
                if i > 0:
                    self.write(', ')
                self.visit(base)

            if len(node.bases) > 1:
                self.write(')')

    def _visit_CreateObjectNode(self, node, *object_keywords, after_name=None):
        self._visit_namespaces(node)
        self.write('CREATE', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if after_name:
            after_name()
        if node.commands:
            self.write(' {')
            self.new_lines = 1
            self.indentation += 1
            for cmd in node.commands:
                self.visit(cmd)
                self.new_lines = 1
            self.indentation -= 1
            self.write('}')
        self.new_lines = 1

    def _visit_AlterObjectNode(self, node, *object_keywords, allow_short=True):
        self._visit_namespaces(node)
        self.write('ALTER', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if node.commands:
            if len(node.commands) == 1 and allow_short:
                self.write(' ')
                self.visit(node.commands[0])
            else:
                self.write(' {')
                self.new_lines = 1
                self.indentation += 1
                for cmd in node.commands:
                    self.visit(cmd)
                    self.new_lines = 1
                self.indentation -= 1
                self.write('}')
        self.new_lines = 1

    def _visit_DropObjectNode(self, node, *object_keywords):
        self._visit_namespaces(node)
        self.write('DROP', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if node.commands:
            self.write(' {')
            self.new_lines = 1
            self.indentation += 1
            for cmd in node.commands:
                self.visit(cmd)
                self.new_lines = 1
            self.indentation -= 1
            self.write('}')
        self.new_lines = 1

    def visit_RenameNode(self, node):
        self.write('RENAME TO ')
        self.visit(node.new_name)

    def visit_SetSpecialFieldNode(self, node):
        if node.value:
            self.write('SET ')
        else:
            self.write('DROP ')

        if node.name == 'is_abstract':
            self.write('ABSTRACT')
        elif node.name == 'is_final':
            self.write('FINAL')
        else:
            raise EdgeQLSourceGeneratorError(
                'unknown special field: {!r}'.format(node.name))

    def visit_AlterAddInheritNode(self, node):
        self.write('INHERIT ')
        self.visit_list(node.bases)
        if node.position is not None:
            self.write(' ')
            self.visit(node.position)

    def visit_AlterDropInheritNode(self, node):
        self.write('DROP INHERIT ')
        self.visit_list(node.bases)

    def visit_CreateDatabaseNode(self, node):
        self._visit_CreateObjectNode(node, 'DATABASE')

    def visit_AlterDatabaseNode(self, node):
        self._visit_AlterObjectNode(node, 'DATABASE')

    def visit_DropDatabaseNode(self, node):
        self._visit_DropObjectNode(node, 'DATABASE')

    def visit_CreateDeltaNode(self, node):
        self._visit_CreateObjectNode(node, 'DELTA')

    def visit_CommitDeltaNode(self, node):
        self._visit_namespaces(node)
        self.write('COMMIT DELTA')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_AlterDeltaNode(self, node):
        self._visit_AlterObjectNode(node, 'DELTA')

    def visit_DropDeltaNode(self, node):
        self._visit_DropObjectNode(node, 'DELTA')

    def visit_CreateModuleNode(self, node):
        self._visit_CreateObjectNode(node, 'MODULE')

    def visit_AlterModuleNode(self, node):
        self._visit_AlterObjectNode(node, 'MODULE')

    def visit_DropModuleNode(self, node):
        self._visit_DropObjectNode(node, 'MODULE')

    def visit_CreateActionNode(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, 'ACTION', after_name=after_name)

    def visit_AlterActionNode(self, node):
        self._visit_AlterObjectNode(node, 'ACTION')

    def visit_DropActionNode(self, node):
        self._visit_DropObjectNode(node, 'ACTION')

    def visit_CreateEventNode(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, 'EVENT', after_name=after_name)

    def visit_CreateAttributeNode(self, node):
        def after_name():
            self.write(' ')
            self.visit(node.type)

        self._visit_CreateObjectNode(node, 'ATTRIBUTE', after_name=after_name)

    def visit_DropAttributeNode(self, node):
        self._visit_DropObjectNode(node, 'ATTRIBUTE')

    def visit_CreateAttributeValueNode(self, node):
        self.write('SET ')
        self.visit(node.name)
        if (isinstance(node.value, edgeql_ast.ExpressionTextNode) or
                node.as_expr):
            self.write(' := ')
        else:
            self.write(' = ')
        self.visit(node.value)

    def visit_AlterAttributeValueNode(self, node):
        self.write('SET ')
        self.visit(node.name)
        self.write(' = ')
        self.visit(node.value)

    def visit_DropAttributeValueNode(self, node):
        self.write('DROP ATTRIBUTE ')
        self.visit(node.name)

    def visit_CreateConstraintNode(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, 'CONSTRAINT', after_name=after_name)

    def visit_AlterConstraintNode(self, node):
        self._visit_AlterObjectNode(node, 'CONSTRAINT')

    def visit_DropConstraintNode(self, node):
        self._visit_DropObjectNode(node, 'CONSTRAINT')

    def visit_CreateConcreteConstraintNode(self, node):
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        keywords.append('CONSTRAINT')
        self._visit_CreateObjectNode(node, *keywords)

    def visit_AlterConcreteConstraintNode(self, node):
        self._visit_AlterObjectNode(node, 'CONSTRAINT', allow_short=False)

    def visit_DropConcreteConstraintNode(self, node):
        self._visit_DropObjectNode(node, 'CONSTRAINT')

    def visit_CreateAtomNode(self, node):
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('ATOM')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, *keywords, after_name=after_name)

    def visit_AlterAtomNode(self, node):
        self._visit_AlterObjectNode(node, 'ATOM')

    def visit_DropAtomNode(self, node):
        self._visit_DropObjectNode(node, 'ATOM')

    def visit_CreateLinkPropertyNode(self, node):
        self._visit_CreateObjectNode(node, 'LINK PROPERTY')

    def visit_AlterLinkPropertyNode(self, node):
        self._visit_AlterObjectNode(node, 'LINK PROPERTY')

    def visit_DropLinkPropertyNode(self, node):
        self._visit_DropObjectNode(node, 'LINK PROPERTY')

    def visit_CreateConcreteLinkPropertyNode(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('LINK PROPERTY')

        def after_name():
            self.write(' TO ')
            self.visit(node.target)
        self._visit_CreateObjectNode(node, *keywords, after_name=after_name)

    def visit_AlterConcreteLinkPropertyNode(self, node):
        self._visit_AlterObjectNode(node, 'LINK PROPERTY', allow_short=False)

    def visit_DropConcreteLinkPropertyNode(self, node):
        self._visit_DropObjectNode(node, 'LINK PROPERTY')

    def visit_CreateLinkNode(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, 'LINK', after_name=after_name)

    def visit_AlterLinkNode(self, node):
        self._visit_AlterObjectNode(node, 'LINK')

    def visit_DropLinkNode(self, node):
        self._visit_DropObjectNode(node, 'LINK')

    def visit_CreateConcreteLinkNode(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('LINK')

        def after_name():
            self.write(' TO ')
            self.visit_list(node.targets, newlines=False)
        self._visit_CreateObjectNode(node, *keywords, after_name=after_name)

    def visit_AlterConcreteLinkNode(self, node):
        self._visit_AlterObjectNode(node, 'LINK', allow_short=False)

    def visit_DropConcreteLinkNode(self, node):
        self._visit_DropObjectNode(node, 'LINK')

    def visit_AlterTargetNode(self, node):
        self.write('ALTER TARGET ')
        self.visit_list(node.targets, newlines=False)

    def visit_CreateConceptNode(self, node):
        keywords = []

        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('CONCEPT')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObjectNode(node, *keywords, after_name=after_name)

    def visit_AlterConceptNode(self, node):
        self._visit_AlterObjectNode(node, 'CONCEPT')

    def visit_DropConceptNode(self, node):
        self._visit_DropObjectNode(node, 'CONCEPT')

    def visit_CreateIndexNode(self, node):
        after_name = lambda: self.write('(', node.expr, ')')
        self._visit_CreateObjectNode(node, 'INDEX', after_name=after_name)

    def visit_DropIndexNode(self, node):
        self._visit_DropObjectNode(node, 'INDEX')

    def visit_CreateLocalPolicyNode(self, node):
        self.write('CREATE POLICY FOR ')
        self.visit(node.event)
        self.write(' TO ')
        self.visit_list(node.actions)

    def visit_AlterLocalPolicyNode(self, node):
        self.write('ALTER POLICY FOR ')
        self.visit(node.event)
        self.write(' TO ')
        self.visit_list(node.actions)

    def visit_CreateFunctionNode(self, node):
        self._visit_CreateObjectNode(node, 'FUNCTION')

    def visit_AlterFunctionNode(self, node):
        self._visit_AlterObjectNode(node, 'FUNCTION')

    def visit_DropFunctionNode(self, node):
        self._visit_DropObjectNode(node, 'FUNCTION')


generate_source = EdgeQLSourceGenerator.to_source
