##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.ast import codegen, AST, base

from . import ast as edgeql_ast
from . import quote as edgeql_quote


_module_name_re = re.compile(r'^(?!=\d)\w+(\.(?!=\d)\w+)*$')


def any_ident_to_str(ident):
    if _module_name_re.match(ident):
        return ident
    else:
        return ident_to_str(ident)


def ident_to_str(ident):
    return edgeql_quote.disambiguate_identifier(ident)


class EdgeQLSourceGeneratorError(EdgeDBError):
    pass


class EdgeQLSourceGenerator(codegen.SourceGenerator):
    def visit(self, node, **kwargs):
        method = 'visit_' + node.__class__.__name__
        if method == 'visit_list':
            return self.visit_list(node, terminator=';')
        else:
            visitor = getattr(self, method, self.generic_visit)
            return visitor(node, **kwargs)

    def generic_visit(self, node):
        raise EdgeQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__)

    def _visit_aliases(self, node):
        if node.aliases:
            self.write('WITH')
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(node.aliases)
            self.new_lines = 1
            self.indentation -= 1

    def visit_CGENode(self, node):
        self.write(ident_to_str(node.alias))
        self.write(' := ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1

    def _visit_returning(self, node):
        if node.targets:
            self.new_lines = 1
            self.write('RETURNING')
            if node.single:
                self.write(' SINGLETON')
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(node.targets)
            self.indentation -= 1

    def visit_InsertQueryNode(self, node):
        # need to parenthesise when INSERT appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDLNode))

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('INSERT')
        self.indentation += 1
        self.new_lines = 1
        self.visit(node.subject, parenthesise=False)
        self.indentation -= 1
        self.new_lines = 1

        if node.pathspec:
            self.indentation += 1
            self._visit_pathspec(node.pathspec)
            self.indentation -= 1

        if node.source:
            self.write(' FROM')
            self.indentation += 1
            self.new_lines = 1
            self.visit(node.source)
            self.indentation -= 1
            self.new_lines = 1

        self._visit_returning(node)
        if parenthesise:
            self.write(')')

    def visit_UpdateQueryNode(self, node):
        # need to parenthesise when UPDATE appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDLNode))

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('UPDATE')
        self.indentation += 1
        self.new_lines = 1
        self.visit(node.subject, parenthesise=False)
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

        self._visit_returning(node)
        if parenthesise:
            self.write(')')

    def visit_UpdateExprNode(self, node):
        self.visit(node.expr)
        self.write(' = ')
        self.visit(node.value)

    def visit_DeleteQueryNode(self, node):
        # need to parenthesise when DELETE appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDLNode))

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('DELETE ')
        self.visit(node.subject, parenthesise=False)

        if node.where:
            self.write(' WHERE ')
            self.visit(node.where)
        self._visit_returning(node)
        if parenthesise:
            self.write(')')

    def visit_SelectQueryNode(self, node):
        # need to parenthesise when SELECT appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDLNode))

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        if node.op:
            # Upper level set operation node (UNION/INTERSECT)
            self.visit(node.op_larg)
            self.new_lines = 1
            self.write(' ', node.op, ' ')
            self.new_lines = 1
            self.visit(node.op_rarg)
        else:
            self.write('SELECT')
            if node.single:
                self.write(' SINGLETON')
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
                self.visit_list(node.groupby, separator=' THEN')
                self.new_lines = 1
                self.indentation -= 1
            if node.having:
                self.write('HAVING')
                self.new_lines = 1
                self.indentation += 1
                self.visit(node.having)
                self.new_lines = 1
                self.indentation -= 1

        if node.orderby:
            self.write('ORDER BY')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.orderby, separator=' THEN')
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
        if parenthesise:
            self.write(')')

    def visit_ValuesQueryNode(self, node):
        # need to parenthesise when VALUES appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDLNode))

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)
        self.write('VALUES')
        self.indentation += 1
        self.new_lines = 1
        for i, e in enumerate(node.targets):
            if i > 0:
                self.write(',')
                self.new_lines = 1
            self.visit(e)
        self.indentation -= 1
        self.new_lines = 1

        if node.orderby:
            self.write('ORDER BY')
            self.new_lines = 1
            self.indentation += 1
            self.visit_list(node.orderby, separator=' THEN')
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

        if parenthesise:
            self.write(')')

    def visit_NamespaceAliasDeclNode(self, node):
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
        self.write('MODULE ')
        self.write(any_ident_to_str(node.namespace))

    def visit_ExpressionAliasDeclNode(self, node):
        self.write(ident_to_str(node.alias))
        self.write(' := ')
        self.visit(node.expr)

    def visit_DetachedPathDeclNode(self, node):
        self.write(ident_to_str(node.alias))
        self.write(' := DETACHED ')
        self.visit(node.expr)

    def visit_SelectExprNode(self, node):
        self.visit(node.expr)

    def visit_SortExprNode(self, node):
        self.visit(node.path)
        if node.direction:
            self.write(' ')
            self.write(node.direction)
        if node.nones_order:
            self.write(' NULLS ')
            self.write(node.nones_order.upper())

    def visit_ExistsPredicateNode(self, node):
        self.write('EXISTS (')
        self.visit(node.expr)
        self.write(')')

    def visit_UnaryOpNode(self, node):
        op = str(node.op).upper()
        self.write(op)
        if op.isalnum():
            self.write(' ')
        self.visit(node.operand)

    def visit_BinOpNode(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_IfElseNode(self, node):
        self.write('(')
        self.visit(node.if_expr)
        self.write(' IF ')
        self.visit(node.condition)
        self.write(' ELSE ')
        self.visit(node.else_expr)
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

    def visit_ArrayNode(self, node):
        self.write('[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_MappingNode(self, node):
        self.write('{')
        for i, (key, value) in enumerate(node.items):
            if i > 0:
                self.write(', ')
            self.visit(key)
            self.write(': ')
            self.visit(value)

        self.write('}')

    def visit_PathNode(self, node, *, withtarget=True, parenthesise=True):
        for i, e in enumerate(node.steps):
            if i > 0:
                if not isinstance(e, edgeql_ast.LinkPropExprNode):
                    self.write('.')

            if i == 0 and isinstance(e, edgeql_ast.PathStepNode):
                self.visit(e, withtarget=withtarget, parenthesise=parenthesise)
            else:
                self.visit(e, withtarget=withtarget)

        if node.pathspec:
            self.write(' ')
            self._visit_pathspec(node.pathspec)

    def _visit_pathspec(self, pathspec):
        if pathspec:
            self.write('{')
            self.indentation += 1
            self.new_lines = 1
            self.visit_list(pathspec)
            self.indentation -= 1
            self.new_lines = 1
            self.write('}')

    def visit_PathStepNode(self, node, *, withtarget=True, parenthesise=True):
        # XXX: this one and other places need identifier quoting
        if node.namespace:
            txt = '({}::{})' if parenthesise else '{}::{}'
            self.write(txt.format(ident_to_str(node.namespace),
                                  ident_to_str(node.expr)))
        else:
            self.write(ident_to_str(node.expr))

    def visit_LinkExprNode(self, node, *, withtarget=True):
        self.visit(node.expr, withtarget=withtarget)

    def visit_LinkPropExprNode(self, node, *, withtarget=True):
        self.visit(node.expr, withtarget=withtarget)

    def visit_LinkNode(self, node, *, quote=True, withtarget=True):
        if node.type == 'property':
            self.write('@')
        elif node.direction and node.direction != '>':
            self.write(node.direction)

        if node.namespace:
            self.write('({}::{})'.format(ident_to_str(node.namespace),
                                         ident_to_str(node.name)))
        else:
            self.write(ident_to_str(node.name))

        if withtarget and node.target and node.type != 'property':
            self.write('[TO ')
            self.visit(node.target, parenthesise=False)
            self.write(']')

    def visit_SelectPathSpecNode(self, node):
        # PathSpecNode can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.

        self.visit(node.expr, withtarget=False)
        if node.recurse:
            self.write('*')
            if node.recurse_limit:
                self.visit(node.recurse_limit)

        if not node.compexpr and (
                node.pathspec or node.expr.steps[-1].expr.target):
            self.write(': ')
            if node.expr.steps[-1].expr.target:
                self.visit(node.expr.steps[-1].expr.target)
            if node.pathspec:
                self._visit_pathspec(node.pathspec)

        if node.where:
            self.write(' WHERE ')
            self.visit(node.where)

        if node.orderby:
            self.write(' ORDER BY ')
            self.visit_list(node.orderby, separator=' THEN', newlines=False)

        if node.offset:
            self.write(' OFFSET ')
            self.visit(node.offset)

        if node.limit:
            self.write(' LIMIT ')
            self.visit(node.limit)

        if node.compexpr:
            self.write(' := ')
            self.visit(node.compexpr)

            if node.pathspec:
                self._visit_pathspec(node.pathspec)

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
            index = str(node.index)
            if '.' in index:
                self.write('{')
            self.write(index)
            if '.' in index:
                self.write('}')
        else:
            self.write('NULL')

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

        if node.agg_filter:
            self.write(' WHERE ')
            self.visit(node.agg_filter)

        if node.agg_sort:
            self.write(' ORDER BY ')
            self.visit_list(node.agg_sort, separator=' THEN')

        self.write(')')

        if node.window:
            self.write(' OVER (')
            self.new_lines = 1
            self.indentation += 1

            if node.window.partition:
                self.write('PARTITION BY ')
                self.visit_list(node.window.partition, newlines=False)
                self.new_lines = 1

            if node.window.orderby:
                self.write('ORDER BY ')
                self.visit_list(node.window.orderby, separator=' THEN')

            self.indentation -= 1
            self.new_lines = 1
            self.write(')')

    def visit_NamedArgNode(self, node):
        self.write(node.name)
        self.write(' := ')
        self.visit(node.arg)

    def visit_TypeCastNode(self, node):
        self.write('<')
        self.visit(node.type)
        self.write('>')
        self.visit(node.expr)

    def visit_TypeInterpretationNode(self, node, *, withtarget=True):
        self.write('(')
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

    def visit_ClassRefNode(self, node, *, parenthesise=True):
        if node.module and parenthesise:
            self.write('(')

        if node.module:
            self.write(ident_to_str(node.module))
            self.write('::')

        self.write(ident_to_str(node.name))

        if node.module and parenthesise:
            self.write(')')

    def visit_NoneTestNode(self, node):
        self.visit(node.expr)
        self.write(' IS None')

    def visit_TypeNameNode(self, node):
        if isinstance(node.maintype, edgeql_ast.PathNode):
            self.visit(node.maintype)
        else:
            self.visit(node.maintype, parenthesise=False)
        if node.subtypes:
            self.write('<')
            self.visit_list(node.subtypes, newlines=False, parenthesise=False)
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
                self.visit(base, parenthesise=False)

            if len(node.bases) > 1:
                self.write(')')

    def _visit_CreateObjectNode(self, node, *object_keywords, after_name=None):
        self._visit_aliases(node)
        self.write('CREATE', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        if after_name:
            after_name()
        if node.commands:
            self.write(' {')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.commands)
            self.indentation -= 1
            self.write('}')
        self.new_lines = 1

    def _visit_AlterObjectNode(self, node, *object_keywords, allow_short=True):
        self._visit_aliases(node)
        self.write('ALTER', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
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
        self._visit_aliases(node)
        self.write('DROP', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        if node.commands:
            self.write(' {')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.commands)
            self.indentation -= 1
            self.write('}')
        self.new_lines = 1

    def visit_RenameNode(self, node):
        self.write('RENAME TO ')
        self.visit(node.new_name, parenthesise=False)

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
        def after_name():
            if node.parents:
                self.write(' FROM ')
                self.visit(node.parents)

            if node.target:
                self.write(' TO ')
                from edgedb.lang.schema import generate_source as schema_sg
                self.write(edgeql_quote.dollar_quote_literal(
                    schema_sg(node.target)))
        self._visit_CreateObjectNode(node, 'DELTA', after_name=after_name)

    def visit_CommitDeltaNode(self, node):
        self._visit_aliases(node)
        self.write('COMMIT DELTA')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        self.new_lines = 1

    def visit_GetDeltaNode(self, node):
        self._visit_aliases(node)
        self.write('GET DELTA')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
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
        self.write(' := ')
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
        def after_name():
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')
            self.write(' RETURNING ')
            if node.single:
                self.write('SINGLETON ')
            self.visit(node.returning)

        self._visit_CreateObjectNode(
            node,
            'AGGREGATE' if node.aggregate else 'FUNCTION',
            after_name=after_name)

    def visit_AlterFunctionNode(self, node):
        self._visit_AlterObjectNode(node, 'FUNCTION')

    def visit_DropFunctionNode(self, node):
        self._visit_DropObjectNode(node, 'FUNCTION')

    def visit_FuncArgNode(self, node):
        if node.mode:
            self.write(node.mode, ' ')
        self.write(ident_to_str(node.name), ' ')
        self.visit(node.type)

        if node.default:
            self.write(' = ')
            self.visit(node.default)

    @classmethod
    def to_source(
            cls, node, indent_with=' ' * 4, add_line_information=False,
            pretty=True):
        # make sure that all the parents are properly set
        #
        if isinstance(node, (list, tuple)):
            for n in node:
                base.fix_parent_links(n)
        else:
            base.fix_parent_links(node)

        return super().to_source(
            node, indent_with, add_line_information, pretty)


generate_source = EdgeQLSourceGenerator.to_source
