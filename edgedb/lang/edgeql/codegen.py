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

    def visit_CGE(self, node):
        self.write(ident_to_str(node.alias))
        self.write(' := ')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node.expr)
        self.indentation -= 1
        self.new_lines = 1

    def visit_Coalesce(self, node):
        self.visit_list(node.args, separator=' ?? ', newlines=False)

    def _visit_returning(self, node):
        if node.result is not None:
            self.new_lines = 1
            self.write('RETURNING')
            if node.single:
                self.write(' SINGLETON')
            self.indentation += 1
            self.new_lines = 1
            self.visit(node.result)
            self.indentation -= 1

    def visit_InsertQuery(self, node):
        # need to parenthesise when INSERT appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDL))

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

    def visit_UpdateQuery(self, node):
        # need to parenthesise when UPDATE appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDL))

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

    def visit_UpdateExpr(self, node):
        self.visit(node.expr)
        self.write(' = ')
        self.visit(node.value)

    def visit_DeleteQuery(self, node):
        # need to parenthesise when DELETE appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDL))

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

    def visit_SelectQuery(self, node):
        # need to parenthesise when SELECT appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDL))

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
            self.visit(node.result)
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

    def visit_ValuesQuery(self, node):
        # need to parenthesise when VALUES appears as an expression
        #
        parenthesise = (isinstance(node.parent, edgeql_ast.Base) and
                        not isinstance(node.parent, edgeql_ast.DDL))

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)
        self.write('VALUES')
        self.indentation += 1
        self.new_lines = 1
        self.visit_list(node.result)
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

    def visit_NamespaceAliasDecl(self, node):
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
        self.write('MODULE ')
        self.write(any_ident_to_str(node.namespace))

    def visit_ExpressionAliasDecl(self, node):
        self.write(ident_to_str(node.alias))
        self.write(' := ')
        self.visit(node.expr)

    def visit_SortExpr(self, node):
        self.visit(node.path)
        if node.direction:
            self.write(' ')
            self.write(node.direction)
        if node.nones_order:
            self.write(' EMPTY ')
            self.write(node.nones_order.upper())

    def visit_ExistsPredicate(self, node):
        self.write('EXISTS (')
        self.visit(node.expr)
        self.write(')')

    def visit_UnaryOp(self, node):
        op = str(node.op).upper()
        self.write(op)
        if op.isalnum():
            self.write(' ')
        self.visit(node.operand)

    def visit_BinOp(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_IfElse(self, node):
        self.write('(')
        self.visit(node.if_expr)
        self.write(' IF ')
        self.visit(node.condition)
        self.write(' ELSE ')
        self.visit(node.else_expr)
        self.write(')')

    def visit_Tuple(self, node):
        self.write('(')
        count = len(node.elements)
        for i, e in enumerate(node.elements):
            self.visit(e)
            if i != count - 1:
                self.write(', ')

        if count == 1:
            self.write(',')

        self.write(')')

    def visit_EmptyCollection(self, node):
        self.write('[]')

    def visit_Array(self, node):
        self.write('[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_Mapping(self, node):
        self.write('[')
        for i, (key, value) in enumerate(zip(node.keys, node.values)):
            if i > 0:
                self.write(', ')
            self.visit(key)
            self.write(' -> ')
            self.visit(value)

        self.write(']')

    def visit_Struct(self, node):
        self.write('{')
        self.indentation += 1
        self.new_lines = 1
        self.visit_list(node.elements, newlines=True, separator=',')
        self.indentation -= 1
        self.new_lines = 1
        self.write('}')

    def visit_StructElement(self, node):
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.val)

    def visit_Path(self, node, *, parenthesise=True):
        for i, e in enumerate(node.steps):
            if i > 0 or node.partial:
                if getattr(e, 'type', None) != 'property':
                    self.write('.')

            if i == 0:
                if isinstance(e, edgeql_ast.ClassRef):
                    self.visit(e, parenthesise=parenthesise)
                elif not isinstance(e, (edgeql_ast.Ptr,
                                        edgeql_ast.TypeFilter)):
                    self.write('(')
                    self.visit(e)
                    self.write(')')
                else:
                    self.visit(e)
            else:
                self.visit(e)

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

    def visit_Ptr(self, node, *, quote=True):
        if node.type == 'property':
            self.write('@')
        elif node.direction and node.direction != '>':
            self.write(node.direction)

        self.visit(node.ptr, parenthesise=True)
        if (not isinstance(node.parent.parent,
                           edgeql_ast.SelectPathSpec) and
                node.target is not None):
            self.write('[IS ')
            self.visit(node.target, parenthesise=False)
            self.write(']')

    def visit_SelectPathSpec(self, node):
        # PathSpec can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.

        self.visit(node.expr)
        if node.recurse:
            self.write('*')
            if node.recurse_limit:
                self.visit(node.recurse_limit)

        if not node.compexpr and (node.pathspec or node.expr.steps[-1].target):
            self.write(': ')
            if node.expr.steps[-1].target:
                self.visit(node.expr.steps[-1].target)
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

    def visit_Parameter(self, node):
        self.write('$')
        self.write(node.name)

    def visit_EmptySet(self, node):
        self.write('EMPTY')

    def visit_UnionSet(self, node):
        self.write('UNION')

    def visit_Constant(self, node):
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

    def visit_DefaultValue(self, node):
        self.write('DEFAULT')

    def visit_FunctionCall(self, node):
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

    def visit_NamedArg(self, node):
        self.write(node.name)
        self.write(' := ')
        self.visit(node.arg)

    def visit_TypeCast(self, node):
        self.write('<')
        self.visit(node.type)
        self.write('>')
        self.visit(node.expr)

    def visit_TypeFilter(self, node):
        self.visit(node.expr)
        self.write('[IS ')
        self.visit(node.type)
        self.write(']')

    def visit_Indirection(self, node):
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        for indirection in node.indirection:
            self.visit(indirection)

    def visit_Slice(self, node):
        self.write('[')
        if node.start:
            self.visit(node.start)
        self.write(':')
        if node.stop:
            self.visit(node.stop)
        self.write(']')

    def visit_Index(self, node):
        self.write('[')
        self.visit(node.index)
        self.write(']')

    def visit_ClassRef(self, node, *, parenthesise=True):
        if node.module and parenthesise:
            self.write('(')

        if node.module:
            self.write(ident_to_str(node.module))
            self.write('::')

        self.write(ident_to_str(node.name))

        if node.module and parenthesise:
            self.write(')')

    def visit_NoneTest(self, node):
        self.visit(node.expr)
        self.write(' IS None')

    def visit_TypeName(self, node):
        if isinstance(node.maintype, edgeql_ast.Path):
            self.visit(node.maintype)
        else:
            self.visit(node.maintype, parenthesise=False)
        if node.subtypes:
            self.write('<')
            self.visit_list(node.subtypes, newlines=False, parenthesise=False)
            self.write('>')

    # DDL nodes

    def visit_ExpressionText(self, node):
        self.write('(', node.expr, ')')

    def visit_Position(self, node):
        self.write(node.position)
        if node.ref:
            self.write(' ')
            self.visit(node.ref)

    def _ddl_visit_bases(self, node):
        if getattr(node, 'bases', None):
            self.write(' INHERITING ')
            if len(node.bases) > 1:
                self.write('(')
            for i, b in enumerate(node.bases):
                if i > 0:
                    self.write(', ')
                self.visit(b, parenthesise=False)

            if len(node.bases) > 1:
                self.write(')')

    def _visit_CreateObject(self, node, *object_keywords, after_name=None,
                            render_commands=True):
        self._visit_aliases(node)
        self.write('CREATE', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        if after_name:
            after_name()
        if node.commands and render_commands:
            self.write(' {')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.commands)
            self.indentation -= 1
            self.write('}')
        self.new_lines = 1

    def _visit_AlterObject(self, node, *object_keywords, allow_short=True):
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

    def _visit_DropObject(self, node, *object_keywords):
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

    def visit_Rename(self, node):
        self.write('RENAME TO ')
        self.visit(node.new_name, parenthesise=False)

    def visit_SetSpecialField(self, node):
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

    def visit_AlterAddInherit(self, node):
        self.write('INHERIT ')
        self.visit_list(node.bases)
        if node.position is not None:
            self.write(' ')
            self.visit(node.position)

    def visit_AlterDropInherit(self, node):
        self.write('DROP INHERIT ')
        self.visit_list(node.bases)

    def visit_CreateDatabase(self, node):
        self._visit_CreateObject(node, 'DATABASE')

    def visit_AlterDatabase(self, node):
        self._visit_AlterObject(node, 'DATABASE')

    def visit_DropDatabase(self, node):
        self._visit_DropObject(node, 'DATABASE')

    def visit_CreateDelta(self, node):
        def after_name():
            if node.parents:
                self.write(' FROM ')
                self.visit(node.parents)

            if node.target:
                self.write(' TO ', node.language, ' ')
                from edgedb.lang.schema import generate_source as schema_sg
                self.write(edgeql_quote.dollar_quote_literal(
                    schema_sg(node.target)))
        self._visit_CreateObject(node, 'MIGRATION', after_name=after_name)

    def visit_CommitDelta(self, node):
        self._visit_aliases(node)
        self.write('COMMIT MIGRATION')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        self.new_lines = 1

    def visit_GetDelta(self, node):
        self._visit_aliases(node)
        self.write('GET MIGRATION')
        self.write(' ')
        self.visit(node.name, parenthesise=False)
        self.new_lines = 1

    def visit_AlterDelta(self, node):
        self._visit_AlterObject(node, 'MIGRATION')

    def visit_DropDelta(self, node):
        self._visit_DropObject(node, 'MIGRATION')

    def visit_CreateModule(self, node):
        self._visit_CreateObject(node, 'MODULE')

    def visit_AlterModule(self, node):
        self._visit_AlterObject(node, 'MODULE')

    def visit_DropModule(self, node):
        self._visit_DropObject(node, 'MODULE')

    def visit_CreateAction(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ACTION', after_name=after_name)

    def visit_AlterAction(self, node):
        self._visit_AlterObject(node, 'ACTION')

    def visit_DropAction(self, node):
        self._visit_DropObject(node, 'ACTION')

    def visit_CreateEvent(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'EVENT', after_name=after_name)

    def visit_CreateAttribute(self, node):
        def after_name():
            self.write(' ')
            self.visit(node.type)

        self._visit_CreateObject(node, 'ATTRIBUTE', after_name=after_name)

    def visit_DropAttribute(self, node):
        self._visit_DropObject(node, 'ATTRIBUTE')

    def visit_CreateAttributeValue(self, node):
        self.write('SET ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_AlterAttributeValue(self, node):
        self.write('SET ')
        self.visit(node.name)
        self.write(' = ')
        self.visit(node.value)

    def visit_DropAttributeValue(self, node):
        self.write('DROP ATTRIBUTE ')
        self.visit(node.name)

    def visit_CreateConstraint(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'CONSTRAINT', after_name=after_name)

    def visit_AlterConstraint(self, node):
        self._visit_AlterObject(node, 'CONSTRAINT')

    def visit_DropConstraint(self, node):
        self._visit_DropObject(node, 'CONSTRAINT')

    def visit_CreateConcreteConstraint(self, node):
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        keywords.append('CONSTRAINT')
        self._visit_CreateObject(node, *keywords)

    def visit_AlterConcreteConstraint(self, node):
        self._visit_AlterObject(node, 'CONSTRAINT', allow_short=False)

    def visit_DropConcreteConstraint(self, node):
        self._visit_DropObject(node, 'CONSTRAINT')

    def visit_CreateAtom(self, node):
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('ATOM')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterAtom(self, node):
        self._visit_AlterObject(node, 'ATOM')

    def visit_DropAtom(self, node):
        self._visit_DropObject(node, 'ATOM')

    def visit_CreateLinkProperty(self, node):
        self._visit_CreateObject(node, 'LINK PROPERTY')

    def visit_AlterLinkProperty(self, node):
        self._visit_AlterObject(node, 'LINK PROPERTY')

    def visit_DropLinkProperty(self, node):
        self._visit_DropObject(node, 'LINK PROPERTY')

    def visit_CreateConcreteLinkProperty(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('LINK PROPERTY')

        def after_name():
            self.write(' TO ')
            self.visit(node.target)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteLinkProperty(self, node):
        self._visit_AlterObject(node, 'LINK PROPERTY', allow_short=False)

    def visit_DropConcreteLinkProperty(self, node):
        self._visit_DropObject(node, 'LINK PROPERTY')

    def visit_CreateLink(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'LINK', after_name=after_name)

    def visit_AlterLink(self, node):
        self._visit_AlterObject(node, 'LINK')

    def visit_DropLink(self, node):
        self._visit_DropObject(node, 'LINK')

    def visit_CreateConcreteLink(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('LINK')

        def after_name():
            self.write(' TO ')
            self.visit_list(node.targets, newlines=False)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteLink(self, node):
        self._visit_AlterObject(node, 'LINK', allow_short=False)

    def visit_DropConcreteLink(self, node):
        self._visit_DropObject(node, 'LINK')

    def visit_AlterTarget(self, node):
        self.write('ALTER TARGET ')
        self.visit_list(node.targets, newlines=False)

    def visit_CreateConcept(self, node):
        keywords = []

        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('CONCEPT')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcept(self, node):
        self._visit_AlterObject(node, 'CONCEPT')

    def visit_DropConcept(self, node):
        self._visit_DropObject(node, 'CONCEPT')

    def visit_CreateIndex(self, node):
        after_name = lambda: self.write('(', node.expr, ')')
        self._visit_CreateObject(node, 'INDEX', after_name=after_name)

    def visit_DropIndex(self, node):
        self._visit_DropObject(node, 'INDEX')

    def visit_CreateLocalPolicy(self, node):
        self.write('CREATE POLICY FOR ')
        self.visit(node.event)
        self.write(' TO ')
        self.visit_list(node.actions)

    def visit_AlterLocalPolicy(self, node):
        self.write('ALTER POLICY FOR ')
        self.visit(node.event)
        self.write(' TO ')
        self.visit_list(node.actions)

    def visit_CreateFunction(self, node):
        def after_name():
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')
            self.write(' RETURNING ')
            if node.single:
                self.write('SINGLETON ')
            self.visit(node.returning)

            if node.commands:
                self.write('{')
                self.new_lines = 1
                self.indentation += 1
                self.visit_list(node.commands, terminator=';')
                self.new_lines = 1

            if node.code.from_name:
                self.write(f' FROM {node.code.language} {typ} ')
                self.write(f'{node.code.from_name!r}')
            else:
                self.write(f' FROM {node.code.language} ')
                self.write(edgeql_quote.dollar_quote_literal(
                    node.code.code))

            if node.commands:
                self.write(';')
                self.new_lines = 1
                self.indentation -= 1
                self.write('}')

        typ = 'AGGREGATE' if node.aggregate else 'FUNCTION'
        self._visit_CreateObject(node, typ, after_name=after_name,
                                 render_commands=False)

    def visit_AlterFunction(self, node):
        self._visit_AlterObject(node, 'FUNCTION')

    def visit_DropFunction(self, node):
        self._visit_DropObject(node, 'FUNCTION')

    def visit_FuncArg(self, node):
        if node.variadic:
            self.write('*')
        if node.name is not None:
            self.write('$', ident_to_str(node.name), ': ')
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
