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


import re

from edb.lang.common.exceptions import EdgeDBError
from edb.lang.common.ast import codegen, AST, base

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

    def _needs_parentheses(self, node):
        return (
            isinstance(node.parent, edgeql_ast.Base) and
            not isinstance(node.parent, edgeql_ast.DDL)
        )

    def generic_visit(self, node, *args, **kwargs):
        raise EdgeQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__)

    def _block_ws(self, change, newlines=True):
        if newlines:
            self.indentation += change
            self.new_lines = 1
        else:
            self.write(' ')

    def _visit_aliases(self, node):
        if node.aliases or node.cardinality:
            self.write('WITH')
            self._block_ws(1)
            if node.aliases:
                self.visit_list(node.aliases)
            if node.cardinality:
                self.write(f"CARDINALITY '{node.cardinality}'")
            self._block_ws(-1)

    def _visit_filter(self, node, newlines=True):
        if node.where:
            self.write('FILTER')
            self._block_ws(1, newlines)
            self.visit(node.where)
            self._block_ws(-1, newlines)

    def _visit_order(self, node, newlines=True):
        if node.orderby:
            self.write('ORDER BY')
            self._block_ws(1, newlines)
            self.visit_list(node.orderby, separator=' THEN', newlines=newlines)
            self._block_ws(-1, newlines)

    def _visit_offset_limit(self, node, newlines=True):
        if node.offset is not None:
            self.write('OFFSET')
            self._block_ws(1, newlines)
            self.visit(node.offset)
            self._block_ws(-1, newlines)
        if node.limit is not None:
            self.write('LIMIT')
            self._block_ws(1, newlines)
            self.visit(node.limit)
            self._block_ws(-1, newlines)

    def visit_AliasedExpr(self, node):
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
            self._block_ws(1)

        self.visit(node.expr)

        if node.alias:
            self._block_ws(-1)

    def visit_Coalesce(self, node):
        self.visit_list(node.args, separator=' ?? ', newlines=False)

    def visit_InsertQuery(self, node):
        # need to parenthesise when INSERT appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('INSERT')
        self._block_ws(1)
        self.visit(node.subject)
        self._block_ws(-1)

        if node.shape:
            self.indentation += 1
            self._visit_shape(node.shape)
            self.indentation -= 1

        if parenthesise:
            self.write(')')

    def visit_UpdateQuery(self, node):
        # need to parenthesise when UPDATE appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('UPDATE')
        self._block_ws(1)
        self.visit(node.subject)
        self._block_ws(-1)

        self._visit_filter(node)

        self.new_lines = 1
        self.write('SET ')
        self._visit_shape(node.shape)

        if parenthesise:
            self.write(')')

    def visit_UpdateExpr(self, node):
        self.visit(node.expr)
        self.write(' = ')
        self.visit(node.value)

    def visit_DeleteQuery(self, node):
        # need to parenthesise when DELETE appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self.write('DELETE ')
        self.visit(node.subject)

        if parenthesise:
            self.write(')')

    def visit_SelectQuery(self, node):
        # XXX: need to parenthesise when SELECT appears as an expression,
        # the actual passed value is ignored.
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        self.write('SELECT')
        self._block_ws(1)
        if node.result_alias:
            self.write(node.result_alias, ' := ')
        self.visit(node.result)
        self._block_ws(-1)
        self._visit_filter(node)
        self._visit_order(node)
        self._visit_offset_limit(node)
        if parenthesise:
            self.write(')')

    def visit_ForQuery(self, node):
        # need to parenthesise when GROUP appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        self.write('FOR ')
        self.write(ident_to_str(node.iterator_alias))
        self.write(' IN ')
        self.visit(node.iterator)
        # guarantee an newline here
        self.new_lines = 1
        self.write('UNION ')
        if node.result_alias:
            self.write(node.result_alias, ' := ')
        self._block_ws(1)
        self.visit(node.result)
        self.indentation -= 1

        self._visit_filter(node)
        self._visit_order(node)
        self._visit_offset_limit(node)

        if parenthesise:
            self.write(')')

    def visit_GroupQuery(self, node):
        # need to parenthesise when GROUP appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        self.write('GROUP')
        self._block_ws(1)
        if node.subject_alias:
            self.write(any_ident_to_str(node.subject_alias), ' := ')
        self.visit(node.subject)
        self._block_ws(-1)
        self.write('USING')
        self._block_ws(1)
        self.visit_list(node.using)
        self._block_ws(-1)
        self.write('BY')
        self._block_ws(1)
        self.visit_list(node.by, newlines=False)
        self._block_ws(-1)
        self.write('INTO', any_ident_to_str(node.into))

        # guarantee an newline here
        self.write('UNION ')
        if node.result_alias:
            self.write(any_ident_to_str(node.result_alias), ' := ')
        self._block_ws(1)
        self.visit(node.result)
        self.indentation -= 1

        self._visit_filter(node)
        self._visit_order(node)
        self._visit_offset_limit(node)

        if parenthesise:
            self.write(')')

    def visit_GroupExpr(self, node):
        # GROUP expression is always parenthesised
        self.write('(')
        self._block_ws(1)
        self.write('GROUP ')
        if node.subject_alias:
            self.write(node.subject_alias, ' := ')
        self.visit(node.subject)
        self.write(' BY ')
        self._block_ws(1)
        self.visit_list(node.by)
        self._block_ws(-2)
        self.write(')')

    def visit_ByExpr(self, node):
        if node.each is not None:
            if node.each:
                self.write('EACH ')
            else:
                self.write('SET OF ')

        self.visit(node.expr)

    def visit_GroupBuiltin(self, node):
        self.write(node.name, '(')
        self.visit_list(node.elements, newlines=False)
        self.write(')')

    def visit_ModuleAliasDecl(self, node):
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
        self.write('MODULE ')
        self.write(any_ident_to_str(node.module))

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

    def visit_RequiredExpr(self, node):
        self.write('REQUIRED (')
        self.visit(node.expr)
        self.write(')')

    def visit_DetachedExpr(self, node):
        self.write('DETACHED ')
        self.visit(node.expr)

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

    def visit_IsOp(self, node):
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_TypeOp(self, node):
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
        self.visit_list(node.elements, newlines=False)
        if count == 1:
            self.write(',')

        self.write(')')

    def visit_Set(self, node):
        self.write('{')
        self.visit_list(node.elements, newlines=False)
        self.write('}')

    def visit_EmptyCollection(self, node):
        self.write('[]')

    def visit_Array(self, node):
        self.write('[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_NamedTuple(self, node):
        self.write('(')
        self._block_ws(1)
        self.visit_list(node.elements, newlines=True, separator=',')
        self._block_ws(-1)
        self.write(')')

    def visit_TupleElement(self, node):
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.val)

    def visit_Path(self, node):
        for i, e in enumerate(node.steps):
            if i > 0 or node.partial:
                if getattr(e, 'type', None) != 'property':
                    self.write('.')

            if i == 0:
                if isinstance(e, edgeql_ast.ObjectRef):
                    self.visit(e)
                elif isinstance(e, (edgeql_ast.Source,
                                    edgeql_ast.Subject)):
                    self.visit(e)
                elif not isinstance(e, (edgeql_ast.Ptr,
                                        edgeql_ast.Set,
                                        edgeql_ast.Tuple,
                                        edgeql_ast.NamedTuple,
                                        edgeql_ast.TypeFilter)):
                    self.write('(')
                    self.visit(e)
                    self.write(')')
                else:
                    self.visit(e)
            else:
                self.visit(e)

    def visit_Shape(self, node):
        if node.expr is not None:
            # shape.expr may be None in Function RETURNING declaration,
            # when the shape is used to described a returned struct.
            self.visit(node.expr)
        self.write(' ')
        self._visit_shape(node.elements)

    def _visit_shape(self, shape):
        if shape:
            self.write('{')
            self._block_ws(1)
            self.visit_list(shape)
            self._block_ws(-1)
            self.write('}')

    def visit_Ptr(self, node, *, quote=True):
        if node.type == 'property':
            self.write('@')
        elif node.direction and node.direction != '>':
            self.write(node.direction)

        self.visit(node.ptr)
        if (not isinstance(node.parent.parent, edgeql_ast.ShapeElement) and
                node.target is not None):
            self.write('[IS ')
            self.visit(node.target)
            self.write(']')

    def visit_ShapeElement(self, node):
        # PathSpec can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.

        if len(node.expr.steps) == 1:
            self.visit(node.expr)
        else:
            self.write('[IS ')
            self.visit(node.expr.steps[0])
            self.write('].')
            self.visit(node.expr.steps[1])

        if node.recurse:
            self.write('*')
            if node.recurse_limit:
                self.visit(node.recurse_limit)

        if not node.compexpr and (node.elements or node.expr.steps[-1].target):
            self.write(': ')
            if node.expr.steps[-1].target:
                self.visit(node.expr.steps[-1].target)
            if node.elements:
                self._visit_shape(node.elements)

        if node.where:
            self.write(' FILTER ')
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

    def visit_Parameter(self, node):
        self.write('$')
        self.write(node.name)

    def visit_Constant(self, node):
        if isinstance(node.value, str):
            self.write(edgeql_quote.quote_literal(node.value))
        elif isinstance(node.value, AST):
            self.visit(node.value)
        else:
            self.write(str(node.value))

    def visit_FunctionCall(self, node):
        if isinstance(node.func, tuple):
            self.write('::'.join(node.func))
        else:
            self.write(ident_to_str(node.func))

        self.write('(')

        for i, arg in enumerate(node.args):
            if i > 0:
                self.write(', ')
            self.visit(arg)

        self.write(')')

        if node.window:
            self.write(' OVER (')
            self._block_ws(1)

            if node.window.partition:
                self.write('PARTITION BY ')
                self.visit_list(node.window.partition, newlines=False)
                self.new_lines = 1

            if node.window.orderby:
                self.write('ORDER BY ')
                self.visit_list(node.window.orderby, separator=' THEN')

            self._block_ws(-1)
            self.write(')')

    def visit_FuncArg(self, node):
        if node.name:
            self.write(node.name)
            self.write(' := ')
        self.visit(node.arg)

        if node.filter:
            self.write(' FILTER ')
            self.visit(node.filter)

        if node.sort:
            self.write(' ORDER BY ')
            self.visit_list(node.sort, separator=' THEN')

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

    def visit_ObjectRef(self, node):
        if node.module:
            self.write(ident_to_str(node.module))
            self.write('::')

        self.write(ident_to_str(node.name))

    def visit_Source(self, node):
        self.write('__source__')

    def visit_Subject(self, node):
        self.write('__subject__')

    def visit_NoneTest(self, node):
        self.visit(node.expr)
        self.write(' IS None')

    def visit_TypeName(self, node):
        parenthesize = (
            isinstance(node.parent, (edgeql_ast.IsOp, edgeql_ast.TypeOp)) and
            node.subtypes is not None
        )
        if parenthesize:
            self.write('(')
        if node.name is not None:
            self.write(ident_to_str(node.name), ': ')
        if isinstance(node.maintype, edgeql_ast.Path):
            self.visit(node.maintype)
        else:
            self.visit(node.maintype)
        if node.subtypes is not None:
            self.write('<')
            self.visit_list(node.subtypes, newlines=False)
            if node.dimensions is not None:
                for dim in node.dimensions:
                    if dim is None:
                        self.write('[]')
                    else:
                        self.write('[', str(dim), ']')
            self.write('>')
        if parenthesize:
            self.write(')')

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
            self.write(' EXTENDING ')
            if len(node.bases) > 1:
                self.write('(')
            for i, b in enumerate(node.bases):
                if i > 0:
                    self.write(', ')
                self.visit(b)

            if len(node.bases) > 1:
                self.write(')')

    def _visit_CreateObject(self, node, *object_keywords, after_name=None,
                            render_commands=True):
        self._visit_aliases(node)
        self.write('CREATE', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if after_name:
            after_name()
        if node.commands and render_commands:
            self.write(' {')
            self._block_ws(1)
            self.visit_list(node.commands, terminator=';')
            self.indentation -= 1
            self.write('}')

    def _visit_AlterObject(self, node, *object_keywords, allow_short=True):
        self._visit_aliases(node)
        self.write('ALTER', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if node.commands:
            if len(node.commands) == 1 and allow_short:
                self.write(' ')
                self.visit(node.commands[0])
            else:
                self.write(' {')
                self._block_ws(1)
                self.visit_list(node.commands, terminator=';')
                self._block_ws(-1)
                self.write('}')

    def _visit_DropObject(self, node, *object_keywords):
        self._visit_aliases(node)
        self.write('DROP', *object_keywords, delimiter=' ')
        self.write(' ')
        self.visit(node.name)
        if node.commands:
            self.write(' {')
            self._block_ws(1)
            self.visit_list(node.commands, terminator=';')
            self.indentation -= 1
            self.write('}')

    def visit_Rename(self, node):
        self.write('RENAME TO ')
        self.visit(node.new_name)

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
        self.write('EXTENDING ')
        self.visit_list(node.bases)
        if node.position is not None:
            self.write(' ')
            self.visit(node.position)

    def visit_AlterDropInherit(self, node):
        self.write('DROP EXTENDING ')
        self.visit_list(node.bases)

    def visit_CreateDatabase(self, node):
        self._visit_CreateObject(node, 'DATABASE')

    def visit_AlterDatabase(self, node):
        self._visit_AlterObject(node, 'DATABASE')

    def visit_DropDatabase(self, node):
        self._visit_DropObject(node, 'DATABASE')

    def visit_CreateRole(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ROLE', after_name=after_name)

    def visit_AlterRole(self, node):
        self._visit_AlterObject(node, 'ROLE')

    def visit_DropRole(self, node):
        self._visit_DropObject(node, 'ROLE')

    def visit_CreateDelta(self, node):
        def after_name():
            if node.parents:
                self.write(' FROM ')
                self.visit(node.parents)

            if node.target:
                self.write(' TO ', node.language, ' ')
                from edb.lang.schema import generate_source as schema_sg
                self.write(edgeql_quote.dollar_quote_literal(
                    schema_sg(node.target)))
        self._visit_CreateObject(node, 'MIGRATION', after_name=after_name)

    def visit_CommitDelta(self, node):
        self._visit_aliases(node)
        self.write('COMMIT MIGRATION')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_GetDelta(self, node):
        self._visit_aliases(node)
        self.write('GET MIGRATION')
        self.write(' ')
        self.visit(node.name)
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

    def visit_CreateView(self, node):
        self._visit_CreateObject(node, 'VIEW')

    def visit_AlterView(self, node):
        self._visit_AlterObject(node, 'VIEW')

    def visit_DropView(self, node):
        self._visit_DropObject(node, 'VIEW')

    def visit_CreateEvent(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'EVENT', after_name=after_name)

    def visit_CreateAttribute(self, node):
        def after_name():
            self.write(' ')
            self.visit(node.type)

        self._visit_CreateObject(node, 'ABSTRACT ATTRIBUTE',
                                 after_name=after_name)

    def visit_DropAttribute(self, node):
        self._visit_DropObject(node, 'ABSTRACT ATTRIBUTE')

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
        def after_name():
            if node.args:
                self.write('(')
                self.visit_list(node.args, newlines=False)
                self.write(')')
            if node.subject:
                self.write(' ON (')
                self.visit(node.subject)
                self.write(')')

            self._ddl_visit_bases(node)

        self._visit_CreateObject(node, 'ABSTRACT CONSTRAINT',
                                 after_name=after_name)

    def visit_AlterConstraint(self, node):
        self._visit_AlterObject(node, 'ABSTRACT CONSTRAINT')

    def visit_DropConstraint(self, node):
        self._visit_DropObject(node, 'ABSTRACT CONSTRAINT')

    def visit_CreateConcreteConstraint(self, node):
        def after_name():
            if node.args:
                self.write('(')
                self.visit_list(node.args, newlines=False)
                self.write(')')
            if node.subject:
                self.write(' on (')
                self.visit(node.subject)
                self.write(')')

        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        keywords.append('CONSTRAINT')
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteConstraint(self, node):
        self._visit_AlterObject(node, 'CONSTRAINT', allow_short=False)

    def visit_DropConcreteConstraint(self, node):
        self._visit_DropObject(node, 'CONSTRAINT')

    def visit_CreateScalarType(self, node):
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('SCALAR')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterScalarType(self, node):
        self._visit_AlterObject(node, 'SCALAR TYPE')

    def visit_DropScalarType(self, node):
        self._visit_DropObject(node, 'SCALAR TYPE')

    def visit_CreateProperty(self, node):
        self._visit_CreateObject(node, 'ABSTRACT PROPERTY')

    def visit_AlterProperty(self, node):
        self._visit_AlterObject(node, 'ABSTRACT PROPERTY')

    def visit_DropProperty(self, node):
        self._visit_DropObject(node, 'ABSTRACT PROPERTY')

    def visit_CreateConcreteProperty(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('PROPERTY')

        def after_name():
            self.write(' -> ')
            self.visit(node.target)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteProperty(self, node):
        self._visit_AlterObject(node, 'PROPERTY', allow_short=False)

    def visit_DropConcreteProperty(self, node):
        self._visit_DropObject(node, 'PROPERTY')

    def visit_CreateLink(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ABSTRACT LINK', after_name=after_name)

    def visit_AlterLink(self, node):
        self._visit_AlterObject(node, 'ABSTRACT LINK')

    def visit_DropLink(self, node):
        self._visit_DropObject(node, 'ABSTRACT LINK')

    def visit_CreateConcreteLink(self, node):
        keywords = []

        if node.is_required:
            keywords.append('REQUIRED')
        keywords.append('LINK')

        def after_name():
            self.write(' -> ')
            self.visit(node.target)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteLink(self, node):
        self._visit_AlterObject(node, 'LINK', allow_short=False)

    def visit_DropConcreteLink(self, node):
        self._visit_DropObject(node, 'LINK')

    def visit_AlterTarget(self, node):
        self.write('ALTER TYPE ')
        self.visit_list(node.target, newlines=False)

    def visit_CreateObjectType(self, node):
        keywords = []

        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterObjectType(self, node):
        self._visit_AlterObject(node, 'TYPE')

    def visit_DropObjectType(self, node):
        self._visit_DropObject(node, 'TYPE')

    def visit_CreateIndex(self, node):
        def after_name():
            self.write(' ON ')
            self.visit(node.expr)
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
            self.write(' -> ')
            if node.set_returning:
                self.write(node.set_returning.upper(), ' ')
            self.visit(node.returning)

            if node.commands or node.initial_value:
                self.write('{')
                self._block_ws(1)
                self.visit_list(node.commands, terminator=';')
                self.new_lines = 1

                if node.initial_value:
                    self.write('INITIAL VALUE ')
                    self.visit(node.initial_value)
                    self.write(';')

            if node.code.from_name:
                self.write(f' FROM {node.code.language} {typ} ')
                self.write(f'{node.code.from_name!r}')
            else:
                self.write(f' FROM {node.code.language} ')
                self.write(edgeql_quote.dollar_quote_literal(
                    node.code.code))

            if node.commands or node.initial_value:
                self.write(';')
                self._block_ws(-1)
                self.write('}')

        typ = 'AGGREGATE' if node.aggregate else 'FUNCTION'
        self._visit_CreateObject(node, typ, after_name=after_name,
                                 render_commands=False)

    def visit_AlterFunction(self, node):
        self._visit_AlterObject(node, 'FUNCTION')

    def visit_DropFunction(self, node):
        self._visit_DropObject(node, 'FUNCTION')

    def visit_FuncParam(self, node):
        if node.name is not None:
            self.write('$', ident_to_str(node.name), ': ')
        if node.qualifier:
            self.write(node.qualifier.upper(), ' ')
        self.visit(node.type)

        if node.default:
            self.write(' = ')
            self.visit(node.default)

    def visit_SessionStateDecl(self, node):
        self.write('SET')
        self._block_ws(1)
        self.visit_list(node.items)
        self._block_ws(-1)

    @classmethod
    def to_source(
            cls, node, indent_with=' ' * 4, add_line_information=False,
            pretty=True):
        # make sure that all the parents are properly set
        if isinstance(node, (list, tuple)):
            for n in node:
                base.fix_parent_links(n)
        else:
            base.fix_parent_links(node)

        return super().to_source(
            node, indent_with, add_line_information, pretty)


generate_source = EdgeQLSourceGenerator.to_source
