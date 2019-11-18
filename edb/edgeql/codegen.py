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

import itertools
import re

from edb import errors
from edb.common.ast import codegen, base

from . import ast as qlast
from . import quote as edgeql_quote
from . import qltypes


_module_name_re = re.compile(r'^(?!=\d)\w+(\.(?!=\d)\w+)*$')


def any_ident_to_str(ident):
    if _module_name_re.match(ident):
        return ident
    else:
        return ident_to_str(ident)


def ident_to_str(ident):
    return edgeql_quote.quote_ident(ident)


def param_to_str(ident):
    return '$' + edgeql_quote.quote_ident(
        ident, allow_reserved=True)


def module_to_str(module):
    return '.'.join([ident_to_str(part) for part in module.split('.')])


class EdgeQLSourceGeneratorError(errors.EdgeDBError):
    pass


class EdgeSchemaSourceGeneratorError(errors.EdgeDBError):
    pass


class EdgeQLSourceGenerator(codegen.SourceGenerator):
    def __init__(self, *args, sdlmode=False, descmode=False,
                 unsorted=False, limit_ref_classes=frozenset(), **kwargs):
        super().__init__(*args, **kwargs)
        self.sdlmode = sdlmode
        self.descmode = descmode
        self.unsorted = unsorted
        self.limit_ref_classes = limit_ref_classes

    def visit(self, node, **kwargs):
        method = 'visit_' + node.__class__.__name__
        if method == 'visit_list':
            return self.visit_list(node, terminator=';')
        else:
            visitor = getattr(self, method, self.generic_visit)
            return visitor(node, **kwargs)

    def _write_keywords(self, *kws):
        kwstring = ' '.join(kws)
        if self.sdlmode:
            kwstring = kwstring.lower()
        self.write(kwstring)

    def _needs_parentheses(self, node):
        return (
            node.parent is not None and (
                not isinstance(node.parent, qlast.Base)
                or not isinstance(node.parent, qlast.DDL)
                or isinstance(node.parent, qlast.SetField)
            )
        )

    def generic_visit(self, node, *args, **kwargs):
        if isinstance(node, qlast.SDL):
            raise EdgeQLSourceGeneratorError(
                f'No method to generate code for {node.__class__.__name__}')
        else:
            raise EdgeQLSourceGeneratorError(
                f'No method to generate code for {node.__class__.__name__}')

    def _block_ws(self, change, newlines=True):
        if newlines:
            self.indentation += change
            self.new_lines = 1
        else:
            self.write(' ')

    def _visit_aliases(self, node):
        if node.aliases:
            self.write('WITH')
            self._block_ws(1)
            if node.aliases:
                self.visit_list(node.aliases)
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

        self.write('DELETE')
        self._block_ws(1)
        if node.subject_alias:
            self.write(node.subject_alias, ' := ')
        self.visit(node.subject)
        self._block_ws(-1)
        self._visit_filter(node)
        self._visit_order(node)
        self._visit_offset_limit(node)
        if parenthesise:
            self.write(')')

    def visit_SelectQuery(self, node):
        # XXX: need to parenthesise when SELECT appears as an expression,
        # the actual passed value is ignored.
        parenthesise = self._needs_parentheses(node)
        if node.implicit:
            parenthesise = parenthesise and bool(node.aliases)

        if parenthesise:
            self.write('(')

        if not node.implicit or node.aliases:
            self._visit_aliases(node)
            self.write('SELECT')
            self._block_ws(1)

        if node.result_alias:
            self.write(node.result_alias, ' := ')
        self.visit(node.result)
        if not node.implicit or node.aliases:
            self._block_ws(-1)
        else:
            self.write(' ')
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
            self.write(' AS ')
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

    def visit_DetachedExpr(self, node):
        self.write('DETACHED ')
        self.visit(node.expr)

    def visit_UnaryOp(self, node):
        op = str(node.op).upper()
        self.write(op)
        if op.isalnum():
            self.write(' (')
        self.visit(node.operand)
        if op.isalnum():
            self.write(')')

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
                if (getattr(e, 'type', None) != 'property'
                        and not isinstance(e, qlast.TypeIndirection)):
                    self.write('.')

            if i == 0:
                if isinstance(e, qlast.ObjectRef):
                    self.visit(e)
                elif isinstance(e, (qlast.Source,
                                    qlast.Subject)):
                    self.visit(e)
                elif not isinstance(e, (qlast.Ptr,
                                        qlast.Set,
                                        qlast.Tuple,
                                        qlast.NamedTuple,
                                        qlast.TypeIndirection)):
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

    def visit_TypeIndirection(self, node):
        self.write('[IS ')
        self.visit(node.type)
        self.write(']')

    def visit_ShapeElement(self, node):
        # PathSpec can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.

        quals = []
        if node.required:
            quals.append('required')

        if node.cardinality is qltypes.Cardinality.MANY:
            quals.append('multi')
        elif node.cardinality is qltypes.Cardinality.ONE:
            quals.append('single')

        if quals:
            self.write(*quals, delimiter=' ')
            self.write(' ')

        if len(node.expr.steps) == 1:
            self.visit(node.expr)
        else:
            self.visit(node.expr.steps[0])
            if not isinstance(node.expr.steps[1], qlast.TypeIndirection):
                self.write('.')
                self.visit(node.expr.steps[1])

        if not node.compexpr and (node.elements
                                  or isinstance(node.expr.steps[-1],
                                                qlast.TypeIndirection)):
            self.write(': ')
            if isinstance(node.expr.steps[-1], qlast.TypeIndirection):
                self.visit(node.expr.steps[-1].type)
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
        self.write(param_to_str(node.name))

    def visit_StringConstant(self, node):
        self.write(node.quote, node.value, node.quote)

    def visit_RawStringConstant(self, node):
        if node.quote.startswith('$'):
            self.write(node.quote, node.value, node.quote)
        else:
            self.write('r', node.quote, node.value, node.quote)

    def visit_IntegerConstant(self, node):
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_FloatConstant(self, node):
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_DecimalConstant(self, node):
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_BooleanConstant(self, node):
        self.write(node.value)

    def visit_BytesConstant(self, node):
        self.write('b', node.quote, node.value, node.quote)

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

        if node.kwargs:
            if node.args:
                self.write(', ')

            for i, (name, arg) in enumerate(node.kwargs.items()):
                if i > 0:
                    self.write(', ')
                self.write(f'{name} := ')
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

    def visit_AnyType(self, node):
        self.write('anytype')

    def visit_AnyTuple(self, node):
        self.write('anytuple')

    def visit_TypeCast(self, node):
        self.write('<')
        self.visit(node.type)
        self.write('>')
        self.visit(node.expr)

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
        if node.itemclass:
            self.write(node.itemclass)
            self.write(' ')
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

    def visit_TypeExprLiteral(self, node):
        self.visit(node.val)

    def visit_TypeName(self, node):
        parenthesize = (
            isinstance(node.parent, (qlast.IsOp, qlast.TypeOp,
                                     qlast.Introspect)) and
            node.subtypes is not None
        )
        if parenthesize:
            self.write('(')
        if node.name is not None:
            self.write(ident_to_str(node.name), ': ')
        if isinstance(node.maintype, qlast.Path):
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

    def visit_Introspect(self, node):
        self.write('INTROSPECT ')
        self.visit(node.type)

    def visit_TypeOf(self, node):
        self.write('TYPEOF ')
        self.visit(node.expr)

    # DDL nodes

    def visit_Position(self, node):
        self.write(node.position)
        if node.ref:
            self.write(' ')
            self.visit(node.ref)

    def _ddl_visit_bases(self, node):
        if getattr(node, 'bases', None):
            self._write_keywords(' EXTENDING ')
            self.visit_list(node.bases, newlines=False)

    def _ddl_visit_body(self, commands, group_by_system_comment, *,
                        allow_short: bool = False):
        if self.limit_ref_classes:
            commands = filter(
                lambda c: c.name.itemclass in self.limit_ref_classes,
                commands,
            )

        commands = list(commands)

        if len(commands) == 1 and allow_short:
            self.write(' ')
            self.visit(commands[0])
        elif len(commands) > 0:
            self.write(' {')
            self._block_ws(1)

            if group_by_system_comment:
                sort_key = lambda c: (c.system_comment or '', c.name.name)
                group_key = lambda c: c.system_comment or ''
                if not self.unsorted:
                    commands = sorted(commands, key=sort_key)
                groups = itertools.groupby(commands, group_key)
                for i, (comment, items) in enumerate(groups):
                    if i > 0:
                        self.new_lines = 2
                    if comment:
                        self.write('#')
                        self.new_lines = 1
                        self.write(f'# {comment}')
                        self.new_lines = 1
                        self.write('#')
                        self.new_lines = 1
                    self.visit_list(list(items), terminator=';')
            elif self.descmode or self.sdlmode:
                sort_key = lambda c: (c.name.itemclass or '', c.name.name)
                if not self.unsorted:
                    commands = sorted(commands, key=sort_key)
                self.visit_list(list(commands), terminator=';')
            else:
                self.visit_list(list(commands), terminator=';')

            self._block_ws(-1)
            self.write('}')

    def _visit_CreateObject(self, node, *object_keywords, after_name=None,
                            render_commands=True, unqualified=False,
                            named=True, group_by_system_comment=False):
        self._visit_aliases(node)
        if self.sdlmode:
            self.write(*[kw.lower() for kw in object_keywords], delimiter=' ')
        else:
            self.write('CREATE', *object_keywords, delimiter=' ')
        if named:
            self.write(' ')
            if unqualified or not node.name.module:
                self.write(ident_to_str(node.name.name))
            else:
                self.write(ident_to_str(node.name.module), '::',
                           ident_to_str(node.name.name))
        if after_name:
            after_name()

        commands = node.commands
        if commands and render_commands:
            self._ddl_visit_body(
                commands,
                group_by_system_comment=group_by_system_comment,
            )

    def _visit_AlterObject(self, node, *object_keywords, allow_short=True,
                           after_name=None, unqualified=False, named=True,
                           ignored_cmds=frozenset(),
                           group_by_system_comment=False):
        self._visit_aliases(node)
        if self.sdlmode:
            self.write(*[kw.lower() for kw in object_keywords], delimiter=' ')
        else:
            self.write('ALTER', *object_keywords, delimiter=' ')
        if named:
            self.write(' ')
            if unqualified or not node.name.module:
                self.write(ident_to_str(node.name.name))
            else:
                self.write(ident_to_str(node.name.module), '::',
                           ident_to_str(node.name.name))
        if after_name:
            after_name()

        commands = [cmd for cmd in node.commands if cmd not in ignored_cmds]

        if commands:
            self._ddl_visit_body(
                commands,
                group_by_system_comment=group_by_system_comment,
                allow_short=allow_short,
            )

    def _visit_DropObject(self, node, *object_keywords, unqualified=False,
                          after_name=None, named=True):
        self._visit_aliases(node)
        self.write('DROP', *object_keywords, delimiter=' ')
        if named:
            self.write(' ')
            if unqualified or not node.name.module:
                self.write(ident_to_str(node.name.name))
            else:
                self.write(ident_to_str(node.name.module), '::',
                           ident_to_str(node.name.name))
        if after_name:
            after_name()
        if node.commands:
            self.write(' {')
            self._block_ws(1)
            self.visit_list(node.commands, terminator=';')
            self.indentation -= 1
            self.write('}')

    def visit_Rename(self, node):
        self.write('RENAME TO ')
        self.visit(node.new_name)

    def _process_SetSpecialField(self, node):
        keywords = []

        if node.value:
            keywords.append('SET')
        else:
            keywords.append('DROP')

        fname = node.name.name

        if fname == 'is_abstract':
            keywords.append('ABSTRACT')
        elif fname == 'delegated':
            keywords.append('DELEGATED')
        elif fname == 'is_final':
            keywords.append('FINAL')
        elif fname == 'required':
            keywords.append('REQUIRED')
        elif fname == 'cardinality':
            if node.value is qltypes.Cardinality.ONE:
                keywords.append('SINGLE')
            else:
                keywords.append('MULTI')
        else:
            raise EdgeQLSourceGeneratorError(
                'unknown special field: {!r}'.format(fname))

        return keywords

    def visit_SetSpecialField(self, node):
        keywords = self._process_SetSpecialField(node)
        self.write(*keywords, delimiter=' ')

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

    def visit_CreateMigration(self, node):
        def after_name():
            if node.parents:
                self.write(' FROM ')
                self.visit(node.parents)

            if node.target:
                if node.language:
                    self.write(' TO ', node.language, ' ')
                    # generate source from the target node
                    self.write(
                        edgeql_quote.dollar_quote_literal(
                            generate_source(node.target)))
                else:
                    self.write(' TO {')
                    self._block_ws(1)
                    self.visit(node.target)
                    self.indentation -= 1
                    self.write('}')

        self._visit_CreateObject(node, 'MIGRATION', after_name=after_name)

    def visit_CommitMigration(self, node):
        self._visit_aliases(node)
        self.write('COMMIT MIGRATION')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_GetMigration(self, node):
        self._visit_aliases(node)
        self.write('GET MIGRATION')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_AlterMigration(self, node):
        self._visit_AlterObject(node, 'MIGRATION')

    def visit_DropMigration(self, node):
        self._visit_DropObject(node, 'MIGRATION')

    def visit_CreateModule(self, node):
        self._visit_CreateObject(node, 'MODULE')

    def visit_AlterModule(self, node):
        self._visit_AlterObject(node, 'MODULE')

    def visit_DropModule(self, node):
        self._visit_DropObject(node, 'MODULE')

    def visit_CreateView(self, node):
        if (len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name.name == 'expr'):

            self._visit_CreateObject(node, 'VIEW', render_commands=False)
            self.write(' := ')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.commands[0].value)
            self.indentation -= 1
            self.new_lines = 1
        else:
            self._visit_CreateObject(node, 'VIEW')

    def visit_AlterView(self, node):
        self._visit_AlterObject(node, 'VIEW')

    def visit_DropView(self, node):
        self._visit_DropObject(node, 'VIEW')

    def visit_SetField(self, node):
        if not self.sdlmode:
            self.write('SET ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_CreateAnnotation(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        if node.inheritable:
            tag = 'ABSTRACT INHERITABLE ANNOTATION'
        else:
            tag = 'ABSTRACT ANNOTATION'
        self._visit_CreateObject(node, tag, after_name=after_name)

    def visit_DropAnnotation(self, node):
        self._visit_DropObject(node, 'ABSTRACT ANNOTATION')

    def visit_CreateAnnotationValue(self, node):
        if self.sdlmode:
            self.write('annotation ')
        else:
            self.write('SET ANNOTATION ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_DropAnnotationValue(self, node):
        self.write('DROP ANNOTATION ')
        self.visit(node.name)

    def visit_CreateConstraint(self, node):
        def after_name():
            if node.params:
                self.write('(')
                self.visit_list(node.params, newlines=False)
                self.write(')')
            if node.subjectexpr:
                self._write_keywords(' ON ')
                self.write('(')
                self.visit(node.subjectexpr)
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
            if node.subjectexpr:
                self._write_keywords(' ON ')
                self.write('(')
                self.visit(node.subjectexpr)
                self.write(')')

        keywords = []
        if node.delegated:
            keywords.append('DELEGATED')
        keywords.append('CONSTRAINT')
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterConcreteConstraint(self, node):
        def after_name():
            if node.args:
                self.write('(')
                self.visit_list(node.args, newlines=False)
                self.write(')')
            if node.subjectexpr:
                self._write_keywords(' ON ')
                self.write('(')
                self.visit(node.subjectexpr)
                self.write(')')

        self._visit_AlterObject(node, 'CONSTRAINT', allow_short=False,
                                after_name=after_name)

    def visit_DropConcreteConstraint(self, node):
        def after_name():
            if node.args:
                self.write('(')
                self.visit_list(node.args, newlines=False)
                self.write(')')
            if node.subjectexpr:
                self._write_keywords(' ON ')
                self.write('(')
                self.visit(node.subjectexpr)
                self.write(')')

        self._visit_DropObject(node, 'CONSTRAINT', after_name=after_name)

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
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ABSTRACT PROPERTY',
                                 after_name=after_name)

    def visit_AlterProperty(self, node):
        self._visit_AlterObject(node, 'ABSTRACT PROPERTY')

    def visit_DropProperty(self, node):
        self._visit_DropObject(node, 'ABSTRACT PROPERTY')

    def visit_CreateConcreteProperty(self, node):
        keywords = []
        if self.sdlmode and node.declared_overloaded:
            keywords.append('OVERLOADED')
        if node.is_required:
            keywords.append('REQUIRED')
        if node.cardinality is qltypes.Cardinality.ONE:
            keywords.append('SINGLE')
        elif node.cardinality is qltypes.Cardinality.MANY:
            keywords.append('MULTI')
        keywords.append('PROPERTY')

        pure_computable = (
            len(node.commands) == 0
            or (
                len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name.name == 'expr'
            )
        )

        def after_name():
            self._ddl_visit_bases(node)
            if node.target is not None:
                if isinstance(node.target, qlast.TypeExpr):
                    self.write(' -> ')
                    self.visit(node.target)
                elif pure_computable:
                    # computable
                    self.write(' := (')
                    self.visit(node.target)
                    self.write(')')

        self._visit_CreateObject(
            node, *keywords, after_name=after_name, unqualified=True,
            render_commands=not pure_computable)

    def _process_AlterConcretePointer_for_SDL(self, node):
        keywords = []
        specials = set()

        for command in node.commands:
            if isinstance(command, qlast.SetSpecialField):
                kw = self._process_SetSpecialField(command)
                specials.add(command)
                if kw[0] == 'SET':
                    keywords.append(kw[1])

        order = ['REQUIRED', 'SINGLE', 'MULTI']
        keywords.sort(key=lambda i: order.index(i))

        return keywords, specials

    def visit_AlterConcreteProperty(self, node):
        keywords = []
        ignored_cmds = set()
        if self.sdlmode:
            if not self.descmode:
                keywords.append('OVERLOADED')
            quals, ignored_cmds = self._process_AlterConcretePointer_for_SDL(
                node)
            keywords.extend(quals)

            type_cmd = None
            for cmd in node.commands:
                if isinstance(cmd, qlast.SetPropertyType):
                    ignored_cmds.add(cmd)
                    type_cmd = cmd
                    break

            def after_name():
                self._ddl_visit_bases(node)
                if type_cmd is not None:
                    self.write(' -> ')
                    self.visit(type_cmd.type)
        else:
            after_name = None

        keywords.append('PROPERTY')
        self._visit_AlterObject(
            node, *keywords, ignored_cmds=ignored_cmds,
            allow_short=False, unqualified=True,
            after_name=after_name)

    def visit_DropConcreteProperty(self, node):
        self._visit_DropObject(node, 'PROPERTY', unqualified=True)

    def visit_CreateLink(self, node):
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ABSTRACT LINK', after_name=after_name)

    def visit_AlterLink(self, node):
        self._visit_AlterObject(node, 'ABSTRACT LINK')

    def visit_DropLink(self, node):
        self._visit_DropObject(node, 'ABSTRACT LINK')

    def visit_CreateConcreteLink(self, node):
        keywords = []

        if self.sdlmode and node.declared_overloaded:
            keywords.append('OVERLOADED')
        if node.is_required:
            keywords.append('REQUIRED')
        if node.cardinality is qltypes.Cardinality.ONE:
            keywords.append('SINGLE')
        elif node.cardinality is qltypes.Cardinality.MANY:
            keywords.append('MULTI')
        keywords.append('LINK')

        def after_name():
            self._ddl_visit_bases(node)
            if node.target is not None:
                if isinstance(node.target, qlast.TypeExpr):
                    self.write(' -> ')
                    self.visit(node.target)
                elif pure_computable:
                    # computable
                    self.write(' := (')
                    self.visit(node.target)
                    self.write(')')

        pure_computable = (
            len(node.commands) == 0
            or (
                len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name.name == 'expr'
            )
        )

        self._visit_CreateObject(
            node, *keywords, after_name=after_name, unqualified=True,
            render_commands=not pure_computable)

    def visit_AlterConcreteLink(self, node):
        keywords = []
        ignored_cmds = set()
        if self.sdlmode:
            if (not self.descmode
                    or not node.system_comment
                    or 'inherited from' not in node.system_comment):
                keywords.append('OVERLOADED')
            quals, ignored_cmds = self._process_AlterConcretePointer_for_SDL(
                node)
            keywords.extend(quals)

            type_cmd = None
            inherit_cmd = None
            for cmd in node.commands:
                if isinstance(cmd, qlast.SetLinkType):
                    ignored_cmds.add(cmd)
                    type_cmd = cmd
                elif isinstance(cmd, qlast.AlterAddInherit):
                    ignored_cmds.add(cmd)
                    inherit_cmd = cmd

            def after_name():
                if inherit_cmd:
                    self._ddl_visit_bases(inherit_cmd)
                if type_cmd is not None:
                    self.write(' -> ')
                    self.visit(type_cmd.type)
        else:
            after_name = None

        keywords.append('LINK')
        self._visit_AlterObject(
            node, *keywords, ignored_cmds=ignored_cmds,
            allow_short=False, unqualified=True, after_name=after_name)

    def visit_DropConcreteLink(self, node):
        self._visit_DropObject(node, 'LINK', unqualified=True)

    def visit_SetPropertyType(self, node):
        self.write('SET TYPE ')
        self.visit(node.type)

    def visit_SetLinkType(self, node):
        self.write('SET TYPE ')
        self.visit(node.type)

    def visit_OnTargetDelete(self, node):
        self.write('ON TARGET DELETE ', node.cascade)

    def visit_CreateObjectType(self, node):
        keywords = []

        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(
            node, *keywords, after_name=after_name)

    def visit_AlterObjectType(self, node):
        self._visit_AlterObject(node, 'TYPE')

    def visit_DropObjectType(self, node):
        self._visit_DropObject(node, 'TYPE')

    def visit_CreateIndex(self, node):
        def after_name():
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')
        self._visit_CreateObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_AlterIndex(self, node):
        def after_name():
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')
        self._visit_AlterObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_DropIndex(self, node):
        def after_name():
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')
        self._visit_DropObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_CreateFunction(self, node):
        def after_name():
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')
            self.write(' -> ')
            self.write(node.returning_typemod.to_edgeql(), ' ')
            self.visit(node.returning)

            if node.commands:
                self.write(' {')
                self._block_ws(1)
                self.visit_list(node.commands, terminator=';')
                self.new_lines = 1

            if node.code.from_function:
                from_clause = f' FROM {node.code.language} FUNCTION '
                if self.sdlmode:
                    from_clause = from_clause.lower()
                self.write(from_clause)
                self.write(f'{node.code.from_function!r}')
            else:
                from_clause = f' FROM {node.code.language} '
                if self.sdlmode:
                    from_clause = from_clause.lower()
                self.write(from_clause)
                self.write(edgeql_quote.dollar_quote_literal(
                    node.code.code))

            self._block_ws(-1)
            if node.commands:
                self.write(';')
                self.write('}')

        self._visit_CreateObject(node, 'FUNCTION', after_name=after_name,
                                 render_commands=False)

    def visit_AlterFunction(self, node):
        self._visit_AlterObject(node, 'FUNCTION')

    def visit_DropFunction(self, node):
        self._visit_DropObject(node, 'FUNCTION')

    def visit_FuncParam(self, node):
        kind = node.kind.to_edgeql()
        if kind:
            self.write(kind, ' ')

        if node.name is not None:
            self.write(ident_to_str(node.name), ': ')

        typemod = node.typemod.to_edgeql()
        if typemod:
            self.write(typemod, ' ')

        self.visit(node.type)

        if node.default:
            self.write(' = ')
            self.visit(node.default)

    def visit_ConfigSet(self, node):
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' SET ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.expr)

    def visit_ConfigInsert(self, node):
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' INSERT ')
        self.visit(node.name)
        self.indentation += 1
        self._visit_shape(node.shape)
        self.indentation -= 1

    def visit_ConfigReset(self, node):
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' RESET ')
        self.visit(node.name)
        self._visit_filter(node)

    def visit_SessionSetAliasDecl(self, node):
        self.write('SET')
        if node.alias:
            self.write(' ALIAS ')
            self.write(ident_to_str(node.alias))
            self.write(' AS MODULE ')
            self.write(node.module)
        else:
            self.write(' MODULE ')
            self.write(node.module)

    def visit_SessionResetAllAliases(self, node):
        self.write('RESET ALIAS *')

    def visit_SessionResetModule(self, node):
        self.write('RESET MODULE')

    def visit_SessionResetAliasDecl(self, node):
        self.write('RESET ALIAS ')
        self.write(node.alias)

    def visit_StartTransaction(self, node):
        self.write('START TRANSACTION')

        mods = []

        if node.isolation is not None:
            mods.append(f'ISOLATION {node.isolation.value}')

        if node.access is not None:
            mods.append(node.access.value)

        if node.deferrable is not None:
            mods.append(node.deferrable.value)

        if mods:
            self.write(' ' + ', '.join(mods))

    def visit_RollbackTransaction(self, node):
        self.write('ROLLBACK')

    def visit_CommitTransaction(self, node):
        self.write('COMMIT')

    def visit_DeclareSavepoint(self, node):
        self.write(f'DECLARE SAVEPOINT {node.name}')

    def visit_RollbackToSavepoint(self, node):
        self.write(f'ROLLBACK TO SAVEPOINT {node.name}')

    def visit_ReleaseSavepoint(self, node):
        self.write(f'RELEASE SAVEPOINT {node.name}')

    def visit_DescribeStmt(self, node):
        self.write(f'DESCRIBE ')
        if node.object:
            self.visit(node.object)
        else:
            self.write('SCHEMA')
        if node.language:
            self.write(' AS ', node.language)
        if node.options:
            self.write(' ')
            self.visit(node.options)

    def visit_Options(self, node):
        for i, opt in enumerate(node.options.values()):
            if i > 0:
                self.write(' ')
            self.write(opt.name)
            if not isinstance(opt, qlast.Flag):
                self.write(f' {opt.val}')

    # SDL nodes

    def visit_Schema(self, node):
        sdl_codegen = self.__class__(
            indent_with=self.indent_with,
            add_line_information=self.add_line_information,
            pretty=self.pretty,
            unsorted=self.unsorted,
            sdlmode=True,
            descmode=self.descmode,
            limit_ref_classes=self.limit_ref_classes)
        sdl_codegen.indentation = self.indentation
        sdl_codegen.current_line = self.current_line
        sdl_codegen.visit_list(node.declarations, terminator=';')
        self.result.extend(sdl_codegen.result)

    @classmethod
    def to_source(
            cls, node, indent_with=' ' * 4, add_line_information=False,
            pretty=True, sdlmode=False, descmode=False,
            limit_ref_classes=frozenset(),
            unsorted=False):
        # make sure that all the parents are properly set
        if isinstance(node, (list, tuple)):
            for n in node:
                base.fix_parent_links(n)
        else:
            base.fix_parent_links(node)

        return super().to_source(
            node, indent_with, add_line_information, pretty,
            sdlmode=sdlmode, descmode=descmode, unsorted=unsorted,
            limit_ref_classes=limit_ref_classes)


generate_source = EdgeQLSourceGenerator.to_source
