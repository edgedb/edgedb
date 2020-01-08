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
from typing import *  # NoQA

import itertools
import re

from edb import errors
from edb.common.ast import codegen, base
from edb.common import typeutils

from . import ast as qlast
from . import quote as edgeql_quote
from . import qltypes


_module_name_re = re.compile(r'^(?!=\d)\w+(\.(?!=\d)\w+)*$')


def any_ident_to_str(ident: str) -> str:
    if _module_name_re.match(ident):
        return ident
    else:
        return ident_to_str(ident)


def ident_to_str(ident: str) -> str:
    return edgeql_quote.quote_ident(ident)


def param_to_str(ident: str) -> str:
    return '$' + edgeql_quote.quote_ident(
        ident, allow_reserved=True)


def module_to_str(module: str) -> str:
    return '.'.join([ident_to_str(part) for part in module.split('.')])


class EdgeQLSourceGeneratorError(errors.InternalServerError):
    pass


class EdgeSchemaSourceGeneratorError(errors.InternalServerError):
    pass


class EdgeQLSourceGenerator(codegen.SourceGenerator):

    def __init__(
        self, *args: Any,
        sdlmode: bool = False,
        descmode: bool = False,
        unsorted: bool = False,
        limit_ref_classes:
            Optional[AbstractSet[qltypes.SchemaObjectClass]] = None,
        **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sdlmode = sdlmode
        self.descmode = descmode
        self.unsorted = unsorted
        self.limit_ref_classes = limit_ref_classes

    def visit(
        self,
        node: Union[qlast.Base, Sequence[qlast.Base]],
        **kwargs: Any
    ) -> None:
        if isinstance(node, list):
            self.visit_list(node, terminator=';')
        else:
            method = 'visit_' + node.__class__.__name__
            visitor = getattr(self, method, self.generic_visit)
            visitor(node, **kwargs)

    def _write_keywords(self, *kws: str) -> None:
        kwstring = ' '.join(kws)
        if self.sdlmode:
            kwstring = kwstring.lower()
        self.write(kwstring)

    def _needs_parentheses(self, node) -> bool:  # type: ignore
        # The "parent" attribute is set by calling `_fix_parent_links`
        # before traversing the AST.  Since it's not an attribute that
        # can be inferred by static typing we ignore typing for this
        # function.
        return (
            node._parent is not None and (
                not isinstance(node._parent, qlast.Base)
                or not isinstance(node._parent, qlast.DDL)
                or isinstance(node._parent, qlast.SetField)
            )
        )

    def generic_visit(self, node: qlast.Base,
                      *args: Any, **kwargs: Any) -> None:
        if isinstance(node, qlast.SDL):
            raise EdgeQLSourceGeneratorError(
                f'No method to generate code for {node.__class__.__name__}')
        else:
            raise EdgeQLSourceGeneratorError(
                f'No method to generate code for {node.__class__.__name__}')

    def _block_ws(self, change: int, newlines: bool = True) -> None:
        if newlines:
            self.indentation += change
            self.new_lines = 1
        else:
            self.write(' ')

    def _visit_aliases(self, node: qlast.Command) -> None:
        if node.aliases:
            self.write('WITH')
            self._block_ws(1)
            if node.aliases:
                self.visit_list(node.aliases)
            self._block_ws(-1)

    def _visit_filter(self, node: qlast.FilterMixin,
                      newlines: bool = True) -> None:
        if node.where:
            self.write('FILTER')
            self._block_ws(1, newlines)
            self.visit(node.where)
            self._block_ws(-1, newlines)

    def _visit_order(self, node: qlast.OrderByMixin,
                     newlines: bool = True) -> None:
        if node.orderby:
            self.write('ORDER BY')
            self._block_ws(1, newlines)
            self.visit_list(node.orderby, separator=' THEN', newlines=newlines)
            self._block_ws(-1, newlines)

    def _visit_offset_limit(self, node: qlast.OffsetLimitMixin,
                            newlines: bool = True) -> None:
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

    def visit_AliasedExpr(self, node: qlast.AliasedExpr) -> None:
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
            self._block_ws(1)

        self.visit(node.expr)

        if node.alias:
            self._block_ws(-1)

    def visit_InsertQuery(self, node: qlast.InsertQuery) -> None:
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

    def visit_UpdateQuery(self, node: qlast.UpdateQuery) -> None:
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

    def visit_DeleteQuery(self, node: qlast.DeleteQuery) -> None:
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

    def visit_SelectQuery(self, node: qlast.SelectQuery) -> None:
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

    def visit_ForQuery(self, node: qlast.ForQuery) -> None:
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

    def visit_GroupQuery(self, node: qlast.GroupQuery) -> None:
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

    def visit_ByExpr(self, node: qlast.ByExpr) -> None:
        if node.each is not None:
            if node.each:
                self.write('EACH ')
            else:
                self.write('SET OF ')

        self.visit(node.expr)

    def visit_GroupBuiltin(self, node: qlast.GroupBuiltin) -> None:
        self.write(node.name, '(')
        self.visit_list(node.elements, newlines=False)
        self.write(')')

    def visit_ModuleAliasDecl(self, node: qlast.ModuleAliasDecl) -> None:
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' AS ')
        self.write('MODULE ')
        self.write(any_ident_to_str(node.module))

    def visit_SortExpr(self, node: qlast.SortExpr) -> None:
        self.visit(node.path)
        if node.direction:
            self.write(' ')
            self.write(node.direction)
        if node.nones_order:
            self.write(' EMPTY ')
            self.write(node.nones_order.upper())

    def visit_DetachedExpr(self, node: qlast.DetachedExpr) -> None:
        self.write('DETACHED ')
        self.visit(node.expr)

    def visit_UnaryOp(self, node: qlast.UnaryOp) -> None:
        op = str(node.op).upper()
        self.write(op)
        if op.isalnum():
            self.write(' (')
        self.visit(node.operand)
        if op.isalnum():
            self.write(')')

    def visit_BinOp(self, node: qlast.BinOp) -> None:
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_IsOp(self, node: qlast.IsOp) -> None:
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_TypeOp(self, node: qlast.TypeOp) -> None:
        self.write('(')
        self.visit(node.left)
        self.write(' ' + str(node.op).upper() + ' ')
        self.visit(node.right)
        self.write(')')

    def visit_IfElse(self, node: qlast.IfElse) -> None:
        self.write('(')
        self.visit(node.if_expr)
        self.write(' IF ')
        self.visit(node.condition)
        self.write(' ELSE ')
        self.visit(node.else_expr)
        self.write(')')

    def visit_Tuple(self, node: qlast.Tuple) -> None:
        self.write('(')
        count = len(node.elements)
        self.visit_list(node.elements, newlines=False)
        if count == 1:
            self.write(',')

        self.write(')')

    def visit_Set(self, node: qlast.Set) -> None:
        self.write('{')
        self.visit_list(node.elements, newlines=False)
        self.write('}')

    def visit_Array(self, node: qlast.Array) -> None:
        self.write('[')
        self.visit_list(node.elements, newlines=False)
        self.write(']')

    def visit_NamedTuple(self, node: qlast.NamedTuple) -> None:
        self.write('(')
        self._block_ws(1)
        self.visit_list(node.elements, newlines=True, separator=',')
        self._block_ws(-1)
        self.write(')')

    def visit_TupleElement(self, node: qlast.TupleElement) -> None:
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.val)

    def visit_Path(self, node: qlast.Path) -> None:
        for i, e in enumerate(node.steps):
            if i > 0 or node.partial:
                if (getattr(e, 'type', None) != 'property'
                        and not isinstance(e, qlast.TypeIntersection)):
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
                                        qlast.TypeIntersection,
                                        qlast.Parameter)):
                    self.write('(')
                    self.visit(e)
                    self.write(')')
                else:
                    self.visit(e)
            else:
                self.visit(e)

    def visit_Shape(self, node: qlast.Shape) -> None:
        if node.expr is not None:
            # shape.expr may be None in Function RETURNING declaration,
            # when the shape is used to described a returned struct.
            self.visit(node.expr)
        self.write(' ')
        self._visit_shape(node.elements)

    def _visit_shape(self, shape: Sequence[qlast.ShapeElement]) -> None:
        if shape:
            self.write('{')
            self._block_ws(1)
            self.visit_list(shape)
            self._block_ws(-1)
            self.write('}')

    def visit_Ptr(self, node: qlast.Ptr, *, quote: bool = True) -> None:
        if node.type == 'property':
            self.write('@')
        elif node.direction and node.direction != '>':
            self.write(node.direction)

        self.visit(node.ptr)

    def visit_TypeIntersection(self, node: qlast.TypeIntersection) -> None:
        self.write('[IS ')
        self.visit(node.type)
        self.write(']')

    def visit_ShapeElement(self, node: qlast.ShapeElement) -> None:
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
            if not isinstance(node.expr.steps[1], qlast.TypeIntersection):
                self.write('.')
                self.visit(node.expr.steps[1])

        if not node.compexpr and (node.elements
                                  or isinstance(node.expr.steps[-1],
                                                qlast.TypeIntersection)):
            self.write(': ')
            if isinstance(node.expr.steps[-1], qlast.TypeIntersection):
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

    def visit_Parameter(self, node: qlast.Parameter) -> None:
        self.write(param_to_str(node.name))

    def visit_StringConstant(self, node: qlast.StringConstant) -> None:
        self.write(node.quote, node.value, node.quote)

    def visit_RawStringConstant(self, node: qlast.RawStringConstant) -> None:
        if node.quote.startswith('$'):
            self.write(node.quote, node.value, node.quote)
        else:
            self.write('r', node.quote, node.value, node.quote)

    def visit_IntegerConstant(self, node: qlast.IntegerConstant) -> None:
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_FloatConstant(self, node: qlast.FloatConstant) -> None:
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_DecimalConstant(self, node: qlast.DecimalConstant) -> None:
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_BigintConstant(self, node: qlast.BigintConstant) -> None:
        if node.is_negative:
            self.write('-')
        self.write(node.value)

    def visit_BooleanConstant(self, node: qlast.BooleanConstant) -> None:
        self.write(node.value)

    def visit_BytesConstant(self, node: qlast.BytesConstant) -> None:
        self.write('b', node.quote, node.value, node.quote)

    def visit_FunctionCall(self, node: qlast.FunctionCall) -> None:
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

    def visit_AnyType(self, node: qlast.AnyType) -> None:
        self.write('anytype')

    def visit_AnyTuple(self, node: qlast.AnyTuple) -> None:
        self.write('anytuple')

    def visit_TypeCast(self, node: qlast.TypeCast) -> None:
        self.write('<')
        self.visit(node.type)
        self.write('>')
        self.visit(node.expr)

    def visit_Indirection(self, node: qlast.Indirection) -> None:
        self.write('(')
        self.visit(node.arg)
        self.write(')')
        for indirection in node.indirection:
            self.visit(indirection)

    def visit_Slice(self, node: qlast.Slice) -> None:
        self.write('[')
        if node.start:
            self.visit(node.start)
        self.write(':')
        if node.stop:
            self.visit(node.stop)
        self.write(']')

    def visit_Index(self, node: qlast.Index) -> None:
        self.write('[')
        self.visit(node.index)
        self.write(']')

    def visit_ObjectRef(self, node: qlast.ObjectRef) -> None:
        if node.itemclass:
            self.write(node.itemclass)
            self.write(' ')
        if node.module:
            self.write(ident_to_str(node.module))
            self.write('::')
        self.write(ident_to_str(node.name))

    def visit_Source(self, node: qlast.Source) -> None:
        self.write('__source__')

    def visit_Subject(self, node: qlast.Subject) -> None:
        self.write('__subject__')

    def visit_TypeExprLiteral(self, node: qlast.TypeExprLiteral) -> None:
        self.visit(node.val)

    def visit_TypeName(self, node: qlast.TypeName) -> None:
        parenthesize = (
            isinstance(node._parent, (qlast.IsOp, qlast.TypeOp,  # type: ignore
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

    def visit_Introspect(self, node: qlast.Introspect) -> None:
        self.write('INTROSPECT ')
        self.visit(node.type)

    def visit_TypeOf(self, node: qlast.TypeOf) -> None:
        self.write('TYPEOF ')
        self.visit(node.expr)

    # DDL nodes

    def visit_Position(self, node: qlast.Position) -> None:
        self.write(node.position)
        if node.ref:
            self.write(' ')
            self.visit(node.ref)

    def _ddl_visit_bases(self, node: qlast.BasesMixin) -> None:
        if node.bases:
            self._write_keywords(' EXTENDING ')
            self.visit_list(node.bases, newlines=False)

    def _ddl_visit_body(
        self,
        commands: Sequence[qlast.DDLCommand],
        group_by_system_comment: bool, *,
        allow_short: bool = False
    ) -> None:
        if self.limit_ref_classes:
            commands = [
                c for c in commands
                # If c.name is an ObjectRef we want to check if the
                # property in in the white list;
                # if it's not, then it's a regular SetField for things
                # like 'default' or 'readonly'.
                if (
                    (isinstance(c.name, qlast.ObjectRef)
                        and c.name.itemclass in self.limit_ref_classes)
                    or not isinstance(c.name, qlast.ObjectRef)
                )
            ]

        if len(commands) == 1 and allow_short:
            self.write(' ')
            self.visit(commands[0])
        elif len(commands) > 0:
            self.write(' {')
            self._block_ws(1)

            if group_by_system_comment:
                sort_key = lambda c: (
                    c.system_comment or '',
                    c.name.name if isinstance(c.name, qlast.ObjectRef)
                    else c.name
                )
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
                sort_key = lambda c: (
                    (c.name.itemclass or '')
                    if isinstance(c.name, qlast.ObjectRef)
                    else '',
                    c.name.name if isinstance(c.name, qlast.ObjectRef)
                    else c.name
                )

                if not self.unsorted:
                    commands = sorted(commands, key=sort_key)
                self.visit_list(list(commands), terminator=';')
            else:
                self.visit_list(list(commands), terminator=';')

            self._block_ws(-1)
            self.write('}')

    def _visit_CreateObject(
        self,
        node: qlast.CreateObject,
        *object_keywords: str,
        after_name: Optional[Callable[[], None]] = None,
        render_commands: bool = True,
        unqualified: bool = False,
        named: bool = True,
        group_by_system_comment: bool = False
    ) -> None:
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
        if node.create_if_not_exists and not self.sdlmode:
            self.write(' IF NOT EXISTS')
        if after_name:
            after_name()

        commands = node.commands
        if commands and render_commands:
            self._ddl_visit_body(
                commands,
                group_by_system_comment=group_by_system_comment,
            )

    def _visit_AlterObject(
        self,
        node: qlast.AlterObject,
        *object_keywords: str,
        allow_short: bool = True,
        after_name: Optional[Callable[[], None]] = None,
        unqualified: bool = False,
        named: bool = True,
        ignored_cmds: Optional[AbstractSet[qlast.DDLCommand]] = None,
        group_by_system_comment: bool = False
    ) -> None:
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

        commands = node.commands
        if ignored_cmds:
            commands = [cmd for cmd in commands
                        if cmd not in ignored_cmds]

        if commands:
            self._ddl_visit_body(
                commands,
                group_by_system_comment=group_by_system_comment,
                allow_short=allow_short,
            )

    def _visit_DropObject(
        self,
        node: qlast.DropObject,
        *object_keywords: str,
        unqualified: bool = False,
        after_name: Optional[Callable[[], None]] = None,
        named: bool = True
    ) -> None:
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

    def visit_Rename(self, node: qlast.Rename) -> None:
        self.write('RENAME TO ')
        self.visit(node.new_name)

    def _process_SetSpecialField(
        self,
        node: qlast.SetSpecialField
    ) -> List[str]:

        keywords = []

        if node.value:
            keywords.append('SET')
        else:
            keywords.append('DROP')

        fname = node.name

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

    def visit_SetSpecialField(self, node: qlast.SetSpecialField) -> None:
        if node.name == 'expr':
            self._write_keywords('USING')
            self.write(' (')
            self.visit(node.value)
            self.write(')')
        else:
            keywords = self._process_SetSpecialField(node)
            self.write(*keywords, delimiter=' ')

    def visit_AlterAddInherit(self, node: qlast.AlterAddInherit) -> None:
        if node.bases:
            self.write('EXTENDING ')
            self.visit_list(node.bases)
            if node.position is not None:
                self.write(' ')
                self.visit(node.position)

    def visit_AlterDropInherit(self, node: qlast.AlterDropInherit) -> None:
        if node.bases:
            self.write('DROP EXTENDING ')
            self.visit_list(node.bases)

    def visit_CreateDatabase(self, node: qlast.CreateDatabase) -> None:
        self._visit_CreateObject(node, 'DATABASE')

    def visit_AlterDatabase(self, node: qlast.AlterDatabase) -> None:
        self._visit_AlterObject(node, 'DATABASE')

    def visit_DropDatabase(self, node: qlast.DropDatabase) -> None:
        self._visit_DropObject(node, 'DATABASE')

    def visit_CreateRole(self, node: qlast.CreateRole) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ROLE', after_name=after_name)

    def visit_AlterRole(self, node: qlast.AlterRole) -> None:
        self._visit_AlterObject(node, 'ROLE')

    def visit_DropRole(self, node: qlast.DropRole) -> None:
        self._visit_DropObject(node, 'ROLE')

    def visit_CreateMigration(self, node: qlast.CreateMigration) -> None:
        def after_name() -> None:
            if node.parents:
                self.write(' FROM ')
                self.visit(node.parents)

            if node.target:
                self.write(' TO {')
                self._block_ws(1)
                self.visit(node.target)
                self.indentation -= 1
                self.write('}')

        self._visit_CreateObject(node, 'MIGRATION', after_name=after_name)

    def visit_CommitMigration(self, node: qlast.CommitMigration) -> None:
        self._visit_aliases(node)
        self.write('COMMIT MIGRATION')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_GetMigration(self, node: qlast.GetMigration) -> None:
        self._visit_aliases(node)
        self.write('GET MIGRATION')
        self.write(' ')
        self.visit(node.name)
        self.new_lines = 1

    def visit_AlterMigration(self, node: qlast.AlterMigration) -> None:
        self._visit_AlterObject(node, 'MIGRATION')

    def visit_DropMigration(self, node: qlast.DropMigration) -> None:
        self._visit_DropObject(node, 'MIGRATION')

    def visit_CreateModule(self, node: qlast.CreateModule) -> None:
        self._visit_CreateObject(node, 'MODULE')
        # Hack to handle the SDL version of this with an empty block.
        if self.sdlmode and not node.commands:
            self.write('{}')

    def visit_AlterModule(self, node: qlast.AlterModule) -> None:
        self._visit_AlterObject(node, 'MODULE')

    def visit_DropModule(self, node: qlast.DropModule) -> None:
        self._visit_DropObject(node, 'MODULE')

    def visit_CreateAlias(self, node: qlast.CreateAlias) -> None:
        if (len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetSpecialField)
                and node.commands[0].name == 'expr'):

            self._visit_CreateObject(node, 'ALIAS', render_commands=False)
            self.write(' := (')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.commands[0].value)
            self.indentation -= 1
            self.new_lines = 1
            self.write(')')
        else:
            self._visit_CreateObject(node, 'ALIAS')

    def visit_AlterAlias(self, node: qlast.AlterAlias) -> None:
        self._visit_AlterObject(node, 'ALIAS')

    def visit_DropAlias(self, node: qlast.DropAlias) -> None:
        self._visit_DropObject(node, 'ALIAS')

    def visit_SetField(self, node: qlast.SetField) -> None:
        if not self.sdlmode:
            self.write('SET ')
        self.write(f'{node.name} := ')
        self.visit(node.value)

    def visit_CreateAnnotation(self, node: qlast.CreateAnnotation) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        if node.inheritable:
            tag = 'ABSTRACT INHERITABLE ANNOTATION'
        else:
            tag = 'ABSTRACT ANNOTATION'
        self._visit_CreateObject(node, tag, after_name=after_name)

    def visit_DropAnnotation(self, node: qlast.DropAnnotation) -> None:
        self._visit_DropObject(node, 'ABSTRACT ANNOTATION')

    def visit_CreateAnnotationValue(
        self,
        node: qlast.CreateAnnotationValue
    ) -> None:
        if self.sdlmode:
            self.write('annotation ')
        else:
            self.write('SET ANNOTATION ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_DropAnnotationValue(
        self,
        node: qlast.DropAnnotationValue
    ) -> None:
        self.write('DROP ANNOTATION ')
        self.visit(node.name)

    def visit_CreateConstraint(self, node: qlast.CreateConstraint) -> None:
        def after_name() -> None:
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

    def visit_AlterConstraint(self, node: qlast.AlterConstraint) -> None:
        self._visit_AlterObject(node, 'ABSTRACT CONSTRAINT')

    def visit_DropConstraint(self, node: qlast.DropConstraint) -> None:
        self._visit_DropObject(node, 'ABSTRACT CONSTRAINT')

    def visit_CreateConcreteConstraint(
        self,
        node: qlast.CreateConcreteConstraint
    ) -> None:
        def after_name() -> None:
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

    def visit_AlterConcreteConstraint(
        self,
        node: qlast.AlterConcreteConstraint
    ) -> None:
        def after_name() -> None:
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

    def visit_DropConcreteConstraint(
        self,
        node: qlast.DropConcreteConstraint
    ) -> None:
        def after_name() -> None:
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

    def visit_CreateScalarType(self, node: qlast.CreateScalarType) -> None:
        keywords = []
        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('SCALAR')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterScalarType(self, node: qlast.AlterScalarType) -> None:
        self._visit_AlterObject(node, 'SCALAR TYPE')

    def visit_DropScalarType(self, node: qlast.DropScalarType) -> None:
        self._visit_DropObject(node, 'SCALAR TYPE')

    def visit_CreateProperty(self, node: qlast.CreateProperty) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ABSTRACT PROPERTY',
                                 after_name=after_name)

    def visit_AlterProperty(self, node: qlast.AlterProperty) -> None:
        self._visit_AlterObject(node, 'ABSTRACT PROPERTY')

    def visit_DropProperty(self, node: qlast.DropProperty) -> None:
        self._visit_DropObject(node, 'ABSTRACT PROPERTY')

    def visit_CreateConcreteProperty(
        self,
        node: qlast.CreateConcreteProperty
    ) -> None:
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
                and isinstance(node.commands[0], qlast.SetSpecialField)
                and node.commands[0].name == 'expr'
            )
        )

        def after_name() -> None:
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

    def _process_AlterConcretePointer_for_SDL(
        self,
        node: Union[qlast.AlterConcreteProperty, qlast.AlterConcreteLink],
    ) -> Tuple[List[str], FrozenSet[qlast.DDLCommand]]:
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

        return keywords, frozenset(specials)

    def visit_AlterConcreteProperty(
        self,
        node: qlast.AlterConcreteProperty
    ) -> None:
        keywords = []
        ignored_cmds: Set[qlast.DDLCommand] = set()
        after_name: Optional[Callable[[], None]] = None

        if self.sdlmode:
            if not self.descmode:
                keywords.append('OVERLOADED')
            quals, ignored_cmds_r = self._process_AlterConcretePointer_for_SDL(
                node)
            keywords.extend(quals)
            ignored_cmds.update(ignored_cmds_r)

            type_cmd = None
            for cmd in node.commands:
                if isinstance(cmd, qlast.SetPropertyType):
                    ignored_cmds.add(cmd)
                    type_cmd = cmd
                    break

            def after_name() -> None:
                if type_cmd is not None:
                    self.write(' -> ')
                    self.visit(type_cmd.type)

        keywords.append('PROPERTY')
        self._visit_AlterObject(
            node, *keywords, ignored_cmds=ignored_cmds,
            allow_short=False, unqualified=True,
            after_name=after_name)

    def visit_DropConcreteProperty(
        self,
        node: qlast.DropConcreteProperty
    ) -> None:
        self._visit_DropObject(node, 'PROPERTY', unqualified=True)

    def visit_CreateLink(self, node: qlast.CreateLink) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, 'ABSTRACT LINK', after_name=after_name)

    def visit_AlterLink(self, node: qlast.AlterLink) -> None:
        self._visit_AlterObject(node, 'ABSTRACT LINK')

    def visit_DropLink(self, node: qlast.DropLink) -> None:
        self._visit_DropObject(node, 'ABSTRACT LINK')

    def visit_CreateConcreteLink(
        self,
        node: qlast.CreateConcreteLink
    ) -> None:
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

        def after_name() -> None:
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
                and isinstance(node.commands[0], qlast.SetSpecialField)
                and node.commands[0].name == 'expr'
            )
        )

        self._visit_CreateObject(
            node, *keywords, after_name=after_name, unqualified=True,
            render_commands=not pure_computable)

    def visit_AlterConcreteLink(self, node: qlast.AlterConcreteLink) -> None:
        keywords = []
        ignored_cmds: Set[qlast.DDLCommand] = set()

        after_name: Optional[Callable[[], None]]

        if self.sdlmode:
            if (not self.descmode
                    or not node.system_comment
                    or 'inherited from' not in node.system_comment):
                keywords.append('OVERLOADED')
            quals, ignored_cmds_r = self._process_AlterConcretePointer_for_SDL(
                node)
            keywords.extend(quals)
            ignored_cmds.update(ignored_cmds_r)

            type_cmd = None
            inherit_cmd = None
            for cmd in node.commands:
                if isinstance(cmd, qlast.SetLinkType):
                    ignored_cmds.add(cmd)
                    type_cmd = cmd
                elif isinstance(cmd, qlast.AlterAddInherit):
                    ignored_cmds.add(cmd)
                    inherit_cmd = cmd

            def after_name() -> None:
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

    def visit_DropConcreteLink(self, node: qlast.DropConcreteLink) -> None:
        self._visit_DropObject(node, 'LINK', unqualified=True)

    def visit_SetPropertyType(self, node: qlast.SetPropertyType) -> None:
        self.write('SET TYPE ')
        self.visit(node.type)

    def visit_SetLinkType(self, node: qlast.SetLinkType) -> None:
        self.write('SET TYPE ')
        self.visit(node.type)

    def visit_OnTargetDelete(self, node: qlast.OnTargetDelete) -> None:
        self.write('ON TARGET DELETE ', node.cascade)

    def visit_CreateObjectType(self, node: qlast.CreateObjectType) -> None:
        keywords = []

        if node.is_abstract:
            keywords.append('ABSTRACT')
        if node.is_final:
            keywords.append('FINAL')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(
            node, *keywords, after_name=after_name)

    def visit_AlterObjectType(self, node: qlast.AlterObjectType) -> None:
        self._visit_AlterObject(node, 'TYPE')

    def visit_DropObjectType(self, node: qlast.DropObjectType) -> None:
        self._visit_DropObject(node, 'TYPE')

    def visit_CreateIndex(self, node: qlast.CreateIndex) -> None:
        def after_name() -> None:
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')

        self._visit_CreateObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_AlterIndex(self, node: qlast.AlterIndex) -> None:
        def after_name() -> None:
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')

        self._visit_AlterObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_DropIndex(self, node: qlast.DropIndex) -> None:
        def after_name() -> None:
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.expr)
            self.write(')')

        self._visit_DropObject(
            node, 'INDEX', after_name=after_name, named=False)

    def visit_CreateFunction(self, node: qlast.CreateFunction) -> None:
        def after_name() -> None:
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
            else:
                self.write(' ')

            if node.code.from_function:
                from_clause = f'USING {node.code.language} FUNCTION '
                if self.sdlmode:
                    from_clause = from_clause.lower()
                self.write(from_clause)
                self.write(f'{node.code.from_function!r}')
            elif node.code.language is qlast.Language.EdgeQL:
                assert node.code.code
                self._write_keywords('USING')
                self.write(f' ({node.code.code})')
            else:
                from_clause = f'USING {node.code.language} '
                if self.sdlmode:
                    from_clause = from_clause.lower()
                self.write(from_clause)
                if node.code.code:
                    self.write(edgeql_quote.dollar_quote_literal(
                        node.code.code))

            self._block_ws(-1)
            if node.commands:
                self.write(';')
                self.write('}')

        self._visit_CreateObject(node, 'FUNCTION', after_name=after_name,
                                 render_commands=False)

    def visit_AlterFunction(self, node: qlast.AlterFunction) -> None:
        self._visit_AlterObject(node, 'FUNCTION')

    def visit_DropFunction(self, node: qlast.DropFunction) -> None:
        self._visit_DropObject(node, 'FUNCTION')

    def visit_FuncParam(self, node: qlast.FuncParam) -> None:
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

    def visit_ConfigSet(self, node: qlast.ConfigSet) -> None:
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' SET ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.expr)

    def visit_ConfigInsert(self, node: qlast.ConfigInsert) -> None:
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' INSERT ')
        self.visit(node.name)
        self.indentation += 1
        self._visit_shape(node.shape)
        self.indentation -= 1

    def visit_ConfigReset(self, node: qlast.ConfigReset) -> None:
        self.write('CONFIGURE')
        self.write(' SYSTEM' if node.system else ' SESSION')
        self.write(' RESET ')
        self.visit(node.name)
        self._visit_filter(node)

    def visit_SessionSetAliasDecl(
        self,
        node: qlast.SessionSetAliasDecl
    ) -> None:
        self.write('SET')
        if node.alias:
            self.write(' ALIAS ')
            self.write(ident_to_str(node.alias))
            self.write(' AS MODULE ')
            self.write(node.module)
        else:
            self.write(' MODULE ')
            self.write(node.module)

    def visit_SessionResetAllAliases(
        self,
        node: qlast.SessionResetAllAliases
    ) -> None:
        self.write('RESET ALIAS *')

    def visit_SessionResetModule(self, node: qlast.SessionResetModule) -> None:
        self.write('RESET MODULE')

    def visit_SessionResetAliasDecl(
        self,
        node: qlast.SessionResetAliasDecl
    ) -> None:
        self.write('RESET ALIAS ')
        self.write(node.alias)

    def visit_StartTransaction(self, node: qlast.StartTransaction) -> None:
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

    def visit_RollbackTransaction(
        self,
        node: qlast.RollbackTransaction
    ) -> None:
        self.write('ROLLBACK')

    def visit_CommitTransaction(self, node: qlast.CommitTransaction) -> None:
        self.write('COMMIT')

    def visit_DeclareSavepoint(self, node: qlast.DeclareSavepoint) -> None:
        self.write(f'DECLARE SAVEPOINT {node.name}')

    def visit_RollbackToSavepoint(
        self,
        node: qlast.RollbackToSavepoint
    ) -> None:
        self.write(f'ROLLBACK TO SAVEPOINT {node.name}')

    def visit_ReleaseSavepoint(self, node: qlast.ReleaseSavepoint) -> None:
        self.write(f'RELEASE SAVEPOINT {node.name}')

    def visit_DescribeStmt(self, node: qlast.DescribeStmt) -> None:
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

    def visit_Options(self, node: qlast.Options) -> None:
        for i, opt in enumerate(node.options.values()):
            if i > 0:
                self.write(' ')
            self.write(opt.name)
            if not isinstance(opt, qlast.Flag):
                self.write(f' {opt.val}')

    # SDL nodes

    def visit_Schema(self, node: qlast.Schema) -> None:
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

    def visit_ModuleDeclaration(self, node: qlast.ModuleDeclaration) -> None:
        self.write('module ')
        # the name is always unqualified here
        self.write(ident_to_str(node.name.name))
        self.write('{')
        self._block_ws(1)
        self.visit_list(node.declarations, terminator=';')
        self._block_ws(-1)
        self.write('}')

    @classmethod
    def to_source(  # type: ignore
        cls,
        node: Union[qlast.Base, Sequence[qlast.Base]],
        indent_with: str = ' ' * 4,
        add_line_information: bool = False,
        pretty: bool = True,
        sdlmode: bool = False,
        descmode: bool = False,
        limit_ref_classes:
            Optional[AbstractSet[qltypes.SchemaObjectClass]] = None,
        unsorted: bool = False,
    ) -> str:
        if isinstance(node, (list, tuple)):
            for n in node:
                _fix_parent_links(n)
        else:
            assert isinstance(node, qlast.Base)
            _fix_parent_links(node)

        return super().to_source(
            node, indent_with, add_line_information, pretty,
            sdlmode=sdlmode, descmode=descmode, unsorted=unsorted,
            limit_ref_classes=limit_ref_classes)


def _fix_parent_links(node: qlast.Base) -> qlast.Base:
    # NOTE: Do not use this legacy function in new code!
    # Using AST.parent is an anti-pattern. Instead write code
    # that uses singledispatch and maintains a proper context.

    node._parent = None

    for _field, value in base.iter_fields(node):
        if isinstance(value, dict):
            for n in value.values():
                if base.is_ast_node(n):
                    _fix_parent_links(n)
                    n._parent = node

        elif typeutils.is_container(value):
            for n in value:
                if base.is_ast_node(n):
                    _fix_parent_links(n)
                    n._parent = node

        elif base.is_ast_node(value):
            _fix_parent_links(value)
            value._parent = node

    return node


generate_source = EdgeQLSourceGenerator.to_source
