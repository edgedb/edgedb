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
from typing import *

import itertools
import re

from edb import errors
from edb.common.ast import codegen, base
from edb.common import typeutils

from . import ast as qlast
from . import quote as edgeql_quote
from . import qltypes


_module_name_re = re.compile(r'^(?!=\d)\w+(\.(?!=\d)\w+)*$')
_BYTES_ESCAPE_RE = re.compile(b'[\\\'\x00-\x1f\x7e-\xff]')
_NON_PRINTABLE_RE = re.compile(
    r'[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F\n]')
_ESCAPES = {
    b'\\': b'\\\\',
    b'\'': b'\\\'',
    b'\t': b'\\t',
    b'\n': b'\\n',
}


if TYPE_CHECKING:
    import enum
    Enum_T = TypeVar('Enum_T', bound=enum.Enum)


def _bytes_escape(match: Match[bytes]) -> bytes:
    char = match.group(0)
    try:
        return _ESCAPES[char]
    except KeyError:
        return b'\\x%02x' % char[0]


def any_ident_to_str(ident: str) -> str:
    if _module_name_re.match(ident):
        return ident
    else:
        return ident_to_str(ident)


def ident_to_str(ident: str, allow_num: bool=False) -> str:
    return edgeql_quote.quote_ident(ident, allow_num=allow_num)


def param_to_str(ident: str) -> str:
    return '$' + edgeql_quote.quote_ident(
        ident, allow_reserved=True, allow_num=True)


def module_to_str(module: str) -> str:
    return '::'.join([
        any_ident_to_str(part) for part in module.split('::')
    ])


class EdgeQLSourceGeneratorError(errors.InternalServerError):
    pass


class EdgeSchemaSourceGeneratorError(errors.InternalServerError):
    pass


class EdgeQLSourceGenerator(codegen.SourceGenerator):

    def __init__(
        self, *args: Any,
        sdlmode: bool = False,
        descmode: bool = False,
        # Uppercase keywords for backwards compatibility with older migrations.
        uppercase: bool = False,
        unsorted: bool = False,
        limit_ref_classes:
            Optional[AbstractSet[qltypes.SchemaObjectClass]] = None,
        **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sdlmode = sdlmode
        self.descmode = descmode
        self.uppercase = uppercase
        self.unsorted = unsorted
        self.limit_ref_classes = limit_ref_classes

    def visit(
        self,
        node: Union[qlast.Base, List[qlast.Base]],
        **kwargs: Any
    ) -> None:
        if isinstance(node, list):
            self.visit_list(node, terminator=';')
        else:
            method = 'visit_' + node.__class__.__name__
            visitor = getattr(self, method, self.generic_visit)
            visitor(node, **kwargs)

    def _kw_case(self, *kws: str) -> str:
        kwstring = ' '.join(kws)
        if self.uppercase:
            kwstring = kwstring.upper()
        else:
            kwstring = kwstring.lower()
        return kwstring

    def _write_keywords(self, *kws: str) -> None:
        self.write(self._kw_case(*kws))

    def _needs_parentheses(self, node) -> bool:  # type: ignore
        # The "parent" attribute is set by calling `_fix_parent_links`
        # before traversing the AST.  Since it's not an attribute that
        # can be inferred by static typing we ignore typing for this
        # function.
        return (
            node._parent is not None and (
                not isinstance(node._parent, qlast.Base)
                or not isinstance(node._parent, qlast.DDL)
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
            self._write_keywords('WITH')
            self._block_ws(1)
            if node.aliases:
                self.visit_list(node.aliases)
            self._block_ws(-1)

    def _visit_filter(self, node: qlast.FilterMixin,
                      newlines: bool = True) -> None:
        if node.where:
            self._write_keywords('FILTER')
            self._block_ws(1, newlines)
            self.visit(node.where)
            self._block_ws(-1, newlines)

    def _visit_order(self, node: qlast.OrderByMixin,
                     newlines: bool = True) -> None:
        if node.orderby:
            self._write_keywords('ORDER BY')
            self._block_ws(1, newlines)
            self.visit_list(
                node.orderby,
                separator=self._kw_case(' THEN'), newlines=newlines
            )
            self._block_ws(-1, newlines)

    def _visit_offset_limit(self, node: qlast.OffsetLimitMixin,
                            newlines: bool = True) -> None:
        if node.offset is not None:
            self._write_keywords('OFFSET')
            self._block_ws(1, newlines)
            self.visit(node.offset)
            self._block_ws(-1, newlines)
        if node.limit is not None:
            self._write_keywords('LIMIT')
            self._block_ws(1, newlines)
            self.visit(node.limit)
            self._block_ws(-1, newlines)

    def visit_OptionallyAliasedExpr(
            self, node: qlast.OptionallyAliasedExpr) -> None:
        if node.alias:
            self.write(ident_to_str(node.alias))
            self.write(' := ')
            self._block_ws(1)

        self.visit(node.expr)

        if node.alias:
            self._block_ws(-1)

    def visit_AliasedExpr(self, node: qlast.AliasedExpr) -> None:
        self.visit_OptionallyAliasedExpr(node)

    def visit_InsertQuery(self, node: qlast.InsertQuery) -> None:
        # need to parenthesise when INSERT appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self._write_keywords('INSERT')
        self._block_ws(1)
        self.visit(node.subject)
        self._block_ws(-1)

        if node.shape:
            self.indentation += 1
            self._visit_shape(node.shape)
            self.indentation -= 1

        if node.unless_conflict:
            on_expr, else_expr = node.unless_conflict
            self._write_keywords('UNLESS CONFLICT')

            if on_expr:
                self._write_keywords(' ON ')
                self.visit(on_expr)

                if else_expr:
                    self._write_keywords(' ELSE ')
                    self.visit(else_expr)

        if parenthesise:
            self.write(')')

    def visit_UpdateQuery(self, node: qlast.UpdateQuery) -> None:
        # need to parenthesise when UPDATE appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')
        self._visit_aliases(node)
        self._write_keywords('UPDATE')
        self._block_ws(1)
        self.visit(node.subject)
        self._block_ws(-1)

        self._visit_filter(node)

        self.new_lines = 1
        self._write_keywords('SET ')
        self._visit_shape(node.shape)

        if parenthesise:
            self.write(')')

    def visit_DeleteQuery(self, node: qlast.DeleteQuery) -> None:
        # need to parenthesise when DELETE appears as an expression
        parenthesise = self._needs_parentheses(node)

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        self._write_keywords('DELETE')
        self._block_ws(1)
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
            self._write_keywords('SELECT')
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

        self._write_keywords('FOR ')
        self.write(ident_to_str(node.iterator_alias))
        self._write_keywords(' IN ')
        self.visit(node.iterator)
        # guarantee an newline here
        self.new_lines = 1
        self._write_keywords('UNION ')
        self._block_ws(1)
        self.visit(node.result)
        self.indentation -= 1

        if parenthesise:
            self.write(')')

    def visit_GroupingIdentList(self, atom: qlast.GroupingIdentList) -> None:
        self.write('(')
        self.visit_list(atom.elements, newlines=False)
        self.write(')')

    def visit_GroupingSimple(self, node: qlast.GroupingSimple) -> None:
        self.visit(node.element)

    def visit_GroupingSets(self, node: qlast.GroupingSets) -> None:
        self.write('{')
        self.visit_list(node.sets, newlines=False)
        self.write('}')

    def visit_GroupingOperation(self, node: qlast.GroupingOperation) -> None:
        self._write_keywords(node.oper)
        self.write(' (')
        self.visit_list(node.elements, newlines=False)
        self.write(')')

    def visit_GroupQuery(
            self, node: qlast.GroupQuery, no_paren: bool=False) -> None:
        # need to parenthesise when GROUP appears as an expression
        parenthesise = self._needs_parentheses(node) and not no_paren

        if parenthesise:
            self.write('(')

        self._visit_aliases(node)

        if isinstance(node, qlast.InternalGroupQuery):
            self._write_keywords('FOR ')
        self._write_keywords('GROUP')
        self._block_ws(1)
        if node.subject_alias:
            self.write(ident_to_str(node.subject_alias), ' := ')
        self.visit(node.subject)
        self._block_ws(-1)
        if node.using is not None:
            self._write_keywords('USING')
            self._block_ws(1)
            self.visit_list(node.using, newlines=False)
            self._block_ws(-1)
        self._write_keywords('BY ')
        self.visit_list(node.by)

        if parenthesise:
            self.write(')')

    def visit_InternalGroupQuery(self, node: qlast.InternalGroupQuery) -> None:
        parenthesise = self._needs_parentheses(node)
        if parenthesise:
            self.write('(')

        self.visit_GroupQuery(node, no_paren=True)
        self._block_ws(0)
        self._write_keywords('INTO ')
        self.write(ident_to_str(node.group_alias))
        if node.grouping_alias:
            self.write(', ')
            self.write(ident_to_str(node.grouping_alias))
        self.write(' ')
        self._block_ws(0)
        self._write_keywords('UNION ')
        self.visit(node.result)

        if node.where:
            self._write_keywords(' FILTER ')
            self.visit(node.where)

        if node.orderby:
            self._write_keywords(' ORDER BY ')
            self.visit_list(
                node.orderby,
                separator=self._kw_case(' THEN'), newlines=False
            )

        if parenthesise:
            self.write(')')

    def visit_ModuleAliasDecl(self, node: qlast.ModuleAliasDecl) -> None:
        if node.alias:
            self.write(ident_to_str(node.alias))
            self._write_keywords(' AS ')
        self._write_keywords('MODULE ')
        self.write(module_to_str(node.module))

    def visit_SortExpr(self, node: qlast.SortExpr) -> None:
        self.visit(node.path)
        if node.direction:
            self.write(' ')
            self.write(node.direction)
        if node.nones_order:
            self._write_keywords(' EMPTY ')
            self.write(node.nones_order.upper())

    def visit_DetachedExpr(self, node: qlast.DetachedExpr) -> None:
        self._write_keywords('DETACHED ')
        self.visit(node.expr)

    def visit_GlobalExpr(self, node: qlast.GlobalExpr) -> None:
        self._write_keywords('GLOBAL ')
        self.visit(node.name)

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
        self._write_keywords(' IF ')
        self.visit(node.condition)
        self._write_keywords(' ELSE ')
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
                elif isinstance(e, qlast.Anchor):
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

        self.write(ident_to_str(node.ptr.name, allow_num=True))

    def visit_TypeIntersection(self, node: qlast.TypeIntersection) -> None:
        self._write_keywords('[IS ')
        self.visit(node.type)
        self.write(']')

    def visit_ShapeElement(self, node: qlast.ShapeElement) -> None:
        # PathSpec can only contain LinkExpr or LinkPropExpr,
        # and must not be quoted.

        quals = []
        if node.required is not None:
            if node.required:
                quals.append('required')
            else:
                quals.append('optional')

        if node.cardinality:
            quals.append(node.cardinality.as_ptr_qual())

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
            if len(node.expr.steps) == 3:
                self.visit(node.expr.steps[2])

        if not node.compexpr and node.elements:
            self.write(': ')
            self._visit_shape(node.elements)

        if node.where:
            self._write_keywords(' FILTER ')
            self.visit(node.where)

        if node.orderby:
            self._write_keywords(' ORDER BY ')
            self.visit_list(
                node.orderby,
                separator=self._kw_case(' THEN'), newlines=False
            )

        if node.offset:
            self._write_keywords(' OFFSET ')
            self.visit(node.offset)

        if node.limit:
            self._write_keywords(' LIMIT ')
            self.visit(node.limit)

        if node.compexpr:
            if node.operation is None:
                raise AssertionError(
                    f'ShapeElement.operation is unexpectedly None'
                )

            if node.operation.op is qlast.ShapeOp.ASSIGN:
                self.write(' := ')
            elif node.operation.op is qlast.ShapeOp.APPEND:
                self.write(' += ')
            elif node.operation.op is qlast.ShapeOp.SUBTRACT:
                self.write(' -= ')
            else:
                raise NotImplementedError(
                    f'unexpected shape operation: {node.operation.op!r}'
                )
            self.visit(node.compexpr)

    def visit_Parameter(self, node: qlast.Parameter) -> None:
        self.write(param_to_str(node.name))

    def visit_Placeholder(self, node: qlast.Placeholder) -> None:
        self.write('\\(')
        self.write(node.name)
        self.write(')')

    def visit_StringConstant(self, node: qlast.StringConstant) -> None:
        if not _NON_PRINTABLE_RE.search(node.value):
            for d in ("'", '"', '$$'):
                if d not in node.value:
                    if '\\' in node.value and d != '$$':
                        self.write('r', d, node.value, d)
                    else:
                        self.write(d, node.value, d)
                    return
            self.write(edgeql_quote.dollar_quote_literal(node.value))
            return
        self.write(repr(node.value))

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
        val = _BYTES_ESCAPE_RE.sub(_bytes_escape, node.value)
        self.write("b'", val.decode('utf-8', 'backslashreplace'), "'")

    def visit_FunctionCall(self, node: qlast.FunctionCall) -> None:
        if isinstance(node.func, tuple):
            self.write(
                f'{ident_to_str(node.func[0])}::{ident_to_str(node.func[1])}')
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
                self.write(f'{edgeql_quote.quote_ident(name)} := ')
                self.visit(arg)

        self.write(')')

        if node.window:
            self._write_keywords(' OVER (')
            self._block_ws(1)

            if node.window.partition:
                self._write_keywords('PARTITION BY ')
                self.visit_list(node.window.partition, newlines=False)
                self.new_lines = 1

            if node.window.orderby:
                self._write_keywords('ORDER BY ')
                self.visit_list(
                    node.window.orderby, separator=self._kw_case(' THEN'))

            self._block_ws(-1)
            self.write(')')

    def visit_AnyType(self, node: qlast.AnyType) -> None:
        self.write('anytype')

    def visit_AnyTuple(self, node: qlast.AnyTuple) -> None:
        self.write('anytuple')

    def visit_TypeCast(self, node: qlast.TypeCast) -> None:
        self.write('<')
        if node.cardinality_mod is qlast.CardinalityModifier.Optional:
            self.write('optional ')
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

    def visit_Anchor(self, node: qlast.Anchor) -> None:
        self.write(node.name)

    def visit_Subject(self, node: qlast.Subject) -> None:
        self.write(node.name)

    def visit_Source(self, node: qlast.Source) -> None:
        self.write(node.name)

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

    def _ddl_clean_up_commands(
        self,
        commands: Sequence[qlast.Base],
    ) -> Sequence[qlast.Base]:
        # Always omit orig_expr fields from output since we are
        # using the original expression in TEXT output
        # already.
        return [
            c for c in commands
            if (
                not isinstance(c, qlast.SetField)
                or not c.name.startswith('orig_')
            )
        ]

    def _ddl_visit_body(
        self,
        commands: Sequence[qlast.Base],
        group_by_system_comment: bool = False,
        *,
        allow_short: bool = False
    ) -> None:
        if self.limit_ref_classes:
            commands = [
                c for c in commands
                if (
                    not isinstance(c, qlast.ObjectDDL)
                    or c.name.itemclass in self.limit_ref_classes
                )
            ]

        commands = self._ddl_clean_up_commands(commands)
        if len(commands) == 1 and allow_short and not (
            isinstance(commands[0], qlast.ObjectDDL)
        ):
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
                    (c.name.itemclass or '', c.name.name)
                    if isinstance(c, qlast.ObjectDDL)
                    else ('', c.name if isinstance(c, qlast.SetField) else '')
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
            self._write_keywords('CREATE', *object_keywords)
        if named:
            self.write(' ')
            if unqualified or not node.name.module:
                self.write(ident_to_str(node.name.name))
            else:
                self.write(ident_to_str(node.name.module), '::',
                           ident_to_str(node.name.name))
        if after_name:
            after_name()
        if node.create_if_not_exists and not self.sdlmode:
            self._write_keywords(' IF NOT EXISTS')

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
        ignored_cmds: Optional[AbstractSet[qlast.DDLOperation]] = None,
        group_by_system_comment: bool = False
    ) -> None:
        self._visit_aliases(node)
        if self.sdlmode:
            self.write(*[kw.lower() for kw in object_keywords], delimiter=' ')
        else:
            self._write_keywords('ALTER', *object_keywords)
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
        self._write_keywords('DROP', *object_keywords)
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
        self._write_keywords('RENAME TO ')
        self.visit(node.new_name)

    def visit_AlterAddInherit(self, node: qlast.AlterAddInherit) -> None:
        if node.bases:
            self._write_keywords('EXTENDING ')
            self.visit_list(node.bases)
            if node.position is not None:
                self.write(' ')
                self.visit(node.position)

    def visit_AlterDropInherit(self, node: qlast.AlterDropInherit) -> None:
        if node.bases:
            self._write_keywords('DROP EXTENDING ')
            self.visit_list(node.bases)

    def visit_CreateDatabase(self, node: qlast.CreateDatabase) -> None:
        self._visit_CreateObject(node, 'DATABASE')

    def visit_AlterDatabase(self, node: qlast.AlterDatabase) -> None:
        self._visit_AlterObject(node, 'DATABASE')

    def visit_DropDatabase(self, node: qlast.DropDatabase) -> None:
        self._visit_DropObject(node, 'DATABASE')

    def visit_CreateRole(self, node: qlast.CreateRole) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        keywords = []
        if node.superuser:
            keywords.append('SUPERUSER')
        keywords.append('ROLE')
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterRole(self, node: qlast.AlterRole) -> None:
        self._visit_AlterObject(node, 'ROLE')

    def visit_DropRole(self, node: qlast.DropRole) -> None:
        self._visit_DropObject(node, 'ROLE')

    def visit_CreateExtensionPackage(
        self,
        node: qlast.CreateExtensionPackage,
    ) -> None:
        self._write_keywords('CREATE EXTENSION PACKAGE')
        self.write(' ')
        self.write(ident_to_str(node.name.name))
        self._write_keywords(' VERSION ')
        self.visit(node.version)
        if node.body.text:
            self.write(' {')
            self._block_ws(1)
            self.write(self.indent_text(node.body.text))
            self._block_ws(-1)
            self.write('}')
        elif node.body.commands:
            self._ddl_visit_body(node.body.commands)

    def visit_DropExtensionPackage(
        self,
        node: qlast.DropExtensionPackage,
    ) -> None:
        def after_name() -> None:
            self._write_keywords(' VERSION ')
            self.visit(node.version)
        self._visit_DropObject(
            node, 'EXTENSION PACKAGE', after_name=after_name)

    def visit_CreateExtension(
        self,
        node: qlast.CreateExtension,
    ) -> None:
        if self.sdlmode or self.descmode:
            self._write_keywords('using extension')
        else:
            self._write_keywords('CREATE EXTENSION')
        self.write(' ')
        self.write(ident_to_str(node.name.name))
        if node.version is not None:
            self._write_keywords(' version ')
            self.visit(node.version)
        if node.commands:
            self._ddl_visit_body(node.commands)

    def visit_DropExtension(
        self,
        node: qlast.DropExtension,
    ) -> None:
        self._visit_DropObject(node, 'EXTENSION')

    def visit_CreateFuture(
        self,
        node: qlast.CreateFuture,
    ) -> None:
        if self.sdlmode or self.descmode:
            self._write_keywords('using future')
        else:
            self._write_keywords('CREATE FUTURE')
        self.write(' ')
        self.write(ident_to_str(node.name.name))

    def visit_DropFuture(
        self,
        node: qlast.DropFuture,
    ) -> None:
        self._visit_DropObject(node, 'FUTURE')

    def visit_CreateMigration(self, node: qlast.CreateMigration) -> None:
        self._write_keywords('CREATE')
        if node.metadata_only:
            self._write_keywords(' APPLIED')
        self._write_keywords(' MIGRATION')
        if node.name is not None:
            self.write(' ')
            self.write(ident_to_str(node.name.name))
            self._write_keywords(' ONTO ')
            if node.parent is not None:
                self.write(ident_to_str(node.parent.name))
            else:
                self._write_keywords('initial')
        if node.body.text:
            self.write(' {')
            self._block_ws(1)
            self.write(self.indent_text(node.body.text))
            self._block_ws(-1)
            self.write('}')
        elif node.body.commands:
            self._ddl_visit_body(node.body.commands)

    def visit_StartMigration(self, node: qlast.StartMigration) -> None:
        if isinstance(node.target, qlast.CommittedSchema):
            self._write_keywords('START MIGRATION TO COMMITTED SCHEMA')
        else:
            self._write_keywords('START MIGRATION TO {')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.target)
            self.indentation -= 1
            self.new_lines = 1
            self.write('}')

    def visit_CommitMigration(self, node: qlast.CommitMigration) -> None:
        self._write_keywords('COMMIT MIGRATION')

    def visit_AbortMigration(self, node: qlast.AbortMigration) -> None:
        self._write_keywords('ABORT MIGRATION')

    def visit_PopulateMigration(self, node: qlast.PopulateMigration) -> None:
        self._write_keywords('POPULATE MIGRATION')

    def visit_StartMigrationRewrite(
            self, node: qlast.StartMigrationRewrite) -> None:
        self._write_keywords('START MIGRATION REWRITE')

    def visit_CommitMigrationRewrite(
            self, node: qlast.CommitMigrationRewrite) -> None:
        self._write_keywords('COMMIT MIGRATION REWRITE')

    def visit_AbortMigrationRewrite(
            self, node: qlast.AbortMigrationRewrite) -> None:
        self._write_keywords('ABORT MIGRATION REWRITE')

    def visit_DescribeCurrentMigration(
        self,
        node: qlast.DescribeCurrentMigration,
    ) -> None:
        self._write_keywords('DESCRIBE CURRENT MIGRATION AS ')
        self.write(node.language.upper())

    def visit_AlterCurrentMigrationRejectProposed(
        self,
        node: qlast.AlterCurrentMigrationRejectProposed,
    ) -> None:
        self._write_keywords('ALTER CURRENT MIGRATION REJECT PROPOSED')

    def visit_AlterMigration(self, node: qlast.AlterMigration) -> None:
        self._visit_AlterObject(node, 'MIGRATION')

    def visit_DropMigration(self, node: qlast.DropMigration) -> None:
        self._visit_DropObject(node, 'MIGRATION')

    def visit_ResetSchema(
            self, node: qlast.ResetSchema) -> None:
        self._write_keywords(f'RESET SCHEMA TO {node.target}')

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
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name == 'expr'):

            self._visit_CreateObject(node, 'ALIAS', render_commands=False)
            self.write(' := (')
            self.new_lines = 1
            self.indentation += 1
            expr = node.commands[0].value
            assert expr is not None
            self.visit(expr)
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
        if node.special_syntax:
            if node.name == 'expr':
                if node.value is None:
                    self._write_keywords('RESET', 'EXPRESSION')
                else:
                    self._write_keywords('USING')
                    self.write(' (')
                    self.visit(node.value)
                    self.write(')')
            elif node.name == 'condition':
                if node.value is None:
                    self._write_keywords('RESET', 'WHEN')
                else:
                    self._write_keywords('WHEN')
                    self.write(' (')
                    self.visit(node.value)
                    self.write(')')
            elif node.name == 'target':
                if node.value is None:
                    self._write_keywords('RESET', 'TYPE')
                else:
                    self._write_keywords('SET', 'TYPE ')
                    self.visit(node.value)
            else:
                keywords = self._process_special_set(node)
                self.write(*keywords, delimiter=' ')
        elif node.value:
            if not self.sdlmode:
                self._write_keywords('SET ')
            self.write(f'{node.name} := ')
            if not isinstance(node.value, (qlast.BaseConstant, qlast.Set)):
                self.write('(')
            self.visit(node.value)
            if not isinstance(node.value, (qlast.BaseConstant, qlast.Set)):
                self.write(')')
        elif not self.sdlmode:
            self._write_keywords('RESET ')
            self.write(node.name)

    def _eval_bool_expr(
        self,
        expr: Union[qlast.Expr, qlast.TypeExpr],
    ) -> bool:
        if not isinstance(expr, qlast.BooleanConstant):
            raise AssertionError(f'expected BooleanConstant, got {expr!r}')
        return expr.value == 'true'

    def _eval_enum_expr(
        self,
        expr: Union[qlast.Expr, qlast.TypeExpr],
        enum_type: Type[Enum_T],
    ) -> Enum_T:
        if not isinstance(expr, qlast.StringConstant):
            raise AssertionError(f'expected StringConstant, got {expr!r}')
        return enum_type(expr.value)

    def _process_special_set(
        self,
        node: qlast.SetField
    ) -> List[str]:

        keywords: List[str] = []
        fname = node.name

        if fname == 'required':
            if node.value is None:
                keywords.extend(('RESET', 'OPTIONALITY'))
            elif self._eval_bool_expr(node.value):
                keywords.extend(('SET', 'REQUIRED'))
            else:
                keywords.extend(('SET', 'OPTIONAL'))
        elif fname == 'abstract':
            if node.value is None:
                keywords.extend(('RESET', 'ABSTRACT'))
            elif self._eval_bool_expr(node.value):
                keywords.extend(('SET', 'ABSTRACT'))
            else:
                keywords.extend(('SET', 'NOT', 'ABSTRACT'))
        elif fname == 'delegated':
            if node.value is None:
                keywords.extend(('RESET', 'DELEGATED'))
            elif self._eval_bool_expr(node.value):
                keywords.extend(('SET', 'DELEGATED'))
            else:
                keywords.extend(('SET', 'NOT', 'DELEGATED'))
        elif fname == 'cardinality':
            if node.value is None:
                keywords.extend(('RESET', 'CARDINALITY'))
            elif node.value:
                value = self._eval_enum_expr(
                    node.value, qltypes.SchemaCardinality)
                keywords.extend(('SET', value.to_edgeql()))
        elif fname == 'owned':
            if node.value is None:
                keywords.extend(('DROP', 'OWNED'))
            elif self._eval_bool_expr(node.value):
                keywords.extend(('SET', 'OWNED'))
            else:
                keywords.extend(('DROP', 'OWNED'))
        else:
            raise EdgeQLSourceGeneratorError(
                'unknown special field: {!r}'.format(fname))

        return keywords

    def visit_CreateAnnotation(self, node: qlast.CreateAnnotation) -> None:
        after_name = lambda: self._ddl_visit_bases(node)
        if node.inheritable:
            tag = 'ABSTRACT INHERITABLE ANNOTATION'
        else:
            tag = 'ABSTRACT ANNOTATION'
        self._visit_CreateObject(node, tag, after_name=after_name)

    def visit_AlterAnnotation(self, node: qlast.AlterAnnotation) -> None:
        self._visit_AlterObject(node, 'ABSTRACT ANNOTATION')

    def visit_DropAnnotation(self, node: qlast.DropAnnotation) -> None:
        self._visit_DropObject(node, 'ABSTRACT ANNOTATION')

    def visit_CreateAnnotationValue(
        self,
        node: qlast.CreateAnnotationValue
    ) -> None:
        if self.sdlmode:
            self._write_keywords('annotation ')
        else:
            self._write_keywords('CREATE ANNOTATION ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_AlterAnnotationValue(
        self,
        node: qlast.AlterAnnotationValue
    ) -> None:
        self._write_keywords('ALTER ANNOTATION ')
        self.visit(node.name)
        self.write(' ')
        if node.value:
            self.write(':= ')
            self.visit(node.value)
        else:
            # The command should be a DROP OWNED
            assert len(node.commands) == 1
            self.visit(node.commands[0])

    def visit_DropAnnotationValue(
        self,
        node: qlast.DropAnnotationValue
    ) -> None:
        self._write_keywords('DROP ANNOTATION ')
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

    def _after_constraint(self, node: qlast.ConcreteConstraintOp) -> None:
        if node.args:
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')
        if node.subjectexpr:
            self._write_keywords(' ON ')
            self.write('(')
            self.visit(node.subjectexpr)
            self.write(')')
        if node.except_expr:
            self._write_keywords(' EXCEPT ')
            self.write('(')
            self.visit(node.except_expr)
            self.write(')')

    def visit_CreateConcreteConstraint(
        self,
        node: qlast.CreateConcreteConstraint
    ) -> None:
        keywords = []
        if node.delegated:
            keywords.append('DELEGATED')
        keywords.append('CONSTRAINT')
        self._visit_CreateObject(
            node, *keywords, after_name=lambda: self._after_constraint(node))

    def visit_AlterConcreteConstraint(
        self,
        node: qlast.AlterConcreteConstraint
    ) -> None:
        self._visit_AlterObject(
            node, 'CONSTRAINT', allow_short=False,
            after_name=lambda: self._after_constraint(node))

    def visit_DropConcreteConstraint(
        self,
        node: qlast.DropConcreteConstraint
    ) -> None:
        self._visit_DropObject(node, 'CONSTRAINT',
                               after_name=lambda: self._after_constraint(node))

    def _format_access_kinds(self, kinds: List[qltypes.AccessKind]) -> str:
        # Canonicalize the order, since the schema loses track
        kinds = [k for k in list(qltypes.AccessKind) if k in kinds]
        if kinds == list(qltypes.AccessKind):
            return 'all'
        skinds = ', '.join(str(kind).lower() for kind in kinds)
        skinds = skinds.replace("update", "update ")
        skinds = skinds.replace("update read, update write", "update")
        return skinds

    def visit_CreateAccessPolicy(
        self,
        node: qlast.CreateAccessPolicy
    ) -> None:
        def after_name() -> None:
            if node.condition:
                self._block_ws(1)
                self._write_keywords('WHEN ')
                self.write('(')
                self.visit(node.condition)
                self.write(')')
                self._block_ws(-1)
            self._block_ws(1)
            self._write_keywords(str(node.action) + ' ')
            if node.access_kinds:
                self._write_keywords(
                    self._format_access_kinds(node.access_kinds) + ' ')
            if node.expr:
                self._write_keywords('USING ')
                self.write('(')
                self.visit(node.expr)
                self.write(')')

        keywords = []
        keywords.extend(['ACCESS', 'POLICY'])
        self._visit_CreateObject(
            node, *keywords, after_name=after_name, unqualified=True)
        # This is left hanging from after_name, so that subcommands
        # get double indented
        self.indentation -= 1

    def visit_SetAccessPerms(self, node: qlast.SetAccessPerms) -> None:
        self._write_keywords(str(node.action) + ' ')
        self._write_keywords(self._format_access_kinds(node.access_kinds))

    def visit_AlterAccessPolicy(self, node: qlast.AlterAccessPolicy) -> None:
        self._visit_AlterObject(node, 'ACCESS POLICY', unqualified=True)

    def visit_DropAccessPolicy(self, node: qlast.DropAccessPolicy) -> None:
        self._visit_DropObject(node, 'ACCESS POLICY', unqualified=True)

    def visit_CreateScalarType(self, node: qlast.CreateScalarType) -> None:
        keywords = []
        if node.abstract:
            keywords.append('ABSTRACT')
        keywords.append('SCALAR')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(node, *keywords, after_name=after_name)

    def visit_AlterScalarType(self, node: qlast.AlterScalarType) -> None:
        self._visit_AlterObject(node, 'SCALAR TYPE')

    def visit_DropScalarType(self, node: qlast.DropScalarType) -> None:
        self._visit_DropObject(node, 'SCALAR TYPE')

    def visit_CreatePseudoType(self, node: qlast.CreatePseudoType) -> None:
        keywords = []
        keywords.append('PSEUDO')
        keywords.append('TYPE')
        self._visit_CreateObject(node, *keywords)

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
        else:
            if node.is_required is True:
                keywords.append('REQUIRED')
            elif node.is_required is False:
                keywords.append('OPTIONAL')
            # else: `is_required` is None
        if node.cardinality:
            keywords.append(node.cardinality.as_ptr_qual().upper())
        keywords.append('PROPERTY')

        pure_computable = (
            len(node.commands) == 0
            or (
                len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name == 'expr'
                and not isinstance(node.target, qlast.TypeExpr)
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
    ) -> Tuple[List[str], FrozenSet[qlast.DDLOperation]]:
        keywords = []
        specials = set()

        for command in node.commands:
            if isinstance(command, qlast.SetField) and command.special_syntax:
                kw = self._process_special_set(command)
                specials.add(command)
                if kw[0] == 'SET':
                    keywords.append(kw[1])

        order = ['OPTIONAL', 'REQUIRED', 'SINGLE', 'MULTI']
        keywords.sort(key=lambda i: order.index(i))

        return keywords, frozenset(specials)

    def visit_AlterConcreteProperty(
        self,
        node: qlast.AlterConcreteProperty
    ) -> None:
        keywords = []
        ignored_cmds: Set[qlast.DDLOperation] = set()
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
                if isinstance(cmd, qlast.SetPointerType):
                    ignored_cmds.add(cmd)
                    type_cmd = cmd
                    break

            def after_name() -> None:
                if type_cmd is not None:
                    self.write(' -> ')
                    assert type_cmd.value
                    self.visit(type_cmd.value)

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
        else:
            if node.is_required is True:
                keywords.append("REQUIRED")
            elif node.is_required is False:
                keywords.append("OPTIONAL")
            # else: node.is_required is None
        if node.cardinality:
            keywords.append(node.cardinality.as_ptr_qual().upper())
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
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name == 'expr'
                and not isinstance(node.target, qlast.TypeExpr)
            )
        )

        self._visit_CreateObject(
            node, *keywords, after_name=after_name, unqualified=True,
            render_commands=not pure_computable)

    def visit_AlterConcreteLink(self, node: qlast.AlterConcreteLink) -> None:
        keywords = []
        ignored_cmds: Set[qlast.DDLOperation] = set()

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
                if isinstance(cmd, qlast.SetPointerType):
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
                    assert type_cmd.value
                    self.visit(type_cmd.value)
        else:
            after_name = None

        keywords.append('LINK')
        self._visit_AlterObject(
            node, *keywords, ignored_cmds=ignored_cmds,
            allow_short=False, unqualified=True, after_name=after_name)

    def visit_DropConcreteLink(self, node: qlast.DropConcreteLink) -> None:
        self._visit_DropObject(node, 'LINK', unqualified=True)

    def visit_SetPointerType(self, node: qlast.SetPointerType) -> None:
        if node.value is None:
            self._write_keywords('RESET TYPE')
        else:
            self._write_keywords('SET TYPE ')
            self.visit(node.value)
            if node.cast_expr is not None:
                self._write_keywords(' USING (')
                self.visit(node.cast_expr)
                self.write(')')

    def visit_SetPointerCardinality(
        self,
        node: qlast.SetPointerCardinality,
    ) -> None:
        if node.value is None:
            self._write_keywords('RESET CARDINALITY')
        else:
            value = self._eval_enum_expr(node.value, qltypes.SchemaCardinality)
            self._write_keywords('SET ')
            self.write(value.to_edgeql())
        if node.conv_expr is not None:
            self._write_keywords(' USING (')
            self.visit(node.conv_expr)
            self.write(')')

    def visit_SetPointerOptionality(
        self,
        node: qlast.SetPointerOptionality,
    ) -> None:
        if node.value is None:
            self._write_keywords('RESET OPTIONALITY')
        else:
            if self._eval_bool_expr(node.value):
                self._write_keywords('SET REQUIRED')
            else:
                self._write_keywords('SET OPTIONAL')
            if node.fill_expr is not None:
                self._write_keywords(' USING (')
                self.visit(node.fill_expr)
                self.write(')')

    def visit_OnTargetDelete(self, node: qlast.OnTargetDelete) -> None:
        if node.cascade is None:
            self._write_keywords('RESET ON TARGET DELETE')
        else:
            self._write_keywords('ON TARGET DELETE', node.cascade.to_edgeql())

    def visit_OnSourceDelete(self, node: qlast.OnSourceDelete) -> None:
        if node.cascade is None:
            self._write_keywords('RESET ON SOURCE DELETE')
        else:
            self._write_keywords('ON SOURCE DELETE', node.cascade.to_edgeql())

    def visit_CreateObjectType(self, node: qlast.CreateObjectType) -> None:
        keywords = []

        if node.abstract:
            keywords.append('ABSTRACT')
        keywords.append('TYPE')

        after_name = lambda: self._ddl_visit_bases(node)
        self._visit_CreateObject(
            node, *keywords, after_name=after_name)

    def visit_AlterObjectType(self, node: qlast.AlterObjectType) -> None:
        self._visit_AlterObject(node, 'TYPE')

    def visit_DropObjectType(self, node: qlast.DropObjectType) -> None:
        self._visit_DropObject(node, 'TYPE')

    def _after_index(self, node: qlast.ConcreteIndexCommand) -> None:
        if node.kwargs:
            self.write('(')
            for i, (name, arg) in enumerate(node.kwargs.items()):
                if i > 0:
                    self.write(', ')
                self.write(f'{edgeql_quote.quote_ident(name)} := ')
                self.visit(arg)
            self.write(')')

        self._write_keywords(' ON ')
        self.write('(')
        self.visit(node.expr)
        self.write(')')

        if node.except_expr:
            self._write_keywords(' EXCEPT ')
            self.write('(')
            self.visit(node.except_expr)
            self.write(')')

    def visit_IndexType(
        self,
        node: qlast.IndexType
    ) -> None:
        self.visit(node.name)

        if node.kwargs:
            self.write('(')
            for i, (name, arg) in enumerate(node.kwargs.items()):
                if i > 0:
                    self.write(', ')
                self.write(f'{edgeql_quote.quote_ident(name)} := ')
                self.visit(arg)
            self.write(')')

    def visit_CreateIndex(self, node: qlast.CreateIndex) -> None:
        def after_name() -> None:
            if node.params:
                self.write('(')
                self.visit_list(node.params, newlines=False)
                self.write(')')

            if node.kwargs:
                self.write('(')
                for i, (name, arg) in enumerate(node.kwargs.items()):
                    if i > 0:
                        self.write(', ')
                    self.write(f'{edgeql_quote.quote_ident(name)} := ')
                    self.visit(arg)
                self.write(')')

            if node.index_types:
                self._write_keywords(' USING ')
                self.visit_list(node.index_types, newlines=False)

            self._ddl_visit_bases(node)

            if node.commands or node.code:
                self.write(' {')
                self._block_ws(1)
                commands = self._ddl_clean_up_commands(node.commands)
                self.visit_list(commands, terminator=';')
                self.new_lines = 1

                if node.code:
                    self._write_keywords('USING', node.code.language)
                    self.write(edgeql_quote.dollar_quote_literal(
                        node.code.code))
                    self.write(';')

                self._block_ws(-1)
                self.write('}')

        self._visit_CreateObject(node, 'ABSTRACT INDEX',
                                 after_name=after_name)

    def visit_AlterIndex(self, node: qlast.AlterIndex) -> None:
        self._visit_AlterObject(node, 'ABSTRACT INDEX')

    def visit_DropIndex(self, node: qlast.DropIndex) -> None:
        self._visit_DropObject(node, 'ABSTRACT INDEX')

    def visit_IndexCode(self, node: qlast.IndexCode) -> None:
        self._write_keywords('USING', node.language)
        self.write(edgeql_quote.dollar_quote_literal(
            node.code))

    def visit_CreateConcreteIndex(
        self,
        node: qlast.CreateConcreteIndex
    ) -> None:
        self._visit_CreateObject(
            node, 'INDEX', named=node.name.name != 'idx',
            after_name=lambda: self._after_index(node))

    def visit_AlterConcreteIndex(
        self,
        node: qlast.AlterConcreteIndex
    ) -> None:
        self._visit_AlterObject(
            node, 'INDEX', named=node.name.name != 'idx',
            after_name=lambda: self._after_index(node))

    def visit_DropConcreteIndex(
        self,
        node: qlast.DropConcreteIndex
    ) -> None:
        self._visit_DropObject(
            node, 'INDEX', named=node.name.name != 'idx',
            after_name=lambda: self._after_index(node))

    def visit_CreateOperator(self, node: qlast.CreateOperator) -> None:
        def after_name() -> None:
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')
            self.write(' -> ')
            self.write(node.returning_typemod.to_edgeql(), ' ')
            self.visit(node.returning)

            if node.abstract:
                return

            if node.commands:
                self.write(' {')
                self._block_ws(1)
                commands = self._ddl_clean_up_commands(node.commands)
                self.visit_list(commands, terminator=';')
                self.new_lines = 1
            else:
                self.write(' ')

            if node.code.from_operator:
                from_clause = f'USING {node.code.language} OPERATOR '
                self._write_keywords(from_clause)
                op, *types = node.code.from_operator
                op_str = op
                if types:
                    op_str += f'({",".join(types)})'
                self.write(f'{op_str!r}', ';')
            if node.code.from_function:
                from_clause = f'USING {node.code.language} OPERATOR '
                self._write_keywords(from_clause)
                op, *types = node.code.from_function
                op_str = op
                if types:
                    op_str += f'({",".join(types)})'
                self.write(f'{op_str!r}', ';')
            if node.code.from_expr:
                from_clause = f'USING {node.code.language} EXPRESSION'
                self._write_keywords(from_clause, ';')
            elif node.code.code:
                from_clause = f'USING {node.code.language} '
                self._write_keywords(from_clause)
                self.write(
                    edgeql_quote.dollar_quote_literal(
                        node.code.code),
                    ';'
                )

            self._block_ws(-1)
            if node.commands:
                self.write('}')

        op_type = []
        if node.abstract:
            op_type.append('ABSTRACT')
        if node.kind:
            op_type.append(node.kind.upper())
        op_type.append('OPERATOR')

        self._visit_CreateObject(node, *op_type, after_name=after_name,
                                 render_commands=False)

    def visit_AlterOperator(self, node: qlast.AlterOperator) -> None:
        def after_name() -> None:
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')

        op_type = []
        if node.kind:
            op_type.append(node.kind.upper())
        op_type.append('OPERATOR')
        self._visit_AlterObject(node, *op_type, after_name=after_name)

    def visit_DropOperator(self, node: qlast.DropOperator) -> None:
        def after_name() -> None:
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')

        op_type = []
        if node.kind:
            op_type.append(node.kind.upper())
        op_type.append('OPERATOR')
        self._visit_DropObject(node, *op_type, after_name=after_name)

    def _function_after_name(
        self, node: Union[qlast.CreateFunction, qlast.AlterFunction]
    ) -> None:
        self.write('(')
        self.visit_list(node.params, newlines=False)
        self.write(')')
        if isinstance(node, qlast.CreateFunction):
            self.write(' -> ')
            self._write_keywords(node.returning_typemod.to_edgeql(), '')
            self.visit(node.returning)

        if node.commands:
            self.write(' {')
            self._block_ws(1)
            commands = self._ddl_clean_up_commands(node.commands)
            self.visit_list(commands, terminator=';')
            self.new_lines = 1
        else:
            self.write(' ')

        had_using = True
        if node.code.from_function:
            from_clause = f'USING {node.code.language} FUNCTION '
            self._write_keywords(from_clause)
            self.write(f'{node.code.from_function!r}')
        elif node.code.language is qlast.Language.EdgeQL:
            if node.nativecode:
                self._write_keywords('USING')
                self.write(' (')
                self.visit(node.nativecode)
                self.write(')')
            elif node.code.code:
                self._write_keywords('USING')
                self.write(f' ({node.code.code})')
            else:
                had_using = False
        else:
            from_clause = f'USING {node.code.language} '
            self._write_keywords(from_clause)
            if node.code.code:
                self.write(edgeql_quote.dollar_quote_literal(
                    node.code.code))

        if node.commands:
            self._block_ws(-1)
            if had_using:
                self.write(';')
            self.write('}')

    def visit_CreateFunction(self, node: qlast.CreateFunction) -> None:
        self._visit_CreateObject(
            node, 'FUNCTION',
            after_name=lambda: self._function_after_name(node),
            render_commands=False)

    def visit_AlterFunction(self, node: qlast.AlterFunction) -> None:
        def after_name() -> None:
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')

        self._visit_AlterObject(
            node, 'FUNCTION',
            after_name=lambda: self._function_after_name(node),
            ignored_cmds=set(node.commands))

    def visit_DropFunction(self, node: qlast.DropFunction) -> None:
        def after_name() -> None:
            self.write('(')
            self.visit_list(node.params, newlines=False)
            self.write(')')
        self._visit_DropObject(node, 'FUNCTION', after_name=after_name)

    def visit_FuncParam(self, node: qlast.FuncParam) -> None:
        kind = node.kind.to_edgeql()
        if kind:
            self._write_keywords(kind, '')

        if node.name is not None:
            self.write(ident_to_str(node.name), ': ')

        typemod = node.typemod.to_edgeql()
        if typemod:
            self._write_keywords(typemod, '')

        self.visit(node.type)

        if node.default:
            self.write(' = ')
            self.visit(node.default)

    def visit_CreateCast(self, node: qlast.CreateCast) -> None:
        def after_name() -> None:
            self.write(' ')
            self.visit(node.from_type)
            self._write_keywords(' to ')
            self.visit(node.to_type)

            self.write(' {')
            self._block_ws(1)

            if node.commands:
                commands = self._ddl_clean_up_commands(node.commands)
                self.visit_list(commands, terminator=';')
                self.new_lines = 1

            from_clause = f'USING {node.code.language} '
            code = ''

            if node.code.from_function:
                from_clause += 'FUNCTION'
                code = f'{node.code.from_function!r}'
            elif node.code.from_cast:
                from_clause += 'CAST'
            elif node.code.from_expr:
                from_clause += 'EXPRESSION'
            elif node.code.code:
                code = edgeql_quote.dollar_quote_literal(node.code.code)

            self._write_keywords(from_clause)
            if code:
                self.write(' ', code)
            self.write(';')
            self.new_lines = 1

            if node.allow_assignment:
                self._write_keywords('ALLOW ASSIGNMENT;')
                self.new_lines = 1
            if node.allow_implicit:
                self._write_keywords('ALLOW IMPLICIT;')
                self.new_lines = 1

            self._block_ws(-1)
            self.write('}')

        self._visit_CreateObject(
            node, 'CAST', 'FROM',
            named=False, after_name=after_name, render_commands=False
        )

    def visit_AlterCast(self, node: qlast.AlterCast) -> None:
        def after_name() -> None:
            self._write_keywords('FROM ')
            self.visit(node.from_type)
            self._write_keywords(' TO ')
            self.visit(node.to_type)
        self._visit_AlterObject(
            node,
            'CAST',
            named=False,
            after_name=after_name,
        )

    def visit_DropCast(self, node: qlast.DropCast) -> None:
        def after_name() -> None:
            self._write_keywords('FROM ')
            self.visit(node.from_type)
            self._write_keywords(' TO ')
            self.visit(node.to_type)
        self._visit_DropObject(
            node,
            'CAST',
            named=False,
            after_name=after_name,
        )

    def visit_SetGlobalType(self, node: qlast.SetGlobalType) -> None:
        if node.value is None:
            self._write_keywords('RESET TYPE')
        else:
            self._write_keywords('SET TYPE ')
            self.visit(node.value)
            if node.cast_expr is not None:
                self._write_keywords(' USING (')
                self.visit(node.cast_expr)
                self.write(')')
            elif node.reset_value:
                self._write_keywords(' RESET TO DEFAULT')

    def visit_CreateGlobal(
        self,
        node: qlast.CreateGlobal
    ) -> None:
        keywords = []
        if node.is_required is True:
            keywords.append('REQUIRED')
        elif node.is_required is False:
            keywords.append('OPTIONAL')
        if node.cardinality:
            keywords.append(node.cardinality.as_ptr_qual().upper())
        keywords.append('GLOBAL')

        pure_computable = (
            len(node.commands) == 0
            or (
                len(node.commands) == 1
                and isinstance(node.commands[0], qlast.SetField)
                and node.commands[0].name == 'expr'
                and not isinstance(node.target, qlast.TypeExpr)
            )
        )

        def after_name() -> None:
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
            node, *keywords, after_name=after_name,
            render_commands=not pure_computable)

    def visit_AlterGlobal(self, node: qlast.AlterGlobal) -> None:
        self._visit_AlterObject(node, 'GLOBAL')

    def visit_DropGlobal(self, node: qlast.DropGlobal) -> None:
        self._visit_DropObject(node, 'GLOBAL')

    def visit_ConfigSet(self, node: qlast.ConfigSet) -> None:
        if node.scope == qltypes.ConfigScope.GLOBAL:
            self._write_keywords('SET GLOBAL ')
        else:
            self._write_keywords('CONFIGURE ')
            self.write(node.scope.to_edgeql())
            self._write_keywords(' SET ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.expr)

    def visit_ConfigInsert(self, node: qlast.ConfigInsert) -> None:
        self._write_keywords('CONFIGURE ')
        self.write(node.scope.to_edgeql())
        self._write_keywords(' INSERT ')
        self.visit(node.name)
        self.indentation += 1
        self._visit_shape(node.shape)
        self.indentation -= 1

    def visit_ConfigReset(self, node: qlast.ConfigReset) -> None:
        if node.scope == qltypes.ConfigScope.GLOBAL:
            self._write_keywords('RESET GLOBAL ')
        else:
            self._write_keywords('CONFIGURE ')
            self.write(node.scope.to_edgeql())
            self._write_keywords(' RESET ')
        self.visit(node.name)
        self._visit_filter(node)

    def visit_SessionSetAliasDecl(
        self,
        node: qlast.SessionSetAliasDecl
    ) -> None:
        self._write_keywords('SET')
        if node.alias:
            self._write_keywords(' ALIAS ')
            self.write(ident_to_str(node.alias))
            self._write_keywords(' AS MODULE ')
            self.write(node.module)
        else:
            self._write_keywords(' MODULE ')
            self.write(node.module)

    def visit_SessionResetAllAliases(
        self,
        node: qlast.SessionResetAllAliases
    ) -> None:
        self._write_keywords('RESET ALIAS *')

    def visit_SessionResetModule(self, node: qlast.SessionResetModule) -> None:
        self._write_keywords('RESET MODULE')

    def visit_SessionResetAliasDecl(
        self,
        node: qlast.SessionResetAliasDecl
    ) -> None:
        self._write_keywords('RESET ALIAS ')
        self.write(node.alias)

    def visit_StartTransaction(self, node: qlast.StartTransaction) -> None:
        self._write_keywords('START TRANSACTION')

        mods = []

        if node.isolation is not None:
            mods.append(f'ISOLATION {node.isolation.value}')

        if node.access is not None:
            mods.append(node.access.value)

        if node.deferrable is not None:
            mods.append(node.deferrable.value)

        if mods:
            self._write_keywords(' ' + ', '.join(mods))

    def visit_RollbackTransaction(
        self,
        node: qlast.RollbackTransaction
    ) -> None:
        self._write_keywords('ROLLBACK')

    def visit_CommitTransaction(self, node: qlast.CommitTransaction) -> None:
        self._write_keywords('COMMIT')

    def visit_DeclareSavepoint(self, node: qlast.DeclareSavepoint) -> None:
        self._write_keywords('DECLARE SAVEPOINT ')
        self.write(node.name)

    def visit_RollbackToSavepoint(
        self,
        node: qlast.RollbackToSavepoint
    ) -> None:
        self._write_keywords('ROLLBACK TO SAVEPOINT ')
        self.write(node.name)

    def visit_ReleaseSavepoint(self, node: qlast.ReleaseSavepoint) -> None:
        self._write_keywords('RELEASE SAVEPOINT ')
        self.write(node.name)

    def visit_DescribeStmt(self, node: qlast.DescribeStmt) -> None:
        self._write_keywords('DESCRIBE ')
        if isinstance(node.object, qlast.DescribeGlobal):
            self.write(node.object.to_edgeql())
        else:
            self.visit(node.object)
        if node.language:
            self._write_keywords(' AS ')
            self.write(node.language)
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
        self._write_keywords('module ')
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
        # Uppercase keywords for backwards compatibility with older migrations.
        uppercase: bool = False,
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
            sdlmode=sdlmode, descmode=descmode, uppercase=uppercase,
            unsorted=unsorted, limit_ref_classes=limit_ref_classes)


def _fix_parent_links(node: qlast.Base) -> qlast.Base:
    # NOTE: Do not use this legacy function in new code!
    # Using AST.parent is an anti-pattern. Instead write code
    # that uses singledispatch and maintains a proper context.

    node._parent = None  # type: ignore

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
