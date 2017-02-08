##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import ast, parsing

from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_src

from edgedb.lang.edgeql import ast as qlast


class ASTError(EdgeDBError):
    pass


class PathId(tuple):
    """Unique identifier of a path in an expression."""

    def rptr(self):
        if len(self) > 1:
            genptr = self[-2][0]
            direction = self[-2][1]
            if direction == s_pointers.PointerDirection.Outbound:
                src = self[-3]
            else:
                src = self[-1]

            if isinstance(src, s_src.Source):
                return src.pointers.get(genptr.name)
            else:
                return None
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[-2][1]
        else:
            return None

    def iter_prefixes(self):
        yield self.__class__(self[:1])

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self.__class__(self[:i + 2])
            else:
                break

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def extend(self, link, direction, target):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if not link.generic():
            link = link.bases[0]

        return self + ((link, direction), target)

    def __add__(self, other):
        return self.__class__(super().__add__(other))

    def __str__(self):
        if not self:
            return ''

        result = f'({self[0].name})'

        for i in range(1, len(self) - 1, 2):
            ptr = self[i][0]
            ptrdir = self[i][1]
            tgt = self[i + 1]

            if tgt:
                lexpr = f'({ptr.name} [IS {tgt.name}])'
            else:
                lexpr = f'({ptr.name})'

            if isinstance(ptr, s_lprops.LinkProperty):
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        return result

    __repr__ = __str__


class Base(ast.AST):

    __ast_hidden__ = {'context'}

    context: parsing.ParserContext

    def __repr__(self):
        return (
            f'<ir.{self.__class__.__name__} at 0x{id(self):x}>'
        )


class Pointer(Base):

    source: Base
    target: Base
    ptrcls: so.Class
    direction: str
    anchor: str
    show_as_anchor: str
    source_is_computed: bool


class Set(Base):

    path_id: PathId
    real_path_id: PathId
    scls: so.NodeClass
    sources: set
    expr: Base
    rptr: Pointer
    anchor: str
    show_as_anchor: str
    shape: Base

    def __repr__(self):
        return \
            f'<ir.Set \'{self.path_id or self.scls.name}\' at 0x{id(self):x}'


class Expr(Base):
    pass


class EmptySet(Base):
    pass


class Constant(Expr):

    value: object
    type: so.NodeClass

    def __init__(self, *args, type, **kwargs):
        if type is None:
            raise ValueError('type argument must not be None')
        super().__init__(*args, type=type, **kwargs)


class Parameter(Base):

    name: str
    type: so.NodeClass


class Shape(Base):

    set: Set
    scls: so.NodeClass
    rptr: Pointer
    elements: typing.List[Base]


class StructElement(Base):
    name: str
    val: Base


class Struct(Expr):
    elements: typing.List[StructElement]


class Sequence(Expr):

    elements: typing.List[Base]


class Array(Expr):

    elements: typing.List[Base]


class Mapping(Expr):

    keys: typing.List[Base]
    values: typing.List[Base]


class SetOp(Expr):
    left: Base
    right: Base
    op: ast.ops.Operator


class BinOp(Expr):

    left: Base
    right: Base
    op: ast.ops.Operator


class UnaryOp(Expr):

    expr: Base
    op: ast.ops.Operator


class ExistPred(Expr):

    expr: Base
    negated: bool = False


class IfElseExpr(Expr):

    condition: Base
    if_expr: Base
    else_expr: Base


class Coalesce(Base):
    args: typing.List[Base]


class SortExpr(Base):

    expr: Base
    direction: str
    nones_order: qlast.NonesOrder


class FunctionCall(Expr):

    func: so.Class
    args: typing.List[Base]
    kwargs: dict
    agg_sort: typing.List[SortExpr]
    agg_filter: Base
    partition: typing.List[Base]
    window: bool


class StructIndirection(Expr):

    expr: Base
    name: str


class IndexIndirection(Expr):

    expr: Base
    index: Base


class SliceIndirection(Expr):

    expr: Base
    start: Base
    stop: Base
    step: Base


class TypeRef(Expr):

    maintype: str
    subtypes: typing.List[sn.Name]


class TypeCast(Expr):
    """<Type>Expr"""

    expr: Base
    type: TypeRef


class TypeFilter(Expr):
    """Expr[IS Type]"""

    path_id: PathId
    expr: Base
    type: TypeRef


class CompositeType(Base):

    node: so.Class
    shape: list


class Stmt(Base):

    substmts: list
    result: Base


class SelectStmt(Stmt):

    where: Base
    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base


class GroupStmt(Stmt):
    subject: Base
    groupby: typing.List[Base]
    where: Base
    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base


class MutatingStmt(Stmt):

    shape: Shape


class InsertStmt(MutatingStmt):
    pass


class UpdateStmt(MutatingStmt):

    where: Base


class DeleteStmt(MutatingStmt):

    where: Base


TextSearchOperator = qlast.TextSearchOperator
EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
SetOperator = qlast.SetOperator
