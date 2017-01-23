##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import ast, parsing
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as so
from edgedb.lang.edgeql import ast as qlast


class ASTError(EdgeDBError):
    pass


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


class Set(Base):

    path_id: list
    real_path_id: list
    scls: so.NodeClass
    sources: set
    expr: Base
    rptr: Pointer
    anchor: str
    show_as_anchor: str

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

    elements: typing.List[Base]
    scls: so.NodeClass
    set: Set
    rptr: Pointer


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

    path_id: list
    expr: Base
    type: TypeRef


class CompositeType(Base):

    node: so.Class
    pathspec: list


class Stmt(Base):

    substmts: list
    result: Base


class SelectStmt(Stmt):

    where: Base
    groupby: typing.List[Base]
    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base
    set_op: ast.ops.Operator
    set_op_larg: Stmt
    set_op_rarg: Stmt


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
