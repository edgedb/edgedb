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

from .pathid import PathId


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

    path_id: PathId
    real_path_id: PathId
    scls: so.NodeClass
    source: Base
    view_source: Base
    expr: Base
    rptr: Pointer
    anchor: str
    show_as_anchor: str
    shape: typing.List[Base]
    path_scope: typing.Dict[PathId, int]
    specific_path_scope: typing.Set[Base]

    def __repr__(self):
        return \
            f'<ir.Set \'{self.path_id or self.scls.name}\' at 0x{id(self):x}>'


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


class TupleElement(Base):

    name: str
    val: Base


class Tuple(Expr):
    named: bool = False
    elements: typing.List[TupleElement]


class Array(Expr):

    elements: typing.List[Base]


class Mapping(Expr):

    keys: typing.List[Base]
    values: typing.List[Base]


class SetOp(Expr):
    left: Base
    right: Base
    op: ast.ops.Operator
    exclusive: bool = False


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
    if_expr: Base  # noqa (pyflakes bug)
    else_expr: Base  # noqa (pyflakes bug)
    singleton: bool = False


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
    agg_set_modifier: qlast.SetModifier
    partition: typing.List[Base]
    window: bool
    initial_value: Base


class TupleIndirection(Expr):

    expr: Base
    name: str
    path_id: PathId


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

    result: Base
    singleton: bool
    main_stmt: Base
    parent_stmt: Base
    iterator_stmt: Base
    substmts: list
    path_scope: typing.Dict[PathId, int]
    specific_path_scope: typing.Set[Set]


class SelectStmt(Stmt):

    where: Base
    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base


class GroupStmt(Stmt):
    subject: Base
    groupby: typing.List[Base]
    result: SelectStmt
    group_path_id: PathId


class MutatingStmt(Stmt):
    subject: Set


class InsertStmt(MutatingStmt):
    pass


class UpdateStmt(MutatingStmt):

    where: Base


class DeleteStmt(MutatingStmt):

    where: Base


EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
EquivalenceOperator = qlast.EquivalenceOperator
SetOperator = qlast.SetOperator
SetModifier = qlast.SetModifier
