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


import typing

from edb.lang.common.exceptions import EdgeDBError
from edb.lang.common import ast, compiler, parsing

from edb.lang.schema import modules as s_modules
from edb.lang.schema import name as sn
from edb.lang.schema import objects as so
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast

from .pathid import PathId, WeakNamespace  # noqa
from .scopetree import InvalidScopeConfiguration, ScopeTreeNode  # noqa


def new_scope_tree():
    return ScopeTreeNode(fenced=True)


EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
EquivalenceOperator = qlast.EquivalenceOperator
SetOperator = qlast.SetOperator
SetModifier = qlast.SetModifier
SetQualifier = qlast.SetQualifier
Cardinality = qlast.Cardinality

UNION = qlast.UNION

EQUIVALENT = qlast.EQUIVALENT
NEQUIVALENT = qlast.NEQUIVALENT


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
    ptrcls: so.Object
    direction: s_pointers.PointerDirection
    anchor: typing.Union[str, ast.MetaAST]
    show_as_anchor: typing.Union[str, ast.MetaAST]

    @property
    def is_inbound(self):
        return self.direction == s_pointers.PointerDirection.Inbound


class Set(Base):

    path_id: PathId
    path_scope_id: int
    scls: s_types.Type
    source: Base
    view_source: Base
    expr: Base
    rptr: Pointer
    anchor: typing.Union[str, ast.MetaAST]
    show_as_anchor: typing.Union[str, ast.MetaAST]
    shape: typing.List[Base]

    def __repr__(self):
        return \
            f'<ir.Set \'{self.path_id or self.scls.name}\' at 0x{id(self):x}>'


class Command(Base):
    pass


class Statement(Command):

    expr: Set
    views: typing.Dict[sn.Name, s_types.Type]
    params: typing.Dict[str, s_types.Type]
    cardinality: Cardinality
    scope_tree: ScopeTreeNode
    scope_map: typing.Dict[Set, str]
    source_map: typing.Dict[s_pointers.Pointer,
                            typing.Tuple[qlast.Expr, compiler.ContextLevel]]


class Expr(Base):
    pass


class EmptySet(Set):
    pass


class Constant(Expr):

    value: object
    type: s_types.Type

    def __init__(self, *args, type, **kwargs):
        if type is None:
            raise ValueError('type argument must not be None')
        super().__init__(*args, type=type, **kwargs)


class Parameter(Base):

    name: str
    type: s_types.Type


class TupleElement(Base):

    name: str
    val: Base


class Tuple(Expr):
    named: bool = False
    elements: typing.List[TupleElement]


class Array(Expr):

    elements: typing.List[Base]


class SetOp(Expr):
    left: Set
    right: Set
    op: ast.ops.Operator
    exclusive: bool = False


class BaseBinOp(Expr):

    left: Base
    right: Base
    op: ast.ops.Operator


class BinOp(BaseBinOp):
    pass


class UnaryOp(Expr):

    expr: Base
    op: ast.ops.Operator


class ExistPred(Expr):

    expr: Set
    negated: bool = False


class DistinctOp(Expr):
    expr: Base


class EquivalenceOp(BaseBinOp):
    pass


class IfElseExpr(Expr):

    condition: Set
    if_expr: Set  # noqa (pyflakes bug)
    else_expr: Set  # noqa (pyflakes bug)


class Coalesce(Base):
    left: Set
    lcardinality: Cardinality = Cardinality.DEFAULT
    right: Set
    rcardinality: Cardinality = Cardinality.DEFAULT


class SortExpr(Base):

    expr: Base
    direction: str
    nones_order: qlast.NonesOrder


class FunctionCall(Expr):

    func: so.Object
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


class Stmt(Base):

    name: str
    result: Base
    cardinality: Cardinality = Cardinality.DEFAULT
    parent_stmt: Base
    iterator_stmt: Base


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


class SessionStateCmd(Command):

    modaliases: typing.Dict[typing.Optional[str], s_modules.Module]
