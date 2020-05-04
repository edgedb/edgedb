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


"""IR expression tree node definitions.

The IR expression tree is produced by the EdgeQL compiler
(see :mod:`edgeql.compiler`).  It is a self-contained representation
of an EdgeQL expression, which, together with the accompanying scope tree
(:mod:`ir.scopetree`) is sufficient to produce a backend query (e.g. SQL)
without any other input or context.

The most common part of the IR expression tree is the :class:`~Set` class.
Every expression is encoded as a ``Set`` instance that contains all common
metadata, such as the expression type, its symbolic identity (PathId) and
other useful bits.  The ``Set.expr`` field contains the specific node for
the expression.  The expression nodes usually refer to ``Set`` nodes
rather than other nodes directly.

For example, the EdgeQL expression ``SELECT str_lower('ABC') ++ 'd'``
yields the following IR (roughly):

Set (
  expr = SelectStmt (
    result = Set (
      expr = OperatorCall (
        args = [
          CallArg (
            expr = Set (
              expr = FunctionCall (
                args = [
                  CallArg (
                    expr = Set ( expr = StringConstant ( value = 'ABC' ) ),
                  ),
                  CallArg (
                    expr = Set ( expr = StringConstant ( value = 'd' ) ),
                  )
                ]
              )
            )
          )
        ]
      )
    )
  )
)
"""

from __future__ import annotations

import dataclasses
import typing
import uuid

from edb.common import ast, compiler, parsing

from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from .pathid import PathId, AnyNamespace, WeakNamespace  # noqa
from .scopetree import InvalidScopeConfiguration, ScopeTreeNode  # noqa


def new_scope_tree() -> ScopeTreeNode:
    return ScopeTreeNode(fenced=True)


class Base(ast.AST):
    __abstract_node__ = True
    __ast_hidden__ = {'context'}

    context: parsing.ParserContext

    def __repr__(self) -> str:
        return (
            f'<ir.{self.__class__.__name__} at 0x{id(self):x}>'
        )


class ImmutableBase(ast.ImmutableASTMixin, Base):
    __abstract_node__ = True


class ViewShapeMetadata(Base):

    has_implicit_id: bool = False


class TypeRef(ImmutableBase):
    # The id of the referenced type
    id: uuid.UUID
    # The module id of the referenced type
    module_id: uuid.UUID
    # Full name of the type, not necessarily schema-addressable,
    # used for annotations only.
    name_hint: sn.Name
    # The ref of the underlying material type, if this is a view type,
    # else None.
    material_type: typing.Optional[TypeRef]
    # If this is a scalar type, base_type would be the highest
    # non-abstract base type.
    base_type: typing.Optional[TypeRef]
    # If this is a union type, this would be a set of
    # union elements.
    union: typing.FrozenSet[TypeRef]
    # Whether the union is specified by an exhaustive list of
    # types, and type inheritance should not be considered.
    union_is_concrete: bool = False
    # If this is an intersection type, this would be a set of
    # intersection elements.
    intersection: typing.FrozenSet[TypeRef]
    # If this is a union type, this would be the nearest common
    # ancestor of the union members.
    common_parent: typing.Optional[TypeRef]
    # If this node is an element of a collection, and the
    # collection elements are named, this would be then
    # name of the element.
    element_name: str
    # The kind of the collection type if this is a collection
    collection: str
    # Collection subtypes if this is a collection
    subtypes: typing.Tuple[TypeRef, ...]
    # True, if this describes a scalar type
    is_scalar: bool = False
    # True, if this describes a view
    is_view: bool = False
    # True, if this describes an abstract type
    is_abstract: bool = False
    # True, if the collection type is persisted in the schema
    in_schema: bool = False
    # True, if this describes an opaque union type
    is_opaque_union: bool = False


class AnyTypeRef(TypeRef):
    pass


class AnyTupleRef(TypeRef):
    pass


class BasePointerRef(ImmutableBase):
    __abstract_node__ = True

    # cardinality fields need to be mutable for lazy cardinality inference.
    __ast_mutable_fields__ = frozenset(('dir_cardinality', 'out_cardinality'))

    name: sn.Name
    shortname: sn.Name
    path_id_name: typing.Optional[sn.Name]
    std_parent_name: sn.Name
    out_source: TypeRef
    out_target: TypeRef
    direction: s_pointers.PointerDirection
    source_ptr: PointerRef
    base_ptr: typing.Optional[BasePointerRef]
    material_ptr: BasePointerRef
    union_components: typing.Set[BasePointerRef]
    union_is_concrete: bool
    has_properties: bool
    is_derived: bool
    is_computable: bool
    # Relation cardinality in the direction specified
    # by *direction*.
    dir_cardinality: qltypes.Cardinality
    # Outbound cardinality of the pointer.
    out_cardinality: qltypes.Cardinality

    @property
    def dir_target(self) -> TypeRef:
        if self.direction is s_pointers.PointerDirection.Outbound:
            return self.out_target
        else:
            return self.out_source

    @property
    def required(self) -> bool:
        return self.out_cardinality.to_schema_value()[0]


class PointerRef(BasePointerRef):

    id: uuid.UUID
    module_id: uuid.UUID


class TupleIndirectionLink(s_pointers.PseudoPointer):
    """A Link-alike that can be used in tuple indirection path ids."""

    def __init__(
        self,
        source: so.Object,
        target: s_types.Type,
        *,
        element_name: str,
    ) -> None:
        self._source = source
        self._target = target
        self._name = sn.Name(module='__tuple__', name=str(element_name))

    def __hash__(self) -> int:
        return hash((self.__class__, self._name))

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self._name == other._name

    def get_name(self, schema: s_schema.Schema) -> str:
        return self._name

    def get_cardinality(
        self,
        schema: s_schema.Schema
    ) -> qltypes.SchemaCardinality:
        return qltypes.SchemaCardinality.ONE

    def singular(
        self,
        schema: s_schema.Schema,
        direction: s_pointers.PointerDirection =
            s_pointers.PointerDirection.Outbound
    ) -> bool:
        return True

    def scalar(self) -> bool:
        return self._target.is_scalar()

    def get_source(self, schema: s_schema.Schema) -> so.Object:
        return self._source

    def get_target(self, schema: s_schema.Schema) -> s_types.Type:
        return self._target

    def is_tuple_indirection(self) -> bool:
        return True

    def get_computable(self, schema: s_schema.Schema) -> bool:
        return False


class TupleIndirectionPointerRef(BasePointerRef):
    pass


class TypeIntersectionLink(s_pointers.PseudoPointer):
    """A Link-alike that can be used in type intersection path ids."""

    def __init__(
        self,
        source: so.Object,
        target: s_types.Type,
        *,
        optional: bool,
        is_empty: bool,
        is_subtype: bool,
        rptr_specialization: typing.Iterable[PointerRef] = (),
        cardinality: qltypes.SchemaCardinality,
    ) -> None:
        name = 'optindirection' if optional else 'indirection'
        self._name = sn.Name(module='__type__', name=name)
        self._source = source
        self._target = target
        self._cardinality = cardinality
        self._optional = optional
        self._is_empty = is_empty
        self._is_subtype = is_subtype
        self._rptr_specialization = frozenset(rptr_specialization)

    def get_name(self, schema: s_schema.Schema) -> sn.Name:
        return self._name

    def get_cardinality(
        self,
        schema: s_schema.Schema
    ) -> qltypes.SchemaCardinality:
        return self._cardinality

    def get_computable(self, schema: s_schema.Schema) -> bool:
        return False

    def is_type_intersection(self) -> bool:
        return True

    def is_optional(self) -> bool:
        return self._optional

    def is_empty(self) -> bool:
        return self._is_empty

    def is_subtype(self) -> bool:
        return self._is_subtype

    def get_rptr_specialization(self) -> typing.FrozenSet[PointerRef]:
        return self._rptr_specialization

    def get_source(self, schema: s_schema.Schema) -> so.Object:
        return self._source

    def get_target(self, schema: s_schema.Schema) -> s_types.Type:
        return self._target

    def singular(
        self,
        schema: s_schema.Schema,
        direction: s_pointers.PointerDirection =
            s_pointers.PointerDirection.Outbound
    ) -> bool:
        if direction is s_pointers.PointerDirection.Outbound:
            return (self.get_cardinality(schema) is
                    qltypes.SchemaCardinality.ONE)
        else:
            return True

    def scalar(self) -> bool:
        return self._target.is_scalar()


class TypeIntersectionPointerRef(BasePointerRef):

    optional: bool
    is_empty: bool
    is_subtype: bool
    rptr_specialization: typing.FrozenSet[PointerRef]


class Pointer(Base):

    source: Set
    target: Set
    ptrref: BasePointerRef
    direction: s_pointers.PointerDirection
    anchor: typing.Union[str, ast.MetaAST]
    show_as_anchor: typing.Union[str, ast.MetaAST]

    @property
    def is_inbound(self) -> bool:
        return self.direction == s_pointers.PointerDirection.Inbound


class TypeIntersectionPointer(Pointer):

    optional: bool
    ptrref: TypeIntersectionPointerRef


class TupleIndirectionPointer(Pointer):

    ptrref: TupleIndirectionPointerRef


class Expr(Base):
    __abstract_node__ = True


class ImmutableExpr(Expr, ImmutableBase):
    __abstract_node__ = True


class Set(Base):

    __ast_frozen_fields__ = frozenset({'typeref'})

    path_id: PathId
    path_scope_id: typing.Optional[int]
    typeref: TypeRef
    expr: Expr
    rptr: Pointer
    anchor: typing.Optional[str]
    show_as_anchor: typing.Optional[str]
    shape: typing.List[typing.Tuple[Set, qlast.ShapeOp]]

    def __repr__(self) -> str:
        return f'<ir.Set \'{self.path_id}\' at 0x{id(self):x}>'


class EmptySet(Set):
    pass


class Command(Base):
    __abstract_node__ = True


@dataclasses.dataclass(frozen=True)
class Param:
    """Query parameter with it's schema type and IR type"""

    name: str
    """Parameter name"""

    required: bool
    """Whether parameter is OPTIONAL or REQUIRED"""

    schema_type: s_types.Type
    """Schema type"""

    ir_type: TypeRef
    """IR type reference"""


class Statement(Command):

    expr: Set
    views: typing.Dict[sn.Name, s_types.Type]
    params: typing.List[Param]
    cardinality: qltypes.Cardinality
    volatility: qltypes.Volatility
    stype: s_types.Type
    view_shapes: typing.Dict[so.Object, typing.List[s_pointers.Pointer]]
    view_shapes_metadata: typing.Dict[so.Object, ViewShapeMetadata]
    schema: s_schema.Schema
    schema_refs: typing.FrozenSet[so.Object]
    new_coll_types: typing.FrozenSet[s_types.Collection]
    scope_tree: ScopeTreeNode
    source_map: typing.Dict[s_pointers.Pointer,
                            typing.Tuple[qlast.Expr,
                                         compiler.ContextLevel,
                                         PathId,
                                         typing.Optional[WeakNamespace]]]


class TypeIntrospection(ImmutableExpr):

    typeref: TypeRef


class ConstExpr(ImmutableExpr):
    __abstract_node__ = True
    typeref: TypeRef


class BaseConstant(ConstExpr):
    __abstract_node__ = True
    value: typing.Any

    def __init__(
        self,
        *args: typing.Any,
        typeref: TypeRef,
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(*args, typeref=typeref, **kwargs)
        if self.typeref is None:
            raise ValueError('cannot create irast.Constant without a type')
        if self.value is None:
            raise ValueError('cannot create irast.Constant without a value')


class BaseStrConstant(BaseConstant):
    __abstract_node__ = True
    value: str


class StringConstant(BaseStrConstant):
    pass


class IntegerConstant(BaseStrConstant):
    pass


class FloatConstant(BaseStrConstant):
    pass


class DecimalConstant(BaseStrConstant):
    pass


class BigintConstant(BaseStrConstant):
    pass


class BooleanConstant(BaseStrConstant):
    pass


class BytesConstant(BaseConstant):

    value: bytes


class ConstantSet(ConstExpr):

    elements: typing.Tuple[BaseConstant, ...]


class Parameter(ImmutableExpr):

    name: str
    required: bool
    typeref: TypeRef


class TupleElement(ImmutableBase):

    name: str
    val: Set
    path_id: PathId


class Tuple(ImmutableExpr):

    named: bool = False
    elements: typing.List[TupleElement]
    typeref: TypeRef


class Array(ImmutableExpr):

    elements: typing.List[Set]
    typeref: TypeRef


class TypeCheckOp(ImmutableExpr):

    left: Set
    right: TypeRef
    op: str
    result: typing.Optional[bool] = None


class SortExpr(Base):

    expr: Set
    direction: str
    nones_order: qlast.NonesOrder


class CallArg(ImmutableBase):
    """Call argument."""

    # cardinality fields need to be mutable for lazy cardinality inference.
    __ast_mutable_fields__ = frozenset(('cardinality',))

    expr: Set
    cardinality: qltypes.Cardinality = qltypes.Cardinality.ONE


class Call(ImmutableExpr):
    """Operator or a function call."""
    __abstract_node__ = True

    # Bound callable has polymorphic parameters and
    # a polymorphic return type.
    func_polymorphic: bool

    # Bound callable's name.
    func_shortname: sn.Name

    # The id of the module in which the callable is defined.
    func_module_id: uuid.UUID

    # If the bound callable is a "USING SQL" callable, this
    # attribute will be set to the name of the SQL function.
    func_sql_function: typing.Optional[str]

    # Whether the return value of the function should be
    # explicitly cast into the declared function return type.
    force_return_cast: bool

    # Bound arguments.
    args: typing.List[CallArg]

    # Typemods of parameters.  This list corresponds to ".args"
    # (so `zip(args, params_typemods)` is valid.)
    params_typemods: typing.List[qltypes.TypeModifier]

    # Return type and typemod.  In bodies of polymorphic functions
    # the return type can be polymorphic; in queries the return
    # type will be a concrete schema type.
    typeref: TypeRef
    typemod: qltypes.TypeModifier

    # If the return type is a tuple, this will contain a list
    # of tuple element path ids relative to the call set.
    tuple_path_ids: typing.List[PathId]

    # Volatility of the funciton or operator.
    volatility: qltypes.Volatility


class FunctionCall(Call):

    # initial value needed for aggregate function calls to correctly
    # handle empty set
    func_initial_value: Set

    # True if the bound function has a variadic parameter and
    # there are no arguments that are bound to it.
    has_empty_variadic: bool = False

    # The underlying SQL function has OUT parameters.
    sql_func_has_out_params: bool = False

    # Error to raise if the underlying SQL function returns NULL.
    error_on_null_result: typing.Optional[str] = None

    # Set to the type of the variadic parameter of the bound function
    # (or None, if the function has no variadic parameters.)
    variadic_param_type: typing.Optional[TypeRef] = None

    # True if function requires a session to be executed
    session_only: bool = False


class OperatorCall(Call):

    # The kind of the bound operator (INFIX, PREFIX, etc.).
    operator_kind: qltypes.OperatorKind

    # If this operator maps directly onto an SQL operator, this
    # will contain the operator name, and, optionally, backend
    # operand types.
    sql_operator: typing.Optional[typing.Tuple[str, ...]] = None

    # The name of the origin operator if this is a derivative operator.
    origin_name: sn.Name

    # The module id of the origin operator if this is a derivative operator.
    origin_module_id: uuid.UUID


class IndexIndirection(ImmutableExpr):

    expr: Base
    index: Base


class SliceIndirection(ImmutableExpr):

    expr: Base
    start: Base
    stop: Base
    step: Base


class TypeCast(ImmutableExpr):
    """<Type>ImmutableExpr"""

    expr: Set
    cast_module_id: uuid.UUID
    cast_name: str
    from_type: TypeRef
    to_type: TypeRef
    sql_function: str
    sql_cast: bool
    sql_expr: bool


class Stmt(Expr):
    __abstract_node__ = True
    name: str
    result: Set
    cardinality: qltypes.Cardinality
    parent_stmt: typing.Optional[Stmt]
    iterator_stmt: typing.Optional[Set]
    hoisted_iterators: typing.List[Set]


class FilteredStmt(Stmt):
    __abstract_node__ = True
    where: Set
    where_card: qltypes.Cardinality


class SelectStmt(FilteredStmt):

    orderby: typing.List[SortExpr]
    offset: typing.Optional[Set]
    limit: typing.Optional[Set]
    implicit_wrapper: bool = False


class GroupStmt(Stmt):
    subject: Set
    groupby: typing.List[Set]
    result: Set
    group_path_id: PathId


class MutatingStmt(Stmt):
    __abstract_node__ = True
    subject: Set


class InsertStmt(MutatingStmt):
    pass


class UpdateStmt(MutatingStmt, FilteredStmt):
    pass


class DeleteStmt(MutatingStmt, FilteredStmt):
    pass


class SessionStateCmd(Command):

    modaliases: typing.Dict[typing.Optional[str], s_mod.Module]
    testmode: bool


class ConfigCommand(Command):
    __abstract_node__ = True
    name: str
    system: bool
    cardinality: qltypes.SchemaCardinality
    requires_restart: bool
    backend_setting: str
    scope_tree: ScopeTreeNode


class ConfigSet(ConfigCommand):

    expr: Set


class ConfigReset(ConfigCommand):

    selector: typing.Optional[Set] = None


class ConfigInsert(ConfigCommand, Expr):

    expr: Set
