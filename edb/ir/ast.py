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

from .pathid import PathId, WeakNamespace
from .scopetree import InvalidScopeConfiguration, ScopeTreeNode  # noqa


def new_scope_tree():
    return ScopeTreeNode(fenced=True)


class Base(ast.AST):

    __ast_hidden__ = {'context'}

    context: parsing.ParserContext

    def __repr__(self):
        return (
            f'<ir.{self.__class__.__name__} at 0x{id(self):x}>'
        )


class ImmutableBase(ast.ImmutableASTMixin, Base):
    pass


class ViewShapeMetadata(Base):

    has_implicit_id: bool = False


class TypeRef(ImmutableBase):
    # The id of the referenced type
    id: uuid.UUID
    # The module id of the referenced type
    module_id: uuid.UUID
    # Full name of the type, not necessarily schema-addressable,
    # used for annotations only.
    name_hint: str
    # The ref of the underlying material type, if this is a view type,
    # else None.
    material_type: typing.Optional['TypeRef']
    # If this is a scalar type, base_type would be the highest
    # non-abstract base type.
    base_type: typing.Optional['TypeRef']
    # If this is a union type, this would be a set of
    # union elements.
    children: typing.FrozenSet['TypeRef']
    # If this is a union type, this would be the nearest common
    # ancestor of the union members.
    common_parent: typing.Optional['TypeRef']
    # If this node is an element of a collection, and the
    # collection elements are named, this would be then
    # name of the element.
    element_name: str
    # The kind of the collection type if this is a collection
    collection: str
    # Collection subtypes if this is a collection
    subtypes: typing.Tuple['TypeRef', ...]
    # True, if this describes a scalar type
    is_scalar: bool = False
    # True, if this describes a view
    is_view: bool = False
    # True, if this describes an abstract type
    is_abstract: bool = False


class AnyTypeRef(TypeRef):
    pass


class AnyTupleRef(TypeRef):
    pass


class BasePointerRef(ImmutableBase):

    # cardinality fields need to be mutable for lazy cardinality inference.
    __ast_mutable_fields__ = ('dir_cardinality', 'out_cardinality')

    name: sn.Name
    shortname: sn.Name
    dir_source: TypeRef
    dir_target: TypeRef
    out_source: TypeRef
    out_target: TypeRef
    direction: s_pointers.PointerDirection
    parent_ptr: 'BasePointerRef'
    material_ptr: 'BasePointerRef'
    derived_from_ptr: 'BasePointerRef'
    descendants: typing.Set['BasePointerRef']
    has_properties: bool
    required: bool
    # Relation cardinality in the direction specified
    # by *direction*.
    dir_cardinality: qltypes.Cardinality
    # Outbound cardinality of the pointer.
    out_cardinality: qltypes.Cardinality


class PointerRef(BasePointerRef):

    id: uuid.UUID
    module_id: uuid.UUID


class TupleIndirectionLink(s_pointers.PointerLike):
    """A Link-alike that can be used in tuple indirection path ids."""

    def __init__(self, element_name):
        self._name = sn.Name(module='__tuple__', name=str(element_name))

    def __hash__(self):
        return hash((self.__class__, self._name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._name == other._name

    def get_shortname(self, schema):
        return self._name

    def get_name(self, schema):
        return self._name

    def get_displayname(self, schema):
        return self._name

    def has_user_defined_properties(self, schema):
        return False

    def get_required(self, schema):
        return True

    def get_cardinality(self, schema):
        return qltypes.Cardinality.ONE

    def get_path_id_name(self, schema):
        return self._name

    def get_derived_from(self, schema):
        return None

    def is_link_property(self, schema):
        return False

    def generic(self, schema):
        return False

    def get_source(self, schema):
        return None

    def singular(self, schema,
                 direction=s_pointers.PointerDirection.Outbound) -> bool:
        return True

    def scalar(self):
        return self._target.is_scalar()

    def material_type(self, schema):
        return self

    def is_pure_computable(self, schema):
        return False

    def is_tuple_indirection(self):
        return True


class TupleIndirectionPointerRef(BasePointerRef):
    pass


class TypeIndirectionLink(s_pointers.PointerLike):
    """A Link-alike that can be used in type indirection path ids."""

    def __init__(self, source, target, *, optional, ancestral, cardinality):
        name = 'optindirection' if optional else 'indirection'
        self._name = sn.Name(module='__type__', name=name)
        self._source = source
        self._target = target
        self._cardinality = cardinality
        self._optional = optional
        self._ancestral = ancestral

    def get_name(self, schema):
        return self._name

    def get_shortname(self, schema):
        return self._name

    def get_displayname(self, schema):
        return self._name

    def has_user_defined_properties(self, schema):
        return False

    def get_required(self, schema):
        return True

    def get_cardinality(self, schema):
        return self._cardinality

    def get_path_id_name(self, schema):
        return self._name

    def get_derived_from(self, schema):
        return None

    def is_link_property(self, schema):
        return False

    def is_type_indirection(self):
        return True

    def is_optional(self):
        return self._optional

    def is_ancestral(self):
        return self._ancestral

    def generic(self, schema):
        return False

    def get_source(self, schema):
        return self._source

    def get_target(self, schema):
        return self._target

    def singular(self, schema,
                 direction=s_pointers.PointerDirection.Outbound) -> bool:
        if direction is s_pointers.PointerDirection.Outbound:
            return self.get_cardinality(schema) is qltypes.Cardinality.ONE
        else:
            return True

    def scalar(self):
        return self._target.is_scalar()

    def material_type(self, schema):
        return self

    def is_pure_computable(self, schema):
        return False


class TypeIndirectionPointerRef(BasePointerRef):

    optional: bool
    ancestral: bool


class Pointer(Base):

    source: Base
    target: Base
    ptrref: BasePointerRef
    direction: s_pointers.PointerDirection
    anchor: typing.Union[str, ast.MetaAST]
    show_as_anchor: typing.Union[str, ast.MetaAST]

    @property
    def is_inbound(self):
        return self.direction == s_pointers.PointerDirection.Inbound


class TypeIndirectionPointer(Pointer):

    optional: bool


class TypeIntrospection(ImmutableBase):

    typeref: TypeRef


class Set(Base):

    __ast_frozen_fields__ = ('typeref',)

    path_id: PathId
    path_scope_id: int
    typeref: TypeRef
    expr: Base
    rptr: Pointer
    anchor: typing.Union[str, ast.MetaAST]
    show_as_anchor: typing.Union[str, ast.MetaAST]
    shape: typing.List[Base]

    def __repr__(self):
        return f'<ir.Set \'{self.path_id}\' at 0x{id(self):x}>'


class EmptySet(Set):
    pass


class Command(Base):
    pass


class Statement(Command):

    expr: Set
    views: typing.Dict[sn.Name, s_types.Type]
    params: typing.Dict[str, s_types.Type]
    cardinality: qltypes.Cardinality
    stype: s_types.Type
    view_shapes: typing.Dict[so.Object, typing.List[s_pointers.Pointer]]
    view_shapes_metadata: typing.Dict[so.Object, ViewShapeMetadata]
    schema: s_schema.Schema
    scope_tree: ScopeTreeNode
    source_map: typing.Dict[s_pointers.Pointer,
                            typing.Tuple[qlast.Expr,
                                         compiler.ContextLevel,
                                         PathId,
                                         typing.Optional[WeakNamespace]]]


class Expr(ImmutableBase):
    pass


class ConstExpr(Expr):

    typeref: TypeRef


class BaseConstant(ConstExpr):

    value: str

    def __init__(self, *args, typeref, **kwargs):
        super().__init__(*args, typeref=typeref, **kwargs)
        if self.typeref is None:
            raise ValueError('cannot create irast.Constant without a type')
        if self.value is None:
            raise ValueError('cannot create irast.Constant without a value')


class StringConstant(BaseConstant):
    pass


class RawStringConstant(BaseConstant):
    pass


class IntegerConstant(BaseConstant):
    pass


class FloatConstant(BaseConstant):
    pass


class DecimalConstant(BaseConstant):
    pass


class BooleanConstant(BaseConstant):
    pass


class BytesConstant(BaseConstant):

    value: bytes


class ConstantSet(ConstExpr):

    elements: typing.Tuple[BaseConstant, ...]


class Parameter(Expr):

    name: str
    typeref: TypeRef


class TupleElement(ImmutableBase):

    name: str
    val: Set
    path_id: PathId


class Tuple(Expr):

    named: bool = False
    elements: typing.List[TupleElement]
    typeref: TypeRef


class Array(Expr):

    elements: typing.List[Base]
    typeref: TypeRef


class TypeCheckOp(Expr):

    left: Set
    right: typing.Union[TypeRef, Array]
    op: str
    result: typing.Optional[bool] = None


class SortExpr(Base):

    expr: Base
    direction: str
    nones_order: qlast.NonesOrder


class CallArg(ImmutableBase):
    """Call argument."""

    # cardinality fields need to be mutable for lazy cardinality inference.
    __ast_mutable_fields__ = ('cardinality',)

    expr: Base
    cardinality: qltypes.Cardinality = qltypes.Cardinality.ONE


class Call(Expr):
    """Operator or a function call."""

    # Bound callable has polymorphic parameters and
    # a polymorphic return type.
    func_polymorphic: bool

    # Bound callable's name.
    func_shortname: sn.Name

    # The id of the module in which the callable is defined.
    func_module_id: uuid.UUID

    # If the bound callable is a "FROM SQL" callable, this
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
    tuple_path_ids: typing.Optional[typing.List[PathId]]


class FunctionCall(Call):

    # initial value needed for aggregate function calls to correctly
    # handle empty set
    func_initial_value: Base

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


class TypeCast(Expr):
    """<Type>Expr"""

    expr: Base
    cast_module_id: uuid.UUID
    cast_name: str
    from_type: TypeRef
    to_type: TypeRef
    sql_function: str
    sql_cast: bool
    sql_expr: bool


class Stmt(Base):

    name: str
    result: Base
    cardinality: qltypes.Cardinality
    parent_stmt: Base
    iterator_stmt: Base


class FilteredStmt(Stmt):

    where: Base
    where_card: qltypes.Cardinality


class SelectStmt(FilteredStmt):

    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base
    implicit_wrapper: bool = False


class GroupStmt(Stmt):
    subject: Base
    groupby: typing.List[Base]
    result: SelectStmt
    group_path_id: PathId


class MutatingStmt(Stmt):
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

    name: str
    system: bool
    cardinality: qltypes.Cardinality
    requires_restart: bool
    backend_setting: str
    scope_tree: ScopeTreeNode


class ConfigSet(ConfigCommand):

    expr: Set


class ConfigFilter(Base):

    property_name: str
    value: Set


class ConfigReset(ConfigCommand):

    selector: typing.Optional[Set] = None


class ConfigInsert(ConfigCommand):

    expr: Set
