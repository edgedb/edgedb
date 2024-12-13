# mypy: implicit-reexport

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

import abc
import dataclasses
import typing
import uuid

from edb.common import ast, compiler, span, markup, enum as s_enum

from edb import errors

from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from .pathid import PathId, Namespace  # noqa
from .scopetree import ScopeTreeNode  # noqa


Span = span.Span


def new_scope_tree() -> ScopeTreeNode:
    return ScopeTreeNode(fenced=True)


class Base(ast.AST):
    __abstract_node__ = True
    __ast_hidden__ = {'span'}

    span: typing.Optional[Span] = None

    def __repr__(self) -> str:
        return (
            f'<ir.{self.__class__.__name__} at 0x{id(self):x}>'
        )


# DEBUG: Probably don't actually keep this forever?
@markup.serializer.serializer.register(Base)
def _serialize_to_markup_base(ir: Base, *, ctx: typing.Any) -> typing.Any:
    node = ast.serialize_to_markup(ir, ctx=ctx)
    has_span = bool(ir.span)
    node.add_child(
        label='has_span', node=markup.serialize(has_span, ctx=ctx))
    child = node.children.pop()
    node.children.insert(1, child)
    return node


class ImmutableBase(ast.ImmutableASTMixin, Base):
    __abstract_node__ = True


class ViewShapeMetadata(Base):

    has_implicit_id: bool = False


class TypeRef(ImmutableBase):
    # Hide ancestors and children from debug spew because they are
    # incredibly noisy.
    __ast_hidden__ = {'ancestors', 'children'}

    # The id of the referenced type
    id: uuid.UUID
    # Full name of the type, not necessarily schema-addressable,
    # used for annotations only.
    name_hint: sn.Name
    # Name hint of the real underlying type, if the type ref was created
    # with an explicitly specified typename.
    orig_name_hint: typing.Optional[sn.Name] = None
    # The ref of the underlying material type, if this is a view type,
    # else None.
    material_type: typing.Optional[TypeRef] = None
    # If this is a scalar type, base_type would be the highest
    # non-abstract base type.
    base_type: typing.Optional[TypeRef] = None
    # A set of type children descriptors, if necessary for
    # this type description.
    children: typing.Optional[typing.FrozenSet[TypeRef]] = None
    # A set of type ancestor descriptors, if necessary for
    # this type description.
    ancestors: typing.Optional[typing.FrozenSet[TypeRef]] = None
    # If this is a compound type, this is a non-overlapping set of
    # constituent types.
    union: typing.Optional[typing.FrozenSet[TypeRef]] = None
    # Whether the union is specified by an exhaustive list of
    # types, and type inheritance should not be considered.
    union_is_exhaustive: bool = False
    # If this is a complex type, record the expression used to generate the
    # type. This is used later to get the correct rvar in `get_path_var`.
    expr_intersection: typing.Optional[typing.FrozenSet[TypeRef]] = None
    expr_union: typing.Optional[typing.FrozenSet[TypeRef]] = None
    # If this node is an element of a collection, and the
    # collection elements are named, this would be then
    # name of the element.
    element_name: typing.Optional[str] = None
    # The kind of the collection type if this is a collection
    collection: typing.Optional[str] = None
    # Collection subtypes if this is a collection
    subtypes: typing.Tuple[TypeRef, ...] = ()
    # True, if this describes a scalar type
    is_scalar: bool = False
    # True, if this describes a view
    is_view: bool = False
    # True, if this describes a cfg view
    is_cfg_view: bool = False
    # True, if this describes an abstract type
    is_abstract: bool = False
    # True, if the collection type is persisted in the schema
    in_schema: bool = False
    # True, if this describes an opaque union type
    is_opaque_union: bool = False
    # Does this need to call a custom json cast function
    needs_custom_json_cast: bool = False
    # If this has a schema-configured backend type, what is it
    sql_type: typing.Optional[str] = None
    # If this has a schema-configured custom sql serialization, what is it
    custom_sql_serialization: typing.Optional[str] = None

    def __repr__(self) -> str:
        return f'<ir.TypeRef \'{self.name_hint}\' at 0x{id(self):x}>'

    @property
    def real_material_type(self) -> TypeRef:
        return self.material_type or self

    @property
    def real_base_type(self) -> TypeRef:
        return self.base_type or self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class AnyTypeRef(TypeRef):
    pass


class AnyTupleRef(TypeRef):
    pass


class AnyObjectRef(TypeRef):
    pass


class BasePointerRef(ImmutableBase):
    __abstract_node__ = True

    # Hide children to reduce noise
    __ast_hidden__ = {'children'}

    # cardinality fields need to be mutable for lazy cardinality inference.
    # and children because we update pointers with newly derived children
    __ast_mutable_fields__ = frozenset(
        ('in_cardinality', 'out_cardinality', 'children',
         'is_computable')
    )

    # The defaults set here are mostly to try to reduce debug spew output.
    name: sn.QualName
    shortname: sn.QualName
    std_parent_name: typing.Optional[sn.QualName] = None
    out_source: TypeRef
    out_target: TypeRef
    source_ptr: typing.Optional[PointerRef] = None
    base_ptr: typing.Optional[BasePointerRef] = None
    material_ptr: typing.Optional[BasePointerRef] = None
    children: typing.FrozenSet[BasePointerRef] = frozenset()
    union_components: typing.Optional[typing.Set[BasePointerRef]] = None
    intersection_components: typing.Optional[typing.Set[BasePointerRef]] = None
    union_is_exhaustive: bool = False
    has_properties: bool = False
    is_derived: bool = False
    is_computable: bool = False
    # Outbound cardinality of the pointer.
    out_cardinality: qltypes.Cardinality
    # Inbound cardinality of the pointer.
    in_cardinality: qltypes.Cardinality = qltypes.Cardinality.MANY
    defined_here: bool = False
    computed_link_alias: typing.Optional[BasePointerRef] = None
    computed_link_alias_is_backward: typing.Optional[bool] = None

    def dir_target(self, direction: s_pointers.PointerDirection) -> TypeRef:
        if direction is s_pointers.PointerDirection.Outbound:
            return self.out_target
        else:
            return self.out_source

    def dir_source(self, direction: s_pointers.PointerDirection) -> TypeRef:
        if direction is s_pointers.PointerDirection.Outbound:
            return self.out_source
        else:
            return self.out_target

    def dir_cardinality(
        self, direction: s_pointers.PointerDirection
    ) -> qltypes.Cardinality:
        if direction is s_pointers.PointerDirection.Outbound:
            return self.out_cardinality
        else:
            return self.in_cardinality

    @property
    def required(self) -> bool:
        return self.out_cardinality.to_schema_value()[0]

    def descendants(self) -> typing.Set[BasePointerRef]:
        res = set(self.children)
        for child in self.children:
            res.update(child.descendants())
        return res

    @property
    def real_material_ptr(self) -> BasePointerRef:
        return self.material_ptr or self

    @property
    def real_base_ptr(self) -> BasePointerRef:
        return self.base_ptr or self

    def __repr__(self) -> str:
        return f'<ir.{type(self).__name__} \'{self.name}\' at 0x{id(self):x}>'


class PointerRef(BasePointerRef):
    id: uuid.UUID


class ConstraintRef(ImmutableBase):
    # The id of the constraint
    id: uuid.UUID


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
        self._name = sn.QualName(
            module='__tuple__', name=str(element_name))

    def __hash__(self) -> int:
        return hash((self.__class__, self._source, self._name))

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self._source == other._source and self._name == other._name

    def get_name(self, schema: s_schema.Schema) -> sn.QualName:
        return self._name

    def get_cardinality(
        self, schema: s_schema.Schema
    ) -> qltypes.SchemaCardinality:
        return qltypes.SchemaCardinality.One

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


class SpecialPointerRef(BasePointerRef):
    """Pointer ref used for internal columns, such as __fts_document__"""
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
        self._name = sn.QualName(module='__type__', name=name)
        self._source = source
        self._target = target
        self._cardinality = cardinality
        self._optional = optional
        self._is_empty = is_empty
        self._is_subtype = is_subtype
        self._rptr_specialization = frozenset(rptr_specialization)

    def get_name(self, schema: s_schema.Schema) -> sn.QualName:
        return self._name

    def get_cardinality(
        self, schema: s_schema.Schema
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
                    qltypes.SchemaCardinality.One)
        else:
            return True

    def scalar(self) -> bool:
        return self._target.is_scalar()


class TypeIntersectionPointerRef(BasePointerRef):

    optional: bool
    is_empty: bool
    is_subtype: bool
    rptr_specialization: typing.FrozenSet[PointerRef]


class Expr(Base):
    __abstract_node__ = True

    if typing.TYPE_CHECKING:
        @property
        @abc.abstractmethod
        def typeref(self) -> TypeRef:
            raise NotImplementedError

    # Sets to materialize at this point, keyed by the type/ptr id.
    materialized_sets: typing.Optional[
        typing.Dict[uuid.UUID, MaterializedSet]] = None


class Pointer(Expr):

    source: Set
    ptrref: BasePointerRef
    direction: s_pointers.PointerDirection

    # If the pointer is a computed pointer (or a computed pointer
    # definition), the expression.
    expr: typing.Optional[Expr] = None

    is_definition: bool
    # Set when we have placed an rptr to help route link properties
    # but it is not a genuine pointer use.
    is_phony: bool = False
    anchor: typing.Optional[str] = None
    show_as_anchor: typing.Optional[str] = None

    @property
    def is_inbound(self) -> bool:
        return self.direction == s_pointers.PointerDirection.Inbound

    @property
    def dir_cardinality(self) -> qltypes.Cardinality:
        return self.ptrref.dir_cardinality(self.direction)

    @property
    def typeref(self) -> TypeRef:
        return self.ptrref.dir_target(self.direction)


class TypeIntersectionPointer(Pointer):

    optional: bool
    ptrref: TypeIntersectionPointerRef
    is_definition: bool = False


class TupleIndirectionPointer(Pointer):

    ptrref: TupleIndirectionPointerRef
    is_definition: bool = False


class ImmutableExpr(Expr, ImmutableBase):
    __abstract_node__ = True


class BindingKind(s_enum.StrEnum):
    With = 'With'
    For = 'For'
    Select = 'Select'
    Schema = 'Schema'


class TypeRoot(Expr):
    # This will be replicated in the enclosing set.
    typeref: TypeRef

    # Whether this is a reference to a global that is cached in a
    # materialized CTE in the query.
    is_cached_global: bool = False

    # Whether to force this to not select subtypes
    skip_subtypes: bool = False


class RefExpr(Expr):
    '''Different expressions sorts that refer to some kind of binding.'''
    __abstract_node__ = True
    typeref: TypeRef


class MaterializedExpr(RefExpr):
    pass


class VisibleBindingExpr(RefExpr):
    pass


class InlinedParameterExpr(RefExpr):
    required: bool
    is_global: bool


T_expr_co = typing.TypeVar('T_expr_co', covariant=True, bound=Expr)


# SetE is the base 'Set' type, and it is parameterized over what kind
# of expression it holds. Most code uses the Set alias below, which
# instantiates it with Expr.
# irutils.is_set_instance can be used to refine the type.
class SetE(Base, typing.Generic[T_expr_co]):
    '''A somewhat overloaded metadata container for expressions.

    Its primary purpose is to be the holder for expression metadata
    such as path_id.

    It *also* contains shape applications.
    '''

    __ast_frozen_fields__ = frozenset({'typeref'})

    # N.B: Make sure to add new fields to setgen.new_set_from_set!

    path_id: PathId
    path_scope_id: typing.Optional[int] = None
    typeref: TypeRef
    expr: T_expr_co
    shape: typing.Tuple[typing.Tuple[SetE[Pointer], qlast.ShapeOp], ...] = ()

    anchor: typing.Optional[str] = None
    show_as_anchor: typing.Optional[str] = None
    # A pointer to a set nested within this one has a shape and the same
    # typeref, if such a set exists.
    shape_source: typing.Optional[Set] = None
    is_binding: typing.Optional[BindingKind] = None
    is_schema_alias: bool = False

    is_materialized_ref: bool = False
    # A ref to a visible binding (like a for iterator variable) should
    # never need to be compiled--it should always be found. We set a
    # flag instead of clearing expr because clearing expr can mess up
    # card/multi inference.
    is_visible_binding_ref: bool = False

    # Whether to force this to ignore rewrites. Very dangerous!
    # Currently for preventing duplicate explicit .id
    # insertions to BaseObject and for ignoring other access policies
    # inside access policy expressions.
    #
    # N.B: This is defined on Set and not on TypeRoot because we use the Set
    # to join against target types on links, and to ensure rvars.
    ignore_rewrites: bool = False

    def __repr__(self) -> str:
        return f'<ir.Set \'{self.path_id}\' at 0x{id(self):x}>'

# We set its name to Set because that's what we want visitors to use.


SetE.__name__ = 'Set'

if typing.TYPE_CHECKING:
    Set = SetE[Expr]
else:
    Set = SetE


DUMMY_SET = Set()  # type: ignore[call-arg]


class Command(Base):
    __abstract_node__ = True


@dataclasses.dataclass(frozen=True, kw_only=True)
class Param:
    """Query parameter with its schema type and IR type"""

    name: str
    """Parameter name"""

    required: bool
    """Whether parameter is OPTIONAL or REQUIRED"""

    schema_type: s_types.Type
    """Schema type"""

    ir_type: TypeRef
    """IR type reference"""

    sub_params: SubParams | None = None
    """Sub-parameters containing tuple components.

    If the param needs to be split into multiple real postgres params
    in order to implement tuples, this collects those parameters and
    the decoder expression.
    """

    @property
    def is_sub_param(self) -> bool:
        return (
            self.name.startswith('__edb_decoded_') and self.name.endswith('__')
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class SubParams:
    """Information about sub-parameters needed for tuple components.

    If the param needs to be split into multiple real postgres params
    in order to implement tuples, this collects those parameters and
    the decoder expression.
    """
    trans_type: ParamTransType
    decoder_edgeql: qlast.Expr
    params: tuple[Param, ...]
    decoder_ir: Set | None = None


@dataclasses.dataclass(eq=False)
class ParamTransType:
    """Representation of how a tuple-containing parameter type is broken down.

    The key thing here is that each node contains the index corresponding
    to which sub-parameter that node in the argument type corresponds with.
    See edgeql.compiler.tuple_args for details.

    The reason we track this in a separate data structure (instead of just
    having an dict from TypeRefs to indexes, say) is that TypeRefs will often
    be shared among identical types, but we need to track different indexes
    for different components of a type.
    (For example, if we have an param type `tuple<str, str>`, this gets
    decomposed into two `str` params, with indexes 0 and 1.
    """
    typeref: TypeRef
    idx: int

    def flatten(self) -> tuple[typing.Any, ...]:
        """Flatten out the trans type into a tuple representation.

        The idea here is to produce something that our inner loop in cython
        can consume efficiently.
        """
        raise NotImplementedError


@dataclasses.dataclass(eq=False)
class ParamScalar(ParamTransType):
    def flatten(self) -> tuple[typing.Any, ...]:
        return (int(qltypes.TypeTag.SCALAR), self.idx)


@dataclasses.dataclass(eq=False)
class ParamTuple(ParamTransType):
    typs: tuple[tuple[typing.Optional[str], ParamTransType], ...]

    def flatten(self) -> tuple[typing.Any, ...]:
        return (
            (int(qltypes.TypeTag.TUPLE), self.idx)
            + tuple(x.flatten() for _, x in self.typs)
        )


@dataclasses.dataclass(eq=False)
class ParamArray(ParamTransType):
    typ: ParamTransType

    def flatten(self) -> tuple[typing.Any, ...]:
        return (int(qltypes.TypeTag.ARRAY), self.idx, self.typ.flatten())


@dataclasses.dataclass(frozen=True)
class Global(Param):
    global_name: sn.QualName
    """The name of the global"""

    has_present_arg: bool
    """Whether this global needs a companion parameter indicating whether
    the global is present.

    This is needed when a global has a default but also is optional,
    and so we need to distinguish "unset" and "set to {}".
    """


@dataclasses.dataclass(frozen=True)
class ScriptInfo:
    """Result of preprocessing a script of multiple statements"""

    params: typing.Dict[str, Param]
    """All parameters in all statements in the script"""

    schema: s_schema.Schema
    """The schema after preprocessing. (Collections may have been created.)"""


class MaterializeVolatile(Base):
    pass


class MaterializeVisible(Base):
    __ast_hidden__ = {'sets'}
    sets: typing.Set[typing.Tuple[PathId, Set]]
    path_scope_id: int


@markup.serializer.serializer.register(MaterializeVisible)
def _serialize_to_markup_mat_vis(
    ir: MaterializeVisible, *, ctx: typing.Any
) -> typing.Any:
    # We want to show the path_ids but *not* to show the full sets
    node = ast.serialize_to_markup(ir, ctx=ctx)
    fixed = {(x, y.path_id) for x, y in ir.sets}
    node.add_child(label='uses', node=markup.serialize(fixed, ctx=ctx))
    return node


MaterializeReason = typing.Union[MaterializeVolatile, MaterializeVisible]


class ComputableInfo(typing.NamedTuple):

    qlexpr: qlast.Expr
    irexpr: typing.Optional[typing.Union[Set, Expr]]
    context: compiler.ContextLevel
    path_id: PathId
    path_id_ns: typing.Optional[Namespace]
    shape_op: qlast.ShapeOp
    should_materialize: typing.Sequence[MaterializeReason]


class Statement(Command):

    expr: Set
    views: typing.Dict[sn.Name, s_types.Type]
    params: typing.List[Param]
    globals: typing.List[Global]
    cardinality: qltypes.Cardinality
    volatility: qltypes.Volatility
    multiplicity: qltypes.Multiplicity
    stype: s_types.Type
    view_shapes: typing.Dict[so.Object, typing.List[s_pointers.Pointer]]
    view_shapes_metadata: typing.Dict[s_types.Type, ViewShapeMetadata]
    schema: s_schema.Schema
    schema_refs: typing.FrozenSet[so.Object]
    schema_ref_exprs: typing.Optional[
        typing.Dict[so.Object, typing.Set[qlast.Base]]]
    scope_tree: ScopeTreeNode
    dml_exprs: typing.List[qlast.Base]
    type_rewrites: typing.Dict[typing.Tuple[uuid.UUID, bool], Set]
    singletons: typing.List[PathId]
    triggers: tuple[tuple[Trigger, ...], ...]
    warnings: tuple[errors.EdgeDBError, ...]


class TypeIntrospection(ImmutableExpr):

    # The type value to return
    output_typeref: TypeRef
    # The type value *of the output*
    typeref: TypeRef


class ConstExpr(Expr):
    __abstract_node__ = True
    typeref: TypeRef


class EmptySet(ConstExpr):
    pass


class BaseConstant(ConstExpr, ImmutableExpr):
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

    def _init_copy(self) -> BaseConstant:
        return self.__class__(typeref=self.typeref, value=self.value)


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


class ConstantSet(ConstExpr, ImmutableExpr):

    elements: typing.Tuple[BaseConstant | Parameter, ...]


class Parameter(ImmutableExpr):

    name: str
    required: bool
    typeref: TypeRef
    # None means not a global. Otherwise, whether this is an implicitly
    # created global for a function call.
    is_implicit_global: typing.Optional[bool] = None

    @property
    def is_global(self) -> bool:
        return self.is_implicit_global is not None


class TupleElement(ImmutableBase):

    name: str
    val: Set
    path_id: typing.Optional[PathId] = None


class Tuple(ImmutableExpr):

    named: bool = False
    elements: typing.List[TupleElement]
    typeref: TypeRef


class Array(ImmutableExpr):

    elements: typing.Sequence[Set]
    typeref: TypeRef


class TypeCheckOp(ImmutableExpr):

    left: Set
    right: TypeRef
    op: str
    result: typing.Optional[bool] = None
    typeref: TypeRef


class SortExpr(Base):

    expr: Set
    direction: typing.Optional[qlast.SortOrder]
    nones_order: typing.Optional[qlast.NonesOrder]


class CallArg(ImmutableBase):
    """Call argument."""

    # cardinality fields need to be mutable for lazy cardinality inference.
    __ast_mutable_fields__ = frozenset(('cardinality', 'multiplicity'))

    expr: Set
    """PathId for the __type__ link of object type arguments."""
    expr_type_path_id: typing.Optional[PathId] = None
    cardinality: qltypes.Cardinality = qltypes.Cardinality.UNKNOWN
    multiplicity: qltypes.Multiplicity = qltypes.Multiplicity.UNKNOWN
    is_default: bool = False
    param_typemod: qltypes.TypeModifier


class Call(ImmutableExpr):
    """Operator or a function call."""
    __abstract_node__ = True

    # Bound callable has polymorphic parameters and
    # a polymorphic return type.
    func_polymorphic: bool

    # Bound callable's name.
    func_shortname: sn.QualName

    # Whether the bound callable is a "USING SQL EXPRESSION" callable.
    func_sql_expr: bool = False

    # Whether the return value of the function should be
    # explicitly cast into the declared function return type.
    force_return_cast: bool

    # Bound arguments.
    # Named arguments are indexed by argument name.
    # Positional arguments are indexed by argument position.
    args: typing.Dict[typing.Union[int, str], CallArg]

    # Return type and typemod.  In bodies of polymorphic functions
    # the return type can be polymorphic; in queries the return
    # type will be a concrete schema type.
    typeref: TypeRef
    typemod: qltypes.TypeModifier

    # If the return type is a tuple, this will contain a list
    # of tuple element path ids relative to the call set.
    tuple_path_ids: typing.List[PathId]

    # Volatility of the function or operator.
    volatility: qltypes.Volatility

    # Whether the underlying implementation is strict in all its required
    # arguments (NULL inputs lead to NULL results). If not, we need to
    # filter at the call site.
    impl_is_strict: bool = False

    # Kind of a hack: indicates that when possible we should pass arguments
    # to this function as a subquery-as-an-expression.
    # See comment in schema/functions.py for more discussion.
    prefer_subquery_args: bool = False

    # If this is a set of call but is allowed in singleton expressions.
    is_singleton_set_of: typing.Optional[bool] = None


class FunctionCall(Call):

    __ast_mutable_fields__ = frozenset((
        'extras', 'body'
    ))

    # If the bound callable is a "USING SQL" callable, this
    # attribute will be set to the name of the SQL function.
    func_sql_function: typing.Optional[str]

    # initial value needed for aggregate function calls to correctly
    # handle empty set
    func_initial_value: typing.Optional[Set] = None

    # True if the bound function has a variadic parameter and
    # there are no arguments that are bound to it.
    has_empty_variadic: bool = False

    # The underlying SQL function has OUT parameters.
    sql_func_has_out_params: bool = False

    # backend_name for the underlying function
    backend_name: typing.Optional[uuid.UUID] = None

    # Error to raise if the underlying SQL function returns NULL.
    error_on_null_result: typing.Optional[str] = None

    # Whether the generic function preserves optionality of the generic
    # argument(s).
    preserves_optionality: bool = False

    # Whether the generic function preserves upper cardinality of the generic
    # argument(s).
    preserves_upper_cardinality: bool = False

    # Set to the type of the variadic parameter of the bound function
    # (or None, if the function has no variadic parameters.)
    variadic_param_type: typing.Optional[TypeRef] = None

    # Additional arguments representing global variables
    global_args: typing.Optional[typing.List[Set]] = None

    # Any extra information useful for compilation of special-case callables.
    extras: typing.Optional[dict[str, typing.Any]] = None

    # Inline body of the callable.
    body: typing.Optional[Set] = None


class OperatorCall(Call):

    # The kind of the bound operator (INFIX, PREFIX, etc.).
    operator_kind: qltypes.OperatorKind

    # If the bound callable is a "USING SQL FUNCTION" callable, this
    # attribute will be set to the name of the SQL function.
    sql_function: typing.Optional[typing.Tuple[str, ...]] = None

    # If this operator maps directly onto an SQL operator, this
    # will contain the operator name, and, optionally, backend
    # operand types.
    sql_operator: typing.Optional[typing.Tuple[str, ...]] = None

    # The name of the origin operator if this is a derivative operator.
    origin_name: typing.Optional[sn.QualName] = None

    # The module id of the origin operator if this is a derivative operator.
    origin_module_id: typing.Optional[uuid.UUID] = None


class IndexIndirection(ImmutableExpr):

    expr: Base
    index: Base
    typeref: TypeRef


class SliceIndirection(ImmutableExpr):

    expr: Set
    start: typing.Optional[Base]
    stop: typing.Optional[Base]
    typeref: TypeRef


class TypeCast(ImmutableExpr):
    """<Type>ImmutableExpr"""

    expr: Set
    cast_name: typing.Optional[sn.QualName] = None
    from_type: TypeRef
    to_type: TypeRef
    cardinality_mod: typing.Optional[qlast.CardinalityModifier] = None
    sql_function: typing.Optional[str] = None
    sql_cast: bool
    sql_expr: bool
    error_message_context: typing.Optional[str] = None

    @property
    def typeref(self) -> TypeRef:
        return self.to_type


class MaterializedSet(Base):
    # Hide uses to reduce spew; we produce our own simpler uses
    __ast_hidden__ = {'use_sets'}
    materialized: Set
    reason: typing.Sequence[MaterializeReason]

    # We really only want the *paths* of all the places it is used,
    # but we need to store the sets to take advantage of weak
    # namespace rewriting.
    use_sets: typing.List[Set]
    cardinality: qltypes.Cardinality = qltypes.Cardinality.UNKNOWN

    # Whether this has been "finalized" by stmtctx; just for supporting some
    # assertions
    finalized: bool = False

    @property
    def uses(self) -> typing.List[PathId]:
        return [x.path_id for x in self.use_sets]


@markup.serializer.serializer.register(MaterializedSet)
def _serialize_to_markup_mat_set(
    ir: MaterializedSet, *, ctx: typing.Any
) -> typing.Any:
    # We want to show the path_ids but *not* to show the full uses
    node = ast.serialize_to_markup(ir, ctx=ctx)
    node.add_child(label='uses', node=markup.serialize(ir.uses, ctx=ctx))
    return node


class Stmt(Expr):
    __abstract_node__ = True
    # Hide parent_stmt to reduce debug spew and to hide it from find_children
    __ast_hidden__ = {'parent_stmt'}

    name: typing.Optional[str] = None
    # Parts of the edgeql->IR compiler need to create statements and fill in
    # the result later, but making it Optional would cause lots of errors,
    # so we stick a dummy set set in.
    result: Set = DUMMY_SET
    parent_stmt: typing.Optional[Stmt] = None
    iterator_stmt: typing.Optional[Set] = None
    bindings: typing.Optional[list[tuple[Set, qltypes.Volatility]]] = None

    @property
    def typeref(self) -> TypeRef:
        return self.result.typeref


class FilteredStmt(Stmt):
    __abstract_node__ = True
    where: typing.Optional[Set] = None
    where_card: qltypes.Cardinality = qltypes.Cardinality.UNKNOWN


class SelectStmt(FilteredStmt):

    orderby: typing.Optional[typing.List[SortExpr]] = None
    offset: typing.Optional[Set] = None
    limit: typing.Optional[Set] = None
    implicit_wrapper: bool = False

    # An expression to use instead of this one for the purpose of
    # cardinality/multiplicity inference. This is used for when something
    # is desugared in a way that doesn't preserve cardinality, but we
    # need to anyway.
    card_inference_override: typing.Optional[Set] = None


class GroupStmt(FilteredStmt):
    subject: Set = DUMMY_SET
    using: typing.Dict[str, typing.Tuple[Set, qltypes.Cardinality]] = (
        ast.field(factory=dict))
    by: typing.List[qlast.GroupingElement]
    result: Set = DUMMY_SET
    group_binding: Set = DUMMY_SET
    grouping_binding: typing.Optional[Set] = None
    orderby: typing.Optional[typing.List[SortExpr]] = None
    # Optimization information
    group_aggregate_sets: typing.Dict[
        typing.Optional[Set], typing.FrozenSet[PathId]
    ] = ast.field(factory=dict)


class MutatingLikeStmt(Expr):
    """Represents statements that are "like" mutations for certain purposes.

    In particular, it includes both MutatingStmt, representing actual
    mutations, and TriggerAnchor, which is a way to signal that
    something should (or should not) see certain mutation overlays in
    the backend without being an actual mutation.
    """
    __abstract_node__ = True


class TriggerAnchor(MutatingLikeStmt):

    """A placeholder to be put in trigger __old__ nodes.

    The idea here is that in the backend, it will be treated as if it
    was a MutatingStmt for the purposes of determining whether to use
    overlays.
    """
    typeref: TypeRef


class MutatingStmt(Stmt, MutatingLikeStmt):
    __abstract_node__ = True
    # Parts of the edgeql->IR compiler need to create statements and fill in
    # the subject later, but making it Optional would cause lots of errors,
    # so we stick a dummy set in.
    subject: Set = DUMMY_SET
    # Conflict checks that we should manually raise constraint violations
    # for.
    conflict_checks: typing.Optional[typing.List[OnConflictClause]] = None
    # Access policy checks that we should raise errors on
    write_policies: typing.Dict[uuid.UUID, WritePolicies] = ast.field(
        factory=dict
    )
    # Access policy checks that we should filter on
    read_policies: typing.Dict[uuid.UUID, ReadPolicyExpr] = ast.field(
        factory=dict
    )

    # Rewrites of the subject shape
    rewrites: typing.Optional[Rewrites] = None

    @property
    def material_type(self) -> TypeRef:
        """The proper material type being operated on.

        This should have all views stripped out.
        """
        raise NotImplementedError


class ReadPolicyExpr(Base):
    expr: Set
    cardinality: qltypes.Cardinality = qltypes.Cardinality.UNKNOWN


class WritePolicies(Base):
    policies: typing.List[WritePolicy]


class WritePolicy(Base):
    expr: Set
    action: qltypes.AccessPolicyAction
    name: str
    error_msg: typing.Optional[str]

    cardinality: qltypes.Cardinality = qltypes.Cardinality.UNKNOWN


class Trigger(Base):
    expr: Set
    # All the relevant dml
    affected: set[tuple[TypeRef, MutatingStmt]]
    all_affected_types: set[TypeRef]
    source_type: TypeRef
    kinds: set[qltypes.TriggerKind]
    scope: qltypes.TriggerScope

    # N.B: Semantically and in the external language, delete triggers
    # don't have a __new__ set, but we give it one in the
    # implementation (identical to the old set), to help make the
    # implementation more uniform.
    new_set: Set
    old_set: typing.Optional[Set]


class OnConflictClause(Base):
    constraint: typing.Optional[ConstraintRef]
    select_ir: Set
    always_check: bool
    else_ir: typing.Optional[Set]
    update_query_set: typing.Optional[Set] = None
    else_fail: typing.Optional[MutatingStmt] = None


class InsertStmt(MutatingStmt):
    on_conflict: typing.Optional[OnConflictClause] = None
    final_typeref: typing.Optional[TypeRef] = None

    @property
    def material_type(self) -> TypeRef:
        return self.subject.typeref.real_material_type

    @property
    def typeref(self) -> TypeRef:
        return self.final_typeref or self.result.typeref


# N.B: The PointerRef corresponds to the *definition* point of the rewrite.
RewritesOfType = typing.Dict[str, typing.Tuple[SetE[Pointer], BasePointerRef]]


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class Rewrites:
    old_path_id: typing.Optional[PathId]

    by_type: typing.Dict[TypeRef, RewritesOfType]


class UpdateStmt(MutatingStmt, FilteredStmt):
    _material_type: TypeRef | None = None

    @property
    def material_type(self) -> TypeRef:
        assert self._material_type
        return self._material_type

    sql_mode_link_only: bool = False


class DeleteStmt(MutatingStmt, FilteredStmt):
    _material_type: TypeRef | None = None

    links_to_delete: typing.Dict[
        uuid.UUID,
        typing.Tuple[PointerRef, ...]
    ] = ast.field(factory=dict)

    @property
    def material_type(self) -> TypeRef:
        assert self._material_type
        return self._material_type


class SessionStateCmd(Command):

    modaliases: typing.Dict[typing.Optional[str], s_mod.Module]
    testmode: bool


class ConfigCommand(Command, Expr):
    __abstract_node__ = True
    name: str
    scope: qltypes.ConfigScope
    cardinality: qltypes.SchemaCardinality
    requires_restart: bool
    backend_setting: typing.Optional[str]
    is_system_config: bool
    globals: typing.Optional[typing.List[Global]] = None
    scope_tree: typing.Optional[ScopeTreeNode] = None

    params: typing.List[Param] = ast.field(factory=list)
    schema: typing.Optional[s_schema.Schema] = None


class ConfigSet(ConfigCommand):

    expr: Set
    required: bool
    backend_expr: typing.Optional[Set] = None

    @property
    def typeref(self) -> TypeRef:
        return self.expr.typeref


class ConfigReset(ConfigCommand):

    selector: typing.Optional[Set] = None

    @property
    def typeref(self) -> TypeRef:
        return TypeRef(
            id=so.get_known_type_id('anytype'),
            name_hint=sn.UnqualName('anytype'),
        )


class ConfigInsert(ConfigCommand):

    expr: Set

    @property
    def typeref(self) -> TypeRef:
        return self.expr.typeref


class FTSDocument(ImmutableExpr):
    """
    Text and information on how to search through it.

    Constructed with `std::fts::with_options`.
    """

    text: Set

    language: Set

    language_domain: typing.Set[str]

    weight: typing.Optional[str]

    typeref: TypeRef


# StaticIntrospection is only used in static evaluation (staeval.py),
# but unfortunately the IR AST node can only be defined here.
class StaticIntrospection(Tuple):

    ir: TypeIntrospection
    schema: s_schema.Schema

    @property
    def meta_type(self) -> s_objtypes.ObjectType:
        return self.schema.get_by_id(
            self.ir.typeref.id, type=s_objtypes.ObjectType
        )

    @property
    def output_type(self) -> s_types.Type:
        return self.schema.get_by_id(
            self.ir.output_typeref.id, type=s_types.Type
        )

    @property
    def elements(self) -> typing.List[TupleElement]:
        from . import staeval

        rv = []
        schema = self.schema
        output_type = self.output_type
        for ptr in self.meta_type.get_pointers(schema).objects(schema):
            field_sn = ptr.get_shortname(schema)
            field_name = field_sn.name
            field_type = ptr.get_target(schema)
            assert field_type is not None
            try:
                field_value = output_type.get_field_value(schema, field_name)
            except LookupError:
                continue
            try:
                val = staeval.coerce_py_const(field_type.id, field_value)
            except staeval.UnsupportedExpressionError:
                continue
            ref = TypeRef(id=field_type.id, name_hint=field_sn)
            vset = Set(expr=val, typeref=ref, path_id=PathId.from_typeref(ref))
            rv.append(TupleElement(name=field_name, val=vset))
        return rv

    @elements.setter
    def elements(self, elements: typing.List[TupleElement]) -> None:
        pass

    def get_field_value(self, name: sn.QualName) -> ConstExpr | TypeCast:
        from . import staeval

        ptr = self.meta_type.getptr(self.schema, name.get_local_name())
        rv_type = ptr.get_target(self.schema)
        assert rv_type is not None
        rv_value = self.output_type.get_field_value(self.schema, name.name)
        return staeval.coerce_py_const(rv_type.id, rv_value)
