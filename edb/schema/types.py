#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import collections.abc
import enum
import types
import typing
import uuid

from edb import errors

from edb.common import checked
from edb.common import uuidgen

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import compiler as qlcompiler

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as s_name
from . import objects as so
from . import schema as s_schema
from . import utils

if typing.TYPE_CHECKING:
    # We cannot use `from typing import *` in this file due to name conflict
    # with local Tuple and Type classes.
    from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional
    from typing import AbstractSet, Sequence, Union
    from edb.common import parsing


TYPE_ID_NAMESPACE = uuidgen.UUID('00e50276-2502-11e7-97f2-27fe51238dbd')
MAX_TYPE_DISTANCE = 1_000_000_000


class ExprType(enum.IntEnum):
    """Enumeration to identify the type of an expression in aliases."""
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()
    Delete = enum.auto()


TypeT = typing.TypeVar('TypeT', bound='Type')
InheritingTypeT = typing.TypeVar('InheritingTypeT', bound='InheritingType')
CollectionTypeT = typing.TypeVar('CollectionTypeT', bound='Collection')
CollectionExprAliasT = typing.TypeVar(
    'CollectionExprAliasT', bound='CollectionExprAlias'
)


class Type(
    so.SubclassableObject,
    s_anno.AnnotationSubject,
    s_abc.Type,
):
    """A schema item that is a valid *type*."""

    # If this type is an alias, expr will contain an expression that
    # defines it.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None, coerce=True, compcoef=0.909)

    # For a type representing an expression alias, this would contain the
    # expression type.  Non-alias types have None here.
    expr_type = so.SchemaField(
        ExprType,
        default=None, compcoef=0.909)

    # True for aliases defined by CREATE ALIAS, false for local
    # aliases in queries.
    alias_is_persistent = so.SchemaField(
        bool,
        default=False, compcoef=None)

    # If this type is a view defined by a nested shape expression,
    # and the nested shape contains references to link properties,
    # rptr will contain the inbound pointer class.
    rptr = so.SchemaField(
        so.Object,
        weak_ref=True,
        default=None, compcoef=0.909)

    # The OID by which the backend refers to the type.
    backend_id = so.SchemaField(
        int,
        default=None, inheritable=False)

    def is_blocking_ref(
        self, schema: s_schema.Schema, reference: so.Object
    ) -> bool:
        return reference != self.get_rptr(schema)

    def derive_subtype(
        self: TypeT,
        schema: s_schema.Schema,
        *,
        name: s_name.QualName,
        mark_derived: bool = False,
        attrs: Optional[Mapping[str, Any]] = None,
        inheritance_merge: bool = True,
        preserve_path_id: bool = False,
        transient: bool = False,
        inheritance_refdicts: Optional[AbstractSet[str]] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, TypeT]:

        if self.get_name(schema) == name:
            raise errors.SchemaError(
                f'cannot derive {self!r}({name}) from itself')

        derived_attrs: Dict[str, object] = {}

        if attrs is not None:
            derived_attrs.update(attrs)

        derived_attrs['name'] = name
        derived_attrs['bases'] = so.ObjectList.create(schema, [self])

        cmd = sd.get_object_delta_command(
            objtype=type(self),
            cmdtype=sd.CreateObject,
            schema=schema,
            name=name,
        )

        for k, v in derived_attrs.items():
            cmd.set_attribute_value(k, v)

        context = sd.CommandContext(
            modaliases={},
            schema=schema,
        )

        delta = sd.DeltaRoot()

        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            if not inheritance_merge:
                context.current().inheritance_merge = False

            if inheritance_refdicts is not None:
                context.current().inheritance_refdicts = inheritance_refdicts

            if mark_derived:
                context.current().mark_derived = True

            if transient:
                context.current().transient_derivation = True

            if preserve_path_id:
                context.current().preserve_path_id = True

            delta.add(cmd)
            schema = delta.apply(schema, context)

        derived = typing.cast(TypeT, schema.get(name))

        return schema, derived

    def is_type(self) -> bool:
        return True

    def is_object_type(self) -> bool:
        return False

    def is_union_type(self, schema: s_schema.Schema) -> bool:
        return False

    def is_intersection_type(self, schema: s_schema.Schema) -> bool:
        return False

    def is_compound_type(self, schema: s_schema.Schema) -> bool:
        return False

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return False

    def is_any(self, schema: s_schema.Schema) -> bool:
        return False

    def is_anytuple(self, schema: s_schema.Schema) -> bool:
        return False

    def find_any(self, schema: s_schema.Schema) -> Optional[Type]:
        if self.is_any(schema):
            return self
        else:
            return None

    def contains_any(self, schema: s_schema.Schema) -> bool:
        return self.is_any(schema)

    def contains_json(self, schema: s_schema.Schema) -> bool:
        return False

    def is_scalar(self) -> bool:
        return False

    def is_collection(self) -> bool:
        return False

    def is_array(self) -> bool:
        return False

    def is_tuple(self, schema: s_schema.Schema) -> bool:
        return False

    def is_enum(self, schema: s_schema.Schema) -> bool:
        return False

    def test_polymorphic(self, schema: s_schema.Schema, poly: Type) -> bool:
        """Check if this type can be matched by a polymorphic type.

        Examples:

            - `array<anyscalar>`.test_polymorphic(`array<anytype>`) -> True
            - `array<str>`.test_polymorphic(`array<anytype>`) -> True
            - `array<int64>`.test_polymorphic(`anyscalar`) -> False
            - `float32`.test_polymorphic(`anyint`) -> False
            - `int32`.test_polymorphic(`anyint`) -> True
        """

        if not poly.is_polymorphic(schema):
            raise TypeError('expected a polymorphic type as a second argument')

        if poly.is_any(schema):
            return True

        return self._test_polymorphic(schema, poly)

    def resolve_polymorphic(
        self, schema: s_schema.Schema, other: Type
    ) -> Optional[Type]:
        """Resolve the polymorphic type component.

        Examples:

            - `array<anytype>`.resolve_polymorphic(`array<int>`) -> `int`
            - `array<anytype>`.resolve_polymorphic(`tuple<int>`) -> None
        """
        if not self.is_polymorphic(schema):
            return None

        return self._resolve_polymorphic(schema, other)

    def to_nonpolymorphic(
        self: TypeT, schema: s_schema.Schema, concrete_type: Type
    ) -> typing.Tuple[s_schema.Schema, Type]:
        """Produce an non-polymorphic version of self.

        Example:
            `array<anytype>`.to_nonpolymorphic(`int`) -> `array<int>`
            `tuple<int, anytype>`.to_nonpolymorphic(`str`) -> `tuple<int, str>`
        """
        if not self.is_polymorphic(schema):
            raise TypeError('non-polymorphic type')

        return self._to_nonpolymorphic(schema, concrete_type)

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        return False

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> Optional[Type]:
        raise NotImplementedError(
            f'{type(self)} does not support resolve_polymorphic()')

    def _to_nonpolymorphic(
        self: TypeT,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> typing.Tuple[s_schema.Schema, Type]:
        raise NotImplementedError(
            f'{type(self)} does not support to_nonpolymorphic()')

    def is_view(self, schema: s_schema.Schema) -> bool:
        return self.get_expr_type(schema) is not None

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if self.implicitly_castable_to(other, schema):
            return True
        elif self.assignment_castable_to(other, schema):
            return True
        else:
            return False

    def assignment_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        return self.implicitly_castable_to(other, schema)

    def implicitly_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        return -1

    def find_common_implicitly_castable_type(
        self: TypeT,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[TypeT]]:
        return schema, None

    def get_union_of(
        self: TypeT,
        schema: s_schema.Schema,
    ) -> Optional[so.ObjectSet[TypeT]]:
        return None

    def get_is_opaque_union(self, schema: s_schema.Schema) -> bool:
        return False

    def get_intersection_of(
        self: TypeT,
        schema: s_schema.Schema,
    ) -> Optional[so.ObjectSet[TypeT]]:
        return None

    def material_type(
        self: TypeT, schema: s_schema.Schema
    ) -> typing.Tuple[s_schema.Schema, TypeT]:
        return schema, self

    def peel_view(self, schema: s_schema.Schema) -> Type:
        return self

    def get_common_parent_type_distance(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> int:
        raise NotImplementedError

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        return not self.is_view(schema)

    def as_shell(self, schema: s_schema.Schema) -> TypeShell:
        name = typing.cast(s_name.QualName, self.get_name(schema))

        if union_of := self.get_union_of(schema):
            assert isinstance(self, so.QualifiedObject)
            return UnionTypeShell(
                components=[
                    o.as_shell(schema) for o in union_of.objects(schema)
                ],
                module=name.module,
                opaque=self.get_is_opaque_union(schema),
            )
        elif intersection_of := self.get_intersection_of(schema):
            assert isinstance(self, so.QualifiedObject)
            return IntersectionTypeShell(
                components=[
                    o.as_shell(schema) for o in intersection_of.objects(schema)
                ],
                module=name.module,
            )
        else:
            return TypeShell(
                name=name,
                schemaclass=type(self),
                is_abstract=self.get_is_abstract(schema),
            )


class InheritingType(so.DerivableInheritingObject, Type):

    def material_type(
        self, schema: s_schema.Schema
    ) -> typing.Tuple[s_schema.Schema, InheritingType]:
        return schema, self.get_nearest_non_derived_parent(schema)

    def peel_view(self, schema: s_schema.Schema) -> Type:
        # When self is a view, this returns the class the view
        # is derived from (which may be another view).  If no
        # parent class is available, returns self.
        if self.is_view(schema):
            return typing.cast(Type, self.get_bases(schema).first(schema))
        else:
            return self

    def get_common_parent_type_distance(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> int:
        if other.is_any(schema) or self.is_any(schema):
            return MAX_TYPE_DISTANCE

        if not isinstance(other, type(self)):
            return -1

        if self == other:
            return 0

        ancestor = utils.get_class_nearest_common_ancestor(
            schema, [self, other])

        if ancestor is None:
            return -1
        elif ancestor == self:
            return 0
        else:
            ancestors = list(self.get_ancestors(schema).objects(schema))
            return ancestors.index(ancestor) + 1


class TypeShell(so.ObjectShell):

    schemaclass: typing.Type[Type]

    def __init__(
        self,
        *,
        name: s_name.Name,
        origname: Optional[s_name.Name] = None,
        displayname: Optional[str] = None,
        expr: Optional[str] = None,
        is_abstract: bool = False,
        schemaclass: typing.Type[Type] = Type,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> None:
        super().__init__(
            name=name,
            origname=origname,
            displayname=displayname,
            schemaclass=schemaclass,
            sourcectx=sourcectx,
        )

        self.is_abstract = is_abstract
        self.expr = expr

    def resolve(self, schema: s_schema.Schema) -> Type:
        return schema.get(
            self.name,
            type=self.schemaclass,
            sourcectx=self.sourcectx,
        )

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return self.is_abstract

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.Command:
        raise NotImplementedError


class TypeExprShell(TypeShell):

    components: typing.Tuple[TypeShell, ...]
    module: str

    def __init__(
        self,
        components: Iterable[TypeShell],
        module: str,
    ) -> None:
        self.components = tuple(components)
        self.module = module

    def resolve(self, schema: s_schema.Schema) -> Type:
        raise NotImplementedError

    def resolve_components(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[InheritingType, ...]:
        return tuple(
            typing.cast(InheritingType, c.resolve(schema))
            for c in self.components
        )

    def get_components(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell, ...]:
        return self.components


class UnionTypeShell(TypeExprShell):

    def __init__(
        self,
        components: Iterable[TypeShell],
        module: str,
        opaque: bool = False,
    ) -> None:
        self.components = tuple(components)
        self.module = module
        self.opaque = opaque

    def resolve(self, schema: s_schema.Schema) -> Type:
        components = self.resolve_components(schema)
        type_id, _ = get_union_type_id(
            schema, components, opaque=self.opaque, module=self.module)
        return schema.get_by_id(type_id, type=Type)

    def get_name(
        self,
        schema: s_schema.Schema,
    ) -> s_name.Name:
        _, name = get_union_type_id(
            schema,
            self.components,
            opaque=self.opaque,
            module=self.module,
        )
        return name

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.Command:

        type_id, name = get_union_type_id(
            schema,
            self.components,
            opaque=self.opaque,
            module=self.module,
        )

        cmd = CreateUnionType(classname=name)
        cmd.set_attribute_value('id', type_id)
        cmd.set_attribute_value('name', name)
        cmd.set_attribute_value('components', tuple(self.components))
        cmd.set_attribute_value('is_opaque_union', self.opaque)
        return cmd

    def __repr__(self) -> str:
        dn = 'UnionType'
        comps = ' | '.join(repr(c) for c in self.components)
        return f'<{type(self).__name__} {dn}({comps}) at 0x{id(self):x}>'


class CompoundTypeCommandContext(sd.ObjectCommandContext[InheritingType]):
    pass


class CompoundTypeCommand(
    sd.QualifiedObjectCommand[InheritingType],
    context_class=CompoundTypeCommandContext,
):
    pass


class CreateUnionType(sd.CreateObject[InheritingType], CompoundTypeCommand):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        if not context.canonical:
            components = [
                c.resolve(schema)
                for c in self.get_attribute_value('components')
            ]

            new_schema, union_type = utils.get_union_type(
                schema,
                components,
                opaque=self.get_attribute_value('is_opaque_union') or False,
                module=self.classname.module,
            )

            delta = union_type.as_create_delta(
                schema=new_schema,
                context=so.ComparisonContext(),
            )

            self.add(delta)

        for cmd in self.get_subcommands():
            schema = cmd.apply(schema, context)

        return schema


class IntersectionTypeShell(TypeExprShell):

    def resolve(self, schema: s_schema.Schema) -> Type:
        components = self.resolve_components(schema)
        type_id, _ = get_intersection_type_id(
            schema, components, module=self.module)
        return schema.get_by_id(type_id, type=Type)

    def get_name(
        self,
        schema: s_schema.Schema,
    ) -> s_name.Name:
        _, name = get_intersection_type_id(
            schema,
            self.components,
            module=self.module,
        )
        return name


class Collection(Type, s_abc.Collection):

    schema_name: typing.ClassVar[str]

    #: True for collection types that are stored in schema persistently
    is_persistent = so.SchemaField(
        bool,
        default=False,
        compcoef=None,
    )

    @classmethod
    def get_displayname_static(cls, name: s_name.Name) -> str:
        return type_displayname_from_name(name)

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(st.is_polymorphic(schema)
                   for st in self.get_subtypes(schema))

    def find_any(self, schema: s_schema.Schema) -> Optional[Type]:
        for st in self.get_subtypes(schema):
            any_t = st.find_any(schema)
            if any_t is not None:
                return any_t

        return None

    def contains_any(self, schema: s_schema.Schema) -> bool:
        return any(st.contains_any(schema) for st in self.get_subtypes(schema))

    def contains_json(self, schema: s_schema.Schema) -> bool:
        return any(
            st.contains_json(schema) for st in self.get_subtypes(schema))

    def contains_object(self, schema: s_schema.Schema) -> bool:
        return any(
            st.contains_object(schema) if isinstance(st, Collection)
            else st.is_object_type()
            for st in self.get_subtypes(schema)
        )

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    def is_collection(self) -> bool:
        return True

    def get_common_parent_type_distance(
            self, other: Type, schema: s_schema.Schema) -> int:
        if other.is_any(schema):
            return 1

        if other.__class__ is not self.__class__:
            return -1

        other = typing.cast(Collection, other)
        other_types = other.get_subtypes(schema)
        my_types = self.get_subtypes(schema)

        type_dist = 0
        for ot, my in zip(other_types, my_types):
            el_dist = my.get_common_parent_type_distance(ot, schema)
            if el_dist < 0:
                return -1
            else:
                type_dist += el_dist

        return type_dist

    def _issubclass(
        self, schema: s_schema.Schema, parent: so.SubclassableObject
    ) -> bool:
        if isinstance(parent, Type) and parent.is_any(schema):
            return True

        if parent.__class__ is not self.__class__:
            return False

        # The cast below should not be necessary but Mypy does not believe
        # that a.__class__ == b.__class__ is enough.
        parent_types = typing.cast(Collection, parent).get_subtypes(schema)
        my_types = self.get_subtypes(schema)

        for pt, my in zip(parent_types, my_types):
            if not pt.is_any(schema) and not my.issubclass(schema, pt):
                return False

        return True

    def issubclass(
        self,
        schema: s_schema.Schema,
        parent: Union[
            so.SubclassableObject,
            typing.Tuple[so.SubclassableObject, ...],
        ],
    ) -> bool:
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)

        if isinstance(parent, Type) and parent.is_any(schema):
            return True

        return self._issubclass(schema, parent)

    @classmethod
    def compare_field_value(
        cls,
        field: so.Field[typing.Type[so.T]],
        our_value: so.T,
        their_value: so.T,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        # Disregard differences in generated names, because those
        # contain type ids which are volatile.
        if field.name == 'name':
            if (
                str(our_value).startswith('__id:')
                and str(their_value).startswith('__id:')
            ):
                return 1.0

        return super().compare_field_value(
            field,
            our_value,
            their_value,
            our_schema=our_schema,
            their_schema=their_schema,
            context=context,
        )

    @classmethod
    def compare_values(
        cls,
        ours: Optional[Collection],
        theirs: Optional[Collection],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
        compcoef: float,
    ) -> float:
        if ours is None and theirs is None:
            return 1.0
        elif ours is None or theirs is None:
            return compcoef

        if type(ours) is not type(theirs):
            basecoef = 0.2
        else:
            my_subtypes = ours.get_subtypes(our_schema)
            other_subtypes = theirs.get_subtypes(their_schema)

            if len(my_subtypes) != len(other_subtypes):
                basecoef = 0.2
            else:
                similarity = []
                for i, st in enumerate(my_subtypes):
                    ot = other_subtypes[i]
                    if isinstance(st, type(ot)) or isinstance(ot, type(st)):
                        similarity.append(
                            st.compare(
                                ot, our_schema=our_schema,
                                their_schema=their_schema, context=context))
                    else:
                        similarity.append(0)

                basecoef = sum(similarity) / len(similarity)

            my_typemods = ours.get_typemods(our_schema)
            other_typemods = theirs.get_typemods(their_schema)

            if my_typemods != other_typemods:
                basecoef = 0.2

        return basecoef + (1 - basecoef) * compcoef

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        raise NotImplementedError

    def get_typemods(self, schema: s_schema.Schema) -> Any:
        return ()

    @classmethod
    def get_class(
        cls, schema_name: str
    ) -> Union[typing.Type[Array], typing.Type[Tuple]]:
        if schema_name == 'array':
            return Array
        elif schema_name == 'tuple':
            return Tuple

        raise errors.SchemaError(
            'unknown collection type: {!r}'.format(schema_name))

    @classmethod
    def from_subtypes(
        cls,
        schema: s_schema.Schema,
        subtypes: Any,
        typemods: Any = None,
    ) -> typing.Tuple[s_schema.Schema, Collection]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__} '
            f'{self.id} at 0x{id(self):x}>'
        )

    def dump(self, schema: s_schema.Schema) -> str:
        return repr(self)

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'collection'

    def as_colltype_delete_delta(
        self,
        schema: s_schema.Schema,
        *,
        if_exists: bool = False,
        expiring_refs: AbstractSet[so.Object],
        view_name: Optional[s_name.QualName] = None,
    ) -> sd.Command:
        raise NotImplementedError


Dimensions = checked.FrozenCheckedList[int]
Array_T = typing.TypeVar("Array_T", bound="Array")


class CollectionTypeShell(TypeShell):

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell, ...]:
        raise NotImplementedError

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        raise NotImplementedError

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(
            st.is_polymorphic(schema) for st in self.get_subtypes(schema)
        )


class Array(
    Collection,
    s_abc.Array,
    qlkind=qltypes.SchemaObjectClass.ARRAY_TYPE,
):

    schema_name = 'array'

    element_type = so.SchemaField(
        Type,
        # We want a low compcoef so that array types are *never* altered.
        compcoef=0,
    )

    dimensions = so.SchemaField(
        Dimensions,
        coerce=True,
        # We want a low compcoef so that array types are *never* altered.
        compcoef=0,
    )

    @classmethod
    def create(
        cls: typing.Type[Array_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        dimensions: Sequence[int] = (),
        element_type: Any,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Array_T]:
        if not dimensions:
            dimensions = [-1]

        if dimensions != [-1]:
            raise errors.UnsupportedFeatureError(
                f'multi-dimensional arrays are not supported')

        if id is so.NoDefault:
            quals = []
            if name is not None:
                quals.append(str(name))
            id = generate_array_type_id(
                schema, element_type, dimensions, *quals)

        if name is None:
            dn = f'array<{element_type.get_displayname(schema)}>'
            name = type_name_from_id_and_displayname(id, dn)

        result = typing.cast(Array_T, schema.get_by_id(id, default=None))
        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                element_type=element_type,
                dimensions=dimensions,
                **kwargs,
            )

        return schema, result

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return self.get_element_type(schema).is_tuple(schema)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return (
            f'array<{self.get_element_type(schema).get_displayname(schema)}>')

    def is_array(self) -> bool:
        return True

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.QualName,
        attrs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Array]:
        assert not kwargs
        return Array.from_subtypes(
            schema,
            [self.get_element_type(schema)],
            self.get_typemods(schema),
            name=name,
            **(attrs or {}),
        )

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        return (self.get_element_type(schema),)

    def get_typemods(self, schema: s_schema.Schema) -> typing.Tuple[Any, ...]:
        return (self.get_dimensions(schema),)

    def implicitly_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        if not isinstance(other, Array):
            return False

        return self.get_element_type(schema).implicitly_castable_to(
            other.get_element_type(schema), schema)

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        if not isinstance(other, Array):
            return -1

        return self.get_element_type(schema).get_implicit_cast_distance(
            other.get_element_type(schema), schema)

    def assignment_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Array):
            return False

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Array):
            return False

        return self.get_element_type(schema).castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
        self: Array_T,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[Array_T]]:

        if not isinstance(other, Array):
            return schema, None

        if self == other:
            return schema, self

        my_el = self.get_element_type(schema)
        schema, subtype = my_el.find_common_implicitly_castable_type(
            other.get_element_type(schema), schema)

        if subtype is None:
            return schema, None

        return type(self).from_subtypes(schema, [subtype])

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> Optional[Type]:
        if not isinstance(concrete_type, Array):
            return None

        return self.get_element_type(schema).resolve_polymorphic(
            schema, concrete_type.get_element_type(schema))

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> typing.Tuple[s_schema.Schema, Array]:
        return Array.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        if other.is_any(schema):
            return True

        if not isinstance(other, Array):
            return False

        return self.get_element_type(schema).test_polymorphic(
            schema, other.get_element_type(schema))

    @classmethod
    def from_subtypes(
        cls: typing.Type[Array_T],
        schema: s_schema.Schema,
        subtypes: Sequence[Type],
        typemods: Any = None,
        *,
        name: Optional[s_name.QualName] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Array_T]:
        if len(subtypes) != 1:
            raise errors.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]

        if isinstance(stype, Array):
            raise errors.UnsupportedFeatureError(
                f'nested arrays are not supported')

        # One-dimensional unbounded array.
        dimensions = [-1]

        return cls.create(
            schema,
            element_type=stype,
            dimensions=dimensions,
            name=name,
            id=id,
            **kwargs,
        )

    @classmethod
    def create_shell(
        cls,
        schema: s_schema.Schema,
        *,
        subtypes: Sequence[TypeShell],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
        expr: Optional[str] = None,
    ) -> ArrayTypeShell:
        if not typemods:
            typemods = ([-1],)

        st = next(iter(subtypes))

        if name is None:
            name = s_name.UnqualName('__unresolved__')

        return ArrayTypeShell(
            subtype=st,
            typemods=typemods,
            name=name,
            expr=expr,
            schemaclass=cls,
        )

    def as_shell(self, schema: s_schema.Schema) -> ArrayTypeShell:
        expr = self.get_expr(schema)
        expr_text = expr.text if expr is not None else None
        return type(self).create_shell(
            schema,
            subtypes=[st.as_shell(schema) for st in self.get_subtypes(schema)],
            typemods=self.get_typemods(schema),
            name=self.get_name(schema),
            expr=expr_text,
        )

    def material_type(
        self: Array_T,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Array_T]:
        # We need to resolve material types based on the subtype recursively.

        st = self.get_element_type(schema)
        schema, stm = st.material_type(schema)
        if stm != st:
            return self.__class__.from_subtypes(
                schema,
                [stm],
                typemods=self.get_typemods(schema),
            )
        else:
            return (schema, self)

    def as_colltype_delete_delta(
        self,
        schema: s_schema.Schema,
        *,
        if_exists: bool = False,
        expiring_refs: AbstractSet[so.Object] = frozenset(),
        view_name: Optional[s_name.QualName] = None,
    ) -> Union[DeleteArray, DeleteArrayExprAlias]:
        cmd: Union[DeleteArray, DeleteArrayExprAlias]
        if view_name is None:
            cmd = DeleteArray(
                classname=self.get_name(schema),
                if_unused=True,
                if_exists=if_exists,
                expiring_refs=expiring_refs,
            )
        else:
            cmd = DeleteArrayExprAlias(
                classname=view_name,
                if_exists=if_exists,
                expiring_refs=expiring_refs,
            )

        el = self.get_element_type(schema)
        if isinstance(el, Collection):
            cmd.add(el.as_colltype_delete_delta(schema, expiring_refs={self}))

        return cmd


class ArrayTypeShell(CollectionTypeShell):

    def __init__(
        self,
        *,
        name: s_name.Name,
        expr: Optional[str] = None,
        subtype: TypeShell,
        typemods: typing.Tuple[typing.Any, ...],
        schemaclass: typing.Type[Array] = Array,
    ) -> None:
        super().__init__(name=name, schemaclass=schemaclass, expr=expr)
        self.subtype = subtype
        self.typemods = typemods

    def get_name(self, schema: s_schema.Schema) -> s_name.Name:
        if str(self.name) == '__unresolved__':
            typemods = self.typemods
            dimensions = typemods[0]
            tid = generate_array_type_id(schema, self.subtype, dimensions)
            self.name = type_name_from_id_and_displayname(
                tid, f'array<{self.subtype.get_displayname(schema)}>')
        return self.name

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell, ...]:
        return (self.subtype,)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return f'array<{self.subtype.get_displayname(schema)}>'

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        name = self.get_name(schema)
        stable_type_id = type_id_from_name(name)
        if stable_type_id is not None:
            return stable_type_id

        dimensions = self.typemods[0]
        quals: typing.List[str] = [str(name)]
        if self.expr is not None:
            quals.append(self.expr)
        return generate_array_type_id(
            schema,
            self.subtype,
            dimensions,
            *quals,
        )

    def resolve(self, schema: s_schema.Schema) -> Array:
        return schema.get_by_id(self.get_id(schema), type=Array)

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ca: Union[CreateArray, CreateArrayExprAlias]
        cmd = sd.CommandGroup()
        type_id = self.get_id(schema)
        if view_name is None:
            ca = CreateArray(
                classname=self.get_name(schema),
                if_not_exists=True,
            )
        else:
            ca = CreateArrayExprAlias(
                classname=view_name,
            )

        el = self.subtype
        if (isinstance(el, CollectionTypeShell)
                and schema.get_by_id(el.get_id(schema), None) is None):
            cmd.add(el.as_create_delta(schema))

        ca.set_attribute_value('id', type_id)
        ca.set_attribute_value('name', ca.classname)
        ca.set_attribute_value('element_type', el)
        ca.set_attribute_value('is_persistent', True)
        ca.set_attribute_value('dimensions', self.typemods[0])

        if attrs:
            for k, v in attrs.items():
                ca.set_attribute_value(k, v)

        cmd.add(ca)

        return cmd


class CollectionExprAlias(so.QualifiedObject, Collection):

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'view'


class ArrayExprAlias(
    CollectionExprAlias,
    Array,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete Array).
    pass


Tuple_T = typing.TypeVar('Tuple_T', bound='Tuple')


class Tuple(
    Collection,
    s_abc.Tuple,
    qlkind=qltypes.SchemaObjectClass.TUPLE_TYPE,
):

    schema_name = 'tuple'

    named = so.SchemaField(
        bool,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0.01,
    )

    element_types = so.SchemaField(
        so.ObjectDict[str, Type],
        coerce=True,
        # We want a low compcoef so that tuples are *never* altered.
        compcoef=0.01,
        # Tuple element types cannot be represented by a direct link,
        # because the element types may be duplicate, so we need a
        # proxy object.
        reflection_proxy=('schema::TupleElement', 'type'),
    )

    @classmethod
    def create(
        cls: typing.Type[Tuple_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        element_types: Mapping[str, Type],
        named: bool = False,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Tuple_T]:
        element_types = types.MappingProxyType(element_types)
        if id is so.NoDefault:
            quals = []
            if name is not None:
                quals.append(str(name))
            id = generate_tuple_type_id(schema, element_types, named, *quals)

        if name is None:
            st_names = ', '.join(
                st.get_displayname(schema) for st in element_types.values()
            )
            name = type_name_from_id_and_displayname(id, f'tuple<{st_names}>')

        result = typing.cast(Tuple_T, schema.get_by_id(id, default=None))
        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                named=named,
                element_types=element_types,
                **kwargs,
            )

        return schema, result

    def get_displayname(self, schema: s_schema.Schema) -> str:
        st_names = ', '.join(st.get_displayname(schema)
                             for st in self.get_subtypes(schema))
        return f'tuple<{st_names}>'

    def is_tuple(self, schema: s_schema.Schema) -> bool:
        return True

    def is_named(self, schema: s_schema.Schema) -> bool:
        return self.get_named(schema)

    def get_element_names(self, schema: s_schema.Schema) -> Sequence[str]:
        return tuple(self.get_element_types(schema).keys(schema))

    def iter_subtypes(
        self, schema: s_schema.Schema
    ) -> Iterator[typing.Tuple[str, Type]]:
        yield from self.get_element_types(schema).items(schema)

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        return self.get_element_types(schema).values(schema)

        if self.element_types:
            return self.element_types.objects(schema)
        else:
            return []

    def normalize_index(self, schema: s_schema.Schema, field: str) -> str:
        if self.is_named(schema) and field.isdecimal():
            idx = int(field)
            el_names = self.get_element_names(schema)
            if idx >= 0 and idx < len(el_names):
                return el_names[idx]
            else:
                raise errors.InvalidReferenceError(
                    f'{field} is not a member of '
                    f'{self.get_displayname(schema)}')

        return field

    def index_of(self, schema: s_schema.Schema, field: str) -> int:
        if field.isdecimal():
            idx = int(field)
            el_names = self.get_element_names(schema)
            if idx >= 0 and idx < len(el_names):
                if self.is_named(schema):
                    return el_names.index(field)
                else:
                    return idx
        elif self.is_named(schema):
            el_names = self.get_element_names(schema)
            try:
                return el_names.index(field)
            except ValueError:
                pass

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def get_subtype(self, schema: s_schema.Schema, field: str) -> Type:
        # index can be a name or a position
        if field.isdecimal():
            idx = int(field)
            subtypes_l = list(self.get_subtypes(schema))
            if idx >= 0 and idx < len(subtypes_l):
                return subtypes_l[idx]

        elif self.is_named(schema):
            subtypes_d = dict(self.iter_subtypes(schema))
            if field in subtypes_d:
                return subtypes_d[field]

        raise errors.InvalidReferenceError(
            f'{field} is not a member of {self.get_displayname(schema)}')

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.QualName,
        attrs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Tuple]:
        assert not kwargs
        return Tuple.from_subtypes(
            schema,
            dict(self.iter_subtypes(schema)),
            self.get_typemods(schema),
            name=name,
            **(attrs or {}),
        )

    @classmethod
    def from_subtypes(
        cls: typing.Type[Tuple_T],
        schema: s_schema.Schema,
        subtypes: Union[Iterable[Type], Mapping[str, Type]],
        typemods: Any = None,
        *,
        name: Optional[s_name.QualName] = None,
        id: Union[uuid.UUID, so.NoDefaultT] = so.NoDefault,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Tuple_T]:
        named = False
        if typemods is not None:
            named = typemods.get('named', False)

        types: Mapping[str, Type]
        if isinstance(subtypes, collections.abc.Mapping):
            types = subtypes
        else:
            types = {str(i): type for i, type in enumerate(subtypes)}
        return cls.create(schema, element_types=types, named=named,
                          name=name, id=id, **kwargs)

    @classmethod
    def create_shell(
        cls,
        schema: s_schema.Schema,
        *,
        subtypes: Mapping[str, TypeShell],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
    ) -> TupleTypeShell:
        if name is None:
            name = s_name.UnqualName(name='__unresolved__')

        return TupleTypeShell(
            subtypes=subtypes,
            typemods=typemods,
            name=name,
        )

    def as_shell(self, schema: s_schema.Schema) -> TupleTypeShell:
        stshells: Dict[str, TypeShell] = {}

        for n, st in self.iter_subtypes(schema):
            stshells[n] = st.as_shell(schema)

        return type(self).create_shell(
            schema,
            subtypes=stshells,
            typemods=self.get_typemods(schema),
            name=self.get_name(schema),
        )

    def implicitly_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Tuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.implicitly_castable_to(ot, schema):
                return False

        return True

    def get_implicit_cast_distance(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> int:
        if not isinstance(other, Tuple):
            return -1

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return -1

        total_dist = 0

        for st, ot in zip(self_subtypes, other_subtypes):
            dist = st.get_implicit_cast_distance(ot, schema)
            if dist < 0:
                return -1

            total_dist += dist

        return total_dist

    def assignment_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Tuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.assignment_castable_to(ot, schema):
                return False

        return True

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Tuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        for st, ot in zip(self_subtypes, other_subtypes):
            if not st.castable_to(ot, schema):
                return False

        return True

    def find_common_implicitly_castable_type(
        self: Tuple_T,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[Tuple_T]]:

        if not isinstance(other, Tuple):
            return schema, None

        if self == other:
            return schema, self

        subs = self.get_subtypes(schema)
        other_subs = other.get_subtypes(schema)

        if len(subs) != len(other_subs):
            return schema, None

        new_types: List[Type] = []
        for st, ot in zip(subs, other_subs):
            schema, nt = st.find_common_implicitly_castable_type(ot, schema)
            if nt is None:
                return schema, None

            new_types.append(nt)

        if self.is_named(schema) and other.is_named(schema):
            my_names = self.get_element_names(schema)
            other_names = other.get_element_names(schema)
            if my_names == other_names:
                return type(self).from_subtypes(
                    schema, dict(zip(my_names, new_types)), {"named": True}
                )

        return type(self).from_subtypes(schema, new_types)

    def get_typemods(self, schema: s_schema.Schema) -> Dict[str, bool]:
        return {'named': self.is_named(schema)}

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return any(
            st.contains_array_of_tuples(schema)
            if isinstance(st, Collection) else False
            for st in self.get_subtypes(schema)
        )

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> Optional[Type]:
        if not isinstance(concrete_type, Tuple):
            return None

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = concrete_type.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return None

        for source, target in zip(self_subtypes, other_subtypes):
            if source.is_polymorphic(schema):
                return source.resolve_polymorphic(schema, target)

        return None

    def _to_nonpolymorphic(
        self: Tuple_T,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> typing.Tuple[s_schema.Schema, Tuple_T]:
        new_types: List[Type] = []
        for st in self.get_subtypes(schema):
            if st.is_polymorphic(schema):
                schema, nst = st.to_nonpolymorphic(schema, concrete_type)
            else:
                nst = st
            new_types.append(nst)

        if self.is_named(schema):
            return type(self).from_subtypes(
                schema,
                dict(zip(self.get_element_names(schema), new_types)),
                {"named": True},
            )

        return type(self).from_subtypes(schema, new_types)

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        if other.is_any(schema) or other.is_anytuple(schema):
            return True
        if not isinstance(other, Tuple):
            return False

        self_subtypes = self.get_subtypes(schema)
        other_subtypes = other.get_subtypes(schema)

        if len(self_subtypes) != len(other_subtypes):
            return False

        return all(st.test_polymorphic(schema, ot)
                   for st, ot in zip(self_subtypes, other_subtypes))

    def material_type(
        self: Tuple_T,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Tuple_T]:
        # We need to resolve material types of all the subtypes recursively.
        new_material_type = False
        subtypes = {}

        for st_name, st in self.iter_subtypes(schema):
            schema, stm = st.material_type(schema)
            if stm != st:
                new_material_type = True
            subtypes[st_name] = stm

        if new_material_type or str(self.get_name(schema)) != str(self.id):
            return self.__class__.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return schema, self

    def as_colltype_delete_delta(
        self,
        schema: s_schema.Schema,
        *,
        if_exists: bool = False,
        expiring_refs: AbstractSet[so.Object] = frozenset(),
        view_name: Optional[s_name.QualName] = None,
    ) -> Union[DeleteTuple, DeleteTupleExprAlias]:
        cmd: Union[DeleteTuple, DeleteTupleExprAlias]
        if view_name is None:
            cmd = DeleteTuple(
                classname=self.get_name(schema),
                if_unused=True,
                if_exists=if_exists,
                expiring_refs=expiring_refs,
            )
        else:
            cmd = DeleteTupleExprAlias(
                classname=view_name,
                if_exists=if_exists,
                expiring_refs=expiring_refs,
            )

        for el in self.get_subtypes(schema):
            if isinstance(el, Collection):
                cmd.add(
                    el.as_colltype_delete_delta(schema, expiring_refs={self}))

        return cmd


class TupleTypeShell(CollectionTypeShell):

    def __init__(
        self,
        *,
        name: s_name.Name,
        expr: Optional[str] = None,
        subtypes: Mapping[str, TypeShell],
        typemods: Any = None,
    ) -> None:
        super().__init__(name=name, schemaclass=Tuple, expr=expr)
        self.subtypes = subtypes
        self.typemods = typemods

    def get_name(self, schema: s_schema.Schema) -> s_name.Name:
        if str(self.name) == '__unresolved__':
            typemods = self.typemods
            subtypes = self.subtypes
            named = typemods is not None and typemods.get('named', False)
            tid = generate_tuple_type_id(schema, subtypes, named)
            st_names = ', '.join(
                st.get_displayname(schema) for st in subtypes.values()
            )
            name = type_name_from_id_and_displayname(tid, f'tuple<{st_names}>')
            self.name = name
        return self.name

    def get_displayname(self, schema: s_schema.Schema) -> str:
        st_names = ', '.join(st.get_displayname(schema)
                             for st in self.get_subtypes(schema))
        return f'tuple<{st_names}>'

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell, ...]:
        return tuple(self.subtypes.values())

    def iter_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> Iterator[typing.Tuple[str, TypeShell]]:
        return iter(self.subtypes.items())

    def is_named(self) -> bool:
        return self.typemods is not None and self.typemods.get('named', False)

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        name = self.get_name(schema)
        stable_type_id = type_id_from_name(name)
        if stable_type_id is not None:
            return stable_type_id

        named = self.is_named()

        quals: typing.List[str] = [str(name)]
        if self.expr is not None:
            quals.append(self.expr)

        return generate_tuple_type_id(schema, self.subtypes, named, *quals)

    def resolve(self, schema: s_schema.Schema) -> Tuple:
        return schema.get_by_id(self.get_id(schema), type=Tuple)

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ct: Union[CreateTuple, CreateTupleExprAlias]
        cmd = sd.CommandGroup()
        type_id = self.get_id(schema)
        if view_name is None:
            ct = CreateTuple(
                classname=self.get_name(schema),
                if_not_exists=True,
            )
        else:
            ct = CreateTupleExprAlias(
                classname=view_name,
            )

        for el in self.subtypes.values():
            if (isinstance(el, CollectionTypeShell)
                    and schema.get_by_id(el.get_id(schema), None) is None):
                cmd.add(el.as_create_delta(schema))

        named = self.is_named()
        ct.set_attribute_value('id', type_id)
        ct.set_attribute_value('name', ct.classname)
        ct.set_attribute_value('named', named)
        ct.set_attribute_value('is_persistent', True)
        ct.set_attribute_value('element_types', self.subtypes)

        if attrs:
            for k, v in attrs.items():
                ct.set_attribute_value(k, v)

        cmd.add(ct)

        return cmd


class TupleExprAlias(
    CollectionExprAlias,
    Tuple,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete Tuple).
    pass


def type_name_from_id_and_displayname(
    id: uuid.UUID,
    displayname: str,
) -> s_name.UnqualName:
    return s_name.UnqualName(
        name=f'__id:{id}:{s_name.mangle_name(displayname)}',
    )


def is_type_id_name(name: str) -> bool:
    return name.startswith('__id:')


def type_id_from_name(name: s_name.Name) -> Optional[uuid.UUID]:
    strname = str(name)
    if strname.startswith('__id:'):
        parts = strname.split(':', maxsplit=2)
        return uuid.UUID(parts[1])
    else:
        return None


def type_displayname_from_name(name: s_name.Name) -> str:
    strname = str(name)
    if is_type_id_name(strname):
        parts = strname.split(':', maxsplit=2)
        return s_name.unmangle_name(parts[2])
    else:
        return strname


def generate_type_id(id_str: str) -> uuid.UUID:
    return uuidgen.uuid5(TYPE_ID_NAMESPACE, id_str)


def generate_tuple_type_id(
    schema: s_schema.Schema,
    element_types: Mapping[str, Union[Type, TypeShell]],
    named: bool = False,
    *quals: str,
) -> uuid.UUID:
    id_str = ','.join(
        f'{n}:{st.get_id(schema)}' for n, st in element_types.items())
    id_str = f'{"named" if named else ""}tuple-{id_str}'
    if quals:
        id_str = f'{id_str}_{"-".join(quals)}'
    return generate_type_id(id_str)


def generate_array_type_id(
    schema: s_schema.Schema,
    element_type: Union[Type, TypeShell],
    dimensions: Sequence[int] = (),
    *quals: str,
) -> uuid.UUID:
    id_basis = f'array-{element_type.get_id(schema)}-{dimensions}'
    if quals:
        id_basis = f'{id_basis}-{"-".join(quals)}'
    return generate_type_id(id_basis)


def get_union_type_id(
    schema: s_schema.Schema,
    components: typing.Iterable[Union[Type, TypeShell]],
    *,
    opaque: bool = False,
    module: typing.Optional[str] = None,
) -> typing.Tuple[uuid.UUID, s_name.QualName]:

    component_ids = sorted(str(t.get_id(schema)) for t in components)
    if opaque:
        nqname = f"(opaque: {' | '.join(component_ids)})"
    else:
        nqname = f"({' | '.join(component_ids)})"
    name = s_name.QualName(name=nqname, module=module or '__derived__')
    return generate_type_id(str(name)), name


def get_intersection_type_id(
    schema: s_schema.Schema,
    components: typing.Iterable[Union[Type, TypeShell]], *,
    module: typing.Optional[str]=None,
) -> typing.Tuple[uuid.UUID, s_name.QualName]:

    component_ids = sorted(str(t.get_id(schema)) for t in components)
    name = s_name.QualName(
        name=f"({' & '.join(component_ids)})",
        module=module or '__derived__',
    )

    return generate_type_id(str(name)), name


def ensure_schema_type_expr_type(
    schema: s_schema.Schema,
    type_shell: TypeExprShell,
    parent_cmd: sd.Command,
    *,
    src_context: typing.Optional[parsing.ParserContext] = None,
    context: sd.CommandContext,
) -> Optional[sd.Command]:

    module = type_shell.module
    components = type_shell.components

    if isinstance(type_shell, UnionTypeShell):
        type_id, type_name = get_union_type_id(
            schema,
            components,
            opaque=type_shell.opaque,
            module=module,
        )
    elif isinstance(type_shell, IntersectionTypeShell):
        type_id, type_name = get_intersection_type_id(
            schema,
            components,
            module=module,
        )
    else:
        raise AssertionError(f'unexpected type shell: {type_shell!r}')

    texpr_type = schema.get_by_id(type_id, None, type=Type)

    cmd = None
    if texpr_type is None:
        cmd = type_shell.as_create_delta(schema)
        if cmd is not None:
            parent_cmd.add_prerequisite(cmd)

    return cmd


class TypeCommand(sd.ObjectCommand[TypeT]):

    @classmethod
    def _get_alias_expr(cls, astnode: qlast.CreateAlias) -> qlast.Expr:
        expr = qlast.get_ddl_field_value(astnode, 'expr')
        if expr is None:
            raise errors.InvalidAliasDefinitionError(
                f'missing required view expression', context=astnode.context)
        assert isinstance(expr, qlast.Expr)
        return expr

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_attribute_value('expr'):
            return None
        elif (
            (union_of := self.get_attribute_value('union_of')) is not None
            and union_of.items
        ):
            return None
        elif (
            (int_of := self.get_attribute_value('intersection_of')) is not None
            and int_of.items
        ):
            return None
        else:
            return super().get_ast(schema, context, parent_node=parent_node)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:
        assert field.name == 'expr'
        return type(value).compiled(
            value,
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=context.modaliases,
                in_ddl_context_name='type definition',
                track_schema_ref_exprs=track_schema_ref_exprs,
            ),
        )

    def _create_begin(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        assert isinstance(self.scls, Type)
        if not self.scls.is_view(schema):
            delta_root = context.top().op
            assert isinstance(delta_root, sd.DeltaRoot)
            delta_root.new_types.add(self.scls.id)
        return schema


class InheritingTypeCommand(
    sd.QualifiedObjectCommand[InheritingTypeT],
    TypeCommand[InheritingTypeT],
    inheriting.InheritingObjectCommand[InheritingTypeT],
):
    pass


class CreateInheritingType(
    InheritingTypeCommand[InheritingTypeT],
    inheriting.CreateInheritingObject[InheritingTypeT],
):

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:

        super().validate_create(schema, context)

        bases = self.get_resolved_attribute_value(
            'bases',
            schema=schema,
            context=context,
        )
        if bases:
            for base in bases.objects(schema):
                if base.contains_any(schema):
                    base_type_name = base.get_displayname(schema)
                    raise errors.SchemaError(
                        f"{base_type_name!r} cannot be a parent type")


class CollectionTypeCommandContext(sd.ObjectCommandContext[Collection]):
    pass


class CollectionTypeCommand(TypeCommand[CollectionTypeT],
                            context_class=CollectionTypeCommandContext):

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        # CollectionTypeCommand cannot have its own AST because it is a
        # side-effect of some other command.
        return None


class CollectionExprAliasCommand(
    sd.QualifiedObjectCommand[CollectionExprAliasT],
    TypeCommand[CollectionExprAliasT],
    context_class=CollectionTypeCommandContext,
):

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        # CollectionTypeCommand cannot have its own AST because it is a
        # side-effect of some other command.
        return None


class CreateCollectionType(
    CollectionTypeCommand[CollectionTypeT],
    sd.CreateObject[CollectionTypeT],
):
    pass


class DeleteCollectionType(
    CollectionTypeCommand[CollectionTypeT],
    sd.DeleteObject[CollectionTypeT],
):
    pass


class CreateCollectionExprAlias(
    CollectionExprAliasCommand[CollectionExprAliasT],
    sd.CreateObject[CollectionExprAliasT],
):
    pass


class DeleteCollectionExprAlias(
    CollectionExprAliasCommand[CollectionExprAliasT],
    sd.DeleteObject[CollectionExprAliasT],
):
    pass


class CreateTuple(CreateCollectionType[Tuple]):
    pass


class CreateTupleExprAlias(CreateCollectionExprAlias[TupleExprAlias]):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class RenameTupleExprAlias(
    CollectionExprAliasCommand[TupleExprAlias],
    sd.RenameObject[TupleExprAlias],
):
    pass


class AlterTupleExprAlias(
    CollectionExprAliasCommand[TupleExprAlias],
    sd.AlterObject[TupleExprAlias],
):
    pass


class CreateArray(CreateCollectionType[Array]):
    pass


class CreateArrayExprAlias(CreateCollectionExprAlias[ArrayExprAlias]):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class RenameArrayExprAlias(
    CollectionExprAliasCommand[ArrayExprAlias],
    sd.RenameObject[ArrayExprAlias],
):
    pass


class AlterArrayExprAlias(
    CollectionExprAliasCommand[ArrayExprAlias],
    sd.AlterObject[ArrayExprAlias],
):
    pass


class DeleteTuple(DeleteCollectionType[Tuple]):
    pass


class DeleteTupleExprAlias(DeleteCollectionExprAlias[TupleExprAlias]):
    pass


class DeleteArray(DeleteCollectionType[Array]):
    pass


class DeleteArrayExprAlias(DeleteCollectionExprAlias[ArrayExprAlias]):
    pass


def materialize_type_in_attribute(
    schema: s_schema.Schema,
    context: sd.CommandContext,
    cmd: sd.Command,
    attrname: str,
) -> s_schema.Schema:
    assert isinstance(cmd, sd.ObjectCommand)

    type_ref = cmd.get_local_attribute_value(attrname)
    if type_ref is None:
        return schema

    srcctx = cmd.get_attribute_source_context('target')

    if isinstance(type_ref, TypeExprShell):
        cc_cmd = ensure_schema_type_expr_type(
            schema,
            type_ref,
            parent_cmd=cmd,
            src_context=srcctx,
            context=context,
        )
        if cc_cmd is not None:
            schema = cc_cmd.apply(schema, context)

    if isinstance(type_ref, CollectionTypeShell):
        # If the current command is a fragment, we want the collection
        # creation to live in the parent operation, in order for the
        # logic to skip it if the object already exists to work.
        op = (cmd.get_parent_op(context)
              if isinstance(cmd, sd.AlterObjectFragment) else cmd)

        make_coll = type_ref.as_create_delta(schema)
        op.add_prerequisite(make_coll)
        schema = make_coll.apply(schema, context)

    if isinstance(type_ref, TypeShell):
        try:
            type_ref.resolve(schema)
        except errors.InvalidReferenceError as e:
            refname = type_ref.get_refname(schema)
            if refname is not None:
                utils.enrich_schema_lookup_error(
                    e,
                    refname,
                    modaliases=context.modaliases,
                    schema=schema,
                    item_type=Type,
                    context=srcctx,
                )
            raise
    elif not isinstance(type_ref, Type):
        raise AssertionError(
            f'unexpected value in type attribute {attrname!r} of '
            f'{cmd.get_verbosename()}: {type_ref!r}'
        )

    return schema
