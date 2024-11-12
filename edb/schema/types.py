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

import collections
import collections.abc
import enum
import typing
import uuid

from edb import errors

from edb.common import checked

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import compiler as qlcompiler

from . import abc as s_abc
from . import annos as s_anno
from . import casts as s_casts
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as s_name
from . import objects as so
from . import schema as s_schema
from . import utils

if typing.TYPE_CHECKING:
    from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional
    from typing import AbstractSet, Sequence, Union, Callable
    from edb.common import parsing


MAX_TYPE_DISTANCE = 1_000_000_000


class ExprType(enum.IntEnum):
    """Enumeration to identify the type of an expression in aliases."""
    Select = enum.auto()
    Insert = enum.auto()
    Update = enum.auto()
    Delete = enum.auto()
    Group = enum.auto()

    def is_update(self) -> bool:
        return self == ExprType.Update

    def is_insert(self) -> bool:
        return self == ExprType.Insert

    def is_mutation(self) -> bool:
        return self != ExprType.Select and self != ExprType.Group


TypeT = typing.TypeVar('TypeT', bound='Type')
TypeT_co = typing.TypeVar('TypeT_co', bound='Type', covariant=True)
InheritingTypeT = typing.TypeVar('InheritingTypeT', bound='InheritingType')
CollectionTypeT = typing.TypeVar('CollectionTypeT', bound='Collection')
CollectionTypeT_co = typing.TypeVar(
    'CollectionTypeT_co', bound='Collection', covariant=True)
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

    # True for views.  This should always match the value of
    # `bool(expr_type)`, but can be exported in the introspection
    # schema without revealing weird internals.
    from_alias = so.SchemaField(
        bool,
        default=False,
        # cannot alter type from being produced by an alias into an actual type
        compcoef=0.0,
    )

    # True when from a global. The purpose of this is to ensure that
    # the types from globals and aliases can't be migrated between
    # each other.
    from_global = so.SchemaField(
        bool,
        default=False, compcoef=0.2)

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

    # True for types that cannot be persistently stored.
    # See std::fts::document for an example.
    transient = so.SchemaField(bool, default=False)

    def compare(
        self,
        other: so.Object,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        # We need to be able to compare objects and scalars, in some places
        if (
            isinstance(other, Type)
            and not isinstance(other, self.__class__)
            and not isinstance(self, other.__class__)
        ):
            return 0.0

        return super().compare(
            other,
            our_schema=our_schema, their_schema=their_schema, context=context)

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
        transient: bool = False,
        preserve_endpoint_ptrs: bool = False,
        inheritance_refdicts: Optional[AbstractSet[str]] = None,
        stdmode: bool = False,
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
        derived_attrs['from_alias'] = bool(derived_attrs.get('expr_type'))

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
            stdmode=stdmode,
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
                if not preserve_endpoint_ptrs:
                    context.current().slim_links = True

            delta.add(cmd)
            schema = delta.apply(schema, context)

        derived = typing.cast(TypeT, schema.get(name))

        return schema, derived

    def is_object_type(self) -> bool:
        return False

    def is_free_object_type(self, schema: s_schema.Schema) -> bool:
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

    def is_anyobject(self, schema: s_schema.Schema) -> bool:
        return False

    def is_scalar(self) -> bool:
        return False

    def is_collection(self) -> bool:
        return False

    def is_array(self) -> bool:
        return False

    def is_json(self, schema: s_schema.Schema) -> bool:
        return False

    def is_tuple(self, schema: s_schema.Schema) -> bool:
        return False

    def is_range(self) -> bool:
        return False

    def is_multirange(self) -> bool:
        return False

    def is_enum(self, schema: s_schema.Schema) -> bool:
        return False

    def is_sequence(self, schema: s_schema.Schema) -> bool:
        return False

    def is_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return False

    def find_predicate(
        self,
        pred: Callable[[Type], bool],
        schema: s_schema.Schema,
    ) -> Optional[Type]:
        if pred(self):
            return self
        else:
            return None

    def contains_predicate(
        self,
        pred: Callable[[Type], bool],
        schema: s_schema.Schema,
    ) -> bool:
        return bool(self.find_predicate(pred, schema))

    def find_generic(self, schema: s_schema.Schema) -> Optional[Type]:
        return self.find_predicate(
            lambda x: x.is_any(schema) or x.is_anyobject(schema), schema
        )

    def contains_object(self, schema: s_schema.Schema) -> bool:
        return self.contains_predicate(lambda x: x.is_object_type(), schema)

    def contains_json(self, schema: s_schema.Schema) -> bool:
        return self.contains_predicate(lambda x: x.is_json(schema), schema)

    def find_array(self, schema: s_schema.Schema) -> Optional[Type]:
        return self.find_predicate(lambda x: x.is_array(), schema)

    def contains_array_of_tuples(self, schema: s_schema.Schema) -> bool:
        return self.contains_predicate(
            lambda x: x.is_array_of_tuples(schema), schema)

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
        if poly.is_anyobject(schema) and self.is_object_type():
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
        return self.get_from_alias(schema)

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
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[Type]]:
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

    def as_shell(
        self: TypeT,
        schema: s_schema.Schema,
    ) -> TypeShell[TypeT]:
        name = typing.cast(s_name.QualName, self.get_name(schema))

        if (
            (union_of := self.get_union_of(schema))
            and not self.is_view(schema)
        ):
            assert isinstance(self, so.QualifiedObject)
            return UnionTypeShell(
                module=name.module,
                components=[
                    o.as_shell(schema) for o in union_of.objects(schema)
                ],
                opaque=self.get_is_opaque_union(schema),
                schemaclass=type(self),
            )
        elif (
            (intersection_of := self.get_intersection_of(schema))
            and not self.is_view(schema)
        ):
            assert isinstance(self, so.QualifiedObject)
            return IntersectionTypeShell(
                module=name.module,
                components=[
                    o.as_shell(schema) for o in intersection_of.objects(schema)
                ],
                schemaclass=type(self),
            )
        else:
            return TypeShell(
                name=name,
                schemaclass=type(self),
            )

    def record_cmd_object_aux_data(
        self,
        schema: s_schema.Schema,
        cmd: sd.ObjectCommand[Type],
    ) -> None:
        super().record_cmd_object_aux_data(schema, cmd)
        if self.is_compound_type(schema):
            cmd.set_object_aux_data('is_compound_type', True)

    def as_type_delete_if_unused(
        self: TypeT,
        schema: s_schema.Schema,
    ) -> Optional[sd.DeleteObject[TypeT]]:
        """If this is type is owned by other objects, delete it if unused.

        For types that get created behind the scenes as part of
        another object, such as collection types and union types, this
        should generate an appropriate deletion. Otherwise, it should
        return None.
        """

        return None

    def _is_deletable(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        # this type was already deleted by some other op
        # (probably alias types cleanup)
        return schema.get_by_id(self.id, default=None) is not None


class QualifiedType(so.QualifiedObject, Type):
    pass


class InheritingType(so.DerivableInheritingObject, QualifiedType):

    def material_type(
        self: InheritingTypeT,
        schema: s_schema.Schema_T,
    ) -> typing.Tuple[s_schema.Schema_T, InheritingTypeT]:
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

        ancestors = utils.get_class_nearest_common_ancestors(
            schema, [self, other])

        if not ancestors:
            return -1
        elif self in ancestors:
            return 0
        else:
            all_ancestors = list(self.get_ancestors(schema).objects(schema))
            return min(
                all_ancestors.index(ancestor) + 1 for ancestor in ancestors)


class TypeShell(so.ObjectShell[TypeT_co]):

    schemaclass: typing.Type[TypeT_co]
    extra_args: tuple[qlast.Expr | qlast.TypeExpr, ...] | None

    def __init__(
        self,
        *,
        name: s_name.Name,
        origname: Optional[s_name.Name] = None,
        displayname: Optional[str] = None,
        expr: Optional[str] = None,
        schemaclass: typing.Type[TypeT_co],
        sourcectx: Optional[parsing.Span] = None,
        extra_args: tuple[qlast.Expr] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            origname=origname,
            displayname=displayname,
            schemaclass=schemaclass,
            sourcectx=sourcectx,
        )

        self.expr = expr
        self.extra_args = extra_args

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return self.resolve(schema).is_polymorphic(schema)

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.Command:
        raise errors.UnsupportedFeatureError(
            f'unsupported type intersection in schema {str(view_name)}',
            hint=f'Type intersections are currently '
                 f'unsupported as valid link targets.',
            span=self.sourcectx,
        )


class TypeExprShell(TypeShell[TypeT_co]):

    components: typing.Tuple[TypeShell[TypeT_co], ...]

    def __init__(
        self,
        *,
        name: s_name.Name,
        components: Iterable[TypeShell[TypeT_co]],
        schemaclass: typing.Type[TypeT_co],
        sourcectx: Optional[parsing.Span] = None,
    ) -> None:
        super().__init__(
            name=name,
            schemaclass=schemaclass,
            sourcectx=sourcectx,
        )
        self.components = tuple(components)

    def resolve_components(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeT_co, ...]:
        return tuple(c.resolve(schema) for c in self.components)

    def get_components(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[TypeT_co], ...]:
        return self.components


class UnionTypeShell(TypeExprShell[TypeT_co]):

    def __init__(
        self,
        *,
        module: str,
        components: Iterable[TypeShell[TypeT_co]],
        opaque: bool = False,
        schemaclass: typing.Type[TypeT_co],
        sourcectx: Optional[parsing.Span] = None,
    ) -> None:
        name = get_union_type_name(
            (c.name for c in components),
            opaque=opaque,
            module=module,
        )
        super().__init__(
            name=name,
            components=components,
            schemaclass=schemaclass,
            sourcectx=sourcectx,
        )
        self.opaque = opaque

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.Command:
        assert isinstance(self.name, s_name.QualName)
        cmd = CreateUnionType(classname=self.name)
        cmd.set_attribute_value('name', self.name)
        cmd.set_attribute_value('components', tuple(self.components))
        cmd.set_attribute_value('is_opaque_union', self.opaque)
        cmd.set_attribute_value('span', self.sourcectx)
        return cmd

    def __repr__(self) -> str:
        dn = 'UnionType'
        comps = ' | '.join(repr(c) for c in self.components)
        return f'<{type(self).__name__} {dn}({comps}) at 0x{id(self):x}>'


class AlterType(sd.AlterObject[TypeT]):

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if hasattr(self, 'scls') and self.scls.get_from_alias(schema):
            # This is a nested view type, e.g
            # __FooAlias_bar produced by  FooAlias := (SELECT Foo { bar: ... })
            # and should obviously not appear as a top level definition.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class RenameType(sd.RenameObject[TypeT]):

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: TypeT,
    ) -> None:
        super()._canonicalize(schema, context, scls)

        # Now, see if there are any compound or collection types using
        # this type as a component.  We must rename them, as they derive
        # their names from the names of their component types.
        # We must be careful about the order in which we consider the
        # referrers, because they may reference each other as well, and
        # so we must proceed with renames starting from the simplest type.
        referrers = collections.defaultdict(set)
        referrer_map = schema.get_referrers_ex(scls, scls_type=Type)
        for (_, field_name), objs in referrer_map.items():
            for obj in objs:
                referrers[obj].add(field_name)

        ref_order = sd.sort_by_cross_refs(schema, referrers)

        for ref_type in ref_order:
            field_names = referrers[ref_type]
            for field_name in field_names:
                if field_name == 'union_of' or field_name == 'intersection_of':
                    orig_ref_type_name = ref_type.get_name(schema)
                    assert isinstance(orig_ref_type_name, s_name.QualName)
                    components = ref_type.get_field_value(
                        schema, field_name)
                    assert components is not None
                    component_names = set(components.names(schema))
                    component_names.discard(self.classname)
                    component_names.add(self.new_name)

                    if field_name == 'union_of':
                        new_ref_type_name = get_union_type_name(
                            component_names,
                            module=orig_ref_type_name.module,
                            opaque=ref_type.get_is_opaque_union(schema),
                        )
                    else:
                        new_ref_type_name = get_intersection_type_name(
                            component_names,
                            module=orig_ref_type_name.module,
                        )

                    self.add(self.init_rename_branch(
                        ref_type,
                        new_ref_type_name,
                        schema=schema,
                        context=context,
                    ))
                elif (
                    isinstance(ref_type, Tuple)
                    and field_name == 'element_types'
                ):
                    subtypes = {
                        k: st.get_name(schema)
                        for k, st in (
                            ref_type.get_element_types(schema).items(schema)
                        )
                    }
                    new_tup_type_name = Tuple.generate_name(
                        subtypes,
                        named=ref_type.is_named(schema),
                    )
                    self.add(self.init_rename_branch(
                        ref_type,
                        new_tup_type_name,
                        schema=schema,
                        context=context,
                    ))
                elif (
                    isinstance(ref_type, Array)
                    and field_name == 'element_type'
                ):
                    new_arr_type_name = Array.generate_name(
                        ref_type.get_element_type(schema).get_name(schema)
                    )
                    self.add(self.init_rename_branch(
                        ref_type,
                        new_arr_type_name,
                        schema=schema,
                        context=context,
                    ))

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if (
            self.maybe_get_object_aux_data('is_compound_type')
            or self.scls.is_view(schema)
        ):
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class DeleteType(sd.DeleteObject[TypeT]):

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.maybe_get_object_aux_data('is_compound_type'):
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class RenameInheritingType(
    RenameType[InheritingTypeT],
    inheriting.RenameInheritingObject[InheritingTypeT],
):
    pass


class DeleteInheritingType(
    DeleteType[InheritingTypeT],
    inheriting.DeleteInheritingObject[InheritingTypeT],
):
    pass


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

        from edb.schema import types as s_types

        if not context.canonical:
            components: Sequence[s_types.Type] = [
                c.resolve(schema)
                for c in self.get_attribute_value('components')
            ]

            try:
                new_schema, union_type, created = utils.ensure_union_type(
                    schema,
                    components,
                    opaque=self.get_attribute_value('is_opaque_union') or False,
                    module=self.classname.module,
                )
            except errors.SchemaError as e:
                union_name = (
                    '(' + ' | '.join(sorted(
                    c.get_displayname(schema)
                    for c in components
                    )) + ')'
                )
                e.args = (
                    (f'cannot create union {union_name} {e.args[0]}',)
                    + e.args[1:]
                )
                e.set_span(self.get_attribute_value('span'))
                raise e

            if created:
                delta = union_type.as_create_delta(
                    schema=new_schema,
                    context=so.ComparisonContext(),
                )

                self.add(delta)

        for cmd in self.get_subcommands():
            schema = cmd.apply(schema, context)

        return schema


class IntersectionTypeShell(TypeExprShell[TypeT_co]):
    def __init__(
        self,
        *,
        module: str,
        components: Iterable[TypeShell[TypeT_co]],
        schemaclass: type[TypeT_co],
        sourcectx: parsing.Span | None = None,
    ) -> None:
        name = get_intersection_type_name(
            (c.name for c in components),
            module=module,
        )

        super().__init__(
            name=name,
            components=components,
            schemaclass=schemaclass,
            sourcectx=sourcectx
        )


_collection_impls: Dict[str, typing.Type[Collection]] = {}


class Collection(Type, s_abc.Collection):

    _schema_name: typing.ClassVar[typing.Optional[str]] = None

    #: True for collection types that are stored in schema persistently
    is_persistent = so.SchemaField(
        bool,
        default=False,
        compcoef=None,
    )

    def __init_subclass__(
        cls,
        *,
        schema_name: typing.Optional[str] = None,
    ) -> None:
        super().__init_subclass__()
        if schema_name is not None:
            if existing := _collection_impls.get(schema_name):
                raise TypeError(
                    f"{schema_name} is already implemented by {existing}")
            _collection_impls[schema_name] = cls
            cls._schema_name = schema_name

    def as_create_delta(
        self: CollectionTypeT,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.CreateObject[CollectionTypeT]:
        delta = super().as_create_delta(schema=schema, context=context)
        assert isinstance(delta, sd.CreateObject)
        if not isinstance(self, CollectionExprAlias):
            delta.if_not_exists = True
        return delta

    def as_delete_delta(
        self: CollectionTypeT,
        *,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[CollectionTypeT]:
        delta = super().as_delete_delta(schema=schema, context=context)
        assert isinstance(delta, sd.DeleteObject)
        if not isinstance(self, CollectionExprAlias):
            delta.if_exists = True
            if not (
                isinstance(self, Array)
                and self.get_element_type(schema).is_scalar()
            ):
                # Arrays of scalars are special, because we create them
                # implicitly and overload reference checks to never
                # delete them unless the scalar is also deleted.
                delta.if_unused = True
        return delta

    @classmethod
    def get_displayname_static(cls, name: s_name.Name) -> str:
        if isinstance(name, s_name.QualName):
            return str(name)
        else:
            return s_name.unmangle_name(str(name))

    @classmethod
    def get_schema_name(cls) -> str:
        if cls._schema_name is None:
            raise TypeError(
                f"{cls.get_schema_class_displayname()} is not "
                f"a concrete collection type"
            )
        return cls._schema_name

    def get_generated_name(self, schema: s_schema.Schema) -> s_name.UnqualName:
        """Return collection type name generated from element types.

        Unlike get_name(), which might return a custom name, this will always
        return a name derived from the names of the collection element type(s).
        """
        raise NotImplementedError

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(st.is_polymorphic(schema)
                   for st in self.get_subtypes(schema))

    def find_predicate(
        self,
        pred: Callable[[Type], bool],
        schema: s_schema.Schema,
    ) -> Optional[Type]:
        if pred(self):
            return self
        for st in self.get_subtypes(schema):
            res = st.find_predicate(pred, schema)
            if res is not None:
                return res

        return None

    def is_collection(self) -> bool:
        return True

    def get_common_parent_type_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
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
        if isinstance(parent, Type) and parent.is_anyobject(schema):
            if isinstance(self, Type) and self.is_object_type():
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

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        raise NotImplementedError

    def get_typemods(self, schema: s_schema.Schema) -> Any:
        return ()

    @classmethod
    def get_class(cls, schema_name: str) -> typing.Type[Collection]:
        coll_type = _collection_impls.get(schema_name)
        if coll_type:
            return coll_type
        else:
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

    # We define this specifically to override children
    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'collection'

    def as_type_delete_if_unused(
        self: CollectionTypeT,
        schema: s_schema.Schema,
    ) -> Optional[sd.DeleteObject[CollectionTypeT]]:
        if not self._is_deletable(schema):
            return None

        return self.init_delta_command(
            schema,
            sd.DeleteObject,
            if_unused=True,
            if_exists=True,
        )


Dimensions = checked.FrozenCheckedList[int]
Array_T = typing.TypeVar("Array_T", bound="Array")
Array_T_co = typing.TypeVar("Array_T_co", bound="Array", covariant=True)


class CollectionTypeShell(TypeShell[CollectionTypeT_co]):

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[Type], ...]:
        raise NotImplementedError

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(
            st.is_polymorphic(schema) for st in self.get_subtypes(schema)
        )


class CollectionExprAlias(QualifiedType, Collection):

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'expression alias'

    @classmethod
    def get_underlying_schema_class(cls) -> typing.Type[Collection]:
        """Return the concrete collection class for this ExprAlias class."""
        raise NotImplementedError

    def as_underlying_type_delete_if_unused(
        self,
        schema: s_schema.Schema,
    ) -> sd.DeleteObject[Type]:
        """Return a conditional deletion command for the underlying type object
        """
        return sd.get_object_delta_command(
            objtype=type(self).get_underlying_schema_class(),
            cmdtype=sd.DeleteObject,
            schema=schema,
            name=self.get_generated_name(schema),
            if_unused=True,
            if_exists=True,
        )

    def as_type_delete_if_unused(
        self: CollectionExprAliasT,
        schema: s_schema.Schema,
    ) -> Optional[sd.DeleteObject[CollectionExprAliasT]]:
        if not self._is_deletable(schema):
            return None

        cmd = self.init_delta_command(schema, sd.DeleteObject, if_exists=True)
        cmd.add_prerequisite(self.as_underlying_type_delete_if_unused(schema))
        return cmd


class Array(
    Collection,
    s_abc.Array,
    qlkind=qltypes.SchemaObjectClass.ARRAY_TYPE,
    schema_name='array',
):

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
    def generate_name(
        cls,
        element_name: s_name.Name,
    ) -> s_name.UnqualName:
        return s_name.UnqualName(
            f'array<{s_name.mangle_name(str(element_name))}>',
        )

    @classmethod
    def create(
        cls: typing.Type[Array_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Optional[uuid.UUID] = None,
        dimensions: Sequence[int] = (),
        element_type: Any,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Array_T]:
        if not dimensions:
            dimensions = [-1]

        if dimensions != [-1]:
            raise errors.UnsupportedFeatureError(
                f'multi-dimensional arrays are not supported')

        if name is None:
            name = cls.generate_name(element_type.get_name(schema))

        if isinstance(name, s_name.QualName):
            result = schema.get(name, type=cls, default=None)
        else:
            result = schema.get_global(cls, name, default=None)

        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                element_type=element_type,
                dimensions=dimensions,
                **kwargs,
            )
            # Compute material type so that we can retrieve it safely later
            schema, _ = result.material_type(schema)

        return schema, result

    def get_generated_name(self, schema: s_schema.Schema) -> s_name.UnqualName:
        return type(self).generate_name(
            self.get_element_type(schema).get_name(schema),
        )

    def is_array_of_tuples(self, schema: s_schema.Schema) -> bool:
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
    ) -> typing.Tuple[s_schema.Schema, ArrayExprAlias]:
        assert not kwargs
        return ArrayExprAlias.from_subtypes(
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
            from . import scalars as s_scalars

            if not isinstance(other, s_scalars.ScalarType):
                return False
            if other.is_polymorphic(schema):
                return False
            right = other.get_base_for_cast(schema)
            return s_casts.is_assignment_castable(schema, self, right)

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, Array):
            from . import scalars as s_scalars

            if not isinstance(other, s_scalars.ScalarType):
                return False
            if other.is_polymorphic(schema):
                return False
            right = other.get_base_for_cast(schema)
            return s_casts.is_assignment_castable(schema, self, right)

        return self.get_element_type(schema).castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[Array]]:

        if not isinstance(other, Array):
            return schema, None

        if self == other:
            return schema, self

        my_el = self.get_element_type(schema)
        schema, subtype = my_el.find_common_implicitly_castable_type(
            other.get_element_type(schema), schema)

        if subtype is None:
            return schema, None

        return Array.from_subtypes(schema, [subtype])

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
        st = self.get_subtypes(schema=schema)[0]
        # TODO: maybe we should have a generic nested polymorphic algo?
        if isinstance(st, (Range, MultiRange)):
            schema, newst = st.to_nonpolymorphic(schema, concrete_type)
        else:
            newst = concrete_type

        return Array.from_subtypes(schema, (newst,))

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

        schema, ty = cls.create(
            schema,
            element_type=stype,
            dimensions=dimensions,
            name=name,
            **kwargs,
        )
        return schema, ty

    @classmethod
    def create_shell(
        cls: typing.Type[Array_T],
        schema: s_schema.Schema,
        *,
        subtypes: Sequence[TypeShell[Type]],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
        expr: Optional[str] = None,
    ) -> ArrayTypeShell[Array_T]:
        if not typemods:
            typemods = ([-1],)

        st = next(iter(subtypes))

        return ArrayTypeShell(
            subtype=st,
            typemods=typemods,
            name=name,
            expr=expr,
            schemaclass=cls,
        )

    def as_shell(
        self: Array_T,
        schema: s_schema.Schema,
    ) -> ArrayTypeShell[Array_T]:
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
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Array]:
        # We need to resolve material types based on the subtype recursively.

        st = self.get_element_type(schema)
        schema, stm = st.material_type(schema)
        if stm != st or isinstance(self, ArrayExprAlias):
            return Array.from_subtypes(
                schema,
                [stm],
                typemods=self.get_typemods(schema),
            )
        else:
            return (schema, self)


class ArrayTypeShell(CollectionTypeShell[Array_T_co]):

    schemaclass: typing.Type[Array_T_co]

    def __init__(
        self,
        *,
        name: Optional[s_name.Name],
        expr: Optional[str] = None,
        subtype: TypeShell[Type],
        typemods: typing.Tuple[typing.Any, ...],
        schemaclass: typing.Type[Array_T_co],
    ) -> None:
        if name is None:
            name = schemaclass.generate_name(subtype.name)

        super().__init__(name=name, schemaclass=schemaclass, expr=expr)
        self.subtype = subtype
        self.typemods = typemods

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[Type], ...]:
        return (self.subtype,)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return f'array<{self.subtype.get_displayname(schema)}>'

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ca: Union[CreateArray, CreateArrayExprAlias]
        cmd = sd.CommandGroup()
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
        if isinstance(el, CollectionTypeShell):
            cmd.add(el.as_create_delta(schema))

        ca.set_attribute_value('name', ca.classname)
        ca.set_attribute_value('element_type', el)
        ca.set_attribute_value('is_persistent', True)
        ca.set_attribute_value('abstract', self.is_polymorphic(schema))
        ca.set_attribute_value('dimensions', self.typemods[0])

        if attrs:
            for k, v in attrs.items():
                ca.set_attribute_value(k, v)

        cmd.add(ca)

        return cmd


class ArrayExprAlias(
    CollectionExprAlias,
    Array,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete Array).

    @classmethod
    def get_underlying_schema_class(cls) -> typing.Type[Collection]:
        return Array


Tuple_T = typing.TypeVar('Tuple_T', bound='Tuple')
Tuple_T_co = typing.TypeVar('Tuple_T_co', bound='Tuple', covariant=True)


class Tuple(
    Collection,
    s_abc.Tuple,
    qlkind=qltypes.SchemaObjectClass.TUPLE_TYPE,
    schema_name='tuple',
):

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
    def generate_name(
        cls,
        element_names: Mapping[str, s_name.Name],
        named: bool = False,
    ) -> s_name.UnqualName:
        if named:
            st_names = ', '.join(
                f'{n}:{st}' for n, st in element_names.items()
            )
        else:
            st_names = ', '.join(str(st) for st in element_names.values())

        return s_name.UnqualName(f'tuple<{s_name.mangle_name(st_names)}>')

    @classmethod
    def create(
        cls: typing.Type[Tuple_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Optional[uuid.UUID] = None,
        element_types: Mapping[str, Type],
        named: bool = False,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Tuple_T]:
        el_types = so.ObjectDict[str, Type].create(schema, element_types)
        if name is None:
            name = cls.generate_name(
                {n: el.get_name(schema) for n, el in element_types.items()},
                named,
            )

        if isinstance(name, s_name.QualName):
            result = schema.get(name, type=cls, default=None)
        else:
            result = schema.get_global(cls, name, default=None)

        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                named=named,
                element_types=el_types,
                **kwargs,
            )
            # Compute material type so that we can retrieve it safely later
            schema, _ = result.material_type(schema)

        return schema, result

    def get_generated_name(self, schema: s_schema.Schema) -> s_name.UnqualName:
        els = {n: st.get_name(schema) for n, st in self.iter_subtypes(schema)}
        return type(self).generate_name(els, self.is_named(schema))

    def get_displayname(self, schema: s_schema.Schema) -> str:
        if self.is_named(schema):
            st_names = ', '.join(
                f'{name}: {st.get_displayname(schema)}'
                for name, st in self.get_element_types(schema).items(schema)
            )
        else:
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
    ) -> typing.Tuple[s_schema.Schema, TupleExprAlias]:
        assert not kwargs
        return TupleExprAlias.from_subtypes(
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
        schema, ty = cls.create(
            schema, element_types=types, named=named, name=name, **kwargs)
        return schema, ty

    @classmethod
    def create_shell(
        cls: typing.Type[Tuple_T],
        schema: s_schema.Schema,
        *,
        subtypes: Mapping[str, TypeShell[Type]],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
    ) -> TupleTypeShell[Tuple_T]:
        return TupleTypeShell(
            subtypes=subtypes,
            typemods=typemods,
            name=name,
            schemaclass=cls,
        )

    def as_shell(
        self: Tuple_T,
        schema: s_schema.Schema,
    ) -> TupleTypeShell[Tuple_T]:
        stshells: Dict[str, TypeShell[Type]] = {}

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

        if (
            self.is_named(schema)
            and other.is_named(schema)
            and (self.get_element_names(schema)
                 != other.get_element_names(schema))
        ):
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

        if (
            self.is_named(schema)
            and other.is_named(schema)
            and (self.get_element_names(schema)
                 != other.get_element_names(schema))
        ):
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

        if (
            self.is_named(schema)
            and other.is_named(schema)
            and (self.get_element_names(schema)
                 != other.get_element_names(schema))
        ):
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
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[Tuple]]:

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
                return Tuple.from_subtypes(
                    schema, dict(zip(my_names, new_types)), {"named": True}
                )

        return Tuple.from_subtypes(schema, new_types)

    def get_typemods(self, schema: s_schema.Schema) -> Dict[str, bool]:
        return {'named': self.is_named(schema)}

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
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Tuple]:
        # We need to resolve material types of all the subtypes recursively.
        new_material_type = False
        subtypes = {}

        for st_name, st in self.iter_subtypes(schema):
            schema, stm = st.material_type(schema)
            if stm != st:
                new_material_type = True
            subtypes[st_name] = stm

        if new_material_type or isinstance(self, TupleExprAlias):
            return Tuple.from_subtypes(
                schema, subtypes, typemods=self.get_typemods(schema))
        else:
            return schema, self


class TupleTypeShell(CollectionTypeShell[Tuple_T_co]):

    schemaclass: typing.Type[Tuple_T_co]

    def __init__(
        self,
        *,
        name: Optional[s_name.Name],
        subtypes: Mapping[str, TypeShell[Type]],
        typemods: Any = None,
        schemaclass: typing.Type[Tuple_T_co],
    ) -> None:
        if name is None:
            named = typemods is not None and typemods.get('named', False)
            name = schemaclass.generate_name(
                {n: st.name for n, st in subtypes.items()},
                named,
            )

        super().__init__(name=name, schemaclass=schemaclass)
        self.subtypes = subtypes
        self.typemods = typemods

    def get_displayname(self, schema: s_schema.Schema) -> str:
        st_names = ', '.join(st.get_displayname(schema)
                             for st in self.get_subtypes(schema))
        return f'tuple<{st_names}>'

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[Type], ...]:
        return tuple(self.subtypes.values())

    def iter_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> Iterator[typing.Tuple[str, TypeShell[Type]]]:
        return iter(self.subtypes.items())

    def is_named(self) -> bool:
        return self.typemods is not None and self.typemods.get('named', False)

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> Union[CreateTuple, CreateTupleExprAlias]:
        ct: Union[CreateTuple, CreateTupleExprAlias]

        plain_tuple = self._as_plain_create_delta(schema)
        if view_name is None:
            ct = plain_tuple
        else:
            ct = CreateTupleExprAlias(classname=view_name)
            self._populate_create_delta(schema, ct, attrs=attrs)

        for el in self.subtypes.values():
            if isinstance(el, CollectionTypeShell):
                ct.add_prerequisite(el.as_create_delta(schema))

        if view_name is not None:
            ct.add_prerequisite(plain_tuple)

        return ct

    def _as_plain_create_delta(
        self,
        schema: s_schema.Schema,
    ) -> CreateTuple:
        name = self.schemaclass.generate_name(
            {n: st.get_name(schema) for n, st in self.subtypes.items()},
            self.is_named(),
        )
        ct = CreateTuple(classname=name, if_not_exists=True)
        self._populate_create_delta(schema, ct)
        return ct

    def _populate_create_delta(
        self,
        schema: s_schema.Schema,
        ct: Union[CreateTuple, CreateTupleExprAlias],
        *,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> None:
        named = self.is_named()
        ct.set_attribute_value('name', ct.classname)
        ct.set_attribute_value('named', named)
        ct.set_attribute_value('abstract', self.is_polymorphic(schema))
        ct.set_attribute_value('is_persistent', True)
        ct.set_attribute_value('element_types', self.subtypes)

        if attrs:
            for k, v in attrs.items():
                ct.set_attribute_value(k, v)


class TupleExprAlias(
    CollectionExprAlias,
    Tuple,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete Tuple).

    @classmethod
    def get_underlying_schema_class(cls) -> typing.Type[Collection]:
        return Tuple


Range_T = typing.TypeVar('Range_T', bound='Range')
Range_T_co = typing.TypeVar('Range_T_co', bound='Range', covariant=True)


class Range(
    Collection,
    s_abc.Range,
    qlkind=qltypes.SchemaObjectClass.RANGE_TYPE,
    schema_name='range',
):

    element_type = so.SchemaField(
        Type,
        # We want a low compcoef so that range types are *never* altered.
        compcoef=0,
    )

    @classmethod
    def generate_name(
        cls,
        element_name: s_name.Name,
    ) -> s_name.UnqualName:
        return s_name.UnqualName(
            f'range<{s_name.mangle_name(str(element_name))}>',
        )

    @classmethod
    def create(
        cls: typing.Type[Range_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Optional[uuid.UUID] = None,
        element_type: Any,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Range_T]:
        if name is None:
            name = cls.generate_name(element_type.get_name(schema))

        if isinstance(name, s_name.QualName):
            result = schema.get(name, type=cls, default=None)
        else:
            result = schema.get_global(cls, name, default=None)

        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                element_type=element_type,
                **kwargs,
            )

        return schema, result

    def get_generated_name(self, schema: s_schema.Schema) -> s_name.UnqualName:
        return type(self).generate_name(
            self.get_element_type(schema).get_name(schema),
        )

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return (
            f'range<{self.get_element_type(schema).get_displayname(schema)}>')

    def is_range(self) -> bool:
        return True

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.QualName,
        attrs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, RangeExprAlias]:
        assert not kwargs
        return RangeExprAlias.from_subtypes(
            schema,
            [self.get_element_type(schema)],
            self.get_typemods(schema),
            name=name,
            **(attrs or {}),
        )

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        return (self.get_element_type(schema),)

    def implicitly_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        if not isinstance(other, (Range, MultiRange)):
            return False

        my_el = self.get_element_type(schema)
        other_el = other.get_element_type(schema)

        if isinstance(other, MultiRange):
            # Only valid implicit cast to multirange is the one that preserves
            # the element type.
            return my_el.issubclass(schema, other_el)

        return my_el.implicitly_castable_to(other_el, schema)

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        if not isinstance(other, (Range, MultiRange)):
            return -1

        extra = 1 if isinstance(other, MultiRange) else 0
        return self.get_element_type(schema).get_implicit_cast_distance(
            other.get_element_type(schema), schema) + extra

    def assignment_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, (Range, MultiRange)):
            return False

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, (Range, MultiRange)):
            return False

        return self.get_element_type(schema).castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[RangeLike]]:

        if not isinstance(other, (Range, MultiRange)):
            return schema, None

        if self == other:
            return schema, self

        my_el = self.get_element_type(schema)
        other_el = other.get_element_type(schema)

        if (
            isinstance(other, MultiRange)
            and not my_el.issubclass(schema, other_el)
        ):
            # Only valid implicit cast to multirange is the one that preserves
            # the element type.
            return schema, None

        schema, subtype = my_el.find_common_implicitly_castable_type(
            other_el, schema)

        if subtype is None:
            return schema, None

        # Implicitly castable target is based on the `other` subtype because
        # it may be Range or MultiRange. We also need to account for
        # CollectionExprAlias.
        if isinstance(other, CollectionExprAlias):
            other_t = other.get_underlying_schema_class()
            # Keeps mypy happy, even though these have to be exactly one of
            # those two types and not merely subclasses.
            assert issubclass(other_t, (Range, MultiRange))
        else:
            other_t = type(other)

        # mypy is not happy even if I try issubclass or a cast for the result
        # of get_underlying_schema_class, so I'm casting the return here
        # return typing.cast(typing.Tuple[s_schema.Schema, RangeLike],
        #                    other_t.from_subtypes(schema, [subtype]))
        return other_t.from_subtypes(schema, [subtype])

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> Optional[Type]:
        if not isinstance(concrete_type, Range):
            return None

        return self.get_element_type(schema).resolve_polymorphic(
            schema, concrete_type.get_element_type(schema))

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> typing.Tuple[s_schema.Schema, Range]:
        return Range.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        if other.is_any(schema):
            return True

        if not isinstance(other, (Range, MultiRange)):
            return False

        return self.get_element_type(schema).test_polymorphic(
            schema, other.get_element_type(schema))

    @classmethod
    def from_subtypes(
        cls: typing.Type[Range_T],
        schema: s_schema.Schema,
        subtypes: Sequence[Type],
        typemods: Any = None,
        *,
        name: Optional[s_name.QualName] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, Range_T]:
        if len(subtypes) != 1:
            raise errors.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]
        anypoint = schema.get('std::anypoint', type=Type)

        if not stype.issubclass(schema, anypoint):
            raise errors.UnsupportedFeatureError(
                f'unsupported range subtype: {stype.get_displayname(schema)}'
            )

        return cls.create(
            schema,
            element_type=stype,
            name=name,
            **kwargs,
        )

    @classmethod
    def create_shell(
        cls: typing.Type[Range_T],
        schema: s_schema.Schema,
        *,
        subtypes: Sequence[TypeShell[Type]],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
    ) -> RangeTypeShell[Range_T]:
        st = next(iter(subtypes))

        return RangeTypeShell(
            subtype=st,
            typemods=typemods,
            name=name,
            schemaclass=cls,
        )

    def as_shell(
        self: Range_T,
        schema: s_schema.Schema,
    ) -> RangeTypeShell[Range_T]:
        return type(self).create_shell(
            schema,
            subtypes=[st.as_shell(schema) for st in self.get_subtypes(schema)],
            typemods=self.get_typemods(schema),
            name=self.get_name(schema),
        )

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Range]:
        # We need to resolve material types based on the subtype recursively.

        st = self.get_element_type(schema)
        schema, stm = st.material_type(schema)
        if stm != st or isinstance(self, RangeExprAlias):
            return Range.from_subtypes(
                schema,
                [stm],
                typemods=self.get_typemods(schema),
            )
        else:
            return (schema, self)


class RangeTypeShell(CollectionTypeShell[Range_T_co]):

    schemaclass: typing.Type[Range_T_co]

    def __init__(
        self,
        *,
        name: Optional[s_name.Name],
        subtype: TypeShell[Type],
        typemods: typing.Tuple[typing.Any, ...],
        schemaclass: typing.Type[Range_T_co],
    ) -> None:
        if name is None:
            name = schemaclass.generate_name(subtype.name)

        super().__init__(name=name, schemaclass=schemaclass)
        self.subtype = subtype
        self.typemods = typemods

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[Type], ...]:
        return (self.subtype,)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return f'range<{self.subtype.get_displayname(schema)}>'

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ca: Union[CreateRange, CreateRangeExprAlias]
        cmd = sd.CommandGroup()
        if view_name is None:
            ca = CreateRange(
                classname=self.get_name(schema),
                if_not_exists=True,
            )
        else:
            ca = CreateRangeExprAlias(
                classname=view_name,
            )

        el = self.subtype
        if isinstance(el, CollectionTypeShell):
            cmd.add(el.as_create_delta(schema))

        ca.set_attribute_value('name', ca.classname)
        ca.set_attribute_value('element_type', el)
        ca.set_attribute_value('is_persistent', True)
        ca.set_attribute_value('abstract', self.is_polymorphic(schema))

        if attrs:
            for k, v in attrs.items():
                ca.set_attribute_value(k, v)

        cmd.add(ca)

        return cmd


class RangeExprAlias(
    CollectionExprAlias,
    Range,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete Range).

    @classmethod
    def get_underlying_schema_class(cls) -> typing.Type[Collection]:
        return Range


MultiRange_T = typing.TypeVar('MultiRange_T', bound='MultiRange')
MultiRange_T_co = typing.TypeVar(
    'MultiRange_T_co', bound='MultiRange', covariant=True)


class MultiRange(
    Collection,
    s_abc.MultiRange,
    qlkind=qltypes.SchemaObjectClass.MULTIRANGE_TYPE,
    schema_name='multirange',
):

    element_type = so.SchemaField(
        Type,
        # We want a low compcoef so that multirange types are *never* altered.
        compcoef=0,
    )

    @classmethod
    def generate_name(
        cls,
        element_name: s_name.Name,
    ) -> s_name.UnqualName:
        return s_name.UnqualName(
            f'multirange<{s_name.mangle_name(str(element_name))}>',
        )

    @classmethod
    def create(
        cls: typing.Type[MultiRange_T],
        schema: s_schema.Schema,
        *,
        name: Optional[s_name.Name] = None,
        id: Optional[uuid.UUID] = None,
        element_type: Any,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, MultiRange_T]:
        if name is None:
            name = cls.generate_name(element_type.get_name(schema))

        if isinstance(name, s_name.QualName):
            result = schema.get(name, type=cls, default=None)
        else:
            result = schema.get_global(cls, name, default=None)

        if result is None:
            schema, result = super().create_in_schema(
                schema,
                id=id,
                name=name,
                element_type=element_type,
                **kwargs,
            )

        return schema, result

    def get_generated_name(self, schema: s_schema.Schema) -> s_name.UnqualName:
        return type(self).generate_name(
            self.get_element_type(schema).get_name(schema),
        )

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return f'''multirange<{self.get_element_type(schema)
                                   .get_displayname(schema)}>'''

    def is_multirange(self) -> bool:
        return True

    def derive_subtype(
        self,
        schema: s_schema.Schema,
        *,
        name: s_name.QualName,
        attrs: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, MultiRangeExprAlias]:
        assert not kwargs
        return MultiRangeExprAlias.from_subtypes(
            schema,
            [self.get_element_type(schema)],
            self.get_typemods(schema),
            name=name,
            **(attrs or {}),
        )

    def get_subtypes(self, schema: s_schema.Schema) -> typing.Tuple[Type, ...]:
        return (self.get_element_type(schema),)

    def implicitly_castable_to(
        self, other: Type, schema: s_schema.Schema
    ) -> bool:
        if not isinstance(other, MultiRange):
            return False

        return self.get_element_type(schema).implicitly_castable_to(
            other.get_element_type(schema), schema)

    def get_implicit_cast_distance(
        self, other: Type, schema: s_schema.Schema
    ) -> int:
        if not isinstance(other, MultiRange):
            return -1

        return self.get_element_type(schema).get_implicit_cast_distance(
            other.get_element_type(schema), schema)

    def assignment_castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, MultiRange):
            return False

        return self.get_element_type(schema).assignment_castable_to(
            other.get_element_type(schema), schema)

    def castable_to(
        self,
        other: Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, MultiRange):
            return False

        return self.get_element_type(schema).castable_to(
            other.get_element_type(schema), schema)

    def find_common_implicitly_castable_type(
        self: MultiRange,
        other: Type,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, Optional[MultiRange]]:

        if not isinstance(other, MultiRange):
            return schema, None

        if self == other:
            return schema, self

        my_el = self.get_element_type(schema)
        schema, subtype = my_el.find_common_implicitly_castable_type(
            other.get_element_type(schema), schema)

        if subtype is None:
            return schema, None

        return MultiRange.from_subtypes(schema, [subtype])

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> Optional[Type]:
        # polymorphic multiranges can resolve using concrete multiranges and
        # ranges because ranges are implicitly castable into multiranges.
        if not isinstance(concrete_type, (Range, MultiRange)):
            return None

        return self.get_element_type(schema).resolve_polymorphic(
            schema, concrete_type.get_element_type(schema))

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: Type,
    ) -> typing.Tuple[s_schema.Schema, MultiRange]:
        return MultiRange.from_subtypes(schema, (concrete_type,))

    def _test_polymorphic(self, schema: s_schema.Schema, other: Type) -> bool:
        if other.is_any(schema):
            return True

        if not isinstance(other, MultiRange):
            return False

        return self.get_element_type(schema).test_polymorphic(
            schema, other.get_element_type(schema))

    @classmethod
    def from_subtypes(
        cls: typing.Type[MultiRange_T],
        schema: s_schema.Schema,
        subtypes: Sequence[Type],
        typemods: Any = None,
        *,
        name: Optional[s_name.QualName] = None,
        **kwargs: Any,
    ) -> typing.Tuple[s_schema.Schema, MultiRange_T]:
        if len(subtypes) != 1:
            raise errors.SchemaError(
                f'unexpected number of subtypes, expecting 1: {subtypes!r}')
        stype = subtypes[0]
        anypoint = schema.get('std::anypoint', type=Type)

        if not stype.issubclass(schema, anypoint):
            raise errors.UnsupportedFeatureError(
                f'unsupported range subtype: {stype.get_displayname(schema)}'
            )

        return cls.create(
            schema,
            element_type=stype,
            name=name,
            **kwargs,
        )

    @classmethod
    def create_shell(
        cls: typing.Type[MultiRange_T],
        schema: s_schema.Schema,
        *,
        subtypes: Sequence[TypeShell[Type]],
        typemods: Any = None,
        name: Optional[s_name.Name] = None,
    ) -> MultiRangeTypeShell[MultiRange_T]:
        st = next(iter(subtypes))

        if name is None:
            name = cls.generate_name(
                st.get_name(schema),
            )

        return MultiRangeTypeShell(
            subtype=st,
            typemods=typemods,
            name=name,
            schemaclass=cls,
        )

    def as_shell(
        self: MultiRange_T,
        schema: s_schema.Schema,
    ) -> MultiRangeTypeShell[MultiRange_T]:
        return type(self).create_shell(
            schema,
            subtypes=[st.as_shell(schema) for st in self.get_subtypes(schema)],
            typemods=self.get_typemods(schema),
            name=self.get_name(schema),
        )

    def material_type(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[s_schema.Schema, MultiRange]:
        # We need to resolve material types based on the subtype recursively.

        st = self.get_element_type(schema)
        schema, stm = st.material_type(schema)
        if stm != st or isinstance(self, MultiRangeExprAlias):
            return MultiRange.from_subtypes(
                schema,
                [stm],
                typemods=self.get_typemods(schema),
            )
        else:
            return (schema, self)


class MultiRangeTypeShell(CollectionTypeShell[MultiRange_T_co]):

    schemaclass: typing.Type[MultiRange_T_co]

    def __init__(
        self,
        *,
        name: s_name.Name,
        subtype: TypeShell[Type],
        typemods: typing.Tuple[typing.Any, ...],
        schemaclass: typing.Type[MultiRange_T_co],
    ) -> None:
        super().__init__(name=name, schemaclass=schemaclass)
        self.subtype = subtype
        self.typemods = typemods

    def get_subtypes(
        self,
        schema: s_schema.Schema,
    ) -> typing.Tuple[TypeShell[Type], ...]:
        return (self.subtype,)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return f'multirange<{self.subtype.get_displayname(schema)}>'

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        *,
        view_name: Optional[s_name.QualName] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> sd.CommandGroup:
        ca: Union[CreateMultiRange, CreateMultiRangeExprAlias]
        cmd = sd.CommandGroup()
        if view_name is None:
            ca = CreateMultiRange(
                classname=self.get_name(schema),
                if_not_exists=True,
            )
        else:
            ca = CreateMultiRangeExprAlias(
                classname=view_name,
            )

        el = self.subtype
        if isinstance(el, CollectionTypeShell):
            cmd.add(el.as_create_delta(schema))

        ca.set_attribute_value('name', ca.classname)
        ca.set_attribute_value('element_type', el)
        ca.set_attribute_value('is_persistent', True)
        ca.set_attribute_value('abstract', self.is_polymorphic(schema))

        if attrs:
            for k, v in attrs.items():
                ca.set_attribute_value(k, v)

        cmd.add(ca)

        return cmd


class MultiRangeExprAlias(
    CollectionExprAlias,
    MultiRange,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
):
    # N.B: Don't add any SchemaFields to this class, they won't be
    # reflected properly (since this inherits from the concrete MultiRange).

    @classmethod
    def get_underlying_schema_class(cls) -> typing.Type[Collection]:
        return MultiRange


RangeLike = typing.Union[Range, MultiRange]


def get_union_type_name(
    component_names: typing.Iterable[s_name.Name],
    *,
    opaque: bool = False,
    module: typing.Optional[str] = None,
) -> s_name.QualName:
    sorted_name_list = sorted(
        str(name).replace('::', ':') for name in component_names)
    if opaque:
        nqname = f"(opaque: {' | '.join(sorted_name_list)})"
    else:
        nqname = f"({' | '.join(sorted_name_list)})"
    return s_name.QualName(name=nqname, module=module or '__derived__')


def get_intersection_type_name(
    component_names: typing.Iterable[s_name.Name],
    *,
    module: typing.Optional[str] = None,
) -> s_name.QualName:
    sorted_name_list = sorted(
        str(name).replace('::', ':') for name in component_names)
    nqname = f"({' & '.join(sorted_name_list)})"
    return s_name.QualName(name=nqname, module=module or '__derived__')


def ensure_schema_type_expr_type(
    schema: s_schema.Schema,
    type_shell: TypeExprShell[Type],
    parent_cmd: sd.Command,
    *,
    span: typing.Optional[parsing.Span] = None,
    context: sd.CommandContext,
) -> Optional[sd.Command]:

    name = type_shell.get_name(schema)
    texpr_type = schema.get(name, default=None, type=Type)
    cmd = None
    if texpr_type is None:
        cmd = type_shell.as_create_delta(schema)
        if cmd is not None:
            parent_cmd.add_prerequisite(cmd)

    return cmd


def type_dummy_expr(
    typ: Type,
    schema: s_schema.Schema,
) -> Optional[s_expr.Expression]:
    if isinstance(typ, so.DerivableInheritingObject):
        typ = typ.get_nearest_non_derived_parent(schema)
    text = f'assert_exists(<{typ.get_displayname(schema)}>{{}})'
    return s_expr.Expression(text=text)


class TypeCommand(sd.ObjectCommand[TypeT]):

    @classmethod
    def _get_alias_expr(cls, astnode: qlast.CreateAlias) -> qlast.Expr:
        expr = qlast.get_ddl_field_value(astnode, 'expr')
        if expr is None:
            raise errors.InvalidAliasDefinitionError(
                f'missing required view expression',
                span=astnode.span
            )
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
    ) -> s_expr.CompiledExpression:
        assert field.name == 'expr'
        return value.compiled(
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=context.modaliases,
                in_ddl_context_name='type definition',
                track_schema_ref_exprs=track_schema_ref_exprs,
            ),
            context=context,
        )

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'expr':
            return type_dummy_expr(self.scls, schema)
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

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
    def _validate_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: so.ObjectList[InheritingTypeT],
        shells: Mapping[s_name.QualName, TypeShell[InheritingTypeT]],
        is_derived: bool,
    ) -> None:
        for base in bases.objects(schema):
            if base.find_generic(schema) is not None or (
                base.is_free_object_type(schema) and not is_derived
            ):
                base_type_name = base.get_displayname(schema)
                shell = shells.get(base.get_name(schema))
                raise errors.SchemaError(
                    f"{base_type_name!r} cannot be a parent type",
                    span=shell.sourcectx if shell is not None else None,
                )


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

        shells = self.get_attribute_value('bases')
        if isinstance(shells, so.ObjectList):
            # XXX: fix set_attribute_value shell hygiene
            shells = shells.as_shell(schema)
        shell_map = {s.get_name(schema): s for s in shells}
        bases = self.get_resolved_attribute_value(
            'bases',
            schema=schema,
            context=context,
        )
        self._validate_bases(
            schema,
            context,
            bases,
            shell_map,
            is_derived=self.get_attribute_value('is_derived') or False,
        )


class RebaseInheritingType(
    InheritingTypeCommand[InheritingTypeT],
    inheriting.RebaseInheritingObject[InheritingTypeT],
):
    def validate_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_alter(schema, context)
        shell_map = {}
        for base_shells, _ in self.added_bases:
            shell_map.update({s.get_name(schema): s for s in base_shells})
        bases = self.get_resolved_attribute_value(
            'bases',
            schema=schema,
            context=context,
        )
        self._validate_bases(
            schema,
            context,
            bases,
            shell_map,
            is_derived=self.scls.get_is_derived(schema),
        )


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
    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        # Even if we create a collection while setting up internal
        # things, we don't mark it internal, since something visible
        # might use it later.
        self.set_attribute_value('internal', False)
        return super().canonicalize_attributes(schema, context)

    def validate_object(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> None:
        super().validate_object(schema, context)

        if isinstance(self.scls, (Range, MultiRange)):
            from . import scalars as s_scalars
            from edb.pgsql import types as pgtypes

            st = self.scls.get_subtypes(schema)[0]

            # general rule of what's supported
            supported = (
                isinstance(st, s_scalars.ScalarType) and st.is_base_type(schema)
            )

            if supported:
                # actually test that it's supported
                try:
                    pgtypes.pg_type_from_object(schema, self.scls)
                except Exception:
                    supported = False

            if not supported:
                raise errors.UnsupportedFeatureError(
                    f'unsupported range subtype: {st.get_displayname(schema)}'
                )


class AlterCollectionType(
    CollectionTypeCommand[CollectionTypeT],
    AlterType[CollectionTypeT],
    sd.AlterObject[CollectionTypeT],
):
    pass


class RenameCollectionType(
    CollectionTypeCommand[CollectionTypeT],
    RenameType[CollectionTypeT],
):
    pass


class DeleteCollectionType(
    CollectionTypeCommand[CollectionTypeT],
    sd.DeleteObject[CollectionTypeT],
):
    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_begin(schema, context)
        if not context.canonical:
            for el in self.scls.get_subtypes(schema):
                if op := el.as_type_delete_if_unused(schema):
                    self.add_caused(op)
        return schema


class CreateCollectionExprAlias(
    CollectionExprAliasCommand[CollectionExprAliasT],
    sd.CreateObject[CollectionExprAliasT],
):
    pass


class DeleteCollectionExprAlias(
    CollectionExprAliasCommand[CollectionExprAliasT],
    DeleteCollectionType[CollectionExprAliasT],
):

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: CollectionExprAliasT,
    ) -> List[sd.Command]:
        ops = super()._canonicalize(schema, context, scls)
        ops.append(scls.as_underlying_type_delete_if_unused(schema))
        return ops


class CreateTuple(CreateCollectionType[Tuple]):
    pass


class AlterTuple(AlterCollectionType[Tuple]):
    pass


class RenameTuple(RenameCollectionType[Tuple]):
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


class AlterArray(AlterCollectionType[Array]):
    pass


class RenameArray(RenameCollectionType[Array]):
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


class CreateRange(CreateCollectionType[Range]):
    pass


class AlterRange(AlterCollectionType[Range]):
    pass


class RenameRange(RenameCollectionType[Range]):
    pass


class CreateRangeExprAlias(CreateCollectionExprAlias[RangeExprAlias]):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class RenameRangeExprAlias(
    CollectionExprAliasCommand[RangeExprAlias],
    sd.RenameObject[RangeExprAlias],
):
    pass


class AlterRangeExprAlias(
    CollectionExprAliasCommand[RangeExprAlias],
    sd.AlterObject[RangeExprAlias],
):
    pass


class CreateMultiRange(CreateCollectionType[MultiRange]):
    pass


class AlterMultiRange(AlterCollectionType[MultiRange]):
    pass


class RenameMultiRange(RenameCollectionType[MultiRange]):
    pass


class CreateMultiRangeExprAlias(CreateCollectionExprAlias[MultiRangeExprAlias]):
    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> typing.Type[qlast.CreateAlias]:
        # Can't just use class-level astnode because that creates a
        # duplicate in ast -> command mapping.
        return qlast.CreateAlias


class RenameMultiRangeExprAlias(
    CollectionExprAliasCommand[MultiRangeExprAlias],
    sd.RenameObject[MultiRangeExprAlias],
):
    pass


class AlterMultiRangeExprAlias(
    CollectionExprAliasCommand[MultiRangeExprAlias],
    sd.AlterObject[MultiRangeExprAlias],
):
    pass


class DeleteTuple(DeleteCollectionType[Tuple]):
    pass


class DeleteTupleExprAlias(DeleteCollectionExprAlias[TupleExprAlias]):
    pass


class DeleteArray(DeleteCollectionType[Array]):
    # Prevent array types from getting deleted unless the element
    # type is being deleted too.
    def _has_outside_references(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> bool:
        if super()._has_outside_references(schema, context):
            return True

        el_type = self.scls.get_element_type(schema)
        if el_type.is_scalar() and not context.is_deleting(el_type):
            return True

        return False


class DeleteArrayExprAlias(DeleteCollectionExprAlias[ArrayExprAlias]):
    pass


class DeleteRange(DeleteCollectionType[Range]):
    pass


class DeleteRangeExprAlias(DeleteCollectionExprAlias[RangeExprAlias]):
    pass


class DeleteMultiRange(DeleteCollectionType[MultiRange]):
    pass


class DeleteMultiRangeExprAlias(DeleteCollectionExprAlias[MultiRangeExprAlias]):
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

    span = cmd.get_attribute_span('target')

    if isinstance(type_ref, TypeExprShell):
        cc_cmd = ensure_schema_type_expr_type(
            schema,
            type_ref,
            parent_cmd=cmd,
            span=span,
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
                    span=span,
                )
            raise
        except errors.InvalidPropertyDefinitionError as e:
            e.set_span(span)
            raise
    elif not isinstance(type_ref, Type):
        raise AssertionError(
            f'unexpected value in type attribute {attrname!r} of '
            f'{cmd.get_verbosename()}: {type_ref!r}'
        )

    return schema


def is_type_compatible(
    type_a: Type,
    type_b: Type,
    *,
    schema: s_schema.Schema,
) -> bool:
    """Check whether two types have compatible SQL representations.

    EdgeQL implicit casts need to be turned into explicit casts in
    some places, since the semantics differ from SQL's.
    """

    schema, material_type_a = type_a.material_type(schema)
    schema, material_type_b = type_b.material_type(schema)

    def labels_compatible(t_a: Type, t_b: Type) -> bool:
        if t_a == t_b:
            return True

        if isinstance(t_a, Tuple) and isinstance(t_b, Tuple):
            if t_a.get_is_persistent(schema) and t_b.get_is_persistent(schema):
                return False

            # For tuples, we also (recursively) check that the element
            # names match
            return all(
                name_a == name_b
                and labels_compatible(st_a, st_b)
                for (name_a, st_a), (name_b, st_b)
                in zip(t_a.iter_subtypes(schema),
                       t_b.iter_subtypes(schema))
            )
        elif isinstance(t_a, Array) and isinstance(t_b, Array):
            t_as = t_a.get_element_type(schema)
            t_bs = t_b.get_element_type(schema)
            return (
                not isinstance(t_as, Tuple) and labels_compatible(t_as, t_bs)
            )
        elif isinstance(t_a, Range) and isinstance(t_b, Range):
            t_as = t_a.get_element_type(schema)
            t_bs = t_b.get_element_type(schema)
            return labels_compatible(t_as, t_bs)
        elif isinstance(t_a, MultiRange) and isinstance(t_b, MultiRange):
            t_as = t_a.get_element_type(schema)
            t_bs = t_b.get_element_type(schema)
            return labels_compatible(t_as, t_bs)
        else:
            return True

    return (
        material_type_b.issubclass(schema, material_type_a)
        and labels_compatible(material_type_a, material_type_b)
    )
