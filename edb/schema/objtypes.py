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

from typing import Optional, Tuple, Type, Iterable, List, Set, cast

import collections

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import abc as s_abc
from . import annos as s_anno
from . import constraints
from . import delta as sd
from . import functions as s_func
from . import inheriting
from . import links
from . import properties
from . import name as sn
from . import objects as so
from . import pointers
from . import policies
from . import schema as s_schema
from . import sources
from . import triggers
from . import types as s_types
from . import unknown_pointers
from . import utils


class ObjectTypeRefMixin(so.Object):
    # We stick access policies and triggers in their own class as a
    # hack, to allow us to ensure that access_policies comes later in
    # the refdicts list than pointers does, so that pointers are
    # always created before access policies when creating an inherited
    # type.
    access_policies_refs = so.RefDict(
        attr='access_policies',
        requires_explicit_overloaded=True,
        backref_attr='subject',
        ref_cls=policies.AccessPolicy)

    access_policies = so.SchemaField(
        so.ObjectIndexByUnqualifiedName[policies.AccessPolicy],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.857,
        default=so.DEFAULT_CONSTRUCTOR)

    triggers_refs = so.RefDict(
        attr='triggers',
        requires_explicit_overloaded=True,
        backref_attr='subject',
        ref_cls=triggers.Trigger)

    triggers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName[triggers.Trigger],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.857,
        default=so.DEFAULT_CONSTRUCTOR)


class ObjectType(
    sources.Source,
    constraints.ConsistencySubject,
    s_types.InheritingType,

    so.InheritingObject,  # Help reflection figure out the right db MRO
    s_types.Type,  # Help reflection figure out the right db MRO
    s_anno.AnnotationSubject,  # Help reflection figure out the right db MRO
    ObjectTypeRefMixin,
    s_abc.ObjectType,
    qlkind=qltypes.SchemaObjectClass.TYPE,
    data_safe=False,
):

    union_of = so.SchemaField(
        so.ObjectSet["ObjectType"],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        type_is_generic_self=True,
        compcoef=0.0,
    )

    intersection_of = so.SchemaField(
        so.ObjectSet["ObjectType"],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        type_is_generic_self=True,
    )

    is_opaque_union = so.SchemaField(
        bool,
        default=False,
    )

    def is_object_type(self) -> bool:
        return True

    def is_free_object_type(self, schema: s_schema.Schema) -> bool:
        if self.get_name(schema) == sn.QualName('std', 'FreeObject'):
            return True

        FreeObject = schema.get(
            'std::FreeObject', type=ObjectType, default=None)
        if FreeObject is None:
            # Possible in bootstrap before FreeObject is declared
            return False
        else:
            return self.issubclass(schema, FreeObject)

    def is_fake_object_type(self, schema: s_schema.Schema) -> bool:
        return self.is_free_object_type(schema)

    def is_material_object_type(self, schema: s_schema.Schema) -> bool:
        return not (
            self.is_fake_object_type(schema)
            or self.is_compound_type(schema)
            or self.is_view(schema)
        )

    def is_union_type(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_union_of(schema))

    def is_intersection_type(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_intersection_of(schema))

    def is_compound_type(self, schema: s_schema.Schema) -> bool:
        return self.is_union_type(schema) or self.is_intersection_type(schema)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        if self.is_view(schema) and not self.get_alias_is_persistent(schema):
            schema, mtype = self.material_type(schema)
        else:
            mtype = self

        union_of = mtype.get_union_of(schema)
        if union_of:
            if self.get_is_opaque_union(schema):
                std_obj = schema.get('std::BaseObject', type=ObjectType)
                return std_obj.get_displayname(schema)
            else:
                comp_dns = sorted(
                    (c.get_displayname(schema)
                     for c in union_of.objects(schema)))
                return '(' + ' | '.join(comp_dns) + ')'
        else:
            intersection_of = mtype.get_intersection_of(schema)
            if intersection_of:
                comp_dns = sorted(
                    (c.get_displayname(schema)
                     for c in intersection_of.objects(schema)))
                # Elide BaseObject from display, because `& BaseObject`
                # is a nop.
                return '(' + ' & '.join(
                    dn for dn in comp_dns if dn != 'std::BaseObject'
                ) + ')'
            elif mtype == self:
                return super().get_displayname(schema)
            else:
                return mtype.get_displayname(schema)

    def getrptrs(
        self,
        schema: s_schema.Schema,
        name: str,
        *,
        sources: Iterable[so.Object] = ()
    ) -> Set[links.Link]:
        if sn.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')

        ptrs: Set[links.Link] = set()

        ancestor_objects = self.get_ancestors(schema).objects(schema)

        for obj in (self,) + ancestor_objects:
            ptrs.update(
                lnk for lnk in schema.get_referrers(
                    obj, scls_type=links.Link, field_name='target')
                if (
                    lnk.get_shortname(schema).name == name
                    and lnk.get_source_type(schema).is_material_object_type(
                        schema)
                    # Only grab the "base" pointers
                    and all(
                        b.is_non_concrete(schema)
                        for b in lnk.get_bases(schema).objects(schema)
                    )
                    and (not sources or lnk.get_source_type(schema) in sources)
                )
            )

        for intersection in self.get_intersection_of(schema).objects(schema):
            ptrs.update(intersection.getrptrs(schema, name, sources=sources))

        unions = schema.get_referrers(
            self, scls_type=ObjectType, field_name='union_of')

        for union in unions:
            ptrs.update(union.getrptrs(schema, name, sources=sources))

        return ptrs

    def get_relevant_triggers(
        self, kind: qltypes.TriggerKind, schema: s_schema.Schema
    ) -> list[triggers.Trigger]:
        return [
            t for t in self.get_triggers(schema).objects(schema)
            if kind in t.get_kinds(schema)
        ]

    def implicitly_castable_to(
        self, other: s_types.Type, schema: s_schema.Schema
    ) -> bool:
        return self.issubclass(schema, other)

    def find_common_implicitly_castable_type(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Optional[ObjectType]]:
        if not isinstance(other, ObjectType):
            return schema, None

        nearest_common_ancestors = utils.get_class_nearest_common_ancestors(
            schema, [self, other]
        )
        # We arbitrarily select the first nearest common ancestor
        nearest_common_ancestor = (
            nearest_common_ancestors[0] if nearest_common_ancestors else None)

        if nearest_common_ancestor is not None:
            assert isinstance(nearest_common_ancestor, ObjectType)
        return (
            schema,
            nearest_common_ancestor,
        )

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.QualName, ...]:
        return (
            sn.QualName(module='std', name='BaseObject'),
            sn.QualName(module='std', name='Object'),
            sn.QualName(module='std', name='FreeObject'),
        )

    @classmethod
    def get_default_base_name(cls) -> sn.QualName:
        return sn.QualName(module='std', name='Object')

    def _issubclass(
        self, schema: s_schema.Schema, parent: so.SubclassableObject
    ) -> bool:
        if self == parent:
            return True

        if (
            (my_union := self.get_union_of(schema))
            and not self.get_is_opaque_union(schema)
        ):
            # A union is considered a subclass of a type, if
            # ALL its components are subclasses of that type.
            return all(
                t._issubclass(schema, parent)
                for t in my_union.objects(schema)
            )

        if my_intersection := self.get_intersection_of(schema):
            # An intersection is considered a subclass of a type, if
            # ANY of its components are subclasses of that type.
            return any(
                t._issubclass(schema, parent)
                for t in my_intersection.objects(schema)
            )

        lineage = self.get_ancestors(schema).objects(schema)
        if parent in lineage:
            return True

        elif isinstance(parent, ObjectType):
            if (
                (parent_union := parent.get_union_of(schema))
                and not parent.get_is_opaque_union(schema)
            ):
                # A type is considered a subclass of a union type,
                # if it is a subclass of ANY of the union components.
                return (
                    parent.get_is_opaque_union(schema)
                    or any(
                        self._issubclass(schema, t)
                        for t in parent_union.objects(schema)
                    )
                )

            if parent_intersection := parent.get_intersection_of(schema):
                # A type is considered a subclass of an intersection type,
                # if it is a subclass of ALL of the intersection components.
                return all(
                    self._issubclass(schema, t)
                    for t in parent_intersection.objects(schema)
                )

        return False

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        return not self.is_view(schema) or refdict.attr == 'pointers'

    def as_type_delete_if_unused(
        self,
        schema: s_schema.Schema,
    ) -> Optional[sd.DeleteObject[ObjectType]]:
        if not self._is_deletable(schema):
            return None

        # References to aliases can only occur inside other aliases,
        # so when they go, we need to delete the reference also.
        # Compound types also need to be deleted when their last
        # referrer goes.
        if (
            self.is_view(schema)
            and self.get_alias_is_persistent(schema)
        ) or self.is_compound_type(schema):
            return self.init_delta_command(
                schema,
                sd.DeleteObject,
                if_unused=True,
                if_exists=True,
            )
        else:
            return None

    def _test_polymorphic(
        self, schema: s_schema.Schema, other: s_types.Type
    ) -> bool:
        if other.is_anyobject(schema):
            return True
        return False


def get_or_create_union_type(
    schema: s_schema.Schema,
    components: Iterable[ObjectType],
    *,
    transient: bool = False,
    opaque: bool = False,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, ObjectType, bool]:

    name = s_types.get_union_type_name(
        (c.get_name(schema) for c in components),
        opaque=opaque,
        module=module,
    )

    objtype = schema.get(name, default=None, type=ObjectType)
    created = objtype is None
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::BaseObject', type=ObjectType)

        schema, objtype = std_object.derive_subtype(
            schema,
            name=name,
            attrs=dict(
                union_of=so.ObjectSet.create(schema, components),
                is_opaque_union=opaque,
                abstract=True,
            ),
            transient=transient,
        )

        if not opaque:

            schema = sources.populate_pointer_set_for_source_union(
                schema,
                cast(List[sources.Source], components),
                objtype,
                modname=module,
            )

    return schema, objtype, created


def get_or_create_intersection_type(
    schema: s_schema.Schema,
    components: Iterable[ObjectType],
    *,
    module: Optional[str] = None,
    transient: bool = False,
) -> Tuple[s_schema.Schema, ObjectType]:

    name = s_types.get_intersection_type_name(
        (c.get_name(schema) for c in components),
        module=module,
    )

    objtype = schema.get(name, default=None, type=ObjectType)
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::BaseObject', type=ObjectType)

        schema, objtype = std_object.derive_subtype(
            schema,
            name=name,
            attrs=dict(
                intersection_of=so.ObjectSet.create(schema, components),
                abstract=True,
            ),
            transient=transient,
        )

        ptrs_dict = collections.defaultdict(list)

        for component in components:
            for pn, ptr in component.get_pointers(schema).items(schema):
                ptrs_dict[pn].append(ptr)

        intersection_pointers = {}

        for pn, ptrs in ptrs_dict.items():
            if len(ptrs) > 1:
                # The pointer is present in more than one component.
                schema, ptr = pointers.get_or_create_intersection_pointer(
                    schema,
                    ptrname=pn,
                    source=objtype,
                    components=set(ptrs),
                    transient=transient,
                )
            else:
                ptr = ptrs[0]

            intersection_pointers[pn] = ptr

        for pn, ptr in intersection_pointers.items():
            if objtype.maybe_get_ptr(schema, pn) is None:
                schema = objtype.add_pointer(schema, ptr)

    assert isinstance(objtype, ObjectType)
    return schema, objtype


class ObjectTypeCommandContext(
    links.LinkSourceCommandContext[ObjectType],
    properties.PropertySourceContext[ObjectType],
    unknown_pointers.UnknownPointerSourceContext[ObjectType],
    policies.AccessPolicySourceCommandContext[ObjectType],
    triggers.TriggerSourceCommandContext[ObjectType],
    sd.ObjectCommandContext[ObjectType],
    constraints.ConsistencySubjectCommandContext,
    s_anno.AnnotationSubjectCommandContext,
):
    pass


class ObjectTypeCommand(
    s_types.InheritingTypeCommand[ObjectType],
    constraints.ConsistencySubjectCommand[ObjectType],
    sources.SourceCommand[ObjectType],
    links.LinkSourceCommand[ObjectType],
    context_class=ObjectTypeCommandContext,
):
    def validate_object(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> None:
        if (
            not context.stdmode
            and not context.testmode
            and self.scls.is_material_object_type(schema)
        ):
            for base in self.scls.get_bases(schema).objects(schema):
                name = base.get_name(schema)
                if (
                    sn.UnqualName(name.module) in s_schema.STD_MODULES
                    and name not in (
                        sn.QualName('std', 'BaseObject'),
                        sn.QualName('std', 'Object'),
                    )
                ):
                    raise errors.SchemaDefinitionError(
                        f"cannot extend system type '{name}'",
                        span=self.span,
                    )

        # Internal consistency check: our stdlib and extension types
        # shouldn't extend std::Object, which is reserved for user
        # types.
        if (
            self.scls.is_material_object_type(schema)
            and self.classname.get_root_module_name() in s_schema.STD_MODULES
        ):
            for base in self.scls.get_bases(schema).objects(schema):
                name = base.get_name(schema)
                if name == sn.QualName('std', 'Object'):
                    raise errors.SchemaDefinitionError(
                        f"standard lib/extension type '{self.classname}' "
                        f"cannot extend std::Object",
                        hint="try BaseObject",
                    )


class CreateObjectType(
    ObjectTypeCommand,
    s_types.CreateInheritingType[ObjectType],
):
    astnode = qlast.CreateObjectType

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if (self.get_attribute_value('expr_type')
                and not self.get_attribute_value('expr')):
            # This is a nested view type, e.g
            # __FooAlias_bar produced by  FooAlias := (SELECT Foo { bar: ... })
            # and should obviously not appear as a top level definition.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)

    def _get_ast_node(
        self, schema: s_schema.Schema, context: sd.CommandContext
    ) -> Type[qlast.DDLOperation]:
        if self.get_attribute_value('expr_type'):
            return qlast.CreateAlias
        else:
            return super()._get_ast_node(schema, context)

    def _create_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            not context.canonical
            and self.scls.is_material_object_type(schema)
        ):
            # Propagate changes to any functions that depend on
            # ancestor types in order to recompute the inheritance
            # situation.
            schema = self._propagate_if_expr_refs(
                schema,
                context,
                action='creating an object type',
                include_ancestors=True,
                filter=s_func.Function,
            )

        return super()._create_finalize(schema, context)


class RenameObjectType(
    ObjectTypeCommand,
    s_types.RenameInheritingType[ObjectType],
):
    pass


class RebaseObjectType(
    ObjectTypeCommand,
    s_types.RebaseInheritingType[ObjectType],
):
    pass


class AlterObjectType(
    ObjectTypeCommand,
    s_types.AlterType[ObjectType],
    inheriting.AlterInheritingObject[ObjectType],
):
    astnode = qlast.AlterObjectType

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)

        if (
            not context.canonical
            and bool(self.get_subcommands(type=policies.AccessPolicyCommand))
        ):
            from . import functions
            # If we have any policy commands, we need to propagate to update
            # functions. We also need to propagate to anything that updates
            # an ancestor.
            #
            # Note that the ancestor search does not generate
            # quadratically many updates in the case that this change
            # was propagated from an ancestor, since the
            # _propagate_if_expr_refs call in the ancestor temporarily
            # eliminates the ref!
            schema = self._propagate_if_expr_refs(
                schema,
                context,
                action=self.get_friendly_description(schema=schema),
                include_ancestors=True,
                filter=functions.Function,
            )

        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        if not context.canonical:
            # If this type is contained in any unions, we need to
            # update them with any additions or alterations made to
            # this type. (Deletions are already handled in DeletePointer.)
            unions = schema.get_referrers(
                self.scls, scls_type=ObjectType, field_name='union_of')

            orig_disable = context.disable_dep_verification

            for union in unions:
                if union.get_is_opaque_union(schema):
                    continue

                delete = union.init_delta_command(schema, sd.DeleteObject)

                context.disable_dep_verification = True
                delete.apply(schema, context)
                context.disable_dep_verification = orig_disable
                # We run the delete to populate the tree, but then instead
                # of actually deleting the object, we just remove the names.
                # This is because the pointers in the types we are looking
                # at might themselves reference the union, so we need
                # them in the schema to produce the correct as_alter_delta.
                nschema = _delete_to_delist(delete, schema)

                nschema, nunion, _ = utils.ensure_union_type(
                    nschema,
                    types=union.get_union_of(schema).objects(schema),
                    opaque=union.get_is_opaque_union(schema),
                    module=union.get_name(schema).module,
                )
                assert isinstance(nunion, ObjectType)

                diff = union.as_alter_delta(
                    other=nunion,
                    self_schema=schema,
                    other_schema=nschema,
                    confidence=1.0,
                    context=so.ComparisonContext(),
                )

                schema = diff.apply(schema, context)
                self.add(diff)

        return super()._alter_finalize(schema, context)


def _delete_to_delist(
    delete: sd.DeleteObject[so.Object],
    schema: s_schema.Schema,
) -> s_schema.Schema:
    """Delist all of the objects mentioned in a delete tree.

    This removes their names from the schema but preserves the actual
    objects.
    """
    schema = schema.delist(delete.classname)
    for sub in delete.get_subcommands(type=sd.DeleteObject):
        schema = _delete_to_delist(sub, schema)
    return schema


class DeleteObjectType(
    ObjectTypeCommand,
    s_types.DeleteType[ObjectType],
    inheriting.DeleteInheritingObject[ObjectType],
):
    astnode = qlast.DropObjectType

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_orig_attribute_value('expr_type'):
            # This is an alias type, appropriate DDL would be generated
            # from the corresponding DeleteAlias node.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)

    def _delete_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            not context.canonical
            and self.scls.is_material_object_type(schema)
        ):
            # Propagate changes to any functions that depend on
            # ancestor types in order to recompute the inheritance
            # situation.
            schema = self._propagate_if_expr_refs(
                schema,
                context,
                action='deleting an object type',
                include_self=False,
                include_ancestors=True,
                filter=s_func.Function,
            )

        return super()._delete_finalize(schema, context)
