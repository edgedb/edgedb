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

from typing import *  # NoQA

import collections

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import abc as s_abc
from . import annos as s_anno
from . import constraints
from . import delta as sd
from . import inheriting
from . import links
from . import lproperties
from . import name as sn
from . import objects as so
from . import pointers
from . import sources
from . import types as s_types
from . import utils

if TYPE_CHECKING:
    from . import schema as s_schema


class BaseObjectType(sources.Source,
                     s_types.Type,
                     constraints.ConsistencySubject,
                     s_anno.AnnotationSubject,
                     s_abc.ObjectType):

    union_of = so.SchemaField(
        so.ObjectSet,
        default=so.ObjectSet,
        coerce=True)

    intersection_of = so.SchemaField(
        so.ObjectSet,
        default=so.ObjectSet,
        coerce=True)

    is_opaque_union = so.SchemaField(
        bool,
        default=False,
        introspectable=False)

    @classmethod
    def get_schema_class_displayname(cls):
        return 'object type'

    def is_object_type(self):
        return True

    def is_union_type(self, schema) -> bool:
        return bool(self.get_union_of(schema))

    def get_displayname(self, schema):
        if self.is_view(schema) and not self.get_alias_is_persistent(schema):
            mtype = self.material_type(schema)
        else:
            mtype = self

        union_of = mtype.get_union_of(schema)
        if union_of:
            if self.get_is_opaque_union(schema):
                std_obj = schema.get('std::Object')
                return std_obj.get_displayname(schema)
            else:
                comps = sorted(union_of.objects(schema), key=lambda o: o.id)
                return ' | '.join(c.get_displayname(schema) for c in comps)
        else:
            intersection_of = mtype.get_intersection_of(schema)
            if intersection_of:
                comps = sorted(intersection_of.objects(schema),
                               key=lambda o: o.id)
                return ' & '.join(c.get_displayname(schema) for c in comps)
            elif mtype is self:
                return super().get_displayname(schema)
            else:
                return mtype.get_displayname(schema)

    def getrptrs(self, schema, name, *, sources=()):
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')

        ptrs = {
            l for l in schema.get_referrers(self, scls_type=links.Link,
                                            field_name='target')
            if (
                l.get_shortname(schema).name == name
                and not l.get_source(schema).is_view(schema)
                and l.get_is_local(schema)
                and (not sources or l.get_source(schema) in sources)
            )
        }

        for obj in self.get_ancestors(schema).objects(schema):
            ptrs.update(
                l for l in schema.get_referrers(obj, scls_type=links.Link,
                                                field_name='target')
                if (
                    l.get_shortname(schema).name == name
                    and not l.get_source(schema).is_view(schema)
                    and l.get_is_local(schema)
                    and (not sources or l.get_source(schema) in sources)
                )
            )

        return ptrs

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        return self.issubclass(schema, other)

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> Optional[s_types.Type]:
        return utils.get_class_nearest_common_ancestor(schema, [self, other])

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='Object')
        )

    @classmethod
    def get_default_base_name(cls):
        return sn.Name(module='std', name='Object')

    def _issubclass(self, schema, parent):
        if self == parent:
            return True

        my_union = self.get_union_of(schema)
        if my_union and not self.get_is_opaque_union(schema):
            # A union is considered a subclass of a type, if
            # ALL its components are subclasses of that type.
            return all(
                t._issubclass(schema, parent)
                for t in my_union.objects(schema)
            )

        my_intersection = self.get_intersection_of(schema)
        if my_intersection:
            # An intersection is considered a subclass of a type, if
            # ANY of its components are subclasses of that type.
            return any(
                t._issubclass(schema, parent)
                for t in my_intersection.objects(schema)
            )

        lineage = self.get_ancestors(schema).objects(schema)
        if parent in lineage:
            return True

        elif isinstance(parent, BaseObjectType):
            parent_union = parent.get_union_of(schema)
            if parent_union:
                # A type is considered a subclass of a union type,
                # if it is a subclass of ANY of the union components.
                return (
                    parent.get_is_opaque_union(schema)
                    or any(
                        self._issubclass(schema, t)
                        for t in parent_union.objects(schema)
                    )
                )

            parent_intersection = parent.get_intersection_of(schema)
            if parent_intersection:
                # A type is considered a subclass of an intersection type,
                # if it is a subclass of ALL of the intersection components.
                return all(
                    self._issubclass(schema, t)
                    for t in parent_intersection.objects(schema)
                )

        return False

    def _reduce_to_ref(self, schema):
        union_of = self.get_union_of(schema)
        if union_of:
            my_name = self.get_name(schema)
            return (
                s_types.ExistingUnionTypeRef(
                    components=[
                        c._reduce_to_ref(schema)[0]
                        for c in union_of.objects(schema)
                    ],
                    name=my_name,
                ),
                my_name,
            )

        intersection_of = self.get_intersection_of(schema)
        if intersection_of:
            my_name = self.get_name(schema)
            return (
                s_types.ExistingIntersectionTypeRef(
                    components=[
                        c._reduce_to_ref(schema)[0]
                        for c in intersection_of.objects(schema)
                    ],
                    name=my_name,
                ),
                my_name,
            )

        return super()._reduce_to_ref(schema)


class ObjectType(BaseObjectType, qlkind=qltypes.SchemaObjectClass.TYPE):
    pass


class DerivedObjectType(BaseObjectType):
    pass


def get_union_type_attrs(
        schema,
        components: Iterable[s_types.Type], *,
        module: Optional[str]=None):

    name = sn.Name(
        name='|'.join(sorted(str(t.id) for t in components)),
        module=module or '__derived__',
    )

    type_id = s_types.generate_type_id(name)

    return dict(
        id=type_id,
        name=name,
        bases=[schema.get('std::Object')],
        union_of=so.ObjectSet.create(schema, components),
    )


def get_or_create_union_type(
    schema: s_schema.Schema,
    components: Iterable[ObjectType],
    *,
    opaque: bool = False,
    module: Optional[str] = None,
) -> ObjectType:

    name = sn.Name(
        name='|'.join(sorted(str(t.id) for t in components)),
        module=module or '__derived__',
    )

    type_id = s_types.generate_type_id(name)
    objtype = schema.get_by_id(type_id, None)
    created = objtype is None
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::Object')

        schema, objtype = std_object.derive_subtype(
            schema,
            name=name,
            attrs=dict(
                id=type_id,
                union_of=so.ObjectSet.create(schema, components),
                is_opaque_union=opaque,
            ),
        )

        if not opaque:
            schema = sources.populate_pointer_set_for_source_union(
                schema,
                components,
                objtype,
                modname=module,
            )

    return schema, objtype, created


def get_intersection_type_attrs(
        schema,
        components: Iterable[s_types.Type], *,
        module: Optional[str]=None):

    name = sn.Name(
        name=f"({' & '.join(sorted(str(t.id) for t in components))})",
        module=module or '__derived__',
    )

    type_id = s_types.generate_type_id(name)

    return dict(
        id=type_id,
        name=name,
        bases=[schema.get('std::Object')],
        intersection_of=so.ObjectSet.create(schema, components),
    )


def get_or_create_intersection_type(
    schema: s_schema.Schema,
    components: Iterable[ObjectType],
    *,
    module: Optional[str] = None,
) -> ObjectType:

    name = sn.Name(
        name=f"({' & '.join(sorted(str(t.id) for t in components))})",
        module=module or '__derived__',
    )

    type_id = s_types.generate_type_id(name)
    objtype = schema.get_by_id(type_id, None)
    created = objtype is None
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::Object')

        schema, objtype = std_object.derive_subtype(
            schema,
            name=name,
            attrs=dict(
                id=type_id,
                intersection_of=so.ObjectSet.create(schema, components),
            ),
        )

        ptrs = collections.defaultdict(list)

        for component in components:
            for pn, ptr in component.get_pointers(schema).items(schema):
                ptrs[pn].append(ptr)

        intersection_pointers = {}

        for pn, ptrs in ptrs.items():
            if len(ptrs) > 1:
                # The pointer is present in more than one component.
                schema, ptr = pointers.get_or_create_intersection_pointer(
                    schema,
                    ptrname=pn,
                    source=objtype,
                    components=set(ptrs),
                )
            else:
                ptr = ptrs[0]

            intersection_pointers[pn] = ptr

        for pn, ptr in intersection_pointers.items():
            if objtype.getptr(schema, pn) is None:
                schema = objtype.add_pointer(schema, ptr)

    return schema, objtype, created


class ObjectTypeCommandContext(sd.ObjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               s_anno.AnnotationSubjectCommandContext,
                               links.LinkSourceCommandContext,
                               lproperties.PropertySourceContext):
    pass


class ObjectTypeCommand(constraints.ConsistencySubjectCommand,
                        sources.SourceCommand, links.LinkSourceCommand,
                        s_types.TypeCommand,
                        schema_metaclass=ObjectType,
                        context_class=ObjectTypeCommandContext):
    pass


class CreateObjectType(ObjectTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateObjectType

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDL],
    ) -> Optional[qlast.DDL]:
        if (self.get_attribute_value('expr_type')
                and not self.get_attribute_value('expr')):
            # This is a nested view type, e.g
            # __FooAlias_bar produced by  FooAlias := (SELECT Foo { bar: ... })
            # and should obviously not appear as a top level definition.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)

    def _get_ast_node(self, schema, context):
        if self.get_attribute_value('expr_type'):
            return qlast.CreateAlias
        else:
            return super()._get_ast_node(schema, context)


class RenameObjectType(ObjectTypeCommand, sd.RenameObject):
    pass


class RebaseObjectType(ObjectTypeCommand, inheriting.RebaseInheritingObject):
    pass


class AlterObjectType(ObjectTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterObjectType

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class DeleteObjectType(ObjectTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropObjectType
