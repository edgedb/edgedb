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

from typing import *

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


class ObjectType(
    s_types.InheritingType,
    sources.Source,
    constraints.ConsistencySubject,
    s_anno.AnnotationSubject,
    s_abc.ObjectType,
    qlkind=qltypes.SchemaObjectClass.TYPE,
):

    union_of = so.SchemaField(
        so.ObjectSet["ObjectType"],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True)

    intersection_of = so.SchemaField(
        so.ObjectSet["ObjectType"],
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True)

    is_opaque_union = so.SchemaField(
        bool,
        default=False,
        introspectable=False)

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'object type'

    def is_object_type(self) -> bool:
        return True

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

    def getrptrs(
        self,
        schema: s_schema.Schema,
        name: Union[sn.Name, str],
        *,
        sources: Iterable[so.Object] = ()
    ) -> Set[links.Link]:
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')
        ptrs = {
            lnk for lnk in schema.get_referrers(self, scls_type=links.Link,
                                                field_name='target')
            if (
                isinstance(lnk, links.Link)
                and lnk.get_shortname(schema).name == name
                and not lnk.get_source_type(schema).is_view(schema)
                and lnk.get_is_local(schema)
                and (not sources or lnk.get_source_type(schema) in sources)
            )
        }

        for obj in self.get_ancestors(schema).objects(schema):
            ptrs.update(
                lnk for lnk in schema.get_referrers(obj, scls_type=links.Link,
                                                    field_name='target')
                if (
                    isinstance(lnk, links.Link)
                    and lnk.get_shortname(schema).name == name
                    and not lnk.get_source_type(schema).is_view(schema)
                    and lnk.get_is_local(schema)
                    and (not sources or lnk.get_source_type(schema) in sources)
                )
            )

        return ptrs

    def implicitly_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema
    ) -> bool:
        return self.issubclass(schema, other)

    def find_common_implicitly_castable_type(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Optional[ObjectType]]:
        assert isinstance(other, ObjectType)
        nearest_common_ancestor = utils.get_class_nearest_common_ancestor(
            schema, [self, other]
        )
        if nearest_common_ancestor is not None:
            assert isinstance(nearest_common_ancestor, ObjectType)
        return (
            schema,
            nearest_common_ancestor,
        )

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.Name, ...]:
        return (
            sn.Name(module='std', name='BaseObject'),
            sn.Name(module='std', name='Object'),
        )

    @classmethod
    def get_default_base_name(cls) -> sn.Name:
        return sn.Name(module='std', name='Object')

    def _issubclass(
        self,
        schema: s_schema.Schema,
        parent: so.SubclassableObject
    ) -> bool:
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

        elif isinstance(parent, ObjectType):
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

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        return not self.is_view(schema) or refdict.attr == 'pointers'


def get_or_create_union_type(
    schema: s_schema.Schema,
    components: Iterable[ObjectType],
    *,
    opaque: bool = False,
    module: Optional[str] = None,
) -> Tuple[s_schema.Schema, ObjectType, bool]:

    type_id, name = s_types.get_union_type_id(
        schema,
        components,
        module=module,
    )

    objtype = schema.get_by_id(type_id, None, type=ObjectType)
    created = objtype is None
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::BaseObject', type=ObjectType)

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
) -> Tuple[s_schema.Schema, ObjectType, bool]:

    type_id, name = s_types.get_intersection_type_id(
        schema,
        components,
        module=module,
    )

    objtype = schema.get_by_id(type_id, None)
    created = objtype is None
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::BaseObject', type=ObjectType)

        schema, objtype = std_object.derive_subtype(
            schema,
            name=name,
            attrs=dict(
                id=type_id,
                intersection_of=so.ObjectSet.create(schema, components),
            ),
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
                )
            else:
                ptr = ptrs[0]

            intersection_pointers[pn] = ptr

        for pn, ptr in intersection_pointers.items():
            if objtype.getptr(schema, pn) is None:
                schema = objtype.add_pointer(schema, ptr)

    assert isinstance(objtype, ObjectType)
    return schema, objtype, created


class ObjectTypeCommandContext(sd.ObjectCommandContext[ObjectType],
                               constraints.ConsistencySubjectCommandContext,
                               s_anno.AnnotationSubjectCommandContext,
                               links.LinkSourceCommandContext,
                               lproperties.PropertySourceContext):
    pass


class ObjectTypeCommand(s_types.InheritingTypeCommand[ObjectType],
                        constraints.ConsistencySubjectCommand[ObjectType],
                        sources.SourceCommand, links.LinkSourceCommand,
                        schema_metaclass=ObjectType,
                        context_class=ObjectTypeCommandContext):
    pass


class CreateObjectType(ObjectTypeCommand,
                       inheriting.CreateInheritingObject[ObjectType]):
    astnode = qlast.CreateObjectType

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.ObjectDDL)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, sd.QualifiedObjectCommand)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd

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
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext
    ) -> Type[qlast.DDLOperation]:
        if self.get_attribute_value('expr_type'):
            return qlast.CreateAlias
        else:
            return super()._get_ast_node(schema, context)


class RenameObjectType(ObjectTypeCommand, sd.RenameObject):
    pass


class RebaseObjectType(ObjectTypeCommand,
                       inheriting.RebaseInheritingObject[ObjectType]):
    pass


class AlterObjectType(ObjectTypeCommand,
                      inheriting.AlterInheritingObject[ObjectType]):
    astnode = qlast.AlterObjectType

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.ObjectDDL)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, sd.QualifiedObjectCommand)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class DeleteObjectType(ObjectTypeCommand,
                       inheriting.DeleteInheritingObject[ObjectType]):
    astnode = qlast.DropObjectType
