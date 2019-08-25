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

import typing

from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import annotations
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
from . import types_delta as s_types_d
from . import utils


class BaseObjectType(sources.Source,
                     s_types.Type,
                     constraints.ConsistencySubject,
                     annotations.AnnotationSubject,
                     s_abc.ObjectType):

    union_of = so.SchemaField(
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

    def get_displayname(self, schema):
        if self.is_view(schema):
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
            if mtype is self:
                return super().get_displayname(schema)
            else:
                return mtype.get_displayname(schema)

    def getrptrs(self, schema, name):
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')

        ptrs = {
            l for l in schema.get_referrers(self, scls_type=links.Link,
                                            field_name='target')
            if (l.get_shortname(schema).name == name
                and not l.get_source(schema).is_view(schema)
                and l.get_is_local(schema))
        }

        for obj in self.get_ancestors(schema).objects(schema):
            ptrs.update(
                l for l in schema.get_referrers(obj, scls_type=links.Link,
                                                field_name='target')
                if (l.get_shortname(schema).name == name
                    and not l.get_source(schema).is_view(schema)
                    and l.get_is_local(schema))
            )

        return ptrs

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        return self.issubclass(schema, other)

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:
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
        my_vchildren = self.get_union_of(schema)

        if not my_vchildren:
            lineage = inheriting.compute_lineage(schema, self)

            if parent in lineage:
                return True
            elif isinstance(parent, BaseObjectType):
                vchildren = parent.get_union_of(schema)
                if vchildren:
                    return bool(set(vchildren.objects(schema)) & set(lineage))
                else:
                    return False
            else:
                return False
        else:
            return all(c._issubclass(schema, parent)
                       for c in my_vchildren.objects(schema))


class ObjectType(BaseObjectType):
    pass


class DerivedObjectType(BaseObjectType):
    pass


def get_union_type_attrs(
        schema,
        components: typing.Iterable[ObjectType], *,
        module: typing.Optional[str]=None):

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
        schema,
        components: typing.Iterable[ObjectType],
        *,
        opaque: bool=False,
        module: typing.Optional[str]=None) -> ObjectType:

    name = sn.Name(
        name='|'.join(sorted(str(t.id) for t in components)),
        module=module or '__derived__',
    )

    type_id = s_types.generate_type_id(name)
    objtype = schema.get_by_id(type_id, None)
    if objtype is None:
        components = list(components)

        std_object = schema.get('std::Object')

        schema, objtype = std_object.derive(
            schema, std_object, name=name,
            attrs=dict(
                id=type_id,
                union_of=so.ObjectSet.create(schema, components),
                is_opaque_union=opaque,
            ),
        )

        if not opaque:
            union_pointers = {}

            for pn, ptr in components[0].get_pointers(schema).items(schema):
                ptrs = [ptr]
                for component in components[1:]:
                    other_ptr = component.get_pointers(schema).get(
                        schema, pn, None)
                    if other_ptr is None:
                        break
                    ptrs.append(other_ptr)

                if len(ptrs) == len(components):
                    # The pointer is present in all components.
                    if len(ptrs) == 1:
                        ptr = ptrs[0]
                    else:
                        ptrs = set(ptrs)
                        schema, ptr = pointers.get_or_create_union_pointer(
                            schema,
                            ptrname=pn,
                            source=objtype,
                            direction=pointers.PointerDirection.Outbound,
                            components=ptrs,
                        )

                    union_pointers[pn] = ptr

            if union_pointers:
                for pn, ptr in union_pointers.items():
                    if objtype.getptr(schema, pn) is None:
                        schema = objtype.add_pointer(schema, ptr)

    return schema, objtype


class ObjectTypeCommandContext(sd.ObjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               annotations.AnnotationSubjectCommandContext,
                               links.LinkSourceCommandContext,
                               lproperties.PropertySourceContext):
    pass


class ObjectTypeCommand(constraints.ConsistencySubjectCommand,
                        sources.SourceCommand, links.LinkSourceCommand,
                        s_types_d.TypeCommand,
                        schema_metaclass=ObjectType,
                        context_class=ObjectTypeCommandContext):
    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'is_derived':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class CreateObjectType(ObjectTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateObjectType


class RenameObjectType(ObjectTypeCommand, sd.RenameObject):
    pass


class RebaseObjectType(ObjectTypeCommand, inheriting.RebaseInheritingObject):
    pass


class AlterObjectType(ObjectTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterObjectType


class DeleteObjectType(ObjectTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropObjectType
