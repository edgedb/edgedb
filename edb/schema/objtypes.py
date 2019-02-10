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

from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import attributes
from . import constraints
from . import delta as sd
from . import inheriting
from . import links
from . import lproperties
from . import name as sn
from . import nodes
from . import sources
from . import types as s_types
from . import utils


class BaseObjectType(sources.Source, nodes.Node):
    pass


class ObjectType(BaseObjectType, constraints.ConsistencySubject,
                 attributes.AttributeSubject, s_abc.ObjectType):

    def is_object_type(self):
        return True

    def get_displayname(self, schema):
        if self.is_view(schema):
            return self.material_type(schema).get_displayname(schema)
        else:
            return super().get_displayname(schema)

    def getrptrs(self, schema, name):
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')

        ptrs = {
            l for l in schema.get_referrers(self, scls_type=links.Link,
                                            field_name='target')
            if l.get_shortname(schema).name == name
        }

        for obj in self.get_mro(schema).objects(schema):
            ptrs.update(
                l for l in schema.get_referrers(obj, scls_type=links.Link,
                                                field_name='target')
                if l.get_shortname(schema).name == name
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


class UnionObjectType(BaseObjectType,
                      constraints.ConsistencySubject, s_types.Type):
    pass


class DerivedObjectType(BaseObjectType,
                        constraints.ConsistencySubject, s_types.Type):
    pass


class ObjectTypeCommandContext(sd.ObjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               attributes.AttributeSubjectCommandContext,
                               links.LinkSourceCommandContext,
                               lproperties.PropertySourceContext,
                               nodes.NodeCommandContext):
    pass


class ObjectTypeCommand(constraints.ConsistencySubjectCommand,
                        sources.SourceCommand, links.LinkSourceCommand,
                        nodes.NodeCommand,
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
