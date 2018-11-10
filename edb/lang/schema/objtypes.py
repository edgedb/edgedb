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


from edb.lang.edgeql import ast as qlast

from . import constraints
from . import delta as sd
from . import inheriting
from . import links
from . import lproperties
from . import name as sn
from . import named
from . import nodes
from . import referencing
from . import sources
from . import types as s_types


class SourceNode(sources.Source, nodes.Node):
    pass


class ObjectType(SourceNode, constraints.ConsistencySubject):
    _type = 'ObjectType'

    def is_object_type(self):
        return True

    class ReversePointerResolver:
        @classmethod
        def getptr_from_nqname(cls, schema, source, name):
            ptrs = set()

            for link in schema.get_objects(type='link'):
                if (link.shortname.name == name and
                        link.target is not None and
                        source.issubclass(link.target)):
                    ptrs.add(link)

            return ptrs

        @classmethod
        def getptr_from_fqname(cls, schema, source, name):
            ptrs = set()

            for link in schema.get_objects(type='link'):
                if (link.shortname == name and
                        link.target is not None and
                        source.issubclass(link.target)):
                    ptrs.add(link)

            return ptrs

        @classmethod
        def getptr(cls, schema, source, name):
            if sn.Name.is_qualified(name):
                return cls.getptr_from_fqname(schema, source, name)
            else:
                return cls.getptr_from_nqname(schema, source, name)

        @classmethod
        def getptr_inherited_from(cls, source, schema,
                                  base_ptr_class, skip_scalar):
            result = set()
            for link in schema.get_objects(type='link'):
                if link.issubclass(base_ptr_class) \
                        and link.target is not None \
                        and (not skip_scalar or not link.scalar()) \
                        and source.issubclass(link.target):
                    result.add(link)
            return result

    def getrptr_descending(self, schema, name):
        return self._getptr_descending(schema, name,
                                       self.__class__.ReversePointerResolver)

    def getrptr_ascending(self, schema, name, include_inherited=False):
        return self._getptr_ascending(schema, name,
                                      self.__class__.ReversePointerResolver,
                                      include_inherited=include_inherited)

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        return self.issubclass(other)

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='Object')
        )

    @classmethod
    def get_default_base_name(cls):
        return sn.Name(module='std', name='Object')


class UnionObjectType(SourceNode,
                      constraints.ConsistencySubject, s_types.Type):
    pass


class DerivedObjectType(SourceNode,
                        constraints.ConsistencySubject, s_types.Type):
    pass


class ObjectTypeCommandContext(sd.ObjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               links.LinkSourceCommandContext,
                               lproperties.PropertySourceContext,
                               nodes.NodeCommandContext):
    pass


class ObjectTypeCommand(constraints.ConsistencySubjectCommand,
                        sources.SourceCommand, links.LinkSourceCommand,
                        nodes.NodeCommand,
                        schema_metaclass=ObjectType,
                        context_class=ObjectTypeCommandContext):
    def _apply_field_ast(self, context, node, op):
        if op.property == 'is_derived':
            pass
        else:
            super()._apply_field_ast(context, node, op)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)
        cmd = cls._handle_view_op(cmd, astnode, context, schema)
        return cmd


class CreateObjectType(ObjectTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateObjectType


class RenameObjectType(ObjectTypeCommand, named.RenameNamedObject):
    pass


class RebaseObjectType(ObjectTypeCommand, referencing.RebaseReferencingObject):
    pass


class AlterObjectType(ObjectTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterObjectType


class DeleteObjectType(ObjectTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropObjectType
