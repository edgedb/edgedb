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


from edb.lang.common import struct
from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import error as s_err
from . import name as sn
from . import named
from . import objects as so


class Module(named.NamedObject):
    # Override 'name' to str type, since modules don't have
    # fully-qualified names.
    name = so.SchemaField(str)


class ModuleCommandContext(sd.CommandContextToken):
    def __init__(self, schema, op, module):
        super().__init__(op)
        self.module = module


class ModuleCommand(named.NamedObjectCommand, schema_metaclass=Module,
                    context_class=ModuleCommandContext):

    classname = struct.Field(str)

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        if astnode.name.module:
            classname = sn.Name(module=astnode.name.module,
                                name=astnode.name.name)
        else:
            classname = astnode.name.name

        return classname


class CreateModule(named.CreateNamedObject, ModuleCommand):
    astnode = qlast.CreateModule

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        metaclass = self.get_schema_metaclass()
        if schema.get_module(self.classname) is not None:
            raise s_err.SchemaError(
                f'module {self.classname!r} already exists',
                context=self.source_context)
        return metaclass.create_in_schema(schema, **props)


class AlterModule(named.CreateOrAlterNamedObject, ModuleCommand):
    astnode = qlast.AlterModule

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(self.module, name, value)

        return schema, self.module


class DeleteModule(ModuleCommand):
    astnode = qlast.DropModule

    def apply(self, schema, context):
        self.module = schema.get_module(self.classname)
        schema = schema.delete_module(self.module)
        return schema, self.module
