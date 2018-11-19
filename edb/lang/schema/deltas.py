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


"""Implementation of MIGRATION objects."""


from edb.lang.edgeql import ast as qlast

from . import ast as s_ast
from . import delta as sd
from . import named
from . import objects as so


class Delta(named.NamedObject):
    parents = so.SchemaField(
        so.ObjectList,
        default=so.ObjectList, coerce=True, inheritable=False)

    target = so.SchemaField(
        s_ast.Schema,
        inheritable=False, default=None, introspectable=False)

    commands = so.SchemaField(
        sd.CommandList,
        default=sd.CommandList,
        coerce=True, inheritable=False, introspectable=False)


class DeltaCommandContext(sd.CommandContextToken):
    pass


class DeltaCommand(named.NamedObjectCommand, schema_metaclass=Delta,
                   context_class=DeltaCommandContext):
    pass


class CreateDelta(named.CreateNamedObject, DeltaCommand):
    astnode = qlast.CreateDelta

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if astnode.target is not None:
            cmd.add(sd.AlterObjectProperty(
                property='target',
                new_value=astnode.target
            ))

        return cmd

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        metaclass = self.get_schema_metaclass()
        return metaclass.create_in_schema(schema, **props)


class AlterDelta(named.CreateOrAlterNamedObject, DeltaCommand):
    astnode = qlast.AlterDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(delta, name, value)

        return schema, delta


class DeleteDelta(DeltaCommand):
    astnode = qlast.DropDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        schema = schema.delete_delta(delta)
        return schema, delta


class CommitDelta(DeltaCommand):
    astnode = qlast.CommitDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        for cmd in delta.get_commands(schema):
            schema, _ = cmd.apply(schema, context)

        return schema, delta


class GetDelta(DeltaCommand):
    astnode = qlast.GetDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        return schema, delta
