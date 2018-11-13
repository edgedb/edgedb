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

from . import declarative as s_decl
from . import delta as sd
from . import named
from . import objects as so
from . import schema as s_schema
from . import std as s_std


class Delta(named.NamedObject):
    parents = so.Field(named.NamedObjectList,
                       default=named.NamedObjectList,
                       coerce=True, inheritable=False)

    target = so.Field(s_schema.Schema, inheritable=False,
                      default=None, introspectable=False)

    commands = so.Field(sd.CommandList,
                        default=sd.CommandList,
                        coerce=True, inheritable=False,
                        introspectable=False)


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
            target = s_std.load_std_schema()
            s_decl.load_module_declarations(target, [
                (cmd.classname.module, astnode.target)
            ])

            modules = (
                set(target.modules) - {'std', 'schema', 'stdattrs'})
            if len(modules) != 1:
                raise RuntimeError('unexpected delta module structure')

            modname = next(iter(modules))

            diff = sd.delta_module(target, schema, modname)
            migration = list(diff.get_subcommands())

            for op in cmd.get_subcommands(type=sd.AlterObjectProperty):
                if op.property == 'commands':
                    op.new_value = migration + op.new_value
                    break
            else:
                cmd.add(sd.AlterObjectProperty(
                    property='commands',
                    new_value=migration
                ))

            cmd.add(sd.AlterObjectProperty(
                property='target',
                new_value=target
            ))

        return cmd

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        metaclass = self.get_schema_metaclass()
        delta = metaclass(**props)
        schema = schema.add_delta(delta)
        return schema, delta


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
        for cmd in delta.commands:
            schema, _ = cmd.apply(schema, context)

        return schema, delta


class GetDelta(DeltaCommand):
    astnode = qlast.GetDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        return schema, delta
