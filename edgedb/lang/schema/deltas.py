##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Implementation of MIGRATION objects."""


from edgedb.lang.edgeql import ast as qlast

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
                      ephemeral=True)

    commands = so.Field(sd.CommandList,
                        default=sd.CommandList,
                        coerce=True, inheritable=False,
                        ephemeral=True)


class DeltaCommandContext(sd.CommandContextToken):
    pass


class DeltaCommand(named.NamedObjectCommand, schema_metaclass=Delta,
                   context_class=DeltaCommandContext):
    pass


class CreateDelta(named.CreateNamedObject, DeltaCommand):
    astnode = qlast.CreateDelta

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

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
        schema.add_delta(delta)
        return delta


class AlterDelta(named.CreateOrAlterNamedObject, DeltaCommand):
    astnode = qlast.AlterDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(delta, name, value)

        return delta


class DeleteDelta(DeltaCommand):
    astnode = qlast.DropDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        schema.delete_delta(delta)
        return delta


class CommitDelta(DeltaCommand):
    astnode = qlast.CommitDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        for cmd in delta.commands:
            cmd.apply(schema, context)

        return delta


class GetDelta(DeltaCommand):
    astnode = qlast.GetDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        return delta
