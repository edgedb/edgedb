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
from . import name as sn
from . import named
from . import objects as so
from . import schema as s_schema
from . import std as s_std


class Delta(named.NamedClass):
    parents = so.Field(named.NamedClassList,
                       default=named.NamedClassList,
                       coerce=True, private=True)

    target = so.Field(s_schema.Schema, private=True,
                      introspectable=False)

    commands = so.Field(sd.CommandList,
                        default=sd.CommandList,
                        coerce=True, private=True,
                        introspectable=False)


class DeltaCommandContext(sd.CommandContextToken):
    pass


class DeltaCommand(named.NamedClassCommand, schema_metaclass=Delta,
                   context_class=DeltaCommandContext):
    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        if astnode.name.module:
            classname = sn.Name(module=astnode.name.module,
                                name=astnode.name.name)
        else:
            classname = astnode.name.name

        return classname


class CreateDelta(named.CreateNamedClass, DeltaCommand):
    astnode = qlast.CreateDelta

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if astnode.target is not None:
            target = s_std.load_std_schema()
            s_decl.load_module_declarations(target, [
                (cmd.classname.module, astnode.target)
            ])

            cmd.add(sd.AlterClassProperty(
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


class AlterDelta(named.CreateOrAlterNamedClass, DeltaCommand):
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
        return delta


class GetDelta(DeltaCommand):
    astnode = qlast.GetDelta

    def apply(self, schema, context):
        delta = schema.get_delta(self.classname)
        return delta
