##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Implementation of DELTA objects."""


from edgedb.lang.edgeql import ast as qlast

from . import declarative as s_decl
from . import delta as sd
from . import name as sn
from . import named
from . import objects as so
from . import schema as s_schema
from . import std as s_std


class Delta(named.NamedPrototype):
    parents = so.Field(named.NamedPrototypeList,
                       default=named.NamedPrototypeList,
                       coerce=True, private=True)

    target = so.Field(s_schema.ProtoSchema, private=True)

    commands = so.Field(sd.CommandList,
                        default=sd.CommandList,
                        coerce=True, private=True)


class DeltaCommandContext(sd.CommandContextToken):
    pass


class DeltaCommand(named.NamedPrototypeCommand):
    context_class = DeltaCommandContext

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        if astnode.name.module:
            prototype_name = sn.Name(module=astnode.name.module,
                                     name=astnode.name.name)
        else:
            prototype_name = astnode.name.name

        return prototype_name

    @classmethod
    def _get_prototype_class(cls):
        return Delta


class CreateDelta(named.CreateNamedPrototype, DeltaCommand):
    astnode = qlast.CreateDeltaNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        if astnode.target is not None:
            target = s_std.load_std_schema()
            s_decl.load_module_declarations(target, [
                (cmd.prototype_name.module, astnode.target)
            ])

            cmd.add(sd.AlterPrototypeProperty(
                property='target',
                new_value=target
            ))

        return cmd

    def apply(self, schema, context):
        props = self.get_struct_properties(schema)
        delta = self.prototype_class(**props)
        schema.add_delta(delta)
        return delta


class AlterDelta(named.CreateOrAlterNamedPrototype, DeltaCommand):
    astnode = qlast.AlterDeltaNode

    def apply(self, schema, context):
        delta = schema.get_delta(self.prototype_name)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(delta, name, value)

        return delta


class DeleteDelta(DeltaCommand):
    astnode = qlast.DropDeltaNode

    def apply(self, schema, context):
        delta = schema.get_delta(self.prototype_name)
        schema.delete_delta(delta)
        return delta


class CommitDelta(DeltaCommand):
    astnode = qlast.CommitDeltaNode

    def apply(self, schema, context):
        delta = schema.get_delta(self.prototype_name)
        return delta
