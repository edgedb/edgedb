##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.functional import hybridmethod

from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import primary


class FunctionCommandContext(sd.PrototypeCommandContext):
    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if (name == 'proto' and value is not None
                and value.__class__.__name__ != 'Function'):
            assert False, value


class FunctionCommand(sd.PrototypeCommand):
    context_class = FunctionCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Function


class CreateFunction(named.CreateNamedPrototype, FunctionCommand):
    astnode = qlast.CreateFunctionNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.add(sd.AlterPrototypeProperty(
            property='returntype',
            new_value=so.PrototypeRef(
                prototype_name=sn.Name(
                    module=astnode.returning.maintype.module,
                    name=astnode.returning.maintype.name)
            )
        ))

        cmd.add(sd.AlterPrototypeProperty(
            property='aggregate',
            new_value=astnode.aggregate
        ))

        return cmd


class RenameFunction(named.RenameNamedPrototype, FunctionCommand):
    pass


class AlterFunction(named.AlterNamedPrototype, FunctionCommand):
    astnode = qlast.AlterFunctionNode


class DeleteFunction(named.DeleteNamedPrototype, FunctionCommand):
    astnode = qlast.DropFunctionNode


class Function(named.NamedPrototype):
    _type = 'function'

    paramtypes = so.Field(so.PrototypeDict, default=None, coerce=True,
                          compcoef=0.4)
    paramkinds = so.Field(dict, compcoef=0.3, default=None)
    paramdefaults = so.Field(expr.ExpressionDict, default=None, coerce=True)
    returntype = so.Field(primary.Prototype, compcoef=0.2)
    aggregate = so.Field(bool, default=False, compcoef=0.4)

    delta_driver = sd.DeltaDriver(
        create=CreateFunction,
        alter=AlterFunction,
        rename=RenameFunction,
        delete=DeleteFunction
    )
