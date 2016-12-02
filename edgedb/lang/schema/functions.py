##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import primary


class FunctionCommandContext(sd.ClassCommandContext):
    pass


class FunctionCommand(sd.ClassCommand):
    context_class = FunctionCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Function


class CreateFunction(named.CreateNamedClass, FunctionCommand):
    astnode = qlast.CreateFunctionNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.add(sd.AlterClassProperty(
            property='returntype',
            new_value=so.ClassRef(
                classname=sn.Name(
                    module=astnode.returning.maintype.module,
                    name=astnode.returning.maintype.name)
            )
        ))

        cmd.add(sd.AlterClassProperty(
            property='aggregate',
            new_value=astnode.aggregate
        ))

        return cmd


class RenameFunction(named.RenameNamedClass, FunctionCommand):
    pass


class AlterFunction(named.AlterNamedClass, FunctionCommand):
    astnode = qlast.AlterFunctionNode


class DeleteFunction(named.DeleteNamedClass, FunctionCommand):
    astnode = qlast.DropFunctionNode


class Function(primary.PrimaryClass):
    _type = 'function'

    paramtypes = so.Field(so.ClassDict, default=None, coerce=True,
                          compcoef=0.4)
    paramkinds = so.Field(dict, compcoef=0.3, default=None)
    paramdefaults = so.Field(expr.ExpressionDict, default=None, coerce=True)
    returntype = so.Field(primary.PrimaryClass, compcoef=0.2)
    aggregate = so.Field(bool, default=False, compcoef=0.4)

    delta_driver = sd.DeltaDriver(
        create=CreateFunction,
        alter=AlterFunction,
        rename=RenameFunction,
        delete=DeleteFunction
    )
