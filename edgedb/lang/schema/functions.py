##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors as ql_errors

from . import delta as sd
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import primary


class FunctionCommandContext(sd.ClassCommandContext):
    pass


class FunctionCommandMixin:
    context_class = FunctionCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Function


class CreateFunction(named.CreateNamedClass, FunctionCommandMixin):
    astnode = qlast.CreateFunctionNode

    def get_struct_properties(self, schema):
        props = super().get_struct_properties(schema)

        quals = []
        if props.get('paramtypes'):
            for param in props['paramtypes']:
                quals.append(param.name)

        fname = props['name']
        props['name'] = sn.Name(
            module=fname.module,
            name=named.NamedClass.get_specialized_name(fname, *quals))

        return props

    def _add_to_schema(self, schema):
        func = self.scls

        funcs = schema.get_functions(self.classname, None)
        if funcs:
            is_aggregate = funcs[-1].aggregate
            if func.aggregate and not is_aggregate:
                raise ql_errors.EdgeQLError(
                    f'Cannot create an aggregate function {self.classname}: '
                    f'a non-aggregate function with the same name '
                    f'is already defined', context=self.source_context)
            elif not func.aggregate and is_aggregate:
                raise ql_errors.EdgeQLError(
                    f'Cannot create a function {self.classname}: '
                    f'an aggregate function with the same name '
                    f'is already defined', context=self.source_context)

        super()._add_to_schema(schema)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        paramdefaults = []
        paramnames = []
        paramtypes = []
        for argi, arg in enumerate(astnode.args, 1):
            paramnames.append(arg.name)

            paramdefaults.append(arg.default)

            paramtypes.append(
                so.ClassRef(classname=sn.Name(
                    module=arg.type.maintype.module,
                    name=arg.type.maintype.name)))

            if arg.variadic:
                cmd.add(sd.AlterClassProperty(
                    property='varparam',
                    new_value=argi
                ))

        cmd.add(sd.AlterClassProperty(
            property='paramnames',
            new_value=paramnames
        ))

        cmd.add(sd.AlterClassProperty(
            property='paramtypes',
            new_value=paramtypes
        ))

        cmd.add(sd.AlterClassProperty(
            property='paramdefaults',
            new_value=paramdefaults
        ))

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

        if astnode.code is not None:
            cmd.add(sd.AlterClassProperty(
                property='language',
                new_value=astnode.code.language
            ))
            if astnode.code.from_name is not None:
                cmd.add(sd.AlterClassProperty(
                    property='from_function',
                    new_value=astnode.code.from_name
                ))
            else:
                cmd.add(sd.AlterClassProperty(
                    property='code',
                    new_value=astnode.code.code
                ))

        return cmd


class RenameFunction(named.RenameNamedClass, FunctionCommandMixin):
    pass


class AlterFunction(named.AlterNamedClass, FunctionCommandMixin):
    astnode = qlast.AlterFunctionNode


class DeleteFunction(named.DeleteNamedClass, FunctionCommandMixin):
    astnode = qlast.DropFunctionNode


class Function(primary.PrimaryClass):
    _type = 'function'

    paramnames = so.Field(so.StringList, default=None, coerce=True,
                          compcoef=0.4)

    paramtypes = so.Field(so.TypeList, default=None, coerce=True,
                          compcoef=0.4)

    # Number of the variadic parameter (+1)
    varparam = so.Field(int, default=None, compcoef=0.4)

    paramdefaults = so.Field(expr.ExpressionList, default=None, coerce=True)
    returntype = so.Field(primary.PrimaryClass, compcoef=0.2)
    aggregate = so.Field(bool, default=False, compcoef=0.4)

    code = so.Field(str, default=None, compcoef=0.4)
    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True)
    from_function = so.Field(str, default=None, compcoef=0.4)

    delta_driver = sd.DeltaDriver(
        create=CreateFunction,
        alter=AlterFunction,
        rename=RenameFunction,
        delete=DeleteFunction
    )
