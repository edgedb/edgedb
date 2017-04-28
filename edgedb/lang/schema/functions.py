##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors as ql_errors
from edgedb.lang.edgeql import codegen

from . import delta as sd
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import utils


class FunctionCommandContext(sd.ClassCommandContext):
    pass


class FunctionCommandMixin:
    context_class = FunctionCommandContext

    @classmethod
    def _get_metaclass(cls):
        return Function

    @classmethod
    def _get_function_fullname(cls, name, paramtypes):
        quals = []
        if paramtypes:
            for pt in paramtypes:
                if isinstance(pt, so.ClassRef):
                    quals.append(pt.classname)
                elif isinstance(pt, so.Collection):
                    quals.append(pt.schema_name)
                    if isinstance(pt.element_type, so.ClassRef):
                        quals.append(pt.element_type.classname)
                    else:
                        quals.append(pt.element_type.name)
                else:
                    quals.append(pt.name)

        return sn.Name(
            module=name.module,
            name=named.NamedClass.get_specialized_name(name, *quals))

    @classmethod
    def _parameters_from_ast(cls, astnode):
        paramdefaults = []
        paramnames = []
        paramtypes = []
        variadic = None
        for argi, arg in enumerate(astnode.args, 1):
            paramnames.append(arg.name)

            default = None
            if arg.default is not None:
                default = codegen.generate_source(arg.default)
            paramdefaults.append(default)

            paramtypes.append(utils.ast_to_typeref(arg.type))

            if arg.variadic:
                variadic = argi

        return paramnames, paramdefaults, paramtypes, variadic


class CreateFunction(named.CreateNamedClass, FunctionCommandMixin):
    astnode = qlast.CreateFunction

    def get_struct_properties(self, schema):
        props = super().get_struct_properties(schema)
        props['name'] = self._get_function_fullname(props['name'],
                                                    props.get('paramtypes'))
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

        paramnames, paramdefaults, paramtypes, variadic = \
            cls._parameters_from_ast(astnode)

        if variadic is not None:
            cmd.add(sd.AlterClassProperty(
                property='varparam',
                new_value=variadic
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
            new_value=utils.ast_to_typeref(astnode.returning)
        ))

        cmd.add(sd.AlterClassProperty(
            property='aggregate',
            new_value=astnode.aggregate
        ))

        cmd.add(sd.AlterClassProperty(
            property='set_returning',
            new_value=astnode.set_returning
        ))

        if astnode.initial_value is not None:
            iv = codegen.generate_source(astnode.initial_value)
            cmd.add(sd.AlterClassProperty(
                property='initial_value',
                new_value=iv
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
    astnode = qlast.AlterFunction


class DeleteFunction(named.DeleteNamedClass, FunctionCommandMixin):
    astnode = qlast.DropFunction

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        name = super()._classname_from_ast(astnode, context, schema)

        _, _, paramtypes, _ = cls._parameters_from_ast(astnode)

        return cls._get_function_fullname(name, paramtypes)


class Function(primary.PrimaryClass):
    _type = 'function'

    paramnames = so.Field(so.StringList, default=None, coerce=True,
                          compcoef=0.4)

    paramtypes = so.Field(so.TypeList, default=None, coerce=True,
                          compcoef=0.4)

    # Number of the variadic parameter (+1)
    varparam = so.Field(int, default=None, compcoef=0.4)

    paramdefaults = so.Field(expr.ExpressionList, default=None, coerce=True)
    returntype = so.Field(so.Class, compcoef=0.2)
    aggregate = so.Field(bool, default=False, compcoef=0.4)

    code = so.Field(str, default=None, compcoef=0.4)
    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True)
    from_function = so.Field(str, default=None, compcoef=0.4)

    initial_value = so.Field(expr.ExpressionText, default=None, compcoef=0.4,
                             coerce=True)

    set_returning = so.Field(bool, default=False, compcoef=0.4)

    delta_driver = sd.DeltaDriver(
        create=CreateFunction,
        alter=AlterFunction,
        rename=RenameFunction,
        delete=DeleteFunction
    )
