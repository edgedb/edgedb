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


from edb.lang.common import persistent_hash as ph
from edb.lang.common import struct
from edb.lang.common import typed

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors
from edb.lang.edgeql import codegen
from edb.lang.edgeql import functypes as ft

from . import delta as sd
from . import expr
from . import name as sn
from . import named
from . import objects as so
from . import types as s_types
from . import utils


class Parameter(struct.Struct):

    pos = struct.Field(int)
    name = struct.Field(str)
    default = struct.Field(str, default=None)
    type = struct.Field(so.Object)
    typemod = struct.Field(ft.TypeModifier,
                           default=ft.TypeModifier.SINGLETON,
                           coerce=True)
    kind = struct.Field(ft.ParameterKind, coerce=True)

    def persistent_hash(self):
        return ph.persistent_hash((
            self.pos, self.name, self.default,
            self.type, self.typemod, self.kind,
        ))

    def _resolve_type_refs(self, schema):
        if isinstance(self.type, so.ObjectRef):
            result = self.copy()
            result.type = utils.resolve_typeref(self.type, schema)
            return result
        else:
            return self


class FuncParameterList(typed.TypedList, type=Parameter):

    def get_by_name(self, name) -> Parameter:
        for param in self:
            if param.name == name:
                return param

    @property
    def variadic(self):
        try:
            return self.__variadic
        except AttributeError:
            pass

        self.__variadic = None
        for param in self:
            if param.kind is ft.ParameterKind.VARIADIC:
                self.__variadic = param
                return param

    @classmethod
    def from_ast(cls, astnode, modaliases, schema, *, allow_named=True):
        params = []

        for argi, arg in enumerate(astnode.args):
            argd = None
            if arg.default is not None:
                argd = codegen.generate_source(arg.default)

            argt = utils.ast_to_typeref(
                arg.type, modaliases=modaliases, schema=schema)

            if arg.kind is ft.ParameterKind.NAMED_ONLY and not allow_named:
                raise ql_errors.EdgeQLError(
                    'named only parameters are not allowed in this context',
                    context=astnode.context)

            param = Parameter(
                pos=argi,
                name=arg.name,
                type=argt,
                typemod=arg.typemod,
                kind=arg.kind,
                default=argd)

            params.append(param)

        return cls(params)


class Function(so.NamedObject):
    _type = 'function'

    params = so.Field(FuncParameterList, default=None,
                      coerce=True, compcoef=0.4)

    return_type = so.Field(so.Object, compcoef=0.2)
    aggregate = so.Field(bool, default=False, compcoef=0.4)

    code = so.Field(str, default=None, compcoef=0.4)
    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True)
    from_function = so.Field(str, default=None, compcoef=0.4)

    initial_value = so.Field(expr.ExpressionText, default=None, compcoef=0.4,
                             coerce=True)

    return_typemod = so.Field(ft.TypeModifier, compcoef=0.4, coerce=True)


class FunctionCommandContext(sd.ObjectCommandContext):
    pass


class FunctionCommandMixin:
    @classmethod
    def _get_function_fullname(cls, name, params: FuncParameterList):
        quals = []
        if params:
            for param in params:
                pt = param.type
                if isinstance(pt, so.ObjectRef):
                    quals.append(pt.classname)
                elif isinstance(pt, s_types.Collection):
                    quals.append(pt.schema_name)
                    if isinstance(pt.element_type, so.ObjectRef):
                        quals.append(pt.element_type.classname)
                    else:
                        quals.append(pt.element_type.name)
                else:
                    quals.append(pt.name)

        return sn.Name(
            module=name.module,
            name=named.NamedObject.get_specialized_name(name, *quals))


class FunctionCommand(named.NamedObjectCommand, FunctionCommandMixin,
                      schema_metaclass=Function,
                      context_class=FunctionCommandContext):
    pass


class CreateFunction(named.CreateNamedObject, FunctionCommand):
    astnode = qlast.CreateFunction

    def get_struct_properties(self, schema):
        props = super().get_struct_properties(schema)

        props['name'] = self._get_function_fullname(
            props['name'], props['params'])
        return props

    def _add_to_schema(self, schema):
        props = super().get_struct_properties(schema)

        fullname = self._get_function_fullname(
            props['name'], props['params'])

        # check that params of type 'any' don't have defaults
        for p in props.get('params'):
            if p.type.name == 'std::any' and p.default is not None:
                raise ql_errors.EdgeQLError(
                    f'Cannot create a function {self.classname}: '
                    f'polymorphic parameter of type {p.type.name} cannot have '
                    f'a default value', context=self.source_context)

        func = schema.get(fullname, None)
        if func:
            raise ql_errors.EdgeQLError(
                f'Cannot create a function {self.classname}: '
                f'a function with the same signature '
                f'is already defined', context=self.source_context)

        super()._add_to_schema(schema)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        modaliases = context.modaliases

        params = FuncParameterList.from_ast(astnode, modaliases, schema)

        cmd.add(sd.AlterObjectProperty(
            property='params',
            new_value=params
        ))

        cmd.add(sd.AlterObjectProperty(
            property='return_type',
            new_value=utils.ast_to_typeref(
                astnode.returning, modaliases=modaliases, schema=schema)
        ))

        cmd.add(sd.AlterObjectProperty(
            property='aggregate',
            new_value=astnode.aggregate
        ))

        cmd.add(sd.AlterObjectProperty(
            property='return_typemod',
            new_value=astnode.returning_typemod
        ))

        if astnode.initial_value is not None:
            iv = codegen.generate_source(astnode.initial_value)
            cmd.add(sd.AlterObjectProperty(
                property='initial_value',
                new_value=iv
            ))

        if astnode.code is not None:
            cmd.add(sd.AlterObjectProperty(
                property='language',
                new_value=astnode.code.language
            ))
            if astnode.code.from_name is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_function',
                    new_value=astnode.code.from_name
                ))
            else:
                cmd.add(sd.AlterObjectProperty(
                    property='code',
                    new_value=astnode.code.code
                ))

        return cmd


class RenameFunction(named.RenameNamedObject, FunctionCommand):
    pass


class AlterFunction(named.AlterNamedObject, FunctionCommand):
    astnode = qlast.AlterFunction


class DeleteFunction(named.DeleteNamedObject, FunctionCommand):
    astnode = qlast.DropFunction

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        name = super()._classname_from_ast(astnode, context, schema)

        params = FuncParameterList.from_ast(
            astnode, context.modaliases, schema)

        return cls._get_function_fullname(name, params)
