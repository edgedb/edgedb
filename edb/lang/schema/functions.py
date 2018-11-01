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


import copy
import functools
import types
import typing

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

    # Private field for caching default value AST.
    _ql_default = struct.Field(object, default=None)

    def get_ql_default(self):
        if self._ql_default is None:
            # Defaults are simple constants, so copying shouldn't
            # be too slow (certainly faster that parsing).

            from edb.lang.edgeql import parser as ql_parser

            qd = ql_parser.parse_fragment(self.default)
            self._ql_default = qd

        return copy.deepcopy(self._ql_default)

    def get_ir_default(self, *, schema):
        if self.default is None:
            return None

        from edb.lang.edgeql import compiler as ql_compiler
        from edb.lang.ir import utils as irutils

        ql_default = self.get_ql_default()

        ir = ql_compiler.compile_ast_fragment_to_ir(
            ql_default, schema,
            location=f'default of the {self.name} parameter')

        if not irutils.is_const(ir):
            raise ValueError('expression not constant')

        return ir

    def __hash__(self):
        return hash((
            self.pos, self.name, self.default,
            self.type, self.typemod, self.kind,
        ))

    def persistent_hash(self):
        return ph.persistent_hash((
            self.pos, self.name, self.default,
            self.type, self.typemod, self.kind,
        ))

    def as_str(self):
        ret = []
        if self.kind is not ft.ParameterKind.POSITIONAL:
            ret.append(self.kind.to_edgeql())
            ret.append(' ')

        ret.append(f'{self.name}: ')

        if self.typemod is not ft.TypeModifier.SINGLETON:
            ret.append(self.typemod.to_edgeql())
            ret.append(' ')
        ret.append(self.type.name)

        if self.default is not None:
            ret.append(f'={self.default}')

        return ''.join(ret)

    def _resolve_type_refs(self, schema):
        if isinstance(self.type, so.ObjectRef):
            result = self.copy()
            result.type = utils.resolve_typeref(self.type, schema)
            return result

        elif isinstance(self.type, s_types.Collection):
            resolved = utils.resolve_typeref(self.type, schema)
            if resolved is not self.type:
                result = self.copy()
                result.type = resolved
            else:
                result = self
            return result

        elif (self.kind is ft.ParameterKind.VARIADIC and
                isinstance(self.type.get_subtypes()[0], so.ObjectRef)):
            subtype = self.type.get_subtypes()[0]
            result = self.copy()
            result.type = s_types.Array.from_subtypes(
                [utils.resolve_typeref(subtype, schema)])
            return result

        else:
            return self


class PgParams(typing.NamedTuple):

    params: typing.List[Parameter]
    has_param_wo_default: bool


class FuncParameterList(typed.FrozenTypedList, type=Parameter):

    @functools.lru_cache(200)
    def get_by_name(self, name) -> Parameter:
        for param in self:
            if param.name == name:
                return param

    @functools.lru_cache(200)
    def as_pg_params(self):
        params = []
        named = []
        variadic = None
        has_param_wo_default = False
        for param in self:
            if param.kind is ft.ParameterKind.POSITIONAL:
                if param.default is None:
                    has_param_wo_default = True
                params.append(param)
            elif param.kind is ft.ParameterKind.NAMED_ONLY:
                if param.default is None:
                    has_param_wo_default = True
                named.append(param)
            else:
                variadic = param

        if variadic is not None:
            params.append(variadic)

        if named:
            named.sort(key=lambda p: p.name)
            named.extend(params)
            params = named

        params = PgParams(
            params=params,
            has_param_wo_default=has_param_wo_default)

        return params

    def as_str(self):
        ret = []
        for param in self:
            ret.append(param.as_str())
        return '(' + ', '.join(ret) + ')'

    @functools.lru_cache(200)
    def has_polymorphic(self):
        return any(p.type.is_polymorphic() for p in self)

    @property
    @functools.lru_cache(200)
    def named_only(self):
        named = {}
        for param in self:
            if param.kind is ft.ParameterKind.NAMED_ONLY:
                named[param.name] = param
        return types.MappingProxyType(named)

    @property
    @functools.lru_cache(200)
    def variadic(self):
        for param in self:
            if param.kind is ft.ParameterKind.VARIADIC:
                return param

    def persistent_hash(self):
        return ph.persistent_hash(tuple(self))

    @classmethod
    def from_ast(cls, astnode, modaliases, schema, *, allow_named=True):
        params = []

        for argi, arg in enumerate(astnode.args):
            argd = None
            if arg.default is not None:
                argd = codegen.generate_source(arg.default)

            argt = utils.ast_to_typeref(
                arg.type, modaliases=modaliases, schema=schema)

            if arg.kind is ft.ParameterKind.VARIADIC:
                argt = s_types.Array.from_subtypes((argt,))

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

    code = so.Field(str, default=None, compcoef=0.4)
    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True)
    from_function = so.Field(str, default=None, compcoef=0.4)

    initial_value = so.Field(expr.ExpressionText, default=None, compcoef=0.4,
                             coerce=True)

    return_typemod = so.Field(ft.TypeModifier, compcoef=0.4, coerce=True)

    @property
    def inlined_defaults(self):
        # This can be relaxed to just `language is EdgeQL` when we
        # support non-constant defaults.
        return bool(self.language is qlast.Language.EdgeQL and
                    self.params.named_only)


class FunctionCommandContext(sd.ObjectCommandContext):
    pass


class FunctionCommandMixin:
    @classmethod
    def _get_function_fullname(cls, name, params: FuncParameterList):
        pgp = params.as_pg_params()

        quals = []
        for param in pgp.params:
            pt = param.type
            if isinstance(pt, so.ObjectRef):
                quals.append(pt.classname)
            elif isinstance(pt, s_types.Collection):
                quals.append(pt.schema_name)
                for st in pt.get_subtypes():
                    if isinstance(st, so.ObjectRef):
                        quals.append(st.classname)
                    else:
                        quals.append(st.name)
            else:
                quals.append(pt.name)

            pk = param.kind
            if pk is ft.ParameterKind.NAMED_ONLY:
                quals.append(f'$NO-{param.name}-{pt.name}$')
            elif pk is ft.ParameterKind.VARIADIC:
                quals.append(f'$V$')

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
        from edb.lang.ir import utils as irutils

        props = super().get_struct_properties(schema)

        params: FuncParameterList = props['params']
        name = props['name']
        language = props['language']
        return_type = props['return_type']
        return_typemod = props['return_typemod']
        from_function = props.get('from_function')
        has_polymorphic = params.has_polymorphic()

        fullname = self._get_function_fullname(name, params)
        get_signature = lambda: f'{self.classname}{params.as_str()}'

        func = schema.get(fullname, None)
        if func:
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} function: '
                f'a function with the same signature '
                f'is already defined',
                context=self.source_context)

        if return_type.is_polymorphic() and not has_polymorphic:
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} function: '
                f'function returns a polymorphic type but has no '
                f'polymorphic parameters',
                context=self.source_context)

        overloaded_funcs = schema.get_functions(name, ())
        has_from_function = from_function

        for func in overloaded_funcs:
            if func.params.named_only.keys() != params.named_only.keys():
                raise ql_errors.EdgeQLError(
                    f'cannot create {get_signature()} function: '
                    f'overloading another function with different '
                    f'named only parameters: '
                    f'"{func.shortname}{func.params.as_str()}"',
                    context=self.source_context)

            if ((has_polymorphic or func.params.has_polymorphic()) and (
                    func.return_typemod != return_typemod)):

                raise ql_errors.EdgeQLError(
                    f'cannot create polymorphic {get_signature()} -> '
                    f'{return_typemod.to_edgeql()} {return_type.name} '
                    f'function: overloading another function with different '
                    f'return type {func.return_typemod.to_edgeql()} '
                    f'{func.return_type.name}',
                    context=self.source_context)

            if func.from_function:
                has_from_function = func.from_function

        if has_from_function:
            if (from_function != has_from_function or
                    any(f.from_function != has_from_function
                        for f in overloaded_funcs)):
                raise ql_errors.EdgeQLError(
                    f'cannot create {get_signature()}: '
                    f'overloading "FROM SQL FUNCTION" functions is '
                    f'allowed only when all functions point to the same '
                    f'SQL function',
                    context=self.source_context)

        if (language == qlast.Language.EdgeQL and
                any(p.typemod is ft.TypeModifier.SET_OF for p in params)):
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} function: '
                f'SET OF parameters in user-defined EdgeQL functions are '
                f'not yet supported',
                context=self.source_context)

        # check that params of type 'anytype' don't have defaults
        for p in params:
            if p.default is None:
                continue

            try:
                default = p.get_ir_default(schema=schema)
            except Exception as ex:
                raise ql_errors.EdgeQLError(
                    f'cannot create {get_signature()} function: '
                    f'invalid default value {p.default} of parameter '
                    f'${p.name}: {ex}',
                    context=self.source_context)

            check_default_type = True
            if p.type.is_polymorphic():
                if irutils.is_empty(default):
                    check_default_type = False
                else:
                    raise ql_errors.EdgeQLError(
                        f'cannot create {get_signature()} function: '
                        f'polymorphic parameter of type {p.type.name} cannot '
                        f'have a non-empty default value',
                        context=self.source_context)
            elif (p.typemod is ft.TypeModifier.OPTIONAL and
                    irutils.is_empty(default)):
                check_default_type = False

            if check_default_type:
                default_type = irutils.infer_type(default, schema)
                if not default_type.assignment_castable_to(p.type, schema):
                    raise ql_errors.EdgeQLError(
                        f'cannot create {get_signature()} function: '
                        f'invalid declaration of parameter ${p.name}: '
                        f'unexpected type of the default expression: '
                        f'{default_type.displayname}, expected '
                        f'{p.type.displayname}',
                        context=self.source_context)

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
            property='return_typemod',
            new_value=astnode.returning_typemod
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
