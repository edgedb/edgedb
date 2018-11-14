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


class Parameter(named.NamedObject):

    _type = 'parameter'

    num = so.Field(int, frozen=True, compcoef=0.4)
    default = so.Field(str, default=None, frozen=True, compcoef=0.4)
    type = so.Field(so.Object, frozen=True, compcoef=0.4)
    typemod = so.Field(ft.TypeModifier,
                       default=ft.TypeModifier.SINGLETON,
                       coerce=True, frozen=True, compcoef=0.4)
    kind = so.Field(ft.ParameterKind, coerce=True, frozen=True, compcoef=0.4)

    # Private field for caching default value AST.
    _ql_default = so.Field(
        object,
        default=None, ephemeral=True, introspectable=False, hashable=False)

    @classmethod
    def get_shortname(cls, fullname) -> str:
        parts = str(fullname.name).split('@@', 1)
        if len(parts) == 2:
            return cls.unmangle_name(parts[0])
        elif '::' in fullname:
            return sn.Name(fullname).name
        else:
            return fullname

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

    def as_str(self):
        ret = []
        if self.kind is not ft.ParameterKind.POSITIONAL:
            ret.append(self.kind.to_edgeql())
            ret.append(' ')

        ret.append(f'{self.shortname}: ')

        if self.typemod is not ft.TypeModifier.SINGLETON:
            ret.append(self.typemod.to_edgeql())
            ret.append(' ')
        ret.append(self.type.name)

        if self.default is not None:
            ret.append(f'={self.default}')

        return ''.join(ret)


class ParameterCommandContext(sd.ObjectCommandContext):
    pass


class ParameterCommand(named.NamedObjectCommand,
                       schema_metaclass=Parameter,
                       context_class=ParameterCommandContext):
    pass


class CreateParameter(ParameterCommand, named.CreateNamedObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        for sub in cmd.get_subcommands(type=sd.AlterObjectProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(schema, context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteParameter(ParameterCommand, named.DeleteNamedObject):
    pass


class PgParams(typing.NamedTuple):

    params: typing.List[Parameter]
    has_param_wo_default: bool


class FuncParameterList(so.FrozenObjectList, type=Parameter):

    @functools.lru_cache(200)
    def get_by_name(self, name) -> Parameter:
        for param in self:
            if param.shortname == name:
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

    def has_polymorphic(self, schema):
        return any(p.type.is_polymorphic(schema) for p in self)

    @property
    @functools.lru_cache(200)
    def named_only(self):
        named = {}
        for param in self:
            if param.kind is ft.ParameterKind.NAMED_ONLY:
                named[param.shortname] = param

        return types.MappingProxyType(named)

    @property
    @functools.lru_cache(200)
    def variadic(self):
        for param in self:
            if param.kind is ft.ParameterKind.VARIADIC:
                return param

    def persistent_hash(self, *, schema):
        return ph.persistent_hash(tuple(self), schema=schema)

    @classmethod
    def from_ast(cls, schema, astnode, modaliases, *,
                 func_fqname=None, allow_named=True):
        params = []

        if not getattr(astnode, 'params', None):
            return cls(params)

        for i, param in enumerate(astnode.params):
            paramd = None
            if param.default is not None:
                paramd = codegen.generate_source(param.default)

            paramt = utils.ast_to_typeref(
                param.type, modaliases=modaliases, schema=schema)

            if param.kind is ft.ParameterKind.VARIADIC:
                paramt = s_types.Array.from_subtypes((paramt,))

            if param.kind is ft.ParameterKind.NAMED_ONLY and not allow_named:
                raise ql_errors.EdgeQLError(
                    'named only parameters are not allowed in this context',
                    context=astnode.context)

            if func_fqname is not None:
                param_name = sn.Name(
                    module=func_fqname.module,
                    name=Parameter.get_specialized_name(
                        param.name, func_fqname)
                )
            else:
                # We cannot yet determine the proper parameter name,
                # as it is dependent on the function name, which, itself
                # depends on arg shortnames.
                param_name = sn.Name(
                    module='__placeholder__',
                    name=param.name
                )

            params.append(
                Parameter(
                    num=i,
                    name=param_name,
                    type=paramt,
                    typemod=param.typemod,
                    kind=param.kind,
                    default=paramd
                )
            )

        return cls(params)


class CallableObject(so.NamedObject):

    params = so.SchemaField(
        FuncParameterList,
        coerce=True, compcoef=0.4, default=None,
        simpledelta=False)

    return_type = so.SchemaField(
        s_types.Type, compcoef=0.2)

    return_typemod = so.SchemaField(
        ft.TypeModifier, compcoef=0.4, coerce=True)

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        context = context or so.ComparisonContext()

        def param_is_inherited(schema, func, param):
            qualname = Parameter.get_specialized_name(
                param.shortname, func.name)
            return qualname != param.name.name

        with context(old, new):
            delta = super().delta(old, new, context=context,
                                  old_schema=old_schema, new_schema=new_schema)

            if old:
                oldcoll = [p for p in old.get_params(old_schema)
                           if not param_is_inherited(old_schema, old, p)]
            else:
                oldcoll = []

            if new:
                newcoll = [p for p in new.get_params(new_schema)
                           if not param_is_inherited(new_schema, new, p)]
            else:
                newcoll = []

            cls.delta_sets(oldcoll, newcoll, delta, context,
                           old_schema=old_schema, new_schema=new_schema)

        return delta


class CallableCommand(named.NamedObjectCommand):

    def _make_constructor_args(self, schema, context):
        # Make sure the parameter objects exist first and foremost.
        for op in self.get_subcommands(metaclass=Parameter):
            schema, _ = op.apply(schema, context=context)

        schema, props = super()._make_constructor_args(schema, context)

        params = []

        for cr_param in self.get_subcommands(type=ParameterCommand):
            param = schema.get(cr_param.classname)
            params.append(param)

        props['params'] = FuncParameterList(params)

        return schema, props

    def _alter_innards(self, schema, context, scls):
        schema = super()._alter_innards(schema, context, scls)

        for op in self.get_subcommands(metaclass=Parameter):
            schema, _ = op.apply(schema, context=context)

        return schema

    def _delete_innards(self, schema, context, scls):
        schema = super()._delete_innards(schema, context, scls)

        for op in self.get_subcommands(metaclass=Parameter):
            schema, _ = op.apply(schema, context=context)

        return schema

    @classmethod
    def _get_function_name_quals(
            cls, schema, name, params: FuncParameterList) -> typing.List[str]:
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
                quals.append(f'$NO-{param.shortname}-{pt.name}$')
            elif pk is ft.ParameterKind.VARIADIC:
                quals.append(f'$V$')

        return quals

    @classmethod
    def _get_function_fullname(
            cls, schema, name, params: FuncParameterList) -> sn.Name:
        quals = cls._get_function_name_quals(schema, name, params)
        return sn.Name(
            module=name.module,
            name=named.NamedObject.get_specialized_name(name, *quals))


class CreateCallableObject(named.CreateNamedObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = FuncParameterList.from_ast(
            schema, astnode, context.modaliases, func_fqname=cmd.classname)

        for param in params:
            param_delta = param.delta(
                None, param, context=None,
                old_schema=None, new_schema=schema)
            cmd.add(param_delta)

        return cmd


class DeleteCallableObject(named.DeleteNamedObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = FuncParameterList.from_ast(
            schema, astnode, context.modaliases, func_fqname=cmd.classname)

        for param in params:
            param_delta = param.delta(
                param, None, context=None,
                old_schema=schema, new_schema=None)
            cmd.add(param_delta)

        return cmd


class Function(CallableObject):
    _type = 'function'

    code = so.Field(str, default=None, compcoef=0.4, frozen=True)
    language = so.Field(qlast.Language, default=None, compcoef=0.4,
                        coerce=True, frozen=True)
    from_function = so.Field(str, default=None, compcoef=0.4, frozen=True)

    initial_value = so.Field(expr.ExpressionText, default=None, compcoef=0.4,
                             coerce=True, frozen=True)

    def has_inlined_defaults(self, schema):
        # This can be relaxed to just `language is EdgeQL` when we
        # support non-constant defaults.
        return bool(self.language is qlast.Language.EdgeQL and
                    self.get_params(schema).named_only)


class FunctionCommandContext(sd.ObjectCommandContext):
    pass


class FunctionCommand(CallableCommand,
                      schema_metaclass=Function,
                      context_class=FunctionCommandContext):

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        name = super()._classname_from_ast(schema, astnode, context)

        params = FuncParameterList.from_ast(
            schema, astnode, context.modaliases)

        return cls._get_function_fullname(schema, name, params)


class CreateFunction(CreateCallableObject, FunctionCommand):
    astnode = qlast.CreateFunction

    def _add_to_schema(self, schema, context):
        from edb.lang.ir import utils as irutils

        params: FuncParameterList = self.scls.get_params(schema)
        fullname = self.scls.name
        shortname = Function.get_shortname(fullname)
        language = self.scls.language
        return_type = self.scls.get_return_type(schema)
        return_typemod = self.scls.get_return_typemod(schema)
        from_function = self.scls.from_function
        has_polymorphic = params.has_polymorphic(schema)

        get_signature = lambda: f'{shortname}{params.as_str()}'

        func = schema.get(fullname, None)
        if func:
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} function: '
                f'a function with the same signature '
                f'is already defined',
                context=self.source_context)

        if return_type.is_polymorphic(schema) and not has_polymorphic:
            raise ql_errors.EdgeQLError(
                f'cannot create {get_signature()} function: '
                f'function returns a polymorphic type but has no '
                f'polymorphic parameters',
                context=self.source_context)

        overloaded_funcs = schema.get_functions(shortname, ())
        has_from_function = from_function

        for func in overloaded_funcs:
            func_params = func.get_params(schema)
            if func_params.named_only.keys() != params.named_only.keys():
                raise ql_errors.EdgeQLError(
                    f'cannot create {get_signature()} function: '
                    f'overloading another function with different '
                    f'named only parameters: '
                    f'"{func.shortname}{func_params.as_str()}"',
                    context=self.source_context)

            if ((has_polymorphic or func_params.has_polymorphic(schema)) and (
                    func.get_return_typemod(schema) != return_typemod)):

                func_return_typemod = func.get_return_typemod(schema)
                raise ql_errors.EdgeQLError(
                    f'cannot create polymorphic {get_signature()} -> '
                    f'{return_typemod.to_edgeql()} {return_type.name} '
                    f'function: overloading another function with different '
                    f'return type {func_return_typemod.to_edgeql()} '
                    f'{func.get_return_type(schema).name}',
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
            if p.type.is_polymorphic(schema):
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

        return super()._add_to_schema(schema, context)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases
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


class DeleteFunction(DeleteCallableObject, FunctionCommand):
    astnode = qlast.DropFunction
