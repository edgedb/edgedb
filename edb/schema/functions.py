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


from __future__ import annotations

import types
from typing import *  # NoQA

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr
from . import name as sn
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


def param_as_str(schema, param):
    ret = []
    kind = param.get_kind(schema)
    typemod = param.get_typemod(schema)
    default = param.get_default(schema)

    if kind is not ft.ParameterKind.POSITIONAL:
        ret.append(kind.to_edgeql())
        ret.append(' ')

    ret.append(f'{param.get_shortname(schema)}: ')

    if typemod is not ft.TypeModifier.SINGLETON:
        ret.append(typemod.to_edgeql())
        ret.append(' ')
    ret.append(param.get_type(schema).get_name(schema))

    if default is not None:
        ret.append(f'={default.origtext}')

    return ''.join(ret)


# Non-schema description of a parameter.
class ParameterDesc(NamedTuple, s_abc.Parameter):

    num: int
    name: str
    default: expr.Expression
    type: s_types.Type
    typemod: ft.TypeModifier
    kind: ft.ParameterKind

    @classmethod
    def from_ast(cls, schema, modaliases,
                 num: int, astnode) -> ParameterDesc:
        paramd = None
        if astnode.default is not None:
            defexpr = expr.Expression.from_ast(
                astnode.default, schema, modaliases, as_fragment=True)
            paramd = expr.Expression.compiled(
                defexpr, schema, modaliases=modaliases, as_fragment=True)

        paramt = utils.resolve_typeref(
            utils.ast_to_typeref(
                astnode.type, modaliases=modaliases, schema=schema),
            schema)

        if astnode.kind is ft.ParameterKind.VARIADIC:
            paramt = s_types.Array.from_subtypes(schema, (paramt,))

        return cls(
            num=num,
            name=astnode.name,
            type=paramt,
            typemod=astnode.typemod,
            kind=astnode.kind,
            default=paramd
        )

    def get_shortname(self, schema):
        return self.name

    def get_name(self, schema):
        return self.name

    def get_kind(self, _):
        return self.kind

    def get_default(self, _):
        return self.default

    def get_type(self, _):
        return self.type

    def get_typemod(self, _):
        return self.typemod

    def as_str(self, schema) -> str:
        return param_as_str(schema, self)

    @classmethod
    def from_create_delta(cls, schema, context, cmd):
        props = cmd.get_struct_properties(schema)
        props['name'] = Parameter.paramname_from_fullname(props['name'])
        return schema, cls(
            num=props['num'],
            name=props['name'],
            type=props['type'],
            typemod=props['typemod'],
            kind=props['kind'],
            default=props.get('default'),
        )

    def as_create_delta(self, schema, func_fqname, *, context):
        CreateParameter = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.CreateObject, Parameter)

        param_name = sn.Name(
            module=func_fqname.module,
            name=sn.get_specialized_name(
                self.get_name(schema), func_fqname)
        )

        cmd = CreateParameter(classname=param_name)

        cmd.add(sd.AlterObjectProperty(
            property='name',
            new_value=param_name,
        ))

        cmd.add(sd.AlterObjectProperty(
            property='type',
            new_value=utils.reduce_to_typeref(schema, self.type),
        ))

        if self.type.is_collection() and not self.type.is_polymorphic(schema):
            sd.ensure_schema_collection(
                schema, self.type, cmd, context=context)

        for attr in ('num', 'typemod', 'kind', 'default'):
            cmd.add(sd.AlterObjectProperty(
                property=attr,
                new_value=getattr(self, attr),
            ))

        return cmd

    def as_delete_delta(self, schema, func_fqname, *, context):
        DeleteParameter = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.DeleteObject, Parameter)

        param_name = sn.Name(
            module=func_fqname.module,
            name=sn.get_specialized_name(
                self.get_name(schema), func_fqname)
        )

        cmd = DeleteParameter(classname=param_name)

        if self.type.is_collection() and not self.type.is_polymorphic(schema):
            param = schema.get(param_name)
            sd.cleanup_schema_collection(schema, self.type, param, cmd,
                                         context=context)

        return cmd


class Parameter(so.ObjectFragment, s_abc.Parameter):

    num = so.SchemaField(
        int, compcoef=0.4)

    default = so.SchemaField(
        expr.Expression, default=None, compcoef=0.4)

    type = so.SchemaField(
        s_types.Type, compcoef=0.4)

    typemod = so.SchemaField(
        ft.TypeModifier,
        default=ft.TypeModifier.SINGLETON,
        coerce=True, compcoef=0.4)

    kind = so.SchemaField(
        ft.ParameterKind, coerce=True, compcoef=0.4)

    @classmethod
    def paramname_from_fullname(cls, fullname):
        parts = str(fullname.name).split('@@', 1)
        if len(parts) == 2:
            return sn.unmangle_name(parts[0])
        elif '::' in fullname:
            return sn.Name(fullname).name
        else:
            return fullname

    def get_verbosename(self, schema, *, with_parent: bool=False) -> str:
        vn = super().get_verbosename(schema)
        if with_parent:
            pfns = [r for r in schema.get_referrers(self)
                    if isinstance(r, CallableObject)]
            if pfns:
                pvn = pfns[0].get_verbosename(schema, with_parent=True)
                return f'{vn} of {pvn}'
            else:
                return vn
        else:
            return vn

    def get_shortname(self, schema) -> str:
        fullname = self.get_name(schema)
        return self.paramname_from_fullname(fullname)

    def get_ql_default(self, schema):
        return self.get_default(schema).qlast

    def get_ir_default(self, *, schema):
        from edb.ir import utils as irutils

        defexpr = self.get_default(schema)
        defexpr = expr.Expression.compiled(
            defexpr, as_fragment=True, schema=schema)
        ir = defexpr.irast
        if not irutils.is_const(ir.expr):
            raise ValueError('expression not constant')
        return ir

    def as_str(self, schema) -> str:
        return param_as_str(schema, self)


class CallableCommandContext(sd.ObjectCommandContext,
                             s_anno.AnnotationSubjectCommandContext):
    pass


class ParameterCommandContext(sd.ObjectCommandContext):
    pass


class ParameterCommand(referencing.StronglyReferencedObjectCommand,
                       schema_metaclass=Parameter,
                       context_class=ParameterCommandContext,
                       referrer_context_class=CallableCommandContext):
    pass


class CreateParameter(ParameterCommand, sd.CreateObject):

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


class DeleteParameter(ParameterCommand, sd.DeleteObject):
    pass


class PgParams(NamedTuple):

    params: Tuple[Parameter, ...]
    has_param_wo_default: bool

    @classmethod
    def from_params(cls, schema, params):
        pg_params = []
        named = []
        variadic = None
        has_param_wo_default = False

        if isinstance(params, FuncParameterList):
            params = params.objects(schema)

        for param in params:
            param_kind = param.get_kind(schema)
            param_default = param.get_default(schema)

            if param_kind is ft.ParameterKind.POSITIONAL:
                if param_default is None:
                    has_param_wo_default = True
                pg_params.append(param)
            elif param_kind is ft.ParameterKind.NAMED_ONLY:
                if param_default is None:
                    has_param_wo_default = True
                named.append(param)
            else:
                variadic = param

        if variadic is not None:
            pg_params.append(variadic)

        if named:
            named.sort(key=lambda p: p.get_name(schema))
            named.extend(pg_params)
            pg_params = named

        return cls(
            params=tuple(pg_params),
            has_param_wo_default=has_param_wo_default)


class FuncParameterList(so.ObjectList, type=Parameter):

    def get_by_name(self, schema, name) -> Parameter:
        for param in self.objects(schema):
            if param.get_shortname(schema) == name:
                return param

    def as_str(self, schema):
        ret = []
        for param in self.objects(schema):
            ret.append(param.as_str(schema))
        return '(' + ', '.join(ret) + ')'

    def has_polymorphic(self, schema):
        return any(p.get_type(schema).is_polymorphic(schema)
                   for p in self.objects(schema))

    def find_named_only(self, schema) -> Mapping[str, Parameter]:
        named = {}
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.NAMED_ONLY:
                named[param.get_shortname(schema)] = param

        return types.MappingProxyType(named)

    def find_variadic(self, schema) -> Optional[Parameter]:
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.VARIADIC:
                return param

    @classmethod
    def from_ast(cls, schema, astnode, modaliases, *, func_fqname,
                 prepend: Optional[List[qlast.FuncParam]]=None):
        if not getattr(astnode, 'params', None):
            return cls.create(schema, [])

        if prepend is None:
            prepend = []

        params = []

        for num, param in enumerate(prepend + list(astnode.params)):
            param_desc = ParameterDesc.from_ast(
                schema, modaliases, num, param)

            param_name = sn.Name(
                module=func_fqname.module,
                name=sn.get_specialized_name(
                    param.name, func_fqname)
            )

            schema, param = Parameter.create_in_schema(
                schema,
                num=num,
                name=param_name,
                type=param_desc.type,
                typemod=param_desc.typemod,
                kind=param_desc.kind,
                default=param_desc.default)

            params.append(param)

        return schema, cls.create(schema, params)


class VolatilitySubject(so.Object):

    volatility = so.SchemaField(
        ft.Volatility, default=ft.Volatility.VOLATILE,
        compcoef=0.4, coerce=True, allow_ddl_set=True)


class CallableLike:
    """A minimal callable object interface required by multidispatch."""

    def has_inlined_defaults(self, schema) -> bool:
        raise NotImplementedError

    def get_params(self, schema) -> FuncParameterList:
        raise NotImplementedError

    def get_return_type(self, schema) -> s_types.Type:
        raise NotImplementedError

    def get_return_typemod(self, schema) -> ft.TypeModifier:
        raise NotImplementedError

    def get_verbosename(self, schema) -> str:
        raise NotImplementedError

    def get_is_abstract(self, schema) -> bool:
        raise NotImplementedError


class CallableObject(s_anno.AnnotationSubject, CallableLike):

    params = so.SchemaField(
        FuncParameterList,
        coerce=True, compcoef=0.4, default=FuncParameterList,
        inheritable=False, simpledelta=False)

    return_type = so.SchemaField(
        s_types.Type, compcoef=0.2)

    return_typemod = so.SchemaField(
        ft.TypeModifier, compcoef=0.4, coerce=True)

    is_abstract = so.SchemaField(
        bool, default=False, compcoef=0.909)

    @classmethod
    def delta(cls, old, new, *, context=None, old_schema, new_schema):
        context = context or so.ComparisonContext()

        def param_is_inherited(schema, func, param):
            qualname = sn.get_specialized_name(
                param.get_shortname(schema), func.get_name(schema))
            return qualname != param.get_name(schema).name

        with context(old, new):
            delta = super().delta(old, new, context=context,
                                  old_schema=old_schema, new_schema=new_schema)

            if old:
                old_params = old.get_params(old_schema).objects(old_schema)
                oldcoll = [p for p in old_params
                           if not param_is_inherited(old_schema, old, p)]
            else:
                oldcoll = []

            if new:
                new_params = new.get_params(new_schema).objects(new_schema)
                newcoll = [p for p in new_params
                           if not param_is_inherited(new_schema, new, p)]
            else:
                newcoll = []

            delta.update(cls.delta_sets(
                oldcoll, newcoll, context,
                old_schema=old_schema, new_schema=new_schema))

        return delta

    @classmethod
    def _get_fqname_quals(
            cls, schema,
            params: List[ParameterDesc]) -> Tuple[str, ...]:
        pgp = PgParams.from_params(schema, params)

        quals = []
        for param in pgp.params:
            pt = param.get_type(schema)
            if isinstance(pt, s_abc.Collection):
                quals.append(pt.schema_name)
                for st in pt.get_subtypes(schema):
                    quals.append(st.get_name(schema))
            else:
                quals.append(pt.get_name(schema))

            pk = param.get_kind(schema)
            if pk is ft.ParameterKind.NAMED_ONLY:
                quals.append(
                    f'$NO-{param.get_name(schema)}-{pt.get_name(schema)}$')
            elif pk is ft.ParameterKind.VARIADIC:
                quals.append(f'$V$')

        return tuple(quals)

    @classmethod
    def get_fqname(cls, schema, shortname: sn.Name,
                   params: List[ParameterDesc],
                   *extra_quals: str) -> sn.Name:

        quals = cls._get_fqname_quals(schema, params)
        return sn.Name(
            module=shortname.module,
            name=sn.get_specialized_name(shortname, *(quals + extra_quals)))

    def has_inlined_defaults(self, schema):
        return False

    def is_blocking_ref(self, schema, reference):
        # Paramters cannot be deleted via DDL syntax,
        # so the only possible scenario is the deletion of
        # the host function.
        return not isinstance(reference, Parameter)


class CallableCommand(sd.ObjectCommand):

    def _prepare_create_fields(self, schema, context):
        params = self.get_attribute_value('params')

        if params is None:
            try:
                params = self._get_params(schema, context)
            except errors.InvalidReferenceError:
                # Make sure the parameter objects exist.
                for op in self.get_subcommands(metaclass=Parameter):
                    schema, _ = op.apply(schema, context=context)

                params = self._get_params(schema, context)

        schema, props = super()._prepare_create_fields(schema, context)
        props['params'] = params
        return schema, props

    def _get_params(self, schema, context):
        params = []
        for cr_param in self.get_subcommands(type=ParameterCommand):
            param = schema.get(cr_param.classname)
            params.append(param)
        return FuncParameterList.create(schema, params)

    def _alter_innards(self, schema, context, scls):
        schema = super()._alter_innards(schema, context, scls)

        for op in self.get_subcommands(metaclass=Parameter):
            schema, _ = op.apply(schema, context=context)

        return schema

    @classmethod
    def _get_param_desc_from_ast(cls, schema, modaliases, astnode, *,
                                 param_offset: int=0):
        params = []
        if not hasattr(astnode, 'params'):
            # Some Callables, like the concrete constraints,
            # have no params in their AST.
            return []

        for num, param in enumerate(astnode.params, param_offset):
            param_desc = ParameterDesc.from_ast(
                schema, modaliases, num, param)
            params.append(param_desc)

        return params

    @classmethod
    def _get_param_desc_from_delta(cls, schema, context, cmd):
        params = []
        for subcmd in cmd.get_subcommands(type=CreateParameter):
            schema, param = ParameterDesc.from_create_delta(
                schema, context, subcmd)
            params.append(param)

        return schema, params


class CreateCallableObject(CallableCommand, sd.CreateObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        for param in params:
            cmd.add(param.as_create_delta(
                schema, cmd.classname, context=context))

        if hasattr(astnode, 'returning'):
            modaliases = context.modaliases

            return_type_ref = utils.ast_to_typeref(
                astnode.returning, modaliases=modaliases, schema=schema)

            return_type = utils.resolve_typeref(
                return_type_ref, schema=schema)

            if (return_type.is_collection()
                    and not return_type.is_polymorphic(schema)):
                sd.ensure_schema_collection(
                    schema, return_type, cmd,
                    src_context=astnode.returning.context,
                    context=context,
                )

            cmd.add(sd.AlterObjectProperty(
                property='return_type',
                new_value=return_type_ref,
            ))

            cmd.add(sd.AlterObjectProperty(
                property='return_typemod',
                new_value=astnode.returning_typemod
            ))

        return cmd


class DeleteCallableObject(CallableCommand, sd.DeleteObject):

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        for param in params:
            cmd.add(param.as_delete_delta(
                schema, cmd.classname, context=context))

        obj = schema.get(cmd.classname)
        return_type = obj.get_return_type(schema)
        if (return_type.is_collection()
                and not return_type.is_polymorphic(schema)):
            sd.cleanup_schema_collection(
                schema, return_type, obj, cmd, context=context,
                src_context=astnode.context)

        return cmd


class Function(CallableObject, VolatilitySubject, s_abc.Function,
               qlkind=ft.SchemaObjectClass.FUNCTION):

    code = so.SchemaField(
        str, default=None, compcoef=0.4)

    language = so.SchemaField(
        qlast.Language, default=None, compcoef=0.4, coerce=True)

    from_function = so.SchemaField(
        str, default=None, compcoef=0.4, introspectable=False)

    from_expr = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=False)

    force_return_cast = so.SchemaField(
        bool, default=False, compcoef=0.9, introspectable=False)

    sql_func_has_out_params = so.SchemaField(
        bool, default=False, compcoef=0.9, introspectable=False)

    error_on_null_result = so.SchemaField(
        str, default=None, compcoef=0.9, introspectable=False)

    initial_value = so.SchemaField(
        expr.Expression, default=None, compcoef=0.4, coerce=True)

    session_only = so.SchemaField(
        bool, default=False, compcoef=0.4, coerce=True, allow_ddl_set=True)

    def has_inlined_defaults(self, schema):
        # This can be relaxed to just `language is EdgeQL` when we
        # support non-constant defaults.
        return bool(self.get_language(schema) is qlast.Language.EdgeQL and
                    self.get_params(schema).find_named_only(schema))

    def get_verbosename(self, schema, *, with_parent: bool=False) -> str:
        params = self.get_params(schema)
        sn = self.get_shortname(schema)
        return f'function {sn}{params.as_str(schema)}'


class FunctionCommandContext(CallableCommandContext):
    pass


class FunctionCommand(CallableCommand,
                      schema_metaclass=Function,
                      context_class=FunctionCommandContext):

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        name = super()._classname_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        return cls.get_schema_metaclass().get_fqname(schema, name, params)

    def compile_expr_field(self, schema, context, field, value):
        if field.name == 'initial_value':
            return type(value).compiled(
                value,
                schema=schema,
                allow_generic_type_output=True,
                parent_object_type=self.get_schema_metaclass(),
            )
        else:
            return super().compile_expr_field(schema, context, field, value)


class CreateFunction(CreateCallableObject, FunctionCommand):
    astnode = qlast.CreateFunction

    def _create_begin(self, schema, context):
        from edb.ir import utils as irutils

        fullname = self.classname
        shortname = sn.shortname_from_fullname(fullname)
        schema, cp = self._get_param_desc_from_delta(schema, context, self)
        signature = f'{shortname}({", ".join(p.as_str(schema) for p in cp)})'

        func = schema.get(fullname, None)
        if func:
            raise errors.DuplicateFunctionDefinitionError(
                f'cannot create the `{signature}` function: '
                f'a function with the same signature '
                f'is already defined',
                context=self.source_context)

        schema = super()._create_begin(schema, context)

        params: FuncParameterList = self.scls.get_params(schema)
        language = self.scls.get_language(schema)
        return_type = self.scls.get_return_type(schema)
        return_typemod = self.scls.get_return_typemod(schema)
        from_function = self.scls.get_from_function(schema)
        has_polymorphic = params.has_polymorphic(schema)
        polymorphic_return_type = return_type.is_polymorphic(schema)
        named_only = params.find_named_only(schema)
        session_only = self.scls.get_session_only(schema)

        # Certain syntax is only allowed in "EdgeDB developer" mode,
        # i.e. when populating std library, etc.
        if not context.stdmode and not context.testmode:
            if has_polymorphic or polymorphic_return_type:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'generic types are not supported in '
                    f'user-defined functions',
                    context=self.source_context)
            elif from_function:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'"USING SQL FUNCTION" is not supported in '
                    f'user-defined functions',
                    context=self.source_context)
            elif language != qlast.Language.EdgeQL:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'"USING {language}" is not supported in '
                    f'user-defined functions',
                    context=self.source_context)

        if polymorphic_return_type and not has_polymorphic:
            raise errors.InvalidFunctionDefinitionError(
                f'cannot create `{signature}` function: '
                f'function returns a generic type but has no '
                f'generic parameters',
                context=self.source_context)

        overloaded_funcs = schema.get_functions(shortname, ())
        has_from_function = from_function

        for func in overloaded_funcs:
            func_params = func.get_params(schema)
            func_named_only = func_params.find_named_only(schema)
            func_from_function = func.get_from_function(schema)

            if func_named_only.keys() != named_only.keys():
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'overloading another function with different '
                    f'named only parameters: '
                    f'"{func.get_shortname(schema)}'
                    f'{func_params.as_str(schema)}"',
                    context=self.source_context)

            if ((has_polymorphic or func_params.has_polymorphic(schema)) and (
                    func.get_return_typemod(schema) != return_typemod)):

                func_return_typemod = func.get_return_typemod(schema)
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create the polymorphic `{signature} -> '
                    f'{return_typemod.to_edgeql()} '
                    f'{return_type.get_displayname(schema)}` '
                    f'function: overloading another function with different '
                    f'return type {func_return_typemod.to_edgeql()} '
                    f'{func.get_return_type(schema).get_displayname(schema)}',
                    context=self.source_context)

            if session_only != func.get_session_only(schema):
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'overloading another function with different '
                    f'`session_only` flag',
                    context=self.source_context)

            if func_from_function:
                has_from_function = func_from_function

        if has_from_function:
            if (from_function != has_from_function or
                    any(f.get_from_function(schema) != has_from_function
                        for f in overloaded_funcs)):
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create the `{signature}` function: '
                    f'overloading "USING SQL FUNCTION" functions is '
                    f'allowed only when all functions point to the same '
                    f'SQL function',
                    context=self.source_context)

        if (language == qlast.Language.EdgeQL and
                any(p.get_typemod(schema) is ft.TypeModifier.SET_OF
                    for p in params.objects(schema))):
            raise errors.UnsupportedFeatureError(
                f'cannot create the `{signature}` function: '
                f'SET OF parameters in user-defined EdgeQL functions are '
                f'not supported',
                context=self.source_context)

        # check that params of type 'anytype' don't have defaults
        for p in params.objects(schema):
            p_default = p.get_default(schema)
            if p_default is None:
                continue

            p_type = p.get_type(schema)

            try:
                ir_default = p.get_ir_default(schema=schema)
            except Exception as ex:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create the `{signature}` function: '
                    f'invalid default value {p_default.text!r} of parameter '
                    f'{p.get_displayname(schema)!r}: {ex}',
                    context=self.source_context)

            check_default_type = True
            if p_type.is_polymorphic(schema):
                if irutils.is_empty(ir_default.expr):
                    check_default_type = False
                else:
                    raise errors.InvalidFunctionDefinitionError(
                        f'cannot create the `{signature}` function: '
                        f'polymorphic parameter of type '
                        f'{p_type.get_displayname(schema)} cannot '
                        f'have a non-empty default value',
                        context=self.source_context)
            elif (p.get_typemod(schema) is ft.TypeModifier.OPTIONAL and
                    irutils.is_empty(ir_default.expr)):
                check_default_type = False

            if check_default_type:
                default_type = ir_default.stype
                if not default_type.assignment_castable_to(p_type, schema):
                    raise errors.InvalidFunctionDefinitionError(
                        f'cannot create the `{signature}` function: '
                        f'invalid declaration of parameter '
                        f'{p.get_displayname(schema)!r}: '
                        f'unexpected type of the default expression: '
                        f'{default_type.get_displayname(schema)}, expected '
                        f'{p_type.get_displayname(schema)}',
                        context=self.source_context)

        return schema

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if astnode.code is not None:
            cmd.add(sd.AlterObjectProperty(
                property='language',
                new_value=astnode.code.language
            ))
            if astnode.code.from_function is not None:
                cmd.add(sd.AlterObjectProperty(
                    property='from_function',
                    new_value=astnode.code.from_function
                ))
            else:
                cmd.add(sd.AlterObjectProperty(
                    property='code',
                    new_value=astnode.code.code
                ))

        return cmd

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        params = []
        for op in self.get_subcommands(type=ParameterCommand):
            props = op.get_struct_properties(schema)
            num = props['num']
            default = props.get('default')
            param = qlast.FuncParam(
                name=Parameter.paramname_from_fullname(props['name']),
                type=utils.typeref_to_ast(schema, props['type']),
                typemod=props['typemod'],
                kind=props['kind'],
                default=default.qlast if default is not None else None,
            )
            params.append((num, param))

        params.sort(key=lambda e: e[0])

        node.params = [p[1] for p in params]

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'return_type':
            node.returning = utils.typeref_to_ast(schema, op.new_value)

        elif op.property == 'return_typemod':
            node.returning_typemod = op.new_value

        elif op.property == 'code':
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.code = op.new_value

        elif op.property == 'language':
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.language = op.new_value

        elif op.property == 'from_function' and op.new_value:
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.from_function = op.new_value

        elif op.property == 'from_expr' and op.new_value:
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.from_expr = op.new_value

        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameFunction(sd.RenameObject, FunctionCommand):
    pass


class AlterFunction(sd.AlterObject, FunctionCommand):
    astnode = qlast.AlterFunction


class DeleteFunction(DeleteCallableObject, FunctionCommand):
    astnode = qlast.DropFunction
