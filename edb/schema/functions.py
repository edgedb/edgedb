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

import abc
import types
from typing import *

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


if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import schema as s_schema


def param_as_str(
    schema: s_schema.Schema,
    param: Union[ParameterDesc, Parameter],
) -> str:
    ret = []
    kind = param.get_kind(schema)
    typemod = param.get_typemod(schema)
    default = param.get_default(schema)

    if kind is not ft.ParameterKind.POSITIONAL:
        ret.append(kind.to_edgeql())
        ret.append(' ')

    ret.append(f'{param.get_parameter_name(schema)}: ')

    if typemod is not ft.TypeModifier.SINGLETON:
        ret.append(typemod.to_edgeql())
        ret.append(' ')

    paramt: Union[s_types.Type, s_types.TypeShell]
    if isinstance(param, ParameterDesc):
        paramt = param.get_type_shell(schema)
    else:
        paramt = param.get_type(schema)

    ret.append(paramt.get_displayname(schema))

    if default is not None:
        ret.append(f'={default.origtext}')

    return ''.join(ret)


class ParameterLike(s_abc.Parameter):

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_name(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_kind(self, _: s_schema.Schema) -> ft.ParameterKind:
        raise NotImplementedError

    def get_default(self, _: s_schema.Schema) -> Optional[expr.Expression]:
        raise NotImplementedError

    def get_type(self, _: s_schema.Schema) -> s_types.Type:
        raise NotImplementedError

    def get_typemod(self, _: s_schema.Schema) -> ft.TypeModifier:
        raise NotImplementedError

    def as_str(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError


# Non-schema description of a parameter.
class ParameterDesc(ParameterLike):

    num: int
    name: str
    default: Optional[expr.Expression]
    type: s_types.TypeShell
    typemod: ft.TypeModifier
    kind: ft.ParameterKind

    def __init__(
        self,
        *,
        num: int,
        name: str,
        default: Optional[expr.Expression],
        type: s_types.TypeShell,
        typemod: ft.TypeModifier,
        kind: ft.ParameterKind,
    ) -> None:
        self.num = num
        self.name = name
        self.default = default
        self.type = type
        self.typemod = typemod
        self.kind = kind

    @classmethod
    def from_ast(
        cls,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        num: int,
        astnode: qlast.FuncParam,
    ) -> ParameterDesc:

        paramd = None
        if astnode.default is not None:
            defexpr = expr.Expression.from_ast(
                astnode.default, schema, modaliases, as_fragment=True)
            paramd = expr.Expression.compiled(
                defexpr, schema, modaliases=modaliases, as_fragment=True)

        paramt_ast = astnode.type

        if astnode.kind is ft.ParameterKind.VARIADIC:
            paramt_ast = qlast.TypeName(
                maintype=qlast.ObjectRef(
                    name='array',
                ),
                subtypes=[paramt_ast],
            )

        assert isinstance(paramt_ast, qlast.TypeName)
        paramt = utils.ast_to_type_shell(
            paramt_ast,
            modaliases=modaliases,
            schema=schema,
        )

        return cls(
            num=num,
            name=astnode.name,
            type=paramt,
            typemod=astnode.typemod,
            kind=astnode.kind,
            default=paramd
        )

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        return self.name

    def get_name(self, schema: s_schema.Schema) -> str:
        return self.name

    def get_kind(self, _: s_schema.Schema) -> ft.ParameterKind:
        return self.kind

    def get_default(self, _: s_schema.Schema) -> Optional[expr.Expression]:
        return self.default

    def get_type(self, schema: s_schema.Schema) -> s_types.Type:
        return self.type.resolve(schema)

    def get_type_shell(self, schema: s_schema.Schema) -> s_types.TypeShell:
        return self.type

    def get_typemod(self, _: s_schema.Schema) -> ft.TypeModifier:
        return self.typemod

    def as_str(self, schema: s_schema.Schema) -> str:
        return param_as_str(schema, self)

    @classmethod
    def from_create_delta(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        cmd: CreateParameter,
    ) -> Tuple[s_schema.Schema, ParameterDesc]:
        props = cmd.get_attributes(schema, context)
        props['name'] = Parameter.paramname_from_fullname(props['name'])
        if not isinstance(props['type'], s_types.TypeShell):
            paramt = props['type'].as_shell(schema)
        else:
            paramt = props['type']
        return schema, cls(
            num=props['num'],
            name=props['name'],
            type=paramt,
            typemod=props['typemod'],
            kind=props['kind'],
            default=props.get('default'),
        )

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        func_fqname: sn.Name,
        *,
        context: sd.CommandContext,
    ) -> sd.Command:
        CreateParameter = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.CreateObject, Parameter)

        param_name = sn.Name(
            module=func_fqname.module,
            name=sn.get_specialized_name(
                self.get_name(schema), func_fqname)
        )

        cmd = CreateParameter(classname=param_name)
        cmd.set_attribute_value('name', param_name)
        cmd.set_attribute_value('type', self.type)

        if isinstance(self.type, s_types.CollectionTypeShell):
            s_types.ensure_schema_collection(
                schema, self.type, cmd, context=context)

        for attr in ('num', 'typemod', 'kind', 'default'):
            cmd.set_attribute_value(attr, getattr(self, attr))

        return cmd

    def as_delete_delta(
        self,
        schema: s_schema.Schema,
        func_fqname: sn.Name,
        *,
        context: sd.CommandContext,
    ) -> sd.Command:
        DeleteParameter = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.DeleteObject, Parameter)

        param_name = sn.Name(
            module=func_fqname.module,
            name=sn.get_specialized_name(
                self.get_name(schema), func_fqname)
        )

        cmd = DeleteParameter(classname=param_name)

        if isinstance(self.type, s_types.CollectionTypeShell):
            param = schema.get(param_name, type=Parameter)
            s_types.cleanup_schema_collection(
                schema,
                self.type.resolve(schema),
                param,
                cmd,
                context=context,
            )

        return cmd


class Parameter(so.ObjectFragment, ParameterLike):

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
    def paramname_from_fullname(cls, fullname: sn.Name) -> str:
        parts = str(fullname.name).split('@@', 1)
        if len(parts) == 2:
            return sn.unmangle_name(parts[0])
        elif '::' in fullname:
            return sn.Name(fullname).name
        else:
            return fullname

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool = False,
    ) -> str:
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

    def get_shortname(self, schema: s_schema.Schema) -> sn.Name:
        return sn.Name(
            module=self.get_name(schema).module,
            name=self.get_parameter_name(schema),
        )

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.get_parameter_name(schema)

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        fullname = self.get_name(schema)
        return self.paramname_from_fullname(fullname)

    def get_ql_default(self, schema: s_schema.Schema) -> qlast.Base:
        ql_default = self.get_default(schema)
        assert ql_default is not None
        return ql_default.qlast

    def get_ir_default(self, *, schema: s_schema.Schema) -> irast.Base:
        from edb.ir import ast as irast
        from edb.ir import utils as irutils

        defexpr = self.get_default(schema)
        assert defexpr is not None
        defexpr = expr.Expression.compiled(
            defexpr, as_fragment=True, schema=schema)
        ir = defexpr.irast
        assert isinstance(ir, (Function, irast.Statement))
        if not irutils.is_const(ir.expr):
            raise ValueError('expression not constant')
        return ir

    def as_str(self, schema: s_schema.Schema) -> str:
        return param_as_str(schema, self)


class CallableCommandContext(sd.ObjectCommandContext['CallableObject'],
                             s_anno.AnnotationSubjectCommandContext):
    pass


class ParameterCommandContext(sd.ObjectCommandContext[Parameter]):
    pass


# type ignore below, because making Parameter
# a referencing.ReferencedObject breaks the code
class ParameterCommand(
    referencing.StronglyReferencedObjectCommand[Parameter],  # type: ignore
    schema_metaclass=Parameter,
    context_class=ParameterCommandContext,
    referrer_context_class=CallableCommandContext
):
    pass


class CreateParameter(ParameterCommand,
                      sd.CreateObject[Parameter],
                      sd.CreateObjectFragment):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        for sub in cmd.get_subcommands(type=sd.AlterObjectProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        return cmd


class DeleteParameter(ParameterCommand, sd.DeleteObject[Parameter]):
    pass


class PgParams(NamedTuple):

    params: Tuple[Parameter, ...]
    has_param_wo_default: bool

    @classmethod
    def from_params(
        cls,
        schema: s_schema.Schema,
        params: Union[Sequence[ParameterLike], ParameterLikeList],
    ) -> PgParams:
        pg_params = []
        named = []
        variadic = None
        has_param_wo_default = False

        if isinstance(params, ParameterLikeList):
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
            params=tuple(cast(List[Parameter], pg_params)),
            has_param_wo_default=has_param_wo_default)


class ParameterLikeList(abc.ABC):

    @abc.abstractmethod
    def get_by_name(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> Optional[ParameterLike]:
        raise NotImplementedError

    @abc.abstractmethod
    def as_str(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def has_polymorphic(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def find_named_only(
        self,
        schema: s_schema.Schema,
    ) -> Mapping[str, ParameterLike]:
        raise NotImplementedError

    @abc.abstractmethod
    def find_variadic(
        self,
        schema: s_schema.Schema,
    ) -> Optional[ParameterLike]:
        raise NotImplementedError

    @abc.abstractmethod
    def objects(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[ParameterLike, ...]:
        raise NotImplementedError


class FuncParameterList(so.ObjectList[Parameter], ParameterLikeList):

    def get_by_name(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> Optional[Parameter]:
        for param in self.objects(schema):
            if param.get_parameter_name(schema) == name:
                return param
        return None

    def as_str(self, schema: s_schema.Schema) -> str:
        ret = []
        for param in self.objects(schema):
            ret.append(param.as_str(schema))
        return '(' + ', '.join(ret) + ')'

    def has_polymorphic(self, schema: s_schema.Schema) -> bool:
        return any(p.get_type(schema).is_polymorphic(schema)
                   for p in self.objects(schema))

    def find_named_only(
        self,
        schema: s_schema.Schema,
    ) -> Mapping[str, Parameter]:
        named = {}
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.NAMED_ONLY:
                named[param.get_parameter_name(schema)] = param

        return types.MappingProxyType(named)

    def find_variadic(self, schema: s_schema.Schema) -> Optional[Parameter]:
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.VARIADIC:
                return param
        return None


class VolatilitySubject(so.Object):

    volatility = so.SchemaField(
        ft.Volatility, default=ft.Volatility.VOLATILE,
        compcoef=0.4, coerce=True, allow_ddl_set=True)


class CallableLike:
    """A minimal callable object interface required by multidispatch."""

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    def get_params(self, schema: s_schema.Schema) -> ParameterLikeList:
        raise NotImplementedError

    def get_return_type(self, schema: s_schema.Schema) -> s_types.Type:
        raise NotImplementedError

    def get_return_typemod(self, schema: s_schema.Schema) -> ft.TypeModifier:
        raise NotImplementedError

    def get_verbosename(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_is_abstract(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError


class CallableObject(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    CallableLike,
):

    params = so.SchemaField(
        FuncParameterList,
        coerce=True, compcoef=0.4, default=so.DEFAULT_CONSTRUCTOR,
        inheritable=False, simpledelta=False)

    return_type = so.SchemaField(
        s_types.Type, compcoef=0.2)

    return_typemod = so.SchemaField(
        ft.TypeModifier, compcoef=0.4, coerce=True)

    is_abstract = so.SchemaField(
        bool, default=False, compcoef=0.909)

    @classmethod
    def delta(
        cls,
        old: Optional[so.Object],
        new: Optional[so.Object],
        *,
        context: so.ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> sd.ObjectCommand[so.Object]:

        def param_is_inherited(
            schema: s_schema.Schema,
            func: CallableObject,
            param: ParameterLike
        ) -> bool:
            qualname = sn.get_specialized_name(
                param.get_parameter_name(schema), func.get_name(schema))
            param_name = param.get_name(schema)
            assert isinstance(param_name, sn.Name)
            return qualname != param_name.name

        delta = super().delta(
            old,
            new,
            context=context,
            old_schema=old_schema,
            new_schema=new_schema,
        )

        if old:
            assert isinstance(old, CallableObject)
            assert old_schema is not None
            old_params = old.get_params(old_schema).objects(old_schema)
            oldcoll = [
                p for p in old_params
                if not param_is_inherited(old_schema, old, p)
            ]
        else:
            oldcoll = []

        if new:
            assert isinstance(new, CallableObject)
            new_params = new.get_params(new_schema).objects(new_schema)
            newcoll = [
                p for p in new_params
                if not param_is_inherited(new_schema, new, p)
            ]
        else:
            newcoll = []

        delta.add(
            cls.delta_sets(
                oldcoll,
                newcoll,
                context=context,
                old_schema=old_schema,
                new_schema=new_schema,
            )
        )

        return delta

    @classmethod
    def _get_fqname_quals(
        cls,
        schema: s_schema.Schema,
        params: List[ParameterDesc],
    ) -> Tuple[str, ...]:
        pgp = PgParams.from_params(schema, params)

        quals: List[str] = []
        for param in pgp.params:
            assert isinstance(param, ParameterDesc)
            pt = param.get_type_shell(schema)
            if isinstance(pt, s_types.CollectionTypeShell):
                quals.append(pt.get_schema_class_displayname())
                pt_id = str(pt.get_id(schema))
            else:
                pt_id = pt.name

            quals.append(pt_id)
            pk = param.get_kind(schema)
            if pk is ft.ParameterKind.NAMED_ONLY:
                quals.append(f'$NO-{param.get_name(schema)}-{pt_id}$')
            elif pk is ft.ParameterKind.VARIADIC:
                quals.append(f'$V$')

        return tuple(quals)

    @classmethod
    def get_fqname(
        cls,
        schema: s_schema.Schema,
        shortname: sn.Name,
        params: List[ParameterDesc],
        *extra_quals: str,
    ) -> sn.Name:

        quals = cls._get_fqname_quals(schema, params)
        return sn.Name(
            module=shortname.module,
            name=sn.get_specialized_name(shortname, *(quals + extra_quals)))

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        return False

    def is_blocking_ref(
        self,
        schema: s_schema.Schema,
        reference: so.Object,
    ) -> bool:
        # Paramters cannot be deleted via DDL syntax,
        # so the only possible scenario is the deletion of
        # the host function.
        return not isinstance(reference, Parameter)


class CallableCommand(sd.QualifiedObjectCommand[CallableObject]):

    def _get_params(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> so.ObjectList[Parameter]:
        params = []
        for cr_param in self.get_subcommands(type=ParameterCommand):
            param = schema.get(cr_param.classname, type=Parameter)
            params.append(param)
        return FuncParameterList.create(schema, params)

    @classmethod
    def _get_param_desc_from_ast(
        cls,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        astnode: qlast.ObjectDDL,
        *,
        param_offset: int=0,
    ) -> List[ParameterDesc]:
        params = []
        if not hasattr(astnode, 'params'):
            # Some Callables, like the concrete constraints,
            # have no params in their AST.
            return []
        assert isinstance(astnode, qlast.CallableObject)

        for num, param in enumerate(astnode.params, param_offset):
            param_desc = ParameterDesc.from_ast(
                schema, modaliases, num, param)
            params.append(param_desc)

        return params

    @classmethod
    def _get_param_desc_from_delta(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        cmd: sd.Command,
    ) -> Tuple[s_schema.Schema, List[ParameterDesc]]:
        params = []
        for subcmd in cmd.get_subcommands(type=CreateParameter):
            schema, param = ParameterDesc.from_create_delta(
                schema, context, subcmd)
            params.append(param)

        return schema, params


class AlterCallableObject(CallableCommand,
                          sd.AlterObject[CallableObject]):

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_innards(schema, context)

        for op in self.get_subcommands(metaclass=Parameter):
            schema = op.apply(schema, context=context)

        return schema


class CreateCallableObject(CallableCommand, sd.CreateObject[CallableObject]):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.CreateObject)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        for param in params:
            # as_create_delta requires the specific type
            assert isinstance(cmd.classname, sn.SchemaName)
            cmd.add(param.as_create_delta(
                schema, cmd.classname, context=context))

        if hasattr(astnode, 'returning'):
            assert isinstance(astnode, (qlast.CreateOperator,
                                        qlast.CreateFunction))
            assert isinstance(astnode.returning, qlast.TypeName)
            modaliases = context.modaliases

            return_type = utils.ast_to_type_shell(
                astnode.returning,
                modaliases=modaliases,
                schema=schema,
            )

            if isinstance(return_type, s_types.CollectionTypeShell):
                s_types.ensure_schema_collection(
                    schema, return_type, cmd,
                    src_context=astnode.returning.context,
                    context=context,
                )

            cmd.set_attribute_value(
                'return_type', return_type)
            cmd.set_attribute_value(
                'return_typemod', astnode.returning_typemod)

        return cmd

    def get_resolved_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, Any]:
        params = self.get_attribute_value('params')

        if params is None:
            params = self._get_params(schema, context)

        props = super().get_resolved_attributes(schema, context)
        props['params'] = params
        return props


class DeleteCallableObject(CallableCommand,
                           sd.DeleteObject[CallableObject]):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.ObjectDDL)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        for param in params:
            assert isinstance(cmd.classname, sn.SchemaName)
            cmd.add(param.as_delete_delta(
                schema, cmd.classname, context=context))

        obj: CallableObject = schema.get(cmd.classname)

        return_type = obj.get_return_type(schema)
        if return_type.is_collection():
            s_types.cleanup_schema_collection(
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

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        # This can be relaxed to just `language is EdgeQL` when we
        # support non-constant defaults.
        return bool(self.get_language(schema) is qlast.Language.EdgeQL and
                    self.get_params(schema).find_named_only(schema))

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool=False,
    ) -> str:
        params = self.get_params(schema)
        sn = self.get_shortname(schema)
        return f'function {sn}{params.as_str(schema)}'


class FunctionCommandContext(CallableCommandContext):
    pass


class FunctionCommand(CallableCommand,
                      schema_metaclass=Function,
                      context_class=FunctionCommandContext):

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.SchemaName:
        # _classname_from_ast signature expects qlast.NamedDDL,
        # but _get_param_desc_from_ast expects a ObjectDDL,
        # which is more specific
        assert isinstance(astnode, qlast.ObjectDDL)
        name = super()._classname_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        return cls.get_schema_metaclass().get_fqname(schema, name, params)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: expr.Expression,
    ) -> expr.Expression:
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

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        from edb.ir import ast as irast
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

        assert isinstance(self.scls, (Function, irast.Statement))

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
            assert isinstance(ir_default, irast.Statement)

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
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.CreateFunction)

        if astnode.code is not None:
            cmd.set_attribute_value(
                'language',
                astnode.code.language,
            )
            if astnode.code.from_function is not None:
                cmd.set_attribute_value(
                    'from_function',
                    astnode.code.from_function
                )
            else:
                cmd.set_attribute_value(
                    'code',
                    astnode.code.code,
                )

        return cmd

    def _apply_fields_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> None:
        super()._apply_fields_ast(schema, context, node)

        params = []
        for op in self.get_subcommands(type=ParameterCommand):
            props = op.get_resolved_attributes(schema, context)
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

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateFunction)
        new_value: Any = op.new_value

        if op.property == 'return_type':
            node.returning = utils.typeref_to_ast(schema, new_value)

        elif op.property == 'return_typemod':
            node.returning_typemod = new_value

        elif op.property == 'code':
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.code = new_value

        elif op.property == 'language':
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.language = new_value

        elif op.property == 'from_function' and new_value:
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.from_function = new_value

        elif op.property == 'from_expr' and new_value:
            if node.code is None:
                node.code = qlast.FunctionCode()
            node.code.from_expr = new_value

        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameFunction(sd.RenameObject, FunctionCommand):
    pass


class AlterFunction(AlterCallableObject,
                    FunctionCommand):
    astnode = qlast.AlterFunction


class DeleteFunction(DeleteCallableObject, FunctionCommand):
    astnode = qlast.DropFunction
