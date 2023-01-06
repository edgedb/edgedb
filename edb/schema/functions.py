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
import uuid
from typing import *

from edb import errors

from edb.common import parsing
from edb.common import struct
from edb.common import verutils

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes
from edb.common import uuidgen

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import globals as s_globals
from . import name as sn
from . import objects as so
from . import referencing
from . import types as s_types
from . import utils


if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import schema as s_schema

    ParameterLike_T = TypeVar("ParameterLike_T", bound="ParameterLike")


def param_as_str(
    schema: s_schema.Schema,
    param: Union[ParameterDesc, Parameter],
) -> str:
    ret = []
    kind = param.get_kind(schema)
    typemod = param.get_typemod(schema)
    default = param.get_default(schema)

    if kind is not ft.ParameterKind.PositionalParam:
        ret.append(kind.to_edgeql())
        ret.append(' ')

    ret.append(f'{param.get_parameter_name(schema)}: ')

    if typemod is not ft.TypeModifier.SingletonType:
        ret.append(typemod.to_edgeql())
        ret.append(' ')

    paramt: Union[s_types.Type, s_types.TypeShell[s_types.Type]]
    if isinstance(param, ParameterDesc):
        paramt = param.get_type_shell(schema)
    else:
        paramt = param.get_type(schema)

    ret.append(paramt.get_displayname(schema))

    if default is not None:
        ret.append(f'={default.text}')

    return ''.join(ret)


def canonical_param_sort(
    schema: s_schema.Schema,
    params: Iterable[ParameterLike_T],
) -> Tuple[ParameterLike_T, ...]:

    canonical_order = []
    named = []
    variadic = None

    for param in params:
        param_kind = param.get_kind(schema)

        if param_kind is ft.ParameterKind.PositionalParam:
            canonical_order.append(param)
        elif param_kind is ft.ParameterKind.NamedOnlyParam:
            named.append(param)
        else:
            variadic = param

    if variadic is not None:
        canonical_order.append(variadic)

    if named:
        named.sort(key=lambda p: p.get_name(schema))
        named.extend(canonical_order)
        canonical_order = named

    return tuple(canonical_order)


def param_is_inherited(
    schema: s_schema.Schema,
    func: CallableObject,
    param: ParameterLike,
) -> bool:
    qualname = sn.get_specialized_name(
        sn.UnqualName(param.get_parameter_name(schema)),
        str(func.get_name(schema)),
    )
    param_name = param.get_name(schema)
    assert isinstance(param_name, sn.QualName)
    return qualname != param_name.name


class ParameterLike(s_abc.Parameter):

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_name(self, schema: s_schema.Schema) -> sn.Name:
        raise NotImplementedError

    def get_kind(self, _: s_schema.Schema) -> ft.ParameterKind:
        raise NotImplementedError

    def get_default(self, _: s_schema.Schema) -> Optional[s_expr.Expression]:
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
    name: sn.Name
    default: Optional[s_expr.Expression]
    type: s_types.TypeShell[s_types.Type]
    typemod: ft.TypeModifier
    kind: ft.ParameterKind

    def __init__(
        self,
        *,
        num: int,
        name: sn.Name,
        default: Optional[s_expr.Expression],
        type: s_types.TypeShell[s_types.Type],
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
            defexpr = s_expr.Expression.from_ast(
                astnode.default, schema, modaliases, as_fragment=True)
            paramd = defexpr.compiled(
                schema,
                as_fragment=True,
                options=qlcompiler.CompilerOptions(
                    modaliases=modaliases,
                )
            )

        paramt_ast = astnode.type

        if astnode.kind is ft.ParameterKind.VariadicParam:
            paramt_ast = qlast.TypeName(
                maintype=qlast.ObjectRef(
                    name='array',
                ),
                subtypes=[paramt_ast],
            )

        assert isinstance(paramt_ast, qlast.TypeName)
        paramt = utils.ast_to_type_shell(
            paramt_ast,
            metaclass=s_types.Type,
            modaliases=modaliases,
            schema=schema,
        )

        return cls(
            num=num,
            name=sn.UnqualName(astnode.name),
            type=paramt,
            typemod=astnode.typemod,
            kind=astnode.kind,
            default=paramd
        )

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        return str(self.name)

    def get_name(self, schema: s_schema.Schema) -> sn.Name:
        return self.name

    def get_kind(self, _: s_schema.Schema) -> ft.ParameterKind:
        return self.kind

    def get_default(self, _: s_schema.Schema) -> Optional[s_expr.Expression]:
        return self.default

    def get_type(self, schema: s_schema.Schema) -> s_types.Type:
        return self.type.resolve(schema)

    def get_type_shell(
        self,
        schema: s_schema.Schema,
    ) -> s_types.TypeShell[s_types.Type]:
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

    def get_fqname(
        self,
        schema: s_schema.Schema,
        func_fqname: sn.QualName,
    ) -> sn.QualName:
        return sn.QualName(
            func_fqname.module,
            sn.get_specialized_name(self.get_name(schema), str(func_fqname))
        )

    def as_create_delta(
        self,
        schema: s_schema.Schema,
        func_fqname: sn.QualName,
        *,
        context: sd.CommandContext,
    ) -> sd.CreateObject[Parameter]:
        CreateParameter = sd.get_object_command_class_or_die(
            sd.CreateObject, Parameter)

        param_name = self.get_fqname(schema, func_fqname)

        cmd = CreateParameter(classname=param_name)
        cmd.set_attribute_value('name', param_name)
        cmd.set_attribute_value('type', self.type)

        for attr in ('num', 'typemod', 'kind', 'default'):
            cmd.set_attribute_value(attr, getattr(self, attr))

        return cmd


def _params_are_all_required_singletons(
    params: Sequence[ParameterLike], schema: s_schema.Schema,
) -> bool:
    return all(
        param.get_kind(schema) is not ft.ParameterKind.VariadicParam
        and param.get_typemod(schema) is ft.TypeModifier.SingletonType
        and param.get_default(schema) is None
        for param in params
    )


def make_func_param(
    *,
    name: str,
    type: qlast.TypeExpr,
    typemod: qltypes.TypeModifier = qltypes.TypeModifier.SingletonType,
    kind: qltypes.ParameterKind,
    default: Optional[qlast.Expr] = None,
) -> qlast.FuncParam:
    # If the param is variadic, strip the array from the type in the schema
    if kind is ft.ParameterKind.VariadicParam:
        assert (
            isinstance(type, qlast.TypeName)
            and isinstance(type.maintype, qlast.ObjectRef)
            and type.maintype.name == 'array'
            and type.subtypes
        )
        type = type.subtypes[0]

    return qlast.FuncParam(
        name=name,
        type=type,
        typemod=typemod,
        kind=kind,
        default=default,
    )


class Parameter(
    so.ObjectFragment,
    ParameterLike,
    qlkind=ft.SchemaObjectClass.PARAMETER,
    data_safe=True,
):

    num = so.SchemaField(
        int, compcoef=0.4)

    default = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.4)

    type = so.SchemaField(
        s_types.Type, compcoef=0.4)

    typemod = so.SchemaField(
        ft.TypeModifier,
        default=ft.TypeModifier.SingletonType,
        coerce=True, compcoef=0.4)

    kind = so.SchemaField(
        ft.ParameterKind, coerce=True, compcoef=0.4)

    @classmethod
    def paramname_from_fullname(cls, fullname: sn.Name) -> str:
        parts = str(fullname.name).split('@', 1)
        if len(parts) == 2:
            return sn.unmangle_name(parts[0])
        else:
            return fullname.name

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

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.QualName:
        assert isinstance(name, sn.QualName)
        return sn.QualName(
            module='__',
            name=cls.paramname_from_fullname(name),
        )

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        shortname = cls.get_shortname_static(name)
        return shortname.name

    def get_parameter_name(self, schema: s_schema.Schema) -> str:
        fullname = self.get_name(schema)
        return self.paramname_from_fullname(fullname)

    def get_ir_default(self, *, schema: s_schema.Schema) -> irast.Statement:
        from edb.ir import utils as irutils

        defexpr = self.get_default(schema)
        assert defexpr is not None
        defexpr = defexpr.compiled(
            as_fragment=True, schema=schema)
        ir = defexpr.irast
        if not irutils.is_const(ir.expr):
            raise ValueError('expression not constant')
        return ir

    def as_str(self, schema: s_schema.Schema) -> str:
        return param_as_str(schema, self)

    @classmethod
    def compare_field_value(
        cls,
        field: so.Field[Type[so.T]],
        our_value: so.T,
        their_value: so.T,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        # Only compare the actual param name, not the full name.
        if field.name == 'name':
            assert isinstance(our_value, sn.Name)
            assert isinstance(their_value, sn.Name)
            if (
                cls.paramname_from_fullname(our_value) ==
                cls.paramname_from_fullname(their_value)
            ):
                return 1.0

        return super().compare_field_value(
            field,
            our_value,
            their_value,
            our_schema=our_schema,
            their_schema=their_schema,
            context=context,
        )

    def get_ast(self, schema: s_schema.Schema) -> qlast.FuncParam:
        default = self.get_default(schema)
        kind = self.get_kind(schema)

        return make_func_param(
            name=self.get_parameter_name(schema),
            type=utils.typeref_to_ast(schema, self.get_type(schema)),
            typemod=self.get_typemod(schema),
            kind=kind,
            default=default.qlast if default else None,
        )


class CallableCommandContext(sd.ObjectCommandContext['CallableObject'],
                             s_anno.AnnotationSubjectCommandContext):
    pass


class ParameterCommandContext(sd.ObjectCommandContext[Parameter]):
    pass


# type ignore below, because making Parameter
# a referencing.ReferencedObject breaks the code
class ParameterCommand(
    referencing.ReferencedObjectCommandBase[Parameter],  # type: ignore
    context_class=ParameterCommandContext,
    referrer_context_class=CallableCommandContext
):

    is_strong_ref = struct.Field(bool, default=True)

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        # ParameterCommand cannot have its own AST because it is a
        # side-effect of a FunctionCommand.
        return None

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        return s_types.materialize_type_in_attribute(
            schema, context, self, 'type')


class CreateParameter(ParameterCommand, sd.CreateObject[Parameter]):

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
    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_begin(schema, context)
        if not context.canonical:
            if op := self.scls.get_type(schema).as_type_delete_if_dead(schema):
                self.add_caused(op)
        return schema


class RenameParameter(ParameterCommand, sd.RenameObject[Parameter]):
    pass


class AlterParameter(ParameterCommand, sd.AlterObject[Parameter]):
    pass


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
    def has_set_of(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def has_objects(self, schema: s_schema.Schema) -> bool:
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
    def has_required_params(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def objects(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[ParameterLike, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_in_canonical_order(
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

    def has_type_mod(
            self, schema: s_schema.Schema, mod: ft.TypeModifier) -> bool:
        return any(p.get_typemod(schema) is mod for p in self.objects(schema))

    def has_set_of(self, schema: s_schema.Schema) -> bool:
        return self.has_type_mod(schema, ft.TypeModifier.SetOfType)

    def has_objects(self, schema: s_schema.Schema) -> bool:
        return any(p.get_type(schema).is_object_type()
                   for p in self.objects(schema))

    def find_named_only(
        self,
        schema: s_schema.Schema,
    ) -> Mapping[str, Parameter]:
        named = {}
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.NamedOnlyParam:
                named[param.get_parameter_name(schema)] = param

        return types.MappingProxyType(named)

    def find_variadic(self, schema: s_schema.Schema) -> Optional[Parameter]:
        for param in self.objects(schema):
            if param.get_kind(schema) is ft.ParameterKind.VariadicParam:
                return param
        return None

    def has_required_params(self, schema: s_schema.Schema) -> bool:
        return any(
            param.get_kind(schema) is not ft.ParameterKind.VariadicParam
            and param.get_default(schema) is None
            for param in self.objects(schema)
        )

    def get_in_canonical_order(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[Parameter, ...]:
        return canonical_param_sort(schema, self.objects(schema))

    def get_ast(self, schema: s_schema.Schema) -> List[qlast.FuncParam]:
        result = []
        for param in self.objects(schema):
            result.append(param.get_ast(schema))
        return result

    @classmethod
    def compare_values(
        cls,
        ours_o: so.ObjectCollection[Parameter],
        theirs_o: so.ObjectCollection[Parameter],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
        compcoef: float,
    ) -> float:
        ours = list(ours_o.objects(our_schema))
        theirs = list(theirs_o.objects(their_schema))

        # Because parameter lists can't currently be ALTERed, any
        # changes are catastrophic, so return compcoef on any mismatch
        # at all.
        if len(ours) != len(theirs):
            return compcoef
        for param1, param2 in zip(ours, theirs):
            coef = param1.compare(
                param2, our_schema=our_schema,
                their_schema=their_schema, context=context)
            if coef != 1.0:
                return compcoef

        return 1.0


class VolatilitySubject(so.Object):

    volatility = so.SchemaField(
        ft.Volatility, default=ft.Volatility.Volatile,
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

    def get_signature_as_str(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_verbosename(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def get_abstract(self, schema: s_schema.Schema) -> bool:
        raise NotImplementedError


CallableObjectT = TypeVar('CallableObjectT', bound='CallableObject')


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

    abstract = so.SchemaField(
        bool, default=False, inheritable=False, compcoef=0.909)

    impl_is_strict = so.SchemaField(
        bool, default=True, compcoef=0.4)

    def as_create_delta(
        self: CallableObjectT,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[CallableObjectT]:
        delta = super().as_create_delta(schema, context)

        new_params = self.get_params(schema).objects(schema)
        for p in new_params:
            if not param_is_inherited(schema, self, p):
                delta.add_prerequisite(
                    p.as_create_delta(schema=schema, context=context),
                )

        return delta

    def as_alter_delta(
        self: CallableObjectT,
        other: CallableObjectT,
        *,
        self_schema: s_schema.Schema,
        other_schema: s_schema.Schema,
        confidence: float,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[CallableObjectT]:
        delta = super().as_alter_delta(
            other,
            self_schema=self_schema,
            other_schema=other_schema,
            confidence=confidence,
            context=context,
        )

        old_params = self.get_params(self_schema).objects(self_schema)
        oldcoll = [
            p for p in old_params
            if not param_is_inherited(self_schema, self, p)
        ]

        new_params = other.get_params(other_schema).objects(other_schema)
        newcoll = [
            p for p in new_params
            if not param_is_inherited(other_schema, other, p)
        ]

        delta.add_prerequisite(
            sd.delta_objects(
                oldcoll,
                newcoll,
                sclass=Parameter,
                context=context,
                old_schema=self_schema,
                new_schema=other_schema,
            ),
        )

        return delta

    def as_delete_delta(
        self: CallableObjectT,
        *,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[CallableObjectT]:
        delta = super().as_delete_delta(schema=schema, context=context)
        old_params = self.get_params(schema).objects(schema)
        for p in old_params:
            if not param_is_inherited(schema, self, p):
                delta.add(p.as_delete_delta(schema=schema, context=context))

        return delta

    @classmethod
    def _get_fqname_quals(
        cls,
        schema: s_schema.Schema,
        params: List[ParameterDesc],
    ) -> Tuple[str, ...]:
        quals: List[str] = []
        canonical_order = canonical_param_sort(schema, params)
        for param in canonical_order:
            pt = param.get_type_shell(schema)

            pt_id = str(pt.get_name(schema))
            quals.append(pt_id)
            pk = param.get_kind(schema)
            if pk is ft.ParameterKind.NamedOnlyParam:
                quals.append(f'$NO-{param.get_name(schema)}-{pt_id}$')
            elif pk is ft.ParameterKind.VariadicParam:
                quals.append(f'$V$')

        return tuple(quals)

    @classmethod
    def get_fqname(
        cls,
        schema: s_schema.Schema,
        shortname: sn.QualName,
        params: List[ParameterDesc],
        *extra_quals: str,
    ) -> sn.QualName:

        quals = cls._get_fqname_quals(schema, params)
        return sn.QualName(
            module=shortname.module,
            name=sn.get_specialized_name(shortname, *(quals + extra_quals)))

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        return False

    def is_blocking_ref(
        self,
        schema: s_schema.Schema,
        reference: so.Object,
    ) -> bool:
        # Parameters cannot be deleted via DDL syntax,
        # so the only possible scenario is the deletion of
        # the host function.
        return not isinstance(reference, Parameter)


class ParametrizedCommand(sd.ObjectCommand[so.Object_T]):
    def _get_params(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> FuncParameterList:
        params = self.get_attribute_value('params')
        result: Any
        if params is None:
            param_list = []
            for cr_param in self.get_subcommands(type=ParameterCommand):
                param = schema.get(cr_param.classname, type=Parameter)
                param_list.append(param)
            result = FuncParameterList.create(schema, param_list)
        elif isinstance(params, so.ObjectCollectionShell):
            result = params.resolve(schema)
        else:
            result = params

        assert isinstance(result, FuncParameterList)
        return result

    @classmethod
    def _get_param_desc_from_params_ast(
        cls,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        params: List[qlast.FuncParam],
        *,
        param_offset: int=0,
    ) -> List[ParameterDesc]:
        return [
            ParameterDesc.from_ast(schema, modaliases, num, param)
            for num, param in enumerate(params, param_offset)
        ]

    @classmethod
    def _get_param_desc_from_ast(
        cls,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        astnode: qlast.ObjectDDL,
        *,
        param_offset: int=0,
    ) -> List[ParameterDesc]:
        if not hasattr(astnode, 'params'):
            # Some Callables, like the concrete constraints,
            # have no params in their AST.
            return []
        assert isinstance(astnode, qlast.CallableObjectCommand)
        return cls._get_param_desc_from_params_ast(
            schema, modaliases, astnode.params, param_offset=param_offset)

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


class CallableCommand(sd.QualifiedObjectCommand[CallableObjectT],
                      ParametrizedCommand[CallableObjectT]):

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        return s_types.materialize_type_in_attribute(
            schema, context, self, 'return_type')


class RenameCallableObject(
    CallableCommand[CallableObjectT],
    sd.RenameObject[CallableObjectT],
):
    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: CallableObjectT,
    ) -> None:
        super()._canonicalize(schema, context, scls)

        # Don't do anything for concrete constraints
        if not isinstance(scls, Function) and not scls.get_abstract(schema):
            return

        # params don't get picked up by the base _canonicalize because
        # they aren't RefDicts (and use a different mangling scheme to
        # boot), so we need to do it ourselves.
        param_list = scls.get_params(schema)
        params = CallableCommand._get_param_desc_from_params_ast(
            schema, context.modaliases, param_list.get_ast(schema))

        assert isinstance(self.new_name, sn.QualName)
        for dparam, oparam in zip(params, param_list.objects(schema)):
            self.add(self.init_rename_branch(
                oparam,
                dparam.get_fqname(schema, self.new_name),
                schema=schema,
                context=context,
            ))


class AlterCallableObject(
    CallableCommand[CallableObjectT],
    sd.AlterObject[CallableObjectT],
):

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.CallableObjectCommand]:
        node = cast(
            Optional[qlast.CallableObjectCommand],
            # Skip AlterObject's _get_ast, since we don't want to
            # filter things without subcommands. (Since updating
            # nativecode isn't a subcommand in the AST.)
            super(sd.AlterObject, self)._get_ast(
                schema, context, parent_node=parent_node)
        )

        if not node:
            return None

        scls = self.get_object(schema, context)
        node.params = scls.get_params(schema).get_ast(schema)

        return node

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_innards(schema, context)

        for op in self.get_subcommands(metaclass=Parameter):
            schema = op.apply(schema, context=context)

        return schema


class CreateCallableObject(
    CallableCommand[CallableObjectT],
    sd.CreateObject[CallableObjectT],
):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.CreateObject)
        assert isinstance(cmd, CreateCallableObject)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        for param in params:
            # as_create_delta requires the specific type
            cmd.add_prerequisite(param.as_create_delta(
                schema, cmd.classname, context=context))

        if hasattr(astnode, 'returning'):
            assert isinstance(astnode, (qlast.CreateOperator,
                                        qlast.CreateFunction))
            assert isinstance(astnode.returning, qlast.TypeName)
            modaliases = context.modaliases

            return_type = utils.ast_to_type_shell(
                astnode.returning,
                metaclass=s_types.Type,
                modaliases=modaliases,
                schema=schema,
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
        params = self._get_params(schema, context)
        props = super().get_resolved_attributes(schema, context)
        props['params'] = params
        return props

    def _skip_param(self, props: Dict[str, Any]) -> bool:
        return False

    def _get_params_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> List[Tuple[int, qlast.FuncParam]]:
        params: List[Tuple[int, qlast.FuncParam]] = []
        for op in self.get_subcommands(type=ParameterCommand):
            props = op.get_resolved_attributes(schema, context)
            if self._skip_param(props):
                continue

            num: int = props['num']
            default = props.get('default')
            param = make_func_param(
                name=Parameter.paramname_from_fullname(props['name']),
                type=utils.typeref_to_ast(schema, props['type']),
                typemod=props['typemod'],
                kind=props['kind'],
                default=default.qlast if default is not None else None,
            )
            params.append((num, param))

        params.sort(key=lambda e: e[0])

        return params

        node.params = [p[1] for p in params]

    def _apply_fields_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> None:
        super()._apply_fields_ast(schema, context, node)
        params = self._get_params_ast(schema, context, node)
        if isinstance(node, qlast.CallableObjectCommand):
            node.params = [p[1] for p in params]


class DeleteCallableObject(
    CallableCommand[CallableObjectT],
    sd.DeleteObject[CallableObjectT],
):
    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_begin(schema, context)
        scls = self.scls
        if (
            not context.canonical
            # Don't do anything for concrete constraints
            and (isinstance(scls, Function) or scls.get_abstract(schema))
        ):
            for param in scls.get_params(schema).objects(schema):
                self.add(param.init_delta_command(schema, sd.DeleteObject))

            return_type = scls.get_return_type(schema)
            if op := return_type.as_type_delete_if_dead(schema):
                self.add_caused(op)

        return schema


class Function(
    CallableObject,
    VolatilitySubject,
    s_abc.Function,
    qlkind=ft.SchemaObjectClass.FUNCTION,
    data_safe=True,
):

    used_globals = so.SchemaField(
        so.ObjectList[s_globals.Global],
        coerce=True, compcoef=0.0, default=so.DEFAULT_CONSTRUCTOR,
        inheritable=False)

    # A backend_name that is shared between all overloads of the same
    # function, to make them independent from the actual name.
    backend_name = so.SchemaField(
        uuid.UUID,
        default=None,
    )

    code = so.SchemaField(
        str, default=None, compcoef=0.4)

    nativecode = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.9,
        reflection_name='body')

    language = so.SchemaField(
        qlast.Language, default=None, compcoef=0.4, coerce=True,
        reflection_name='language_real')

    reflected_language = so.SchemaField(
        str, reflection_name='language')

    from_function = so.SchemaField(
        str, default=None, compcoef=0.4)

    from_expr = so.SchemaField(
        bool, default=False, compcoef=0.4)

    force_return_cast = so.SchemaField(
        bool, default=False, compcoef=0.9)

    sql_func_has_out_params = so.SchemaField(
        bool, default=False, compcoef=0.9)

    error_on_null_result = so.SchemaField(
        str, default=None, compcoef=0.9)

    #: For a generic function, if True, indicates that the
    #: optionality of the result set should be the same as
    #: of the generic argument.  (See std::assert_single).
    preserves_optionality = so.SchemaField(
        bool, default=False, compcoef=0.99)

    #: For a generic function, if True, indicates that the
    #: upper cardinality of the result set should be the same as
    #: of the generic argument.  (See std::assert_exists).
    preserves_upper_cardinality = so.SchemaField(
        bool, default=False, compcoef=0.99)

    initial_value = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.4, coerce=True)

    has_dml = so.SchemaField(
        bool, default=False)

    # This flag indicates that this function is intended to be used as
    # a generic fallback implementation for a particular polymorphic
    # function. The fallback implementation is exempted from the
    # limitation that all polymorphic functions have to map to the
    # same function in Postgres. There can only be at most one
    # fallback implementation for any given polymorphic function.
    #
    # The flag is intended for internal use for standard library
    # functions.
    fallback = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.909,
    )

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        # This can be relaxed to just `language is EdgeQL` when we
        # support non-constant defaults.
        return bool(self.get_language(schema) is qlast.Language.EdgeQL and
                    self.get_params(schema).find_named_only(schema))

    def get_signature_as_str(
        self,
        schema: s_schema.Schema,
    ) -> str:
        params = self.get_params(schema)
        sn = self.get_shortname(schema)
        return f"{sn}{params.as_str(schema)}"

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool=False,
    ) -> str:
        return f"function '{self.get_signature_as_str(schema)}'"

    def get_dummy_body(self, schema: s_schema.Schema) -> s_expr.Expression:
        """Return a minimal function body that satisfies its return type."""
        rt = self.get_return_type(schema)

        if rt.is_scalar():
            # scalars and enums can be cast from a string
            text = f'SELECT <{rt.get_displayname(schema)}>""'
        elif rt.is_object_type():
            # just grab an object of the appropriate type
            text = f'SELECT {rt.get_displayname(schema)} LIMIT 1'
        else:
            # Can't easily create a valid cast, so just cast empty set
            # into the given type. Technically this potentially breaks
            # cardinality requirement, but since this is a dummy
            # expression it doesn't matter at the moment.
            text = f'SELECT <{rt.get_displayname(schema)}>{{}}'

        return s_expr.Expression(text=text)

    def find_object_param_overloads(
        self,
        schema: s_schema.Schema,
        *,
        srcctx: Optional[parsing.ParserContext] = None,
    ) -> Optional[Tuple[List[Function], int]]:
        """Find if this function overloads another in object parameter.

        If so, check the following rules:

            - in the signatures of functions, only the overloaded object
              parameter must differ, the number and the types of other
              parameters must be the same across all object-overloaded
              functions;
            - the names of arguments in object-overloaded functions must
              match.

        If there are object overloads, return a tuple containing the list
        of overloaded functions and the position of the overloaded parameter.
        """
        params = self.get_params(schema)
        if not params.has_objects(schema):
            return None

        new_params = params.objects(schema)
        new_pt = tuple(p.get_type(schema) for p in new_params)

        diff_param = -1
        overloads = []
        sn = self.get_shortname(schema)
        for f in schema.get_functions(sn):
            if f == self:
                continue

            f_params = f.get_params(schema)
            if not f_params.has_objects(schema):
                continue

            ext_params = f_params.objects(schema)
            ext_pt = (p.get_type(schema) for p in ext_params)

            this_diff_param = -1
            non_obj_param_diff = False
            multi_overload = False

            for i, (new_t, ext_t) in enumerate(zip(new_pt, ext_pt)):
                if new_t != ext_t:
                    if new_t.is_object_type() and ext_t.is_object_type():
                        if (
                            this_diff_param != -1
                            or (
                                this_diff_param != -1
                                and diff_param != -1
                                and diff_param != this_diff_param
                            )
                            or non_obj_param_diff
                        ):
                            multi_overload = True
                            break
                        else:
                            this_diff_param = i
                    else:
                        non_obj_param_diff = True
                        if this_diff_param != -1:
                            multi_overload = True
                            break

            if this_diff_param != -1:
                if not multi_overload:
                    multi_overload = len(new_params) != len(ext_params)

                if multi_overload:
                    # Multiple dispatch of object-taking functions is
                    # not supported.
                    my_sig = self.get_signature_as_str(schema)
                    other_sig = f.get_signature_as_str(schema)
                    raise errors.UnsupportedFeatureError(
                        f'cannot create the `{my_sig}` function: '
                        f'overloading an object type-receiving '
                        f'function with differences in the remaining '
                        f'parameters is not supported',
                        context=srcctx,
                        details=(
                            f"Other function is defined as `{other_sig}`"
                        )
                    )

                if not all(
                    new_p.get_parameter_name(schema)
                    == ext_p.get_parameter_name(schema)
                    for new_p, ext_p in zip(new_params, ext_params)
                ):
                    # And also _all_ parameter names must match due to
                    # current implementation constraints.
                    my_sig = self.get_signature_as_str(schema)
                    other_sig = f.get_signature_as_str(schema)
                    raise errors.UnsupportedFeatureError(
                        f'cannot create the `{my_sig}` '
                        f'function: overloading an object type-receiving '
                        f'function with differences in the names of '
                        f'parameters is not supported',
                        context=srcctx,
                        details=(
                            f"Other function is defined as `{other_sig}`"
                        )
                    )

                diff_param = this_diff_param
                overloads.append(f)

        if diff_param == -1:
            return None
        else:
            return (overloads, diff_param)


class FunctionCommandContext(CallableCommandContext):
    pass


class FunctionCommand(
    CallableCommand[Function],
    context_class=FunctionCommandContext,
):

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        # _classname_from_ast signature expects qlast.NamedDDL,
        # but _get_param_desc_from_ast expects a ObjectDDL,
        # which is more specific
        assert isinstance(astnode, qlast.ObjectDDL)
        name = super()._classname_from_ast(schema, astnode, context)

        params = cls._get_param_desc_from_ast(
            schema, context.modaliases, astnode)

        return cls.get_schema_metaclass().get_fqname(schema, name, params)

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field == 'nativecode':
            return 'nativecode'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        if field.name == 'initial_value':
            return value.compiled(
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    allow_generic_type_output=True,
                    schema_object_context=self.get_schema_metaclass(),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                ),
            )
        elif field.name == 'nativecode':
            return self.compile_this_function(
                schema,
                context,
                value,
                track_schema_ref_exprs,
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs)

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'nativecode':
            func = schema.get(self.classname, type=Function)
            return func.get_dummy_body(schema)
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def _get_attribute_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: str,
    ) -> Any:
        val = self.get_resolved_attribute_value(
            name,
            schema=schema,
            context=context,
        )
        mcls = self.get_schema_metaclass()
        if val is None:
            field = mcls.get_field(name)
            assert isinstance(field, so.SchemaField)
            val = field.default

        if val is None:
            raise AssertionError(
                f'missing required {name} for {mcls.__name__}'
            )
        return val

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        # When volatility is altered, we need to force a
        # reconsideration of nativecode if it exists in order to check
        # it against the new volatility or compute the volatility on a
        # RESET.  This is kind of unfortunate.
        if (
            isinstance(self, sd.AlterObject)
            and self.has_attribute_value('volatility')
            and not self.has_attribute_value('nativecode')
            and (nativecode := self.scls.get_nativecode(schema)) is not None
        ):
            self.set_attribute_value(
                'nativecode',
                nativecode.not_compiled()
            )

        # Resolving 'nativecode' has side effects on has_dml and
        # volatility, so force it to happen as part of
        # canonicalization of attributes.
        super().get_resolved_attribute_value(
            'nativecode', schema=schema, context=context)
        return schema

    def compile_this_function(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        body: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        params = self._get_params(schema, context)
        language = self._get_attribute_value(schema, context, 'language')
        return_type = self._get_attribute_value(schema, context, 'return_type')
        return_typemod = self._get_attribute_value(
            schema, context, 'return_typemod')

        expr = compile_function(
            schema,
            context,
            body=body,
            params=params,
            language=language,
            return_type=return_type,
            return_typemod=return_typemod,
            track_schema_ref_exprs=track_schema_ref_exprs,
        )

        ir = expr.irast

        if ir.dml_exprs:
            if context.allow_dml_in_functions:
                # DML inside function body detected. Right now is a good
                # opportunity to raise exceptions or give warnings.
                self.set_attribute_value('has_dml', True)
            else:
                raise errors.InvalidFunctionDefinitionError(
                    'data-modifying statements are not allowed in function'
                    ' bodies',
                    context=ir.dml_exprs[0].context,
                )

        spec_volatility: Optional[ft.Volatility] = (
            self.get_specified_attribute_value('volatility', schema, context))

        if spec_volatility is None:
            self.set_attribute_value('volatility', ir.volatility,
                                     computed=True)

        # If a volatility is specified, it can be more volatile than the
        # inferred volatility but not less.
        if spec_volatility is not None and spec_volatility < ir.volatility:
            # When restoring from old versions, just ignore the problem
            # and use the inferred volatility
            if context.compat_ver_is_before(
                (1, 0, verutils.VersionStage.ALPHA, 8)
            ):
                self.set_attribute_value('volatility', ir.volatility)
            else:
                raise errors.InvalidFunctionDefinitionError(
                    f'volatility mismatch in function declared as '
                    f'{str(spec_volatility).lower()}',
                    details=f'Actual volatility is '
                            f'{str(ir.volatility).lower()}',
                    context=body.qlast.context,
                )

        globs = [schema.get(glob.global_name, type=s_globals.Global)
                 for glob in ir.globals]
        self.set_attribute_value('used_globals', globs)

        return expr

    @classmethod
    def localnames_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> Set[str]:
        localnames = super().localnames_from_ast(
            schema, astnode, context
        )
        if isinstance(astnode, (qlast.CreateFunction, qlast.AlterFunction)):
            localnames |= {param.name for param in astnode.params}

        return localnames


class CreateFunction(CreateCallableObject[Function], FunctionCommand):
    astnode = qlast.CreateFunction

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        from edb.ir import utils as irutils

        fullname = self.classname
        shortname = sn.shortname_from_fullname(fullname)
        schema, cp = self._get_param_desc_from_delta(schema, context, self)
        signature = f'{shortname}({", ".join(p.as_str(schema) for p in cp)})'

        if func := schema.get(fullname, None):
            raise errors.DuplicateFunctionDefinitionError(
                f'cannot create the `{signature}` function: '
                f'a function with the same signature '
                f'is already defined',
                context=self.source_context)

        if not context.canonical:
            fullname = self.classname
            shortname = sn.shortname_from_fullname(fullname)
            if others := schema.get_functions(
                    sn.QualName(fullname.module, shortname.name), ()):
                backend_name = others[0].get_backend_name(schema)
            else:
                backend_name = uuidgen.uuid1mc()
            self.set_attribute_value('backend_name', backend_name)

            if (
                self.has_attribute_value("code")
                or self.has_attribute_value("nativecode")
            ):
                self.set_attribute_value(
                    'impl_is_strict',
                    _params_are_all_required_singletons(cp, schema),
                )

        # Check if other schema objects with the same name (ignoring
        # signature, of course) exist.
        if other := schema.get(
                sn.QualName(fullname.module, shortname.name), None):
            raise errors.SchemaError(
                f'{other.get_verbosename(schema)} already exists')

        schema = super()._create_begin(schema, context)

        params: FuncParameterList = self.scls.get_params(schema)

        language = self.scls.get_language(schema)
        return_type = self.scls.get_return_type(schema)
        return_typemod = self.scls.get_return_typemod(schema)
        from_function = self.scls.get_from_function(schema)
        has_polymorphic = params.has_polymorphic(schema)
        has_set_of = params.has_set_of(schema)
        has_objects = params.has_objects(schema)
        polymorphic_return_type = return_type.is_polymorphic(schema)
        named_only = params.find_named_only(schema)
        fallback = self.scls.get_fallback(schema)
        preserves_opt = self.scls.get_preserves_optionality(schema)
        preserves_upper_card = self.scls.get_preserves_upper_cardinality(
            schema)

        if preserves_opt and not has_set_of:
            raise errors.InvalidFunctionDefinitionError(
                f'cannot create `{signature}` function: '
                f'"preserves_optionality" makes no sense '
                f'in a non-aggregate function',
                context=self.source_context)

        if preserves_upper_card and not has_set_of:
            raise errors.InvalidFunctionDefinitionError(
                f'cannot create `{signature}` function: '
                f'"preserves_upper_cardinality" makes no sense '
                f'in a non-aggregate function',
                context=self.source_context)

        if preserves_upper_card and (
            return_typemod is not ft.TypeModifier.SetOfType
        ):
            raise errors.InvalidFunctionDefinitionError(
                f'cannot create `{signature}` function: '
                f'"preserves_upper_cardinality" makes no sense '
                f'in a function not returning SET OF',
                context=self.source_context)

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
            func_preserves_opt = func.get_preserves_optionality(schema)
            func_preserves_upper_card = func.get_preserves_upper_cardinality(
                schema)

            if func_named_only.keys() != named_only.keys():
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'overloading another function with different '
                    f'named only parameters: '
                    f'"{func.get_signature_as_str(schema)}"',
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

            if fallback and func.get_fallback(schema) and self.scls != func:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create the polymorphic `{signature} -> '
                    f'{return_typemod.to_edgeql()} '
                    f'{return_type.get_displayname(schema)}` '
                    f'function: only one generic fallback per polymorphic '
                    f'function is allowed',
                    context=self.source_context)

            if func_from_function:
                has_from_function = func_from_function

            if func_preserves_opt != preserves_opt:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'overloading another function with different '
                    f'"preserves_optionality" attribute: '
                    f'`{func.get_signature_as_str(schema)}`',
                    context=self.source_context)

            if func_preserves_upper_card != preserves_upper_card:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create `{signature}` function: '
                    f'overloading another function with different '
                    f'"preserves_upper_cardinality" attribute: '
                    f'`{func.get_signature_as_str(schema)}`',
                    context=self.source_context)

        if has_objects:
            self.scls.find_object_param_overloads(
                schema, srcctx=self.source_context)

        if has_from_function:
            # Ignore the generic fallback when considering
            # from_function for polymorphic functions.
            if (not fallback and from_function != has_from_function or
                    any(not f.get_fallback(schema) and
                        f.get_from_function(schema) != has_from_function
                        for f in overloaded_funcs)):
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot create the `{signature}` function: '
                    f'overloading "USING SQL FUNCTION" functions is '
                    f'allowed only when all functions point to the same '
                    f'SQL function',
                    context=self.source_context)

        if (language == qlast.Language.EdgeQL and
                any(p.get_typemod(schema) is ft.TypeModifier.SetOfType
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
            elif (p.get_typemod(schema) is ft.TypeModifier.OptionalType and
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
        assert isinstance(cmd, CreateFunction)

        reflected_language = 'builtin'

        assert isinstance(astnode, qlast.CreateFunction)
        if astnode.code is not None:
            cmd.set_attribute_value(
                'language',
                astnode.code.language,
            )
            if astnode.code.language is qlast.Language.EdgeQL:
                reflected_language = 'EdgeQL'

                nativecode_expr: qlast.Base

                if astnode.nativecode is not None:
                    nativecode_expr = astnode.nativecode
                else:
                    assert astnode.code.code is not None
                    nativecode_expr = qlparser.parse(astnode.code.code)

                nativecode = s_expr.Expression.from_ast(
                    nativecode_expr,
                    schema,
                    context.modaliases,
                    context.localnames,
                )

                cmd.set_attribute_value(
                    'nativecode',
                    nativecode,
                )
            elif astnode.code.from_function is not None:
                cmd.set_attribute_value(
                    'from_function',
                    astnode.code.from_function
                )
            elif (
                astnode.code.from_expr is not None
                and astnode.code.code is None
            ):
                cmd.set_attribute_value(
                    'from_expr',
                    astnode.code.from_expr,
                )
            else:
                cmd.set_attribute_value(
                    'code',
                    astnode.code.code,
                )

        cmd.set_attribute_value('reflected_language', reflected_language)

        return cmd

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


class RenameFunction(RenameCallableObject[Function], FunctionCommand):
    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        ctx = context.current()
        assert isinstance(ctx.op, AlterFunction)
        name = sd.QualifiedObjectCommand._classname_from_ast(
            schema, astnode, context)

        quals = list(sn.quals_from_fullname(ctx.op.classname))
        out = sn.QualName(
            name=sn.get_specialized_name(name, *quals),
            module=name.module
        )
        return out

    def validate_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        cur_shortname = sn.shortname_from_fullname(self.classname)
        cur_name = sn.QualName(self.classname.module, cur_shortname.name)

        new_shortname = sn.shortname_from_fullname(self.new_name)
        assert isinstance(self.new_name, sn.QualName)
        new_name = sn.QualName(self.new_name.module, new_shortname.name)

        if cur_name == new_name:
            return

        existing = schema.get_functions(cur_name)
        if len(existing) > 1:
            raise errors.SchemaError(
                'renaming an overloaded function is not allowed',
                context=self.source_context)

        target = schema.get_functions(new_name, ())
        if target:
            raise errors.SchemaError(
                f"can not rename function to '{new_name!s}' because "
                f"a function with the same name already exists, and "
                f"renaming into an overload is not supported",
                context=self.source_context)


class AlterFunction(AlterCallableObject[Function], FunctionCommand):

    astnode = qlast.AlterFunction

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        if context.canonical:
            return schema

        if self.has_attribute_value("fallback"):
            overloaded_funcs = schema.get_functions(
                self.scls.get_shortname(schema), ())

            if len([func for func in overloaded_funcs
                    if func.get_fallback(schema)]) > 1:
                raise errors.InvalidFunctionDefinitionError(
                    f'cannot alter the polymorphic '
                    f'{self.scls.get_verbosename(schema)}: '
                    f'only one generic fallback per polymorphic '
                    f'function is allowed',
                    context=self.source_context)

        # If volatility or nativecode changed, propagate that to
        # referring exprs
        if not (
            self.has_attribute_value("volatility")
            or self.has_attribute_value("nativecode")
        ):
            return schema

        # We also need to propagate changes to "parent"
        # overloads. This is mainly so they can get the proper global
        # variables updated.
        extra_refs: Optional[Dict[so.Object, List[str]]] = None
        if (overloaded := scls.find_object_param_overloads(schema)):
            ov_funcs, ov_idx = overloaded
            cur_type = (
                scls.get_params(schema).objects(schema)[ov_idx].
                get_type(schema)
            )
            extra_refs = {
                f: ['nativecode'] for f in ov_funcs
                if (f_type := f.get_params(schema).objects(schema)[ov_idx].
                    get_type(schema))
                and f_type != cur_type and cur_type.issubclass(schema, f_type)
            }

        vn = scls.get_verbosename(schema, with_parent=True)
        schema = self._propagate_if_expr_refs(
            schema, context, metadata_only=False, extra_refs=extra_refs,
            action=f'alter the definition of {vn}')

        return schema

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:

        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.AlterFunction)

        if astnode.code is not None:
            if (
                astnode.code.from_function is not None or
                astnode.code.from_expr
            ):
                raise errors.EdgeQLSyntaxError(
                    'altering function code is only supported for '
                    'pure EdgeQL functions',
                    context=astnode.context
                )

            nativecode_expr: Optional[qlast.Expr] = None
            if astnode.nativecode is not None:
                nativecode_expr = astnode.nativecode
            elif (
                astnode.code.language is qlast.Language.EdgeQL
                and astnode.code.code is not None
            ):
                nativecode_expr = qlparser.parse(astnode.code.code)
            else:
                cmd.set_attribute_value(
                    'code',
                    astnode.code.code,
                )

            if nativecode_expr is not None:
                nativecode = s_expr.Expression.from_ast(
                    nativecode_expr,
                    schema,
                    context.modaliases,
                    context.localnames,
                )

                cmd.set_attribute_value(
                    'nativecode',
                    nativecode,
                )

        return cmd

    def _get_attribute_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: str,
    ) -> Any:
        val = self.get_resolved_attribute_value(
            name,
            schema=schema,
            context=context,
        )
        if val is None:
            val = self.scls.get_field_value(schema, name)
        if val is None:
            mcls = self.get_schema_metaclass()
            raise AssertionError(
                f'missing required {name} for {mcls.__name__}'
            )

        return val

    def _get_params(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> FuncParameterList:
        return self.scls.get_params(schema)

    def canonicalize_alter_from_external_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        # Produce a param desc list which we use to find a new name.
        param_list = self.scls.get_params(schema)
        params = CallableCommand._get_param_desc_from_params_ast(
            schema, context.modaliases, param_list.get_ast(schema))
        name = sn.shortname_from_fullname(self.classname)
        assert isinstance(name, sn.QualName), "expected qualified name"
        new_fname = CallableObject.get_fqname(schema, name, params)
        if new_fname == self.classname:
            return

        # Do the rename
        rename = self.scls.init_delta_command(
            schema, sd.RenameObject, new_name=new_fname)
        rename.set_attribute_value(
            'name', value=new_fname, orig_value=self.classname)
        self.add(rename)


class DeleteFunction(DeleteCallableObject[Function], FunctionCommand):
    astnode = qlast.DropFunction

    def _apply_fields_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> None:
        super()._apply_fields_ast(schema, context, node)

        params = []
        for op in self.get_subcommands(type=ParameterCommand):
            props = op.get_orig_attributes(schema, context)
            num: int = props['num']
            param = make_func_param(
                name=Parameter.paramname_from_fullname(props['name']),
                type=utils.typeref_to_ast(schema, props['type']),
                typemod=props['typemod'],
                kind=props['kind'],
            )
            params.append((num, param))

        params.sort(key=lambda e: e[0])

        assert isinstance(node, qlast.CallableObjectCommand)
        node.params = [p[1] for p in params]


def get_params_symtable(
    params: FuncParameterList,
    schema: s_schema.Schema,
    *,
    inlined_defaults: bool,
) -> Dict[str, qlast.Expr]:

    anchors: Dict[str, qlast.Expr] = {}

    defaults_mask = qlast.TypeCast(
        expr=qlast.Parameter(name='__defaults_mask__'),
        type=qlast.TypeName(
            maintype=qlast.ObjectRef(
                module='std',
                name='bytes',
            ),
        ),
    )

    for pi, p in enumerate(params.get_in_canonical_order(schema)):
        p_shortname = p.get_parameter_name(schema)
        p_is_optional = (
            p.get_typemod(schema) is not ft.TypeModifier.SingletonType
        )
        anchors[p_shortname] = qlast.TypeCast(
            expr=qlast.Parameter(name=p_shortname),
            cardinality_mod=(
                qlast.CardinalityModifier.Optional if p_is_optional else None
            ),
            type=utils.typeref_to_ast(schema, p.get_type(schema)),
        )

        p_default = p.get_default(schema)
        if p_default is None:
            continue

        if not inlined_defaults:
            continue

        anchors[p_shortname] = qlast.IfElse(
            condition=qlast.BinOp(
                left=qlast.FunctionCall(
                    func=('std', 'bytes_get_bit'),
                    args=[
                        defaults_mask,
                        qlast.IntegerConstant(value=str(pi)),
                    ]),
                op='=',
                right=qlast.IntegerConstant(value='0'),
            ),
            if_expr=anchors[p_shortname],
            else_expr=qlast._Optional(expr=p_default.qlast),
        )

    return anchors


def compile_function(
    schema: s_schema.Schema,
    context: sd.CommandContext,
    *,
    body: s_expr.Expression,
    params: FuncParameterList,
    language: qlast.Language,
    return_type: s_types.Type,
    return_typemod: ft.TypeModifier,
    track_schema_ref_exprs: bool=False,
) -> s_expr.CompiledExpression:
    assert language is qlast.Language.EdgeQL

    has_inlined_defaults = bool(params.find_named_only(schema))

    param_anchors = get_params_symtable(
        params,
        schema,
        inlined_defaults=has_inlined_defaults,
    )

    compiled = body.compiled(
        schema,
        options=qlcompiler.CompilerOptions(
            anchors=param_anchors,
            func_params=params,
            apply_query_rewrites=not context.stdmode,
            track_schema_ref_exprs=track_schema_ref_exprs,
        ),
    )

    ir = compiled.irast
    schema = ir.schema

    if (not ir.stype.issubclass(schema, return_type)
            and not ir.stype.implicitly_castable_to(return_type, schema)):
        raise errors.InvalidFunctionDefinitionError(
            f'return type mismatch in function declared to return '
            f'{return_type.get_verbosename(schema)}',
            details=f'Actual return type is '
                    f'{ir.stype.get_verbosename(schema)}',
            context=body.qlast.context,
        )

    if (return_typemod is not ft.TypeModifier.SetOfType
            and ir.cardinality.is_multi()):
        raise errors.InvalidFunctionDefinitionError(
            f'return cardinality mismatch in function declared to return '
            f'a singleton',
            details=(
                f'Function may return a set with more than one element.'
            ),
            context=body.qlast.context,
        )
    elif (return_typemod is ft.TypeModifier.SingletonType
            and ir.cardinality.can_be_zero()):
        raise errors.InvalidFunctionDefinitionError(
            f'return cardinality mismatch in function declared to return '
            f'exactly one value',
            details=(
                f'Function may return an empty set.'
            ),
            context=body.qlast.context,
        )

    return compiled
