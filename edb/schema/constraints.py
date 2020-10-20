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
from typing import *

from edb import errors

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser
from edb.edgeql import utils as qlutils
from edb.schema import scalars as s_scalars

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import objects as so
from . import types as s_types
from . import pseudo as s_pseudo
from . import referencing
from . import utils


if TYPE_CHECKING:
    from edb.common import parsing as c_parsing
    from edb.schema import schema as s_schema


T = TypeVar('T')


def _assert_not_none(value: Optional[T]) -> T:
    if value is None:
        raise TypeError("A value is expected")
    return value


def merge_constraint_params(
    constraint: Constraint,
    supers: List[Constraint],
    field_name: str,
    *,
    ignore_local: bool,
    schema: s_schema.Schema,
) -> Any:
    if constraint.get_subject(schema) is None:
        # consistency of abstract constraint params is checked
        # in CreateConstraint.validate_create
        return constraint.get_explicit_field_value(schema, field_name, None)
    else:
        # concrete constraints cannot redefined parameters and always
        # inherit from super.
        return supers[0].get_explicit_field_value(schema, field_name, None)


class Constraint(referencing.ReferencedInheritingObject,
                 s_func.CallableObject, s_abc.Constraint,
                 qlkind=ft.SchemaObjectClass.CONSTRAINT):

    params = so.SchemaField(
        s_func.FuncParameterList,
        coerce=True,
        compcoef=0.4,
        default=so.DEFAULT_CONSTRUCTOR,
        inheritable=True,
        merge_fn=merge_constraint_params,
    )

    expr = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.909,
        coerce=True)

    orig_expr = so.SchemaField(
        str, default=None, coerce=True, allow_ddl_set=True,
        ephemeral=True,
    )

    subjectexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.833, coerce=True,
        ddl_identity=True)

    orig_subjectexpr = so.SchemaField(
        str, default=None, coerce=True,
        allow_ddl_set=True,
        ephemeral=True,
    )

    finalexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.909, coerce=True)

    subject = so.SchemaField(
        so.Object, default=None, inheritable=False)

    args = so.SchemaField(
        s_expr.ExpressionList,
        default=None, coerce=True, inheritable=False,
        compcoef=0.875, ddl_identity=True)

    delegated = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.9,
    )

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True)

    is_aggregate = so.SchemaField(
        bool, default=False, compcoef=0.971, allow_ddl_set=False)

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool=False
    ) -> str:
        is_abstract = self.generic(schema)
        vn = super().get_verbosename(schema)
        if is_abstract:
            return f'abstract {vn}'
        else:
            if with_parent:
                subject = self.get_subject(schema)
                assert subject is not None
                pvn = subject.get_verbosename(
                    schema, with_parent=True)
                return f'{vn} of {pvn}'
            else:
                return vn

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_subject(schema) is None

    def get_subject(self, schema: s_schema.Schema) -> ConsistencySubject:
        return cast(
            ConsistencySubject,
            self.get_field_value(schema, 'subject'),
        )

    def format_error_message(
        self,
        schema: s_schema.Schema,
    ) -> str:
        errmsg = self.get_errmessage(schema)
        subject = self.get_subject(schema)
        titleattr = subject.get_annotation(schema, 'std::title')

        if not titleattr:
            subjname = subject.get_shortname(schema)
            subjtitle = subjname.name
        else:
            subjtitle = titleattr

        args = self.get_args(schema)
        if args:
            args_ql: List[qlast.Base] = [
                qlast.Path(steps=[qlast.ObjectRef(name=subjtitle)]),
            ]

            args_ql.extend(arg.qlast for arg in args)

            constr_base: Constraint = schema.get(
                self.get_name(schema), type=type(self))

            index_parameters = qlutils.index_parameters(
                args_ql,
                parameters=constr_base.get_params(schema),
                schema=schema,
            )

            expr = constr_base.get_field_value(schema, 'expr')
            expr_ql = qlparser.parse(expr.text)

            qlutils.inline_parameters(expr_ql, index_parameters)

            args_map = {name: edgeql.generate_source(val, pretty=False)
                        for name, val in index_parameters.items()}
        else:
            args_map = {'__subject__': subjtitle}

        assert errmsg is not None
        formatted = errmsg.format(**args_map)

        return formatted

    def as_alter_delta(
        self,
        other: Constraint,
        *,
        self_schema: s_schema.Schema,
        other_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.Command:
        return super().as_alter_delta(
            other,
            self_schema=self_schema,
            other_schema=other_schema,
            context=context,
        )

    def get_ddl_identity(
        self,
        schema: s_schema.Schema,
    ) -> Optional[Dict[str, str]]:
        ddl_identity = super().get_ddl_identity(schema)

        if (
            ddl_identity is not None
            and self.field_is_inherited(schema, 'subjectexpr')
        ):
            ddl_identity.pop('subjectexpr', None)

        return ddl_identity

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.Name, ...]:
        return (
            sn.Name(module='std', name='constraint'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.Name:
        return sn.Name('std::constraint')


class ConsistencySubject(
    so.QualifiedObject,
    so.InheritingObject,
    s_anno.AnnotationSubject,
):
    constraints_refs = so.RefDict(
        attr='constraints',
        ref_cls=Constraint)

    constraints = so.SchemaField(
        so.ObjectIndexByFullname[Constraint],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.887,
        default=so.DEFAULT_CONSTRUCTOR
    )

    def add_constraint(
        self,
        schema: s_schema.Schema,
        constraint: Constraint,
        replace: bool = False,
    ) -> s_schema.Schema:
        return self.add_classref(
            schema,
            'constraints',
            constraint,
            replace=replace,
        )

    def can_accept_constraints(self, schema: s_schema.Schema) -> bool:
        return True


class ConsistencySubjectCommandContext:
    # context mixin
    pass


class ConsistencySubjectCommand(
    inheriting.InheritingObjectCommand[so.InheritingObjectT],
):
    pass


class ConstraintCommandContext(sd.ObjectCommandContext[Constraint],
                               s_anno.AnnotationSubjectCommandContext):
    pass


class ConstraintCommand(
    referencing.ReferencedInheritingObjectCommand[Constraint],
    s_func.CallableCommand[Constraint],
    schema_metaclass=Constraint,
    context_class=ConstraintCommandContext,
    referrer_context_class=ConsistencySubjectCommandContext,
):

    @classmethod
    def _validate_subcommands(
        cls,
        astnode: qlast.DDLOperation,
    ) -> None:
        # check that 'subject' and 'subjectexpr' are not set as annotations
        for command in astnode.commands:
            if isinstance(command, qlast.BaseSetField):
                cname = command.name
                if cname in {'subject', 'subjectexpr'}:
                    raise errors.InvalidConstraintDefinitionError(
                        f'{cname} is not a valid constraint annotation',
                        context=command.context)

    @classmethod
    def _classname_quals_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        base_name: str,
        referrer_name: str,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        if isinstance(astnode, qlast.CreateConstraint):
            return ()
        exprs = []
        args = cls._constraint_args_from_ast(schema, astnode, context)
        for arg in args:
            exprs.append(arg.text)

        assert isinstance(astnode, qlast.ConstraintOp)
        if astnode.subjectexpr:
            # use the normalized text directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.subjectexpr, schema, context.modaliases)
            exprs.append(expr.text)

        return (cls._name_qual_from_exprs(schema, exprs),)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.SchemaName
    ) -> Tuple[str, ...]:
        quals = sn.quals_from_fullname(name)
        return (quals[-1],)

    @classmethod
    def _constraint_args_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> List[s_expr.Expression]:
        args = []
        assert isinstance(astnode, qlast.ConstraintOp)

        if astnode.args:
            for arg in astnode.args:
                arg_expr = s_expr.Expression.from_ast(
                    arg, schema, context.modaliases)
                args.append(arg_expr)

        return args

    def get_verbosename(self) -> str:
        mcls = self.get_schema_metaclass()
        vname = mcls.get_verbosename_static(self.classname)
        if self.get_attribute_value('is_abstract'):
            vname = f'abstract {vname}'
        return vname

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:

        base: Optional[so.Object] = None
        if isinstance(self, AlterConstraint):
            base = self.scls.get_subject(schema)
        else:
            referrer_ctx = self.get_referrer_context(context)
            if referrer_ctx:
                base = referrer_ctx.op.scls

        if base is not None:
            # Concrete constraint
            if field.name == 'expr':
                # Concrete constraints cannot redefine the base check
                # expressions, and so the only way we should get here
                # is through field inheritance, so check that the
                # value is compiled and move on.
                if not value.is_compiled():
                    mcls = self.get_schema_metaclass()
                    dn = mcls.get_schema_class_displayname()
                    raise errors.InternalServerError(
                        f'uncompiled expression in the {field.name!r} field of'
                        f' {dn} {self.classname!r}'
                    )
                return value

            elif field.name in {'subjectexpr', 'finalexpr'}:
                anchors = {'__subject__': base}
                path_prefix_anchor = (
                    '__subject__' if isinstance(base, s_types.Type) else None
                )
                return s_expr.Expression.compiled(
                    value,
                    schema=schema,
                    options=qlcompiler.CompilerOptions(
                        modaliases=context.modaliases,
                        anchors=anchors,
                        path_prefix_anchor=path_prefix_anchor,
                        allow_generic_type_output=True,
                        schema_object_context=self.get_schema_metaclass(),
                        apply_query_rewrites=not context.stdmode,
                        track_schema_ref_exprs=track_schema_ref_exprs,
                    ),
                )

            else:
                return super().compile_expr_field(
                    schema, context, field, value)

        elif field.name in ('expr', 'subjectexpr'):
            # Abstract constraint.
            params = self._get_params(schema, context)

            param_anchors = s_func.get_params_symtable(
                params,
                schema,
                inlined_defaults=False,
            )

            return s_expr.Expression.compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    anchors=param_anchors,
                    func_params=params,
                    allow_generic_type_output=True,
                    schema_object_context=self.get_schema_metaclass(),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                ),
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs)

    @classmethod
    def get_inherited_ref_name(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        parent: so.Object,
        name: str,
    ) -> qlast.ObjectRef:
        refctx = cls.get_referrer_context(context)
        assert refctx is not None
        # reduce name to shortname
        if sn.Name.is_qualified(name):
            shortname: str = sn.shortname_from_fullname(sn.Name(name))
        else:
            shortname = name

        assert isinstance(refctx.op.classname, sn.SchemaName)
        nref = qlast.ObjectRef(
            name=shortname,
            module=refctx.op.classname.module,
        )

        return nref

    def get_ref_implicit_base_delta(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refcls: Constraint,
        implicit_bases: List[Constraint],
    ) -> inheriting.BaseDelta_T:
        child_bases = refcls.get_bases(schema).objects(schema)

        default_base = refcls.get_default_base_name()
        explicit_bases = [
            b for b in child_bases
            # abstract constraints play a similar role to default_base
            if not b.get_is_abstract(schema)
            and b.generic(schema) and b.get_name(schema) != default_base
        ]

        new_bases = implicit_bases + explicit_bases
        return inheriting.delta_bases(
            [b.get_name(schema) for b in child_bases],
            [b.get_name(schema) for b in new_bases],
        )

    def get_ast_attr_for_field(self, field: str) -> Optional[str]:
        if field in ('subjectexpr', 'args'):
            return field
        else:
            return None

    def get_ddl_identity_fields(
        self,
        context: sd.CommandContext,
    ) -> Tuple[so.Field[Any], ...]:
        id_fields = super().get_ddl_identity_fields(context)
        omit_fields = set()
        if not self.has_ddl_identity('subjectexpr'):
            omit_fields.add('subjectexpr')
        if self.get_referrer_context(context) is None:
            omit_fields.add('args')

        if omit_fields:
            return tuple(f for f in id_fields if f.name not in omit_fields)
        else:
            return id_fields

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
        # Set up the constraint parameters as part of names to be
        # ignored in expression normalization.
        if isinstance(astnode, qlast.CreateConstraint):
            localnames |= {param.name for param in astnode.params}
        elif isinstance(astnode, qlast.AlterConstraint):
            # ALTER ABSTRACT CONSTRAINT doesn't repeat the params,
            # but we can get them from the schema.
            objref = astnode.name

            # Merge the context modaliases and the command modaliases.
            modaliases = dict(context.modaliases)
            modaliases.update(
                cls._modaliases_from_ast(schema, astnode, context))
            # Get the original constraint.
            constr = schema.get(
                objref.name,
                module_aliases=modaliases,
                type=Constraint,
            )

            localnames |= {param.get_parameter_name(schema) for param in
                           constr.get_params(schema).objects(schema)}

        return localnames


class CreateConstraint(
    ConstraintCommand,
    s_func.CreateCallableObject[Constraint],
    referencing.CreateReferencedInheritingObject[Constraint],
):

    astnode = [qlast.CreateConcreteConstraint, qlast.CreateConstraint]
    referenced_astnode = qlast.CreateConcreteConstraint

    @classmethod
    def _get_param_desc_from_ast(
        cls,
        schema: s_schema.Schema,
        modaliases: Mapping[Optional[str], str],
        astnode: qlast.ObjectDDL,
        *,
        param_offset: int=0
    ) -> List[s_func.ParameterDesc]:
        if not hasattr(astnode, 'params'):
            # Concrete constraint.
            return []

        params = super()._get_param_desc_from_ast(
            schema, modaliases, astnode, param_offset=param_offset + 1)

        params.insert(0, s_func.ParameterDesc(
            num=param_offset,
            name='__subject__',
            default=None,
            type=s_pseudo.PseudoTypeShell(name='anytype'),
            typemod=ft.TypeModifier.SingletonType,
            kind=ft.ParameterKind.PositionalParam,
        ))

        return params

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_create(schema, context)

        if self.get_referrer_context(context) is not None:
            # The checks below apply only to abstract constraints.
            return

        base_params: Optional[s_func.FuncParameterList] = None
        base_with_params: Optional[Constraint] = None

        bases = self.get_resolved_attribute_value(
            'bases',
            schema=schema,
            context=context,
        )

        for base in bases.objects(schema):
            params = base.get_params(schema)
            if params and len(params) > 1:
                # All constraints have __subject__ parameter
                # auto-injected, hence the "> 1" check.
                if base_params is not None:
                    raise errors.InvalidConstraintDefinitionError(
                        f'{self.get_verbosename()} '
                        f'extends multiple constraints '
                        f'with parameters',
                        context=self.source_context,
                    )
                base_params = params
                base_with_params = base

        if base_params:
            assert base_with_params is not None

            params = self._get_params(schema, context)
            if not params or len(params) == 1:
                # All constraints have __subject__ parameter
                # auto-injected, hence the "== 1" check.
                raise errors.InvalidConstraintDefinitionError(
                    f'{self.get_verbosename()} '
                    f'must define parameters to reflect parameters of '
                    f'the {base_with_params.get_verbosename(schema)} '
                    f'it extends',
                    context=self.source_context,
                )

            if len(params) < len(base_params):
                raise errors.InvalidConstraintDefinitionError(
                    f'{self.get_verbosename()} '
                    f'has fewer parameters than the '
                    f'{base_with_params.get_verbosename(schema)} '
                    f'it extends',
                    context=self.source_context,
                )

            # Skipping the __subject__ param
            for base_param, param in zip(base_params.objects(schema)[1:],
                                         params.objects(schema)[1:]):

                param_name = param.get_parameter_name(schema)
                base_param_name = base_param.get_parameter_name(schema)

                if param_name != base_param_name:
                    raise errors.InvalidConstraintDefinitionError(
                        f'the {param_name!r} parameter of the '
                        f'{self.get_verbosename()} '
                        f'must be renamed to {base_param_name!r} '
                        f'to match the signature of the base '
                        f'{base_with_params.get_verbosename(schema)} ',
                        context=self.source_context,
                    )

                param_type = param.get_type(schema)
                base_param_type = base_param.get_type(schema)

                if (
                    not base_param_type.is_polymorphic(schema)
                    and param_type.is_polymorphic(schema)
                ):
                    raise errors.InvalidConstraintDefinitionError(
                        f'the {param_name!r} parameter of the '
                        f'{self.get_verbosename()} cannot '
                        f'be of generic type because the corresponding '
                        f'parameter of the '
                        f'{base_with_params.get_verbosename(schema)} '
                        f'it extends has a concrete type',
                        context=self.source_context,
                    )

                if (
                    not base_param_type.is_polymorphic(schema) and
                    not param_type.is_polymorphic(schema) and
                    not param_type.implicitly_castable_to(
                        base_param_type, schema)
                ):
                    raise errors.InvalidConstraintDefinitionError(
                        f'the {param_name!r} parameter of the '
                        f'{self.get_verbosename()} has type of '
                        f'{param_type.get_displayname(schema)} that '
                        f'is not implicitly castable to the '
                        f'corresponding parameter of the '
                        f'{base_with_params.get_verbosename(schema)} with '
                        f'type {base_param_type.get_displayname(schema)}',
                        context=self.source_context,
                    )

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            schema = super()._create_begin(schema, context)
            return schema

        subject = referrer_ctx.scls
        assert isinstance(subject, ConsistencySubject)
        if not subject.can_accept_constraints(schema):
            raise errors.UnsupportedFeatureError(
                f'constraints cannot be defined on '
                f'{subject.get_verbosename(schema)}',
                context=self.source_context,
            )

        if not context.canonical:
            props = self.get_attributes(schema, context)
            props.pop('name')
            props.pop('subject', None)
            fullname = self.classname
            shortname = sn.shortname_from_fullname(fullname)
            self._populate_concrete_constraint_attrs(
                schema,
                context,
                subject_obj=subject,
                name=shortname,
                sourcectx=self.source_context,
                **props)

            self.set_attribute_value('subject', subject)

        return super()._create_begin(schema, context)

    def _populate_concrete_constraint_attrs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        subject_obj: Optional[so.Object],
        *,
        name: str,
        subjectexpr: Optional[s_expr.Expression] = None,
        sourcectx: Optional[c_parsing.ParserContext] = None,
        args: Any = None,
        **kwargs: Any
    ) -> None:
        from edb.ir import ast as ir_ast
        from edb.ir import utils as ir_utils

        constr_base = schema.get(name, type=Constraint)

        orig_subjectexpr = subjectexpr
        orig_subject = subject_obj
        base_subjectexpr = constr_base.get_field_value(schema, 'subjectexpr')
        if subjectexpr is None:
            subjectexpr = base_subjectexpr
        elif (base_subjectexpr is not None
                and subjectexpr.text != base_subjectexpr.text):
            raise errors.InvalidConstraintDefinitionError(
                f'subjectexpr is already defined for {name!r}'
            )

        if (isinstance(subject_obj, s_scalars.ScalarType)
                and constr_base.get_is_aggregate(schema)):
            raise errors.InvalidConstraintDefinitionError(
                f'{constr_base.get_verbosename(schema)} may not '
                f'be used on scalar types'
            )

        if subjectexpr is not None:
            subject_ql = subjectexpr.qlast
            subject = subject_ql
        else:
            subject = subject_obj

        expr: s_expr.Expression = constr_base.get_field_value(schema, 'expr')
        if not expr:
            raise errors.InvalidConstraintDefinitionError(
                f'missing constraint expression in {name!r}')

        # Re-parse instead of using expr.qlast, because we mutate
        # the AST below.
        expr_ql = qlparser.parse(expr.text)

        if not args:
            args = constr_base.get_field_value(schema, 'args')

        attrs = dict(kwargs)
        inherited = dict()
        if orig_subjectexpr is not None:
            attrs['subjectexpr'] = orig_subjectexpr
        else:
            base_subjectexpr = constr_base.get_subjectexpr(schema)
            if base_subjectexpr is not None:
                attrs['subjectexpr'] = base_subjectexpr
                inherited['subjectexpr'] = True

        errmessage = attrs.get('errmessage')
        if not errmessage:
            errmessage = constr_base.get_errmessage(schema)
            inherited['errmessage'] = True

        attrs['errmessage'] = errmessage

        if subject is not orig_subject:
            # subject has been redefined
            assert isinstance(subject, qlast.Base)
            qlutils.inline_anchors(
                expr_ql, anchors={qlast.Subject().name: subject})
            subject = orig_subject

        if args:
            args_ql: List[qlast.Base] = [
                qlast.Path(steps=[qlast.Subject()]),
            ]
            args_ql.extend(arg.qlast for arg in args)
            args_map = qlutils.index_parameters(
                args_ql,
                parameters=constr_base.get_params(schema),
                schema=schema,
            )
            qlutils.inline_parameters(expr_ql, args_map)

        attrs['args'] = args

        if expr == '__subject__':
            expr_context = sourcectx
        else:
            expr_context = None

        assert subject is not None
        path_prefix_anchor = (
            qlast.Subject().name if isinstance(subject, s_types.Type)
            else None
        )

        final_expr = s_expr.Expression.compiled(
            s_expr.Expression.from_ast(expr_ql, schema, {}),
            schema=schema,
            options=qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: subject},
                path_prefix_anchor=path_prefix_anchor,
                apply_query_rewrites=not context.stdmode,
            ),
        )

        bool_t = schema.get('std::bool', type=s_scalars.ScalarType)
        assert isinstance(final_expr.irast, ir_ast.Statement)

        expr_type = final_expr.irast.stype
        if not expr_type.issubclass(schema, bool_t):
            raise errors.InvalidConstraintDefinitionError(
                f'{name} constraint expression expected '
                f'to return a bool value, got '
                f'{expr_type.get_verbosename(schema)}',
                context=expr_context
            )

        if (subjectexpr is not None and isinstance(subject_obj, s_types.Type)
                and subject_obj.is_object_type()):
            final_subjectexpr = s_expr.Expression.compiled(
                subjectexpr,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    anchors={qlast.Subject().name: subject},
                    path_prefix_anchor=path_prefix_anchor,
                    singletons=frozenset({subject_obj}),
                    apply_query_rewrites=not context.stdmode,
                ),
            )
            assert isinstance(final_subjectexpr.irast, ir_ast.Statement)

            if final_subjectexpr.irast.cardinality.is_multi():
                refs = ir_utils.get_longest_paths(final_expr.irast)
                if len(refs) > 1:
                    raise errors.InvalidConstraintDefinitionError(
                        "Constraint with multi cardinality may not "
                        "reference multiple links or properties",
                        context=expr_context
                    )

        attrs['return_type'] = constr_base.get_return_type(schema)
        attrs['return_typemod'] = constr_base.get_return_typemod(schema)
        attrs['finalexpr'] = final_expr
        attrs['params'] = constr_base.get_params(schema)
        attrs['is_abstract'] = False

        for k, v in attrs.items():
            self.set_attribute_value(k, v, inherited=bool(inherited.get(k)))

    @classmethod
    def as_inherited_ref_cmd(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        astnode: qlast.ObjectDDL,
        parents: Any,
    ) -> sd.ObjectCommand[Constraint]:
        cmd = super().as_inherited_ref_cmd(schema, context, astnode, parents)

        args = cls._constraint_args_from_ast(schema, astnode, context)
        if args:
            cmd.set_attribute_value('args', args)

        subj_expr = parents[0].get_subjectexpr(schema)
        if subj_expr is not None:
            cmd.set_attribute_value('subjectexpr', subj_expr)

        cmd.set_attribute_value(
            'bases', so.ObjectList.create(schema, parents))

        return cmd

    @classmethod
    def as_inherited_ref_ast(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: str,
        parent: so.Object,
    ) -> qlast.ObjectDDL:
        assert isinstance(parent, Constraint)
        astnode_cls = cls.referenced_astnode
        nref = cls.get_inherited_ref_name(schema, context, parent, name)
        args = []

        parent_args = parent.get_args(schema)
        if parent_args:
            parent_args = parent.get_args(schema)
            assert parent_args is not None
            for arg_expr in parent_args:
                arg = edgeql.parse_fragment(arg_expr.text)
                args.append(arg)

        subj_expr = parent.get_subjectexpr(schema)
        if subj_expr is not None:
            subj_expr_ql = edgeql.parse_fragment(subj_expr.text)
        else:
            subj_expr_ql = None

        astnode = astnode_cls(name=nref, args=args, subjectexpr=subj_expr_ql)

        return astnode

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> CreateConstraint:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteConstraint):
            if astnode.delegated:
                cmd.set_attribute_value('delegated', astnode.delegated)

            args = cls._constraint_args_from_ast(schema, astnode, context)
            if args:
                cmd.set_attribute_value('args', args)

        elif isinstance(astnode, qlast.CreateConstraint):
            params = cls._get_param_desc_from_ast(
                schema, context.modaliases, astnode)

            for param in params:
                if param.get_kind(schema) is ft.ParameterKind.NamedOnlyParam:
                    raise errors.InvalidConstraintDefinitionError(
                        'named only parameters are not allowed '
                        'in this context',
                        context=astnode.context)

                if param.get_default(schema) is not None:
                    raise errors.InvalidConstraintDefinitionError(
                        'constraints do not support parameters '
                        'with defaults',
                        context=astnode.context)

        if cmd.get_attribute_value('return_type') is None:
            cmd.set_attribute_value(
                'return_type',
                schema.get('std::bool'),
            )

        if cmd.get_attribute_value('return_typemod') is None:
            cmd.set_attribute_value(
                'return_typemod',
                ft.TypeModifier.SingletonType,
            )

        assert isinstance(astnode, (qlast.CreateConstraint,
                                    qlast.CreateConcreteConstraint))
        # 'subjectexpr' can be present in either astnode type
        if astnode.subjectexpr:
            orig_text = cls.get_orig_expr_text(schema, astnode, 'subjectexpr')

            subjectexpr = s_expr.Expression.from_ast(
                astnode.subjectexpr,
                schema,
                context.modaliases,
                context.localnames,
                orig_text=orig_text,
            )

            cmd.set_attribute_value(
                'subjectexpr',
                subjectexpr,
            )

        cls._validate_subcommands(astnode)
        assert isinstance(cmd, CreateConstraint)
        return cmd

    def _apply_fields_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> None:
        super()._apply_fields_ast(schema, context, node)

        if isinstance(node, qlast.CreateConstraint):
            params = []
            for op in self.get_subcommands(type=s_func.ParameterCommand):
                props = op.get_resolved_attributes(schema, context)
                pname = s_func.Parameter.paramname_from_fullname(props['name'])
                if pname == '__subject__':
                    continue
                num: int = props['num']
                default = props.get('default')
                param = qlast.FuncParam(
                    name=pname,
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
        if op.property == 'delegated':
            if isinstance(node, qlast.CreateConcreteConstraint):
                assert isinstance(op.new_value, bool)
                node.delegated = op.new_value
            else:
                node.commands.append(
                    qlast.SetSpecialField(
                        name='delegated',
                        value=op.new_value,
                    )
                )
            return
        elif op.property == 'args':
            assert isinstance(op.new_value, s_expr.ExpressionList)
            node.args = [arg.qlast for arg in op.new_value]
            return

        super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell]:
        if isinstance(astnode, qlast.CreateConcreteConstraint):
            classname = cls._classname_from_ast(schema, astnode, context)
            base_name = sn.shortname_from_fullname(classname)
            base = utils.ast_objref_to_object_shell(
                qlast.ObjectRef(
                    module=base_name.module,
                    name=base_name.name,
                ),
                metaclass=Constraint,
                schema=schema,
                modaliases=context.modaliases,
            )
            return [base]
        else:
            return super()._classbases_from_ast(schema, astnode, context)


class RenameConstraint(ConstraintCommand, sd.RenameObject[Constraint]):
    pass


class AlterConstraintOwned(
    referencing.AlterOwned[Constraint],
    schema_metaclass=Constraint,
    referrer_context_class=ConsistencySubjectCommandContext,
):
    astnode = qlast.AlterConstraintOwned


class AlterConstraint(
    ConstraintCommand,
    referencing.AlterReferencedInheritingObject[Constraint],
):
    astnode = [qlast.AlterConcreteConstraint, qlast.AlterConstraint]
    referenced_astnode = qlast.AlterConcreteConstraint

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> AlterConstraint:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterConstraint)

        if isinstance(astnode, (qlast.CreateConcreteConstraint,
                                qlast.AlterConcreteConstraint)):
            if getattr(astnode, 'delegated', False):
                assert isinstance(astnode, qlast.CreateConcreteConstraint)
                cmd.set_attribute_value('delegated', astnode.delegated)

            new_name = None
            for op in cmd.get_subcommands(type=RenameConstraint):
                new_name = op.new_name

            if new_name is not None:
                cmd.set_attribute_value('name', new_name)

        cls._validate_subcommands(astnode)
        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'delegated':
            node.delegated = op.new_value
        else:
            super()._apply_field_ast(schema, context, node, op)

    def validate_alter(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_alter(schema, context)

        self_delegated = self.get_attribute_value('delegated')
        if not self_delegated:
            return

        concrete_bases = [
            b for b in self.scls.get_bases(schema).objects(schema)
            if not b.generic(schema) and not b.get_delegated(schema)
        ]
        if concrete_bases:
            tgt_repr = self.scls.get_verbosename(schema, with_parent=True)
            bases_repr = ', '.join(
                b.get_subject(schema).get_verbosename(schema, with_parent=True)
                for b in concrete_bases
            )
            raise errors.InvalidConstraintDefinitionError(
                f'cannot redefine {tgt_repr} as delegated:'
                f' it is defined as non-delegated in {bases_repr}',
                context=self.source_context,
            )


class DeleteConstraint(
    ConstraintCommand,
    referencing.DeleteReferencedInheritingObject[Constraint],
    s_func.DeleteCallableObject[Constraint],
):
    astnode = [qlast.DropConcreteConstraint, qlast.DropConstraint]
    referenced_astnode = qlast.DropConcreteConstraint

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'args':
            assert isinstance(op.old_value, s_expr.ExpressionList)
            node.args = [arg.qlast for arg in op.old_value]
            return

        super()._apply_field_ast(schema, context, node, op)


class RebaseConstraint(
    ConstraintCommand,
    referencing.RebaseReferencedInheritingObject[Constraint],
):
    pass
