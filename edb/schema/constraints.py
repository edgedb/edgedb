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
from edb.common import verutils

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser
from edb.edgeql import utils as qlutils
from edb.edgeql import qltypes

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
        # concrete constraints cannot redefine parameters and always
        # inherit from super.
        return supers[0].get_explicit_field_value(schema, field_name, None)


def constraintname_from_fullname(name: sn.Name) -> sn.QualName:
    assert isinstance(name, sn.QualName)
    # the dict key for constraints drops the first qual, which makes
    # it independent of where it is declared
    short = sn.shortname_from_fullname(name)
    quals = sn.quals_from_fullname(name)
    return sn.QualName(
        name=sn.get_specialized_name(short, *quals[1:]),
        module='__',
    )


def _constraint_object_key(schema: s_schema.Schema, o: so.Object) -> sn.Name:
    return constraintname_from_fullname(o.get_name(schema))


class ObjectIndexByConstraintName(
    so.ObjectIndexBase[sn.Name, so.Object_T],
    key=_constraint_object_key,
):

    @classmethod
    def get_key_for_name(
        cls,
        schema: s_schema.Schema,
        name: sn.Name,
    ) -> sn.Name:
        return constraintname_from_fullname(name)


class Constraint(
    referencing.ReferencedInheritingObject,
    s_func.CallableObject, s_abc.Constraint,
    qlkind=ft.SchemaObjectClass.CONSTRAINT,
    data_safe=True,
):

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

    subjectexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.833, coerce=True,
        ddl_identity=True)

    finalexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.909, coerce=True)

    except_expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        ddl_identity=True,
    )

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
        special_ddl_syntax=True,
        compcoef=0.9,
    )

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True)

    is_aggregate = so.SchemaField(
        bool, default=False, compcoef=0.971, allow_ddl_set=False)

    def get_name_impacting_ancestors(
        self, schema: s_schema.Schema,
    ) -> List[Constraint]:
        if self.generic(schema):
            return []
        else:
            return [self.get_nearest_generic_parent(schema)]

    def get_constraint_origins(
            self, schema: s_schema.Schema) -> List[Constraint]:
        origins: List[Constraint] = []
        for base in self.get_bases(schema).objects(schema):
            if not base.generic(schema) and not base.get_delegated(schema):
                origins.extend(
                    x for x in base.get_constraint_origins(schema)
                    if x not in origins
                )

        return [self] if not origins else origins

    def is_independent(self, schema: s_schema.Schema) -> bool:
        return (
            not self.descendants(schema)
            and self.get_constraint_origins(schema) == [self]
        )

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool = False
    ) -> str:
        vn = super().get_verbosename(schema, with_parent=with_parent)
        if self.generic(schema):
            return f'abstract {vn}'
        else:
            # concrete constraint must have a subject
            assert self.get_subject(schema) is not None
            return vn

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_subject(schema) is None

    def get_subject(self, schema: s_schema.Schema) -> ConsistencySubject:
        return cast(
            ConsistencySubject,
            self.get_field_value(schema, 'subject'),
        )

    def format_error(
        self,
        schema: s_schema.Schema,
    ) -> str:
        subject = self.get_subject(schema)
        titleattr = subject.get_annotation(schema, sn.QualName('std', 'title'))

        if not titleattr:
            subjname = subject.get_shortname(schema)
            subjtitle = subjname.name
        else:
            subjtitle = titleattr

        return self.format_error_message(schema, subjtitle)

    def format_error_message(
        self,
        schema: s_schema.Schema,
        subjtitle: str,
    ) -> str:
        errmsg = self.get_errmessage(schema)
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
        confidence: float,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[Constraint]:
        return super().as_alter_delta(
            other,
            self_schema=self_schema,
            other_schema=other_schema,
            confidence=confidence,
            context=context,
        )

    def as_delete_delta(
        self,
        *,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[Constraint]:
        return super().as_delete_delta(schema=schema, context=context)

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
    def get_root_classes(cls) -> Tuple[sn.QualName, ...]:
        return (
            sn.QualName(module='std', name='constraint'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.QualName:
        return sn.QualName('std', 'constraint')


class ConsistencySubject(
    so.QualifiedObject,
    so.InheritingObject,
    s_anno.AnnotationSubject,
):
    constraints_refs = so.RefDict(
        attr='constraints',
        ref_cls=Constraint)

    constraints = so.SchemaField(
        ObjectIndexByConstraintName[Constraint],
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
            if isinstance(command, qlast.SetField):
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
        base_name: sn.Name,
        referrer_name: sn.QualName,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        if isinstance(astnode, qlast.CreateConstraint):
            return ()
        exprs = []
        args = cls._constraint_args_from_ast(schema, astnode, context)
        for arg in args:
            exprs.append(arg.text)

        assert isinstance(astnode, qlast.ConcreteConstraintOp)
        if astnode.subjectexpr:
            # use the normalized text directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.subjectexpr, schema, context.modaliases)
            exprs.append(expr.text)
        if astnode.except_expr:
            # use the normalized text directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.except_expr, schema, context.modaliases)
            # but mangle it a bit, so that we can distinguish between
            # on and except when only one is present
            exprs.append('!' + expr.text)

        return (cls._name_qual_from_exprs(schema, exprs),)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.QualName
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
        assert isinstance(astnode, qlast.ConcreteConstraintOp)

        if astnode.args:
            for arg in astnode.args:
                arg_expr = s_expr.Expression.from_ast(
                    arg, schema, context.modaliases)
                args.append(arg_expr)

        return args

    @classmethod
    def as_inherited_ref_ast(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: sn.Name,
        parent: so.Object,
    ) -> qlast.ObjectDDL:
        assert isinstance(parent, Constraint)
        astnode_cls = cls.referenced_astnode  # type: ignore
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
        if (
            subj_expr is None
            # Don't include subjectexpr if it was inherited from an
            # abstract constraint.
            or parent.get_nearest_generic_parent(
                schema).get_subjectexpr(schema) is not None
        ):
            subj_expr_ql = None
        else:
            subj_expr_ql = edgeql.parse_fragment(subj_expr.text)

        except_expr = parent.get_except_expr(schema)
        if except_expr:
            except_expr_ql = except_expr.qlast
        else:
            except_expr_ql = None

        astnode = astnode_cls(
            name=nref, args=args, subjectexpr=subj_expr_ql,
            except_expr=except_expr_ql)

        return cast(qlast.ObjectDDL, astnode)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        from . import pointers as s_pointers

        base: Optional[so.Object] = None
        if isinstance(self, AlterConstraint):
            base = self.scls.get_subject(schema)
        else:
            referrer_ctx = self.get_referrer_context(context)
            if referrer_ctx:
                base = referrer_ctx.op.scls

        if base is not None:
            assert isinstance(base, (s_types.Type, s_pointers.Pointer))
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
                # HACK: Not *really* compiled, but...
                return value  # type: ignore

            elif field.name in {'subjectexpr', 'finalexpr', 'except_expr'}:
                return value.compiled(
                    schema=schema,
                    options=qlcompiler.CompilerOptions(
                        modaliases=context.modaliases,
                        anchors={qlast.Subject().name: base},
                        path_prefix_anchor=qlast.Subject().name,
                        singletons=frozenset([base]),
                        allow_generic_type_output=True,
                        schema_object_context=self.get_schema_metaclass(),
                        apply_query_rewrites=False,
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

            return value.compiled(
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

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name in {'expr', 'subjectexpr', 'finalexpr'}:
            return s_expr.Expression(text='SELECT false')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    @classmethod
    def get_inherited_ref_name(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        parent: so.Object,
        name: sn.Name,
    ) -> qlast.ObjectRef:
        bn = sn.shortname_from_fullname(name)
        return utils.name_to_ast_ref(bn)

    def get_ref_implicit_base_delta(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refcls: Constraint,
        implicit_bases: List[Constraint],
    ) -> inheriting.BaseDelta_T[Constraint]:
        child_bases = refcls.get_bases(schema).objects(schema)

        return inheriting.delta_bases(
            [b.get_name(schema) for b in child_bases],
            [b.get_name(schema) for b in implicit_bases],
            t=Constraint,
        )

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in ('subjectexpr', 'args', 'except_expr'):
            return field
        elif (
            field == 'delegated'
            and astnode is qlast.CreateConcreteConstraint
        ):
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)

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
                utils.ast_ref_to_name(objref),
                module_aliases=modaliases,
                type=Constraint,
            )

            localnames |= {param.get_parameter_name(schema) for param in
                           constr.get_params(schema).objects(schema)}

        return localnames

    def _populate_concrete_constraint_attrs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        subject_obj: Optional[so.Object],
        *,
        name: sn.QualName,
        subjectexpr: Optional[s_expr.Expression] = None,
        subjectexpr_inherited: bool = False,
        sourcectx: Optional[c_parsing.ParserContext] = None,
        args: Any = None,
        **kwargs: Any
    ) -> None:
        from edb.ir import ast as ir_ast
        from edb.ir import utils as ir_utils
        from . import pointers as s_pointers
        from . import links as s_links
        from . import objtypes as s_objtypes
        from . import scalars as s_scalars

        bases = self.get_resolved_attribute_value(
            'bases', schema=schema, context=context,
        )
        if not bases:
            bases = self.scls.get_bases(schema)
        constr_base = bases.objects(schema)[0]
        # If we have a concrete base, then we should inherit all of
        # these attrs through the normal inherit_fields() mechanisms,
        # and populating them ourselves will just mess up
        # inherited_fields.
        if not constr_base.generic(schema):
            return

        orig_subjectexpr = subjectexpr
        orig_subject = subject_obj
        base_subjectexpr = constr_base.get_field_value(schema, 'subjectexpr')
        if subjectexpr is None:
            subjectexpr = base_subjectexpr
        elif (base_subjectexpr is not None
                and subjectexpr.text != base_subjectexpr.text):
            raise errors.InvalidConstraintDefinitionError(
                f'subjectexpr is already defined for {name}',
                context=sourcectx,
            )

        if (isinstance(subject_obj, s_scalars.ScalarType)
                and constr_base.get_is_aggregate(schema)):
            raise errors.InvalidConstraintDefinitionError(
                f'{constr_base.get_verbosename(schema)} may not '
                f'be used on scalar types',
                context=sourcectx,
            )

        if (
            subjectexpr is None
            and isinstance(subject_obj, s_objtypes.ObjectType)
        ):
            raise errors.InvalidConstraintDefinitionError(
                "constraints on object types must have an 'on' clause",
                context=sourcectx,
            )

        if subjectexpr is not None:
            subject_ql = subjectexpr.qlast
            subject = subject_ql
        else:
            subject = subject_obj

        expr: s_expr.Expression = constr_base.get_field_value(schema, 'expr')
        if not expr:
            raise errors.InvalidConstraintDefinitionError(
                f'missing constraint expression in {name}')

        # Re-parse instead of using expr.qlast, because we mutate
        # the AST below.
        expr_ql = qlparser.parse(expr.text)

        if not args:
            args = constr_base.get_field_value(schema, 'args')

        attrs = dict(kwargs)
        inherited = dict()
        if orig_subjectexpr is not None:
            attrs['subjectexpr'] = orig_subjectexpr
            inherited['subjectexpr'] = subjectexpr_inherited
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

        if subject_obj:
            assert isinstance(subject_obj, (s_types.Type, s_pointers.Pointer))
            singletons = frozenset({subject_obj})
        else:
            singletons = frozenset()

        assert subject is not None
        final_expr = s_expr.Expression.from_ast(expr_ql, schema, {}).compiled(
            schema=schema,
            options=qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: subject},
                path_prefix_anchor=qlast.Subject().name,
                singletons=singletons,
                apply_query_rewrites=False,
                schema_object_context=self.get_schema_metaclass(),
            ),
        )

        bool_t = schema.get('std::bool', type=s_scalars.ScalarType)

        expr_type = final_expr.irast.stype
        expr_schema = final_expr.irast.schema
        if not expr_type.issubclass(expr_schema, bool_t):
            raise errors.InvalidConstraintDefinitionError(
                f'{name} constraint expression expected '
                f'to return a bool value, got '
                f'{expr_type.get_verbosename(expr_schema)}',
                context=sourcectx
            )

        except_expr = attrs.get('except_expr')
        if except_expr:
            if isinstance(subject, s_pointers.Pointer):
                raise errors.InvalidConstraintDefinitionError(
                    "only object constraints may use EXCEPT",
                    context=sourcectx
                )

        if subjectexpr is not None:
            options = qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: subject},
                path_prefix_anchor=qlast.Subject().name,
                singletons=singletons,
                apply_query_rewrites=False,
                schema_object_context=self.get_schema_metaclass(),
            )

            final_subjectexpr = subjectexpr.compiled(
                schema=schema, options=options
            )

            refs = ir_utils.get_longest_paths(final_expr.irast)

            final_except_expr = None
            if except_expr:
                final_except_expr = except_expr.compiled(
                    schema=schema, options=options
                )
                refs |= ir_utils.get_longest_paths(final_except_expr.irast)

            has_multi = False
            for ref in refs:
                assert subject_obj
                while ref.rptr:
                    rptr = ref.rptr
                    if rptr.dir_cardinality.is_multi():
                        has_multi = True

                    # We don't need to look further than the subject,
                    # which is always valid. (And which is a singleton
                    # in a constraint expression if it is itself a
                    # singleton, regardless of other parts of the path.)
                    if (
                        isinstance(rptr.ptrref, ir_ast.PointerRef)
                        and rptr.ptrref.id == subject_obj.id
                    ):
                        break

                    if (not isinstance(rptr.ptrref,
                                       ir_ast.TupleIndirectionPointerRef)
                            and rptr.ptrref.source_ptr is None
                            and rptr.source.rptr is not None):
                        if isinstance(subject, s_links.Link):
                            raise errors.InvalidConstraintDefinitionError(
                                "link constraints may not access "
                                "the link target",
                                context=sourcectx
                            )
                        else:
                            raise errors.InvalidConstraintDefinitionError(
                                "constraints cannot contain paths with more "
                                "than one hop",
                                context=sourcectx
                            )

                    ref = rptr.source

            if has_multi and len(refs) > 1:
                raise errors.InvalidConstraintDefinitionError(
                    "cannot reference multiple links or properties in a "
                    "constraint where at least one link or property is MULTI",
                    context=sourcectx
                )

            if has_multi and ir_utils.contains_set_of_op(
                    final_subjectexpr.irast):
                raise errors.InvalidConstraintDefinitionError(
                    "cannot use aggregate functions or operators "
                    "in a non-aggregating constraint",
                    context=sourcectx
                )

            if (
                final_subjectexpr.irast.volatility
                != qltypes.Volatility.Immutable
            ):
                raise errors.InvalidConstraintDefinitionError(
                    f'constraint expressions must be immutable',
                    context=final_subjectexpr.irast.context,
                )

            if final_except_expr:
                if (
                    final_except_expr.irast.volatility
                    != qltypes.Volatility.Immutable
                ):
                    raise errors.InvalidConstraintDefinitionError(
                        f'constraint expressions must be immutable',
                        context=final_except_expr.irast.context,
                    )

        if final_expr.irast.volatility != qltypes.Volatility.Immutable:
            raise errors.InvalidConstraintDefinitionError(
                f'constraint expressions must be immutable',
                context=sourcectx,
            )

        attrs['finalexpr'] = final_expr
        attrs['params'] = constr_base.get_params(schema)
        inherited['params'] = True
        attrs['abstract'] = False

        for k, v in attrs.items():
            self.set_attribute_value(k, v, inherited=bool(inherited.get(k)))


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
        if not isinstance(astnode, qlast.CallableObjectCommand):
            # Concrete constraint.
            return []

        params = super()._get_param_desc_from_ast(
            schema, modaliases, astnode, param_offset=param_offset + 1)

        params.insert(0, s_func.ParameterDesc(
            num=param_offset,
            name=sn.UnqualName('__subject__'),
            default=None,
            type=s_pseudo.PseudoTypeShell(name=sn.UnqualName('anytype')),
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
            assert isinstance(shortname, sn.QualName), \
                "expected qualified name"
            self._populate_concrete_constraint_attrs(
                schema,
                context,
                subject_obj=subject,
                name=shortname,
                subjectexpr_inherited=self.is_attribute_inherited(
                    'subjectexpr'),
                sourcectx=self.source_context,
                **props,
            )

            self.set_attribute_value('subject', subject)

        return super()._create_begin(schema, context)

    @classmethod
    def as_inherited_ref_cmd(
        cls,
        *,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        astnode: qlast.ObjectDDL,
        bases: List[Constraint],
        referrer: so.Object,
    ) -> sd.ObjectCommand[Constraint]:
        cmd = super().as_inherited_ref_cmd(
            schema=schema,
            context=context,
            astnode=astnode,
            bases=bases,
            referrer=referrer,
        )

        args = cls._constraint_args_from_ast(schema, astnode, context)
        if args:
            cmd.set_attribute_value('args', args)

        subj_expr = bases[0].get_subjectexpr(schema)
        if subj_expr is not None:
            cmd.set_attribute_value('subjectexpr', subj_expr, inherited=True)

        params = bases[0].get_params(schema)
        if params is not None:
            cmd.set_attribute_value('params', params, inherited=True)

        return cmd

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

            if (
                orig_text is not None
                and context.compat_ver_is_before(
                    (1, 0, verutils.VersionStage.ALPHA, 6)
                )
            ):
                # Versions prior to a6 used a different expression
                # normalization strategy, so we must renormalize the
                # expression.
                expr_ql = qlcompiler.renormalize_compat(
                    astnode.subjectexpr,
                    orig_text,
                    schema=schema,
                    localnames=context.localnames,
                )
            else:
                expr_ql = astnode.subjectexpr

            subjectexpr = s_expr.Expression.from_ast(
                expr_ql,
                schema,
                context.modaliases,
                context.localnames,
            )

            cmd.set_attribute_value(
                'subjectexpr',
                subjectexpr,
            )

        if (
            isinstance(astnode, qlast.CreateConcreteConstraint)
            and astnode.except_expr
        ):
            except_expr = s_expr.Expression.from_ast(
                astnode.except_expr,
                schema,
                context.modaliases,
                context.localnames,
            )

            cmd.set_attribute_value('except_expr', except_expr)

        cls._validate_subcommands(astnode)
        assert isinstance(cmd, CreateConstraint)
        return cmd

    def _skip_param(self, props: Dict[str, Any]) -> bool:
        pname = s_func.Parameter.paramname_from_fullname(props['name'])
        return pname == '__subject__'

    def _get_params_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
    ) -> List[Tuple[int, qlast.FuncParam]]:
        if isinstance(node, qlast.CreateConstraint):
            return super()._get_params_ast(schema, context, node)
        else:
            return []

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if (
            op.property == 'args'
            and isinstance(node, (qlast.CreateConcreteConstraint,
                                  qlast.AlterConcreteConstraint))
        ):
            assert isinstance(op.new_value, s_expr.ExpressionList)
            args = []
            for arg in op.new_value:
                exprast = arg.qlast
                assert isinstance(exprast, qlast.Expr), "expected qlast.Expr"
                args.append(exprast)
            node.args = args
            return

        super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell[Constraint]]:
        if isinstance(astnode, qlast.CreateConcreteConstraint):
            classname = cls._classname_from_ast(schema, astnode, context)
            base_name = sn.shortname_from_fullname(classname)
            assert isinstance(base_name, sn.QualName), \
                "expected qualified name"
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


class RenameConstraint(
    ConstraintCommand,
    s_func.RenameCallableObject[Constraint],
    referencing.RenameReferencedInheritingObject[Constraint],
):
    @classmethod
    def _classname_quals_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        base_name: sn.Name,
        referrer_name: sn.QualName,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        parent_op = cls.get_parent_op(context)
        assert isinstance(parent_op.classname, sn.QualName)
        return cls._classname_quals_from_name(parent_op.classname)

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)

        if not context.canonical and self.scls.get_abstract(schema):
            self._propagate_ref_rename(schema, context, self.scls)

        return schema


class AlterConstraintOwned(
    referencing.AlterOwned[Constraint],
    ConstraintCommand,
    field='owned',
    referrer_context_class=ConsistencySubjectCommandContext,
):
    pass


class AlterConstraint(
    ConstraintCommand,
    referencing.AlterReferencedInheritingObject[Constraint],
):
    astnode = [qlast.AlterConcreteConstraint, qlast.AlterConstraint]
    referenced_astnode = qlast.AlterConcreteConstraint

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            schema = super()._alter_begin(schema, context)
            return schema

        subject = referrer_ctx.scls
        assert isinstance(subject, ConsistencySubject)

        if not context.canonical:
            props = self.get_attributes(schema, context)
            props.pop('name', None)
            props.pop('subject', None)
            props.pop('expr', None)
            args = props.pop('args', None)
            if not args:
                args = self.scls.get_args(schema)
            subjectexpr = props.pop('subjectexpr', None)
            subjectexpr_inherited = self.is_attribute_inherited('subjectexpr')
            if not subjectexpr:
                subjectexpr_inherited = self.scls.field_is_inherited(
                    schema, 'subjectexpr')
                subjectexpr = self.scls.get_subjectexpr(schema)
            fullname = self.classname
            shortname = sn.shortname_from_fullname(fullname)
            assert isinstance(shortname, sn.QualName), \
                "expected qualified name"
            self._populate_concrete_constraint_attrs(
                schema,
                context,
                subject_obj=subject,
                name=shortname,
                subjectexpr=subjectexpr,
                subjectexpr_inherited=subjectexpr_inherited,
                args=args,
                sourcectx=self.source_context,
                **props,
            )

        return super()._alter_begin(schema, context)

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

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.scls.get_abstract(schema):
            return super()._get_ast(schema, context, parent_node=parent_node)

        # We need to make sure to include subjectexpr and args
        # in the AST, since they are really part of the name.
        op = self.as_inherited_ref_ast(
            schema, context, self.scls.get_name(schema),
            self.scls,
        )
        self._apply_fields_ast(schema, context, op)

        if (op is not None and hasattr(op, 'commands') and
                not op.commands):
            return None

        return op

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

    def canonicalize_alter_from_external_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        if (
            not self.get_attribute_value('abstract')
            and (subjectexpr :=
                 self.get_attribute_value('subjectexpr')) is not None
        ):
            # To compute the new name, we construct an AST of the
            # constraint, since that is the infrastructure we have for
            # computing the classname.
            name = sn.shortname_from_fullname(self.classname)
            assert isinstance(name, sn.QualName), "expected qualified name"
            ast = qlast.CreateConcreteConstraint(
                name=qlast.ObjectRef(name=name.name, module=name.module),
                subjectexpr=subjectexpr.qlast,
                args=[],
            )
            quals = sn.quals_from_fullname(self.classname)
            new_name = self._classname_from_ast_and_referrer(
                schema, sn.QualName.from_string(quals[0]), ast, context)
            if new_name == self.classname:
                return

            rename = self.scls.init_delta_command(
                schema, sd.RenameObject, new_name=new_name)
            rename.set_attribute_value(
                'name', value=new_name, orig_value=self.classname)
            self.add(rename)

    def _get_params(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_func.FuncParameterList:
        return self.scls.get_params(schema)


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
            assert isinstance(node, qlast.DropConcreteConstraint)
            node.args = [arg.qlast for arg in op.old_value]
            return

        super()._apply_field_ast(schema, context, node, op)


class RebaseConstraint(
    ConstraintCommand,
    referencing.RebaseReferencedInheritingObject[Constraint],
):
    def _get_bases_for_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        bases: Tuple[so.ObjectShell[Constraint], ...],
    ) -> Tuple[so.ObjectShell[Constraint], ...]:
        return ()
