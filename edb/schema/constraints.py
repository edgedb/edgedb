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


import hashlib

from edb import errors

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft

from . import abc as s_abc
from . import annotations
from . import delta as sd
from . import expr as s_expr
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import objects as so
from . import pseudo as s_pseudo
from . import referencing
from . import utils


class Constraint(inheriting.InheritingObject, s_func.CallableObject,
                 s_abc.Constraint):

    expr = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.909,
        coerce=True, allow_ddl_set=True)

    subjectexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.833, coerce=True)

    finalexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.909, coerce=True)

    subject = so.SchemaField(
        so.Object, default=None, inheritable=False)

    args = so.SchemaField(
        s_expr.ExpressionList,
        default=None, coerce=True, inheritable=False,
        compcoef=0.875)

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True)

    def get_verbosename(self, schema, *, with_parent: bool=False) -> str:
        is_abstract = self.generic(schema)
        vn = super().get_verbosename(schema)
        if is_abstract:
            return f'abstract {vn}'
        else:
            if with_parent:
                pvn = self.get_subject(schema).get_verbosename(
                    schema, with_parent=True)
                return f'{vn} of {pvn}'
            else:
                return vn

    def generic(self, schema):
        return self.get_subject(schema) is None

    def init_derived(self, schema, source, *qualifiers,
                     as_copy, mark_derived=False,
                     merge_bases=None, attrs=None,
                     dctx=None, **kwargs):

        if attrs is None:
            attrs = {}

        attrs['subject'] = source

        qualifiers = list(qualifiers)
        qualifiers.extend(self.get_derived_quals(schema, source, attrs))

        return super().init_derived(
            schema, source, *qualifiers, as_copy=as_copy,
            mark_derived=mark_derived, merge_bases=merge_bases,
            attrs=attrs, dctx=dctx, **kwargs)

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        schema = super().finalize(
            schema, bases=bases,
            apply_defaults=apply_defaults, dctx=dctx)

        self_params = self.get_explicit_field_value(schema, 'params', None)
        if not self.generic(schema) and self_params is None:
            schema = self.set_field_value(schema, 'params', [])

            if dctx is not None:
                from . import delta as sd

                dctx.current().op.add(sd.AlterObjectProperty(
                    property='params',
                    new_value=self.get_params(schema),
                    source='default'
                ))

        return schema

    def get_derived_quals(self, schema, source, attrs=None):
        if attrs and attrs.get('finalexpr'):
            finalexpr = attrs['finalexpr']
        else:
            finalexpr = self.get_field_value(schema, 'finalexpr')

        return (self._name_qual_from_expr(schema, finalexpr.text),)

    @classmethod
    def _name_qual_from_expr(self, schema, expr):
        m = hashlib.sha1()
        m.update(expr.encode())
        return m.hexdigest()

    @classmethod
    def _dummy_subject(cls, schema):
        # Point subject placeholder to a dummy pointer to make EdgeQL
        # pipeline happy.
        return s_pseudo.Any.instance

    @classmethod
    def get_concrete_constraint_attrs(
            cls, schema, subject, *, name, subjectexpr=None,
            sourcectx=None, args=[], modaliases=None, **kwargs):
        from edb.edgeql import parser as qlparser
        from edb.edgeql import utils as qlutils

        constr_base = schema.get(name, module_aliases=modaliases)
        module_aliases = {}

        orig_subject = subject
        base_subjectexpr = constr_base.get_field_value(schema, 'subjectexpr')
        if subjectexpr is None:
            subjectexpr = base_subjectexpr
        elif (base_subjectexpr is not None
                and subjectexpr.text != base_subjectexpr.text):
            raise errors.InvalidConstraintDefinitionError(
                'subjectexpr is already defined for ' +
                f'{str(name)!r}')

        if subjectexpr is not None:
            subject_ql = subjectexpr.qlast
            if subject_ql is None:
                subject_ql = qlparser.parse(subjectexpr.text, module_aliases)

            subject = subject_ql

        expr: s_expr.Expression = constr_base.get_field_value(schema, 'expr')
        if not expr:
            raise errors.InvalidConstraintDefinitionError(
                f'missing constraint expression in {name!r}')

        expr_ql = qlparser.parse(expr.text, module_aliases)

        if not args:
            args = constr_base.get_field_value(schema, 'args')

        attrs = dict(kwargs)

        if subject is not orig_subject:
            # subject has been redefined
            qlutils.inline_anchors(expr_ql, anchors={qlast.Subject: subject})
            subject = orig_subject

        args_map = None
        if args:
            args_ql = [
                qlast.Path(steps=[qlast.Subject()]),
            ]

            args_ql.extend(
                qlparser.parse(arg.text, module_aliases) for arg in args
            )

            args_map = qlutils.index_parameters(
                args_ql,
                parameters=constr_base.get_params(schema),
                schema=schema)

            qlutils.inline_parameters(expr_ql, args_map)

            args_map = {name: edgeql.generate_source(val, pretty=False)
                        for name, val in args_map.items()}

            errmessage = attrs.get('errmessage')
            if not errmessage:
                errmessage = constr_base.get_errmessage(schema)

            args_map['__subject__'] = '{__subject__}'
            attrs['errmessage'] = errmessage.format(**args_map)

        attrs['args'] = args

        if expr == '__subject__':
            expr_context = sourcectx
        else:
            expr_context = None

        final_expr = s_expr.Expression.compiled(
            s_expr.Expression.from_ast(expr_ql, schema, module_aliases),
            schema=schema,
            modaliases=module_aliases,
            anchors={qlast.Subject: subject},
        )

        bool_t = schema.get('std::bool')
        expr_type = final_expr.irast.stype
        if not expr_type.issubclass(schema, bool_t):
            raise errors.InvalidConstraintDefinitionError(
                f'{name} constraint expression expected '
                f'to return a bool value, got '
                f'{expr_type.get_name(schema).name!r}',
                context=expr_context
            )

        attrs['finalexpr'] = final_expr

        return constr_base, attrs

    @classmethod
    def create_concrete_constraint(
            cls, schema, subject, *, name, subjectexpr=None,
            sourcectx=None, args=[], modaliases=None, **kwargs):

        if subject.is_scalar() and subject.is_enum(schema):
            raise errors.UnsupportedFeatureError(
                f'constraints cannot be defined on an enumerated type',
                context=sourcectx,
            )

        constr_base, attrs = cls.get_concrete_constraint_attrs(
            schema, subject, name=name, subjectexpr=subjectexpr,
            sourcectx=sourcectx, args=args, modaliases=modaliases, **kwargs)

        schema, constraint = constr_base.derive(
            schema,
            subject,
            merge_bases=[constr_base],
            attrs=attrs)

        return schema, constraint, attrs

    def format_error_message(self, schema):
        errmsg = self.get_errmessage(schema)
        subject = self.get_subject(schema)
        titleattr = subject.get_annotation(schema, 'std::title')

        if not titleattr:
            subjname = subject.get_shortname(schema)
            subjtitle = subjname.name
        else:
            subjtitle = titleattr

        formatted = errmsg.format(__subject__=subjtitle)

        return formatted

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='constraint'),
        )

    @classmethod
    def get_default_base_name(self):
        return sn.Name('std::constraint')


class ConsistencySubject(inheriting.InheritingObject):
    constraints_refs = so.RefDict(
        attr='constraints',
        local_attr='own_constraints',
        ref_cls=Constraint)

    constraints = so.SchemaField(
        so.ObjectIndexByFullname,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectIndexByFullname, hashable=False)

    own_constraints = so.SchemaField(
        so.ObjectIndexByFullname, compcoef=0.887,
        inheritable=False, ephemeral=True,
        coerce=True,
        default=so.ObjectIndexByFullname)

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        schema, item = super().inherit_pure(schema, item, source, dctx=dctx)

        item_constraints = item.get_own_constraints(schema)
        if any(c.get_is_abstract(schema)
               for c in item_constraints.objects(schema)):
            # Have abstract constraints, cannot go pure inheritance,
            # must create a derived Object with materialized
            # constraints.
            schema, item = type(item).derive_from_root(
                schema,
                source=source,
                merge_bases=[item],
                unqualified_name=item.get_shortname(schema).name,
                dctx=dctx,
            )

        return schema, item

    def finish_classref_dict_merge(self, schema, bases, attr):
        schema = super().finish_classref_dict_merge(schema, bases, attr)

        if attr == 'constraints':
            # Materialize unmerged abstract constraints
            all_constraints = self.get_constraints(schema)

            for cn, constraint in all_constraints.items(schema):
                if not constraint.get_is_abstract(schema):
                    # Constraint is not delegated, nothing to do.
                    continue

                if self.get_own_constraints(schema).has(schema, cn):
                    # The constraint is declared locally, nothing to do.
                    continue

                quals = constraint.get_derived_quals(schema, self)
                der_name = constraint.get_derived_name(
                    schema, self, *quals)

                if self.get_own_constraints(schema).has(schema, der_name):
                    # The constraint is declared locally, nothing to do.
                    continue

                for base in bases:
                    quals = constraint.get_derived_quals(schema, base)
                    der_name = constraint.get_derived_name(
                        schema, base, *quals)
                    constr = all_constraints.get(schema, der_name, None)
                    if (constr is not None and
                            not constr.get_is_abstract(schema)):
                        break
                else:
                    schema, constraint = constraint.derive_copy(
                        schema, self,
                        attrs={'is_abstract': False, 'inherited': True})

                    schema = self.add_constraint(schema, constraint)

        return schema

    def add_constraint(self, schema, constraint, replace=False):
        return self.add_classref(
            schema, 'constraints', constraint, replace=replace)


class ConsistencySubjectCommandContext:
    # context mixin
    pass


class ConsistencySubjectCommand(inheriting.InheritingObjectCommand):
    pass


class ConstraintCommandContext(sd.ObjectCommandContext,
                               annotations.AnnotationSubjectCommandContext):
    pass


class ConstraintCommand(
        referencing.ReferencedInheritingObjectCommand,
        s_func.CallableCommand,
        schema_metaclass=Constraint, context_class=ConstraintCommandContext,
        referrer_context_class=ConsistencySubjectCommandContext):

    @classmethod
    def _validate_subcommands(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as annotations
        for command in astnode.commands:
            cname = command.name
            if cls._is_special_name(cname):
                raise errors.InvalidConstraintDefinitionError(
                    f'{cname.name} is not a valid constraint annotation',
                    context=command.context)

    @classmethod
    def _is_special_name(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as annotations
        return (astnode.name in {'subject', 'subjectexpr'} and
                not astnode.module)

    def _prepare_create_fields(self, schema, context):
        schema, props = super()._prepare_create_fields(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            # Concrete constraints never declare parameters.
            props.pop('params', None)

        return schema, props

    def compile_expr_field(self, schema, context, field, value):
        from edb.edgeql import compiler as qlcompiler

        if field.name in ('expr', 'subjectexpr'):
            params = self._get_params(schema, context)
            anchors, _ = (
                qlcompiler.get_param_anchors_for_callable(
                    params, schema)
            )
            referrer_ctx = self.get_referrer_context(context)
            if referrer_ctx is not None:
                anchors['__subject__'] = referrer_ctx.op.scls

            return s_expr.Expression.compiled(
                value,
                schema=schema,
                modaliases=context.modaliases,
                anchors=anchors,
                func_params=params,
                allow_generic_type_output=True,
                parent_object_type=self.get_schema_metaclass(),
            )
        else:
            return super().compile_expr_field(schema, context, field, value)


class CreateConstraint(ConstraintCommand,
                       s_func.CreateCallableObject,
                       referencing.CreateReferencedInheritingObject):

    astnode = [qlast.CreateConcreteConstraint, qlast.CreateConstraint]
    referenced_astnode = qlast.CreateConcreteConstraint

    @classmethod
    def _get_param_desc_from_ast(cls, schema, modaliases, astnode, *,
                                 param_offset: int=0):

        if not hasattr(astnode, 'params'):
            # Concrete constraint.
            return []

        params = super()._get_param_desc_from_ast(
            schema, modaliases, astnode, param_offset=param_offset + 1)

        params.insert(0, s_func.ParameterDesc(
            num=param_offset,
            name='__subject__',
            default=None,
            type=s_pseudo.Any.instance,
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
        ))

        return params

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return super()._create_begin(schema, context)

        schema, props = self._get_create_fields(schema, context)

        if props.get('finalexpr'):
            return super()._create_begin(schema, context)

        props.pop('name')
        subject = props.pop('subject')
        fullname = self.classname
        shortname = sn.shortname_from_fullname(fullname)
        schema, self.scls, attrs = Constraint.create_concrete_constraint(
            schema, subject, name=shortname, **props)

        for attr, value in attrs.items():
            self.set_attribute_value(attr, value)

        return schema

    @classmethod
    def _constraint_args_from_ast(cls, schema, astnode, context):
        args = []

        if astnode.args:
            for arg in astnode.args:
                arg_expr = s_expr.Expression.from_ast(
                    arg, schema, context.modaliases)
                args.append(arg_expr)

        return args

    @classmethod
    def _classname_quals_from_ast(cls, schema, astnode, base_name,
                                  referrer_name, context):
        subject = schema.get(referrer_name, None)
        if subject is None:
            return ()

        props = {}
        args = cls._constraint_args_from_ast(schema, astnode, context)
        if args:
            props['args'] = args
        if astnode.subjectexpr:
            props['subjectexpr'] = s_expr.Expression.from_ast(
                astnode.subject, schema, context.modaliases)

        _, attrs = Constraint.get_concrete_constraint_attrs(
            schema, subject, name=base_name,
            sourcectx=astnode.context,
            modaliases=context.modaliases, **props)

        return (Constraint._name_qual_from_expr(
            schema, attrs['finalexpr'].text),)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteConstraint):
            args = cls._constraint_args_from_ast(schema, astnode, context)
            if args:
                cmd.add(
                    sd.AlterObjectProperty(
                        property='args',
                        new_value=args
                    )
                )

        elif isinstance(astnode, qlast.CreateConstraint):
            params = cls._get_param_desc_from_ast(
                schema, context.modaliases, astnode)

            for param in params:
                if param.get_kind(schema) is ft.ParameterKind.NAMED_ONLY:
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
            cmd.add(sd.AlterObjectProperty(
                property='return_type',
                new_value=utils.reduce_to_typeref(
                    schema, schema.get('std::bool')
                )
            ))

        if cmd.get_attribute_value('return_typemod') is None:
            cmd.add(sd.AlterObjectProperty(
                property='return_typemod',
                new_value=ft.TypeModifier.SINGLETON,
            ))

        # 'subjectexpr' can be present in either astnode type
        if astnode.subjectexpr:
            subjectexpr = s_expr.Expression.from_ast(
                astnode.subjectexpr,
                schema,
                context.modaliases,
            )

            cmd.add(sd.AlterObjectProperty(
                property='subjectexpr',
                new_value=subjectexpr
            ))

        cls._validate_subcommands(astnode)

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'is_derived':
            pass
        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'subjectexpr':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _classbases_from_ast(cls, schema, astnode, context):
        if isinstance(astnode, qlast.CreateConcreteConstraint):
            classname = cls._classname_from_ast(schema, astnode, context)
            base_name = sn.shortname_from_fullname(classname)
            base = schema.get(base_name)
            return so.ObjectList.create(
                schema, [utils.reduce_to_typeref(schema, base)])
        else:
            return super()._classbases_from_ast(schema, astnode, context)


class RenameConstraint(ConstraintCommand, sd.RenameObject):
    pass


class AlterConstraint(ConstraintCommand, sd.AlterObject):
    astnode = [qlast.AlterConcreteConstraint, qlast.AlterConstraint]
    referenced_astnode = qlast.AlterConcreteConstraint

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.AlterConcreteConstraint):
            subject_ctx = context.get(ConsistencySubjectCommandContext)
            new_subject_name = None

            for op in subject_ctx.op.get_subcommands(
                    type=sd.RenameObject):
                new_subject_name = op.new_name

            if new_subject_name is not None:
                cmd.add(
                    sd.AlterObjectProperty(
                        property='subject',
                        new_value=so.ObjectRef(
                            classname=new_subject_name
                        )
                    )
                )

            new_name = None
            for op in cmd.get_subcommands(type=RenameConstraint):
                new_name = op.new_name

            if new_name is not None:
                cmd.add(
                    sd.AlterObjectProperty(
                        property='name',
                        new_value=new_name
                    )
                )

        cls._validate_subcommands(astnode)

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'subject':
            return
        super()._apply_field_ast(schema, context, node, op)


class DeleteConstraint(ConstraintCommand, s_func.DeleteCallableObject):
    astnode = [qlast.DropConcreteConstraint, qlast.DropConstraint]
    referenced_astnode = qlast.DropConcreteConstraint
