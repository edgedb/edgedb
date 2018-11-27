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

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import functypes as ft

from . import abc as s_abc
from . import attributes
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
        s_expr.ExpressionText, default=None, compcoef=0.909,
        coerce=True)

    subjectexpr = so.SchemaField(
        s_expr.ExpressionText,
        default=None, compcoef=0.833, coerce=True)

    finalexpr = so.SchemaField(
        s_expr.ExpressionText,
        default=None, compcoef=0.909, coerce=True)

    subject = so.SchemaField(
        so.Object, default=None, inheritable=False)

    args = so.SchemaField(
        s_expr.ExpressionList,
        default=None, coerce=True, inheritable=False,
        compcoef=0.875)

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True)

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

        return (self._name_qual_from_expr(schema, finalexpr),)

    @classmethod
    def _name_qual_from_expr(self, schema, expr):
        m = hashlib.sha1()
        m.update(expr.encode())
        return m.hexdigest()

    @classmethod
    def _dummy_subject(cls, schema):
        # Point subject placeholder to a dummy pointer to make EdgeQL
        # pipeline happy.
        return s_pseudo.Any.create()

    @classmethod
    def _normalize_constraint_expr(
            cls, schema, module_aliases, expr, subject, *,
            inline_anchors=False):
        from edb.lang.edgeql import parser as edgeql_parser
        from edb.lang.edgeql import utils as edgeql_utils

        if isinstance(expr, str):
            tree = edgeql_parser.parse(expr, module_aliases)
        else:
            tree = expr

        ir, edgeql_tree, _ = edgeql_utils.normalize_tree(
            tree, schema, modaliases=module_aliases,
            anchors={qlast.Subject: subject}, inline_anchors=inline_anchors)

        return edgeql_tree.result, ir

    @classmethod
    def normalize_constraint_expr(
            cls, schema, module_aliases, expr, *,
            subject=None, constraint_name, expr_context=None,
            enforce_boolean=False):

        if subject is None:
            subject = cls._dummy_subject(schema)

        edgeql_tree, ir_result = cls._normalize_constraint_expr(
            schema, module_aliases, expr, subject)

        if enforce_boolean:
            bool_t = schema.get('std::bool')
            expr_type = ir_result.stype
            if not expr_type.issubclass(schema, bool_t):
                raise errors.InvalidConstraintDefinitionError(
                    f'{constraint_name} constraint expression expected '
                    f'to return a bool value, got '
                    f'{expr_type.get_name(schema).name!r}',
                    context=expr_context
                )

        expr = edgeql.generate_source(edgeql_tree, pretty=False)
        return expr

    @classmethod
    def get_concrete_constraint_attrs(
            cls, schema, subject, *, name, subjectexpr=None,
            sourcectx=None, args=[], modaliases=None, **kwargs):
        from edb.lang.edgeql import utils as edgeql_utils
        from edb.lang.edgeql import parser as edgeql_parser

        constr_base = schema.get(name, module_aliases=modaliases)
        module_aliases = {}

        orig_subject = subject
        base_subjectexpr = constr_base.get_field_value(schema, 'subjectexpr')
        if subjectexpr is None:
            subjectexpr = base_subjectexpr
        elif base_subjectexpr is not None and subjectexpr != base_subjectexpr:
            raise errors.InvalidConstraintDefinitionError(
                'subjectexpr is already defined for ' +
                f'{str(name)!r}')

        if subjectexpr is not None:
            subject, _ = cls._normalize_constraint_expr(
                schema, {}, subjectexpr, subject)

        expr = constr_base.get_field_value(schema, 'expr')
        if not expr:
            raise errors.InvalidConstraintDefinitionError(
                f'missing constraint expression in {name!r}')

        expr_ql = edgeql_parser.parse(expr, module_aliases)

        if not args:
            args = constr_base.get_field_value(schema, 'args')

        attrs = dict(kwargs)

        args_map = None
        if args:
            args_ql = [
                edgeql_parser.parse(arg, module_aliases) for arg in args
            ]

            args_map = edgeql_utils.index_parameters(
                args_ql,
                parameters=constr_base.get_params(schema),
                schema=schema)

            edgeql_utils.inline_parameters(expr_ql, args_map)

            args_map = {name: edgeql.generate_source(val, pretty=False)
                        for name, val in args_map.items()}

            errmessage = attrs.get('errmessage')
            if not errmessage:
                errmessage = constr_base.get_errmessage(schema)

            attrs['errmessage'] = errmessage.format(
                __subject__='{__subject__}', **args_map)

            args = list(args_map.values())

        attrs['args'] = args

        if expr == '__subject__':
            expr_context = sourcectx
        else:
            expr_context = None

        if subject is not orig_subject:
            # subject has been redefined
            subject_anchor = qlast.SubExpr(
                expr=subject,
                anchors={
                    qlast.Subject: orig_subject
                }
            )
        else:
            subject_anchor = subject

        expr_text = cls.normalize_constraint_expr(
            schema, module_aliases, expr_ql,
            subject=subject_anchor,
            constraint_name=name,
            enforce_boolean=True,
            expr_context=expr_context)

        attrs['finalexpr'] = expr_text

        return constr_base, attrs

    @classmethod
    def create_concrete_constraint(
            cls, schema, subject, *, name, subjectexpr=None,
            sourcectx=None, args=[], modaliases=None, **kwargs):
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
        titleattr = subject.get_attribute(schema, 'std::title')

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
            generic = next(iter(item.get_bases(schema).objects(schema)))
            schema, item = generic.derive(
                schema, source=source,
                merge_bases=[item], dctx=dctx)

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
                        schema, self, attrs={'is_abstract': False})

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
                               attributes.AttributeSubjectCommandContext):
    pass


class ConstraintCommand(
        referencing.ReferencedInheritingObjectCommand,
        s_func.CallableCommand,
        schema_metaclass=Constraint, context_class=ConstraintCommandContext,
        referrer_context_class=ConsistencySubjectCommandContext):

    @classmethod
    def _validate_subcommands(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as attributes
        for command in astnode.commands:
            if cls._is_special_name(command.name):
                raise errors.InvalidConstraintDefinitionError(
                    f'{command.name.name} is not a valid constraint attribute',
                    context=command.context)

    @classmethod
    def _is_special_name(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as attributes
        return (astnode.name in {'subject', 'subjectexpr'} and
                not astnode.module)

    def _make_constructor_args(self, schema, context):
        schema, props = super()._make_constructor_args(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            # Concrete constraints never declare parameters.
            props.pop('params', None)

        return schema, props


class CreateConstraint(ConstraintCommand,
                       s_func.CreateCallableObject,
                       referencing.CreateReferencedInheritingObject):

    astnode = [qlast.CreateConcreteConstraint, qlast.CreateConstraint]
    referenced_astnode = qlast.CreateConcreteConstraint

    def _create_begin(self, schema, context):
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            # Abstract constraint
            return super()._create_begin(schema, context)

        schema, props = self._make_constructor_args(schema, context)

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
    def _constraint_args_from_ast(cls, schema, astnode):
        args = []

        if astnode.args:
            for arg in astnode.args:
                arg_expr = s_expr.ExpressionText(
                    edgeql.generate_source(arg.arg, pretty=False))
                args.append(arg_expr)

        return args

    @classmethod
    def _classname_quals_from_ast(cls, schema, astnode, base_name,
                                  referrer_name, context):
        subject = schema.get(referrer_name, None)
        if subject is None:
            return ()

        props = {}
        args = cls._constraint_args_from_ast(schema, astnode)
        if args:
            props['args'] = args
        if astnode.subject:
            props['subjectexpr'] = s_expr.ExpressionText(
                edgeql.generate_source(astnode.subject, pretty=False))

        _, attrs = Constraint.get_concrete_constraint_attrs(
            schema, subject, name=base_name,
            sourcectx=astnode.context,
            modaliases=context.modaliases, **props)

        return (Constraint._name_qual_from_expr(schema, attrs['finalexpr']),)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteConstraint):
            args = cls._constraint_args_from_ast(schema, astnode)
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

        # 'subject' can be present in either astnode type
        if astnode.subject:
            subjectexpr = s_expr.ExpressionText(
                edgeql.generate_source(astnode.subject, pretty=False))

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
        elif op.property == 'subject':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)


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
