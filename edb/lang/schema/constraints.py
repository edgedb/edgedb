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


import itertools

from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors
from edb.lang.edgeql import functypes as ft

from . import delta as sd
from . import error as s_errors
from . import expr as s_expr
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import referencing
from . import utils


class CumulativeBoolExpr(s_expr.ExpressionText):
    @classmethod
    def merge_values(cls, target, sources, field_name, *, schema):
        def join(values):
            # Make the list unique without losing the order.
            values = list(dict.fromkeys(values).keys())
            if not values:
                return None
            elif len(values) == 1:
                return values[0]
            else:
                return ' and '.join(f'({v})' for v in values)

        return utils.merge_reduce(target, sources, field_name,
                                  schema=schema, f=join)


class Constraint(inheriting.InheritingObject, s_func.CallableObject):
    _type = 'constraint'

    expr = so.SchemaField(
        s_expr.ExpressionText, default=None, compcoef=0.909,
        coerce=True)

    subjectexpr = so.SchemaField(
        s_expr.ExpressionText,
        default=None, compcoef=0.833, coerce=True)

    localfinalexpr = so.SchemaField(
        CumulativeBoolExpr,
        default=None, coerce=True, hashable=False, inheritable=False,
        introspectable=False)

    finalexpr = so.SchemaField(
        CumulativeBoolExpr,
        default=None, coerce=True, hashable=False, compcoef=0.909)

    subject = so.SchemaField(
        so.Object, default=None, inheritable=False)

    args = so.SchemaField(
        s_expr.ExpressionList,
        default=None, coerce=True, inheritable=False,
        compcoef=0.875)

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971)

    def generic(self, schema):
        return self.get_subject(schema) is None

    def merge_localexprs(self, obj, schema):
        schema = self.set_field_value(
            schema,
            'localfinalexpr',
            CumulativeBoolExpr.merge_values(
                self, [obj], 'localfinalexpr', schema=schema))
        return schema

    def init_derived(self, schema, source, *qualifiers,
                     as_copy, mark_derived=False, add_to_schema=False,
                     merge_bases=None, attrs=None,
                     dctx=None, **kwargs):

        if attrs is None:
            attrs = {}

        attrs['subject'] = source

        return super().init_derived(
            schema, source, *qualifiers, as_copy=as_copy,
            mark_derived=mark_derived, add_to_schema=add_to_schema,
            merge_bases=merge_bases, attrs=attrs, dctx=dctx, **kwargs)

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

    @classmethod
    def _dummy_subject(cls):
        from . import scalars as s_scalars

        # Point subject placeholder to a dummy pointer to make EdgeQL
        # pipeline happy.
        return s_scalars.ScalarType(name=sn.Name('std::_subject_tgt'))

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

        return edgeql_tree.result, ir.expr.expr.result

    @classmethod
    def normalize_constraint_expr(
            cls, schema, module_aliases, expr, *,
            subject=None, constraint, expr_context=None,
            enforce_boolean=False):
        from edb.lang.ir import utils as irutils

        if subject is None:
            subject = cls._dummy_subject()

        edgeql_tree, ir_result = cls._normalize_constraint_expr(
            schema, module_aliases, expr, subject)

        if enforce_boolean:
            bool_t = schema.get('std::bool')
            expr_type = irutils.infer_type(ir_result, schema)
            if not expr_type.issubclass(schema, bool_t):
                raise s_errors.SchemaDefinitionError(
                    f'{constraint.displayname} constraint expression expected '
                    f'to return a bool value, got {expr_type.name.name!r}',
                    context=expr_context
                )

        expr = edgeql.generate_source(edgeql_tree, pretty=False)
        return expr

    @classmethod
    def process_specialized_constraint(cls, schema, constraint, params=None):
        from edb.lang.edgeql import utils as edgeql_utils
        from edb.lang.edgeql import parser as edgeql_parser

        subject = constraint.get_subject(schema)
        assert subject is not None

        module_aliases = {}

        subjectexpr = constraint.get_field_value(schema, 'subjectexpr')

        # check to make sure that the specialized constraint doesn't redefine
        # an already defined subjectexpr
        if subjectexpr is not None:
            for base in constraint.bases:
                base_se = base.get_field_value(schema, 'subjectexpr')
                if base_se and base_se != subjectexpr:
                    raise s_errors.InvalidConstraintDefinitionError(
                        'subjectexpr is already defined for ' +
                        f'{constraint.name!r}')

            subject, _ = cls._normalize_constraint_expr(
                schema, {}, subjectexpr, subject)

        expr = constraint.get_field_value(schema, 'expr')
        if not expr:
            raise s_errors.InvalidConstraintDefinitionError(
                f'missing constraint expression in {constraint.name!r}')

        expr_ql = edgeql_parser.parse(expr, module_aliases)

        if params:
            args = params
        else:
            args = constraint.get_field_value(schema, 'args')

        args_map = None
        if args:
            args_ql = [
                edgeql_parser.parse(arg, module_aliases) for arg in args
            ]

            args_map = edgeql_utils.index_parameters(
                args_ql,
                parameters=constraint.get_params(schema),
                schema=schema)

            edgeql_utils.inline_parameters(expr_ql, args_map)

            args_map = {name: edgeql.generate_source(val, pretty=False)
                        for name, val in args_map.items()}

            schema = constraint.set_field_value(
                schema,
                'errmessage',
                constraint.get_errmessage(schema).format(
                    __subject__='{__subject__}', **args_map))

            args = list(args_map.values())

        if expr == '__subject__':
            expr_context = \
                constraint.get_attribute_source_context(schema, 'subjectexpr')
        else:
            expr_context = \
                constraint.get_attribute_source_context(schema, 'expr')

        if subject is not constraint.get_subject(schema):
            # subject has been redefined
            subject_anchor = qlast.SubExpr(
                expr=subject,
                anchors={
                    qlast.Subject: constraint.get_subject(schema)
                }
            )
        else:
            subject_anchor = subject

        expr_text = cls.normalize_constraint_expr(
            schema, module_aliases, expr_ql,
            subject=subject_anchor,
            constraint=constraint,
            enforce_boolean=True,
            expr_context=expr_context)

        schema = constraint.update(schema, {
            'expr': expr_text,
            'localfinalexpr': expr_text,
            'finalexpr': expr_text,
            'args': args or None,
        })

        return schema

    def format_error_message(self, schema):
        errmsg = self.get_errmessage(schema)
        subject = self.get_subject(schema)
        subjtitle = subject.title

        if not subjtitle:
            try:
                subjname = subject.shortname
            except AttributeError:
                subjname = subject.name

            subjtitle = subjname.name

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


class ConsistencySubject(referencing.ReferencingObject):
    constraints_refs = referencing.RefDict(
        attr='constraints',
        local_attr='own_constraints',
        ref_cls=Constraint)

    constraints = so.SchemaField(
        so.ObjectMapping,
        inheritable=False, ephemeral=True, coerce=True,
        default=so.ObjectMapping, hashable=False)

    own_constraints = so.SchemaField(
        so.ObjectMapping, compcoef=0.887,
        inheritable=False, ephemeral=True,
        coerce=True,
        default=so.ObjectMapping)

    @classmethod
    def inherit_pure(cls, schema, item, source, *, dctx=None):
        schema, item = super().inherit_pure(schema, item, source, dctx=dctx)

        item_constraints = item.get_constraints(schema)
        if any(c.is_abstract for c in item_constraints.objects(schema)):
            # Have abstract constraints, cannot go pure inheritance,
            # must create a derived Object with materialized
            # constraints.
            generic = item.bases[0]
            schema, item = generic.derive(
                schema, source=source, add_to_schema=True,
                merge_bases=[item], dctx=dctx)

        return schema, item

    def begin_classref_dict_merge(self, schema, bases, attr):
        if attr == 'constraints':
            # Make sure abstract constraints from parents are mixed in
            # properly.
            constraints = set(self.get_constraints(schema).names(schema))
            inherited = itertools.chain.from_iterable(
                b.get_constraints(schema).objects(schema) for b in bases)
            constraints.update(c.shortname
                               for c in inherited if c.is_abstract)
            return schema, constraints
        else:
            return super().begin_classref_dict_merge(schema, bases, attr)

    def finish_classref_dict_merge(self, schema, bases, attr):
        schema = super().finish_classref_dict_merge(schema, bases, attr)

        if attr == 'constraints':
            # Materialize unmerged abstract constraints
            for cn, constraint in self.get_constraints(schema).items(schema):
                if (constraint.is_abstract and
                        not self.get_own_constraints(schema).has(schema, cn)):
                    schema, constraint = constraint.derive_copy(
                        schema, self, add_to_schema=True,
                        attrs=dict(is_abstract=False))

                    schema = self.add_constraint(schema, constraint)

        return schema

    def add_constraint(self, schema, constraint, replace=False):
        return self.add_classref(
            schema, 'constraints', constraint, replace=replace)


class ConsistencySubjectCommandContext:
    # context mixin
    pass


class ConsistencySubjectCommand(referencing.ReferencingObjectCommand):
    pass


class ConstraintCommandContext(sd.ObjectCommandContext):
    pass


class ConstraintCommand(
        referencing.ReferencedInheritingObjectCommand,
        s_func.CallableCommand,
        schema_metaclass=Constraint, context_class=ConstraintCommandContext,
        referrer_context_class=ConsistencySubjectCommandContext):

    def _create_begin(self, schema, context):
        schema = super()._create_begin(schema, context)

        referrer_ctx = self.get_referrer_context(context)
        if (referrer_ctx is not None and
                self.scls.get_finalexpr(schema) is None):
            schema = Constraint.process_specialized_constraint(
                schema, self.scls)

        return schema

    @classmethod
    def _validate_subcommands(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as attributes
        for command in astnode.commands:
            if cls._is_special_name(command.name):
                raise s_errors.SchemaDefinitionError(
                    f'{command.name.name} is not a valid constraint attribute',
                    context=command.context)

    @classmethod
    def _is_special_name(cls, astnode):
        # check that 'subject' and 'subjectexpr' are not set as attributes
        return (astnode.name in {'subject', 'subjectexpr'} and
                not astnode.module)


class CreateConstraint(ConstraintCommand,
                       s_func.CreateCallableObject,
                       referencing.CreateReferencedInheritingObject):

    astnode = [qlast.CreateConcreteConstraint, qlast.CreateConstraint]
    referenced_astnode = qlast.CreateConcreteConstraint

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteConstraint):
            if astnode.args:
                args = []

                for arg in astnode.args:
                    arg_expr = s_expr.ExpressionText(
                        edgeql.generate_source(arg.arg, pretty=False))
                    args.append(arg_expr)

                cmd.add(
                    sd.AlterObjectProperty(
                        property='args',
                        new_value=args
                    )
                )

        elif isinstance(astnode, qlast.CreateConstraint):
            params = s_func.FuncParameterList.from_ast(
                schema, astnode, context.modaliases,
                func_fqname=cmd.classname)

            for param in params:
                if param.get_kind(schema) is ft.ParameterKind.NAMED_ONLY:
                    raise ql_errors.EdgeQLError(
                        'named only parameters are not allowed '
                        'in this context',
                        context=astnode.context)

                if param.get_default(schema) is not None:
                    raise ql_errors.EdgeQLError(
                        'constraints do not support parameters '
                        'with defaults',
                        context=astnode.context)

        if cmd.get_attribute_value('return_type') is None:
            cmd.add(sd.AlterObjectProperty(
                property='return_type',
                new_value=so.ObjectRef(
                    classname=sn.Name('std::bool'),
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


class RenameConstraint(ConstraintCommand, named.RenameNamedObject):
    pass


class AlterConstraint(ConstraintCommand, named.AlterNamedObject):
    astnode = [qlast.AlterConcreteConstraint, qlast.AlterConstraint]
    referenced_astnode = qlast.AlterConcreteConstraint

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.AlterConcreteConstraint):
            subject_ctx = context.get(ConsistencySubjectCommandContext)
            new_subject_name = None

            for op in subject_ctx.op.get_subcommands(
                    type=named.RenameNamedObject):
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
