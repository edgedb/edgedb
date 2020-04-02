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

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import objects as so
from . import pseudo as s_pseudo
from . import referencing
from . import utils


if TYPE_CHECKING:
    from edb.common import parsing as c_parsing
    from edb.schema import scalars as s_scalars
    from edb.schema import schema as s_schema


T = TypeVar('T')


def _assert_not_none(value: Optional[T]) -> T:
    if value is None:
        raise TypeError("A value is expected")
    return value


class Constraint(referencing.ReferencedInheritingObject,
                 s_func.CallableObject, s_abc.Constraint,
                 qlkind=ft.SchemaObjectClass.CONSTRAINT):

    expr = so.SchemaField(
        s_expr.Expression, default=None, compcoef=0.909,
        coerce=True)

    orig_expr = so.SchemaField(
        str, default=None, coerce=True, allow_ddl_set=True,
        ephemeral=True,
    )

    subjectexpr = so.SchemaField(
        s_expr.Expression,
        default=None, compcoef=0.833, coerce=True)

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
        compcoef=0.875)

    delegated = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.9,
    )

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True)

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

    @classmethod
    def _dummy_subject(
        cls,
        schema: s_schema.Schema,
    ) -> Optional[s_pseudo.PseudoType]:
        # Point subject placeholder to a dummy pointer to make EdgeQL
        # pipeline happy.
        return s_pseudo.PseudoType.get(schema, 'anytype')

    @classmethod
    def get_concrete_constraint_attrs(
        cls,
        schema: s_schema.Schema,
        subject: Optional[so.Object],
        *,
        name: str,
        subjectexpr: Optional[s_expr.Expression] = None,
        sourcectx: Optional[c_parsing.ParserContext] = None,
        args: Any = None,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        **kwargs: Any
    ) -> Tuple[Any, Dict[str, Any], Dict[str, bool]]:
        # constr_base, attrs, inherited
        from edb.edgeql import parser as qlparser
        from edb.edgeql import utils as qlutils
        from edb.ir import ast as ir_ast

        constr_base: Constraint = schema.get(name, module_aliases=modaliases)

        module_aliases: Mapping[Optional[str], str] = {}

        orig_subjectexpr = subjectexpr
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
            args_map = None
            args_ql: List[qlast.Base] = [
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

        attrs['args'] = args

        if expr == '__subject__':
            expr_context = sourcectx
        else:
            expr_context = None

        assert subject is not None
        final_expr = s_expr.Expression.compiled(
            s_expr.Expression.from_ast(expr_ql, schema, module_aliases),
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=module_aliases,
                anchors={qlast.Subject().name: subject},
            ),
        )

        bool_t: s_scalars.ScalarType = schema.get('std::bool')
        assert isinstance(final_expr.irast, ir_ast.Statement)

        expr_type = final_expr.irast.stype
        if not expr_type.issubclass(schema, bool_t):
            raise errors.InvalidConstraintDefinitionError(
                f'{name} constraint expression expected '
                f'to return a bool value, got '
                f'{expr_type.get_verbosename(schema)}',
                context=expr_context
            )

        attrs['return_type'] = constr_base.get_return_type(schema)
        attrs['return_typemod'] = constr_base.get_return_typemod(schema)
        attrs['finalexpr'] = final_expr
        attrs['params'] = constr_base.get_params(schema)
        attrs['is_abstract'] = False

        return constr_base, attrs, inherited

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
            from edb.edgeql import parser as qlparser
            from edb.edgeql import utils as qlutils

            args_ql: List[qlast.Base] = [
                qlast.Path(steps=[qlast.ObjectRef(name=subjtitle)]),
            ]

            args_ql.extend(
                qlparser.parse(arg.text) for arg in args
            )

            constr_base: Constraint = schema.get(self.get_name(schema))

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

    @classmethod
    def delta_properties(
        cls,
        delta: sd.ObjectCommand[so.Object],
        old: Optional[so.Object],
        new: so.Object,
        *,
        context: so.ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> None:
        super().delta_properties(
            delta, old, new, context=context,
            old_schema=old_schema, new_schema=new_schema)

        if new is not None:
            assert isinstance(new, Constraint)

            if new.get_subject(new_schema) is not None:
                new_params = new.get_params(new_schema)

                if old is not None:
                    assert isinstance(old, Constraint)
                    assert old_schema is not None

                if old is None or new_params != old.get_params(
                    _assert_not_none(old_schema)
                ):
                    delta.set_attribute_value(
                        'params',
                        new_params,
                        inherited=True,
                    )

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
    s_func.CallableCommand,
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
            assert isinstance(command, (qlast.NamedDDL, qlast.BaseSetField))
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

        subjexpr_text = cls.get_orig_expr_text(schema, astnode, 'subjectexpr')

        assert isinstance(astnode, qlast.ConstraintOp)
        if subjexpr_text is None and astnode.subjectexpr:
            # if not, then use the origtext directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.subjectexpr, schema, context.modaliases)
            subjexpr_text = expr.origtext

        if subjexpr_text:
            exprs.append(subjexpr_text)

        return (cls._name_qual_from_exprs(schema, exprs),)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.SchemaName
    ) -> Tuple[str]:
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

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
    ) -> s_expr.Expression:

        if field.name in ('expr', 'subjectexpr'):
            if not isinstance(self, CreateConstraint):
                raise TypeError("ALTER-ing constraint expressions "
                                "is not supported")
            params = self._get_params(schema, context)

            anchors: Dict[str, Any] = {}
            param_anchors = s_func.get_params_symtable(
                params,
                schema,
                inlined_defaults=False,
            )
            anchors.update(param_anchors)
            referrer_ctx = self.get_referrer_context(context)

            if referrer_ctx is not None:
                assert isinstance(referrer_ctx.op, sd.ObjectCommand)
                anchors['__subject__'] = referrer_ctx.op.scls

            return s_expr.Expression.compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    anchors=anchors,
                    func_params=params,
                    allow_generic_type_output=True,
                    schema_object_context=self.get_schema_metaclass(),
                ),
            )
        else:
            return super().compile_expr_field(schema, context, field, value)

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


class CreateConstraint(
    ConstraintCommand,
    s_func.CreateCallableObject,
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
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
        ))

        return params

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is None:
            return super()._create_begin(schema, context)

        subject = referrer_ctx.scls
        assert isinstance(subject, ConsistencySubject)
        if not subject.can_accept_constraints(schema):
            raise errors.UnsupportedFeatureError(
                f'constraints cannot be defined on '
                f'{subject.get_verbosename(schema)}',
                context=self.source_context,
            )

        if not context.canonical:
            props = self.get_resolved_attributes(schema, context)
            props.pop('name')
            props.pop('subject', None)
            fullname = self.classname
            shortname = sn.shortname_from_fullname(fullname)
            constr_base, attrs, inh = Constraint.get_concrete_constraint_attrs(
                schema,
                subject,
                name=shortname,
                sourcectx=self.source_context,
                **props)

            for k, v in attrs.items():
                inherited = inh.get(k)
                self.set_attribute_value(k, v, inherited=bool(inherited))

            self.set_attribute_value('subject', subject)

        return super()._create_begin(schema, context)

    @classmethod
    def as_inherited_ref_cmd(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        astnode: qlast.ObjectDDL,
        parents: Any,
    ) -> sd.Command:
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
            cmd.set_attribute_value(
                'return_type',
                schema.get('std::bool'),
            )

        if cmd.get_attribute_value('return_typemod') is None:
            cmd.set_attribute_value(
                'return_typemod',
                ft.TypeModifier.SINGLETON,
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
                num = props['num']
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

    def get_ast_attr_for_field(self, field: str) -> Optional[str]:
        if field == 'subjectexpr':
            return 'subjectexpr'
        else:
            return None

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
    ) -> so.ObjectList[Constraint]:
        if isinstance(astnode, qlast.CreateConcreteConstraint):
            classname = cls._classname_from_ast(schema, astnode, context)
            base_name = sn.shortname_from_fullname(classname)
            base = schema.get(base_name, type=Constraint)
            return so.ObjectList.create(schema, [base])
        else:
            return super()._classbases_from_ast(schema, astnode, context)


class RenameConstraint(ConstraintCommand, sd.RenameObject):
    pass


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


class DeleteConstraint(
    ConstraintCommand,
    referencing.DeleteReferencedInheritingObject[Constraint],
    s_func.DeleteCallableObject,
):
    astnode = [qlast.DropConcreteConstraint, qlast.DropConstraint]
    referenced_astnode = qlast.DropConcreteConstraint


class RebaseConstraint(
    ConstraintCommand,
    referencing.RebaseReferencedInheritingObject[Constraint],
):
    pass
