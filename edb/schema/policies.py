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

from edb.common import checked

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import name as sn
from . import objects as so
from . import referencing
from . import schema as s_schema
from . import sources as s_sources
from . import types as s_types


class AccessPolicy(
    referencing.ReferencedInheritingObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ACCESS_POLICY,
    data_safe=True,
):

    condition = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    action = so.SchemaField(
        qltypes.AccessPolicyAction,
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    _access_kinds = so.SchemaField(
        checked.FrozenCheckedList[qltypes.AccessKind],
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    subject = so.SchemaField(
        so.InheritingObject,
        compcoef=None,
        inheritable=False)

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'access policy'

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        sn = cls.get_shortname_static(name)
        if sn.module == '__':
            return sn.name
        else:
            return str(sn)

    def get_derived_name_base(
        self,
        schema: s_schema.Schema,
    ) -> sn.QualName:
        shortname = self.get_shortname(schema)
        return sn.QualName(module='__', name=shortname.name)

    def get_access_kinds(
        self, schema: s_schema.Schema
    ) -> checked.FrozenCheckedList[qltypes.AccessKind]:
        return self.get__access_kinds(schema)


class AccessPolicyCommandContext(
    sd.ObjectCommandContext[AccessPolicy],
    s_anno.AnnotationSubjectCommandContext,
):
    pass


class AccessPolicySourceCommandContext(s_sources.SourceCommandContext):
    pass


class AccessPolicyCommand(
    referencing.ReferencedInheritingObjectCommand[AccessPolicy],
    s_anno.AnnotationSubjectCommand[AccessPolicy],
    context_class=AccessPolicyCommandContext,
    referrer_context_class=AccessPolicySourceCommandContext,
):
    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        from edb.ir import ast as irast

        schema = super().canonicalize_attributes(schema, context)

        parent_ctx = self.get_referrer_context_or_die(context)
        source = parent_ctx.op.scls
        pol_name = self.get_verbosename(parent=source.get_verbosename(schema))

        for field in ('expr', 'condition'):
            if (expr := self.get_local_attribute_value(field)) is None:
                continue

            vname = 'when' if field == 'condition' else 'using'

            expression = self.compile_expr_field(
                schema, context,
                field=AccessPolicy.get_field(field),
                value=expr,
            )
            assert isinstance(expression.irast, irast.Statement)

            zero = expression.irast.cardinality.can_be_zero()
            if zero or expression.irast.cardinality.is_multi():
                srcctx = self.get_attribute_source_context(field)
                if zero:
                    problem = 'an empty set'
                else:
                    problem = 'more than one element'
                raise errors.SchemaDefinitionError(
                    f'possibly {problem} returned by {vname} '
                    f'expression for the {pol_name} ',
                    context=srcctx
                )

            target = schema.get(sn.QualName('std', 'bool'), type=s_types.Type)
            expr_type = expression.irast.stype
            if not expr_type.issubclass(schema, target):
                srcctx = self.get_attribute_source_context(field)
                raise errors.SchemaDefinitionError(
                    f'{vname} expression for {pol_name} is of invalid type: '
                    f'{expr_type.get_displayname(schema)}, '
                    f'expected {target.get_displayname(schema)}',
                    context=srcctx,
                )

        return schema

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:
        if field.name in {'expr', 'condition'}:
            parent_ctx = self.get_referrer_context_or_die(context)
            source = parent_ctx.op.get_object(schema, context)
            parent_vname = source.get_verbosename(schema)
            pol_name = self.get_verbosename(parent=parent_vname)
            in_ddl_context_name = pol_name

            assert isinstance(source, s_types.Type)

            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors={qlast.Subject().name: source},
                    path_prefix_anchor=qlast.Subject().name,
                    singletons=frozenset({source}),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=in_ddl_context_name,
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
        if field.name in {'expr', 'condition'}:
            return s_expr.Expression(text='false')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = context.get_referrer_name(referrer_ctx)

            shortname = sn.QualName(module='__', name=astnode.name.name)

            name = sn.QualName(
                module=referrer_name.module,
                name=sn.get_specialized_name(shortname, str(referrer_name)),
            )
        else:
            name = super()._classname_from_ast(schema, astnode, context)

        return name

    def _deparse_name(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: sn.Name,
    ) -> qlast.ObjectRef:

        ref = super()._deparse_name(schema, context, name)
        ref.module = ''
        return ref


class CreateAccessPolicy(
    AccessPolicyCommand,
    referencing.CreateReferencedInheritingObject[AccessPolicy],
):
    referenced_astnode = astnode = qlast.CreateAccessPolicy

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field in ('expr', 'condition', 'action')
            and issubclass(astnode, qlast.CreateAccessPolicy)
        ):
            return field
        elif (
            field == '_access_kinds'
            and issubclass(astnode, qlast.CreateAccessPolicy)
        ):
            return 'access_kinds'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateAccessPolicy)
        assert isinstance(cmd, AccessPolicyCommand)

        if astnode.condition is not None:
            cmd.set_attribute_value(
                'condition',
                s_expr.Expression.from_ast(
                    astnode.condition, schema, context.modaliases,
                    context.localnames,
                ),
                source_context=astnode.condition.context,
            )

        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases,
                context.localnames,
            ),
            source_context=astnode.expr.context,
        )

        cmd.set_attribute_value('action', astnode.action)
        cmd.set_attribute_value('_access_kinds', astnode.access_kinds)

        return cmd


class RenameAccessPolicy(
    AccessPolicyCommand,
    referencing.RenameReferencedInheritingObject[AccessPolicy],
):
    pass


class RebaseAccessPolicy(
    AccessPolicyCommand,
    referencing.RebaseReferencedInheritingObject[AccessPolicy],
):
    pass


class AlterAccessPolicy(
    AccessPolicyCommand,
    referencing.AlterReferencedInheritingObject[AccessPolicy],
):
    referenced_astnode = astnode = qlast.AlterAccessPolicy


class DeleteAccessPolicy(
    AccessPolicyCommand,
    referencing.DeleteReferencedInheritingObject[AccessPolicy],
):
    referenced_astnode = astnode = qlast.DropAccessPolicy
