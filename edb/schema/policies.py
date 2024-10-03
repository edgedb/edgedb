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
from typing import Any, Optional, Type, List, TYPE_CHECKING

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import futures as s_futures
from . import name as sn
from . import objects as so
from . import properties as s_props
from . import referencing
from . import schema as s_schema
from . import sources as s_sources
from . import types as s_types

if TYPE_CHECKING:
    from . import objtypes as s_objtypes


class AccessPolicy(
    referencing.NamedReferencedInheritingObject,
    so.InheritingObject,  # Help reflection figure out the right db MRO
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ACCESS_POLICY,
    data_safe=True,
):

    condition = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    action = so.SchemaField(
        qltypes.AccessPolicyAction,
        coerce=True,
        compcoef=0.85,
        special_ddl_syntax=True,
    )

    access_kinds = so.SchemaField(
        so.MultiPropSet[qltypes.AccessKind],
        coerce=True,
        compcoef=0.85,
        special_ddl_syntax=True,
    )

    subject = so.SchemaField(
        so.InheritingObject,
        compcoef=None,
        inheritable=False)

    errmessage = so.SchemaField(
        str, default=None, compcoef=0.971, allow_ddl_set=True
    )

    # We don't support SET/DROP OWNED owned on policies so we set its
    # compcoef to 0.0
    owned = so.SchemaField(
        bool,
        default=False,
        inheritable=False,
        compcoef=0.0,
        reflection_method=so.ReflectionMethod.AS_LINK,
        special_ddl_syntax=True,
    )

    def get_expr_refs(self, schema: s_schema.Schema) -> List[so.Object]:
        objs: List[so.Object] = []
        if (condition := self.get_condition(schema)) and condition.refs:
            objs.extend(condition.refs.objects(schema))
        if (expr := self.get_expr(schema)) and expr.refs:
            objs.extend(expr.refs.objects(schema))
        return objs

    def get_subject(self, schema: s_schema.Schema) -> s_objtypes.ObjectType:
        subj: s_objtypes.ObjectType = self.get_field_value(schema, 'subject')
        return subj

    def get_original_subject(
        self, schema: s_schema.Schema
    ) -> s_objtypes.ObjectType:
        ancs = (self,) + self.get_ancestors(schema).objects(schema)
        return ancs[-1].get_subject(schema)


class AccessPolicyCommandContext(
    sd.ObjectCommandContext[AccessPolicy],
    s_anno.AnnotationSubjectCommandContext,
):
    pass


class AccessPolicySourceCommandContext(
        s_sources.SourceCommandContext[s_sources.Source_T]):
    pass


class AccessPolicyCommand(
    referencing.NamedReferencedInheritingObjectCommand[AccessPolicy],
    s_anno.AnnotationSubjectCommand[AccessPolicy],
    context_class=AccessPolicyCommandContext,
    referrer_context_class=AccessPolicySourceCommandContext,
):
    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
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

            span = self.get_attribute_span(field)

            if expression.irast.cardinality.can_be_zero():
                raise errors.SchemaDefinitionError(
                    f'possibly an empty set returned by {vname} '
                    f'expression for the {pol_name} ',
                    span=span
                )

            if expression.irast.cardinality.is_multi():
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by {vname} '
                    f'expression for the {pol_name} ',
                    span=span
                )

            if expression.irast.volatility.is_volatile():
                raise errors.SchemaDefinitionError(
                    f'{pol_name} has a volatile {vname} expression, '
                    f'which is not allowed',
                    span=span
                )

            target = schema.get(sn.QualName('std', 'bool'), type=s_types.Type)
            expr_type = expression.irast.stype
            if not expr_type.issubclass(expression.irast.schema, target):
                span = self.get_attribute_span(field)
                raise errors.SchemaDefinitionError(
                    f'{vname} expression for {pol_name} is of invalid type: '
                    f'{expr_type.get_displayname(schema)}, '
                    f'expected {target.get_displayname(schema)}',
                    span=self.span,
                )

        return schema

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
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
                    anchors={'__subject__': source},
                    path_prefix_anchor='__subject__',
                    singletons=frozenset({source}),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=in_ddl_context_name,
                    detached=True,
                ),
                context=context,
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

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        subject = self.scls.get_subject(schema)

        for obj in self.scls.get_expr_refs(schema):
            if isinstance(obj, s_props.Property):
                # Disable access of link properties with default
                # values from all the post-check DML changes. This is
                # because linkprop default values currently come from
                # the postgres side, so we don't have access to them
                # before actually doing the link table inserts.
                # TODO: Fix this.
                if (
                    obj.get_source(schema)
                    and obj.is_link_property(schema)
                    and obj.get_default(schema)
                    and any(
                        kind.is_data_check()
                        for kind in self.scls.get_access_kinds(schema)
                    )
                ):
                    pol_name = self.get_verbosename(
                        parent=subject.get_verbosename(schema))
                    obj_name = obj.get_verbosename(schema, with_parent=True)
                    raise errors.UnsupportedFeatureError(
                        f'insert and update write access policies may not '
                        f'refer to link properties with default values: '
                        f'{pol_name} refers to {obj_name}',
                        span=self.span,
                    )


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
            field in ('expr', 'condition', 'action', 'access_kinds')
            and issubclass(astnode, qlast.CreateAccessPolicy)
        ):
            return field
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
                span=astnode.condition.span,
            )

        if astnode.expr:
            cmd.set_attribute_value(
                'expr',
                s_expr.Expression.from_ast(
                    astnode.expr, schema, context.modaliases,
                    context.localnames,
                ),
                span=astnode.expr.span,
            )

        cmd.set_attribute_value('action', astnode.action)
        cmd.set_attribute_value('access_kinds', astnode.access_kinds)

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

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)

        # If either action or access_kinds appears, make sure the
        # other one does as well, so that _apply_field_ast has
        # a canonical setup to work with.
        if (
            self.has_attribute_value('action')
            and not self.has_attribute_value('access_kinds')
        ):
            self.set_attribute_value(
                'access_kinds', self.scls.get_access_kinds(schema))
        elif (
            self.has_attribute_value('access_kinds')
            and not self.has_attribute_value('action')
        ):
            self.set_attribute_value('action', self.scls.get_action(schema))

        # TODO: We may wish to support this in the future but it will
        # take some thought.
        if (
            self.get_attribute_value('owned')
            and not self.get_orig_attribute_value('owned')
        ):
            raise errors.SchemaDefinitionError(
                f'cannot alter the definition of inherited access policy '
                f'{self.scls.get_displayname(schema)}',
                span=self.span
            )

        return schema

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'action':
            pass
        elif op.property == 'access_kinds':
            node.commands.append(
                qlast.SetAccessPerms(
                    action=self.get_attribute_value('action'),
                    access_kinds=op.new_value,
                )
            )
        else:
            super()._apply_field_ast(schema, context, node, op)


# This is kind of a hack: we never actually instantiate this class, we
# just use its _cmd_tree_from_ast to produce a command group with two
# property sets.
class AlterAccessPolicyPerms(
    referencing.ReferencedInheritingObjectCommand[AccessPolicy],
    referrer_context_class=AccessPolicyCommandContext,
):
    referenced_astnode = astnode = qlast.SetAccessPerms

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.SetAccessPerms)
        cmd = sd.CommandGroup()
        cmd.add(
            sd.AlterObjectProperty(
                property='action',
                new_value=astnode.action,
                span=astnode.span,
            )
        )
        cmd.add(
            sd.AlterObjectProperty(
                property='access_kinds',
                new_value=astnode.access_kinds,
                span=astnode.span,
            )
        )
        return cmd


class DeleteAccessPolicy(
    AccessPolicyCommand,
    referencing.DeleteReferencedInheritingObject[AccessPolicy],
):
    referenced_astnode = astnode = qlast.DropAccessPolicy


@s_futures.register_handler('nonrecursive_access_policies')
def toggle_nonrecursive_access_policies(
    cmd: s_futures.FutureBehaviorCommand,
    schema: s_schema.Schema,
    context: sd.CommandContext,
    on: bool,
) -> tuple[s_schema.Schema, sd.Command]:
    # Nothing to do anymore
    group = sd.CommandGroup()
    return schema, group
