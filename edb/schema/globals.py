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
from typing import Any, Optional, Type, TYPE_CHECKING

from edb import errors

from edb.common import struct

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import expraliases as s_expraliases
from . import name as sn
from . import objects as so
from . import types as s_types
from . import utils

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class Global(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.GLOBAL,
    data_safe=True,
):

    target = so.SchemaField(
        s_types.Type,
        compcoef=0.85,
        special_ddl_syntax=True,
    )

    required = so.SchemaField(
        bool,
        default=False,
        compcoef=0.909,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    cardinality = so.SchemaField(
        qltypes.SchemaCardinality,
        default=qltypes.SchemaCardinality.One,
        compcoef=0.833,
        coerce=True,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    # Computable globals have this set to an expression
    # defining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        default=None,
        coerce=True,
        compcoef=0.909,
    )

    created_types = so.SchemaField(
        so.ObjectSet[s_types.Type],
        default=so.DEFAULT_CONSTRUCTOR,
    )

    def is_computable(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_expr(schema))

    def needs_present_arg(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_default(schema)) and not self.get_required(schema)


class GlobalCommandContext(
    sd.ObjectCommandContext[so.Object],
    s_anno.AnnotationSubjectCommandContext
):
    pass


class GlobalCommand(
    s_expraliases.AliasLikeCommand[Global],
    context_class=GlobalCommandContext,
):
    TYPE_FIELD_NAME = 'target'
    ALIAS_LIKE_EXPR_FIELDS = ('expr', 'default')

    @classmethod
    def _get_alias_name(cls, type_name: sn.QualName) -> sn.QualName:
        return cls._mangle_name(type_name, include_module_in_name=False)

    @classmethod
    def _is_computable(cls, obj: Global, schema: s_schema.Schema) -> bool:
        return obj.is_computable(schema)

    def _check_expr(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        expression = self.get_attribute_value('expr')
        assert isinstance(expression, s_expr.Expression)
        # If it's not compiled, don't worry about it. This should just
        # be a dummy expression.
        if not expression.irast:
            return schema

        required, card = expression.irast.cardinality.to_schema_value()

        spec_required: Optional[bool] = (
            self.get_specified_attribute_value('required', schema, context))
        spec_card: Optional[qltypes.SchemaCardinality] = (
            self.get_specified_attribute_value('cardinality', schema, context))

        glob_name = self.get_verbosename()

        if spec_required and not required:
            span = self.get_attribute_span('target')
            raise errors.SchemaDefinitionError(
                f'possibly an empty set returned by an '
                f'expression for the computed '
                f'{glob_name} '
                f"explicitly declared as 'required'",
                span=span
            )

        if (
            spec_card is qltypes.SchemaCardinality.One
            and card is not qltypes.SchemaCardinality.One
        ):
            span = self.get_attribute_span('target')
            raise errors.SchemaDefinitionError(
                f'possibly more than one element returned by an '
                f'expression for the computed '
                f'{glob_name} '
                f"explicitly declared as 'single'",
                span=span
            )

        if spec_card is None:
            self.set_attribute_value('cardinality', card, computed=True)

        if spec_required is None:
            self.set_attribute_value('required', required, computed=True)

        return schema

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)

        if self.get_attribute_value('expr'):
            schema = self._check_expr(schema, context)

        schema = s_types.materialize_type_in_attribute(
            schema, context, self, 'target')

        return schema

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        scls = self.scls
        is_computable = scls.is_computable(schema)

        target = scls.get_target(schema)

        if not is_computable:
            if (
                scls.get_required(schema)
                and not scls.get_default(schema)
            ):
                raise errors.SchemaDefinitionError(
                    "required globals must have a default",
                    span=self.span,
                )
            if scls.get_cardinality(schema) == qltypes.SchemaCardinality.Many:
                raise errors.SchemaDefinitionError(
                    "non-computed globals may not be multi",
                    span=self.span,
                )
            if target.contains_object(schema):
                raise errors.SchemaDefinitionError(
                    "non-computed globals may not have have object type",
                    span=self.span,
                )

        default_expr = scls.get_default(schema)

        if default_expr is not None:
            default_expr = default_expr.ensure_compiled(schema, context=context)

            default_schema = default_expr.irast.schema
            default_type = default_expr.irast.stype

            span = self.get_attribute_span('default')

            if is_computable:
                raise errors.SchemaDefinitionError(
                    f'computed globals may not have default values',
                    span=span,
                )

            if not default_type.assignment_castable_to(target, default_schema):
                raise errors.SchemaDefinitionError(
                    f'default expression is of invalid type: '
                    f'{default_type.get_displayname(default_schema)}, '
                    f'expected {target.get_displayname(schema)}',
                    span=span,
                )

            ptr_cardinality = scls.get_cardinality(schema)
            default_required, default_cardinality = \
                default_expr.irast.cardinality.to_schema_value()

            if (ptr_cardinality is qltypes.SchemaCardinality.One
                    and default_cardinality != ptr_cardinality):
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by '
                    f'the default expression for '
                    f'{scls.get_verbosename(schema)} declared as '
                    f"'single'",
                    span=span,
                )

            if scls.get_required(schema) and not default_required:
                raise errors.SchemaDefinitionError(
                    f'possibly no elements returned by '
                    f'the default expression for '
                    f'{scls.get_verbosename(schema)} declared as '
                    f"'required'",
                    span=span,
                )

            if default_expr.irast.volatility.is_volatile():
                raise errors.SchemaDefinitionError(
                    f'{scls.get_verbosename(schema)} has a volatile '
                    f'default expression, which is not allowed',
                    span=span,
                )

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        if field.name in {'default', 'expr'}:
            ptr_name = self.get_verbosename()
            in_ddl_context_name = None
            if field.name == 'expr':
                in_ddl_context_name = f'computed {ptr_name}'

            return value.compiled(
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=in_ddl_context_name,
                ),
                context=context,
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs)


class CreateGlobal(
    s_expraliases.CreateAliasLike[Global],
    GlobalCommand,
):
    astnode = qlast.CreateGlobal

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field == 'required'
            and issubclass(astnode, qlast.CreateGlobal)
        ):
            return 'is_required'
        elif (
            field == 'cardinality'
            and issubclass(astnode, qlast.CreateGlobal)
        ):
            return 'cardinality'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateGlobal)
        if op.property == 'target':
            if not node.target:
                expr: Optional[s_expr.Expression] = (
                    self.get_attribute_value('expr')
                )
                if expr is not None:
                    node.target = expr.parse()
                else:
                    t = op.new_value
                    assert isinstance(t, (so.Object, so.ObjectShell))
                    node.target = utils.typeref_to_ast(schema, t)
        else:
            super()._apply_field_ast(schema, context, node, op)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateGlobal)
        assert isinstance(cmd, GlobalCommand)

        if astnode.is_required is not None:
            cmd.set_attribute_value(
                'required',
                astnode.is_required,
                span=astnode.span,
            )

        if astnode.cardinality is not None:
            cmd.set_attribute_value(
                'cardinality',
                astnode.cardinality,
                span=astnode.span,
            )

        assert astnode.target is not None

        if isinstance(astnode.target, qlast.TypeExpr):
            type_ref = utils.ast_to_type_shell(
                astnode.target,
                metaclass=s_types.Type,
                modaliases=context.modaliases,
                schema=schema,
            )
            cmd.set_attribute_value(
                'target',
                type_ref,
                span=astnode.target.span,
            )

        else:
            # computable
            qlcompiler.normalize(
                astnode.target,
                schema=schema,
                modaliases=context.modaliases
            )
            cmd.set_attribute_value(
                'expr',
                s_expr.Expression.from_ast(
                    astnode.target, schema, context.modaliases,
                    context.localnames,
                ),
            )

        if (
            cmd.has_attribute_value('expr')
            and cmd.has_attribute_value('target')
        ):
            raise errors.UnsupportedFeatureError(
                "cannot specify a type and an expression for a global",
                span=astnode.span,
            )

        return cmd


class RenameGlobal(
    s_expraliases.RenameAliasLike[Global],
    GlobalCommand,
):
    pass


class AlterGlobal(
    s_expraliases.AlterAliasLike[Global],
    GlobalCommand,
):
    astnode = qlast.AlterGlobal

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            old_expr = self.scls.get_expr(schema)
            has_expr = self.has_attribute_value('expr')
            clears_expr = has_expr and not self.get_attribute_value('expr')

            # Force reconsideration of the expression if cardinality
            # or required is changed.
            if (
                (
                    self.has_attribute_value('cardinality')
                    or self.has_attribute_value('required')
                )
                and not has_expr
                and old_expr
            ):
                self.set_attribute_value(
                    'expr',
                    s_expr.Expression.not_compiled(old_expr),
                )

            # Produce an error when setting a type on something with
            # an expression
            if (
                self.get_attribute_value('target')
                and (
                    (self.scls.get_expr(schema) or has_expr)
                    and not clears_expr
                )
            ):
                raise errors.UnsupportedFeatureError(
                    "cannot specify a type and an expression for a global",
                    span=self.span,
                )

            if clears_expr and old_expr:
                # If the expression was explicitly set to None,
                # that means that `RESET EXPRESSION` was executed
                # and this is no longer a computable.
                computed_fields = self.scls.get_computed_fields(schema)
                if (
                    'required' in computed_fields
                    and not self.has_attribute_value('required')
                ):
                    self.set_attribute_value('required', None)
                if (
                    'cardinality' in computed_fields
                    and not self.has_attribute_value('cardinality')
                ):
                    self.set_attribute_value('cardinality', None)

        return super()._alter_begin(schema, context)


class SetGlobalType(
    sd.AlterSpecialObjectField[Global],
    field='target',
):

    cast_expr = struct.Field(s_expr.Expression, default=None)
    reset_value = struct.Field(bool, default=False)

    def get_verb(self) -> str:
        return 'alter the type of'

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        orig_target = scls.get_explicit_field_value(
            orig_schema, 'target', None)
        new_target = scls.get_target(schema)

        if not orig_target or orig_target == new_target:
            return schema

        if not context.canonical:
            if self.cast_expr:
                raise errors.UnsupportedFeatureError(
                    f'USING casts for SET TYPE on globals are not supported',
                    hint='Use RESET TO DEFAULT instead',
                    span=self.span,
                )

            if not self.reset_value:
                raise errors.SchemaDefinitionError(
                    f"SET TYPE on global must explicitly reset the "
                    f"global's value",
                    hint='Use RESET TO DEFAULT after the type',
                    span=self.span,
                )

        return schema

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, SetGlobalType)
        if (
            isinstance(astnode, qlast.SetGlobalType)
            and astnode.cast_expr is not None
        ):
            cmd.cast_expr = s_expr.Expression.from_ast(
                astnode.cast_expr,
                schema,
                context.modaliases,
                context.localnames,
            )
        if isinstance(astnode, qlast.SetGlobalType):
            cmd.reset_value = astnode.reset_value

        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        set_field = super()._get_ast(schema, context, parent_node=parent_node)
        if set_field is None or self.is_attribute_computed('target'):
            return None
        else:
            assert isinstance(set_field, qlast.SetField)
            assert not isinstance(set_field.value, qlast.Expr)

            case_expr = None
            if self.cast_expr:
                assert isinstance(self.cast_expr, s_expr.Expression)
                case_expr = self.cast_expr.parse()

            return qlast.SetGlobalType(
                value=set_field.value,
                cast_expr=case_expr,
                reset_value=self.reset_value,
            )

    def record_diff_annotations(
        self,
        *,
        schema: s_schema.Schema,
        orig_schema: Optional[s_schema.Schema],
        context: so.ComparisonContext,
        object: Optional[so.Object],
        orig_object: Optional[so.Object],
    ) -> None:
        super().record_diff_annotations(
            schema=schema,
            orig_schema=orig_schema,
            context=context,
            orig_object=orig_object,
            object=object,
        )

        if orig_schema is None:
            return

        if (
            not self.get_orig_attribute_value('expr')
            and not self.get_attribute_value('expr')
        ):
            self.reset_value = True


class DeleteGlobal(
    s_expraliases.DeleteAliasLike[Global],
    GlobalCommand,
):
    astnode = qlast.DropGlobal
