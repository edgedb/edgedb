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

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import name as sn
from . import inheriting as s_inheriting
from . import objects as so
from . import referencing
from . import schema as s_schema
from . import types as s_types


class Rewrite(
    referencing.ReferencedInheritingObject,
    so.InheritingObject,  # Help reflection figure out the right db MRO
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.REWRITE,
    data_safe=True,
):

    kind = so.SchemaField(
        qltypes.RewriteKind,
        coerce=True,
        compcoef=0.0,
        special_ddl_syntax=True,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    subject = so.SchemaField(
        so.InheritingObject, compcoef=None, inheritable=False
    )

    def should_propagate(self, schema: s_schema.Schema) -> bool:
        # Rewrites should override rewrites on properties of an extended object
        # type. But overriding *objects* would be hard, so we just disable
        # inheritance for rewrites, and do lookups into parent object types
        # when retrieving them.
        return False


class RewriteCommandContext(
    sd.ObjectCommandContext[Rewrite],
    s_anno.AnnotationSubjectCommandContext,
):
    pass


class RewriteSubjectCommandContext:
    pass


class RewriteSubjectCommand(
    s_inheriting.InheritingObjectCommand[so.InheritingObjectT],
):
    pass


class RewriteCommand(
    referencing.ReferencedInheritingObjectCommand[Rewrite],
    s_anno.AnnotationSubjectCommand[Rewrite],
    context_class=RewriteCommandContext,
    referrer_context_class=RewriteSubjectCommandContext,
):
    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)

        for field in ('expr',):
            if (expr := self.get_local_attribute_value(field)) is None:
                continue

            self.compile_expr_field(
                schema,
                context,
                field=Rewrite.get_field(field),
                value=expr,
            )

        return schema

    def _get_kind(
        self,
        schema: s_schema.Schema,
    ) -> qltypes.RewriteKind:
        return self.get_attribute_value('kind') or self.scls.get_kind(schema)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool = False,
    ) -> s_expr.CompiledExpression:
        if field.name == 'expr':
            from edb.ir import pathid
            from . import pointers

            parent_ctx = self.get_referrer_context_or_die(context)
            pointer = parent_ctx.op.get_object(schema, context)
            assert isinstance(pointer, pointers.Pointer)
            source = pointer.get_source(schema)
            assert isinstance(source, s_types.Type)
            # XXX: in_ddl_context_name is disabled for now because
            # it causes the compiler to reject DML; we might actually
            # want it for something, though, so we might need to
            # improve that restriction.
            # parent_vname = source.get_verbosename(schema)
            # pol_name = self.get_verbosename(parent=parent_vname)
            # in_ddl_context_name = pol_name

            kind = self._get_kind(schema)

            anchors = {}

            # __subject__
            anchors["__subject__"] = pathid.PathId.from_type(
                schema,
                source,
                typename=sn.QualName(module="__derived__", name="__subject__"),
            )
            # __specified__
            bool_type = schema.get("std::bool", type=s_types.Type)
            schema, specified_type = s_types.Tuple.create(
                schema,
                named=True,
                element_types={
                    pn.name: bool_type
                    for pn in source.get_pointers(schema).keys(schema)
                },
            )
            anchors['__specified__'] = specified_type

            # __old__
            if qltypes.RewriteKind.Update == kind:
                anchors['__old__'] = pathid.PathId.from_type(
                    schema,
                    source,
                    typename=sn.QualName(module='__derived__', name='__old__'),
                )

            singletons = frozenset(anchors.values())

            assert isinstance(source, s_types.Type)

            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    path_prefix_anchor="__subject__",
                    anchors=anchors,
                    singletons=singletons,
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    # in_ddl_context_name=in_ddl_context_name,
                    detached=True,
                ),
            )
        else:
            return super().compile_expr_field(
                schema, context, field, value, track_schema_ref_exprs
            )

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'expr':
            return s_expr.Expression(text='false')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        # XXX: verify we don't have the same bug as access policies
        # where linkprop defaults are broken.
        # (I think we won't need to, since we'll operate after
        # the *real* operations)
        pass

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        """
        Converts a single `qlast.RewriteCommand` into multiple
        `schema.RewriteCommand`s, one for each kind.
        """

        group = sd.CommandGroup()

        assert isinstance(astnode, qlast.RewriteCommand)

        for kind in astnode.kinds:
            # use kind for the name
            astnode.name = qlast.ObjectRef(name=str(kind))

            cmd = super()._cmd_tree_from_ast(schema, astnode, context)
            assert isinstance(cmd, RewriteCommand)

            cmd.set_attribute_value('kind', kind)
            group.add(cmd)
        return group


class CreateRewrite(
    RewriteCommand,
    referencing.CreateReferencedInheritingObject[Rewrite],
):
    referenced_astnode = astnode = qlast.CreateRewrite

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in ('kind', 'expr') and issubclass(
            astnode, qlast.CreateRewrite
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
        group = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(group, sd.CommandGroup)
        assert isinstance(astnode, qlast.CreateRewrite)

        for cmd in group.ops:
            assert isinstance(cmd, CreateRewrite)

            cmd.set_attribute_value(
                'expr',
                s_expr.Expression.from_ast(
                    astnode.expr,
                    schema,
                    context.modaliases,
                    context.localnames,
                ),
                source_context=astnode.expr.context,
            )
        return group


class RebaseRewrite(
    RewriteCommand,
    referencing.RebaseReferencedInheritingObject[Rewrite],
):
    pass


class AlterRewrite(
    RewriteCommand,
    referencing.AlterReferencedInheritingObject[Rewrite],
):
    referenced_astnode = astnode = qlast.AlterRewrite

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)

        # TODO: We may wish to support this in the future but it will
        # take some thought.
        if self.get_attribute_value(
            'owned'
        ) and not self.get_orig_attribute_value('owned'):
            raise errors.SchemaDefinitionError(
                f'cannot alter the definition of inherited trigger '
                f'{self.scls.get_displayname(schema)}',
                context=self.source_context,
            )

        return schema


class DeleteRewrite(
    RewriteCommand,
    referencing.DeleteReferencedInheritingObject[Rewrite],
):
    referenced_astnode = astnode = qlast.DropRewrite
