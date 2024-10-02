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
from typing import Any, Optional, Type, cast, TYPE_CHECKING

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

if TYPE_CHECKING:
    from . import pointers as s_pointers


class Rewrite(
    referencing.NamedReferencedInheritingObject,
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

    # 0.0 because we don't support ALTER yet
    expr = so.SchemaField(
        s_expr.Expression,
        compcoef=0.0,
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

    def get_ptr_target(self, schema: s_schema.Schema) -> s_types.Type:
        pointer: s_pointers.Pointer = cast(
            's_pointers.Pointer', self.get_subject(schema))
        ptr_target = pointer.get_target(schema)
        assert ptr_target
        return ptr_target


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
    referencing.NamedReferencedInheritingObjectCommand[Rewrite],
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
            from edb.common import ast
            from edb.ir import ast as irast
            from edb.ir import pathid
            from . import pointers as s_pointers
            from . import objtypes as s_objtypes
            from . import links as s_links

            parent_ctx = self.get_referrer_context_or_die(context)
            pointer = parent_ctx.op.scls
            assert isinstance(pointer, s_pointers.Pointer)

            source = pointer.get_source(schema)
            if isinstance(source, s_objtypes.ObjectType):
                subject = source
            elif isinstance(source, s_links.Link):
                subject = source.get_target(schema)
                assert subject

                span = self.get_attribute_span('expr')
                raise errors.SchemaDefinitionError(
                    'rewrites on link properties are not supported',
                    span=span,
                )
            else:
                raise NotImplementedError('unsupported rewrite source')

            # XXX: in_ddl_context_name is disabled for now because
            # it causes the compiler to reject DML; we might actually
            # want it for something, though, so we might need to
            # improve that restriction.
            # parent_vname = source.get_verbosename(schema)
            # pol_name = self.get_verbosename(parent=parent_vname)
            # in_ddl_context_name = pol_name

            kind = self._get_kind(schema)

            anchors: dict[str, s_types.Type | pathid.PathId] = {}

            # __subject__
            anchors["__subject__"] = pathid.PathId.from_type(
                schema,
                subject,
                typename=sn.QualName(module="__derived__", name="__subject__"),
                env=None,
            )
            # __specified__
            bool_type = schema.get("std::bool", type=s_types.Type)
            schema, specified_type = s_types.Tuple.create(
                schema,
                named=True,
                element_types={
                    pn.name: bool_type
                    for pn in subject.get_pointers(schema).keys(schema)
                },
            )
            anchors['__specified__'] = specified_type

            # __old__
            if qltypes.RewriteKind.Update == kind:
                anchors['__old__'] = pathid.PathId.from_type(
                    schema,
                    subject,
                    typename=sn.QualName(module='__derived__', name='__old__'),
                    env=None,
                )

            singletons = frozenset(anchors.values())

            # If the `__specified__` anchor is used, create references to the
            # matching pointers.
            #
            # These references are necessary in order to compute the dependency
            # and ordering of Rewrite commands when producing DDL.
            #
            # If creating Type T with two properties, A and B, such that
            # A has a Rewrite containing `__specified__.B`.
            #
            # Without the references, the DDL may look like:
            # - Create Type T
            #   - Create Property A
            #     - Create Rewrite using __specified__.B
            #   - Create Property B
            #
            # This will cause an issue when compiling the Rewrite. At that
            # point, the schema will not know about B and so the tuple will not
            # have element `.B`.
            #
            # The reference will cause the reordering of commands and the DDL
            # may instead look like:
            # - Create Object O
            #   - Create Property A
            #   - Create Property B
            #   - Alter Property A
            #     - Create Rewrite using __specified__.B
            #
            # With Create Rewrite ordered after Property B, the tuple for
            # `__specified__` will correctly have element `.B`.
            def find_extra_refs(ir_expr: irast.Set) -> set[so.Object]:
                def find_specified(node: irast.TupleIndirectionPointer) -> bool:
                    return node.source.anchor == '__specified__'

                ref_ptr_names: set[str] = set()
                for tuple_node in ast.find_children(
                    ir_expr,
                    irast.TupleIndirectionPointer,
                    test_func=find_specified,
                ):
                    ref_ptr_names.add(tuple_node.ptrref.name.name)

                ref_ptrs: set[so.Object] = set(
                    pointer
                    for pointer in subject.get_pointers(schema).objects(schema)
                    if pointer.get_shortname(schema).name in ref_ptr_names
                )

                return ref_ptrs

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
                find_extra_refs=find_extra_refs,
                context=context,
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
            return s_types.type_dummy_expr(
                self.scls.get_ptr_target(schema), schema)
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        expr: s_expr.Expression = self.scls.get_expr(schema)

        if not expr.irast:
            expr = self.compile_expr_field(
                schema, context, Rewrite.get_field('expr'), expr
            )
            assert expr.irast

        ir = expr.irast
        compiled_schema = ir.schema
        typ: s_types.Type = ir.stype

        if (
            typ.is_view(compiled_schema)
            # Using an alias/global always creates a new subtype view,
            # but we want to allow those here, so check whether there
            # is a shape more directly.
            and not (
                len(shape := ir.view_shapes.get(typ, [])) == 1
                and shape[0].is_id_pointer(compiled_schema)
            )
        ):
            span = self.get_attribute_span('expr')
            raise errors.SchemaDefinitionError(
                f'rewrite expression may not include a shape',
                span=span,
            )

        ptr_target = self.scls.get_ptr_target(compiled_schema)
        if not typ.assignment_castable_to(ptr_target, compiled_schema):
            span = self.get_attribute_span('expr')
            raise errors.SchemaDefinitionError(
                f'rewrite expression is of invalid type: '
                f'{typ.get_displayname(compiled_schema)}, '
                f'expected {ptr_target.get_displayname(compiled_schema)}',
                span=span,
            )

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
            newnode = astnode.replace(
                name=qlast.ObjectRef(module='__', name=str(kind)),
                kinds=kind,
            )

            cmd = super()._cmd_tree_from_ast(schema, newnode, context)
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
                span=astnode.expr.span,
            )
        return group

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'kind':
            assert isinstance(node, qlast.CreateRewrite)
            node.kinds = [self.get_attribute_value('kind')]
        else:
            super()._apply_field_ast(schema, context, node, op)


class RebaseRewrite(
    RewriteCommand,
    referencing.RebaseReferencedInheritingObject[Rewrite],
):
    pass


class RenameRewrite(
    RewriteCommand,
    referencing.RenameReferencedInheritingObject[Rewrite],
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
                span=self.span,
            )

        return schema


class DeleteRewrite(
    RewriteCommand,
    referencing.DeleteReferencedInheritingObject[Rewrite],
):
    referenced_astnode = astnode = qlast.DropRewrite

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        assert isinstance(node, qlast.DropRewrite)
        skind = sn.shortname_from_fullname(self.classname).name
        node.kinds = [qltypes.RewriteKind(skind)]
        return node
