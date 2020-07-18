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

from edb import edgeql
from edb import errors
from edb.common import ast
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing


if TYPE_CHECKING:
    from . import schema as s_schema
    from . import types as s_types


class Index(referencing.ReferencedInheritingObject, s_anno.AnnotationSubject):

    subject = so.SchemaField(so.Object)

    expr = so.SchemaField(
        s_expr.Expression,
        coerce=True,
        compcoef=0.909,
        ddl_identity=True,
    )

    # Text representation of the original expression that's been
    # parsed and re-generated, but not normalized.
    orig_expr = so.SchemaField(
        str,
        default=None,
        coerce=True,
        allow_ddl_set=True,
        ephemeral=True,
    )

    def __repr__(self) -> str:
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__

    @classmethod
    def get_shortname_static(cls, name: str) -> sn.Name:
        quals = sn.quals_from_fullname(name)
        ptr_qual = quals[2]
        expr_qual = quals[1]
        return sn.Name(
            module='__',
            name=f'{ptr_qual}_{expr_qual[:8]}',
        )

    @classmethod
    def get_displayname_static(cls, name: str) -> str:
        shortname = cls.get_shortname_static(name)
        return shortname.name


class IndexableSubject(so.InheritingObject):
    indexes_refs = so.RefDict(
        attr='indexes',
        ref_cls=Index)

    indexes = so.SchemaField(
        so.ObjectIndexByFullname[Index],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.909,
        default=so.DEFAULT_CONSTRUCTOR)

    def add_index(
        self,
        schema: s_schema.Schema,
        index: Index,
    ) -> s_schema.Schema:
        return self.add_classref(schema, 'indexes', index)


class IndexSourceCommandContext:
    pass


class IndexSourceCommand(inheriting.InheritingObjectCommand[Index]):
    pass


class IndexCommandContext(sd.ObjectCommandContext[Index],
                          s_anno.AnnotationSubjectCommandContext):
    pass


class IndexCommand(
    referencing.ReferencedInheritingObjectCommand[Index],
    schema_metaclass=Index,
    context_class=IndexCommandContext,
    referrer_context_class=IndexSourceCommandContext,
):

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.Name:
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname
            assert isinstance(referrer_name, sn.Name)

            shortname = sn.Name(
                module='__',
                name=astnode.name.name,
            )

            quals = cls._classname_quals_from_ast(
                schema, astnode, shortname, referrer_name, context)

            name = sn.Name(
                module=referrer_name.module,
                name=sn.get_specialized_name(
                    shortname,
                    referrer_name,
                    *quals,
                ),
            )
        else:
            name = super()._classname_from_ast(schema, astnode, context)

        return name

    @classmethod
    def _classname_quals_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        base_name: str,
        referrer_name: str,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        assert isinstance(astnode, qlast.IndexOp)
        expr_text = cls.get_orig_expr_text(schema, astnode, 'expr')

        if expr_text is None:
            # if not, then use the origtext directly from the expression
            expr = s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases)
            expr_text = expr.origtext

        assert expr_text is not None
        expr_qual = cls._name_qual_from_exprs(schema, (expr_text,))

        ptrs = ast.find_children(astnode, lambda n: isinstance(n, qlast.Ptr))
        ptr_name_qual = '_'.join(ptr.ptr.name for ptr in ptrs)
        if not ptr_name_qual:
            ptr_name_qual = 'idx'

        return (expr_qual, ptr_name_qual)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.SchemaName
    ) -> Tuple[str, ...]:
        quals = sn.quals_from_fullname(name)
        return tuple(quals[-2:])

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[Index, so.NoDefaultT] = so.NoDefault,
    ) -> Index:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[str] = None,
        default: None = None,
    ) -> Optional[Index]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[Index, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[Index]:
        try:
            return super().get_object(
                schema, context, name=name, default=default
            )
        except errors.InvalidReferenceError:
            referrer_ctx = self.get_referrer_context_or_die(context)
            referrer = referrer_ctx.scls
            expr = self.get_ddl_identity('expr')
            raise errors.InvalidReferenceError(
                f"index on ({expr.origtext}) does not exist on "
                f"{referrer.get_verbosename(schema)}"
            ) from None

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.ObjectCommand[Index]:
        assert isinstance(astnode, qlast.IndexOp)
        cmd = super()._cmd_from_ast(schema, astnode, context)
        orig_text = cls.get_orig_expr_text(schema, astnode, 'expr')
        cmd.ddl_identity = {
            'expr': s_expr.Expression.from_ast(
                astnode.expr,
                schema,
                context.modaliases,
                orig_text=orig_text,
            ),
        }
        return cmd

    def get_ast_attr_for_field(self, field: str) -> Optional[str]:
        if field == 'expr':
            return 'expr'
        else:
            return None


class CreateIndex(
    IndexCommand,
    referencing.CreateReferencedInheritingObject[Index],
):
    astnode = qlast.CreateIndex
    referenced_astnode = qlast.CreateIndex

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.CreateIndex)
        orig_text = cls.get_orig_expr_text(schema, astnode, 'expr')
        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                astnode.expr,
                schema,
                context.modaliases,
                orig_text=orig_text,
            ),
        )

        return cmd

    @classmethod
    def as_inherited_ref_ast(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: str,
        parent: referencing.ReferencedObject,
    ) -> qlast.ObjectDDL:
        assert isinstance(parent, Index)
        nref = cls.get_inherited_ref_name(schema, context, parent, name)
        astnode_cls = cls.referenced_astnode

        expr = parent.get_expr(schema)
        if expr is not None:
            expr_ql = edgeql.parse_fragment(expr.origtext)
        else:
            expr_ql = None

        return astnode_cls(
            name=nref,
            expr=expr_ql,
        )

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
    ) -> s_expr.Expression:
        from . import objtypes as s_objtypes

        singletons: List[s_types.Type]
        if field.name == 'expr':
            # type ignore below, for the class is used as mixin
            parent_ctx = context.get_ancestor(
                IndexSourceCommandContext,  # type: ignore
                self
            )
            assert parent_ctx is not None
            assert isinstance(parent_ctx.op, sd.ObjectCommand)
            subject_name = parent_ctx.op.classname
            subject = schema.get(subject_name, default=None)

            if isinstance(subject, s_abc.Pointer):
                singletons = []
                path_prefix_anchor = None
            else:
                assert isinstance(subject, s_objtypes.ObjectType)
                singletons = [subject]
                path_prefix_anchor = qlast.Subject().name

            expr = type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors={qlast.Subject().name: subject},
                    path_prefix_anchor=path_prefix_anchor,
                    singletons=frozenset(singletons),
                ),
            )

            # Check that the inferred cardinality is no more than 1
            if expr.irast.cardinality.is_multi():
                raise errors.ResultCardinalityMismatchError(
                    f'possibly more than one element returned by '
                    f'the index expression where only singletons '
                    f'are allowed')

            return expr
        else:
            return super().compile_expr_field(schema, context, field, value)


class RenameIndex(
    IndexCommand,
    referencing.RenameReferencedInheritingObject[Index],
):

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> RenameIndex:
        return cast(
            RenameIndex,
            super()._cmd_from_ast(schema, astnode, context),
        )


class AlterIndexOwned(IndexCommand, referencing.AlterOwned[Index]):
    astnode = qlast.AlterIndexOwned


class AlterIndex(
    IndexCommand,
    referencing.AlterReferencedInheritingObject[Index],
):
    astnode = qlast.AlterIndex


class DeleteIndex(
    IndexCommand,
    referencing.DeleteReferencedInheritingObject[Index],
):
    astnode = qlast.DropIndex

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.DropIndex)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                astnode.expr, schema, context.modaliases),
        )

        return cmd


class RebaseIndex(
    IndexCommand,
    referencing.RebaseReferencedInheritingObject[Index],
):
    pass
