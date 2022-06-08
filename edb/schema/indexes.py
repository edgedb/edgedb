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
from edb.common import parsing
from edb.common import verutils
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

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


class Index(
    referencing.ReferencedInheritingObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.INDEX,
    data_safe=True,
):

    subject = so.SchemaField(
        so.Object,
        compcoef=None,
        inheritable=False)

    expr = so.SchemaField(
        s_expr.Expression,
        coerce=True,
        compcoef=0.909,
        ddl_identity=True,
    )

    except_expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        ddl_identity=True,
    )

    def __repr__(self) -> str:
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.QualName:
        quals = sn.quals_from_fullname(name)
        ptr_qual = quals[2]
        expr_qual = quals[1]
        return sn.QualName(
            module='__',
            name=f'{ptr_qual}_{expr_qual[:8]}',
        )

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        shortname = cls.get_shortname_static(name)
        return shortname.name


IndexableSubject_T = TypeVar('IndexableSubject_T', bound='IndexableSubject')


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


class IndexSourceCommand(
    inheriting.InheritingObjectCommand[IndexableSubject_T],
):
    pass


class IndexCommandContext(sd.ObjectCommandContext[Index],
                          s_anno.AnnotationSubjectCommandContext):
    pass


class IndexCommand(
    referencing.ReferencedInheritingObjectCommand[Index],
    context_class=IndexCommandContext,
    referrer_context_class=IndexSourceCommandContext,
):

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname
            assert isinstance(referrer_name, sn.QualName)

            shortname = sn.QualName(
                module='__',
                name=astnode.name.name,
            )

            quals = cls._classname_quals_from_ast(
                schema, astnode, shortname, referrer_name, context)

            name = sn.QualName(
                module=referrer_name.module,
                name=sn.get_specialized_name(
                    shortname,
                    str(referrer_name),
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
        base_name: sn.Name,
        referrer_name: sn.QualName,
        context: sd.CommandContext,
    ) -> Tuple[str, ...]:
        assert isinstance(astnode, qlast.IndexCommand)
        # use the normalized text directly from the expression
        expr = s_expr.Expression.from_ast(
            astnode.expr, schema, context.modaliases)
        expr_text = expr.text
        assert expr_text is not None
        exprs = [expr_text]

        if astnode.except_expr:
            expr = s_expr.Expression.from_ast(
                astnode.except_expr, schema, context.modaliases)
            exprs.append('!' + expr.text)

        expr_qual = cls._name_qual_from_exprs(schema, exprs)

        ptrs = ast.find_children(astnode, lambda n: isinstance(n, qlast.Ptr))
        ptr_name_qual = '_'.join(ptr.ptr.name for ptr in ptrs)
        if not ptr_name_qual:
            ptr_name_qual = 'idx'

        return (expr_qual, ptr_name_qual)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.QualName
    ) -> Tuple[str, ...]:
        quals = sn.quals_from_fullname(name)
        return tuple(quals[-2:])

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[Index, so.NoDefaultT] = so.NoDefault,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Index:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: None = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[Index]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[Index, so.NoDefaultT, None] = so.NoDefault,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[Index]:
        try:
            return super().get_object(
                schema, context, name=name,
                default=default, sourcectx=sourcectx,
            )
        except errors.InvalidReferenceError:
            referrer_ctx = self.get_referrer_context_or_die(context)
            referrer = referrer_ctx.scls
            expr = self.get_ddl_identity('expr')
            raise errors.InvalidReferenceError(
                f"index on ({expr.text}) does not exist on "
                f"{referrer.get_verbosename(schema)}"
            ) from None

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.ObjectCommand[Index]:
        cmd = super()._cmd_from_ast(schema, astnode, context)
        if isinstance(astnode, qlast.IndexCommand):
            cmd.set_ddl_identity(
                'expr',
                s_expr.Expression.from_ast(
                    astnode.expr,
                    schema,
                    context.modaliases,
                ),
            )
        return cmd

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in ('expr', 'except_expr'):
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.Expression:
        singletons: List[s_types.Type]
        if field.name in {'expr', 'except_expr'}:
            # type ignore below, for the class is used as mixin
            parent_ctx = context.get_ancestor(
                IndexSourceCommandContext,  # type: ignore
                self
            )
            assert parent_ctx is not None
            assert isinstance(parent_ctx.op, sd.ObjectCommand)
            subject = parent_ctx.op.get_object(schema, context)

            expr = type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors={qlast.Subject().name: subject},
                    path_prefix_anchor=qlast.Subject().name,
                    singletons=frozenset([subject]),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                ),
            )

            # Check that the inferred cardinality is no more than 1
            from edb.ir import ast as ir_ast
            assert isinstance(expr.irast, ir_ast.Statement)
            if expr.irast.cardinality.is_multi():
                raise errors.ResultCardinalityMismatchError(
                    f'possibly more than one element returned by '
                    f'the index expression where only singletons '
                    f'are allowed',
                    context=value.qlast.context,
                )

            if expr.irast.volatility != qltypes.Volatility.Immutable:
                raise errors.SchemaDefinitionError(
                    f'index expressions must be immutable',
                    context=value.qlast.context,
                )

            return expr
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
        if field.name == 'expr':
            return s_expr.Expression(text='0')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')


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

        if (
            orig_text is not None
            and context.compat_ver_is_before(
                (1, 0, verutils.VersionStage.ALPHA, 6)
            )
        ):
            # Versions prior to a6 used a different expression
            # normalization strategy, so we must renormalize the
            # expression.
            expr_ql = qlcompiler.renormalize_compat(
                astnode.expr,
                orig_text,
                schema=schema,
                localnames=context.localnames,
            )
        else:
            expr_ql = astnode.expr

        cmd.set_attribute_value(
            'expr',
            s_expr.Expression.from_ast(
                expr_ql,
                schema,
                context.modaliases,
            ),
        )

        if astnode.except_expr:
            cmd.set_attribute_value(
                'except_expr',
                s_expr.Expression.from_ast(
                    astnode.except_expr,
                    schema,
                    context.modaliases,
                ),
            )

        return cmd

    @classmethod
    def as_inherited_ref_ast(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        name: sn.Name,
        parent: referencing.ReferencedObject,
    ) -> qlast.ObjectDDL:
        assert isinstance(parent, Index)
        astnode_cls = cls.referenced_astnode

        expr = parent.get_expr(schema)
        if expr is None:
            expr_ql = None
        else:
            expr_ql = edgeql.parse_fragment(expr.text)

        except_expr = parent.get_except_expr(schema)
        if except_expr:
            except_expr_ql = except_expr.qlast
        else:
            except_expr_ql = None

        return astnode_cls(name=qlast.ObjectRef(name='idx'), expr=expr_ql,
                           except_expr=except_expr_ql)


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


class AlterIndexOwned(
    IndexCommand,
    referencing.AlterOwned[Index],
    field='owned',
):
    pass


class AlterIndex(
    IndexCommand,
    referencing.AlterReferencedInheritingObject[Index],
):
    astnode = qlast.AlterIndex

    def canonicalize_alter_from_external_ref(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        if (
            not self.get_attribute_value('abstract')
            and (indexexpr := self.get_attribute_value('expr')) is not None
        ):
            # To compute the new name, we construct an AST of the
            # index, since that is the infrastructure we have for
            # computing the classname.
            ast = qlast.CreateIndex(
                name=qlast.ObjectRef(name="idx", module="__"),
                expr=indexexpr.qlast,
            )
            quals = sn.quals_from_fullname(self.classname)
            new_name = self._classname_from_ast_and_referrer(
                schema, sn.QualName.from_string(quals[0]), ast, context)
            if new_name == self.classname:
                return

            rename = self.scls.init_delta_command(
                schema, sd.RenameObject, new_name=new_name)
            rename.set_attribute_value(
                'name', value=new_name, orig_value=self.classname)
            self.add(rename)


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
