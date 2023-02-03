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
from . import pointers as s_pointers
from . import objects as so
from . import referencing
from . import scalars as s_scalars
from . import types as s_types


if TYPE_CHECKING:
    from . import schema as s_schema


# The name used for default concrete indexes
DEFAULT_INDEX = sn.QualName(module='__', name='idx')


def is_index_valid_for_type(
    index: Index,
    expr_type: s_types.Type,
    schema: s_schema.Schema
) -> bool:
    # HACK: currently this helper just hardcodes the permitted index & type
    # combinations, but this should be inferred based on index definition.
    index_name = str(index.get_name(schema))
    match index_name:
        case 'pg::hash':
            return True
        case 'pg::btree':
            return True
        case 'pg::gin':
            return expr_type.is_array()
        case 'pg::gist':
            return expr_type.is_range()
        case 'pg::spgist':
            return (
                expr_type.is_range()
                or
                expr_type.issubclass(
                    schema,
                    schema.get('std::str', type=s_scalars.ScalarType),
                )
            )
        case 'pg::brin':
            return (
                expr_type.is_range()
                or
                expr_type.issubclass(
                    schema,
                    (
                        schema.get('std::anyreal',
                                   type=s_scalars.ScalarType),
                        schema.get('std::bytes',
                                   type=s_scalars.ScalarType),
                        schema.get('std::str',
                                   type=s_scalars.ScalarType),
                        schema.get('std::uuid',
                                   type=s_scalars.ScalarType),
                        schema.get('std::datetime',
                                   type=s_scalars.ScalarType),
                        schema.get('std::duration',
                                   type=s_scalars.ScalarType),
                        schema.get('cal::local_datetime',
                                   type=s_scalars.ScalarType),
                        schema.get('cal::local_date',
                                   type=s_scalars.ScalarType),
                        schema.get('cal::local_time',
                                   type=s_scalars.ScalarType),
                        schema.get('cal::relative_duration',
                                   type=s_scalars.ScalarType),
                        schema.get('cal::date_duration',
                                   type=s_scalars.ScalarType),
                    )
                )
            )

    return False


class Index(
    referencing.ReferencedInheritingObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.INDEX,
    data_safe=True,
):

    subject = so.SchemaField(
        so.Object,
        default=None,
        compcoef=None,
        inheritable=False,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
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

    def get_verbosename(
        self,
        schema: s_schema.Schema,
        *,
        with_parent: bool = False
    ) -> str:
        vn = super().get_verbosename(schema, with_parent=with_parent)
        if self.get_abstract(schema):
            return f'abstract {vn}'
        else:
            # concrete index must have a subject
            assert self.get_subject(schema) is not None
            return vn

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_subject(schema) is None

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.QualName:
        quals = sn.quals_from_fullname(name)

        if quals:
            ptr_qual = quals[2]
            expr_qual = quals[1]
            return sn.QualName(
                module='__',
                name=f'{ptr_qual}_{expr_qual[:8]}',
            )
        else:
            assert isinstance(name, sn.QualName)
            return name

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
        # We actually want to override how ReferencedObjectCommand determines
        # the classname
        shortname = super(
            referencing.ReferencedObjectCommand, cls
        )._classname_from_ast(schema, astnode, context)

        referrer_ctx = cls.get_referrer_context(context)
        if referrer_ctx is not None:

            referrer_name = referrer_ctx.op.classname
            assert isinstance(referrer_name, sn.QualName)
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
        assert isinstance(astnode, qlast.ConcreteIndexCommand)
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

        ptrs = ast.find_children(astnode, qlast.Ptr)
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
        if isinstance(astnode, qlast.ConcreteIndexCommand):
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
    ) -> s_expr.CompiledExpression:
        from edb.ir import utils as irutils
        from edb.ir import ast as irast

        if field.name in {'expr', 'except_expr'}:
            # type ignore below, for the class is used as mixin
            parent_ctx = context.get_ancestor(
                IndexSourceCommandContext,  # type: ignore
                self
            )
            assert parent_ctx is not None
            assert isinstance(parent_ctx.op, sd.ObjectCommand)
            subject = parent_ctx.op.get_object(schema, context)

            expr = value.compiled(
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors={qlast.Subject().name: subject},
                    path_prefix_anchor=qlast.Subject().name,
                    singletons=frozenset([subject]),
                    apply_query_rewrites=False,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                ),
            )

            # Check that the inferred cardinality is no more than 1
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

            refs = irutils.get_longest_paths(expr.irast)

            has_multi = False
            for ref in refs:
                assert subject
                while ref.rptr:
                    rptr = ref.rptr
                    if rptr.dir_cardinality.is_multi():
                        has_multi = True

                    # We don't need to look further than the subject,
                    # which is always valid. (And which is a singleton
                    # in an index expression if it is itself a
                    # singleton, regardless of other parts of the path.)
                    if (
                        isinstance(rptr.ptrref, irast.PointerRef)
                        and rptr.ptrref.id == subject.id
                    ):
                        break
                    ref = rptr.source

            if has_multi and irutils.contains_set_of_op(expr.irast):
                raise errors.SchemaDefinitionError(
                    "cannot use aggregate functions or operators "
                    "in an index expression",
                    context=self.source_context,
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
    astnode = [qlast.CreateConcreteIndex, qlast.CreateIndex]
    referenced_astnode = qlast.CreateConcreteIndex

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, (qlast.CreateConcreteIndex,
                                    qlast.CreateIndex))

        if isinstance(astnode, qlast.CreateIndex):
            cmd.set_attribute_value('abstract', True)

        elif isinstance(astnode, qlast.CreateConcreteIndex):
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
        assert expr is not None
        expr_ql = edgeql.parse_fragment(expr.text)

        except_expr = parent.get_except_expr(schema)
        if except_expr:
            except_expr_ql = except_expr.qlast
        else:
            except_expr_ql = None

        return astnode_cls(
            name=qlast.ObjectRef(
                module=DEFAULT_INDEX.module, name=DEFAULT_INDEX.name
            ),
            expr=expr_ql,
            except_expr=except_expr_ql,
        )

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_create(schema, context)

        referrer_ctx = self.get_referrer_context(context)

        name = sn.shortname_from_fullname(
            self.get_resolved_attribute_value(
                'name',
                schema=schema,
                context=context,
            )
        )

        if referrer_ctx is None:
            # Creating abstract indexes is only allowed in "EdgeDB developer"
            # mode, i.e. when populating std library, etc.
            if not context.stdmode and not context.testmode:
                raise errors.SchemaDefinitionError(
                    f'cannot create {self.get_verbosename()} '
                    f'because user-defined abstract indexes are not '
                    f'supported',
                    context=self.source_context
                )

            # Make sure that all bases are ultimately inherited from the same
            # root base class.
            bases = self.get_resolved_attribute_value(
                'bases',
                schema=schema,
                context=context,
            )
            if bases:
                root = None
                for base in bases.objects(schema):

                    lineage = [base] + list(
                        base.get_ancestors(schema).objects(schema))

                    if root is None:
                        root = lineage[-1]
                    elif root != lineage[-1]:
                        raise errors.SchemaDefinitionError(
                            f'cannot create {self.get_verbosename()} '
                            f'because it extends incompatible abstract indxes',
                            context=self.source_context
                        )
            return

        # The checks below apply only to concrete indexes.
        subject = referrer_ctx.scls
        assert isinstance(subject, (s_types.Type, s_pointers.Pointer))

        # Ensure that the name of the index (if given) matches an existing
        # abstract index.
        #
        # HACK: the old concrete indexes all have names in form __::idx, but
        # this should be the actual name provided. Also the index without name
        # defaults to '__::idx'.
        if name != DEFAULT_INDEX and (index := schema.get(name, type=Index)):
            # only abstract indexes should have unmangled names
            assert index.get_abstract(schema)

            # Make sure that the concrete index expression type matches the
            # abstract index type.
            expr = self.get_resolved_attribute_value(
                'expr',
                schema=schema,
                context=context,
            )
            options = qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: subject},
                path_prefix_anchor=qlast.Subject().name,
                singletons=frozenset([subject]),
                apply_query_rewrites=False,
                schema_object_context=self.get_schema_metaclass(),
            )
            comp_expr = s_expr.Expression.compiled(
                expr, schema=schema, options=options
            )
            expr_type = comp_expr.irast.stype

            if not is_index_valid_for_type(index, expr_type, schema):
                raise errors.SchemaDefinitionError(
                    f'index expression ({expr.text}) '
                    f'is not of a valid type for the '
                    f'{index.get_verbosename(schema)}',
                    context=self.source_context
                )


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
    astnode = [qlast.AlterConcreteIndex, qlast.AlterIndex]
    referenced_astnode = qlast.AlterConcreteIndex

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
            name = sn.shortname_from_fullname(self.classname)
            assert isinstance(name, sn.QualName), "expected qualified name"
            ast = qlast.CreateConcreteIndex(
                name=qlast.ObjectRef(name=name.name, module=name.module),
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
    astnode = [qlast.DropConcreteIndex, qlast.DropIndex]
    referenced_astnode = qlast.DropConcreteIndex

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.DropConcreteIndex)
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
