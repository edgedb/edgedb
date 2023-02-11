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
from edb.common import parsing
from edb.common import verutils
from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import expr as s_expr
from . import functions as s_func
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
        case 'fts::textsearch':
            return expr_type.issubclass(
                schema, schema.get('std::str', type=s_scalars.ScalarType))
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

    # These can only appear in base abstract index definitions. These
    # determine how indexes can be configured.
    params = so.SchemaField(
        s_func.FuncParameterList,
        coerce=True,
        compcoef=0.4,
        default=so.DEFAULT_CONSTRUCTOR,
        inheritable=False,
    )

    # These can appear in abstract indexes extending an existing one in order
    # to override exisitng parameters. Also they can appear in concrete
    # indexes.
    kwargs = so.SchemaField(
        s_expr.ExpressionDict,
        coerce=True,
        compcoef=0,
        default=so.DEFAULT_CONSTRUCTOR,
        inheritable=False,
        ddl_identity=True,
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
        # baseline name for indexes
        vn = self.get_displayname(schema)

        if self.get_abstract(schema):
            return f"abstract index '{vn}'"
        else:
            # concrete index must have a subject
            assert self.get_subject(schema) is not None

            # add kwargs (if any) to the concrete name
            kwargs = self.get_kwargs(schema)
            if kwargs:
                kw = []
                for key, val in kwargs.items():
                    kw.append(f'{key}:={val.text}')
                vn = f'{vn}({", ".join(kw)})'

            vn = f"index {vn!r}"

            if with_parent:
                return self.add_parent_name(vn, schema)
            return vn

    def add_parent_name(
        self,
        base_name: str,
        schema: s_schema.Schema,
    ) -> str:
        # Remove the placeholder name of the generic index.
        if base_name == f"index '{DEFAULT_INDEX}'":
            base_name = 'index'

        return super().add_parent_name(base_name, schema)

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_subject(schema) is None

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.QualName:
        name = sn.shortname_from_fullname(name)
        assert isinstance(name, sn.QualName)
        return name

    def get_all_kwargs(
        self,
        schema: s_schema.Schema,
    ) -> s_expr.ExpressionDict:
        kwargs = s_expr.ExpressionDict()
        all_kw = type(self).get_field('kwargs').merge_fn(
            self,
            self.get_ancestors(schema).objects(schema),
            'kwargs',
            schema=schema,
        )
        if all_kw:
            kwargs.update(all_kw)

        return kwargs

    def get_root(
        self,
        schema: s_schema.Schema,
    ) -> Index:
        if not self.get_abstract(schema):
            name = sn.shortname_from_fullname(self.get_name(schema))
            index = schema.get(name, type=Index)
        else:
            index = self

        if index.get_bases(schema):
            return index.get_ancestors(schema).objects(schema)[-1]
        else:
            return index

    def get_concrete_kwargs(
        self,
        schema: s_schema.Schema,
    ) -> s_expr.ExpressionDict:
        assert not self.get_abstract(schema)

        name = sn.shortname_from_fullname(self.get_name(schema))
        index = schema.get(name, type=Index)
        root = self.get_root(schema)

        kwargs = index.get_all_kwargs(schema)
        kwargs.update(self.get_kwargs(schema))

        for param in root.get_params(schema).objects(schema):
            kwname = param.get_parameter_name(schema)
            if (
                kwname not in kwargs and
                (val := param.get_default(schema)) is not None
            ):
                kwargs[kwname] = val

        return kwargs


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
    s_func.ParametrizedCommand[Index],
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
        exprs = []

        kwargs = cls._index_kwargs_from_ast(schema, astnode, context)
        for key, val in kwargs.items():
            exprs.append(f'{key}:={val.text}')

        # use the normalized text directly from the expression
        expr = s_expr.Expression.from_ast(
            astnode.expr, schema, context.modaliases)
        expr_text = expr.text
        assert expr_text is not None
        exprs.append(expr_text)

        if astnode.except_expr:
            expr = s_expr.Expression.from_ast(
                astnode.except_expr, schema, context.modaliases)
            exprs.append('!' + expr.text)

        return (cls._name_qual_from_exprs(schema, exprs),)

    @classmethod
    def _classname_quals_from_name(
        cls,
        name: sn.QualName
    ) -> Tuple[str, ...]:
        quals = sn.quals_from_fullname(name)
        return tuple(quals[-1:])

    @classmethod
    def _index_kwargs_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: sd.CommandContext,
    ) -> Dict[str, s_expr.Expression]:
        kwargs = dict()
        # Some abstract indexes and all concrete index commands have kwargs.
        assert isinstance(astnode, (qlast.CreateIndex,
                                    qlast.ConcreteIndexCommand))

        for key, val in astnode.kwargs.items():
            kwargs[key] = s_expr.Expression.from_ast(
                val, schema, context.modaliases, as_fragment=True)

        return kwargs

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

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        astnode = super()._get_ast(schema, context, parent_node=parent_node)

        kwargs = self.get_resolved_attribute_value(
            'kwargs',
            schema=schema,
            context=context,
        )
        if kwargs:
            assert isinstance(astnode, (qlast.CreateIndex,
                                        qlast.ConcreteIndexCommand))
            astnode.kwargs = {
                name: expr.qlast for name, expr in kwargs.items()
            }

        return astnode

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in ('kwargs', 'expr', 'except_expr'):
            return field
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def get_ddl_identity_fields(
        self,
        context: sd.CommandContext,
    ) -> Tuple[so.Field[Any], ...]:
        id_fields = super().get_ddl_identity_fields(context)
        omit_fields = set()

        if (
            self.get_attribute_value('abstract')
            and not self.get_attribute_value('bases')
        ):
            # Base abstract indexes don't have kwargs at all.
            omit_fields.add('kwargs')

        if omit_fields:
            return tuple(f for f in id_fields if f.name not in omit_fields)
        else:
            return id_fields

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

        assert isinstance(cmd, IndexCommand)
        assert isinstance(astnode, (qlast.CreateConcreteIndex,
                                    qlast.CreateIndex))

        if isinstance(astnode, qlast.CreateIndex):
            cmd.set_attribute_value('abstract', True)

            params = cls._get_param_desc_from_ast(
                schema, context.modaliases, astnode)
            for param in params:
                # as_create_delta requires the specific type
                cmd.add_prerequisite(param.as_create_delta(
                    schema, cmd.classname, context=context))

            # There are several possibilities for abstract indexes:
            # 1) base abstract index
            # 2) an abstract index extending another one
            # 3) an abstract index listing index fallback alternatives
            if astnode.bases is None:
                if astnode.index_types is None:
                    # This actually defines a new index (1).
                    pass
                else:
                    # This is for index fallback alternatives (3).
                    raise NotImplementedError("Index fallback not implemented")
            else:
                # Extending existing indexes for composition (2).
                kwargs = cls._index_kwargs_from_ast(schema, astnode, context)
                if kwargs:
                    cmd.set_attribute_value('kwargs', kwargs)

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

            kwargs = cls._index_kwargs_from_ast(schema, astnode, context)
            if kwargs:
                cmd.set_attribute_value('kwargs', kwargs)

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

    def _validate_kwargs(
        self,
        schema: s_schema.Schema,
        params: s_func.FuncParameterList,
        kwargs: s_expr.ExpressionDict,
        ancestor_name: str,
    ) -> None:
        if not kwargs:
            return

        if not params:
            raise errors.SchemaDefinitionError(
                f'the {ancestor_name} does not support any parameters',
                context=self.source_context
            )

        # Make sure that the kwargs are valid.
        for key in kwargs:
            expr = kwargs[key]
            param = params.get_by_name(schema, key)
            if param is None:
                raise errors.SchemaDefinitionError(
                    f'the {ancestor_name} does not have a parameter {key!r}',
                    context=self.source_context
                )

            param_type = param.get_type(schema)
            comp_expr = s_expr.Expression.compiled(expr, schema=schema)
            expr_type = comp_expr.irast.stype

            if (
                not param_type.is_polymorphic(schema) and
                not expr_type.is_polymorphic(schema) and
                not expr_type.implicitly_castable_to(
                    param_type, schema)
            ):
                raise errors.SchemaDefinitionError(
                    f'the {key!r} parameter of the '
                    f'{self.get_verbosename()} has type of '
                    f'{expr_type.get_displayname(schema)} that '
                    f'is not implicitly castable to the '
                    f'corresponding parameter of the '
                    f'{ancestor_name} with type '
                    f'{param_type.get_displayname(schema)}',
                    context=self.source_context,
                )

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_create(schema, context)

        referrer_ctx = self.get_referrer_context(context)

        # Get kwargs if any, so that we can process them later.
        kwargs = self.get_resolved_attribute_value(
            'kwargs',
            schema=schema,
            context=context,
        )

        if referrer_ctx is None:
            # Make sure that all bases are ultimately inherited from the same
            # root base class.
            bases = self.get_resolved_attribute_value(
                'bases',
                schema=schema,
                context=context,
            )
            if bases:
                # Users can extend abstract indexes.
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

                # We should have found a root because we have bases.
                assert root is not None
                # Make sure that the kwargs are valid.
                self._validate_kwargs(
                    schema,
                    root.get_params(schema),
                    kwargs,
                    root.get_verbosename(schema),
                )

            else:
                # Creating new abstract indexes is only allowed in "EdgeDB
                # developer" mode, i.e. when populating std library, etc.
                if not context.stdmode and not context.testmode:
                    raise errors.SchemaDefinitionError(
                        f'cannot create {self.get_verbosename()} '
                        f'because user-defined abstract indexes are not '
                        f'supported',
                        context=self.source_context
                    )

            return

        # The checks below apply only to concrete indexes.
        subject = referrer_ctx.scls
        assert isinstance(subject, (s_types.Type, s_pointers.Pointer))

        # Ensure that the name of the index (if given) matches an existing
        # abstract index.
        name = sn.shortname_from_fullname(
            self.get_resolved_attribute_value(
                'name',
                schema=schema,
                context=context,
            )
        )

        # HACK: the old concrete indexes all have names in form __::idx, but
        # this should be the actual name provided. Also the index without name
        # defaults to '__::idx'.
        if name != DEFAULT_INDEX and (index := schema.get(name, type=Index)):
            # only abstract indexes should have unmangled names
            assert index.get_abstract(schema)
            root = index.get_root(schema)

            # Make sure that kwargs and parameters match in name and type.
            # Also make sure that all parameters have values at this point
            # (either default or provided in kwargs).
            params = root.get_params(schema)
            inh_kwargs = index.get_all_kwargs(schema)

            self._validate_kwargs(schema,
                                  params,
                                  kwargs,
                                  index.get_verbosename(schema))

            unused_names = {p.get_parameter_name(schema)
                            for p in params.objects(schema)}
            if kwargs:
                unused_names -= set(kwargs)
            if inh_kwargs:
                unused_names -= set(inh_kwargs)
            if unused_names:
                # Check that all of these parameters have defaults.
                for pname in list(unused_names):
                    param = params.get_by_name(schema, pname)
                    if param and param.get_default(schema) is not None:
                        unused_names.discard(pname)

            if unused_names:
                names = ', '.join(repr(n) for n in sorted(unused_names))
                raise errors.SchemaDefinitionError(
                    f'cannot create {self.get_verbosename()} '
                    f'because the following parameters are still undefined: '
                    f'{names}.',
                    context=self.source_context
                )

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

            if not is_index_valid_for_type(root, expr_type, schema):
                raise errors.SchemaDefinitionError(
                    f'index expression ({expr.text}) '
                    f'is not of a valid type for the '
                    f'{index.get_verbosename(schema)}',
                    context=self.source_context
                )

    def get_resolved_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, Any]:
        params = self._get_params(schema, context)
        props = super().get_resolved_attributes(schema, context)
        props['params'] = params
        return props


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
