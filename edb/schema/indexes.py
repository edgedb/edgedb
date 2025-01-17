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
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    Mapping,
    Sequence,
    Dict,
    List,
    cast,
    overload,
    TYPE_CHECKING,
)

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
from . import schema as s_schema
from . import utils


if TYPE_CHECKING:
    from . import objtypes as s_objtypes


# The name used for default concrete indexes
DEFAULT_INDEX = sn.QualName(module='__', name='idx')


def is_index_valid_for_type(
    index: Index,
    expr_type: s_types.Type,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> bool:
    index_allows_tuples = is_index_supporting_tuples(index, schema)

    for index_match in schema.get_referrers(
        index, scls_type=IndexMatch, field_name='index',
    ):
        valid_type = index_match.get_valid_type(schema)
        if index_allows_tuples:
            if is_subclass_or_tuple(expr_type, valid_type, schema):
                return True
        elif expr_type.issubclass(schema, valid_type):
            return True

    if context.testmode and str(index.get_name(schema)) == 'default::test':
        # For functional tests of abstract indexes.
        return expr_type.issubclass(
            schema,
            schema.get('std::str', type=s_scalars.ScalarType),
        )

    return False


def is_index_supporting_tuples(
    index: Index,
    schema: s_schema.Schema,
) -> bool:
    index_name = str(index.get_name(schema))
    return index_name in {
        "std::fts::index",
        "ext::pg_trgm::gin",
        "ext::pg_trgm::gist",
        "pg::gist",
        "pg::gin",
        "pg::brin",
    }


def is_subclass_or_tuple(
    ty: s_types.Type, parent: s_types.Type, schema: s_schema.Schema
) -> bool:
    if isinstance(ty, s_types.Tuple):
        for (_, st) in ty.iter_subtypes(schema):
            if not st.issubclass(schema, parent):
                return False
        return True
    else:
        return ty.issubclass(schema, parent)


def _merge_deferrability(
    a: qltypes.IndexDeferrability,
    b: qltypes.IndexDeferrability,
) -> qltypes.IndexDeferrability:
    if a is b:
        return a
    else:
        if a is qltypes.IndexDeferrability.Prohibited:
            raise ValueError(f"{a} and {b} are incompatible")
        elif a is qltypes.IndexDeferrability.Permitted:
            return b
        else:
            return a


def merge_deferrability(
    idx: Index,
    bases: List[Index],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
) -> Optional[qltypes.IndexDeferrability]:
    """Merge function for abstract index deferrability."""

    return utils.merge_reduce(
        idx,
        bases,
        field_name=field_name,
        ignore_local=ignore_local,
        schema=schema,
        f=_merge_deferrability,
        type=qltypes.IndexDeferrability,
    )


def merge_deferred(
    idx: Index,
    bases: List[Index],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
) -> Optional[bool]:
    """Merge function for the DEFERRED qualifier on indexes."""

    if idx.is_non_concrete(schema):
        return None

    if bases:
        deferrability = next(iter(bases)).get_deferrability(schema)
    else:
        deferrability = qltypes.IndexDeferrability.Prohibited

    local_deferred = idx.get_explicit_local_field_value(
        schema, field_name, None)

    idx_repr = idx.get_verbosename(schema, with_parent=True)

    if not idx.is_defined_here(schema):
        ignore_local = True

    if ignore_local:
        return deferrability is qltypes.IndexDeferrability.Required
    elif local_deferred is None:
        # No explicit local declaration, derive from abstract index
        # deferrability.
        if deferrability is qltypes.IndexDeferrability.Required:
            raise errors.SchemaDefinitionError(
                f"{idx_repr} must be declared as deferred"
            )
        else:
            return False
    else:
        if (
            local_deferred
            and deferrability is qltypes.IndexDeferrability.Prohibited
        ):
            raise errors.SchemaDefinitionError(
                f"{idx_repr} cannot be declared as deferred"
            )
        elif (
            not local_deferred
            and deferrability is qltypes.IndexDeferrability.Required
        ):
            raise errors.SchemaDefinitionError(
                f"{idx_repr} must be declared as deferred"
            )

        return local_deferred  # type: ignore


def get_index_match_fullname_from_names(
    valid_type: sn.Name,
    index: sn.Name,
) -> sn.QualName:
    std = not (
        (
            isinstance(valid_type, sn.QualName)
            and sn.UnqualName(valid_type.module) not in s_schema.STD_MODULES
        ) or (
            isinstance(index, sn.QualName)
            and sn.UnqualName(index.module) not in s_schema.STD_MODULES
        )
    )
    module = 'std' if std else '__ext_index_matches__'

    quals = [str(valid_type), str(index)]
    shortname = sn.QualName(module, 'index_match')
    return sn.QualName(
        module=shortname.module,
        name=sn.get_specialized_name(shortname, *quals),
    )


def get_index_match_fullname(
    schema: s_schema.Schema,
    valid_type: s_types.TypeShell[s_types.Type],
    index: so.ObjectShell[Index],
) -> sn.QualName:
    return get_index_match_fullname_from_names(
        valid_type.get_name(schema),
        index.get_name(schema),
    )


class Index(
    referencing.ReferencedInheritingObject,
    so.InheritingObject,  # Help reflection figure out the right db MRO
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.INDEX,
    data_safe=True,
):
    # redefine, so we can change compcoef
    bases = so.SchemaField(
        so.ObjectList['Index'],  # type: ignore
        type_is_generic_self=True,
        default=so.DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.0,  # can't rebase
    )

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

    # Appears in base abstract index definitions and defines how the index
    # is represented in postgres.
    code = so.SchemaField(
        str,
        default=None,
        compcoef=None,
        inheritable=False,
        allow_ddl_set=True,
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

    type_args = so.SchemaField(
        so.ObjectList[so.Object],
        coerce=True,
        compcoef=0,
        default=so.DEFAULT_CONSTRUCTOR,
        inheritable=False,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.0,
        ddl_identity=True,
    )

    except_expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.0,
        ddl_identity=True,
    )

    deferrability = so.SchemaField(
        qltypes.IndexDeferrability,
        default=qltypes.IndexDeferrability.Prohibited,
        coerce=True,
        compcoef=0.909,
        merge_fn=merge_deferrability,
        allow_ddl_set=True,
    )

    deferred = so.SchemaField(
        bool,
        default=False,
        compcoef=0.909,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
        merge_fn=merge_deferred,
    )

    def __repr__(self) -> str:
        cls = self.__class__
        return '<{}.{} {!r} at 0x{:x}>'.format(
            cls.__module__, cls.__name__, self.id, id(self))

    __str__ = __repr__

    def as_delete_delta(
        self,
        *,
        schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[Index]:
        delta = super().as_delete_delta(schema=schema, context=context)
        old_params = self.get_params(schema).objects(schema)
        for p in old_params:
            delta.add(p.as_delete_delta(schema=schema, context=context))

        return delta

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
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

    def is_non_concrete(self, schema: s_schema.Schema) -> bool:
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

    def get_ddl_identity(
        self,
        schema: s_schema.Schema,
    ) -> Optional[Dict[str, Any]]:
        v = super().get_ddl_identity(schema) or {}
        v['kwargs'] = self.get_all_kwargs(schema)
        return v

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

        root = self.get_root(schema)

        kwargs = self.get_all_kwargs(schema)

        for param in root.get_params(schema).objects(schema):
            kwname = param.get_parameter_name(schema)
            if (
                kwname not in kwargs and
                (val := param.get_default(schema)) is not None
            ):
                kwargs[kwname] = val

        for k, v in kwargs.items():
            kwargs[k] = v.ensure_compiled(
                schema,
                as_fragment=True,
                options=qlcompiler.CompilerOptions(
                    schema_object_context=s_func.Parameter,
                ),
                context=None,
            )

        return kwargs

    def get_concrete_kwargs_as_values(
        self,
        schema: s_schema.Schema,
    ) -> dict[str, Any]:
        kwargs = self.get_concrete_kwargs(schema)
        return {
            k: v.assert_compiled().as_python_value()
            for k, v in kwargs.items()
        }

    def is_defined_here(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        """
        Returns True iff the index has not been inherited from a parent subject,
        and was originally defined on the subject.
        """
        return all(
            base.get_abstract(schema)
            for base in self.get_bases(schema).objects(schema)
        )


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


class IndexMatch(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.INDEX_MATCH,
    data_safe=True,
    abstract=False,
):

    valid_type = so.SchemaField(
        s_types.Type, compcoef=0.5)

    index = so.SchemaField(
        Index, compcoef=0.5)


class IndexSourceCommandContext:
    pass


class IndexSourceCommand(
    inheriting.InheritingObjectCommand[IndexableSubject_T],
):
    pass


class IndexCommandContext(sd.ObjectCommandContext[Index],
                          s_anno.AnnotationSubjectCommandContext):
    pass


class IndexMatchCommandContext(sd.ObjectCommandContext[IndexMatch],
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
        astnode: qlast.ObjectDDL,
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
        astnode: qlast.ObjectDDL,
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
    def _classname_quals_from_name(cls, name: sn.QualName) -> Tuple[str, ...]:
        quals = sn.quals_from_fullname(name)
        return tuple(quals[-1:])

    @classmethod
    def _index_kwargs_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
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
        sourcectx: Optional[parsing.Span] = None,
    ) -> Index:
        ...

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: None = None,
        sourcectx: Optional[parsing.Span] = None,
    ) -> Optional[Index]:
        ...

    def get_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[Index, so.NoDefaultT, None] = so.NoDefault,
        sourcectx: Optional[parsing.Span] = None,
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

        kwargs: Optional[Mapping[str, s_expr.Expression]] = (
            self.get_resolved_attribute_value(
                'kwargs',
                schema=schema,
                context=context,
            )
        )
        if kwargs:
            assert isinstance(astnode, (qlast.CreateIndex,
                                        qlast.ConcreteIndexCommand))
            astnode.kwargs = {
                name: expr.parse() for name, expr in kwargs.items()
            }

        return astnode

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if field in ('kwargs', 'expr', 'except_expr'):
            return field
        elif (
            field == 'deferred'
            and astnode is qlast.CreateConcreteIndex
        ):
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

    def get_friendly_object_name_for_description(
        self,
        *,
        parent_op: Optional[sd.Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Optional[so.Object_T] = None,
        object_desc: Optional[str] = None,
    ) -> str:
        friendly_name: str = 'index'

        expr: Optional[s_expr.Expression] = None
        if (
            self.has_ddl_identity('expr') and
            (expr := self.get_ddl_identity('expr'))
        ):
            expr_text = expr.text
            if expr_text[0] != '(' or expr_text[-1] != ')':
                expr_text = '(' + expr_text + ')'

            friendly_name = f"index on {expr_text}"

        if not isinstance(parent_op, sd.ObjectCommand):
            return f"{friendly_name}"
        else:
            return f"{friendly_name} of {parent_op.get_verbosename()}"

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        from edb.ir import ast as irast
        from edb.ir import utils as irutils

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
                    anchors={'__subject__': subject},
                    path_prefix_anchor='__subject__',
                    singletons=frozenset([subject]),
                    apply_query_rewrites=False,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    detached=True,
                ),
                context=context,
            )

            # Check that the inferred cardinality is no more than 1
            if expr.irast.cardinality.is_multi():
                raise errors.SchemaDefinitionError(
                    f'possibly more than one element returned by '
                    f'the index expression where only singletons '
                    f'are allowed',
                    span=value.parse().span,
                )

            if expr.irast.volatility != qltypes.Volatility.Immutable:
                raise errors.SchemaDefinitionError(
                    f'index expressions must be immutable',
                    span=value.parse().span,
                )

            refs = irutils.get_longest_paths(expr.irast)

            has_multi = False
            for ref in refs:
                assert subject
                # Subject is a singleton in an index expression if it is itself
                # a singleton, regardless of other parts of the path.
                if irutils.ref_contains_multi(ref, subject.id):
                    has_multi = True
                    break

            if set_of_op := irutils.find_set_of_op(
                expr.irast,
                has_multi,
            ):
                label = (
                    'function'
                    if isinstance(set_of_op, irast.FunctionCall) else
                    'operator'
                )
                op_name = str(set_of_op.func_shortname)
                raise errors.SchemaDefinitionError(
                    f"cannot use SET OF {label} '{op_name}' "
                    f"in an index expression",
                    span=set_of_op.span
                )

            # compile the expression to sql to preempt errors downstream
            utils.try_compile_irast_to_sql_tree(expr, self.span)

            return expr
        elif field.name == "kwargs":
            parent_ctx = context.get_ancestor(
                IndexSourceCommandContext,  # type: ignore
                self
            )
            if parent_ctx is not None:
                assert isinstance(parent_ctx.op, sd.ObjectCommand)
                subject = parent_ctx.op.get_object(schema, context)
                subject_vname = subject.get_verbosename(schema)
                idx_name = self.get_verbosename(parent=subject_vname)
            else:
                idx_name = self.get_verbosename()
            return type(value).compiled(
                value,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    apply_query_rewrites=not context.stdmode,
                    track_schema_ref_exprs=track_schema_ref_exprs,
                    in_ddl_context_name=idx_name,
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
        if field.name == 'expr':
            return s_expr.Expression(text='0')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)

        referrer_ctx = self.get_referrer_context(context)
        if referrer_ctx is not None:
            # Concrete index
            deferrability = self.get_attribute_value("deferrability")
            if deferrability is not None:
                raise errors.SchemaDefinitionError(
                    "deferrability can only be specified on abstract indexes",
                    span=self.get_attribute_span("deferrability"),
                )
        return schema

    def ast_ignore_field_ownership(self, field: str) -> bool:
        """Whether to force generating an AST even though field isn't owned"""
        return field == "deferred"

    def _append_subcmd_ast(
        self,
        schema: s_schema.Schema,
        node: qlast.DDLOperation,
        subcmd: sd.Command,
        context: sd.CommandContext,
    ) -> None:
        if isinstance(subcmd, s_anno.AnnotationValueCommand):
            pname = sn.shortname_from_fullname(subcmd.classname)
            assert isinstance(pname, sn.QualName)
            # Skip injected annotations
            if pname.module == "ext::ai":
                return

        super()._append_subcmd_ast(schema, node, subcmd, context)


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

            if astnode.deferred is not None:
                cmd.set_attribute_value(
                    'deferred',
                    astnode.deferred,
                    span=astnode.span,
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

        except_expr: s_expr.Expression | None = parent.get_except_expr(schema)
        if except_expr:
            except_expr_ql = except_expr.parse()
        else:
            except_expr_ql = None

        qlkwargs = {
            key: val.parse() for key, val in parent.get_kwargs(schema).items()
        }

        return astnode_cls(
            name=cls.get_inherited_ref_name(schema, context, parent, name),
            kwargs=qlkwargs,
            expr=expr_ql,
            except_expr=except_expr_ql,
            deferred=parent.get_deferred(schema),
        )

    @classmethod
    def get_inherited_ref_name(
        cls,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        parent: so.Object,
        name: sn.Name,
    ) -> qlast.ObjectRef:
        bn = sn.shortname_from_fullname(name)
        return utils.name_to_ast_ref(bn)

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
                span=self.span
            )

        # Make sure that the kwargs are valid.
        for key in kwargs:
            expr = kwargs[key]
            param = params.get_by_name(schema, key)
            if param is None:
                raise errors.SchemaDefinitionError(
                    f'the {ancestor_name} does not have a parameter {key!r}',
                    span=self.span
                )

            param_type = param.get_type(schema)
            comp_expr = s_expr.Expression.compiled(
                expr, schema=schema, context=None)
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
                    span=self.span,
                )

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_object(schema, context)

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
                            span=self.span
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
                        span=self.span
                    )

            return

        # The checks below apply only to concrete indexes.
        subject = referrer_ctx.scls
        assert isinstance(subject, (s_types.Type, s_pointers.Pointer))
        assert isinstance(subject, IndexableSubject)

        if (
            is_object_scope_index(schema, self.scls)
            and isinstance(subject, s_pointers.Pointer)
        ):
            dn = self.scls.get_displayname(schema)
            raise errors.SchemaDefinitionError(
                f"{dn} cannot be declared on links",
                span=self.span,
            )

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
        if name != DEFAULT_INDEX and (
            abs_index := schema.get(name, type=Index)
        ):
            # only abstract indexes should have unmangled names
            assert abs_index.get_abstract(schema)
            root = abs_index.get_root(schema)

            # For indexes that can only appear once per object, call
            # get_effective_object_index for its side-effect of
            # checking the error.
            if is_exclusive_object_scope_index(schema, self.scls):
                effective, others = get_effective_object_index(
                    schema, subject, root.get_name(schema), span=self.span)
                if effective == self.scls and others:
                    other = others[0]
                    if (
                        other.get_concrete_kwargs_as_values(schema)
                        != self.scls.get_concrete_kwargs_as_values(schema)
                    ):
                        subject_name = subject.get_verbosename(schema)
                        other_subject = other.get_subject(schema)
                        assert other_subject
                        other_name = other_subject.get_verbosename(schema)
                        raise errors.InvalidDefinitionError(
                            f"{root.get_name(schema)} indexes defined for "
                            f"{subject_name} with different "
                            f"parameters than on base type {other_name}",
                            span=self.span,
                        )

            # Make sure that kwargs and parameters match in name and type.
            # Also make sure that all parameters have values at this point
            # (either default or provided in kwargs).
            params = root.get_params(schema)
            inh_kwargs = self.scls.get_all_kwargs(schema)

            self._validate_kwargs(schema,
                                  params,
                                  kwargs,
                                  abs_index.get_verbosename(schema))

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
                    span=self.span
                )

            # Make sure that the concrete index expression type matches the
            # abstract index type.
            expr = self.get_resolved_attribute_value(
                'expr',
                schema=schema,
                context=context,
            )
            options = qlcompiler.CompilerOptions(
                anchors={'__subject__': subject},
                path_prefix_anchor='__subject__',
                singletons=frozenset([subject]),
                apply_query_rewrites=False,
                schema_object_context=self.get_schema_metaclass(),
            )
            comp_expr = s_expr.Expression.compiled(
                expr, schema=schema, options=options, context=context
            )
            expr_type = comp_expr.irast.stype

            if not is_index_valid_for_type(
                root, expr_type, comp_expr.schema, context,
            ):
                hint = None
                if str(name) == 'std::fts::index':
                    hint = (
                        'std::fts::document can be constructed with '
                        'std::fts::with_options(str, ...)'
                    )

                raise errors.SchemaDefinitionError(
                    f'index expression ({expr.text}) '
                    f'is not of a valid type for the '
                    f'{self.scls.get_verbosename(comp_expr.schema)}',
                    span=self.span,
                    details=hint,
                )

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if (
            referrer_ctx is not None
            and not context.canonical
            and is_ext_ai_index(schema, self.scls)
        ):
            schema = self._inject_ext_ai_model_dependency(schema, context)

        return schema

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        referrer_ctx = self.get_referrer_context(context)
        if (
            referrer_ctx is not None
            and not context.canonical
            and is_ext_ai_index(schema, self.scls)
        ):
            self._copy_ext_ai_model_annotations(schema, context)

        return super()._create_innards(schema, context)

    def get_resolved_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, Any]:
        params = self._get_params(schema, context)
        props = super().get_resolved_attributes(schema, context)
        props['params'] = params
        return props

    @classmethod
    def _classbases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> List[so.ObjectShell[Index]]:
        if (
            isinstance(astnode, qlast.CreateConcreteIndex)
            and astnode.name
            and astnode.name.module != DEFAULT_INDEX.module
            and astnode.name.name != DEFAULT_INDEX.name
        ):
            base = utils.ast_objref_to_object_shell(
                astnode.name,
                metaclass=Index,
                schema=schema,
                modaliases=context.modaliases,
            )
            return [base]
        else:
            return super()._classbases_from_ast(schema, astnode, context)

    def _inject_ext_ai_model_dependency(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        model_stype = self._get_referenced_embedding_model(schema, context)

        type_args = so.ObjectList.create(
            schema,
            [model_stype],
        )

        self.set_attribute_value(
            "type_args",
            type_args.as_shell(schema),
        )

        return self.scls.update(schema, {"type_args": type_args})

    def _copy_ext_ai_model_annotations(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        # Copy ext::ai:: annotations declared on the model specified
        # by the `embedding_model` kwarg.  This is necessary to avoid
        # expensive lookups later where the index is used.
        model_stype = self._get_referenced_embedding_model(schema, context)
        model_stype_vn = model_stype.get_verbosename(schema)
        model_annos = model_stype.get_annotations(schema)
        my_name = self.scls.get_name(schema)
        idx_defined_here = self.scls.is_defined_here(schema)

        for model_anno in model_annos.objects(schema):
            anno_name = model_anno.get_shortname(schema)
            if anno_name.module != "ext::ai":
                continue
            value = model_anno.get_value(schema)
            if value is None or value == "<must override>":
                raise errors.SchemaDefinitionError(
                    f"{model_stype_vn} is missing a value for the "
                    f"'{anno_name}' annotation"
                )
            anno_sname = sn.get_specialized_name(
                anno_name,
                str(my_name),
            )
            anno_fqname = sn.QualName(my_name.module, anno_sname)
            schema1 = model_anno.update(
                schema,
                {
                    "name": anno_fqname,
                    "subject": self.scls,
                },
            )
            anno_copy = schema1.get(
                anno_fqname,
                type=s_anno.AnnotationValue,
            )

            anno_cmd: sd.ObjectCommand[s_anno.AnnotationValue]
            if idx_defined_here:
                anno_cmd = anno_copy.as_create_delta(
                    schema1, so.ComparisonContext())
                anno_cmd.discard_attribute("bases")
                anno_cmd.discard_attribute("ancestors")
            else:
                anno_cmd = sd.get_object_delta_command(
                    objtype=s_anno.AnnotationValue,
                    cmdtype=sd.AlterObject,
                    schema=schema,
                    name=anno_fqname,
                )
                anno_cmd.set_attribute_value("owned", True)

            self.add(anno_cmd)

        model_dimensions = model_stype.must_get_json_annotation(
            schema,
            sn.QualName("ext::ai", "embedding_model_max_output_dimensions"),
            int,
        )
        supports_shortening = model_stype.must_get_json_annotation(
            schema,
            sn.QualName("ext::ai", "embedding_model_supports_shortening"),
            bool,
        )

        kwargs = self.scls.get_concrete_kwargs_as_values(schema)
        specified_dimensions = kwargs["dimensions"]

        MAX_DIM = 2000  # pgvector limit

        if specified_dimensions is None:
            if model_dimensions > MAX_DIM:
                if not supports_shortening:
                    raise errors.SchemaDefinitionError(
                        f"{model_stype_vn} returns embeddings with over "
                        f"{MAX_DIM} dimensions, does not support embedding "
                        f"shortening, and thus cannot be used with "
                        f"this index",
                        span=self.span,
                    )
                else:
                    dimensions = MAX_DIM
            else:
                dimensions = model_dimensions
        else:
            if specified_dimensions > MAX_DIM:
                raise errors.SchemaDefinitionError(
                    f"cannot use more than {MAX_DIM} dimensions with "
                    f"this index",
                    span=self.span,
                )
            elif specified_dimensions > model_dimensions:
                raise errors.SchemaDefinitionError(
                    f"{model_stype_vn} does not support more than "
                    f"{model_dimensions} dimensions, "
                    f"got {specified_dimensions}",
                    span=self.span,
                )
            elif (
                specified_dimensions != model_dimensions
                and not supports_shortening
            ):
                raise errors.SchemaDefinitionError(
                    f"{model_stype_vn} returns embeddings with over "
                    f"{model_dimensions} dimensions, and does not support "
                    f"embedding shortening, and thus {specified_dimensions} "
                    f"cannot be used for this index",
                    span=self.span,
                )
            else:
                dimensions = specified_dimensions

        dims_anno_sname = sn.get_specialized_name(
            sn.QualName("ext::ai", "embedding_dimensions"),
            str(my_name),
        )
        alter_anno = sd.get_object_delta_command(
            objtype=s_anno.AnnotationValue,
            cmdtype=sd.AlterObject,
            schema=schema,
            name=sn.QualName(my_name.module, dims_anno_sname),
        )
        alter_anno.set_attribute_value("value", str(dimensions))
        alter_anno.set_attribute_value("owned", True)
        self.add(alter_anno)

    def _get_referenced_embedding_model(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_objtypes.ObjectType:
        # Copy ext::ai:: annotations declared on the model specified
        # by the `embedding_model` kwarg.  This is necessary to avoid
        # expensive lookups later where the index is used.
        kwargs = self.scls.get_concrete_kwargs_as_values(schema)
        model_name = kwargs["embedding_model"]

        models = get_defined_ext_ai_embedding_models(schema, model_name)
        if len(models) == 0:
            raise errors.SchemaDefinitionError(
                f'undefined embedding model: no subtype of '
                f'ext::ai::EmbeddingModel is annotated as {model_name!r}',
                span=self.span,
            )
        elif len(models) > 1:
            models_dn = [
                model.get_displayname(schema) for model in models.values()
            ]
            raise errors.SchemaDefinitionError(
                f'expecting only one embedding model to be annotated '
                f'with ext::ai::model_name={model_name!r}: got multiple: '
                f'{", ".join(models_dn)}',
                span=self.span,
            )

        return next(iter(models.values()))


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

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        referrer_ctx = self.get_referrer_context(context)
        if (
            referrer_ctx is not None
            and not context.canonical
            and is_ext_ai_index(schema, self.scls)
        ):
            schema = self._fixup_ext_ai_model_annotations(schema, context)

        return schema

    def _fixup_ext_ai_model_annotations(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        # Fixup the special ext::ai annotations that got copied to an
        # ai index. They are always owned, even if the index is not,
        # and so we have some hackiness to keep that true when DROP OWNED
        # is run on the index.
        # TODO: Can this be rationalized more?

        for ref in self.scls.get_annotations(schema).objects(schema):
            anno_name = ref.get_shortname(schema)
            if anno_name.module != "ext::ai":
                continue
            alter = ref.init_delta_command(schema, sd.AlterObject)
            alter.set_attribute_value('owned', True)
            if anno_name.name == 'embedding_dimensions':
                alter.set_attribute_value(
                    'value', ref.get_value(schema), inherited=False)
            schema = alter.apply(schema, context)
            self.add(alter)

        return schema


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
            assert isinstance(indexexpr, s_expr.Expression)

            # To compute the new name, we construct an AST of the
            # index, since that is the infrastructure we have for
            # computing the classname.
            name = sn.shortname_from_fullname(self.classname)
            assert isinstance(name, sn.QualName), "expected qualified name"
            ast = qlast.CreateConcreteIndex(
                name=qlast.ObjectRef(name=name.name, module=name.module),
                expr=indexexpr.parse(),
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

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_begin(schema, context)
        if not context.canonical:
            for param in self.scls.get_params(schema).objects(schema):
                self.add(param.init_delta_command(schema, sd.DeleteObject))
        return schema

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.ConcreteIndexCommand):
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


def get_effective_object_index(
    schema: s_schema.Schema,
    subject: IndexableSubject,
    base_idx_name: sn.QualName,
    span: Optional[parsing.Span] = None,
) -> tuple[Optional[Index], Sequence[Index]]:
    """
    Returns the effective index of a subject and any overridden fs indexes
    """
    indexes: so.ObjectIndexByFullname[Index] = subject.get_indexes(schema)

    base = schema.get(base_idx_name, type=Index, default=None)
    if base is None:
        # Abstract base index does not exist.
        return (None, ())

    object_indexes = [
        ind
        for ind in indexes.objects(schema)
        if ind.issubclass(schema, base)
    ]
    if len(object_indexes) == 0:
        return (None, ())

    object_indexes_defined_here = [
        ind for ind in object_indexes if ind.is_defined_here(schema)
    ]

    if len(object_indexes_defined_here) > 0:
        # indexes defined here have priority

        if len(object_indexes_defined_here) > 1:
            subject_name = subject.get_displayname(schema)
            raise errors.InvalidDefinitionError(
                f'multiple {base_idx_name} indexes defined for {subject_name}',
                span=span,
            )
        effective = object_indexes_defined_here[0]
        overridden = [
            i.get_implicit_bases(schema)[0]
            for i in object_indexes if i != effective
        ]

    else:
        # there are no object-scoped indexes defined on the subject
        # the inherited indexes take effect

        if len(object_indexes) > 1:
            subject_name = subject.get_displayname(schema)
            raise errors.InvalidDefinitionError(
                f'multiple {base_idx_name} indexes '
                f'inherited for {subject_name}',
                span=span,
            )

        effective = object_indexes[0]
        overridden = []

    return (effective, overridden)


class IndexMatchCommand(sd.QualifiedObjectCommand[IndexMatch],
                        context_class=IndexMatchCommandContext):

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        if not context.stdmode and not context.testmode:
            raise errors.UnsupportedFeatureError(
                'user-defined index matches are not supported',
                span=astnode.span
            )

        return super()._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        assert isinstance(astnode, qlast.IndexMatchCommand)
        modaliases = context.modaliases

        valid_type = utils.ast_to_type_shell(
            astnode.valid_type,
            metaclass=s_types.Type,
            modaliases=modaliases,
            schema=schema,
        )

        index = utils.ast_objref_to_object_shell(
            astnode.name,
            metaclass=Index,
            modaliases=context.modaliases,
            schema=schema,
        )

        return get_index_match_fullname(schema, valid_type, index)

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        schema = s_types.materialize_type_in_attribute(
            schema, context, self, 'valid_type')
        return schema


class CreateIndexMatch(IndexMatchCommand, sd.CreateObject[IndexMatch]):
    astnode = qlast.CreateIndexMatch

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        fullname = self.classname
        index_match = schema.get(fullname, None)
        if index_match:
            valid_type = self.get_attribute_value('valid_type')
            index = self.get_attribute_value('index')

            raise errors.DuplicateDefinitionError(
                f'an index match for {valid_type.get_displayname(schema)!r} '
                f'using {index.get_displayname(schema)!r} is already defined',
                span=self.span)

        return super()._create_begin(schema, context)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        assert isinstance(astnode, qlast.CreateIndexMatch)

        modaliases = context.modaliases

        valid_type = utils.ast_to_type_shell(
            astnode.valid_type,
            metaclass=s_types.Type,
            modaliases=modaliases,
            schema=schema,
        )
        cmd.set_attribute_value('valid_type', valid_type)

        index = utils.ast_objref_to_object_shell(
            qlast.ObjectRef(
                module=astnode.name.module,
                name=astnode.name.name,
            ),
            metaclass=Index,
            modaliases=context.modaliases,
            schema=schema,
        )
        cmd.set_attribute_value('index', index)

        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        assert isinstance(node, qlast.CreateIndexMatch)
        new_value: Any = op.new_value

        if op.property == 'valid_type':
            # In an index match we can only have pure types, so this is going
            # to be a TypeName.
            node.valid_type = cast(qlast.TypeName,
                                   utils.typeref_to_ast(schema, new_value))

        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteIndexMatch(IndexMatchCommand, sd.DeleteObject[IndexMatch]):
    astnode = qlast.DropIndexMatch

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_begin(schema, context)
        if not context.canonical:
            valid_type = self.scls.get_valid_type(schema)
            if op := valid_type.as_type_delete_if_unused(schema):
                self.add_caused(op)
        return schema


# XXX: the below hardcode should be replaced by an index scope
#      field instead.
def is_object_scope_index(
    schema: s_schema.Schema,
    index: Index,
) -> bool:
    return (
        is_fts_index(schema, index)
        or is_ext_ai_index(schema, index)
    )


def is_exclusive_object_scope_index(
    schema: s_schema.Schema,
    index: Index,
) -> bool:
    return is_object_scope_index(schema, index)


def is_fts_index(
    schema: s_schema.Schema,
    index: Index,
) -> bool:
    fts_index = schema.get(sn.QualName("std::fts", "index"), type=Index)
    return index.issubclass(schema, fts_index)


def get_ai_index_id(
    schema: s_schema.Schema,
    index: Index,
) -> str:
    # TODO: Use the model name?
    return f'base'


def is_ext_ai_index(
    schema: s_schema.Schema,
    index: Index,
) -> bool:
    ai_index = schema.get(
        sn.QualName("ext::ai", "index"),
        type=Index,
        default=None,
    )
    if ai_index is None:
        return False
    else:
        return index.issubclass(schema, ai_index)


_embedding_model = sn.QualName("ext::ai", "EmbeddingModel")
_model_name = sn.QualName("ext::ai", "model_name")


def get_defined_ext_ai_embedding_models(
    schema: s_schema.Schema,
    model_name: Optional[str] = None,
) -> dict[str, s_objtypes.ObjectType]:
    from . import objtypes as s_objtypes

    base_embedding_model = schema.get(
        _embedding_model,
        type=s_objtypes.ObjectType,
    )

    def _flt(
        schema: s_schema.Schema,
        anno: s_anno.AnnotationValue,
    ) -> bool:
        if anno.get_shortname(schema) != _model_name:
            return False

        subject = anno.get_subject(schema)
        value = anno.get_value(schema)

        return (
            value is not None and value != "<must override>"
            and (model_name is None or anno.get_value(schema) == model_name)
            and isinstance(subject, s_objtypes.ObjectType)
            and subject.issubclass(schema, base_embedding_model)
        )

    annos = schema.get_objects(
        type=s_anno.AnnotationValue,
        extra_filters=(_flt,),
    )

    result = {}
    for anno in annos:
        subject = anno.get_subject(schema)
        assert isinstance(subject, s_objtypes.ObjectType)
        result[anno.get_value(schema)] = subject

    return result
