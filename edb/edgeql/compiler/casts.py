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


"""EdgeQL compiler routines for type casts."""


from __future__ import annotations

import json
from typing import (
    Optional,
    Tuple,
    Union,
    Iterable,
    Mapping,
    cast,
    TYPE_CHECKING,
)

from edb import errors

from edb.common import parsing

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.schema import casts as s_casts
from edb.schema import constraints as s_constr
from edb.schema import functions as s_func
from edb.schema import indexes as s_indexes
from edb.schema import name as sn
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils
from edb.schema import name as s_name

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import polyres
from . import setgen
from . import typegen
from . import viewgen

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


def compile_cast(
    ir_expr: Union[irast.Set, irast.Expr],
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
    cardinality_mod: Optional[qlast.CardinalityModifier] = None,
) -> irast.Set:

    if new_stype.is_polymorphic(ctx.env.schema) and span is not None:
        # If we have no span we don't know whether this is a direct cast
        # or some implicit cast being processed.
        raise errors.QueryError(
            f'cannot cast into generic type '
            f'{new_stype.get_displayname(ctx.env.schema)!r}',
            hint="Please ensure you don't use generic "
                 '"any" types or abstract scalars.',
            span=span)

    if (
        isinstance(ir_expr, irast.Set)
        and isinstance(ir_expr.expr, irast.EmptySet)
    ):
        # For the common case of casting an empty set, we simply
        # generate a new empty set node of the requested type.
        return setgen.new_empty_set(
            stype=new_stype,
            alias=ir_expr.path_id.target_name_hint.name,
            ctx=ctx,
            span=ir_expr.span)

    if isinstance(new_stype, s_types.Array) and (
        irutils.is_untyped_empty_array_expr(ir_expr)
        or (
            isinstance(ir_expr, irast.Set)
            and irutils.is_untyped_empty_array_expr(
                irutils.unwrap_set(ir_expr).expr)
        )
    ):
        # Ditto for empty arrays.
        new_typeref = typegen.type_to_typeref(new_stype, ctx.env)
        return setgen.ensure_set(
            irast.Array(elements=[], typeref=new_typeref), ctx=ctx)

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    orig_stype = setgen.get_set_type(ir_set, ctx=ctx)

    if new_stype.is_polymorphic(ctx.env.schema):
        raise errors.QueryError(
            f'expression returns value of indeterminate type',
            span=span)

    if (orig_stype == new_stype and
            cardinality_mod is not qlast.CardinalityModifier.Required):
        return ir_set
    if orig_stype.is_object_type() and new_stype.is_object_type():
        # Object types cannot be cast between themselves,
        # as cast is a _constructor_ operation, and the only
        # valid way to construct an object is to INSERT it.
        raise errors.QueryError(
            f'cannot cast object type '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}, use '
            f'`...[IS {new_stype.get_displayname(ctx.env.schema)}]` instead',
            span=span)

    # The only valid object type cast other than <uuid> is from anytype,
    # and thus it must be an empty set.
    if (
        orig_stype.is_any(ctx.env.schema)
        and new_stype.is_object_type()
    ):
        return setgen.new_empty_set(
            stype=new_stype,
            ctx=ctx,
            span=ir_expr.span)

    uuid_t = ctx.env.get_schema_type_and_track(sn.QualName('std', 'uuid'))
    if (
        orig_stype.issubclass(ctx.env.schema, uuid_t)
        and new_stype.is_object_type()
    ):
        return _find_object_by_id(ir_expr, new_stype, ctx=ctx)

    json_t = ctx.env.get_schema_type_and_track(sn.QualName('std', 'json'))
    if (
        isinstance(ir_set.expr, irast.Array)
        and (
            isinstance(new_stype, s_types.Array)
            or new_stype.issubclass(ctx.env.schema, json_t)
        )
    ):
        cast_element = ('array', None)
        if ctx.collection_cast_info is not None:
            ctx.collection_cast_info.path_elements.append(cast_element)

        result = _cast_array_literal(
            ir_set, orig_stype, new_stype, span=span, ctx=ctx)

        if ctx.collection_cast_info is not None:
            ctx.collection_cast_info.path_elements.pop()

        return result

    if orig_stype.is_tuple(ctx.env.schema):
        return _cast_tuple(
            ir_set, orig_stype, new_stype, span=span, ctx=ctx)

    if isinstance(orig_stype, s_types.Array):
        if not s_types.is_type_compatible(
            orig_stype, new_stype, schema=ctx.env.schema
        ) and (
            not isinstance(new_stype, s_types.Array)
            and isinstance(
                (el_type := orig_stype.get_subtypes(ctx.env.schema)[0]),
                s_scalars.ScalarType,
            )
        ):
            # We're not casting to another array, so for purposes of matching
            # the right cast we want to reduce orig_stype to an array of the
            # built-in base type as that's what the cast will actually
            # expect.
            ir_set = _cast_to_base_array(
                ir_set, el_type, orig_stype, ctx=ctx)

        if isinstance(new_stype, s_types.Array):
            cast_element = ('array', None)
            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.append(cast_element)

            result = _cast_array(
                ir_set, orig_stype, new_stype, span=span, ctx=ctx)

            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.pop()

            return result

        else:
            return _cast_array(
                ir_set, orig_stype, new_stype, span=span, ctx=ctx)

    if isinstance(orig_stype, s_types.Range):
        if s_types.is_type_compatible(
            orig_stype, new_stype, schema=ctx.env.schema
        ):
            # Casting between compatible types is unnecessary. It is important
            # to catch things like RangeExprAlias and Range being of the same
            # type and not neding a cast.
            return ir_set
        else:
            if isinstance(new_stype, s_types.MultiRange):
                # For multirange target type we might need to first upcast the
                # range into corresponding multirange and then do a separate
                # cast for the subtype.
                if (
                    (ost := orig_stype.get_subtypes(schema=ctx.env.schema)) !=
                        new_stype.get_subtypes(schema=ctx.env.schema)
                ):
                    ctx.env.schema, mr_stype = \
                        s_types.MultiRange.from_subtypes(ctx.env.schema, ost)
                    ir_set = _inheritance_cast_to_ir(
                        ir_set, orig_stype, mr_stype,
                        cardinality_mod=cardinality_mod, ctx=ctx)
                    return _cast_multirange(
                        ir_set, mr_stype, new_stype, span=span, ctx=ctx)

                else:
                    # The subtypes match, so this is a direct upcast from
                    # range to multirange.
                    return _inheritance_cast_to_ir(
                        ir_set, orig_stype, new_stype,
                        cardinality_mod=cardinality_mod, ctx=ctx)

            return _cast_range(
                ir_set, orig_stype, new_stype, span=span, ctx=ctx)

    if orig_stype.is_multirange():
        if s_types.is_type_compatible(
            orig_stype, new_stype, schema=ctx.env.schema
        ):
            # Casting between compatible types is unnecessary. It is important
            # to catch things like MultiRangeExprAlias and MultiRange being of
            # the same type and not neding a cast.
            return ir_set
        else:
            return _cast_multirange(
                ir_set, orig_stype, new_stype, span=span, ctx=ctx)

    if orig_stype.issubclass(ctx.env.schema, new_stype):
        # The new type is a supertype of the old type,
        # and is always a wider domain, so we simply reassign
        # the stype.
        return _inheritance_cast_to_ir(
            ir_set, orig_stype, new_stype,
            cardinality_mod=cardinality_mod, ctx=ctx)

    if (
        new_stype.issubclass(ctx.env.schema, orig_stype)
        or _has_common_concrete_scalar(orig_stype, new_stype, ctx=ctx)
    ):
        # The new type is a subtype or a sibling type of a shared
        # ancestor, so may potentially have a more restrictive domain,
        # generate a cast call.
        return _inheritance_cast_to_ir(
            ir_set, orig_stype, new_stype,
            cardinality_mod=cardinality_mod, ctx=ctx)

    if (
        new_stype.issubclass(ctx.env.schema, json_t)
        and ir_set.path_id.is_objtype_path()
    ):
        # JSON casts of objects are special: we want the full shape
        # and not just an identity.
        viewgen.late_compile_view_shapes(ir_set, ctx=ctx)
    elif orig_stype.issubclass(ctx.env.schema, json_t):

        if base_stype := _get_concrete_scalar_base(new_stype, ctx):
            # Casts from json to custom scalars may have special handling.
            # So we turn the type cast json->x into json->base and base->x.
            base_ir = compile_cast(ir_expr, base_stype, span=span, ctx=ctx)

            return compile_cast(
                base_ir,
                new_stype,
                cardinality_mod=cardinality_mod,
                span=span,
                ctx=ctx,
            )

        elif isinstance(
            new_stype, s_types.Array
        ) and not new_stype.get_subtypes(ctx.env.schema)[0].issubclass(
            ctx.env.schema, json_t
        ):
            # Turn casts from json->array<T> into json->array<json>
            # and array<json>->array<T>.
            ctx.env.schema, json_array_typ = s_types.Array.from_subtypes(
                ctx.env.schema, [json_t]
            )
            json_array_ir = compile_cast(
                ir_expr,
                json_array_typ,
                cardinality_mod=cardinality_mod,
                span=span,
                ctx=ctx,
            )
            return compile_cast(
                json_array_ir, new_stype, span=span, ctx=ctx
            )

        elif isinstance(new_stype, s_types.Tuple):
            return _cast_json_to_tuple(
                ir_set,
                orig_stype,
                new_stype,
                cardinality_mod,
                span=span,
                ctx=ctx,
            )

        elif isinstance(new_stype, s_types.Range):
            return _cast_json_to_range(
                ir_set,
                orig_stype,
                new_stype,
                cardinality_mod,
                span=span,
                ctx=ctx,
            )

        elif isinstance(new_stype, s_types.MultiRange):
            return _cast_json_to_multirange(
                ir_set,
                orig_stype,
                new_stype,
                cardinality_mod,
                span=span,
                ctx=ctx,
            )

    # Constraints and indexes require an immutable expression, but pg cast is
    # only stable. In this specific case, we use cast wrapper function that
    # is declared to be immutable.
    if orig_stype.is_enum(ctx.env.schema) or new_stype.is_enum(ctx.env.schema):
        objctx = ctx.env.options.schema_object_context
        if objctx in (s_constr.Constraint, s_indexes.Index):

            str_typ = ctx.env.schema.get(
                sn.QualName("std", "str"),
                type=s_types.Type,
            )
            orig_str = orig_stype.issubclass(ctx.env.schema, str_typ)
            new_str = new_stype.issubclass(ctx.env.schema, str_typ)
            if orig_str or new_str:
                return _cast_enum_str_immutable(
                    ir_expr, orig_stype, new_stype, ctx=ctx
                )

    return _compile_cast(
        ir_expr,
        orig_stype,
        new_stype,
        cardinality_mod=cardinality_mod,
        span=span,
        ctx=ctx,
    )


def _has_common_concrete_scalar(
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> bool:
    schema = ctx.env.schema
    return bool(
        isinstance(orig_stype, s_scalars.ScalarType)
        and isinstance(new_stype, s_scalars.ScalarType)
        and (orig_base := orig_stype.maybe_get_topmost_concrete_base(schema))
        and (new_base := new_stype.maybe_get_topmost_concrete_base(schema))
        and orig_base == new_base
    )


def _get_concrete_scalar_base(
    stype: s_types.Type, ctx: context.ContextLevel
) -> Optional[s_types.Type]:
    """Returns None if stype is not scalar or if it is already topmost"""

    if stype.is_enum(ctx.env.schema):
        return ctx.env.get_schema_type_and_track(sn.QualName('std', 'str'))

    if not isinstance(stype, s_scalars.ScalarType):
        return None
    if topmost := stype.maybe_get_topmost_concrete_base(ctx.env.schema):
        if topmost != stype:
            return topmost
    return None


def _compile_cast(
    ir_expr: Union[irast.Set, irast.Expr],
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
    cardinality_mod: Optional[qlast.CardinalityModifier],
) -> irast.Set:

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)

    if cast is None:
        raise errors.QueryError(
            f'cannot cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} to '
            f'{new_stype.get_displayname(ctx.env.schema)!r}',
            span=span or ir_set.span)

    return _cast_to_ir(ir_set, cast, orig_stype, new_stype,
                       cardinality_mod, ctx=ctx)


def _cast_to_ir(
    ir_set: irast.Set,
    cast: s_casts.Cast,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    cardinality_mod: Optional[qlast.CardinalityModifier] = None,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    cast_name = cast.get_name(ctx.env.schema)
    cast_ir = irast.TypeCast(
        expr=ir_set,
        from_type=orig_typeref,
        to_type=new_typeref,
        cardinality_mod=cardinality_mod,
        cast_name=cast_name,
        sql_function=cast.get_from_function(ctx.env.schema),
        sql_cast=cast.get_from_cast(ctx.env.schema),
        sql_expr=bool(cast.get_code(ctx.env.schema)),
        error_message_context=cast_message_context(ctx),
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


def _inheritance_cast_to_ir(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    cardinality_mod: Optional[qlast.CardinalityModifier],
    ctx: context.ContextLevel,
) -> irast.Set:

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    cast_ir = irast.TypeCast(
        expr=ir_set,
        from_type=orig_typeref,
        to_type=new_typeref,
        cardinality_mod=cardinality_mod,
        cast_name=None,
        sql_function=None,
        sql_cast=True,
        sql_expr=False,
        error_message_context=cast_message_context(ctx),
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


class CastParamListWrapper(s_func.ParameterLikeList):

    def __init__(self, params: Iterable[s_func.ParameterDesc]) -> None:
        self._params = tuple(params)

    def get_by_name(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> s_func.ParameterDesc:
        raise NotImplementedError

    def as_str(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def find_named_only(
        self,
        schema: s_schema.Schema,
    ) -> Mapping[str, s_func.ParameterDesc]:
        return {}

    def find_variadic(
        self,
        schema: s_schema.Schema,
    ) -> Optional[s_func.ParameterDesc]:
        return None

    def has_polymorphic(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    def has_objects(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    def has_set_of(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    def objects(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_func.ParameterDesc, ...]:
        return self._params

    def has_required_params(self, schema: s_schema.Schema) -> bool:
        return True

    def get_in_canonical_order(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_func.ParameterDesc, ...]:
        return self._params


class CastCallableWrapper(s_func.CallableLike):
    # A wrapper around a cast object to make it quack like a callable
    # for the purposes of polymorphic resolution.
    def __init__(self, cast: s_casts.Cast) -> None:
        self._cast = cast

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        return False

    def get_params(
        self,
        schema: s_schema.Schema,
    ) -> s_func.ParameterLikeList:
        from_type_param = s_func.ParameterDesc(
            num=0,
            name=sn.UnqualName('val'),
            type=self._cast.get_from_type(schema).as_shell(schema),
            typemod=ft.TypeModifier.SingletonType,
            kind=ft.ParameterKind.PositionalParam,
            default=None,
        )

        to_type_param = s_func.ParameterDesc(
            num=0,
            name=sn.UnqualName('_'),
            type=self._cast.get_to_type(schema).as_shell(schema),
            typemod=ft.TypeModifier.SingletonType,
            kind=ft.ParameterKind.PositionalParam,
            default=None,
        )

        return CastParamListWrapper((from_type_param, to_type_param))

    def get_return_type(self, schema: s_schema.Schema) -> s_types.Type:
        return self._cast.get_to_type(schema)

    def get_return_typemod(self, schema: s_schema.Schema) -> ft.TypeModifier:
        return ft.TypeModifier.SingletonType

    def get_verbosename(self, schema: s_schema.Schema) -> str:
        return self._cast.get_verbosename(schema)

    def get_abstract(self, schema: s_schema.Schema) -> bool:
        return False


def _find_cast(
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> Optional[s_casts.Cast]:

    # Don't try to pick up casts when there is a direct subtyping
    # relationship.
    if (orig_stype.issubclass(ctx.env.schema, new_stype)
            or new_stype.issubclass(ctx.env.schema, orig_stype)
            or _has_common_concrete_scalar(orig_stype, new_stype, ctx=ctx)):
        return None

    casts = ctx.env.schema.get_casts_to_type(new_stype)
    if not casts and isinstance(new_stype, s_types.InheritingType):
        ancestors = new_stype.get_ancestors(ctx.env.schema)
        for t in ancestors.objects(ctx.env.schema):
            casts = ctx.env.schema.get_casts_to_type(t)
            if casts:
                break
        else:
            return None

    dummy_set = irast.DUMMY_SET
    args = [
        (orig_stype, dummy_set),
        (new_stype, dummy_set),
    ]

    matched = polyres.find_callable(
        (CastCallableWrapper(c) for c in casts), args=args, kwargs={}, ctx=ctx)

    if len(matched) == 1:
        return cast(CastCallableWrapper, matched[0].func)._cast
    elif len(matched) > 1:
        raise errors.QueryError(
            f'cannot unambiguously cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)
    else:
        return None


def _cast_json_to_tuple(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Tuple,
    cardinality_mod: Optional[qlast.CardinalityModifier],
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    with ctx.new() as subctx:
        subctx.allow_factoring()
        pathctx.register_set_in_scope(ir_set, ctx=subctx)

        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        # Top-level json->tuple casts should produce an empty set on
        # null inputs, but error on missing fields or null subelements
        allow_null = cardinality_mod != qlast.CardinalityModifier.Required

        # Only json arrays or objects can be cast to tuple.
        # If not in the top level cast, raise an exception here
        json_object_args: list[qlast.Expr] = [
            source_path,
            qlast.Constant.boolean(allow_null),
        ]
        if error_message_context := cast_message_context(subctx):
            json_object_args.append(qlast.Constant.string(
                json.dumps({
                    "error_message_context": error_message_context
                })
            ))

        # Don't validate NULLs. They are filtered out with the json nulls.
        json_objects = qlast.IfElse(
            condition=qlast.UnaryOp(
                op='EXISTS',
                operand=source_path,
            ),
            if_expr=qlast.FunctionCall(
                func=('__std__', '__tuple_validate_json'),
                args=json_object_args,
            ),
            else_expr=qlast.TypeCast(
                expr=qlast.Set(elements=[]),
                type=typegen.type_to_ql_typeref(orig_stype, ctx=ctx),
            ),
        )

        json_objects_ir = dispatch.compile(json_objects, ctx=subctx)

    with ctx.new() as subctx:
        pathctx.register_set_in_scope(json_objects_ir, ctx=subctx)
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(json_objects_ir, 'a')

        # Filter out json nulls and postgress NULLs.
        # Nulls at the top level cast can be ignored.
        filtered = qlast.SelectQuery(
            result=source_path,
            where=qlast.BinOp(
                left=qlast.FunctionCall(
                    func=('__std__', 'json_typeof'), args=[source_path]
                ),
                op='!=',
                right=qlast.Constant.string('null'),
            ),
        )
        filtered_ir = dispatch.compile(filtered, ctx=subctx)
        source_path = subctx.create_anchor(filtered_ir, 'a')

        # TODO: try using jsonb_to_record instead of a bunch of
        # json_get calls and see if that is faster.
        elements = []
        for new_el_name, new_st in new_stype.iter_subtypes(ctx.env.schema):
            cast_element = ('tuple', new_el_name)
            if subctx.collection_cast_info is not None:
                subctx.collection_cast_info.path_elements.append(cast_element)

            json_get_kwargs: dict[str, qlast.Expr] = {}
            if error_message_context := cast_message_context(subctx):
                json_get_kwargs['detail'] = qlast.Constant.string(
                    json.dumps({
                        "error_message_context": error_message_context
                    })
                )
            val_e = qlast.FunctionCall(
                func=('__std__', '__json_get_not_null'),
                args=[
                    source_path,
                    qlast.Constant.string(new_el_name),
                ],
                kwargs=json_get_kwargs
            )

            val = dispatch.compile(val_e, ctx=subctx)

            val = compile_cast(
                val, new_st,
                cardinality_mod=qlast.CardinalityModifier.Required,
                ctx=subctx, span=span)

            if subctx.collection_cast_info is not None:
                subctx.collection_cast_info.path_elements.pop()

            elements.append(irast.TupleElement(name=new_el_name, val=val))

        return setgen.new_tuple_set(
            elements,
            named=new_stype.is_named(ctx.env.schema),
            ctx=subctx,
        )


def _cast_tuple(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    assert isinstance(orig_stype, s_types.Tuple)

    # Make sure the source tuple expression is pinned in the scope,
    # so that we don't generate a cross-product of it by evaluating
    # the tuple indirections.
    pathctx.register_set_in_scope(ir_set, ctx=ctx)

    direct_cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)
    orig_subtypes = dict(orig_stype.iter_subtypes(ctx.env.schema))

    if direct_cast is not None:
        # Direct casting to non-tuple involves casting each tuple
        # element and also keeping the cast around the whole tuple.
        # This is to trigger the downstream logic of casting
        # objects (in elements of the tuple).
        elements = []
        for n in orig_subtypes:
            val = setgen.tuple_indirection_set(
                ir_set,
                source=orig_stype,
                ptr_name=n,
                ctx=ctx,
            )
            val_type = setgen.get_set_type(val, ctx=ctx)
            # Element cast
            cast_element = ('tuple', n)
            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.append(cast_element)

            val = compile_cast(val, new_stype, ctx=ctx, span=span)

            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.pop()

            elements.append(irast.TupleElement(name=n, val=val))

        new_tuple = setgen.new_tuple_set(
            elements,
            named=orig_stype.is_named(ctx.env.schema),
            ctx=ctx,
        )

        return _cast_to_ir(
            new_tuple, direct_cast, orig_stype, new_stype, ctx=ctx)

    if not new_stype.is_tuple(ctx.env.schema):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)

    assert isinstance(new_stype, s_types.Tuple)
    new_subtypes = list(new_stype.iter_subtypes(ctx.env.schema))
    if len(orig_subtypes) != len(new_subtypes):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}: '
            f'the number of elements is not the same',
            span=span)

    # For tuple-to-tuple casts we generate a new tuple
    # to simplify things on sqlgen side.
    elements = []
    for i, n in enumerate(orig_subtypes):
        val = setgen.tuple_indirection_set(
            ir_set,
            source=orig_stype,
            ptr_name=n,
            ctx=ctx,
        )
        val_type = setgen.get_set_type(val, ctx=ctx)
        new_el_name, new_st = new_subtypes[i]
        if val_type != new_st:
            # Element cast
            cast_element = ('tuple', new_el_name)
            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.append(cast_element)

            val = compile_cast(val, new_st, ctx=ctx, span=span)

            if ctx.collection_cast_info is not None:
                ctx.collection_cast_info.path_elements.pop()

        elements.append(irast.TupleElement(name=new_el_name, val=val))

    return setgen.new_tuple_set(
        elements,
        named=new_stype.is_named(ctx.env.schema),
        ctx=ctx,
    )


def _cast_range(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    assert isinstance(orig_stype, s_types.Range)

    direct_cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)
    if direct_cast is not None:
        return _cast_to_ir(
            ir_set, direct_cast, orig_stype, new_stype, ctx=ctx
        )

    if not new_stype.is_range():
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)
    assert isinstance(new_stype, s_types.Range)
    el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]
    ql_el_type = typegen.type_to_ql_typeref(el_type, ctx=ctx)

    el_cast = _find_cast(orig_el_type, el_type, span=span, ctx=ctx)
    if el_cast is None:
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)

    with ctx.new() as subctx:
        subctx.allow_factoring()
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        cast = qlast.FunctionCall(
            func=('__std__', 'range'),
            args=[
                qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'range_get_lower'),
                        args=[source_path],
                    ),
                    type=ql_el_type,
                ),
                qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'range_get_upper'),
                        args=[source_path],
                    ),
                    type=ql_el_type,
                ),
            ],
            kwargs={
                "inc_lower": qlast.FunctionCall(
                    func=('__std__', 'range_is_inclusive_lower'),
                    args=[source_path],
                ),
                "inc_upper": qlast.FunctionCall(
                    func=('__std__', 'range_is_inclusive_upper'),
                    args=[source_path],
                ),
                "empty": qlast.FunctionCall(
                    func=('__std__', 'range_is_empty'),
                    args=[source_path],
                ),
            }
        )

        if el_type.contains_json(subctx.env.schema):
            subctx.implicit_limit = 0

        return dispatch.compile(cast, ctx=subctx)


def _cast_multirange(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    assert isinstance(orig_stype, s_types.MultiRange)

    direct_cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)
    if direct_cast is not None:
        return _cast_to_ir(
            ir_set, direct_cast, orig_stype, new_stype, ctx=ctx
        )

    if not new_stype.is_multirange():
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)
    assert isinstance(new_stype, s_types.MultiRange)
    el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]

    el_cast = _find_cast(orig_el_type, el_type, span=span, ctx=ctx)
    if el_cast is None:
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            span=span)

    ctx.env.schema, new_range_type = s_types.Range.from_subtypes(
        ctx.env.schema, [el_type])
    ql_range_type = typegen.type_to_ql_typeref(new_range_type, ctx=ctx)
    with ctx.new() as subctx:
        subctx.allow_factoring()
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        # multirange(
        #     array_agg(
        #         <range<el_type>>multirange_unpack(orig)
        #     )
        # )
        cast = qlast.FunctionCall(
            func=('__std__', 'multirange'),
            args=[
                qlast.FunctionCall(
                    func=('__std__', 'array_agg'),
                    args=[
                        qlast.TypeCast(
                            expr=qlast.FunctionCall(
                                func=('__std__', 'multirange_unpack'),
                                args=[source_path],
                            ),
                            type=ql_range_type,
                        ),
                    ],
                ),
            ],
        )

        if el_type.contains_json(subctx.env.schema):
            subctx.implicit_limit = 0

        return dispatch.compile(cast, ctx=subctx)


def _cast_json_to_range(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Range,
    cardinality_mod: Optional[qlast.CardinalityModifier],
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    with ctx.new() as subctx:
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        check_args: list[qlast.Expr] = [source_path]
        if error_message_context := cast_message_context(subctx):
            check_args.append(qlast.Constant.string(
                json.dumps({
                    "error_message_context": error_message_context
                })
            ))
        check = qlast.FunctionCall(
            func=('__std__', '__range_validate_json'),
            args=check_args
        )

        check_ir = dispatch.compile(check, ctx=subctx)
        source_path = subctx.create_anchor(check_ir, 'b')

        range_el_t = new_stype.get_element_type(ctx.env.schema)
        ql_range_el_t = typegen.type_to_ql_typeref(range_el_t, ctx=subctx)
        bool_t = ctx.env.get_schema_type_and_track(sn.QualName('std', 'bool'))
        ql_bool_t = typegen.type_to_ql_typeref(bool_t, ctx=subctx)

        def compile_with_range_element(
            expr: qlast.Expr,
            element_name: str,
        ) -> irast.Set:
            cast_element = ('range', element_name)
            if subctx.collection_cast_info is not None:
                subctx.collection_cast_info.path_elements.append(cast_element)

            expr_ir = dispatch.compile(expr, ctx=subctx)

            if subctx.collection_cast_info is not None:
                subctx.collection_cast_info.path_elements.pop()

            return expr_ir

        lower: qlast.Expr = qlast.TypeCast(
            expr=qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.Constant.string('lower'),
                ],
            ),
            type=ql_range_el_t,
        )
        lower_ir = compile_with_range_element(lower, 'lower')
        lower = subctx.create_anchor(lower_ir, 'lower')

        upper: qlast.Expr = qlast.TypeCast(
            expr=qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.Constant.string('upper'),
                ],
            ),
            type=ql_range_el_t,
        )
        upper_ir = compile_with_range_element(upper, 'upper')
        upper = subctx.create_anchor(upper_ir, 'upper')

        inc_lower: qlast.Expr = qlast.TypeCast(
            expr=qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.Constant.string('inc_lower'),
                ],
                kwargs={
                    'default': qlast.FunctionCall(
                        func=('__std__', 'to_json'),
                        args=[qlast.Constant.string("true")],
                    ),
                },
            ),
            type=ql_bool_t,
        )
        inc_lower_ir = compile_with_range_element(inc_lower, 'inc_lower')
        inc_lower = subctx.create_anchor(inc_lower_ir, 'inc_lower')

        inc_upper: qlast.Expr = qlast.TypeCast(
            expr=qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.Constant.string('inc_upper'),
                ],
                kwargs={
                    'default': qlast.FunctionCall(
                        func=('__std__', 'to_json'),
                        args=[qlast.Constant.string("false")],
                    ),
                },
            ),
            type=ql_bool_t,
        )
        inc_upper_ir = compile_with_range_element(inc_upper, 'inc_upper')
        inc_upper = subctx.create_anchor(inc_upper_ir, 'inc_upper')

        empty: qlast.Expr = qlast.TypeCast(
            expr=qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.Constant.string('empty'),
                ],
                kwargs={
                    'default': qlast.FunctionCall(
                        func=('__std__', 'to_json'),
                        args=[qlast.Constant.string("false")],
                    ),
                },
            ),
            type=ql_bool_t,
        )
        empty_ir = compile_with_range_element(empty, 'empty')
        empty = subctx.create_anchor(empty_ir, 'empty')

        cast = qlast.FunctionCall(
            func=('__std__', 'range'),
            args=[lower, upper],
            # inc_lower and inc_upper are required to be present for
            # non-empty casts from json, and this is checked in
            # __range_validate_json. We still need to provide default
            # arguments when fetching them, though, since if those
            # arguments to range are {} it will cause {"empty": true}
            # to evaluate to {}.
            kwargs={
                "inc_lower": inc_lower,
                "inc_upper": inc_upper,
                "empty": empty,
            }
        )

        return dispatch.compile(cast, ctx=subctx)


def _cast_json_to_multirange(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.MultiRange,
    cardinality_mod: Optional[qlast.CardinalityModifier],
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    ctx.env.schema, new_range_type = s_types.Range.from_subtypes(
        ctx.env.schema, new_stype.get_subtypes(ctx.env.schema))
    ctx.env.schema, new_array_type = s_types.Array.from_subtypes(
        ctx.env.schema, [new_range_type])
    ql_array_range_type = typegen.type_to_ql_typeref(new_array_type, ctx=ctx)
    with ctx.new() as subctx:
        # We effectively want to do the following:
        # multirange(<array<range<subtype>>>a)
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        cast = qlast.FunctionCall(
            func=('__std__', 'multirange'),
            args=[
                qlast.TypeCast(
                    expr=source_path,
                    type=ql_array_range_type,
                ),
            ],
        )

        return dispatch.compile(cast, ctx=subctx)


def _cast_to_base_array(
    ir_set: irast.Set,
    el_stype: s_scalars.ScalarType,
    orig_stype: s_types.Array,
    ctx: context.ContextLevel,
    cardinality_mod: Optional[qlast.CardinalityModifier]=None
) -> irast.Set:

    base_stype = el_stype.get_base_for_cast(ctx.env.schema)
    assert isinstance(base_stype, s_types.Type)
    ctx.env.schema, new_stype = s_types.Array.from_subtypes(
        ctx.env.schema, [base_stype])

    return _inheritance_cast_to_ir(
        ir_set, orig_stype, new_stype,
        cardinality_mod=cardinality_mod, ctx=ctx)


def _cast_array(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    assert isinstance(orig_stype, s_types.Array)

    direct_cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                span=span)
        assert isinstance(new_stype, s_types.Array)
        el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    elif new_stype.is_json(ctx.env.schema):
        el_type = new_stype
    else:
        # We're casting an array into something that's not an array (e.g. a
        # vector), so we don't need to match element types.
        return _cast_to_ir(
            ir_set, direct_cast, orig_stype, new_stype, ctx=ctx)

    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]

    el_cast = _find_cast(orig_el_type, el_type, span=span, ctx=ctx)

    if el_cast is not None and el_cast.get_from_cast(ctx.env.schema):
        # Simple cast
        return _cast_to_ir(
            ir_set, el_cast, orig_stype, new_stype, ctx=ctx)
    else:
        with ctx.new() as subctx:
            subctx.allow_factoring()

            subctx.anchors = subctx.anchors.copy()
            source_path = subctx.create_anchor(ir_set, 'a')

            unpacked = qlast.FunctionCall(
                func=('__std__', 'array_unpack'),
                args=[source_path],
            )

            enumerated = dispatch.compile(
                qlast.FunctionCall(
                    func=('__std__', 'enumerate'),
                    args=[unpacked],
                ),
                ctx=subctx,
            )

            enumerated_ref = subctx.create_anchor(enumerated, 'e')

            elements = qlast.FunctionCall(
                func=('__std__', 'array_agg'),
                args=[
                    qlast.SelectQuery(
                        result=qlast.TypeCast(
                            expr=astutils.extend_path(enumerated_ref, '1'),
                            type=typegen.type_to_ql_typeref(
                                el_type,
                                ctx=subctx,
                            ),
                            cardinality_mod=qlast.CardinalityModifier.Required,
                            span=span,
                        ),
                        orderby=[
                            qlast.SortExpr(
                                path=astutils.extend_path(enumerated_ref, '0'),
                                direction=qlast.SortOrder.Asc,
                            ),
                        ],
                    ),
                ],
            )

            # Force the elements to be correlated with whatever the
            # anchor was. (Doing it this way ensures a NULL check,
            # and just registering it in the scope would not.)
            correlated_elements = astutils.extend_path(
                qlast.Tuple(elements=[source_path, elements]), '1'
            )
            correlated_query = qlast.SelectQuery(result=correlated_elements)

            if el_type.contains_json(subctx.env.schema):
                subctx.implicit_limit = 0

            array_ir = dispatch.compile(correlated_query, ctx=subctx)
            assert isinstance(array_ir, irast.Set)

            if direct_cast is not None:
                ctx.env.schema, array_stype = s_types.Array.from_subtypes(
                    ctx.env.schema, [el_type])
                return _cast_to_ir(
                    array_ir, direct_cast, array_stype, new_stype, ctx=ctx
                )
            else:
                return array_ir


def _cast_array_literal(
    ir_set: irast.Set,
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    span: Optional[parsing.Span],
    ctx: context.ContextLevel,
) -> irast.Set:

    assert isinstance(ir_set.expr, irast.Array)

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    direct_cast = _find_cast(orig_stype, new_stype, span=span, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                span=span) from None
        assert isinstance(new_stype, s_types.Array)
        el_type = new_stype.get_subtypes(ctx.env.schema)[0]
        intermediate_stype = orig_stype

    else:
        el_type = new_stype
        ctx.env.schema, intermediate_stype = s_types.Array.from_subtypes(
            ctx.env.schema, [el_type])

    intermediate_typeref = typegen.type_to_typeref(
        intermediate_stype, env=ctx.env)
    casted_els = []
    for el in ir_set.expr.elements:
        el = compile_cast(el, el_type,
                          cardinality_mod=qlast.CardinalityModifier.Required,
                          ctx=ctx, span=span)
        casted_els.append(el)

    new_array = setgen.ensure_set(
        irast.Array(elements=casted_els, typeref=intermediate_typeref),
        ctx=ctx)

    if direct_cast is not None:
        return _cast_to_ir(
            new_array, direct_cast, intermediate_stype, new_stype, ctx=ctx)

    else:
        cast_ir = irast.TypeCast(
            expr=new_array,
            from_type=orig_typeref,
            to_type=new_typeref,
            sql_cast=True,
            sql_expr=False,
            span=span,
            error_message_context=cast_message_context(ctx),
        )

    return setgen.ensure_set(cast_ir, ctx=ctx)


def _cast_enum_str_immutable(
    ir_expr: Union[irast.Set, irast.Expr],
    orig_stype: s_types.Type,
    new_stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    """
    Compiles cast between an enum and std::str
    under the assumption that this expression must be immutable.
    """

    if new_stype.is_enum(ctx.env.schema):
        enum_stype = new_stype
        suffix = "_from_str"
    else:
        enum_stype = orig_stype
        suffix = "_into_str"

    name: s_name.Name = enum_stype.get_name(ctx.env.schema)
    name = cast(s_name.QualName, name)
    cast_name = s_name.QualName(
        module=name.module, name=str(enum_stype.id) + suffix
    )

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)

    cast_ir = irast.TypeCast(
        expr=setgen.ensure_set(ir_expr, ctx=ctx),
        from_type=orig_typeref,
        to_type=new_typeref,
        cardinality_mod=None,
        cast_name=cast_name,
        sql_function=None,
        sql_cast=False,
        sql_expr=True,
        error_message_context=cast_message_context(ctx),
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


def _find_object_by_id(
    ir_expr: Union[irast.Set, irast.Expr],
    new_stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    with ctx.new() as subctx:
        subctx.anchors = subctx.anchors.copy()

        ir_set = setgen.ensure_set(ir_expr, ctx=subctx)
        uuid_anchor = subctx.create_anchor(ir_set, name='a')

        object_name = s_utils.name_to_ast_ref(
            new_stype.get_name(ctx.env.schema)
        )

        select_id = qlast.SelectQuery(
            result=qlast.DetachedExpr(expr=qlast.Path(steps=[object_name])),
            where=qlast.BinOp(
                left=qlast.Path(
                    steps=[qlast.Ptr(name='id', direction='>')],
                    partial=True,
                ),
                op='=',
                right=qlast.Path(steps=[qlast.ObjectRef(name='_id')]),
            ),
        )

        error_message = qlast.BinOp(
            left=qlast.Constant.string(
                value=(
                    repr(new_stype.get_displayname(ctx.env.schema))
                    + ' with id \''
                )
            ),
            op='++',
            right=qlast.BinOp(
                left=qlast.TypeCast(
                    expr=qlast.Path(steps=[qlast.ObjectRef(name='_id')]),
                    type=qlast.TypeName(maintype=qlast.ObjectRef(name='str')),
                ),
                op='++',
                right=qlast.Constant.string('\' does not exist'),
            ),
        )

        exists_ql = qlast.FunctionCall(
            func='assert_exists',
            args=[select_id],
            kwargs={'message': error_message},
        )

        for_query = qlast.ForQuery(
            iterator=uuid_anchor, iterator_alias='_id', result=exists_ql
        )

        return dispatch.compile(for_query, ctx=subctx)


def cast_message_context(ctx: context.ContextLevel) -> Optional[str]:
    if (
        ctx.collection_cast_info is not None
        and ctx.collection_cast_info.path_elements
    ):
        from_name = (
            ctx.collection_cast_info.from_type.get_displayname(ctx.env.schema)
        )
        to_name = (
            ctx.collection_cast_info.to_type.get_displayname(ctx.env.schema)
        )
        path_msg = ''.join(
            _collection_element_message_context(path_element)
            for path_element in ctx.collection_cast_info.path_elements
        )
        return (
            f"while casting '{from_name}' to '{to_name}', {path_msg}"
        )
    else:
        return None


def _collection_element_message_context(
    path_element: Tuple[str, Optional[str]]
) -> str:
    if path_element[0] == 'tuple':
        return f"at tuple element '{path_element[1]}', "
    elif path_element[0] == 'array':
        return f'in array elements, '
    elif path_element[0] == 'range':
        return f"in range parameter '{path_element[1]}', "
    else:
        raise NotImplementedError
