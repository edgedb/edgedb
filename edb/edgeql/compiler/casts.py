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

from typing import *

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
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel,
        cardinality_mod: Optional[qlast.CardinalityModifier]=None
) -> irast.Set:

    if isinstance(ir_expr, irast.EmptySet):
        # For the common case of casting an empty set, we simply
        # generate a new EmptySet node of the requested type.
        return setgen.new_empty_set(
            stype=new_stype,
            alias=ir_expr.path_id.target_name_hint.name,
            ctx=ctx,
            srcctx=ir_expr.context)

    if irutils.is_untyped_empty_array_expr(ir_expr):
        # Ditto for empty arrays.
        new_typeref = typegen.type_to_typeref(new_stype, ctx.env)
        return setgen.ensure_set(
            irast.Array(elements=[], typeref=new_typeref), ctx=ctx)

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    orig_stype = setgen.get_set_type(ir_set, ctx=ctx)

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
            context=srcctx)

    uuid_t = ctx.env.get_schema_type_and_track(sn.QualName('std', 'uuid'))
    if (
        orig_stype.issubclass(ctx.env.schema, uuid_t)
        and new_stype.is_object_type()
    ):
        return _find_object_by_id(ir_expr, new_stype, ctx=ctx)

    json_t = ctx.env.get_schema_type_and_track(
        sn.QualName('std', 'json'))

    if isinstance(ir_set.expr, irast.Array):
        return _cast_array_literal(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if orig_stype.is_tuple(ctx.env.schema):
        return _cast_tuple(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if orig_stype.is_array() and not s_types.is_type_compatible(
        orig_stype, new_stype, schema=ctx.env.schema
    ):
        return _cast_array(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if orig_stype.is_range() and not s_types.is_type_compatible(
        orig_stype, new_stype, schema=ctx.env.schema
    ):
        return _cast_range(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

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
        with ctx.new() as subctx:
            subctx.implicit_id_in_shapes = False
            subctx.implicit_tid_in_shapes = False
            subctx.implicit_tname_in_shapes = False
            viewgen.late_compile_view_shapes(ir_set, ctx=subctx)
    else:
        if orig_stype.issubclass(ctx.env.schema, json_t) and new_stype.is_enum(
            ctx.env.schema
        ):
            # Casts from json to enums need some special handling
            # here, where we have access to the enum type. Just turn
            # it into json->str and str->enum.
            str_typ = ctx.env.get_schema_type_and_track(sn.QualName('std', 'str'))
            str_ir = compile_cast(ir_expr, str_typ, srcctx=srcctx, ctx=ctx)
            return compile_cast(
                str_ir,
                new_stype,
                cardinality_mod=cardinality_mod,
                srcctx=srcctx,
                ctx=ctx,
            )

        if (
            orig_stype.issubclass(ctx.env.schema, json_t)
            and isinstance(new_stype, s_types.Array)
            and not new_stype.get_subtypes(ctx.env.schema)[0].issubclass(
                ctx.env.schema, json_t
            )
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
                srcctx=srcctx,
                ctx=ctx,
            )
            return compile_cast(
                json_array_ir, new_stype, srcctx=srcctx, ctx=ctx
            )

        if orig_stype.issubclass(ctx.env.schema, json_t) and isinstance(
            new_stype, s_types.Tuple
        ):
            return _cast_json_to_tuple(
                ir_set,
                orig_stype,
                new_stype,
                cardinality_mod,
                srcctx=srcctx,
                ctx=ctx,
            )

        if orig_stype.issubclass(ctx.env.schema, json_t) and isinstance(
            new_stype, s_types.Range
        ):
            return _cast_json_to_range(
                ir_set,
                orig_stype,
                new_stype,
                cardinality_mod,
                srcctx=srcctx,
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
        srcctx=srcctx,
        ctx=ctx,
    )


def _has_common_concrete_scalar(
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        ctx: context.ContextLevel) -> bool:
    schema = ctx.env.schema
    return bool(
        isinstance(orig_stype, s_scalars.ScalarType)
        and isinstance(new_stype, s_scalars.ScalarType)
        and (orig_base := orig_stype.maybe_get_topmost_concrete_base(schema))
        and (new_base := new_stype.maybe_get_topmost_concrete_base(schema))
        and orig_base == new_base
    )


def _compile_cast(
        ir_expr: Union[irast.Set, irast.Expr],
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel,
        cardinality_mod: Optional[qlast.CardinalityModifier]) -> irast.Set:

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if cast is None:
        raise errors.QueryError(
            f'cannot cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} to '
            f'{new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx or ir_set.context)

    return _cast_to_ir(ir_set, cast, orig_stype, new_stype,
                       cardinality_mod, ctx=ctx)


def _cast_to_ir(
        ir_set: irast.Set,
        cast: s_casts.Cast,
        orig_stype: s_types.Type,
        new_stype: s_types.Type,
        cardinality_mod: Optional[qlast.CardinalityModifier]=None,
        *,
        ctx: context.ContextLevel) -> irast.Set:

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
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


def _inheritance_cast_to_ir(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type,
        *,
        cardinality_mod: Optional[qlast.CardinalityModifier],
        ctx: context.ContextLevel) -> irast.Set:

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
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> Optional[s_casts.Cast]:

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

    dummy_set = irast.EmptySet()  # type: ignore
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
            context=srcctx)
    else:
        return None


def _cast_json_to_tuple(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Tuple,
        cardinality_mod: Optional[qlast.CardinalityModifier],
        *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    with ctx.new() as subctx:
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')

        # Top-level json->tuple casts should produce an empty set on
        # null inputs, but error on missing fields or null
        # subelements, so filter out json nulls directly here to
        # distinguish those cases.
        if cardinality_mod != qlast.CardinalityModifier.Required:
            pathctx.register_set_in_scope(ir_set, ctx=subctx)

            check = qlast.FunctionCall(
                func=('__std__', 'json_typeof'), args=[source_path]
            )
            filtered = qlast.SelectQuery(
                result=source_path,
                where=qlast.BinOp(
                    left=check,
                    op='!=',
                    right=qlast.StringConstant(value='null'),
                )
            )
            filtered_ir = dispatch.compile(filtered, ctx=subctx)
            source_path = subctx.create_anchor(filtered_ir, 'a')

        # TODO: try using jsonb_to_record instead of a bunch of
        # json_get calls and see if that is faster.
        elements = []
        for new_el_name, new_st in new_stype.iter_subtypes(ctx.env.schema):
            val_e = qlast.FunctionCall(
                func=('__std__', 'json_get'),
                args=[
                    source_path,
                    qlast.StringConstant(value=new_el_name),
                ],
            )

            val = dispatch.compile(val_e, ctx=subctx)

            val = compile_cast(
                val, new_st,
                cardinality_mod=qlast.CardinalityModifier.Required,
                ctx=subctx, srcctx=srcctx)

            elements.append(irast.TupleElement(name=new_el_name, val=val))

        return setgen.new_tuple_set(
            elements,
            named=new_stype.is_named(ctx.env.schema),
            ctx=subctx,
        )


def _cast_tuple(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(orig_stype, s_types.Tuple)

    # Make sure the source tuple expression is pinned in the scope,
    # so that we don't generate a cross-product of it by evaluating
    # the tuple indirections.
    pathctx.register_set_in_scope(ir_set, ctx=ctx)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)
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
            val = compile_cast(val, new_stype, ctx=ctx, srcctx=srcctx)

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
            context=srcctx)

    assert isinstance(new_stype, s_types.Tuple)
    new_subtypes = list(new_stype.iter_subtypes(ctx.env.schema))
    if len(orig_subtypes) != len(new_subtypes):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}: '
            f'the number of elements is not the same',
            context=srcctx)

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
            val = compile_cast(val, new_st, ctx=ctx, srcctx=srcctx)

        elements.append(irast.TupleElement(name=new_el_name, val=val))

    return setgen.new_tuple_set(
        elements,
        named=new_stype.is_named(ctx.env.schema),
        ctx=ctx,
    )


def _cast_range(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(orig_stype, s_types.Range)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)
    if direct_cast is not None:
        return _cast_to_ir(
            ir_set, direct_cast, orig_stype, new_stype, ctx=ctx
        )

    if not new_stype.is_range():
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)
    assert isinstance(new_stype, s_types.Range)
    el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]
    ql_el_type = typegen.type_to_ql_typeref(el_type, ctx=ctx)

    el_cast = _find_cast(orig_el_type, el_type, srcctx=srcctx, ctx=ctx)
    if el_cast is None:
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)

    with ctx.new() as subctx:
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
            subctx.inhibit_implicit_limit = True

        return dispatch.compile(cast, ctx=subctx)


def _cast_json_to_range(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Range,
        cardinality_mod: Optional[qlast.CardinalityModifier],
        *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    with ctx.new() as subctx:
        subctx.anchors = subctx.anchors.copy()
        source_path = subctx.create_anchor(ir_set, 'a')
        check = qlast.FunctionCall(
            func=('__std__', '__range_validate_json'), args=[source_path]
        )
        check_ir = dispatch.compile(check, ctx=subctx)
        source_path = subctx.create_anchor(check_ir, 'b')

        range_el_t = new_stype.get_element_type(ctx.env.schema)
        ql_range_el_t = typegen.type_to_ql_typeref(range_el_t, ctx=subctx)
        bool_t = ctx.env.get_schema_type_and_track(sn.QualName('std', 'bool'))
        ql_bool_t = typegen.type_to_ql_typeref(bool_t, ctx=subctx)

        cast = qlast.FunctionCall(
            func=('__std__', 'range'),
            args=[
                qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'json_get'),
                        args=[
                            source_path,
                            qlast.StringConstant(value='lower'),
                        ],
                    ),
                    type=ql_range_el_t,
                ),
                qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'json_get'),
                        args=[
                            source_path,
                            qlast.StringConstant(value='upper'),
                        ],
                    ),
                    type=ql_range_el_t,
                ),
            ],
            # inc_lower and inc_upper are required to be present for
            # non-empty casts from json, and this is checked in
            # __range_validate_json. We still need to provide default
            # arguments when fetching them, though, since if those
            # arguments to range are {} it will cause {"empty": true}
            # to evaluate to {}.
            kwargs={
                "inc_lower": qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'json_get'),
                        args=[
                            source_path,
                            qlast.StringConstant(value='inc_lower'),
                        ],
                        kwargs={
                            'default': qlast.FunctionCall(
                                func=('__std__', 'to_json'),
                                args=[qlast.StringConstant(value="true")],
                            ),
                        },
                    ),
                    type=ql_bool_t
                ),
                "inc_upper": qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'json_get'),
                        args=[
                            source_path,
                            qlast.StringConstant(value='inc_upper'),
                        ],
                        kwargs={
                            'default': qlast.FunctionCall(
                                func=('__std__', 'to_json'),
                                args=[qlast.StringConstant(value="false")],
                            ),
                        },
                    ),
                    type=ql_bool_t
                ),
                "empty": qlast.TypeCast(
                    expr=qlast.FunctionCall(
                        func=('__std__', 'json_get'),
                        args=[
                            source_path,
                            qlast.StringConstant(value='empty'),
                        ],
                        kwargs={
                            'default': qlast.FunctionCall(
                                func=('__std__', 'to_json'),
                                args=[qlast.StringConstant(value="false")],
                            ),
                        },
                    ),
                    type=ql_bool_t
                ),
            }
        )

        return dispatch.compile(cast, ctx=subctx)


def _cast_array(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(orig_stype, s_types.Array)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx)
        assert isinstance(new_stype, s_types.Array)
        el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    else:
        el_type = new_stype

    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]

    el_cast = _find_cast(orig_el_type, el_type, srcctx=srcctx, ctx=ctx)

    if el_cast is not None and el_cast.get_from_cast(ctx.env.schema):
        # Simple cast
        return _cast_to_ir(
            ir_set, el_cast, orig_stype, new_stype, ctx=ctx)
    else:
        with ctx.new() as subctx:
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

            if el_type.contains_json(subctx.env.schema):
                subctx.inhibit_implicit_limit = True

            array_ir = dispatch.compile(correlated_elements, ctx=subctx)
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
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(ir_set.expr, irast.Array)

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx) from None
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
                          ctx=ctx, srcctx=srcctx)
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
                    steps=[
                        qlast.Ptr(
                            ptr=qlast.ObjectRef(name='id'), direction='>'
                        )
                    ],
                    partial=True,
                ),
                op='=',
                right=qlast.Path(steps=[qlast.ObjectRef(name='_id')]),
            ),
        )

        error_message = qlast.BinOp(
            left=qlast.StringConstant(
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
                right=qlast.StringConstant(value='\' does not exist'),
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
