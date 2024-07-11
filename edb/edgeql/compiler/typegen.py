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


"""EdgeQL compiler type-related helpers."""


from __future__ import annotations

from typing import Optional, Tuple, Sequence, List, Set, cast, overload

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import abc as s_abc
from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast

from . import context
from . import dispatch
from . import schemactx
from . import setgen


def amend_empty_set_type(
    es: irast.SetE[irast.EmptySet],
    t: s_types.Type,
    env: context.Environment
) -> None:
    env.set_types[es] = t
    alias = es.path_id.target_name_hint.name
    typename = s_name.QualName(module='__derived__', name=alias)
    es.path_id = irast.PathId.from_type(
        env.schema, t, env=env, typename=typename,
        namespace=es.path_id.namespace,
    )


def infer_common_type(
    irs: Sequence[irast.Set], env: context.Environment
) -> Optional[s_types.Type]:
    if not irs:
        raise errors.QueryError(
            'cannot determine common type of an empty set',
            span=irs[0].span)

    types = []
    empties = []

    seen_object = False
    seen_scalar = False
    seen_coll = False

    for i, arg in enumerate(irs):
        if (
            isinstance(arg.expr, irast.EmptySet)
            and env.set_types[arg] is None
        ):
            empties.append(i)
            continue

        t = env.set_types[arg]
        if isinstance(t, s_abc.Collection):
            seen_coll = True
        elif isinstance(t, s_scalars.ScalarType):
            seen_scalar = True
        else:
            seen_object = True
        types.append(t)

    if seen_coll + seen_scalar + seen_object > 1:
        raise errors.QueryError(
            'cannot determine common type',
            span=irs[0].span)

    if not types:
        raise errors.QueryError(
            'cannot determine common type of an empty set',
            span=irs[0].span)

    common_type = None
    if seen_scalar or seen_coll:
        it = iter(types)
        common_type = next(it)
        while True:
            next_type = next(it, None)
            if next_type is None:
                break
            env.schema, common_type = (
                common_type.find_common_implicitly_castable_type(
                    next_type,
                    env.schema,
                )
            )
            if common_type is None:
                break
    else:
        common_types = s_utils.get_class_nearest_common_ancestors(
            env.schema,
            cast(Sequence[s_types.InheritingType], types),
        )
        # We arbitrarily select the first nearest common ancestor
        common_type = common_types[0] if common_types else None

    if common_type is None:
        return None

    for i in empties:
        amend_empty_set_type(
            cast(irast.SetE[irast.EmptySet], irs[i]), common_type, env)

    return common_type


def type_to_ql_typeref(
    t: s_types.Type,
    *,
    _name: Optional[str] = None,
    ctx: context.ContextLevel,
) -> qlast.TypeExpr:
    return s_utils.typeref_to_ast(
        ctx.env.schema,
        t,
        disambiguate_std='std' in ctx.modaliases,
    )


def ql_typeexpr_to_ir_typeref(
    ql_t: qlast.TypeExpr, *, ctx: context.ContextLevel
) -> irast.TypeRef:

    stype = ql_typeexpr_to_type(ql_t, ctx=ctx)
    return irtyputils.type_to_typeref(
        ctx.env.schema, stype, cache=ctx.env.type_ref_cache
    )


def ql_typeexpr_to_type(
    ql_t: qlast.TypeExpr, *, ctx: context.ContextLevel
) -> s_types.Type:

    (op, _, types) = (
        _ql_typeexpr_get_types(ql_t, ctx=ctx)
    )
    return _ql_typeexpr_combine_types(op, types, ctx=ctx)


def _ql_typeexpr_combine_types(
        op: Optional[str], types: List[s_types.Type], *,
        ctx: context.ContextLevel
) -> s_types.Type:
    if len(types) == 1:
        return types[0]
    elif op == '|':
        return schemactx.get_union_type(types, ctx=ctx)
    elif op == '&':
        return schemactx.get_intersection_type(types, ctx=ctx)
    else:
        raise errors.InternalServerError('This should never happen')


def _ql_typeexpr_get_types(
    ql_t: qlast.TypeExpr, *, ctx: context.ContextLevel
) -> Tuple[Optional[str], bool, List[s_types.Type]]:

    if isinstance(ql_t, qlast.TypeOf):
        with ctx.new() as subctx:
            # Use an empty scope tree, to avoid polluting things pointlessly
            subctx.path_scope = irast.ScopeTreeNode()
            subctx.expr_exposed = context.Exposure.UNEXPOSED
            orig_rewrites = ctx.env.type_rewrites.copy()
            ir_set = dispatch.compile(ql_t.expr, ctx=subctx)
            stype = setgen.get_set_type(ir_set, ctx=subctx)
            ctx.env.type_rewrites = orig_rewrites

        return (None, True, [stype])

    elif isinstance(ql_t, qlast.TypeOp):
        if ql_t.op in ['|', '&']:
            (left_op, left_leaf, left_types) = (
                _ql_typeexpr_get_types(ql_t.left, ctx=ctx)
            )
            (right_op, right_leaf, right_types) = (
                _ql_typeexpr_get_types(ql_t.right, ctx=ctx)
            )

            # We need to validate that type ops are applied only to
            # object types. So we check the base case here, when the
            # left or right operand is a single type, because if it's
            # a longer list, then we know that it was already composed
            # of "|" or "&", or it is the result of inference by
            # "typeof" and is a list of object types anyway.
            if left_leaf and not left_types[0].is_object_type():
                raise errors.UnsupportedFeatureError(
                    f'cannot use type operator {ql_t.op!r} with non-object '
                    f'type {left_types[0].get_displayname(ctx.env.schema)}',
                    span=ql_t.left.span)
            if right_leaf and not right_types[0].is_object_type():
                raise errors.UnsupportedFeatureError(
                    f'cannot use type operator {ql_t.op!r} with non-object '
                    f'type {right_types[0].get_displayname(ctx.env.schema)}',
                    span=ql_t.right.span)

            # if an operand is either a single type or uses the same operator,
            # flatten it into the result types list.
            # if an operand has a different operator is used, its types should
            # be combined into a new type before appending to the result types.
            types: List[s_types.Type] = []
            types += (
                left_types
                if left_op is None or left_op == ql_t.op else
                [_ql_typeexpr_combine_types(left_op, left_types, ctx=ctx)]
            )
            types += (
                right_types
                if right_op is None or right_op == ql_t.op else
                [_ql_typeexpr_combine_types(right_op, right_types, ctx=ctx)]
            )

            return (ql_t.op, False, types)

        raise errors.UnsupportedFeatureError(
            f'type operator {ql_t.op!r} is not implemented',
            span=ql_t.span)

    elif isinstance(ql_t, qlast.TypeName):
        return (None, True, [_ql_typename_to_type(ql_t, ctx=ctx)])

    else:
        raise errors.EdgeQLSyntaxError("Unexpected type expression",
                                       span=ql_t.span)


def _ql_typename_to_type(
    ql_t: qlast.TypeName, *, ctx: context.ContextLevel
) -> s_types.Type:
    if ql_t.subtypes:
        assert isinstance(ql_t.maintype, qlast.ObjectRef)
        coll = s_types.Collection.get_class(ql_t.maintype.name)
        ct: s_types.Type

        if issubclass(coll, s_abc.Tuple):
            t_subtypes = {}
            named = False
            for si, st in enumerate(ql_t.subtypes):
                if st.name:
                    named = True
                    type_name = st.name
                else:
                    type_name = str(si)

                t_subtypes[type_name] = ql_typeexpr_to_type(st, ctx=ctx)

            ctx.env.schema, ct = coll.from_subtypes(
                ctx.env.schema, t_subtypes, {'named': named})
            return ct
        else:
            a_subtypes = []
            for st in ql_t.subtypes:
                a_subtypes.append(ql_typeexpr_to_type(st, ctx=ctx))

            ctx.env.schema, ct = coll.from_subtypes(ctx.env.schema, a_subtypes)
            return ct
    else:
        return schemactx.get_schema_type(ql_t.maintype, ctx=ctx)


@overload
def ptrcls_from_ptrref(
    ptrref: irast.PointerRef,
    *,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:
    ...


@overload
def ptrcls_from_ptrref(
    ptrref: irast.TupleIndirectionPointerRef,
    *,
    ctx: context.ContextLevel,
) -> irast.TupleIndirectionLink:
    ...


@overload
def ptrcls_from_ptrref(
    ptrref: irast.TypeIntersectionPointerRef,
    *,
    ctx: context.ContextLevel,
) -> irast.TypeIntersectionLink:
    ...


@overload
def ptrcls_from_ptrref(
    ptrref: irast.BasePointerRef,
    *,
    ctx: context.ContextLevel,
) -> s_pointers.PointerLike:
    ...


def ptrcls_from_ptrref(
    ptrref: irast.BasePointerRef,
    *,
    ctx: context.ContextLevel,
) -> s_pointers.PointerLike:

    cached = ctx.env.ptr_ref_cache.get_ptrcls_for_ref(ptrref)
    if cached is not None:
        return cached

    ctx.env.schema, ptr = irtyputils.ptrcls_from_ptrref(
        ptrref, schema=ctx.env.schema)

    return ptr


def ptr_to_ptrref(
    ptrcls: s_pointers.Pointer,
    *,
    ctx: context.ContextLevel,
) -> irast.PointerRef:
    return irtyputils.ptrref_from_ptrcls(
        schema=ctx.env.schema,
        ptrcls=ptrcls,
        cache=ctx.env.ptr_ref_cache,
        typeref_cache=ctx.env.type_ref_cache,
    )


def collapse_type_intersection_rptr(
    ir_set: irast.Set,
    *,
    ctx: context.ContextLevel,
) -> Tuple[irast.Set, List[s_pointers.Pointer]]:

    ind_prefix, ind_ptrs = irutils.collapse_type_intersection(ir_set)
    if not ind_ptrs:
        return ir_set, []

    rptr_specialization: Set[irast.PointerRef] = set()
    for ind_ptr in ind_ptrs:
        for ind_ptr in ind_ptrs:
            if ind_ptr.ptrref.rptr_specialization:
                rptr_specialization.update(
                    ind_ptr.ptrref.rptr_specialization)
            elif (
                not ind_ptr.ptrref.is_empty
                and isinstance(ind_ptr.source.expr, irast.Pointer)
            ):
                assert isinstance(ind_ptr.source.expr.ptrref, irast.PointerRef)
                rptr_specialization.add(ind_ptr.source.expr.ptrref)

    ptrs = [ptrcls_from_ptrref(ptrref, ctx=ctx)
            for ptrref in rptr_specialization]

    return ind_prefix, ptrs


def type_from_typeref(
    t: irast.TypeRef,
    env: context.Environment,
) -> s_types.Type:
    env.schema, styp = irtyputils.ir_typeref_to_type(env.schema, t)
    return styp


def type_to_typeref(
    t: s_types.Type,
    env: context.Environment,
) -> irast.TypeRef:
    schema = env.schema
    cache = env.type_ref_cache
    expr_type = t.get_expr_type(env.schema)
    include_children = (
        expr_type is s_types.ExprType.Update
        or expr_type is s_types.ExprType.Delete
        or isinstance(t, s_objtypes.ObjectType)
    )
    include_ancestors = (
        expr_type is s_types.ExprType.Insert
        or expr_type is s_types.ExprType.Update
        or expr_type is s_types.ExprType.Delete
    )
    return irtyputils.type_to_typeref(
        schema,
        t,
        include_children=include_children,
        include_ancestors=include_ancestors,
        cache=cache,
    )
