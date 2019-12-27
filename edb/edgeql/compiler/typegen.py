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

from typing import *  # NoQA

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import abc as s_abc
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types

from edb.edgeql import ast as qlast

from . import astutils
from . import context
from . import dispatch
from . import schemactx
from . import setgen


def type_to_ql_typeref(
    t: s_types.Type,
    *,
    _name: Optional[str] = None,
    ctx: context.ContextLevel,
) -> qlast.TypeName:

    return astutils.type_to_ql_typeref(t, schema=ctx.env.schema)


def ql_typeexpr_to_ir_typeref(
        ql_t: qlast.TypeExpr, *,
        ctx: context.ContextLevel) -> irast.TypeRef:

    stype = ql_typeexpr_to_type(ql_t, ctx=ctx)
    return irtyputils.type_to_typeref(
        ctx.env.schema, stype, cache=ctx.env.type_ref_cache
    )


def ql_typeexpr_to_type(
        ql_t: qlast.TypeExpr, *,
        ctx: context.ContextLevel) -> s_types.Type:

    types = _ql_typeexpr_to_type(ql_t, ctx=ctx)
    if len(types) > 1:
        return schemactx.get_union_type(types, ctx=ctx)
    else:
        return types[0]


def _ql_typeexpr_to_type(
        ql_t: qlast.TypeExpr, *,
        ctx: context.ContextLevel) -> List[s_types.Type]:

    if isinstance(ql_t, qlast.TypeOf):
        with ctx.newscope(fenced=True, temporary=True) as subctx:
            ir_set = setgen.ensure_set(dispatch.compile(ql_t.expr, ctx=subctx),
                                       ctx=subctx)
            stype = setgen.get_set_type(ir_set, ctx=subctx)

        return [stype]

    elif isinstance(ql_t, qlast.TypeOp):
        if ql_t.op == '|':
            return (_ql_typeexpr_to_type(ql_t.left, ctx=ctx) +
                    _ql_typeexpr_to_type(ql_t.right, ctx=ctx))

        raise errors.UnsupportedFeatureError(
            f'type operator {ql_t.op!r} is not implemented',
            context=ql_t.context)

    elif isinstance(ql_t, qlast.TypeName):
        return [_ql_typename_to_type(ql_t, ctx=ctx)]

    else:
        raise errors.InternalServerError(f'unexpected TypeExpr: {ql_t!r}')


def _ql_typename_to_type(
        ql_t: qlast.TypeName, *,
        ctx: context.ContextLevel) -> s_types.Type:
    if ql_t.subtypes:
        assert isinstance(ql_t.maintype, qlast.ObjectRef)
        coll = s_types.Collection.get_class(ql_t.maintype.name)

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

            return coll.from_subtypes(
                ctx.env.schema, t_subtypes, {'named': named})
        else:
            a_subtypes = []
            for st in ql_t.subtypes:
                a_subtypes.append(ql_typeexpr_to_type(st, ctx=ctx))

            return coll.from_subtypes(ctx.env.schema, a_subtypes)
    else:
        return schemactx.get_schema_type(ql_t.maintype, ctx=ctx)


@overload
def ptrcls_from_ptrref(  # NoQA: F811
    ptrref: irast.PointerRef, *,
    ctx: context.ContextLevel,
) -> s_pointers.Pointer:
    ...


@overload
def ptrcls_from_ptrref(  # NoQA: F811
    ptrref: irast.TupleIndirectionPointerRef, *,
    ctx: context.ContextLevel,
) -> irast.TupleIndirectionLink:
    ...


@overload
def ptrcls_from_ptrref(  # NoQA: F811
    ptrref: irast.TypeIntersectionPointerRef, *,
    ctx: context.ContextLevel,
) -> irast.TypeIntersectionLink:
    ...


@overload
def ptrcls_from_ptrref(  # NoQA: F811
    ptrref: irast.BasePointerRef, *,
    ctx: context.ContextLevel,
) -> s_pointers.PointerLike:
    ...


def ptrcls_from_ptrref(  # NoQA: F811
    ptrref: irast.BasePointerRef, *,
    ctx: context.ContextLevel,
) -> s_pointers.PointerLike:

    cached = ctx.env.ptr_ref_cache.get_ptrcls_for_ref(ptrref)
    if cached is not None:
        return cached

    return irtyputils.ptrcls_from_ptrref(ptrref, schema=ctx.env.schema)


def collapse_type_intersection_rptr(
    ir_set: irast.Set, *,
    ctx: context.ContextLevel,
) -> Tuple[irast.Set, List[s_pointers.Pointer]]:

    ind_prefix, ind_ptrs = irutils.collapse_type_intersection(ir_set)
    if not ind_ptrs:
        return ir_set, []

    rptr_specialization: Set[irast.PointerRef] = set()
    for ind_ptr in ind_ptrs:
        for ind_ptr in ind_ptrs:
            rptr_specialization.update(
                ind_ptr.ptrref.rptr_specialization)

    ptrs = [ptrcls_from_ptrref(ptrref, ctx=ctx)
            for ptrref in rptr_specialization]

    return ind_prefix, ptrs


def type_to_typeref(
    t: s_types.Type, env: context.Environment
) -> irast.TypeRef:
    schema = env.schema
    cache = env.type_ref_cache
    return irtyputils.type_to_typeref(schema, t, cache=cache)
