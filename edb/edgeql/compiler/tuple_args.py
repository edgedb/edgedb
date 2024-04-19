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


"""Implementation of tuple argument decoding compiler.

Postgres does not support passing records (tuples in edgeql) as query
parameters, and so we need to go to some length to work around this.

All of the trickyness here comes from the interaction with arrays;
without arrays, we could just split a tuple into multiple parameters.
The singly-nested case is also still fairly simple: turn an array of
tuples into multiple parallel arrays, such that `array<tuple<str, int64>>`
becomes `array<str>` and `array<int64>`.

The doubly-nested case, in which the tuple itself contains an array
(for example, `array<tuple<str, array<int64>>>`), is trickier:
Postgres does not allow nested arrays (except if there is an intervening
record type).

The key insight to resolve this dilemma is that a nested array type
`array<array<T>>` can be transformed into two non-nested arrays with
types `array<T>` and `array<int32>`, where the `array<T>` contains all
of the elements of the nested arrays flattened out and the
`array<int>` contains the indexes into the flattened array indicating where
each of the nested arrays begins (followed by the length of the flattened
array, so that pairs of adjacent elements form the slice indexes into
the flattened array).

As an example, consider a parameter of type `array<tuple<str, array<int64>>>`,
with the value:
[
    ('foo', [100]),
    ('bar', [101, 102]),
    ('baz', [103, 104, 105]),
]
We will encode this into three arguments, with types `array<str>`,
`array<int32>`, and `array<int64>`, with values:
  ['foo', 'bar', 'baz']
  [0, 1, 3, 6]
  [100, 101, 102, 103, 104, 105]


The encoding algorithm is straightforward: we traverse the type and
the input data in tandem, appending data into the appropriate argument
arrays and tracking array lengths. This is implemented our cython
protocol server, in edb.server.protocol.args_ser, and operates directly
on the wire encodings.

The decoding needs to be done as part of the SQL query we execute, so
we generate an EdgeQL query that decodes to the proper type. The generated
code operates in a top-down manner, looping over the arrays and constructing
the value in a single pass.

The code we generate for our running example could look something like:
  with v0 := <array<str>>$0, v1 := <array<int32>>$1, v2 := <array<int64>>$2,
  select array_agg((for i in range_unpack(range(0, len(v0))) union (
    (
      v0[i],
      array_agg((for j in range_unpack(v1[i], v1[i + 1]) union (v2[i]))),
    )
  )))
In this case, since the nested array is simply an array of a scalar, we can
do an optimization and use slicing instead of an array_agg+for:
  with v0 := <array<str>>$0, v1 := <array<int32>>$1, v2 := <array<int64>>$2,
  select array_agg((for i in range_unpack(range(0, len(v0))) union (
    (
      v0[i],
      v2[v1[i] : v1[i + 1]],
    )
  )))

The decoder queries will get placed in a CTE in the generated SQL.
"""

from __future__ import annotations

import dataclasses

from typing import Optional, Tuple, Sequence, TYPE_CHECKING

from edb import errors
from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import typeutils as irtypeutils

from edb.schema import name as sn
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast

from . import context
from . import dispatch
from . import typegen

if TYPE_CHECKING:
    from edb.schema import schema as s_schema

# Since we process tuple types recusively in our cython server, insert
# a recursion depth check here, to be confident that this won't blow
# our C stack. (Though in practice I would expect anything that might
# blow it to blow the python stack while compiling the translation.)
MAX_NESTING = 20


def _lmost_is_array(typ: irast.ParamTransType) -> bool:
    while isinstance(typ, irast.ParamTuple):
        _, typ = typ.typs[0]
    return isinstance(typ, irast.ParamArray)


def translate_type(
    typeref: irast.TypeRef,
    *,
    schema: s_schema.Schema,
) -> tuple[irast.ParamTransType, tuple[irast.TypeRef, ...]]:
    """Translate the type of a tuple-containing param to multiple params.

    This computes a list of parameter types, as well as a
    ParamTransType that clones the type information but augments each
    node in the type with indexes that correspond to which parameter
    data is drawn from. This is used to drive the encoder and the
    decoder generator.
    """

    typs: list[irast.TypeRef] = []

    def trans(
        typ: irast.TypeRef, in_array: bool, depth: int
    ) -> irast.ParamTransType:
        if depth > MAX_NESTING:
            raise errors.QueryError(
                f'type of parameter is too deeply nested')

        start = len(typs)

        if irtypeutils.is_array(typ):
            # If our array is appearing already inside another array,
            # we need to add an extra parameter
            if in_array:
                int_typeref = schema.get(
                    sn.QualName('std', 'int32'), type=s_types.Type)
                nschema, array_styp = s_types.Array.from_subtypes(
                    schema, [int_typeref])
                typs.append(irtypeutils.type_to_typeref(
                    nschema, array_styp, cache=None))

            return irast.ParamArray(
                typeref=typ,
                idx=start,
                typ=trans(typ.subtypes[0], in_array=True, depth=depth + 1),
            )

        elif irtypeutils.is_tuple(typ):
            return irast.ParamTuple(
                typeref=typ,
                idx=start,
                typs=tuple(
                    (
                        t.element_name,
                        trans(t, in_array=in_array, depth=depth + 1),
                    )
                    for t in typ.subtypes
                ),
            )

        else:
            nt = typ
            # If this appears in an array, the param needs to be an array
            if in_array:
                nschema, styp = irtypeutils.ir_typeref_to_type(schema, typ)
                nschema, styp = s_types.Array.from_subtypes(nschema, [styp])
                nt = irtypeutils.type_to_typeref(nschema, styp, cache=None)
            typs.append(nt)
            return irast.ParamScalar(typeref=typ, idx=start)

    t = trans(typeref, in_array=False, depth=0)
    return t, tuple(typs)


def _ref_to_ast(
    typeref: irast.TypeRef, *, ctx: context.ContextLevel
) -> qlast.TypeExpr:
    ctx.env.schema, styp = irtypeutils.ir_typeref_to_type(
        ctx.env.schema, typeref)
    return s_utils.typeref_to_ast(ctx.env.schema, styp)


def _get_alias(
    name: str, *, ctx: context.ContextLevel
) -> Tuple[str, qlast.Path]:
    alias = ctx.aliases.get(name)
    return alias, qlast.Path(
        steps=[qlast.ObjectRef(name=alias)],
    )


def _plus_const(expr: qlast.Expr, val: int) -> qlast.Expr:
    if val == 0:
        return expr
    return qlast.BinOp(
        left=expr,
        op='+',
        right=qlast.Constant.integer(val),
    )


def _index(expr: qlast.Expr, idx: qlast.Expr) -> qlast.Indirection:
    return qlast.Indirection(arg=expr, indirection=[qlast.Index(index=idx)])


def _make_tuple(
    fields: Sequence[tuple[Optional[str], qlast.Expr]]
) -> qlast.NamedTuple | qlast.Tuple:
    is_named = fields and fields[0][0]
    if is_named:
        return qlast.NamedTuple(elements=[
            qlast.TupleElement(name=qlast.Ptr(name=not_none(f)), val=e)
            for f, e in fields
        ])
    else:
        return qlast.Tuple(
            elements=[e for _, e in fields]
        )


def make_decoder(
    ptyp: irast.ParamTransType,
    qparams: tuple[irast.Param, ...],
    *,
    ctx: context.ContextLevel,
) -> qlast.Expr:
    """Generate a decoder for tuple parameters.

    More details in the module docstring.
    """
    params: list[qlast.Expr] = [
        qlast.TypeCast(
            expr=qlast.Parameter(name=param.name),
            type=_ref_to_ast(param.ir_type, ctx=ctx),
            cardinality_mod=(
                qlast.CardinalityModifier.Optional if not param.required
                else None
            ),
        )
        for param in qparams
    ]

    def mk(typ: irast.ParamTransType, idx: Optional[qlast.Expr]) -> qlast.Expr:
        if isinstance(typ, irast.ParamScalar):
            expr = params[typ.idx]
            if idx is not None:
                expr = _index(expr, idx)
            return expr

        elif isinstance(typ, irast.ParamTuple):
            return _make_tuple([(f, mk(t, idx=idx)) for f, t in typ.typs])

        elif isinstance(typ, irast.ParamArray):
            inner_idx_alias, inner_idx = _get_alias('idx', ctx=ctx)

            lo: qlast.Expr
            hi: qlast.Expr
            if idx is None:
                lo = qlast.Constant.integer(0)
                hi = qlast.FunctionCall(
                    func=('__std__', 'len'), args=[params[typ.idx]])
                # If the leftmost element inside a toplevel array is
                # itself an array, subtract 1 from the length (since
                # array params have an extra element). We also need to
                # call `max` to prevent generating an invalid range.
                if _lmost_is_array(typ.typ):
                    hi = qlast.FunctionCall(
                        func=('__std__', 'max'), args=[
                            qlast.Set(elements=[lo, _plus_const(hi, -1)])])
            else:
                lo = _index(params[typ.idx], idx)
                hi = _index(params[typ.idx], _plus_const(idx, 1))

            # If the contents is just a scalar, then we can take
            # values directly from the scalar array parameter, without
            # needing to iterate over the array directly.
            # This is an optimization, and not necessary for correctness.
            if isinstance(typ.typ, irast.ParamScalar):
                sub = params[typ.typ.idx]
                # If we are in an array, do a slice
                if idx is not None:
                    sub = qlast.Indirection(
                        arg=sub,
                        indirection=[qlast.Slice(start=lo, stop=hi)],
                    )
                return sub

            sub_expr = mk(typ.typ, idx=inner_idx)

            # For some reason, this is much faster if force the range to
            # be over int64 instead of int32.
            lo = qlast.TypeCast(
                expr=lo,
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(module='__std__', name='int64')
                ),
            )

            loop = qlast.ForQuery(
                iterator_alias=inner_idx_alias,
                # TODO: Using _gen_series would be marginally faster,
                # but it isn't actually available in distributions
                # iterator=qlast.FunctionCall(
                #     func=('__std__', '_gen_series'),
                #     args=[lo, _plus_const(hi, -1)],
                # ),
                iterator=qlast.FunctionCall(
                    func=('__std__', 'range_unpack'), args=[
                        qlast.FunctionCall(
                            func=('__std__', 'range'),
                            args=[lo, hi],
                        )
                    ]
                ),
                result=sub_expr,
            )
            res: qlast.Expr = qlast.FunctionCall(
                func=('__std__', 'array_agg'), args=[loop],
            )

            # If the param is optional, and we are still at the
            # top-level, insert a filter so that our aggregate doesn't
            # create something from nothing.
            if not qparams[typ.idx].required and idx is None:
                res = qlast.SelectQuery(
                    result=res,
                    where=qlast.UnaryOp(op='EXISTS', operand=params[typ.idx]),
                )

            return res

        else:
            raise AssertionError(f'bogus type {typ}')

    decoder = mk(ptyp, idx=None)

    return decoder


def create_sub_params(
    name: str,
    required: bool,
    typeref: irast.TypeRef,
    pt: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> Optional[irast.SubParams]:
    """Create sub parameters for a new param, if needed.

    We need to do this if there is a tuple in the type.
    """
    if not (
        (
            pt.is_tuple(ctx.env.schema)
            or pt.is_anytuple(ctx.env.schema)
            or pt.contains_array_of_tuples(ctx.env.schema)
        )
        and not ctx.env.options.func_params
        and not ctx.env.options.json_parameters
    ):
        return None

    pdt, arg_typs = translate_type(typeref, schema=ctx.env.schema)
    params = tuple([
        irast.Param(
            name=f'__edb_decoded_{name}_{i}__',
            required=required,
            ir_type=arg_typeref,
            schema_type=typegen.type_from_typeref(arg_typeref, env=ctx.env),
        )
        for i, arg_typeref in enumerate(arg_typs)
    ])

    decode_ql = make_decoder(pdt, params, ctx=ctx)

    return irast.SubParams(
        trans_type=pdt, decoder_edgeql=decode_ql, params=params)


def finish_sub_params(
    subps: irast.SubParams,
    *,
    ctx: context.ContextLevel,
) -> Optional[irast.SubParams]:
    """Finalize the subparams by compiling the IR in the proper context.

    We can't just compile it when doing create_sub_params, since that is
    called from preprocessing and so is shared between queries.
    """
    with ctx.newscope(fenced=True) as subctx:
        decode_ir = dispatch.compile(subps.decoder_edgeql, ctx=subctx)

    return dataclasses.replace(subps, decoder_ir=decode_ir)
