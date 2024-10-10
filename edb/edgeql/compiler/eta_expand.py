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


"""η-expansion of tuples and arrays.

Our shape compiler is only able to produce shape outputs for objects
at places in the program that get fairly directly routed into the
output. To compensate for this, when we have an expression like
`[User {name}][0]`, the shape output is actually computed *after* the
array indexing. This works well, but fails when the thing we need to
do late shape injection into is a collection type that cannot have a
shape put on it directly: `[(User {name}, 20)][0]`.
To solve this problem, we use η-expansion.

η-expansion is a technique coming from the study of the lambda
caluclus, where it means to expand an expression `e` into `λx.ex`,
where `x` is a variable that does not appear in `e` (or, in Python-speak
`lambda x: e(x)`). Setting aside questions about what happens if `e` does
not terminate, this new expression `λx.ex` will be equivalent to `e`.

In the traditional untyped lambda calculus, where everything is a
function (from functions to functions), this is the whole story.
But the world of *typed* lambda calculi introduce some interesting
new considerations:
  1. Instead of it being possible to η-expand *any* expression,
     now only expressions actually of function type may be expanded.
     This allows us to define of notion of an expression being "η-long",
     which means that it is "fully η-expanded" (and that any new expansion
     would create a reducible expression, where a lambda appears directly
     on the LHS of an application).
  2. If other types are introduced, we can define notions of η-expansion
     for them as well. The key idea is that expanded expression should
     explicitly construct an object of the desired type.

     For a pair type, for example, we would expand `e` into `(e[0], e[1])`.
     This also can be done to produce an "η-long" form: for example,
     if we have the type `Tuple[int, Tuple[int, int]]`, we would expand
     that into `(e[0], (e[1][0], e[1][1]))`.

This key idea, of expanding a term into one that explicitly constructs
an object of the desired type, is exactly what we need to ensure that
we can inject shapes into the output.

As a set-based query language, we also need to do some extra work to
preserve the ordering of elements.

Our core rules are:
    EXPAND_ORDERED(t, e) =
        WITH enum := enumerate(e)
        SELECT EXPAND(t, enum.1) ORDER BY enum.0

    EXPAND(tuple<t, s>, p) = (EXPAND(t, p.0), EXPAND(s, p.1))

    EXPAND(array<t>, p) =
        (p, array_agg(EXPAND_ORDERED(t, array_unpack(p)))).1

    EXPAND(non_collection_type, p) = p

They are discussed in more detail at the implementation sites.
"""


from __future__ import annotations

from typing import Tuple

from edb.ir import ast as irast

from edb.schema import name as sn
from edb.schema import types as s_types

from edb.edgeql import ast as qlast

from . import astutils
from . import context
from . import dispatch
from . import setgen


# If true, we disregard the optimizations meant to avoid unnecessary
# expansions. This is useful as a bug-finding tool, since η-expansion
# found lots of bugs, but mostly in test cases that didn't *really*
# need it.
ALWAYS_EXPAND = False


def needs_eta_expansion_expr(
    ir: irast.Expr,
    stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> bool:
    """Determine if an expr is in need of η-expansion

    In general, any expression of an object-containing
    tuple or array type needs expansion unless it is:
        * A tuple literal
        * An empty array literal
        * A one-element array literal
        * A call to array_agg
    in which none of the arguments are sets that need expansion.
    """
    if isinstance(ir, irast.SelectStmt):
        return needs_eta_expansion(
            ir.result, has_clauses=bool(ir.where or ir.orderby), ctx=ctx)

    if isinstance(stype, s_types.Array):
        if isinstance(ir, irast.Array):
            return bool(ir.elements) and (
                len(ir.elements) != 1
                or needs_eta_expansion(ir.elements[0], ctx=ctx)
            )
        elif (
            isinstance(ir, irast.FunctionCall)
            and ir.func_shortname == sn.QualName('std', 'array_agg')
        ):
            return needs_eta_expansion(ir.args[0].expr, ctx=ctx)
        else:
            return True

    elif isinstance(stype, s_types.Tuple):
        if isinstance(ir, irast.Tuple):
            return any(
                needs_eta_expansion(el.val, ctx=ctx) for el in ir.elements
            )
        else:
            return True

    else:
        return False


def needs_eta_expansion(
    ir: irast.Set,
    *,
    has_clauses: bool = False,
    ctx: context.ContextLevel,
) -> bool:
    """Determine if a set is in need of η-expansion"""
    stype = setgen.get_set_type(ir, ctx=ctx)

    if not (
        isinstance(stype, (s_types.Array, s_types.Tuple))
        and stype.contains_object(ctx.env.schema)
    ):
        return False

    if ALWAYS_EXPAND:
        return True

    # Object containing arrays always need to be eta expanded if they
    # might be processed by a clause. This is because the pgsql side
    # will produce *either* a value or serialized for array_agg/array
    # literals.
    if has_clauses and (
        (subarray := stype.find_array(ctx.env.schema))
        and subarray.contains_object(ctx.env.schema)
    ):
        return True

    # If we are directly projecting an element out of a tuple, we can just
    # look through to the relevant tuple element. This is probably not
    # an important optimization to support, but our expansion can generate
    # this idiom, so on principle I wanted to support it.
    if (
        isinstance(ir.expr, irast.TupleIndirectionPointer)
        and isinstance(ir.expr.source.expr, irast.Tuple)
    ):
        name = ir.expr.ptrref.shortname.name
        els = [x for x in ir.expr.source.expr.elements if x.name == name]
        if len(els) == 1:
            return needs_eta_expansion(els[0].val, ctx=ctx)

    if not ir.expr or (
        ir.is_binding and ir.is_binding != irast.BindingKind.Select
    ):
        return True

    return needs_eta_expansion_expr(ir.expr, stype, ctx=ctx)


def _get_alias(
    name: str, *, ctx: context.ContextLevel
) -> Tuple[str, qlast.Path]:
    alias = ctx.aliases.get(name)
    return alias, qlast.Path(
        steps=[qlast.ObjectRef(name=alias)],
    )


def eta_expand_ir(
    ir: irast.Set,
    *,
    toplevel: bool=False,
    ctx: context.ContextLevel,
) -> irast.Set:
    """η-expansion of an IR set.

    Our core implementation of η-expansion operates on an AST,
    so this mostly just checks that we really want to expand
    and then sets up an anchor for the AST based implementation
    to run on.
    """
    if (
        ctx.env.options.schema_object_context
        or ctx.env.options.func_params
        or ctx.env.options.schema_view_mode
    ):
        return ir

    if not needs_eta_expansion(ir, ctx=ctx):
        return ir

    with ctx.new() as subctx:
        subctx.allow_factoring()

        subctx.anchors = subctx.anchors.copy()
        source_ref = subctx.create_anchor(ir)

        alias, path = _get_alias('eta', ctx=subctx)
        qry = qlast.SelectQuery(
            result=eta_expand_ordered(
                path, setgen.get_set_type(ir, ctx=subctx), ctx=subctx
            ),
            aliases=[
                qlast.AliasedExpr(alias=alias, expr=source_ref)
            ],
        )
        if toplevel:
            subctx.toplevel_stmt = None
        return dispatch.compile(qry, ctx=subctx)


def eta_expand_ordered(
    expr: qlast.Expr,
    stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> qlast.Expr:
    """Do an order-preserving η-expansion

    Unlike in the lambda calculus, edgeql is a set-based language
    with a notion of ordering, which we need to preserve.
    We do this by using enumerate and ORDER BY on it:
        EXPAND_ORDERED(t, e) =
            WITH enum := enumerate(e)
            SELECT EXPAND(t, enum.1) ORDER BY enum.0
    """
    enumerated = qlast.FunctionCall(
        func=('__std__', 'enumerate'), args=[expr]
    )

    enumerated_alias, enumerated_path = _get_alias('enum', ctx=ctx)

    element_path = astutils.extend_path(enumerated_path, '1')
    result_expr = eta_expand(element_path, stype, ctx=ctx)

    return qlast.SelectQuery(
        result=result_expr,
        orderby=[
            qlast.SortExpr(path=astutils.extend_path(enumerated_path, '0'))
        ],
        aliases=[
            qlast.AliasedExpr(alias=enumerated_alias, expr=enumerated)
        ],
    )


def eta_expand(
    path: qlast.Path,
    stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> qlast.Expr:
    """η-expansion of an AST path"""
    if not ALWAYS_EXPAND and not stype.contains_object(ctx.env.schema):
        # This isn't strictly right from a "fully η expanding" perspective,
        # but for our uses, we only need to make sure that objects are
        # exposed to the output, so we can skip anything not containing one.
        return path

    if isinstance(stype, s_types.Array):
        return eta_expand_array(path, stype, ctx=ctx)

    elif isinstance(stype, s_types.Tuple):
        return eta_expand_tuple(path, stype, ctx=ctx)

    else:
        return path


def eta_expand_tuple(
    path: qlast.Path,
    stype: s_types.Tuple,
    *,
    ctx: context.ContextLevel,
) -> qlast.Expr:
    """η-expansion of tuples

    η-expansion of tuple types is straightforward and traditional:
        EXPAND(tuple<t, s>, p) = (EXPAND(t, p.0), EXPAND(s, p.1))
    is the case for pairs. n-ary and named cases are generalized in the
    obvious way.
    The one exception is that the expansion of the empty tuple type is
    `p` and not `()`, to ensure that the path appears in the output.
    """
    if not stype.get_subtypes(ctx.env.schema):
        return path

    els = [
        qlast.TupleElement(
            name=qlast.Ptr(name=name),
            val=eta_expand(astutils.extend_path(path, name), subtype, ctx=ctx),
        )
        for name, subtype in stype.iter_subtypes(ctx.env.schema)
    ]

    if stype.is_named(ctx.env.schema):
        return qlast.NamedTuple(elements=els)
    else:
        return qlast.Tuple(elements=[el.val for el in els])


def eta_expand_array(
    path: qlast.Path,
    stype: s_types.Array,
    *,
    ctx: context.ContextLevel,
) -> qlast.Expr:
    """η-expansion of arrays

    η-expansion of array types is is a little peculiar to edgeql and less
    grounded in typed lambda calculi:
        EXPAND(array<t>, p) =
            (p, array_agg(EXPAND_ORDERED(t, array_unpack(p)))).1

    We use a similar approach for compiling casts.

    The tuple projection trick serves to make sure that we iterate over
    `p` *outside* of the array_agg (or else all the arrays would get
    aggregated together) as well as ensuring that `p` appears in the expansion
    in a non-fenced position (or else sorting it from outside wouldn't work).

    (If it wasn't for the latter requirement, we could just use a FOR.
    I find it a little unsatisfying that our η-expansion needs to use this
    trick, and the pgsql compiler needed to be hacked to make it work.)
    """

    unpacked = qlast.FunctionCall(
        func=('__std__', 'array_unpack'), args=[path]
    )

    expanded = eta_expand_ordered(
        unpacked, stype.get_element_type(ctx.env.schema), ctx=ctx)

    agg_expr = qlast.FunctionCall(
        func=('__std__', 'array_agg'), args=[expanded]
    )

    return astutils.extend_path(
        qlast.Tuple(elements=[path, agg_expr]), '1'
    )
