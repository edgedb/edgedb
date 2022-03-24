#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


"""EdgeQL multiplicity inference.

A top-down multiplicity inferer that traverses the full AST populating
multiplicity fields and performing multiplicity checks.
"""


from __future__ import annotations
from typing import *

import dataclasses
import functools
import itertools

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from . import cardinality
from . import context as inf_ctx
from . import types as inf_types
from . import utils as inf_utils


ZERO = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.ZERO)
ONE = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.ONE)
MANY = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.MANY)
DISTINCT_UNION = inf_ctx.MultiplicityInfo(
    own=qltypes.Multiplicity.ONE,
    disjoint_union=True,
)


@dataclasses.dataclass(frozen=True, eq=False)
class ContainerMultiplicityInfo(inf_ctx.MultiplicityInfo):
    """Multiplicity descriptor for an expression returning a container"""

    #: Individual multiplicity values for container elements.
    elements: Tuple[inf_ctx.MultiplicityInfo, ...] = ()


def _max_multiplicity(
    args: Iterable[inf_ctx.MultiplicityInfo]
) -> inf_ctx.MultiplicityInfo:
    # Coincidentally, the lexical order of multiplicity is opposite of
    # order of multiplicity values.
    arg_list = [a.own for a in args]
    if not arg_list:
        max_mult = qltypes.Multiplicity.ONE
    else:
        max_mult = min(arg_list)

    return inf_ctx.MultiplicityInfo(own=max_mult)


def _common_multiplicity(
    args: Iterable[irast.Base],
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return _max_multiplicity(
        infer_multiplicity(a, scope_tree=scope_tree, ctx=ctx) for a in args)


@functools.singledispatch
def _infer_multiplicity(
    ir: irast.Base,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # return MANY
    raise ValueError(f'infer_multiplicity: cannot handle {ir!r}')


@_infer_multiplicity.register
def __infer_statement(
    ir: irast.Statement,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_config_insert(
    ir: irast.ConfigInsert,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_config_set(
    ir: irast.ConfigSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_config_reset(
    ir: irast.ConfigReset,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    if ir.selector:
        return infer_multiplicity(
            ir.selector, scope_tree=scope_tree, ctx=ctx)
    else:
        return ONE


@_infer_multiplicity.register
def __infer_empty_set(
    ir: irast.EmptySet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return ZERO


@_infer_multiplicity.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # TODO: The result is always ONE, but we still want to actually
    # introspect the expression. Unfortunately, currently the
    # expression is not available at this stage.
    #
    # E.g. consider:
    #   WITH X := Foo {bar := {Bar, Bar}}
    #   SELECT INTROSPECT TYPEOF X.bar;
    return ONE


def _infer_shape(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> None:
    for shape_set, shape_op in ir.shape:
        new_scope = inf_utils.get_set_scope(shape_set, scope_tree, ctx=ctx)
        if shape_set.expr and shape_set.rptr:
            expr_mult = infer_multiplicity(
                shape_set.expr, scope_tree=new_scope, ctx=ctx)

            ptrref = shape_set.rptr.ptrref
            if (
                expr_mult.is_many()
                and shape_op is not qlast.ShapeOp.APPEND
                and shape_op is not qlast.ShapeOp.SUBTRACT
                and irtyputils.is_object(ptrref.out_target)
            ):
                ctx.env.schema, ptrcls = irtyputils.ptrcls_from_ptrref(
                    ptrref, schema=ctx.env.schema)
                assert isinstance(ptrcls, s_pointers.Pointer)
                desc = ptrcls.get_verbosename(ctx.env.schema)
                if not is_mutation:
                    desc = f"computed {desc}"
                raise errors.QueryError(
                    f'possibly not a distinct set returned by an '
                    f'expression for a {desc}',
                    hint=(
                        f'You can use assert_distinct() around the expression '
                        f'to turn this into a runtime assertion, or the '
                        f'DISTINCT operator to silently discard duplicate '
                        f'elements.'
                    ),
                    context=shape_set.context
                )

        _infer_shape(
            shape_set, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)


def _infer_set(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    result = _infer_set_inner(
        ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)
    ctx.inferred_multiplicity[ir, scope_tree, ctx.distinct_iterator] = result
    # The shape doesn't affect multiplicity, but requires validation.
    _infer_shape(ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)

    return result


def _infer_set_inner(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    rptr = ir.rptr
    new_scope = cardinality.inf_utils.get_set_scope(ir, scope_tree, ctx=ctx)

    if ir.expr is None:
        expr_mult = None
    else:
        expr_mult = infer_multiplicity(ir.expr, scope_tree=new_scope, ctx=ctx)

    if rptr is not None:
        rptrref = rptr.ptrref
        src_mult = infer_multiplicity(
            rptr.source, scope_tree=new_scope, ctx=ctx)

        if isinstance(rptrref, irast.TupleIndirectionPointerRef):
            if isinstance(src_mult, ContainerMultiplicityInfo):
                idx = irtyputils.get_tuple_element_index(rptrref)
                path_mult = src_mult.elements[idx]
            else:
                # All bets are off for tuple elements coming from
                # opaque tuples.
                path_mult = MANY
        elif not irtyputils.is_object(ir.typeref):
            # This is not an expression and is some kind of scalar, so
            # multiplicity cannot be guaranteed to be ONE (most scalar
            # expressions don't have an implicit requirement to be sets)
            # unless we also have an exclusive constraint.
            if (
                expr_mult is not None
                and inf_utils.find_visible(rptr.source, new_scope) is not None
            ):
                path_mult = expr_mult
            else:
                schema = ctx.env.schema
                # We should only have some kind of path terminating in a
                # property here.
                assert isinstance(rptrref, irast.PointerRef)
                ptr = schema.get_by_id(rptrref.id, type=s_pointers.Pointer)
                if ptr.is_exclusive(schema):
                    # Got an exclusive constraint
                    path_mult = ONE
                else:
                    path_mult = MANY
        else:
            # This is some kind of a link at the end of a path.
            # Therefore the target is a proper set.
            path_mult = ONE

    elif expr_mult is not None:
        path_mult = expr_mult

    else:
        # Evidently this is not a pointer, expression, or a scalar.
        # This is an object type and therefore a proper set.
        path_mult = ONE

    if (
        not path_mult.is_many()
        and irutils.get_path_root(ir).path_id == ctx.distinct_iterator
    ):
        path_mult = dataclasses.replace(path_mult, disjoint_union=True)

    # Mark free object roots
    if irtyputils.is_free_object(ir.typeref) and not ir.expr:
        path_mult = dataclasses.replace(path_mult, fresh_free_object=True)

    # Remove free object freshness when we see them through a binding
    if ir.is_binding == irast.BindingKind.With and path_mult.fresh_free_object:
        path_mult = dataclasses.replace(path_mult, fresh_free_object=False)

    return path_mult


@_infer_multiplicity.register
def __infer_func_call(
    ir: irast.FunctionCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    card = cardinality.infer_cardinality(ir, scope_tree=scope_tree, ctx=ctx)
    args_mult = []
    for arg in ir.args:
        arg_mult = infer_multiplicity(arg.expr, scope_tree=scope_tree, ctx=ctx)
        args_mult.append(arg_mult)
        arg.multiplicity = arg_mult.own

    if card.is_single():
        return ONE
    elif str(ir.func_shortname) == 'std::assert_distinct':
        return ONE
    elif str(ir.func_shortname) == 'std::assert_exists':
        return args_mult[0]
    elif str(ir.func_shortname) == 'std::enumerate':
        # The output of enumerate is always of multiplicity ONE because
        # it's a set of tuples with first elements being guaranteed to be
        # distinct.
        return ContainerMultiplicityInfo(
            own=qltypes.Multiplicity.ONE,
            elements=(ONE,) + tuple(args_mult),
        )
    else:
        # If the function returns a set (for any reason), all bets are off
        # and the maximum multiplicity cannot be inferred.
        return MANY


@_infer_multiplicity.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    mult = []
    cards = []
    for arg in ir.args:
        cards.append(
            cardinality.infer_cardinality(
                arg.expr, scope_tree=scope_tree, ctx=ctx))
        mult.append(
            infer_multiplicity(
                arg.expr, scope_tree=scope_tree, ctx=ctx))

    op_name = str(ir.func_shortname)

    if op_name == 'std::UNION':
        # UNION will produce multiplicity MANY unless most or all of
        # the elements multiplicity is ZERO (from an empty set), or
        # all of the elements are sets of unrelated object types of
        # multiplicity at most ONE, or if all elements have been
        # proven to be disjoint (e.g. a UNION of INSERTs).
        result = ZERO

        arg_type = inf_types.infer_type(ir.args[0].expr, env=ctx.env)
        if isinstance(arg_type, s_objtypes.ObjectType):
            types: List[s_objtypes.ObjectType] = [
                inf_types.infer_type(arg.expr, env=ctx.env)  # type: ignore
                for arg in ir.args
            ]

            lineages = [
                (t,) + tuple(t.descendants(ctx.env.schema))
                for t in types
            ]
            flattened = tuple(itertools.chain.from_iterable(lineages))
            types_disjoint = len(flattened) == len(frozenset(flattened))
        else:
            types_disjoint = False

        for m in mult:
            if m.is_one():
                if (
                    result.is_zero()
                    or types_disjoint
                    or (result.disjoint_union and m.disjoint_union)
                ):
                    result = m
                else:
                    result = MANY
                    break
            elif m.is_many():
                result = MANY
                break
            else:
                # ZERO
                pass

        return result

    elif op_name == 'std::DISTINCT':
        if mult[0] == ZERO:
            return ZERO
        else:
            return ONE
    elif op_name == 'std::IF':
        # If the cardinality of the condition is more than ONE, then
        # the multiplicity cannot be inferred.
        if cards[1].is_single():
            # Now it's just a matter of the multiplicity of the
            # possible results.
            return _max_multiplicity((mult[0], mult[2]))
        else:
            return MANY
    elif op_name == 'std::??':
        return _max_multiplicity((mult[0], mult[1]))
    else:
        # The rest of the operators (other than UNION, DISTINCT, or
        # IF..ELSE). We can ignore the SET OF args because the results
        # are actually proportional to the element-wise args in our
        # operators.
        result = _max_multiplicity(mult)
        if result.is_many():
            return result

        # Even when arguments are of multiplicity ONE, we cannot
        # exclude the possibility of the result being of multiplicity
        # MANY. We need to check that at most one argument has
        # cardinality more than ONE.

        if len([card for card in cards if card.is_multi()]) > 1:
            return MANY
        else:
            return result


@_infer_multiplicity.register
def __infer_const(
    ir: irast.BaseConstant,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return ONE


@_infer_multiplicity.register
def __infer_param(
    ir: irast.Parameter,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return ONE


@_infer_multiplicity.register
def __infer_const_set(
    ir: irast.ConstantSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    if len(ir.elements) == len({el.value for el in ir.elements}):
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Unless this is a singleton, the multiplicity cannot be assumed to be ONE.
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)
    if card is not None and card.is_single():
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_typecast(
    ir: irast.TypeCast,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx,
    )


def _infer_stmt_multiplicity(
    ir: irast.FilteredStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # WITH block bindings need to be validated, they don't have to
    # have multiplicity ONE, but their sub-expressions must be valid.
    for part in (ir.bindings or []):
        infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)

    subj = ir.subject if isinstance(ir, irast.MutatingStmt) else ir.result
    result = infer_multiplicity(
        subj,
        scope_tree=scope_tree,
        ctx=ctx,
    )

    if ir.where:
        infer_multiplicity(ir.where, scope_tree=scope_tree, ctx=ctx)
        filtered_ptrs = cardinality.extract_filters(
            subj, ir.where, scope_tree, ctx)
        for _, flt_expr in filtered_ptrs:
            # Check if any of the singleton filter expressions in FILTER
            # reference enclosing iterators with multiplicity ONE, and
            # if so, indicate to the enclosing iterator that this UNION
            # is guaranteed to be disjoint.
            if (
                (irutils.get_path_root(flt_expr).path_id
                 == ctx.distinct_iterator)
                and not infer_multiplicity(
                    flt_expr, scope_tree=scope_tree, ctx=ctx
                ).is_many()
            ):
                return DISTINCT_UNION

    return result


def _infer_for_multiplicity(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    itset = ir.iterator_stmt
    assert itset is not None
    itexpr = itset.expr
    assert itexpr is not None
    itmult = infer_multiplicity(itset, scope_tree=scope_tree, ctx=ctx)

    if itmult != MANY:
        new_iter = itset.path_id if not ctx.distinct_iterator else None
        ctx = ctx._replace(distinct_iterator=new_iter)
    result_mult = infer_multiplicity(ir.result, scope_tree=scope_tree, ctx=ctx)

    if isinstance(ir.result.expr, irast.InsertStmt):
        # A union of inserts always has multiplicity ONE
        return ONE
    elif itmult.is_many():
        return MANY
    else:
        if result_mult.disjoint_union or result_mult.fresh_free_object:
            return result_mult
        else:
            return MANY


@_infer_multiplicity.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:

    if ir.iterator_stmt is not None:
        return _infer_for_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)
    else:
        stmt_mult = _infer_stmt_multiplicity(
            ir, scope_tree=scope_tree, ctx=ctx)

        clauses = (
            [ir.limit, ir.offset]
            + [sort.expr for sort in (ir.orderby or ())]
        )

        for clause in filter(None, clauses):
            new_scope = inf_utils.get_set_scope(clause, scope_tree, ctx=ctx)
            infer_multiplicity(clause, scope_tree=new_scope, ctx=ctx)

        return stmt_mult


@_infer_multiplicity.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # INSERT will always return a proper set, but we still want to
    # process the sub-expressions.
    infer_multiplicity(
        ir.subject, is_mutation=True, scope_tree=scope_tree, ctx=ctx
    )
    new_scope = inf_utils.get_set_scope(ir.result, scope_tree, ctx=ctx)
    infer_multiplicity(
        ir.result, is_mutation=True, scope_tree=new_scope, ctx=ctx
    )

    if ir.on_conflict:
        for part in [ir.on_conflict.select_ir, ir.on_conflict.else_ir]:
            if part:
                infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)

    return DISTINCT_UNION


@_infer_multiplicity.register
def __infer_update_stmt(
    ir: irast.UpdateStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Presumably UPDATE will always return a proper set, even if it's
    # fed something with higher multiplicity, but we still want to
    # process the expression being updated.
    infer_multiplicity(
        ir.result, is_mutation=True, scope_tree=scope_tree, ctx=ctx,
    )
    result = _infer_stmt_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)
    if result is ZERO:
        return ZERO
    else:
        return ONE


@_infer_multiplicity.register
def __infer_delete_stmt(
    ir: irast.DeleteStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Presumably DELETE will always return a proper set, even if it's
    # fed something with higher multiplicity, but we still want to
    # process the expression being deleted.
    infer_multiplicity(
        ir.result, is_mutation=True, scope_tree=scope_tree, ctx=ctx,
    )
    result = _infer_stmt_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)
    if result is ZERO:
        return ZERO
    else:
        return ONE


@_infer_multiplicity.register
def __infer_group_stmt(
    ir: irast.GroupStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    infer_multiplicity(ir.subject, scope_tree=scope_tree, ctx=ctx)
    for binding, _ in ir.using.values():
        infer_multiplicity(binding, scope_tree=scope_tree, ctx=ctx)
    result_mult = _infer_stmt_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)

    for clause in (ir.orderby or ()):
        new_scope = inf_utils.get_set_scope(clause.expr, scope_tree, ctx=ctx)
        infer_multiplicity(clause.expr, scope_tree=new_scope, ctx=ctx)

    if result_mult.fresh_free_object:
        return result_mult
    else:
        return MANY


@_infer_multiplicity.register
def __infer_slice(
    ir: irast.SliceIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Slice indirection multiplicity is guaranteed to be ONE as long
    # as the cardinality of this expression is at most one, otherwise
    # the results of index indirection can contain values with
    # multiplicity > 1.
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)
    if card is not None and card.is_single():
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_index(
    ir: irast.IndexIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Index indirection multiplicity is guaranteed to be ONE as long
    # as the cardinality of this expression is at most one, otherwise
    # the results of index indirection can contain values with
    # multiplicity > 1.
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)
    if card is not None and card.is_single():
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_array(
    ir: irast.Array,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return _common_multiplicity(ir.elements, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_tuple(
    ir: irast.Tuple,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    els = tuple(
        infer_multiplicity(el.val, scope_tree=scope_tree, ctx=ctx)
        for el in ir.elements
    )
    cards = [
        cardinality.infer_cardinality(el.val, scope_tree=scope_tree, ctx=ctx)
        for el in ir.elements
    ]

    num_many = sum(card.is_multi() for card in cards)
    new_els = []
    for el, card in zip(els, cards):
        # If more than one tuple element is many, everything has MANY
        # multiplicity.
        if num_many > 1:
            el = MANY
        # If exactly one tuple element is many, then *that* element
        # can keep its underlying multiplicity, while everything else
        # becomes MANY.
        elif num_many == 1 and card.is_single():
            el = MANY
        new_els.append(el)

    return ContainerMultiplicityInfo(
        own=_max_multiplicity(els).own,
        elements=tuple(new_els),
    )


def infer_multiplicity(
    ir: irast.Base,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:

    result = ctx.inferred_multiplicity.get(
        (ir, scope_tree, ctx.distinct_iterator))
    if result is not None:
        return result

    # We can use cardinality as a helper in determining multiplicity,
    # since singletons have multiplicity one.
    card = cardinality.infer_cardinality(
        ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)

    if isinstance(ir, irast.EmptySet):
        result = ZERO
    elif isinstance(ir, irast.Set):
        result = _infer_set(
            ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx,
        )
    else:
        result = _infer_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)

    if card is not None and card.is_single() and result.is_many():
        # We've validated multiplicity, so now we can just override it
        # safely.
        result = ONE

    if not isinstance(result, inf_ctx.MultiplicityInfo):
        raise errors.QueryError(
            'could not determine the multiplicity of '
            'set produced by expression',
            context=ir.context)

    ctx.inferred_multiplicity[ir, scope_tree, ctx.distinct_iterator] = result

    return result
