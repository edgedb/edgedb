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

import functools

from edb import errors

from edb.edgeql import qltypes

from edb.schema import pointers as s_pointers

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from . import cardinality
from . import context as inference_context


ZERO = qltypes.Multiplicity.ZERO
ONE = qltypes.Multiplicity.ONE
MANY = qltypes.Multiplicity.MANY


def _max_multiplicity(
    args: Iterable[qltypes.Multiplicity]
) -> qltypes.Multiplicity:
    # Coincidentally, the lexical order of multiplicity is opposite of
    # order of multiplicity values.
    arg_list = list(args)
    if not arg_list:
        return ZERO
    else:
        return min(arg_list)


def _common_multiplicity(
    args: Iterable[irast.Base],
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return _max_multiplicity(
        infer_multiplicity(a, scope_tree=scope_tree, ctx=ctx) for a in args)


@functools.singledispatch
def _infer_multiplicity(
    ir: irast.Expr,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    # return MANY
    raise ValueError(f'infer_multiplicity: cannot handle {ir!r}')


@_infer_multiplicity.register
def __infer_none(
    ir: None,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    # Here for debugging purposes.
    raise ValueError('invalid infer_multiplicity(None, schema) call')


@_infer_multiplicity.register
def __infer_statement(
    ir: irast.Statement,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_empty_set(
    ir: irast.EmptySet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return ZERO


@_infer_multiplicity.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> None:
    for shape_set, _ in ir.shape:
        new_scope = cardinality._get_set_scope(shape_set, scope_tree, ctx=ctx)
        if shape_set.expr and shape_set.rptr:
            expr_mult = infer_multiplicity(
                shape_set.expr, scope_tree=new_scope, ctx=ctx)

            ptrref = shape_set.rptr.ptrref
            if expr_mult is MANY and irtyputils.is_object(ptrref.out_target):
                raise errors.QueryError(
                    f'possibly not a strict set returned by an '
                    f'expression for a computable '
                    f'{ptrref.shortname.name}.',
                    hint=(
                        f'Use DISTINCT for the entire computable expression '
                        f'to resolve this.'
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    result = _infer_set_inner(
        ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)
    ctx.inferred_multiplicity[ir, scope_tree] = result
    # The shape doesn't affect multiplicity, but requires validation.
    _infer_shape(ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)

    return result


def _infer_set_inner(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    rptr = ir.rptr
    new_scope = cardinality._get_set_scope(ir, scope_tree, ctx=ctx)

    if rptr is not None:
        # Validate the source
        infer_multiplicity(rptr.source, scope_tree=new_scope, ctx=ctx)

    if ir.expr:
        expr_mult = infer_multiplicity(ir.expr, scope_tree=new_scope, ctx=ctx)

    if rptr is not None:
        rptrref = rptr.ptrref

        if isinstance(rptr.ptrref, irast.TupleIndirectionPointerRef):
            # All bets are off for tuple elements.
            return MANY
        elif not irtyputils.is_object(ir.typeref):
            # This is not an expression and is some kind of scalar, so
            # multiplicity cannot be guaranteed to be ONE (most scalar
            # expressions don't have an implicit requirement to be sets)
            # unless we also have an exclusive constraint.

            if rptr is not None:
                schema = ctx.env.schema
                # We should only have some kind of path terminating in a
                # property here.
                assert isinstance(rptrref, irast.PointerRef)
                ptr = schema.get_by_id(rptrref.id, type=s_pointers.Pointer)
                if ptr.is_exclusive(schema):
                    # Got an exclusive constraint
                    return ONE

            return MANY

        else:
            # This is some kind of a link at the end of a path.
            # Therefore the target is a proper set.
            return ONE

    elif ir.expr is not None:
        return expr_mult

    else:
        # Evidently this is not a pointer, expression, or a scalar.
        # This is an object type and therefore a proper set.
        return ONE


@_infer_multiplicity.register
def __infer_func_call(
    ir: irast.FunctionCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    # If the function returns a set (for any reason), all bets are off
    # and the maximum multiplicity cannot be inferred.
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)

    # We still want to validate the multiplicity of the arguments, though.
    for arg in ir.args:
        infer_multiplicity(arg.expr, scope_tree=scope_tree, ctx=ctx)

    if card is not None and card.is_single():
        return ONE
    elif str(ir.func_shortname) == 'std::enumerate':
        # Technically the output of enumerate is always of
        # multiplicity ONE because it's a set of tuples with first
        # elements being guaranteed to be distinct.
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
        # the elements multiplicity is ZERO (from an empty set).
        result = ZERO
        for m in mult:
            if m is ONE and result is ZERO:
                result = m
            elif m is ONE and result is not ZERO:
                return MANY
            elif m is MANY:
                return MANY
        return result

    elif op_name == 'std::DISTINCT':
        if mult[0] is ZERO:
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

    else:
        # The rest of the operators (other than UNION, DISTINCT, or
        # IF..ELSE). We can ignore the SET OF args because the results
        # are actually proportional to the element-wise args in our
        # operators.
        result = _max_multiplicity(mult)
        if result is MANY:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return ONE


@_infer_multiplicity.register
def __infer_param(
    ir: irast.Parameter,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return ONE


@_infer_multiplicity.register
def __infer_const_set(
    ir: irast.ConstantSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    if len(ir.elements) == len({el.value for el in ir.elements}):
        return ONE
    else:
        return MANY


@_infer_multiplicity.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return infer_multiplicity(
        ir.expr, scope_tree=scope_tree, ctx=ctx,
    )


def _infer_stmt_multiplicity(
    ir: irast.FilteredStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    result = infer_multiplicity(
        ir.subject if isinstance(ir, irast.MutatingStmt) else ir.result,
        scope_tree=scope_tree,
        ctx=ctx,
    )

    # WITH block bindings need to be validated, they don't have to
    # have multiplicity ONE, but their sub-expressions must be valid.
    #
    # Inferring how the FILTER clause affects multiplicity is in
    # general impossible, but we still want to ensure that the FILTER
    # expression has valid multiplicity.
    for part in ir.bindings + [ir.where]:
        if part:
            infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)

    return result


def _infer_for_multiplicity(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:

    assert ir.iterator_stmt is not None
    itexpr = ir.iterator_stmt.expr

    if isinstance(ir.result.expr, irast.SelectStmt):
        union = ir.result.expr
        if (isinstance(union.where, irast.Set) and
                isinstance(union.where.expr, irast.OperatorCall) and
                str(union.where.expr.func_shortname) == 'std::='):

            op = union.where.expr
            left, right = (a.expr for a in op.args)

            # The iterator set may be wrapped in an `enumerate`, this
            # requires different handling.
            has_enumerate = (
                isinstance(itexpr, irast.SelectStmt) and
                isinstance(itfn := itexpr.result.expr, irast.FunctionCall) and
                str(itfn.func_shortname) == 'std::enumerate'
            )

            # First make sure that the cardinality of the FILTER
            # expression is is no more than 1. Then make sure both
            # operands are paths.
            if union.where_card.is_single():
                it = None
                if left.rptr is not None:
                    it = right
                elif right.rptr is not None:
                    it = left

                if it is not None:
                    if has_enumerate:
                        assert isinstance(itfn, irast.FunctionCall)
                        enumerate_mult = infer_multiplicity(
                            itfn.args[0].expr, scope_tree=scope_tree, ctx=ctx,
                        )
                        if (
                            enumerate_mult is ONE
                            and it.rptr is not None
                            and isinstance(
                                it.rptr,
                                irast.TupleIndirectionPointer
                            )
                            # Tuple comes from the iterator set
                            and it.rptr.source.expr is itexpr
                            # the indirection is accessing element 1
                            and str(it.rptr.ptrref.name) == '__tuple__::1'
                        ):
                            return ONE
                    elif (it.is_binding and it.expr is itexpr):
                        return ONE

    elif isinstance(ir.result.expr, irast.InsertStmt):
        # A union of inserts always has multiplicity ONE
        return ONE

    return MANY


@_infer_multiplicity.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    result = _infer_stmt_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)
    itmult = None

    if ir.iterator_stmt:
        # If this is a FOR, then there's a common pattern which can be
        # detected and the multiplicity of it is ONE. Otherwise it
        # cannot be reliably inferred.
        #
        # The pattern is: FOR x IN {<set of multiplicity ONE>} UNION
        # (SELECT ... FILTER .prop = x) As long as the FILTER has just
        # a single .prop = x expression, this is going to be a bunch
        # of disjoint unions and the final multiplicity will be ONE.
        itmult = infer_multiplicity(
            ir.iterator_stmt, scope_tree=scope_tree, ctx=ctx,
        )

    # OFFSET, LIMIT and ORDER BY have already been validated to be
    # singletons, but their sub-expressions (if any) still need to be
    # validated.
    for part in [ir.limit, ir.offset] + [sort.expr for sort in ir.orderby]:
        if part:
            new_scope = cardinality._get_set_scope(part, scope_tree, ctx=ctx)
            infer_multiplicity(part, scope_tree=new_scope, ctx=ctx)

    if itmult is not None:
        if itmult is ONE:
            return _infer_for_multiplicity(
                ir, scope_tree=scope_tree, ctx=ctx)

        return MANY
    else:
        return result


@_infer_multiplicity.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    # INSERT will always return a proper set, but we still want to
    # process the sub-expressions.
    infer_multiplicity(
        ir.subject, is_mutation=True, scope_tree=scope_tree, ctx=ctx
    )
    new_scope = cardinality._get_set_scope(ir.result, scope_tree, ctx=ctx)
    infer_multiplicity(
        ir.result, is_mutation=True, scope_tree=new_scope, ctx=ctx
    )

    if ir.on_conflict:
        for part in [ir.on_conflict.select_ir, ir.on_conflict.else_ir]:
            if part:
                infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)

    return ONE


@_infer_multiplicity.register
def __infer_update_stmt(
    ir: irast.UpdateStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    raise NotImplementedError


@_infer_multiplicity.register
def __infer_slice(
    ir: irast.SliceIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
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
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return _common_multiplicity(ir.elements, scope_tree=scope_tree, ctx=ctx)


@_infer_multiplicity.register
def __infer_tuple(
    ir: irast.Tuple,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:
    return _common_multiplicity(
        [el.val for el in ir.elements], scope_tree=scope_tree, ctx=ctx
    )


def infer_multiplicity(
    ir: irast.Base,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Multiplicity:

    result = ctx.inferred_multiplicity.get((ir, scope_tree))
    if result is not None:
        return result

    # We can use cardinality as a helper in determining multiplicity,
    # since singletons have multiplicity one.
    card = cardinality.infer_cardinality(
        ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)

    if isinstance(ir, irast.Set):
        result = _infer_set(
            ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx,
        )
    else:
        result = _infer_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)

    if card is not None and card.is_single():
        # We've validated multiplicity, so now we can just override it
        # safely.
        result = ONE

    if result not in {ZERO, ONE, MANY}:
        raise errors.QueryError(
            'could not determine the multiplicity of '
            'set produced by expression',
            context=ir.context)

    ctx.inferred_multiplicity[ir, scope_tree] = result

    return result
