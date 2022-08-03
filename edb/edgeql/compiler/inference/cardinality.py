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


"""EdgeQL cardinality inference.

A top-down cardinality inferer that traverses the full AST populating
cardinality fields and performing cardinality checks.
"""


from __future__ import annotations
from typing import *

import enum
import functools
import uuid

from edb import errors
from edb.common import parsing

from edb.edgeql import qltypes

from edb.schema import name as sn
from edb.schema import types as s_types
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import constraints as s_constraints

from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.ir import typeutils
from edb.edgeql import ast as qlast

from . import context as inference_context
from . import utils as inf_utils
from . import volatility

from .. import context


AT_MOST_ONE = qltypes.Cardinality.AT_MOST_ONE
ONE = qltypes.Cardinality.ONE
MANY = qltypes.Cardinality.MANY
AT_LEAST_ONE = qltypes.Cardinality.AT_LEAST_ONE


class CardinalityBound(int, enum.Enum):
    '''This enum is used to perform some of the cardinality operations.'''
    ZERO = 0
    ONE = 1
    MANY = 2

    def as_required(self) -> bool:
        return self is CB_ONE

    def as_schema_cardinality(self) -> qltypes.SchemaCardinality:
        if self is CB_MANY:
            return qltypes.SchemaCardinality.Many
        else:
            return qltypes.SchemaCardinality.One

    @classmethod
    def from_required(cls, required: bool) -> CardinalityBound:
        return CB_ONE if required else CB_ZERO

    @classmethod
    def from_schema_value(
        cls,
        card: qltypes.SchemaCardinality
    ) -> CardinalityBound:
        if card is qltypes.SchemaCardinality.Many:
            return CB_MANY
        else:
            return CB_ONE


CB_ZERO = CardinalityBound.ZERO
CB_ONE = CardinalityBound.ONE
CB_MANY = CardinalityBound.MANY


class CardinalityBounds(NamedTuple):
    lower: CardinalityBound
    upper: CardinalityBound


def _card_to_bounds(
    card: qltypes.Cardinality
) -> CardinalityBounds:
    lower, upper = card.to_schema_value()
    return CardinalityBounds(
        CardinalityBound.from_required(lower),
        CardinalityBound.from_schema_value(upper),
    )


def _bounds_to_card(
    lower: CardinalityBound,
    upper: CardinalityBound,
) -> qltypes.Cardinality:
    return qltypes.Cardinality.from_schema_value(
        lower.as_required(),
        upper.as_schema_cardinality(),
    )


def cartesian_cardinality(
    args: Iterable[qltypes.Cardinality],
) -> qltypes.Cardinality:
    '''Cardinality of Cartesian product of multiple args.'''

    card = list(zip(*(_card_to_bounds(a) for a in args)))
    if card:
        lower, upper = card
        return _bounds_to_card(min(lower), max(upper))
    else:
        # no args is indicative of a empty set
        return AT_MOST_ONE


def max_cardinality(
    args: Iterable[qltypes.Cardinality],
) -> qltypes.Cardinality:
    '''Maximum lower and upper bound of specified cardinalities.'''

    card = list(zip(*(_card_to_bounds(a) for a in args)))
    if card:
        lower, upper = card
        return _bounds_to_card(max(lower), max(upper))
    else:
        # no args is indicative of a empty set
        return AT_MOST_ONE


def _union_cardinality(
    args: Iterable[qltypes.Cardinality],
) -> qltypes.Cardinality:
    '''Cardinality of UNION of multiple args.'''

    card = list(zip(*(_card_to_bounds(a) for a in args)))
    if card:
        lower, upper = card
        return _bounds_to_card(
            max(lower),
            CB_MANY if len(upper) > 1 else upper[0],
        )
    else:
        # no args is indicative of a empty set
        return AT_MOST_ONE


VOLATILE = qltypes.Volatility.Volatile


def _check_op_volatility(
    args: Sequence[irast.Base],
    cards: Sequence[qltypes.Cardinality],
    ctx: inference_context.InfCtx,
) -> None:
    vols = [volatility.infer_volatility(a, env=ctx.env) for a in args]

    # Check the rules on volatility correlation: volatile operations
    # can't be cross producted with any potentially multi set. We
    # check this by assuming that a voltile operation is AT_MOST_ONE
    # and making sure that the resulting cartesian cardinality isn't
    # multi.
    for i, vol in enumerate(vols):
        if vol == VOLATILE:
            cards2 = list(cards)
            cards2[i] = AT_MOST_ONE
            if cartesian_cardinality(cards2).is_multi():
                raise errors.QueryError(
                    "can not take cross product of volatile operation",
                    context=args[i].context
                )


def _common_cardinality(
    args: Sequence[irast.Base],
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    cards = [
        infer_cardinality(
            a, scope_tree=scope_tree, ctx=ctx
        ) for a in args
    ]
    _check_op_volatility(args, cards, ctx=ctx)

    return cartesian_cardinality(cards)


@functools.singledispatch
def _infer_cardinality(
    ir: irast.Base,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    raise ValueError(f'infer_cardinality: cannot handle {ir!r}')


@_infer_cardinality.register
def __infer_statement(
    ir: irast.Statement,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_config_insert(
    ir: irast.ConfigInsert,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.expr, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_config_set(
    ir: irast.ConfigSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    card = infer_cardinality(
        ir.expr, scope_tree=scope_tree, ctx=ctx)
    if ir.required and card.can_be_zero():
        raise errors.QueryError(
            f"possibly an empty set returned for "
            f"a global declared as 'required'",
            context=ir.context,
        )
    if ir.cardinality.is_single() and not card.is_single():
        raise errors.QueryError(
            f"possibly more than one element returned for "
            f"a global declared as 'single'",
            context=ir.context,
        )

    return card


@_infer_cardinality.register
def __infer_config_reset(
    ir: irast.ConfigReset,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    if ir.selector:
        return infer_cardinality(
            ir.selector, scope_tree=scope_tree, ctx=ctx)
    else:
        return ONE


@_infer_cardinality.register
def __infer_empty_set(
    ir: irast.EmptySet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return _infer_set(
        ir, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_typeref(
    ir: irast.TypeRef,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return AT_MOST_ONE


@_infer_cardinality.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return ONE


def _infer_pointer_cardinality(
    *,
    ptrcls: s_pointers.Pointer,
    ptrref: Optional[irast.BasePointerRef],
    irexpr: irast.Base,
    specified_required: Optional[bool] = None,
    specified_card: Optional[qltypes.SchemaCardinality] = None,
    is_mut_assignment: bool = False,
    shape_op: qlast.ShapeOp = qlast.ShapeOp.ASSIGN,
    source_ctx: Optional[parsing.ParserContext] = None,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> None:

    env = ctx.env

    if specified_required is None:
        spec_lower_bound = None
    else:
        spec_lower_bound = CardinalityBound.from_required(specified_required)

    if specified_card is None:
        spec_upper_bound = None
    else:
        spec_upper_bound = CardinalityBound.from_schema_value(specified_card)

    expr_card = infer_cardinality(
        irexpr, scope_tree=scope_tree, ctx=ctx)

    ptrcls_schema_card = ptrcls.get_cardinality(env.schema)

    # Infer cardinality and convert it back to schema values of "ONE/MANY".
    if shape_op is qlast.ShapeOp.APPEND:
        # += in shape always means MANY
        inferred_card = qltypes.Cardinality.MANY
    elif shape_op is qlast.ShapeOp.SUBTRACT:
        # -= does not increase cardinality, but it may result in an empty set,
        # hence AT_MOST_ONE.
        inferred_card = qltypes.Cardinality.AT_MOST_ONE
    else:
        # Pull cardinality from the ptrcls, if it exists.
        # (This generally will have been populated by the source_map
        # handling in infer_toplevel_cardinality().)
        if ptrcls_schema_card.is_known():
            inferred_card = qltypes.Cardinality.from_schema_value(
                not expr_card.can_be_zero(), ptrcls_schema_card
            )
        else:
            inferred_card = expr_card

    if spec_upper_bound is None and spec_lower_bound is None:
        # Common case of no explicit specifier and no overloading.
        ptr_card = inferred_card
    else:
        # Verify that the explicitly specified (or inherited) cardinality is
        # within the cardinality bounds inferred from the expression, except
        # for mutations we punt the lower cardinality bound check to the
        # runtime DML constraint as that would produce a more meaningful error.
        inf_lower_bound, inf_upper_bound = _card_to_bounds(inferred_card)

        if spec_upper_bound is None:
            upper_bound = inf_upper_bound
        else:
            if inf_upper_bound > spec_upper_bound:
                desc = ptrcls.get_verbosename(env.schema)
                if not is_mut_assignment:
                    desc = f"computed {desc}"
                raise errors.QueryError(
                    f"possibly more than one element returned by an "
                    f"expression for a {desc} declared as 'single'",
                    context=source_ctx,
                )
            upper_bound = spec_upper_bound

        if spec_lower_bound is None:
            lower_bound = inf_lower_bound
        else:
            if inf_lower_bound < spec_lower_bound:
                if is_mut_assignment:
                    lower_bound = inf_lower_bound
                else:
                    desc = f"computed {ptrcls.get_verbosename(env.schema)}"
                    raise errors.QueryError(
                        f"possibly an empty set returned by an "
                        f"expression for a {desc} declared as 'required'",
                        context=source_ctx,
                    )
            else:
                lower_bound = spec_lower_bound

        ptr_card = _bounds_to_card(lower_bound, upper_bound)

    if (
        not ptrcls_schema_card.is_known()
        or ptrcls in ctx.env.pointer_specified_info
    ):
        if ptrcls_schema_card.is_known():
            # If we are overloading an existing pointer, take the _maximum_
            # of the cardinalities.  In practice this only means that we might
            # raise the lower bound, since the other redefinitions of bounds
            # are prohibited above and in viewgen.
            ptrcls_card = qltypes.Cardinality.from_schema_value(
                ptrcls.get_required(env.schema),
                ptrcls_schema_card,
            )
            if is_mut_assignment:
                ptr_card = cartesian_cardinality((ptrcls_card, ptr_card))
            else:
                ptr_card = max_cardinality((ptrcls_card, ptr_card))
        required, card = ptr_card.to_schema_value()
        env.schema = ptrcls.set_field_value(env.schema, 'cardinality', card)
        env.schema = ptrcls.set_field_value(env.schema, 'required', required)
        _update_cardinality_in_derived(ptrcls, env=ctx.env)

    if ptrref:
        out_card, in_card = typeutils.cardinality_from_ptrcls(
            env.schema, ptrcls)
        assert in_card is not None
        assert out_card is not None
        ptrref.in_cardinality = in_card
        ptrref.out_cardinality = out_card


def _update_cardinality_in_derived(
        ptrcls: s_pointers.Pointer, *,
        env: context.Environment) -> None:

    children = env.pointer_derivation_map.get(ptrcls)
    if children:
        ptrcls_cardinality = ptrcls.get_cardinality(env.schema)
        ptrcls_required = ptrcls.get_required(env.schema)
        assert ptrcls_cardinality.is_known()
        for child in children:
            env.schema = child.set_field_value(
                env.schema, 'cardinality', ptrcls_cardinality)
            env.schema = child.set_field_value(
                env.schema, 'required', ptrcls_required)
            _update_cardinality_in_derived(child, env=env)


def _infer_shape(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> None:
    # Mark the source of the shape as being a singleton. We can't just
    # rely on the scope tree, where it might appear as optional
    # (giving us AT_MOST_ONE instead of ONE).
    ctx = ctx._replace(singletons=ctx.singletons | {ir.path_id})

    for shape_set, shape_op in ir.shape:
        new_scope = inf_utils.get_set_scope(shape_set, scope_tree, ctx=ctx)
        if shape_set.expr and shape_set.rptr:
            ptrref = shape_set.rptr.ptrref

            ctx.env.schema, ptrcls = typeutils.ptrcls_from_ptrref(
                ptrref, schema=ctx.env.schema)
            assert isinstance(ptrcls, s_pointers.Pointer)
            specified_card, specified_required, _ = (
                ctx.env.pointer_specified_info.get(ptrcls,
                                                   (None, False, None)))
            assert isinstance(shape_set.expr, irast.Stmt)

            _infer_pointer_cardinality(
                ptrcls=ptrcls,
                ptrref=ptrref,
                source_ctx=shape_set.context,
                irexpr=shape_set.expr,
                is_mut_assignment=is_mutation,
                specified_card=specified_card,
                specified_required=specified_required,
                shape_op=shape_op,
                scope_tree=new_scope,
                ctx=ctx,
            )

        _infer_shape(shape_set, is_mutation=is_mutation, scope_tree=scope_tree,
                     ctx=ctx)


def _infer_set(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:

    # First compute (or look up) the "intrinsic" cardinality of the set
    if not (result := ctx.inferred_cardinality.get(ir)):
        result = _infer_set_inner(
            ir, is_mutation=is_mutation,
            scope_tree=scope_tree, ctx=ctx)

        # We need to cache the main result before doing the shape,
        # since sometimes the shape will refer to the enclosing set.
        ctx.inferred_cardinality[ir] = result

        new_scope = inf_utils.get_set_scope(ir, scope_tree, ctx=ctx)
        _infer_shape(
            ir, is_mutation=is_mutation, scope_tree=new_scope, ctx=ctx)

    # With that in hand, compute the cardinality of a *reference* to the
    # set from this location in the tree.
    if ir.path_id in ctx.singletons:
        return ONE
    elif (node := inf_utils.find_visible(ir, scope_tree)) is not None:
        if not node.optional:
            return ONE
        # If the set is visible, but optional, it must have upper bound ONE
        # but we still want to compute the lower bound.
        else:
            return _bounds_to_card(_card_to_bounds(result).lower, CB_ONE)
    else:
        return result


def _infer_set_inner(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    rptr = ir.rptr
    new_scope = inf_utils.get_set_scope(ir, scope_tree, ctx=ctx)

    if ir.expr:
        expr_card = infer_cardinality(ir.expr, scope_tree=new_scope, ctx=ctx)

    if rptr is not None:
        rptrref = rptr.ptrref

        source_card = infer_cardinality(
            rptr.source, scope_tree=scope_tree, ctx=ctx,
        )

        ctx.env.schema, ptrcls = typeutils.ptrcls_from_ptrref(
            rptrref, schema=ctx.env.schema)
        if ir.expr:
            assert isinstance(ir.expr, irast.Stmt)
            assert isinstance(ptrcls, s_pointers.Pointer)
            _infer_pointer_cardinality(
                ptrcls=ptrcls,
                ptrref=rptrref,
                irexpr=ir.expr,
                scope_tree=scope_tree,
                ctx=ctx,
            )

    # We have now inferred all of the subtrees we need to, so it is
    # safe to return.
    if rptr is not None:
        if isinstance(rptrref, irast.TypeIntersectionPointerRef):
            ind_prefix, ind_ptrs = irutils.collapse_type_intersection(ir)
            if ind_prefix.rptr is None:
                # This prefix will be inferred by the source inference above,
                # so this will just hit the cache and so it is OK for us to
                # be doing it conditionally.
                prefix_card = infer_cardinality(
                    ind_prefix, scope_tree=scope_tree, ctx=ctx,
                )

                card = cartesian_cardinality([prefix_card, AT_MOST_ONE])
            else:
                # Expression before type intersection is a path,
                # i.e Foo.<bar[IS Type].  In this case we must
                # take possible intersection specialization of the
                # link union into account.
                # We're basically restating the body of this function
                # in this block, but with extra conditions.
                if inf_utils.find_visible(ind_prefix, new_scope) is not None:
                    return AT_MOST_ONE
                else:
                    rptr_spec: Set[irast.PointerRef] = set()
                    for ind_ptr in ind_ptrs:
                        rptr_spec.update(ind_ptr.ptrref.rptr_specialization)

                    rptr_spec_card = _union_cardinality(
                        s.dir_cardinality(ind_prefix.rptr.direction)
                        for s in rptr_spec)

                    # If the intersection has an rptr_specialization,
                    # then we take a step back and start with
                    # the source of *that*, which lets us take
                    # advantage of std::exclusive on links when using
                    # reverse pointers with multiple possibilities.
                    if rptr_spec:
                        # Already inferred, should just be hitting cache.
                        source_card = infer_cardinality(
                            ind_prefix.rptr.source,
                            scope_tree=scope_tree, ctx=ctx,
                        )

                    # The resulting cardinality is the cartesian
                    # product of the base to which the type
                    # intersection is applied and the cardinality due
                    # to type intersection itself.
                    card = cartesian_cardinality([source_card, rptr_spec_card])

        else:
            if rptrref.union_components:
                # We use cartesian cardinality instead of union cardinality
                # because the union of pointers in this context is disjoint
                # in a sense that for any specific source only a given union
                # component is used.
                rptrref_card = cartesian_cardinality(
                    c.dir_cardinality(rptr.direction)
                    for c in rptrref.union_components
                )
            elif ctx.ignore_computed_cards and ir.expr:
                rptrref_card = expr_card
            else:
                rptrref_card = rptrref.dir_cardinality(rptr.direction)

            if rptrref_card.is_single():
                card = cartesian_cardinality((source_card, rptrref_card))
            else:
                card = MANY

    elif isinstance(ir, irast.EmptySet):
        card = AT_MOST_ONE
    elif ir.expr is not None:
        card = expr_card
    elif typeutils.is_free_object(ir.typeref) and not ir.is_binding:
        card = ONE
    else:
        card = MANY

    # If this node is an optional argument bound at this location,
    # but it can't actually be zero, clear the optionality to avoid
    # subpar codegen.
    if (
        (node := scope_tree.find_child(ir.path_id)) is not None
        and node.optional
        and not card.can_be_zero()
    ):
        node.optional = False

    return card


@_infer_cardinality.register
def __infer_func_call(
    ir: irast.FunctionCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return_card = (
        MANY if ir.typemod is qltypes.TypeModifier.SetOfType else
        AT_MOST_ONE if ir.typemod is qltypes.TypeModifier.OptionalType else
        ONE
    )

    ret_lower_bound, ret_upper_bound = _card_to_bounds(return_card)

    for glob_arg in (ir.global_args or ()):
        infer_cardinality(glob_arg, scope_tree=scope_tree, ctx=ctx)

    if ir.preserves_optionality or ir.preserves_upper_cardinality:
        # This is a generic aggregate function which preserves the
        # optionality and/or upper cardinality of its generic
        # argument.  For simplicity we are deliberately not checking
        # the parameters here as that would have been done at the time
        # of declaration.
        arg_cards = []

        for arg, typemod in zip(ir.args, ir.params_typemods):
            arg.cardinality = infer_cardinality(
                arg.expr, scope_tree=scope_tree, ctx=ctx)

            if typemod is not qltypes.TypeModifier.OptionalType:
                arg_cards.append(arg.cardinality)

        arg_card = zip(*(_card_to_bounds(card) for card in arg_cards))
        arg_lower, arg_upper = arg_card
        lower = (
            min(arg_lower) if ir.preserves_optionality else
            CB_ONE if ir.func_shortname == sn.QualName('std', 'assert_exists')
            else ret_lower_bound
        )
        upper = (max(arg_upper) if ir.preserves_upper_cardinality
                 else ret_upper_bound)
        return _bounds_to_card(lower, upper)

    else:
        # For regular non-OPTIONAL functions, the general rule of
        # Cartesian cardinality of arguments applies, although we still
        # have to account for the declared return cardinality, as the
        # function might be OPTIONAL or SET OF in its return type.
        #
        # If a function is OPTIONAL in its parameters, which includes
        # aggregate functions, then we compute a Cartesian cardinality
        # of functions's _non-OPTIONAL_ arguments and its return
        # cardinality, but only in the upper bound, since we cannot know
        # how the function behaves in OPTIONAL arguments.
        non_aggregate_args = []
        non_aggregate_arg_cards = []
        singleton_args = []
        singleton_arg_cards = []
        all_singletons = True

        for arg, typemod in zip(ir.args, ir.params_typemods):
            arg.cardinality = infer_cardinality(
                arg.expr, scope_tree=scope_tree, ctx=ctx)
            if typemod is not qltypes.TypeModifier.SetOfType:
                non_aggregate_args.append(arg.expr)
                non_aggregate_arg_cards.append(arg.cardinality)
            if typemod is qltypes.TypeModifier.SingletonType:
                singleton_args.append(arg.expr)
                singleton_arg_cards.append(arg.cardinality)
            else:
                all_singletons = False

        if non_aggregate_args:
            _check_op_volatility(
                non_aggregate_args, non_aggregate_arg_cards, ctx=ctx)

        if not singleton_args:
            # Either no arguments at all, or all arguments are non-singletons,
            # so the declared return cardinality is as specific as we can get.
            return return_card
        else:
            result = cartesian_cardinality(singleton_arg_cards + [return_card])
            if not all_singletons:
                result = _bounds_to_card(
                    ret_lower_bound, _card_to_bounds(result).upper)
            return result


@_infer_cardinality.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    cards = []
    for arg in ir.args:
        arg.cardinality = infer_cardinality(
            arg.expr, scope_tree=scope_tree, ctx=ctx)
        cards.append(arg.cardinality)

    if str(ir.func_shortname) == 'std::UNION':
        # UNION needs to "add up" cardinalities.
        return _union_cardinality(cards)
    elif str(ir.func_shortname) == 'std::??':
        # Coalescing takes the maximum of both lower and upper bounds.
        return max_cardinality(cards)
    else:
        args: List[irast.Base] = []
        all_optional = False

        if ir.typemod is qltypes.TypeModifier.SetOfType:
            # this is DISTINCT and IF..ELSE
            args = [a.expr for a in ir.args]
        else:
            all_optional = True
            for arg, typemod in zip(ir.args, ir.params_typemods):
                if typemod is not qltypes.TypeModifier.SetOfType:
                    all_optional &= (
                        typemod is qltypes.TypeModifier.OptionalType
                    )
                    args.append(arg.expr)

        if args:
            card = _common_cardinality(
                args, scope_tree=scope_tree, ctx=ctx,
            )
            if all_optional:
                # An operator that has all optional arguments and
                # doesn't return a SET OF returns at least ONE result
                # (we currently don't have operators that return
                # OPTIONAL). So we upgrade the lower bound.
                card = _bounds_to_card(CB_ONE, _card_to_bounds(card).upper)

            return card
        else:
            if ir.typemod is qltypes.TypeModifier.OptionalType:
                return AT_MOST_ONE
            else:
                return ONE


@_infer_cardinality.register
def __infer_const(
    ir: irast.BaseConstant,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return ONE


@_infer_cardinality.register
def __infer_param(
    ir: irast.Parameter,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return ONE if ir.required else AT_MOST_ONE


@_infer_cardinality.register
def __infer_const_set(
    ir: irast.ConstantSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return ONE if len(ir.elements) == 1 else AT_LEAST_ONE


@_infer_cardinality.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.left, scope_tree=scope_tree, ctx=ctx,
    )


@_infer_cardinality.register
def __infer_typecast(
    ir: irast.TypeCast,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    card = infer_cardinality(
        ir.expr, scope_tree=scope_tree, ctx=ctx,
    )
    # json values can be 'null', which produces an empty set, which we
    # need to reflect in the cardinality.
    if (
        typeutils.is_json(ir.from_type)
        and not ir.cardinality_mod == qlast.CardinalityModifier.Required
    ):
        card = _bounds_to_card(CB_ZERO, _card_to_bounds(card).upper)
    return card


def _is_ptr_or_self_ref(
    ir_expr: irast.Base,
    result_expr: irast.Set,
    env: context.Environment,
) -> bool:
    if not isinstance(ir_expr, irast.Set):
        return False
    else:
        ir_set = ir_expr
        srccls = env.set_types[result_expr]

        return (
            isinstance(srccls, s_objtypes.ObjectType)
            and (
                ir_expr.path_id == result_expr.path_id
                or (
                    (rptr := ir_set.rptr) is not None
                    and isinstance(rptr.ptrref, irast.PointerRef)
                    and not rptr.ptrref.is_computable
                    and _is_ptr_or_self_ref(rptr.source, result_expr, env)
                )
            )
        )


def extract_filters(
    result_set: irast.Set,
    filter_set: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> Sequence[Tuple[Sequence[s_pointers.Pointer], irast.Set]]:

    env = ctx.env
    schema = env.schema
    scope_tree = inf_utils.get_set_scope(filter_set, scope_tree, ctx=ctx)

    expr = filter_set.expr
    if isinstance(expr, irast.OperatorCall):
        if str(expr.func_shortname) == 'std::=':
            left, right = (a.expr for a in expr.args)

            op_card = _common_cardinality(
                [left, right], scope_tree=scope_tree, ctx=ctx
            )
            result_stype = env.set_types[result_set]

            if op_card.is_multi():
                pass

            elif (
                (left_matches := _is_ptr_or_self_ref(left, result_set, env))
                or _is_ptr_or_self_ref(right, result_set, env)
            ):
                # If the match was on the right, flip the args
                if not left_matches:
                    left, right = right, left

                if infer_cardinality(
                    right, scope_tree=scope_tree, ctx=ctx,
                ).is_single():
                    ptrs = []
                    left_stype = env.set_types[left]
                    if left_stype == result_stype:
                        assert isinstance(left_stype, s_objtypes.ObjectType)
                        _ptr = left_stype.getptr(schema, sn.UnqualName('id'))
                        ptrs.append(_ptr)
                    else:
                        while left.path_id != result_set.path_id:
                            assert left.rptr is not None
                            _ptr = env.schema.get(left.rptr.ptrref.name,
                                                  type=s_pointers.Pointer)
                            ptrs.append(_ptr)
                            left = left.rptr.source
                        ptrs.reverse()

                    return [(ptrs, right)]

        elif str(expr.func_shortname) == 'std::AND':
            left, right = (a.expr for a in expr.args)

            left_filters = extract_filters(
                result_set, left, scope_tree, ctx
            )
            right_filters = extract_filters(
                result_set, right, scope_tree, ctx
            )

            return [*left_filters, *right_filters]

    return []


def _all_have_exclusive(
    ptrs: Sequence[s_pointers.Pointer],
    ctx: inference_context.InfCtx,
) -> bool:
    return all(
        bool(ptr.get_exclusive_constraints(ctx.env.schema))
        for ptr in ptrs
    )


def _track_all_constraint_refs(
    ptrs: Sequence[s_pointers.Pointer],
    ctx: inference_context.InfCtx,
) -> None:
    for ptr in ptrs:
        for constr in ptr.get_exclusive_constraints(ctx.env.schema):
            # We need to track all schema refs, since an expression
            # in the schema needs to depend on any constraint
            # that affects its cardinality.
            ctx.env.add_schema_ref(constr, None)


def extract_exclusive_filters(
    result_set: irast.Set,
    filter_set: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> List[Tuple[Tuple[s_pointers.Pointer, irast.Set], ...]]:

    filtered_ptrs = extract_filters(result_set, filter_set, scope_tree, ctx)

    results: List[Tuple[Tuple[s_pointers.Pointer, irast.Set], ...]] = []
    if filtered_ptrs:
        schema = ctx.env.schema
        # Only look at paths where all trailing pointers are exclusive;
        # that is, if we see `.foo.bar`, `bar` must be exclusive.
        # If that's the case, then we can look at whether `.foo` is
        # exclusive or used in an exclusive object constraint.
        filtered_ptrs_map = {
            ptrs[0].get_nearest_non_derived_parent(schema): (ptrs, expr)
            for ptrs, expr in filtered_ptrs
            if _all_have_exclusive(ptrs[1:], ctx)
        }
        ptr_set = set(filtered_ptrs_map)
        # First look at each referenced pointer and see if it has
        # an exclusive constraint.
        for ptr, (ptrs, expr) in filtered_ptrs_map.items():
            if _all_have_exclusive([ptr], ctx):
                # Bingo, got an equality filter on a pointer with a
                # unique constraint
                results.append(((ptr, expr),))
                _track_all_constraint_refs(ptrs, ctx)

        # Then look at all the object exclusive constraints
        result_stype = ctx.env.set_types[result_set]
        obj_exclusives = get_object_exclusive_constraints(
            result_stype, ptr_set, ctx.env)
        for constr, obj_exc_ptrs in obj_exclusives.items():
            results.append(
                tuple((ptr, filtered_ptrs_map[ptr][1]) for ptr in obj_exc_ptrs)
            )
            ctx.env.add_schema_ref(constr, None)
            for ptr in obj_exc_ptrs:
                _track_all_constraint_refs(filtered_ptrs_map[ptr][0], ctx)

    return results


def get_object_exclusive_constraints(
    typ: s_types.Type,
    ptr_set: Set[s_pointers.Pointer],
    env: context.Environment,
) -> Dict[s_constraints.Constraint, FrozenSet[s_pointers.Pointer]]:
    """Collect any exclusive object constraints that apply.

    An object constraint applies if all of the pointers referenced
    in it are filtered on in the query.
    """

    if not isinstance(typ, s_objtypes.ObjectType):
        return {}

    schema = env.schema
    exclusive = schema.get('std::exclusive', type=s_constraints.Constraint)

    cnstrs = {}
    typ = typ.get_nearest_non_derived_parent(schema)
    for constr in typ.get_constraints(schema).objects(schema):
        if (
            constr.issubclass(schema, exclusive)
            and (subjectexpr := constr.get_subjectexpr(schema))
            # We ignore constraints with except expressions, because
            # they can't actually ensure cardinality
            and not constr.get_except_expr(schema)
        ):
            if subjectexpr.refs is None:
                continue
            pointer_refs = frozenset({
                x for x in subjectexpr.refs.objects(schema)
                if isinstance(x, s_pointers.Pointer)
            })
            # If all of the referenced pointers are filtered on,
            # we match.
            if pointer_refs.issubset(ptr_set):
                cnstrs[constr] = pointer_refs

    return cnstrs


def _analyse_filter_clause(
    result_set: irast.Set,
    result_card: qltypes.Cardinality,
    filter_clause: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    if extract_exclusive_filters(result_set, filter_clause, scope_tree, ctx):
        return AT_MOST_ONE
    else:
        return result_card


def _infer_matset_cardinality(
    materialized_sets: Optional[Dict[uuid.UUID, irast.MaterializedSet]],
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> None:
    if not materialized_sets:
        return

    for mat_set in materialized_sets.values():
        if (len(mat_set.uses) <= 1
                or mat_set.cardinality != qltypes.Cardinality.UNKNOWN):
            continue
        assert mat_set.materialized
        # set it to something to prevent recursion
        mat_set.cardinality = MANY
        new_scope = inf_utils.get_set_scope(
            mat_set.materialized, scope_tree, ctx=ctx)
        mat_set.cardinality = infer_cardinality(
            mat_set.materialized, scope_tree=new_scope, ctx=ctx,
        )


def _infer_stmt_cardinality(
    ir: irast.FilteredStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    result_card = infer_cardinality(
        ir.subject if isinstance(ir, irast.MutatingStmt) else ir.result,
        is_mutation=isinstance(ir, irast.MutatingStmt),
        scope_tree=scope_tree,
        ctx=ctx,
    )
    if ir.where:
        ir.where_card = infer_cardinality(
            ir.where, scope_tree=scope_tree, ctx=ctx,
        )
        # Cross with AT_MOST_ONE to ensure result can be empty
        result_card = cartesian_cardinality([result_card, AT_MOST_ONE])

    if result_card.is_multi() and ir.where:
        result_card = _analyse_filter_clause(
            ir.result, result_card, ir.where, scope_tree, ctx)

    _infer_matset_cardinality(
        ir.materialized_sets, scope_tree=scope_tree, ctx=ctx)

    if isinstance(ir, irast.MutatingStmt):
        pctx = ctx._replace(singletons=ctx.singletons | {ir.result.path_id})
        for pol in [
            *ir.read_policy_exprs.values(), *ir.write_policy_exprs.values()
        ]:
            pol.cardinality = infer_cardinality(
                pol.expr, scope_tree=scope_tree, ctx=pctx)

    return result_card


def _infer_singleton_only(
    part: irast.Set,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    new_scope = inf_utils.get_set_scope(part, scope_tree, ctx=ctx)
    card = infer_cardinality(part, scope_tree=new_scope, ctx=ctx)
    if card.is_multi():
        raise errors.QueryError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=part.context)
    return card


@_infer_cardinality.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:

    if ir.iterator_stmt:
        iter_card = infer_cardinality(
            ir.iterator_stmt, scope_tree=scope_tree, ctx=ctx,
        )

    stmt_card = _infer_stmt_cardinality(ir, scope_tree=scope_tree, ctx=ctx)

    for part in [ir.limit, ir.offset] + [
            sort.expr for sort in (ir.orderby or ())]:
        if part:
            _infer_singleton_only(part, scope_tree=scope_tree, ctx=ctx)

    if ir.limit is not None:
        if (
            isinstance(ir.limit.expr, irast.IntegerConstant)
            and ir.limit.expr.value == '1'
        ):
            # Explicit LIMIT 1 clause.
            stmt_card = _bounds_to_card(
                _card_to_bounds(stmt_card).lower, CB_ONE)
        elif (
            not isinstance(ir.limit.expr, irast.IntegerConstant)
            or ir.limit.expr.value == '0'
        ):
            # LIMIT 0 or a non-static LIMIT that could be 0
            stmt_card = _bounds_to_card(
                CB_ZERO, _card_to_bounds(stmt_card).upper)

    if ir.offset is not None:
        stmt_card = _bounds_to_card(
            CB_ZERO, _card_to_bounds(stmt_card).upper)

    if ir.iterator_stmt:
        stmt_card = cartesian_cardinality((stmt_card, iter_card))

    return stmt_card


@_infer_cardinality.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:

    infer_cardinality(
        ir.subject, is_mutation=True, scope_tree=scope_tree, ctx=ctx
    )
    new_scope = inf_utils.get_set_scope(ir.result, scope_tree, ctx=ctx)
    infer_cardinality(
        ir.result, is_mutation=True, scope_tree=new_scope, ctx=ctx
    )

    assert not ir.iterator_stmt, "InsertStmt shouldn't ever have an iterator"

    _infer_matset_cardinality(
        ir.materialized_sets, scope_tree=scope_tree, ctx=ctx)

    # INSERT without a FOR is always a singleton.
    if not ir.on_conflict:
        return ONE
    # ... except if UNLESS CONFLICT is used
    else:
        # Note: If we start supporting ELSE without ON, we'll need to
        # factor the cardinality of this into the else_card below
        infer_cardinality(
            ir.on_conflict.select_ir, scope_tree=scope_tree, ctx=ctx)

        card = AT_MOST_ONE
        if ir.on_conflict.else_ir:
            else_card = infer_cardinality(
                ir.on_conflict.else_ir, scope_tree=scope_tree, ctx=ctx)
            card = max_cardinality((card, else_card))
            if ir.write_policy_exprs:
                card = _bounds_to_card(CB_ZERO, _card_to_bounds(card).upper)

        return card


@_infer_cardinality.register
def __infer_update_stmt(
    ir: irast.UpdateStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    infer_cardinality(
        ir.result, is_mutation=True,
        scope_tree=scope_tree, ctx=ctx,
    )

    return _infer_stmt_cardinality(ir, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_delete_stmt(
    ir: irast.DeleteStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    infer_cardinality(
        ir.result, scope_tree=scope_tree, ctx=ctx,
    )

    return _infer_stmt_cardinality(ir, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_group_stmt(
    ir: irast.GroupStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    infer_cardinality(ir.subject, scope_tree=scope_tree, ctx=ctx)
    for key, (binding, _) in ir.using.items():
        binding_card = _infer_singleton_only(
            binding, scope_tree=scope_tree, ctx=ctx)
        ir.using[key] = binding, binding_card

    infer_cardinality(ir.group_binding, scope_tree=scope_tree, ctx=ctx)

    _infer_stmt_cardinality(ir, scope_tree=scope_tree, ctx=ctx)

    for part in (ir.orderby or ()):
        _infer_singleton_only(part.expr, scope_tree=scope_tree, ctx=ctx)

    return MANY


@_infer_cardinality.register
def __infer_slice(
    ir: irast.SliceIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    # slice indirection cardinality depends on the cardinality of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_cardinality(args, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_index(
    ir: irast.IndexIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    # index indirection cardinality depends on both the cardinality of
    # the base expression and the index expression
    return _common_cardinality(
        [ir.expr, ir.index], scope_tree=scope_tree, ctx=ctx,
    )


@_infer_cardinality.register
def __infer_array(
    ir: irast.Array,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    return _common_cardinality(ir.elements, scope_tree=scope_tree, ctx=ctx)


@_infer_cardinality.register
def __infer_tuple(
    ir: irast.Tuple,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    if not ir.elements:
        return ONE
    return _common_cardinality(
        [el.val for el in ir.elements], scope_tree=scope_tree, ctx=ctx
    )


def infer_cardinality(
    ir: irast.Base,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inference_context.InfCtx,
) -> qltypes.Cardinality:
    key = (ir, scope_tree, ctx.singletons)
    result = ctx.inferred_cardinality.get(key)
    if result is not None:
        return result

    if isinstance(ir, irast.Set):
        result = _infer_set(
            ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx,
        )
    else:
        result = _infer_cardinality(ir, scope_tree=scope_tree, ctx=ctx)

    if result not in {AT_MOST_ONE, ONE, MANY, AT_LEAST_ONE}:
        raise errors.QueryError(
            'could not determine the cardinality of '
            'set produced by expression',
            context=ir.context)

    ctx.inferred_cardinality[key] = result

    return result


def is_subset_cardinality(
    card0: qltypes.Cardinality,
    card1: qltypes.Cardinality
) -> bool:
    '''Determine if card0 is a subset of card1.'''
    l0, u0 = _card_to_bounds(card0)
    l1, u1 = _card_to_bounds(card1)

    return l0 >= l1 and u0 <= u1
