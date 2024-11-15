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
from typing import Tuple, Iterable, List

import dataclasses
import functools
import itertools

from edb.common.typeutils import downcast

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
from . import utils as inf_utils


EMPTY = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.EMPTY)
UNIQUE = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.UNIQUE)
DUPLICATE = inf_ctx.MultiplicityInfo(own=qltypes.Multiplicity.DUPLICATE)
DISTINCT_UNION = inf_ctx.MultiplicityInfo(
    own=qltypes.Multiplicity.UNIQUE,
    disjoint_union=True,
)


@dataclasses.dataclass(frozen=True, eq=False)
class ContainerMultiplicityInfo(inf_ctx.MultiplicityInfo):
    """Multiplicity descriptor for an expression returning a container"""

    #: Individual multiplicity values for container elements.
    elements: Tuple[inf_ctx.MultiplicityInfo, ...] = ()


def _max_multiplicity(
    args: Iterable[inf_ctx.MultiplicityInfo],
) -> inf_ctx.MultiplicityInfo:
    arg_list = [a.own for a in args]
    if not arg_list:
        max_mult = qltypes.Multiplicity.UNIQUE
    else:
        max_mult = max(arg_list)

    return inf_ctx.MultiplicityInfo(own=max_mult)


def _min_multiplicity(
    args: Iterable[inf_ctx.MultiplicityInfo],
) -> inf_ctx.MultiplicityInfo:
    arg_list = [a.own for a in args]
    if not arg_list:
        min_mult = qltypes.Multiplicity.UNIQUE
    else:
        min_mult = min(arg_list)

    return inf_ctx.MultiplicityInfo(own=min_mult)


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
    # return DUPLICATE
    raise ValueError(f'infer_multiplicity: cannot handle {ir!r}')


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
        return UNIQUE


@_infer_multiplicity.register
def __infer_empty_set(
    ir: irast.EmptySet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return EMPTY


@_infer_multiplicity.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # TODO: The result is always UNIQUE, but we still want to actually
    # introspect the expression. Unfortunately, currently the
    # expression is not available at this stage.
    #
    # E.g. consider:
    #   WITH X := Foo {bar := {Bar, Bar}}
    #   SELECT INTROSPECT TYPEOF X.bar;
    return UNIQUE


@_infer_multiplicity.register
def __infer_type_root(
    ir: irast.TypeRoot,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return UNIQUE


@_infer_multiplicity.register
def __infer_cleared(
    ir: irast.RefExpr,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return DUPLICATE


def _infer_shape(
    ir: irast.Set,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> None:
    for shape_set, shape_op in ir.shape:
        new_scope = inf_utils.get_set_scope(shape_set, scope_tree, ctx=ctx)

        rptr = shape_set.expr
        if rptr.expr:
            expr_mult = infer_multiplicity(
                rptr.expr, scope_tree=new_scope, ctx=ctx)

            ptrref = rptr.ptrref
            if (
                expr_mult.is_duplicate()
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
                    span=shape_set.span
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
        ir, scope_tree=scope_tree, ctx=ctx
    )
    ctx.inferred_multiplicity[ir, scope_tree, ctx.distinct_iterator] = result

    # The shape doesn't affect multiplicity, but requires validation.
    _infer_shape(ir, is_mutation=is_mutation, scope_tree=scope_tree, ctx=ctx)

    return result


def _infer_set_inner(
    ir: irast.Set,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    new_scope = inf_utils.get_set_scope(ir, scope_tree, ctx=ctx)

    # TODO: Migrate to Pointer-as-Expr well, and not half-assedly.
    sub_expr = irutils.sub_expr(ir)
    if sub_expr is None:
        expr_mult = None
    else:
        expr_mult = infer_multiplicity(sub_expr, scope_tree=new_scope, ctx=ctx)

    if isinstance(ir.expr, irast.Pointer):
        ptr = ir.expr
        src_mult = infer_multiplicity(
            ptr.source, scope_tree=new_scope, ctx=ctx
        )

        if isinstance(ptr.ptrref, irast.TupleIndirectionPointerRef):
            if isinstance(src_mult, ContainerMultiplicityInfo):
                idx = irtyputils.get_tuple_element_index(ptr.ptrref)
                path_mult = src_mult.elements[idx]
            else:
                # All bets are off for tuple elements coming from
                # opaque tuples.
                path_mult = DUPLICATE
        elif not irtyputils.is_object(ir.typeref):
            # This is not an expression and is some kind of scalar, so
            # multiplicity cannot be guaranteed to be UNIQUE (most scalar
            # expressions don't have an implicit requirement to be sets)
            # unless we also have an exclusive constraint.
            if (
                expr_mult is not None
                and inf_utils.find_visible(ptr.source, new_scope) is not None
            ):
                path_mult = expr_mult
            else:
                schema = ctx.env.schema
                # We should only have some kind of path terminating in a
                # property here.
                assert isinstance(ptr.ptrref, irast.PointerRef)
                pointer = schema.get_by_id(
                    ptr.ptrref.id, type=s_pointers.Pointer
                )
                if pointer.is_exclusive(schema):
                    # Got an exclusive constraint
                    path_mult = UNIQUE
                else:
                    path_mult = DUPLICATE
        else:
            # This is some kind of a link at the end of a path.
            # Therefore the target is a proper set.
            path_mult = UNIQUE

    elif expr_mult is not None:
        path_mult = expr_mult

    else:
        # Evidently this is not a pointer, expression, or a scalar.
        # This is an object type and therefore a proper set.
        path_mult = UNIQUE

    if (
        not path_mult.is_duplicate()
        and irutils.get_path_root(ir).path_id == ctx.distinct_iterator
    ):
        path_mult = dataclasses.replace(path_mult, disjoint_union=True)

    # Mark free object roots
    if irutils.is_trivial_free_object(ir):
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
    for arg in ir.args.values():
        arg_mult = infer_multiplicity(arg.expr, scope_tree=scope_tree, ctx=ctx)
        args_mult.append(arg_mult)
        arg.multiplicity = arg_mult.own

    if ir.global_args:
        for g_arg in ir.global_args:
            _infer_set(g_arg, scope_tree=scope_tree, ctx=ctx)

    if ir.body:
        infer_multiplicity(ir.body, scope_tree=scope_tree, ctx=ctx)

    if card.is_single():
        return UNIQUE
    elif str(ir.func_shortname) == 'std::assert_distinct':
        return UNIQUE
    elif str(ir.func_shortname) == 'std::assert_exists':
        return args_mult[1]
    elif str(ir.func_shortname) == 'std::enumerate':
        # The output of enumerate is always of multiplicity UNIQUE because
        # it's a set of tuples with first elements being guaranteed to be
        # distinct.
        return ContainerMultiplicityInfo(
            own=qltypes.Multiplicity.UNIQUE,
            elements=(UNIQUE,) + tuple(args_mult),
        )
    else:
        # If the function returns a set (for any reason), all bets are off
        # and the maximum multiplicity cannot be inferred.
        return DUPLICATE


@_infer_multiplicity.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    card = cardinality.infer_cardinality(ir, scope_tree=scope_tree, ctx=ctx)
    mult: List[inf_ctx.MultiplicityInfo] = []
    cards: List[qltypes.Cardinality] = []
    for arg in ir.args.values():
        cards.append(
            cardinality.infer_cardinality(
                arg.expr, scope_tree=scope_tree, ctx=ctx
            )
        )

        m = infer_multiplicity(arg.expr, scope_tree=scope_tree, ctx=ctx)
        arg.multiplicity = m.own
        mult.append(m)

    op_name = str(ir.func_shortname)

    if op_name == 'std::UNION':
        # UNION will produce multiplicity DUPLICATE unless most or all of
        # the elements multiplicity is ZERO (from an empty set), or
        # all of the elements are sets of unrelated object types of
        # multiplicity at most UNIQUE, or if all elements have been
        # proven to be disjoint (e.g. a UNION of INSERTs).
        result = EMPTY

        arg_type = ctx.env.set_types[ir.args[0].expr]
        if isinstance(arg_type, s_objtypes.ObjectType):
            types: List[s_objtypes.ObjectType] = [
                downcast(s_objtypes.ObjectType, ctx.env.set_types[arg.expr])
                for arg in ir.args.values()
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
            if m.is_unique():
                if (
                    result.is_empty()
                    or types_disjoint
                    or (result.disjoint_union and m.disjoint_union)
                ):
                    result = m
                else:
                    result = DUPLICATE
                    break
            elif m.is_duplicate():
                result = DUPLICATE
                break
            else:
                # ZERO
                pass

        return result

    elif op_name == 'std::EXCEPT':
        # EXCEPT will produce multiplicity no greater than that of its first
        # argument.
        return mult[0]

    elif op_name == 'std::INTERSECT':
        # INTERSECT will produce the minimum multiplicity of its arguments.
        return _min_multiplicity((mult[0], mult[1]))

    elif op_name == 'std::DISTINCT':
        if mult[0] == EMPTY:
            return EMPTY
        else:
            return UNIQUE
    elif op_name == 'std::IF':
        # If the cardinality of the condition is more than ONE, then
        # the multiplicity cannot be inferred.
        if cards[1].is_single():
            # Now it's just a matter of the multiplicity of the
            # possible results.
            return _max_multiplicity((mult[0], mult[2]))
        else:
            return DUPLICATE
    elif op_name == 'std::??':
        return _max_multiplicity((mult[0], mult[1]))
    elif card.is_single():
        return UNIQUE
    elif op_name in ('std::++', 'std::+'):
        # Operators known to be injective.
        # Basically just done to avoid breaking backward compatability
        # more than was necessary, because we used to *always* use this
        # path, which was wrong.
        result = _max_multiplicity(mult)
        if result.is_duplicate():
            return result

        # Even when arguments are of multiplicity UNIQUE, we cannot
        # exclude the possibility of the result being of multiplicity
        # DUPLICATE. We need to check that at most one argument has
        # cardinality more than ONE.

        if len([card for card in cards if card.is_multi()]) > 1:
            return DUPLICATE
        else:
            return result
    else:
        # Everything else.
        return DUPLICATE


@_infer_multiplicity.register
def __infer_const(
    ir: irast.BaseConstant,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return UNIQUE


@_infer_multiplicity.register
def __infer_param(
    ir: irast.Parameter,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return UNIQUE


@_infer_multiplicity.register
def __infer_inlined_param(
    ir: irast.InlinedParameterExpr,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return UNIQUE


@_infer_multiplicity.register
def __infer_const_set(
    ir: irast.ConstantSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Is it worth doing this? It won't trigger in the common case of having
    # performed constant extraction.
    els = set()
    for el in ir.elements:
        if isinstance(el, irast.BaseConstant):
            els.add(el.value)
        else:
            return DUPLICATE

    if len(ir.elements) == len(els):
        return UNIQUE
    else:
        return DUPLICATE


@_infer_multiplicity.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Unless this is a singleton, multiplicity cannot be assumed to be UNIQUE
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)

    infer_multiplicity(ir.left, scope_tree=scope_tree, ctx=ctx)

    if card is not None and card.is_single():
        return UNIQUE
    else:
        return DUPLICATE


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
    # WITH block bindings need to be validated; they don't have to
    # have multiplicity UNIQUE, but their sub-expressions must be valid.
    for part, _ in (ir.bindings or []):
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
            # reference enclosing iterators with multiplicity UNIQUE, and
            # if so, indicate to the enclosing iterator that this UNION
            # is guaranteed to be disjoint.
            if (
                irutils.get_path_root(flt_expr).path_id
                == ctx.distinct_iterator
                or irutils.get_path_root(irutils.unwrap_set(flt_expr)).path_id
                == ctx.distinct_iterator
            ) and not infer_multiplicity(
                flt_expr, scope_tree=scope_tree, ctx=ctx
            ).is_duplicate():
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

    if itmult != DUPLICATE:
        new_iter = itset.path_id if not ctx.distinct_iterator else None
        ctx = ctx._replace(distinct_iterator=new_iter)
    result_mult = infer_multiplicity(ir.result, scope_tree=scope_tree, ctx=ctx)

    if isinstance(ir.result.expr, irast.InsertStmt):
        # A union of inserts always has multiplicity UNIQUE
        return UNIQUE
    elif itmult.is_duplicate():
        return DUPLICATE
    else:
        if result_mult.disjoint_union or result_mult.fresh_free_object:
            return result_mult
        else:
            return DUPLICATE


@_infer_multiplicity.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:

    if ir.iterator_stmt is not None:
        stmt_mult = _infer_for_multiplicity(ir, scope_tree=scope_tree, ctx=ctx)
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

    if ir.card_inference_override:
        stmt_mult = infer_multiplicity(
            ir.card_inference_override, scope_tree=scope_tree, ctx=ctx)

    return stmt_mult


@_infer_multiplicity.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # WITH block bindings need to be validated, they don't have to
    # have multiplicity UNIQUE, but their sub-expressions must be valid.
    for part, _ in (ir.bindings or []):
        infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)

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
        _infer_on_conflict_clause(
            ir.on_conflict, scope_tree=scope_tree, ctx=ctx
        )

    _infer_mutating_stmt(ir, scope_tree=scope_tree, ctx=ctx)

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

    _infer_mutating_stmt(ir, scope_tree=scope_tree, ctx=ctx)

    if result is EMPTY:
        return EMPTY
    else:
        return UNIQUE


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

    _infer_mutating_stmt(ir, scope_tree=scope_tree, ctx=ctx)

    if result is EMPTY:
        return EMPTY
    else:
        return UNIQUE


def _infer_mutating_stmt(
    ir: irast.MutatingStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> None:
    if ir.conflict_checks:
        for clause in ir.conflict_checks:
            _infer_on_conflict_clause(clause, scope_tree=scope_tree, ctx=ctx)

    for write_pol in ir.write_policies.values():
        for policy in write_pol.policies:
            infer_multiplicity(policy.expr, scope_tree=scope_tree, ctx=ctx)

    for read_pol in ir.read_policies.values():
        infer_multiplicity(read_pol.expr, scope_tree=scope_tree, ctx=ctx)

    if ir.rewrites:
        for rewrites in ir.rewrites.by_type.values():
            for rewrite, _ in rewrites.values():
                infer_multiplicity(
                    rewrite,
                    is_mutation=True,
                    scope_tree=scope_tree,
                    ctx=ctx,
                )


def _infer_on_conflict_clause(
    ir: irast.OnConflictClause,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> None:
    for part in [ir.select_ir, ir.else_ir, ir.update_query_set]:
        if part:
            infer_multiplicity(part, scope_tree=scope_tree, ctx=ctx)


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

    infer_multiplicity(ir.group_binding, scope_tree=scope_tree, ctx=ctx)
    if ir.grouping_binding:
        infer_multiplicity(ir.grouping_binding, scope_tree=scope_tree, ctx=ctx)

    for set in ir.group_aggregate_sets:
        if set:
            infer_multiplicity(set, scope_tree=scope_tree, ctx=ctx)

    if result_mult.fresh_free_object:
        return result_mult
    else:
        return DUPLICATE


@_infer_multiplicity.register
def __infer_slice(
    ir: irast.SliceIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Slice indirection multiplicity is guaranteed to be UNIQUE as long
    # as the cardinality of this expression is at most one, otherwise
    # the results of index indirection can contain values with
    # multiplicity > 1.

    infer_multiplicity(ir.expr, scope_tree=scope_tree, ctx=ctx)
    if ir.start:
        infer_multiplicity(ir.start, scope_tree=scope_tree, ctx=ctx)
    if ir.stop:
        infer_multiplicity(ir.stop, scope_tree=scope_tree, ctx=ctx)

    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)
    if card is not None and card.is_single():
        return UNIQUE
    else:
        return DUPLICATE


@_infer_multiplicity.register
def __infer_index(
    ir: irast.IndexIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    # Index indirection multiplicity is guaranteed to be UNIQUE as long
    # as the cardinality of this expression is at most one, otherwise
    # the results of index indirection can contain values with
    # multiplicity > 1.
    card = cardinality.infer_cardinality(
        ir, scope_tree=scope_tree, ctx=ctx)

    infer_multiplicity(ir.expr, scope_tree=scope_tree, ctx=ctx)
    infer_multiplicity(ir.index, scope_tree=scope_tree, ctx=ctx)

    if card is not None and card.is_single():
        return UNIQUE
    else:
        return DUPLICATE


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
        # If more than one tuple element is many, everything has DUPLICATE
        # multiplicity.
        if num_many > 1:
            el = DUPLICATE
        # If exactly one tuple element is many, then *that* element
        # can keep its underlying multiplicity, while everything else
        # becomes DUPLICATE.
        elif num_many == 1 and card.is_single():
            el = DUPLICATE
        new_els.append(el)

    return ContainerMultiplicityInfo(
        own=_max_multiplicity(els).own,
        elements=tuple(new_els),
    )


@_infer_multiplicity.register
def __infer_trigger_anchor(
    ir: irast.TriggerAnchor,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return UNIQUE


@_infer_multiplicity.register
def __infer_searchable_string(
    ir: irast.FTSDocument,
    *,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    return _common_multiplicity(
        (ir.text, ir.language), scope_tree=scope_tree, ctx=ctx
    )


def infer_multiplicity(
    ir: irast.Base,
    *,
    is_mutation: bool=False,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> inf_ctx.MultiplicityInfo:
    assert ctx.make_updates, (
        "multiplicity inference hasn't implemented make_updates=False yet")

    result = ctx.inferred_multiplicity.get(
        (ir, scope_tree, ctx.distinct_iterator))
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

    if card is not None and card.is_single() and result.is_duplicate():
        # We've validated multiplicity, so now we can just override it
        # safely.
        result = UNIQUE

    if not isinstance(result, inf_ctx.MultiplicityInfo):
        raise errors.QueryError(
            'could not determine the multiplicity of '
            'set produced by expression',
            span=ir.span)

    ctx.inferred_multiplicity[ir, scope_tree, ctx.distinct_iterator] = result

    return result
