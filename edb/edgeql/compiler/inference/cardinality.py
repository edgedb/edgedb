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


from __future__ import annotations
from typing import *

import enum
import functools

from edb import errors

from edb.edgeql import qltypes

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers

from edb.ir import ast as irast
from edb.ir import utils as irutils

from .. import context

if TYPE_CHECKING:
    from edb.schema import constraints as s_constr


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
            return qltypes.SchemaCardinality.MANY
        else:
            return qltypes.SchemaCardinality.ONE

    @classmethod
    def from_required(cls, required: bool) -> CardinalityBound:
        return CB_ONE if required else CB_ZERO

    @classmethod
    def from_schema_value(
        cls,
        card: qltypes.SchemaCardinality
    ) -> CardinalityBound:
        if card is qltypes.SchemaCardinality.MANY:
            return CB_MANY
        else:
            return CB_ONE


CB_ZERO = CardinalityBound.ZERO
CB_ONE = CardinalityBound.ONE
CB_MANY = CardinalityBound.MANY


def _card_to_bounds(
    card: qltypes.Cardinality
) -> Tuple[CardinalityBound, CardinalityBound]:
    lower, upper = card.to_schema_value()
    return (
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


def _get_set_scope(
        ir_set: irast.Set,
        scope_tree: irast.ScopeTreeNode) -> irast.ScopeTreeNode:

    if ir_set.path_scope_id:
        new_scope = scope_tree.root.find_by_unique_id(ir_set.path_scope_id)
        if new_scope is None:
            raise errors.InternalServerError(
                f'dangling scope pointer to node with uid'
                f':{ir_set.path_scope_id} in {ir_set!r}'
            )
    else:
        new_scope = scope_tree

    return new_scope


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


def _coalesce_cardinality(
    args: Iterable[qltypes.Cardinality],
) -> qltypes.Cardinality:
    '''Cardinality of ?? of multiple args.'''

    card = list(zip(*(_card_to_bounds(a) for a in args)))
    if card:
        lower, upper = card
        return _bounds_to_card(max(lower), max(upper))
    else:
        # no args is indicative of a empty set
        return AT_MOST_ONE


def _common_cardinality(
    args: Iterable[irast.Base],
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return cartesian_cardinality(
        infer_cardinality(
            a,
            scope_tree=scope_tree,
            singletons=singletons,
            env=env
        ) for a in args
    )


@functools.singledispatch
def _infer_cardinality(
    ir: irast.Expr,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    raise ValueError(f'infer_cardinality: cannot handle {ir!r}')


@_infer_cardinality.register
def __infer_none(
    ir: None,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    # Here for debugging purposes.
    raise ValueError('invalid infer_cardinality(None, schema) call')


@_infer_cardinality.register
def __infer_statement(
    ir: irast.Statement,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.expr, scope_tree=scope_tree, singletons=singletons, env=env)


@_infer_cardinality.register
def __infer_config_insert(
    ir: irast.ConfigInsert,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.expr, scope_tree=scope_tree, singletons=singletons, env=env)


@_infer_cardinality.register
def __infer_emptyset(
    ir: irast.EmptySet,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return AT_MOST_ONE


@_infer_cardinality.register
def __infer_typeref(
    ir: irast.TypeRef,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return AT_MOST_ONE


@_infer_cardinality.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return ONE


def _is_visible(
    ir: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    env: context.Environment,
) -> bool:
    parent_fence = scope_tree.parent_fence
    if parent_fence is not None:
        if scope_tree.namespaces:
            path_id = ir.path_id.strip_namespace(scope_tree.namespaces)
        else:
            path_id = ir.path_id

        return parent_fence.is_visible(path_id)
    else:
        return False


@_infer_cardinality.register
def __infer_set(
    ir: irast.Set,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if _is_visible(ir, scope_tree, env) or ir.path_id in singletons:
        return ONE

    rptr = ir.rptr
    if rptr is not None:

        rptrref = rptr.ptrref
        if isinstance(rptrref, irast.TypeIntersectionPointerRef):
            ind_prefix, ind_ptrs = irutils.collapse_type_intersection(ir)
            new_scope = _get_set_scope(ir, scope_tree)
            if ind_prefix.rptr is None:
                return infer_cardinality(
                    ind_prefix,
                    scope_tree=new_scope,
                    singletons=singletons,
                    env=env,
                )
            else:
                # Expression before type intersection is a path,
                # i.e Foo.<bar[IS Type].  In this case we must
                # take possible intersection specialization of the
                # link union into account.
                # We're basically restating the body of this function
                # in this block, but with extra conditions.
                if _is_visible(ind_prefix, new_scope, env):
                    return AT_MOST_ONE
                else:
                    rptr_spec: Set[irast.PointerRef] = set()
                    for ind_ptr in ind_ptrs:
                        rptr_spec.update(ind_ptr.ptrref.rptr_specialization)

                    rptr_spec_card = _union_cardinality(
                        s.dir_cardinality for s in rptr_spec)
                    base_card = infer_cardinality(
                        rptr.source,
                        scope_tree=new_scope,
                        singletons=singletons,
                        env=env,
                    )

                    # The resulting cardinality is the cartesian
                    # product of the base to which the type
                    # intersection is applied and the cardinality due
                    # to type intersection itself.
                    return cartesian_cardinality([base_card, rptr_spec_card])

        else:
            if rptrref.union_components:
                # We use cartesian cardinality instead of union cardinality
                # because the union of pointers in this context is disjoint
                # in a sense that for any specific source only a given union
                # component is used.
                rptrref_card = cartesian_cardinality(
                    c.dir_cardinality for c in rptrref.union_components
                )
            else:
                rptrref_card = rptrref.dir_cardinality

            if rptrref_card.is_single():
                new_scope = _get_set_scope(ir, scope_tree)
                source_card = infer_cardinality(
                    rptr.source,
                    scope_tree=new_scope,
                    singletons=singletons,
                    env=env,
                )
                return cartesian_cardinality((source_card, rptrref_card))
            else:
                return MANY
    elif ir.expr is not None:
        new_scope = _get_set_scope(ir, scope_tree)
        return infer_cardinality(
            ir.expr,
            scope_tree=new_scope,
            singletons=singletons,
            env=env,
        )
    else:
        return MANY


@_infer_cardinality.register
def __infer_func_call(
    ir: irast.FunctionCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    # the cardinality of the function call depends on the cardinality
    # of non-SET_OF arguments AND the cardinality of the function
    # return value
    SET_OF = qltypes.TypeModifier.SET_OF
    if ir.typemod is SET_OF:
        return MANY
    else:
        args = []
        # process positional args
        for arg, typemod in zip(ir.args, ir.params_typemods):
            if typemod is not SET_OF:
                args.append(arg.expr)

        if args:
            return _common_cardinality(
                args,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
        else:
            if ir.typemod is qltypes.TypeModifier.OPTIONAL:
                return AT_MOST_ONE
            else:
                return ONE


@_infer_cardinality.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.func_shortname == 'std::UNION':
        # UNION needs to "add up" cardinalities.
        return _union_cardinality(
            infer_cardinality(
                a.expr,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env
            ) for a in ir.args
        )
    elif ir.func_shortname == 'std::??':
        # Coalescing takes the maximum of both lower and upper bounds.
        return _coalesce_cardinality(
            infer_cardinality(
                a.expr,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env
            ) for a in ir.args
        )
    else:
        args: List[irast.Base] = []
        all_optional = False

        if ir.typemod is qltypes.TypeModifier.SET_OF:
            # this is DISTINCT and IF..ELSE
            args = [a.expr for a in ir.args]
        else:
            all_optional = True
            for arg, typemod in zip(ir.args, ir.params_typemods):
                if typemod is not qltypes.TypeModifier.SET_OF:
                    all_optional &= typemod is qltypes.TypeModifier.OPTIONAL
                    args.append(arg.expr)

        if args:
            card = _common_cardinality(
                args,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
            if all_optional:
                # An operator that has all optional arguments and
                # doesn't return a SET OF returns at least ONE result
                # (we currently don't have operators that return
                # OPTIONAL). So we upgrade the lower bound.
                _, upper = _card_to_bounds(card)
                card = _bounds_to_card(CB_ONE, upper)

            return card
        else:
            return AT_MOST_ONE


@_infer_cardinality.register
def __infer_const(
    ir: irast.BaseConstant,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return ONE


@_infer_cardinality.register
def __infer_param(
    ir: irast.Parameter,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return AT_MOST_ONE if ir.optional else ONE


@_infer_cardinality.register
def __infer_const_set(
    ir: irast.ConstantSet,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return ONE if len(ir.elements) == 1 else AT_LEAST_ONE


@_infer_cardinality.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.left,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


@_infer_cardinality.register
def __infer_typecast(
    ir: irast.TypeCast,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return infer_cardinality(
        ir.expr,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


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
            isinstance(srccls, s_objtypes.ObjectType) and
            ir_set.expr is None and
            (env.set_types[ir_set] == srccls or (
                ir_set.rptr is not None and
                srccls.getptr(
                    env.schema,
                    ir_set.rptr.ptrref.shortname.name) is not None
            ))
        )


def extract_filters(
    result_set: irast.Set,
    filter_set: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> Sequence[Tuple[s_pointers.Pointer, irast.Set]]:

    schema = env.schema
    scope_tree = _get_set_scope(filter_set, scope_tree)

    ptr: s_pointers.Pointer

    ptr_filters = []
    expr = filter_set.expr
    if isinstance(expr, irast.OperatorCall):
        if expr.func_shortname == 'std::=':
            left, right = (a.expr for a in expr.args)

            op_card = _common_cardinality(
                [left, right],
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
            result_stype = env.set_types[result_set]

            if op_card.is_multi():
                pass

            elif _is_ptr_or_self_ref(left, result_set, env):
                if infer_cardinality(
                    right,
                    scope_tree=scope_tree,
                    singletons=singletons,
                    env=env,
                ).is_single():
                    left_stype = env.set_types[left]
                    if left_stype == result_stype:
                        assert isinstance(left_stype, s_objtypes.ObjectType)
                        _ptr = left_stype.getptr(schema, 'id')
                    else:
                        _ptr = env.schema.get(left.rptr.ptrref.name)

                    assert isinstance(_ptr, s_pointers.Pointer)
                    ptr = _ptr

                    ptr_filters.append((ptr, right))

            elif _is_ptr_or_self_ref(right, result_set, env):
                if infer_cardinality(
                    left,
                    scope_tree=scope_tree,
                    singletons=singletons,
                    env=env,
                ).is_single():
                    right_stype = env.set_types[right]
                    if right_stype == result_stype:
                        assert isinstance(right_stype, s_objtypes.ObjectType)
                        _ptr = right_stype.getptr(schema, 'id')
                    else:
                        _ptr = env.schema.get(right.rptr.ptrref.name)

                    assert isinstance(_ptr, s_pointers.Pointer)
                    ptr = _ptr

                    ptr_filters.append((ptr, left))

        elif expr.func_shortname == 'std::AND':
            left, right = (a.expr for a in expr.args)

            ptr_filters.extend(
                extract_filters(
                    result_set, left, scope_tree, singletons, env
                )
            )
            ptr_filters.extend(
                extract_filters(
                    result_set, right, scope_tree, singletons, env
                )
            )

    return ptr_filters


def _analyse_filter_clause(
    result_set: irast.Set,
    result_card: qltypes.Cardinality,
    filter_clause: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:

    schema = env.schema
    filtered_ptrs = extract_filters(
        result_set, filter_clause, scope_tree, singletons, env)

    if filtered_ptrs:
        exclusive_constr: s_constr.Constraint = schema.get('std::exclusive')

        for ptr, _ in filtered_ptrs:
            ptr = ptr.get_nearest_non_derived_parent(env.schema)
            is_unique = (
                ptr.is_id_pointer(schema) or
                any(c.issubclass(schema, exclusive_constr)
                    for c in ptr.get_constraints(schema).objects(schema))
            )
            if is_unique:
                # Bingo, got an equality filter on a link with a
                # unique constraint
                return AT_MOST_ONE

    return result_card


def _infer_stmt_cardinality(
    result_set: irast.Set,
    filter_clause: Optional[irast.Set],
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    result_card = infer_cardinality(
        result_set,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )
    if result_card.is_single() or filter_clause is None:
        return result_card

    return _analyse_filter_clause(
        result_set, result_card, filter_clause, scope_tree, singletons, env)


@_infer_cardinality.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.cardinality:
        return ir.cardinality
    else:
        if (ir.limit is not None and
                isinstance(ir.limit.expr, irast.IntegerConstant) and
                ir.limit.expr.value == '1'):
            # Explicit LIMIT 1 clause.
            stmt_card = AT_MOST_ONE
        else:
            stmt_card = _infer_stmt_cardinality(
                ir.result,
                ir.where,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )

        if ir.iterator_stmt:
            iter_card = infer_cardinality(
                ir.iterator_stmt,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
            stmt_card = cartesian_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.cardinality:
        return ir.cardinality
    else:
        if ir.iterator_stmt:
            # XXX: is this branch ever invoked?
            return infer_cardinality(
                ir.iterator_stmt,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
        else:
            # INSERT without a FOR is always a singleton.
            return ONE


@_infer_cardinality.register
def __infer_update_stmt(
    ir: irast.UpdateStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.cardinality:
        return ir.cardinality
    else:
        stmt_card = _infer_stmt_cardinality(
            ir.subject,
            ir.where,
            scope_tree=scope_tree,
            singletons=singletons,
            env=env,
        )

        if ir.iterator_stmt:
            iter_card = infer_cardinality(
                ir.iterator_stmt,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
            stmt_card = cartesian_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register
def __infer_delete_stmt(
    ir: irast.DeleteStmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.cardinality:
        return ir.cardinality
    else:
        stmt_card = _infer_stmt_cardinality(
            ir.subject,
            None,
            singletons=singletons,
            scope_tree=scope_tree,
            env=env,
        )

        if ir.iterator_stmt:
            iter_card = infer_cardinality(
                ir.iterator_stmt,
                scope_tree=scope_tree,
                singletons=singletons,
                env=env,
            )
            stmt_card = cartesian_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register
def __infer_stmt(
    ir: irast.Stmt,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    if ir.cardinality:
        return ir.cardinality
    else:
        return infer_cardinality(
            ir.result,
            scope_tree=scope_tree,
            singletons=singletons,
            env=env,
        )


@_infer_cardinality.register
def __infer_slice(
    ir: irast.SliceIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    # slice indirection cardinality depends on the cardinality of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_cardinality(
        args,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


@_infer_cardinality.register
def __infer_index(
    ir: irast.IndexIndirection,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    # index indirection cardinality depends on both the cardinality of
    # the base expression and the index expression
    return _common_cardinality(
        [ir.expr, ir.index],
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


@_infer_cardinality.register
def __infer_array(
    ir: irast.Array,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return _common_cardinality(
        ir.elements,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


@_infer_cardinality.register
def __infer_tuple(
    ir: irast.Tuple,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId],
    env: context.Environment,
) -> qltypes.Cardinality:
    return _common_cardinality(
        [el.val for el in ir.elements],
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )


def infer_cardinality(
    ir: irast.Base,
    *,
    scope_tree: irast.ScopeTreeNode,
    singletons: Collection[irast.PathId] = (),
    env: context.Environment,
) -> qltypes.Cardinality:
    result = env.inferred_cardinality.get((ir, scope_tree))
    if result is not None:
        return result

    result = _infer_cardinality(
        ir,
        scope_tree=scope_tree,
        singletons=singletons,
        env=env,
    )

    if result not in {AT_MOST_ONE, ONE, MANY, AT_LEAST_ONE}:
        raise errors.QueryError(
            'could not determine the cardinality of '
            'set produced by expression',
            context=ir.context)

    env.inferred_cardinality[ir, scope_tree] = result

    return result


def is_subset_cardinality(
    card0: qltypes.Cardinality,
    card1: qltypes.Cardinality
) -> bool:
    '''Determine if card0 is a subset of card1.'''
    l0, u0 = _card_to_bounds(card0)
    l1, u1 = _card_to_bounds(card1)

    return l0 >= l1 and u0 <= u1
