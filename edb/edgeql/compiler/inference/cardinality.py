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

import functools
from typing import *  # NoQA

from edb import errors

from edb.edgeql import qltypes

from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers

from edb.ir import ast as irast


ONE = qltypes.Cardinality.ONE
MANY = qltypes.Cardinality.MANY


def _get_set_scope(
        ir_set: irast.Set,
        scope_tree: irast.ScopeTreeNode) -> irast.ScopeTreeNode:

    new_scope = None
    if ir_set.path_scope_id:
        new_scope = scope_tree.root.find_by_unique_id(ir_set.path_scope_id)
    if new_scope is None:
        new_scope = scope_tree

    return new_scope


def _max_cardinality(args):
    if all(a == ONE for a in args):
        return ONE
    else:
        return MANY


def _common_cardinality(args, scope_tree, env):
    return _max_cardinality(
        infer_cardinality(a, scope_tree, env) for a in args)


@functools.singledispatch
def _infer_cardinality(ir, scope_tree, env):
    raise ValueError(f'infer_cardinality: cannot handle {ir!r}')


@_infer_cardinality.register(type(None))
def __infer_none(ir, scope_tree, env):
    # Here for debugging purposes.
    raise ValueError('invalid infer_cardinality(None, schema) call')


@_infer_cardinality.register(irast.Statement)
def __infer_statement(ir, scope_tree, env):
    return infer_cardinality(ir.expr, scope_tree, env)


@_infer_cardinality.register(irast.ConfigInsert)
def __infer_config_insert(ir, scope_tree, env):
    return infer_cardinality(ir.expr, scope_tree, env)


@_infer_cardinality.register(irast.EmptySet)
def __infer_emptyset(ir, scope_tree, env):
    return ONE


@_infer_cardinality.register(irast.TypeRef)
def __infer_typeref(ir, scope_tree, env):
    return ONE


@_infer_cardinality.register(irast.TypeIntrospection)
def __infer_type_introspection(ir, scope_tree, env):
    return ONE


@_infer_cardinality.register(irast.Set)
def __infer_set(ir, scope_tree, env):
    parent_fence = scope_tree.parent_fence
    if parent_fence is not None:
        if scope_tree.namespaces:
            path_id = ir.path_id.strip_namespace(scope_tree.namespaces)
        else:
            path_id = ir.path_id

        if parent_fence.is_visible(path_id):
            return ONE

    if ir.rptr is not None:
        if ir.rptr.ptrref.dir_cardinality is qltypes.Cardinality.ONE:
            new_scope = _get_set_scope(ir, scope_tree)
            return infer_cardinality(ir.rptr.source, new_scope, env)
        else:
            return MANY
    elif ir.expr is not None:
        new_scope = _get_set_scope(ir, scope_tree)
        return infer_cardinality(ir.expr, new_scope, env)
    else:
        return MANY


@_infer_cardinality.register(irast.FunctionCall)
def __infer_func_call(ir, scope_tree, env):
    # the cardinality of the function call depends on the cardinality
    # of non-SET_OF arguments AND the cardinality of the function
    # return value
    SET_OF = qltypes.TypeModifier.SET_OF
    if ir.typemod is qltypes.TypeModifier.SET_OF:
        return MANY
    else:
        args = []
        # process positional args
        for arg, typemod in zip(ir.args, ir.params_typemods):
            if typemod is not SET_OF:
                args.append(arg.expr)

        if args:
            return _common_cardinality(args, scope_tree, env)
        else:
            return ONE


@_infer_cardinality.register(irast.OperatorCall)
def __infer_oper_call(ir, scope_tree, env):
    if ir.func_shortname == 'std::UNION':
        return MANY
    else:
        args = []

        if ir.typemod is qltypes.TypeModifier.SET_OF:
            args = [a.expr for a in ir.args]
        else:
            for arg, typemod in zip(ir.args, ir.params_typemods):
                if typemod is not qltypes.TypeModifier.SET_OF:
                    args.append(arg.expr)

        if args:
            return _common_cardinality(args, scope_tree, env)
        else:
            return ONE


@_infer_cardinality.register(irast.BaseConstant)
@_infer_cardinality.register(irast.Parameter)
def __infer_const_or_param(ir, scope_tree, env):
    return ONE


@_infer_cardinality.register(irast.ConstantSet)
def __infer_const_set(ir, scope_tree, env):
    return ONE if len(ir.elements) == 1 else MANY


@_infer_cardinality.register(irast.TypeCheckOp)
def __infer_typecheckop(ir, scope_tree, env):
    return infer_cardinality(ir.left, scope_tree, env)


@_infer_cardinality.register(irast.TypeCast)
def __infer_typecast(ir, scope_tree, env):
    return infer_cardinality(ir.expr, scope_tree, env)


def _is_ptr_or_self_ref(
        ir_expr: irast.Base,
        result_expr: irast.Set,
        env) -> bool:
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
        env) -> Sequence[Tuple[s_pointers.Pointer, irast.Set]]:

    schema = env.schema
    scope_tree = _get_set_scope(filter_set, scope_tree)

    ptr_filters = []
    expr = filter_set.expr
    if isinstance(expr, irast.OperatorCall):
        if expr.func_shortname == 'std::=':
            left, right = (a.expr for a in expr.args)

            op_card = _common_cardinality(
                [left, right], scope_tree, env)
            result_stype = env.set_types[result_set]

            if op_card == MANY:
                pass

            elif _is_ptr_or_self_ref(left, result_set, env):
                if infer_cardinality(right, scope_tree, env) == ONE:
                    left_stype = env.set_types[left]
                    if left_stype == result_stype:
                        ptr = left_stype.getptr(schema, 'id')
                    else:
                        ptr = env.schema.get(left.rptr.ptrref.name)

                    ptr_filters.append((ptr, right))

            elif _is_ptr_or_self_ref(right, result_set, env):
                if infer_cardinality(left, scope_tree, env) == ONE:
                    right_stype = env.set_types[right]
                    if right_stype == result_stype:
                        ptr = right_stype.getptr(schema, 'id')
                    else:
                        ptr = env.schema.get(right.rptr.ptrref.name)

                    ptr_filters.append((ptr, left))

        elif expr.func_shortname == 'std::AND':
            left, right = (a.expr for a in expr.args)

            ptr_filters.extend(
                extract_filters(result_set, left, scope_tree, env))
            ptr_filters.extend(
                extract_filters(result_set, right, scope_tree, env))

    return ptr_filters


def _analyse_filter_clause(
        result_set: irast.Set, filter_clause: irast.Set,
        scope_tree: irast.ScopeTreeNode,
        env) -> qltypes.Cardinality:

    schema = env.schema
    filtered_ptrs = extract_filters(result_set, filter_clause, scope_tree, env)

    if filtered_ptrs:
        exclusive_constr = schema.get('std::exclusive')

        for ptr, _ in filtered_ptrs:
            ptr = cast(
                s_pointers.Pointer,
                ptr.get_nearest_non_derived_parent(env.schema),
            )
            is_unique = (
                ptr.is_id_pointer(schema) or
                any(c.issubclass(schema, exclusive_constr)
                    for c in ptr.get_constraints(schema).objects(schema))
            )
            if is_unique:
                # Bingo, got an equality filter on a link with a
                # unique constraint
                return ONE

    return MANY


def _infer_stmt_cardinality(
        result_set: irast.Set, filter_clause: Optional[irast.Set],
        scope_tree: irast.ScopeTreeNode,
        env) -> qltypes.Cardinality:
    result_card = infer_cardinality(result_set, scope_tree, env)
    if result_card == ONE or filter_clause is None:
        return result_card

    return _analyse_filter_clause(
        result_set, filter_clause, scope_tree, env)


@_infer_cardinality.register(irast.SelectStmt)
def __infer_select_stmt(ir, scope_tree, env):
    if ir.cardinality:
        return ir.cardinality
    else:
        if (ir.limit is not None and
                isinstance(ir.limit.expr, irast.IntegerConstant) and
                ir.limit.expr.value == '1'):
            # Explicit LIMIT 1 clause.
            stmt_card = ONE
        else:
            stmt_card = _infer_stmt_cardinality(
                ir.result, ir.where, scope_tree, env)

        if ir.iterator_stmt:
            iter_card = infer_cardinality(ir.iterator_stmt, scope_tree, env)
            stmt_card = _max_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register(irast.InsertStmt)
def __infer_insert_stmt(ir, scope_tree, env):
    if ir.cardinality:
        return ir.cardinality
    else:
        if ir.iterator_stmt:
            return infer_cardinality(ir.iterator_stmt, scope_tree, env)
        else:
            # INSERT without a FOR is always a singleton.
            return ONE


@_infer_cardinality.register(irast.UpdateStmt)
def __infer_update_stmt(ir, scope_tree, env):
    if ir.cardinality:
        return ir.cardinality
    else:
        stmt_card = _infer_stmt_cardinality(
            ir.subject, ir.where, scope_tree, env)

        if ir.iterator_stmt:
            iter_card = infer_cardinality(ir.iterator_stmt, scope_tree, env)
            stmt_card = _max_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register(irast.DeleteStmt)
def __infer_delete_stmt(ir, scope_tree, env):
    if ir.cardinality:
        return ir.cardinality
    else:
        stmt_card = _infer_stmt_cardinality(
            ir.subject, None, scope_tree, env)

        if ir.iterator_stmt:
            iter_card = infer_cardinality(ir.iterator_stmt, scope_tree, env)
            stmt_card = _max_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register(irast.Stmt)
def __infer_stmt(ir, scope_tree, env):
    if ir.cardinality:
        return ir.cardinality
    else:
        return infer_cardinality(ir.result, scope_tree, env)


@_infer_cardinality.register(irast.SliceIndirection)
def __infer_slice(ir, scope_tree, env):
    # slice indirection cardinality depends on the cardinality of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_cardinality(args, scope_tree, env)


@_infer_cardinality.register(irast.IndexIndirection)
def __infer_index(ir, scope_tree, env):
    # index indirection cardinality depends on both the cardinality of
    # the base expression and the index expression
    return _common_cardinality([ir.expr, ir.index], scope_tree, env)


@_infer_cardinality.register(irast.Array)
def __infer_array(ir, scope_tree, env):
    return _common_cardinality(ir.elements, scope_tree, env)


@_infer_cardinality.register(irast.Tuple)
def __infer_tuple(ir, scope_tree, env):
    return _common_cardinality(
        [el.val for el in ir.elements], scope_tree, env)


@_infer_cardinality.register(irast.TupleIndirection)
def __infer_tuple_indirection(ir, scope_tree, env):
    # the cardinality of the tuple indirection is the same as the
    # cardinality of the underlying tuple
    return infer_cardinality(ir.expr, scope_tree, env)


def infer_cardinality(ir, scope_tree, env):
    result = env.inferred_cardinality.get((ir, scope_tree))
    if result is not None:
        return result

    result = _infer_cardinality(ir, scope_tree, env)

    if result not in {ONE, MANY}:
        raise errors.QueryError(
            'could not determine the cardinality of '
            'set produced by expression',
            context=ir.context)

    env.inferred_cardinality[ir, scope_tree] = result

    return result
