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


import functools
import typing

from edb.lang.common import ast

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors as ql_errors

from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import schema as s_schema
from edb.lang.schema import sources as s_sources

from edb.lang.ir import ast as irast


ONE = irast.Cardinality.ONE
MANY = irast.Cardinality.MANY


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


def _common_cardinality(args, scope_tree, schema):
    return _max_cardinality(
        infer_cardinality(a, scope_tree, schema) for a in args)


@functools.singledispatch
def _infer_cardinality(ir, scope_tree, schema):
    raise ValueError(f'infer_cardinality: cannot handle {ir!r}')


@_infer_cardinality.register(type(None))
def __infer_none(ir, scope_tree, schema):
    # Here for debugging purposes.
    raise ValueError('invalid infer_cardinality(None, schema) call')


@_infer_cardinality.register(irast.Statement)
def __infer_statement(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


@_infer_cardinality.register(irast.EmptySet)
def __infer_emptyset(ir, scope_tree, schema):
    return ONE


@_infer_cardinality.register(irast.TypeRef)
def __infer_typeref(ir, scope_tree, schema):
    return ONE


@_infer_cardinality.register(irast.Set)
def __infer_set(ir, scope_tree, schema):
    parent_fence = scope_tree.parent_fence
    if parent_fence is not None:
        if scope_tree.namespaces:
            path_id = ir.path_id.strip_namespace(scope_tree.namespaces)
        else:
            path_id = ir.path_id

        if parent_fence.is_visible(path_id):
            return ONE

    if ir.rptr is not None:
        if ir.rptr.ptrcls.singular(ir.rptr.direction):
            new_scope = _get_set_scope(ir, scope_tree)
            return infer_cardinality(ir.rptr.source, new_scope, schema)
        else:
            return MANY
    elif ir.expr is not None:
        new_scope = _get_set_scope(ir, scope_tree)
        return infer_cardinality(ir.expr, new_scope, schema)
    else:
        return MANY


@_infer_cardinality.register(irast.FunctionCall)
def __infer_func_call(ir, scope_tree, schema):
    if ir.func.return_typemod is qlast.TypeModifier.SET_OF:
        return MANY
    else:
        return ONE


@_infer_cardinality.register(irast.Constant)
@_infer_cardinality.register(irast.Parameter)
def __infer_const_or_param(ir, scope_tree, schema):
    return ONE


@_infer_cardinality.register(irast.Coalesce)
def __infer_coalesce(ir, scope_tree, schema):
    return _common_cardinality([ir.left, ir.right], scope_tree, schema)


@_infer_cardinality.register(irast.SetOp)
def __infer_setop(ir, scope_tree, schema):
    if ir.op == qlast.UNION:
        if not ir.exclusive:
            # Exclusive UNIONs are generated from IF ELSE expressions.
            result = MANY
        else:
            result = _common_cardinality(
                [ir.left, ir.right], scope_tree, schema)
    else:
        result = infer_cardinality(ir.left, scope_tree, schema)

    return result


@_infer_cardinality.register(irast.DistinctOp)
def __infer_distinctop(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


@_infer_cardinality.register(irast.BinOp)
def __infer_binop(ir, scope_tree, schema):
    return _common_cardinality([ir.left, ir.right], scope_tree, schema)


@_infer_cardinality.register(irast.EquivalenceOp)
def __infer_equivop(ir, scope_tree, schema):
    return _common_cardinality([ir.left, ir.right], scope_tree, schema)


@_infer_cardinality.register(irast.TypeCheckOp)
def __infer_typecheckop(ir, scope_tree, schema):
    return infer_cardinality(ir.left, scope_tree, schema)


@_infer_cardinality.register(irast.UnaryOp)
def __infer_unaryop(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


@_infer_cardinality.register(irast.IfElseExpr)
def __infer_ifelse(ir, scope_tree, schema):
    return _common_cardinality([ir.if_expr, ir.else_expr, ir.condition],
                               scope_tree, schema)


@_infer_cardinality.register(irast.TypeCast)
def __infer_typecast(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


def _is_ptr_or_self_ref(
        ir_expr: irast.Base,
        srccls: s_sources.Source,
        schema: s_schema.Schema) -> bool:
    if not isinstance(ir_expr, irast.Set):
        return False
    else:
        ir_set = ir_expr

        return (
            isinstance(srccls, s_objtypes.ObjectType) and
            ir_set.expr is None and
            (ir_set.scls == srccls or (
                ir_set.rptr is not None and
                srccls.getptr(schema, ir_set.rptr.ptrcls.shortname) is not None
            ))
        )


def _extract_filters(
        result_set: irast.Set, ir_set: irast.Set,
        scope_tree: typing.Set[irast.PathId],
        schema: s_schema.Schema) -> typing.Sequence[s_pointers.Pointer]:

    scope_tree = _get_set_scope(ir_set, scope_tree)

    ptr_filters = []
    expr = ir_set.expr
    if isinstance(expr, irast.BinOp):
        if expr.op == ast.ops.EQ:
            op_card = _common_cardinality(
                [expr.left, expr.right], scope_tree, schema)

            if op_card == MANY:
                pass
            elif _is_ptr_or_self_ref(expr.left, result_set.scls, schema):
                if infer_cardinality(expr.right, scope_tree, schema) == ONE:
                    if expr.left.scls == result_set.scls:
                        ptr_filters.append(expr.left.scls.pointers['std::id'])
                    else:
                        ptr_filters.append(expr.left.rptr.ptrcls)
            elif _is_ptr_or_self_ref(expr.right, result_set.scls, schema):
                if infer_cardinality(expr.left, scope_tree, schema) == ONE:
                    if expr.right.scls == result_set.scls:
                        ptr_filters.append(expr.right.scls.pointers['std::id'])
                    else:
                        ptr_filters.append(expr.right.rptr.ptrcls)

        elif expr.op == ast.ops.AND:
            ptr_filters.extend(
                _extract_filters(result_set, expr.left, scope_tree, schema))
            ptr_filters.extend(
                _extract_filters(result_set, expr.right, scope_tree, schema))

    return ptr_filters


def _analyse_filter_clause(
        result_set: irast.Set, filter_clause: irast.Set,
        scope_tree: typing.Set[irast.PathId],
        schema: s_schema.Schema) -> irast.Cardinality:

    filtered_ptrs = _extract_filters(result_set, filter_clause,
                                     scope_tree, schema)

    if filtered_ptrs:
        unique_constr = schema.get('std::unique')

        for ptr in filtered_ptrs:
            is_unique = (
                ptr.is_id_pointer() or
                any(c.issubclass(unique_constr)
                    for c in ptr.constraints.values())
            )
            if is_unique:
                # Bingo, got an equality filter on a link with a
                # unique constraint
                return ONE

    return MANY


def _infer_stmt_cardinality(
        result_set: irast.Set, filter_clause: typing.Optional[irast.Set],
        scope_tree: typing.Set[irast.PathId],
        schema: s_schema.Schema) -> irast.Cardinality:
    result_card = infer_cardinality(result_set, scope_tree, schema)
    if result_card == ONE or filter_clause is None:
        return result_card

    return _analyse_filter_clause(
        result_set, filter_clause, scope_tree, schema)


@_infer_cardinality.register(irast.SelectStmt)
def __infer_select_stmt(ir, scope_tree, schema):
    if ir.cardinality:
        return ir.cardinality
    else:
        if (ir.limit is not None and
                isinstance(ir.limit.expr, irast.Constant) and
                ir.limit.expr.value == 1):
            # Explicit LIMIT 1 clause.
            stmt_card = ONE
        else:
            stmt_card = _infer_stmt_cardinality(
                ir.result, ir.where, scope_tree, schema)

        if ir.iterator_stmt:
            iter_card = infer_cardinality(ir.iterator_stmt, scope_tree, schema)
            stmt_card = _max_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register(irast.InsertStmt)
def __infer_insert_stmt(ir, scope_tree, schema):
    if ir.cardinality:
        return ir.cardinality
    else:
        if ir.iterator_stmt:
            return infer_cardinality(ir.iterator_stmt, scope_tree, schema)
        else:
            # INSERT without a FOR is always a singleton.
            return ONE


@_infer_cardinality.register(irast.UpdateStmt)
@_infer_cardinality.register(irast.DeleteStmt)
def __infer_update_delete_stmt(ir, scope_tree, schema):
    if ir.cardinality:
        return ir.cardinality
    else:
        stmt_card = _infer_stmt_cardinality(
            ir.subject, ir.where, scope_tree, schema)

        if ir.iterator_stmt:
            iter_card = infer_cardinality(ir.iterator_stmt, scope_tree, schema)
            stmt_card = _max_cardinality((stmt_card, iter_card))

        return stmt_card


@_infer_cardinality.register(irast.Stmt)
def __infer_stmt(ir, scope_tree, schema):
    if ir.cardinality:
        return ir.cardinality
    else:
        return infer_cardinality(ir.result, scope_tree, schema)


@_infer_cardinality.register(irast.ExistPred)
def __infer_exist(ir, scope_tree, schema):
    return ONE


@_infer_cardinality.register(irast.SliceIndirection)
def __infer_slice(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


@_infer_cardinality.register(irast.IndexIndirection)
def __infer_index(ir, scope_tree, schema):
    return infer_cardinality(ir.expr, scope_tree, schema)


@_infer_cardinality.register(irast.Array)
@_infer_cardinality.register(irast.Tuple)
@_infer_cardinality.register(irast.TupleIndirection)
def __infer_map(ir, scope_tree, schema):
    return ONE


def infer_cardinality(ir, scope_tree, schema):
    try:
        return ir._inferred_cardinality_[scope_tree]
    except (AttributeError, KeyError):
        pass

    result = _infer_cardinality(ir, scope_tree, schema)

    if result not in {ONE, MANY}:
        raise ql_errors.EdgeQLError(
            'could not determine the cardinality of '
            'set produced by expression',
            context=ir.context)

    try:
        cache = ir._inferred_cardinality_
    except AttributeError:
        cache = ir._inferred_cardinality_ = {}

    cache[scope_tree] = result

    return result
