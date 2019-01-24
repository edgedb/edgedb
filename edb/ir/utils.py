#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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


import json
import typing

from edb import errors

from edb.common import ast

from edb.edgeql import qltypes as ft

from . import ast as irast
from . import typeutils


def get_terminal_references(ir):
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_variables(ir):
    result = set()
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


def is_coalesce_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.INFIX and
        ir.func_shortname == 'std::??'
    )


def is_set_membership_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.INFIX and
        ir.func_shortname in {'std::IN', 'std::NOT IN'}
    )


def is_distinct_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.PREFIX and
        ir.func_shortname == 'std::DISTINCT'
    )


def is_union_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.INFIX and
        ir.func_shortname == 'std::UNION'
    )


def is_exists_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.PREFIX and
        ir.func_shortname == 'std::EXISTS'
    )


def is_ifelse_expr(ir):
    return (
        isinstance(ir, irast.OperatorCall) and
        ir.operator_kind is ft.OperatorKind.TERNARY and
        ir.func_shortname == 'std::IF'
    )


def is_empty_array_expr(ir):
    return (
        isinstance(ir, irast.Array)
        and not ir.elements
    )


def is_untyped_empty_array_expr(ir):
    return (
        is_empty_array_expr(ir)
        and (ir.typeref is None or typeutils.is_generic(ir.typeref))
    )


def is_empty(ir_expr):
    return (
        isinstance(ir_expr, irast.EmptySet) or
        (isinstance(ir_expr, irast.Array) and not ir_expr.elements) or
        (isinstance(ir_expr, irast.Set) and is_empty(ir_expr.expr))
    )


def is_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        (isinstance(ir_expr.expr, irast.SelectStmt) and
            isinstance(ir_expr.expr.result, irast.Set)) or
        ir_expr.view_source is not None
    )


def is_subquery_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        isinstance(ir_expr.expr, irast.Stmt)
    )


def is_scalar_view_set(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        len(ir_expr.path_id) == 1 and
        ir_expr.path_id.is_scalar_path() and
        ir_expr.path_id.is_view_path()
    )


def is_inner_view_reference(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.view_source is not None
    )


def is_simple_path(ir_expr):
    return (
        isinstance(ir_expr, irast.Set) and
        ir_expr.expr is None and
        (ir_expr.rptr is None or is_simple_path(ir_expr.rptr.source))
    )


def is_implicit_wrapper(ir_expr):
    return (
        isinstance(ir_expr, irast.SelectStmt) and
        ir_expr.implicit_wrapper
    )


def unwrap_set(ir_set):
    if is_implicit_wrapper(ir_set.expr):
        return ir_set.expr.result
    else:
        return ir_set


def wrap_stmt_set(ir_set):
    if is_subquery_set(ir_set):
        src_stmt = ir_set.expr
    elif is_inner_view_reference(ir_set):
        src_stmt = ir_set.view_source.expr
    else:
        raise ValueError('expecting subquery IR set or a view reference')

    stmt = irast.SelectStmt(
        result=ir_set,
        path_scope=src_stmt.path_scope,
        specific_path_scope=src_stmt.specific_path_scope
    )
    return stmt


def get_source_context_as_json(
        expr: irast.Base,
        exctype=errors.InternalServerError) -> typing.Optional[str]:
    if expr.context:
        details = json.dumps({
            'line': expr.context.start.line,
            'column': expr.context.start.column,
            'name': expr.context.name,
            'code': exctype.get_code(),
        })

    else:
        details = None

    return details


def is_type_indirection_reference(ir_expr):
    if not isinstance(ir_expr, irast.Set):
        return False

    rptr = ir_expr.rptr
    if rptr is None:
        return False

    ir_source = rptr.source

    if ir_source.path_id.is_type_indirection_path():
        source_is_type_indirection = True
    elif ir_source.expr is not None:
        src_expr_path_id = ir_source.expr.result.path_id.src_path()
        source_is_type_indirection = (
            src_expr_path_id and src_expr_path_id.is_type_indirection_path())
    else:
        source_is_type_indirection = False

    return source_is_type_indirection
