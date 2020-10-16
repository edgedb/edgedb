#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import functools

from edb import errors

from edb.edgeql import qltypes

from edb.ir import ast as irast

from .. import context


IMMUTABLE = qltypes.Volatility.Immutable
STABLE = qltypes.Volatility.Stable
VOLATILE = qltypes.Volatility.Volatile


def _max_volatility(args: Iterable[qltypes.Volatility]) -> qltypes.Volatility:
    # We rely on a happy coincidence that the lexical
    # order of volatility constants coincides with the volatility
    # level.
    arg_list = list(args)
    if not arg_list:
        return IMMUTABLE
    else:
        return max(arg_list)


def _common_volatility(
    args: Iterable[irast.Base],
    env: context.Environment,
) -> qltypes.Volatility:
    return _max_volatility(
        infer_volatility(a, env) for a in args)


@functools.singledispatch
def _infer_volatility(
    ir: irast.Base,
    env: context.Environment,
) -> qltypes.Volatility:
    raise ValueError(f'infer_volatility: cannot handle {ir!r}')


@_infer_volatility.register(type(None))
def __infer_none(
    ir: None,
    env: context.Environment,
) -> qltypes.Volatility:
    # Here for debugging purposes.
    raise ValueError('invalid infer_volatility(None, schema) call')


@_infer_volatility.register
def __infer_statement(
    ir: irast.Statement,
    env: context.Environment,
) -> qltypes.Volatility:
    return infer_volatility(ir.expr, env)


@_infer_volatility.register
def __infer_config_insert(
    ir: irast.ConfigInsert,
    env: context.Environment,
) -> qltypes.Volatility:
    return infer_volatility(ir.expr, env)


@_infer_volatility.register
def __infer_emptyset(
    ir: irast.EmptySet,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_typeref(
    ir: irast.TypeRef,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_set(
    ir: irast.Set,
    env: context.Environment,
) -> qltypes.Volatility:
    if ir.is_binding:
        return STABLE
    elif ir.rptr is not None:
        return infer_volatility(ir.rptr.source, env)
    elif ir.expr is not None:
        return infer_volatility(ir.expr, env)
    else:
        return STABLE


@_infer_volatility.register
def __infer_func_call(
    ir: irast.FunctionCall,
    env: context.Environment,
) -> qltypes.Volatility:
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    env: context.Environment,
) -> qltypes.Volatility:
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility.register
def __infer_const(
    ir: irast.BaseConstant,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_param(
    ir: irast.Parameter,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_const_set(
    ir: irast.ConstantSet,
    env: context.Environment,
) -> qltypes.Volatility:
    return IMMUTABLE


@_infer_volatility.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    env: context.Environment,
) -> qltypes.Volatility:
    return infer_volatility(ir.left, env)


@_infer_volatility.register
def __infer_typecast(
    ir: irast.TypeCast,
    env: context.Environment,
) -> qltypes.Volatility:
    return infer_volatility(ir.expr, env)


@_infer_volatility.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    env: context.Environment,
) -> qltypes.Volatility:
    components = []

    if ir.iterator_stmt is not None:
        components.append(ir.iterator_stmt)

    components.append(ir.result)

    if ir.where is not None:
        components.append(ir.where)

    if ir.orderby:
        components.extend(o.expr for o in ir.orderby)

    if ir.offset is not None:
        components.append(ir.offset)

    if ir.limit is not None:
        components.append(ir.limit)

    components.extend(ir.bindings)

    return _common_volatility(components, env)


@_infer_volatility.register
def __infer_group_stmt(
    ir: irast.GroupStmt,
    env: context.Environment,
) -> qltypes.Volatility:
    raise NotImplementedError


@_infer_volatility.register
def __infer_insert_stmt(
    ir: irast.InsertStmt,
    env: context.Environment,
) -> qltypes.Volatility:
    return VOLATILE


@_infer_volatility.register
def __infer_update_stmt(
    ir: irast.UpdateStmt,
    env: context.Environment,
) -> qltypes.Volatility:
    return VOLATILE


@_infer_volatility.register
def __infer_delete_stmt(
    ir: irast.DeleteStmt,
    env: context.Environment,
) -> qltypes.Volatility:
    return VOLATILE


@_infer_volatility.register
def __infer_slice(
    ir: irast.SliceIndirection,
    env: context.Environment,
) -> qltypes.Volatility:
    # slice indirection volatility depends on the volatility of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_volatility(args, env)


@_infer_volatility.register
def __infer_index(
    ir: irast.IndexIndirection,
    env: context.Environment,
) -> qltypes.Volatility:
    # index indirection volatility depends on both the volatility of
    # the base expression and the index expression
    return _common_volatility([ir.expr, ir.index], env)


@_infer_volatility.register
def __infer_array(
    ir: irast.Array,
    env: context.Environment,
) -> qltypes.Volatility:
    return _common_volatility(ir.elements, env)


@_infer_volatility.register
def __infer_tuple(
    ir: irast.Tuple,
    env: context.Environment,
) -> qltypes.Volatility:
    return _common_volatility(
        [el.val for el in ir.elements], env)


def infer_volatility(
    ir: irast.Base,
    env: context.Environment,
) -> qltypes.Volatility:
    result = env.inferred_volatility.get(ir)
    if result is not None:
        return result

    result = _infer_volatility(ir, env)

    if result not in {VOLATILE, STABLE, IMMUTABLE}:
        raise errors.QueryError(
            'could not determine the volatility of '
            'set produced by expression',
            context=ir.context)

    env.inferred_volatility[ir] = result

    return result
