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
from edb.ir import typeutils as irtyputils

from .. import context


InferredVolatility = context.InferredVolatility


IMMUTABLE = qltypes.Volatility.Immutable
STABLE = qltypes.Volatility.Stable
VOLATILE = qltypes.Volatility.Volatile


# Volatility inference computes two volatility results:
# A basic one, and one for consumption by materialization.
#
# The one for consumption by materialization differs in that it
# (counterintuitively) does not consider DML to be volatile
# (since DML has its own "materialization" mechanism).
#
# We represent this output as a pair, but for ergonomics, inference
# functions are allowed to still produce a single volatility value,
# which is normalized when necessary.

def _normalize_volatility(vol: InferredVolatility) -> Tuple[
        qltypes.Volatility, qltypes.Volatility]:
    if not isinstance(vol, tuple):
        return (vol, vol)
    else:
        return vol


def _max_volatility(args: Iterable[InferredVolatility]) -> InferredVolatility:
    # We rely on a happy coincidence that the lexical
    # order of volatility constants coincides with the volatility
    # level.
    arg_list = list(args)
    if not arg_list:
        return IMMUTABLE
    else:
        nargs = [_normalize_volatility(x) for x in arg_list]
        return (
            max(x[0] for x in nargs),
            max(x[1] for x in nargs),
        )


def _common_volatility(
    args: Iterable[irast.Base],
    env: context.Environment,
) -> InferredVolatility:
    return _max_volatility(
        _infer_volatility(a, env) for a in args)


@functools.singledispatch
def _infer_volatility_inner(
    ir: irast.Base,
    env: context.Environment,
) -> InferredVolatility:
    raise ValueError(f'infer_volatility: cannot handle {ir!r}')


@_infer_volatility_inner.register(type(None))
def __infer_none(
    ir: None,
    env: context.Environment,
) -> InferredVolatility:
    # Here for debugging purposes.
    raise ValueError('invalid infer_volatility(None, schema) call')


@_infer_volatility_inner.register
def __infer_statement(
    ir: irast.Statement,
    env: context.Environment,
) -> InferredVolatility:
    return _infer_volatility(ir.expr, env)


@_infer_volatility_inner.register
def __infer_config_command(
    ir: irast.ConfigCommand,
    env: context.Environment,
) -> InferredVolatility:
    return VOLATILE


@_infer_volatility_inner.register
def __infer_emptyset(
    ir: irast.EmptySet,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_typeref(
    ir: irast.TypeRef,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_type_introspection(
    ir: irast.TypeIntrospection,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_set(
    ir: irast.Set,
    env: context.Environment,
) -> InferredVolatility:
    if ir.rptr is not None:
        src_vol = _infer_volatility(ir.rptr.source, env)
        # If source is an object, then a pointer reference implies
        # a table scan, and so we can assume STABLE at the minimum.
        if irtyputils.is_object(ir.rptr.source.typeref):
            vol = _max_volatility((src_vol, STABLE))
        else:
            vol = src_vol
    elif ir.expr is not None:
        vol = _infer_volatility(ir.expr, env)
    else:
        vol = STABLE

    # Cache our best-known as to this point volatility, to prevent
    # infinite recursion.
    env.inferred_volatility[ir] = vol

    if ir.shape:
        vol = _max_volatility([
            _common_volatility(
                (el.expr for el, _ in ir.shape if el.expr), env
            ),
            vol,
        ])

    if ir.is_binding:
        vol = STABLE

    return vol


@_infer_volatility_inner.register
def __infer_func_call(
    ir: irast.FunctionCall,
    env: context.Environment,
) -> InferredVolatility:
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility_inner.register
def __infer_oper_call(
    ir: irast.OperatorCall,
    env: context.Environment,
) -> InferredVolatility:
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility_inner.register
def __infer_const(
    ir: irast.BaseConstant,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_param(
    ir: irast.Parameter,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_const_set(
    ir: irast.ConstantSet,
    env: context.Environment,
) -> InferredVolatility:
    return IMMUTABLE


@_infer_volatility_inner.register
def __infer_typecheckop(
    ir: irast.TypeCheckOp,
    env: context.Environment,
) -> InferredVolatility:
    return _infer_volatility(ir.left, env)


@_infer_volatility_inner.register
def __infer_typecast(
    ir: irast.TypeCast,
    env: context.Environment,
) -> InferredVolatility:
    return _infer_volatility(ir.expr, env)


@_infer_volatility_inner.register
def __infer_select_stmt(
    ir: irast.SelectStmt,
    env: context.Environment,
) -> InferredVolatility:
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

    if ir.bindings is not None:
        components.extend(ir.bindings)

    return _common_volatility(components, env)


@_infer_volatility_inner.register
def __infer_group_stmt(
    ir: irast.GroupStmt,
    env: context.Environment,
) -> InferredVolatility:
    components = [ir.subject, ir.result] + [v for v, _ in ir.using.values()]
    return _common_volatility(components, env)


@_infer_volatility_inner.register
def __infer_dml_stmt(
    ir: irast.MutatingStmt,
    env: context.Environment,
) -> InferredVolatility:
    # For materialization purposes, DML is not volatile.  (Since it
    # has its *own* elaborate mechanism using top-level CTEs).
    return VOLATILE, STABLE


@_infer_volatility_inner.register
def __infer_slice(
    ir: irast.SliceIndirection,
    env: context.Environment,
) -> InferredVolatility:
    # slice indirection volatility depends on the volatility of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_volatility(args, env)


@_infer_volatility_inner.register
def __infer_index(
    ir: irast.IndexIndirection,
    env: context.Environment,
) -> InferredVolatility:
    # index indirection volatility depends on both the volatility of
    # the base expression and the index expression
    return _common_volatility([ir.expr, ir.index], env)


@_infer_volatility_inner.register
def __infer_array(
    ir: irast.Array,
    env: context.Environment,
) -> InferredVolatility:
    return _common_volatility(ir.elements, env)


@_infer_volatility_inner.register
def __infer_tuple(
    ir: irast.Tuple,
    env: context.Environment,
) -> InferredVolatility:
    return _common_volatility(
        [el.val for el in ir.elements], env)


def _infer_volatility(
    ir: irast.Base,
    env: context.Environment,
) -> InferredVolatility:
    result = env.inferred_volatility.get(ir)
    if result is not None:
        return result

    result = _infer_volatility_inner(ir, env)

    env.inferred_volatility[ir] = result

    return result


def infer_volatility(
    ir: irast.Base,
    env: context.Environment,
    *,
    for_materialization: bool=False,
) -> qltypes.Volatility:
    result = _normalize_volatility(_infer_volatility(ir, env))[
        for_materialization]

    if result not in {VOLATILE, STABLE, IMMUTABLE}:
        raise errors.QueryError(
            'could not determine the volatility of '
            'set produced by expression',
            context=ir.context)

    return result
