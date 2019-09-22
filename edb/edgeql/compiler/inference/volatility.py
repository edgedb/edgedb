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

from edb import errors

from edb.edgeql import qltypes

from edb.ir import ast as irast


IMMUTABLE = qltypes.Volatility.IMMUTABLE
STABLE = qltypes.Volatility.STABLE
VOLATILE = qltypes.Volatility.VOLATILE


def _max_volatility(args):
    # We rely on a happy coincidence that the lexical
    # order of volatility constants coincides with the volatility
    # level.
    return max(args)


def _common_volatility(args, env):
    return _max_volatility(
        infer_volatility(a, env) for a in args)


@functools.singledispatch
def _infer_volatility(ir, env):
    raise ValueError(f'infer_volatility: cannot handle {ir!r}')


@_infer_volatility.register(type(None))
def __infer_none(ir, env):
    # Here for debugging purposes.
    raise ValueError('invalid infer_volatility(None, schema) call')


@_infer_volatility.register(irast.Statement)
def __infer_statement(ir, env):
    return infer_volatility(ir.expr, env)


@_infer_volatility.register(irast.ConfigInsert)
def __infer_config_insert(ir, env):
    return infer_volatility(ir.expr, env)


@_infer_volatility.register(irast.EmptySet)
def __infer_emptyset(ir, env):
    return IMMUTABLE


@_infer_volatility.register(irast.TypeRef)
def __infer_typeref(ir, env):
    return IMMUTABLE


@_infer_volatility.register(irast.TypeIntrospection)
def __infer_type_introspection(ir, env):
    return IMMUTABLE


@_infer_volatility.register(irast.Set)
def __infer_set(ir, env):
    if ir.rptr is not None:
        return infer_volatility(ir.rptr.source, env)
    elif ir.expr is not None:
        return infer_volatility(ir.expr, env)
    else:
        return STABLE


@_infer_volatility.register(irast.FunctionCall)
def __infer_func_call(ir, env):
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility.register(irast.OperatorCall)
def __infer_oper_call(ir, env):
    if ir.args:
        return _max_volatility([
            _common_volatility((arg.expr for arg in ir.args), env),
            ir.volatility
        ])
    else:
        return ir.volatility


@_infer_volatility.register(irast.BaseConstant)
@_infer_volatility.register(irast.Parameter)
def __infer_const_or_param(ir, env):
    return IMMUTABLE


@_infer_volatility.register(irast.ConstantSet)
def __infer_const_set(ir, env):
    return IMMUTABLE


@_infer_volatility.register(irast.TypeCheckOp)
def __infer_typecheckop(ir, env):
    return infer_volatility(ir.left, env)


@_infer_volatility.register(irast.TypeCast)
def __infer_typecast(ir, env):
    return infer_volatility(ir.expr, env)


@_infer_volatility.register(irast.SelectStmt)
def __infer_select_stmt(ir, env):
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

    return _common_volatility(components, env)


@_infer_volatility.register(irast.InsertStmt)
def __infer_insert_stmt(ir, env):
    return VOLATILE


@_infer_volatility.register(irast.UpdateStmt)
def __infer_update_stmt(ir, env):
    return VOLATILE


@_infer_volatility.register(irast.DeleteStmt)
def __infer_delete_stmt(ir, env):
    return VOLATILE


@_infer_volatility.register(irast.SliceIndirection)
def __infer_slice(ir, env):
    # slice indirection volatility depends on the volatility of
    # the base expression and the slice index expressions
    args = [ir.expr]
    if ir.start is not None:
        args.append(ir.start)
    if ir.stop is not None:
        args.append(ir.stop)

    return _common_volatility(args, env)


@_infer_volatility.register(irast.IndexIndirection)
def __infer_index(ir, env):
    # index indirection volatility depends on both the volatility of
    # the base expression and the index expression
    return _common_volatility([ir.expr, ir.index], env)


@_infer_volatility.register(irast.Array)
def __infer_array(ir, env):
    return _common_volatility(ir.elements, env)


@_infer_volatility.register(irast.Tuple)
def __infer_tuple(ir, env):
    return _common_volatility(
        [el.val for el in ir.elements], env)


@_infer_volatility.register(irast.TupleIndirection)
def __infer_tuple_indirection(ir, env):
    # the volatility of the tuple indirection is the same as the
    # volatility of the underlying tuple
    return infer_volatility(ir.expr, env)


def infer_volatility(ir, env):
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
