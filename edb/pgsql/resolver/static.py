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

"""Static evaluation for SQL."""

import functools
from typing import *

from edb import errors

from edb.pgsql import ast as pgast

from . import context

Context = context.ResolverContextLevel


@functools.singledispatch
def eval(expr: pgast.BaseExpr, *, ctx: Context) -> Optional[pgast.BaseExpr]:
    return None


def eval_list(
    exprs: List[pgast.BaseExpr], *, ctx: Context
) -> Optional[List[pgast.BaseExpr]]:
    res = []
    for expr in exprs:
        r = eval(expr, ctx=ctx)
        if not r:
            return None
        res.append(r)
    return res


def name_in_pg_catalog(name: Sequence[str]) -> Optional[str]:
    if len(name) == 1 or name[0] == 'pg_catalog':
        return name[-1]
    return None


@eval.register
def eval_BaseConstant(
    expr: pgast.BaseConstant, *, ctx: Context
) -> Optional[pgast.BaseExpr]:
    return expr


@eval.register
def eval_TypeCast(
    expr: pgast.TypeCast, *, ctx: Context
) -> Optional[pgast.BaseExpr]:
    arg = eval(expr.arg, ctx=ctx)
    if not arg:
        return None

    if isinstance(arg, pgast.StringConstant):

        type_name = name_in_pg_catalog(expr.type_name.name)

        if type_name == 'text':
            return arg

        if type_name == 'bool':
            string = arg.val.lower()

            if 'true'.startswith(string) or 'yes'.startswith(string):
                return pgast.BooleanConstant(val=True)

            if 'false'.startswith(string) or 'no'.startswith(string):
                return pgast.BooleanConstant(val=False)
            raise errors.QueryError('invalid cast', context=expr.arg)
        return arg

    return None


@eval.register
def eval_FuncCall(
    expr: pgast.FuncCall,
    *,
    ctx: Context,
) -> Optional[pgast.BaseExpr]:
    if len(expr.name) >= 3:
        raise errors.QueryError("unknown function", context=expr.context)

    fn_name = name_in_pg_catalog(expr.name)
    if not fn_name:
        return None

    if fn_name == 'current_schemas':
        return eval_current_schemas(expr, ctx=ctx)

    if fn_name == 'current_database':
        return pgast.StringConstant(val=ctx.options.current_database)

    if fn_name == 'current_query':
        return pgast.StringConstant(val=ctx.options.current_query)

    return None


def eval_current_schemas(
    expr: pgast.FuncCall, ctx: Context
) -> Optional[pgast.BaseExpr]:
    args = eval_list(expr.args, ctx=ctx)
    if not args:
        return None

    if isinstance(args[0], pgast.BooleanConstant):
        include_implicit = args[0].val
    else:
        return None

    res = []

    if include_implicit:
        # if any temporary object has been created in current session,
        # here we should also append res.append('pg_temp_xxx') were xxx is
        # a number assigned by the server.

        res.append('pg_catalog')
    res.extend(ctx.options.search_path)

    return pgast.ArrayExpr(elements=[pgast.StringConstant(val=r) for r in res])


@eval.register
def eval_SQLValueFunction(
    expr: pgast.SQLValueFunction,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    from edb.pgsql.ast import SQLValueFunctionOP as op

    pass_through = [
        op.CURRENT_DATE,
        op.CURRENT_TIME,
        op.CURRENT_TIME_N,
        op.CURRENT_TIMESTAMP,
        op.CURRENT_TIMESTAMP_N,
        op.LOCALTIME,
        op.LOCALTIME_N,
        op.LOCALTIMESTAMP,
        op.LOCALTIMESTAMP_N,
    ]
    if expr.op in pass_through:
        return expr

    user = [
        op.CURRENT_ROLE,
        op.CURRENT_USER,
        op.USER,
        op.SESSION_USER,
    ]
    if expr.op in user:
        return pgast.StringConstant(val=ctx.options.current_user)

    if expr.op == op.CURRENT_CATALOG:
        return pgast.StringConstant(val=ctx.options.current_database)

    if expr.op == op.CURRENT_SCHEMA:
        # note: PG also does a check that this schema exists and proceeds to
        # the next one in the search path
        return pgast.StringConstant(val=ctx.options.search_path[0])

    # this should never happen
    raise NotImplementedError()
