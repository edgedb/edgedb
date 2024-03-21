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
import platform
from typing import Optional, Sequence, List

from edb import errors

from edb.pgsql import ast as pgast
from edb.pgsql import parser as pgparser
from edb.server import defines

from . import context
from . import dispatch

Context = context.ResolverContextLevel


@functools.singledispatch
def eval(expr: pgast.BaseExpr, *, ctx: Context) -> Optional[pgast.BaseExpr]:
    """
    Tries to statically evaluate expr, recursing into sub-expressions.
    Returns None if that is not possible.
    """
    return None


def eval_list(
    exprs: List[pgast.BaseExpr], *, ctx: Context
) -> Optional[List[pgast.BaseExpr]]:
    """
    Tries to statically evaluate exprs, recursing into sub-expressions.
    Returns None if that is not possible.
    """
    res = []
    for expr in exprs:
        r = eval(expr, ctx=ctx)
        if not r:
            return None
        res.append(r)
    return res


def name_in_pg_catalog(name: Sequence[str]) -> Optional[str]:
    """
    Strips `pg_catalog.` schema name from an SQL ident. Because pg_catalog is
    always the first schema in search_path, every ident without schema name
    defaults to is treaded
    """

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
    if expr.type_name.array_bounds:
        return None

    pg_catalog_name = name_in_pg_catalog(expr.type_name.name)
    if pg_catalog_name == 'regclass':
        return cast_to_regclass(expr.arg, ctx)

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
            raise errors.QueryError('invalid cast', span=expr.arg.span)

    return None


privilege_inquiry_functions_args = {
    'has_any_column_privilege': 2,
    'has_column_privilege': 3,
    'has_database_privilege': 2,
    'has_foreign_data_wrapper_privilege': 2,
    'has_function_privilege': 2,
    'has_language_privilege': 2,
    'has_parameter_privilege': 2,
    'has_schema_privilege': 2,
    'has_sequence_privilege': 2,
    'has_server_privilege': 2,
    'has_table_privilege': 2,
    'has_tablespace_privilege': 2,
    'has_type_privilege': 2,
    'pg_has_role': 2,
}


@eval.register
def eval_FuncCall(
    expr: pgast.FuncCall,
    *,
    ctx: Context,
) -> Optional[pgast.BaseExpr]:
    if len(expr.name) >= 3:
        raise errors.QueryError("unknown function", span=expr.span)

    fn_name = name_in_pg_catalog(expr.name)
    if not fn_name:
        return None

    if fn_name == 'current_schemas':
        return eval_current_schemas(expr, ctx=ctx)

    if fn_name == 'current_database':
        return pgast.StringConstant(val=ctx.options.current_database)

    if fn_name == 'current_query':
        return pgast.StringConstant(val=ctx.options.current_query)

    if fn_name == 'version':
        from edb import buildmeta

        edgedb_version = buildmeta.get_version_line()

        return pgast.StringConstant(
            val=" ".join(
                [
                    "PostgreSQL",
                    defines.PGEXT_POSTGRES_VERSION,
                    f"(EdgeDB {edgedb_version}),",
                    platform.architecture()[0],
                ]
            ),
        )

    if fn_name == "set_config":
        # HACK: allow set_config('search_path', '', false) to support pg_dump
        # HACK: allow set_config('bytea_output','hex',false) to support pgadmin
        # HACK: allow set_config('jit', ...) to support asyncpg
        if args := eval_list(expr.args, ctx=ctx):
            name, value, is_local = args
            if isinstance(name, pgast.StringConstant):
                if (
                    isinstance(value, pgast.StringConstant)
                    and isinstance(is_local, pgast.BooleanConstant)
                ):
                    if (
                        name.val == "search_path"
                        and value.val == ""
                        and not is_local.val
                    ):
                        return value

                    if (
                        name.val == "bytea_output"
                        and value.val == "hex"
                        and not is_local.val
                    ):
                        return value

                if name.val == "jit":
                    return value

        raise errors.QueryError(
            "function set_config is not supported", span=expr.span
        )

    if fn_name == 'current_setting':
        arg = require_a_string_literal(expr, fn_name, ctx)

        val = None
        if arg == 'search_path':
            val = ', '.join(ctx.options.search_path)
        if val:
            return pgast.StringConstant(val=val)
        return expr

    if fn_name == "pg_filenode_relation":
        raise errors.QueryError(
            f"function pg_catalog.{fn_name} is not supported",
            span=expr.span,
        )

    if fn_name == "to_regclass":
        arg = require_a_string_literal(expr, fn_name, ctx)
        return to_regclass(arg, ctx=ctx)

    cast_arg_to_regclass = {
        'pg_relation_filenode',
        'pg_relation_filepath',
        'pg_relation_size',
    }

    if fn_name in cast_arg_to_regclass:
        regclass_oid = cast_to_regclass(expr.args[0], ctx=ctx)
        return pgast.FuncCall(
            name=('pg_catalog', fn_name), args=[regclass_oid]
        )

    if fn_name in privilege_inquiry_functions_args:
        # For privilege inquiry functions, we strip the leading user (role),
        # so the inquiry refers to current user's privileges.
        # This is needed because the exposed username is not necessarily the
        # same as the user we use to connect to Postgres instance.
        # We do not allow creating additional users, so this should not be a
        # problem anyway.
        # See: https://www.postgresql.org/docs/15/functions-info.html

        # TODO: deny INSERT, UPDATE and all other unsupported functions
        allowed_args = privilege_inquiry_functions_args[fn_name]
        fn_args = expr.args[-allowed_args:]
        fn_args = dispatch.resolve_list(fn_args, ctx=ctx)

        # schema and table names need to be remapped. This is accomplished
        # with wrapper functions defined in metaschema.py.
        has_wrapper = {
            'has_schema_privilege',
            'has_table_privilege',
            'has_column_privilege',
        }
        if fn_name in has_wrapper:
            return pgast.FuncCall(name=('edgedbsql', fn_name), args=fn_args)

        return pgast.FuncCall(name=('pg_catalog', fn_name), args=fn_args)

    if fn_name == 'pg_table_is_visible':
        arg_0 = dispatch.resolve(expr.args[0], ctx=ctx)

        # our *_is_visible functions need search_path, passed in as an array
        arg_1 = pgast.ArrayExpr(
            elements=[
                pgast.StringConstant(val=v)
                for v in ctx.options.search_path
            ]
        )

        return pgast.FuncCall(
            name=('edgedbsql', fn_name),
            args=[arg_0, arg_1]
        )

    return None


def require_a_string_literal(
    expr: pgast.FuncCall, fn_name: str, ctx: Context
) -> str:
    args = eval_list(expr.args, ctx=ctx)
    if not (
        args and len(args) == 1 and isinstance(args[0], pgast.StringConstant)
    ):
        raise errors.QueryError(
            f"function pg_catalog.{fn_name} requires a string literal",
            span=expr.span,
        )

    return args[0].val


def cast_to_regclass(param: pgast.BaseExpr, ctx: Context) -> pgast.BaseExpr:
    """
    Equivalent to `::regclass` in SQL.

    Converts a string constant or a oid to a "registered class"
    (fully-qualified name of the table/index/sequence).
    In practice, type of resulting expression is oid.
    """

    expr = eval(param, ctx=ctx)
    if isinstance(expr, pgast.StringConstant):
        return to_regclass(expr.val, ctx=ctx)
    if isinstance(expr, pgast.NumericConstant):
        return expr
    raise errors.QueryError(
        "casting to `regclass` requires a string or number literal",
        span=param.span,
    )


def to_regclass(reg_class_name: str, ctx: Context) -> pgast.BaseExpr:
    """
    Equivalent to `to_regclass(text reg_class_name)` in SQL.

    Parses a string as an SQL identifier (with optional schema name and
    database name) and returns an SQL expression that evaluates to the
    "registered class" of that ident.
    """
    from edb.pgsql.common import quote_literal as ql

    try:
        [stmt] = pgparser.parse(f'SELECT {reg_class_name}')
        assert isinstance(stmt, pgast.SelectStmt)
        [target] = stmt.target_list
        assert isinstance(target.val, pgast.ColumnRef)
        name = target.val.name
    except Exception:
        return pgast.NullConstant()

    if len(name) < 2:
        name = (ctx.options.search_path[0], name[0])

    namespace, rel_name = name
    assert isinstance(namespace, str)
    assert isinstance(rel_name, str)

    # A bit hacky to parse SQL here, but I don't want to construct pgast
    [stmt] = pgparser.parse(
        f'''
        SELECT pc.oid
        FROM edgedbsql.pg_class pc
        JOIN edgedbsql.pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE {ql(namespace)} = pn.nspname AND pc.relname = {ql(rel_name)}
        '''
    )
    assert isinstance(stmt, pgast.SelectStmt)
    return pgast.SubLink(operator=None, expr=stmt)


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
