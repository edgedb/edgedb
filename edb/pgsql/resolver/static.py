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
from edb.pgsql.ast import SQLValueFunctionOP as val_func_op
from edb.pgsql import common
from edb.pgsql import parser as pgparser

from edb.server import defines
from edb.server.pgcon import errors as pgerror
from edb.server.compiler.sql import DisableNormalization

from . import context
from . import dispatch

V = common.versioned_schema

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
    Raises DisableNormalization if param refs are encountered.
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


# Functions that are inquiring about privileges of users or schemas.
# Dict from function name into number of trailing arguments that are passed
# trough.
PRIVILEGE_INQUIRY_FUNCTIONS_ARGS = {
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

# Allowed functions from pg_catalog that start with `pg_`.
# By default, all such functions are forbidden by default.
# To see the list of forbidden functions, use `edb ls-forbidden-functions`.
ALLOWED_ADMIN_FUNCTIONS = frozenset(
    {
        'pg_is_in_recovery',
        'pg_is_wal_replay_paused',
        'pg_get_wal_replay_pause_state',
        'pg_column_size',
        'pg_column_compression',
        'pg_database_size',
        'pg_indexes_size',
        'pg_relation_size',
        'pg_size_bytes',
        'pg_size_pretty',
        'pg_table_size',
        'pg_tablespace_size',
        'pg_total_relation_size',
        'pg_relation_filenode',
        'pg_relation_filepath',
        'pg_filenode_relation',
        'pg_char_to_encoding',
        'pg_column_is_updatable',
        'pg_conf_load_time',
        'pg_current_xact_id',
        'pg_current_xact_id_if_assigned',
        'pg_describe_object',
        'pg_encoding_max_length',
        'pg_encoding_to_char',
        'pg_get_constraintdef',
        'pg_get_expr',
        'pg_get_function_arg_default',
        'pg_get_function_arguments',
        'pg_get_function_identity_arguments',
        'pg_get_function_result',
        'pg_get_functiondef',
        'pg_get_indexdef',
        'pg_get_keywords',
        'pg_get_multixact_members',
        'pg_get_object_address',
        'pg_get_partition_constraintdef',
        'pg_get_partkeydef',
        'pg_get_publication_tables',
        'pg_get_replica_identity_index',
        'pg_get_replication_slots',
        'pg_get_ruledef',
        'pg_get_serial_sequence',
        'pg_get_shmem_allocations',
        'pg_get_statisticsobjdef',
        'pg_get_triggerdef',
        'pg_get_userbyid',
        'pg_get_viewdef',
        'pg_options_to_table',
        'pg_has_role',
        'pg_function_is_visible',
        'pg_opclass_is_visible',
        'pg_operator_is_visible',
        'pg_opfamily_is_visible',
        'pg_statistics_obj_is_visible',
        'pg_table_is_visible',
        'pg_ts_config_is_visible',
        'pg_ts_dict_is_visible',
        'pg_ts_parser_is_visible',
        'pg_ts_template_is_visible',
        'pg_type_is_visible',
        'pg_index_column_has_property',
        'pg_index_has_property',
        'pg_is_in_backup',
        'pg_is_other_temp_schema',
        'pg_jit_available',
        'pg_relation_is_updatable',
        'pg_sequence_last_value',
        'pg_sequence_parameters',
        'pg_timezone_abbrevs',
        'pg_timezone_names',
        'pg_typeof',
        'pg_visible_in_snapshot',
        'pg_xact_commit_timestamp',
        'pg_xact_status',
        'pg_partition_ancestors',
        'pg_backend_pid',
        'pg_wal_lsn_diff',
        'pg_last_wal_replay_lsn',
        'pg_current_wal_flush_lsn',
        'pg_relation_is_publishable',
    }
)


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

    if fn_name.startswith('pg_') and fn_name not in ALLOWED_ADMIN_FUNCTIONS:
        raise errors.QueryError(
            f"forbidden function '{fn_name}'",
            span=expr.span,
            pgext_code=pgerror.ERROR_INSUFFICIENT_PRIVILEGE,
        )

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
                    str(defines.PGEXT_POSTGRES_VERSION),
                    f"(Gel {edgedb_version}),",
                    platform.architecture()[0],
                ]
            ),
        )

    if fn_name == "set_config":
        # HACK: pg_dump
        #       - set_config('search_path', '', false)
        #       - set_config(name, 'view, foreign-table', false)
        # HACK: pgadmin
        #       - set_config('bytea_output','hex',false)
        # HACK: asyncpg
        #       - set_config('jit', ...)
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

        elif args := eval_list(expr.args[1:], ctx=ctx):
            value, is_local = args
            if (
                isinstance(value, pgast.StringConstant)
                and isinstance(is_local, pgast.BooleanConstant)
            ):
                if (
                    value.val == "view, foreign-table"
                    and not is_local.val
                ):
                    return value

        raise errors.QueryError(
            "function set_config is not supported",
            span=expr.span,
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
        )

    if fn_name == 'current_setting':
        arg = require_string_param(expr, ctx)

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
            pgext_code=pgerror.ERROR_FEATURE_NOT_SUPPORTED,
        )

    if fn_name == "pg_get_serial_sequence":
        # we do not expose sequences, so any calls to this function returns NULL
        return pgast.NullConstant()

    if fn_name == "to_regclass":
        arg = require_string_param(expr, ctx)
        return pgast.TypeCast(
            arg=to_regclass(arg, ctx=ctx),
            type_name=pgast.TypeName(name=('pg_catalog', 'regclass')),
        )

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

    if num_allowed_args := PRIVILEGE_INQUIRY_FUNCTIONS_ARGS.get(fn_name, None):
        # For privilege inquiry functions, we strip the leading user (role),
        # so the inquiry refers to current user's privileges.
        # This is needed because the exposed username is not necessarily the
        # same as the user we use to connect to Postgres instance.
        # We do not allow creating additional users, so this should not be a
        # problem anyway.
        # See: https://www.postgresql.org/docs/15/functions-info.html

        # TODO: deny INSERT, UPDATE and all other unsupported functions
        fn_args = expr.args[-num_allowed_args:]
        fn_args = dispatch.resolve_list(fn_args, ctx=ctx)

        # schema and table names need to be remapped. This is accomplished
        # with wrapper functions defined in metaschema.py.
        has_wrapper = {
            'has_schema_privilege',
            'has_table_privilege',
            'has_column_privilege',
        }
        if fn_name in has_wrapper:
            return pgast.FuncCall(name=(V('edgedbsql'), fn_name), args=fn_args)

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
            name=(V('edgedbsql'), fn_name),
            args=[arg_0, arg_1]
        )

    return None


def require_string_param(
    expr: pgast.FuncCall, ctx: Context
) -> str:
    args = eval_list(expr.args, ctx=ctx)

    arg = args[0] if args and len(args) == 1 else None
    if not isinstance(arg, pgast.StringConstant):
        raise errors.QueryError(
            f"function pg_catalog.{expr.name[-1]} requires a string literal",
            span=expr.span,
            pgext_code=pgerror.ERROR_UNDEFINED_FUNCTION
        )

    return arg.val


def require_bool_param(
    expr: pgast.FuncCall, ctx: Context
) -> bool:
    args = eval_list(expr.args, ctx=ctx)

    arg = args[0] if args and len(args) == 1 else None
    if not isinstance(arg, pgast.BooleanConstant):
        raise errors.QueryError(
            f"function pg_catalog.{expr.name[-1]} requires a boolean literal",
            span=expr.span,
            pgext_code=pgerror.ERROR_UNDEFINED_FUNCTION
        )

    return arg.val


def cast_to_regclass(param: pgast.BaseExpr, ctx: Context) -> pgast.BaseExpr:
    """
    Equivalent to `::regclass` in SQL.

    Converts a string constant or a oid to a "registered class"
    (fully-qualified name of the table/index/sequence).
    In practice, type of resulting expression is oid.
    """

    expr = eval(param, ctx=ctx)
    res: pgast.BaseExpr
    if isinstance(expr, pgast.NullConstant):
        res = pgast.NullConstant()
    elif isinstance(expr, pgast.StringConstant) and expr.val.isnumeric():
        # We need to treat numeric string constants as numbers, apparently.
        res = pgast.NumericConstant(val=expr.val)

    elif isinstance(expr, pgast.StringConstant):
        res = to_regclass(expr.val, ctx=ctx)
    elif isinstance(expr, pgast.NumericConstant):
        res = expr
    else:
        # This is a complex expression of unknown type.
        # If we knew the type is numeric, we could lookup the internal oid by
        # the public oid.
        # But if the type if string, we'd have to implement to_regclass in SQL.

        # The problem is that we don't know the type statically.
        # So let's insert a runtime type check with an 'unsupported' message for
        # strings.
        param = dispatch.resolve(param, ctx=ctx)
        res = pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=pgast.Expr(
                        lexpr=pgast.FuncCall(
                            name=('pg_typeof',),
                            args=[param]
                        ),
                        name='IN',
                        rexpr=pgast.ImplicitRowExpr(
                            args=[
                                pgast.StringConstant(val='integer'),
                                pgast.StringConstant(val='smallint'),
                                pgast.StringConstant(val='bigint'),
                                pgast.StringConstant(val='oid'),
                            ]
                        )
                    ),
                    result=param
                )
            ],
            defresult=pgast.FuncCall(
                name=(V('edgedb'), 'raise'),
                args=[
                    pgast.NumericConstant(val='1'),
                    pgast.StringConstant(
                        val=pgerror.ERROR_FEATURE_NOT_SUPPORTED
                    ),
                    pgast.StringConstant(val='cannot cast text to regclass'),
                ]
            )
        )
    return pgast.TypeCast(
        arg=res,
        type_name=pgast.TypeName(name=('pg_catalog', 'regclass')),
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
        FROM {V('edgedbsql')}.pg_class pc
        JOIN {V('edgedbsql')}.pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE {ql(namespace)} = pn.nspname AND pc.relname = {ql(rel_name)}
        '''
    )
    assert isinstance(stmt, pgast.SelectStmt)
    return pgast.SubLink(operator=None, expr=stmt)


def eval_current_schemas(
    expr: pgast.FuncCall, ctx: Context
) -> Optional[pgast.BaseExpr]:
    include_implicit = require_bool_param(expr, ctx)

    res = []
    if include_implicit:
        # if any temporary object has been created in current session,
        # here we should also append res.append('pg_temp_xxx') were xxx is
        # a number assigned by the server.

        res.append('pg_catalog')
    res.extend(ctx.options.search_path)

    return pgast.ArrayExpr(elements=[pgast.StringConstant(val=r) for r in res])


VALUE_FUNC_PASS_THROUGH = frozenset({
    val_func_op.CURRENT_DATE,
    val_func_op.CURRENT_TIME,
    val_func_op.CURRENT_TIME_N,
    val_func_op.CURRENT_TIMESTAMP,
    val_func_op.CURRENT_TIMESTAMP_N,
    val_func_op.LOCALTIME,
    val_func_op.LOCALTIME_N,
    val_func_op.LOCALTIMESTAMP,
    val_func_op.LOCALTIMESTAMP_N,
})

VALUE_FUNC_USER = frozenset({
    val_func_op.CURRENT_ROLE,
    val_func_op.CURRENT_USER,
    val_func_op.USER,
    val_func_op.SESSION_USER,
})


@eval.register
def eval_SQLValueFunction(
    expr: pgast.SQLValueFunction,
    *,
    ctx: Context,
) -> pgast.BaseExpr:
    if expr.op in VALUE_FUNC_PASS_THROUGH:
        return expr

    if expr.op in VALUE_FUNC_USER:
        return pgast.StringConstant(val=ctx.options.current_user)

    if expr.op == val_func_op.CURRENT_CATALOG:
        return pgast.StringConstant(val=ctx.options.current_database)

    if expr.op == val_func_op.CURRENT_SCHEMA:
        # note: PG also does a check that this schema exists and proceeds to
        # the next one in the search path
        return pgast.StringConstant(val=ctx.options.search_path[0])

    # this should never happen
    raise NotImplementedError()


@eval.register
def eval_ParamRef(
    _expr: pgast.ParamRef,
    *,
    ctx: Context,
) -> Optional[pgast.BaseExpr]:
    if len(ctx.options.normalized_params) > 0:
        raise DisableNormalization()
    else:
        return None
