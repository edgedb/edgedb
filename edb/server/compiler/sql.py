#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Optional,
    Tuple,
    Mapping,
    Sequence,
    List,
)

import dataclasses
import functools
import hashlib
import immutables

from edb import errors

from edb.schema import schema as s_schema

from edb.pgsql import ast as pgast
from edb.pgsql import common as pg_common
from edb.pgsql import codegen as pg_codegen
from edb.pgsql import parser as pg_parser

from . import dbstate


# Frontend-only settings. Maps setting name into their mutability flag.
FE_SETTINGS_MUTABLE: immutables.Map[str, bool] = immutables.Map(
    {
        'search_path': True,
        'allow_user_specified_id': True,
        'server_version': False,
        'server_version_num': False,
    }
)


def compile_sql(
    query_str: str,
    *,
    schema: s_schema.Schema,
    tx_state: dbstate.SQLTransactionState,
    prepared_stmt_map: Mapping[str, str],
    current_database: str,
    current_user: str,
) -> List[dbstate.SQLQueryUnit]:
    opts = ResolverOptionsPartial(
        query_str=query_str,
        current_database=current_database,
        current_user=current_user,
    )

    stmts = pg_parser.parse(query_str, propagate_spans=True)
    sql_units = []
    for stmt in stmts:
        orig_text = pg_codegen.generate_source(stmt)
        fe_settings = tx_state.current_fe_settings()

        unit = dbstate.SQLQueryUnit(
            orig_query=orig_text,
            fe_settings=fe_settings,
            query=orig_text,
        )

        if isinstance(stmt, pgast.VariableSetStmt):
            # GOTCHA: setting is frontend-only regardless of its mutability
            fe_only = stmt.name in FE_SETTINGS_MUTABLE

            if fe_only:
                if not FE_SETTINGS_MUTABLE[stmt.name]:
                    raise errors.QueryError(
                        f'parameter "{stmt.name}" cannot be changed',
                        pgext_code='55P02',  # cant_change_runtime_param
                    )
                value = pg_codegen.generate_source(stmt.args)
                unit.set_vars = {stmt.name: value}
            elif stmt.scope == pgast.OptionsScope.SESSION:
                if len(stmt.args.args) == 1 and isinstance(
                    stmt.args.args[0], pgast.StringConstant
                ):
                    # this value is unquoted for restoring state in pgcon
                    value = stmt.args.args[0].val
                else:
                    value = pg_codegen.generate_source(stmt.args)
                unit.set_vars = {stmt.name: value}
            unit.frontend_only = fe_only
            if fe_only:
                unit.command_complete_tag = dbstate.TagPlain(tag=b"SET")
            unit.is_local = stmt.scope == pgast.OptionsScope.TRANSACTION
        elif isinstance(stmt, pgast.VariableResetStmt):
            fe_only = stmt.name in FE_SETTINGS_MUTABLE
            if fe_only and stmt.name and not FE_SETTINGS_MUTABLE[stmt.name]:
                raise errors.QueryError(
                    f'parameter "{stmt.name}" cannot be changed',
                    pgext_code='55P02',  # cant_change_runtime_param
                )
            if fe_only or stmt.scope == pgast.OptionsScope.SESSION:
                unit.set_vars = {stmt.name: None}
            unit.frontend_only = fe_only
            if fe_only:
                unit.command_complete_tag = dbstate.TagPlain(tag=b"RESET")
            unit.is_local = stmt.scope == pgast.OptionsScope.TRANSACTION

        elif isinstance(stmt, pgast.VariableShowStmt):
            unit.get_var = stmt.name
            unit.frontend_only = stmt.name in FE_SETTINGS_MUTABLE
            if unit.frontend_only:
                unit.command_complete_tag = dbstate.TagPlain(tag=b"SHOW")

        elif isinstance(stmt, pgast.SetTransactionStmt):
            if stmt.scope == pgast.OptionsScope.SESSION:
                unit.set_vars = {
                    f"default_{name}": (
                        value.val
                        if isinstance(value, pgast.StringConstant)
                        else pg_codegen.generate_source(value)
                    )
                    for name, value in stmt.options.options.items()
                }

        elif isinstance(stmt, (pgast.BeginStmt, pgast.StartStmt)):
            unit.tx_action = dbstate.TxAction.START
        elif isinstance(stmt, pgast.CommitStmt):
            unit.tx_action = dbstate.TxAction.COMMIT
            unit.tx_chain = stmt.chain or False

        elif isinstance(stmt, pgast.RollbackStmt):
            unit.tx_action = dbstate.TxAction.ROLLBACK
            unit.tx_chain = stmt.chain or False

        elif isinstance(stmt, pgast.SavepointStmt):
            unit.tx_action = dbstate.TxAction.DECLARE_SAVEPOINT
            unit.sp_name = stmt.savepoint_name

        elif isinstance(stmt, pgast.ReleaseStmt):
            unit.tx_action = dbstate.TxAction.RELEASE_SAVEPOINT
            unit.sp_name = stmt.savepoint_name

        elif isinstance(stmt, pgast.RollbackToStmt):
            unit.tx_action = dbstate.TxAction.ROLLBACK_TO_SAVEPOINT
            unit.sp_name = stmt.savepoint_name

        elif isinstance(stmt, pgast.TwoPhaseTransactionStmt):
            raise NotImplementedError(
                "two-phase transactions are not supported"
            )
        elif isinstance(stmt, pgast.PrepareStmt):
            # Translate the underlying query.
            stmt_source, complete_tag = resolve_query(
                stmt.query, schema, tx_state, opts
            )
            if stmt.argtypes:
                param_types = []
                for pt in stmt.argtypes:
                    param_types.append(pg_codegen.generate_source(pt))
                param_text = f"({', '.join(param_types)})"
            else:
                param_text = ""

            sql_trailer = f"{param_text} AS ({stmt_source.text})"

            mangled_stmt_name = compute_stmt_name(
                f"PREPARE {pg_common.quote_ident(stmt.name)}{sql_trailer}",
                tx_state,
            )

            sql_text = (
                f"PREPARE {pg_common.quote_ident(mangled_stmt_name)}"
                f"{sql_trailer}"
            )

            unit.query = sql_text
            unit.prepare = dbstate.PrepareData(
                stmt_name=stmt.name,
                be_stmt_name=mangled_stmt_name.encode("utf-8"),
                query=stmt_source.text,
                translation_data=stmt_source.translation_data,
            )
            unit.command_complete_tag = dbstate.TagPlain(tag=b"PREPARE")

        elif isinstance(stmt, pgast.ExecuteStmt):
            orig_name = stmt.name
            mangled_name = prepared_stmt_map.get(orig_name)
            if not mangled_name:
                raise errors.QueryError(
                    f"prepared statement \"{orig_name}\" does " f"not exist",
                    pgext_code='26000',  # invalid_sql_statement_name
                )
            stmt.name = mangled_name

            unit.query = pg_codegen.generate_source(stmt)
            unit.execute = dbstate.ExecuteData(
                stmt_name=orig_name,
                be_stmt_name=mangled_name.encode("utf-8"),
            )
        elif isinstance(stmt, pgast.DeallocateStmt):
            orig_name = stmt.name
            mangled_name = prepared_stmt_map.get(orig_name)
            if not mangled_name:
                raise errors.QueryError(
                    f"prepared statement \"{orig_name}\" does " f"not exist",
                    pgext_code='26000',  # invalid_sql_statement_name
                )
            stmt.name = mangled_name

            unit.query = pg_codegen.generate_source(stmt)
            unit.deallocate = dbstate.DeallocateData(
                stmt_name=orig_name,
                be_stmt_name=mangled_name.encode("utf-8"),
            )
            unit.command_complete_tag = dbstate.TagPlain(tag=b"DEALLOCATE")

        elif isinstance(stmt, pgast.LockStmt):
            if stmt.mode not in ('ACCESS SHARE', 'ROW SHARE', 'SHARE'):
                raise NotImplementedError("exclusive lock is not supported")
            # just ignore
            unit.query = "DO $$ BEGIN END $$;"
        else:
            assert isinstance(stmt, (pgast.Query, pgast.CopyStmt))
            source, complete_tag = resolve_query(stmt, schema, tx_state, opts)

            unit.query = source.text
            unit.translation_data = source.translation_data
            unit.command_complete_tag = complete_tag

        unit.stmt_name = compute_stmt_name(unit.query, tx_state).encode("utf-8")

        tx_state.apply(unit)
        sql_units.append(unit)

    if not sql_units:
        # Cluvio will try to execute an empty query
        sql_units.append(
            dbstate.SQLQueryUnit(
                orig_query='',
                query='',
                fe_settings=tx_state.current_fe_settings(),
            )
        )

    return sql_units


@dataclasses.dataclass(kw_only=True, eq=False, repr=False)
class ResolverOptionsPartial:
    current_user: str
    current_database: str
    query_str: str


def resolve_query(
    stmt: pgast.Base,
    schema: s_schema.Schema,
    tx_state: dbstate.SQLTransactionState,
    opts: ResolverOptionsPartial,
) -> Tuple[pg_codegen.SQLSource, Optional[dbstate.CommandCompleteTag]]:
    from edb.pgsql import resolver as pg_resolver

    search_path: Sequence[str] = ("public",)
    allow_user_specified_id: bool = False

    try:
        sp = tx_state.get("search_path")
    except KeyError:
        sp = None
    if isinstance(sp, str):
        search_path = parse_search_path(sp)

    try:
        allow_id = tx_state.get("allow_user_specified_id")
    except KeyError:
        allow_id = None
    if isinstance(allow_id, str):
        allow_user_specified_id = bool(allow_id)

    options = pg_resolver.Options(
        current_user=opts.current_user,
        current_database=opts.current_database,
        current_query=opts.query_str,
        search_path=search_path,
        allow_user_specified_id=allow_user_specified_id,
    )
    resolved, complete_tag = pg_resolver.resolve(stmt, schema, options)
    return (
        pg_codegen.generate(resolved, with_translation_data=True),
        complete_tag,
    )


def compute_stmt_name(text: str, tx_state: dbstate.SQLTransactionState) -> str:
    stmt_hash = hashlib.sha1(text.encode("utf-8"))
    for setting_name in sorted(FE_SETTINGS_MUTABLE):
        try:
            setting_value = tx_state.get(setting_name)
        except KeyError:
            pass
        else:
            stmt_hash.update(f"{setting_name}:{setting_value}".encode("utf-8"))
    return f"edb{stmt_hash.hexdigest()}"


@functools.cache
def parse_search_path(search_path_str: str) -> list[str]:
    search_path_stmt = pg_parser.parse(f"SET search_path = {search_path_str}")[
        0
    ]
    assert isinstance(search_path_stmt, pgast.VariableSetStmt)
    return [
        arg.val
        for arg in search_path_stmt.args.args
        if isinstance(arg, pgast.StringConstant)
    ]
