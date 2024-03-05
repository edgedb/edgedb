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
    Any,
    Optional,
    Tuple,
    Union,
    AbstractSet,
    Iterable,
    Mapping,
    Sequence,
    Dict,
    List,
    NamedTuple,
    cast,
    TYPE_CHECKING,
)

import dataclasses
import functools
import json
import hashlib
import pickle
import textwrap
import time
import uuid

import immutables

from edb import errors

from edb.common.typeutils import not_none

from edb.server import defines
from edb.server import config

from edb import edgeql
from edb.common import debug
from edb.common import verutils
from edb.common import uuidgen

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from edb.ir import staeval as ireval
from edb.ir import ast as irast

from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import extensions as s_ext
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import properties as s_props
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import reflection as s_refl
from edb.schema import roles as s_role
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import version as s_ver

from edb.pgsql import ast as pgast
from edb.pgsql import compiler as pg_compiler
from edb.pgsql import codegen as pg_codegen
from edb.pgsql import common as pg_common
from edb.pgsql import debug as pg_debug
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import params as pg_params
from edb.pgsql import patches as pg_patches
from edb.pgsql import types as pg_types
from edb.pgsql import delta as pg_delta

from . import dbstate
from . import enums
from . import explain
from . import sertypes
from . import status
from . import ddl
from . import rpc

if TYPE_CHECKING:
    from edb.pgsql import metaschema


EMPTY_MAP: immutables.Map[Any, Any] = immutables.Map()


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState:

    user_schema: s_schema.Schema
    global_schema: s_schema.Schema
    cached_reflection: immutables.Map[str, Tuple[str, ...]]


@dataclasses.dataclass(frozen=True)
class CompileContext:

    compiler_state: CompilerState
    state: dbstate.CompilerConnectionState
    output_format: enums.OutputFormat
    expected_cardinality_one: bool
    protocol_version: defines.ProtocolVersion
    expect_rollback: bool = False
    json_parameters: bool = False
    schema_reflection_mode: bool = False
    implicit_limit: int = 0
    inline_typeids: bool = False
    inline_typenames: bool = False
    inline_objectids: bool = True
    schema_object_ids: Optional[
        Mapping[tuple[s_name.Name, Optional[str]], uuid.UUID]] = None
    source: Optional[edgeql.Source] = None
    backend_runtime_params: pg_params.BackendRuntimeParams = (
        pg_params.get_default_runtime_params())
    compat_ver: Optional[verutils.Version] = None
    bootstrap_mode: bool = False
    internal_schema_mode: bool = False
    log_ddl_as_migrations: bool = True
    dump_restore_mode: bool = False
    notebook: bool = False
    persistent_cache: bool = False
    cache_key: Optional[uuid.UUID] = None

    def _assert_not_in_migration_block(
        self,
        ql: qlast.Base
    ) -> None:
        """Check that a START MIGRATION block is *not* active."""
        current_tx = self.state.current_tx()
        mstate = current_tx.get_migration_state()
        if mstate is not None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} in a migration block',
                context=ql.context,
            )

    def _assert_in_migration_block(
        self,
        ql: qlast.Base
    ) -> dbstate.MigrationState:
        """Check that a START MIGRATION block *is* active."""
        current_tx = self.state.current_tx()
        mstate = current_tx.get_migration_state()
        if mstate is None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} outside of a migration block',
                context=ql.context,
            )
        return mstate

    def _assert_not_in_migration_rewrite_block(
        self,
        ql: qlast.Base
    ) -> None:
        """Check that a START MIGRATION REWRITE block is *not* active."""
        current_tx = self.state.current_tx()
        mstate = current_tx.get_migration_rewrite_state()
        if mstate is not None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} in a migration rewrite block',
                context=ql.context,
            )

    def _assert_in_migration_rewrite_block(
        self,
        ql: qlast.Base
    ) -> dbstate.MigrationRewriteState:
        """Check that a START MIGRATION REWRITE block *is* active."""
        current_tx = self.state.current_tx()
        mstate = current_tx.get_migration_rewrite_state()
        if mstate is None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} outside of a migration rewrite block',
                context=ql.context,
            )
        return mstate


DEFAULT_MODULE_ALIASES_MAP: immutables.Map[Optional[str], str] = (
    immutables.Map({None: defines.DEFAULT_MODULE_ALIAS}))


def compile_edgeql_script(
    ctx: CompileContext,
    eql: str,
) -> Tuple[s_schema.Schema, str]:

    sql = _compile_ql_script(ctx, eql)
    new_schema = ctx.state.current_tx().get_schema(
        ctx.compiler_state.std_schema)
    assert isinstance(new_schema, s_schema.ChainedSchema)
    return new_schema.get_top_schema(), sql


def new_compiler(
    std_schema: s_schema.Schema,
    reflection_schema: s_schema.Schema,
    schema_class_layout: s_refl.SchemaClassLayout,
    *,
    backend_runtime_params: Optional[pg_params.BackendRuntimeParams] = None,
    local_intro_query: Optional[str] = None,
    global_intro_query: Optional[str] = None,
    config_spec: Optional[config.Spec] = None,
) -> Compiler:
    """Create and return a compiler instance."""

    # XXX: THIS IS NOT GREAT
    assert isinstance(std_schema, s_schema.FlatSchema)
    assert isinstance(reflection_schema, s_schema.FlatSchema)

    if not backend_runtime_params:
        backend_runtime_params = pg_params.get_default_runtime_params()

    if not config_spec:
        config_spec = config.load_spec_from_schema(std_schema)

    return Compiler(CompilerState(
        std_schema=std_schema,
        refl_schema=reflection_schema,
        schema_class_layout=schema_class_layout,
        backend_runtime_params=backend_runtime_params,
        config_spec=config_spec,
        local_intro_query=local_intro_query,
        global_intro_query=global_intro_query,
    ))


async def new_compiler_from_pg(con: metaschema.PGConnection) -> Compiler:
    num_patches = await get_patch_count(con)

    std_schema, reflection_schema = await load_std_and_reflection_schema(
        con, num_patches)

    return new_compiler(
        std_schema=std_schema,
        reflection_schema=reflection_schema,
        schema_class_layout=await load_schema_class_layout(
            con, num_patches
        ),
        local_intro_query=await load_schema_intro_query(
            con, num_patches, 'local_intro_query'
        ),
        global_intro_query=await load_schema_intro_query(
            con, num_patches, 'global_intro_query'
        ),
        config_spec=None,
    )


def new_compiler_context(
    *,
    compiler_state: CompilerState,
    user_schema: s_schema.Schema,
    global_schema: s_schema.Schema=s_schema.EMPTY_SCHEMA,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    expected_cardinality_one: bool = False,
    json_parameters: bool = False,
    schema_reflection_mode: bool = False,
    output_format: enums.OutputFormat = enums.OutputFormat.BINARY,
    bootstrap_mode: bool = False,
    internal_schema_mode: bool = False,
    protocol_version: defines.ProtocolVersion = defines.CURRENT_PROTOCOL,
    backend_runtime_params: pg_params.BackendRuntimeParams = (
        pg_params.get_default_runtime_params()),
) -> CompileContext:
    """Create and return an ad-hoc compiler context."""

    state = dbstate.CompilerConnectionState(
        user_schema=user_schema,
        global_schema=global_schema,
        modaliases=immutables.Map(modaliases) if modaliases else EMPTY_MAP,
        session_config=EMPTY_MAP,
        database_config=EMPTY_MAP,
        system_config=EMPTY_MAP,
        cached_reflection=EMPTY_MAP,
    )

    ctx = CompileContext(
        compiler_state=compiler_state,
        state=state,
        output_format=output_format,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=json_parameters,
        schema_reflection_mode=schema_reflection_mode,
        bootstrap_mode=bootstrap_mode,
        internal_schema_mode=internal_schema_mode,
        protocol_version=protocol_version,
        backend_runtime_params=backend_runtime_params,
    )

    return ctx


async def get_patch_count(backend_conn: metaschema.PGConnection) -> int:
    """Get the number of applied patches."""
    num_patches = await backend_conn.sql_fetch_val(
        b'''
            SELECT json::json from edgedbinstdata.instdata
            WHERE key = 'num_patches';
        ''',
    )
    res: int = json.loads(num_patches) if num_patches else 0
    return res


async def load_std_and_reflection_schema(
    backend_conn: metaschema.PGConnection,
    patches: int,
) -> tuple[s_schema.Schema, s_schema.Schema]:
    vkey = pg_patches.get_version_key(patches)

    # stdschema and reflschema are combined in one pickle to preserve sharing.
    key = f"std_and_reflection_schema{vkey}"
    data = await backend_conn.sql_fetch_val(
        b"""
        SELECT bin FROM edgedbinstdata.instdata
        WHERE key = $1
        """,
        args=[key.encode("utf-8")],
    )
    try:
        std_schema: s_schema.FlatSchema
        refl_schema: s_schema.FlatSchema
        std_schema, refl_schema = pickle.loads(data)
        if vkey != pg_patches.get_version_key(len(pg_patches.PATCHES)):
            std_schema = s_schema.upgrade_schema(std_schema)
            refl_schema = s_schema.upgrade_schema(refl_schema)
        return (std_schema, refl_schema)
    except Exception as e:
        raise RuntimeError(
            'could not load std schema pickle') from e


async def load_schema_intro_query(
    backend_conn: metaschema.PGConnection,
    patches: int,
    kind: str,
) -> str:
    kind += pg_patches.get_version_key(patches)
    return (await backend_conn.sql_fetch_val(
        b"""
        SELECT text FROM edgedbinstdata.instdata
        WHERE key = $1::text;
        """,
        args=[kind.encode("utf-8")],
    )).decode('utf-8')


async def load_schema_class_layout(
    backend_conn: metaschema.PGConnection,
    patches: int,
) -> s_refl.SchemaClassLayout:
    key = f'classlayout{pg_patches.get_version_key(patches)}'
    data = await backend_conn.sql_fetch_val(
        b"""
        SELECT bin FROM edgedbinstdata.instdata
        WHERE key = $1::text;
        """,
        args=[key.encode("utf-8")],
    )
    try:
        return cast(s_refl.SchemaClassLayout, pickle.loads(data))
    except Exception as e:
        raise RuntimeError(
            'could not load schema class layout pickle') from e


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompilerState:

    std_schema: s_schema.FlatSchema
    refl_schema: s_schema.FlatSchema
    schema_class_layout: s_refl.SchemaClassLayout

    backend_runtime_params: pg_params.BackendRuntimeParams
    config_spec: config.Spec

    local_intro_query: Optional[str]
    global_intro_query: Optional[str]

    @functools.cached_property
    def state_serializer_factory(self) -> sertypes.StateSerializerFactory:
        # TODO: This factory will probably need to become per-db once
        # config spec differs between databases. See also #5836.
        return sertypes.StateSerializerFactory(
            self.std_schema, self.config_spec
        )

    @functools.cached_property
    def compilation_config_serializer(self) -> sertypes.InputShapeSerializer:
        return (
            self.state_serializer_factory.make_compilation_config_serializer()
        )


class Compiler:

    state: CompilerState

    def __init__(self, state: CompilerState):
        self.state = state

    @staticmethod
    def _try_compile_rollback(eql: Union[edgeql.Source, bytes]) -> tuple[
        dbstate.QueryUnitGroup, int
    ]:
        source: Union[str, edgeql.Source]
        if isinstance(eql, edgeql.Source):
            source = eql
        else:
            source = eql.decode()
        statements = edgeql.parse_block(source)

        stmt = statements[0]
        unit = None
        if isinstance(stmt, qlast.RollbackTransaction):
            sql = b'ROLLBACK;'
            unit = dbstate.QueryUnit(
                status=b'ROLLBACK',
                sql=(sql,),
                tx_rollback=True,
                cacheable=False)

        elif isinstance(stmt, qlast.RollbackToSavepoint):
            sql = f'ROLLBACK TO {pg_common.quote_ident(stmt.name)};'.encode()
            unit = dbstate.QueryUnit(
                status=b'ROLLBACK TO SAVEPOINT',
                sql=(sql,),
                tx_savepoint_rollback=True,
                sp_name=stmt.name,
                cacheable=False)

        if unit is not None:
            rv = dbstate.QueryUnitGroup()
            rv.append(unit)
            return rv, len(statements) - 1

        raise errors.TransactionError(
            'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
        )  # pragma: no cover

    def compile_notebook(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: immutables.Map[str, Tuple[str, ...]],
        database_config: immutables.Map[str, config.SettingValue],
        system_config: immutables.Map[str, config.SettingValue],
        queries: List[str],
        protocol_version: defines.ProtocolVersion,
        implicit_limit: int = 0,
    ) -> List[
        Tuple[
            bool,
            Union[dbstate.QueryUnit, Tuple[str, str, Dict[int, str]]]
        ]
    ]:

        state = dbstate.CompilerConnectionState(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=reflection_cache,
        )

        state.start_tx()

        result: List[
            Tuple[
                bool,
                Union[dbstate.QueryUnit, Tuple[str, str, Dict[int, str]]]
            ]
        ] = []

        for query in queries:
            try:
                source = edgeql.Source.from_string(query)

                ctx = CompileContext(
                    compiler_state=self.state,
                    state=state,
                    output_format=enums.OutputFormat.BINARY,
                    expected_cardinality_one=False,
                    implicit_limit=implicit_limit,
                    inline_typeids=False,
                    inline_typenames=True,
                    json_parameters=False,
                    source=source,
                    protocol_version=protocol_version,
                    notebook=True,
                )

                result.append(
                    (False, compile(ctx=ctx, source=source)[0]))
            except Exception as ex:
                fields = {}
                typename = 'Error'
                if (isinstance(ex, errors.EdgeDBError) and
                        type(ex) is not errors.EdgeDBError):
                    fields = ex._attrs
                    typename = type(ex).__name__
                result.append(
                    (True, (typename, str(ex), fields)))
                break

        return result

    def compile_sql(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: immutables.Map[str, Tuple[str, ...]],
        database_config: immutables.Map[str, config.SettingValue],
        system_config: immutables.Map[str, config.SettingValue],
        query_str: str,
        tx_state: dbstate.SQLTransactionState,
        prepared_stmt_map: Mapping[str, str],
        current_database: str,
        current_user: str,
    ) -> List[dbstate.SQLQueryUnit]:
        state = dbstate.CompilerConnectionState(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=reflection_cache,
        )
        schema = state.current_tx().get_schema(self.state.std_schema)

        from edb.pgsql import parser as pg_parser
        from edb.pgsql import resolver as pg_resolver
        from edb.pgsql import codegen as pg_codegen

        @functools.cache
        def parse_search_path(search_path_str: str) -> list[str]:
            search_path_stmt = pg_parser.parse(
                f"SET search_path = {search_path_str}"
            )[0]
            assert isinstance(search_path_stmt, pgast.VariableSetStmt)
            return [
                arg.val for arg in search_path_stmt.args.args
                if isinstance(arg, pgast.StringConstant)
            ]

        def translate_query(stmt: pgast.Base) -> pg_codegen.SQLSource:
            args = {}
            try:
                search_path = tx_state.get("search_path")
            except KeyError:
                search_path = None
            if isinstance(search_path, str):
                args['search_path'] = parse_search_path(search_path)
            options = pg_resolver.Options(
                current_user=current_user,
                current_database=current_database,
                current_query=query_str,
                **args
            )
            resolved = pg_resolver.resolve(stmt, schema, options)
            return pg_codegen.generate(
                resolved, with_translation_data=True
            )

        def compute_stmt_name(text: str) -> str:
            stmt_hash = hashlib.sha1(text.encode("utf-8"))
            for setting_name in sorted(fe_settings_mutable):
                try:
                    setting_value = tx_state.get(setting_name)
                except KeyError:
                    pass
                else:
                    stmt_hash.update(
                        f"{setting_name}:{setting_value}".encode("utf-8"))
            return f"edb{stmt_hash.hexdigest()}"

        pg_gen_source = functools.partial(
            pg_codegen.generate_source, pretty=False)

        # frontend-only settings (key) and their mutability (value)
        fe_settings_mutable = {
            'search_path': True,
            'server_version': False,
            'server_version_num': False,
        }
        stmts = pg_parser.parse(query_str)
        sql_units = []
        for stmt in stmts:
            orig_text = pg_gen_source(stmt)

            if debug.flags.sql_input:
                debug.header('SQL Input')
                debug.dump_code(
                    pg_codegen.generate_source(stmt, pretty=True), lexer='sql'
                )

            unit_ctor = functools.partial(
                dbstate.SQLQueryUnit,
                orig_query=orig_text,
                fe_settings=tx_state.current_fe_settings(),
            )

            if isinstance(stmt, pgast.VariableSetStmt):
                # GOTCHA: setting is frontend-only regardless of its mutability
                fe_only = stmt.name in fe_settings_mutable

                args = {
                    "query": orig_text,
                    "frontend_only": fe_only,
                    "command_tag": b"SET",
                    "is_local": stmt.scope == pgast.OptionsScope.TRANSACTION,
                }
                if fe_only:
                    if not fe_settings_mutable[stmt.name]:
                        raise errors.QueryError(
                            f'parameter "{stmt.name}" cannot be changed',
                            pgext_code='55P02',  # cant_change_runtime_param
                        )
                    value = pg_codegen.generate_source(stmt.args)
                    args["set_vars"] = {stmt.name: value}
                elif stmt.scope == pgast.OptionsScope.SESSION:
                    if len(stmt.args.args) == 1 and isinstance(
                        stmt.args.args[0], pgast.StringConstant
                    ):
                        # this value is unquoted for restoring state in pgcon
                        value = stmt.args.args[0].val
                    else:
                        value = pg_gen_source(stmt.args)
                    args["set_vars"] = {stmt.name: value}
                unit = unit_ctor(**args)
            elif isinstance(stmt, pgast.VariableResetStmt):
                fe_only = stmt.name in fe_settings_mutable
                if (
                    fe_only and stmt.name
                    and not fe_settings_mutable[stmt.name]
                ):
                    raise errors.QueryError(
                        f'parameter "{stmt.name}" cannot be changed',
                        pgext_code='55P02',  # cant_change_runtime_param
                    )
                args = {
                    "query": orig_text,
                    "frontend_only": fe_only,
                    "command_tag": b"RESET",
                    "is_local": stmt.scope == pgast.OptionsScope.TRANSACTION,
                }
                if fe_only or stmt.scope == pgast.OptionsScope.SESSION:
                    args["set_vars"] = {stmt.name: None}
                unit = unit_ctor(**args)
            elif isinstance(stmt, pgast.VariableShowStmt):
                unit = unit_ctor(
                    query=orig_text,
                    get_var=stmt.name,
                    frontend_only=stmt.name in fe_settings_mutable,
                    command_tag=b"SHOW",
                )
            elif isinstance(stmt, pgast.SetTransactionStmt):
                args = {"query": orig_text}
                if stmt.scope == pgast.OptionsScope.SESSION:
                    args["set_vars"] = {
                        f"default_{name}": value.val
                        if isinstance(value, pgast.StringConstant)
                        else pg_gen_source(value)
                        for name, value in stmt.options.options.items()
                    }
                unit = unit_ctor(**args)
            elif isinstance(stmt, (pgast.BeginStmt, pgast.StartStmt)):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.START,
                )
            elif isinstance(stmt, pgast.CommitStmt):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.COMMIT,
                    tx_chain=stmt.chain or False,
                )
            elif isinstance(stmt, pgast.RollbackStmt):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.ROLLBACK,
                    tx_chain=stmt.chain or False,
                )
            elif isinstance(stmt, pgast.SavepointStmt):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.DECLARE_SAVEPOINT,
                    sp_name=stmt.savepoint_name,
                )
            elif isinstance(stmt, pgast.ReleaseStmt):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.RELEASE_SAVEPOINT,
                    sp_name=stmt.savepoint_name,
                )
            elif isinstance(stmt, pgast.RollbackToStmt):
                unit = unit_ctor(
                    query=orig_text,
                    tx_action=dbstate.TxAction.ROLLBACK_TO_SAVEPOINT,
                    sp_name=stmt.savepoint_name,
                )
            elif isinstance(stmt, pgast.TwoPhaseTransactionStmt):
                raise NotImplementedError(
                    "two-phase transactions are not supported"
                )
            elif isinstance(stmt, pgast.PrepareStmt):
                # Translate the underlying query.
                stmt_source = translate_query(stmt.query)
                if stmt.argtypes:
                    param_types = []
                    for pt in stmt.argtypes:
                        param_types.append(pg_gen_source(pt))
                    param_text = f"({', '.join(param_types)})"
                else:
                    param_text = ""

                sql_trailer = f"{param_text} AS ({stmt_source.text})"

                mangled_stmt_name = compute_stmt_name(
                    f"PREPARE {pg_common.quote_ident(stmt.name)}{sql_trailer}"
                )

                sql_text = (
                    f"PREPARE {pg_common.quote_ident(mangled_stmt_name)}"
                    f"{sql_trailer}"
                )

                unit = unit_ctor(
                    query=sql_text,
                    prepare=dbstate.PrepareData(
                        stmt_name=stmt.name,
                        be_stmt_name=mangled_stmt_name.encode("utf-8"),
                        query=stmt_source.text,
                        translation_data=stmt_source.translation_data,
                    ),
                    command_tag=b"PREPARE",
                )
            elif isinstance(stmt, pgast.ExecuteStmt):
                orig_name = stmt.name
                mangled_name = prepared_stmt_map.get(orig_name)
                if not mangled_name:
                    raise errors.QueryError(
                        f"prepared statement \"{orig_name}\" does "
                        f"not exist",
                        pgext_code='26000',  # invalid_sql_statement_name
                    )
                stmt.name = mangled_name

                unit = unit_ctor(
                    query=pg_gen_source(stmt),
                    execute=dbstate.ExecuteData(
                        stmt_name=orig_name,
                        be_stmt_name=mangled_name.encode("utf-8"),
                    ),
                )
            elif isinstance(stmt, pgast.DeallocateStmt):
                orig_name = stmt.name
                mangled_name = prepared_stmt_map.get(orig_name)
                if not mangled_name:
                    raise errors.QueryError(
                        f"prepared statement \"{orig_name}\" does "
                        f"not exist",
                        pgext_code='26000',  # invalid_sql_statement_name
                    )
                stmt.name = mangled_name
                unit = unit_ctor(
                    query=pg_gen_source(stmt),
                    deallocate=dbstate.DeallocateData(
                        stmt_name=orig_name,
                        be_stmt_name=mangled_name.encode("utf-8"),
                    ),
                    command_tag=b"DEALLOCATE",
                )
            elif isinstance(stmt, pgast.LockStmt):
                if stmt.mode not in ('ACCESS SHARE', 'ROW SHARE', 'SHARE'):
                    raise NotImplementedError(
                        "exclusive lock is not supported"
                    )
                # just ignore
                unit = unit_ctor(query="DO $$ BEGIN END $$;")
            else:
                source = translate_query(stmt)
                unit = unit_ctor(
                    query=source.text,
                    translation_data=source.translation_data,
                )

            if debug.flags.sql_output:
                debug.header('SQL Output')
                debug.dump_code(unit.query, lexer='sql')

            unit.stmt_name = compute_stmt_name(unit.query).encode("utf-8")

            tx_state.apply(unit)
            sql_units.append(unit)

        if not sql_units:
            # Cluvio will try to execute an empty query
            sql_units.append(dbstate.SQLQueryUnit(
                orig_query='',
                query='',
                fe_settings=tx_state.current_fe_settings()
            ))

        return sql_units

    def compile_request(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: immutables.Map[str, Tuple[str, ...]],
        database_config: Optional[immutables.Map[str, config.SettingValue]],
        system_config: Optional[immutables.Map[str, config.SettingValue]],
        serialized_request: bytes,
        original_query: str,
    ) -> Tuple[
        dbstate.QueryUnitGroup, Optional[dbstate.CompilerConnectionState]
    ]:
        request = rpc.CompilationRequest(
            self.state.compilation_config_serializer
        )
        request.deserialize(serialized_request, original_query)

        units, cstate = self.compile(
            user_schema,
            global_schema,
            reflection_cache,
            database_config,
            system_config,
            request.source,
            request.modaliases,
            request.session_config,
            request.output_format,
            request.expect_one,
            request.implicit_limit,
            request.inline_typeids,
            request.inline_typenames,
            request.protocol_version,
            request.inline_objectids,
            request.json_parameters,
            persistent_cache=bool(debug.flags.persistent_cache),
            cache_key=request.get_cache_key(),
        )
        return units, cstate

    def compile(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: immutables.Map[str, Tuple[str, ...]],
        database_config: Optional[immutables.Map[str, config.SettingValue]],
        system_config: Optional[immutables.Map[str, config.SettingValue]],
        source: edgeql.Source,
        sess_modaliases: Optional[immutables.Map[Optional[str], str]],
        sess_config: Optional[immutables.Map[str, config.SettingValue]],
        output_format: enums.OutputFormat,
        expect_one: bool,
        implicit_limit: int,
        inline_typeids: bool,
        inline_typenames: bool,
        protocol_version: defines.ProtocolVersion,
        inline_objectids: bool = True,
        json_parameters: bool = False,
        persistent_cache: bool = False,
        cache_key: Optional[uuid.UUID] = None,
    ) -> Tuple[dbstate.QueryUnitGroup,
               Optional[dbstate.CompilerConnectionState]]:

        if sess_config is None:
            sess_config = EMPTY_MAP

        if database_config is None:
            database_config = EMPTY_MAP

        if system_config is None:
            system_config = EMPTY_MAP

        if sess_modaliases is None:
            sess_modaliases = DEFAULT_MODULE_ALIASES_MAP

        state = dbstate.CompilerConnectionState(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=sess_modaliases,
            session_config=sess_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=reflection_cache,
        )

        ctx = CompileContext(
            compiler_state=self.state,
            state=state,
            output_format=output_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            json_parameters=json_parameters,
            source=source,
            protocol_version=protocol_version,
            persistent_cache=persistent_cache,
            cache_key=cache_key,
        )

        unit_group = compile(ctx=ctx, source=source)
        tx_started = False
        for unit in unit_group:
            if unit.tx_id:
                tx_started = True
                break

        if tx_started:
            return unit_group, ctx.state
        else:
            return unit_group, None

    def compile_in_tx_request(
        self,
        state: dbstate.CompilerConnectionState,
        txid: int,
        serialized_request: bytes,
        original_query: str,
        expect_rollback: bool = False,
    ) -> Tuple[
        dbstate.QueryUnitGroup, Optional[dbstate.CompilerConnectionState]
    ]:
        request = rpc.CompilationRequest(
            self.state.compilation_config_serializer
        )
        request.deserialize(serialized_request, original_query)

        units, cstate = self.compile_in_tx(
            state,
            txid,
            request.source,
            request.output_format,
            request.expect_one,
            request.implicit_limit,
            request.inline_typeids,
            request.inline_typenames,
            request.protocol_version,
            request.inline_objectids,
            request.json_parameters,
            expect_rollback=expect_rollback,
            persistent_cache=bool(debug.flags.persistent_cache),
            cache_key=request.get_cache_key(),
        )
        return units, cstate

    def compile_in_tx(
        self,
        state: dbstate.CompilerConnectionState,
        txid: int,
        source: edgeql.Source,
        output_format: enums.OutputFormat,
        expect_one: bool,
        implicit_limit: int,
        inline_typeids: bool,
        inline_typenames: bool,
        protocol_version: defines.ProtocolVersion,
        inline_objectids: bool = True,
        json_parameters: bool = False,
        expect_rollback: bool = False,
        persistent_cache: bool = False,
        cache_key: Optional[uuid.UUID] = None,
    ) -> Tuple[dbstate.QueryUnitGroup, dbstate.CompilerConnectionState]:
        if (
            expect_rollback and
            state.current_tx().id != txid and
            not state.can_sync_to_savepoint(txid)
        ):
            # This is a special case when COMMIT MIGRATION fails, the compiler
            # doesn't have the right transaction state, so we just roll back.
            return self._try_compile_rollback(source)[0], state
        else:
            state.sync_tx(txid)

        ctx = CompileContext(
            compiler_state=self.state,
            state=state,
            output_format=output_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            source=source,
            protocol_version=protocol_version,
            json_parameters=json_parameters,
            expect_rollback=expect_rollback,
            persistent_cache=persistent_cache,
            cache_key=cache_key,
        )

        return compile(ctx=ctx, source=source), ctx.state

    def interpret_backend_error(
        self,
        user_schema: bytes,
        global_schema: bytes,
        error_fields: dict[str, str],
        from_graphql: bool,
    ) -> errors.EdgeDBError:
        from . import errormech

        schema = s_schema.ChainedSchema(
            self.state.std_schema,
            pickle.loads(user_schema),
            pickle.loads(global_schema),
        )
        rv: errors.EdgeDBError = errormech.interpret_backend_error(
            schema, error_fields, from_graphql=from_graphql
        )
        return rv

    def parse_json_schema(
        self,
        schema_json: bytes,
        base_schema: s_schema.Schema | None,
    ) -> s_schema.Schema:
        if base_schema is None:
            base_schema = self.state.std_schema
        else:
            base_schema = s_schema.ChainedSchema(
                self.state.std_schema,
                s_schema.EMPTY_SCHEMA,
                base_schema,
            )

        return s_refl.parse_into(
            base_schema=base_schema,
            schema=s_schema.EMPTY_SCHEMA,
            data=schema_json,
            schema_class_layout=self.state.schema_class_layout,
        )

    def parse_db_config(
        self, db_config_json: bytes, user_schema: s_schema.Schema
    ) -> immutables.Map[str, config.SettingValue]:
        spec = config.ChainedSpec(
            self.state.config_spec,
            config.load_ext_spec_from_schema(
                user_schema,
                self.state.std_schema,
            ),
        )
        return config.from_json(spec, db_config_json)

    def parse_global_schema(self, global_schema_json: bytes) -> bytes:
        global_schema = self.parse_json_schema(global_schema_json, None)
        return pickle.dumps(global_schema, -1)

    def parse_user_schema_db_config(
        self,
        user_schema_json: bytes,
        db_config_json: bytes,
        global_schema_pickle: bytes,
    ) -> dbstate.ParsedDatabase:
        global_schema = pickle.loads(global_schema_pickle)
        user_schema = self.parse_json_schema(user_schema_json, global_schema)
        db_config = self.parse_db_config(db_config_json, user_schema)
        ext_config_settings = config.load_ext_settings_from_schema(
            s_schema.ChainedSchema(
                self.state.std_schema,
                user_schema,
                s_schema.EMPTY_SCHEMA,
            )
        )
        state_serializer = self.state.state_serializer_factory.make(
            user_schema,
            global_schema,
            defines.CURRENT_PROTOCOL,
        )
        return dbstate.ParsedDatabase(
            user_schema_pickle=pickle.dumps(user_schema, -1),
            schema_version=_get_schema_version(user_schema),
            database_config=db_config,
            ext_config_settings=ext_config_settings,
            protocol_version=defines.CURRENT_PROTOCOL,
            state_serializer=state_serializer,
        )

    def make_state_serializer(
        self,
        protocol_version: defines.ProtocolVersion,
        user_schema_pickle: bytes,
        global_schema_pickle: bytes,
    ) -> sertypes.StateSerializer:
        user_schema = pickle.loads(user_schema_pickle)
        global_schema = pickle.loads(global_schema_pickle)
        return self.state.state_serializer_factory.make(
            user_schema,
            global_schema,
            protocol_version,
        )

    def make_compilation_config_serializer(
        self
    ) -> sertypes.InputShapeSerializer:
        return self.state.compilation_config_serializer

    def describe_database_dump(
        self,
        user_schema_json: bytes,
        global_schema_json: bytes,
        db_config_json: bytes,
        protocol_version: defines.ProtocolVersion,
        with_secrets: bool,
    ) -> DumpDescriptor:
        global_schema = self.parse_json_schema(global_schema_json, None)
        user_schema = self.parse_json_schema(user_schema_json, global_schema)
        database_config = self.parse_db_config(db_config_json, user_schema)
        schema = s_schema.ChainedSchema(
            self.state.std_schema,
            user_schema,
            global_schema
        )

        sys_config_ddl = config.to_edgeql(
            self.state.config_spec, database_config, with_secrets=with_secrets,
        )
        # We need to put extension DDL configs *after* we have
        # reloaded the schema
        user_config_ddl = config.to_edgeql(
            config.load_ext_spec_from_schema(
                user_schema, self.state.std_schema),
            database_config,
            with_secrets=with_secrets,
        )

        schema_ddl = s_ddl.ddl_text_from_schema(
            schema, include_migrations=True)

        all_objects: Iterable[s_obj.Object] = schema.get_objects(
            exclude_stdlib=True,
            exclude_global=True,
        )
        ids = []
        sequences = []
        for obj in all_objects:
            if isinstance(obj, s_obj.QualifiedObject):
                ql_class = ''
            else:
                ql_class = str(type(obj).get_ql_class_or_die())

            ids.append((
                str(obj.get_name(schema)),
                ql_class,
                obj.id.bytes,
            ))

            if isinstance(obj, s_types.Type) and obj.is_sequence(schema):
                sequences.append(obj.id)

        objtypes = schema.get_objects(
            type=s_objtypes.ObjectType,
            exclude_stdlib=True,
        )
        descriptors = []

        cfg_object = schema.get('cfg::ConfigObject', type=s_objtypes.ObjectType)
        for objtype in objtypes:
            if objtype.is_union_type(schema) or objtype.is_view(schema):
                continue
            if objtype.issubclass(schema, cfg_object):
                continue
            descriptors.extend(_describe_object(schema, objtype,
                                                protocol_version))

        dynamic_ddl = []
        if sequences:
            seq_ids = ', '.join(
                pg_common.quote_literal(str(seq_id))
                for seq_id in sequences
            )
            dynamic_ddl.append(
                f'SELECT edgedb._dump_sequences(ARRAY[{seq_ids}]::uuid[])'
            )

        return DumpDescriptor(
            schema_ddl='\n'.join([sys_config_ddl, schema_ddl, user_config_ddl]),
            schema_dynamic_ddl=tuple(dynamic_ddl),
            schema_ids=ids,
            blocks=descriptors,
        )

    def describe_database_restore(
        self,
        user_schema_pickle: bytes,
        global_schema_pickle: bytes,
        dump_server_ver_str: str,
        dump_catalog_version: Optional[int],
        schema_ddl: bytes,
        schema_ids: List[Tuple[str, str, bytes]],
        blocks: List[Tuple[bytes, bytes]],  # type_id, typespec
        protocol_version: defines.ProtocolVersion,
    ) -> RestoreDescriptor:
        schema_object_ids = {
            (
                s_name.name_from_string(name),
                qltype if qltype else None
            ): uuidgen.from_bytes(objid)
            for name, qltype, objid in schema_ids
        }

        dump_server_ver = verutils.parse_version(dump_server_ver_str)

        # catalog_version didn't exist until late in the 3.0 cycle,
        # but we can just treat that as being version 0
        dump_catalog_version = dump_catalog_version or 0

        state = dbstate.CompilerConnectionState(
            user_schema=pickle.loads(user_schema_pickle),
            global_schema=pickle.loads(global_schema_pickle),
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            database_config=EMPTY_MAP,
            system_config=EMPTY_MAP,
            cached_reflection=EMPTY_MAP,
        )

        ctx = CompileContext(
            compiler_state=self.state,
            state=state,
            output_format=enums.OutputFormat.BINARY,
            expected_cardinality_one=False,
            compat_ver=dump_server_ver,
            schema_object_ids=schema_object_ids,
            log_ddl_as_migrations=False,
            protocol_version=protocol_version,
            dump_restore_mode=True,
        )

        ctx.state.start_tx()

        dump_with_extraneous_computables = (
            dump_server_ver < (1, 0, verutils.VersionStage.ALPHA, 8)
        )

        dump_with_ptr_item_id = dump_with_extraneous_computables

        allow_dml_in_functions = (
            dump_server_ver < (1, 0, verutils.VersionStage.BETA, 1)
        )

        # This change came late in the 3.0 dev cycle, and with it we
        # switched to using catalog versions for this, so that nightly
        # dumps might work.
        dump_with_dunder_type = (
            dump_catalog_version < 2023_02_16_00_00
        )

        schema_ddl_text = schema_ddl.decode('utf-8')

        if allow_dml_in_functions:
            schema_ddl_text = (
                'CONFIGURE CURRENT DATABASE '
                'SET allow_dml_in_functions := true;\n'
                + schema_ddl_text
            )

        ddl_source = edgeql.Source.from_string(schema_ddl_text)

        # The state serializer generated below is somehow inappropriate,
        # so it's simply ignored here and the I/O process will do it on its own
        units = compile(ctx=ctx, source=ddl_source).units

        schema = ctx.state.current_tx().get_schema(
            ctx.compiler_state.std_schema)

        if allow_dml_in_functions:
            # Check if any functions actually contained DML.
            for func in schema.get_objects(
                type=s_func.Function,
                exclude_stdlib=True,
            ):
                if func.get_has_dml(schema):
                    break
            else:
                ddl_source = edgeql.Source.from_string(
                    'CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;',
                )
                units += compile(ctx=ctx, source=ddl_source).units

        restore_blocks = []
        tables = []
        repopulate_units = []
        for schema_object_id_bytes, typedesc in blocks:
            schema_object_id = uuidgen.from_bytes(schema_object_id_bytes)
            obj = schema.get_by_id(schema_object_id)
            desc = sertypes.parse(typedesc, protocol_version)
            elided_col_set = set()
            mending_desc: list[Optional[DataMendingDescriptor]] = []

            if isinstance(obj, s_props.Property):
                assert isinstance(desc, sertypes.NamedTupleDesc)
                desc_ptrs = list(desc.fields.keys())
                cols = {
                    'source': 'source',
                    'target': 'target',
                }

                mending_desc.append(None)
                mending_desc.append(_get_ptr_mending_desc(schema, obj))

                if dump_with_ptr_item_id:
                    elided_col_set.add('ptr_item_id')
                    mending_desc.append(None)

            elif isinstance(obj, s_links.Link):
                assert isinstance(desc, sertypes.NamedTupleDesc)
                desc_ptrs = list(desc.fields.keys())

                cols = {}
                ptrs = dict(obj.get_pointers(schema).items(schema))
                for ptr_name in desc_ptrs:
                    if dump_with_ptr_item_id and ptr_name == 'ptr_item_id':
                        elided_col_set.add(ptr_name)
                        cols[ptr_name] = ptr_name
                        mending_desc.append(None)
                    else:
                        ptr = ptrs[s_name.UnqualName(ptr_name)]
                        if (
                            dump_with_extraneous_computables
                            and ptr.is_pure_computable(schema)
                        ):
                            elided_col_set.add(ptr_name)
                            mending_desc.append(None)

                        if not ptr.is_dumpable(schema):
                            continue

                        stor_info = pg_types.get_pointer_storage_info(
                            ptr,
                            schema=schema,
                            source=obj,
                            link_bias=True,
                        )

                        cols[ptr_name] = stor_info.column_name
                        mending_desc.append(
                            _get_ptr_mending_desc(schema, ptr))

            elif isinstance(obj, s_objtypes.ObjectType):
                assert isinstance(desc, sertypes.ShapeDesc)
                desc_ptrs = list(desc.fields.keys())

                cols = {}
                ptrs = dict(obj.get_pointers(schema).items(schema))
                for ptr_name in desc_ptrs:
                    ptr = ptrs[s_name.UnqualName(ptr_name)]
                    if (
                        dump_with_extraneous_computables
                        and ptr.is_pure_computable(schema)
                    ) or (
                        dump_with_dunder_type
                        and ptr_name == '__type__'
                    ):
                        elided_col_set.add(ptr_name)
                        mending_desc.append(None)

                    if not ptr.is_dumpable(schema):
                        continue

                    stor_info = pg_types.get_pointer_storage_info(
                        ptr,
                        schema=schema,
                        source=obj,
                    )

                    if stor_info.table_type == 'ObjectType':
                        ptr_name = ptr.get_shortname(schema).name
                        cols[ptr_name] = stor_info.column_name
                        mending_desc.append(
                            _get_ptr_mending_desc(schema, ptr))

                cmd = pg_delta.get_reindex_sql(obj, schema)
                if cmd:
                    repopulate_units.append(cmd)

            else:
                raise AssertionError(
                    f'unexpected object type in restore '
                    f'type descriptor: {obj!r}'
                )

            _check_dump_layout(
                frozenset(desc_ptrs),
                frozenset(cols),
                elided_col_set,
                label=obj.get_verbosename(schema, with_parent=True),
            )

            table_name = pg_common.get_backend_name(
                schema, obj, catenate=True)

            elided_cols = tuple(i for i, pn in enumerate(desc_ptrs)
                                if pn in elided_col_set)

            col_list = (
                pg_common.quote_ident(cols[pn])
                for pn in desc_ptrs
                if pn not in elided_col_set
            )

            stmt = (
                f'COPY {table_name} '
                f'({", ".join(col_list)})'
                f'FROM STDIN WITH BINARY'
            ).encode()

            restore_blocks.append(
                RestoreBlockDescriptor(
                    schema_object_id=schema_object_id,
                    sql_copy_stmt=stmt,
                    compat_elided_cols=elided_cols,
                    data_mending_desc=tuple(mending_desc),
                )
            )

            tables.append(table_name)

        return RestoreDescriptor(
            units=units,
            blocks=restore_blocks,
            tables=tables,
            repopulate_units=repopulate_units,
        )

    def analyze_explain_output(
        self,
        query_asts_pickled: bytes,
        data: list[list[bytes]],
    ) -> bytes:
        return explain.analyze_explain_output(
            query_asts_pickled, data, self.state.std_schema)


def compile_schema_storage_in_delta(
    ctx: CompileContext,
    delta: s_delta.Command,
    block: pg_dbops.SQLBlock,
    context: Optional[s_delta.CommandContext] = None,
) -> None:

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    funcblock = block.add_block()
    cmdblock = block.add_block()

    meta_blocks: List[Tuple[str, Dict[str, Any]]] = []

    # Use a provided context if one was passed in, which lets us
    # used the cached values for resolved properties. (Which is
    # important, since if there were renames we won't necessarily
    # be able to resolve them just using the new schema.)
    if not context:
        context = s_delta.CommandContext()
    else:
        context.renames.clear()
        context.early_renames.clear()

    s_refl.generate_metadata_write_edgeql(
        delta,
        classlayout=ctx.compiler_state.schema_class_layout,
        schema=schema,
        context=context,
        blocks=meta_blocks,
        internal_schema_mode=ctx.internal_schema_mode,
        stdmode=ctx.bootstrap_mode,
    )

    cache = current_tx.get_cached_reflection()

    with cache.mutate() as cache_mm:
        for eql, args in meta_blocks:
            eql_hash = hashlib.sha1(eql.encode()).hexdigest()
            fname = ('edgedb', f'__rh_{eql_hash}')

            if eql_hash in cache_mm:
                argnames = cache_mm[eql_hash]
            else:
                sql, argmap = _compile_schema_storage_stmt(ctx, eql)
                argnames = tuple(arg.name for arg in argmap)

                func = pg_dbops.Function(
                    name=fname,
                    args=[(argname, 'json') for argname in argnames],
                    returns='json',
                    text=sql,
                )

                # We drop first instead of using or_replace, in case
                # something about the arguments changed.
                df = pg_dbops.DropFunction(
                    name=func.name, args=func.args or (), if_exists=True
                )
                df.generate(funcblock)

                cf = pg_dbops.CreateFunction(func)
                cf.generate(funcblock)

                cache_mm[eql_hash] = argnames

            argvals = []
            for argname in argnames:
                argvals.append(pg_common.quote_literal(args[argname]))

            cmdblock.add_command(textwrap.dedent(f'''\
                PERFORM {pg_common.qname(*fname)}({", ".join(argvals)});
            '''))

    ctx.state.current_tx().update_cached_reflection(cache_mm.finish())


def _compile_schema_storage_stmt(
    ctx: CompileContext,
    eql: str,
) -> tuple[str, Sequence[dbstate.Param]]:

    schema = ctx.state.current_tx().get_schema(ctx.compiler_state.std_schema)

    try:
        # Switch to the shadow introspection/reflection schema.
        ctx.state.current_tx().update_schema(
            # Trick dbstate to set the effective schema
            # to refl_schema.
            s_schema.ChainedSchema(
                ctx.compiler_state.std_schema,
                ctx.compiler_state.refl_schema,
                s_schema.EMPTY_SCHEMA
            )
        )

        newctx = CompileContext(
            compiler_state=ctx.compiler_state,
            state=ctx.state,
            json_parameters=True,
            schema_reflection_mode=True,
            output_format=enums.OutputFormat.JSON,
            expected_cardinality_one=False,
            bootstrap_mode=ctx.bootstrap_mode,
            protocol_version=ctx.protocol_version,
            backend_runtime_params=ctx.backend_runtime_params,
        )

        source = edgeql.Source.from_string(eql)
        unit_group = compile(ctx=newctx, source=source)

        sql_stmts = []
        for u in unit_group:
            for stmt in u.sql:
                stmt = stmt.strip()
                if not stmt.endswith(b';'):
                    stmt += b';'

                sql_stmts.append(stmt)

        if len(sql_stmts) > 1:
            raise errors.InternalServerError(
                'compilation of schema update statement'
                ' yielded more than one SQL statement'
            )

        sql = sql_stmts[0].strip(b';').decode()
        argmap: Optional[Sequence[dbstate.Param]] = unit_group[0].in_type_args
        if argmap is None:
            argmap = ()

        return sql, argmap

    finally:
        # Restore the regular schema.
        ctx.state.current_tx().update_schema(schema)


def _get_schema_version(user_schema: s_schema.Schema) -> uuid.UUID:
    ver = user_schema.get_global(s_ver.SchemaVersion, "__schema_version__")
    return ver.get_version(user_schema)


def _compile_ql_script(
    ctx: CompileContext,
    eql: str,
) -> str:

    source = edgeql.Source.from_string(eql)
    unit_group = compile(ctx=ctx, source=source)

    sql_stmts = []
    for u in unit_group:
        for stmt in u.sql:
            stmt = stmt.strip()
            if not stmt.endswith(b';'):
                stmt += b';'

            sql_stmts.append(stmt)

    return b'\n'.join(sql_stmts).decode()


def _get_compile_options(
    ctx: CompileContext, *, is_explain: bool = False,
) -> qlcompiler.CompilerOptions:
    can_have_implicit_fields = (
        ctx.output_format is enums.OutputFormat.BINARY)

    return qlcompiler.CompilerOptions(
        modaliases=ctx.state.current_tx().get_modaliases(),
        implicit_tid_in_shapes=(
            can_have_implicit_fields and ctx.inline_typeids
        ),
        implicit_tname_in_shapes=(
            can_have_implicit_fields and ctx.inline_typenames
        ),
        implicit_id_in_shapes=(
            can_have_implicit_fields and ctx.inline_objectids
        ),
        json_parameters=ctx.json_parameters,
        implicit_limit=ctx.implicit_limit,
        bootstrap_mode=ctx.bootstrap_mode,
        apply_query_rewrites=(
            not ctx.bootstrap_mode
            and not ctx.schema_reflection_mode
            and not bool(
                _get_config_val(ctx, '__internal_no_apply_query_rewrites'))
        ),
        apply_user_access_policies=_get_config_val(
            ctx, 'apply_access_policies'),
        allow_user_specified_id=_get_config_val(
            ctx, 'allow_user_specified_id') or ctx.schema_reflection_mode,
        expand_inhviews=(
            debug.flags.edgeql_expand_inhviews
            and not ctx.bootstrap_mode
            and not ctx.schema_reflection_mode
        ) or is_explain,
        testmode=_get_config_val(ctx, '__internal_testmode'),
        schema_reflection_mode=(
            ctx.schema_reflection_mode
            or _get_config_val(ctx, '__internal_query_reflschema')
        )
    )


# Types and default values for EXPLAIN parameters
EXPLAIN_PARAMS = dict(
    buffers=('std::bool', False),
    execute=('std::bool', True),
)


def _compile_ql_explain(
    ctx: CompileContext,
    ql: qlast.ExplainStmt,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
) -> dbstate.BaseQuery:
    args = {k: v for k, (_, v) in EXPLAIN_PARAMS.items()}

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    # Evaluate and typecheck arguments
    if ql.args:
        for el in ql.args.elements:
            name = el.name.name
            if name not in EXPLAIN_PARAMS:
                raise errors.QueryError(
                    f"unknown ANALYZE argument '{name}'",
                    context=el.context,
                )
            arg_ir = qlcompiler.compile_ast_to_ir(
                el.val,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=current_tx.get_modaliases(),
                ),
            )
            exp_typ = schema.get(EXPLAIN_PARAMS[name][0], type=s_types.Type)
            if not arg_ir.stype.issubclass(schema, exp_typ):
                raise errors.QueryError(
                    f"incorrect type for ANALYZE argument '{name}': "
                    f"expected '{exp_typ.get_name(schema)}', "
                    f"got '{arg_ir.stype.get_name(schema)}'",
                    context=el.context,
                )

            args[name] = ireval.evaluate_to_python_val(arg_ir.expr, schema)

    analyze = 'ANALYZE true, ' if args['execute'] else ''
    buffers = 'BUFFERS, ' if args['buffers'] else ''
    exp_command = f'EXPLAIN ({analyze}{buffers}FORMAT JSON, VERBOSE true)'

    ctx = dataclasses.replace(
        ctx,
        inline_typeids=False,
        inline_typenames=False,
        implicit_limit=0,
        output_format=enums.OutputFormat.BINARY,
    )

    config_vals = _get_compilation_config_vals(ctx)
    modaliases = ctx.state.current_tx().get_modaliases()
    explain_data = (config_vals, args, modaliases)

    query = _compile_ql_query(
        ctx, ql.query, script_info=script_info,
        explain_data=explain_data, cacheable=False)
    if isinstance(query, dbstate.NullQuery):
        raise errors.QueryError(
            f"cannot ANALYZE inside of a migration",
            context=ql.context,
        )

    assert len(query.sql) == 1, query.sql

    out_type_data, out_type_id = sertypes.describe(
        schema,
        schema.get("std::str", type=s_types.Type),
        protocol_version=ctx.protocol_version,
    )

    sql_bytes = exp_command.encode('utf-8') + query.sql[0]
    sql_hash = _hash_sql(
        sql_bytes,
        mode=str(ctx.output_format).encode(),
        intype=query.in_type_id,
        outtype=out_type_id.bytes)

    return dataclasses.replace(
        query,
        is_explain=True,
        append_rollback=args['execute'],
        cacheable=False,
        sql=(sql_bytes,),
        sql_hash=sql_hash,
        cardinality=enums.Cardinality.ONE,
        out_type_data=out_type_data,
        out_type_id=out_type_id.bytes,
    )


def _compile_ql_administer(
    ctx: CompileContext,
    ql: qlast.AdministerStmt,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
) -> dbstate.BaseQuery:
    if ql.expr.func == 'statistics_update':
        if not _get_config_val(ctx, '__internal_testmode'):
            raise errors.QueryError(
                'statistics_update() can only be executed in test mode',
                context=ql.context)

        if ql.expr.args or ql.expr.kwargs:
            raise errors.QueryError(
                'statistics_update() does not take arguments',
                context=ql.expr.context,
            )

        return dbstate.MaintenanceQuery(sql=(b'ANALYZE',))
    elif ql.expr.func == 'schema_repair':
        return ddl.administer_repair_schema(ctx, ql)
    elif ql.expr.func == 'reindex':
        return ddl.administer_reindex(ctx, ql)
    elif ql.expr.func == 'vacuum':
        return ddl.administer_vacuum(ctx, ql)
    else:
        raise errors.QueryError(
            'Unknown ADMINISTER function',
            context=ql.expr.context,
        )


def _compile_ql_query(
    ctx: CompileContext,
    ql: qlast.Query | qlast.Command,
    *,
    script_info: Optional[irast.ScriptInfo] = None,
    cacheable: bool = True,
    migration_block_query: bool = False,
    explain_data: object = None,
) -> dbstate.Query | dbstate.NullQuery:

    is_explain = explain_data is not None
    current_tx = ctx.state.current_tx()

    base_schema = (
        ctx.compiler_state.std_schema
        if not _get_config_val(ctx, '__internal_query_reflschema')
        else ctx.compiler_state.refl_schema
    )
    schema = current_tx.get_schema(base_schema)

    options = _get_compile_options(ctx, is_explain=is_explain)
    ir = qlcompiler.compile_ast_to_ir(
        ql,
        schema=schema,
        script_info=script_info,
        options=options,
    )

    result_cardinality = enums.cardinality_from_ir_value(ir.cardinality)

    # This low-hanging-fruit is temporary; persistent cache should cover all
    # cacheable cases properly in future changes.
    use_persistent_cache = (
        ctx.persistent_cache
        and cacheable
        and not ctx.bootstrap_mode
        and script_info is None
        and ctx.cache_key is not None
    )

    sql_res = pg_compiler.compile_ir_to_sql_tree(
        ir,
        expected_cardinality_one=ctx.expected_cardinality_one,
        output_format=_convert_format(ctx.output_format),
        backend_runtime_params=ctx.backend_runtime_params,
        expand_inhviews=options.expand_inhviews,
        detach_params=bool(use_persistent_cache and debug.flags.func_cache),
    )

    pg_debug.dump_ast_and_query(sql_res.ast, ir)

    if use_persistent_cache:
        if debug.flags.func_cache:
            cache_sql, sql_ast = _build_cache_function(ctx, ir, sql_res)
        else:
            sql_ast = sql_res.ast
            cache_sql = (b"", b"")
    else:
        sql_ast = sql_res.ast
        cache_sql = None

    if (
        (mstate := current_tx.get_migration_state())
        and not migration_block_query
    ):
        mstate = mstate._replace(
            accepted_cmds=mstate.accepted_cmds + (ql,),
        )
        current_tx.update_migration_state(mstate)

        return dbstate.NullQuery()

    sql_text = pg_codegen.generate_source(sql_ast)
    sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

    globals = None
    if ir.globals:
        globals = [
            (str(glob.global_name), glob.has_present_arg)
            for glob in ir.globals
        ]

    out_type_id: uuid.UUID
    if ctx.output_format is enums.OutputFormat.NONE:
        out_type_id = sertypes.NULL_TYPE_ID
        out_type_data = sertypes.NULL_TYPE_DESC
        result_cardinality = enums.Cardinality.NO_RESULT
    elif ctx.output_format is enums.OutputFormat.BINARY:
        out_type_data, out_type_id = sertypes.describe(
            ir.schema, ir.stype,
            ir.view_shapes, ir.view_shapes_metadata,
            inline_typenames=ctx.inline_typenames,
            protocol_version=ctx.protocol_version)
    else:
        out_type_data, out_type_id = sertypes.describe(
            ir.schema,
            ir.schema.get("std::str", type=s_types.Type),
            protocol_version=ctx.protocol_version,
        )

    in_type_args, in_type_data, in_type_id = describe_params(
        ctx, ir, sql_res.argmap, script_info
    )

    sql_hash = _hash_sql(
        sql_bytes,
        mode=str(ctx.output_format).encode(),
        intype=in_type_id.bytes,
        outtype=out_type_id.bytes)

    if is_explain:
        if isinstance(ir.schema, s_schema.ChainedSchema):
            # Strip the std schema out
            ir.schema = s_schema.ChainedSchema(
                top_schema=ir.schema._top_schema,
                global_schema=ir.schema._global_schema,
                base_schema=s_schema.EMPTY_SCHEMA,
            )
        query_asts = pickle.dumps((ql, ir, sql_res.ast, explain_data))
    else:
        query_asts = None

    return dbstate.Query(
        sql=(sql_bytes,),
        sql_hash=sql_hash,
        cache_sql=cache_sql,
        cardinality=result_cardinality,
        globals=globals,
        in_type_id=in_type_id.bytes,
        in_type_data=in_type_data,
        in_type_args=in_type_args,
        out_type_id=out_type_id.bytes,
        out_type_data=out_type_data,
        cacheable=cacheable,
        has_dml=bool(ir.dml_exprs),
        query_asts=query_asts,
    )


def _build_cache_function(
    ctx: CompileContext,
    ir: irast.Statement,
    sql_res: pg_compiler.CompileResult,
) -> tuple[tuple[bytes, bytes], pgast.Base]:
    sql_ast = sql_res.ast
    assert ctx.cache_key is not None
    key = ctx.cache_key.hex
    returns_record = False
    set_returning = True
    return_type: tuple[str, ...] = ("unknown",)
    match ctx.output_format:
        case enums.OutputFormat.NONE:
            return_type = ("void",)

        case enums.OutputFormat.BINARY:
            if ir.stype.is_object_type():
                return_type = ("record",)
                returns_record = True
            else:
                return_type = pg_types.pg_type_from_ir_typeref(
                    ir.expr.typeref.base_type or ir.expr.typeref
                )

        case enums.OutputFormat.JSON:
            return_type = ("json",)

        case enums.OutputFormat.JSON_ELEMENTS:
            return_type = ("json",)
    if returns_record:
        assert isinstance(sql_ast, pgast.ReturningQuery)
        sql_ast.target_list.append(
            pgast.ResTarget(
                name="sentinel",
                val=pgast.BooleanConstant(val=True),
            ),
        )
    func = pg_dbops.Function(
        name=("edgedb", f"__qh_{key}"),
        args=[(None, arg) for arg in sql_res.detached_params or []],
        returns=return_type,
        set_returning=set_returning,
        text=pg_codegen.generate_source(sql_ast),
    )
    if not ir.dml_exprs:
        func.volatility = "stable"
    cf = pg_dbops.SQLBlock()
    pg_dbops.CreateFunction(func).generate(cf)
    df = pg_dbops.SQLBlock()
    pg_dbops.DropFunction(
        name=func.name, args=func.args or (), if_exists=True
    ).generate(df)
    func_call = pgast.FuncCall(
        name=("edgedb", f"__qh_{key}"),
        args=[
            pgast.TypeCast(
                arg=pgast.ParamRef(number=i),
                type_name=pgast.TypeName(name=arg),
            )
            for i, arg in enumerate(sql_res.detached_params or [], 1)
        ],
        coldeflist=[],
    )
    if returns_record:
        func_call.coldeflist.extend(
            [
                pgast.ColumnDef(
                    name="result",
                    typename=pgast.TypeName(name=("record",)),
                ),
                pgast.ColumnDef(
                    name="sentinel",
                    typename=pgast.TypeName(name=("bool",)),
                ),
            ]
        )
        sql_ast = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(val=pgast.ColumnRef(name=("result",))),
            ],
            from_clause=[
                pgast.RangeFunction(
                    functions=[func_call],
                    is_rowsfrom=True,
                ),
            ],
        )
    else:
        sql_ast = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=func_call)],
        )
    cache_sql = (
        cf.to_string().encode(defines.EDGEDB_ENCODING),
        df.to_string().encode(defines.EDGEDB_ENCODING),
    )
    return cache_sql, sql_ast


def describe_params(
    ctx: CompileContext,
    ir: irast.Statement | irast.ConfigCommand,
    argmap: Dict[str, pgast.Param],
    script_info: Optional[irast.ScriptInfo],
) -> Tuple[Optional[list[dbstate.Param]], bytes, uuid.UUID]:
    in_type_args = None
    params: list[tuple[str, s_types.Type, bool]] = []
    assert ir.schema
    if ir.params:
        params, in_type_args = _extract_params(
            ir.params,
            argmap=argmap,
            script_info=script_info,
            schema=ir.schema,
            ctx=ctx,
        )

    in_type_data, in_type_id = sertypes.describe_params(
        schema=ir.schema,
        params=params,
        protocol_version=ctx.protocol_version,
    )
    return in_type_args, in_type_data, in_type_id


def _compile_ql_transaction(
    ctx: CompileContext, ql: qlast.Transaction
) -> dbstate.TxControlQuery:

    cacheable = True

    modaliases = None
    final_user_schema: Optional[s_schema.Schema] = None
    final_cached_reflection = None
    final_global_schema: Optional[s_schema.Schema] = None
    sp_name = None
    sp_id = None

    if ctx.expect_rollback and not isinstance(
        ql, (qlast.RollbackTransaction, qlast.RollbackToSavepoint)
    ):
        raise errors.TransactionError(
            'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
        )

    if isinstance(ql, qlast.StartTransaction):
        ctx._assert_not_in_migration_block(ql)

        ctx.state.start_tx()

        sqls = 'START TRANSACTION'
        if ql.access is not None:
            sqls += f' {ql.access.value}'
        if ql.deferrable is not None:
            sqls += f' {ql.deferrable.value}'
        sqls += ';'
        sql = (sqls.encode(),)

        action = dbstate.TxAction.START
        cacheable = False

    elif isinstance(ql, qlast.CommitTransaction):
        ctx._assert_not_in_migration_block(ql)

        cur_tx = ctx.state.current_tx()
        final_user_schema = cur_tx.get_user_schema_if_updated()
        final_cached_reflection = cur_tx.get_cached_reflection_if_updated()
        final_global_schema = cur_tx.get_global_schema_if_updated()

        new_state = ctx.state.commit_tx()
        modaliases = new_state.modaliases

        sql = (b'COMMIT',)
        cacheable = False
        action = dbstate.TxAction.COMMIT

    elif isinstance(ql, qlast.RollbackTransaction):
        new_state = ctx.state.rollback_tx()
        modaliases = new_state.modaliases

        sql = (b'ROLLBACK',)
        cacheable = False
        action = dbstate.TxAction.ROLLBACK

    elif isinstance(ql, qlast.DeclareSavepoint):
        tx = ctx.state.current_tx()
        sp_id = tx.declare_savepoint(ql.name)

        pgname = pg_common.quote_ident(ql.name)
        sql = (f'SAVEPOINT {pgname}'.encode(),)

        cacheable = False
        action = dbstate.TxAction.DECLARE_SAVEPOINT

        sp_name = ql.name

    elif isinstance(ql, qlast.ReleaseSavepoint):
        ctx.state.current_tx().release_savepoint(ql.name)
        pgname = pg_common.quote_ident(ql.name)
        sql = (f'RELEASE SAVEPOINT {pgname}'.encode(),)
        action = dbstate.TxAction.RELEASE_SAVEPOINT

    elif isinstance(ql, qlast.RollbackToSavepoint):
        tx = ctx.state.current_tx()
        new_state = tx.rollback_to_savepoint(ql.name)
        modaliases = new_state.modaliases

        pgname = pg_common.quote_ident(ql.name)
        sql = (f'ROLLBACK TO SAVEPOINT {pgname};'.encode(),)
        cacheable = False
        action = dbstate.TxAction.ROLLBACK_TO_SAVEPOINT
        sp_name = ql.name

    else:  # pragma: no cover
        raise ValueError(f'expected a transaction AST node, got {ql!r}')

    return dbstate.TxControlQuery(
        sql=sql,
        action=action,
        cacheable=cacheable,
        modaliases=modaliases,
        user_schema=final_user_schema,
        cached_reflection=final_cached_reflection,
        global_schema=final_global_schema,
        sp_name=sp_name,
        sp_id=sp_id,
    )


def _compile_ql_sess_state(
    ctx: CompileContext, ql: qlast.SessionCommand
) -> dbstate.SessionStateQuery:
    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    aliases = ctx.state.current_tx().get_modaliases()

    if isinstance(ql, qlast.SessionSetAliasDecl):
        try:
            schema.get_global(s_mod.Module, ql.decl.module)
        except errors.InvalidReferenceError:
            raise errors.UnknownModuleError(
                f'module {ql.decl.module!r} does not exist'
            ) from None

        aliases = aliases.set(ql.decl.alias, ql.decl.module)

    elif isinstance(ql, qlast.SessionResetModule):
        aliases = aliases.set(None, defines.DEFAULT_MODULE_ALIAS)

    elif isinstance(ql, qlast.SessionResetAllAliases):
        aliases = DEFAULT_MODULE_ALIASES_MAP

    elif isinstance(ql, qlast.SessionResetAliasDecl):
        aliases = aliases.delete(ql.alias)

    else:  # pragma: no cover
        raise errors.InternalServerError(
            f'unsupported SET command type {type(ql)!r}')

    ctx.state.current_tx().update_modaliases(aliases)

    return dbstate.SessionStateQuery(
        sql=(),
    )


def _get_config_spec(
    ctx: CompileContext, config_op: config.Operation
) -> config.Spec:
    config_spec = ctx.compiler_state.config_spec
    if config_op.setting_name not in config_spec:
        # We don't typically bother tracking the user config spec in
        # the compiler workers (to avoid needing to bother with
        # transmitting, caching, or computing it). If we hit a config
        # op that needs it, load the spec.
        config_spec = config.ChainedSpec(
            config_spec,
            config.load_ext_spec_from_schema(
                ctx.state.current_tx().get_user_schema(),
                ctx.compiler_state.std_schema,
            ),
        )
    return config_spec


def _inject_config_cache_clear(sql_ast: pgast.Base) -> pgast.Base:
    """Inject a call to clear the config cache into a config op.

    The trickiness here is that we can't just do the delete in a
    statement before the config op, since RESET config ops query the
    views and so might populate the cache, and we can't do it in a
    statement directly after (unless we rework the server), since then
    the query won't return anything.

    So we instead fiddle around with the query to inject a call.
    """
    assert isinstance(sql_ast, pgast.Query)
    ctes = sql_ast.ctes or []
    sql_ast.ctes = None

    ctes.append(pgast.CommonTableExpr(
        name="_conv_rel",
        query=sql_ast,
    ))
    clear_qry = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                name="_dummy",
                val=pgast.FuncCall(
                    name=('edgedb', '_clear_sys_config_cache'),
                    args=[],
                ),
            ),
        ],
    )
    ctes.append(pgast.CommonTableExpr(
        name="_clear_cache",
        query=clear_qry,
        materialized=True,
    ))
    force_qry = pgast.UpdateStmt(
        targets=[pgast.UpdateTarget(
            name='flag', val=pgast.BooleanConstant(val=True)
        )],
        relation=pgast.RelRangeVar(relation=pgast.Relation(
            schemaname='edgedb', name='_dml_dummy')),
        where_clause=pgast.Expr(
            name="=",
            lexpr=pgast.ColumnRef(name=["id"]),
            rexpr=pgast.SelectStmt(
                from_clause=[pgast.RelRangeVar(relation=ctes[-1])],
                target_list=[
                    pgast.ResTarget(
                        val=pgast.FuncCall(
                            name=('count',), args=[pgast.Star()]),
                    )
                ],
            ),
        )
    )

    if (
        not isinstance(sql_ast, pgast.DMLQuery)
        or sql_ast.returning_list
    ):
        ctes.append(pgast.CommonTableExpr(
            name="_force_clear",
            query=force_qry,
            materialized=True,
        ))
        sql_ast = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(val=pgast.ColumnRef(
                    name=["_conv_rel", pgast.Star()])),
            ],
            ctes=ctes,
            from_clause=[
                pgast.RelRangeVar(relation=ctes[-3]),
            ],
        )
    else:
        sql_ast = force_qry
        force_qry.ctes = ctes

    return sql_ast


def _compile_ql_config_op(
    ctx: CompileContext, ql: qlast.ConfigOp
) -> dbstate.SessionStateQuery:

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    modaliases = current_tx.get_modaliases()
    session_config = current_tx.get_session_config()
    database_config = current_tx.get_database_config()

    if ql.scope is not qltypes.ConfigScope.SESSION:
        ctx._assert_not_in_migration_block(ql)

    if (
        ql.scope is qltypes.ConfigScope.INSTANCE
        and not current_tx.is_implicit()
    ):
        raise errors.QueryError(
            'CONFIGURE INSTANCE cannot be executed in a transaction block')

    ir = qlcompiler.compile_ast_to_ir(
        ql,
        schema=schema,
        options=qlcompiler.CompilerOptions(
            modaliases=modaliases,
            in_server_config_op=True,
            dump_restore_mode=ctx.dump_restore_mode,
        ),
    )

    globals = None
    if ir.globals:
        globals = [
            (str(glob.global_name), glob.has_present_arg)
            for glob in ir.globals
        ]

    if isinstance(ir, irast.Statement):
        cfg_ir = ir.expr.expr
    else:
        cfg_ir = ir

    is_backend_setting = bool(getattr(cfg_ir, 'backend_setting', None))
    requires_restart = bool(getattr(cfg_ir, 'requires_restart', False))
    is_system_config = bool(getattr(cfg_ir, 'is_system_config', False))

    sql_res = pg_compiler.compile_ir_to_sql_tree(
        ir,
        backend_runtime_params=ctx.backend_runtime_params,
    )

    sql_ast = sql_res.ast
    if not ctx.bootstrap_mode and ql.scope in (
        qltypes.ConfigScope.DATABASE,
        qltypes.ConfigScope.SESSION,
    ):
        sql_ast = _inject_config_cache_clear(sql_ast)

    pretty = bool(
        debug.flags.edgeql_compile or debug.flags.edgeql_compile_sql_text)
    sql_text = pg_codegen.generate_source(
        sql_ast,
        pretty=pretty,
    )
    if pretty:
        debug.dump_code(sql_text, lexer='sql')

    sql: tuple[bytes, ...] = (
        sql_text.encode(),
    )

    in_type_args, in_type_data, in_type_id = describe_params(
        ctx, ir, sql_res.argmap, None
    )

    if ql.scope is qltypes.ConfigScope.SESSION:
        config_op = ireval.evaluate_to_config_op(ir, schema=schema)

        session_config = config_op.apply(
            _get_config_spec(ctx, config_op),
            session_config,
        )
        current_tx.update_session_config(session_config)

    elif ql.scope is qltypes.ConfigScope.DATABASE:
        try:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)
        except ireval.UnsupportedExpressionError:
            # This is a complex config object operation, the
            # op will be produced by the compiler as json.
            config_op = None
        else:
            database_config = config_op.apply(
                _get_config_spec(ctx, config_op),
                database_config,
            )
            current_tx.update_database_config(database_config)

    elif ql.scope in (
            qltypes.ConfigScope.INSTANCE, qltypes.ConfigScope.GLOBAL):
        try:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)
        except ireval.UnsupportedExpressionError:
            # This is a complex config object operation, the
            # op will be produced by the compiler as json.
            config_op = None
    else:
        raise AssertionError(f'unexpected configuration scope: {ql.scope}')

    return dbstate.SessionStateQuery(
        sql=sql,
        is_backend_setting=is_backend_setting,
        is_system_config=is_system_config,
        config_scope=ql.scope,
        requires_restart=requires_restart,
        config_op=config_op,
        globals=globals,
        in_type_args=in_type_args,
        in_type_data=in_type_data,
        in_type_id=in_type_id.bytes,
    )


def _compile_dispatch_ql(
    ctx: CompileContext,
    ql: qlast.Base,
    source: Optional[edgeql.Source] = None,
    *,
    in_script: bool=False,
    script_info: Optional[irast.ScriptInfo] = None,
) -> Tuple[dbstate.BaseQuery, enums.Capability]:
    if isinstance(ql, qlast.MigrationCommand):
        query = ddl.compile_dispatch_ql_migration(
            ctx, ql, in_script=in_script
        )
        if isinstance(query, dbstate.MigrationControlQuery):
            capability = enums.Capability.DDL
            if query.tx_action:
                capability |= enums.Capability.TRANSACTION
            return query, capability
        elif isinstance(query, dbstate.DDLQuery):
            return query, enums.Capability.DDL
        else:  # DESCRIBE CURRENT MIGRATION
            return query, enums.Capability(0)

    elif isinstance(ql, qlast.DDLCommand):
        return (
            ddl.compile_and_apply_ddl_stmt(ctx, ql, source=source),
            enums.Capability.DDL,
        )

    elif isinstance(ql, qlast.Transaction):
        return (
            _compile_ql_transaction(ctx, ql),
            enums.Capability.TRANSACTION,
        )

    elif isinstance(ql, qlast.SessionCommand_tuple):
        return (
            _compile_ql_sess_state(ctx, ql),
            enums.Capability.SESSION_CONFIG,
        )

    elif isinstance(ql, qlast.ConfigOp):
        if ql.scope is qltypes.ConfigScope.SESSION:
            capability = enums.Capability.SESSION_CONFIG
        elif ql.scope is qltypes.ConfigScope.GLOBAL:
            # We want the notebook protocol to be able to SET
            # GLOBAL but not CONFIGURE SESSION, but they are
            # merged in the capabilities header. Splitting them
            # out introduces compatability headaches, so for now
            # we keep them merged and hack around it for the notebook.
            if ctx.notebook:
                capability = enums.Capability(0)
            else:
                capability = enums.Capability.SESSION_CONFIG
        else:
            capability = enums.Capability.PERSISTENT_CONFIG
        return (
            _compile_ql_config_op(ctx, ql),
            capability,
        )

    elif isinstance(ql, qlast.ExplainStmt):
        query = _compile_ql_explain(ctx, ql, script_info=script_info)
        caps = enums.Capability(0)
        if (
            isinstance(query, (dbstate.Query, dbstate.SimpleQuery))
            and query.has_dml
        ):
            caps |= enums.Capability.MODIFICATIONS
        return (query, caps)

    elif isinstance(ql, qlast.AdministerStmt):
        query = _compile_ql_administer(ctx, ql, script_info=script_info)
        caps = enums.Capability(0)
        return (query, caps)

    else:
        assert isinstance(ql, (qlast.Query, qlast.Command))
        query = _compile_ql_query(ctx, ql, script_info=script_info)
        caps = enums.Capability(0)
        if (
            isinstance(query, (dbstate.Query, dbstate.SimpleQuery))
            and query.has_dml
        ):
            caps |= enums.Capability.MODIFICATIONS
        return (query, caps)


def compile(
    *,
    ctx: CompileContext,
    source: edgeql.Source,
) -> dbstate.QueryUnitGroup:
    current_tx = ctx.state.current_tx()
    if current_tx.get_migration_state() is not None:
        original = edgeql.Source.from_string(source.text())
        ctx = dataclasses.replace(
            ctx,
            source=original,
            implicit_limit=0,
        )
        return _try_compile(ctx=ctx, source=original)

    try:
        return _try_compile(ctx=ctx, source=source)
    except errors.EdgeQLSyntaxError as original_err:
        if isinstance(source, edgeql.NormalizedSource):
            # try non-normalized source
            try:
                original = edgeql.Source.from_string(source.text())
                ctx = dataclasses.replace(ctx, source=original)
                _try_compile(ctx=ctx, source=original)
            except errors.EdgeQLSyntaxError as denormalized_err:
                raise denormalized_err
            except Exception:
                raise original_err
            else:
                raise AssertionError(
                    "Normalized query is broken while original is valid")
        else:
            raise original_err


def _try_compile(
    *,
    ctx: CompileContext,
    source: edgeql.Source,
) -> dbstate.QueryUnitGroup:
    if _get_config_val(ctx, '__internal_testmode'):
        # This is a bad but simple way to emulate a slow compilation for tests.
        # Ideally, we should have a testmode function that is hooked to sleep
        # as `simple_special_case`, or wait for a notification from the test.
        sentinel = "# EDGEDB_TEST_COMPILER_SLEEP = "
        text = source.text()
        if text.startswith(sentinel):
            time.sleep(float(text[len(sentinel):text.index("\n")]))

    default_cardinality = enums.Cardinality.NO_RESULT
    statements = edgeql.parse_block(source)
    statements_len = len(statements)

    if not len(statements):  # pragma: no cover
        raise errors.ProtocolError('nothing to compile')

    rv = dbstate.QueryUnitGroup()

    is_script = statements_len > 1
    script_info = None
    if is_script:
        if ctx.expect_rollback:
            # We are in a failed transaction expecting a rollback, while a
            # script cannot be a rollback
            raise errors.TransactionError(
                'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
            )

        script_info = qlcompiler.preprocess_script(
            statements,
            schema=ctx.state.current_tx().get_schema(
                ctx.compiler_state.std_schema),
            options=_get_compile_options(ctx)
        )
        non_trailing_ctx = dataclasses.replace(
            ctx, output_format=enums.OutputFormat.NONE)

    final_user_schema: Optional[s_schema.Schema] = None

    for i, stmt in enumerate(statements):
        is_trailing_stmt = i == statements_len - 1
        stmt_ctx = ctx if is_trailing_stmt else non_trailing_ctx

        _check_force_database_error(stmt_ctx, stmt)

        # Initialize user_schema_version with the version this query is
        # going to be compiled upon. This can be overwritten later by DDLs.
        try:
            schema_version = _get_schema_version(
                stmt_ctx.state.current_tx().get_user_schema()
            )
        except errors.InvalidReferenceError:
            schema_version = None

        comp, capabilities = _compile_dispatch_ql(
            stmt_ctx,
            stmt,
            source=source if not is_script else None,
            script_info=script_info,
            in_script=is_script,
        )

        unit = dbstate.QueryUnit(
            sql=(),
            status=status.get_status(stmt),
            cardinality=default_cardinality,
            capabilities=capabilities,
            output_format=stmt_ctx.output_format,
            cache_key=ctx.cache_key,
            user_schema_version=schema_version,
        )

        if not comp.is_transactional:
            if is_script:
                raise errors.QueryError(
                    f'cannot execute {status.get_status(stmt).decode()} '
                    f'with other commands in one block',
                    context=stmt.context,
                )

            if not ctx.state.current_tx().is_implicit():
                raise errors.QueryError(
                    f'cannot execute {status.get_status(stmt).decode()} '
                    f'in a transaction',
                    context=stmt.context,
                )

            unit.is_transactional = False

        if isinstance(comp, dbstate.Query):
            unit.sql = comp.sql
            unit.cache_sql = comp.cache_sql
            unit.globals = comp.globals
            unit.in_type_args = comp.in_type_args

            unit.sql_hash = comp.sql_hash

            unit.out_type_data = comp.out_type_data
            unit.out_type_id = comp.out_type_id
            unit.in_type_data = comp.in_type_data
            unit.in_type_id = comp.in_type_id

            unit.cacheable = comp.cacheable

            if comp.is_explain:
                unit.is_explain = True
                unit.query_asts = comp.query_asts

            if comp.append_rollback:
                unit.append_rollback = True

            if is_trailing_stmt:
                unit.cardinality = comp.cardinality

        elif isinstance(comp, dbstate.SimpleQuery):
            unit.sql = comp.sql
            unit.in_type_args = comp.in_type_args

        elif isinstance(comp, dbstate.DDLQuery):
            unit.sql = comp.sql
            unit.create_db = comp.create_db
            unit.drop_db = comp.drop_db
            unit.drop_db_reset_connections = comp.drop_db_reset_connections
            unit.create_db_template = comp.create_db_template
            unit.create_db_mode = comp.create_db_mode
            unit.ddl_stmt_id = comp.ddl_stmt_id
            if not ctx.dump_restore_mode:
                if comp.user_schema is not None:
                    final_user_schema = comp.user_schema
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                    unit.user_schema_version = (
                        _get_schema_version(comp.user_schema)
                    )
                    unit.extensions, unit.ext_config_settings = (
                        _extract_extensions(ctx, comp.user_schema)
                    )
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)
                if comp.global_schema is not None:
                    unit.global_schema = pickle.dumps(comp.global_schema, -1)
                    unit.roles = _extract_roles(comp.global_schema)

            unit.config_ops.extend(comp.config_ops)

        elif isinstance(comp, dbstate.TxControlQuery):
            if is_script:
                raise errors.QueryError(
                    "Explicit transaction control commands cannot be executed "
                    "in an implicit transaction block"
                )
            unit.sql = comp.sql
            unit.cacheable = comp.cacheable

            if not ctx.dump_restore_mode:
                if comp.user_schema is not None:
                    final_user_schema = comp.user_schema
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                    unit.user_schema_version = (
                        _get_schema_version(comp.user_schema)
                    )
                    unit.extensions, unit.ext_config_settings = (
                        _extract_extensions(ctx, comp.user_schema)
                    )
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)
                if comp.global_schema is not None:
                    unit.global_schema = pickle.dumps(comp.global_schema, -1)
                    unit.roles = _extract_roles(comp.global_schema)

            if comp.modaliases is not None:
                unit.modaliases = comp.modaliases

            if comp.action == dbstate.TxAction.START:
                if unit.tx_id is not None:
                    raise errors.InternalServerError(
                        'already in transaction')
                unit.tx_id = ctx.state.current_tx().id
            elif comp.action == dbstate.TxAction.COMMIT:
                unit.tx_commit = True
            elif comp.action == dbstate.TxAction.ROLLBACK:
                unit.tx_rollback = True
            elif comp.action is dbstate.TxAction.ROLLBACK_TO_SAVEPOINT:
                unit.tx_savepoint_rollback = True
                unit.sp_name = comp.sp_name
            elif comp.action is dbstate.TxAction.DECLARE_SAVEPOINT:
                unit.tx_savepoint_declare = True
                unit.sp_name = comp.sp_name
                unit.sp_id = comp.sp_id

        elif isinstance(comp, dbstate.MigrationControlQuery):
            unit.sql = comp.sql
            unit.cacheable = comp.cacheable

            if not ctx.dump_restore_mode:
                if comp.user_schema is not None:
                    final_user_schema = comp.user_schema
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                    unit.user_schema_version = (
                        _get_schema_version(comp.user_schema)
                    )
                    unit.extensions, unit.ext_config_settings = (
                        _extract_extensions(ctx, comp.user_schema)
                    )
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)
            unit.ddl_stmt_id = comp.ddl_stmt_id

            if comp.modaliases is not None:
                unit.modaliases = comp.modaliases

            if comp.tx_action == dbstate.TxAction.START:
                if unit.tx_id is not None:
                    raise errors.InternalServerError(
                        'already in transaction')
                unit.tx_id = ctx.state.current_tx().id
            elif comp.tx_action == dbstate.TxAction.COMMIT:
                unit.tx_commit = True
            elif comp.tx_action == dbstate.TxAction.ROLLBACK:
                unit.tx_rollback = True
            elif comp.action == dbstate.MigrationAction.ABORT:
                unit.tx_abort_migration = True

        elif isinstance(comp, dbstate.SessionStateQuery):
            unit.sql = comp.sql
            unit.globals = comp.globals

            if comp.config_scope is qltypes.ConfigScope.INSTANCE:
                if (not ctx.state.current_tx().is_implicit() or
                        statements_len > 1):
                    raise errors.QueryError(
                        'CONFIGURE INSTANCE cannot be executed in a '
                        'transaction block')

                unit.system_config = True
            elif comp.config_scope is qltypes.ConfigScope.GLOBAL:
                unit.needs_readback = True

            elif comp.config_scope is qltypes.ConfigScope.DATABASE:
                unit.database_config = True
                unit.needs_readback = True

            if comp.is_backend_setting:
                unit.backend_config = True
            if comp.requires_restart:
                unit.config_requires_restart = True
            if comp.is_system_config:
                unit.is_system_config = True

            unit.modaliases = ctx.state.current_tx().get_modaliases()

            if comp.config_op is not None:
                unit.config_ops.append(comp.config_op)

            if comp.in_type_args:
                unit.in_type_args = comp.in_type_args
            if comp.in_type_data:
                unit.in_type_data = comp.in_type_data
            if comp.in_type_id:
                unit.in_type_id = comp.in_type_id

            unit.has_set = True

        elif isinstance(comp, dbstate.MaintenanceQuery):
            unit.sql = comp.sql

        elif isinstance(comp, dbstate.NullQuery):
            pass

        else:  # pragma: no cover
            raise errors.InternalServerError('unknown compile state')

        if unit.in_type_args:
            unit.in_type_args_real_count = sum(
                len(p.sub_params[0]) if p.sub_params else 1
                for p in unit.in_type_args
            )

        rv.append(unit)

    if script_info:
        if ctx.state.current_tx().is_implicit():
            if ctx.state.current_tx().get_migration_state():
                raise errors.QueryError(
                    "Cannot leave an incomplete migration in scripts"
                )
            if ctx.state.current_tx().get_migration_rewrite_state():
                raise errors.QueryError(
                    "Cannot leave an incomplete migration rewrite "
                    "in scripts"
                )

        params, in_type_args = _extract_params(
            list(script_info.params.values()),
            argmap=None, script_info=None, schema=script_info.schema,
            ctx=ctx)

        in_type_data, in_type_id = sertypes.describe_params(
            schema=script_info.schema,
            params=params,
            protocol_version=ctx.protocol_version,
        )
        rv.in_type_id = in_type_id.bytes
        rv.in_type_args = in_type_args
        rv.in_type_data = in_type_data

    if final_user_schema is not None:
        rv.state_serializer = ctx.compiler_state.state_serializer_factory.make(
            final_user_schema,
            ctx.state.current_tx().get_global_schema(),
            ctx.protocol_version,
        )

    # Sanity checks
    for unit in rv:  # pragma: no cover
        na_cardinality = (
            unit.cardinality is enums.Cardinality.NO_RESULT
        )
        if unit.cacheable and (
            unit.config_ops or unit.modaliases or unit.user_schema or
            unit.cached_reflection
        ):
            raise errors.InternalServerError(
                f'QueryUnit {unit!r} is cacheable but has config/aliases')

        if not na_cardinality and (
                len(unit.sql) > 1 or
                unit.tx_commit or
                unit.tx_rollback or
                unit.tx_savepoint_rollback or
                unit.out_type_id is sertypes.NULL_TYPE_ID or
                unit.system_config or
                unit.config_ops or
                unit.modaliases or
                unit.has_set or
                unit.has_ddl or
                not unit.sql_hash):
            raise errors.InternalServerError(
                f'unit has invalid "cardinality": {unit!r}')

    multi_card = rv.cardinality in (
        enums.Cardinality.MANY, enums.Cardinality.AT_LEAST_ONE,
    )
    if multi_card and ctx.expected_cardinality_one:
        raise errors.ResultCardinalityMismatchError(
            f'the query has cardinality {unit.cardinality.name} '
            f'which does not match the expected cardinality ONE')

    return rv


def _extract_params(
    params: List[irast.Param],
    *,
    schema: s_schema.Schema,
    argmap: Optional[Dict[str, pgast.Param]],
    script_info: Optional[irast.ScriptInfo],
    ctx: CompileContext,
) -> Tuple[List[tuple[str, s_types.Type, bool]], List[dbstate.Param]]:
    first_param = next(iter(params)) if params else None
    has_named_params = first_param and not first_param.name.isdecimal()

    if (src := ctx.source) is not None:
        first_extra = src.first_extra()
    else:
        first_extra = None

    all_params = script_info.params.values() if script_info else params
    total_params = len([p for p in all_params if not p.is_sub_param])
    user_params = first_extra if first_extra is not None else total_params

    if script_info is not None:
        outer_mapping = {n: i for i, n in enumerate(script_info.params)}
        # Count however many of *our* arguments are user_params
        user_params = sum(
            outer_mapping[n.name] < user_params for n in params
            if not n.is_sub_param)
    else:
        outer_mapping = None

    oparams: list[Optional[tuple[str, s_obj.Object, bool]]] = (
        [None] * user_params)
    in_type_args: list[Optional[dbstate.Param]] = [None] * user_params
    for idx, param in enumerate(params):
        if param.is_sub_param:
            continue
        if argmap is not None:
            sql_param = argmap[param.name]
            idx = sql_param.logical_index - 1
        if idx >= user_params:
            continue

        if ctx.json_parameters:
            schema_type = schema.get('std::json')
        else:
            schema_type = param.schema_type

        array_tid = None
        if isinstance(schema_type, s_types.Array):
            el_type = schema_type.get_element_type(schema)
            array_tid = el_type.id

        # NB: We'll need to turn this off for script args
        if (
            not script_info
            and not has_named_params
            and str(idx) != param.name
        ):
            raise RuntimeError(
                'positional argument name disagrees '
                'with its actual position')

        oparams[idx] = (
            param.name,
            schema_type,
            param.required,
        )

        if param.sub_params:
            assert not ctx.json_parameters
            array_tids: list[Optional[uuid.UUID]] = []
            for p in param.sub_params.params:
                if isinstance(p.schema_type, s_types.Array):
                    el_type = p.schema_type.get_element_type(schema)
                    array_tids.append(el_type.id)
                else:
                    array_tids.append(None)

            sub_params = (
                array_tids, param.sub_params.trans_type.flatten())
        else:
            sub_params = None

        in_type_args[idx] = dbstate.Param(
            name=param.name,
            required=param.required,
            array_type_id=array_tid,
            outer_idx=outer_mapping[param.name] if outer_mapping else None,
            sub_params=sub_params,
        )

    return oparams, in_type_args  # type: ignore[return-value]


def _describe_object(
    schema: s_schema.Schema,
    source: s_obj.Object,
    protocol_version: defines.ProtocolVersion,
) -> List[DumpBlockDescriptor]:

    cols = []
    shape = []
    ptrdesc: List[DumpBlockDescriptor] = []

    if isinstance(source, s_props.Property):
        schema, prop_tuple = s_types.Tuple.from_subtypes(
            schema,
            {
                'source': schema.get('std::uuid', type=s_types.Type),
                'target': not_none(source.get_target(schema)),
            },
            {'named': True},
        )

        type_data, type_id = sertypes.describe(
            schema,
            prop_tuple,
            follow_links=False,
            protocol_version=protocol_version,
        )

        cols.extend([
            'source',
            'target',
        ])

    elif isinstance(source, s_links.Link):
        props = {}

        for ptr in source.get_pointers(schema).objects(schema):
            if not ptr.is_dumpable(schema):
                continue

            stor_info = pg_types.get_pointer_storage_info(
                ptr,
                schema=schema,
                source=source,
                link_bias=True,
            )

            cols.append(stor_info.column_name)

            props[ptr.get_shortname(schema).name] = not_none(
                ptr.get_target(schema))

        schema, link_tuple = s_types.Tuple.from_subtypes(
            schema,
            props,
            {'named': True},
        )

        type_data, type_id = sertypes.describe(
            schema,
            link_tuple,
            follow_links=False,
            protocol_version=protocol_version,
        )

    else:
        assert isinstance(source, s_objtypes.ObjectType)
        for ptr in source.get_pointers(schema).objects(schema):
            if not ptr.is_dumpable(schema):
                continue

            stor_info = pg_types.get_pointer_storage_info(
                ptr,
                schema=schema,
                source=source,
            )

            if stor_info.table_type == 'ObjectType':
                cols.append(stor_info.column_name)
                shape.append(ptr)

            link_stor_info = pg_types.get_pointer_storage_info(
                ptr,
                schema=schema,
                source=source,
                link_bias=True,
            )

            if link_stor_info is not None:
                ptrdesc.extend(_describe_object(schema, ptr,
                                                protocol_version))

        type_data, type_id = sertypes.describe(
            schema,
            source,
            view_shapes={source: shape},
            follow_links=False,
            protocol_version=protocol_version,
        )

    table_name = pg_common.get_backend_name(
        schema, source, catenate=True
    )

    stmt = (
        f'COPY {table_name} '
        f'({", ".join(pg_common.quote_ident(c) for c in cols)}) '
        f'TO STDOUT WITH BINARY'
    ).encode()

    return [DumpBlockDescriptor(
        schema_object_id=source.id,
        schema_object_class=type(source).get_ql_class_or_die(),
        schema_deps=tuple(p.schema_object_id for p in ptrdesc),
        type_desc_id=type_id,
        type_desc=type_data,
        sql_copy_stmt=stmt,
    )] + ptrdesc


def _check_dump_layout(
    dump_els: AbstractSet[str],
    schema_els: AbstractSet[str],
    elided_els: AbstractSet[str],
    label: str,
) -> None:
    extra_els = dump_els - (schema_els | elided_els)
    if extra_els:
        raise RuntimeError(
            f'dump data tuple of {label} has extraneous elements: '
            f'{", ".join(extra_els)}'
        )

    missing_els = schema_els - dump_els
    if missing_els:
        raise RuntimeError(
            f'dump data tuple of {label} has missing elements: '
            f'{", ".join(missing_els)}'
        )


def _get_ptr_mending_desc(
    schema: s_schema.Schema,
    ptr: s_pointers.Pointer,
) -> Optional[DataMendingDescriptor]:
    ptr_type = ptr.get_target(schema)
    if isinstance(ptr_type, (s_types.Array, s_types.Tuple)):
        return _get_data_mending_desc(schema, ptr_type)
    else:
        return None


def _get_data_mending_desc(
    schema: s_schema.Schema,
    typ: s_types.Type,
) -> Optional[DataMendingDescriptor]:
    if isinstance(typ, (s_types.Tuple, s_types.Array)):
        elements = tuple(
            _get_data_mending_desc(schema, element)
            for element in typ.get_subtypes(schema)
        )
    else:
        elements = tuple()

    if pg_types.type_has_stable_oid(typ):
        return None
    else:
        return DataMendingDescriptor(
            schema_type_id=typ.id,
            schema_object_class=type(typ).get_ql_class_or_die(),
            elements=elements,
            needs_mending=bool(
                isinstance(typ, (s_types.Tuple, s_types.Array))
                and any(elements)
            )
        )


def _check_force_database_error(
    ctx: CompileContext,
    ql: qlast.Base,
) -> None:
    if isinstance(ql, qlast.ConfigOp):
        return

    try:
        val = _get_config_val(ctx, 'force_database_error')
        # Check the string directly for false to skip a deserialization
        if val is None or val == 'false':
            return
        err = json.loads(val)
        if not err:
            return

        errcls = errors.EdgeDBError.get_error_class_from_name(err['type'])
        if context := err.get('context'):
            filename = context.get('filename')
            position = tuple(
                context.get(k) for k in ('line', 'col', 'start', 'end')
            )
        else:
            filename = None
            position = None

        errval = errcls(
            msg=err.get('message'),
            hint=err.get('hint'),
            details=err.get('details'),
            filename=filename,
            position=position,
        )
    except Exception:
        raise errors.ConfigurationError(
            "invalid 'force_database_error' value'")

    raise errval


def _get_config_val(
    ctx: CompileContext,
    name: str,
) -> Any:
    current_tx = ctx.state.current_tx()
    return config.lookup(
        name,
        current_tx.get_session_config(),
        current_tx.get_database_config(),
        current_tx.get_system_config(),
        spec=ctx.compiler_state.config_spec,
        allow_unrecognized=True,
    )


def _get_compilation_config_vals(ctx: CompileContext) -> Any:
    assert ctx.compiler_state.config_spec is not None
    return {
        k: _get_config_val(ctx, k)
        for k in ctx.compiler_state.config_spec
        if ctx.compiler_state.config_spec[k].affects_compilation
    }


_OUTPUT_FORMAT_MAP = {
    enums.OutputFormat.BINARY: pg_compiler.OutputFormat.NATIVE,
    enums.OutputFormat.JSON: pg_compiler.OutputFormat.JSON,
    enums.OutputFormat.JSON_ELEMENTS: pg_compiler.OutputFormat.JSON_ELEMENTS,
    enums.OutputFormat.NONE: pg_compiler.OutputFormat.NONE,
}


def _convert_format(inp: enums.OutputFormat) -> pg_compiler.OutputFormat:
    try:
        return _OUTPUT_FORMAT_MAP[inp]
    except KeyError:
        raise RuntimeError(f"Output format {inp!r} is not supported")


def _hash_sql(sql: bytes, **kwargs: bytes) -> bytes:
    h = hashlib.sha1(sql)
    for param, val in kwargs.items():
        h.update(param.encode('latin1'))
        h.update(val)
    return h.hexdigest().encode('latin1')


def _extract_extensions(
    ctx: CompileContext, user_schema: s_schema.Schema
) -> tuple[set[str], list[config.Setting]]:
    # XXX: Do we need to return None if extensions/config_spec didn't change?
    names = {
        ext.get_name(user_schema).name
        for ext in user_schema.get_objects(type=s_ext.Extension)
    }
    if names:
        schema = s_schema.ChainedSchema(
            ctx.compiler_state.std_schema, user_schema, s_schema.EMPTY_SCHEMA
        )
        settings = config.load_ext_settings_from_schema(schema)
    else:
        settings = []
    return names, settings


def _extract_roles(
    global_schema: s_schema.Schema
) -> immutables.Map[str, immutables.Map[str, Any]]:
    roles = {}
    for role in global_schema.get_objects(type=s_role.Role):
        role_name = str(role.get_name(global_schema))
        roles[role_name] = immutables.Map(
            name=role_name,
            superuser=role.get_superuser(global_schema),
            password=role.get_password(global_schema),
        )
    return immutables.Map(roles)


class DumpDescriptor(NamedTuple):

    schema_ddl: str
    schema_dynamic_ddl: Tuple[str, ...]
    schema_ids: List[Tuple[str, str, bytes]]
    blocks: Sequence[DumpBlockDescriptor]


class DumpBlockDescriptor(NamedTuple):

    schema_object_id: uuid.UUID
    schema_object_class: qltypes.SchemaObjectClass
    schema_deps: Tuple[uuid.UUID, ...]
    type_desc_id: uuid.UUID
    type_desc: bytes
    sql_copy_stmt: bytes


class RestoreDescriptor(NamedTuple):

    units: Sequence[dbstate.QueryUnit]
    blocks: Sequence[RestoreBlockDescriptor]
    tables: Sequence[str]
    repopulate_units: Sequence[str]


class DataMendingDescriptor(NamedTuple):

    #: The identifier of the EdgeDB type
    schema_type_id: uuid.UUID
    #: The kind of a type we are dealing with
    schema_object_class: qltypes.SchemaObjectClass
    #: If type is a collection, mending descriptors of element types
    elements: Tuple[Optional[DataMendingDescriptor], ...] = tuple()
    #: Whether a datum represented by this descriptor will need mending
    needs_mending: bool = False


class RestoreBlockDescriptor(NamedTuple):

    #: The identifier of the schema object this data is for.
    schema_object_id: uuid.UUID
    #: The COPY SQL statement for this block.
    sql_copy_stmt: bytes
    #: For compatibility with old dumps, a list of column indexes
    #: that should be ignored in the COPY stream.
    compat_elided_cols: Tuple[int, ...]
    #: If the tuple requires mending of unstable Postgres OIDs in data,
    #: this will contain the recursive descriptor on which parts of
    #: each datum need mending.
    data_mending_desc: Tuple[Optional[DataMendingDescriptor], ...]
