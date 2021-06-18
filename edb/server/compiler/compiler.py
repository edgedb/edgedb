# mypy: ignore-errors

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
from typing import *

import collections
import dataclasses
import json
import hashlib
import pickle
import textwrap
import uuid

import asyncpg
import immutables

from edb import errors

from edb.server import defines
from edb.pgsql import compiler as pg_compiler

from edb import edgeql
from edb.common import debug
from edb.common import verutils
from edb.common import uuidgen

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote

from edb.ir import staeval as ireval

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import common as pg_common
from edb.pgsql import types as pg_types

from edb.server import config
from edb.server import pgcluster

from . import dbstate
from . import enums
from . import sertypes
from . import status


EMPTY_MAP = immutables.Map()


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState:

    user_schema: s_schema.Schema
    global_schema: s_schema.Schema
    cached_reflection: immutables.Map[str, Tuple[str, ...]]


@dataclasses.dataclass(frozen=True)
class CompileContext:

    state: dbstate.CompilerConnectionState
    output_format: enums.IoFormat
    expected_cardinality_one: bool
    stmt_mode: enums.CompileStatementMode
    json_parameters: bool = False
    schema_reflection_mode: bool = False
    implicit_limit: int = 0
    inline_typeids: bool = False
    inline_typenames: bool = False
    inline_objectids: bool = True
    schema_object_ids: Optional[Mapping[s_name.Name, uuid.UUID]] = None
    source: Optional[edgeql.Source] = None
    backend_runtime_params: Any = (
        pgcluster.get_default_runtime_params())
    compat_ver: Optional[verutils.Version] = None
    protocol_version: Optional[tuple] = None
    bootstrap_mode: bool = False
    internal_schema_mode: bool = False
    standalone_mode: bool = False
    log_ddl_as_migrations: bool = True


DEFAULT_MODULE_ALIASES_MAP = immutables.Map(
    {None: defines.DEFAULT_MODULE_ALIAS})
_IO_FORMAT_MAP = {
    enums.IoFormat.BINARY: pg_compiler.OutputFormat.NATIVE,
    enums.IoFormat.JSON: pg_compiler.OutputFormat.JSON,
    enums.IoFormat.JSON_ELEMENTS: pg_compiler.OutputFormat.JSON_ELEMENTS,
    enums.IoFormat.SCRIPT: pg_compiler.OutputFormat.SCRIPT,
}

pg_ql = lambda o: pg_common.quote_literal(str(o))


def _convert_format(inp: enums.IoFormat) -> pg_compiler.OutputFormat:
    try:
        return _IO_FORMAT_MAP[inp]
    except KeyError:
        raise RuntimeError(f"IO format {inp!r} is not supported")


def compile_edgeql_script(
    compiler: Compiler,
    ctx: CompileContext,
    eql: str,
) -> Tuple[s_schema.Schema, str]:

    sql, argmap = compiler._compile_ql_script(ctx, eql)
    new_schema = ctx.state.current_tx().get_schema(compiler._std_schema)
    assert isinstance(new_schema, s_schema.ChainedSchema)
    return new_schema.get_top_schema(), sql


def new_compiler(
    *,
    std_schema: s_schema.Schema,
    reflection_schema: s_schema.Schema,
    schema_class_layout: Dict[Type[s_obj.Object], s_refl.SchemaClassLayout],
) -> Compiler:
    """Create and return an ad-hoc compiler instance."""

    compiler = Compiler()
    compiler._std_schema = std_schema
    compiler._refl_schema = reflection_schema
    compiler._schema_class_layout = schema_class_layout

    return compiler


def new_compiler_context(
    *,
    user_schema: s_schema.Schema,
    global_schema: s_schema.Schema=s_schema.FlatSchema(),
    single_statement: bool = False,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    expected_cardinality_one: bool = False,
    json_parameters: bool = False,
    schema_reflection_mode: bool = False,
    output_format: enums.IoFormat = enums.IoFormat.BINARY,
    bootstrap_mode: bool = False,
    internal_schema_mode: bool = False,
    standalone_mode: bool = False,
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
        state=state,
        output_format=output_format,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=json_parameters,
        schema_reflection_mode=schema_reflection_mode,
        bootstrap_mode=bootstrap_mode,
        internal_schema_mode=internal_schema_mode,
        standalone_mode=standalone_mode,
        stmt_mode=(
            enums.CompileStatementMode.SINGLE
            if single_statement else enums.CompileStatementMode.ALL
        ),
    )

    return ctx


async def load_cached_schema(backend_conn, key) -> s_schema.Schema:
    data = await backend_conn.fetchval(f'''\
        SELECT bin FROM edgedbinstdata.instdata
        WHERE key = {pg_common.quote_literal(key)};
    ''')
    try:
        return pickle.loads(data)
    except Exception as e:
        raise RuntimeError(
            'could not load std schema pickle') from e


async def load_std_schema(backend_conn) -> s_schema.Schema:
    return await load_cached_schema(backend_conn, 'stdschema')


async def load_schema_intro_query(backend_conn, kind: str) -> str:
    return await backend_conn.fetchval(f'''\
        SELECT text FROM edgedbinstdata.instdata
        WHERE key = $1::text;
    ''', kind)


async def load_schema_class_layout(backend_conn) -> s_refl.SchemaClassLayout:
    data = await backend_conn.fetchval(f'''\
        SELECT bin FROM edgedbinstdata.instdata
        WHERE key = 'classlayout';
    ''')
    try:
        return pickle.loads(data)
    except Exception as e:
        raise RuntimeError(
            'could not load schema class layout pickle') from e


class Compiler:

    _dbname: Optional[str]
    _cached_db: Optional[CompilerDatabaseState]

    def __init__(
        self,
        *,
        backend_runtime_params: pgcluster.BackendRuntimeParams=
            pgcluster.get_default_runtime_params(),
    ):
        self._dbname = None
        self._cached_db = None
        self._std_schema = None
        self._refl_schema = None
        self._config_spec = None
        self._schema_class_layout = None
        self._local_intro_query = None
        self._global_intro_query = None
        self._backend_runtime_params = backend_runtime_params

    def _hash_sql(self, sql: bytes, **kwargs: bytes):
        h = hashlib.sha1(sql)
        for param, val in kwargs.items():
            h.update(param.encode('latin1'))
            h.update(val)
        return h.hexdigest().encode('latin1')

    async def initialize_from_pg(self, con: asyncpg.Connection) -> None:
        if self._std_schema is None:
            self._std_schema = await load_cached_schema(con, 'stdschema')

        if self._refl_schema is None:
            self._refl_schema = await load_cached_schema(con, 'reflschema')

        if self._schema_class_layout is None:
            self._schema_class_layout = await load_schema_class_layout(con)

        if self._local_intro_query is None:
            self._local_intro_query = await load_schema_intro_query(
                con, 'local_intro_query')

        if self._global_intro_query is None:
            self._global_intro_query = await load_schema_intro_query(
                con, 'global_intro_query')

        if self._config_spec is None:
            self._config_spec = config.load_spec_from_schema(
                self._std_schema)
            config.set_settings(self._config_spec)

    def initialize(
        self,
        std_schema,
        refl_schema,
        schema_class_layout
    ) -> None:
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._config_spec = config.load_spec_from_schema(
            self._std_schema)
        config.set_settings(self._config_spec)

    def get_std_schema(self) -> s_schema.Schema:
        if self._std_schema is None:
            raise AssertionError('compiler is not initialized')
        return self._std_schema

    def _new_delta_context(self, ctx: CompileContext):
        context = s_delta.CommandContext()
        context.testmode = self.get_config_val(ctx, '__internal_testmode')
        context.stdmode = ctx.bootstrap_mode
        context.internal_schema_mode = ctx.internal_schema_mode
        context.schema_object_ids = ctx.schema_object_ids
        context.compat_ver = ctx.compat_ver
        context.backend_runtime_params = self._backend_runtime_params
        context.allow_dml_in_functions = (
            self.get_config_val(ctx, 'allow_dml_in_functions'))
        return context

    def _process_delta(self, ctx: CompileContext, delta):
        """Adapt and process the delta command."""

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        if debug.flags.delta_plan:
            debug.header('Canonical Delta Plan')
            debug.dump(delta, schema=schema)

        pgdelta = pg_delta.CommandMeta.adapt(delta)
        context = self._new_delta_context(ctx)
        schema = pgdelta.apply(schema, context)
        current_tx.update_schema(schema)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(pgdelta, schema=schema)

        db_cmd = any(
            isinstance(c, s_db.DatabaseCommand)
            for c in pgdelta.get_subcommands()
        )

        if db_cmd:
            block = pg_dbops.SQLBlock()
            new_types = frozenset()
        else:
            block = pg_dbops.PLTopBlock()
            new_types = frozenset(str(tid) for tid in pgdelta.new_types)

        # Generate SQL DDL for the delta.
        pgdelta.generate(block)

        # Generate schema storage SQL (DML into schema storage tables).
        subblock = block.add_block()
        self._compile_schema_storage_in_delta(
            ctx, pgdelta, subblock, context=context)

        return block, new_types

    def _compile_schema_storage_in_delta(
        self,
        ctx: CompileContext,
        delta: s_delta.Command,
        block: pg_dbops.SQLBlock,
        context: Optional[s_delta.CommandContext] = None,
    ):

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

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

        s_refl.write_meta(
            delta,
            classlayout=self._schema_class_layout,
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
                    sql, argmap = self._compile_schema_storage_stmt(ctx, eql)
                    argnames = tuple(arg.name for arg in argmap)

                    func = pg_dbops.Function(
                        name=fname,
                        args=[(argname, 'json') for argname in argnames],
                        returns='json',
                        text=sql,
                    )

                    cf = pg_dbops.CreateFunction(func, or_replace=True)
                    cf.generate(block)

                    cache_mm[eql_hash] = argnames

                argvals = []
                for argname in argnames:
                    argvals.append(pg_common.quote_literal(args[argname]))

                block.add_command(f'''
                    PERFORM {pg_common.qname(*fname)}({", ".join(argvals)});
                ''')

        ctx.state.current_tx().update_cached_reflection(cache_mm.finish())

    def _compile_schema_storage_stmt(
        self,
        ctx: CompileContext,
        eql: str,
    ) -> Tuple[str, Dict[str, int]]:

        schema = ctx.state.current_tx().get_schema(self._std_schema)

        try:
            # Switch to the shadow introspection/reflection schema.
            ctx.state.current_tx().update_schema(
                # Trick dbstate to set the effective schema
                # to _refl_schema.
                s_schema.ChainedSchema(
                    self._std_schema,
                    self._refl_schema,
                    s_schema.FlatSchema()
                )
            )

            newctx = CompileContext(
                state=ctx.state,
                stmt_mode=enums.CompileStatementMode.SINGLE,
                json_parameters=True,
                schema_reflection_mode=True,
                output_format=enums.IoFormat.JSON,
                expected_cardinality_one=False,
                bootstrap_mode=ctx.bootstrap_mode,
                protocol_version=ctx.protocol_version,
            )

            return self._compile_ql_script(newctx, eql)

        finally:
            # Restore the regular schema.
            ctx.state.current_tx().update_schema(schema)

    def _assert_not_in_migration_block(
        self,
        ctx: CompileContext,
        ql: qlast.Base
    ) -> None:
        """Check that a START MIGRATION block is *not* active."""
        current_tx = ctx.state.current_tx()
        mstate = current_tx.get_migration_state()
        if mstate is not None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} in a migration block',
                context=ql.context,
            )

    def _assert_in_migration_block(
        self,
        ctx: CompileContext,
        ql: qlast.Base
    ) -> dbstate.MigrationState:
        """Check that a START MIGRATION block *is* active."""
        current_tx = ctx.state.current_tx()
        mstate = current_tx.get_migration_state()
        if mstate is None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} outside of a migration block',
                context=ql.context,
            )
        return mstate

    def _compile_ql_script(
        self,
        ctx: CompileContext,
        eql: str,
    ) -> Tuple[str, Dict[str, int]]:

        source = edgeql.Source.from_string(eql)
        units = self._compile(ctx=ctx, source=source)

        sql_stmts = []
        for u in units:
            for stmt in u.sql:
                stmt = stmt.strip()
                if not stmt.endswith(b';'):
                    stmt += b';'

                sql_stmts.append(stmt)

        if ctx.stmt_mode is enums.CompileStatementMode.SINGLE:
            if len(sql_stmts) > 1:
                raise errors.InternalServerError(
                    'compiler yielded multiple SQL statements despite'
                    ' requested SINGLE statement mode'
                )
            sql = sql_stmts[0].strip(b';')
            argmap = units[0].in_type_args
            if argmap is None:
                argmap = ()
        else:
            sql = b'\n'.join(sql_stmts)
            argmap = ()

        return sql.decode(), argmap

    def _compile_ql_query(
        self,
        ctx: CompileContext,
        ql: qlast.Base,
        *,
        cacheable: bool = True,
        migration_block_query: bool = False,
    ) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()

        native_out_format = (
            ctx.output_format is enums.IoFormat.BINARY
        )

        single_stmt_mode = ctx.stmt_mode is enums.CompileStatementMode.SINGLE

        can_have_implicit_fields = (
            native_out_format and
            single_stmt_mode
        )

        disable_constant_folding = self.get_config_val(
            ctx,
            '__internal_no_const_folding',
        )

        ir = qlcompiler.compile_ast_to_ir(
            ql,
            schema=current_tx.get_schema(self._std_schema),
            options=qlcompiler.CompilerOptions(
                modaliases=current_tx.get_modaliases(),
                implicit_tid_in_shapes=(
                    can_have_implicit_fields and ctx.inline_typeids
                ),
                implicit_tname_in_shapes=(
                    can_have_implicit_fields and ctx.inline_typenames
                ),
                implicit_id_in_shapes=(
                    can_have_implicit_fields and ctx.inline_objectids
                ),
                constant_folding=not disable_constant_folding,
                json_parameters=ctx.json_parameters,
                implicit_limit=ctx.implicit_limit,
                allow_writing_protected_pointers=ctx.schema_reflection_mode,
                apply_query_rewrites=(
                    not ctx.bootstrap_mode
                    and not ctx.schema_reflection_mode
                ),
            ),
        )

        if ir.cardinality.is_single():
            result_cardinality = enums.ResultCardinality.ONE
        else:
            result_cardinality = enums.ResultCardinality.MANY
            if ctx.expected_cardinality_one:
                raise errors.ResultCardinalityMismatchError(
                    f'the query has cardinality {result_cardinality} '
                    f'which does not match the expected cardinality ONE')

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=(
                debug.flags.edgeql_compile
                or debug.flags.edgeql_compile_sql_text
                or debug.flags.delta_execute
            ),
            expected_cardinality_one=ctx.expected_cardinality_one,
            output_format=_convert_format(ctx.output_format),
        )

        if (
            (mstate := current_tx.get_migration_state())
            and not migration_block_query
        ):
            mstate = mstate._replace(
                accepted_cmds=mstate.accepted_cmds + (ql,),
            )
            current_tx.update_migration_state(mstate)

            return dbstate.NullQuery()

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

        if single_stmt_mode:
            if native_out_format:
                out_type_data, out_type_id = sertypes.TypeSerializer.describe(
                    ir.schema, ir.stype,
                    ir.view_shapes, ir.view_shapes_metadata,
                    inline_typenames=ctx.inline_typenames,
                    protocol_version=ctx.protocol_version)
            else:
                out_type_data, out_type_id = \
                    sertypes.TypeSerializer.describe_json()

            in_type_args = None

            if ir.params:
                first_param = next(iter(ir.params))
                named = not first_param.name.isdecimal()
                if (src := ctx.source) is not None:
                    first_extracted = src.first_extra()
                else:
                    first_extracted = None

                if first_extracted is not None:
                    user_params = first_extracted
                else:
                    user_params = len(ir.params)

                subtypes = [None] * user_params
                in_type_args = [None] * user_params
                for param in ir.params:
                    sql_param = argmap[param.name]

                    idx = sql_param.index - 1
                    if first_extracted is not None and idx >= first_extracted:
                        continue

                    array_tid = None
                    if (
                        param.schema_type.is_array()
                        and not ctx.standalone_mode
                    ):
                        el_type = param.schema_type.get_element_type(ir.schema)
                        array_tid = el_type.id
                        # array_tid = el_type.get_backend_id(ir.schema)
                        # if array_tid is None:
                        #     assert array_tid is not None

                    subtypes[idx] = (param.name, param.schema_type)
                    in_type_args[idx] = dbstate.Param(
                        name=param.name,
                        required=sql_param.required,
                        array_type_id=array_tid,
                    )

                ir.schema, params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types=collections.OrderedDict(subtypes),
                    named=named)

            else:
                ir.schema, params_type = s_types.Tuple.create(
                    ir.schema, element_types={}, named=False)

            in_type_data, in_type_id = sertypes.TypeSerializer.describe(
                ir.schema, params_type, {}, {},
                protocol_version=ctx.protocol_version)

            sql_hash = self._hash_sql(
                sql_bytes,
                mode=str(ctx.output_format).encode(),
                intype=in_type_id.bytes,
                outtype=out_type_id.bytes)

            return dbstate.Query(
                sql=(sql_bytes,),
                sql_hash=sql_hash,
                cardinality=result_cardinality,
                in_type_id=in_type_id.bytes,
                in_type_data=in_type_data,
                in_type_args=in_type_args,
                out_type_id=out_type_id.bytes,
                out_type_data=out_type_data,
                cacheable=cacheable,
                has_dml=ir.dml_exprs,
            )

        else:
            if ir.params:
                raise errors.QueryError(
                    'EdgeQL script queries cannot accept parameters')

            return dbstate.SimpleQuery(
                sql=(sql_bytes,),
                has_dml=ir.dml_exprs,
            )

    def _compile_and_apply_ddl_stmt(
        self,
        ctx: CompileContext,
        stmt: qlast.DDLOperation,
    ) -> dbstate.DDLQuery:
        if isinstance(stmt, qlast.GlobalObjectCommand):
            self._assert_not_in_migration_block(ctx, stmt)

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        mstate = current_tx.get_migration_state()
        if (
            mstate is None
            and not ctx.bootstrap_mode
            and ctx.log_ddl_as_migrations
            and not isinstance(
                stmt,
                (qlast.CreateMigration, qlast.GlobalObjectCommand),
            )
        ):
            cm = qlast.CreateMigration(
                body=qlast.NestedQLBlock(
                    commands=[stmt],
                ),
            )
            return self._compile_and_apply_ddl_stmt(ctx, cm)

        delta = s_ddl.delta_from_ddl(
            stmt,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=self.get_config_val(ctx, '__internal_testmode'),
            allow_dml_in_functions=(
                self.get_config_val(ctx, 'allow_dml_in_functions')),
            schema_object_ids=ctx.schema_object_ids,
            compat_ver=ctx.compat_ver,
        )

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta)

        if mstate := current_tx.get_migration_state():
            mstate = mstate._replace(
                accepted_cmds=mstate.accepted_cmds + (stmt,),
            )

            context = self._new_delta_context(ctx)
            orig_schema = schema
            schema = delta.apply(schema, context=context)

            if mstate.last_proposed:
                if (
                    mstate.last_proposed[0].required_user_input
                    or mstate.last_proposed[0].prompt_id.startswith("Rename")
                ):
                    # Cannot auto-apply the proposed DDL
                    # if user input is required.
                    # Also skip auto-applying for renames, since
                    # renames often force a bunch of rethinking.
                    mstate = mstate._replace(last_proposed=tuple())
                else:
                    proposed_stmts = mstate.last_proposed[0].statements
                    ddl_script = '\n'.join(proposed_stmts)
                    proposed_schema = s_ddl.apply_ddl_script(
                        ddl_script, schema=orig_schema)

                    if s_ddl.schemas_are_equal(schema, proposed_schema):
                        # The client has confirmed the proposed migration step,
                        # advance the proposed script.
                        mstate = mstate._replace(
                            last_proposed=mstate.last_proposed[1:],
                        )
                    else:
                        # The client replied with a statement that does not
                        # match what was proposed, reset the proposed script
                        # to force script regeneration on next DESCRIBE.
                        mstate = mstate._replace(last_proposed=tuple())

            current_tx.update_migration_state(mstate)
            current_tx.update_schema(schema)

            return dbstate.DDLQuery(
                sql=(b'SELECT LIMIT 0',),
                user_schema=current_tx.get_user_schema(),
                is_transactional=True,
                single_unit=False,
            )

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = current_tx.get_schema(self._std_schema)
        context = self._new_delta_context(ctx)
        delta.apply(test_schema, context=context)
        delta.canonical = True

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        block, new_types = self._process_delta(ctx, delta)

        ddl_stmt_id: Optional[str] = None

        is_transactional = block.is_transactional()
        if not is_transactional:
            sql = tuple(stmt.encode('utf-8')
                        for stmt in block.get_statements())
        else:
            sql = (block.to_string().encode('utf-8'),)

            if new_types:
                # Inject a query returning backend OIDs for the newly
                # created types.
                ddl_stmt_id = str(uuidgen.uuid1mc())
                new_type_ids = [
                    f'{pg_common.quote_literal(tid)}::uuid'
                    for tid in new_types
                ]
                sql = sql + (textwrap.dedent(f'''\
                    SELECT
                        json_build_object(
                            'ddl_stmt_id',
                            {pg_common.quote_literal(ddl_stmt_id)},
                            'new_types',
                            (SELECT
                                json_object_agg(
                                    "id"::text,
                                    "backend_id"
                                )
                                FROM
                                edgedb."_SchemaType"
                                WHERE
                                    "id" = any(ARRAY[
                                        {', '.join(new_type_ids)}
                                    ])
                            )
                        )::text;
                ''').encode('utf-8'),)

        create_db = None
        drop_db = None
        create_db_template = None
        if isinstance(stmt, qlast.DropDatabase):
            drop_db = stmt.name.name
        elif isinstance(stmt, qlast.CreateDatabase):
            create_db = stmt.name.name
            create_db_template = stmt.template.name if stmt.template else None

        if debug.flags.delta_execute:
            debug.header('Delta Script')
            debug.dump_code(b'\n'.join(sql), lexer='sql')

        return dbstate.DDLQuery(
            sql=sql,
            is_transactional=is_transactional,
            single_unit=(
                (not is_transactional)
                or (drop_db is not None)
                or (create_db is not None)
                or new_types
            ),
            create_db=create_db,
            drop_db=drop_db,
            create_db_template=create_db_template,
            has_role_ddl=isinstance(stmt, qlast.RoleCommand),
            ddl_stmt_id=ddl_stmt_id,
            user_schema=current_tx.get_user_schema_if_updated(),
            cached_reflection=current_tx.get_cached_reflection_if_updated(),
            global_schema=current_tx.get_global_schema_if_updated(),
        )

    def _compile_ql_migration(
        self,
        ctx: CompileContext,
        ql: qlast.MigrationCommand,
    ):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        if isinstance(ql, qlast.CreateMigration):
            self._assert_not_in_migration_block(ctx, ql)

            query = self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.StartMigration):
            self._assert_not_in_migration_block(ctx, ql)

            if current_tx.is_implicit():
                savepoint_name = None
                tx_cmd = qlast.StartTransaction()
            else:
                savepoint_name = str(uuid.uuid4())
                tx_cmd = qlast.DeclareSavepoint(name=savepoint_name)

            tx_query = self._compile_ql_transaction(ctx, tx_cmd)
            assert self._std_schema is not None
            base_schema = s_schema.ChainedSchema(
                self._std_schema,
                s_schema.FlatSchema(),
                current_tx.get_global_schema(),
            )
            target_schema = s_ddl.apply_sdl(
                ql.target,
                base_schema=base_schema,
                current_schema=schema,
                allow_dml_in_functions=(
                    self.get_config_val(ctx, 'allow_dml_in_functions')),
            )

            current_tx.update_migration_state(
                dbstate.MigrationState(
                    parent_migration=schema.get_last_migration(),
                    initial_schema=schema,
                    initial_savepoint=savepoint_name,
                    guidance=s_obj.DeltaGuidance(),
                    target_schema=target_schema,
                    accepted_cmds=tuple(),
                    last_proposed=tuple(),
                ),
            )

            query = dbstate.MigrationControlQuery(
                sql=tx_query.sql,
                action=dbstate.MigrationAction.START,
                tx_action=tx_query.action,
                cacheable=False,
                modaliases=None,
                single_unit=tx_query.single_unit,
            )

        elif isinstance(ql, qlast.PopulateMigration):
            mstate = self._assert_in_migration_block(ctx, ql)

            diff = s_ddl.delta_schemas(
                schema,
                mstate.target_schema,
                guidance=mstate.guidance,
            )
            if debug.flags.delta_plan:
                debug.header('Populate Migration Diff')
                debug.dump(diff, schema=schema)

            new_ddl = tuple(s_ddl.ddlast_from_delta(
                schema, mstate.target_schema, diff))
            all_ddl = mstate.accepted_cmds + new_ddl
            mstate = mstate._replace(accepted_cmds=all_ddl)
            if debug.flags.delta_plan:
                debug.header('Populate Migration DDL AST')
                text = []
                for cmd in new_ddl:
                    debug.dump(cmd)
                    text.append(qlcodegen.generate_source(cmd, pretty=True))
                debug.header('Populate Migration DDL Text')
                debug.dump_code(';\n'.join(text) + ';')
            current_tx.update_migration_state(mstate)

            delta_context = self._new_delta_context(ctx)
            schema = diff.apply(schema, delta_context)
            current_tx.update_schema(schema)

            query = dbstate.MigrationControlQuery(
                sql=(b'SELECT LIMIT 0',),
                tx_action=None,
                action=dbstate.MigrationAction.POPULATE,
                cacheable=False,
                modaliases=None,
                single_unit=False,
            )

        elif isinstance(ql, qlast.DescribeCurrentMigration):
            mstate = self._assert_in_migration_block(ctx, ql)

            if ql.language is qltypes.DescribeLanguage.DDL:
                text = []
                for stmt in mstate.accepted_cmds:
                    text.append(qlcodegen.generate_source(stmt, pretty=True))

                if text:
                    description = ';\n'.join(text) + ';'
                else:
                    description = ''

                desc_ql = edgeql.parse(
                    f'SELECT {qlquote.quote_literal(description)}'
                )
                query = self._compile_ql_query(
                    ctx,
                    desc_ql,
                    cacheable=False,
                    migration_block_query=True,
                )

            elif ql.language is qltypes.DescribeLanguage.JSON:
                confirmed = []
                for stmt in mstate.accepted_cmds:
                    confirmed.append(
                        # Add a terminating semicolon to match
                        # "proposed", which is created by
                        # s_ddl.statements_from_delta.
                        qlcodegen.generate_source(stmt, pretty=True) + ';',
                    )

                if not mstate.last_proposed:
                    guided_diff = s_ddl.delta_schemas(
                        schema,
                        mstate.target_schema,
                        generate_prompts=True,
                        guidance=mstate.guidance,
                    )

                    proposed_ddl = s_ddl.statements_from_delta(
                        schema,
                        mstate.target_schema,
                        guided_diff,
                    )
                    proposed_steps = []

                    if proposed_ddl:
                        for ddl_text, ast, top_op in proposed_ddl:
                            # get_ast has a lot of logic for figuring
                            # out when an op is implicit in a parent
                            # op. get_user_prompt does not have any of
                            # that sort of logic, which makes it
                            # susceptible to producing overly broad
                            # messages. To avoid duplicating that sort
                            # of logic, we recreate the delta from the
                            # AST, and extract a user prompt from
                            # *that*.
                            # This is stupid, and it is slow.
                            top_op2 = s_ddl.cmd_from_ddl(
                                ast,
                                schema=schema,
                                modaliases=current_tx.get_modaliases(),
                            )
                            _, prompt_text = top_op2.get_user_prompt()

                            # The prompt_id still needs to come from
                            # the original op, though, since
                            # orig_cmd_class is lost in ddl.
                            prompt_id, _ = top_op.get_user_prompt()
                            confidence = top_op.get_annotation('confidence')
                            assert confidence is not None

                            step = dbstate.ProposedMigrationStep(
                                statements=(ddl_text,),
                                confidence=confidence,
                                prompt=prompt_text,
                                prompt_id=prompt_id,
                                data_safe=top_op.is_data_safe(),
                                required_user_input=tuple(
                                    top_op.get_required_user_input().items(),
                                ),
                            )
                            proposed_steps.append(step)

                        proposed_desc = proposed_steps[0].to_json()
                    else:
                        proposed_desc = None

                    mstate = mstate._replace(
                        last_proposed=tuple(proposed_steps),
                    )

                    current_tx.update_migration_state(mstate)
                else:
                    proposed_desc = mstate.last_proposed[0].to_json()

                desc = json.dumps({
                    'parent': (
                        str(mstate.parent_migration.get_name(schema))
                        if mstate.parent_migration is not None
                        else 'initial'
                    ),
                    'complete': s_ddl.schemas_are_equal(
                        schema, mstate.target_schema),
                    'confirmed': confirmed,
                    'proposed': proposed_desc,
                }).encode('unicode_escape').decode('utf-8')

                desc_ql = edgeql.parse(
                    f'SELECT to_json({qlquote.quote_literal(desc)})'
                )
                query = self._compile_ql_query(
                    ctx,
                    desc_ql,
                    cacheable=False,
                    migration_block_query=True,
                )

            else:
                raise AssertionError(
                    f'DESCRIBE CURRENT MIGRATION AS {ql.language}'
                    f' is not implemented'
                )

        elif isinstance(ql, qlast.AlterCurrentMigrationRejectProposed):
            mstate = self._assert_in_migration_block(ctx, ql)

            diff = s_ddl.delta_schemas(
                schema,
                mstate.target_schema,
                generate_prompts=True,
                guidance=mstate.guidance,
            )

            try:
                top_command = next(iter(diff.get_subcommands()))
            except StopIteration:
                new_guidance = mstate.guidance
            else:
                if (orig_cmdclass :=
                        top_command.get_annotation('orig_cmdclass')):
                    top_cmdclass = orig_cmdclass
                else:
                    top_cmdclass = type(top_command)

                if issubclass(top_cmdclass, s_delta.AlterObject):
                    new_guidance = mstate.guidance._replace(
                        banned_alters=mstate.guidance.banned_alters | {(
                            top_command.get_schema_metaclass(),
                            (
                                top_command.classname,
                                top_command.get_annotation('new_name'),
                            ),
                        )}
                    )
                elif issubclass(top_cmdclass, s_delta.CreateObject):
                    new_guidance = mstate.guidance._replace(
                        banned_creations=mstate.guidance.banned_creations | {(
                            top_command.get_schema_metaclass(),
                            top_command.classname,
                        )}
                    )
                elif issubclass(top_cmdclass, s_delta.DeleteObject):
                    new_guidance = mstate.guidance._replace(
                        banned_deletions=mstate.guidance.banned_deletions | {(
                            top_command.get_schema_metaclass(),
                            top_command.classname,
                        )}
                    )
                else:
                    raise AssertionError(
                        f'unexpected top-level command in '
                        f'delta diff: {top_cmdclass!r}',
                    )

            mstate = mstate._replace(
                guidance=new_guidance,
                last_proposed=tuple(),
            )
            current_tx.update_migration_state(mstate)

            query = dbstate.MigrationControlQuery(
                sql=(b'SELECT LIMIT 0',),
                tx_action=None,
                action=dbstate.MigrationAction.REJECT_PROPOSED,
                cacheable=False,
                modaliases=None,
                single_unit=False,
            )

        elif isinstance(ql, qlast.CommitMigration):
            mstate = self._assert_in_migration_block(ctx, ql)

            diff = s_ddl.delta_schemas(schema, mstate.target_schema)
            if list(diff.get_subcommands()):
                raise errors.QueryError(
                    'cannot commit incomplete migration',
                    hint=(
                        'Please finish the migration by specifying the'
                        ' remaining DDL operations or run POPULATE MIGRATION'
                        ' to let the system populate the outstanding DDL'
                        ' automatically.'
                    ),
                    context=ql.context,
                )

            last_migration = schema.get_last_migration()
            if last_migration:
                last_migration_ref = s_utils.name_to_ast_ref(
                    last_migration.get_name(schema),
                )
            else:
                last_migration_ref = None

            create_migration = qlast.CreateMigration(
                body=qlast.NestedQLBlock(commands=mstate.accepted_cmds),
                parent=last_migration_ref,
            )

            current_tx.update_schema(mstate.initial_schema)
            current_tx.update_migration_state(None)

            ddl_query = self._compile_and_apply_ddl_stmt(
                ctx,
                create_migration,
            )

            if mstate.initial_savepoint:
                savepoint_name = str(uuid.uuid4())
                tx_cmd = qlast.DeclareSavepoint(name=savepoint_name)
            else:
                tx_cmd = qlast.CommitTransaction()

            tx_query = self._compile_ql_transaction(ctx, tx_cmd)

            query = dbstate.MigrationControlQuery(
                sql=ddl_query.sql + tx_query.sql,
                ddl_stmt_id=ddl_query.ddl_stmt_id,
                action=dbstate.MigrationAction.COMMIT,
                tx_action=tx_query.action,
                cacheable=False,
                modaliases=None,
                single_unit=True,
                user_schema=ctx.state.current_tx().get_user_schema(),
                cached_reflection=current_tx.get_cached_reflection_if_updated()
            )

        elif isinstance(ql, qlast.AbortMigration):
            mstate = self._assert_in_migration_block(ctx, ql)

            if mstate.initial_savepoint:
                savepoint_name = str(uuid.uuid4())
                tx_cmd = qlast.RollbackToSavepoint(name=savepoint_name)
            else:
                tx_cmd = qlast.RollbackTransaction()

            current_tx.update_migration_state(None)
            query = self._compile_ql_transaction(ctx, tx_cmd)

        else:
            raise AssertionError(f'unexpected migration command: {ql}')

        return query

    def _compile_ql_transaction(
            self, ctx: CompileContext,
            ql: qlast.Transaction) -> dbstate.Query:

        cacheable = True
        single_unit = False

        modaliases = None
        final_user_schema: Optional[s_schema.Schema] = None
        final_cached_reflection = None
        final_global_schema: Optional[s_schema.Schema] = None

        if isinstance(ql, qlast.StartTransaction):
            self._assert_not_in_migration_block(ctx, ql)

            ctx.state.start_tx()

            sql = 'START TRANSACTION'
            if ql.isolation is not None:
                sql += f' ISOLATION LEVEL {ql.isolation.value}'
            if ql.access is not None:
                sql += f' {ql.access.value}'
            if ql.deferrable is not None:
                sql += f' {ql.deferrable.value}'
            sql += ';'
            sql = (sql.encode(),)

            action = dbstate.TxAction.START
            cacheable = False

        elif isinstance(ql, qlast.CommitTransaction):
            self._assert_not_in_migration_block(ctx, ql)

            cur_tx = ctx.state.current_tx()
            final_user_schema = cur_tx.get_user_schema_if_updated()
            final_cached_reflection = cur_tx.get_cached_reflection_if_updated()
            final_global_schema = cur_tx.get_global_schema_if_updated()

            new_state: dbstate.TransactionState = ctx.state.commit_tx()
            modaliases = new_state.modaliases

            sql = (b'COMMIT',)
            single_unit = True
            cacheable = False
            action = dbstate.TxAction.COMMIT

        elif isinstance(ql, qlast.RollbackTransaction):
            new_state: dbstate.TransactionState = ctx.state.rollback_tx()
            modaliases = new_state.modaliases

            sql = (b'ROLLBACK',)
            single_unit = True
            cacheable = False
            action = dbstate.TxAction.ROLLBACK

        elif isinstance(ql, qlast.DeclareSavepoint):
            tx = ctx.state.current_tx()
            sp_id = tx.declare_savepoint(ql.name)

            if not ctx.bootstrap_mode:
                pgname = pg_common.quote_ident(ql.name)
                sql = (
                    f'''
                        INSERT INTO _edgecon_current_savepoint(sp_id)
                        VALUES (
                            {pg_ql(sp_id)}
                        )
                        ON CONFLICT (_sentinel) DO
                        UPDATE
                            SET sp_id = {pg_ql(sp_id)};
                    '''.encode(),
                    f'SAVEPOINT {pgname};'.encode()
                )
            else:  # pragma: no cover
                sql = (f'SAVEPOINT {pgname};'.encode(),)

            cacheable = False
            action = dbstate.TxAction.DECLARE_SAVEPOINT

        elif isinstance(ql, qlast.ReleaseSavepoint):
            ctx.state.current_tx().release_savepoint(ql.name)
            pgname = pg_common.quote_ident(ql.name)
            sql = (f'RELEASE SAVEPOINT {pgname}'.encode(),)
            action = dbstate.TxAction.RELEASE_SAVEPOINT

        elif isinstance(ql, qlast.RollbackToSavepoint):
            tx = ctx.state.current_tx()
            new_state: dbstate.TransactionState = tx.rollback_to_savepoint(
                ql.name)
            modaliases = new_state.modaliases

            pgname = pg_common.quote_ident(ql.name)
            sql = (f'ROLLBACK TO SAVEPOINT {pgname}'.encode(),)
            single_unit = True
            cacheable = False
            action = dbstate.TxAction.ROLLBACK_TO_SAVEPOINT

        else:  # pragma: no cover
            raise ValueError(f'expected a transaction AST node, got {ql!r}')

        return dbstate.TxControlQuery(
            sql=sql,
            action=action,
            cacheable=cacheable,
            single_unit=single_unit,
            modaliases=modaliases,
            user_schema=final_user_schema,
            cached_reflection=final_cached_reflection,
            global_schema=final_global_schema,
        )

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.BaseSessionCommand):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        aliases = ctx.state.current_tx().get_modaliases()

        sqlbuf = []

        alias_tpl = lambda alias, module: f'''
            INSERT INTO _edgecon_state(name, value, type)
            VALUES (
                {pg_ql(alias or '')},
                to_jsonb({pg_ql(module)}::text),
                'A'
            )
            ON CONFLICT (name, type) DO
            UPDATE
                SET value = to_jsonb({pg_ql(module)}::text);
        '''.encode()

        if isinstance(ql, qlast.SessionSetAliasDecl):
            try:
                schema.get_global(s_mod.Module, ql.module)
            except errors.InvalidReferenceError:
                raise errors.UnknownModuleError(
                    f'module {ql.module!r} does not exist') from None

            aliases = aliases.set(ql.alias, ql.module)

            if not ctx.bootstrap_mode:
                sql = alias_tpl(ql.alias, ql.module)
                sqlbuf.append(sql)

        elif isinstance(ql, qlast.SessionResetModule):
            aliases = aliases.set(None, defines.DEFAULT_MODULE_ALIAS)

            if not ctx.bootstrap_mode:
                sql = alias_tpl('', defines.DEFAULT_MODULE_ALIAS)
                sqlbuf.append(sql)

        elif isinstance(ql, qlast.SessionResetAllAliases):
            aliases = DEFAULT_MODULE_ALIASES_MAP

            if not ctx.bootstrap_mode:
                sqlbuf.append(
                    b"DELETE FROM _edgecon_state s WHERE s.type = 'A';")
                sqlbuf.append(
                    alias_tpl('', defines.DEFAULT_MODULE_ALIAS))

        elif isinstance(ql, qlast.SessionResetAliasDecl):
            aliases = aliases.delete(ql.alias)

            if not ctx.bootstrap_mode:
                sql = f'''
                    DELETE FROM _edgecon_state s
                    WHERE s.name = {pg_ql(ql.alias)} AND s.type = 'A';
                '''.encode()
                sqlbuf.append(sql)

        else:  # pragma: no cover
            raise errors.InternalServerError(
                f'unsupported SET command type {type(ql)!r}')

        ctx.state.current_tx().update_modaliases(aliases)

        if len(sqlbuf) == 1:
            sql = sqlbuf[0]
        else:
            sql = b'''
            DO LANGUAGE plpgsql $$ BEGIN
            %b
            END; $$;
            ''' % (b''.join(sqlbuf))

        return dbstate.SessionStateQuery(
            sql=(sql,),
        )

    def _compile_ql_config_op(self, ctx: CompileContext, ql: qlast.Base):

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        modaliases = current_tx.get_modaliases()
        session_config = current_tx.get_session_config()
        database_config = current_tx.get_database_config()

        if ql.scope is not qltypes.ConfigScope.SESSION:
            self._assert_not_in_migration_block(ctx, ql)

        if (
            ql.scope is qltypes.ConfigScope.SYSTEM
            and not current_tx.is_implicit()
        ):
            raise errors.QueryError(
                'CONFIGURE SYSTEM cannot be executed in a transaction block')

        ir = qlcompiler.compile_ast_to_ir(
            ql,
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=modaliases,
            ),
        )

        is_backend_setting = bool(getattr(ir, 'backend_setting', None))
        requires_restart = bool(getattr(ir, 'requires_restart', False))

        sql_text, _ = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=(debug.flags.edgeql_compile
                    or debug.flags.edgeql_compile_sql_text),
        )

        sql = (sql_text.encode(),)

        if ql.scope is qltypes.ConfigScope.SESSION:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)

            session_config = config_op.apply(
                config.get_settings(),
                session_config,
            )
            current_tx.update_session_config(session_config)

        elif ql.scope is qltypes.ConfigScope.DATABASE:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)

            database_config = config_op.apply(
                config.get_settings(),
                database_config,
            )
            current_tx.update_database_config(database_config)

        elif ql.scope is qltypes.ConfigScope.SYSTEM:
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
            config_scope=ql.scope,
            requires_restart=requires_restart,
            config_op=config_op,
        )

    def _compile_dispatch_ql(
        self,
        ctx: CompileContext,
        ql: qlast.Base
    ) -> Tuple[dbstate.BaseQuery, enums.Capability]:
        if isinstance(ql, qlast.MigrationCommand):
            query = self._compile_ql_migration(ctx, ql)
            if isinstance(
                query,
                (dbstate.MigrationControlQuery, dbstate.DDLQuery),
            ):
                return (query, enums.Capability.DDL)
            else:  # DESCRIBE CURRENT MIGRATION
                return (query, enums.Capability(0))

        elif isinstance(ql, (qlast.DatabaseCommand, qlast.DDL)):
            return (
                self._compile_and_apply_ddl_stmt(ctx, ql),
                enums.Capability.DDL,
            )

        elif isinstance(ql, qlast.Transaction):
            return (
                self._compile_ql_transaction(ctx, ql),
                enums.Capability.TRANSACTION,
            )

        elif isinstance(ql, (qlast.BaseSessionSet, qlast.BaseSessionReset)):
            return (
                self._compile_ql_sess_state(ctx, ql),
                enums.Capability.SESSION_CONFIG,
            )

        elif isinstance(ql, qlast.ConfigOp):
            if ql.scope is qltypes.ConfigScope.SESSION:
                capability = enums.Capability.SESSION_CONFIG
            else:
                capability = enums.Capability.PERSISTENT_CONFIG
            return (
                self._compile_ql_config_op(ctx, ql),
                capability,
            )

        else:
            query = self._compile_ql_query(ctx, ql)
            caps = enums.Capability(0)
            if (
                isinstance(query, (dbstate.Query, dbstate.SimpleQuery))
                and query.has_dml
            ):
                caps |= enums.Capability.MODIFICATIONS
            return (query, caps)

    def _compile(
        self,
        *,
        ctx: CompileContext,
        source: edgeql.Source,
    ) -> List[dbstate.QueryUnit]:
        current_tx = ctx.state.current_tx()
        if current_tx.get_migration_state() is not None:
            original = edgeql.Source.from_string(source.text())
            ctx = dataclasses.replace(
                ctx,
                source=original,
                implicit_limit=0,
            )
            return self._try_compile(ctx=ctx, source=original)

        try:
            return self._try_compile(ctx=ctx, source=source)
        except errors.EdgeQLSyntaxError as original_err:
            if isinstance(source, edgeql.NormalizedSource):
                # try non-normalized source
                try:
                    original = edgeql.Source.from_string(source.text())
                    ctx = dataclasses.replace(ctx, source=original)
                    self._try_compile(ctx=ctx, source=original)
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
        self,
        *,
        ctx: CompileContext,
        source: edgeql.Source,
    ) -> List[dbstate.QueryUnit]:

        # When True it means that we're compiling for "connection.query()".
        # That means that the returned QueryUnit has to have the in/out codec
        # information, correctly inferred "singleton_result" field etc.
        single_stmt_mode = ctx.stmt_mode is enums.CompileStatementMode.SINGLE
        default_cardinality = enums.ResultCardinality.NO_RESULT

        statements = edgeql.parse_block(source)
        statements_len = len(statements)

        if ctx.stmt_mode is enums.CompileStatementMode.SKIP_FIRST:
            statements = statements[1:]
            if not statements:  # pragma: no cover
                # Shouldn't ever happen as the server tracks the number
                # of statements (via the "try_compile_rollback()" method)
                # before using SKIP_FIRST.
                raise errors.ProtocolError(
                    f'no statements to compile in SKIP_FIRST mode')
        elif single_stmt_mode and statements_len != 1:
            raise errors.ProtocolError(
                f'expected one statement, got {statements_len}')

        if not len(statements):  # pragma: no cover
            raise errors.ProtocolError('nothing to compile')

        units = []
        unit = None

        for stmt in statements:
            comp, capabilities = self._compile_dispatch_ql(ctx, stmt)

            if unit is not None:
                if comp.single_unit:
                    units.append(unit)
                    unit = None

            if unit is None:
                unit = dbstate.QueryUnit(
                    sql=(),
                    status=status.get_status(stmt),
                    cardinality=default_cardinality,
                )
            else:
                unit.status = status.get_status(stmt)

            unit.capabilities |= capabilities

            if not comp.is_transactional:
                if statements_len > 1:
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

                if not comp.single_unit:
                    raise errors.InternalServerError(
                        'non-transactional compilation units must '
                        'be single-unit'
                    )

                unit.is_transactional = False

            if isinstance(comp, dbstate.Query):
                if single_stmt_mode:
                    unit.sql = comp.sql
                    unit.sql_hash = comp.sql_hash

                    unit.out_type_data = comp.out_type_data
                    unit.out_type_id = comp.out_type_id
                    unit.in_type_data = comp.in_type_data
                    unit.in_type_args = comp.in_type_args
                    unit.in_type_id = comp.in_type_id

                    unit.cacheable = comp.cacheable

                    unit.cardinality = comp.cardinality
                else:
                    unit.sql += comp.sql

            elif isinstance(comp, dbstate.SimpleQuery):
                assert not single_stmt_mode
                unit.sql += comp.sql

            elif isinstance(comp, dbstate.DDLQuery):
                unit.sql += comp.sql
                unit.create_db = comp.create_db
                unit.drop_db = comp.drop_db
                unit.create_db_template = comp.create_db_template
                unit.has_role_ddl = comp.has_role_ddl
                unit.ddl_stmt_id = comp.ddl_stmt_id
                if comp.user_schema is not None:
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)
                if comp.global_schema is not None:
                    unit.global_schema = pickle.dumps(comp.global_schema, -1)

                if comp.single_unit:
                    units.append(unit)
                    unit = None

            elif isinstance(comp, dbstate.TxControlQuery):
                unit.sql += comp.sql
                unit.cacheable = comp.cacheable
                if comp.user_schema is not None:
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)

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

                if comp.single_unit:
                    units.append(unit)
                    unit = None

            elif isinstance(comp, dbstate.MigrationControlQuery):
                unit.sql += comp.sql
                unit.cacheable = comp.cacheable
                if comp.user_schema is not None:
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
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
                elif comp.tx_action is dbstate.TxAction.ROLLBACK_TO_SAVEPOINT:
                    unit.tx_savepoint_rollback = True

                if comp.single_unit:
                    units.append(unit)
                    unit = None

            elif isinstance(comp, dbstate.SessionStateQuery):
                unit.sql += comp.sql

                if comp.config_scope is qltypes.ConfigScope.SYSTEM:
                    if (not ctx.state.current_tx().is_implicit() or
                            statements_len > 1):
                        raise errors.QueryError(
                            'CONFIGURE SYSTEM cannot be executed in a '
                            'transaction block')

                    unit.system_config = True
                elif comp.config_scope is qltypes.ConfigScope.DATABASE:
                    unit.database_config = True

                if comp.is_backend_setting:
                    unit.backend_config = True
                if comp.requires_restart:
                    unit.config_requires_restart = True

                if ctx.state.current_tx().is_implicit():
                    unit.modaliases = ctx.state.current_tx().get_modaliases()

                if comp.config_op is not None:
                    unit.config_ops.append(comp.config_op)

                unit.has_set = True

            elif isinstance(comp, dbstate.NullQuery):
                pass

            else:  # pragma: no cover
                raise errors.InternalServerError('unknown compile state')

        if unit is not None:
            units.append(unit)

        if single_stmt_mode:
            if len(units) != 1:  # pragma: no cover
                raise errors.InternalServerError(
                    f'expected 1 compiled unit; got {len(units)}')

        for unit in units:  # pragma: no cover
            # Sanity checks
            na_cardinality = (
                unit.cardinality is enums.ResultCardinality.NO_RESULT
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

        return units

    # API

    @staticmethod
    def try_compile_rollback(eql: bytes):
        statements = edgeql.parse_block(eql.decode())

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
                cacheable=False)

        if unit is not None:
            return unit, len(statements) - 1

        raise errors.TransactionError(
            'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
        )  # pragma: no cover

    def compile_notebook(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: Mapping[str, Tuple[str, ...]],
        database_config: Mapping[str, config.SettingValue],
        system_config: Mapping[str, config.SettingValue],
        queries: List[str],
        implicit_limit: int = 0,
    ) -> List[dbstate.QueryUnit]:

        state = dbstate.CompilerConnectionState(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=reflection_cache,
        )

        ctx = CompileContext(
            state=state,
            output_format=enums.IoFormat.BINARY,
            expected_cardinality_one=False,
            implicit_limit=implicit_limit,
            inline_typenames=True,
            stmt_mode=enums.CompileStatementMode.SINGLE,
            json_parameters=False,
        )

        ctx.state.start_tx()

        result: List[
            Tuple[
                bool,
                Union[dbstate.QueryUnit, Tuple[str, str, Dict[str, str]]]
            ]
        ] = []

        for query in queries:
            try:
                source = edgeql.Source.from_string(query)

                ctx = CompileContext(
                    state=state,
                    output_format=enums.IoFormat.BINARY,
                    expected_cardinality_one=False,
                    implicit_limit=implicit_limit,
                    inline_typeids=False,
                    inline_typenames=True,
                    stmt_mode=enums.CompileStatementMode.SINGLE,
                    json_parameters=False,
                    source=source
                )

                result.append(
                    (False, self._compile(ctx=ctx, source=source)[0]))
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

    def compile(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        reflection_cache: Mapping[str, Tuple[str, ...]],
        database_config: Optional[Mapping[str, config.SettingValue]],
        system_config: Optional[Mapping[str, config.SettingValue]],
        source: edgeql.Source,
        sess_modaliases: Optional[immutables.Map],
        sess_config: Optional[immutables.Map],
        io_format: enums.IoFormat,
        expect_one: bool,
        implicit_limit: int,
        inline_typeids: bool,
        inline_typenames: bool,
        stmt_mode: enums.CompileStatementMode,
        protocol_version: Optional[tuple] = None,
        inline_objectids: bool = True,
        json_parameters: bool = False,
    ) -> Tuple[List[dbstate.QueryUnit],
               Optional[dbstate.CompilerConnectionState]]:

        if sess_config is None:
            sess_config = EMPTY_MAP

        if database_config is None:
            database_config = EMPTY_MAP

        if system_config is None:
            system_config = EMPTY_MAP

        if sess_modaliases is None:
            sess_modaliases = DEFAULT_MODULE_ALIASES_MAP

        assert isinstance(sess_modaliases, immutables.Map)
        assert isinstance(sess_config, immutables.Map)

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
            state=state,
            output_format=io_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            stmt_mode=enums.CompileStatementMode(stmt_mode),
            json_parameters=json_parameters,
            source=source,
            protocol_version=protocol_version,
        )

        units = self._compile(ctx=ctx, source=source)
        tx_control = False
        for unit in units:
            if unit.tx_id:
                tx_control = True
                break

        if tx_control:
            return units, ctx.state
        else:
            return units, None

    def compile_in_tx(
        self,
        state: dbstate.CompilerConnectionState,
        txid: int,
        source: edgeql.Source,
        io_format: enums.IoFormat,
        expect_one: bool,
        implicit_limit: int,
        inline_typeids: bool,
        inline_typenames: bool,
        stmt_mode: enums.CompileStatementMode,
        protocol_version: tuple,
        inline_objectids: bool = True,
    ) -> Tuple[List[dbstate.QueryUnit], dbstate.CompilerConnectionState]:
        state.sync_tx(txid)

        ctx = CompileContext(
            state=state,
            output_format=io_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            stmt_mode=enums.CompileStatementMode(stmt_mode),
            source=source,
            protocol_version=protocol_version,
        )

        return self._compile(ctx=ctx, source=source), ctx.state

    def describe_database_dump(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        database_config: immutables.Map[str, config.SettingValue],
        protocol_version: tuple,
    ) -> DumpDescriptor:
        schema = s_schema.ChainedSchema(
            self._std_schema,
            user_schema,
            global_schema
        )

        config_ddl = config.to_edgeql(config.get_settings(), database_config)

        schema_ddl = s_ddl.ddl_text_from_schema(
            schema, include_migrations=True)

        all_objects = schema.get_objects(
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

        for objtype in objtypes:
            if objtype.is_union_type(schema) or objtype.is_view(schema):
                continue
            descriptors.extend(self._describe_object(schema, objtype,
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
            schema_ddl=config_ddl + '\n' + schema_ddl,
            schema_dynamic_ddl=tuple(dynamic_ddl),
            schema_ids=ids,
            blocks=descriptors,
        )

    def _describe_object(
        self,
        schema: s_schema.Schema,
        source: s_obj.Object,
        protocol_version: tuple,
    ) -> List[DumpBlockDescriptor]:

        cols = []
        shape = []
        ptrdesc: List[DumpBlockDescriptor] = []

        if isinstance(source, s_props.Property):
            schema, prop_tuple = s_types.Tuple.from_subtypes(
                schema,
                {
                    'source': schema.get('std::uuid'),
                    'target': source.get_target(schema),
                },
                {'named': True},
            )

            type_data, type_id = sertypes.TypeSerializer.describe(
                schema,
                prop_tuple,
                view_shapes={},
                view_shapes_metadata={},
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

                props[ptr.get_shortname(schema).name] = ptr.get_target(schema)

            schema, link_tuple = s_types.Tuple.from_subtypes(
                schema,
                props,
                {'named': True},
            )

            type_data, type_id = sertypes.TypeSerializer.describe(
                schema,
                link_tuple,
                view_shapes={},
                view_shapes_metadata={},
                follow_links=False,
                protocol_version=protocol_version,
            )

        else:
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
                    ptrdesc.extend(self._describe_object(schema, ptr,
                                                         protocol_version))

            type_data, type_id = sertypes.TypeSerializer.describe(
                schema,
                source,
                view_shapes={source: shape},
                view_shapes_metadata={},
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
            schema_object_class=type(source).get_ql_class(),
            schema_deps=tuple(p.schema_object_id for p in ptrdesc),
            type_desc_id=type_id,
            type_desc=type_data,
            sql_copy_stmt=stmt,
        )] + ptrdesc

    def _check_dump_layout(
        self,
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

    def describe_database_restore(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        dump_server_ver_str: Optional[str],
        schema_ddl: bytes,
        schema_ids: List[Tuple[str, str, bytes]],
        blocks: List[Tuple[bytes, bytes]],  # type_id, typespec
        protocol_version: tuple,
    ) -> RestoreDescriptor:
        schema_object_ids = {
            (
                s_name.name_from_string(name),
                qltype if qltype else None
            ): uuidgen.from_bytes(objid)
            for name, qltype, objid in schema_ids
        }

        # dump_server_ver_str can be None in dumps generated by early
        # EdgeDB alphas.
        if dump_server_ver_str is not None:
            dump_server_ver = verutils.parse_version(dump_server_ver_str)
        else:
            dump_server_ver = None

        state = dbstate.CompilerConnectionState(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            database_config=EMPTY_MAP,
            system_config=EMPTY_MAP,
            cached_reflection=EMPTY_MAP,
        )

        ctx = CompileContext(
            state=state,
            output_format=enums.IoFormat.BINARY,
            expected_cardinality_one=False,
            stmt_mode=enums.CompileStatementMode.ALL,
            compat_ver=dump_server_ver,
            schema_object_ids=schema_object_ids,
            log_ddl_as_migrations=False,
            protocol_version=protocol_version,
        )

        ctx.state.start_tx()

        dump_with_extraneous_computables = (
            dump_server_ver is None
            or dump_server_ver < (1, 0, verutils.VersionStage.ALPHA, 8)
        )

        dump_with_ptr_item_id = dump_with_extraneous_computables

        allow_dml_in_functions = (
            dump_server_ver is None
            or dump_server_ver < (1, 0, verutils.VersionStage.BETA, 1)
        )

        schema_ddl_text = schema_ddl.decode('utf-8')

        if allow_dml_in_functions:
            schema_ddl_text = (
                'CONFIGURE CURRENT DATABASE '
                'SET allow_dml_in_functions := true;\n'
                + schema_ddl_text
            )

        ddl_source = edgeql.Source.from_string(schema_ddl_text)
        units = self._compile(ctx=ctx, source=ddl_source)
        schema = ctx.state.current_tx().get_schema(self._std_schema)

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
                units += self._compile(ctx=ctx, source=ddl_source)

        restore_blocks = []
        tables = []
        for schema_object_id, typedesc in blocks:
            schema_object_id = uuidgen.from_bytes(schema_object_id)
            obj = schema.get_by_id(schema_object_id)
            desc = sertypes.TypeSerializer.parse(typedesc)
            elided_col_set = set()
            mending_desc = []

            if isinstance(obj, s_props.Property):
                assert isinstance(desc, sertypes.NamedTupleDesc)
                desc_ptrs = list(desc.fields.keys())
                cols = {
                    'source': 'source',
                    'target': 'target',
                }

                mending_desc.append(None)
                mending_desc.append(self._get_ptr_mending_desc(schema, obj))

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
                            self._get_ptr_mending_desc(schema, ptr))

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
                            self._get_ptr_mending_desc(schema, ptr))

            else:
                raise AssertionError(
                    f'unexpected object type in restore '
                    f'type descriptor: {obj!r}'
                )

            self._check_dump_layout(
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
        )

    def _get_ptr_mending_desc(
        self,
        schema: s_schema.Schema,
        ptr: s_pointers.Pointer,
    ) -> Optional[DataMendingDescriptor]:
        ptr_type = ptr.get_target(schema)
        if isinstance(ptr_type, (s_types.Array, s_types.Tuple)):
            return self._get_data_mending_desc(schema, ptr_type)
        else:
            return None

    def _get_data_mending_desc(
        self,
        schema: s_schema.Schema,
        typ: s_types.Type,
    ) -> Optional[DataMendingDescriptor]:
        if isinstance(typ, (s_types.Tuple, s_types.Array)):
            elements = tuple(
                self._get_data_mending_desc(schema, element)
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

    def get_config_val(
        self,
        ctx: CompileContext,
        name: str,
    ) -> Any:
        current_tx = ctx.state.current_tx()
        return config.lookup(
            name,
            current_tx.get_session_config(),
            current_tx.get_database_config(),
            current_tx.get_system_config(),
            allow_unrecognized=True,
        )


class DumpDescriptor(NamedTuple):

    schema_ddl: str
    schema_dynamic_ddl: Tuple[str]
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
