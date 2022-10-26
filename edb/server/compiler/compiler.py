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

import immutables

from edb import errors

from edb.server import defines
from edb.pgsql import compiler as pg_compiler

from edb import edgeql
from edb.common import debug
from edb.common import verutils
from edb.common import uuidgen
from edb.common import ast

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote

from edb.ir import staeval as ireval
from edb.ir import ast as irast

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import properties as s_props
from edb.schema import migrations as s_migrations
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.pgsql import ast as pgast
from edb.pgsql import common as pg_common
from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import params as pg_params
from edb.pgsql import patches as pg_patches
from edb.pgsql import types as pg_types

from edb.server import config

from . import dbstate
from . import enums
from . import sertypes
from . import status

if TYPE_CHECKING:
    from edb.server import pgcon


EMPTY_MAP = immutables.Map()


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState:

    user_schema: s_schema.Schema
    global_schema: s_schema.Schema
    cached_reflection: immutables.Map[str, Tuple[str, ...]]


@dataclasses.dataclass(frozen=True)
class CompileContext:

    state: dbstate.CompilerConnectionState
    output_format: enums.OutputFormat
    expected_cardinality_one: bool
    protocol_version: Tuple[int, int]
    skip_first: bool = False
    expect_rollback: bool = False
    json_parameters: bool = False
    schema_reflection_mode: bool = False
    implicit_limit: int = 0
    inline_typeids: bool = False
    inline_typenames: bool = False
    inline_objectids: bool = True
    schema_object_ids: Optional[Mapping[s_name.Name, uuid.UUID]] = None
    source: Optional[edgeql.Source] = None
    backend_runtime_params: pg_params.BackendRuntimeParams = (
        pg_params.get_default_runtime_params())
    compat_ver: Optional[verutils.Version] = None
    bootstrap_mode: bool = False
    internal_schema_mode: bool = False
    log_ddl_as_migrations: bool = True


DEFAULT_MODULE_ALIASES_MAP = immutables.Map(
    {None: defines.DEFAULT_MODULE_ALIAS})
_OUTPUT_FORMAT_MAP = {
    enums.OutputFormat.BINARY: pg_compiler.OutputFormat.NATIVE,
    enums.OutputFormat.JSON: pg_compiler.OutputFormat.JSON,
    enums.OutputFormat.JSON_ELEMENTS: pg_compiler.OutputFormat.JSON_ELEMENTS,
    enums.OutputFormat.NONE: pg_compiler.OutputFormat.NONE,
}

pg_ql = lambda o: pg_common.quote_literal(str(o))


def _convert_format(inp: enums.OutputFormat) -> pg_compiler.OutputFormat:
    try:
        return _OUTPUT_FORMAT_MAP[inp]
    except KeyError:
        raise RuntimeError(f"Output format {inp!r} is not supported")


def compile_edgeql_script(
    compiler: Compiler,
    ctx: CompileContext,
    eql: str,
) -> Tuple[s_schema.Schema, str]:

    sql = compiler._compile_ql_script(ctx, eql)
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
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    expected_cardinality_one: bool = False,
    json_parameters: bool = False,
    schema_reflection_mode: bool = False,
    output_format: enums.OutputFormat = enums.OutputFormat.BINARY,
    bootstrap_mode: bool = False,
    internal_schema_mode: bool = False,
    protocol_version: Tuple[int, int] = defines.CURRENT_PROTOCOL,
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
        protocol_version=protocol_version,
    )

    return ctx


async def get_patch_count(backend_conn: pgcon.PGConnection) -> int:
    """Get the number of applied patches."""
    num_patches = await backend_conn.sql_fetch_val(
        b'''
            SELECT json::json from edgedbinstdata.instdata
            WHERE key = 'num_patches';
        ''',
    )
    num_patches = json.loads(num_patches) if num_patches else 0
    return num_patches


async def load_cached_schema(
    backend_conn: pgcon.PGConnection,
    patches: int,
    key: str,
) -> s_schema.Schema:
    key += pg_patches.get_version_key(patches)
    data = await backend_conn.sql_fetch_val(
        b"""
        SELECT bin FROM edgedbinstdata.instdata
        WHERE key = $1
        """,
        args=[key.encode("utf-8")],
    )
    try:
        return pickle.loads(data)
    except Exception as e:
        raise RuntimeError(
            'could not load std schema pickle') from e


async def load_std_schema(
    backend_conn: pgcon.PGConnection,
    patches: int,
) -> s_schema.Schema:
    return await load_cached_schema(backend_conn, patches, 'stdschema')


async def load_schema_intro_query(
    backend_conn: pgcon.PGConnection,
    patches: int,
    kind: str,
) -> str:
    kind += pg_patches.get_version_key(patches)
    return await backend_conn.sql_fetch_val(
        b"""
        SELECT text FROM edgedbinstdata.instdata
        WHERE key = $1::text;
        """,
        args=[kind.encode("utf-8")],
    )


async def load_schema_class_layout(
    backend_conn: pgcon.PGConnection,
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
        backend_runtime_params: pg_params.BackendRuntimeParams=
            pg_params.get_default_runtime_params(),
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

    async def initialize_from_pg(self, con: pgcon.PGConnection) -> None:
        num_patches = await get_patch_count(con)

        if self._std_schema is None:
            self._std_schema = await load_cached_schema(
                con, num_patches, 'stdschema')

        if self._refl_schema is None:
            self._refl_schema = await load_cached_schema(
                con, num_patches, 'reflschema')

        if self._schema_class_layout is None:
            self._schema_class_layout = await load_schema_class_layout(
                con, num_patches)

        if self._local_intro_query is None:
            self._local_intro_query = await load_schema_intro_query(
                con, num_patches, 'local_intro_query')

        if self._global_intro_query is None:
            self._global_intro_query = await load_schema_intro_query(
                con, num_patches, 'global_intro_query')

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

    def _get_delta_context_args(self, ctx: CompileContext):
        """Get the args need from delta_from_ddl"""
        return dict(
            testmode=self.get_config_val(ctx, '__internal_testmode'),
            allow_dml_in_functions=(
                self.get_config_val(ctx, 'allow_dml_in_functions')),
            schema_object_ids=ctx.schema_object_ids,
            compat_ver=ctx.compat_ver,
        )

    def _new_delta_context(self, ctx: CompileContext):
        return s_delta.CommandContext(
            backend_runtime_params=self._backend_runtime_params,
            stdmode=ctx.bootstrap_mode,
            internal_schema_mode=ctx.internal_schema_mode,
            **self._get_delta_context_args(ctx),
        )

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

        return block, new_types, pgdelta.config_ops

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

                    # We drop first instead of using or_replace, in case
                    # something about the arguments changed.
                    df = pg_dbops.DropFunction(
                        name=func.name, args=func.args, if_exists=True
                    )
                    df.generate(block)

                    cf = pg_dbops.CreateFunction(func)
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
                json_parameters=True,
                schema_reflection_mode=True,
                output_format=enums.OutputFormat.JSON,
                expected_cardinality_one=False,
                bootstrap_mode=ctx.bootstrap_mode,
                protocol_version=ctx.protocol_version,
            )

            source = edgeql.Source.from_string(eql)
            unit_group = self._compile(ctx=newctx, source=source)

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
            argmap = unit_group[0].in_type_args
            if argmap is None:
                argmap = ()

            return sql, argmap

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

    def _assert_not_in_migration_rewrite_block(
        self,
        ctx: CompileContext,
        ql: qlast.Base
    ) -> None:
        """Check that a START MIGRATION REWRITE block is *not* active."""
        current_tx = ctx.state.current_tx()
        mstate = current_tx.get_migration_rewrite_state()
        if mstate is not None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} in a migration rewrite block',
                context=ql.context,
            )

    def _assert_in_migration_rewrite_block(
        self,
        ctx: CompileContext,
        ql: qlast.Base
    ) -> dbstate.MigrationRewriteState:
        """Check that a START MIGRATION REWRITE block *is* active."""
        current_tx = ctx.state.current_tx()
        mstate = current_tx.get_migration_rewrite_state()
        if mstate is None:
            stmt = status.get_status(ql).decode()
            raise errors.QueryError(
                f'cannot execute {stmt} outside of a migration rewrite block',
                context=ql.context,
            )
        return mstate

    def _compile_ql_script(
        self,
        ctx: CompileContext,
        eql: str,
    ) -> str:

        source = edgeql.Source.from_string(eql)
        unit_group = self._compile(ctx=ctx, source=source)

        sql_stmts = []
        for u in unit_group:
            for stmt in u.sql:
                stmt = stmt.strip()
                if not stmt.endswith(b';'):
                    stmt += b';'

                sql_stmts.append(stmt)

        return b'\n'.join(sql_stmts).decode()

    def _get_compile_options(
        self,
        ctx: CompileContext,
    ) -> qlcompiler.CompilerOptions:
        can_have_implicit_fields = (
            ctx.output_format is enums.OutputFormat.BINARY)

        disable_constant_folding = self.get_config_val(
            ctx,
            '__internal_no_const_folding',
        )

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
            constant_folding=not disable_constant_folding,
            json_parameters=ctx.json_parameters,
            implicit_limit=ctx.implicit_limit,
            bootstrap_mode=ctx.bootstrap_mode,
            apply_query_rewrites=(
                not ctx.bootstrap_mode
                and not ctx.schema_reflection_mode
            ),
            apply_user_access_policies=self.get_config_val(
                ctx, 'apply_access_policies'),
            allow_user_specified_id=self.get_config_val(
                ctx, 'allow_user_specified_id') or ctx.schema_reflection_mode,
            testmode=self.get_config_val(ctx, '__internal_testmode'),
            devmode=self._is_dev_instance(),
        )

    def _compile_ql_debug_sql(
        self,
        ctx: CompileContext,
        ql: qlast.Base,
    ) -> dbstate.BaseQuery:
        # XXX: DEBUG HACK Implement DESCRIBE ALTER <STRING> by
        # (eventually) rewriting <STRING> and returning it
        assert self._is_dev_instance()

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        from edb.pgsql import parser as pg_parser
        from edb.pgsql import resolver as pg_resolver
        from edb.pgsql import codegen as pg_codegen

        stmts = pg_parser.parse(ql.source)
        sql_source = ''
        for stmt in stmts:
            resolved = pg_resolver.resolve(stmt, schema)
            source = pg_codegen.generate_source(resolved)
            sql_source += source + ';'

        print(sql_source)

        # Compile the result as a query that just returns the string
        res_ql = edgeql.parse(f'SELECT {qlquote.quote_literal(sql_source)}')
        query = self._compile_ql_query(
            ctx,
            res_ql,
            cacheable=False,
            migration_block_query=True,
        )
        return query

    def _compile_ql_query(
        self,
        ctx: CompileContext,
        ql: qlast.Base,
        *,
        script_info: Optional[irast.ScriptInfo] = None,
        cacheable: bool = True,
        migration_block_query: bool = False,
    ) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()

        schema = current_tx.get_schema(self._std_schema)
        ir = qlcompiler.compile_ast_to_ir(
            ql,
            schema=schema,
            script_info=script_info,
            options=self._get_compile_options(ctx),
        )

        result_cardinality = enums.cardinality_from_ir_value(ir.cardinality)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=(
                debug.flags.edgeql_compile
                or debug.flags.edgeql_compile_sql_text
                or debug.flags.delta_execute
            ),
            expected_cardinality_one=ctx.expected_cardinality_one,
            output_format=_convert_format(ctx.output_format),
            backend_runtime_params=ctx.backend_runtime_params,
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

        in_type_args = None
        params: list[tuple[str, s_obj.Object, bool]] = []
        has_named_params = False
        if ir.params:
            params, in_type_args = self._extract_params(
                ir.params, argmap=argmap, script_info=script_info,
                schema=ir.schema, ctx=ctx)

        globals = None
        if ir.globals:
            globals = [
                (str(glob.global_name), glob.has_present_arg)
                for glob in ir.globals
            ]

        if ctx.output_format is enums.OutputFormat.NONE:
            out_type_id = sertypes.NULL_TYPE_ID
            out_type_data = sertypes.NULL_TYPE_DESC
            result_cardinality = enums.Cardinality.NO_RESULT
        elif ctx.output_format is enums.OutputFormat.BINARY:
            out_type_data, out_type_id = sertypes.TypeSerializer.describe(
                ir.schema, ir.stype,
                ir.view_shapes, ir.view_shapes_metadata,
                inline_typenames=ctx.inline_typenames,
                protocol_version=ctx.protocol_version)
        else:
            out_type_data, out_type_id = \
                sertypes.TypeSerializer.describe_json()

        if ctx.protocol_version >= (0, 12):
            in_type_data, in_type_id = \
                sertypes.TypeSerializer.describe_params(
                    schema=ir.schema,
                    params=params,
                    protocol_version=ctx.protocol_version,
                )
        else:
            # Legacy protocol support - for restoring pre-0.12 dumps
            if params:
                pschema, params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types=collections.OrderedDict(
                        # keep only param_name/param_type
                        [param[:2] for param in params]
                    ),
                    named=has_named_params)
            else:
                pschema, params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types={},
                    named=has_named_params)

            in_type_data, in_type_id = sertypes.TypeSerializer.describe(
                pschema, params_type, {}, {},
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
            globals=globals,
            in_type_id=in_type_id.bytes,
            in_type_data=in_type_data,
            in_type_args=in_type_args,
            out_type_id=out_type_id.bytes,
            out_type_data=out_type_data,
            cacheable=cacheable,
            has_dml=ir.dml_exprs,
        )

    def _extract_params(
        self,
        params: List[irast.Param],
        *,
        schema: s_schema.Schema,
        argmap: Optional[Dict[str, pgast.Param]],
        script_info: Optional[irast.ScriptInfo],
        ctx: CompileContext,
    ) -> Tuple[List[tuple], List[dbstate.Param]]:
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

        oparams = [None] * user_params
        in_type_args = [None] * user_params
        for idx, param in enumerate(params):
            if param.is_sub_param:
                continue
            if argmap is not None:
                sql_param = argmap[param.name]
                idx = sql_param.logical_index - 1
            if idx >= user_params:
                continue

            array_tid = None
            if param.schema_type.is_array():
                el_type = param.schema_type.get_element_type(schema)
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
                param.schema_type,
                param.required,
            )

            if param.sub_params:
                array_tids = []
                for p in param.sub_params.params:
                    if p.schema_type.is_array():
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

        return oparams, in_type_args

    def _compile_and_apply_ddl_stmt(
        self,
        ctx: CompileContext,
        stmt: qlast.DDLOperation,
        source: Optional[edgeql.Source] = None,
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
                (
                    qlast.CreateMigration,
                    qlast.GlobalObjectCommand,
                    qlast.DropMigration,
                ),
            )
        ):
            allow_bare_ddl = self.get_config_val(ctx, 'allow_bare_ddl')
            if allow_bare_ddl != "AlwaysAllow":
                raise errors.QueryError(
                    "bare DDL statements are not allowed in this database",
                    hint="Use the migration commands instead.",
                    details=(
                        f"The `allow_bare_ddl` configuration variable "
                        f"is set to {str(allow_bare_ddl)!r}.  The "
                        f"`edgedb migrate` command normally sets this "
                        f"to avoid accidental schema changes outside of "
                        f"the migration flow."
                    ),
                    context=stmt.context,
                )
            cm = qlast.CreateMigration(
                body=qlast.NestedQLBlock(
                    commands=[stmt],
                ),
                commands=[qlast.SetField(
                    name='generated_by',
                    value=qlast.Path(steps=[
                        qlast.ObjectRef(name='MigrationGeneratedBy',
                                        module='schema'),
                        qlast.Ptr(ptr=qlast.ObjectRef(name='DDLStatement')),
                    ]),
                )],
            )
            return self._compile_and_apply_ddl_stmt(ctx, cm)

        delta = s_ddl.delta_from_ddl(
            stmt,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            **self._get_delta_context_args(ctx),
        )

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta)

        if mstate := current_tx.get_migration_state():
            mstate = mstate._replace(
                accepted_cmds=mstate.accepted_cmds + (stmt,),
            )

            context = self._new_delta_context(ctx)
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
                    mstate = mstate._replace(last_proposed=None)
                else:
                    proposed_stmts = mstate.last_proposed[0].statements
                    ddl_script = '\n'.join(proposed_stmts)

                    if source and source.text() == ddl_script:
                        # The client has confirmed the proposed migration step,
                        # advance the proposed script.
                        mstate = mstate._replace(
                            last_proposed=mstate.last_proposed[1:],
                        )
                    else:
                        # The client replied with a statement that does not
                        # match what was proposed, reset the proposed script
                        # to force script regeneration on next DESCRIBE.
                        mstate = mstate._replace(last_proposed=None)

            current_tx.update_migration_state(mstate)
            current_tx.update_schema(schema)

            return dbstate.DDLQuery(
                sql=(b'SELECT LIMIT 0',),
                user_schema=current_tx.get_user_schema(),
                is_transactional=True,
                single_unit=False,
            )

        # If we are in a migration rewrite, we also don't actually
        # apply the DDL, just record it. (The DDL also needs to be a
        # CreateMigration.)
        if mrstate := current_tx.get_migration_rewrite_state():
            if not isinstance(stmt, qlast.CreateMigration):
                # This will always fail, and gives us the error we need
                self._assert_not_in_migration_rewrite_block(ctx, stmt)

            mrstate = mrstate._replace(accepted_migrations=(
                mrstate.accepted_migrations + (stmt,)
            ))
            current_tx.update_migration_rewrite_state(mrstate)

            context = self._new_delta_context(ctx)
            schema = delta.apply(schema, context=context)

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
        block, new_types, config_ops = self._process_delta(ctx, delta)

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
        create_ext = None
        drop_ext = None
        if isinstance(stmt, qlast.DropDatabase):
            drop_db = stmt.name.name
        elif isinstance(stmt, qlast.CreateDatabase):
            create_db = stmt.name.name
            create_db_template = stmt.template.name if stmt.template else None
        elif isinstance(stmt, qlast.CreateExtension):
            create_ext = stmt.name.name
        elif isinstance(stmt, qlast.DropExtension):
            drop_ext = stmt.name.name

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
            create_ext=create_ext,
            drop_ext=drop_ext,
            has_role_ddl=isinstance(stmt, qlast.RoleCommand),
            ddl_stmt_id=ddl_stmt_id,
            user_schema=current_tx.get_user_schema_if_updated(),
            cached_reflection=current_tx.get_cached_reflection_if_updated(),
            global_schema=current_tx.get_global_schema_if_updated(),
            config_ops=config_ops,
        )

    def _compile_ql_migration(
        self,
        ctx: CompileContext,
        ql: qlast.MigrationCommand,
        in_script: bool,
    ):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        if (
            ctx.expect_rollback
            and not isinstance(
                ql, (qlast.AbortMigration, qlast.AbortMigrationRewrite))
        ):
            # Only allow ABORT MIGRATION to pass when expecting a rollback
            if current_tx.get_migration_state() is None:
                raise errors.TransactionError(
                    'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
                )
            else:
                raise errors.TransactionError(
                    'expected a ROLLBACK or ABORT MIGRATION command'
                )

        if isinstance(ql, qlast.CreateMigration):
            self._assert_not_in_migration_block(ctx, ql)

            query = self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.StartMigration):
            self._assert_not_in_migration_block(ctx, ql)

            if current_tx.is_implicit() and not in_script:
                savepoint_name = None
                tx_cmd = qlast.StartTransaction()
                tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                query = dbstate.MigrationControlQuery(
                    sql=tx_query.sql,
                    action=dbstate.MigrationAction.START,
                    tx_action=tx_query.action,
                    cacheable=False,
                    modaliases=None,
                    single_unit=tx_query.single_unit,
                )
            else:
                savepoint_name = current_tx.start_migration()
                query = dbstate.MigrationControlQuery(
                    sql=(b'SELECT LIMIT 0',),
                    action=dbstate.MigrationAction.START,
                    tx_action=None,
                    cacheable=False,
                    modaliases=None,
                )

            if isinstance(ql.target, qlast.CommittedSchema):
                mrstate = self._assert_in_migration_rewrite_block(ctx, ql)
                target_schema = mrstate.target_schema

            else:
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
                    testmode=(
                        self.get_config_val(ctx, '__internal_testmode')),
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
                    last_proposed=None,
                ),
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

            new_ddl = tuple(
                s_ddl.ddlast_from_delta(
                    schema,
                    mstate.target_schema,
                    diff,
                    testmode=self.get_config_val(ctx, '__internal_testmode'),
                ),
            )
            all_ddl = mstate.accepted_cmds + new_ddl
            mstate = mstate._replace(
                accepted_cmds=all_ddl,
                last_proposed=None,
            )
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

            # We want to make *certain* that the DDL we generate
            # produces the correct schema when applied, so we reload
            # the diff from the AST instead of just relying on the
            # delta tree. We do this check because it is *very
            # important* that we not emit DDL that moves the schema
            # into the wrong state.
            #
            # The actual check for whether the schema matches is done
            # by DESCRIBE CURRENT MIGRATION AS JSON, to populate the
            # 'complete' flag.
            for cmd in new_ddl:
                reloaded_diff = s_ddl.delta_from_ddl(
                    cmd, schema=schema, modaliases=current_tx.get_modaliases(),
                    **self._get_delta_context_args(ctx),
                )
                schema = reloaded_diff.apply(schema, delta_context)
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
                    # Generate uppercase DDL commands for backwards
                    # compatibility with older migration text.
                    text.append(qlcodegen.generate_source(
                        stmt, pretty=True, uppercase=True))

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
                        #
                        # Also generate uppercase DDL commands for
                        # backwards compatibility with older migration
                        # text.
                        qlcodegen.generate_source(
                            stmt, pretty=True, uppercase=True) + ';',
                    )

                if mstate.last_proposed is None:
                    guided_diff = s_ddl.delta_schemas(
                        schema,
                        mstate.target_schema,
                        generate_prompts=True,
                        guidance=mstate.guidance,
                    )
                    if debug.flags.delta_plan:
                        debug.header(
                            'DESCRIBE CURRENT MIGRATION AS JSON delta')
                        debug.dump(guided_diff)

                    proposed_ddl = s_ddl.statements_from_delta(
                        schema,
                        mstate.target_schema,
                        guided_diff,
                        uppercase=True
                    )
                    proposed_steps = []

                    if proposed_ddl:
                        for ddl_text, ddl_ast, top_op in proposed_ddl:
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
                                ddl_ast,
                                schema=schema,
                                modaliases=current_tx.get_modaliases(),
                            )
                            prompt_key2, prompt_text = (
                                top_op2.get_user_prompt())

                            # Similarly, some placeholders may not have made
                            # it into the actual query, so filter them out.
                            used_placeholders = {
                                p.name for p in ast.find_children(
                                    ddl_ast, qlast.Placeholder)
                            }
                            required_user_input = tuple(
                                (k, v) for k, v in (
                                    top_op.get_required_user_input().items())
                                if k in used_placeholders
                            )

                            # The prompt_id still needs to come from
                            # the original op, though, since
                            # orig_cmd_class is lost in ddl.
                            prompt_key, _ = top_op.get_user_prompt()
                            prompt_id = s_delta.get_object_command_id(
                                prompt_key)
                            confidence = top_op.get_annotation('confidence')
                            assert confidence is not None

                            step = dbstate.ProposedMigrationStep(
                                statements=(ddl_text,),
                                confidence=confidence,
                                prompt=prompt_text,
                                prompt_id=prompt_id,
                                data_safe=top_op.is_data_safe(),
                                required_user_input=required_user_input,
                                operation_key=prompt_key2,
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
                    if mstate.last_proposed:
                        proposed_desc = mstate.last_proposed[0].to_json()
                    else:
                        proposed_desc = None

                complete = False
                if proposed_desc is None:
                    diff = s_ddl.delta_schemas(schema, mstate.target_schema)
                    complete = not bool(diff.get_subcommands())
                    if debug.flags.delta_plan:
                        debug.header(
                            'DESCRIBE CURRENT MIGRATION AS JSON mismatch')
                        debug.dump(diff)

                desc = json.dumps({
                    'parent': (
                        str(mstate.parent_migration.get_name(schema))
                        if mstate.parent_migration is not None
                        else 'initial'
                    ),
                    'complete': complete,
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
            if not mstate.last_proposed:
                # XXX: Or should we compute what the proposal would be?
                new_guidance = mstate.guidance
            else:

                last = mstate.last_proposed[0]
                cmdclass_name, mcls, classname, new_name = last.operation_key
                if new_name is None:
                    new_name = classname

                if cmdclass_name.startswith('Create'):
                    new_guidance = mstate.guidance._replace(
                        banned_creations=mstate.guidance.banned_creations | {
                            (mcls, classname),
                        }
                    )
                elif cmdclass_name.startswith('Delete'):
                    new_guidance = mstate.guidance._replace(
                        banned_deletions=mstate.guidance.banned_deletions | {
                            (mcls, classname),
                        }
                    )
                else:
                    new_guidance = mstate.guidance._replace(
                        banned_alters=mstate.guidance.banned_alters | {
                            (mcls, (classname, new_name)),
                        }
                    )

            mstate = mstate._replace(
                guidance=new_guidance,
                last_proposed=None,
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

            if debug.flags.delta_plan:
                debug.header('Commit Migration DDL AST')
                text = []
                for cmd in mstate.accepted_cmds:
                    debug.dump(cmd)
                    text.append(qlcodegen.generate_source(cmd, pretty=True))
                debug.header('Commit Migration DDL Text')
                debug.dump_code(';\n'.join(text) + ';')

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

            # If we are in a migration rewrite, don't actually apply
            # the change, just record it.
            if mrstate := current_tx.get_migration_rewrite_state():
                current_tx.update_schema(mstate.target_schema)
                mrstate = mrstate._replace(accepted_migrations=(
                    mrstate.accepted_migrations + (create_migration,)
                ))
                current_tx.update_migration_rewrite_state(mrstate)

                query = dbstate.MigrationControlQuery(
                    sql=(b'SELECT LIMIT 0',),
                    action=dbstate.MigrationAction.COMMIT,
                    tx_action=None,
                    cacheable=False,
                    modaliases=None,
                )

            else:
                current_tx.update_schema(mstate.initial_schema)
                current_tx.update_migration_state(None)

                ddl_query = self._compile_and_apply_ddl_stmt(
                    ctx,
                    create_migration,
                )

                if mstate.initial_savepoint:
                    current_tx.commit_migration(mstate.initial_savepoint)
                    sql = ddl_query.sql
                    tx_action = None
                else:
                    tx_cmd = qlast.CommitTransaction()
                    tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                    sql = ddl_query.sql + tx_query.sql
                    tx_action = tx_query.action

                query = dbstate.MigrationControlQuery(
                    sql=sql,
                    ddl_stmt_id=ddl_query.ddl_stmt_id,
                    action=dbstate.MigrationAction.COMMIT,
                    tx_action=tx_action,
                    cacheable=False,
                    modaliases=None,
                    single_unit=True,
                    user_schema=ctx.state.current_tx().get_user_schema(),
                    cached_reflection=(
                        current_tx.get_cached_reflection_if_updated()
                    )
                )

        elif isinstance(ql, qlast.AbortMigration):
            mstate = self._assert_in_migration_block(ctx, ql)

            if mstate.initial_savepoint:
                current_tx.abort_migration(mstate.initial_savepoint)
                sql = (b'SELECT LIMIT 0',)
                tx_action = None
            else:
                tx_cmd = qlast.RollbackTransaction()
                tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                sql = tx_query.sql
                tx_action = tx_query.action

            current_tx.update_migration_state(None)
            query = dbstate.MigrationControlQuery(
                sql=sql,
                action=dbstate.MigrationAction.ABORT,
                tx_action=tx_action,
                cacheable=False,
                modaliases=None,
                single_unit=True,
            )

        elif isinstance(ql, qlast.DropMigration):
            self._assert_not_in_migration_block(ctx, ql)

            query = self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.StartMigrationRewrite):
            self._assert_not_in_migration_block(ctx, ql)
            self._assert_not_in_migration_rewrite_block(ctx, ql)

            # Start a transaction if we aren't in one already
            if current_tx.is_implicit() and not in_script:
                savepoint_name = None
                tx_cmd = qlast.StartTransaction()
                tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                query = dbstate.MigrationControlQuery(
                    sql=tx_query.sql,
                    action=dbstate.MigrationAction.START,
                    tx_action=tx_query.action,
                    cacheable=False,
                    modaliases=None,
                    single_unit=tx_query.single_unit,
                )
            else:
                savepoint_name = current_tx.start_migration()
                query = dbstate.MigrationControlQuery(
                    sql=(b'SELECT LIMIT 0',),
                    action=dbstate.MigrationAction.START,
                    tx_action=None,
                    cacheable=False,
                    modaliases=None,
                )

            # Start from an empty schema except for `module default`
            base_schema = s_schema.ChainedSchema(
                self._std_schema,
                s_schema.FlatSchema(),
                current_tx.get_global_schema(),
            )
            base_schema = s_ddl.apply_sdl(
                qlast.Schema(declarations=[
                    qlast.ModuleDeclaration(
                        name=qlast.ObjectRef(name='default'),
                        declarations=[],
                    )
                ]),
                base_schema=base_schema,
                current_schema=base_schema,
            )

            # Set our current schema to be the empty one
            current_tx.update_schema(base_schema)
            current_tx.update_migration_rewrite_state(
                dbstate.MigrationRewriteState(
                    target_schema=schema,
                    initial_savepoint=savepoint_name,
                    accepted_migrations=tuple(),
                ),
            )
        elif isinstance(ql, qlast.CommitMigrationRewrite):
            self._assert_not_in_migration_block(ctx, ql)
            mrstate = self._assert_in_migration_rewrite_block(ctx, ql)

            diff = s_ddl.delta_schemas(schema, mrstate.target_schema)
            if list(diff.get_subcommands()):
                if debug.flags.delta_plan:
                    debug.header("COMMIT MIGRATION REWRITE mismatch")
                    diff.dump()
                raise errors.QueryError(
                    'cannot commit migration rewrite: schema resulting '
                    'from rewrite does not match committed schema',
                    context=ql.context,
                )

            schema = mrstate.target_schema
            current_tx.update_schema(schema)
            current_tx.update_migration_rewrite_state(None)

            cmds = []
            # Now we find all the migrations...
            migrations = s_delta.sort_by_cross_refs(
                schema,
                schema.get_objects(type=s_migrations.Migration),
            )
            for mig in migrations:
                cmds.append(qlast.DropMigration(
                    name=qlast.ObjectRef(name=mig.get_name(schema).name)
                ))
            for cmd in mrstate.accepted_migrations:
                cmd.metadata_only = True
                cmds.append(cmd)

            if debug.flags.delta_plan:
                debug.header('COMMIT MIGRATION REWRITE DDL text')
                for cmd in cmds:
                    cmd.dump_edgeql()

            sqls = []
            for cmd in cmds:
                ddl_query = self._compile_and_apply_ddl_stmt(
                    ctx, cmd
                )
                # We know nothing serious can be in that query
                # except for the SQL, so it's fine to just discard
                # it all.
                sqls.extend(ddl_query.sql)

            if mrstate.initial_savepoint:
                current_tx.commit_migration(mrstate.initial_savepoint)
                tx_action = None
            else:
                tx_cmd = qlast.CommitTransaction()
                tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                sqls.extend(tx_query.sql)
                tx_action = tx_query.action

            query = dbstate.MigrationControlQuery(
                sql=tuple(sqls),
                action=dbstate.MigrationAction.COMMIT,
                tx_action=tx_action,
                cacheable=False,
                modaliases=None,
                single_unit=True,
                user_schema=ctx.state.current_tx().get_user_schema(),
                cached_reflection=(
                    current_tx.get_cached_reflection_if_updated()
                )
            )

        elif isinstance(ql, qlast.AbortMigrationRewrite):
            mrstate = self._assert_in_migration_rewrite_block(ctx, ql)

            if mrstate.initial_savepoint:
                current_tx.abort_migration(mrstate.initial_savepoint)
                sql = (b'SELECT LIMIT 0',)
                tx_action = None
            else:
                tx_cmd = qlast.RollbackTransaction()
                tx_query = self._compile_ql_transaction(ctx, tx_cmd)
                sql = tx_query.sql
                tx_action = tx_query.action

            current_tx.update_migration_state(None)
            current_tx.update_migration_rewrite_state(None)
            query = dbstate.MigrationControlQuery(
                sql=sql,
                action=dbstate.MigrationAction.ABORT,
                tx_action=tx_action,
                cacheable=False,
                modaliases=None,
                single_unit=True,
            )

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
        sp_name = None
        sp_id = None

        if ctx.expect_rollback and not isinstance(
            ql, (qlast.RollbackTransaction, qlast.RollbackToSavepoint)
        ):
            raise errors.TransactionError(
                'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
            )

        if isinstance(ql, qlast.StartTransaction):
            self._assert_not_in_migration_block(ctx, ql)

            ctx.state.start_tx()

            sql = 'START TRANSACTION'
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

            pgname = pg_common.quote_ident(ql.name)
            sql = (f'SAVEPOINT {pgname}'.encode(),)

            cacheable = False
            action = dbstate.TxAction.DECLARE_SAVEPOINT

            sp_name = ql.name
            sp_id = sp_id

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
            sql = (f'ROLLBACK TO SAVEPOINT {pgname};'.encode(),)
            single_unit = True
            cacheable = False
            action = dbstate.TxAction.ROLLBACK_TO_SAVEPOINT
            sp_name = ql.name

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
            sp_name=sp_name,
            sp_id=sp_id,
        )

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.BaseSessionCommand):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        aliases = ctx.state.current_tx().get_modaliases()

        if isinstance(ql, qlast.SessionSetAliasDecl):
            try:
                schema.get_global(s_mod.Module, ql.module)
            except errors.InvalidReferenceError:
                raise errors.UnknownModuleError(
                    f'module {ql.module!r} does not exist') from None

            aliases = aliases.set(ql.alias, ql.module)

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

    def _compile_ql_config_op(self, ctx: CompileContext, ql: qlast.Base):

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema(self._std_schema)

        modaliases = current_tx.get_modaliases()
        session_config = current_tx.get_session_config()
        database_config = current_tx.get_database_config()

        if ql.scope is not qltypes.ConfigScope.SESSION:
            self._assert_not_in_migration_block(ctx, ql)

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
            ),
        )

        globals = None
        if ir.globals:
            globals = [
                (str(glob.global_name), glob.has_present_arg)
                for glob in ir.globals
            ]

        is_backend_setting = bool(getattr(ir, 'backend_setting', None))
        requires_restart = bool(getattr(ir, 'requires_restart', False))

        sql_text, _ = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=(debug.flags.edgeql_compile
                    or debug.flags.edgeql_compile_sql_text),
            backend_runtime_params=ctx.backend_runtime_params,
        )

        sql = (sql_text.encode(),)

        single_unit = False
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

        elif ql.scope in (
                qltypes.ConfigScope.INSTANCE, qltypes.ConfigScope.GLOBAL):
            try:
                config_op = ireval.evaluate_to_config_op(ir, schema=schema)
            except ireval.UnsupportedExpressionError:
                # This is a complex config object operation, the
                # op will be produced by the compiler as json.
                config_op = None

            single_unit = True
        else:
            raise AssertionError(f'unexpected configuration scope: {ql.scope}')

        return dbstate.SessionStateQuery(
            sql=sql,
            is_backend_setting=is_backend_setting,
            config_scope=ql.scope,
            requires_restart=requires_restart,
            single_unit=single_unit,
            config_op=config_op,
            globals=globals,
        )

    def _compile_dispatch_ql(
        self,
        ctx: CompileContext,
        ql: qlast.Base,
        source: Optional[edgeql.Source] = None,
        *,
        in_script: bool=False,
        script_info: Optional[irast.ScriptInfo] = None,
    ) -> Tuple[dbstate.BaseQuery, enums.Capability]:
        if isinstance(ql, qlast.MigrationCommand):
            query = self._compile_ql_migration(
                ctx, ql, in_script=in_script,
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

        elif isinstance(ql, (qlast.DatabaseCommand, qlast.DDL)):
            return (
                self._compile_and_apply_ddl_stmt(ctx, ql, source=source),
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

        elif isinstance(ql, qlast.SQLDebugStmt):
            return (
                self._compile_ql_debug_sql(ctx, ql),
                enums.Capability.SESSION_CONFIG,
            )

        elif isinstance(ql, qlast.ConfigOp):
            if ql.scope is qltypes.ConfigScope.SESSION:
                capability = enums.Capability.SESSION_CONFIG
            elif ql.scope is qltypes.ConfigScope.GLOBAL:
                capability = enums.Capability.SET_GLOBAL
            else:
                capability = enums.Capability.PERSISTENT_CONFIG
            return (
                self._compile_ql_config_op(ctx, ql),
                capability,
            )

        else:
            query = self._compile_ql_query(ctx, ql, script_info=script_info)
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
    ) -> dbstate.QueryUnitGroup:
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
    ) -> dbstate.QueryUnitGroup:

        default_cardinality = enums.Cardinality.NO_RESULT
        statements = edgeql.parse_block(source)
        statements_len = len(statements)

        if ctx.skip_first:
            statements = statements[1:]
            if not statements:  # pragma: no cover
                # Shouldn't ever happen as the server tracks the number
                # of statements (via the "try_compile_rollback()" method)
                # before using skip_first.
                raise errors.ProtocolError(
                    f'no statements to compile in skip_first mode')

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
                schema=ctx.state.current_tx().get_schema(self._std_schema),
                options=self._get_compile_options(ctx)
            )
            non_trailing_ctx = dataclasses.replace(
                ctx, output_format=enums.OutputFormat.NONE)

        for i, stmt in enumerate(statements):
            is_trailing_stmt = i == statements_len - 1
            stmt_ctx = ctx if is_trailing_stmt else non_trailing_ctx
            comp, capabilities = self._compile_dispatch_ql(
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

                if not comp.single_unit:
                    raise errors.InternalServerError(
                        'non-transactional compilation units must '
                        'be single-unit'
                    )

                unit.is_transactional = False

            if isinstance(comp, dbstate.Query):
                unit.sql = comp.sql
                unit.globals = comp.globals
                unit.in_type_args = comp.in_type_args

                unit.sql_hash = comp.sql_hash

                unit.out_type_data = comp.out_type_data
                unit.out_type_id = comp.out_type_id
                unit.in_type_data = comp.in_type_data
                unit.in_type_id = comp.in_type_id

                unit.cacheable = comp.cacheable

                if is_trailing_stmt:
                    unit.cardinality = comp.cardinality

            elif isinstance(comp, dbstate.SimpleQuery):
                unit.sql = comp.sql
                unit.in_type_args = comp.in_type_args

            elif isinstance(comp, dbstate.DDLQuery):
                unit.sql = comp.sql
                unit.create_db = comp.create_db
                unit.drop_db = comp.drop_db
                unit.create_db_template = comp.create_db_template
                unit.create_ext = comp.create_ext
                unit.drop_ext = comp.drop_ext
                unit.has_role_ddl = comp.has_role_ddl
                unit.ddl_stmt_id = comp.ddl_stmt_id
                if comp.user_schema is not None:
                    unit.user_schema = pickle.dumps(comp.user_schema, -1)
                if comp.cached_reflection is not None:
                    unit.cached_reflection = \
                        pickle.dumps(comp.cached_reflection, -1)
                if comp.global_schema is not None:
                    unit.global_schema = pickle.dumps(comp.global_schema, -1)

                unit.config_ops.extend(comp.config_ops)

            elif isinstance(comp, dbstate.TxControlQuery):
                unit.sql = comp.sql
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
                    unit.sp_name = comp.sp_name
                elif comp.action is dbstate.TxAction.DECLARE_SAVEPOINT:
                    unit.tx_savepoint_declare = True
                    unit.sp_name = comp.sp_name
                    unit.sp_id = comp.sp_id

            elif isinstance(comp, dbstate.MigrationControlQuery):
                unit.sql = comp.sql
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
                    unit.set_global = True

                elif comp.config_scope is qltypes.ConfigScope.DATABASE:
                    unit.database_config = True

                if comp.is_backend_setting:
                    unit.backend_config = True
                if comp.requires_restart:
                    unit.config_requires_restart = True

                unit.modaliases = ctx.state.current_tx().get_modaliases()

                if comp.config_op is not None:
                    unit.config_ops.append(comp.config_op)

                unit.has_set = True

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

            params, in_type_args = self._extract_params(
                list(script_info.params.values()),
                argmap=None, script_info=None, schema=script_info.schema,
                ctx=ctx)

            if ctx.protocol_version >= (0, 12):
                in_type_data, in_type_id = \
                    sertypes.TypeSerializer.describe_params(
                        schema=script_info.schema,
                        params=params,
                        protocol_version=ctx.protocol_version,
                    )
                rv.in_type_id = in_type_id.bytes
                rv.in_type_args = in_type_args
                rv.in_type_data = in_type_data

        for unit in rv:  # pragma: no cover
            if ctx.protocol_version < (0, 12):
                if unit.in_type_id == sertypes.NULL_TYPE_ID.bytes:
                    unit.in_type_id = sertypes.EMPTY_TUPLE_ID.bytes
                    unit.in_type_data = sertypes.EMPTY_TUPLE_DESC

            # Sanity checks

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

    # API

    @staticmethod
    def try_compile_rollback(
        eql: Union[edgeql.Source, bytes], protocol_version: tuple[int, int]
    ):
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
                tx_savepoint_rollback=stmt.name,
                sp_name=stmt.name,
                cacheable=False)

        if unit is not None:
            if protocol_version < (0, 12):
                if unit.in_type_id == sertypes.NULL_TYPE_ID.bytes:
                    unit.in_type_id = sertypes.EMPTY_TUPLE_ID.bytes
                    unit.in_type_data = sertypes.EMPTY_TUPLE_DESC

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
        reflection_cache: Mapping[str, Tuple[str, ...]],
        database_config: Mapping[str, config.SettingValue],
        system_config: Mapping[str, config.SettingValue],
        queries: List[str],
        protocol_version: Tuple[int, int],
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
            output_format=enums.OutputFormat.BINARY,
            expected_cardinality_one=False,
            implicit_limit=implicit_limit,
            inline_typenames=True,
            json_parameters=False,
            protocol_version=protocol_version
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
                    output_format=enums.OutputFormat.BINARY,
                    expected_cardinality_one=False,
                    implicit_limit=implicit_limit,
                    inline_typeids=False,
                    inline_typenames=True,
                    json_parameters=False,
                    source=source,
                    protocol_version=protocol_version
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
        output_format: enums.OutputFormat,
        expect_one: bool,
        implicit_limit: int,
        inline_typeids: bool,
        inline_typenames: bool,
        skip_first: bool,
        protocol_version: Tuple[int, int],
        inline_objectids: bool = True,
        json_parameters: bool = False,
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
            output_format=output_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            skip_first=skip_first,
            json_parameters=json_parameters,
            source=source,
            protocol_version=protocol_version,
        )

        unit_group = self._compile(ctx=ctx, source=source)
        tx_started = False
        for unit in unit_group:
            if unit.tx_id:
                tx_started = True
                break

        if tx_started:
            return unit_group, ctx.state
        else:
            return unit_group, None

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
        skip_first: bool,
        protocol_version: Tuple[int, int],
        inline_objectids: bool = True,
        json_parameters: bool = False,
        expect_rollback: bool = False,
    ) -> Tuple[dbstate.QueryUnitGroup, dbstate.CompilerConnectionState]:
        if (
            expect_rollback and
            state.current_tx().id != txid and
            not state.can_sync_to_savepoint(txid)
        ):
            # This is a special case when COMMIT MIGRATION fails, the compiler
            # doesn't have the right transaction state, so we just roll back.
            return (
                self.try_compile_rollback(source, protocol_version)[0], state
            )
        else:
            state.sync_tx(txid)

        ctx = CompileContext(
            state=state,
            output_format=output_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            inline_typeids=inline_typeids,
            inline_typenames=inline_typenames,
            inline_objectids=inline_objectids,
            skip_first=skip_first,
            source=source,
            protocol_version=protocol_version,
            json_parameters=json_parameters,
            expect_rollback=expect_rollback,
        )

        return self._compile(ctx=ctx, source=source), ctx.state

    def describe_database_dump(
        self,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        database_config: immutables.Map[str, config.SettingValue],
        protocol_version: Tuple[int, int],
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
        protocol_version: Tuple[int, int],
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
        protocol_version: Tuple[int, int],
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

        if (
            (dump_server_ver.major, dump_server_ver.minor) == (1, 0)
            and dump_server_ver.stage is verutils.VersionStage.DEV
        ):
            # Pre-1.0 releases post RC3 have DEV in their stage,
            # but for compatibility comparisons below we need to revert
            # to the pre-1.0-rc3 layout
            dump_server_ver = dump_server_ver._replace(
                stage=verutils.VersionStage.RC,
                stage_no=3,
                local=(
                    ('dev', dump_server_ver.stage_no) + dump_server_ver.local
                ),
            )

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
            output_format=enums.OutputFormat.BINARY,
            expected_cardinality_one=False,
            compat_ver=dump_server_ver,
            schema_object_ids=schema_object_ids,
            log_ddl_as_migrations=False,
            protocol_version=protocol_version,
        )

        ctx.state.start_tx()

        dump_with_extraneous_computables = (
            (
                dump_server_ver is None
                or dump_server_ver < (1, 0, verutils.VersionStage.ALPHA, 8)
            )
            and dump_server_ver.stage is not verutils.VersionStage.DEV
        )

        dump_with_ptr_item_id = dump_with_extraneous_computables

        allow_dml_in_functions = (
            (
                dump_server_ver is None
                or dump_server_ver < (1, 0, verutils.VersionStage.BETA, 1)
            )
            and dump_server_ver.stage is not verutils.VersionStage.DEV
        )

        schema_ddl_text = schema_ddl.decode('utf-8')

        if allow_dml_in_functions:
            schema_ddl_text = (
                'CONFIGURE CURRENT DATABASE '
                'SET allow_dml_in_functions := true;\n'
                + schema_ddl_text
            )

        ddl_source = edgeql.Source.from_string(schema_ddl_text)
        units = self._compile(ctx=ctx, source=ddl_source).units
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
                units += self._compile(ctx=ctx, source=ddl_source).units

        restore_blocks = []
        tables = []
        for schema_object_id, typedesc in blocks:
            schema_object_id = uuidgen.from_bytes(schema_object_id)
            obj = schema.get_by_id(schema_object_id)
            desc = sertypes.TypeSerializer.parse(typedesc, protocol_version)
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

    def _is_dev_instance(self) -> bool:
        # Determine whether we are on a dev instance by the presence
        # of a test schema element.
        return bool(self._std_schema.get('cfg::TestSessionConfig', None))

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
