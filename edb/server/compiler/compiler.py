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
import uuid

import asyncpg
import immutables

from edb import errors
from edb import _edgeql_rust

from edb.server import defines
from edb.server import tokenizer
from edb.pgsql import compiler as pg_compiler

from edb import edgeql
from edb.common import debug
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
from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import modules as s_mod
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.pgsql import ast as pg_ast
from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import codegen as pg_codegen
from edb.pgsql import common as pg_common
from edb.pgsql import types as pg_types

from edb.server import config

from . import dbstate
from . import enums
from . import errormech
from . import sertypes
from . import status


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState:

    dbver: bytes
    schema: s_schema.Schema
    cached_reflection: immutables.Map[str, Tuple[str, ...]]


@dataclasses.dataclass(frozen=True)
class BackendInstanceParams:

    #: Explicit superuser role for instances where
    #: direct creation of SUPERUSER roles is not allowed.
    explicit_superuser_role: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class CompileContext:

    state: dbstate.CompilerConnectionState
    output_format: enums.IoFormat
    expected_cardinality_one: bool
    stmt_mode: enums.CompileStatementMode
    json_parameters: bool = False
    schema_reflection_mode: bool = False
    implicit_limit: int = 0
    schema_object_ids: Optional[Mapping[str, uuid.UUID]] = None
    first_extracted_var: Optional[int] = None
    backend_instance_params: BackendInstanceParams = BackendInstanceParams()


EMPTY_MAP = immutables.Map()
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
    new_schema = ctx.state.current_tx().get_schema()

    return new_schema, sql


def new_compiler(
    *,
    std_schema: s_schema.Schema,
    reflection_schema: s_schema.Schema,
    schema_class_layout: Dict[Type[s_obj.Object], s_refl.SchemaTypeLayout],
    bootstrap_mode: bool = False,
) -> Compiler:
    """Create and return an ad-hoc compiler instance."""

    compiler = Compiler(None)
    compiler._std_schema = std_schema
    compiler._refl_schema = reflection_schema
    compiler._bootstrap_mode = bootstrap_mode
    compiler._schema_class_layout = schema_class_layout

    return compiler


def new_compiler_context(
    schema: s_schema.Schema,
    *,
    single_statement: bool = False,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    expected_cardinality_one: bool = False,
    json_parameters: bool = False,
    schema_reflection_mode: bool = False,
    output_format: enums.IoFormat = enums.IoFormat.BINARY,
) -> CompileContext:
    """Create and return an ad-hoc compiler context."""

    state = dbstate.CompilerConnectionState(
        0,
        schema,
        immutables.Map(modaliases) if modaliases else EMPTY_MAP,
        EMPTY_MAP,
        enums.Capability.ALL,
        EMPTY_MAP,
    )

    ctx = CompileContext(
        state=state,
        output_format=output_format,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=json_parameters,
        schema_reflection_mode=schema_reflection_mode,
        stmt_mode=(
            enums.CompileStatementMode.SINGLE
            if single_statement else enums.CompileStatementMode.ALL
        ),
    )

    return ctx


async def load_cached_schema(backend_conn, key) -> s_schema.Schema:
    data = await backend_conn.fetchval(
        f'SELECT edgedbinstdata.__syscache_{key}();')
    try:
        return pickle.loads(data)
    except Exception as e:
        raise RuntimeError(
            'could not load std schema pickle') from e


async def load_std_schema(backend_conn) -> s_schema.Schema:
    return await load_cached_schema(backend_conn, 'stdschema')


async def load_schema_intro_query(backend_conn) -> str:
    return json.loads(await backend_conn.fetchval(
        'SELECT edgedbinstdata.__syscache_introquery();'))


async def load_schema_class_layout(backend_conn) -> s_schema.Schema:
    data = await backend_conn.fetchval(
        'SELECT edgedbinstdata.__syscache_classlayout();',
    )
    try:
        return pickle.loads(data)
    except Exception as e:
        raise RuntimeError(
            'could not load schema class layout pickle') from e


class BaseCompiler:

    _connect_args: dict
    _dbname: Optional[str]
    _cached_db: Optional[CompilerDatabaseState]

    def __init__(
        self,
        connect_args: dict,
        *,
        backend_instance_params: BackendInstanceParams = (
            BackendInstanceParams()),
    ):
        self._connect_args = connect_args
        self._dbname = None
        self._cached_db = None
        self._std_schema = None
        self._refl_schema = None
        self._config_spec = None
        self._schema_class_layout = None
        self._intro_query = None
        self._backend_instance_params = backend_instance_params

    def _hash_sql(self, sql: bytes, **kwargs: bytes):
        h = hashlib.sha1(sql)
        for param, val in kwargs.items():
            h.update(param.encode('latin1'))
            h.update(val)
        return h.hexdigest().encode('latin1')

    def _wrap_schema(
        self,
        dbver: int,
        schema: s_schema.Schema,
        cached_reflection: immutables.Map[str, Tuple[str, ...]],
    ) -> CompilerDatabaseState:
        return CompilerDatabaseState(
            dbver=dbver,
            schema=schema,
            cached_reflection=cached_reflection,
        )

    async def new_connection(self):
        con_args = self._connect_args.copy()
        con_args['database'] = self._dbname
        try:
            return await asyncpg.connect(**con_args)
        except asyncpg.InvalidCatalogNameError as ex:
            raise errors.AuthenticationError(str(ex)) from ex
        except Exception as ex:
            raise errors.InternalServerError(str(ex)) from ex

    async def introspect(
        self,
        connection: asyncpg.Connection,
    ) -> s_schema.Schema:
        data = await connection.fetch(self._intro_query)
        return s_refl.parse_into(
            schema=self._std_schema,
            data=[r[0] for r in data],
            schema_class_layout=self._schema_class_layout,
        )

    async def _load_reflection_cache(
        self,
        connection: asyncpg.Connection,
    ) -> FrozenSet[str]:
        data = await connection.fetch('''
            SELECT
                eql_hash,
                argnames
            FROM
                ROWS FROM(edgedb._get_cached_reflection())
                    AS t(eql_hash text, argnames text[])
        ''')

        return immutables.Map({
            r['eql_hash']: tuple(r['argnames']) for r in data
        })

    async def _get_database(self, dbver: bytes) -> CompilerDatabaseState:
        if self._cached_db is not None and self._cached_db.dbver == dbver:
            return self._cached_db

        self._cached_db = None

        con = await self.new_connection()
        try:
            await self.ensure_initialized(con)
            schema = await self.introspect(con)
            cached_reflection = await self._load_reflection_cache(con)
            db = self._wrap_schema(dbver, schema, cached_reflection)
            self._cached_db = db
            return db
        finally:
            await con.close()

    async def ensure_initialized(self, con: asyncpg.Connection) -> None:
        if self._std_schema is None:
            self._std_schema = await load_cached_schema(con, 'stdschema')

        if self._refl_schema is None:
            self._refl_schema = await load_cached_schema(con, 'reflschema')

        if self._schema_class_layout is None:
            self._schema_class_layout = await load_schema_class_layout(con)

        if self._intro_query is None:
            self._intro_query = await load_schema_intro_query(con)

        if self._config_spec is None:
            self._config_spec = config.load_spec_from_schema(
                self._std_schema)
            config.set_settings(self._config_spec)

    def get_std_schema(self) -> s_schema.Schema:
        if self._std_schema is None:
            raise AssertionError('compiler is not initialized')
        return self._std_schema

    # API

    async def connect(
        self,
        dbname: str,
        dbver: bytes
    ) -> CompilerDatabaseState:
        self._dbname = dbname
        self._cached_db = None
        await self._get_database(dbver)


class Compiler(BaseCompiler):

    def __init__(
        self,
        connect_args: dict,
        *,
        backend_instance_params: BackendInstanceParams = (
            BackendInstanceParams()),
    ):
        super().__init__(
            connect_args,
            backend_instance_params=backend_instance_params,
        )

        self._current_db_state = None
        self._bootstrap_mode = False

    def _in_testmode(self, ctx: CompileContext):
        current_tx = ctx.state.current_tx()
        session_config = current_tx.get_session_config()

        return config.lookup(
            config.get_settings(),
            '__internal_testmode',
            session_config,
            allow_unrecognized=True)

    def _new_delta_context(self, ctx: CompileContext):
        context = s_delta.CommandContext()
        context.testmode = self._in_testmode(ctx)
        context.stdmode = self._bootstrap_mode
        context.schema_object_ids = ctx.schema_object_ids
        context.backend_superuser_role = (
            self._backend_instance_params.explicit_superuser_role)
        return context

    def _process_delta(self, ctx: CompileContext, delta):
        """Adapt and process the delta command."""

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        if debug.flags.delta_plan:
            debug.header('Canonical Delta Plan')
            debug.dump(delta, schema=schema)

        delta = pg_delta.CommandMeta.adapt(delta)
        context = self._new_delta_context(ctx)
        schema = delta.apply(schema, context)
        current_tx.update_schema(schema)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(delta, schema=schema)

        db_cmd = any(
            isinstance(c, s_db.DatabaseCommand)
            for c in delta.get_subcommands()
        )

        if db_cmd:
            block = pg_dbops.SQLBlock()
            new_types = frozenset()
        else:
            block = pg_dbops.PLTopBlock()
            new_types = frozenset(str(tid) for tid in delta.new_types)

        # Generate SQL DDL for the delta.
        delta.generate(block)

        # Generate schema storage SQL (DML into schema storage tables).
        subblock = block.add_block()
        self._compile_schema_storage_in_delta(ctx, delta, subblock)

        return block, new_types

    def _compile_schema_storage_in_delta(
        self,
        ctx: CompileContext,
        delta: s_delta.Command,
        block: pg_dbops.SQLBlock,
        *,
        is_internal_reflection: bool = False,
        stdmode: bool = False,
    ):

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        meta_blocks: List[Tuple[str, Dict[str, Any]]] = []

        s_refl.write_meta(
            delta,
            classlayout=self._schema_class_layout,
            schema=schema,
            context=s_delta.CommandContext(),
            blocks=meta_blocks,
            is_internal_reflection=is_internal_reflection,
            stdmode=stdmode,
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

        schema = ctx.state.current_tx().get_schema()

        try:
            # Switch to the shadow introspection/reflection schema.
            ctx.state.current_tx().update_schema(self._refl_schema)

            newctx = CompileContext(
                state=ctx.state,
                stmt_mode=enums.CompileStatementMode.SINGLE,
                json_parameters=True,
                schema_reflection_mode=True,
                output_format=enums.IoFormat.JSON,
                expected_cardinality_one=False,
            )

            return self._compile_ql_script(newctx, eql)

        finally:
            # Restore the regular schema.
            ctx.state.current_tx().update_schema(schema)

    def _compile_ql_script(
        self,
        ctx: CompileContext,
        eql: str,
    ) -> Tuple[str, Dict[str, int]]:

        tokens = _edgeql_rust.tokenize(eql)
        units = self._compile(ctx=ctx, tokens=tokens)

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
        cacheable: bool = True,
    ) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()
        session_config = current_tx.get_session_config()

        native_out_format = (
            ctx.output_format is enums.IoFormat.BINARY
        )

        single_stmt_mode = ctx.stmt_mode is enums.CompileStatementMode.SINGLE

        implicit_fields = (
            native_out_format and
            single_stmt_mode
        )

        disable_constant_folding = config.lookup(
            config.get_settings(),
            '__internal_no_const_folding',
            session_config,
            allow_unrecognized=True)

        # the capability to execute transaction or session control
        # commands indicates that session mode is available
        session_mode = ctx.state.capability & (enums.Capability.TRANSACTION |
                                               enums.Capability.SESSION)
        ir = qlcompiler.compile_ast_to_ir(
            ql,
            schema=current_tx.get_schema(),
            options=qlcompiler.CompilerOptions(
                modaliases=current_tx.get_modaliases(),
                implicit_tid_in_shapes=implicit_fields,
                implicit_id_in_shapes=implicit_fields,
                constant_folding=not disable_constant_folding,
                json_parameters=ctx.json_parameters,
                implicit_limit=ctx.implicit_limit,
                session_mode=session_mode,
                allow_writing_protected_pointers=ctx.schema_reflection_mode,
                introspection_schema_rewrites=not ctx.schema_reflection_mode,
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
            pretty=debug.flags.edgeql_compile or debug.flags.delta_execute,
            expected_cardinality_one=ctx.expected_cardinality_one,
            output_format=_convert_format(ctx.output_format),
        )

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

        if single_stmt_mode:
            if native_out_format:
                out_type_data, out_type_id = sertypes.TypeSerializer.describe(
                    ir.schema, ir.stype,
                    ir.view_shapes, ir.view_shapes_metadata)
            else:
                out_type_data, out_type_id = \
                    sertypes.TypeSerializer.describe_json()

            in_type_args = None

            if ir.params:
                first_param = next(iter(ir.params))
                named = not first_param.name.isdecimal()
                if ctx.first_extracted_var is not None:
                    user_params = ctx.first_extracted_var
                else:
                    user_params = len(ir.params)

                subtypes = [None] * user_params
                in_type_args = [None] * user_params
                for param in ir.params:
                    sql_param = argmap[param.name]

                    idx = sql_param.index - 1
                    if(ctx.first_extracted_var is not None and
                            idx >= ctx.first_extracted_var):
                        continue

                    array_tid = None
                    if param.schema_type.is_array():
                        el_type = param.schema_type.get_element_type(ir.schema)
                        array_tid = el_type.get_backend_id(ir.schema)
                        if array_tid is None:
                            assert array_tid is not None

                    subtypes[idx] = (param.name, param.schema_type)
                    in_type_args[idx] = dbstate.Param(
                        name=param.name,
                        required=sql_param.required,
                        array_tid=array_tid,
                    )

                ir.schema, params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types=collections.OrderedDict(subtypes),
                    named=named)

            else:
                ir.schema, params_type = s_types.Tuple.create(
                    ir.schema, element_types={}, named=False)

            in_type_data, in_type_id = sertypes.TypeSerializer.describe(
                ir.schema, params_type, {}, {})

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
            )

        else:
            if ir.params:
                raise errors.QueryError(
                    'EdgeQL script queries cannot accept parameters')

            return dbstate.SimpleQuery(sql=(sql_bytes,))

    def _compile_and_apply_ddl_stmt(
        self,
        ctx: CompileContext,
        stmt: qlast.DDLOperation,
    ) -> dbstate.DDLQuery:
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        delta = s_ddl.delta_from_ddl(
            stmt,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=self._in_testmode(ctx),
            schema_object_ids=ctx.schema_object_ids,
        )

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta)

        if mstate := current_tx.get_migration_state():
            mstate = mstate._replace(
                current_ddl=mstate.current_ddl + (stmt,),
            )

            context = self._new_delta_context(ctx)
            schema = delta.apply(schema, context=context)
            current_tx.update_migration_state(mstate)
            current_tx.update_schema(schema)

            return dbstate.DDLQuery(
                sql=(b'SELECT LIMIT 0',),
                is_transactional=True,
                single_unit=False,
            )

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = current_tx.get_schema()
        context = self._new_delta_context(ctx)
        delta.apply(test_schema, context=context)
        delta.canonical = True

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        block, new_types = self._process_delta(ctx, delta)

        is_transactional = block.is_transactional()
        if not is_transactional:
            sql = tuple(stmt.encode('utf-8')
                        for stmt in block.get_statements())
        else:
            sql = (block.to_string().encode('utf-8'),)

        if debug.flags.delta_execute:
            debug.header('Delta Script')
            debug.dump_code(b'\n'.join(sql), lexer='sql')

        return dbstate.DDLQuery(
            sql=sql,
            is_transactional=is_transactional,
            single_unit=not is_transactional,
            new_types=new_types,
        )

    def _compile_ql_migration(self, ctx: CompileContext, ql: qlast.Migration):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        if isinstance(ql, qlast.CreateMigration):
            query = self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.StartMigration):
            if current_tx.is_implicit():
                savepoint_name = None
                tx_cmd = qlast.StartTransaction()
            else:
                savepoint_name = str(uuid.uuid4())
                tx_cmd = qlast.DeclareSavepoint(name=savepoint_name)

            tx_query = self._compile_ql_transaction(ctx, tx_cmd)
            assert self._std_schema is not None
            target_schema = s_ddl.apply_sdl(
                ql.target,
                base_schema=self._std_schema,
                current_schema=schema,
            )

            current_tx.update_migration_state(
                dbstate.MigrationState(
                    initial_schema=schema,
                    initial_savepoint=savepoint_name,
                    target_schema=target_schema,
                    current_ddl=tuple(),
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
            mstate = current_tx.get_migration_state()
            if mstate is None:
                raise errors.QueryError(
                    'unexpected POPULATE MIGRATION:'
                    ' not currently in a migration block',
                    context=ql.context,
                )

            diff = s_ddl.delta_schemas(schema, mstate.target_schema)
            new_ddl = s_ddl.ddlast_from_delta(mstate.target_schema, diff)
            all_ddl = mstate.current_ddl + new_ddl
            if not mstate.current_ddl:
                mstate = mstate._replace(current_ddl=all_ddl, auto_diff=diff)
            else:
                mstate = mstate._replace(current_ddl=all_ddl)
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
            mstate = current_tx.get_migration_state()
            if mstate is None:
                raise errors.QueryError(
                    'unexpected DESCRIBE CURRENT MIGRATION:'
                    ' not currently in a migration block',
                    context=ql.context,
                )

            if ql.language is qltypes.DescribeLanguage.DDL:
                text = []
                for stmt in mstate.current_ddl:
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
                )

            elif ql.language is qltypes.DescribeLanguage.JSON:
                confirmed = []
                for stmt in mstate.current_ddl:
                    confirmed.append(
                        qlcodegen.generate_source(stmt, pretty=True),
                    )

                diff = s_ddl.delta_schemas(schema, mstate.target_schema)
                proposed = s_ddl.statements_from_delta(
                    mstate.target_schema,
                    diff,
                )

                if proposed:
                    proposed_desc = [{
                        'statements': [{
                            'text': proposed[0],
                        }],
                        'confidence': 1.0,
                    }]
                else:
                    proposed_desc = []

                desc = json.dumps({
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
                )

            else:
                raise AssertionError(
                    f'DESCRIBE CURRENT MIGRATION AS {ql.language}'
                    f' is not implemented'
                )

        elif isinstance(ql, qlast.CommitMigration):
            mstate = current_tx.get_migration_state()
            if mstate is None:
                raise errors.QueryError(
                    'unexpected POPULATE MIGRATION:'
                    ' not currently in a migration block',
                    context=ql.context,
                )

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
                parent=last_migration_ref,
                commands=mstate.current_ddl,
                auto_diff=mstate.auto_diff,
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
                new_types=ddl_query.new_types,
                action=dbstate.MigrationAction.COMMIT,
                tx_action=tx_query.action,
                cacheable=False,
                modaliases=None,
                single_unit=True,
            )

        elif isinstance(ql, qlast.AbortMigration):
            mstate = current_tx.get_migration_state()
            if mstate is None:
                raise errors.QueryError(
                    'unexpected ABORT MIGRATION:'
                    ' not currently in a migration block',
                    context=ql.context,
                )

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

        if isinstance(ql, qlast.StartTransaction):
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

            if not self._bootstrap_mode:
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
            modaliases=modaliases)

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.BaseSessionCommand):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        aliases = ctx.state.current_tx().get_modaliases()

        sqlbuf = []

        alias_tpl = lambda alias, module: f'''
            INSERT INTO _edgecon_state(name, value, type)
            VALUES (
                {pg_ql(alias or '')},
                {pg_ql(module)},
                'A'
            )
            ON CONFLICT (name, type) DO
            UPDATE
                SET value = {pg_ql(module)};
        '''.encode()

        if isinstance(ql, qlast.SessionSetAliasDecl):
            try:
                schema.get_global(s_mod.Module, ql.module)
            except errors.InvalidReferenceError:
                raise errors.UnknownModuleError(
                    f'module {ql.module!r} does not exist') from None

            aliases = aliases.set(ql.alias, ql.module)

            if not self._bootstrap_mode:
                sql = alias_tpl(ql.alias, ql.module)
                sqlbuf.append(sql)

        elif isinstance(ql, qlast.SessionResetModule):
            aliases = aliases.set(None, defines.DEFAULT_MODULE_ALIAS)

            if not self._bootstrap_mode:
                sql = alias_tpl('', defines.DEFAULT_MODULE_ALIAS)
                sqlbuf.append(sql)

        elif isinstance(ql, qlast.SessionResetAllAliases):
            aliases = DEFAULT_MODULE_ALIASES_MAP

            if not self._bootstrap_mode:
                sqlbuf.append(
                    b"DELETE FROM _edgecon_state s WHERE s.type = 'A';")
                sqlbuf.append(
                    alias_tpl('', defines.DEFAULT_MODULE_ALIAS))

        elif isinstance(ql, qlast.SessionResetAliasDecl):
            aliases = aliases.delete(ql.alias)

            if not self._bootstrap_mode:
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
        schema = current_tx.get_schema()

        modaliases = ctx.state.current_tx().get_modaliases()
        session_config = ctx.state.current_tx().get_session_config()

        if ql.system and not current_tx.is_implicit():
            raise errors.QueryError(
                'CONFIGURE SYSTEM cannot be executed in a '
                'transaction block')

        ir = qlcompiler.compile_ast_to_ir(
            ql,
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=modaliases,
            ),
        )

        is_backend_setting = bool(getattr(ir, 'backend_setting', None))
        requires_restart = bool(getattr(ir, 'requires_restart', False))

        if is_backend_setting:
            if isinstance(ql, qlast.ConfigReset):
                val = None
            else:
                # Postgres is fine with all setting types to be passed
                # as strings.
                value = ireval.evaluate_to_python_val(ir.expr, schema=schema)
                val = pg_ast.StringConstant(val=str(value))

            if ir.system:
                sql_ast = pg_ast.AlterSystem(
                    name=ir.backend_setting,
                    value=val,
                )
            else:
                sql_ast = pg_ast.Set(
                    name=ir.backend_setting,
                    value=val,
                )

            sql_text = pg_codegen.generate_source(sql_ast) + ';'

            sql = (sql_text.encode(),)

        else:
            sql_text, _ = pg_compiler.compile_ir_to_sql(
                ir,
                pretty=debug.flags.edgeql_compile,
                output_format=pg_compiler.OutputFormat.JSONB)

            sql = (sql_text.encode(),)

        if not ql.system:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)

            session_config = config_op.apply(
                config.get_settings(),
                session_config)
            ctx.state.current_tx().update_session_config(session_config)
        else:
            try:
                config_op = ireval.evaluate_to_config_op(ir, schema=schema)
            except ireval.UnsupportedExpressionError:
                # This is a complex config object operation, the
                # op will be produced by the compiler as json.
                config_op = None

        return dbstate.SessionStateQuery(
            sql=sql,
            is_backend_setting=is_backend_setting,
            is_system_setting=ql.system,
            requires_restart=requires_restart,
            config_op=config_op,
        )

    def _compile_dispatch_ql(self, ctx: CompileContext, ql: qlast.Base):

        if isinstance(ql, qlast.Migration):
            if not (ctx.state.capability & enums.Capability.DDL):
                raise errors.ProtocolError(
                    f'cannot execute DDL commands for the current connection')
            return self._compile_ql_migration(ctx, ql)

        elif isinstance(ql, qlast.Database):
            if not (ctx.state.capability & enums.Capability.DDL):
                raise errors.ProtocolError(
                    f'cannot execute DDL commands for the current connection')
            return self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.DDL):
            if not (ctx.state.capability & enums.Capability.DDL):
                raise errors.ProtocolError(
                    f'cannot execute DDL commands for the current connection')
            return self._compile_and_apply_ddl_stmt(ctx, ql)

        elif isinstance(ql, qlast.Transaction):
            if not (ctx.state.capability & enums.Capability.TRANSACTION):
                raise errors.ProtocolError(
                    f'cannot execute transaction control commands '
                    f'for the current connection')
            return self._compile_ql_transaction(ctx, ql)

        elif isinstance(ql, (qlast.BaseSessionSet, qlast.BaseSessionReset)):
            if not (ctx.state.capability & enums.Capability.SESSION):
                raise errors.ProtocolError(
                    f'cannot execute session control commands '
                    f'for the current connection')
            return self._compile_ql_sess_state(ctx, ql)

        elif isinstance(ql, qlast.ConfigOp):
            if not (ctx.state.capability & enums.Capability.SESSION):
                raise errors.ProtocolError(
                    f'cannot execute session control commands '
                    f'for the current connection')
            return self._compile_ql_config_op(ctx, ql)

        else:
            if not (ctx.state.capability & enums.Capability.QUERY):
                raise errors.ProtocolError(
                    f'cannot execute query/DML commands '
                    f'for the current connection')
            return self._compile_ql_query(ctx, ql)

    def _compile(
        self,
        *,
        ctx: CompileContext,
        tokens: List[_edgeql_rust.Token],
    ) -> List[dbstate.QueryUnit]:

        # When True it means that we're compiling for "connection.query()".
        # That means that the returned QueryUnit has to have the in/out codec
        # information, correctly inferred "singleton_result" field etc.
        single_stmt_mode = ctx.stmt_mode is enums.CompileStatementMode.SINGLE
        default_cardinality = enums.ResultCardinality.NO_RESULT

        statements = edgeql.parse_block_tokens(tokens)
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
            comp: dbstate.BaseQuery = self._compile_dispatch_ql(ctx, stmt)

            if unit is not None:
                if comp.single_unit:
                    units.append(unit)
                    unit = None

            if unit is None:
                unit = dbstate.QueryUnit(
                    dbver=ctx.state.dbver,
                    sql=(),
                    status=status.get_status(stmt),
                    cardinality=default_cardinality)
            else:
                unit.status = status.get_status(stmt)

            if not comp.is_transactional:
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
                unit.has_ddl = True
                unit.new_types = comp.new_types

            elif isinstance(comp, dbstate.TxControlQuery):
                unit.sql += comp.sql
                unit.cacheable = comp.cacheable

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
                unit.has_ddl = True
                unit.new_types = comp.new_types

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

                if comp.is_system_setting:
                    if (not ctx.state.current_tx().is_implicit() or
                            statements_len > 1):
                        raise errors.QueryError(
                            'CONFIGURE SYSTEM cannot be executed in a '
                            'transaction block')

                    unit.system_config = True

                if comp.is_backend_setting:
                    unit.backend_config = True
                if comp.requires_restart:
                    unit.config_requires_restart = True

                if ctx.state.current_tx().is_implicit():
                    unit.modaliases = ctx.state.current_tx().get_modaliases()

                if comp.config_op is not None:
                    unit.config_ops.append(comp.config_op)

                unit.has_set = True

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
            if unit.cacheable and (unit.config_ops or unit.modaliases):
                raise errors.InternalServerError(
                    f'QueryUnit {unit!r} is cacheable but has config/aliases')
            if not unit.sql:
                raise errors.InternalServerError(
                    f'QueryUnit {unit!r} has no SQL commands in it')
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

    async def _ctx_new_con_state(
        self, *, dbver: bytes, io_format: enums.IoFormat, expect_one: bool,
        modaliases,
        session_config: Optional[immutables.Map],
        stmt_mode: Optional[enums.CompileStatementMode],
        capability: enums.Capability,
        implicit_limit: int=0,
        json_parameters: bool=False,
        schema: Optional[s_schema.Schema] = None,
        schema_object_ids: Optional[Mapping[str, uuid.UUID]] = None,
        first_extracted_var: Optional[int] = None,
    ) -> CompileContext:

        if session_config is None:
            session_config = EMPTY_MAP
        if modaliases is None:
            modaliases = DEFAULT_MODULE_ALIASES_MAP

        assert isinstance(modaliases, immutables.Map)
        assert isinstance(session_config, immutables.Map)

        if schema is None:
            db = await self._get_database(dbver)
            schema = db.schema
            cached_reflection = db.cached_reflection
        else:
            cached_reflection = immutables.Map()

        self._current_db_state = dbstate.CompilerConnectionState(
            dbver,
            schema,
            modaliases,
            session_config,
            capability,
            cached_reflection,
        )

        state = self._current_db_state

        ctx = CompileContext(
            state=state,
            output_format=io_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            stmt_mode=stmt_mode,
            json_parameters=json_parameters,
            schema_object_ids=schema_object_ids,
            first_extracted_var=first_extracted_var,
        )

        return ctx

    async def _ctx_from_con_state(self, *, txid: int,
                                  io_format: enums.IoFormat,
                                  expect_one: bool,
                                  implicit_limit: int,
                                  stmt_mode: enums.CompileStatementMode,
                                  first_extracted_var: Optional[int]=None):
        state = self._load_state(txid)

        ctx = CompileContext(
            state=state,
            output_format=io_format,
            expected_cardinality_one=expect_one,
            implicit_limit=implicit_limit,
            stmt_mode=stmt_mode,
            first_extracted_var=first_extracted_var)

        return ctx

    def _load_state(self, txid: int) -> dbstate.CompilerConnectionState:
        if self._current_db_state is None:  # pragma: no cover
            raise errors.InternalServerError(
                f'failed to lookup transaction with id={txid}')

        if self._current_db_state.current_tx().id == txid:
            return self._current_db_state

        if self._current_db_state.can_rollback_to_savepoint(txid):
            self._current_db_state.rollback_to_savepoint(txid)
            return self._current_db_state

        raise errors.InternalServerError(
            f'failed to lookup transaction or savepoint with id={txid}'
        )  # pragma: no cover

    # API

    async def try_compile_rollback(self, dbver: bytes, eql: bytes):
        statements = edgeql.parse_block(eql.decode())

        stmt = statements[0]
        unit = None
        if isinstance(stmt, qlast.RollbackTransaction):
            sql = b'ROLLBACK;'
            unit = dbstate.QueryUnit(
                dbver=dbver,
                status=b'ROLLBACK',
                sql=(sql,),
                tx_rollback=True,
                cacheable=False)

        elif isinstance(stmt, qlast.RollbackToSavepoint):
            sql = f'ROLLBACK TO {pg_common.quote_ident(stmt.name)};'.encode()
            unit = dbstate.QueryUnit(
                dbver=dbver,
                status=b'ROLLBACK TO SAVEPOINT',
                sql=(sql,),
                tx_savepoint_rollback=True,
                cacheable=False)

        if unit is not None:
            return unit, len(statements) - 1

        raise errors.TransactionError(
            'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
        )  # pragma: no cover

    async def compile_notebook(
        self,
        dbver: bytes,
        queries: List[str],
        implicit_limit: int = 0,
    ) -> List[dbstate.QueryUnit]:

        ctx = await self._ctx_new_con_state(
            dbver=dbver,
            io_format=enums.IoFormat.BINARY,
            expect_one=False,
            implicit_limit=implicit_limit,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            stmt_mode=enums.CompileStatementMode.SINGLE,
            capability=enums.Capability.QUERY | enums.Capability.DDL,
            json_parameters=False,
        )

        ctx.state.start_tx()
        txid = ctx.state.current_tx().id

        result: List[
            Tuple[
                bool,
                Union[dbstate.QueryUnit, Tuple[str, str, Dict[str, str]]]
            ]
        ] = []

        for query in queries:
            ctx = await self._ctx_from_con_state(
                txid=txid,
                io_format=enums.IoFormat.BINARY,
                expect_one=False,
                implicit_limit=implicit_limit,
                stmt_mode=enums.CompileStatementMode.SINGLE)

            try:
                tokens = tokenizer.tokenize(query)
                result.append(
                    (False, self._compile(ctx=ctx, tokens=tokens)[0]))
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

        ctx.state.rollback_tx()

        return result

    async def compile_eql_tokens(
        self,
        dbver: bytes,
        eql_tokens: List[_edgeql_rust.Token],
        sess_modaliases: Optional[immutables.Map],
        sess_config: Optional[immutables.Map],
        io_format: enums.IoFormat,
        expect_one: bool,
        implicit_limit: int,
        stmt_mode: enums.CompileStatementMode,
        capability: enums.Capability,
        first_extracted_var: Optional[int]=None,
        json_parameters: bool=False,
    ) -> List[dbstate.QueryUnit]:

        ctx = await self._ctx_new_con_state(
            dbver=dbver,
            io_format=io_format,
            expect_one=expect_one,
            implicit_limit=implicit_limit,
            modaliases=sess_modaliases,
            session_config=sess_config,
            stmt_mode=enums.CompileStatementMode(stmt_mode),
            capability=capability,
            json_parameters=json_parameters,
            first_extracted_var=first_extracted_var)

        return self._compile(ctx=ctx, tokens=eql_tokens)

    async def compile_eql_tokens_in_tx(
        self,
        txid: int,
        eql_tokens: List[_edgeql_rust.Token],
        io_format: enums.IoFormat,
        expect_one: bool,
        implicit_limit: int,
        stmt_mode: enums.CompileStatementMode,
        first_extracted_var: Optional[int]=None,
    ) -> List[dbstate.QueryUnit]:

        ctx = await self._ctx_from_con_state(
            txid=txid,
            io_format=io_format,
            expect_one=expect_one,
            implicit_limit=implicit_limit,
            stmt_mode=enums.CompileStatementMode(stmt_mode),
            first_extracted_var=first_extracted_var)

        return self._compile(ctx=ctx, tokens=eql_tokens)

    async def interpret_backend_error(self, dbver, fields):
        db = await self._get_database(dbver)
        return errormech.interpret_backend_error(db.schema, fields)

    async def interpret_backend_error_in_tx(self, txid, fields):
        state = self._load_state(txid)
        return errormech.interpret_backend_error(
            state.current_tx().get_schema(), fields)

    async def update_type_ids(self, txid, typemap):
        state = self._load_state(txid)
        tx = state.current_tx()
        schema = tx.get_schema()
        for tid, backend_tid in typemap.items():
            t = schema.get_by_id(uuidgen.UUID(tid))
            schema = t.set_field_value(schema, 'backend_id', backend_tid)
        state.current_tx().update_schema(schema)

    async def _introspect_schema_in_snapshot(
        self,
        tx_snapshot_id: str
    ) -> s_schema.Schema:
        con = await self.new_connection()
        try:
            async with con.transaction(isolation='serializable',
                                       readonly=True):
                await con.execute(
                    f'SET TRANSACTION SNAPSHOT {pg_ql(tx_snapshot_id)};')

                return await self.introspect(con)
        finally:
            await con.close()

    async def describe_database_dump(
        self,
        tx_snapshot_id: str
    ) -> DumpDescriptor:
        schema = await self._introspect_schema_in_snapshot(tx_snapshot_id)

        schema_ddl = s_ddl.ddl_text_from_schema(schema)

        all_objects = schema.get_objects(exclude_stdlib=True)
        ids = []
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

        objtypes = schema.get_objects(
            type=s_objtypes.ObjectType,
            exclude_stdlib=True,
        )
        descriptors = []

        for objtype in objtypes:
            if objtype.is_union_type(schema) or objtype.is_view(schema):
                continue
            descriptors.extend(self._describe_object(schema, objtype))

        return DumpDescriptor(
            schema_ddl=schema_ddl,
            schema_ids=ids,
            blocks=descriptors,
        )

    def _describe_object(
        self,
        schema: s_schema.Schema,
        source: s_obj.Object,
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
                    'ptr_item_id': schema.get('std::uuid'),
                },
                {'named': True},
            )

            type_data, type_id = sertypes.TypeSerializer.describe(
                schema,
                prop_tuple,
                view_shapes={},
                view_shapes_metadata={},
                follow_links=False,
            )

            cols.extend([
                'source',
                'target',
                'ptr_item_id',
            ])

        elif isinstance(source, s_links.Link):
            props = {
                'source': schema.get('std::uuid'),
                'target': schema.get('std::uuid'),
                'ptr_item_id': schema.get('std::uuid'),
            }

            cols.extend([
                'source',
                'target',
                'ptr_item_id',
            ])

            for ptr in source.get_pointers(schema).objects(schema):
                if ptr.is_endpoint_pointer(schema):
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
            )

        else:
            for ptr in source.get_pointers(schema).objects(schema):
                if ptr.is_endpoint_pointer(schema):
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
                    ptrdesc.extend(self._describe_object(schema, ptr))

            type_data, type_id = sertypes.TypeSerializer.describe(
                schema,
                source,
                view_shapes={source: shape},
                view_shapes_metadata={},
                follow_links=False,
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

    async def describe_database_restore(
        self,
        tx_snapshot_id: str,
        schema_ddl: bytes,
        schema_ids: List[Tuple[str, str, bytes]],
        blocks: List[Tuple[bytes, bytes]],  # type_id, typespec
    ) -> RestoreDescriptor:
        schema_object_ids = {
            (name, qltype if qltype else None): uuidgen.from_bytes(objid)
            for name, qltype, objid in schema_ids
        }

        schema = await self._introspect_schema_in_snapshot(tx_snapshot_id)
        ctx = await self._ctx_new_con_state(
            dbver=b'',
            io_format=enums.IoFormat.BINARY,
            expect_one=False,
            modaliases=DEFAULT_MODULE_ALIASES_MAP,
            session_config=EMPTY_MAP,
            stmt_mode=enums.CompileStatementMode.ALL,
            capability=enums.Capability.ALL,
            json_parameters=False,
            schema=schema,
            schema_object_ids=schema_object_ids)
        ctx.state.start_tx()

        units = self._compile(
            ctx=ctx,
            tokens=tokenizer.tokenize(schema_ddl),
        )
        schema = ctx.state.current_tx().get_schema()

        restore_blocks = []
        tables = []
        for schema_object_id, typedesc in blocks:
            schema_object_id = uuidgen.from_bytes(schema_object_id)
            obj = schema._id_to_type.get(schema_object_id)
            desc = sertypes.TypeSerializer.parse(typedesc)

            if isinstance(obj, s_props.Property):
                assert isinstance(desc, sertypes.NamedTupleDesc)
                desc_cols = list(desc.fields.keys())
                if set(desc_cols) != {'source', 'target', 'ptr_item_id'}:
                    raise RuntimeError(
                        'Property table dump data has extra fields')

            elif isinstance(obj, s_links.Link):
                assert isinstance(desc, sertypes.NamedTupleDesc)
                desc_cols = list(desc.fields.keys())

                cols = ['source', 'target', 'ptr_item_id']
                for ptr in obj.get_pointers(schema).objects(schema):
                    if ptr.is_endpoint_pointer(schema):
                        continue
                    stor_info = pg_types.get_pointer_storage_info(
                        ptr,
                        schema=schema,
                        source=obj,
                        link_bias=True,
                    )

                    cols.append(stor_info.column_name)

                if set(desc_cols) != set(cols):
                    raise RuntimeError(
                        'Link table dump data has extra fields')

            else:
                assert isinstance(desc, sertypes.ShapeDesc)
                desc_cols = list(desc.fields.keys())

                cols = []
                for ptr in obj.get_pointers(schema).objects(schema):
                    if ptr.is_endpoint_pointer(schema):
                        continue

                    stor_info = pg_types.get_pointer_storage_info(
                        ptr,
                        schema=schema,
                        source=obj,
                    )

                    if stor_info.table_type == 'ObjectType':
                        cols.append(stor_info.column_name)

                if set(desc_cols) != set(cols):
                    raise RuntimeError(
                        'Object table dump data has extra fields')

            table_name = pg_common.get_backend_name(
                schema, obj, catenate=True)

            stmt = (
                f'COPY {table_name} '
                f'({", ".join(pg_common.quote_ident(c) for c in desc_cols)}) '
                f'FROM STDIN WITH BINARY'
            ).encode()

            restore_blocks.append(
                RestoreBlockDescriptor(
                    schema_object_id=schema_object_id,
                    sql_copy_stmt=stmt,
                )
            )

            tables.append(table_name)

        return RestoreDescriptor(
            units=units,
            blocks=restore_blocks,
            tables=tables,
        )


class DumpDescriptor(NamedTuple):

    schema_ddl: str
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


class RestoreBlockDescriptor(NamedTuple):

    schema_object_id: uuid.UUID
    sql_copy_stmt: bytes
