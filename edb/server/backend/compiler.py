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


import collections
import enum
import hashlib
import pathlib
import typing

import asyncpg
import immutables

from edb import errors

from edb.server import defines
from edb.pgsql import compiler as pg_compiler
from edb.pgsql import intromech

from edb import edgeql
from edb import graphql
from edb.common import debug

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as ql_compiler
from edb.edgeql import codegen as ql_codegen
from edb.edgeql import quote as ql_quote
from edb.edgeql import parser as ql_parser

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import deltas as s_deltas
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import common as pg_common

from . import config
from . import dbstate
from . import errormech
from . import sertypes
from . import stdschema


class CompileStatementMode(enum.Enum):
    SKIP_FIRST = 'skip_first'
    ALL = 'all'
    SINGLE = 'single'


class CompilerDatabaseState(typing.NamedTuple):

    dbver: int
    con_args: dict
    schema: s_schema.Schema


class CompileContext(typing.NamedTuple):

    state: dbstate.CompilerConnectionState
    output_format: pg_compiler.OutputFormat
    legacy_mode: bool
    graphql_mode: bool
    stmt_mode: CompileStatementMode


EMPTY_MAP = immutables.Map()


def compile_bootstrap_script(std_schema: s_schema.Schema,
                             schema: s_schema.Schema,
                             eql: str):

    state = dbstate.CompilerConnectionState(
        0,
        schema,
        EMPTY_MAP,
        EMPTY_MAP)

    ctx = CompileContext(
        state=state,
        output_format=pg_compiler.OutputFormat.JSON,
        legacy_mode=False,
        graphql_mode=False,
        stmt_mode='all')

    compiler = Compiler(None, None)
    compiler._std_schema = std_schema
    compiler._bootstrap_mode = True

    units = compiler._compile(ctx=ctx, eql=eql.encode())

    sql = b'\n'.join(getattr(u, 'sql', b'') for u in units)
    new_schema = state.current_tx().get_schema()

    return new_schema, sql.decode()


class Compiler:

    _connect_args: dict
    _dbname: typing.Optional[str]
    _cached_db: typing.Optional[CompilerDatabaseState]

    def __init__(self, connect_args: dict, data_dir: str):
        self._connect_args = connect_args
        self._dbname = None
        self._cached_db = None
        self._current_db_state = None
        self._bootstrap_mode = False

        if data_dir is not None:
            self._data_dir = pathlib.Path(data_dir)
            self._std_schema = stdschema.load(self._data_dir)
        else:
            self._data_dir = None
            self._std_schema = None

        # Preload parsers.
        ql_parser.preload()

    async def _get_database(self, dbver: int) -> CompilerDatabaseState:
        if self._cached_db is not None and self._cached_db.dbver == dbver:
            return self._cached_db

        assert self._std_schema is not None

        self._cached_db = None

        con_args = self._connect_args.copy()
        con_args['user'] = defines.EDGEDB_SUPERUSER
        con_args['database'] = self._dbname

        con = await asyncpg.connect(**con_args)
        try:
            im = intromech.IntrospectionMech(con)
            schema = await im.readschema(
                schema=self._std_schema,
                exclude_modules=s_schema.STD_MODULES)

            db = CompilerDatabaseState(
                dbver=dbver,
                con_args=con_args,
                schema=schema)

            self._cached_db = db
            return db
        finally:
            await con.close()

    def _hash_sql(self, sql: bytes, **kwargs: bytes):
        h = hashlib.sha1(sql)
        for param, val in kwargs.items():
            h.update(param.encode('latin1'))
            h.update(val)
        return h.hexdigest().encode('latin1')

    def _new_delta_context(self, ctx: CompileContext):
        current_tx = ctx.state.current_tx()
        config = current_tx.get_config()

        context = s_delta.CommandContext()
        context.testmode = bool(config.get('__internal_testmode'))
        context.stdmode = self._bootstrap_mode

        return context

    def _process_delta(self, ctx: CompileContext, delta, schema):
        """Adapt and process the delta command."""

        if debug.flags.delta_plan:
            debug.header('Delta Plan')
            debug.dump(delta, schema=schema)

        delta = pg_delta.CommandMeta.adapt(delta)
        context = self._new_delta_context(ctx)
        schema, _ = delta.apply(schema, context)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(delta, schema=schema)

        return schema, delta

    def _compile_ql_query(
            self, ctx: CompileContext,
            ql: qlast.Base) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()
        config = current_tx.get_config()

        native_out_format = (
            ctx.output_format is pg_compiler.OutputFormat.NATIVE
        )

        single_stmt_mode = ctx.stmt_mode is CompileStatementMode.SINGLE

        implicit_fields = (
            native_out_format and
            not ctx.legacy_mode and
            single_stmt_mode
        )

        disable_constant_folding = config.get(
            '__internal_no_const_folding', False)

        ir = ql_compiler.compile_ast_to_ir(
            ql,
            schema=current_tx.get_schema(),
            modaliases=current_tx.get_modaliases(),
            implicit_tid_in_shapes=implicit_fields,
            implicit_id_in_shapes=implicit_fields,
            disable_constant_folding=disable_constant_folding)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=debug.flags.edgeql_compile,
            output_format=ctx.output_format)

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

        if single_stmt_mode or ctx.legacy_mode:
            if native_out_format:
                out_type_data, out_type_id = sertypes.TypeSerializer.describe(
                    ir.schema, ir.stype,
                    ir.view_shapes, ir.view_shapes_metadata)
            else:
                out_type_data, out_type_id = \
                    sertypes.TypeSerializer.describe_json()

            if ir.params:
                subtypes = [None] * len(ir.params)
                first_param_name = next(iter(ir.params))
                if first_param_name.isdecimal():
                    named = False
                    for param_name, param_type in ir.params.items():
                        subtypes[int(param_name)] = (param_name, param_type)
                else:
                    named = True
                    for param_name, param_type in ir.params.items():
                        subtypes[argmap[param_name] - 1] = (
                            param_name, param_type
                        )
                params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types=collections.OrderedDict(subtypes),
                    named=named)
            else:
                params_type = s_types.Tuple.create(
                    ir.schema, element_types={}, named=False)

            in_type_data, in_type_id = sertypes.TypeSerializer.describe(
                ir.schema, params_type, {}, {})

            sql_hash = self._hash_sql(
                sql_bytes, mode=str(ctx.output_format).encode())

            return dbstate.Query(
                sql=sql_bytes,
                sql_hash=sql_hash,
                in_type_id=in_type_id.bytes,
                in_type_data=in_type_data,
                out_type_id=out_type_id.bytes,
                out_type_data=out_type_data,
            )

        else:
            if ir.params:
                raise errors.QueryError(
                    'queries compiled in script mode cannot accept parameters')

            return dbstate.SimpleQuery(sql=sql_bytes)

    def _compile_and_apply_delta_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()
        context = self._new_delta_context(ctx)

        if isinstance(cmd, s_deltas.CreateDelta):
            delta = None
        else:
            delta = schema.get(cmd.classname)

        with context(s_deltas.DeltaCommandContext(schema, cmd, delta)):
            if isinstance(cmd, s_deltas.CommitDelta):
                ddl_plan = s_delta.DeltaRoot()
                ddl_plan.update(delta.get_commands(schema))
                return self._compile_and_apply_ddl_command(ctx, ddl_plan)

            elif isinstance(cmd, s_deltas.GetDelta):
                delta_ql = s_ddl.ddl_text_from_delta(schema, delta)
                query_ql = qlast.SelectQuery(
                    result=qlast.StringConstant(
                        quote="'",
                        value=ql_quote.escape_string(delta_ql)))
                return self._compile_ql_query(ctx, query_ql)

            elif isinstance(cmd, s_deltas.CreateDelta):
                schema, _ = cmd.apply(schema, context)
                current_tx.update_schema(schema)
                return dbstate.DDLQuery(sql=b'SELECT;')

            else:
                raise errors.InternalServerError(
                    f'unexpected delta command: {cmd!r}')

    def _compile_and_apply_ddl_command(self, ctx: CompileContext, cmd):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(cmd)

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = schema
        context = self._new_delta_context(ctx)
        cmd.apply(test_schema, context=context)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = self._process_delta(ctx, cmd, schema)

        if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
            block = pg_dbops.SQLBlock()
        else:
            block = pg_dbops.PLTopBlock()

        plan.generate(block)
        sql = block.to_string().encode('utf-8')

        current_tx.update_schema(schema)

        return dbstate.DDLQuery(sql=sql)

    def _compile_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        if isinstance(cmd, s_deltas.DeltaCommand):
            return self._compile_and_apply_delta_command(ctx, cmd)

        elif isinstance(cmd, s_delta.Command):
            return self._compile_and_apply_ddl_command(ctx, cmd)

        else:
            raise errors.InternalServerError(f'unexpected plan {cmd!r}')

    def _compile_ql_ddl(self, ctx: CompileContext, ql: qlast.DDL):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        cmd = s_ddl.delta_from_ddl(
            ql,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=bool(current_tx.get_config().get('__internal_testmode')))

        return self._compile_command(ctx, cmd)

    def _compile_ql_migration(self, ctx: CompileContext,
                              ql: typing.Union[qlast.Database, qlast.Delta]):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        cmd = s_ddl.cmd_from_ddl(
            ql,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=bool(current_tx.get_config().get('__internal_testmode')))

        if (isinstance(ql, qlast.CreateDelta) and
                cmd.get_attribute_value('target')):

            assert self._std_schema is not None
            cmd = s_ddl.compile_migration(
                cmd,
                self._std_schema,
                current_tx.get_schema())

        return self._compile_command(ctx, cmd)

    def _compile_ql_transaction(
            self, ctx: CompileContext,
            ql: qlast.Transaction) -> dbstate.Query:

        cacheable = True
        single_unit = False

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

            sql = sql.encode()
            action = dbstate.TxAction.START
            cacheable = False

        elif isinstance(ql, qlast.CommitTransaction):
            ctx.state.commit_tx()
            sql = b'COMMIT'
            single_unit = True
            action = dbstate.TxAction.COMMIT

        elif isinstance(ql, qlast.RollbackTransaction):
            ctx.state.rollback_tx()
            sql = b'ROLLBACK'
            single_unit = True
            action = dbstate.TxAction.ROLLBACK

        elif isinstance(ql, qlast.DeclareSavepoint):
            tx = ctx.state.current_tx()
            sp_id = tx.declare_savepoint(ql.name)

            sql = ''

            if not self._bootstrap_mode:
                q = lambda o: pg_common.quote_literal(str(o))
                pgname = pg_common.quote_ident(ql.name)
                sql += f'''
                    INSERT INTO _edgecon_current_savepoint(sp_id)
                    VALUES (
                        {q(sp_id)}
                    )
                    ON CONFLICT (_sentinel) DO
                    UPDATE
                        SET sp_id = {q(sp_id)};
                '''

            sql += f'SAVEPOINT {pgname};'
            sql = sql.encode()

            cacheable = False
            action = dbstate.TxAction.DECLARE_SAVEPOINT

        elif isinstance(ql, qlast.ReleaseSavepoint):
            ctx.state.current_tx().release_savepoint(ql.name)
            pgname = pg_common.quote_ident(ql.name)
            sql = f'RELEASE SAVEPOINT {pgname}'.encode()
            action = dbstate.TxAction.RELEASE_SAVEPOINT

        elif isinstance(ql, qlast.RollbackToSavepoint):
            ctx.state.current_tx().rollback_to_savepoint(ql.name)
            pgname = pg_common.quote_ident(ql.name)
            sql = f'ROLLBACK TO SAVEPOINT {pgname}'.encode()
            single_unit = True
            action = dbstate.TxAction.ROLLBACK_TO_SAVEPOINT

        else:
            raise ValueError(f'expecting transaction node, got {ql!r}')

        return dbstate.TxControlQuery(
            sql=sql,
            action=action,
            cacheable=cacheable,
            single_unit=single_unit)

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.SetSessionState):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        aliases = {}
        config_vals = {}

        sqlbuf = []

        q = lambda o: pg_common.quote_literal(str(o))

        for item in ql.items:
            if isinstance(item, qlast.SessionSettingModuleDecl):
                try:
                    schema.get(item.module)
                except errors.InvalidReferenceError:
                    raise errors.UnknownModuleError(
                        f'module {item.module!r} does not exist') from None

                aliases[item.alias] = item.module

                if not self._bootstrap_mode:
                    sql = f'''
                        INSERT INTO _edgecon_state(name, value, type)
                        VALUES (
                            {q(item.alias or '')},
                            {q(item.module)},
                            'A'
                        )
                        ON CONFLICT (name, type) DO
                        UPDATE
                            SET value = {q(item.module)};
                        '''
                    sqlbuf.append(sql.encode())

            elif isinstance(item, qlast.SessionSettingConfigDecl):
                name = item.alias

                config_vals[name] = config._setting_val_from_qlast(
                    self._std_schema, name, item.expr)

                item_expr_ql = ql_codegen.generate_source(item.expr)

                if not self._bootstrap_mode:
                    sql = f'''
                        INSERT INTO _edgecon_state(name, value, type)
                        VALUES (
                            {q(name)},
                            {q(item_expr_ql)},
                            'C'
                        )
                        ON CONFLICT (name, type) DO
                        UPDATE
                            SET value = {q(item_expr_ql)};
                        '''
                    sqlbuf.append(sql.encode())

            else:
                raise errors.InternalServerError(
                    f'unsupported SET command type {type(item)!r}')

        aliases = immutables.Map(aliases)
        config_vals = immutables.Map(config_vals)

        if aliases:
            ctx.state.current_tx().update_modaliases(
                ctx.state.current_tx().get_modaliases().update(aliases))
        if config_vals:
            ctx.state.current_tx().update_config(
                ctx.state.current_tx().get_config().update(config_vals))

        return dbstate.SessionStateQuery(
            sql=b''.join(sqlbuf),
            sess_set_modaliases=aliases,
            sess_set_config=config_vals)

    def _compile_dispatch_ql(self, ctx: CompileContext, ql: qlast.Base):

        if isinstance(ql, (qlast.Database, qlast.Delta)):
            return self._compile_ql_migration(ctx, ql)

        elif isinstance(ql, qlast.DDL):
            return self._compile_ql_ddl(ctx, ql)

        elif isinstance(ql, qlast.Transaction):
            return self._compile_ql_transaction(ctx, ql)

        elif isinstance(ql, qlast.SetSessionState):
            return self._compile_ql_sess_state(ctx, ql)

        else:
            return self._compile_ql_query(ctx, ql)

    def _compile(self, *,
                 ctx: CompileContext,
                 eql: bytes) -> typing.List[dbstate.QueryUnit]:

        eql = eql.decode()
        if ctx.graphql_mode:
            assert ctx.stmt_mode is CompileStatementMode.ALL
            eql = graphql.translate(
                ctx.state.current_tx().get_schema(),
                eql,
                variables={}) + ';'
        else:
            eql += ';'

        statements = edgeql.parse_block(eql)
        if ctx.stmt_mode is CompileStatementMode.SKIP_FIRST:
            statements = statements[1:]
            if not statements:
                return []
        elif (ctx.stmt_mode is CompileStatementMode.SINGLE and
                len(statements) > 1):
            raise errors.ProtocolError(
                f'expected one statement, got {len(statements)}')

        units = []
        unit = None

        for stmt in statements:
            comp: dbstate.BaseQuery = self._compile_dispatch_ql(ctx, stmt)

            if unit is not None and (
                    ctx.legacy_mode or
                    (isinstance(comp, dbstate.TxControlQuery) and
                        comp.single_unit)):
                units.append(unit)
                unit = None

            if unit is None:
                unit = dbstate.QueryUnit(dbver=ctx.state.dbver)

            if isinstance(comp, dbstate.Query):
                if (ctx.stmt_mode is CompileStatementMode.SINGLE or
                        ctx.legacy_mode):
                    unit.sql = comp.sql
                    unit.sql_hash = comp.sql_hash

                    unit.out_type_data = comp.out_type_data
                    unit.out_type_id = comp.out_type_id
                    unit.in_type_data = comp.in_type_data
                    unit.in_type_id = comp.in_type_id

                    unit.cacheable = True
                else:
                    unit.sql += comp.sql + b';'

            elif isinstance(comp, dbstate.SimpleQuery):
                unit.sql += comp.sql + b';'

            elif isinstance(comp, dbstate.DDLQuery):
                unit.sql += comp.sql + b';'
                unit.has_ddl = True

            elif isinstance(comp, dbstate.TxControlQuery):
                unit.sql += comp.sql + b';'

                unit.cacheable = comp.cacheable

                if comp.action == dbstate.TxAction.START:
                    unit.tx_id = ctx.state.current_tx().id

                elif comp.action == dbstate.TxAction.COMMIT:
                    unit.tx_commit = True

                    unit.config = ctx.state.current_tx().get_config()
                    unit.modaliases = ctx.state.current_tx().get_modaliases()

                elif comp.action == dbstate.TxAction.ROLLBACK:
                    unit.tx_rollback = True

                    unit.config = ctx.state.current_tx().get_config()
                    unit.modaliases = ctx.state.current_tx().get_modaliases()

                elif comp.action is dbstate.TxAction.ROLLBACK_TO_SAVEPOINT:
                    unit.tx_savepoint_rollback = True

                if comp.single_unit:
                    units.append(unit)
                    unit = None

            elif isinstance(comp, dbstate.SessionStateQuery):
                unit.sql += comp.sql + b';'

                unit.config = ctx.state.current_tx().get_config()
                unit.modaliases = ctx.state.current_tx().get_modaliases()

                unit.has_set = True

            else:
                raise errors.InternalServerError('unknown compile state')

        if unit is not None:
            units.append(unit)

        for unit in units:
            if unit.tx_id:
                unit.config = unit.modaliases = None

            if not unit.sql:
                raise errors.InternalServerError(
                    f'QueryUnit {unit!r} has an empty sql field')

        return units

    async def _ctx_new_con_state(self, *, dbver: int, json_mode: bool,
                                 modaliases, config,
                                 legacy_mode: bool,
                                 graphql_mode: bool,
                                 stmt_mode: CompileStatementMode):

        assert isinstance(modaliases, immutables.Map)
        assert isinstance(config, immutables.Map)

        db = await self._get_database(dbver)
        self._current_db_state = dbstate.CompilerConnectionState(
            dbver, db.schema, modaliases, config)

        state = self._current_db_state

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            state=state,
            output_format=of,
            legacy_mode=legacy_mode,
            graphql_mode=graphql_mode,
            stmt_mode=stmt_mode)

        return ctx

    async def _ctx_from_con_state(self, *, txid: int, json_mode: bool,
                                  legacy_mode: bool,
                                  graphql_mode: bool,
                                  stmt_mode: CompileStatementMode):
        state = self._load_state(txid)

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            state=state,
            output_format=of,
            legacy_mode=legacy_mode,
            graphql_mode=graphql_mode,
            stmt_mode=stmt_mode)

        return ctx

    def _load_state(self, txid: int):
        if self._current_db_state is None:
            raise errors.InternalServerError(
                f'failed to lookup transaction with id={txid}')

        if self._current_db_state.current_tx().id == txid:
            return self._current_db_state

        if self._current_db_state.can_rollback_to_savepoint(txid):
            self._current_db_state.rollback_to_savepoint(txid)
            return self._current_db_state

        raise errors.InternalServerError(
            f'failed to lookup transaction or savepoint with id={txid}')

    # API

    async def connect(self, dbname: str, dbver: int) -> CompilerDatabaseState:
        self._dbname = dbname
        self._cached_db = None
        await self._get_database(dbver)

    async def try_compile_rollback(self, dbver: int, eql: bytes):
        statements = edgeql.parse_block(eql.decode() + ';')

        stmt = statements[0]
        unit = None
        if isinstance(stmt, qlast.RollbackTransaction):
            sql = b'ROLLBACK;'
            unit = dbstate.QueryUnit(
                dbver=dbver,
                sql=sql,
                tx_rollback=True,
                cacheable=True)

        elif isinstance(stmt, qlast.RollbackToSavepoint):
            sql = f'ROLLBACK TO {pg_common.quote_ident(stmt.name)};'.encode()
            unit = dbstate.QueryUnit(
                dbver=dbver,
                sql=sql,
                tx_savepoint_rollback=True,
                cacheable=True)

        if unit is not None:
            return unit, len(statements) - 1

        raise errors.TransactionError(
            'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command')

    async def compile_eql(
            self,
            dbver: int,
            eql: bytes,
            sess_modaliases: immutables.Map,
            sess_config: immutables.Map,
            json_mode: bool,
            legacy_mode: bool,
            graphql_mode: bool,
            stmt_mode: CompileStatementMode) -> typing.List[dbstate.QueryUnit]:

        ctx = await self._ctx_new_con_state(
            dbver=dbver,
            json_mode=json_mode,
            modaliases=sess_modaliases,
            config=sess_config,
            legacy_mode=legacy_mode,
            graphql_mode=graphql_mode,
            stmt_mode=CompileStatementMode(stmt_mode))

        units = self._compile(ctx=ctx, eql=eql)
        if stmt_mode is CompileStatementMode.SINGLE and len(units) != 1:
            raise errors.InternalServerError(
                f'expected 1 compiled unit; got {len(units)}')

        return units

    async def compile_eql_in_tx(
            self,
            txid: int,
            eql: bytes,
            json_mode: bool,
            legacy_mode: bool,
            graphql_mode: bool,
            stmt_mode: CompileStatementMode) -> typing.List[dbstate.QueryUnit]:

        ctx = await self._ctx_from_con_state(
            txid=txid,
            json_mode=json_mode,
            legacy_mode=legacy_mode,
            graphql_mode=graphql_mode,
            stmt_mode=CompileStatementMode(stmt_mode))

        units = self._compile(ctx=ctx, eql=eql)
        if stmt_mode is CompileStatementMode.SINGLE and len(units) != 1:
            raise errors.InternalServerError(
                f'expected 1 compiled unit; got {len(units)}')

        return units

    async def interpret_backend_error(self, dbver, fields):
        db = await self._get_database(dbver)
        return errormech.interpret_backend_error(db.schema, fields)
