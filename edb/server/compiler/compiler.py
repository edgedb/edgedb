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
import dataclasses
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
from edb.common import debug

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as ql_compiler
from edb.edgeql import quote as ql_quote
from edb.edgeql import qltypes

from edb.ir import staeval as ireval

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import deltas as s_deltas
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops
from edb.pgsql import common as pg_common

from edb.server import config

from . import dbstate
from . import enums
from . import errormech
from . import sertypes
from . import status
from . import stdschema


@dataclasses.dataclass(frozen=True)
class CompilerDatabaseState:

    dbver: int
    con_args: dict
    schema: s_schema.Schema


@dataclasses.dataclass(frozen=True)
class CompileContext:

    state: dbstate.CompilerConnectionState
    output_format: pg_compiler.OutputFormat
    expected_cardinality_one: bool
    stmt_mode: enums.CompileStatementMode
    json_parameters: bool = False


EMPTY_MAP = immutables.Map()
DEFAULT_MODULE_ALIASES_MAP = immutables.Map(
    {None: defines.DEFAULT_MODULE_ALIAS})


pg_ql = lambda o: pg_common.quote_literal(str(o))


def compile_bootstrap_script(std_schema: s_schema.Schema,
                             schema: s_schema.Schema,
                             eql: str):

    state = dbstate.CompilerConnectionState(
        0,
        schema,
        EMPTY_MAP,
        EMPTY_MAP,
        enums.Capability.ALL)

    ctx = CompileContext(
        state=state,
        output_format=pg_compiler.OutputFormat.JSON,
        expected_cardinality_one=False,
        stmt_mode=enums.CompileStatementMode.ALL)

    compiler = Compiler(None, None)
    compiler._std_schema = std_schema
    compiler._bootstrap_mode = True

    units = compiler._compile(ctx=ctx, eql=eql.encode())

    sql = b'\n'.join(b''.join(u.sql) for u in units)
    new_schema = state.current_tx().get_schema()

    return new_schema, sql.decode()


class BaseCompiler:

    _connect_args: dict
    _dbname: typing.Optional[str]
    _cached_db: typing.Optional[CompilerDatabaseState]

    def __init__(self, connect_args: dict, data_dir: str):
        self._connect_args = connect_args
        self._dbname = None
        self._cached_db = None

        if data_dir is not None:
            self._data_dir = pathlib.Path(data_dir)
            self._std_schema = stdschema.load_std_schema(self._data_dir)
            config_spec = config.load_spec_from_schema(self._std_schema)
            config.set_settings(config_spec)
        else:
            self._data_dir = None
            self._std_schema = None

    def _hash_sql(self, sql: bytes, **kwargs: bytes):
        h = hashlib.sha1(sql)
        for param, val in kwargs.items():
            h.update(param.encode('latin1'))
            h.update(val)
        return h.hexdigest().encode('latin1')

    def _wrap_schema(self, dbver, con_args, schema) -> CompilerDatabaseState:
        return CompilerDatabaseState(
            dbver=dbver,
            con_args=con_args,
            schema=schema)

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

            db = self._wrap_schema(dbver, con_args, schema)
            self._cached_db = db
            return db
        finally:
            await con.close()

    # API

    async def connect(self, dbname: str, dbver: int) -> CompilerDatabaseState:
        self._dbname = dbname
        self._cached_db = None
        await self._get_database(dbver)


class Compiler(BaseCompiler):

    def __init__(self, connect_args: dict, data_dir: str):
        super().__init__(connect_args, data_dir)

        self._current_db_state = None
        self._bootstrap_mode = False

        if data_dir is not None:
            self._data_dir = pathlib.Path(data_dir)
            self._std_schema = stdschema.load_std_schema(self._data_dir)
        else:
            self._data_dir = None
            self._std_schema = None

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
        session_config = current_tx.get_session_config()

        native_out_format = (
            ctx.output_format is pg_compiler.OutputFormat.NATIVE
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

        ir = ql_compiler.compile_ast_to_ir(
            ql,
            schema=current_tx.get_schema(),
            modaliases=current_tx.get_modaliases(),
            implicit_tid_in_shapes=implicit_fields,
            implicit_id_in_shapes=implicit_fields,
            disable_constant_folding=disable_constant_folding,
            json_parameters=ctx.json_parameters)

        if ir.cardinality is qltypes.Cardinality.ONE:
            result_cardinality = enums.ResultCardinality.ONE
        else:
            result_cardinality = enums.ResultCardinality.MANY

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=debug.flags.edgeql_compile,
            expected_cardinality_one=ctx.expected_cardinality_one,
            output_format=ctx.output_format)

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

        if single_stmt_mode:
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

            in_type_args = None
            if ctx.json_parameters:
                in_type_args = [None] * len(argmap)
                for argname, argpos in argmap.items():
                    in_type_args[argpos - 1] = argname

            sql_hash = self._hash_sql(
                sql_bytes, mode=str(ctx.output_format).encode())

            return dbstate.Query(
                sql=(sql_bytes,),
                sql_hash=sql_hash,
                cardinality=result_cardinality,
                in_type_id=in_type_id.bytes,
                in_type_data=in_type_data,
                in_type_args=in_type_args,
                out_type_id=out_type_id.bytes,
                out_type_data=out_type_data,
            )

        else:
            if ir.params:
                raise errors.QueryError(
                    'EdgeQL script queries cannot accept parameters')

            return dbstate.SimpleQuery(sql=(sql_bytes,))

    def _compile_and_apply_delta_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()
        context = self._new_delta_context(ctx)

        if current_tx.is_implicit():
            if isinstance(cmd, s_deltas.CreateDelta):
                command = 'CREATE MIGRATION'
            elif isinstance(cmd, s_deltas.GetDelta):
                command = 'GET MIGRATION'
            else:
                command = 'COMMIT MIGRATION'
            raise errors.QueryError(
                f'{command} must be executed in a transaction block')

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
                # We must return *some* SQL; return a no-op command.
                return dbstate.DDLQuery(
                    sql=(b'SELECT NULL LIMIT 0;',))

            else:
                raise errors.InternalServerError(
                    f'unexpected delta command: {cmd!r}')  # pragma: no cover

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

        if debug.flags.delta_execute:
            debug.header('Delta Script')
            debug.dump_code(sql, lexer='sql')

        return dbstate.DDLQuery(sql=(sql,))

    def _compile_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        if isinstance(cmd, s_deltas.DeltaCommand):
            return self._compile_and_apply_delta_command(ctx, cmd)

        elif isinstance(cmd, s_delta.Command):
            return self._compile_and_apply_ddl_command(ctx, cmd)

        else:
            raise errors.InternalServerError(
                f'unexpected plan {cmd!r}')  # pragma: no cover

    def _compile_ql_ddl(self, ctx: CompileContext, ql: qlast.DDL):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        cmd = s_ddl.delta_from_ddl(
            ql,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=self._in_testmode(ctx),
        )

        return self._compile_command(ctx, cmd)

    def _compile_ql_migration(self, ctx: CompileContext,
                              ql: typing.Union[qlast.Database, qlast.Delta]):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        cmd = s_ddl.cmd_from_ddl(
            ql,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            testmode=self._in_testmode(ctx))

        if (isinstance(ql, qlast.CreateDelta) and
                cmd.get_attribute_value('target')):

            if current_tx.is_implicit():
                raise errors.QueryError(
                    f'CREATE MIGRATION must be executed in a '
                    f'transaction block')

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
                schema.get(ql.module)
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

        ir = ql_compiler.compile_ast_to_ir(
            ql,
            schema=schema,
            modaliases=modaliases,
        )

        sql_text, _ = pg_compiler.compile_ir_to_sql(
            ir,
            pretty=debug.flags.edgeql_compile,
            output_format=pg_compiler.OutputFormat.JSON)

        if not ql.system:
            config_op = ireval.evaluate_to_config_op(ir, schema=schema)

            session_config = config_op.apply(
                config.get_settings(),
                session_config)
            ctx.state.current_tx().update_session_config(session_config)
        else:
            config_op = None

        return dbstate.SessionStateQuery(
            sql=(sql_text.encode(),),
            is_system_setting=ql.system,
            config_op=config_op,
        )

    def _compile_dispatch_ql(self, ctx: CompileContext, ql: qlast.Base):

        if isinstance(ql, (qlast.Database, qlast.Delta)):
            if not (ctx.state.capability & enums.Capability.DDL):
                raise errors.ProtocolError(
                    f'cannot execute DDL commands for the current connection')
            return self._compile_ql_migration(ctx, ql)

        elif isinstance(ql, qlast.DDL):
            if not (ctx.state.capability & enums.Capability.DDL):
                raise errors.ProtocolError(
                    f'cannot execute DDL commands for the current connection')
            return self._compile_ql_ddl(ctx, ql)

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

    def _compile(self, *,
                 ctx: CompileContext,
                 eql: bytes) -> typing.List[dbstate.QueryUnit]:

        # When True it means that we're compiling for "connection.fetchall()".
        # That means that the returned QueryUnit has to have the in/out codec
        # information, correctly inferred "singleton_result" field etc.
        single_stmt_mode = ctx.stmt_mode is enums.CompileStatementMode.SINGLE
        default_cardinality = enums.ResultCardinality.NOT_APPLICABLE

        eql = eql.decode()

        statements = edgeql.parse_block(eql)
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
                if (isinstance(comp, dbstate.TxControlQuery) and
                        comp.single_unit):
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

            if isinstance(comp, dbstate.Query):
                if single_stmt_mode:
                    unit.sql = comp.sql
                    unit.sql_hash = comp.sql_hash

                    unit.out_type_data = comp.out_type_data
                    unit.out_type_id = comp.out_type_id
                    unit.in_type_data = comp.in_type_data
                    unit.in_type_args = comp.in_type_args
                    unit.in_type_id = comp.in_type_id

                    unit.cacheable = True

                    unit.cardinality = comp.cardinality
                else:
                    unit.sql += comp.sql

            elif isinstance(comp, dbstate.SimpleQuery):
                assert not single_stmt_mode
                unit.sql += comp.sql

            elif isinstance(comp, dbstate.DDLQuery):
                unit.sql += comp.sql
                unit.has_ddl = True

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

            elif isinstance(comp, dbstate.SessionStateQuery):
                unit.sql += comp.sql

                if comp.is_system_setting:
                    if (not ctx.state.current_tx().is_implicit() or
                            statements_len > 1):
                        raise errors.QueryError(
                            'CONFIGURE SYSTEM cannot be executed in a '
                            'transaction block')

                    unit.system_config = True

                if ctx.state.current_tx().is_implicit():
                    unit.modaliases = ctx.state.current_tx().get_modaliases()

                if comp.config_op is not None:
                    if unit.config_ops is None:
                        unit.config_ops = []
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
            if (ctx.expected_cardinality_one and
                    units[0].cardinality is enums.ResultCardinality.MANY):
                raise errors.ResultCardinalityMismatchError(
                    f'the query has cardinality {units[0].cardinality} '
                    f'which does not match the expected cardinality ONE')

        for unit in units:  # pragma: no cover
            # Sanity checks
            na_cardinality = (
                unit.cardinality is enums.ResultCardinality.NOT_APPLICABLE
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
            self, *, dbver: int, json_mode: bool, expect_one: bool,
            modaliases,
            session_config: typing.Optional[immutables.Map],
            stmt_mode: typing.Optional[enums.CompileStatementMode],
            capability: enums.Capability,
            json_parameters: bool=False):

        if session_config is None:
            session_config = EMPTY_MAP
        if modaliases is None:
            modaliases = DEFAULT_MODULE_ALIASES_MAP

        assert isinstance(modaliases, immutables.Map)
        assert isinstance(session_config, immutables.Map)

        db = await self._get_database(dbver)
        self._current_db_state = dbstate.CompilerConnectionState(
            dbver,
            db.schema,
            modaliases,
            session_config,
            capability)

        state = self._current_db_state

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            state=state,
            output_format=of,
            expected_cardinality_one=expect_one,
            stmt_mode=stmt_mode,
            json_parameters=json_parameters)

        return ctx

    async def _ctx_from_con_state(self, *, txid: int, json_mode: bool,
                                  expect_one: bool,
                                  stmt_mode: enums.CompileStatementMode):
        state = self._load_state(txid)

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            state=state,
            output_format=of,
            expected_cardinality_one=expect_one,
            stmt_mode=stmt_mode)

        return ctx

    def _load_state(self, txid: int):
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

    async def try_compile_rollback(self, dbver: int, eql: bytes):
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

    async def compile_eql(
            self,
            dbver: int,
            eql: bytes,
            sess_modaliases: typing.Optional[immutables.Map],
            sess_config: typing.Optional[immutables.Map],
            json_mode: bool,
            expect_one: bool,
            stmt_mode: enums.CompileStatementMode,
            capability: enums.Capability,
            json_parameters: bool=False) -> typing.List[dbstate.QueryUnit]:

        ctx = await self._ctx_new_con_state(
            dbver=dbver,
            json_mode=json_mode,
            expect_one=expect_one,
            modaliases=sess_modaliases,
            session_config=sess_config,
            stmt_mode=enums.CompileStatementMode(stmt_mode),
            capability=capability,
            json_parameters=json_parameters)

        return self._compile(ctx=ctx, eql=eql)

    async def compile_eql_in_tx(
            self,
            txid: int,
            eql: bytes,
            json_mode: bool,
            expect_one: bool,
            stmt_mode: enums.CompileStatementMode
    ) -> typing.List[dbstate.QueryUnit]:

        ctx = await self._ctx_from_con_state(
            txid=txid,
            json_mode=json_mode,
            expect_one=expect_one,
            stmt_mode=enums.CompileStatementMode(stmt_mode))

        return self._compile(ctx=ctx, eql=eql)

    async def interpret_backend_error(self, dbver, fields):
        db = await self._get_database(dbver)
        return errormech.interpret_backend_error(db.schema, fields)
