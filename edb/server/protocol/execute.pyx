#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

from typing import (
    Any,
    Mapping,
    Optional,
)

from edgedb import scram

import asyncio
import base64
import decimal
import hashlib
import json
import logging

import immutables

from edb import errors
from edb.common import debug

from edb import edgeql
from edb.edgeql import qltypes

from edb.pgsql.parser import exceptions as parser_errors

from edb.server import compiler
from edb.server import config
from edb.server import defines as edbdef
from edb.server import metrics
from edb.server.compiler import dbstate
from edb.server.compiler import errormech
from edb.server.compiler cimport rpc
from edb.server.compiler import sertypes
from edb.server.dbview cimport dbview
from edb.server.protocol cimport args_ser
from edb.server.protocol cimport frontend
from edb.server.pgcon cimport pgcon
from edb.server.pgcon import errors as pgerror


cdef object logger = logging.getLogger('edb.server')

cdef object FMT_NONE = compiler.OutputFormat.NONE
cdef WriteBuffer NO_ARGS = args_ser.combine_raw_args()


cdef class ExecutionGroup:
    def __cinit__(self):
        self.group = compiler.QueryUnitGroup()
        self.bind_datas = []

    cdef append(self, object query_unit, WriteBuffer bind_data=NO_ARGS):
        self.group.append(query_unit, serialize=False)
        self.bind_datas.append(bind_data)

    async def execute(
        self,
        pgcon.PGConnection be_conn,
        object dbv,  # can be DatabaseConnectionView or Database
        fe_conn: frontend.AbstractFrontendConnection = None,
        bytes state = None,
    ):
        cdef int dbver

        rv = None
        async with be_conn.parse_execute_script_context():
            dbver = dbv.dbver
            parse_array = [False] * len(self.group)
            be_conn.send_query_unit_group(
                self.group,
                True,  # sync
                self.bind_datas,
                state,
                0,  # start
                len(self.group),  # end
                dbver,
                parse_array,
                None,  # query_prefix
            )
            if state is not None:
                await be_conn.wait_for_state_resp(state, state_sync=0)
            for i, unit in enumerate(self.group):
                ignore_data = unit.output_format == FMT_NONE
                rv = await be_conn.wait_for_command(
                    unit,
                    parse_array[i],
                    dbver,
                    ignore_data=ignore_data,
                    fe_conn=None if ignore_data else fe_conn,
                )
        return rv


cpdef ExecutionGroup build_cache_persistence_units(
    pairs: list[tuple[rpc.CompilationRequest, compiler.QueryUnitGroup]],
    ExecutionGroup group = None,
):
    if group is None:
        group = ExecutionGroup()
    insert_sql = b'''
        INSERT INTO "edgedb"."_query_cache"
        ("key", "schema_version", "input", "output", "evict")
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (key) DO NOTHING
    '''
    sql_hash = hashlib.sha1(insert_sql).hexdigest().encode('latin1')
    for request, units in pairs:
        # FIXME: this is temporary; drop this assertion when we support scripts
        assert len(units) == 1
        query_unit = units[0]

        assert query_unit.cache_sql is not None
        persist, evict = query_unit.cache_sql

        serialized_result = units.maybe_get_serialized(0)
        assert serialized_result is not None

        if evict:
            group.append(compiler.QueryUnit(sql=evict, status=b''))
        if persist:
            group.append(compiler.QueryUnit(sql=persist, status=b''))
        group.append(
            compiler.QueryUnit(sql=insert_sql, sql_hash=sql_hash, status=b''),
            args_ser.combine_raw_args((
                query_unit.cache_key.bytes,
                query_unit.user_schema_version.bytes,
                request.serialize(),
                serialized_result,
                evict,
            )),
        )
    return group


async def describe(
    db: dbview.Database,
    query: str,
    *,
    query_cache_enabled: Optional[bool] = None,
    allow_capabilities: compiler.Capability = compiler.Capability.MODIFICATIONS,
    query_tag: str | None = None,
) -> sertypes.TypeDesc:
    compiled, dbv = await _parse(
        db,
        query,
        query_cache_enabled=query_cache_enabled,
        allow_capabilities=allow_capabilities,
    )
    if query_tag:
        compiled.tag = query_tag

    try:
        desc = sertypes.parse(
            compiled.query_unit_group.out_type_data,
            edbdef.CURRENT_PROTOCOL,
        )
    finally:
        db.tenant.remove_dbview(dbv)

    return desc


async def _parse(
    db: dbview.Database,
    query: str,
    *,
    input_format: compiler.InputFormat = compiler.InputFormat.BINARY,
    output_format: compiler.OutputFormat = compiler.OutputFormat.BINARY,
    allow_capabilities: compiler.Capability = compiler.Capability.MODIFICATIONS,
    use_metrics: bool = True,
    cached_globally: bool = False,
    query_cache_enabled: Optional[bool] = None,
) -> tuple[dbview.CompiledQuery, dbview.DatabaseConnectionView]:
    if query_cache_enabled is None:
        query_cache_enabled = not (
            debug.flags.disable_qcache or debug.flags.edgeql_compile)

    tenant = db.tenant
    dbv = await tenant.new_dbview(
        dbname=db.name,
        query_cache=query_cache_enabled,
        protocol_version=edbdef.CURRENT_PROTOCOL,
    )
    dbv.is_transient = True
    if use_metrics:
        metrics.query_size.observe(
            len(query.encode('utf-8')), tenant.get_instance_name(), 'edgeql'
        )

    query_req = rpc.CompilationRequest(
        source=edgeql.Source.from_string(query),
        protocol_version=edbdef.CURRENT_PROTOCOL,
        schema_version=dbv.schema_version,
        compilation_config_serializer=db.server.compilation_config_serializer,
        input_format=input_format,
        output_format=output_format,
    )

    compiled = await dbv.parse(
        query_req,
        cached_globally=cached_globally,
        use_metrics=use_metrics,
        allow_capabilities=allow_capabilities,
    )

    return compiled, dbv


# TODO: can we merge execute and execute_script?
async def execute(
    be_conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    bind_args: bytes,
    *,
    fe_conn: frontend.AbstractFrontendConnection = None,
    use_prep_stmt: bint = False,
    tx_isolation: edbdef.TxIsolationLevel | None = None,
):
    cdef:
        bytes state = None, orig_state = None
        WriteBuffer bound_args_buf

    query_unit = compiled.query_unit_group[0]

    if not dbv.in_tx():
        orig_state = state = dbv.serialize_state()

    new_types = None
    server = dbv.server
    tenant = dbv.tenant

    data = None

    try:
        if be_conn.last_state == state:
            # the current status in be_conn is in sync with dbview, skip the
            # state restoring
            state = None
        dbv.start(query_unit)
        if query_unit.create_db_template:
            await tenant.on_before_create_db_from_template(
                query_unit.create_db_template,
                dbv.dbname,
                query_unit.create_db_mode,
            )
        if query_unit.drop_db:
            await tenant.on_before_drop_db(
                query_unit.drop_db,
                dbv.dbname,
                close_frontend_conns=query_unit.drop_db_reset_connections,
            )
        if query_unit.system_config:
            await execute_system_config(be_conn, dbv, query_unit, state)
        else:
            config_ops = query_unit.config_ops

            if query_unit.sql:
                if query_unit.user_schema:
                    await be_conn.parse_execute(query=query_unit, state=state)
                    if query_unit.ddl_stmt_id is not None:
                        ddl_ret = be_conn.load_last_ddl_return(query_unit)
                        if ddl_ret and ddl_ret['new_types']:
                            new_types = ddl_ret['new_types']
                else:
                    data_types = []
                    bound_args_buf = args_ser.recode_bind_args(
                        dbv, compiled, bind_args, None, data_types)

                    assert not (query_unit.database_config
                                and query_unit.needs_readback), (
                        "needs_readback+database_config must use execute_script"
                    )
                    read_data = (
                        query_unit.needs_readback or query_unit.is_explain)

                    data = await be_conn.parse_execute(
                        query=query_unit,
                        fe_conn=fe_conn if not read_data else None,
                        bind_data=bound_args_buf,
                        param_data_types=data_types,
                        use_prep_stmt=use_prep_stmt,
                        state=state,
                        dbver=dbv.dbver,
                        use_pending_func_cache=compiled.use_pending_func_cache,
                        tx_isolation=tx_isolation,
                        query_prefix=compiled.make_query_prefix(),
                    )

                    if query_unit.needs_readback and data:
                        config_ops = [
                            config.Operation.from_json(r[0][1:])
                            for r in data
                        ]

                    if query_unit.is_explain:
                        # Go back to the compiler pool to analyze
                        # the explain output.
                        compiler_pool = server.get_compiler_pool()
                        r = await compiler_pool.analyze_explain_output(
                            query_unit.query_asts, data
                        )
                        buf = WriteBuffer.new_message(b'D')
                        buf.write_int16(1)  # 1 column
                        buf.write_len_prefixed_bytes(r)
                        fe_conn.write(buf.end_message())

                if state is not None:
                    # state is restored, clear orig_state so that we can
                    # set be_conn.last_state correctly later
                    orig_state = None

            if query_unit.tx_savepoint_rollback:
                dbv.rollback_tx_to_savepoint(query_unit.sp_name)

            if query_unit.tx_savepoint_declare:
                dbv.declare_savepoint(
                    query_unit.sp_name, query_unit.sp_id)

            if query_unit.create_db_template:
                try:
                    await tenant.on_after_create_db_from_template(
                        query_unit.create_db,
                        query_unit.create_db_template,
                        query_unit.create_db_mode,
                    )
                except Exception:
                    # Clean up the database if we failed to restore into it.
                    # TODO: Is it worth having 'ready' flag that we set after
                    # the database is fully set up, and use that to clean up
                    # databases where a crash prevented doing this cleanup?
                    db_name = f'{tenant.tenant_id}_{query_unit.create_db}'
                    await be_conn.sql_execute(
                        b'drop database "%s"' % db_name.encode('utf-8')
                    )
                    raise

            if query_unit.create_db:
                await tenant.introspect_db(query_unit.create_db)

            if query_unit.drop_db:
                tenant.on_after_drop_db(query_unit.drop_db)

            if config_ops:
                await dbv.apply_config_ops(be_conn, config_ops)

            if query_unit.user_schema and debug.flags.delta_validate_reflection:
                global_schema = (
                    query_unit.global_schema or dbv.get_global_schema_pickle())
                new_user_schema = await dbv.tenant._debug_introspect(
                    be_conn, global_schema)
                compiler_pool = dbv.server.get_compiler_pool()
                await compiler_pool.validate_schema_equivalence(
                    query_unit.user_schema,
                    new_user_schema,
                    global_schema,
                    dbv._last_comp_state,
                )
                query_unit.user_schema = new_user_schema

    except Exception as ex:
        # If we made schema changes, include the new schema in the
        # exception so that it can be used when interpreting.
        if query_unit.user_schema:
            if isinstance(ex, pgerror.BackendError):
                ex._user_schema = query_unit.user_schema
        if query_unit.source_map:
            ex._from_sql = True

        dbv.on_error()

        if query_unit.tx_commit and not be_conn.in_tx() and dbv.in_tx():
            # The COMMIT command has failed. Our Postgres connection
            # isn't in a transaction anymore. Abort the transaction
            # in dbview.
            dbv.abort_tx()
        raise
    else:
        side_effects = dbv.on_success(query_unit, new_types)
        state_serializer = compiled.query_unit_group.state_serializer
        if state_serializer is not None:
            dbv.set_state_serializer(state_serializer)
        if side_effects:
            await process_side_effects(dbv, side_effects, be_conn)
        if not dbv.in_tx() and not query_unit.tx_rollback and query_unit.sql:
            state = dbv.serialize_state()
            if state is not orig_state:
                # In 3 cases the state is changed:
                #   1. The non-tx query changed the state
                #   2. The state is synced with dbview (orig_state is None)
                #   3. We came out from a transaction (orig_state is None)
                # Excluding two special case when the state is NOT changed:
                #   1. An orphan ROLLBACK command without a paring start tx
                #   2. There was no SQL, so the state can't have been synced.
                be_conn.last_state = state
        if compiled.recompiled_cache:
            for req, qu_group in compiled.recompiled_cache:
                dbv.cache_compiled_query(req, qu_group)
    finally:
        if query_unit.drop_db:
            tenant.allow_database_connections(query_unit.drop_db)

    return data


async def execute_script(
    conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    bind_args: bytes,
    *,
    fe_conn: Optional[frontend.AbstractFrontendConnection],
):
    cdef:
        bytes state = None, orig_state = None
        ssize_t sent = 0
        bint in_tx, sync, no_sync
        object user_schema, extensions, ext_config_settings, cached_reflection
        object global_schema, roles
        WriteBuffer bind_data
        int dbver = dbv.dbver
        bint parse

    user_schema = extensions = ext_config_settings = cached_reflection = None
    feature_used_metrics = None
    global_schema = roles = None
    unit_group = compiled.query_unit_group
    query_prefix = compiled.make_query_prefix()
    query_unit = None

    sync = False
    no_sync = False
    in_tx = dbv.in_tx()
    if not in_tx:
        orig_state = state = dbv.serialize_state()

    data = None

    try:
        if conn.last_state == state:
            # the current status in be_conn is in sync with dbview, skip the
            # state restoring
            state = None
        async with conn.parse_execute_script_context():
            parse_array = [False] * len(unit_group)
            for idx, query_unit in enumerate(unit_group):
                if fe_conn is not None and fe_conn.cancelled:
                    raise ConnectionAbortedError

                assert not query_unit.is_explain

                # XXX: pull out?
                # We want to minimize the round trips we need to make, so
                # ideally we buffer up everything, send it once, and then issue
                # one SYNC. This gets messed up if there are commands where
                # we need to read back information, though, such as SET GLOBAL.
                #
                # Because of that, we look for the next command that
                # needs read back (probably there won't be one!), and
                # execute everything up to that point at once,
                # finished by a FLUSH.
                if idx >= sent:
                    no_sync = False
                    for n in range(idx, len(unit_group)):
                        ng = unit_group[n]
                        if ng.ddl_stmt_id or ng.needs_readback:
                            sent = n + 1
                            if ng.needs_readback:
                                no_sync = True
                            break
                    else:
                        sent = len(unit_group)

                    sync = sent == len(unit_group) and not no_sync
                    bind_array = args_ser.recode_bind_args_for_script(
                        dbv, compiled, bind_args, idx, sent)
                    dbver = dbv.dbver
                    conn.send_query_unit_group(
                        unit_group,
                        sync,
                        bind_array,
                        state,
                        idx,
                        sent,
                        dbver,
                        parse_array,
                        query_prefix,
                    )

                if idx == 0 and state is not None:
                    await conn.wait_for_state_resp(state, state_sync=0)
                    # state is restored, clear orig_state so that we can
                    # set conn.last_state correctly later
                    orig_state = None

                new_types = None
                dbv.start_implicit(query_unit)
                config_ops = query_unit.config_ops

                if query_unit.user_schema:
                    user_schema = query_unit.user_schema
                    extensions = query_unit.extensions
                    ext_config_settings = query_unit.ext_config_settings
                    cached_reflection = query_unit.cached_reflection
                    feature_used_metrics = query_unit.feature_used_metrics

                if query_unit.global_schema:
                    global_schema = query_unit.global_schema
                    roles = query_unit.roles

                if query_unit.sql:
                    parse = parse_array[idx]
                    fe_output = query_unit.output_format != FMT_NONE
                    ignore_data = (
                        not fe_output
                        and not query_unit.needs_readback
                    )
                    data = await conn.wait_for_command(
                        query_unit,
                        parse,
                        dbver,
                        ignore_data=ignore_data,
                        fe_conn=fe_conn if fe_output else None,
                    )

                    if query_unit.ddl_stmt_id:
                        ddl_ret = conn.load_last_ddl_return(query_unit)
                        if ddl_ret and ddl_ret['new_types']:
                            new_types = ddl_ret['new_types']

                    if query_unit.needs_readback and data:
                        config_ops = [
                            config.Operation.from_json(r[0][1:])
                            for r in data
                        ]

                if config_ops:
                    await dbv.apply_config_ops(conn, config_ops)

                side_effects = dbv.on_success(query_unit, new_types)
                if side_effects:
                    raise errors.InternalServerError(
                        "Side-effects in implicit transaction!"
                    )

        # Need to sync before calling process_side_effects, which will
        # look at the database. Also, want to sync before we record success,
        # since sync could fail.
        if sent and not sync:
            sync = True
            await conn.sync()

    except Exception as e:
        dbv.on_error()

        # Include the new schema in the exception so that it can be
        # used when interpreting.
        if isinstance(e, pgerror.BackendError):
            e._user_schema = dbv.get_user_schema_pickle()

        if query_unit and query_unit.source_map:
            e._from_sql = True

        if not in_tx and dbv.in_tx():
            # Abort the implicit transaction
            dbv.abort_tx()

        # If something went wrong that is *not* on the backend side, force
        # an error to occur on the SQL side.
        if not isinstance(e, pgerror.BackendError):
            await conn.force_error()

        raise

    else:
        updated_user_schema = False
        if user_schema and debug.flags.delta_validate_reflection:
            cur_global_schema = (
                global_schema or dbv.get_global_schema_pickle())
            new_user_schema = await dbv.tenant._debug_introspect(
                conn, cur_global_schema)
            compiler_pool = dbv.server.get_compiler_pool()
            await compiler_pool.validate_schema_equivalence(
                user_schema,
                new_user_schema,
                cur_global_schema,
                dbv._last_comp_state,
            )
            user_schema = new_user_schema
            updated_user_schema = True

        if not in_tx:
            side_effects = dbv.commit_implicit_tx(
                user_schema,
                extensions,
                ext_config_settings,
                global_schema,
                roles,
                cached_reflection,
                feature_used_metrics,
            )
            if side_effects:
                await process_side_effects(dbv, side_effects, conn)
            state = dbv.serialize_state()
            if state is not orig_state:
                conn.last_state = state
        elif updated_user_schema:
            dbv._in_tx_user_schema_pickle = user_schema

        if unit_group.state_serializer is not None:
            dbv.set_state_serializer(unit_group.state_serializer)

    finally:
        if sent and not sync:
            await conn.sync()

    return data


async def execute_system_config(
    conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    query_unit: compiler.QueryUnit,
    state: bytes | None,
):
    if query_unit.is_system_config:
        dbv.server.before_alter_system_config()

    # Sync state
    await conn.sql_fetch(b'select 1', state=state)

    if query_unit.sql:
        data = await conn.sql_fetch_col(query_unit.sql)
    else:
        data = None

    if data:
        # Prefer encoded op produced by the SQL command.
        if data[0][0] != 0x01:
            raise errors.InternalServerError(
                f"unexpected JSONB version produced by SQL statement for "
                f"CONFIGURE INSTANCE: {data[0][0]}"
            )
        config_ops = [config.Operation.from_json(r[1:]) for r in data]
    else:
        # Otherwise, fall back to staticly evaluated op.
        config_ops = query_unit.config_ops
    await dbv.apply_config_ops(conn, config_ops)

    await conn.sql_execute(b'delete from _config_cache')

    # If this is a backend configuration setting we also
    # need to make sure it has been loaded.
    if query_unit.backend_config:
        await conn.sql_execute(b'SELECT pg_reload_conf()')


async def process_side_effects(dbv, side_effects, conn):
    signal_side_effects(dbv, side_effects)

    if side_effects & dbview.SideEffects.DatabaseConfigChanges:
        tenant = dbv.tenant
        await tenant.process_local_database_config_change(conn, dbv.dbname)


def signal_side_effects(dbv, side_effects):
    tenant = dbv.tenant
    if not tenant.accept_new_tasks:
        return

    if side_effects & dbview.SideEffects.SchemaChanges:
        tenant.create_task(
            tenant.signal_sysevent(
                'schema-changes',
                dbname=dbv.dbname,
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.GlobalSchemaChanges:
        tenant.create_task(
            tenant.signal_sysevent(
                'global-schema-changes',
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.DatabaseConfigChanges:
        tenant.create_task(
            tenant.signal_sysevent(
                'database-config-changes',
                dbname=dbv.dbname,
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.DatabaseChanges:
        tenant.create_task(
            tenant.signal_sysevent(
                'database-changes',
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.InstanceConfigChanges:
        tenant.create_task(
            tenant.signal_sysevent(
                'system-config-changes',
            ),
            interruptable=False,
        )


async def parse_execute_json(
    db: dbview.Database,
    query: str,
    *,
    variables: Mapping[str, Any] = immutables.Map(),
    globals_: Optional[Mapping[str, Any]] = None,
    output_format: compiler.OutputFormat = compiler.OutputFormat.JSON,
    query_cache_enabled: Optional[bool] = None,
    cached_globally: bool = False,
    use_metrics: bool = True,
    tx_isolation: edbdef.TxIsolationLevel | None = None,
    query_tag: str | None = None
) -> bytes:
    # WARNING: only set cached_globally to True when the query is
    # strictly referring to only shared stable objects in user schema
    # or anything from std schema, for example:
    #     YES:  select ext::auth::UIConfig { ... }
    #     NO:   select default::User { ... }
    compiled, dbv = await _parse(
        db,
        query,
        input_format=compiler.InputFormat.JSON,
        output_format=output_format,
        allow_capabilities=compiler.Capability.MODIFICATIONS,
        use_metrics=use_metrics,
        cached_globally=cached_globally,
        query_cache_enabled=query_cache_enabled,
    )
    if query_tag:
        compiled.tag = query_tag

    tenant = db.tenant
    async with tenant.with_pgcon(db.name) as pgcon:
        try:
            return await execute_json(
                pgcon,
                dbv,
                compiled,
                variables=variables,
                globals_=globals_,
                tx_isolation=tx_isolation,
            )
        finally:
            tenant.remove_dbview(dbv)


async def execute_json(
    be_conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    variables: Mapping[str, Any] = immutables.Map(),
    globals_: Optional[Mapping[str, Any]] = None,
    *,
    fe_conn: Optional[frontend.AbstractFrontendConnection] = None,
    use_prep_stmt: bint = False,
    tx_isolation: edbdef.TxIsolationLevel | None = None,
) -> bytes:
    dbv.set_globals(immutables.Map({
        "__::__edb_json_globals__": config.SettingValue(
            name="__::__edb_json_globals__",
            value=_encode_json_value(globals_),
            source='global',
            scope=qltypes.ConfigScope.GLOBAL,
        )
    }))

    qug = compiled.query_unit_group

    args = []
    if qug.in_type_args:
        for param in qug.in_type_args:
            value = variables.get(param.name)
            args.append(value)

    bind_args = _encode_args(args)

    force_script = any(x.needs_readback for x in qug)
    if len(qug) > 1 or force_script:
        if tx_isolation is not None:
            raise errors.InternalServerError(
                "execute_script does not support "
                "modified transaction isolation"
            )
        data = await execute_script(
            be_conn,
            dbv,
            compiled,
            bind_args,
            fe_conn=fe_conn,
        )
    else:
        if tx_isolation is not None:
            if dbv.in_tx():
                raise errors.InternalServerError(
                    "cannot run statement with alternate transaction "
                    "isolation: already in a transaction"
                )

            query_unit = compiled.query_unit_group[0]
            if not query_unit.is_transactional:
                raise errors.InternalServerError(
                    "cannot run statement with alternate transaction "
                    "isolation: statement is not transactional"
                )

        data = await execute(
            be_conn,
            dbv,
            compiled,
            bind_args,
            fe_conn=fe_conn,
            tx_isolation=tx_isolation,
        )

    if fe_conn is None:
        if not data or len(data) > 1 or len(data[0]) != 1:
            raise errors.InternalServerError(
                f'received incorrect response data for a JSON query')

        return data[0][0]
    else:
        return None


class DecimalEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, dict):
            return '{' + ', '.join(
                    f'{self.encode(k)}: {self.encode(v)}'
                    for (k, v) in obj.items()
                ) + '}'
        if isinstance(obj, list):
            return '[' + ', '.join(map(self.encode, obj)) + ']'
        if isinstance(obj, bytes):
            return self.encode(base64.b64encode(obj).decode())
        if isinstance(obj, decimal.Decimal):
            return f'{obj:f}'
        return super().encode(obj)


cdef bytes _encode_json_value(object val):
    jarg = json.dumps(val, cls=DecimalEncoder)

    return b'\x01' + jarg.encode('utf-8')


cdef bytes _encode_args(list args):
    cdef:
        WriteBuffer out_buf = WriteBuffer.new()

    if args:
        out_buf.write_int32(len(args))
        for arg in args:
            out_buf.write_int32(0)  # reserved
            if arg is None:
                out_buf.write_int32(-1)
            else:
                jval = _encode_json_value(arg)
                out_buf.write_int32(len(jval))
                out_buf.write_bytes(jval)

    return bytes(out_buf)


cdef _check_for_ise(exc):
    # Unwrap ExceptionGroup that has only one Exception
    if isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
        exc = exc.exceptions[0]

    if not isinstance(exc, errors.EdgeDBError):
        # TODO(rename): change URL once we can
        nexc = errors.InternalServerError(
            f'{type(exc).__name__}: {exc}',
            hint=(
                f'This is most likely a bug in Gel. '
                f'Please consider opening an issue ticket '
                f'at https://github.com/edgedb/edgedb/issues/new'
                f'?template=bug_report.md'
            ),
        ).with_traceback(exc.__traceback__)
        formatted = getattr(exc, '__formatted_error__', None)
        if formatted:
            nexc.__formatted_error__ = formatted
        if isinstance(exc, BaseExceptionGroup):
            nexc.__cause__ = exc.with_traceback(None)
        exc = nexc

    return exc


async def interpret_error(
    exc: Exception,
    db: dbview.Database,
    *,
    global_schema_pickle: object=None,
    user_schema_pickle: object=None,
    from_graphql: bool=False,
) -> Exception:

    if isinstance(exc, RecursionError):
        exc = errors.UnsupportedFeatureError(
            "The query caused the compiler "
            "stack to overflow. It is likely too deeply nested.",
            hint=(
                "If the query does not contain deep nesting, "
                "this may be a bug."
            ),
        )

    elif isinstance(exc, pgerror.BackendError):
        try:
            from_sql = getattr(exc, '_from_sql', False)
            source_map = getattr(exc, '_source_map', None)
            fields = exc.fields

            static_exc = errormech.static_interpret_backend_error(
                fields, from_graphql=from_graphql
            )

            # only use the backend if schema is required
            if static_exc is errormech.SchemaRequired:
                # Grab the schema from the exception first, if it is present.
                user_schema_pickle = (
                    getattr(exc, '_user_schema', None)
                    or user_schema_pickle
                    or db.user_schema_pickle
                )
                global_schema_pickle = (
                    global_schema_pickle or db._index._global_schema_pickle
                )
                compiler_pool = db._index._server.get_compiler_pool()
                exc = await compiler_pool.interpret_backend_error(
                    user_schema_pickle,
                    global_schema_pickle,
                    fields,
                    from_graphql,
                )

            elif isinstance(static_exc, (
                    errors.DuplicateDatabaseDefinitionError,
                    errors.UnknownDatabaseError)):
                tenant_id = db.tenant.tenant_id
                message = static_exc.args[0].replace(f'{tenant_id}_', '')
                exc = type(static_exc)(message)
            else:
                exc = static_exc

            if from_sql and isinstance(exc, errors.InternalServerError):
                exc = errors.ExecutionError(*exc.args)

            # Translate error position for SQL queries if we can
            if source_map and isinstance(exc, errors.EdgeDBError):
                if 'P' in fields:
                    exc.set_position(
                        0,
                        0,
                        source_map.translate(int(fields['P'])),
                        None,
                    )

            # Include hint/detail from SQL queries also, if we haven't
            # produced our own.
            if from_sql and isinstance(exc, errors.EdgeDBError):
                if 'H' in fields or 'D' in fields:
                    hint = exc.hint or fields.get('H')
                    details = exc.details or fields.get('D')
                    # ... there is some sort of cython bug/"feature"
                    # involving the type annotation above which causes
                    # exc.set_hint_and_details to fail, so we copy it
                    # to a new variable.
                    exc2: object = exc
                    exc2.set_hint_and_details(hint, details)

        except Exception as e:
            from edb.common import debug
            if debug.flags.server:
                debug.dump(e)

            exc = RuntimeError(
                'unhandled error while calling interpret_backend_error(); '
                'run with EDGEDB_DEBUG_SERVER to debug.')

    elif isinstance(exc, parser_errors.PSqlParseError):
        exc = errormech.static_interpret_psql_parse_error(exc)

    return _check_for_ise(exc)


def interpret_simple_error(
    exc: Exception,
) -> Exception:
    """Intepret a protocol error not associated with a query or schema"""

    if isinstance(exc, pgerror.BackendError):
        static_exc = errormech.static_interpret_backend_error(exc.fields)
        if static_exc is not errormech.SchemaRequired:
            exc = static_exc

    return _check_for_ise(exc)
