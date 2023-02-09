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

import decimal
import json

import immutables

from edb import errors
from edb.common import debug

from edb import edgeql
from edb.edgeql import qltypes

from edb.server import compiler
from edb.server import config
from edb.server import defines as edbdef
from edb.server.dbview cimport dbview
from edb.server.protocol cimport args_ser
from edb.server.protocol cimport frontend
from edb.server.pgproto.pgproto cimport WriteBuffer
from edb.server.pgcon cimport pgcon


cdef object FMT_NONE = compiler.OutputFormat.NONE


# TODO: can we merge execute and execute_script?
async def execute(
    be_conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    bind_args: bytes,
    *,
    fe_conn: Optional[frontend.AbstractFrontendConnection] = None,
    use_prep_stmt: bint = False,
    # HACK: A hook from the notebook ext, telling us to skip dbview.start
    # so that it can handle things differently.
    skip_start: bint = False,
):
    cdef:
        bytes state = None, orig_state = None
        WriteBuffer bound_args_buf

    query_unit = compiled.query_unit_group[0]

    if not dbv.in_tx():
        orig_state = state = dbv.serialize_state()

    new_types = None
    server = dbv.server

    data = None

    try:
        if be_conn.last_state == state:
            # the current status in be_conn is in sync with dbview, skip the
            # state restoring
            state = None
        if not skip_start:
            dbv.start(query_unit)
        if query_unit.create_db_template:
            await server._on_before_create_db_from_template(
                query_unit.create_db_template,
                dbv.dbname,
            )
        if query_unit.drop_db:
            await server._on_before_drop_db(query_unit.drop_db, dbv.dbname)
        if query_unit.system_config:
            await execute_system_config(be_conn, dbv, query_unit)
        else:
            config_ops = query_unit.config_ops

            if query_unit.sql:
                if query_unit.ddl_stmt_id:
                    ddl_ret = await be_conn.run_ddl(query_unit, state)
                    if ddl_ret and ddl_ret['new_types']:
                        new_types = ddl_ret['new_types']
                else:
                    bound_args_buf = args_ser.recode_bind_args(
                        dbv, compiled, bind_args)

                    data = await be_conn.parse_execute(
                        query=query_unit,
                        fe_conn=fe_conn if not query_unit.set_global else None,
                        bind_data=bound_args_buf,
                        use_prep_stmt=use_prep_stmt,
                        state=state,
                        dbver=dbv.dbver,
                    )

                    if query_unit.set_global and data:
                        config_ops = [
                            config.Operation.from_json(r[0][1:])
                            for r in data
                        ]

                if state is not None:
                    # state is restored, clear orig_state so that we can
                    # set be_conn.last_state correctly later
                    orig_state = None

            if query_unit.tx_savepoint_rollback:
                dbv.rollback_tx_to_savepoint(query_unit.sp_name)

            if query_unit.tx_savepoint_declare:
                dbv.declare_savepoint(
                    query_unit.sp_name, query_unit.sp_id)

            if query_unit.create_db:
                await server.introspect_db(query_unit.create_db)

            if query_unit.drop_db:
                server._on_after_drop_db(query_unit.drop_db)

            if config_ops:
                await dbv.apply_config_ops(be_conn, config_ops)

    except Exception as ex:
        dbv.on_error()

        if query_unit.tx_commit and not be_conn.in_tx() and dbv.in_tx():
            # The COMMIT command has failed. Our Postgres connection
            # isn't in a transaction anymore. Abort the transaction
            # in dbview.
            dbv.abort_tx()
        raise
    else:
        side_effects = dbv.on_success(query_unit, new_types)
        if side_effects:
            signal_side_effects(dbv, side_effects)
        if not dbv.in_tx():
            state = dbv.serialize_state()
            if state is not orig_state:
                # In 3 cases the state is changed:
                #   1. The non-tx query changed the state
                #   2. The state is synced with dbview (orig_state is None)
                #   3. We came out from a transaction (orig_state is None)
                be_conn.last_state = state

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
        bint in_tx
        object user_schema, cached_reflection, global_schema
        WriteBuffer bind_data

    user_schema = cached_reflection = global_schema = None
    unit_group = compiled.query_unit_group
    if unit_group.tx_control:
        # TODO: move to the server.compiler once binary_v0 is dropped
        raise errors.QueryError(
            "Explicit transaction control commands cannot be executed in "
            "an implicit transaction block"
        )

    in_tx = dbv.in_tx()
    if not in_tx:
        orig_state = state = dbv.serialize_state()

    data = None

    try:
        async with conn.parse_execute_script_context():
            for idx, query_unit in enumerate(unit_group):
                if fe_conn is not None and fe_conn.cancelled:
                    raise ConnectionAbortedError

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
                    for n in range(idx, len(unit_group)):
                        ng = unit_group[n]
                        if ng.ddl_stmt_id or ng.set_global:
                            sent = n + 1
                            break
                    else:
                        sent = len(unit_group)

                    bind_array = args_ser.recode_bind_args_for_script(
                        dbv, compiled, bind_args, idx, sent)
                    conn.send_query_unit_group(
                        unit_group, bind_array, state, idx, sent)

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
                    cached_reflection = query_unit.cached_reflection

                if query_unit.global_schema:
                    global_schema = query_unit.global_schema

                if query_unit.sql:
                    if query_unit.ddl_stmt_id:
                        ddl_ret = await conn.handle_ddl_in_script(
                            query_unit
                        )
                        if ddl_ret and ddl_ret['new_types']:
                            new_types = ddl_ret['new_types']
                    elif query_unit.set_global:
                        globals_data = []
                        for sql in query_unit.sql:
                            globals_data = await conn.wait_for_command(
                                ignore_data=False
                            )
                        if globals_data:
                            config_ops = [
                                config.Operation.from_json(r[0][1:])
                                for r in globals_data
                            ]
                    elif query_unit.output_format == FMT_NONE:
                        for sql in query_unit.sql:
                            await conn.wait_for_command(
                                ignore_data=True
                            )
                    else:
                        for sql in query_unit.sql:
                            data = await conn.wait_for_command(
                                ignore_data=False,
                                fe_conn=fe_conn,
                            )

                if config_ops:
                    await dbv.apply_config_ops(conn, config_ops)

                side_effects = dbv.on_success(query_unit, new_types)
                if side_effects:
                    raise errors.InternalServerError(
                        "Side-effects in implicit transaction!"
                    )

    except Exception as e:
        dbv.on_error()

        if not in_tx and dbv.in_tx():
            # Abort the implicit transaction
            dbv.abort_tx()

        raise

    else:
        if not in_tx:
            side_effects = dbv.commit_implicit_tx(
                user_schema, global_schema, cached_reflection
            )
            if side_effects:
                signal_side_effects(dbv, side_effects)
            state = dbv.serialize_state()
            if state is not orig_state:
                conn.last_state = state

    finally:
        if sent and sent < len(unit_group):
            await conn.sync()

    return data


async def execute_system_config(
    conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    query_unit: compiler.QueryUnit,
):
    if query_unit.sql:
        if len(query_unit.sql) > 1:
            raise errors.InternalServerError(
                "unexpected multiple SQL statements in CONFIGURE INSTANCE "
                "compilation product"
            )
        data = await conn.sql_fetch_col(query_unit.sql[0])
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

    # If this is a backend configuration setting we also
    # need to make sure it has been loaded.
    if query_unit.backend_config:
        await conn.sql_execute(b'SELECT pg_reload_conf()')


def signal_side_effects(dbv, side_effects):
    server = dbv.server
    if not server._accept_new_tasks:
        return

    if side_effects & dbview.SideEffects.SchemaChanges:
        server.create_task(
            server._signal_sysevent(
                'schema-changes',
                dbname=dbv.dbname,
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.GlobalSchemaChanges:
        server.create_task(
            server._signal_sysevent(
                'global-schema-changes',
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.DatabaseConfigChanges:
        server.create_task(
            server._signal_sysevent(
                'database-config-changes',
                dbname=dbv.dbname,
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.DatabaseChanges:
        server.create_task(
            server._signal_sysevent(
                'database-changes',
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.InstanceConfigChanges:
        server.create_task(
            server._signal_sysevent(
                'system-config-changes',
            ),
            interruptable=False,
        )

    if side_effects & dbview.SideEffects.ExtensionChanges:
        server.create_task(
            server._signal_sysevent(
                'extension-changes',
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
) -> bytes:
    if query_cache_enabled is None:
        query_cache_enabled = not (
            debug.flags.disable_qcache or debug.flags.edgeql_compile)

    server = db.server
    dbv = await server.new_dbview(
        dbname=db.name,
        query_cache=query_cache_enabled,
        protocol_version=edbdef.CURRENT_PROTOCOL,
    )

    query_req = dbview.QueryRequestInfo(
        edgeql.Source.from_string(query),
        protocol_version=edbdef.CURRENT_PROTOCOL,
        input_format=compiler.InputFormat.JSON,
        output_format=output_format,
        allow_capabilities=compiler.Capability.MODIFICATIONS,
    )

    compiled = await dbv.parse(query_req)

    pgcon = await server.acquire_pgcon(db.name)
    try:
        return await execute_json(
            pgcon,
            dbv,
            compiled,
            variables=variables,
            globals_=globals_,
        )
    finally:
        server.release_pgcon(db.name, pgcon)


async def execute_json(
    be_conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    variables: Mapping[str, Any] = immutables.Map(),
    globals_: Optional[Mapping[str, Any]] = None,
    *,
    fe_conn: Optional[frontend.AbstractFrontendConnection] = None,
    use_prep_stmt: bint = False,
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

    if len(qug) > 1:
        data = await execute_script(
            be_conn,
            dbv,
            compiled,
            bind_args,
            fe_conn=fe_conn,
        )
    else:
        data = await execute(
            be_conn,
            dbv,
            compiled,
            bind_args,
            fe_conn=fe_conn,
        )

    if fe_conn is None:
        if not data or len(data) > 1 or len(data[0]) != 1:
            raise errors.InternalServerError(
                f'received incorrect response data for a JSON query')

        return data[0][0]
    else:
        return None


cdef bytes _encode_json_value(object val):
    if isinstance(val, decimal.Decimal):
        jarg = str(val)
    else:
        jarg = json.dumps(val)

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
