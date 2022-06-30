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
    Optional,
)

from edb import errors

from edb.server import compiler
from edb.server import config
from edb.server.dbview cimport dbview
from edb.server.protocol cimport args_ser
from edb.server.protocol cimport frontend
from edb.server.pgproto.pgproto cimport WriteBuffer
from edb.server.pgcon cimport pgcon


cdef object FMT_NONE = compiler.OutputFormat.NONE


async def execute(
    be_conn: pgcon.PGConnection,
    dbv: dbview.DatabaseConnectionView,
    compiled: dbview.CompiledQuery,
    bind_args: bytes,
    *,
    fe_conn: Optional[frontend.FrontendConnection] = None,
    use_prep_stmt: bint = False,
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
        dbv.start(query_unit)
        if query_unit.create_db_template:
            await server._on_before_create_db_from_template(
                query_unit.create_db_template, dbv.dbname
            )
        if query_unit.drop_db:
            await server._on_before_drop_db(
                query_unit.drop_db, dbv.dbname)
        if query_unit.system_config:
            await execute_system_config(be_conn, dbv, query_unit)
        else:
            if query_unit.sql:
                if query_unit.ddl_stmt_id:
                    ddl_ret = await be_conn.run_ddl(query_unit, state)
                    if ddl_ret and ddl_ret['new_types']:
                        new_types = ddl_ret['new_types']
                else:
                    bound_args_buf = args_ser.recode_bind_args(
                        dbv, compiled, bind_args)

                    data = await be_conn.parse_execute(
                        query_unit,         # =query
                        fe_conn if not query_unit.set_global else None,
                        bound_args_buf,     # =bind_data
                        use_prep_stmt,      # =use_prep_stmt
                        state,              # =state
                        dbv.dbver,          # =dbver
                        not query_unit.set_global,  # =return_data
                    )
                if state is not None:
                    # state is restored, clear orig_state so that we can
                    # set be_conn.last_state correctly later
                    orig_state = None

            config_ops = query_unit.config_ops
            if query_unit.set_global:
                new_config_ops = await finish_set_global(
                    be_conn, query_unit, state)
                if new_config_ops:
                    config_ops = new_config_ops

            if query_unit.tx_savepoint_rollback:
                dbv.rollback_tx_to_savepoint(query_unit.sp_name)

            if query_unit.tx_savepoint_declare:
                dbv.declare_savepoint(
                    query_unit.sp_name, query_unit.sp_id)

            if query_unit.create_db:
                await server.introspect_db(
                    query_unit.create_db
                )

            if query_unit.drop_db:
                server._on_after_drop_db(
                    query_unit.drop_db)

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
    fe_conn: Optional[frontend.FrontendConnection],
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
    query_unit,
):
    if query_unit.sql:
        data = await conn.simple_query(
            b';'.join(query_unit.sql), ignore_data=False)
    else:
        data = None

    if data:
        # Prefer encoded op produced by the SQL command.
        config_ops = [config.Operation.from_json(r[0]) for r in data]
    else:
        # Otherwise, fall back to staticly evaluated op.
        config_ops = query_unit.config_ops
    await dbv.apply_config_ops(conn, config_ops)

    # If this is a backend configuration setting we also
    # need to make sure it has been loaded.
    if query_unit.backend_config:
        await conn.simple_query(
            b'SELECT pg_reload_conf()', ignore_data=True)


async def finish_set_global(conn, query_unit, state):
    config_ops = None
    try:
        try:
            if state is not None:
                await conn.wait_for_state_resp(
                    state, bool(query_unit.tx_id))
            for sql in query_unit.sql:
                data = await conn.wait_for_command(
                    ignore_data=False
                )
            if data:
                config_ops = [
                    config.Operation.from_json(r[0][1:])
                    for r in data
                ]
        finally:
            await conn.wait_for_sync()
    finally:
        await conn.after_command()
    return config_ops


def signal_side_effects(dbv, side_effects):
    server = dbv.server
    if not server._accept_new_tasks:
        return

    if side_effects & dbview.SideEffects.GlobalSchemaChanges:
        # TODO(fantix): extensions may provide their own session config, so
        # we should push state desc too if that happens.
        server._push_state_desc(dbv.dbname)

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

    if side_effects & dbview.SideEffects.InstanceConfigChanges:
        server.create_task(
            server._signal_sysevent(
                'system-config-changes',
            ),
            interruptable=False,
        )
