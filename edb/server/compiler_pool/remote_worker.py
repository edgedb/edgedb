#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *  # NoQA

import argparse
import gc
import os
import pickle
import signal
import time
import traceback

import immutables

from edb import graphql

from edb.common import debug
from edb.common import devmode
from edb.common import markup

from edb.edgeql import parser as ql_parser

from edb.pgsql import params as pgparams

from edb.schema import schema as s_schema

from edb.server import compiler
from edb.server import config

from . import amsg
from . import state


INITED: bool = False
clients: immutables.Map[int, ClientSchema] = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgparams.BackendRuntimeParams = (
    pgparams.get_default_runtime_params()
)
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
STD_SCHEMA: s_schema.FlatSchema

# "created continuously" means the interval between two consecutive spawns
# is less than NUM_SPAWNS_RESET_INTERVAL seconds.
NUM_SPAWNS_RESET_INTERVAL = 1


class ClientSchema(NamedTuple):
    dbs: state.DatabasesState
    global_schema: s_schema.FlatSchema
    instance_config: immutables.Map[str, config.SettingValue]


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA

    (
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    COMPILER = compiler.Compiler(
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
    )
    STD_SCHEMA = std_schema

    COMPILER.initialize(
        std_schema,
        refl_schema,
        schema_class_layout,
    )


def __sync__(client_id, pickled_schema, invalidation):
    global clients

    for cid in invalidation:
        try:
            clients = clients.delete(cid)
        except KeyError:
            pass
    try:
        client_schema = clients.get(client_id)  # type: ClientSchema
        if pickled_schema:
            if client_schema is None:
                dbs = {
                    dbname: state.DatabaseState(
                        dbname,
                        pickle.loads(pickled_state.user_schema),
                        pickle.loads(pickled_state.reflection_cache),
                        pickle.loads(pickled_state.database_config),
                    )
                    for dbname, pickled_state in pickled_schema.dbs.items()
                }
                if debug.flags.server:
                    print(client_id, "FULL SYNC: ", list(dbs))
                client_schema = ClientSchema(
                    immutables.Map(dbs),
                    pickle.loads(pickled_schema.global_schema),
                    pickle.loads(pickled_schema.instance_config),
                )
                clients = clients.set(client_id, client_schema)
            else:
                updates = {}
                dbs = client_schema.dbs
                if pickled_schema.dbs is not None:
                    for dbname, pickled_state in pickled_schema.dbs.items():
                        db_state = dbs.get(dbname)
                        if db_state is None:
                            assert pickled_state.user_schema is not None
                            assert pickled_state.reflection_cache is not None
                            assert pickled_state.database_config is not None
                            db_state = state.DatabaseState(
                                dbname,
                                pickle.loads(pickled_state.user_schema),
                                pickle.loads(pickled_state.reflection_cache),
                                pickle.loads(pickled_state.database_config),
                            )
                            if debug.flags.server:
                                print(client_id, "DIFF SYNC ADD: ", dbname)
                            dbs = dbs.set(dbname, db_state)
                        else:
                            db_updates = {}
                            if pickled_state.user_schema is not None:
                                db_updates["user_schema"] = pickle.loads(
                                    pickled_state.user_schema
                                )
                            if pickled_state.reflection_cache is not None:
                                db_updates["reflection_cache"] = pickle.loads(
                                    pickled_state.reflection_cache
                                )
                            if pickled_state.database_config is not None:
                                db_updates["database_config"] = pickle.loads(
                                    pickled_state.database_config
                                )
                            if db_updates:
                                if debug.flags.server:
                                    print(
                                        client_id, "DIFF SYNC UPDATE: ", dbname
                                    )
                                dbs = dbs.set(
                                    dbname,
                                    dbs.get(dbname)._replace(**db_updates),
                                )
                if pickled_schema.dropped_dbs is not None:
                    for dbname in pickled_schema.dropped_dbs:
                        if debug.flags.server:
                            print(client_id, "DIFF SYNC DROP: ", dbname)
                        dbs = dbs.delete(dbname)
                if dbs is not client_schema.dbs:
                    updates["dbs"] = dbs
                if pickled_schema.global_schema is not None:
                    updates["global_schema"] = pickle.loads(
                        pickled_schema.global_schema
                    )
                if pickled_schema.instance_config is not None:
                    updates["instance_config"] = pickle.loads(
                        pickled_schema.instance_config
                    )
                if updates:
                    client_schema = client_schema._replace(**updates)
                    clients = clients.set(client_id, client_schema)
        else:
            assert client_schema is not None

    except Exception as ex:
        raise state.FailedStateSync(
            f"failed to sync worker state: {type(ex).__name__}({ex})"
        ) from ex


def compile(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]
    units, cstate = COMPILER.compile(
        db.user_schema,
        client_schema.global_schema,
        db.reflection_cache,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )

    pickled_state = None
    if cstate is not None:
        global LAST_STATE
        LAST_STATE = cstate
        pickled_state = pickle.dumps(cstate, -1)

    return units, pickled_state


def compile_in_tx(cstate, _, *args, **kwargs):
    global LAST_STATE
    if cstate == state.REUSE_LAST_STATE_MARKER:
        cstate = LAST_STATE
    else:
        cstate = pickle.loads(cstate)
    units, cstate = COMPILER.compile_in_tx(cstate, *args, **kwargs)
    LAST_STATE = cstate
    return units, pickle.dumps(cstate, -1)


def compile_notebook(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    global clients
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]

    return COMPILER.compile_notebook(
        db.user_schema,
        client_schema.global_schema,
        db.reflection_cache,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )


def try_compile_rollback(
    *compile_args: Any,
    **compile_kwargs: Any,
):
    return COMPILER.try_compile_rollback(*compile_args, **compile_kwargs)


def compile_graphql(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    global clients
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]

    return graphql.compile_graphql(
        STD_SCHEMA,
        db.user_schema,
        client_schema.global_schema,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )


def call_for_client(client_id, pickled_schema, invalidation, msg, *args):
    __sync__(client_id, pickled_schema, invalidation)
    if msg is None:
        methname = args[0]
        dbname = args[1]
        args = args[2:]
    else:
        assert args == ()
        methname, args = pickle.loads(msg)
        dbname = args[0]
        args = args[6:]

    if methname == "compile":
        meth = compile
    elif methname == "compile_notebook":
        meth = compile_notebook
    elif methname == "compile_graphql":
        meth = compile_graphql
    else:
        meth = getattr(COMPILER, methname)
    return meth(client_id, dbname, *args)


def worker(sockname, version_serial):
    con = amsg.WorkerConnection(sockname, version_serial)
    try:
        for req_id, req in con.iter_request():
            try:
                methname, args = pickle.loads(req)
                if methname == "__init_worker__":
                    meth = __init_worker__
                else:
                    if not INITED:
                        raise RuntimeError(
                            "call on uninitialized compiler worker"
                        )
                    if methname == "call_for_client":
                        meth = call_for_client
                    elif methname == "compile_in_tx":
                        meth = compile_in_tx
                    elif methname == "try_compile_rollback":
                        meth = try_compile_rollback
                    else:
                        meth = getattr(COMPILER, methname)
            except Exception as ex:
                prepare_exception(ex)
                if debug.flags.server:
                    markup.dump(ex)
                data = (1, ex, traceback.format_exc())
            else:
                try:
                    res = meth(*args)
                    data = (0, res)
                except Exception as ex:
                    prepare_exception(ex)
                    if debug.flags.server:
                        markup.dump(ex)
                    data = (1, ex, traceback.format_exc())

            try:
                pickled = pickle.dumps(data, -1)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f"{ex}:\n\n{ex_tb}"
                pickled = pickle.dumps((2, ex_str), -1)

            con.reply(req_id, pickled)
    finally:
        con.abort()


def run_worker(sockname, version_serial):
    with devmode.CoverageConfig.enable_coverage_if_requested():
        worker(sockname, version_serial)


def prepare_exception(ex):
    clear_exception_frames(ex)
    if ex.__traceback__ is not None:
        ex.__traceback__ = ex.__traceback__.tb_next


def clear_exception_frames(er):
    def _clear_exception_frames(er, visited):
        if er in visited:
            return er
        visited.add(er)

        traceback.clear_frames(er.__traceback__)

        if er.__cause__ is not None:
            er.__cause__ = _clear_exception_frames(er.__cause__, visited)
        if er.__context__ is not None:
            er.__context__ = _clear_exception_frames(er.__context__, visited)

        return er

    visited = set()
    _clear_exception_frames(er, visited)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sockname")
    parser.add_argument("--numproc")
    parser.add_argument("--version-serial", type=int)
    args = parser.parse_args()

    ql_parser.preload(allow_rebuild=False)
    gc.freeze()

    if args.numproc is None:
        # Run a single worker process
        run_worker(args.sockname, args.version_serial)
        return

    numproc = int(args.numproc)
    assert numproc >= 1

    # Abort the template process if more than `max_worker_spawns`
    # new workers are created continuously - it probably means the
    # worker cannot start correctly.
    max_worker_spawns = numproc * 2

    children = set()
    continuous_num_spawns = 0

    for _ in range(int(args.numproc)):
        # spawn initial workers
        if pid := os.fork():
            # main process
            children.add(pid)
            continuous_num_spawns += 1
        else:
            # child process
            break
    else:
        # main process - redirect SIGTERM to SystemExit and wait for children
        signal.signal(signal.SIGTERM, lambda *_: exit(os.EX_OK))
        last_spawn_timestamp = time.monotonic()

        try:
            while children:
                pid, status = os.wait()
                children.remove(pid)
                ec = os.waitstatus_to_exitcode(status)
                if ec > 0 or -ec not in {0, signal.SIGINT}:
                    # restart the child process if killed or ending abnormally,
                    # unless we tried too many times continuously
                    now = time.monotonic()
                    if now - last_spawn_timestamp > NUM_SPAWNS_RESET_INTERVAL:
                        continuous_num_spawns = 0
                    last_spawn_timestamp = now
                    continuous_num_spawns += 1
                    if continuous_num_spawns > max_worker_spawns:
                        # GOTCHA: we shouldn't return here because we need the
                        # exception handler below to clean up the workers
                        exit(os.EX_UNAVAILABLE)

                    if pid := os.fork():
                        # main process
                        children.add(pid)
                    else:
                        # child process
                        break
            else:
                # main process - all children ended normally
                return
        except BaseException as e:  # includes SystemExit and KeyboardInterrupt
            # main process - kill and wait for the remaining workers to exit
            try:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                for pid in children:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError:
                        pass
                try:
                    while children:
                        pid, status = os.wait()
                        children.discard(pid)
                except OSError:
                    pass
            finally:
                raise e

    # child process - clear the SIGTERM handler for potential Rust impl
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    run_worker(args.sockname, args.version_serial)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
