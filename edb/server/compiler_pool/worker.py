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

from edb.edgeql import parser as ql_parser

from edb.schema import schema as s_schema

from edb.server import compiler
from edb.server import config
from edb.server import pgcluster

from edb.common import debug
from edb.common import devmode
from edb.common import markup

from . import amsg
from . import state


INITED: bool = False
DBS: state.DatabasesState = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgcluster.BackendRuntimeParams = \
    pgcluster.get_default_runtime_params()
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
STD_SCHEMA: s_schema.FlatSchema
GLOBAL_SCHEMA: s_schema.FlatSchema
INSTANCE_CONFIG: immutables.Map[str, config.SettingValue]

# "created continuously" means the interval between two consecutive spawns
# is less than NUM_SPAWNS_RESET_INTERVAL seconds.
NUM_SPAWNS_RESET_INTERVAL = 1


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global DBS
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    (
        dbs,
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema,
        system_config,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    DBS = dbs
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    COMPILER = compiler.Compiler(
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
    )
    STD_SCHEMA = std_schema
    GLOBAL_SCHEMA = global_schema
    INSTANCE_CONFIG = system_config

    COMPILER.initialize(
        std_schema, refl_schema, schema_class_layout,
    )


def __sync__(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
) -> state.DatabaseState:
    global DBS
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    try:
        db = DBS.get(dbname)
        if db is None:
            assert user_schema is not None
            assert reflection_cache is not None
            assert database_config is not None
            user_schema_unpacked = pickle.loads(user_schema)
            reflection_cache_unpacked = pickle.loads(reflection_cache)
            database_config_unpacked = pickle.loads(database_config)
            db = state.DatabaseState(
                dbname,
                user_schema_unpacked,
                reflection_cache_unpacked,
                database_config_unpacked,
            )
            DBS = DBS.set(dbname, db)
        else:
            updates = {}

            if user_schema is not None:
                updates['user_schema'] = pickle.loads(user_schema)
            if reflection_cache is not None:
                updates['reflection_cache'] = pickle.loads(reflection_cache)
            if database_config is not None:
                updates['database_config'] = pickle.loads(database_config)

            if updates:
                db = db._replace(**updates)
                DBS = DBS.set(dbname, db)

        if global_schema is not None:
            GLOBAL_SCHEMA = pickle.loads(global_schema)

        if system_config is not None:
            INSTANCE_CONFIG = pickle.loads(system_config)

    except Exception as ex:
        raise state.FailedStateSync(
            f'failed to sync worker state: {type(ex).__name__}({ex})') from ex

    return db


def compile(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    units, cstate = COMPILER.compile(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    global LAST_STATE
    LAST_STATE = cstate
    pickled_state = None
    if cstate is not None:
        pickled_state = pickle.dumps(cstate, -1)

    return units, pickled_state


def compile_in_tx(cstate, *args, **kwargs):
    global LAST_STATE
    if cstate == state.REUSE_LAST_STATE_MARKER:
        cstate = LAST_STATE
    else:
        cstate = pickle.loads(cstate)
    units, cstate = COMPILER.compile_in_tx(cstate, *args, **kwargs)
    LAST_STATE = cstate
    return units, pickle.dumps(cstate, -1)


def compile_notebook(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_notebook(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def try_compile_rollback(
    eql: bytes
):
    return COMPILER.try_compile_rollback(eql)


def compile_graphql(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return graphql.compile_graphql(
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def worker(sockname):
    con = amsg.WorkerConnection(sockname)
    try:
        for req_id, req in con.iter_request():
            try:
                methname, args = pickle.loads(req)
                if methname == '__init_worker__':
                    meth = __init_worker__
                else:
                    if not INITED:
                        raise RuntimeError(
                            'call on uninitialized compiler worker')
                    if methname == 'compile':
                        meth = compile
                    elif methname == 'compile_in_tx':
                        meth = compile_in_tx
                    elif methname == 'compile_notebook':
                        meth = compile_notebook
                    elif methname == 'compile_graphql':
                        meth = compile_graphql
                    elif methname == 'try_compile_rollback':
                        meth = try_compile_rollback
                    else:
                        meth = getattr(COMPILER, methname)
            except Exception as ex:
                prepare_exception(ex)
                if debug.flags.server:
                    markup.dump(ex)
                data = (
                    1,
                    ex,
                    traceback.format_exc()
                )
            else:
                try:
                    res = meth(*args)
                    data = (0, res)
                except Exception as ex:
                    prepare_exception(ex)
                    if debug.flags.server:
                        markup.dump(ex)
                    data = (
                        1,
                        ex,
                        traceback.format_exc()
                    )

            try:
                pickled = pickle.dumps(data, -1)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f'{ex}:\n\n{ex_tb}'
                pickled = pickle.dumps((2, ex_str), -1)

            con.reply(req_id, pickled)
    finally:
        con.abort()


def run_worker(sockname):
    with devmode.CoverageConfig.enable_coverage_if_requested():
        worker(sockname)


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
    parser.add_argument('--sockname')
    parser.add_argument('--numproc')
    args = parser.parse_args()

    numproc = int(args.numproc)
    assert numproc > 1

    # Abort the template process if more than `max_worker_spawns`
    # new workers are created continuously - it probably means the
    # worker cannot start correctly.
    max_worker_spawns = numproc * 2

    ql_parser.preload()
    gc.freeze()

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
    run_worker(args.sockname)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
