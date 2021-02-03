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
import asyncio
import os
import pickle
import signal
import traceback

import immutables
import uvloop

from edb import graphql

from edb.schema import schema as s_schema

from edb.server import compiler
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
LAST_STATE = None
STD_SCHEMA: s_schema.FlatSchema
GLOBAL_SCHEMA: s_schema.FlatSchema


async def __init_worker__(
    dbs: state.DatabasesState,
    backend_runtime_params: pgcluster.BackendRuntimeParams,
    std_schema,
    refl_schema,
    schema_class_layout,
) -> None:
    global INITED
    global DBS
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA

    INITED = True
    DBS = dbs
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    COMPILER = compiler.Compiler(
        {},  # XXX
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
    )
    STD_SCHEMA = std_schema

    COMPILER.ensure_initialized2(
        std_schema, refl_schema, schema_class_layout,
    )


def __sync__(
    dbname: str,
    dbver: bytes,
    user_schema: Optional[s_schema.FlatSchema],
    reflection_cache: Optional[state.ReflectionCache],
    global_schema: Optional[s_schema.FlatSchema],
) -> state.DatabaseState:
    global DBS

    db = DBS.get(dbname)
    if db is None or db.dbver != dbver:
        if user_schema is None:
            raise RuntimeError(
                'dbver has changed but the user_schema was not sent')
        db = state.DatabaseState(
            dbname, dbver, user_schema, reflection_cache  # type: ignore
        )
        DBS = DBS.set(dbname, db)

    global GLOBAL_SCHEMA
    if global_schema is not None:
        GLOBAL_SCHEMA = global_schema

    return db


async def compile(
    dbname: str,
    dbver: bytes,
    user_schema: Optional[s_schema.FlatSchema],
    reflection_cache: Optional[state.ReflectionCache],
    global_schema: Optional[s_schema.FlatSchema],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(dbname, dbver, user_schema, reflection_cache, global_schema)

    units, cstate = await COMPILER.compile(
        db.dbver,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        *compile_args,
        **compile_kwargs
    )
    global LAST_STATE
    LAST_STATE = cstate
    return units, pickle.dumps(cstate)


async def compile_in_tx(state, *args, **kwargs):
    global LAST_STATE
    if state == 'LAST':
        state = LAST_STATE
    else:
        state = pickle.loads(state)
    units, state = await COMPILER.compile_in_tx(state, *args, **kwargs)
    LAST_STATE = state
    return units, pickle.dumps(state)


async def compile_notebook(
    dbname: str,
    dbver: bytes,
    user_schema: Optional[s_schema.FlatSchema],
    reflection_cache: Optional[state.ReflectionCache],
    global_schema: Optional[s_schema.FlatSchema],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(dbname, dbver, user_schema, reflection_cache, global_schema)

    return await COMPILER.compile_notebook(
        db.dbver,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        *compile_args,
        **compile_kwargs
    )


async def try_compile_rollback(
    dbver: bytes,
    eql: bytes
):
    return COMPILER.try_compile_rollback(dbver, eql)


async def compile_graphql(
    dbname: str,
    dbver: bytes,
    user_schema: Optional[s_schema.FlatSchema],
    reflection_cache: Optional[state.ReflectionCache],
    global_schema: Optional[s_schema.FlatSchema],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(dbname, dbver, user_schema, reflection_cache, global_schema)

    return graphql.compile_graphql(
        dbver,
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        *compile_args,
        **compile_kwargs
    )


async def worker(sockname):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, on_terminate_worker)

    con = await amsg.worker_connect(sockname)
    try:
        while True:
            try:
                req = await con.next_request()
            except amsg.PoolClosedError:
                os._exit(0)

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
                    res = await meth(*args)
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
                pickled = pickle.dumps(data)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f'{ex}:\n\n{ex_tb}'
                pickled = pickle.dumps((2, ex_str))

            await con.reply(pickled)
    finally:
        con.abort()


def on_terminate_worker():
    # sys.exit() might not do it, apparently.
    os._exit(-1)


def run_worker(sockname):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    with devmode.CoverageConfig.enable_coverage_if_requested():
        asyncio.run(worker(sockname))


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
    args = parser.parse_args()

    try:
        run_worker(args.sockname)
    except amsg.PoolClosedError:
        exit(0)


if __name__ == '__main__':
    main()
