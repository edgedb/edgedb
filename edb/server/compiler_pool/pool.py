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

import asyncio
import logging
import os
import os.path
import pickle
import subprocess
import sys
import time

import immutables

from edb.server import pgcluster

from edb.common import debug
from edb.common import supervisor
from edb.common import taskgroup

from . import amsg
from . import state


DEFAULT_POOL_SIZE = max(os.cpu_count(), 2)
PROCESS_INITIAL_RESPONSE_TIMEOUT = 60.0
KILL_TIMEOUT = 10.0
WORKER_MOD = __name__.rpartition('.')[0] + '.worker'


logger = logging.getLogger("edb.server")
log_metrics = logging.getLogger("edb.server.metrics")


# Inherit sys.path so that import system can find worker class
# in unittests.
_ENV = os.environ.copy()
_ENV['PYTHONPATH'] = ':'.join(sys.path)


class Worker:

    _dbs: state.DatabasesState

    def __init__(
        self,
        manager,
        dbs: state.DatabasesState,
        backend_runtime_params: pgcluster.BackendRuntimeParams,
        std_schema,
        refl_schema,
        schema_class_layout,
        server,
        command_args
    ):
        self._dbs = dbs

        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._global_schema = None

        self._manager = manager
        self._server = server
        self._command_args = command_args
        self._proc = None
        self._con = None
        self._last_used = time.monotonic()
        self._closed = False
        self._sup = None
        self._last_state = None

    def _update_db(
        self,
        dbname,
        new_dbver,
        new_user_schema,
        new_reflection_cache
    ):
        self._dbs = self._dbs.set(dbname, state.DatabaseState(
            name=dbname,
            dbver=new_dbver,
            user_schema=new_user_schema,
            reflection_cache=new_reflection_cache
        ))

    async def _kill_proc(self, proc):
        try:
            proc.kill()
        except ProcessLookupError:
            return

        try:
            await asyncio.wait_for(proc.wait(), KILL_TIMEOUT)
        except Exception:
            proc.terminate()
            raise

    async def _spawn(self):
        self._manager._stats_spawned += 1

        if self._proc is not None:
            self._manager._sup.create_task(self._kill_proc(self._proc))
            self._proc = None

        env = _ENV
        if debug.flags.server:
            env = {'EDGEDB_DEBUG_SERVER': '1', **_ENV}

        self._proc = await asyncio.create_subprocess_exec(
            *self._command_args,
            env=env,
            stdin=subprocess.DEVNULL)
        try:
            self._con = await asyncio.wait_for(
                self._server.get_by_pid(self._proc.pid),
                PROCESS_INITIAL_RESPONSE_TIMEOUT)
        except Exception:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass
            raise

        await self.call(
            '__init_worker__',
            self._dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
        )

    def get_pid(self):
        return self._proc.pid

    async def call(self, method_name, *args):
        assert not self._closed

        if self._con.is_closed():
            await self._spawn()

        msg = pickle.dumps((method_name, args))
        data = await self._con.request(msg)
        status, *data = pickle.loads(data)

        self._last_used = time.monotonic()

        if status == 0:
            return data[0]
        elif status == 1:
            exc, tb = data
            exc.__formatted_error__ = tb
            raise exc
        else:
            exc = RuntimeError(
                'could not serialize result in worker subprocess')
            exc.__formatted_error__ = data[0]
            raise exc

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self._manager._stats_killed += 1
        self._manager._workers.discard(self)
        self._manager._report_worker(self, action="kill")
        try:
            self._proc.terminate()
            await self._proc.wait()
        except ProcessLookupError:
            pass


class Pool:

    _workers_queue: asyncio.Queue

    def __init__(
        self,
        *,
        loop,
        runstate_dir,
        dbindex,
        backend_runtime_params: pgcluster.BackendRuntimeParams,
        std_schema,
        refl_schema,
        schema_class_layout,
        pool_size=DEFAULT_POOL_SIZE,
    ):

        self._loop = loop
        self._dbindex = dbindex

        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout

        self._runstate_dir = runstate_dir

        self._poolsock_name = os.path.join(
            self._runstate_dir, 'compilers.socket')

        assert pool_size >= 1
        self._pool_size = pool_size
        self._workers = set()

        self._server = amsg.Server(self._poolsock_name, loop)

        self._running = False

        self._stats_spawned = 0
        self._stats_killed = 0

        self._sup = None

        self._worker_command_args = [
            sys.executable, '-m', WORKER_MOD,
            '--sockname', self._poolsock_name
        ]

    def is_running(self):
        return self._running

    async def _spawn_worker(self, dbs: state.DatabasesState):
        worker = Worker(
            self,
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            self._server,
            self._worker_command_args
        )
        await worker._spawn()
        self._report_worker(worker)

        self._workers.add(worker)
        self._workers_queue.put_nowait(worker)
        return worker

    async def start(self):
        self._workers_queue = asyncio.Queue()
        self._sup = await supervisor.Supervisor.create()

        await self._server.start()
        self._running = True

        dbs: state.DatabasesState = immutables.Map()
        for db in self._dbindex.iter_dbs():
            dbs = dbs.set(
                db.name,
                state.DatabaseState(
                    name=db.name,
                    dbver=db.dbver,
                    user_schema=db.user_schema,
                    reflection_cache=db.reflection_cache,
                )
            )

        async with taskgroup.TaskGroup(name='compiler-pool-start') as g:
            for _ in range(self._pool_size):
                g.create_task(self._spawn_worker(dbs))

    async def stop(self):
        if not self._running:
            return

        await self._sup.wait()

        await self._server.stop()
        self._server = None

        workers_to_kill = list(self._workers)
        self._workers_queue = asyncio.Queue()
        self._workers.clear()
        self._running = False

        async with taskgroup.TaskGroup(name='compiler-pool-stop') as g:
            for worker in workers_to_kill:
                g.create_task(worker.close())

    def _report_worker(self, worker: Worker, *, action: str = "spawn"):
        action = action.capitalize()
        if not action.endswith("e"):
            action += "e"
        action += "d"
        log_metrics.info(
            "%s a compiler worker with PID %d; pool=%d;"
            + " spawned=%d; killed=%d",
            action,
            worker.get_pid(),
            len(self._workers),
            self._stats_spawned,
            self._stats_killed,
        )

    async def compile(
        self,
        dbname,
        dbver,
        user_schema,
        global_schema,
        reflection_cache,
        *compile_args
    ):
        worker = await self._workers_queue.get()
        try:
            worker_db = worker._dbs.get(dbname)

            if worker_db is None or worker_db.dbver != dbver:
                preargs = (
                    dbname, dbver,
                    user_schema, reflection_cache
                )
                worker._update_db(*preargs)
            else:
                preargs = (
                    dbname, dbver,
                    None, None
                )
            if worker._global_schema is not global_schema:
                preargs += (global_schema,)
            else:
                preargs += (None,)

            units, state = await worker.call(
                'compile',
                *preargs,
                *compile_args
            )
            worker._last_state = state
            return units, state

        finally:
            self._workers_queue.put_nowait(worker)

    async def compile_in_tx(self, txid, state, *compile_args):
        for candidate in tuple(self._workers_queue._queue):
            if candidate._last_state is state:
                self._workers_queue._queue.remove(candidate)
                worker = candidate
                state = 'LAST'
                break
        else:
            worker = await self._workers_queue.get()

        try:
            units, new_state = await worker.call(
                'compile_in_tx',
                state,
                txid,
                *compile_args
            )
            worker._last_state = new_state
            return units, new_state

        finally:
            self._workers_queue.put_nowait(worker)

    async def compile_notebook(
        self,
        dbname,
        dbver,
        user_schema,
        global_schema,
        reflection_cache,
        *compile_args
    ):
        worker = await self._workers_queue.get()
        try:
            worker_db = worker._dbs.get(dbname)

            if worker_db is None or worker_db.dbver != dbver:
                preargs = (
                    dbname, dbver,
                    user_schema, reflection_cache
                )
                worker._update_db(*preargs)
            else:
                preargs = (
                    dbname, dbver,
                    None, None
                )
            if worker._global_schema is not global_schema:
                preargs += (global_schema,)
            else:
                preargs += (None,)

            return await worker.call(
                'compile_notebook',
                *preargs,
                *compile_args
            )

        finally:
            self._workers_queue.put_nowait(worker)

    async def try_compile_rollback(self, dbver: bytes, eql: bytes):
        worker = await self._workers_queue.get()
        try:
            return await worker.call(
                'try_compile_rollback',
                dbver,
                eql
            )
        finally:
            self._workers_queue.put_nowait(worker)

    async def compile_graphql(
        self,
        dbname,
        dbver,
        user_schema,
        global_schema,
        reflection_cache,
        *compile_args
    ):
        worker = await self._workers_queue.get()
        try:
            worker_db = worker._dbs.get(dbname)

            if worker_db is None or worker_db.dbver != dbver:
                preargs = (
                    dbname, dbver,
                    user_schema, reflection_cache
                )
                worker._update_db(*preargs)
            else:
                preargs = (
                    dbname, dbver,
                    None, None
                )
            if worker._global_schema is not global_schema:
                preargs += (global_schema,)
            else:
                preargs += (None,)

            return await worker.call(
                'compile_graphql',
                *preargs,
                *compile_args
            )

        finally:
            self._workers_queue.put_nowait(worker)

    async def describe_database_dump(
        self,
        *args,
        **kwargs
    ):
        worker = await self._workers_queue.get()
        try:
            return await worker.call(
                'describe_database_dump',
                *args,
                **kwargs
            )

        finally:
            self._workers_queue.put_nowait(worker)

    async def describe_database_restore(
        self,
        *args,
        **kwargs
    ):
        worker = await self._workers_queue.get()
        try:
            return await worker.call(
                'describe_database_restore',
                *args,
                **kwargs
            )

        finally:
            self._workers_queue.put_nowait(worker)


async def create_compiler_pool(
    *,
    runstate_dir: str,
    dbindex,
    backend_runtime_params: pgcluster.BackendRuntimeParams,
    std_schema,
    refl_schema,
    schema_class_layout,

) -> Pool:
    loop = asyncio.get_running_loop()
    pool = Pool(
        loop=loop,
        runstate_dir=runstate_dir,
        backend_runtime_params=backend_runtime_params,
        std_schema=std_schema,
        refl_schema=refl_schema,
        schema_class_layout=schema_class_layout,
        dbindex=dbindex,
    )

    await pool.start()
    return pool
