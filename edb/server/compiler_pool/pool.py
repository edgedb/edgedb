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
import functools
import logging
import os
import os.path
import pickle
import signal
import subprocess
import sys
import time

import immutables

from edb.server import pgcluster

from edb.common import debug
from edb.common import taskgroup

from . import amsg
from . import queue
from . import state


PROCESS_INITIAL_RESPONSE_TIMEOUT: float = 60.0
KILL_TIMEOUT: float = 10.0
WORKER_MOD: str = __name__.rpartition('.')[0] + '.worker'


logger = logging.getLogger("edb.server")
log_metrics = logging.getLogger("edb.server.metrics")


# Inherit sys.path so that import system can find worker class
# in unittests.
_ENV = os.environ.copy()
_ENV['PYTHONPATH'] = ':'.join(sys.path)


@functools.lru_cache()
def _pickle_memoized(schema):
    return pickle.dumps(schema, -1)


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
        global_schema,
        system_config,
        server,
        pid
    ):
        self._dbs = dbs
        self._pid = pid

        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._global_schema = global_schema
        self._system_config = system_config
        self._last_pickled_state = None

        self._manager = manager
        self._server = server
        self._con = None
        self._last_used = time.monotonic()
        self._closed = False

    async def _attach(self, init_args_pickled: bytes):
        self._manager._stats_spawned += 1

        self._con = await self._server.get_by_pid(self._pid)

        await self.call(
            '__init_worker__',
            init_args_pickled,
        )

    def get_pid(self):
        return self._pid

    async def call(self, method_name, *args, sync_state=None):
        assert not self._closed

        if self._con.is_closed():
            raise RuntimeError(
                'the connection to the compiler worker process is '
                'unexpectedly closed')

        msg = pickle.dumps((method_name, args))
        data = await self._con.request(msg)
        status, *data = pickle.loads(data)

        self._last_used = time.monotonic()

        if status == 0:
            if sync_state is not None:
                sync_state()
            return data[0]
        elif status == 1:
            exc, tb = data
            if (sync_state is not None and
                    not isinstance(exc, state.FailedStateSync)):
                sync_state()
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
            os.kill(self._pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


class Pool:

    _workers_queue: queue.WorkerQueue[Worker]
    _workers: Set[Worker]

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
        pool_size,
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

        self._server = amsg.Server(
            self._poolsock_name, self._pool_size, loop)

        self._running = None

        self._stats_spawned = 0
        self._stats_killed = 0

    def is_running(self):
        return bool(self._running)

    async def _attach_worker(self, pid: int, init_args, init_args_pickled):
        worker = Worker(  # type: ignore
            self,
            *init_args,
            self._server,
            pid,
        )
        await worker._attach(init_args_pickled)
        self._report_worker(worker)

        self._workers.add(worker)
        self._workers_queue.release(worker)
        return worker

    async def start(self):
        if self._running is not None:
            raise RuntimeError(
                'the compiler pool has already been started once')

        self._workers_queue = queue.WorkerQueue(self._loop)

        await self._server.start()
        self._running = True

        dbs: state.DatabasesState = immutables.Map()
        for db in self._dbindex.iter_dbs():
            dbs = dbs.set(
                db.name,
                state.DatabaseState(
                    name=db.name,
                    user_schema=db.user_schema,
                    reflection_cache=db.reflection_cache,
                    database_config=db.db_config,
                )
            )

        init_args = (
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            self._dbindex.get_global_schema(),
            self._dbindex.get_compilation_system_config(),
        )
        # Pickle once to later send to multiple worker processes.
        init_args_pickled = pickle.dumps(init_args, -1)

        env = _ENV
        if debug.flags.server:
            env = {'EDGEDB_DEBUG_SERVER': '1', **_ENV}
        self._first_proc = await asyncio.create_subprocess_exec(
            *[
                sys.executable, '-m', WORKER_MOD,
                '--sockname', self._poolsock_name,
                '--numproc', str(self._pool_size),
            ],
            env=env,
            stdin=subprocess.DEVNULL,
        )

        await asyncio.wait_for(
            self._server.wait_until_ready(),
            PROCESS_INITIAL_RESPONSE_TIMEOUT
        )

        async with taskgroup.TaskGroup(name='compiler-pool-start') as g:
            for pid in self._server.iter_pids():
                g.create_task(
                    self._attach_worker(pid, init_args, init_args_pickled)
                )

    async def stop(self):
        if not self._running:
            return

        await self._server.stop()
        self._server = None

        self._workers_queue = queue.WorkerQueue(self._loop)
        self._workers.clear()
        self._running = False

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

    def _compute_compile_preargs(
        self,
        worker,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
    ):

        def sync_worker_state_cb(
            *,
            worker,
            dbname,
            user_schema=None,
            global_schema=None,
            reflection_cache=None,
            database_config=None,
            system_config=None,
        ):
            worker_db = worker._dbs.get(dbname)
            if worker_db is None:
                assert user_schema is not None
                assert reflection_cache is not None
                assert global_schema is not None
                assert database_config is not None
                assert system_config is not None

                worker._dbs = worker._dbs.set(dbname, state.DatabaseState(
                    name=dbname,
                    user_schema=user_schema,
                    reflection_cache=reflection_cache,
                    database_config=database_config,
                ))
                worker._global_schema = global_schema
                worker._system_config = system_config
            else:
                if (
                    user_schema is not None
                    or reflection_cache is not None
                    or database_config is not None
                ):
                    worker._dbs = worker._dbs.set(dbname, state.DatabaseState(
                        name=dbname,
                        user_schema=(
                            user_schema or worker_db.user_schema),
                        reflection_cache=(
                            reflection_cache or worker_db.reflection_cache),
                        database_config=(
                            database_config or worker_db.database_config),
                    ))

                if global_schema is not None:
                    worker._global_schema = global_schema
                if system_config is not None:
                    worker._system_config = system_config

        worker_db = worker._dbs.get(dbname)
        preargs = (dbname,)
        to_update = {}

        if worker_db is None:
            preargs += (
                _pickle_memoized(user_schema),
                _pickle_memoized(reflection_cache),
                _pickle_memoized(global_schema),
                _pickle_memoized(database_config),
                _pickle_memoized(system_config),
            )
            to_update = {
                'user_schema': user_schema,
                'reflection_cache': reflection_cache,
                'global_schema': global_schema,
                'database_config': database_config,
                'system_config': system_config,
            }
        else:
            if worker_db.user_schema is not user_schema:
                preargs += (
                    _pickle_memoized(user_schema),
                )
                to_update['user_schema'] = user_schema
            else:
                preargs += (None,)

            if worker_db.reflection_cache is not reflection_cache:
                preargs += (
                    _pickle_memoized(reflection_cache),
                )
                to_update['reflection_cache'] = reflection_cache
            else:
                preargs += (None,)

            if worker._global_schema is not global_schema:
                preargs += (
                    _pickle_memoized(global_schema),
                )
                to_update['global_schema'] = global_schema
            else:
                preargs += (None,)

            if worker_db.database_config is not database_config:
                preargs += (
                    _pickle_memoized(database_config),
                )
                to_update['database_config'] = database_config
            else:
                preargs += (None,)

            if worker._system_config is not system_config:
                preargs += (
                    _pickle_memoized(system_config),
                )
                to_update['system_config'] = system_config
            else:
                preargs += (None,)

        if to_update:
            callback = functools.partial(
                sync_worker_state_cb,
                worker=worker,
                dbname=dbname,
                **to_update
            )
        else:
            callback = None

        return preargs, callback

    async def compile(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._workers_queue.acquire()
        try:
            preargs, sync_state = self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            units, state = await worker.call(
                'compile',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )
            worker._last_pickled_state = state
            return units, state

        finally:
            self._workers_queue.release(worker)

    async def compile_in_tx(self, txid, pickled_state, *compile_args):
        # When we compile a query, the compiler returns a tuple:
        # a QueryUnit and the state the compiler is in if it's in a
        # transaction.  The state contains the information about all savepoints
        # and transient schema changes, so the next time we need to
        # compile a new query in this transaction the state is needed
        # to be passed to the next compiler compiling it.
        #
        # The compile state can be quite heavy and contain multiple versions
        # of schema, configs, and other session-related data. So the compiler
        # worker pickles it before sending it to the IO process, and the
        # IO process doesn't need to ever unpickle it.
        #
        # There's one crucial optimization we do here though. We try to
        # find the compiler process that we used before, that already has
        # this state unpickled. If we can find it, it means that the
        # compiler process won't have to waste time unpickling the state.
        #
        # We use "is" in `w._last_pickled_state is pickled_state` deliberately,
        # because `pickled_state` is saved on the Worker instance and
        # stored in edgecon; we never modify it, so `is` is sufficient and
        # is faster than `==`.
        worker = await self._workers_queue.acquire(
            condition=lambda w: (w._last_pickled_state is pickled_state)
        )

        if worker._last_pickled_state is pickled_state:
            # Since we know that this particular worker already has the
            # state, we don't want to waste resources transferring the
            # state over the network. So we replace the state with a marker,
            # that the compiler process will recognize.
            pickled_state = state.REUSE_LAST_STATE_MARKER

        try:
            units, new_pickled_state = await worker.call(
                'compile_in_tx',
                pickled_state,
                txid,
                *compile_args
            )
            worker._last_pickled_state = new_pickled_state
            return units, new_pickled_state

        finally:
            # Put the worker at the end of the queue so that the chance
            # of reusing it later (and maximising the chance of
            # the w._last_pickled_state is pickled_state` check returning
            # `True` is higher.
            self._workers_queue.release(worker, put_in_front=False)

    async def compile_notebook(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._workers_queue.acquire()
        try:
            preargs, sync_state = self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                'compile_notebook',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._workers_queue.release(worker)

    async def try_compile_rollback(self, eql: bytes):
        worker = await self._workers_queue.acquire()
        try:
            return await worker.call(
                'try_compile_rollback',
                eql
            )
        finally:
            self._workers_queue.release(worker)

    async def compile_graphql(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._workers_queue.acquire()
        try:
            preargs, sync_state = self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                'compile_graphql',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._workers_queue.release(worker)

    async def describe_database_dump(
        self,
        *args,
        **kwargs
    ):
        worker = await self._workers_queue.acquire()
        try:
            return await worker.call(
                'describe_database_dump',
                *args,
                **kwargs
            )

        finally:
            self._workers_queue.release(worker)

    async def describe_database_restore(
        self,
        *args,
        **kwargs
    ):
        worker = await self._workers_queue.acquire()
        try:
            return await worker.call(
                'describe_database_restore',
                *args,
                **kwargs
            )

        finally:
            self._workers_queue.release(worker)


async def create_compiler_pool(
    *,
    runstate_dir: str,
    pool_size: int,
    dbindex,
    backend_runtime_params: pgcluster.BackendRuntimeParams,
    std_schema,
    refl_schema,
    schema_class_layout,

) -> Pool:
    loop = asyncio.get_running_loop()
    pool = Pool(
        loop=loop,
        pool_size=pool_size,
        runstate_dir=runstate_dir,
        backend_runtime_params=backend_runtime_params,
        std_schema=std_schema,
        refl_schema=refl_schema,
        schema_class_layout=schema_class_layout,
        dbindex=dbindex,
    )

    await pool.start()
    return pool
