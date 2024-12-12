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
from typing import Any, Dict, NamedTuple

import asyncio
import collections
import dataclasses
import functools
import hmac
import logging
import os
import os.path
import pickle
import signal
import subprocess
import sys
import time

import immutables

from edb.common import debug

from edb.pgsql import params as pgparams

from edb.server import args as srvargs
from edb.server import dbview
from edb.server import defines
from edb.server import metrics

from . import amsg
from . import queue
from . import state


PROCESS_INITIAL_RESPONSE_TIMEOUT: float = 60.0
KILL_TIMEOUT: float = 10.0
ADAPTIVE_SCALE_UP_WAIT_TIME: float = 3.0
ADAPTIVE_SCALE_DOWN_WAIT_TIME: float = 60.0
WORKER_PKG: str = __name__.rpartition('.')[0] + '.'


logger = logging.getLogger("edb.server")
log_metrics = logging.getLogger("edb.server.metrics")


# Inherit sys.path so that import system can find worker class
# in unittests.
_ENV = os.environ.copy()
_ENV['PYTHONPATH'] = ':'.join(sys.path)


@functools.lru_cache()
def _pickle_memoized(schema):
    return pickle.dumps(schema, -1)


class BaseWorker:

    _dbs: immutables.Map[str, state.PickledDatabaseState]
    _global_schema_pickle: bytes

    def __init__(
        self,
        dbs: immutables.Map[str, state.PickledDatabaseState],
        backend_runtime_params: pgparams.BackendRuntimeParams,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema_pickle,
        system_config,
    ):
        self._dbs = dbs
        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._global_schema_pickle = global_schema_pickle
        self._system_config = system_config
        self._last_pickled_state = None

        self._con = None
        self._last_used = time.monotonic()
        self._closed = False

    async def call(self, method_name, *args, sync_state=None):
        assert not self._closed

        if self._con.is_closed():
            raise RuntimeError(
                'the connection to the compiler worker process is '
                'unexpectedly closed')

        data = await self._request(method_name, args)

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

    async def _request(self, method_name, args):
        msg = pickle.dumps((method_name, args))
        return await self._con.request(msg)


class Worker(BaseWorker):
    def __init__(self, manager, server, pid, *args):
        super().__init__(*args)

        self._pid = pid
        self._last_pickled_state = None
        self._manager = manager
        self._server = server

    async def _attach(self, init_args_pickled: bytes):
        self._manager._stats_spawned += 1

        self._con = self._server.get_by_pid(self._pid)

        await self.call(
            '__init_worker__',
            init_args_pickled,
        )

    def get_pid(self):
        return self._pid

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._manager._stats_killed += 1
        self._manager._workers.pop(self._pid, None)
        self._manager._report_worker(self, action="kill")
        try:
            os.kill(self._pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


class AbstractPool:
    _dbindex: dbview.DatabaseIndex | None = None

    def __init__(self, *, loop, **kwargs):
        self._loop = loop
        self._init_args = self._init(kwargs)

    def _init(self, kwargs: dict[str, Any]) -> None:
        self._backend_runtime_params = kwargs["backend_runtime_params"]
        self._std_schema = kwargs["std_schema"]
        self._refl_schema = kwargs["refl_schema"]
        self._schema_class_layout = kwargs["schema_class_layout"]
        self._dbindex = kwargs.get("dbindex")

    def _get_init_args(self):
        assert self._dbindex is not None
        return self._make_init_args(*self._dbindex.get_cached_compiler_args())

    @functools.lru_cache(1)
    def _make_init_args(self, dbs, global_schema_pickle, system_config):
        init_args = (
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            global_schema_pickle,
            system_config,
        )
        pickled_args = pickle.dumps(init_args, -1)
        return init_args, pickled_args

    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError

    def get_template_pid(self):
        return None

    async def _compute_compile_preargs(
        self,
        method_name: str,
        worker: BaseWorker,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
    ):

        def sync_worker_state_cb(
            *,
            worker: BaseWorker,
            dbname,
            user_schema_pickle=None,
            global_schema_pickle=None,
            reflection_cache=None,
            database_config=None,
            system_config=None,
        ):
            worker_db = worker._dbs.get(dbname)
            if worker_db is None:
                assert user_schema_pickle is not None
                assert reflection_cache is not None
                assert global_schema_pickle is not None
                assert database_config is not None
                assert system_config is not None

                worker._dbs = worker._dbs.set(
                    dbname,
                    state.PickledDatabaseState(
                        user_schema_pickle=user_schema_pickle,
                        reflection_cache=reflection_cache,
                        database_config=database_config,
                    ),
                )
                worker._global_schema_pickle = global_schema_pickle
                worker._system_config = system_config
            else:
                if (
                    user_schema_pickle is not None
                    or reflection_cache is not None
                    or database_config is not None
                ):
                    worker._dbs = worker._dbs.set(
                        dbname,
                        state.PickledDatabaseState(
                            user_schema_pickle=(
                                user_schema_pickle
                                or worker_db.user_schema_pickle
                            ),
                            reflection_cache=(
                                reflection_cache
                                or worker_db.reflection_cache
                            ),
                            database_config=(
                                database_config or worker_db.database_config
                            ),
                        ),
                    )

                if global_schema_pickle is not None:
                    worker._global_schema_pickle = global_schema_pickle
                if system_config is not None:
                    worker._system_config = system_config

        worker_db = worker._dbs.get(dbname)
        preargs = [method_name, dbname]
        to_update = {}

        if worker_db is None:
            preargs.extend([
                user_schema_pickle,
                _pickle_memoized(reflection_cache),
                global_schema_pickle,
                _pickle_memoized(database_config),
                _pickle_memoized(system_config),
            ])
            to_update = {
                'user_schema_pickle': user_schema_pickle,
                'reflection_cache': reflection_cache,
                'global_schema_pickle': global_schema_pickle,
                'database_config': database_config,
                'system_config': system_config,
            }
        else:
            if worker_db.user_schema_pickle is not user_schema_pickle:
                preargs.append(user_schema_pickle)
                to_update['user_schema_pickle'] = user_schema_pickle
            else:
                preargs.append(None)

            if worker_db.reflection_cache is not reflection_cache:
                preargs.append(_pickle_memoized(reflection_cache))
                to_update['reflection_cache'] = reflection_cache
            else:
                preargs.append(None)

            if worker._global_schema_pickle is not global_schema_pickle:
                preargs.append(global_schema_pickle)
                to_update['global_schema_pickle'] = global_schema_pickle
            else:
                preargs.append(None)

            if worker_db.database_config is not database_config:
                preargs.append(_pickle_memoized(database_config))
                to_update['database_config'] = database_config
            else:
                preargs.append(None)

            if worker._system_config is not system_config:
                preargs.append(_pickle_memoized(system_config))
                to_update['system_config'] = system_config
            else:
                preargs.append(None)

        if to_update:
            callback = functools.partial(
                sync_worker_state_cb,
                worker=worker,
                dbname=dbname,
                **to_update
            )
        else:
            callback = None

        return tuple(preargs), callback

    async def _acquire_worker(
        self, *, condition=None, weighter=None, **compiler_args
    ):
        raise NotImplementedError

    def _release_worker(self, worker, *, put_in_front: bool = True):
        raise NotImplementedError

    async def compile(
        self,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
        *compile_args,
        **compiler_args,
    ):
        worker = await self._acquire_worker(**compiler_args)
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                "compile",
                worker,
                dbname,
                user_schema_pickle,
                global_schema_pickle,
                reflection_cache,
                database_config,
                system_config,
            )

            result = await worker.call(
                *preargs,
                *compile_args,
                sync_state=sync_state
            )
            worker._last_pickled_state = result[1]
            if len(result) == 2:
                return *result, 0
            else:
                return result

        finally:
            self._release_worker(worker)

    async def compile_in_tx(
        self,
        dbname,
        user_schema_pickle,
        txid,
        pickled_state,
        state_id,
        *compile_args,
        **compiler_args,
    ):
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
        worker = await self._acquire_worker(
            condition=lambda w: (w._last_pickled_state is pickled_state),
            compiler_args=compiler_args,
        )

        if worker._last_pickled_state is pickled_state:
            # Since we know that this particular worker already has the
            # state, we don't want to waste resources transferring the
            # state over the network. So we replace the state with a marker,
            # that the compiler process will recognize.
            pickled_state = state.REUSE_LAST_STATE_MARKER
            dbname = user_schema_pickle = None
        else:
            worker_db = worker._dbs.get(dbname)
            if worker_db is None:
                dbname = None
            elif worker_db.user_schema_pickle is user_schema_pickle:
                user_schema_pickle = None
            else:
                dbname = None

        try:
            units, new_pickled_state = await worker.call(
                'compile_in_tx',
                dbname,
                user_schema_pickle,
                pickled_state,
                txid,
                *compile_args
            )
            worker._last_pickled_state = new_pickled_state
            return units, new_pickled_state, 0

        finally:
            # Put the worker at the end of the queue so that the chance
            # of reusing it later (and maximising the chance of
            # the w._last_pickled_state is pickled_state` check returning
            # `True` is higher.
            self._release_worker(worker, put_in_front=False)

    async def compile_notebook(
        self,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
        *compile_args,
        **compiler_args,
    ):
        worker = await self._acquire_worker(**compiler_args)
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                "compile_notebook",
                worker,
                dbname,
                user_schema_pickle,
                global_schema_pickle,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._release_worker(worker)

    async def compile_graphql(
        self,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
        *compile_args,
        **compiler_args,
    ):
        worker = await self._acquire_worker(**compiler_args)
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                "compile_graphql",
                worker,
                dbname,
                user_schema_pickle,
                global_schema_pickle,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._release_worker(worker)

    async def compile_sql(
        self,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
        *compile_args,
        **compiler_args,
    ):
        worker = await self._acquire_worker(**compiler_args)
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                "compile_sql",
                worker,
                dbname,
                user_schema_pickle,
                global_schema_pickle,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                *preargs,
                *compile_args,
                sync_state=sync_state
            )
        finally:
            self._release_worker(worker)

    # We use a helper function instead of just fully generating the
    # functions in order to make the backtraces a little better.
    async def _simple_call(self, name, *args, **kwargs):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                name,
                *args,
                **kwargs
            )

        finally:
            self._release_worker(worker)

    async def interpret_backend_error(self, *args, **kwargs):
        return await self._simple_call(
            'interpret_backend_error', *args, **kwargs)

    async def parse_global_schema(self, *args, **kwargs):
        return await self._simple_call(
            'parse_global_schema', *args, **kwargs)

    async def parse_user_schema_db_config(self, *args, **kwargs):
        return await self._simple_call(
            'parse_user_schema_db_config', *args, **kwargs)

    async def make_state_serializer(self, *args, **kwargs):
        return await self._simple_call(
            'make_state_serializer', *args, **kwargs)

    async def make_compilation_config_serializer(self, *args, **kwargs):
        return await self._simple_call(
            'make_compilation_config_serializer', *args, **kwargs)

    async def describe_database_dump(self, *args, **kwargs):
        return await self._simple_call(
            'describe_database_dump', *args, **kwargs)

    async def describe_database_restore(self, *args, **kwargs):
        return await self._simple_call(
            'describe_database_restore', *args, **kwargs)

    async def analyze_explain_output(self, *args, **kwargs):
        return await self._simple_call(
            'analyze_explain_output', *args, **kwargs)

    async def validate_schema_equivalence(self, *args, **kwargs):
        return await self._simple_call(
            'validate_schema_equivalence', *args, **kwargs)

    async def compile_structured_config(self, *args, **kwargs):
        return await self._simple_call(
            'compile_structured_config', *args, **kwargs)

    def get_debug_info(self):
        return {}

    def get_size_hint(self) -> int:
        raise NotImplementedError


class BaseLocalPool(
    AbstractPool, amsg.ServerProtocol, asyncio.SubprocessProtocol
):

    _worker_class = Worker
    _worker_mod = "worker"
    _workers_queue: queue.WorkerQueue[Worker]
    _workers: Dict[int, Worker]

    def __init__(
        self,
        *,
        runstate_dir,
        pool_size,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._runstate_dir = runstate_dir

        self._poolsock_name = os.path.join(self._runstate_dir, 'ipc')
        assert len(self._poolsock_name) <= (
            defines.MAX_RUNSTATE_DIR_PATH
            + defines.MAX_UNIX_SOCKET_PATH_LENGTH
            + 1
        ), "pool IPC socket length exceeds maximum allowed"

        assert pool_size >= 1
        self._pool_size = pool_size
        self._workers = {}

        self._server = amsg.Server(self._poolsock_name, self._loop, self)
        self._ready_evt = asyncio.Event()

        self._running = None

        self._stats_spawned = 0
        self._stats_killed = 0

    def is_running(self):
        return bool(self._running)

    async def _attach_worker(self, pid: int):
        if not self._running:
            return
        logger.debug("Sending init args to worker with PID %s.", pid)
        init_args, init_args_pickled = self._get_init_args()
        worker = self._worker_class(  # type: ignore
            self,
            self._server,
            pid,
            *init_args,
        )
        await worker._attach(init_args_pickled)
        self._report_worker(worker)

        self._workers[pid] = worker
        self._workers_queue.release(worker)
        self._worker_attached()

        logger.debug("started compiler worker process (PID %s)", pid)
        if (
            not self._ready_evt.is_set()
            and len(self._workers) == self._pool_size
        ):
            logger.info(
                f"started {self._pool_size} compiler worker "
                f"process{'es' if self._pool_size > 1 else ''}",
            )
            self._ready_evt.set()

        return worker

    def _worker_attached(self):
        pass

    def worker_connected(self, pid, version):
        logger.debug("Worker with PID %s connected.", pid)
        self._loop.create_task(self._attach_worker(pid))
        metrics.compiler_process_spawns.inc()
        metrics.current_compiler_processes.inc()

    def worker_disconnected(self, pid):
        logger.debug("Worker with PID %s disconnected.", pid)
        self._workers.pop(pid, None)
        metrics.current_compiler_processes.dec()

    async def start(self):
        if self._running is not None:
            raise RuntimeError(
                'the compiler pool has already been started once')

        self._workers_queue = queue.WorkerQueue(self._loop)

        await self._server.start()
        self._running = True

        await self._start()

        await self._wait_ready()

    async def _wait_ready(self):
        await asyncio.wait_for(
            self._ready_evt.wait(),
            PROCESS_INITIAL_RESPONSE_TIMEOUT
        )

    async def _create_compiler_process(self, numproc=None, version=0):
        # Create a new compiler process. When numproc is None, a single
        # standalone compiler worker process is started; if numproc is an int,
        # a compiler template process will be created, which will then fork
        # itself into `numproc` actual worker processes and run as a supervisor

        env = _ENV
        if debug.flags.server:
            env = {'EDGEDB_DEBUG_SERVER': '1', **_ENV}

        cmdline = [sys.executable]
        if sys.flags.isolated:
            cmdline.append('-I')

        cmdline.extend([
            '-m', WORKER_PKG + self._worker_mod,
            '--sockname', self._poolsock_name,
            '--version-serial', str(version),
        ])
        if numproc:
            cmdline.extend([
                '--numproc', str(numproc),
            ])

        transport, _ = await self._loop.subprocess_exec(
            lambda: self,
            *cmdline,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=None,
            stderr=None,
        )
        return transport

    async def _start(self):
        raise NotImplementedError

    async def stop(self):
        if not self._running:
            return
        self._running = False

        await self._server.stop()
        self._server = None

        self._workers_queue = queue.WorkerQueue(self._loop)
        self._workers.clear()

        await self._stop()

    async def _stop(self):
        raise NotImplementedError

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

    async def _acquire_worker(
        self, *, condition=None, weighter=None, **compiler_args
    ):
        while (
            worker := await self._workers_queue.acquire(
                condition=condition, weighter=weighter
            )
        ).get_pid() not in self._workers:
            # The worker was disconnected; skip to the next one.
            pass
        return worker

    def _release_worker(self, worker, *, put_in_front: bool = True):
        # Skip disconnected workers
        if worker.get_pid() in self._workers:
            self._workers_queue.release(worker, put_in_front=put_in_front)

    def get_debug_info(self):
        return dict(
            worker_pids=list(self._workers.keys()),
            template_pid=self.get_template_pid(),
        )


@srvargs.CompilerPoolMode.Fixed.assign_implementation
class FixedPool(BaseLocalPool):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._template_transport = None
        self._template_proc_scheduled = False
        self._template_proc_version = 0

    def _worker_attached(self):
        if len(self._workers) > self._pool_size:
            self._server.kill_outdated_worker(self._template_proc_version)

    def worker_connected(self, pid, version):
        if version < self._template_proc_version:
            logger.debug(
                "Outdated worker with PID %s connected; discard now.", pid
            )
            self._server.get_by_pid(pid).abort()
            metrics.compiler_process_spawns.inc()
        else:
            super().worker_connected(pid, version)

    def process_exited(self):
        # Template process exited
        self._template_transport = None
        if self._running:
            logger.error("Template compiler process exited; recreating now.")
            self._schedule_template_proc(0)

    def get_template_pid(self):
        if self._template_transport is None:
            return None
        else:
            return self._template_transport.get_pid()

    async def _start(self):
        await self._create_template_proc(retry=False)

    async def _create_template_proc(self, retry=True):
        self._template_proc_scheduled = False
        if not self._running:
            return
        self._template_proc_version += 1
        try:
            # Create the template process, which will then fork() into numproc
            # child processes and manage them, so that we don't have to manage
            # the actual compiler worker processes in the main process.
            self._template_transport = await self._create_compiler_process(
                numproc=self._pool_size,
                version=self._template_proc_version,
            )
        except Exception:
            if retry:
                if self._running:
                    t = defines.BACKEND_COMPILER_TEMPLATE_PROC_RESTART_INTERVAL
                    logger.exception(
                        f"Unexpected error occurred creating template compiler"
                        f" process; retry in {t} second{'s' if t > 1 else ''}."
                    )
                    self._schedule_template_proc(t)
            else:
                raise

    def _schedule_template_proc(self, sleep):
        if self._template_proc_scheduled:
            return
        self._template_proc_scheduled = True
        self._loop.call_later(
            sleep, self._loop.create_task, self._create_template_proc()
        )

    async def _stop(self):
        trans, self._template_transport = self._template_transport, None
        if trans is not None:
            trans.terminate()
            await trans._wait()
            trans.close()

    def get_size_hint(self) -> int:
        return self._pool_size


@srvargs.CompilerPoolMode.OnDemand.assign_implementation
class SimpleAdaptivePool(BaseLocalPool):
    def __init__(self, *, pool_size, **kwargs):
        super().__init__(pool_size=1, **kwargs)
        self._worker_transports = {}
        self._expected_num_workers = 0
        self._scale_down_handle = None
        self._max_num_workers = pool_size
        self._cleanups = {}

    async def _start(self):
        async with asyncio.TaskGroup() as g:
            for _i in range(self._pool_size):
                g.create_task(self._create_worker())

    async def _stop(self):
        self._expected_num_workers = 0
        transports, self._worker_transports = self._worker_transports, {}
        for transport in transports.values():
            await transport._wait()
            transport.close()
        for cleanup in list(self._cleanups.values()):
            await cleanup

    async def _acquire_worker(
        self, *, condition=None, weighter=None, **compiler_args
    ):
        scale_up_handle = None
        if (
            self._running
            and self._workers_queue.qsize() == 0
            and (
                len(self._workers)
                == self._expected_num_workers
                < self._max_num_workers
            )
        ):
            scale_up_handle = self._loop.call_later(
                ADAPTIVE_SCALE_UP_WAIT_TIME, self._maybe_scale_up
            )
        if self._scale_down_handle is not None:
            self._scale_down_handle.cancel()
            self._scale_down_handle = None
        try:
            return await super()._acquire_worker(
                condition=condition, weighter=weighter, **compiler_args
            )
        finally:
            if scale_up_handle is not None:
                scale_up_handle.cancel()

    def _release_worker(self, worker, *, put_in_front: bool = True):
        if self._scale_down_handle is not None:
            self._scale_down_handle.cancel()
            self._scale_down_handle = None
        super()._release_worker(worker, put_in_front=put_in_front)
        if (
            self._running and
            self._workers_queue.count_waiters() == 0 and
            len(self._workers) > self._pool_size
        ):
            self._scale_down_handle = self._loop.call_later(
                ADAPTIVE_SCALE_DOWN_WAIT_TIME,
                self._scale_down,
            )

    async def _wait_on_dying(self, pid, trans):
        await trans._wait()
        self._cleanups.pop(pid)

    def worker_disconnected(self, pid):
        num_workers_before = len(self._workers)
        super().worker_disconnected(pid)
        trans = self._worker_transports.pop(pid, None)
        if trans:
            trans.close()
            # amsg.Server notifies us when the *pipe* to the worker closes,
            # so we need to fire off a task to make sure that we wait for
            # the worker to exit, in order to avoid a warning.
            self._cleanups[pid] = (
                self._loop.create_task(self._wait_on_dying(pid, trans)))
        if not self._running:
            return
        if len(self._workers) < self._pool_size:
            # The auto-scaler will not scale down below the pool_size, so we
            # should restart the unexpectedly-exited worker process.
            logger.warning(
                "Compiler worker process[%d] exited unexpectedly; "
                "start a new one now.", pid
            )
            self._loop.create_task(self._create_worker())
            self._expected_num_workers = len(self._workers)
        elif num_workers_before == self._expected_num_workers:
            # This is likely the case when a worker died unexpectedly, and we
            # don't want to restart the worker because the auto-scaler will
            # start a new one again if necessary.
            self._expected_num_workers = len(self._workers)

    def process_exited(self):
        if self._running:
            for pid, transport in list(self._worker_transports.items()):
                if transport.is_closing():
                    self._worker_transports.pop(pid, None)

    async def _create_worker(self):
        # Creates a single compiler worker process.
        transport = await self._create_compiler_process()
        self._worker_transports[transport.get_pid()] = transport
        self._expected_num_workers += 1

    def _maybe_scale_up(self):
        if not self._running:
            return
        logger.info(
            "A compile request has waited for more than %d seconds, "
            "spawn a new compiler worker process now.",
            ADAPTIVE_SCALE_UP_WAIT_TIME,
        )
        self._loop.create_task(self._create_worker())

    def _scale_down(self):
        self._scale_down_handle = None
        if not self._running or len(self._workers) <= self._pool_size:
            return
        logger.info(
            "The compiler pool is not used in %d seconds, scaling down to %d.",
            ADAPTIVE_SCALE_DOWN_WAIT_TIME, self._pool_size,
        )
        self._expected_num_workers = self._pool_size
        for worker in sorted(
            self._workers.values(), key=lambda w: w._last_used
        )[:-self._pool_size]:
            worker.close()

    def get_size_hint(self) -> int:
        return self._max_num_workers


class RemoteWorker(BaseWorker):
    def __init__(self, con, secret, *args):
        super().__init__(*args)
        self._con = con
        self._secret = secret

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._con.abort()

    async def _request(self, method_name, args):
        msg = pickle.dumps((method_name, args))
        digest = hmac.digest(self._secret, msg, "sha256")
        return await self._con.request(digest + msg)


@srvargs.CompilerPoolMode.Remote.assign_implementation
class RemotePool(AbstractPool):
    def __init__(self, *, address, pool_size, **kwargs):
        super().__init__(**kwargs)
        self._pool_addr = address
        self._worker = None
        self._sync_lock = asyncio.Lock()
        self._semaphore = asyncio.BoundedSemaphore(pool_size)
        self._pool_size = pool_size
        secret = os.environ.get("_EDGEDB_SERVER_COMPILER_POOL_SECRET")
        if not secret:
            raise AssertionError(
                "_EDGEDB_SERVER_COMPILER_POOL_SECRET environment variable "
                "is not set"
            )
        self._secret = secret.encode()

    async def start(self, retry=False):
        if self._worker is None:
            self._worker = self._loop.create_future()
        try:
            await self._loop.create_connection(
                lambda: amsg.HubProtocol(
                    loop=self._loop,
                    on_pid=lambda *args: self._loop.create_task(
                        self._connection_made(retry, *args)
                    ),
                    on_connection_lost=self._connection_lost,
                ),
                *self._pool_addr,
            )
        except Exception:
            if not retry:
                raise
            if self._worker is not None:
                self._loop.call_later(1, lambda: self._loop.create_task(
                    self.start(retry=True)
                ))
        else:
            if not retry:
                await self._worker

    async def stop(self):
        if self._worker is not None:
            worker, self._worker = self._worker, None
            if worker.done():
                (await worker).close()

    @functools.lru_cache(1)
    def _make_init_args(self, dbs, global_schema_pickle, system_config):
        init_args = (
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            global_schema_pickle,
            system_config,
        )
        std_args = (
            self._std_schema, self._refl_schema, self._schema_class_layout
        )
        client_args = (dbs, self._backend_runtime_params)
        return init_args, (
            pickle.dumps(std_args, -1),
            pickle.dumps(client_args, -1),
            global_schema_pickle,
            pickle.dumps(system_config, -1),
        )

    async def _connection_made(self, retry, protocol, transport, _pid, version):
        if self._worker is None:
            return
        try:
            init_args, init_args_pickled = self._get_init_args()
            worker = RemoteWorker(
                amsg.HubConnection(transport, protocol, self._loop, version),
                self._secret,
                *init_args,
            )
            await worker.call(
                '__init_server__',
                defines.EDGEDB_CATALOG_VERSION,
                init_args_pickled,
            )
        except state.IncompatibleClient as ex:
            transport.abort()
            if self._worker is not None:
                self._worker.set_exception(ex)
                self._worker = None
        except BaseException as ex:  # noqa: B036
            transport.abort()
            if self._worker is not None:
                if retry:
                    await self.start(retry=True)
                else:
                    self._worker.set_exception(ex)
                    self._worker = None
        else:
            if self._worker is not None:
                self._worker.set_result(worker)

    def _connection_lost(self, _pid):
        if self._worker is not None:
            self._worker = self._loop.create_future()
            self._loop.create_task(self.start(retry=True))

    async def _acquire_worker(
        self, *, condition=None, cmp=None, **compiler_args
    ):
        await self._semaphore.acquire()
        return await self._worker

    def _release_worker(self, worker, *, put_in_front: bool = True):
        if self._sync_lock.locked():
            self._sync_lock.release()
        self._semaphore.release()

    async def compile_in_tx(
        self,
        dbname,
        user_schema_pickle,
        txid,
        pickled_state,
        state_id,
        *compile_args,
        **compiler_args,
    ):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                'compile_in_tx',
                state_id,
                None,  # client_id
                None,  # dbname
                None,  # user_schema_pickle
                state.REUSE_LAST_STATE_MARKER,
                txid,
                *compile_args
            )
        except state.StateNotFound:
            return await worker.call(
                'compile_in_tx',
                0,  # state_id
                None,  # client_id
                None,  # dbname
                user_schema_pickle,
                pickled_state,
                txid,
                *compile_args
            )
        finally:
            self._release_worker(worker)

    async def _compute_compile_preargs(self, *args):
        preargs, callback = await super()._compute_compile_preargs(*args)
        if callback:
            del preargs, callback
            await self._sync_lock.acquire()
            preargs, callback = await super()._compute_compile_preargs(*args)
            if not callback:
                self._sync_lock.release()
        return preargs, callback

    def get_debug_info(self):
        return dict(
            address="{}:{}".format(*self._pool_addr),
            size=self._semaphore._bound_value,  # type: ignore
            free=self._semaphore._value,  # type: ignore
        )

    def get_size_hint(self) -> int:
        return self._pool_size


@dataclasses.dataclass
class TenantSchema:
    client_id: int
    dbs: immutables.Map[str, state.PickledDatabaseState]
    global_schema_pickle: bytes
    system_config: Any


class PickledState(NamedTuple):
    user_schema: bytes | None
    reflection_cache: bytes | None
    database_config: bytes | None


class PickledSchema(NamedTuple):
    dbs: immutables.Map[str, PickledState] | None = None
    global_schema: bytes | None = None
    instance_config: bytes | None = None
    dropped_dbs: tuple = ()


class MultiTenantWorker(Worker):
    current_client_id: int | None
    _cache: collections.OrderedDict[int, TenantSchema]
    _invalidated_clients: list[int]
    _last_used_by_client: dict[int, float]

    def __init__(
        self,
        manager,
        server,
        pid,
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
    ):
        super().__init__(
            manager,
            server,
            pid,
            None,
            backend_runtime_params,
            std_schema,
            refl_schema,
            schema_class_layout,
            None,
            None,
        )
        self.current_client_id = None
        self._cache = collections.OrderedDict()
        self._invalidated_clients = []
        self._last_used_by_client = {}

    def get_tenant_schema(self, client_id: int) -> TenantSchema | None:
        return self._cache.get(client_id)

    def set_tenant_schema(
        self, client_id: int, tenant_schema: TenantSchema
    ) -> None:
        self._cache[client_id] = tenant_schema
        self._cache.move_to_end(client_id, last=False)
        self._last_used_by_client[client_id] = time.monotonic()

    def cache_size(self) -> int:
        return len(self._cache) - len(self._invalidated_clients)

    def last_used(self, client_id) -> float:
        return self._last_used_by_client.get(client_id, 0)

    def invalidate(self, client_id: int) -> None:
        if client_id in self._cache:
            self._invalidated_clients.append(client_id)

    def maybe_invalidate_last(self) -> None:
        if self.cache_size() == self._manager.cache_size:
            client_id = next(reversed(self._cache))
            self._invalidated_clients.append(client_id)

    def get_invalidation(self) -> list[int]:
        return self._invalidated_clients[:]

    def flush_invalidation(self) -> None:
        client_ids, self._invalidated_clients = self._invalidated_clients, []
        for client_id in client_ids:
            self._cache.pop(client_id, None)
            self._last_used_by_client.pop(client_id, None)


@srvargs.CompilerPoolMode.MultiTenant.assign_implementation
class MultiTenantPool(FixedPool):
    _worker_class = MultiTenantWorker  # type: ignore
    _worker_mod = "multitenant_worker"
    _workers: Dict[int, MultiTenantWorker]  # type: ignore

    def __init__(self, *, cache_size, **kwargs):
        super().__init__(**kwargs)
        self._cache_size = cache_size

    @property
    def cache_size(self) -> int:
        return self._cache_size

    def drop_tenant(self, client_id: int):
        for worker in self._workers.values():
            worker.invalidate(client_id)

    @functools.cache
    def _get_init_args(self):
        init_args = (
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
        )
        return init_args, pickle.dumps(init_args, -1)

    def _weighter(self, client_id: int, worker: MultiTenantWorker):
        tenant_schema = worker.get_tenant_schema(client_id)
        return (
            bool(tenant_schema),
            worker.last_used(client_id)
            if tenant_schema
            else self._cache_size - worker.cache_size(),
        )

    async def _acquire_worker(
        self, *, condition=None, weighter=None, **compiler_args
    ):
        client_id = compiler_args.get("client_id")
        if weighter is None and client_id is not None:
            weighter = functools.partial(self._weighter, client_id)
        rv = await super()._acquire_worker(
            condition=condition, weighter=weighter, **compiler_args
        )
        rv.current_client_id = client_id
        return rv

    def _release_worker(self, worker, *, put_in_front: bool = True):
        worker.current_client_id = None
        super()._release_worker(worker, put_in_front=put_in_front)

    async def _compute_compile_preargs(
        self,
        method_name: str,
        worker: BaseWorker,
        dbname,
        user_schema_pickle,
        global_schema_pickle,
        reflection_cache,
        database_config,
        system_config,
    ):
        assert isinstance(worker, MultiTenantWorker)

        def sync_worker_state_cb(
            *,
            worker: MultiTenantWorker,
            client_id,
            dbname,
            user_schema_pickle=None,
            global_schema_pickle=None,
            reflection_cache=None,
            database_config=None,
            instance_config=None,
        ):
            tenant_schema = worker.get_tenant_schema(client_id)
            if tenant_schema is None:
                assert user_schema_pickle is not None
                assert reflection_cache is not None
                assert global_schema_pickle is not None
                assert database_config is not None
                assert instance_config is not None

                tenant_schema = TenantSchema(
                    client_id,
                    immutables.Map([(dbname, state.PickledDatabaseState(
                        user_schema_pickle,
                        reflection_cache,
                        database_config,
                    ))]),
                    global_schema_pickle,
                    instance_config,
                )
                worker.set_tenant_schema(client_id, tenant_schema)
            else:
                worker_db = tenant_schema.dbs.get(dbname)
                if worker_db is None:
                    assert user_schema_pickle is not None
                    assert reflection_cache is not None
                    assert database_config is not None

                    tenant_schema.dbs = tenant_schema.dbs.set(
                        dbname,
                        state.PickledDatabaseState(
                            user_schema_pickle=user_schema_pickle,
                            reflection_cache=reflection_cache,
                            database_config=database_config,
                        ),
                    )

                elif (
                    user_schema_pickle is not None
                    or reflection_cache is not None
                    or database_config is not None
                ):
                    tenant_schema.dbs = tenant_schema.dbs.set(
                        dbname,
                        state.PickledDatabaseState(
                            user_schema_pickle=(
                                user_schema_pickle
                                or worker_db.user_schema_pickle
                            ),
                            reflection_cache=(
                                reflection_cache or worker_db.reflection_cache
                            ),
                            database_config=(
                                database_config or worker_db.database_config
                            ),
                        )
                    )

                if global_schema_pickle is not None:
                    tenant_schema.global_schema_pickle = global_schema_pickle
                if instance_config is not None:
                    tenant_schema.system_config = instance_config
            worker.flush_invalidation()

        client_id = worker.current_client_id
        assert client_id is not None
        tenant_schema = worker.get_tenant_schema(client_id)
        if tenant_schema is None:
            # make room for the new client in this worker
            worker.maybe_invalidate_last()
            to_update = {
                "user_schema_pickle": user_schema_pickle,
                "reflection_cache": reflection_cache,
                "global_schema_pickle": global_schema_pickle,
                "database_config": database_config,
                "instance_config": system_config,
            }
        else:
            worker_db = tenant_schema.dbs.get(dbname)
            if worker_db is None:
                to_update = {
                    "user_schema_pickle": user_schema_pickle,
                    "reflection_cache": reflection_cache,
                    "database_config": database_config,
                }
            else:
                to_update = {}
                if worker_db.user_schema_pickle is not user_schema_pickle:
                    to_update["user_schema_pickle"] = user_schema_pickle
                if worker_db.reflection_cache is not reflection_cache:
                    to_update["reflection_cache"] = reflection_cache
                if worker_db.database_config is not database_config:
                    to_update["database_config"] = database_config
            if (
                tenant_schema.global_schema_pickle
                is not global_schema_pickle
            ):
                to_update["global_schema_pickle"] = global_schema_pickle
            if tenant_schema.system_config is not system_config:
                to_update["instance_config"] = system_config

        if to_update:
            pickled = {
                k.removesuffix("_pickle"): v
                if k.endswith("_pickle")
                else _pickle_memoized(v)
                for k, v in to_update.items()
            }
            if any(f in pickled for f in PickledState._fields):
                db_state = PickledState(
                    **{f: pickled.pop(f, None) for f in PickledState._fields}
                )
                pickled["dbs"] = immutables.Map([(dbname, db_state)])
            pickled_schema = PickledSchema(**pickled)
            callback = functools.partial(
                sync_worker_state_cb,
                worker=worker,
                client_id=client_id,
                dbname=dbname,
                **to_update,
            )
        else:
            pickled_schema = None
            callback = None

        return (
            "call_for_client",
            client_id,
            pickled_schema,
            worker.get_invalidation(),
            None,  # forwarded msg is only used in remote compiler server
            method_name,
            dbname,
        ), callback

    async def compile_in_tx(
        self,
        dbname,
        user_schema_pickle,
        txid,
        pickled_state,
        state_id,
        *compile_args,
        **compiler_args,
    ):
        client_id = compiler_args.get("client_id")

        # Prefer a worker we used last time in the transaction (condition), or
        # (weighter) one with the user schema at tx start so that we can pass
        # over only the pickled state. Then prefer the least-recently used one
        # if many workers passed any check in the weighter, or the most vacant.
        def weighter(w: MultiTenantWorker):
            if ts := w.get_tenant_schema(client_id):
                if db := ts.dbs.get(dbname):
                    return (
                        True,
                        db.user_schema_pickle is user_schema_pickle,
                        w.last_used(client_id),
                    )
                else:
                    return True, False, w.last_used(client_id)
            else:
                return False, False, self._cache_size - w.cache_size()

        worker = await self._acquire_worker(
            condition=lambda w: (w._last_pickled_state is pickled_state),
            weighter=weighter,
            **compiler_args,
        )

        # Avoid sending information that we know the worker already have.
        if worker._last_pickled_state is pickled_state:
            pickled_state = state.REUSE_LAST_STATE_MARKER
            dbname = client_id = user_schema_pickle = None
        else:
            assert isinstance(worker, MultiTenantWorker)
            assert client_id is not None
            tenant_schema = worker.get_tenant_schema(client_id)
            if tenant_schema is None:
                # Just pass state + root user schema if this is a new client in
                # the worker; we don't want to initialize the client as we
                # don't have enough information to do so.
                dbname = client_id = None
            else:
                worker_db = tenant_schema.dbs.get(dbname)
                if worker_db is None:
                    # The worker has the client but not the database
                    dbname = client_id = None
                elif worker_db.user_schema_pickle is user_schema_pickle:
                    # Avoid sending the root user schema because the worker has
                    # it - just send client_id + dbname to reference it, as
                    # well as the state of course.
                    user_schema_pickle = None
                else:
                    # The worker has a different root user schema
                    dbname = client_id = None

        try:
            units, new_pickled_state = await worker.call(
                'compile_in_tx',
                # multitenant_worker is also used in MultiSchemaPool for remote
                # compilers where the first argument "state_id" is used to find
                # worker without passing the pickled state. Here in multi-
                # tenant mode, we already have the pickled state, so "state_id"
                # is not used. Just prepend a fake ID to comply to the API.
                0,  # state_id
                client_id,
                dbname,
                user_schema_pickle,
                pickled_state,
                txid,
                *compile_args
            )
            worker._last_pickled_state = new_pickled_state
            return units, new_pickled_state, 0

        finally:
            self._release_worker(worker, put_in_front=False)


async def create_compiler_pool(
    *,
    runstate_dir: str,
    pool_size: int,
    backend_runtime_params: pgparams.BackendRuntimeParams,
    std_schema,
    refl_schema,
    schema_class_layout,
    pool_class=FixedPool,
    **kwargs,
) -> AbstractPool:
    assert issubclass(pool_class, AbstractPool)
    loop = asyncio.get_running_loop()
    pool = pool_class(
        loop=loop,
        pool_size=pool_size,
        runstate_dir=runstate_dir,
        backend_runtime_params=backend_runtime_params,
        std_schema=std_schema,
        refl_schema=refl_schema,
        schema_class_layout=schema_class_layout,
        **kwargs,
    )

    await pool.start()
    return pool
