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

import asyncio
import base64
import collections
import logging
import os.path
import pickle
import subprocess
import sys
import time

from edb.common import debug
from edb.common import supervisor
from edb.common import taskgroup

from . import amsg


BUFFER_POOL_SIZE = 4
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

    def __init__(self, manager, server, command_args):
        self._manager = manager
        self._server = server
        self._command_args = command_args
        self._proc = None
        self._con = None
        self._last_used = time.monotonic()
        self._closed = False
        self._sup = None

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
        self._manager._report_workers(self, action="kill")
        try:
            self._proc.terminate()
            await self._proc.wait()
        except ProcessLookupError:
            pass


class Manager:

    def __init__(self, *, worker_cls, worker_args,
                 loop, name, runstate_dir, pool_size=BUFFER_POOL_SIZE):

        self._worker_cls = worker_cls
        self._worker_args = worker_args

        self._loop = loop

        self._runstate_dir = runstate_dir

        self._name = name
        self._poolsock_name = os.path.join(
            self._runstate_dir, f'{name}.socket')

        self._pool_size = pool_size
        self._workers_pool = collections.deque()
        self._workers = set()

        self._server = amsg.Server(self._poolsock_name, loop)

        self._running = False

        self._stats_spawned = 0
        self._stats_killed = 0

        self._sup = None

        self._worker_command_args = [
            sys.executable, '-m', WORKER_MOD,

            '--cls-name',
            f'{self._worker_cls.__module__}.{self._worker_cls.__name__}',

            '--cls-args', base64.b64encode(pickle.dumps(self._worker_args)),
            '--sockname', self._poolsock_name
        ]

    def iter_workers(self):
        return iter(frozenset(self._workers))

    def is_running(self):
        return self._running

    async def _spawn_worker(self, *, report: bool = True):
        worker = Worker(self, self._server, self._worker_command_args)
        await worker._spawn()
        if report:
            self._report_workers(worker)
        return worker

    async def _spawn_for_pool(self):
        worker = await self._spawn_worker(report=False)
        self._workers_pool.appendleft(worker)
        self._report_workers(worker)
        return worker

    async def spawn_worker(self):
        """Returns a worker.

        If there is a worker pool, returns one of the existing workers and
        spawns a new one.  Otherwise, creates a new worker on the spot.
        """
        if not self._running:
            raise RuntimeError('cannot spawn a worker: not running')

        if self._workers_pool:
            worker = self._workers_pool.pop()
            self._sup.create_task(self._spawn_for_pool())
        else:
            worker = await self._spawn_worker(report=False)

        self._workers.add(worker)
        if not self._workers_pool:
            self._report_workers(worker)
        return worker

    async def start(self):
        self._sup = await supervisor.Supervisor.create()

        await self._server.start()
        self._running = True

        if self._pool_size:
            async with taskgroup.TaskGroup(name='manager-start') as g:
                for _ in range(self._pool_size):
                    g.create_task(self._spawn_for_pool())

    async def stop(self):
        if not self._running:
            return

        await self._sup.wait()

        await self._server.stop()
        self._server = None

        workers_to_kill = list(self._workers) + list(self._workers_pool)
        self._workers_pool.clear()
        self._workers.clear()
        self._running = False

        async with taskgroup.TaskGroup(
                name=f'{self._name}-manager-stop') as g:
            for worker in workers_to_kill:
                g.create_task(worker.close())

    def _report_workers(self, worker: Worker, *, action: str = "spawn"):
        action = action.capitalize()
        if not action.endswith("e"):
            action += "e"
        action += "d"
        log_metrics.info(
            "%s a %s worker with PID %d; used=%d; pool=%d;"
            + " spawned=%d; killed=%d",
            action,
            self._name,
            worker.get_pid(),
            len(self._workers),
            len(self._workers_pool),
            self._stats_spawned,
            self._stats_killed,
        )


async def create_manager(*, runstate_dir: str, name: str,
                         worker_cls: type, worker_args: dict,
                         pool_size: int) -> Manager:

    loop = asyncio.get_running_loop()
    pool = Manager(
        loop=loop,
        runstate_dir=runstate_dir,
        worker_cls=worker_cls,
        worker_args=worker_args,
        name=name,
        pool_size=pool_size)

    await pool.start()
    return pool
