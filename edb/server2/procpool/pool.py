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


import asyncio
import base64
import os.path
import pickle
import subprocess
import sys
import time

from edb.lang.common import taskgroup

from . import amsg


GC_INTERVAL = 60.0 * 3
PROCESS_INITIAL_RESPONSE_TIMEOUT = 10.0
KILL_TIMEOUT = 10.0
WORKER_MOD = __name__.rpartition('.')[0] + '.worker'


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
            asyncio.create_task(self._kill_proc(self._proc))
            self._proc = None

        self._proc = await asyncio.create_subprocess_exec(
            *self._command_args,
            env=_ENV,
            stdin=subprocess.DEVNULL)
        try:
            self._con = await asyncio.wait_for(
                self._server.get_by_pid(self._proc.pid),
                PROCESS_INITIAL_RESPONSE_TIMEOUT)
        except Exception:
            self._proc.kill()
            raise

    def get_pid(self):
        return self._proc.pid

    async def call(self, method_name, *args):
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
            exc.__text_traceback__ = tb
            raise exc
        else:
            raise RuntimeError(data[0])

    def close(self):
        self._manager._stats_killed += 1
        self._manager._workers.discard(self)
        self._proc.kill()


class Manager:

    def __init__(self, *, worker_cls, worker_args, loop, name, runstate_dir):

        self._worker_cls = worker_cls
        self._worker_args = worker_args

        self._loop = loop

        self._runstate_dir = runstate_dir

        self._name = name
        self._poolsock_name = os.path.join(
            self._runstate_dir, f'{name}.socket')

        self._workers = set()

        self._server = amsg.Server(self._poolsock_name, loop)

        self._running = False

        self._stats_spawned = 0
        self._stats_killed = 0

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

    async def spawn_worker(self):
        if not self._running:
            raise RuntimeError('cannot spawn a worker: not running')
        worker = Worker(self, self._server, self._worker_command_args)
        await worker._spawn()
        self._workers.add(worker)
        return worker

    async def start(self):
        await self._server.start()
        self._running = True

    async def stop(self):
        if not self._running:
            return

        await self._server.stop()
        self._server = None

        for worker in list(self._workers):
            worker.close()

        self._workers.clear()
        self._running = False


class Pool:

    def __init__(self, *, worker_cls, worker_args,
                 loop,
                 max_capacity,
                 min_capacity,
                 runstate_dir,
                 name,
                 gc_interval):

        if min_capacity > max_capacity:
            raise ValueError(
                'min_capacity cannot be greater than max_capacity')

        if min_capacity <= 0 or max_capacity <= 0:
            raise ValueError(
                'min_capacity and max_capacity must be greater than 0')

        self._name = name
        self._loop = loop
        self._manager = Manager(
            loop=loop,
            worker_cls=worker_cls,
            worker_args=worker_args,
            name=name,
            runstate_dir=runstate_dir)

        self._max_capacity = max_capacity
        self._min_capacity = min_capacity
        self._capacity = 0
        self._gc_interval = gc_interval

        self._workers_queue = asyncio.Queue(loop=loop)

        self._gc_task = None

    @property
    def manager(self):
        return self._manager

    async def _worker_gc(self):
        while True:
            await asyncio.sleep(self._gc_interval)
            if (self._capacity <= self._min_capacity or
                    self._workers_queue.empty()):
                continue

            workers_to_kill = set()
            for _ in range(self._capacity - self._min_capacity):
                if self._workers_queue.empty():
                    break

                worker = self._workers_queue.get_nowait()
                workers_to_kill.add(worker)

            if not workers_to_kill:
                continue

            now = time.monotonic()

            workers_killed = set()
            for worker in set(workers_to_kill):
                if now - worker._last_used > self._gc_interval:
                    worker.close()
                    workers_killed.add(worker)
                    self._capacity -= 1

            for worker in (workers_to_kill - workers_killed):
                self._workers_queue.put_nowait(worker)

    async def _spawn_worker(self, *, enqueue=True):
        self._capacity += 1
        try:
            worker = await self._manager.spawn_worker()
            if enqueue:
                self._workers_queue.put_nowait(worker)
            return worker
        except Exception:
            self._capacity -= 1
            raise

    async def acquire(self):
        if not self._manager.is_running():
            raise RuntimeError('the process pool is not running')

        if self._gc_task is not None and self._gc_task.done():
            # Did it crash?  This is not normal.
            self._gc_task.result()
            raise RuntimeError('the process pool is in an undefined state')

        if self._workers_queue.empty() and self._capacity < self._max_capacity:
            return await self._spawn_worker(enqueue=False)

        return await self._workers_queue.get()

    def release(self, worker):
        self._workers_queue.put_nowait(worker)

    async def call(self, method_name, *args):
        worker = await self.acquire()
        try:
            return await worker.call(method_name, *args)
        finally:
            self.release(worker)

    async def start(self):
        if self._manager.is_running():
            raise RuntimeError('already running')

        await self._manager.start()

        try:
            async with taskgroup.TaskGroup(
                    name=f'{self._name}-pool-spawn') as g:

                for i in range(self._min_capacity):
                    g.create_task(self._spawn_worker())

        except taskgroup.TaskGroupError:
            await self.stop()
            raise

        self._gc_task = asyncio.create_task(self._worker_gc())

    async def stop(self):
        if self._gc_task is not None:
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass
            self._gc_task = None

        await self._manager.stop()


async def create_pool(*, max_capacity: int, min_capacity: int,
                      runstate_dir: str, name: str,
                      worker_cls: type, worker_args: tuple,
                      gc_interval: float=GC_INTERVAL) -> Pool:

    loop = asyncio.get_running_loop()
    pool = Pool(
        loop=loop,
        min_capacity=min_capacity,
        max_capacity=max_capacity,
        runstate_dir=runstate_dir,
        worker_cls=worker_cls,
        worker_args=worker_args,
        name=name,
        gc_interval=gc_interval)

    await pool.start()
    return pool


async def create_manager(*, runstate_dir: str, name: str,
                         worker_cls: type, worker_args: tuple) -> Manager:

    loop = asyncio.get_running_loop()
    pool = Manager(
        loop=loop,
        runstate_dir=runstate_dir,
        worker_cls=worker_cls,
        worker_args=worker_args,
        name=name)

    await pool.start()
    return pool
