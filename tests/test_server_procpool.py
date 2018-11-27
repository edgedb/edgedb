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
import os
import signal
import tempfile

import uvloop

from edb.server import _testbase as tb
from edb.server2 import procpool
from edb.lang.common import taskgroup


class MyExc(Exception):
    pass


class Worker:

    def __init__(self, o):
        self._o = o
        self._i = 0

    async def test1(self, t):
        self._i += 1
        await asyncio.sleep(t)
        return self._i

    async def test2(self):
        return self._o

    async def test3(self):
        1 / 0

    async def test4(self):
        e = MyExc()
        e.special = 'spam'
        raise e

    async def test5(self):
        class WillCrashPickle(Exception):
            pass
        raise WillCrashPickle


class TestProcPool(tb.TestCase):

    @classmethod
    def setUpClass(cls):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            asyncio.set_event_loop_policy(None)

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.runstate_dir = self._dir.name

    def tearDown(self):
        self._dir.cleanup()
        self._dir = None

    async def test_procpool_1(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_1')

        try:
            i1 = asyncio.create_task(pool.call('test1', 0.1))
            i2 = asyncio.create_task(pool.call('test1', 0.05))

            i1 = await i1
            i2 = await i2

            self.assertEqual(i1, 1)
            self.assertEqual(i2, 2)
        finally:
            await pool.stop()

    async def test_procpool_2(self):
        pool = await procpool.create_pool(
            max_capacity=5,
            min_capacity=5,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_2')

        try:
            tasks = []
            for i in range(20):
                tasks.append(asyncio.create_task(pool.call('test1', 0.1)))

            await asyncio.gather(*tasks)
        finally:
            await pool.stop()

        results = [t.result() for t in tasks]
        self.assertEqual(results, [
            1, 1, 1, 1, 1,
            2, 2, 2, 2, 2,
            3, 3, 3, 3, 3,
            4, 4, 4, 4, 4
        ])

    async def test_procpool_3(self):
        pool = await procpool.create_pool(
            max_capacity=5,
            min_capacity=5,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_3')

        try:
            r = await pool.call('test2')
        finally:
            await pool.stop()

        self.assertEqual(r, [123])

    async def test_procpool_4(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_4')

        try:
            with self.assertRaises(ZeroDivisionError):
                await pool.call('test3')

            self.assertEqual(await pool.call('test1', 0.1), 1)

            with self.assertRaises(ZeroDivisionError):
                await pool.call('test3')

            self.assertEqual(await pool.call('test1', 0.1), 2)

        finally:
            await pool.stop()

    async def test_procpool_5(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_5')

        try:
            t1 = asyncio.create_task(pool.call('test3'))
            t2 = asyncio.create_task(pool.call('test1', 0.1))
            t3 = asyncio.create_task(pool.call('test3'))
            t4 = asyncio.create_task(pool.call('test1', 0.1))

            await asyncio.gather(t1, t2, t3, t4, return_exceptions=True)

            with self.assertRaises(ZeroDivisionError):
                await t1
            with self.assertRaises(ZeroDivisionError):
                await t3

            self.assertEqual(t2.result(), 1)
            self.assertEqual(t4.result(), 2)

        finally:
            await pool.stop()

    async def test_procpool_6(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_6')

        try:
            with self.assertRaises(MyExc) as e:
                await pool.call('test4')

            self.assertEqual(e.exception.special, 'spam')

        finally:
            await pool.stop()

    async def test_procpool_7(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_7')

        try:
            with self.assertRaisesRegex(RuntimeError, 'pickle local object'):
                await pool.call('test5')

            self.assertEqual(await pool.call('test1', 0.1), 1)

        finally:
            await pool.stop()

    async def test_procpool_8(self):
        pool = await procpool.create_pool(
            max_capacity=1,
            min_capacity=1,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_8')

        worker = next(pool.manager.iter_workers())
        pid = worker.get_pid()

        try:
            t = asyncio.create_task(pool.call('test1', 10))
            await asyncio.sleep(0.1)

            os.kill(pid, signal.SIGTERM)

            with self.assertRaisesRegex(ConnectionError,
                                        'lost connection to the worker'):
                await t

            self.assertEqual(await pool.call('test1', 0.1), 1)

        finally:
            await pool.stop()

    async def test_procpool_9(self):
        pool = await procpool.create_pool(
            max_capacity=10,
            min_capacity=1,
            gc_interval=0.01,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_9')

        try:
            async with taskgroup.TaskGroup() as g:
                for _ in range(100):
                    g.create_task(pool.call('test1', 0.1))

            await asyncio.sleep(1)
            await pool.call('test1', 0.1)

        finally:
            await pool.stop()

    async def test_procpool_10(self):
        pool = await procpool.create_pool(
            max_capacity=10,
            min_capacity=2,
            gc_interval=0.01,
            runstate_dir=self.runstate_dir,
            worker_cls=Worker,
            worker_args=([123],),
            name='test_procpool_10')

        manager = pool.manager

        try:
            async with taskgroup.TaskGroup() as g:
                for _ in range(100):
                    g.create_task(pool.call('test1', 0.1))

            await asyncio.sleep(0.5)

            self.assertEqual(manager._stats_spawned, 10)
            self.assertEqual(manager._stats_killed, 8)

            w1 = await pool.acquire()
            w2 = await pool.acquire()
            w3 = await pool.acquire()

            await asyncio.sleep(0.5)

            self.assertEqual(manager._stats_spawned, 11)
            self.assertEqual(manager._stats_killed, 8)

            await w1.call('test1', 0.1)
            await w2.call('test1', 0.1)
            await w3.call('test1', 0.1)

            self.assertEqual(manager._stats_spawned, 11)
            self.assertEqual(manager._stats_killed, 8)

        finally:
            await pool.stop()

        self.assertEqual(manager._stats_spawned, 11)
        self.assertEqual(manager._stats_killed, 11)
