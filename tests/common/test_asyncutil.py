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
import unittest

from edb.common import asyncutil
from edb.testbase.asyncutils import with_fake_event_loop


class TestDebounce(unittest.TestCase):

    @with_fake_event_loop
    async def test_debounce_01(self):
        loop = asyncio.get_running_loop()
        outs = []
        ins = asyncio.Queue()

        async def output(vs):
            assert loop.time() == int(loop.time())
            outs.append((int(loop.time()), vs))

        async def sleep_until(t):
            await asyncio.sleep(t - loop.time())

        task = asyncio.create_task(asyncutil.debounce(
            ins.get,
            output,
            # Use integers for delays to avoid any possibility of
            # floating point nonsense
            max_wait=500,
            delay_amt=200,
            max_batch_size=4,
        ))

        ins.put_nowait(1)
        await sleep_until(10)
        ins.put_nowait(2)
        ins.put_nowait(3)
        await sleep_until(300)
        ins.put_nowait(4)
        ins.put_nowait(5)
        ins.put_nowait(6)
        await sleep_until(1000)

        # Time 1000 now
        ins.put_nowait(7)
        await sleep_until(1150)
        ins.put_nowait(8)
        ins.put_nowait(9)
        ins.put_nowait(10)
        await sleep_until(1250)
        ins.put_nowait(11)

        ins.put_nowait(12)
        await asyncio.sleep(190)
        ins.put_nowait(13)
        await asyncio.sleep(190)
        ins.put_nowait(14)
        await asyncio.sleep(190)
        self.assertEqual(loop.time(), 1820)
        ins.put_nowait(15)

        # Make sure everything clears out and stop it
        await asyncio.sleep(10000)
        task.cancel()

        self.assertEqual(
            outs,
            [
                # First one right away
                (0, [1]),
                # Next two added at 10 + 200 tick
                (210, [2, 3]),
                # Next three added at 300 + 200 tick
                (500, [4, 5, 6]),
                # First at 1000
                (1000, [7]),
                # Next group at 1250 when the batch fills up
                (1250, [8, 9, 10, 11]),
                # And more at 1750 when time expires on that batch
                (1750, [12, 13, 14]),
                # And the next one (queued at 1820) at 200 after it was queued,
                # since there had been a recent signal when it was queued.
                (2020, [15]),
            ],
        )


class TestExclusiveTask(unittest.TestCase):
    async def _test(self, task: asyncutil.ExclusiveTask, get_counter):
        # double-schedule is effective only once
        task.schedule()
        self.assertTrue(task.scheduled)
        task.schedule()
        self.assertTrue(task.scheduled)
        self.assertEqual(get_counter(), 0)

        # an exclusive task is running, schedule another one with a double shot
        await asyncio.sleep(4)
        self.assertFalse(task.scheduled)
        await asyncio.sleep(1)
        task.schedule()
        self.assertTrue(task.scheduled)
        task.schedule()
        self.assertTrue(task.scheduled)
        self.assertEqual(get_counter(), 1)

        # first task done, second follows immediately
        await asyncio.sleep(5)
        self.assertFalse(task.scheduled)
        self.assertEqual(get_counter(), 3)

        # all done
        await asyncio.sleep(9)
        self.assertFalse(task.scheduled)
        self.assertEqual(get_counter(), 4)

        # works repeatedly
        await asyncio.sleep(1)
        task.schedule()
        self.assertTrue(task.scheduled)
        await asyncio.sleep(3)
        self.assertFalse(task.scheduled)
        await asyncio.sleep(1)
        task.schedule()
        self.assertTrue(task.scheduled)
        self.assertEqual(get_counter(), 5)

        # now stop the scheduled task and wait for the running one to finish
        await asyncio.sleep(1)
        await task.stop()
        self.assertFalse(task.scheduled)
        self.assertEqual(get_counter(), 6)

        # no further schedule allowed
        task.schedule()
        self.assertFalse(task.scheduled)
        await asyncio.sleep(10)
        self.assertEqual(get_counter(), 6)

    @with_fake_event_loop
    async def test_exclusive_task_01(self):
        counter = 0

        @asyncutil.exclusive_task
        async def task():
            nonlocal counter
            counter += 1
            await asyncio.sleep(8)
            counter += 1

        await self._test(task, lambda: counter)

    @with_fake_event_loop
    async def test_exclusive_task_02(self):
        counter = 0

        @asyncutil.exclusive_task()
        async def task():
            nonlocal counter
            counter += 1
            await asyncio.sleep(8)
            counter += 1

        await self._test(task, lambda: counter)

    @with_fake_event_loop
    async def test_exclusive_task_03(self):
        class MyClass:
            def __init__(self):
                self.counter = 0

            @asyncutil.exclusive_task
            async def task(self):
                self.counter += 1
                await asyncio.sleep(8)
                self.counter += 1

        obj = MyClass()
        await self._test(obj.task, lambda: obj.counter)

    @with_fake_event_loop
    async def test_exclusive_task_04(self):
        class MyClass:
            def __init__(self):
                self.counter = 0

            @asyncutil.exclusive_task(slot="another")
            async def task(self):
                self.counter += 1
                await asyncio.sleep(8)
                self.counter += 1

        obj = MyClass()
        await self._test(obj.task, lambda: obj.counter)

    @with_fake_event_loop
    async def test_exclusive_task_05(self):
        class MyClass:
            __slots__ = ("counter", "another",)

            def __init__(self):
                self.counter = 0

            @asyncutil.exclusive_task(slot="another")
            async def task(self):
                self.counter += 1
                await asyncio.sleep(8)
                self.counter += 1

        obj = MyClass()
        await self._test(obj.task, lambda: obj.counter)

    @with_fake_event_loop
    async def test_exclusive_task_06(self):
        class MyClass:
            def __init__(self, factor: int):
                self.counter = 0
                self.factor = factor

            @asyncutil.exclusive_task
            async def task(self):
                self.counter += self.factor
                await asyncio.sleep(8)
                self.counter += self.factor

        obj1 = MyClass(1)
        obj2 = MyClass(2)
        async with asyncio.TaskGroup() as g:
            g.create_task(
                self._test(obj1.task, lambda: obj1.counter // obj1.factor)
            )
            await asyncio.sleep(3)
            g.create_task(
                self._test(obj2.task, lambda: obj2.counter // obj2.factor)
            )

    def test_exclusive_task_07(self):
        with self.assertRaises(TypeError):
            class MyClass:
                __slots__ = ()

                @asyncutil.exclusive_task
                async def task(self):
                    pass

    def test_exclusive_task_08(self):
        with self.assertRaises(TypeError):
            class MyClass:
                __slots__ = ()

                @asyncutil.exclusive_task(slot="missing")
                async def task(self):
                    pass

    def test_exclusive_task_09(self):
        with self.assertRaises(TypeError):
            @asyncutil.exclusive_task
            async def task(*args, **kwargs):
                pass

    def test_exclusive_task_10(self):
        with self.assertRaises(TypeError):
            @asyncutil.exclusive_task
            async def task(*, p):
                pass

    def test_exclusive_task_11(self):
        with self.assertRaises(TypeError):
            class MyClass:
                @asyncutil.exclusive_task
                async def task(self, p):
                    pass

    def test_exclusive_task_12(self):
        with self.assertRaises(TypeError):
            class MyClass:
                @asyncutil.exclusive_task
                @classmethod
                async def task(cls):
                    pass

    @with_fake_event_loop
    async def test_exclusive_task_13(self):
        counter = 0

        class MyClass:
            @asyncutil.exclusive_task
            @staticmethod
            async def task():
                nonlocal counter
                counter += 1
                await asyncio.sleep(8)
                counter += 1

        obj1 = MyClass()
        obj2 = MyClass()

        async with asyncio.TaskGroup() as g:
            g.create_task(
                self._test(obj1.task, lambda: counter)
            )
            g.create_task(
                self._test(obj2.task, lambda: counter)
            )
