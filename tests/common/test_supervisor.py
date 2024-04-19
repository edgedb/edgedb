#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import supervisor
from edb.testbase import server as tb


class TestSupervisor(tb.TestCase):

    async def test_supervisor_01(self):

        async def foo1():
            await asyncio.sleep(0.1)
            return 42

        async def foo2():
            await asyncio.sleep(0.2)
            return 11

        g = await supervisor.Supervisor.create()
        t1 = g.create_task(foo1())
        t2 = g.create_task(foo2())
        await g.wait()

        self.assertEqual(t1.result(), 42)
        self.assertEqual(t2.result(), 11)

    async def test_supervisor_02(self):

        async def foo1():
            await asyncio.sleep(0.1)
            return 42

        async def foo2():
            await asyncio.sleep(0.2)
            return 11

        g = await supervisor.Supervisor.create()
        t1 = g.create_task(foo1())
        await asyncio.sleep(0.15)
        t2 = g.create_task(foo2())
        await g.wait()

        self.assertEqual(t1.result(), 42)
        self.assertEqual(t2.result(), 11)

    async def test_supervisor_03(self):

        async def foo1():
            await asyncio.sleep(1)
            return 42

        async def foo2():
            await asyncio.sleep(0.2)
            return 11

        g = await supervisor.Supervisor.create()

        t1 = g.create_task(foo1())
        await asyncio.sleep(0.15)
        # cancel t1 explicitly, i.e. everything should continue
        # working as expected.
        t1.cancel()

        t2 = g.create_task(foo2())
        await g.wait()

        self.assertTrue(t1.cancelled())
        self.assertEqual(t2.result(), 11)

    async def test_supervisor_04(self):

        NUM = 0
        t2_cancel = False
        t2 = None

        async def foo1():
            await asyncio.sleep(0.1)
            return 1 / 0

        async def foo2():
            nonlocal NUM, t2_cancel
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                t2_cancel = True
                raise
            NUM += 1

        async def runner():
            nonlocal NUM, t2

            g = await supervisor.Supervisor.create()

            g.create_task(foo1())
            t2 = g.create_task(foo2())

            await g.wait()

            NUM += 10

        with self.assertRaisesRegex(ExceptionGroup, r'1 sub-exception'):
            await self.loop.create_task(runner())

        self.assertEqual(NUM, 0)
        self.assertTrue(t2_cancel)
        self.assertTrue(t2.cancelled())

    async def test_supervisor_05(self):

        NUM = 0

        async def foo():
            nonlocal NUM
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                NUM += 1
                raise

        async def runner():
            g = await supervisor.Supervisor.create()

            for _ in range(5):
                g.create_task(foo())

            await g.wait()

        r = self.loop.create_task(runner())
        await asyncio.sleep(0.1)

        self.assertFalse(r.done())
        r.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await r

        self.assertEqual(NUM, 5)

    async def test_supervisor_06(self):

        async def foo1():
            await asyncio.sleep(1)
            return 42

        async def foo2():
            await asyncio.sleep(2)
            return 11

        async def runner():
            g = await supervisor.Supervisor.create()

            g.create_task(foo1())
            g.create_task(foo2())

            await g.wait()

        r = self.loop.create_task(runner())
        await asyncio.sleep(0.05)
        r.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await r

    async def test_supervisor_07(self):
        NUM = 0

        async def foo1():
            nonlocal NUM
            NUM += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                NUM += 10
                await asyncio.sleep(10)
                NUM += 1000
                raise
            return 42

        async def foo2():
            nonlocal NUM
            NUM += 1
            await asyncio.sleep(2)
            NUM += 1000
            return 11

        async def runner():
            g = await supervisor.Supervisor.create()

            g.create_task(foo1())
            g.create_task(foo2())

            await asyncio.sleep(0.1)

            await g.cancel()

        r = self.loop.create_task(runner())
        await asyncio.sleep(0.5)
        r.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await r

        self.assertEqual(NUM, 12)

    async def test_supervisor_08(self):
        NUM = 0

        async def foo1():
            nonlocal NUM
            NUM += 1
            await asyncio.sleep(1)
            NUM += 1000
            return 42

        async def foo2():
            nonlocal NUM
            NUM += 1
            await asyncio.sleep(2)
            NUM += 1000
            return 11

        async def runner():
            g = await supervisor.Supervisor.create()

            g.create_task(foo1())
            g.create_task(foo2())

            await asyncio.sleep(0.1)

            await g.cancel()

        await runner()
        self.assertEqual(NUM, 2)

    async def test_supervisor_09(self):
        NUM = 0

        async def foo1():
            nonlocal NUM
            NUM += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                await asyncio.sleep(0.2)
                NUM += 10
                raise
            NUM += 1000
            return 42

        async def foo2():
            nonlocal NUM
            NUM += 1
            await asyncio.sleep(2)
            NUM += 1000
            return 11

        async def runner():
            g = await supervisor.Supervisor.create()

            g.create_task(foo1())
            g.create_task(foo2())

            await asyncio.sleep(0.1)

            await g.cancel()

        await runner()
        self.assertEqual(NUM, 12)
