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

try:
    import async_solipsism
except ImportError:
    async_solipsism = None  # type: ignore


def with_fake_event_loop(f):
    # async_solpsism creates an event loop with, among other things,
    # a totally fake clock.
    def new(*args, **kwargs):
        loop = async_solipsism.EventLoop()
        try:
            loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()

    return new


@unittest.skipIf(async_solipsism is None, 'async_solipsism is missing')
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
