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

from edb.common import asyncutil
from edb.common import taskgroup
from edb.testbase import server as tb


class MyExc(Exception):
    pass


class TestTaskGroup(tb.TestCase):

    async def test_deferred_cancel_01(self):
        inner_asleep = False
        finished = False

        inner_event = asyncio.Event()
        inner_asleep = asyncio.Event()
        main_waiting = asyncio.Event()

        async def inner():
            nonlocal finished
            inner_asleep.set()
            await inner_event.wait()
            finished = True

        async def outer():
            await asyncutil.deferred_shield(inner())

        task = None

        async def main():
            nonlocal task
            task = asyncio.create_task(outer())

            await inner_asleep.wait()

            task.cancel()
            main_waiting.set()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('not cancelled!')

        # We use the task group so that errors get propagated
        # instead of us hanging in weird ways.
        async with taskgroup.TaskGroup() as g:
            tmain = g.create_task(main())
            await g.create_task(main_waiting.wait())

            # run the loop a bunch to make sure everything is quiescent
            for _ in range(20):
                await asyncio.sleep(0)

            self.assertFalse(task.done())
            self.assertFalse(tmain.done())

            inner_event.set()

            await tmain

            self.assertTrue(task.done())
            self.assertTrue(tmain.done())
            self.assertTrue(finished)
