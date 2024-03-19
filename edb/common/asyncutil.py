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


from __future__ import annotations
from typing import Callable, TypeVar, Awaitable

import asyncio


_T = TypeVar('_T')


async def deferred_shield(arg: Awaitable[_T]) -> _T:
    '''Wait for a future, deferring cancellation until it is complete.

    If you do
        await deferred_shield(something())

    it is approximately equivalent to
        await something()

    except that if the coroutine containing it is cancelled,
    something() is protected from cancellation, and *additionally*
    CancelledError is not raised in the caller until something()
    completes.

    This can be useful if something() contains something that
    shouldn't be interrupted but also can't be safely left running
    asynchronously.
    '''
    task = asyncio.ensure_future(arg)

    ex = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as cex:
            if ex is not None:
                cex.__context__ = ex
            ex = cex
        except Exception:
            if ex:
                raise ex from None
            raise

    if ex:
        raise ex
    return task.result()


async def debounce(
    input: Callable[[], Awaitable[_T]],
    output: Callable[[list[_T]], Awaitable[None]],
    *,
    max_wait: float,
    delay_amt: float,
    max_batch_size: int,
) -> None:
    '''Debounce and batch async events.

    Loops forever unless an operation fails, so should probably be run
    from a task.

    The basic algorithm is that if an event comes in less than
    `delay_amt` since the previous one, then instead of sending it
    immediately, we wait an additional `delay_amt` from then. If we are
    already waiting, any message also extends the wait, up to
    `max_wait`.

    Also, cap the maximum batch size to `max_batch_size`.
    '''
    # I think the algorithm reads more clearly with the params
    # capitalized as constants, though we don't want them like that in
    # the argument list, so reassign them.
    MAX_WAIT, DELAY_AMT, MAX_BATCH_SIZE = max_wait, delay_amt, max_batch_size

    loop = asyncio.get_running_loop()

    batch = []
    last_signal = -MAX_WAIT
    target_time = None

    while True:
        try:
            if target_time is None:
                v = await input()
            else:
                async with asyncio.timeout_at(target_time):
                    v = await input()
        except TimeoutError:
            t = loop.time()
        else:
            batch.append(v)

            t = loop.time()

            # If we aren't current waiting, and we got a
            # notification recently, arrange to wait some before
            # sending it.
            if (
                target_time is None
                and t - last_signal < DELAY_AMT
            ):
                target_time = t + DELAY_AMT
            # If we were already waiting, wait a little longer, though
            # not longer than MAX_WAIT.
            elif (
                target_time is not None
            ):
                target_time = min(
                    max(t + DELAY_AMT, target_time),
                    last_signal + MAX_WAIT,
                )

        # Skip sending the event if we need to wait longer.
        if (
            target_time is not None
            and t < target_time
            and len(batch) < MAX_BATCH_SIZE
        ):
            continue

        await output(batch)
        batch = []
        last_signal = t
        target_time = None
