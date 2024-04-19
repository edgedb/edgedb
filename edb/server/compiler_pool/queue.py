#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
import collections
import typing


W = typing.TypeVar('W')
W2 = typing.TypeVar('W2', contravariant=True)


class _AcquireCondition(typing.Protocol[W2]):

    def __call__(self, worker: W2) -> bool:
        pass


class WorkerQueue(typing.Generic[W]):

    loop: asyncio.AbstractEventLoop

    _waiters: typing.Deque[asyncio.Future[None]]
    _queue: typing.Deque[W]

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._loop = loop
        self._waiters = collections.deque()
        self._queue = collections.deque()

    async def acquire(
        self,
        *,
        condition: typing.Optional[_AcquireCondition[W]]=None,
        weighter=None,
    ) -> W:
        # There can be a race between a waiter scheduled for to wake up
        # and a worker being stolen (due to quota being enforced,
        # for example).  In which case the waiter might get finally
        # woken up with an empty queue -- hence we use a `while` loop here.
        attempts = 0
        while not self._queue:
            waiter = self._loop.create_future()

            attempts += 1
            if attempts > 1:
                # If the waiter was woken up only to discover that
                # it needs to wait again, we don't want it to lose
                # its place in the waiters queue.
                self._waiters.appendleft(waiter)
            else:
                # On the first attempt the waiter goes to the end
                # of the waiters queue.
                self._waiters.append(waiter)

            try:
                await waiter
            except Exception:
                if not waiter.done():
                    waiter.cancel()
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    # The waiter could be removed from self._waiters
                    # by a previous release() call.
                    pass
                if self._queue and not waiter.cancelled():
                    # We were woken up by release(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next_waiter()
                raise

        if len(self._queue) > 1:
            if condition is not None:
                for w in self._queue:
                    if condition(w):
                        self._queue.remove(w)
                        return w
            if weighter is not None:
                rv = self._queue[0]
                weight = weighter(rv)
                it = iter(self._queue)
                next(it)  # skip the first
                for w in it:
                    new_weight = weighter(w)
                    if new_weight > weight:
                        weight = new_weight
                        rv = w
                self._queue.remove(rv)
                return rv

        return self._queue.popleft()

    def release(self, worker: W, *, put_in_front: bool=True) -> None:
        if put_in_front:
            self._queue.appendleft(worker)
        else:
            self._queue.append(worker)
        self._wakeup_next_waiter()

    def qsize(self) -> int:
        return len(self._queue)

    def count_waiters(self) -> int:
        return len(self._waiters)

    def _wakeup_next_waiter(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break
