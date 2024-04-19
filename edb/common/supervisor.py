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
from typing import Optional

import asyncio
import itertools


class Supervisor:

    def __init__(self, *, _name, _loop, _private):
        if _name is None:
            self._name = f'sup#{_name_counter()}'
        else:
            self._name = str(_name)

        self._loop = _loop
        self._unfinished_tasks = 0
        self._cancelled = False
        self._tasks = set()
        self._errors = []
        self._base_error = None
        self._on_completed_fut = None

    @classmethod
    async def create(cls, *, name: Optional[str] = None):
        loop = asyncio.get_running_loop()
        return cls(_loop=loop, _name=name, _private=True)

    def __repr__(self):
        msg = f'<Supervisor {self._name!r}'
        if self._tasks:
            msg += f' tasks:{len(self._tasks)}'
        if self._unfinished_tasks:
            msg += f' unfinished:{self._unfinished_tasks}'
        if self._errors:
            msg += f' errors:{len(self._errors)}'
        if self._cancelled:
            msg += ' cancelling'
        msg += '>'
        return msg

    def create_task(self, coro):
        if self._cancelled:
            raise RuntimeError(
                f'supervisor {self!r} has already been cancelled')

        task = self._loop.create_task(coro)
        task.add_done_callback(self._on_task_done)

        self._unfinished_tasks += 1
        self._tasks.add(task)

        return task

    async def cancel(self):
        self._cancel()

        if self._unfinished_tasks:
            was_cancelled = await self._wait()
            if was_cancelled:
                raise asyncio.CancelledError()

    async def wait(self):
        if self._unfinished_tasks:
            was_cancelled = await self._wait()
            if was_cancelled:
                raise asyncio.CancelledError()

        if self._base_error is not None:
            raise self._base_error

        if self._errors:
            # Exceptions are heavy objects that can have object
            # cycles (bad for GC); let's not keep a reference to
            # a bunch of them.
            errors = self._errors
            self._errors = []

            me = ExceptionGroup('unhandled errors in a Supervisor', errors)
            raise me from None

    async def _wait(self):
        was_cancelled = False

        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
        while self._unfinished_tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = self._loop.create_future()

            try:
                await self._on_completed_fut
            except asyncio.CancelledError:
                was_cancelled = True
                self._cancel()

            self._on_completed_fut = None

        assert self._unfinished_tasks == 0
        self._on_completed_fut = None  # no longer needed

        return was_cancelled

    def _on_task_done(self, task):
        self._unfinished_tasks -= 1

        assert self._unfinished_tasks >= 0

        if self._on_completed_fut is not None and not self._unfinished_tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(True)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        self._errors.append(exc)
        if self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        self._cancel()

    def _cancel(self):
        self._cancelled = True

        for t in self._tasks:
            if not t.done():
                t.cancel()

    def _is_base_error(self, exc):
        assert isinstance(exc, BaseException)
        return not isinstance(exc, Exception)


_name_counter = itertools.count(1).__next__
