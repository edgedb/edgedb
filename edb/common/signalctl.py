#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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
import functools
import typing
import warnings


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


class SignalError(Exception):
    def __init__(self, signo):
        self.signo = signo


class QueueWaiter(asyncio.Queue):
    def done(self):
        return False

    def set_result(self, result):
        self.put_nowait(result)


class SignalController:
    _registry: typing.Dict[
        asyncio.AbstractEventLoop,
        typing.Dict[int, typing.Set[SignalController]],
    ] = {}
    _waiters: typing.Dict[int, typing.Set[asyncio.Future]]

    def __init__(self, *signals):
        self._signals = signals
        self._loop = asyncio.get_running_loop()
        self._waiters = {}

    def __enter__(self):
        registry = self._registry.setdefault(self._loop, {})
        for signal in self._signals:
            controllers = registry.setdefault(signal, set())
            if not controllers:
                self._loop.add_signal_handler(
                    signal, self._signal_callback, signal
                )
            controllers.add(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._waiters:
            warnings.warn(
                "SignalController exited before wait_for() completed."
            )
        registry = self._registry[self._loop]
        for signal in self._signals:
            controllers = registry[signal]
            controllers.discard(self)
            if not controllers:
                del registry[signal]
                self._loop.remove_signal_handler(signal)
                if not registry:
                    del self._registry[self._loop]

    def _on_signal(self, signal):
        for waiter in self._waiters.get(signal, []):
            if not waiter.done():
                waiter.set_result(signal)

    def _register_waiter(self, signal, waiter):
        self._waiters.setdefault(signal, set()).add(waiter)

    def _discard_waiter(self, signal, waiter):
        waiters = self._waiters.get(signal)
        if waiters:
            waiters.discard(waiter)
            if not waiters:
                del self._waiters[signal]

    async def wait_for(self, fut, *, cancel_on=None):
        fut = asyncio.ensure_future(fut)
        if fut.done():
            return fut.result()

        if cancel_on is None:
            cancel_on = self._signals
        while not fut.done():
            waiter = self._loop.create_future()
            cb = functools.partial(_release_waiter, waiter)
            fut.add_done_callback(cb)

            for signal in cancel_on:
                self._register_waiter(signal, waiter)
            try:
                try:
                    signal = await waiter
                except asyncio.CancelledError as e:
                    if not fut.done():
                        fut.cancel(msg=e)
                else:
                    if not fut.done():
                        fut.cancel(msg=SignalError(signal))
            finally:
                fut.remove_done_callback(cb)
                for signal in cancel_on:
                    self._discard_waiter(signal, waiter)

        try:
            return fut.result()
        except asyncio.CancelledError as e:
            ex = e
            while ex is not None:
                if ex.args and isinstance(
                    ex.args[0], (asyncio.CancelledError, SignalError)
                ):
                    break
                ex = ex.__context__
            else:
                raise
        current = rv = ex.args[0].with_traceback(ex.__traceback__)
        ctx = ex.__context__
        while ctx is not None:
            if ctx.args and isinstance(ctx.args[0], asyncio.CancelledError):
                current.__context__ = ctx.args[0]
            elif ctx.args and isinstance(ctx.args[0], SignalError):
                current.__context__ = ctx.args[0].with_traceback(
                    ctx.__traceback__
                )
            else:
                current.__context__ = ctx
            current = current.__context__
            ctx = ctx.__context__
        raise rv

    async def wait_for_signals(self):
        waiter = QueueWaiter()
        for signal in self._signals:
            self._register_waiter(signal, waiter)
        try:
            while True:
                yield await waiter.get()
        finally:
            for signal in self._signals:
                self._discard_waiter(signal, waiter)

    @classmethod
    def _signal_callback(cls, signal):
        registry = cls._registry.get(asyncio.get_running_loop())
        if not registry:
            return
        controllers = registry.get(signal)
        if not controllers:
            return
        for controller in controllers:
            controller._on_signal(signal)
