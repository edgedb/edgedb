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
        # early check: if for any reason fut is already done, just return
        if fut.done():
            return fut.result()

        # by default, capture all signals configured in this controller
        if cancel_on is None:
            cancel_on = self._signals

        # The design here: we'll wait on a separate Future "waiter" for clean
        # cancellation. The waiter might be waken up by 3 different events:
        #   1. The given "fut" is done
        #   2. A signal is captured
        #   3. The "waiter" is cancelled by outer code.
        # For 2 and 3, we'll interrupt the given "fut" by a cancellation
        # carrying the reason as its message. Because our cancellation might be
        # intercepted in the "fut" code - e.g. a finally block or except block
        # that traps (and hopefully re-raises) the CancelledError or
        # BaseException, we need a loop here to ensure all the nested blocks
        # are exhaustively executed until the "fut" is done, meanwhile the
        # signals may keep hitting the "fut" code blocks, and "wait_for" is
        # ready to handle them properly, and chain the SignalError objects
        # with the __context__ or __cause__ attribute as they happen.
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
                    # Event 3: cancelled by outer code.
                    if not fut.done():
                        fut.cancel(msg=e)
                else:
                    # Event 2: "fut" is still running, which means that
                    # "waiter" was woken up by a signal.
                    if not fut.done():
                        assert signal is not None
                        fut.cancel(msg=SignalError(signal))

                # Event 1: "fut" is done - exit the loop naturally.

            finally:
                fut.remove_done_callback(cb)
                # In any case, the "waiter" is done at this time, it needs to
                # be removed from the signal callback chain, even if we still
                # need to wait for the signal in the next loop, with a new
                # "waiter" object.
                for signal in cancel_on:
                    self._discard_waiter(signal, waiter)

        # Now that the "fut" is done, let's check its result. It may end up in
        # 3 different scenarios, listed below inline:
        try:
            # 1. "fut" finished happily without interruption of signal or
            #    cancellation (event 1), just return or raise as it is.
            return fut.result()
        except asyncio.CancelledError as e:
            # For all other cases, it is expected to begin with a cancellation.
            # We need to look into the chain of exceptions for a sign of event
            # 2 or 3, so that we could recover the proper error objects later.
            ex = e
            while ex is not None:
                if ex.args and isinstance(
                    ex.args[0], (asyncio.CancelledError, SignalError)
                ):
                    break
                ex = ex.__context__
            else:
                # 2. We didn't find any clue, this likely means the chain was
                #    lost or broken in the Task code, let's just re-raise.
                raise

        # 3. The first CancelledError that carries event 2 or 3 is found, let's
        #    construct the proper error chain here based on the carriers.
        current = rv = ex.args[0].with_traceback(ex.__traceback__)
        ctx = ex.__context__
        while ctx is not None:
            if ctx.args and isinstance(ctx.args[0], asyncio.CancelledError):
                # Event 3: keep the traceback of the original cancellation
                current.__context__ = ctx.args[0]
            elif ctx.args and isinstance(ctx.args[0], SignalError):
                # Event 2: as SignalError was never directly raised by now,
                # it doesn't have a traceback yet. We're just borrowing the
                # traceback from its carrier CancelledError which reveals the
                # actual line of code where the signal was caught.
                current.__context__ = ctx.args[0].with_traceback(
                    ctx.__traceback__
                )
            else:
                # Preserve whatever else is in the chain.
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
