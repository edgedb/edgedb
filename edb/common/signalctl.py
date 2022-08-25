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
import signal as mod_signal
import typing
import warnings


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


class SignalError(Exception):
    def __init__(self, signo):
        self.signo = signo

    def __str__(self):
        if isinstance(self.signo, mod_signal.Signals):
            return self.signo._name_
        else:
            return str(self.signo)


class SignalHandler:
    def __init__(self, callback, signals, controller):
        self._cancelled = False
        self._callback = callback
        self._signals = signals
        self._controller = controller
        for signal in signals:
            controller._register_waiter(signal, self)

    def done(self):
        return self._cancelled

    def cancelled(self):
        return self._cancelled

    def set_result(self, result):
        asyncio.get_running_loop().call_soon(self._callback, result)

    def cancel(self):
        self._cancelled = True
        for signal in self._signals:
            self._controller._discard_waiter(signal, self)


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
        handlers = [
            waiter
            for waiters in self._waiters.values()
            for waiter in waiters
            if isinstance(waiter, SignalHandler)
        ]
        for handler in handlers:
            handler.cancel()
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

        cancelled_by = None
        outer_cancelled_at_last = False

        # The design here: we'll wait on a separate Future "waiter" for clean
        # cancellation. The waiter might be woken up by 3 different events:
        #   1. The given "fut" is done
        #   2. A signal is captured
        #   3. The "waiter" is cancelled by outer code.
        # For 2, we'll cancel the given "fut" and record the signal in
        # cancelled_by as a __context__ chain to raise in the next step; for 3,
        # we cancel the given "fut" and propagate the CancelledError later.
        #
        # The complexity of this design is: because our cancellation might be
        # intercepted in the "fut" code - e.g. a finally block or except block
        # that traps (and hopefully re-raises) the CancelledError or
        # BaseException, we need a loop here to ensure all the nested blocks
        # are exhaustively executed until the "fut" is done, meanwhile the
        # signals may keep hitting the "fut" code blocks, and "wait_for" is
        # ready to handle them properly, and return all the SignalError objects
        # in a __context__ chain preserving the order as they happen.
        while not fut.done():
            waiter = self._loop.create_future()
            cb = functools.partial(_release_waiter, waiter)
            fut.add_done_callback(cb)

            for signal in cancel_on:
                self._register_waiter(signal, waiter)
            try:
                try:
                    signal = await waiter
                except asyncio.CancelledError:
                    # Event 3: cancelled by outer code.
                    if not fut.done():
                        fut.cancel()
                        outer_cancelled_at_last = True
                else:
                    # Event 2: "fut" is still running, which means that
                    # "waiter" was woken up by a signal.
                    if not fut.done():
                        assert signal is not None
                        fut.cancel()
                        err = SignalError(signal)
                        err.__context__ = cancelled_by
                        cancelled_by = err
                        outer_cancelled_at_last = False

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
            # 1. "fut" finished happily without raising errors (event 1), just
            #    return the result. Even if we've previously recorded signals
            #    (event 2) or cancellations (event 3), it's now handled by the
            #    user, and we shall simply dispose the recorded cancelled_by.
            return fut.result()

        except asyncio.CancelledError as ex:
            # 2. "fut" is cancelled - this usually means we caught a signal,
            #    but it could also be other reasons, see below.
            if cancelled_by is not None:
                # Event 2 happened at least once
                if outer_cancelled_at_last:
                    # If event 3 is the last event, the outer code is probably
                    # expecting a CancelledError, e.g. asyncio.wait_for().
                    # Therefore, we just raise it with signal errors attached.
                    ex.__context__ = cancelled_by
                    raise
                else:
                    # If event 2 is the last event, simply raise the grouped
                    # signal errors, attaching the CancelledError to reveal
                    # where the signals hit the user code. We cannot raise
                    # directly here because cancelled_by.__context__ may have
                    # previously-captured signal errors.
                    cancelled_by.__cause__ = ex
            else:
                # Neither event 2 nor 3 happened, the user code cancelled
                # itself, simply propagate the same error.
                raise

        except Exception as e:
            # 3. For any other errors, we just raise it with the signal errors
            #    attached as __context__ if event 2 happened.
            if cancelled_by is not None:
                e.__context__ = cancelled_by
            raise

        assert cancelled_by is not None
        raise cancelled_by

    def add_handler(self, callback, signals=None) -> SignalHandler:
        if signals is None:
            signals = self._signals
        return SignalHandler(callback, signals, self)

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
