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
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    overload,
    Self,
    TypeVar,
    Type,
)

import asyncio
import inspect
import warnings


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


_Owner = TypeVar("_Owner")
HandlerFunction = Callable[[], Awaitable[None]]
HandlerMethod = Callable[[Any], Awaitable[None]]


class ExclusiveTask:
    """Manages to run a repeatable task once at a time."""

    _handler: HandlerFunction
    _task: asyncio.Task | None
    _scheduled: bool
    _stop_requested: bool

    def __init__(self, handler: HandlerFunction) -> None:
        self._handler = handler
        self._task = None
        self._scheduled = False
        self._stop_requested = False

    @property
    def scheduled(self) -> bool:
        return self._scheduled

    async def _run(self) -> None:
        if self._scheduled and not self._stop_requested:
            self._scheduled = False
        else:
            return
        try:
            await self._handler()
        finally:
            if self._scheduled and not self._stop_requested:
                self._task = asyncio.create_task(self._run())
            else:
                self._task = None

    def schedule(self) -> None:
        """Schedule to run the task as soon as possible.

        If already scheduled, nothing happens; it won't queue up.

        If the task is already running, it will be scheduled to run again as
        soon as the running task is done.
        """
        if not self._stop_requested:
            self._scheduled = True
            if self._task is None:
                self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel scheduled task and wait for the running one to finish.

        After an ExclusiveTask is stopped, no more new schedules are allowed.
        Note: "cancel scheduled task" only means setting self._scheduled to
        False; if an asyncio task is scheduled, stop() will still wait for it.
        """
        self._scheduled = False
        self._stop_requested = True
        if self._task is not None:
            await self._task


class ExclusiveTaskProperty:
    _method: HandlerMethod
    _name: str | None

    def __init__(
        self, method: HandlerMethod, *, slot: str | None = None
    ) -> None:
        self._method = method
        self._name = slot

    def __set_name__(self, owner: Type[_Owner], name: str) -> None:
        if (slots := getattr(owner, "__slots__", None)) is not None:
            if self._name is None:
                raise TypeError("missing slot in @exclusive_task()")
            if self._name not in slots:
                raise TypeError(
                    f"slot {self._name!r} must be defined in __slots__"
                )

        if self._name is None:
            self._name = name

    @overload
    def __get__(self, instance: None, owner: Type[_Owner]) -> Self: ...

    @overload
    def __get__(
        self, instance: _Owner, owner: Type[_Owner]
    ) -> ExclusiveTask: ...

    def __get__(
        self, instance: _Owner | None, owner: Type[_Owner]
    ) -> ExclusiveTask | Self:
        # getattr on the class
        if instance is None:
            return self

        assert self._name is not None

        # getattr on an object with __dict__
        if (d := getattr(instance, "__dict__", None)) is not None:
            if rv := d.get(self._name, None):
                return rv
            rv = ExclusiveTask(self._method.__get__(instance, owner))
            d[self._name] = rv
            return rv

        # getattr on an object with __slots__
        else:
            if rv := getattr(instance, self._name, None):
                return rv
            rv = ExclusiveTask(self._method.__get__(instance, owner))
            setattr(instance, self._name, rv)
            return rv


ExclusiveTaskDecorator = Callable[
    [HandlerFunction | HandlerMethod], ExclusiveTask | ExclusiveTaskProperty
]


def _exclusive_task(
    handler: HandlerFunction | HandlerMethod, *, slot: str | None
) -> ExclusiveTask | ExclusiveTaskProperty:
    sig = inspect.signature(handler)
    params = list(sig.parameters.values())
    if len(params) == 0:
        handler = cast(HandlerFunction, handler)
        if slot is not None:
            warnings.warn(
                "slot is specified but unused in @exclusive_task()",
                stacklevel=2,
            )
        return ExclusiveTask(handler)
    elif len(params) == 1 and params[0].kind in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        handler = cast(HandlerMethod, handler)
        return ExclusiveTaskProperty(handler, slot=slot)
    else:
        raise TypeError("bad signature")


@overload
def exclusive_task(handler: HandlerFunction) -> ExclusiveTask: ...


@overload
def exclusive_task(
    handler: HandlerMethod, *, slot: str | None = None
) -> ExclusiveTaskProperty: ...


@overload
def exclusive_task(*, slot: str | None = None) -> ExclusiveTaskDecorator: ...


def exclusive_task(
    handler: HandlerFunction | HandlerMethod | None = None,
    *,
    slot: str | None = None,
) -> ExclusiveTask | ExclusiveTaskProperty | ExclusiveTaskDecorator:
    """Convert an async function into an ExclusiveTask.

    This decorator can be applied to either top-level functions or methods
    in a class. In the latter case, the exclusiveness is bound to each object
    of the owning class. If the owning class defines __slots__, you must also
    define an extra slot to store the exclusive state and tell exclusive_task()
    by providing the `slot` argument.
    """
    if handler is None:

        def decorator(
            handler: HandlerFunction | HandlerMethod,
        ) -> ExclusiveTask | ExclusiveTaskProperty:
            return _exclusive_task(handler, slot=slot)

        return decorator

    return _exclusive_task(handler, slot=slot)
