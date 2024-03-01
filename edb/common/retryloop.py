#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
    Callable,
    Optional,
    Tuple,
    Type,
)

import asyncio
import random
import time
import types


def const_backoff(delay: float) -> Callable[[int], float]:
    return lambda _: delay


def exp_backoff(
    *,
    factor: float = 0.1,
    jitter_scale: float = 0.001,
) -> Callable[[int], float]:
    def _f(i: int) -> float:
        delay: int = 2 ** i
        return delay * factor + random.randrange(100) * jitter_scale
    return _f


class RetryLoop:

    def __init__(
        self,
        *,
        backoff: Callable[[int], float] = const_backoff(0.5),
        timeout: float | None = None,
        iterations: int | None = None,
        ignore: Type[Exception] | Tuple[Type[Exception], ...] | None = None,
        wait_for: Type[Exception] | Tuple[Type[Exception], ...] | None = None,
        retry_cb: Callable[[Optional[BaseException]], None] | None = None,
    ) -> None:
        assert timeout is not None or iterations is not None

        self._iteration = 0
        self._backoff = backoff
        self._timeout = timeout
        self._max_iterations = iterations
        self._ignore = ignore
        self._wait_for = wait_for
        self._started_at = 0.0
        self._stop_request = False
        self._retry_cb = retry_cb

    def __aiter__(self) -> RetryLoop:
        return self

    async def __anext__(self) -> RetryIteration:
        if self._stop_request:
            raise StopAsyncIteration

        if self._started_at == 0:
            # First run
            self._started_at = time.monotonic()
        else:
            # Second or greater run -- delay before yielding
            delay = self._backoff(self._iteration)
            await asyncio.sleep(delay)

        self._iteration += 1

        return RetryIteration(self)


class RetryIteration:

    def __init__(self, loop: RetryLoop) -> None:
        self._loop = loop

    async def __aenter__(self) -> RetryIteration:
        return self

    async def __aexit__(
        self,
        et: Type[BaseException],
        e: BaseException,
        _tb: types.TracebackType,
    ) -> bool:
        elapsed = time.monotonic() - self._loop._started_at

        if self._loop._ignore is not None:
            # Mode 1: Try until we don't get errors matching `ignore`

            if et is None:
                self._loop._stop_request = True
                return False

            if not isinstance(e, self._loop._ignore):
                # Propagate, it's not the error we expected.
                return False

            if (
                self._loop._timeout is not None
                and elapsed > self._loop._timeout
            ):
                # Propagate -- we've run it enough times.
                return False

            if (
                self._loop._max_iterations is not None
                and self._loop._iteration >= self._loop._max_iterations
            ):
                return False

            if self._loop._retry_cb is not None:
                self._loop._retry_cb(e)

            # Ignore the exception until next run.
            return True

        else:
            # Mode 2: Try until we fail with an error matching `wait_for`

            assert self._loop._wait_for is not None

            if et is not None:
                if isinstance(e, self._loop._wait_for):
                    # We're done, we've got what we waited for.
                    self._loop._stop_request = True
                    return True
                else:
                    # Propagate, it's not the error we expected.
                    return False

            if (
                self._loop._timeout is not None
                and elapsed > self._loop._timeout
            ):
                raise TimeoutError(
                    f'exception matching {self._loop._wait_for!r} '
                    f'has not happened in {self._loop._timeout} seconds')

            if (
                self._loop._max_iterations is not None
                and self._loop._iteration >= self._loop._max_iterations
            ):
                raise TimeoutError(
                    f'exception matching {self._loop._wait_for!r} '
                    f'has not happened in {self._loop._max_iterations} '
                    f'iterations')

            # Ignore the exception until next run.
            return True
