#
# This source file is part of the EdgeDB open source project.
#
# Copyright EdgeDB Inc. and the EdgeDB authors.
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
import logging

from . import retryloop


logger = logging.getLogger("edb.server.asyncwatcher")


class AsyncWatcherProtocol(asyncio.Protocol):
    def __init__(
        self,
        watcher: AsyncWatcher,
    ) -> None:
        self._transport: Optional[asyncio.Transport] = None
        self._watcher = watcher

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore [assignment]
        self.request()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._watcher.incr_metrics_counter("watch-disconnect")
        self._watcher.on_connection_lost()

    def request(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class AsyncWatcher:
    def __init__(self) -> None:
        self._waiter: Optional[asyncio.Future] = None
        self._stop_waiter: Optional[asyncio.Future] = None
        self._protocol: Optional[AsyncWatcherProtocol] = None
        self._watching = False
        self._retry_attempt = 0
        self._backoff = retryloop.exp_backoff()

    async def start_watching(self) -> bool:
        if not self._watching:
            self._watching = True
            try:
                self._protocol = await self._start_watching()
                return True
            except BaseException:
                self.incr_metrics_counter("watch-start-err")
                self._watching = False
                raise
        return False

    async def retry_watching(self) -> None:
        self._retry_attempt += 1
        delay = self._backoff(self._retry_attempt)
        await asyncio.sleep(delay)
        try:
            await self.start_watching()
        except Exception:
            logger.warning(
                "%s failed to start watching, will retry.",
                type(self).__name__,
                exc_info=True,
            )
            asyncio.create_task(self.retry_watching())

    def stop_watching(self) -> None:
        self._watching = False
        protocol, self._protocol = self._protocol, None
        if protocol is not None:
            self._stop_waiter = asyncio.get_running_loop().create_future()
            protocol.close()

    async def wait_stopped_watching(self) -> None:
        if self._stop_waiter is not None:
            await self._stop_waiter

    def on_connection_lost(self) -> None:
        self._protocol = None
        if self._watching:
            self.stop_watching()
            asyncio.create_task(self.retry_watching())
        else:
            waiter, self._stop_waiter = self._stop_waiter, None
            if waiter is not None:
                waiter.set_result(None)

    def on_update(self, data: bytes) -> None:
        self._retry_attempt = 0
        self._on_update(data)

    def _on_update(self, data: bytes) -> None:
        raise NotImplementedError

    async def _start_watching(self) -> AsyncWatcherProtocol:
        raise NotImplementedError

    def consume_tokens(self, tokens: int) -> float:
        # For rate limit - tries to consume the given number of tokens, returns
        # non-zero values as seconds to wait if unsuccessful
        return 0

    def incr_metrics_counter(self, event: str, value: float = 1.0) -> None:
        pass
