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
import edb.server._rust_native._conn_pool as _rust
import asyncio
import time
import typing
import dataclasses
import pickle

from . import config
from .config import logger
from edb.server import rust_async_channel

# Connections must be hashable because we use them to reverse-lookup
# an internal ID.
C = typing.TypeVar("C", bound=typing.Hashable)

CP1 = typing.TypeVar('CP1', covariant=True)
CP2 = typing.TypeVar('CP2', contravariant=True)


class Connector(typing.Protocol[CP1]):

    def __call__(self, dbname: str) -> typing.Awaitable[CP1]:
        pass


class Disconnector(typing.Protocol[CP2]):

    def __call__(self, conn: CP2) -> typing.Awaitable[None]:
        pass


@dataclasses.dataclass
class BlockSnapshot:
    dbname: str
    nwaiters_avg: int
    nconns: int
    npending: int
    nwaiters: int
    quota: int


@dataclasses.dataclass
class SnapshotLog:
    timestamp: float
    event: str
    dbname: str
    value: int


@dataclasses.dataclass
class Snapshot:
    timestamp: float
    capacity: int
    blocks: typing.List[BlockSnapshot]
    log: typing.List[SnapshotLog]

    failed_connects: int
    failed_disconnects: int
    successful_connects: int
    successful_disconnects: int


class StatsCollector(typing.Protocol):

    def __call__(self, stats: Snapshot) -> None:
        pass


class Pool(typing.Generic[C]):
    _pool: _rust.ConnPool
    _next_conn_id: int
    _failed_connects: int
    _failed_disconnects: int
    _successful_connects: int
    _successful_disconnects: int
    _cur_capacity: int
    _max_capacity: int
    _task: typing.Optional[asyncio.Task[None]]
    _acquires: dict[int, asyncio.Future[int]]
    _prunes: dict[int, asyncio.Future[None]]
    _conns: dict[int, C]
    _errors: dict[int, BaseException]
    _conns_held: dict[C, int]
    _loop: asyncio.AbstractEventLoop
    _counts: typing.Any
    _stats_collector: typing.Optional[StatsCollector]

    def __init__(
        self,
        *,
        connect: Connector[C],
        disconnect: Disconnector[C],
        max_capacity: int,
        stats_collector: typing.Optional[StatsCollector] = None,
        min_idle_time_before_gc: float = config.MIN_IDLE_TIME_BEFORE_GC,
    ) -> None:
        # Re-load the logger if it's been mocked for testing
        global logger
        logger = config.logger

        logger.info(
            f'Creating a connection pool with max_capacity={max_capacity}'
        )
        self._connect = connect
        self._disconnect = disconnect
        self._pool = _rust.ConnPool(
            max_capacity, min_idle_time_before_gc, config.STATS_COLLECT_INTERVAL
        )
        self._max_capacity = max_capacity
        self._cur_capacity = 0
        self._next_conn_id = 0
        self._acquires = {}
        self._conns = {}
        self._errors = {}
        self._conns_held = {}
        self._prunes = {}

        self._loop = asyncio.get_running_loop()
        self._channel = rust_async_channel.RustAsyncChannel(
            self._pool,
            self._process_message,
        )

        self._task = self._loop.create_task(self._boot(self._channel))

        self._failed_connects = 0
        self._failed_disconnects = 0
        self._successful_connects = 0
        self._successful_disconnects = 0

        self._counts = None
        self._stats_collector = stats_collector
        if stats_collector:
            stats_collector(self._build_snapshot(now=time.monotonic()))

        pass

    def __del__(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None

    async def close(self) -> None:
        if self._task:
            # Cancel the currently-executing futures
            for acq in self._acquires.values():
                acq.set_exception(asyncio.CancelledError())
            for prune in self._prunes.values():
                prune.set_exception(asyncio.CancelledError())
            logger.info("Closing connection pool...")
            task = self._task
            self._task = None
            task.cancel()
            try:
                await task
            except asyncio.exceptions.CancelledError:
                pass
            self._pool = None
            logger.info("Closed connection pool")

    async def _boot(
        self,
        channel: rust_async_channel.RustAsyncChannel,
    ) -> None:
        logger.info("Python-side connection pool booted")
        try:
            await channel.run()
        finally:
            channel.close()

    def _try_read(self) -> None:
        if self._channel:
            self._channel.read_hint()

    def _process_message(self, msg: typing.Any) -> None:
        # If we're closing, don't dispatch any operations
        if not self._task:
            return
        if msg[0] == 0:
            if f := self._acquires.pop(msg[1], None):
                f.set_result(msg[2])
            else:
                logger.warning(f"Duplicate result for acquire {msg[1]}")
        elif msg[0] == 1:
            self._loop.create_task(self._perform_connect(msg[1], msg[2]))
        elif msg[0] == 2:
            self._loop.create_task(self._perform_disconnect(msg[1]))
        elif msg[0] == 3:
            self._loop.create_task(self._perform_reconnect(msg[1], msg[2]))
        elif msg[0] == 4:
            self._loop.create_task(self._perform_prune(msg[1]))
        elif msg[0] == 5:
            # Note that we might end up with duplicated messages at shutdown
            error = self._errors.pop(msg[2], None)
            if error:
                if f := self._acquires.pop(msg[1], None):
                    f.set_exception(error)
                else:
                    logger.warn(f"Duplicate exception for acquire {msg[1]}")
        elif msg[0] == 6:
            # Pickled metrics
            self._counts = pickle.loads(msg[1])
            if self._stats_collector:
                self._stats_collector(
                    self._build_snapshot(now=time.monotonic())
                )
        else:
            logger.critical(f'Unexpected message: {msg}')

    async def _perform_connect(self, id: int, db: str) -> None:
        self._cur_capacity += 1
        try:
            self._conns[id] = await self._connect(db)
            self._successful_connects += 1
            if self._pool:
                self._pool._completed(id)
        except Exception as e:
            self._errors[id] = e
            if self._pool:
                self._pool._failed(id, e)

    async def _perform_disconnect(self, id: int) -> None:
        try:
            conn = self._conns.pop(id)
            await self._disconnect(conn)
            self._successful_disconnects += 1
            self._cur_capacity -= 1
            if self._pool:
                self._pool._completed(id)
        except Exception as e:
            self._cur_capacity -= 1
            if self._pool:
                self._pool._failed(id, e)

    async def _perform_reconnect(self, id: int, db: str) -> None:
        try:
            # Note that we cannot hold this connection here as there is an
            # implicit expectation that the connection will GC after disconnect
            # but before reconnect.
            conn = self._conns.pop(id)
            await self._disconnect(conn)
            self._successful_disconnects += 1
            try:
                self._conns[id] = await self._connect(db)
                self._successful_connects += 1
                if self._pool:
                    self._pool._completed(id)
            except Exception as e:
                self._errors[id] = e
                if self._pool:
                    self._pool._failed(id, e)

        except Exception as e:
            del self._conns[id]
            self._cur_capacity -= 1
            if self._pool:
                self._pool._failed(id, e)

    async def _perform_prune(self, id: int) -> None:
        self._prunes[id].set_result(None)

    async def acquire(self, dbname: str) -> C:
        """Acquire a connection from the database. This connection must be
        released."""
        if not self._task:
            raise asyncio.CancelledError()
        for i in range(config.CONNECT_FAILURE_RETRIES + 1):
            id = self._next_conn_id
            self._next_conn_id += 1
            acquire: asyncio.Future[int] = asyncio.Future()
            self._acquires[id] = acquire
            self._pool._acquire(id, dbname)
            self._try_read()
            # This may throw!
            try:
                conn = await acquire
                c = self._conns[conn]
                self._conns_held[c] = id
                return c
            except Exception as e:
                # 3D000 - INVALID CATALOG NAME, database does not exist
                # Skip retry and propagate the error immediately
                if getattr(e, 'fields', {}).get('C') == '3D000':
                    raise

                # Allow the final exception to escape
                if i == config.CONNECT_FAILURE_RETRIES:
                    logger.exception(
                        'Failed to acquire connection, will not '
                        f'retry {dbname} ({self._cur_capacity}'
                        'active)'
                    )
                    raise
                logger.exception(
                    'Failed to acquire connection, will retry: '
                    f'{dbname} ({self._cur_capacity} active)'
                )
        raise AssertionError("Unreachable end of loop")

    def release(self, dbname: str, conn: C, discard: bool = False) -> None:
        """Releases a connection back into the pool, discarding or returning it
        in the background."""
        id = self._conns_held.pop(conn)
        if discard:
            self._pool._discard(id)
        else:
            self._pool._release(id)
        self._try_read()

    async def prune_inactive_connections(self, dbname: str) -> None:
        if not self._task:
            raise asyncio.CancelledError()
        id = self._next_conn_id
        self._next_conn_id += 1
        self._prunes[id] = asyncio.Future()
        self._pool._prune(id, dbname)
        await self._prunes[id]
        del self._prunes[id]

    async def prune_all_connections(self) -> None:
        # Brutally close all connections. This is used by HA failover.
        coros = []
        for conn in self._conns.values():
            coros.append(self._disconnect(conn))
        await asyncio.gather(*coros, return_exceptions=True)

    @property
    def active_conns(self) -> int:
        return len(self._conns_held)

    def iterate_connections(self) -> typing.Iterator[C]:
        for conn in self._conns.values():
            yield conn

    def _build_snapshot(self, *, now: float) -> Snapshot:
        blocks: list[BlockSnapshot] = []
        if self._counts:
            block_stats = self._counts['blocks']
            for dbname, stats in block_stats.items():
                v = stats['value']
                block_snapshot = BlockSnapshot(
                    dbname=dbname,
                    nconns=v[_rust.METRIC_ACTIVE],
                    nwaiters_avg=v[_rust.METRIC_WAITING],
                    npending=v[_rust.METRIC_CONNECTING]
                    + v[_rust.METRIC_RECONNECTING],
                    nwaiters=v[_rust.METRIC_WAITING],
                    quota=stats['target'],
                )
                blocks.append(block_snapshot)
            pass

        return Snapshot(
            timestamp=now,
            blocks=blocks,
            capacity=self._cur_capacity,
            log=[],
            failed_connects=self._failed_connects,
            failed_disconnects=self._failed_disconnects,
            successful_connects=self._successful_connects,
            successful_disconnects=self._successful_disconnects,
        )

    @property
    def max_capacity(self) -> int:
        return self._max_capacity

    @property
    def current_capacity(self) -> int:
        return self._cur_capacity

    @property
    def failed_connects(self) -> int:
        return self._failed_connects

    @property
    def failed_disconnects(self) -> int:
        return self._failed_disconnects
