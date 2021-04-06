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

import logging
import typing

import asyncio
import collections
import dataclasses
import time

from . import rolavg


MIN_CONN_TIME_THRESHOLD = 0.01
MIN_QUERY_TIME_THRESHOLD = 0.001
MIN_LOG_TIME_THRESHOLD = 1
CONNECT_FAILURE_RETRIES = 3

logger = logging.getLogger("edb.server")

CP1 = typing.TypeVar('CP1', covariant=True)
CP2 = typing.TypeVar('CP2', contravariant=True)
C = typing.TypeVar('C')


class Connector(typing.Protocol[CP1]):

    def __call__(self, dbname: str) -> typing.Awaitable[CP1]:
        pass


class Disconnector(typing.Protocol[CP2]):

    def __call__(self, conn: CP2) -> typing.Awaitable[None]:
        pass


class StatsCollector(typing.Protocol):

    def __call__(self, stats: Snapshot) -> None:
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


@dataclasses.dataclass
class ConnectionState:
    in_use_since: float = 0
    in_use: bool = False


class Block(typing.Generic[C]):

    loop: asyncio.AbstractEventLoop
    dbname: str
    conns: typing.Dict[C, ConnectionState]
    quota: int
    pending_conns: int
    last_connect_timestamp: float

    conn_acquired_num: int
    conn_waiters_num: int
    conn_waiters: typing.Deque[asyncio.Future[None]]
    conn_queue: typing.Deque[C]
    connect_failures_num: int

    querytime_avg: rolavg.RollingAverage
    nwaiters_avg: rolavg.RollingAverage

    _cached_calibrated_demand: float

    _is_log_batching: bool
    _last_log_timestamp: float
    _log_events: typing.Dict[str, int]

    def __init__(
        self,
        dbname: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.dbname = dbname
        self.conns = {}
        self.quota = 1
        self.pending_conns = 0
        self.last_connect_timestamp = 0

        self.loop = loop

        self.conn_acquired_num = 0
        self.conn_waiters_num = 0
        self.conn_waiters = collections.deque()
        self.conn_queue = collections.deque()
        self.connect_failures_num = 0

        self.querytime_avg = rolavg.RollingAverage(history_size=20)
        self.nwaiters_avg = rolavg.RollingAverage(history_size=3)

        self._is_log_batching = False
        self._last_log_timestamp = 0
        self._log_events = {}

    def count_conns(self) -> int:
        return len(self.conns) + self.pending_conns

    def count_waiters(self) -> int:
        return self.conn_waiters_num

    def count_queued_conns(self) -> int:
        return len(self.conn_queue)

    def count_pending_conns(self) -> int:
        return self.pending_conns

    def count_conns_over_quota(self) -> int:
        return max(self.count_conns() - self.quota, 0)

    def count_approx_available_conns(self) -> int:
        # It's approximate because there might be a race when a connection
        # is being returned to the pool but not yet acquired by a waiter,
        # in which case the number isn't going to be accurate.
        return max(
            self.count_conns() -
            self.conn_acquired_num -
            self.conn_waiters_num,
            0
        )

    def inc_acquire_counter(self) -> None:
        self.conn_acquired_num += 1

    def dec_acquire_counter(self) -> None:
        self.conn_acquired_num -= 1

    def try_steal(self) -> typing.Optional[C]:
        if self.conn_queue:
            conn = self.conn_queue.pop()
            assert conn in self.conns
            return conn
        return None

    async def acquire(self) -> C:
        # There can be a race between a waiter scheduled for to wake up
        # and a connection being stolen (due to quota being enforced,
        # for example).  In which case the waiter might get finally
        # woken up with an empty queue -- hence we use a `while` loop here.
        self.conn_waiters_num += 1
        try:
            attempts = 0
            while not self.conn_queue:
                waiter = self.loop.create_future()

                attempts += 1
                if attempts > 1:
                    # If the waiter was woken up only to discover that
                    # it needs to wait again, we don't want it to lose
                    # its place in the waiters queue.
                    self.conn_waiters.appendleft(waiter)
                else:
                    # On the first attempt the waiter goes to the end
                    # of the waiters queue.
                    self.conn_waiters.append(waiter)

                try:
                    await waiter
                except Exception:
                    if not waiter.done():
                        waiter.cancel()
                    try:
                        self.conn_waiters.remove(waiter)
                    except ValueError:
                        # The waiter could be removed from self.conn_waiters
                        # by a previous release() call.
                        pass
                    if self.conn_queue and not waiter.cancelled():
                        # We were woken up by release(), but can't take
                        # the call.  Wake up the next in line.
                        self._wakeup_next_waiter()
                    raise

            return self.conn_queue.popleft()
        finally:
            self.conn_waiters_num -= 1

    def release(self, conn: C) -> None:
        self.conn_queue.append(conn)
        self._wakeup_next_waiter()

    def abort_waiters(self, e: Exception) -> None:
        while self.conn_waiters:
            waiter = self.conn_waiters.popleft()
            if not waiter.done():
                waiter.set_exception(e)

    def _wakeup_next_waiter(self) -> None:
        while self.conn_waiters:
            waiter = self.conn_waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

    def log_connection(self, event: str, timestamp: float = 0) -> None:
        if not timestamp:
            timestamp = time.monotonic()

        # Add to the backlog if we're in batching, regardless of the time
        if self._is_log_batching:
            self._log_events[event] = self._log_events.setdefault(event, 0) + 1

        # Time check only if we're not in batching
        elif timestamp - self._last_log_timestamp > MIN_LOG_TIME_THRESHOLD:
            logger.info(
                "Connection %s to backend database: %s", event, self.dbname
            )
            self._last_log_timestamp = timestamp

        # Start batching if logging is too frequent, add timer only once here
        else:
            self._is_log_batching = True
            self._log_events = {event: 1}
            self.loop.call_later(
                MIN_LOG_TIME_THRESHOLD, self._log_batched_conns,
            )

    def _log_batched_conns(self) -> None:
        logger.info(
            "Backend connections to database %s: %s "
            "in at least the last %.1f seconds.",
            self.dbname,
            ', '.join(
                f'{num} were {event}'
                for event, num in self._log_events.items()
            ),
            MIN_LOG_TIME_THRESHOLD,
        )
        self._is_log_batching = False
        self._last_log_timestamp = time.monotonic()


class BasePool(typing.Generic[C]):

    _connect_cb: Connector[C]
    _disconnect_cb: Disconnector[C]
    _stats_cb: typing.Optional[StatsCollector]

    _max_capacity: int
    _cur_capacity: int

    _loop: typing.Optional[asyncio.AbstractEventLoop]
    _current_snapshot: typing.Optional[Snapshot]

    _blocks: collections.OrderedDict[str, Block[C]]

    _is_starving: bool

    _failed_connects: int
    _failed_disconnects: int
    _successful_connects: int
    _successful_disconnects: int

    _conntime_avg: rolavg.RollingAverage

    def __init__(
        self,
        *,
        connect: Connector[C],
        disconnect: Disconnector[C],
        max_capacity: int,
        stats_collector: typing.Optional[StatsCollector]=None,
    ) -> None:
        self._connect_cb = connect
        self._disconnect_cb = disconnect
        self._stats_cb = stats_collector

        self._max_capacity = max_capacity
        self._cur_capacity = 0

        self._loop = None
        self._current_snapshot = None

        self._blocks = collections.OrderedDict()
        self._is_starving = False

        self._failed_connects = 0
        self._failed_disconnects = 0
        self._successful_connects = 0
        self._successful_disconnects = 0

        self._conntime_avg = rolavg.RollingAverage(history_size=10)

    @property
    def max_capacity(self) -> int:
        return self._max_capacity

    @property
    def failed_connects(self) -> int:
        return self._failed_connects

    @property
    def failed_disconnects(self) -> int:
        return self._failed_disconnects

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    def _capture_snapshot(self, *, now: float) -> None:
        if self._stats_cb is None:
            return None

        assert self._current_snapshot is None

        bstats: typing.List[BlockSnapshot] = []
        for block in self._blocks.values():
            bstats.append(
                BlockSnapshot(
                    dbname=block.dbname,
                    nwaiters_avg=round(block.nwaiters_avg.avg()),
                    nconns=len(block.conns),
                    npending=block.count_pending_conns(),
                    nwaiters=block.count_waiters(),
                    quota=block.quota,
                )
            )

        bstats.sort(key=lambda b: b.dbname)

        self._current_snapshot = Snapshot(
            timestamp=now,
            blocks=bstats,
            capacity=self._cur_capacity,
            log=[],

            failed_connects=self._failed_connects,
            failed_disconnects=self._failed_disconnects,
            successful_connects=self._successful_connects,
            successful_disconnects=self._successful_disconnects,
        )

    def _report_snapshot(self) -> None:
        if self._stats_cb is None:
            return
        assert self._current_snapshot is not None
        self._stats_cb(self._current_snapshot)
        self._current_snapshot = None

    def _log_to_snapshot(
        self,
        *,
        dbname: str,
        event: str,
        value: int=0,
        now: float=0,
    ) -> None:
        if self._stats_cb is None:
            return
        if now == 0:
            now = time.monotonic()
        assert self._current_snapshot is not None
        self._current_snapshot.log.append(
            SnapshotLog(
                timestamp=now,
                dbname=dbname,
                event=event,
                value=value,
            )
        )

    def _new_block(self, dbname: str) -> Block[C]:
        assert dbname not in self._blocks
        block: Block[C] = Block(dbname, self._get_loop())
        self._blocks[dbname] = block
        block.quota = 1
        if self._is_starving:
            self._blocks.move_to_end(dbname, last=False)
        return block

    def _drop_block(self, block: Block[C]) -> None:
        assert not block.count_waiters()
        assert not block.count_conns()
        assert not block.quota
        self._blocks.pop(block.dbname)

    def _get_block(self, dbname: str) -> Block[C]:
        block = self._blocks.get(dbname)
        if block is None:
            block = self._new_block(dbname)
        return block

    async def _connect(
        self, block: Block[C], started_at: float, event: str
    ) -> None:
        logger.debug(
            "Establishing new connection to backend database: %s", block.dbname
        )
        try:
            conn = await self._connect_cb(block.dbname)
        except Exception as e:
            self._failed_connects += 1
            self._cur_capacity -= 1

            logger.error(
                "Failed to establish a new connection to backend database: %s",
                block.dbname,
                exc_info=True,
            )
            block.connect_failures_num += 1

            if getattr(e, 'fields', {}).get('C') == '3D000':
                # 3D000 - INVALID CATALOG NAME, database does not exist
                # Skip retry and propagate the error immediately
                if block.connect_failures_num <= CONNECT_FAILURE_RETRIES:
                    block.connect_failures_num = CONNECT_FAILURE_RETRIES + 1

            if block.connect_failures_num > CONNECT_FAILURE_RETRIES:
                # Abort all waiters on this block and propagate the error, as
                # we don't have a mapping between waiters and _connect() tasks
                block.abort_waiters(e)
            else:
                # We must retry immediately here (without sleeping), or _tick()
                # will jump in and schedule more retries than what we expected.
                self._schedule_new_conn(block, event)
            return
        else:
            # reset the failure counter if we got the connection back
            block.connect_failures_num = 0
        finally:
            ended_at = time.monotonic()
            self._conntime_avg.add(ended_at - started_at)
            block.pending_conns -= 1
        self._successful_connects += 1
        block.conns[conn] = ConnectionState()
        block.last_connect_timestamp = ended_at

        # Release the connection to block waiters.
        block.release(conn)
        block.log_connection(event, ended_at)

    async def _disconnect(self, conn: C, block: Block[C]) -> None:
        logger.debug(
            "Discarding a connection to backend database: %s", block.dbname
        )
        try:
            await self._disconnect_cb(conn)
        except Exception:
            self._failed_disconnects += 1
            raise
        else:
            self._successful_disconnects += 1
        finally:
            self._cur_capacity -= 1

    async def _transfer(
        self,
        from_block: Block[C],
        from_conn: C,
        to_block: Block[C],
        started_at: float,
    ) -> None:
        self._log_to_snapshot(dbname=from_block.dbname, event='transfer-from')
        await self._disconnect(from_conn, from_block)
        from_block.log_connection('transferred out')
        self._cur_capacity += 1
        await self._connect(to_block, started_at, 'transferred in')

    def _schedule_transfer(
        self,
        from_block: Block[C],
        from_conn: C,
        to_block: Block[C],
    ) -> None:
        started_at = time.monotonic()
        assert not from_block.conns[from_conn].in_use
        from_block.conns.pop(from_conn)
        to_block.pending_conns += 1
        if self._is_starving:
            self._blocks.move_to_end(to_block.dbname, last=True)
            self._blocks.move_to_end(from_block.dbname, last=True)
        self._get_loop().create_task(
            self._transfer(from_block, from_conn, to_block, started_at))

    def _schedule_new_conn(self, block: Block[C],
                           event: str = 'established') -> None:
        started_at = time.monotonic()
        self._cur_capacity += 1
        block.pending_conns += 1
        if self._is_starving:
            self._blocks.move_to_end(block.dbname, last=True)
        self._log_to_snapshot(
            dbname=block.dbname, event='connect', value=block.count_conns())
        self._get_loop().create_task(self._connect(block, started_at, event))

    async def _discard_conn(self, block: Block[C], conn: C) -> None:
        assert not block.conns[conn].in_use
        block.conns.pop(conn)
        self._log_to_snapshot(
            dbname=block.dbname, event='disconnect', value=block.count_conns())
        await self._disconnect(conn, block)
        block.log_connection("discarded")


class Pool(BasePool[C]):

    _new_blocks_waitlist: collections.OrderedDict[Block[C], bool]
    _blocks_over_quota: typing.List[Block[C]]
    _nacquires: int
    _htick: typing.Optional[asyncio.Handle]
    _to_drop: typing.List[Block[C]]

    def __init__(
        self,
        *,
        connect: Connector[C],
        disconnect: Disconnector[C],
        max_capacity: int,
        stats_collector: typing.Optional[StatsCollector]=None,
    ) -> None:
        super().__init__(
            connect=connect,
            disconnect=disconnect,
            stats_collector=stats_collector,
            max_capacity=max_capacity,
        )

        self._new_blocks_waitlist = collections.OrderedDict()

        self._blocks_over_quota = []

        self._nacquires = 0
        self._htick = None
        self._first_tick = True
        self._to_drop = []

    def _maybe_schedule_tick(self) -> None:
        if self._first_tick:
            self._first_tick = False
            self._capture_snapshot(now=time.monotonic())

        if not self._nacquires or self._htick is not None:
            return

        self._htick = self._get_loop().call_later(
            max(self._conntime_avg.avg(), MIN_CONN_TIME_THRESHOLD),
            self._tick
        )

    def _tick(self) -> None:
        self._htick = None
        if self._nacquires:
            self._maybe_schedule_tick()

        now = time.monotonic()

        self._report_snapshot()
        self._capture_snapshot(now=now)

        # If we're managing connections to only one PostgreSQL DB,
        # bail out early. Just give the one and only block we have
        # the max possible quota (which is needed only for logging
        # purposes.)
        nblocks = len(self._blocks)
        if nblocks <= 1:
            self._is_starving = False
            if nblocks:
                first_block = next(iter(self._blocks.values()))
                first_block.quota = self._max_capacity
                first_block.nwaiters_avg.add(first_block.count_waiters())
            return

        need_conns_at_least = 0
        total_nwaiters = 0
        total_calibrated_demand: float = 0
        min_demand = float('inf')
        self._to_drop.clear()
        for block in self._blocks.values():
            nwaiters = block.count_waiters() + block.conn_acquired_num
            block.quota = nwaiters  # will likely be overwritten below
            total_nwaiters += nwaiters
            block.nwaiters_avg.add(nwaiters)
            nwaiters_avg = block.nwaiters_avg.avg()
            if nwaiters_avg:
                need_conns_at_least += 1
            else:
                if not block.count_conns():
                    self._to_drop.append(block)
                    continue

            demand = (
                max(block.nwaiters_avg.avg(), nwaiters) *
                max(block.querytime_avg.avg(), MIN_QUERY_TIME_THRESHOLD)
            )
            total_calibrated_demand += demand
            block._cached_calibrated_demand = demand
            if min_demand > demand:
                min_demand = demand

        self._is_starving = need_conns_at_least >= self._max_capacity
        if self._to_drop:
            for block in self._to_drop:
                self._drop_block(block)

        if not total_nwaiters:
            return

        if total_nwaiters < self._max_capacity:
            # The quota should already be set.
            self._maybe_rebalance()
            return

        if self._is_starving:
            for block in tuple(self._blocks.values()):
                nconns = block.count_conns()
                if nconns == 1:
                    if (
                        now - block.last_connect_timestamp <
                            max(self._conntime_avg.avg(),
                                MIN_CONN_TIME_THRESHOLD)
                    ):
                        # let it keep its connection
                        block.quota = 1
                    else:
                        block.quota = 0
                        self._blocks.move_to_end(block.dbname, last=True)
                elif nconns > 1:
                    block.quota = 0
                    self._blocks.move_to_end(block.dbname, last=True)
                else:
                    block.quota = 1
                    self._blocks.move_to_end(block.dbname, last=True)

                if block.quota:
                    self._log_to_snapshot(
                        dbname=block.dbname, event='set-quota',
                        value=block.quota)
                else:
                    self._log_to_snapshot(
                        dbname=block.dbname, event='reset-quota')

        else:
            capacity_left = self._max_capacity
            if min_demand / total_calibrated_demand * self._max_capacity < 1:
                for block in self._blocks.values():
                    demand = block._cached_calibrated_demand
                    if not demand:
                        block.quota = 0
                        self._log_to_snapshot(
                            dbname=block.dbname, event='reset-quota')

                    k = (self._max_capacity * demand) / total_calibrated_demand
                    if 0 < k <= 1:
                        block.quota = 1
                        self._log_to_snapshot(
                            dbname=block.dbname, event='set-quota',
                            value=block.quota)
                        capacity_left -= 1

            assert capacity_left > 0

            acc: float = 0
            for block in self._blocks.values():
                demand = block._cached_calibrated_demand
                if not demand:
                    continue

                old_acc = acc
                acc += (
                    (capacity_left * demand) / total_calibrated_demand
                )
                block.quota = round(acc) - round(old_acc)

                self._log_to_snapshot(
                    dbname=block.dbname, event='set-quota', value=block.quota)

            self._maybe_rebalance()

    def _maybe_rebalance(self) -> None:
        if self._is_starving:
            return

        self._blocks_over_quota.clear()

        for block in self._blocks.values():
            nconns = block.count_conns()
            quota = block.quota
            if nconns > quota:
                self._try_shrink_block(block)
                if block.count_conns() > quota:
                    self._blocks_over_quota.append(block)
            elif nconns < quota:
                while (
                    block.count_conns() < quota and
                    self._cur_capacity < self._max_capacity
                ):
                    self._schedule_new_conn(block)

        if self._blocks_over_quota:
            self._blocks_over_quota.sort(
                key=lambda b: b.count_conns_over_quota(),
                reverse=True
            )

    def _should_free_conn(self, from_block: Block[C]) -> bool:
        # First, if we only manage one connection to one PostgreSQL DB --
        # we don't need to bother with rebalancing the pool. So we bail out.
        if len(self._blocks) <= 1:
            return False

        from_block_size = from_block.count_conns()

        # Second, we bail out if:
        #
        # * the pool isn't starving, i.e. we have a fewer number of
        #   different DB connections than the max number of connections
        #   allowed;
        #
        # AND
        #
        # * the `from_block` block has fewer connections than its quota.
        if not self._is_starving and from_block_size <= from_block.quota:
            return False

        # Third, we bail out if:
        #
        # * the pool is starving;
        #
        # AND YET
        #
        # * the `from_block` block has only one connection;
        #
        # AND
        #
        # * the block has active waiters in its queue;
        #
        # AND
        #
        # * the block has been holding its last and only connection for
        #   less time than the average time it spends on connecting to
        #   PostgreSQL.
        if (
            self._is_starving and
            from_block_size == 1 and
            from_block.count_waiters() and
            (time.monotonic() - from_block.last_connect_timestamp) <
                max(self._conntime_avg.avg(), MIN_CONN_TIME_THRESHOLD)
        ):
            return False

        return True

    def _maybe_free_conn(
        self,
        from_block: Block[C],
        conn: C,
    ) -> bool:
        if not self._should_free_conn(from_block):
            return False

        label, to_block = self._find_most_starving_block()
        if to_block is None or to_block is from_block:
            return False
        assert label is not None

        self._schedule_transfer(from_block, conn, to_block)

        self._log_to_snapshot(
            dbname=to_block.dbname,
            event=label,
            value=1,
        )

        return True

    def _try_shrink_block(self, block: Block[C]) -> None:
        while (
            block.count_conns_over_quota() and
            self._should_free_conn(block)
        ):
            if (conn := block.try_steal()) is not None:
                _, to_block = self._find_most_starving_block()
                if to_block is not None:
                    self._schedule_transfer(block, conn, to_block)
                else:
                    self._get_loop().create_task(
                        self._discard_conn(block, conn))
            else:
                break

    def _try_steal_conn(self, for_block: Block[C]) -> bool:
        if not self._blocks_over_quota:
            return False
        for block in self._blocks_over_quota:
            if block is for_block or not self._should_free_conn(block):
                continue
            if (conn := block.try_steal()) is not None:
                self._log_to_snapshot(
                    dbname=block.dbname, event='conn-stolen')
                self._schedule_transfer(block, conn, for_block)
                return True
        return False

    def _find_most_starving_block(
        self
    ) -> typing.Tuple[typing.Optional[str], typing.Optional[Block[C]]]:
        to_block = None

        # Find if there are any newly created blocks waiting for their
        # first connection.
        while self._new_blocks_waitlist:
            block, _ = self._new_blocks_waitlist.popitem(last=False)
            if block.count_conns() or not block.count_waiters():
                # This block is already initialized. Skip it.
                # This branch shouldn't happen.
                continue
            to_block = block
            break

        if to_block is not None:
            return 'first-conn', to_block

        # Find if there are blocks without a single connection.
        # Find the one that is starving the most.
        max_need = 0
        for block in self._blocks.values():
            block_size = block.count_conns()
            block_demand = block.count_waiters()

            if block_size or not block_demand:
                continue

            if block_demand > max_need:
                max_need = block_demand
                to_block = block

        if to_block is not None:
            return 'revive-conn', to_block

        # Find all blocks that are under quota and award the most
        # starving one.
        max_need = 0
        for block in self._blocks.values():
            block_size = block.count_conns()
            block_quota = block.quota
            if block_quota > block_size:
                need = block_quota - block_size
                if need > max_need:
                    max_need = need
                    to_block = block

        if to_block:
            return 'redist-conn', to_block

        return None, None

    async def _acquire(self, dbname: str) -> C:
        block = self._get_block(dbname)

        room_for_new_conns = self._cur_capacity < self._max_capacity
        block_nconns = block.count_conns()

        if room_for_new_conns:
            if len(self._blocks) == 1:
                # Managing connections to only one DB and can open more
                # connections.  Or this is before the first tick -- a free
                # for all.
                self._schedule_new_conn(block)
                return await block.acquire()

            if (
                not block_nconns or
                block_nconns < block.quota or
                not block.count_approx_available_conns()
            ):
                # Block has no connections at all.
                self._schedule_new_conn(block)
                return await block.acquire()

        if not block_nconns:
            # This is a block without any connections.
            # Request one of the next released connections to be
            # reallocated for this block.
            if not self._try_steal_conn(block):
                self._new_blocks_waitlist[block] = True
            return await block.acquire()

        if block_nconns < block.quota:
            # Let's see if we can steal a connection from some block
            # that's over quota and open a new one.
            self._try_steal_conn(block)
            return await block.acquire()

        return await block.acquire()

    async def acquire(self, dbname: str) -> C:
        self._nacquires += 1
        self._maybe_schedule_tick()
        try:
            conn = await self._acquire(dbname)
        finally:
            self._nacquires -= 1

        block = self._blocks[dbname]
        assert not block.conns[conn].in_use
        block.inc_acquire_counter()
        block.conns[conn].in_use = True
        block.conns[conn].in_use_since = time.monotonic()

        return conn

    def release(self, dbname: str, conn: C, *, discard: bool=False) -> None:
        try:
            block = self._blocks[dbname]
        except KeyError:
            raise RuntimeError(
                f'cannot release connection {conn!r}: {dbname!r} database '
                f'is not known to the pool'
            ) from None

        try:
            conn_state = block.conns[conn]
        except KeyError:
            raise RuntimeError(
                f'cannot release connection {conn!r}: the connection does not '
                f'belong to the pool'
            ) from None

        if not conn_state.in_use:
            raise RuntimeError(
                f'cannot release connection {conn!r}: the connection was '
                f'never acquired from the pool'
            ) from None

        block.dec_acquire_counter()
        block.querytime_avg.add(time.monotonic() - conn_state.in_use_since)
        conn_state.in_use = False
        conn_state.in_use_since = 0

        self._maybe_schedule_tick()

        if discard:
            self._get_loop().create_task(
                self._discard_conn(block, conn))
            return

        if not self._maybe_free_conn(block, conn):
            block.release(conn)

    async def prune_inactive_connections(self, dbname: str) -> None:
        try:
            block = self._blocks[dbname]
        except KeyError:
            return None

        conns = []
        while (conn := block.try_steal()) is not None:
            conns.append(conn)

        if conns:
            await asyncio.gather(
                *(self._discard_conn(block, conn) for conn in conns),
                return_exceptions=True
            )


class _NaivePool(BasePool[C]):
    """Implements a rather naive and flawed balancing algorithm.

    Should only be used for for testing purposes.
    """

    _conns: typing.Dict[str, typing.Set[C]]
    _last_tick: float

    def __init__(
        self,
        connect: Connector[C],
        disconnect: Disconnector[C],
        max_capacity: int,
        stats_collector: typing.Optional[StatsCollector]=None,
    ) -> None:
        super().__init__(
            connect=connect,
            disconnect=disconnect,
            stats_collector=stats_collector,
            max_capacity=max_capacity,
        )
        self._conns = {}
        self._last_tick = 0

    def _maybe_free_conn(self, block: Block[C], conn: C) -> bool:
        return False

    def _maybe_tick(self) -> None:
        now = time.monotonic()

        if self._last_tick == 0:
            # First time `_tick()` is run.
            self._capture_snapshot(now=now)
            self._last_tick = now
            return

        if now - self._last_tick < 0.1:
            # Not enough time passed since the last tick.
            return

        self._last_tick = now

        self._report_snapshot()
        self._capture_snapshot(now=now)

    async def acquire(self, dbname: str) -> C:
        self._maybe_tick()

        block = self._get_block(dbname)

        if self._cur_capacity < self._max_capacity:
            self._schedule_new_conn(block)

        return await block.acquire()

    def release(self, dbname: str, conn: C) -> None:
        self._maybe_tick()
        this_block = self._get_block(dbname)

        if this_block.count_conns() < this_block.count_waiters():
            this_block.release(conn)
            return

        max_need = 0
        to_block = None
        for block in self._blocks.values():
            block_size = block.count_conns()
            block_demand = block.count_waiters()

            if not block_size and block_demand:
                need = block_demand * 1000
            elif block_size < block_demand:
                need = block_demand - block_size
            else:
                continue

            if need > max_need:
                max_need = block_demand
                to_block = block

        if to_block is this_block or to_block is None:
            this_block.release(conn)
            return

        self._schedule_transfer(this_block, conn, to_block)

        self._log_to_snapshot(
            dbname=to_block.dbname,
            event='free',
            value=1,
        )
