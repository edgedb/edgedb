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


"""
Run with

   $ env EDGEDB_TEST_DEBUG_POOL=1 edb test -k test_server_connpool

to get interactive HTML reports in the `./tmp` directory.

Alternatively, run with

  $ python tests/test_server_pool.py

to get interactive HTML report of all tests aggregated in one HTML
file in `./tmp/connpool.html`.
"""


import asyncio
import collections
import dataclasses
import datetime
import functools
import json
import logging
import os
import random
import statistics
import string
import textwrap
import time
import typing
import unittest
import unittest.mock

from edb.common import taskgroup
from edb.server import connpool


@dataclasses.dataclass
class DBSpec:
    db: str
    start_at: float
    end_at: float
    qps: int
    query_cost_base: float
    query_cost_var: float


@dataclasses.dataclass
class Spec:
    timeout: float
    duration: int
    capacity: int
    conn_cost_base: float
    conn_cost_var: float
    dbs: typing.List[DBSpec]
    desc: str = ''
    disconn_cost_base: float = 0.006
    disconn_cost_var: float = 0.0015


@dataclasses.dataclass
class Simulation:
    latencies: typing.Dict[str, typing.List[float]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )

    failed_disconnects: int = 0
    failed_queries: int = 0

    stats: typing.List[dict] = dataclasses.field(default_factory=list)


class FakeConnection:
    def __init__(self, db: str):
        self._locked = False
        self._db = db

    def lock(self, db):
        if self._db != db:
            raise RuntimeError('a connection for different DB')
        if self._locked:
            raise RuntimeError(
                "attempting to use a connection that's already in use")
        self._locked = True

    def unlock(self, db):
        if self._db != db:
            raise RuntimeError('a connection for different DB')
        if not self._locked:
            raise RuntimeError(
                "attempting to stop using a connection that wasn't used")
        self._locked = False

    def on_connect(self):
        if self._locked:
            raise RuntimeError(
                "attempting to re-connect a connection "
                "that's currently in use")

    def on_disconnect(self):
        if self._locked:
            raise RuntimeError(
                "attempting to disconnect a connection "
                "that's currently in use")


class SimulatedCaseMeta(type):
    def __new__(mcls, name, bases, dct):
        for methname, meth in tuple(dct.items()):
            if not methname.startswith('test_'):
                continue

            @functools.wraps(meth)
            def wrapper(self, meth=meth, testname=methname):
                spec = meth(self)
                asyncio.run(self.simulate(testname, spec))

            wrapper.__name__ = methname
            wrapper.__pooltest__ = True
            dct[methname] = wrapper

        return super().__new__(mcls, name, bases, dct)


class SimulatedCase(unittest.TestCase, metaclass=SimulatedCaseMeta):

    def make_fake_connect(
        self,
        sim: Simulation,
        cost_base: float,
        cost_var: float
    ):
        async def fake_connect(dbname):
            dur = max(cost_base + random.triangular(-cost_var, cost_var), 0.01)
            await asyncio.sleep(dur)
            return FakeConnection(dbname)
        return fake_connect

    def make_fake_disconnect(
        self,
        sim: Simulation,
        cost_base: float,
        cost_var: float
    ):
        async def fake_disconnect(conn, sim=sim):
            dur = max(cost_base + random.triangular(-cost_var, cost_var), 0.01)
            try:
                conn.on_disconnect()
                await asyncio.sleep(dur)
                conn.on_disconnect()
            except Exception:
                sim.failed_disconnects += 1
                raise
        return fake_disconnect

    def make_fake_query(
        self,
        sim: Simulation,
        pool: connpool.Pool,
        db: str,
        dur: float
    ):
        async def query(sim=sim, db=db):
            try:
                st = time.monotonic()
                conn = await pool.acquire(db)
                sim.latencies[db].append(time.monotonic() - st)
                conn.lock(db)
                await asyncio.sleep(dur)
                conn.unlock(db)
                pool.release(db, conn)
            except Exception:
                sim.failed_queries += 1
                raise
        return query()

    def calc_percentiles(
        self,
        lats: typing.List[float]
    ) -> typing.Tuple[float, float, float, float, float, float]:
        lats_len = len(lats)
        lats.sort()
        return (
            lats[lats_len // 99],
            lats[lats_len // 4],
            lats[lats_len // 2],
            lats[lats_len * 3 // 4],
            lats[min(lats_len - lats_len // 99, lats_len - 1)],
            statistics.geometric_mean(lats)
        )

    def calc_total_percentiles(
        self,
        lats: typing.Dict[str, typing.List[float]]
    ) -> typing.Tuple[float, float, float, float, float, float]:
        acc = []
        for i in lats.values():
            acc.extend(i)
        return self.calc_percentiles(acc)

    async def simulate_once(self, spec, pool_cls, *, collect_stats=False):
        sim = Simulation()

        def on_stats(stat):
            stat = dataclasses.asdict(stat)
            sim.stats.append(stat)

        pool = pool_cls(
            connect=self.make_fake_connect(
                sim, spec.conn_cost_base, spec.conn_cost_var),
            disconnect=self.make_fake_disconnect(
                sim, spec.disconn_cost_base, spec.disconn_cost_var),
            stats_collector=on_stats if collect_stats else None,
            max_capacity=spec.capacity,
        )

        TICK_EVERY = 0.001

        started_at = time.monotonic()
        async with taskgroup.TaskGroup() as g:
            elapsed = 0
            while elapsed < spec.duration:
                elapsed = time.monotonic() - started_at

                for db in spec.dbs:
                    if not (db.start_at < elapsed < db.end_at):
                        continue

                    qpt = db.qps * TICK_EVERY
                    if qpt >= 1:
                        qpt = round(qpt)
                    else:
                        qpt = int(random.random() <= qpt)

                    for _ in range(qpt):
                        dur = max(
                            db.query_cost_base +
                            random.triangular(
                                -db.query_cost_var, db.query_cost_var),
                            0.001
                        )
                        g.create_task(
                            self.make_fake_query(sim, pool, db.db, dur)
                        )

                await asyncio.sleep(TICK_EVERY)

        self.assertEqual(sim.failed_disconnects, 0)
        self.assertEqual(sim.failed_queries, 0)

        self.assertEqual(pool.failed_disconnects, 0)
        self.assertEqual(pool.failed_connects, 0)

        if collect_stats:
            pn = f'{type(pool).__module__}.{type(pool).__qualname__}'
            js_data = {
                'test_started_at': started_at,
                'total_lats': self.calc_total_percentiles(sim.latencies),
                'lats': {
                    db: self.calc_percentiles(lats)
                    for db, lats in sim.latencies.items()
                },
                'pool_name': pn,
                'stats': sim.stats,
            }

            return js_data

    async def simulate(self, testname, spec):
        if os.environ.get('EDGEDB_TEST_DEBUG_POOL'):
            js_data = await self.simulate_and_collect_stats(testname, spec)
            if not os.path.exists('tmp'):
                os.mkdir('tmp')
            with open(f'tmp/{testname}.html', 'wt') as f:
                f.write(
                    string.Template(HTML_TPL).safe_substitute(
                        DATA=json.dumps([js_data]))
                )
        else:
            await asyncio.wait_for(
                self.simulate_once(
                    spec, connpool.Pool, collect_stats=False),
                spec.timeout
            )

    async def simulate_and_collect_stats(self, testname, spec):
        pools = [connpool.Pool, connpool._NaivePool]

        js_data = []
        for pool_cls in pools:
            try:
                data = await asyncio.wait_for(
                    self.simulate_once(
                        spec, pool_cls, collect_stats=True),
                    spec.timeout
                )
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(f'timeout with {pool_cls!r}')
            js_data.append(data)

        js_data = {
            'desc': textwrap.dedent(spec.desc) if spec.desc else None,
            'test_name': testname,
            'now': str(datetime.datetime.now()),
            'spec': dataclasses.asdict(spec),
            'runs': js_data
        }

        return js_data

    def simulate_all_and_collect_stats(self):
        specs = {}
        for methname in dir(self):
            if not methname.startswith('test_'):
                continue
            meth = getattr(self, methname)
            if not getattr(meth, '__pooltest__', False):
                continue
            spec = meth.__wrapped__(self)
            specs[methname] = spec

        js_data = []
        for testname, spec in specs.items():
            print(f'Running {testname}...', end='', flush=True)
            js_data.append(
                asyncio.run(
                    self.simulate_and_collect_stats(testname, spec)))
            print('OK')

        html = string.Template(HTML_TPL).safe_substitute(
            DATA=json.dumps(js_data))

        if not os.path.exists('tmp'):
            os.mkdir('tmp')
        with open(f'tmp/connpool.html', 'wt') as f:
            f.write(html)
        now = int(datetime.datetime.now().timestamp())
        with open(f'tmp/connpool_{now}.html', 'wt') as f:
            f.write(html)


class TestServerConnpoolSimulation(SimulatedCase):

    def test_server_connpool_1(self):
        return Spec(
            timeout=20,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.05,
            conn_cost_var=0.01,
            dbs=[
                DBSpec(
                    db=f't{i}',
                    start_at=0,
                    end_at=0.5,
                    qps=50,
                    query_cost_base=0.03,
                    query_cost_var=0.005,
                ) for i in range(6)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.3,
                    end_at=0.7,
                    qps=50,
                    query_cost_base=0.03,
                    query_cost_var=0.005,
                ) for i in range(6, 12)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.6,
                    end_at=0.8,
                    qps=50,
                    query_cost_base=0.03,
                    query_cost_var=0.005,
                ) for i in range(6)
            ]
        )

    def test_server_connpool_2(self):
        return Spec(
            desc='''
            In this test, we have 6x1500qps connections that simulate fast
            queries (0.001..0.006s), and 6x700qps connections that simutale
            slow queries (~0.03s). The algorithm allocates connections
            fairly to both groups, essentially using the
            "demand = avg_query_time * avg_num_of_connection_waiters"
            formula. The QoS is at the same level for all DBs.
            ''',
            timeout=20,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            dbs=[
                DBSpec(
                    db=f't{i}',
                    start_at=0,
                    end_at=0.5,
                    qps=1500,
                    query_cost_base=0.001,
                    query_cost_var=0.005,
                ) for i in range(6)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.3,
                    end_at=0.7,
                    qps=700,
                    query_cost_base=0.03,
                    query_cost_var=0.001,
                ) for i in range(6, 12)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.6,
                    end_at=0.8,
                    qps=700,
                    query_cost_base=0.06,
                    query_cost_var=0.01,
                ) for i in range(6)
            ]
        )

    def test_server_connpool_3(self):
        return Spec(
            timeout=10,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            dbs=[
                DBSpec(
                    db=f't{i}',
                    start_at=0,
                    end_at=0.8,
                    qps=5000,
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ) for i in range(6)
            ]
        )

    def test_server_connpool_4(self):
        return Spec(
            timeout=20,
            duration=1.1,
            capacity=50,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            dbs=[
                DBSpec(
                    db=f't{i}',
                    start_at=0,
                    end_at=0.8,
                    qps=1000,
                    query_cost_base=0.01 * (i + 1),
                    query_cost_var=0.005 * (i + 1),
                ) for i in range(6)
            ]
        )

    def test_server_connpool_5(self):
        return Spec(
            timeout=30,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.15,
            conn_cost_var=0.05,
            dbs=[
                DBSpec(
                    db=f't{i}',
                    start_at=0 + i / 10,
                    end_at=0.5 + i / 10,
                    qps=150,
                    query_cost_base=0.020,
                    query_cost_var=0.005,
                ) for i in range(6)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.3,
                    end_at=0.7,
                    qps=50,
                    query_cost_base=0.008,
                    query_cost_var=0.003,
                ) for i in range(6, 12)
            ] + [
                DBSpec(
                    db=f't{i}',
                    start_at=0.6,
                    end_at=0.8,
                    qps=50,
                    query_cost_base=0.003,
                    query_cost_var=0.002,
                ) for i in range(6)
            ]
        )

    def test_server_connpool_6(self):
        return Spec(
            timeout=10,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.15,
            conn_cost_var=0.05,
            dbs=[
                DBSpec(
                    db=f'one-db',
                    start_at=0 + i / 10,
                    end_at=0.5 + i / 10,
                    qps=150,
                    query_cost_base=0.020,
                    query_cost_var=0.005,
                ) for i in range(6)
            ]
        )

    def test_server_connpool_7(self):
        return Spec(
            desc="""
            The point of this test is to have one connection "t1" that
            just has crazy demand for connections.  Then the "t2" connections
            are infrequent -- so they have a miniscule quota.

            Our goal is to make sure that "t2" has good QoS and gets
            its queries processed as soon as they're submitted.
            """,
            timeout=10,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.05,
            conn_cost_var=0.01,
            dbs=[
                DBSpec(
                    db=f't1',
                    start_at=0,
                    end_at=1.0,
                    qps=500,
                    query_cost_base=0.040,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db=f't2',
                    start_at=0.1,
                    end_at=0.3,
                    qps=30,
                    query_cost_base=0.030,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db=f't2',
                    start_at=0.6,
                    end_at=0.9,
                    qps=30,
                    query_cost_base=0.010,
                    query_cost_var=0.005,
                )
            ]
        )


class TestServerConnectionPool(unittest.TestCase):

    def make_fake_connect(
        self,
        cost_base: float=0.01,
        cost_var: float=0.005
    ):
        async def fake_connect(dbname):
            dur = max(cost_base + random.triangular(-cost_var, cost_var), 0.01)
            await asyncio.sleep(dur)
            return FakeConnection(dbname)

        return fake_connect

    def make_fake_disconnect(
        self,
        cost_base: float=0.01,
        cost_var: float=0.005
    ):
        async def fake_disconnect(conn):
            dur = max(cost_base + random.triangular(-cost_var, cost_var), 0.01)
            conn.on_disconnect()
            await asyncio.sleep(dur)
            conn.on_disconnect()

        return fake_disconnect

    def test_connpool_longtrans(self):
        # The test creates a pool with a capacity of 10 connections.
        # We then acquire a connection to the `bbb` database.
        # We then acquire a connection to the `aaa` database.
        # We then try to acquire yet another connection to the `aaa` database,
        # while the first two connections are still non-released.
        #
        # Previously the pool algorithm would not grant the third connection
        # because the `aaa` quota was set to 1 at the time the first connection
        # to it was acquired. The pool then failed to recognize that there's
        # yet another connection waiting to be acquired and the whole thing
        # would deadlock.

        async def q0(pool, event):
            conn = await pool.acquire('bbb')
            await event.wait()
            pool.release('bbb', conn)

        async def q1(pool, event):
            conn = await pool.acquire('aaa')
            await event.wait()
            pool.release('aaa', conn)

        async def q2(pool, event):
            conn = await pool.acquire('aaa')
            event.set()
            pool.release('aaa', conn)

        async def test(delay: float):
            event = asyncio.Event()

            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=10,
            )

            async with taskgroup.TaskGroup() as g:
                g.create_task(q0(pool, event))
                await asyncio.sleep(delay)
                g.create_task(q1(pool, event))
                await asyncio.sleep(delay)
                g.create_task(q2(pool, event))

        async def main():
            await asyncio.wait_for(test(0.05), timeout=5)
            await asyncio.wait_for(test(0.000001), timeout=5)

        asyncio.run(main())

    def test_connpool_wrong_quota(self):
        async def q(db, pool, *, wait_event=None, set_event=None):
            conn = await pool.acquire(db)
            # print('GOT', db)
            if set_event:
                set_event.set()
            if wait_event:
                await wait_event.wait()
            # print('RELEASE', db)
            pool.release(db, conn)

        async def test(delay: float):
            e1 = asyncio.Event()
            e2 = asyncio.Event()
            e3 = asyncio.Event()

            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=5,
            )

            async with taskgroup.TaskGroup() as g:
                for _ in range(4):
                    g.create_task(q('A', pool, wait_event=e1))

                await asyncio.sleep(0.1)
                g.create_task(q('B', pool, set_event=e2, wait_event=e3))
                await e2.wait()
                g.create_task(q('B', pool, set_event=e3))

                await asyncio.sleep(0.1)
                e1.set()

        async def main():
            await asyncio.wait_for(test(0.05), timeout=5)

        asyncio.run(main())

    class MockLogger(logging.Logger):
        logs: asyncio.Queue

        def __init__(self):
            super().__init__('edb.server')

        def isEnabledFor(self, level):
            return True

        def _log(self, level, msg, args, *other, **kwargs):
            if (
                'established' in args or
                '1 were discarded' in args or
                '1 were established' in args or
                'transferred out' in args or
                'transferred in' in args or
                'discarded' in args
            ):
                self.logs.put_nowait(args)

    @unittest.mock.patch('edb.server.connpool.pool.logger',
                         new_callable=MockLogger)
    @unittest.mock.patch('edb.server.connpool.pool.MIN_LOG_TIME_THRESHOLD',
                         0.2)
    def test_connpool_log_batching(self, logger: MockLogger):
        async def test():
            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=5,
            )
            conn1 = await pool.acquire("block_a")
            args = await logger.logs.get()
            self.assertIn("established", args)
            self.assertIn("block_a", args)

            conn2 = await pool.acquire("block_b")
            start = time.monotonic()
            args = await logger.logs.get()
            self.assertIn("established", args)
            self.assertIn("block_b", args)
            self.assertLess(time.monotonic() - start, 0.2)

            pool.release("block_a", conn1, discard=True)
            start = time.monotonic()
            args = await logger.logs.get()
            self.assertIn("1 were discarded", args)
            self.assertIn("block_a", args)
            self.assertGreater(time.monotonic() - start, 0.2)

            pool.release("block_b", conn2, discard=True)
            start = time.monotonic()
            args = await logger.logs.get()
            self.assertIn("discarded", args)
            self.assertIn("block_b", args)
            self.assertLess(time.monotonic() - start, 0.2)

        async def main():
            logger.logs = asyncio.Queue()
            await asyncio.wait_for(test(), timeout=5)

        asyncio.run(main())

    @unittest.mock.patch('edb.server.connpool.pool.logger.level',
                         logging.CRITICAL)
    def _test_connpool_connect_error(self, error_type, expected_connects):
        connect_called_num = 0
        disconnect_called_num = 0

        async def fake_connect(dbname):
            nonlocal connect_called_num
            connect_called_num += 1
            raise error_type()

        async def fake_disconnect(conn):
            nonlocal disconnect_called_num
            disconnect_called_num += 1

        async def test():
            pool = connpool.Pool(
                connect=fake_connect,
                disconnect=fake_disconnect,
                max_capacity=5,
            )
            with self.assertRaises(error_type):
                await pool.acquire("block_a")
            self.assertEqual(connect_called_num, expected_connects)
            self.assertEqual(disconnect_called_num, 0)
            with self.assertRaises(error_type):
                await pool.acquire("block_a")
            self.assertEqual(connect_called_num, expected_connects + 1)
            self.assertEqual(disconnect_called_num, 0)

        async def main():
            await asyncio.wait_for(test(), timeout=1)

        asyncio.run(main())

    @unittest.mock.patch('edb.server.connpool.pool.CONNECT_FAILURE_RETRIES', 2)
    def test_connpool_connect_error(self):
        from edb.server.pgcon import errors

        class BackendError(errors.BackendError):
            def __init__(self):
                super().__init__(fields={'C': '3D000'})

        self._test_connpool_connect_error(BackendError, 1)

        class ConnectError(Exception):
            pass

        self._test_connpool_connect_error(ConnectError, 3)

    @unittest.mock.patch('edb.server.connpool.pool.CONNECT_FAILURE_RETRIES', 0)
    def test_connpool_connect_error_zero_retry(self):
        class ConnectError(Exception):
            pass

        self._test_connpool_connect_error(ConnectError, 1)

    @unittest.mock.patch('edb.server.connpool.pool.logger',
                         new_callable=MockLogger)
    @unittest.mock.patch('edb.server.connpool.pool.MIN_LOG_TIME_THRESHOLD', 0)
    @unittest.mock.patch('edb.server.connpool.pool.CONNECT_FAILURE_RETRIES', 5)
    def test_connpool_steal_connect_error(self, logger: MockLogger):
        count = 0
        connect = self.make_fake_connect()

        async def fake_connect(dbname):
            if dbname == 'block_a':
                return await connect(dbname)
            else:
                nonlocal count
                count += 1
                if count < 3:
                    raise ValueError()
                else:
                    return await connect(dbname)

        async def test():
            pool = connpool.Pool(
                connect=fake_connect,
                disconnect=self.make_fake_disconnect(),
                max_capacity=2,
            )

            # fill the pool
            conn1 = await pool.acquire("block_a")
            self.assertEqual(await logger.logs.get(),
                             ('established', 'block_a'))
            conn2 = await pool.acquire("block_a")
            self.assertEqual(await logger.logs.get(),
                             ('established', 'block_a'))
            pool.release("block_a", conn1)
            pool.release("block_a", conn2)

            # steal a connection from block_a, with retries
            await pool.acquire("block_b")
            logs = [await logger.logs.get() for i in range(3)]
            self.assertIn(('transferred out', 'block_a'), logs)
            self.assertIn(('discarded', 'block_a'), logs)
            self.assertIn(('transferred in', 'block_b'), logs)

        async def main():
            logger.logs = asyncio.Queue()
            await asyncio.wait_for(test(), timeout=5)

        asyncio.run(main())


HTML_TPL = R'''<!DOCTYPE html>
<html>
    <head>
    <meta charset="UTF-8">

    <link
        rel="stylesheet" type="text/css"
        href="https://unpkg.com/normalize.css@7.0.0/normalize.css" />

    <script
        crossorigin
        src="https://unpkg.com/@babel/standalone/babel.min.js">
    </script>
    <script
        crossorigin
        src="https://unpkg.com/react@17/umd/react.production.min.js">
    </script>
    <script
        crossorigin
        src="https://unpkg.com/react-dom@17/umd/react-dom.production.min.js">
    </script>
    <script
        crossorigin
        src="https://unpkg.com/d3@5.16.0/dist/d3.min.js">
    </script>

    <script>
        var DATA = $DATA;
        var PALETTE = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
            '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
            '#c49c94', '#f7b6d2', '#dbdb8d', '#9edae5'
        ];
    </script>

    <style>
        :root {
            --boxWidth: 3px;
            --boxHeight: 1.6em;
        }

        html {
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
        }

        .num {
            font-weight: bold;
        }
        .smallNum {
            font-size: 80%;
        }

        .smallNum span {
            display: inline flow-root;
        }

        .conns {
            min-width: 500px;
            margin-top: 30px;
        }

        .connBoxes {
        }
        .testRun th {
            padding: 7px;
            margin: 0;
            font-size: 80%;
            text-transform: uppercase;
            text-align: left !important;
        }
        .conns td {
            padding: 4px 7px;
            margin: 0;
        }
        .conns tr {
            padding: 0;
            margin: 0;
        }
        .connBoxes .connBoxCnt {
            display: flex;
        }
        .connBoxes .connBox {
            width: var(--boxWidth);
            height: var(--boxHeight);
            background-color: currentColor;
            opacity: 80%;
        }
        .connBoxes .verBars {
            display: flex;
            flex-direction: column;
        }
        .connBoxes .connBox.connBoxPending {
            background: repeating-linear-gradient(
                45deg,
                currentColor,
                currentColor 2px,
                #e7e7e7 2px,
                #e7e7e7 4px
            );
        }
        header {
            padding: 0 15px;
        }
        .testRun {
            width: 49vw;
            display: grid;
            grid-template-areas:
                "header"
                "server";
            grid-template-rows: auto auto;
            grid-template-columns: auto auto;
            align-content: start;
        }
        button {
            border: none;
            background: none;
            outline: none !important;
        }
        button:hover {
            cursor: pointer;
            border: none;
            background: none;
        }
        .testRun .testHead {
            grid-area: header;
            padding: 0 15px;
        }
        .testRun .testHead p:first-child {
            margin-top: 0;
        }
        .testRun .testServer {
            grid-area: server;
        }
        .testRun .testServer table {
            width: 100%;
        }
        .testRun .testServer table table {
            width: auto;
        }
        .testRun .connsLog {
            padding-left: calc(20px + 1.7em);
        }
        .testRun .testClient {
            grid-area: client;
            width: 250px;
            padding: 50px;
            position: relative;
        }

        .popup {
            position: sticky;
            top: 80px;
            background: #e7e7e7;
            border-radius: 4px;
            padding: 20px 30px;
        }
        .popup h2 {
            font-size: 80%;
            text-transform: uppercase;
        }
        .boxchart {
            width: 600px;
            height: 240px;
        }
        .details {
            display: flex;
            flex-direction: row;
        }
        .runs {
            display: flex;
        }
        .hidden {
            display: none;
        }
        .testTopHead {
            display: flex;
            flex-direction: row;
            padding: 15px 0 0 0;
            border-top: 1px solid #eee;
        }
        .testTop {
            margin-bottom: 30px;
        }
        code {
            background-color: #eee;
            border-radius: 6px;
            display: inline-block;
        }
    </style>

    <script type="text/babel">
        function* nextColor() {
            let i = 0;
            while (1) {
                yield PALETTE[i++];
                if (i >= PALETTE.length) {
                    i = 0;
                }
            }
        }

        class TestResults {
            constructor(data) {
                this._colorGen = nextColor();
                this._nextColor = () => this._colorGen.next().value;

                this.nameToColor = new Map();

                this.desc = data['desc'];
                this.generated_on = data['now'];
                this.test_name = data['test_name'];
                this.spec = data['spec'];
                this.runs = data['runs'];
            }

            getColor(name) {
                let color = this.nameToColor.get(name);
                if (color) {
                    return color;
                }

                color = this._nextColor();
                this.nameToColor.set(name, color);
                return color;
            }
        }

        function formatNum(n) {
            if (n - Math.floor(n)) {
                return n.toFixed(3);
            } else {
                return n.toString()
            }
        }

        class LatencyChart extends React.Component {
            constructor(props) {
                super(props);
                this.svg = React.createRef();
            }

            componentDidMount() {
                this.draw(
                    this.svg.current,
                    this.props.runData.lats,
                    this.props.runData.total_lats,
                );
            }

            draw(el, data, tdata) {
                const height = 240;
                const width = 600;

                console.log(data);

                let maxLat = 0;
                for (let row of Object.values(data)) {
                    if (maxLat < row[4]) {
                        maxLat = row[4];
                    }
                }

                let x0 = d3.scaleBand()
                    .range([0, width - 50])
                    .padding(0.2)
                    .domain(Object.keys(data));

                let y = d3.scaleLinear()
                    .range([height - 40, 0])
                    .domain([0, maxLat]);

                let yAxis = d3.axisLeft(y);
                let xAxis = d3.axisBottom(x0);

                let chart = d3.select(el)
                    .attr('viewBox', `0 0 ${width} ${height}`)
                    .append("g")
                        .attr("transform", 'translate(40, 10)');

                chart.append("g")
                    .attr("transform", `translate(0, ${height - 30})`)
                    .call(xAxis);

                chart.append("g")
                    .call(yAxis)
                    .append("text")
                        .attr("transform", "rotate(-90)")
                        .attr("y", 6)
                        .attr("dy", ".71em")
                        .style("text-anchor", "end");

                let g = chart.selectAll(".bench")
                    .data(Object.entries(data))
                    .enter().append("g");

                let X = (d, i) => x0(d[0]);
                let DX = x0.bandwidth();

                let getColor = (x) => this.props.tr.getColor(x[0]);

                g.append('line')
                    .attr('y1', d => y(d[1][4]))
                    .attr('y2', d => y(d[1][4]))
                    .attr('x1', X)
                    .attr('x2', (d, i) => X(d, i) + DX)
                    .style("stroke", getColor);

                g.append('line')
                    .attr('y1', d => y(d[1][0]))
                    .attr('y2', d => y(d[1][0]))
                    .attr('x1', X)
                    .attr('x2', (d, i) => (X(d, i) + DX))
                    .style("stroke", getColor);

                g.append('line')
                    .attr('y1', d => y(d[1][2]))
                    .attr('y2', d => y(d[1][2]))
                    .attr('x1', X)
                    .attr('x2', (d, i) => (X(d, i) + DX))
                    .style("stroke", getColor);

                g.append('line')
                    .attr('y1', d => y(d[1][0]))
                    .attr('y2', d => y(d[1][1]))
                    .attr('x1', (d, i) => (X(d, i) + DX / 2))
                    .attr('x2', (d, i) => (X(d, i) + DX / 2))
                    .style("stroke", getColor)
                    .style("stroke-dasharray", "2,2");

                g.append('line')
                    .attr('y1', d => y(d[1][4]))
                    .attr('y2', d => y(d[1][3]))
                    .attr('x1', (d, i) => (X(d, i) + DX / 2))
                    .attr('x2', (d, i) => (X(d, i) + DX / 2))
                    .style("stroke", getColor)
                    .style("stroke-dasharray", "2,2");

                g.append('circle')
                    .attr('cy', d => y(d[1][5]))
                    .attr('cx', (d, i) => (X(d, i) + DX / 2))
                    .style("fill", getColor)
                    .attr("r", 3);

                g.append('rect')
                    .attr('y', d => y(d[1][3]))
                    .attr('x', X)
                    .attr("width", DX)
                    .attr('height', d => Math.abs(y(d[1][3]) - y(d[1][1])))
                    .style("stroke", getColor)
                    .style("fill", 'rgba(0, 0, 0, 0)');

                chart.append('line')
                    .attr('y1', d => y(tdata[5]))
                    .attr('y2', d => y(tdata[5]))
                    .attr('x1', 0)
                    .attr('x2', width - 50)
                    .style("stroke", '#f00')
                    .style("stroke-dasharray", "3,1");
            }

            render() {
                const hint = (
                    'The chart shows distribution of connection acquire ' +
                    'times to different DBs. Box charts are percentiles: ' +
                    '1%, 25%, 50%, 75%, 99%; discs show the geometric mean. ' +
                    'The dashed red line is the geometric mean for all DBs.'
                );

                return <div className="boxchart" title={hint}>
                    <svg ref={this.svg}></svg>
                </div>
            }
        }

        function ConnBar({tr, blocks}) {
            const boxes = [];
            let totalConns = 0;
            let bn = 0;

            for (const block of blocks) {
                let w = ((block.nconns / tr.spec.capacity) * 100).toFixed(2);
                boxes.push(
                    <div
                        key={bn}
                        style={{
                            color: tr.getColor(block.dbname),
                            width: w + '%'
                        }}
                        className="connBox"
                    ></div>
                );
                totalConns += block.nconns;

                if (block.npending) {
                    let w = (
                        ((block.npending / tr.spec.capacity) * 100).toFixed(2)
                    );
                    boxes.push(
                        <div
                            key={`${bn}-pending`}
                            style={{
                                color: tr.getColor(block.dbname),
                                width: w + '%'
                            }}
                            className="connBox connBoxPending"
                        ></div>
                    );
                    totalConns += block.npending;
                }
                bn++;
            }

            if (totalConns < tr.spec.capacity) {
                let w = (
                    (
                        (tr.spec.capacity - totalConns) / tr.spec.capacity
                    ) * 100
                ).toFixed(2);

                boxes.push(
                    <div
                        key={bn++}
                        style={{
                            color: 'white',
                            boxShadow: 'inset 0 0 1px #777',
                            width: w + '%'
                        }}
                        className="connBox"
                    ></div>
                );
            }

            const hint = (
                'Illustrates backend connections distribution. Solid boxes ' +
                'are established connections; striped are pending new ' +
                'backend connections.'
            );

            return <div className="connBoxCnt" title={hint}>{boxes}</div>;
        }

        function TestLine({tr, runData, line}) {
            const [collapsed, setCollapsed] = React.useState(true);

            const waiters = [];

            let bn = 0;
            for (const block of line.blocks) {
                waiters.push(
                    <span
                        className="num"
                        style={{color: tr.getColor(block.dbname)}}
                        key={`${bn}-nwaiters`}
                    >{block.nwaiters}</span>
                );
                waiters.push(<>&nbsp;/&nbsp;</>)
                bn++;
            }

            waiters.pop();

            const button = collapsed ?
                <button onClick={() => setCollapsed(false)}>➕</button> :
                <button onClick={() => setCollapsed(true)}>➖</button>;

            let log = null;
            if (!collapsed) {
                let items = [];

                let i = 0;
                for (const log of line.log) {
                    const now = (
                        log.timestamp - runData.test_started_at
                    );
                    items.push(
                        <tr
                            key={items.length}
                            style={{color: tr.getColor(log.dbname)}}
                        >
                            <td>{now.toFixed(3)}</td>
                            <td className="num">{log.event}</td>
                            <td className="num">
                                {formatNum(log.value)}
                            </td>
                        </tr>
                    )
                }

                if (items.length) {
                    log = <tr><td colspan="100">
                        <div className="details">
                            <div className="connsLog">
                                <table>
                                    {items}
                                </table>
                            </div>
                            <TestClientDetails
                                data={line} runData={runData} tr={tr} />
                        </div>
                    </td></tr>;
                } else {
                    log = <tr><td colspan="100">
                        <div className="details">
                            <div className="connsLog">
                                <i>No log data for this tick.</i>
                            </div>
                            <TestClientDetails
                                data={line} runData={runData} tr={tr} />
                        </div>
                    </td></tr>;
                }
            }

            const now = (
                line.timestamp - runData.test_started_at
            );

            return <tbody className="connBoxes">
                <tr>
                    <td>{button}</td>
                    <td>{now.toFixed(3)}</td>
                    <td>
                        <ConnBar tr={tr} blocks={line.blocks} />
                    </td>
                    <td className="num">{line.capacity}</td>
                    <td className="smallNum">{waiters}</td>
                    <td className="smallNum">
                        {line.successful_connects}{' / '}
                        {line.successful_disconnects}
                    </td>
                </tr>
                {log}
            </tbody>
        }

        function TestClientDetails({data, runData, tr}) {
            const now = data.timestamp - runData.test_started_at;

            let loadTable = [];

            let i = 0;
            for (const db of tr.spec.dbs) {
                if (db.start_at < now && db.end_at > now) {
                    loadTable.push(
                        <tr style={{color: tr.getColor(db.db)}}
                            key={`${i}load`}
                        >
                            <td className="num">{db.qps}</td>
                            <td className="num">{db.query_cost_base
                                }s&nbsp;&plusmn;{db.query_cost_var}s</td>
                        </tr>
                    )
                }
            }

            if (loadTable.length) {
                loadTable = <table>
                    <tr>
                        <th>QpS</th>
                        <th>Query Cost</th>
                    </tr>
                    <tbody>{loadTable}</tbody>
                </table>
            } else {
                loadTable = null;
            }

            return <div className="popup">
                <h2>Client Load</h2>
                <div>
                    {loadTable ?? 'No load.'}
                </div>
            </div>
        }

        function TestView({results: tr, collapsible}) {
            const [collapsed, setCollapsed] = React.useState(collapsible);

            const runs = [];
            for (const runData of tr.runs) {
                const lines = [];
                for (const line of runData.stats) {
                    lines.push(
                        <TestLine
                            key={JSON.stringify(line)}
                            tr={tr}
                            runData={runData}
                            line={line}
                        />
                    );
                }

                runs.push(<div className="testRun" key={runData.pool_name}>
                    <div className="testHead">
                        <p>
                            Pool implementation:{' '}
                            <code>{runData.pool_name}</code>
                        </p>

                        <LatencyChart runData={runData} tr={tr}/>
                    </div>
                    <div className={`testServer ${collapsed ? 'hidden' : ''}`}>
                        <table className="conns">
                            <tr>
                                <th width="1%"></th>
                                <th width="5%">Test Time</th>
                                <th style={{maxWidth: '250px'}}>
                                    Allocation of {tr.spec.capacity}{' '}
                                    Connections</th>
                                <th width="5%">Conns</th>
                                <th width="20%">Waiters Avg/3 Ticks</th>
                                <th width="10%">
                                    Connects / Disconnects
                                </th>
                            </tr>

                            {lines}
                        </table>
                    </div>
                </div>);
            }

            let desc = null;
            if (tr.desc) {
                desc = [];
                const ds = tr.desc.split('\n\n');
                for (let i = 0; i < ds.length; i++) {
                    desc.push(<p key={i}>{ds[i]}</p>);
                }
                desc = <div className={collapsed ? 'hidden' : ''}>{desc}</div>;
            }

            let head = <h1>Simulation: {tr.test_name}</h1>;
            if (collapsible) {
                const button = collapsed ?
                    <button onClick={() => setCollapsed(false)}>➕</button> :
                    <button onClick={() => setCollapsed(true)}>➖</button>;
                head = <>{button}{head}</>;
            }
            head = <div className="testTopHead">{head}</div>;

            return <div className="testTop">
                <header>
                    {head}
                    <p>
                        Backend connection cost:{' '}
                        {tr.spec.conn_cost_base}&nbsp;&plusmn;&nbsp;
                        {tr.spec.conn_cost_var} (disconnect{': '}
                        {tr.spec.disconn_cost_base}&nbsp;&plusmn;&nbsp;
                        {tr.spec.disconn_cost_var})
                    </p>
                    {desc}
                </header>
                <div className="runs">
                    {runs}
                </div>
            </div>;
        }

        function AllResults({data}) {
            const items = [];
            for (const d of data) {
                const tr = new TestResults(d);
                items.push(
                    <TestView
                        results={tr} key={tr.test_name}
                        collapsible={data.length > 1} />
                );
            }
            return <>
                {items}
            </>
        }

        ReactDOM.render(
            <AllResults data={DATA} />,
            document.getElementById('root')
        );
    </script>
    </head>

    <body>
        <div id="root"></div>
    </body>
</html>
'''


if __name__ == '__main__':
    TestServerConnpoolSimulation().simulate_all_and_collect_stats()
