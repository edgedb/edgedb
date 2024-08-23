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


from __future__ import annotations
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
import sys
import textwrap
import time
import typing
import unittest
import unittest.mock

from edb.server import connpool
from edb.server.connpool import pool as pool_impl
from edb.tools.test import async_timeout

# TIME_SCALE is used to run the simulation for longer time, the default is 1x.
TIME_SCALE = int(os.environ.get("TIME_SCALE", '1'))

# Running this script individually for the simulation test will exit with
# code 0 if the final score is above MIN_SCORE, non-zero otherwise.
MIN_SCORE = int(os.environ.get('MIN_SCORE', 80))

# As the simulation test report is kinda large (~5MB with TIME_SCALE=10),
# the CI automation will delete old reports beyond CI_MAX_REPORTS.
CI_MAX_REPORTS = int(os.environ.get('CI_MAX_REPORTS', 50))

C = typing.TypeVar('C')


def with_base_test(m):
    @functools.wraps(m)
    def wrapper(self):
        if self.full_qps is None:
            self.full_qps = asyncio.run(
                asyncio.wait_for(self.base_test(), 30 * TIME_SCALE)
            )
        return m(self)
    return wrapper


def calc_percentiles(
    lats: typing.List[float]
) -> typing.Tuple[float, float, float, float, float, float, int]:
    lats_len = len(lats)
    lats.sort()
    return (
        lats[lats_len // 99],
        lats[lats_len // 4],
        lats[lats_len // 2],
        lats[lats_len * 3 // 4],
        lats[min(lats_len - lats_len // 99, lats_len - 1)],
        statistics.geometric_mean(lats),
        lats_len
    )


def calc_total_percentiles(
    lats: typing.Dict[str, typing.List[float]]
) -> typing.Tuple[float, float, float, float, float, float, int]:
    acc = []
    for i in lats.values():
        acc.extend(i)
    return calc_percentiles(acc)


@dataclasses.dataclass
class DBSpec:
    db: str
    start_at: float
    end_at: float
    qps: int
    query_cost_base: float
    query_cost_var: float


@dataclasses.dataclass
class ScoreMethod:
    # Calculates a score in 0 - 100 with the given value linearly scaled to the
    # four landmarks. v100 is the value that worth 100 points, and v0 is the
    # value worth nothing. There is no limit on either v100 or v0 is greater
    # than each other, but the landmarks should be either incremental
    # (v100 > v90 > v60 > v0) or decremental (v100 < v90 < v60 < v10).

    v100: float
    v90: float
    v60: float
    v0: float
    weight: float

    def _calculate(self, value: float) -> float:
        for v1, v2, base, diff in (
            (self.v100, self.v90, 90, 10),
            (self.v90, self.v60, 60, 30),
            (self.v60, self.v0, 0, 60),
        ):
            v_min = min(v1, v2)
            v_max = max(v1, v2)
            if v_min <= value < v_max:
                return base + abs(value - v2) / (v_max - v_min) * diff
        if self.v0 > self.v100:
            return 100 if value < self.v100 else 0
        else:
            return 0 if value < self.v0 else 100

    def calculate(self, sim: Simulation) -> float:
        raise NotImplementedError()


@dataclasses.dataclass
class LatencyDistribution(ScoreMethod):
    # Calculate the score using the average CV of quantiles. This evaluates how
    # the specified groups of latencies vary in distribution.

    group: range  # select a subset from sim.latencies using range()

    @staticmethod
    def calc_average_cv_of_quantiles(
        group_of_lats: typing.Iterable[typing.List[float]], n: int = 10
    ) -> float:
        # Calculates the average CV (coefficient of variation) of the given
        # distributions. The result is a float ranging from zero indicating
        # how different the given distributions are, where zero means no
        # difference. Known defect: when the mean value is close to zero, the
        # coefficient of variation will approach infinity and is therefore
        # sensitive to small changes.
        return statistics.geometric_mean(
            map(
                lambda v: statistics.pstdev(v) / statistics.fmean(v),
                zip(
                    *(
                        statistics.quantiles(lats, n=n)
                        for lats in group_of_lats
                    )
                ),
            )
        )

    def calculate(self, sim: Simulation) -> float:
        group = sim.group_of_latencies(f't{i}' for i in self.group)
        cv = self.calc_average_cv_of_quantiles(group)
        score = self._calculate(cv)
        sim.record_scoring(
            f'Average CV for {self.group}', cv, score, self.weight
        )
        return score * self.weight


@dataclasses.dataclass
class ConnectionOverhead(ScoreMethod):
    # Calculate the score based on the number of disconnects required to service
    # a query on average.

    def calculate(self, sim: Simulation) -> float:
        total = sum(map(lambda x: len(x), sim.latencies.values()))
        value = sim.stats[-1]["successful_disconnects"] / total
        score = self._calculate(value)
        sim.record_scoring(
            'Num of disconnects/query', value, score, self.weight
        )
        return score * self.weight


@dataclasses.dataclass
class PercentileBasedScoreMethod(ScoreMethod):
    percentile: str  # one of ('P1', 'P25', 'P50', 'P75', 'P99', 'Mean')

    def calc_average_percentile(self, sim: Simulation, group: range) -> float:
        # Calculate the arithmetic mean of the specified percentile of the
        # given groups of latencies.
        percentile_names = ('P1', 'P25', 'P50', 'P75', 'P99', 'Mean')
        percentile_index = percentile_names.index(self.percentile)
        return statistics.fmean(
            map(
                lambda lats: calc_percentiles(lats)[percentile_index],
                sim.group_of_latencies(f"t{i}" for i in group),
            )
        )


@dataclasses.dataclass
class LatencyRatio(PercentileBasedScoreMethod):
    # Calculate score based on the ratio of average percentiles between two
    # groups of latencies. This measures how close this ratio is from the
    # expected ratio (v100, v90, etc.).

    dividend: range
    divisor: range

    def calculate(self, sim: Simulation) -> float:
        dividend_percentile = self.calc_average_percentile(sim, self.dividend)
        divisor_percentile = self.calc_average_percentile(sim, self.divisor)
        ratio = dividend_percentile / divisor_percentile
        score = self._calculate(ratio)
        sim.record_scoring(
            f'{self.percentile} ratio {self.dividend}/{self.divisor}',
            ratio, score, self.weight
        )
        return score * self.weight


@dataclasses.dataclass
class AbsoluteLatency(PercentileBasedScoreMethod):
    # Calculate score based on the absolute average latency percentiles of the
    # specified group of latencies. This measures the absolute latency of
    # acquire latencies.

    group: range

    def calculate(self, sim: Simulation) -> float:
        value = self.calc_average_percentile(sim, self.group)
        score = self._calculate(value)
        sim.record_scoring(
            f'Average {self.percentile} of {self.group}',
            value, score, self.weight
        )
        return score * self.weight


@dataclasses.dataclass
class EndingCapacity(ScoreMethod):
    # Calculate the score based on the capacity at the end of the test

    def calculate(self, sim: Simulation) -> float:
        value = sim.stats[-1]["capacity"]
        score = self._calculate(value)
        sim.record_scoring(
            'Ending capacity', value, score, self.weight
        )
        return score * self.weight


@dataclasses.dataclass
class Spec:
    timeout: float
    duration: float
    capacity: int
    conn_cost_base: float
    conn_cost_var: float
    dbs: typing.List[DBSpec]
    desc: str = ''
    disconn_cost_base: float = 0.006
    disconn_cost_var: float = 0.0015
    score: typing.List[ScoreMethod] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.timeout *= TIME_SCALE
        self.duration *= TIME_SCALE
        for db in self.dbs:
            db.start_at *= TIME_SCALE
            db.end_at *= TIME_SCALE

    def asdict(self):
        rv = dataclasses.asdict(self)
        rv.pop('score')
        return rv


@dataclasses.dataclass
class Simulation:
    latencies: typing.Dict[str, typing.List[float]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )

    failed_disconnects: int = 0
    failed_queries: int = 0

    stats: typing.List[dict] = dataclasses.field(default_factory=list)
    scores: typing.List[dict] = dataclasses.field(default_factory=list)

    def group_of_latencies(
        self, keys: typing.Iterable[str]
    ) -> typing.Iterable[typing.List[float]]:
        for key in keys:
            yield self.latencies[key]

    def record_scoring(self, key, value, score, weight):
        self.scores.append({
            'name': key,
            'value': value if isinstance(value, int) else f'{value:.4f}',
            'score': f'{score:.1f}',
            'weight': f'{weight * 100:.0f}%',
        })
        if isinstance(value, int):
            kv = f'{key}: {value}'
        else:
            kv = f'{key}: {value:.4f}'
        score_str = f'score: {score:.1f}'
        weight_str = f'weight: {weight * 100:.0f}%'
        print(f'    {kv.ljust(40)} {score_str.ljust(15)} {weight_str}')


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


class SingleBlockPool(pool_impl.BasePool[C]):
    # used by the base test only

    _queue: asyncio.Queue[C]

    def __init__(
        self,
        *,
        connect,
        disconnect,
        max_capacity: int,
        stats_collector=None,
    ) -> None:
        super().__init__(
            connect=connect,
            disconnect=disconnect,
            max_capacity=max_capacity,
            stats_collector=stats_collector,
        )
        self._queue = asyncio.Queue(max_capacity)

    async def close(self) -> None:
        pass

    async def _async_connect(self, dbname: str) -> None:
        self.release(dbname, await self._connect_cb(dbname))

    async def acquire(self, dbname: str) -> C:
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            if self._cur_capacity < self._max_capacity:
                self._cur_capacity += 1
                self._get_loop().create_task(self._async_connect(dbname))
        return await self._queue.get()

    def release(self, dbname: str, conn: C) -> None:
        self._queue.put_nowait(conn)

    def count_waiters(self):
        return len(self._queue._getters)


class SimulatedCase(unittest.TestCase, metaclass=SimulatedCaseMeta):
    full_qps: typing.Optional[int] = None  # set by the base test

    def setUp(self) -> None:
        if not os.environ.get('EDGEDB_TEST_DEBUG_POOL'):
            raise unittest.SkipTest(
                "Skipped because EDGEDB_TEST_DEBUG_POOL is not set"
            )

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

    async def simulate_db(self, sim, pool, db):
        async with asyncio.TaskGroup() as g:
            await asyncio.sleep(db.start_at)
            len = int((db.end_at - db.start_at) * 1000.0)
            completed = 0
            for t in range(0, len):
                expected = int(t * db.qps / 1000.0)
                diff = expected - completed
                completed += diff
                if diff > 0:
                    g.create_task(self.simulate_queries(sim, pool, g, db,
                                                        t / 1000.0, diff))
            diff = int((db.end_at - db.start_at) * db.qps - completed)
            if diff > 0:
                g.create_task(self.simulate_queries(sim, pool, g, db,
                                                    t / 1000.0, diff))

    async def simulate_queries(self, sim, pool, g, db, delay, n):
        await asyncio.sleep(delay)
        for _ in range(n):
            dur = max(
                db.query_cost_base +
                random.triangular(
                    -db.query_cost_var, db.query_cost_var),
                0.001
            )
            g.create_task(
                self.make_fake_query(sim, pool, db.db, dur)
            )

    async def simulate_once(self, spec, pool_cls, *, collect_stats=False):
        from edb.server.connpool import config
        config.STATS_COLLECT_INTERVAL = 0.01

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
            min_idle_time_before_gc=0.1 * TIME_SCALE,
        )
        print(f"Simulating {pool.__class__}")

        started_at = time.monotonic()
        async with asyncio.TaskGroup() as g:
            for db in spec.dbs:
                g.create_task(self.simulate_db(sim, pool, db))

        self.assertEqual(sim.failed_disconnects, 0)
        self.assertEqual(sim.failed_queries, 0)

        self.assertEqual(pool.failed_disconnects, 0)
        self.assertEqual(pool.failed_connects, 0)

        try:
            for db in sim.latencies:
                int(db[1:])
        except ValueError:
            key_func = lambda x: x
        else:
            key_func = lambda x: int(x[0][1:])

        await pool.close()

        if collect_stats:
            pn = f'{type(pool).__module__}.{type(pool).__qualname__}'
            score = int(round(sum(sm.calculate(sim) for sm in spec.score)))
            print('weighted score:'.rjust(68), score)
            js_data = {
                'test_started_at': started_at,
                'total_lats': calc_total_percentiles(sim.latencies),
                "score": score,
                'scores': sim.scores,
                'lats': {
                    db: calc_percentiles(lats)
                    for db, lats in sorted(sim.latencies.items(), key=key_func)
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
        pools = [connpool.Pool, connpool.Pool2, connpool._NaivePool]

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
            'spec': spec.asdict(),
            'runs': js_data
        }

        return js_data

    async def _base_test_single(
        self, total_duration, qps, sim, pool, query_duration
    ):
        getters = 0
        TICK_EVERY = 0.001
        async with asyncio.TaskGroup() as g:
            db = DBSpec(db='', start_at=0, end_at=total_duration, qps=qps,
                        query_cost_base=query_duration, query_cost_var=0)
            task = g.create_task(self.simulate_db(sim, pool, db))
            while not task.done():
                await asyncio.sleep(TICK_EVERY)
                getters = max(getters, pool.count_waiters())
        return getters

    async def base_test(self) -> int:
        QUERY_DURATION = 0.01
        POOL_SIZE = 100
        verbose = bool(os.environ.get('EDGEDB_TEST_DEBUG_POOL'))
        qps = 100
        getters = 0
        sim = Simulation()

        connect = self.make_fake_connect(sim, 0, 0)
        disconnect = self.make_fake_connect(sim, 0, 0)

        pool: SingleBlockPool = SingleBlockPool(
            connect=connect,
            disconnect=disconnect,
            max_capacity=POOL_SIZE,
        )

        if verbose:
            print('Running the base test to detect the host capacity...')
            print(f'Query duration: {QUERY_DURATION * 1000:.0f}ms, '
                  f'pool size: {POOL_SIZE}')

        while pool._cur_capacity < 10 or getters < 100:
            qps = int(qps * 1.5)
            getters = await self._base_test_single(
                0.2, qps, sim, pool, QUERY_DURATION
            )
            if verbose:
                print(f'Increasing load: {qps} Q/s, {pool._cur_capacity} '
                      f'connections, {getters} waiters')

        if verbose:
            print("OK that's enough. Now go back slowly to find "
                  "the precise load.")
        qps_delta = int(qps / 30)
        last_qps = qps

        while getters > 10:
            last_qps = qps
            qps -= qps_delta
            getters = await self._base_test_single(
                0.35, qps, sim, pool, QUERY_DURATION
            )

            if verbose:
                print(f'Decreasing load: {qps} Q/s, {pool._cur_capacity} '
                      f'connections, {getters} waiters')

        qps = int((last_qps + qps) / 2)
        if verbose:
            print(f'Looks like {qps} is a just-enough Q/s to '
                  f'fully load the pool.')
        return qps

    def simulate_all_and_collect_stats(self) -> int:
        os.environ['EDGEDB_TEST_DEBUG_POOL'] = '1'
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
            print(f'Running {testname}...')
            js_data.append(
                asyncio.run(
                    self.simulate_and_collect_stats(testname, spec)))

        html = string.Template(HTML_TPL).safe_substitute(
            DATA=json.dumps(js_data))
        score = int(round(statistics.fmean(
            sim['runs'][0]['score'] for sim in js_data
        )))

        if os.environ.get("SIMULATION_CI"):
            self.write_ci_report(html, js_data, score)

        else:
            if not os.path.exists('tmp'):
                os.mkdir('tmp')
            with open(f'tmp/connpool.html', 'wt') as f:
                f.write(html)
            now = int(datetime.datetime.now().timestamp())
            with open(f'tmp/connpool_{now}.html', 'wt') as f:
                f.write(html)

        print('Final QoS score:', score)
        return score

    def write_ci_report(self, html, js_data, score):
        sha = os.environ.get('GITHUB_SHA')
        path = f'reports/{sha}.html'
        report_path = f'pool-simulation/{path}'
        i = 1
        while os.path.exists(report_path):
            path = f'reports/{sha}-{i}.html'
            report_path = f'pool-simulation/{path}'
            i += 1
        with open(report_path, 'wt') as f:
            f.write(html)
        try:
            with open(f'pool-simulation/reports.json') as f:
                reports = json.load(f)
        except Exception:
            reports = []
        reports.insert(0, {
            'path': path,
            'sha': sha,
            'ref': os.environ.get('GITHUB_REF'),
            'num_simulations': len(js_data),
            'qos_score': score,
            'datetime': str(datetime.datetime.now()),
        })
        with open(f'pool-simulation/reports.json', 'wt') as f:
            json.dump(reports[:CI_MAX_REPORTS], f)
        with open(f'pool-simulation/reports-archive.json', 'at') as f:
            for report in reports[:CI_MAX_REPORTS - 1:-1]:
                print('Removing outdated report:', report['path'])
                json.dump(report, f)
                print(file=f)
                try:
                    os.unlink(report['path'])
                except OSError as e:
                    print('ERROR:', e)


class TestServerConnpoolSimulation(SimulatedCase):

    def test_server_connpool_1(self):
        return Spec(
            desc='''
            This is a test for Mode D, where 2 groups of blocks race for
            connections in the pool with max capacity set to 6. The first group
            (0-5) has more dedicated time with the pool, so it should have
            relatively lower latency than the second group (6-11). But the QoS
            is focusing on the latency distribution similarity, as we don't
            want to starve only a few blocks because of the lack of capacity.
            Therefore, reconnection is a necessary cost for QoS.
            ''',
            timeout=20,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.05,
            conn_cost_var=0.01,
            score=[
                LatencyDistribution(
                    weight=0.18, group=range(6),
                    v100=0, v90=0.25, v60=0.5, v0=2
                ),
                LatencyDistribution(
                    weight=0.28, group=range(6, 12),
                    v100=0, v90=0.1, v60=0.3, v0=2
                ),
                LatencyDistribution(
                    weight=0.48, group=range(12),
                    v100=0.2, v90=0.45, v60=0.7, v0=2
                ),
                ConnectionOverhead(
                    weight=0.06, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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
            queries (0.001..0.006s), and 6x700qps connections that simulate
            slow queries (~0.03s). The algorithm allocates connections
            fairly to both groups, essentially using the
            "demand = avg_query_time * avg_num_of_connection_waiters"
            formula. The QoS is at the same level for all DBs. (Mode B / C)
            ''',
            timeout=20,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            score=[
                LatencyDistribution(
                    weight=0.15, group=range(6),
                    v100=0, v90=0.2, v60=0.3, v0=2
                ),
                LatencyDistribution(
                    weight=0.25, group=range(6, 12),
                    v100=0, v90=0.05, v60=0.2, v0=2
                ),
                LatencyDistribution(
                    weight=0.45, group=range(12),
                    v100=0.55, v90=0.75, v60=1.0, v0=2
                ),
                ConnectionOverhead(
                    weight=0.15, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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
            desc='''
            This test simply starts 6 same crazy requesters for 6 databases to
            test the pool fairness in Mode C with max capacity of 100.
            ''',
            timeout=10,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            score=[
                LatencyDistribution(
                    weight=0.85, group=range(6), v100=0, v90=0.1, v60=0.2, v0=2
                ),
                ConnectionOverhead(
                    weight=0.15, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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
            desc='''
            Similar to test 3, this test also has 6 requesters for 6 databases,
            they have the same Q/s but with different query cost. In Mode C,
            we should observe equal connection acquisition latency, fair and
            stable connection distribution and reasonable reconnection cost.
            ''',
            timeout=20,
            duration=1.1,
            capacity=50,
            conn_cost_base=0.04,
            conn_cost_var=0.011,
            score=[
                LatencyDistribution(
                    weight=0.9, group=range(6), v100=0, v90=0.1, v60=0.2, v0=2
                ),
                ConnectionOverhead(
                    weight=0.1, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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
            desc='''
            This is a mixed test with pool max capacity set to 6. Requests in
            the first group (0-5) come and go alternatively as time goes on,
            even with different query cost, so its latency similarity doesn't
            matter much, as far as the latency distribution is not too crazy
            and unstable. However the second group (6-11) has a stable
            environment - pressure from the first group is quite even at the
            time the second group works. So we should observe a high similarity
            in the second group. Also due to a low query cost, the second group
            should have a higher priority in connection acquisition, therefore
            a much lower latency distribution comparing to the first group.
            Pool Mode wise, we should observe a transition from Mode A to C,
            then D and eventually back to C. One regression to be aware of is
            that, the last D->C transition should keep the pool running at
            a full capacity.
            ''',
            timeout=30,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.15,
            conn_cost_var=0.05,
            score=[
                LatencyDistribution(
                    weight=0.05, group=range(6),
                    v100=0, v90=0.4, v60=0.8, v0=2
                ),
                LatencyDistribution(
                    weight=0.25, group=range(6, 12),
                    v100=0, v90=0.4, v60=0.8, v0=2
                ),
                LatencyRatio(
                    weight=0.45,
                    percentile='P75',
                    dividend=range(6),
                    divisor=range(6, 12),
                    v100=30, v90=5, v60=2, v0=1,
                ),
                ConnectionOverhead(
                    weight=0.15, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
                EndingCapacity(
                    weight=0.1, v100=6, v90=5, v60=4, v0=3
                ),
            ],
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
            desc='''
            This is a simple test for Mode A. In this case, we don't want to
            have lots of reconnection overhead.
            ''',
            timeout=10,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.15,
            conn_cost_var=0.05,
            score=[
                ConnectionOverhead(
                    weight=1.0, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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
            its queries processed as soon as they're submitted. Therefore,
            "t2" should have way lower connection acquisition cost than "t1".
            """,
            timeout=10,
            duration=1.1,
            capacity=6,
            conn_cost_base=0.05,
            conn_cost_var=0.01,
            score=[
                LatencyRatio(
                    weight=0.2,
                    percentile='P99',
                    dividend=range(1, 2),
                    divisor=range(2, 3),
                    v100=100, v90=50, v60=10, v0=1,
                ),
                LatencyRatio(
                    weight=0.4,
                    percentile='P75',
                    dividend=range(1, 2),
                    divisor=range(2, 3),
                    v100=200, v90=100, v60=20, v0=1,
                ),
                ConnectionOverhead(
                    weight=0.4, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
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

    @with_base_test
    def test_server_connpool_8(self):
        return Spec(
            desc='''
            This test spec is to check the pool connection reusability with a
            single block before the pool reaches its full capacity in Mode A.
            We should observe just enough number of connects to serve the load,
            while there can be very few disconnects because of GC.
            ''',
            timeout=20,
            duration=1.1,
            capacity=100,
            conn_cost_base=0,
            conn_cost_var=0,
            score=[
                ConnectionOverhead(
                    weight=1, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
            dbs=[
                DBSpec(
                    db='t1',
                    start_at=0,
                    end_at=0.1,
                    qps=int(self.full_qps / 32),
                    query_cost_base=0.01,
                    query_cost_var=0,
                ),
                DBSpec(
                    db='t1',
                    start_at=0.1,
                    end_at=0.2,
                    qps=int(self.full_qps / 16),
                    query_cost_base=0.01,
                    query_cost_var=0,
                ),
                DBSpec(
                    db='t1',
                    start_at=0.2,
                    end_at=0.6,
                    qps=int(self.full_qps / 8),
                    query_cost_base=0.01,
                    query_cost_var=0,
                ),
            ]
        )

    @with_base_test
    def test_server_connpool_9(self):
        return Spec(
            desc='''
            This test spec is to check the pool performance with low traffic
            between 3 pre-heated blocks in Mode B. t1 is a reference block,
            t2 has the same qps as t1, but t3 with doubled qps came in while t2
            is active. As the total throughput is low enough, we shouldn't have
            a lot of connects and disconnects, nor a high acquire waiting time.
            ''',
            timeout=20,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.01,
            conn_cost_var=0.005,
            score=[
                LatencyDistribution(
                    group=range(1, 4), weight=0.1,
                    v100=0.2, v90=0.5, v60=1.0, v0=2.0,
                ),
                AbsoluteLatency(
                    group=range(1, 4), percentile='P99', weight=0.1,
                    v100=0.001, v90=0.002, v60=0.004, v0=0.05
                ),
                AbsoluteLatency(
                    group=range(1, 4), percentile='P75', weight=0.2,
                    v100=0.0001, v90=0.0002, v60=0.0004, v0=0.005
                ),
                ConnectionOverhead(
                    weight=0.6, v100=0, v90=0.1, v60=0.2, v0=0.5
                ),
            ],
            dbs=[
                DBSpec(
                    db='t1',
                    start_at=0,
                    end_at=0.1,
                    qps=int(self.full_qps / 32),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t1',
                    start_at=0.1,
                    end_at=0.4,
                    qps=int(self.full_qps / 16),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t2',
                    start_at=0.5,
                    end_at=0.6,
                    qps=int(self.full_qps / 32),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t2',
                    start_at=0.6,
                    end_at=1.0,
                    qps=int(self.full_qps / 16),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t3',
                    start_at=0.7,
                    end_at=0.8,
                    qps=int(self.full_qps / 16),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t3',
                    start_at=0.8,
                    end_at=0.9,
                    qps=int(self.full_qps / 8),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
            ]
        )

    @with_base_test
    def test_server_connpool_10(self):
        return Spec(
            desc='''
            This test spec is to check the pool garbage collection feature.
            t1 is a constantly-running reference block, t2 starts in the middle
            with a full qps and ends early to leave enough time for the pool to
            execute garbage collection.
            ''',
            timeout=10,
            duration=1.1,
            capacity=100,
            conn_cost_base=0.01,
            conn_cost_var=0.005,
            score=[
                EndingCapacity(
                    weight=1.0, v100=10, v90=20, v60=40, v0=100,
                ),
            ],
            dbs=[
                DBSpec(
                    db='t1',
                    start_at=0,
                    end_at=1.0,
                    qps=int(self.full_qps / 32),
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
                DBSpec(
                    db='t2',
                    start_at=0.4,
                    end_at=0.6,
                    qps=int(self.full_qps / 32) * 31,
                    query_cost_base=0.01,
                    query_cost_var=0.005,
                ),
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

        @async_timeout(timeout=5)
        async def test(delay: float):
            event = asyncio.Event()

            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=10,
            )

            async with asyncio.TaskGroup() as g:
                g.create_task(q0(pool, event))
                await asyncio.sleep(delay)
                g.create_task(q1(pool, event))
                await asyncio.sleep(delay)
                g.create_task(q2(pool, event))

        async def main():
            await test(0.05)
            await test(0.000001)

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

        @async_timeout(timeout=5)
        async def test(delay: float):
            e1 = asyncio.Event()
            e2 = asyncio.Event()
            e3 = asyncio.Event()

            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=5,
            )

            async with asyncio.TaskGroup() as g:
                for _ in range(4):
                    g.create_task(q('A', pool, wait_event=e1))

                await asyncio.sleep(0.1)
                g.create_task(q('B', pool, set_event=e2, wait_event=e3))
                await e2.wait()
                g.create_task(q('B', pool, set_event=e3))

                await asyncio.sleep(0.1)
                e1.set()

        async def main():
            await test(0.05)

        asyncio.run(main())

    class MockLogger(logging.Logger):
        logs: asyncio.Queue

        def __init__(self):
            super().__init__('edb.server')

        def isEnabledFor(self, level):
            return True

        def _log(self, level, msg, args, *other, **kwargs):
            if len(args) > 1 and ('block_a' in args or 'block_b' in args):
                self.logs.put_nowait(args)

    @unittest.mock.patch('edb.server.connpool.pool.logger',
                         new_callable=MockLogger)
    @unittest.mock.patch('edb.server.connpool.config.MIN_LOG_TIME_THRESHOLD',
                         0.2)
    def test_connpool_log_batching(self, logger: MockLogger):
        @async_timeout(timeout=5)
        async def test():
            pool = connpool.Pool(
                connect=self.make_fake_connect(),
                disconnect=self.make_fake_disconnect(),
                max_capacity=5,
            )
            if hasattr(pool, '_pool'):
                raise unittest.SkipTest("Pool2 doesn't support this logger")

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
            if "1 were discarded, 1 were established" not in args:
                self.assertIn("1 were established, 1 were discarded", args)
            self.assertIn("block_a", args)
            self.assertGreater(time.monotonic() - start, 0.2)

            pool.release("block_b", conn2, discard=True)
            start = time.monotonic()
            args = await logger.logs.get()
            if 'discarded' not in args:
                self.assertIn("established", args)
            self.assertIn("block_b", args)
            self.assertLess(time.monotonic() - start, 0.2)

        async def main():
            logger.logs = asyncio.Queue()
            await test()

        asyncio.run(main())

    @unittest.mock.patch('edb.server.connpool.pool.logger.level',
                         logging.CRITICAL)
    @unittest.mock.patch('edb.server.connpool.pool2.logger.level',
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

        @async_timeout(timeout=1)
        async def test():
            pool = connpool.Pool(
                connect=fake_connect,
                disconnect=fake_disconnect,
                max_capacity=5,
            )
            with self.assertRaises(error_type):
                await pool.acquire("block_a")
            self.assertEqual(connect_called_num, expected_connects,
                             f"Expected {expected_connects} connect(s), "
                             f"got {connect_called_num}")
            self.assertEqual(disconnect_called_num, 0)
            with self.assertRaises(error_type):
                await pool.acquire("block_a")
            if hasattr(pool, '_pool'):
                self.assertEqual(connect_called_num, expected_connects * 2,
                                f"Expected {expected_connects * 2} connect(s), "
                                f"got {connect_called_num}")
            else:
                self.assertEqual(connect_called_num, expected_connects + 1,
                                f"Expected {expected_connects + 1} connect(s), "
                                f"got {connect_called_num}")
            self.assertEqual(disconnect_called_num, 0)

        async def main():
            await test()

        asyncio.run(main())

    @unittest.mock.patch('edb.server.connpool.config.CONNECT_FAILURE_RETRIES',
                         2)
    def test_connpool_connect_error(self):
        from edb.server.pgcon import errors

        class BackendError(errors.BackendError):
            def __init__(self):
                super().__init__(fields={'C': '3D000'})

        self._test_connpool_connect_error(BackendError, 1)

        class ConnectError(Exception):
            pass

        self._test_connpool_connect_error(ConnectError, 3)

    @unittest.mock.patch('edb.server.connpool.config.CONNECT_FAILURE_RETRIES',
                         0)
    def test_connpool_connect_error_zero_retry(self):
        class ConnectError(Exception):
            pass

        self._test_connpool_connect_error(ConnectError, 1)

    @unittest.mock.patch('edb.server.connpool.pool.logger',
                         new_callable=MockLogger)
    @unittest.mock.patch('edb.server.connpool.config.MIN_LOG_TIME_THRESHOLD', 0)
    @unittest.mock.patch('edb.server.connpool.config.CONNECT_FAILURE_RETRIES',
                         5)
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

        @async_timeout(timeout=5)
        async def test():
            pool = connpool.Pool(
                connect=fake_connect,
                disconnect=self.make_fake_disconnect(),
                max_capacity=2,
            )
            if hasattr(pool, '_pool'):
                raise unittest.SkipTest("Pool2 doesn't support this logger")

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
            await test()

        asyncio.run(main())

    def test_connpool_eternal_starvation(self):
        async def fake_connect(dbname):
            # very fast connect
            return FakeConnection(dbname)

        @async_timeout(timeout=3)
        async def test():
            pool = connpool.Pool(
                connect=fake_connect,
                disconnect=self.make_fake_disconnect(),
                max_capacity=5,
            )
            counter = 0
            event = asyncio.Event()

            async def job(dbname):
                nonlocal counter

                conn = await pool.acquire(dbname)
                counter += 1
                if counter >= 5:
                    event.set()
                await event.wait()
                pool.release(dbname, conn)

            async with asyncio.TaskGroup() as g:
                for n in range(10):
                    g.create_task(job(f"block_{n}"))

        async def main():
            await test()

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

        function ScoreLine({score, scores}) {
            const [collapsed, setCollapsed] = React.useState(true);

            const button = collapsed ?
                <button onClick={() => setCollapsed(false)}>➕</button> :
                <button onClick={() => setCollapsed(true)}>➖</button>;

            let scoreDetail = null;
            if (!collapsed) {
                let items = [];
                for (const detail of scores) {
                    items.push(
                        <tr>
                            <td>{detail.name}</td>
                            <td><code>{detail.value}</code></td>
                            <td><code>{detail.score}</code></td>
                            <td><code>{detail.weight}</code></td>
                        </tr>
                    );
                }

                scoreDetail = <table>
                    <tr>
                        <th>Reason</th>
                        <th>Value</th>
                        <th>Score</th>
                        <th>Weight</th>
                    </tr>
                    {items}
                </table>;
            }

            return <p>
                {button} QoS Score:{' '}
                <code>{score}</code>
                {scoreDetail}
            </p>;
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
                        <ScoreLine score={runData.score}
                                   scores={runData.scores}/>

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


def run():
    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    test_sim = TestServerConnpoolSimulation()
    if test_sim.simulate_all_and_collect_stats() < MIN_SCORE:
        print(
            f'WARNING: the score is below the bar ({MIN_SCORE}), please '
            f'double check the changes made to edb/server/connpool/pool.py'
        )
        sys.exit(1)


if __name__ == '__main__':
    run()
