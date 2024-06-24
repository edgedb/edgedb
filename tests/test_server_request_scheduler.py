#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
from dataclasses import dataclass, field
from typing import Optional, Sequence

import asyncio
import unittest

from edb.server.protocol import request_scheduler as rs
from edb.testbase.asyncutils import with_fake_event_loop


try:
    import async_solipsism
except ImportError:
    async_solipsism = None  # type: ignore


# Simulate tasks which returns a TestData containing an int.
#
# TestResult keeps track of when it has returned "created" and whether it has
# been finalized.
#
# TestParams and TestTask are defined so that the params already have the
# desired TestResults, and TestTask fetches and returns it.
#
# When testing retry behaviour, TestTask will return each provided TestResult
# in sequence.


@dataclass
class TestScheduler(rs.Scheduler):

    params: Optional[list[TestParams]] = None

    execution_report: Optional[rs.ExecutionReport] = None

    async def get_task_params(
        self, context: rs.Context,
    ) -> Optional[Sequence[rs.Params]]:
        return self.params

    def finalize(self, execution_report: rs.ExecutionReport) -> None:
        self.execution_report = execution_report


@dataclass(frozen=True)
class TestData:
    value: int


class TestResult(rs.Result[TestData]):

    # The time this result was "produced"
    time: float = -1

    # when finalized, log the data value and time
    finalize_target: Optional[dict[int, float]] = None

    def __init__(
        self,
        finalize_target: Optional[dict[int, float]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.finalize_target = finalize_target

    async def finalize(self) -> None:
        if self.finalize_target is not None and isinstance(self.data, TestData):
            self.finalize_target[self.data.value] = self.time


@dataclass
class TestParams(rs.Params[TestData]):

    # Cost multiplier used to factor the rate delay
    _cost: int

    # The desired results
    _results: list[TestResult] = field(default_factory=list)

    # The index of the current retry
    _try_index: int = -1

    def cost(self) -> int:
        return self._cost

    def create_task(self) -> TestTask:
        self._try_index += 1
        return TestTask(self)


class TestTask(rs.Task[TestData]):

    def __init__(self, params: TestParams):
        super().__init__(params=params)

    async def run(self) -> Optional[TestResult]:
        assert isinstance(self.params, TestParams)
        if self.params._try_index < len(self.params._results):
            result = self.params._results[self.params._try_index]
            result.time = asyncio.get_running_loop().time()

            return result

        else:
            return None


@unittest.skipIf(async_solipsism is None, 'async_solipsism is missing')
class TestRequests(unittest.TestCase):

    @with_fake_event_loop
    async def test_scheduler_process_01(self):
        # Processing does nothing if scheduler isn't ready

        context = rs.Context(naptime=0)

        # Not ready, not immediate
        self.assertFalse(await TestScheduler(
            ready_time=10,
            execute_immediately=False,
        ).process(context))

        # Not ready, immediate
        self.assertFalse(await TestScheduler(
            ready_time=10,
            execute_immediately=True,
        ).process(context))

        # Ready, not immediate
        self.assertTrue(await TestScheduler(
            ready_time=0,
            execute_immediately=False,
        ).process(context))

        # Ready, immediate
        self.assertTrue(await TestScheduler(
            ready_time=0,
            execute_immediately=True,
        ).process(context))

    @with_fake_event_loop
    async def test_scheduler_process_02(self):
        context = rs.Context(naptime=30)

        service = rs.Service(jitter=False, request_limits=rs.Limits(total=6))

        scheduler = TestScheduler(service=service, params=None)
        self.assertTrue(await scheduler.process(context))

        # Taking a nap
        self.assertEqual(scheduler.ready_time, context.naptime)
        self.assertFalse(scheduler.execute_immediately)

    @with_fake_event_loop
    async def test_scheduler_process_03(self):
        service = rs.Service(
            jitter=False,
            request_limits=rs.Limits(total=6, delay_factor=2)
        )

        # All tasks succeed
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ]
        )
        context = rs.Context(naptime=30)

        self.assertTrue(await scheduler.process(context))

        # Run again right away to see if there's more work
        self.assertIsNone(scheduler.ready_time)
        self.assertTrue(scheduler.execute_immediately)

        # Results are finalized
        self.assertEqual(finalize_target, {1: 0})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.deferred_cost)

        # Delay factor is decreased there are no deferred or errors
        self.assertEqual(1.9, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_scheduler_process_04(self):
        service = rs.Service(
            max_retry_count=1,
            jitter=False,
            request_limits=rs.Limits(total=6, delay_factor=2)
        )

        # A task was deferred
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ]
        )
        context = rs.Context(naptime=30)

        self.assertTrue(await scheduler.process(context))

        # Run again after some delay, delay factor increased to 4
        self.assertAlmostEqual(scheduler.ready_time, 44)
        self.assertTrue(scheduler.execute_immediately)

        # Results are finalized
        self.assertEqual(finalize_target, {1: 0})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(2, report.deferred_cost)

        # Delay factor is increased
        self.assertEqual(4, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_scheduler_process_05(self):
        service = rs.Service(
            max_retry_count=1,
            jitter=False,
            request_limits=rs.Limits(total=6, delay_factor=2)
        )

        # A task has an error
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(data=rs.Error('Error', False)),
                    ]
                ),
            ]
        )
        context = rs.Context(naptime=30)

        self.assertTrue(await scheduler.process(context))

        # Run again after some delay, naptime is greater than delay
        self.assertAlmostEqual(scheduler.ready_time, 30)
        self.assertFalse(scheduler.execute_immediately)

        # Results are finalized
        self.assertEqual(finalize_target, {})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(0, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual(['Error'], report.known_error_messages)
        self.assertEqual(0, report.deferred_cost)

        # Delay factor is unchanged
        self.assertEqual(2, report.updated_limits.delay_factor)

    def test_scheduler_get_combined_ready_time_01(self):
        self.assertTrue(
            TestScheduler.get_combined_ready_time([
                TestScheduler(ready_time=None, execute_immediately=True),
            ])
        )
        self.assertTrue(
            TestScheduler.get_combined_ready_time([
                TestScheduler(ready_time=None, execute_immediately=True),
                TestScheduler(ready_time=10, execute_immediately=True),
                TestScheduler(ready_time=None, execute_immediately=False),
                TestScheduler(ready_time=10, execute_immediately=False),
            ])
        )

        self.assertEqual(
            TestScheduler.get_combined_ready_time([
                TestScheduler(ready_time=10, execute_immediately=True),
                TestScheduler(ready_time=20, execute_immediately=True),
                TestScheduler(ready_time=30, execute_immediately=True),
                TestScheduler(ready_time=None, execute_immediately=False),
                TestScheduler(ready_time=10, execute_immediately=False),
            ]),
            10,
        )

        self.assertFalse(
            TestScheduler.get_combined_ready_time([
                TestScheduler(ready_time=None, execute_immediately=False),
                TestScheduler(ready_time=10, execute_immediately=False),
                TestScheduler(ready_time=20, execute_immediately=False),
                TestScheduler(ready_time=30, execute_immediately=False),
            ]),
            10,
        )

    def test_service_next_delay_01(self):
        # If there were errors, use a non-immediate delay

        success_count = 0
        deferred_count = 0
        error_count = 1

        # No base delay, use naptime
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

        # If there is a base delay, the delay is factored and then limited to
        # a maximum value. Then if there was an error, the greater of the delay
        # and naptime is used.
        #
        # This is equivalent to `max(min(delay*factor, delay_max), naptime)`

        # delay*factor = 22 < delay_max < naptime
        self.assertAlmostEqual(
            rs.Service(
                request_limits=rs.Limits(total=6, delay_factor=2),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=60,
            ),
            (60, False),
        )

        # delay_max < delay*factor = 44 < naptime
        self.assertAlmostEqual(
            rs.Service(
                request_limits=rs.Limits(total=6, delay_factor=4),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=60,
            ),
            (60, False),
        )

        # naptime < delay*factor = 22 < delay_max
        self.assertAlmostEqual(
            rs.Service(
                request_limits=rs.Limits(total=6, delay_factor=2),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=10,
            ),
            (22, False),
        )

        # naptime < delay_max < delay*factor = 44
        self.assertAlmostEqual(
            rs.Service(
                request_limits=rs.Limits(total=6, delay_factor=4),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=10,
            ),
            (30, False),
        )

        # If no request limits are known, just nap
        self.assertEqual(
            rs.Service().next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

    def test_service_next_delay_02(self):
        # If there were no errors and some deferred, use an immediate delay

        success_count = 0
        deferred_count = 1
        error_count = 0

        # No base delay, run immediately
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (None, True),
        )

        # Has delay, run immediately after delay
        self.assertAlmostEqual(
            rs.Service(request_limits=rs.Limits(total=6)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (11, True),
        )

    def test_service_next_delay_03(self):
        # If there were no errors or deferred, and some work was done
        # sucessfully, run immediately.

        success_count = 1
        deferred_count = 0
        error_count = 0

        # No base delay, run immediately
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (None, True),
        )

        # Has delay, run immediately anyways
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=6)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (None, True),
        )

    def test_service_next_delay_04(self):
        # If nothing was done, take a nap.

        success_count = 0
        deferred_count = 0
        error_count = 0

        # No base delay, take a nap
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

        # Has delay, take a nap
        self.assertEqual(
            rs.Service(request_limits=rs.Limits(total=6)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

    def test_limits_update_01(self):
        # Check total takes the "latest" value
        self.assertEqual(
            rs.Limits(total=None).update(rs.Limits(total=None)),
            rs.Limits(total=None),
        )

        self.assertEqual(
            rs.Limits(total=None).update(rs.Limits(total=10)),
            rs.Limits(total=10),
        )

        self.assertEqual(
            rs.Limits(total=None).update(rs.Limits(total=True)),
            rs.Limits(total=True),
        )

        self.assertEqual(
            rs.Limits(total=10).update(rs.Limits(total=None)),
            rs.Limits(total=10),
        )

        self.assertEqual(
            rs.Limits(total=10).update(rs.Limits(total=20)),
            rs.Limits(total=20),
        )

        self.assertEqual(
            rs.Limits(total=10).update(rs.Limits(total=True)),
            rs.Limits(total=True),
        )

        self.assertEqual(
            rs.Limits(total=True).update(rs.Limits(total=None)),
            rs.Limits(total=True),
        )

        self.assertEqual(
            rs.Limits(total=True).update(rs.Limits(total=True)),
            rs.Limits(total=True),
        )

        self.assertEqual(
            rs.Limits(total=True).update(rs.Limits(total=10)),
            rs.Limits(total=10),
        )

        # Check remaining takes the smallest available value
        self.assertEqual(
            rs.Limits(remaining=None).update(rs.Limits(remaining=None)),
            rs.Limits(remaining=None),
        )

        self.assertEqual(
            rs.Limits(remaining=None).update(rs.Limits(remaining=10)),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=10).update(rs.Limits(remaining=None)),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=10).update(rs.Limits(remaining=20)),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=20).update(rs.Limits(remaining=10)),
            rs.Limits(remaining=10),
        )

        # Check that a remaining value resets an unlimited total
        self.assertEqual(
            rs.Limits(total=True, remaining=10).update(rs.Limits()),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(total=True).update(rs.Limits(remaining=10)),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(total=True, remaining=20).update(rs.Limits(remaining=10)),
            rs.Limits(remaining=10),
        )
        # Check total takes the "latest" value

    def test_limits_base_delay_01(self):
        # Unlimited limit has no delay.
        self.assertIsNone(
            rs.Limits(total=True).base_delay(1, guess=60),
        )

        # If number of requests is less than remaining limit, no delay needed.
        self.assertIsNone(
            rs.Limits(remaining=4).base_delay(1, guess=60),
        )

        # If total limit exists, rate is based on limit.
        self.assertAlmostEqual(
            11,
            rs.Limits(total=6).base_delay(1, guess=60),
        )
        self.assertAlmostEqual(
            11,
            rs.Limits(total=6, remaining=4).base_delay(99, guess=60),
        )

        # Otherwise, just use the minimum guess
        self.assertAlmostEqual(
            20,
            rs.Limits().base_delay(1, guess=20),
        )
        self.assertAlmostEqual(
            20,
            rs.Limits(remaining=4).base_delay(99, guess=20),
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_01(self):
        # Unlimited total
        request_limits = rs.Limits(total=True, delay_factor=1)

        # All tasks return a valid result
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=3,
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=4,
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.deferred_cost)

        # No errors or deferred, delay factor is decreased, can't go below 1
        self.assertEqual(1, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_02(self):
        # Unlimited total
        request_limits = rs.Limits(total=True, delay_factor=2)

        # A mix of successes and failures
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        # successful retry
                        TestResult(data=rs.Error('B', True)),
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        ),
                    ]
                ),
                TestParams(
                    _cost=3
                ),
                TestParams(
                    _cost=4, _results=[TestResult(data=rs.Error('D', False))]
                ),
                TestParams(
                    _cost=5,
                    _results=[
                        # unsuccessful retry
                        TestResult(data=rs.Error('E', True)),
                        TestResult(data=rs.Error('E', True)),
                        TestResult(data=rs.Error('E', True)),
                        TestResult(data=rs.Error('E', True)),
                        TestResult(data=rs.Error('E', True)),
                    ]
                ),
            ],
            service=rs.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0},
            finalize_target
        )

        self.assertEqual(2, report.success_count)
        self.assertEqual(1, report.unknown_error_count)
        self.assertEqual(['D'], report.known_error_messages)
        self.assertEqual(5, report.deferred_cost)

        # Deferred request, delay factor is increased
        self.assertEqual(4, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_03(self):
        # The total limit is finite
        request_limits = rs.Limits(total=6, remaining=5, delay_factor=2)

        # Only the first task will be run
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=3,
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=4,
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0},
            finalize_target
        )

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(9, report.deferred_cost)

        # Deferred requests, delay factor increased
        self.assertEqual(4, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_04(self):
        # The total limit is finite
        # But, a sufficient remaining limit is returned by some the result.
        request_limits = rs.Limits(total=12, delay_factor=2)

        # Only the first task returns an updated limit
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                            request_limits=rs.Limits(remaining=9),
                        )
                    ]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=3,
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _cost=4,
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.deferred_cost)

        # All succeed, delay factor reduced
        self.assertEqual(1.9, report.updated_limits.delay_factor)
        # remaining stays None
        self.assertIsNone(report.updated_limits.remaining)

    @with_fake_event_loop
    async def test_execute_specified_01(self):
        # All tasks return a valid result
        results = await rs._execute_specified(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[0, 1, 2, 3],
        )

        self.assertEqual(
            {
                0: TestResult(data=TestData(1)),
                1: TestResult(data=TestData(2)),
                2: TestResult(data=TestData(3)),
                3: TestResult(data=TestData(4)),
            },
            results,
        )

        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [0, 0, 0, 0])

    @with_fake_event_loop
    async def test_execute_specified_02(self):
        # A mix of successes and failures
        results = await rs._execute_specified(
            params=[
                TestParams(
                    _cost=1, _results=[TestResult(data=TestData(1))]
                ),
                TestParams(
                    _cost=2, _results=[TestResult(data=rs.Error('B', True))]
                ),
                TestParams(
                    _cost=3
                ),
                TestParams(
                    _cost=4, _results=[TestResult(data=rs.Error('D', False))]
                ),
            ],
            indexes=[0, 1, 2, 3],
        )

        self.assertEqual(
            {
                0: TestResult(data=TestData(1)),
                1: TestResult(data=rs.Error('B', True)),
                3: TestResult(data=rs.Error('D', False)),
            },
            results,
        )

        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [0, 0, 0])

    @with_fake_event_loop
    async def test_execute_specified_03(self):
        # Run only some tasks
        results = await rs._execute_specified(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[1, 3],
        )

        self.assertEqual(
            {
                1: TestResult(data=TestData(2)),
                3: TestResult(data=TestData(4)),
            },
            results,
        )

        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [0, 0])
