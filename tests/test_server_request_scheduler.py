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

    async def get_params(
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
    _costs: dict[str, int]

    # The desired results
    _results: list[TestResult] = field(default_factory=list)

    # The index of the current retry
    _try_index: int = -1

    def costs(self) -> dict[str, int]:
        return self._costs

    def create_request(self) -> TestTask:
        self._try_index += 1
        return TestTask(self)


class TestTask(rs.Request[TestData]):

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


class TestRequests(unittest.TestCase):

    @with_fake_event_loop
    async def test_timer_is_ready_01(self):
        await asyncio.sleep(10)

        self.assertTrue(rs.Timer(None, True).is_ready())
        self.assertTrue(rs.Timer(5, True).is_ready())
        self.assertTrue(rs.Timer(10, True).is_ready())
        self.assertFalse(rs.Timer(20, True).is_ready())
        self.assertFalse(rs.Timer(40, True).is_ready())
        self.assertFalse(rs.Timer(80, True).is_ready())

        self.assertTrue(rs.Timer(None, False).is_ready())
        self.assertTrue(rs.Timer(5, False).is_ready())
        self.assertTrue(rs.Timer(10, False).is_ready())
        self.assertFalse(rs.Timer(20, False).is_ready())
        self.assertFalse(rs.Timer(40, False).is_ready())
        self.assertFalse(rs.Timer(80, False).is_ready())

    @with_fake_event_loop
    async def test_timer_is_ready_and_urgent_01(self):
        await asyncio.sleep(10)

        self.assertTrue(rs.Timer(None, True).is_ready_and_urgent())
        self.assertTrue(rs.Timer(5, True).is_ready_and_urgent())
        self.assertTrue(rs.Timer(10, True).is_ready_and_urgent())
        self.assertFalse(rs.Timer(20, True).is_ready_and_urgent())
        self.assertFalse(rs.Timer(40, True).is_ready_and_urgent())
        self.assertFalse(rs.Timer(80, True).is_ready_and_urgent())

        self.assertFalse(rs.Timer(None, False).is_ready_and_urgent())
        self.assertFalse(rs.Timer(5, False).is_ready_and_urgent())
        self.assertFalse(rs.Timer(10, False).is_ready_and_urgent())
        self.assertFalse(rs.Timer(20, False).is_ready_and_urgent())
        self.assertFalse(rs.Timer(40, False).is_ready_and_urgent())
        self.assertFalse(rs.Timer(80, False).is_ready_and_urgent())

    @with_fake_event_loop
    async def test_timer_remaining_time_01(self):
        await asyncio.sleep(10)

        self.assertEqual(rs.Timer(None, True).remaining_time(30), 0)
        self.assertEqual(rs.Timer(5, True).remaining_time(30), 0)
        self.assertEqual(rs.Timer(10, True).remaining_time(30), 0.001)
        self.assertEqual(rs.Timer(20, True).remaining_time(30), 10.001)
        self.assertEqual(rs.Timer(40, True).remaining_time(30), 30)
        self.assertEqual(rs.Timer(80, True).remaining_time(30), 30)

        self.assertEqual(rs.Timer(None, False).remaining_time(30), 30)
        self.assertEqual(rs.Timer(5, False).remaining_time(30), 30)
        self.assertEqual(rs.Timer(10, False).remaining_time(30), 30)
        self.assertEqual(rs.Timer(20, False).remaining_time(30), 30)
        self.assertEqual(rs.Timer(40, False).remaining_time(30), 30)
        self.assertEqual(rs.Timer(80, False).remaining_time(30), 30)

    def test_timer_combine_01(self):
        self.assertEqual(
            rs.Timer.combine([
                rs.Timer(None, True),
            ]),
            rs.Timer(None, True),
        )
        self.assertEqual(
            rs.Timer.combine([
                rs.Timer(None, True),
                rs.Timer(10, True),
                rs.Timer(None, False),
                rs.Timer(10, False),
            ]),
            rs.Timer(None, True),
        )

        self.assertEqual(
            rs.Timer.combine([
                rs.Timer(10, True),
                rs.Timer(20, True),
                rs.Timer(30, True),
                rs.Timer(None, False),
                rs.Timer(10, False),
            ]),
            rs.Timer(10, True),
        )

        self.assertEqual(
            rs.Timer.combine([
                rs.Timer(None, False),
                rs.Timer(10, False),
                rs.Timer(20, False),
                rs.Timer(30, False),
            ]),
            rs.Timer(None, False),
        )

        self.assertEqual(
            rs.Timer.combine([
                rs.Timer(10, False),
                rs.Timer(20, False),
                rs.Timer(30, False),
            ]),
            rs.Timer(10, False),
        )

        self.assertIsNone(rs.Timer.combine([]))

    @with_fake_event_loop
    async def test_scheduler_process_01(self):
        # Processing does nothing if scheduler isn't ready

        context = rs.Context(naptime=0)

        # Not ready, not immediate
        self.assertFalse(await TestScheduler(
            service=rs.Service(),
            timer=rs.Timer(10, False),
        ).process(context))

        # Not ready, immediate
        self.assertFalse(await TestScheduler(
            service=rs.Service(),
            timer=rs.Timer(10, True),
        ).process(context))

        # Ready, not immediate
        self.assertTrue(await TestScheduler(
            service=rs.Service(),
            timer=rs.Timer(0, False),
        ).process(context))

        # Ready, immediate
        self.assertTrue(await TestScheduler(
            service=rs.Service(),
            timer=rs.Timer(0, True),
        ).process(context))

    @with_fake_event_loop
    async def test_scheduler_process_02(self):
        context = rs.Context(naptime=30)

        service = rs.Service(
            jitter=False,
            limits={'requests': rs.Limits(total=6)},
        )

        # Take a nap if nothing to do
        scheduler = TestScheduler(service=service, params=None)
        self.assertTrue(await scheduler.process(context))

        # Taking a nap
        self.assertEqual(
            scheduler.timer,
            rs.Timer(context.naptime, False),
        )

        # Empty report
        self.assertIsNone(scheduler.execution_report)

    @with_fake_event_loop
    async def test_scheduler_process_03(self):
        service = rs.Service(
            jitter=False,
            limits={'requests': rs.Limits(total=6, delay_factor=2)},
        )

        # All tasks succeed
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _costs={'requests': 1},
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
        self.assertEqual(
            scheduler.timer,
            rs.Timer(None, True),
        )

        # Results are finalized
        self.assertEqual(finalize_target, {1: 0})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 0}, report.deferred_costs)

        # Delay factor is decreased there are no deferred or errors
        self.assertEqual(
            {'requests': 1.9},
            {
                limit_name: limit.delay_factor
                for limit_name, limit in report.updated_limits.items()
            }
        )
        # Remaining is reset to None after processing
        self.assertEqual(
            {'requests': None},
            {
                limit_name: limit.remaining
                for limit_name, limit in report.updated_limits.items()
            }
        )

    @with_fake_event_loop
    async def test_scheduler_process_04(self):
        service = rs.Service(
            max_retry_count=1,
            jitter=False,
            limits={'requests': rs.Limits(total=6, delay_factor=2)},
        )

        # A task was deferred
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
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
        self.assertEqual(
            scheduler.timer,
            rs.Timer(44, True),
        )

        # Results are finalized
        self.assertEqual(finalize_target, {1: 0})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 2}, report.deferred_costs)

        # Delay factor is increased
        self.assertEqual(
            {'requests': 4},
            {
                limit_name: limit.delay_factor
                for limit_name, limit in report.updated_limits.items()
            }
        )
        # Remaining is reset to None after processing
        self.assertEqual(
            {'requests': None},
            {
                limit_name: limit.remaining
                for limit_name, limit in report.updated_limits.items()
            }
        )

    @with_fake_event_loop
    async def test_scheduler_process_05(self):
        service = rs.Service(
            max_retry_count=1,
            jitter=False,
            limits={'requests': rs.Limits(total=6, delay_factor=2)},
        )

        # A task has an error
        finalize_target: dict[int, float] = {}

        scheduler = TestScheduler(
            service=service,
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(data=rs.Error('Error', False)),
                    ]
                ),
            ]
        )
        context = rs.Context(naptime=30)

        self.assertTrue(await scheduler.process(context))

        # Run again after some delay, naptime is greater than delay
        self.assertEqual(
            scheduler.timer,
            rs.Timer(30, False)
        )

        # Results are finalized
        self.assertEqual(finalize_target, {})

        self.assertIsNotNone(scheduler.execution_report)
        report = scheduler.execution_report

        self.assertEqual(0, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual(['Error'], report.known_error_messages)
        self.assertEqual({'requests': 0}, report.deferred_costs)

        # Delay factor is unchanged
        self.assertEqual(
            {'requests': 2},
            {
                limit_name: limit.delay_factor
                for limit_name, limit in report.updated_limits.items()
            }
        )
        # Remaining is reset to None after processing
        self.assertEqual(
            {'requests': None},
            {
                limit_name: limit.remaining
                for limit_name, limit in report.updated_limits.items()
            }
        )

    @with_fake_event_loop
    async def test_service_next_delay_01(self):
        await asyncio.sleep(1000)

        # If there were errors, use a non-immediate delay

        success_count = 0
        deferred_costs = {'requests': 0}
        error_count = 1

        # No base delay, use naptime
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total='unlimited')},
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1030, False),
        )

        # If there is a base delay, the delay is factored and then limited to
        # a maximum value. Then if there was an error, the greater of the delay
        # and naptime is used.
        #
        # This is equivalent to `max(min(delay*factor, delay_max), naptime)`

        # delay*factor = 22 < delay_max < naptime
        self.assertAlmostEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6, delay_factor=2)},
                delay_max=30,
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=60,
            ),
            rs.Timer(1060, False),
        )

        # delay_max < delay*factor = 44 < naptime
        self.assertAlmostEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6, delay_factor=4)},
                delay_max=30,
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=60,
            ),
            rs.Timer(1060, False),
        )

        # naptime < delay*factor = 22 < delay_max
        self.assertAlmostEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6, delay_factor=2)},
                delay_max=30,
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=10,
            ),
            rs.Timer(1022, False),
        )

        # naptime < delay_max < delay*factor = 44
        self.assertAlmostEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6, delay_factor=4)},
                delay_max=30,
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=10,
            ),
            rs.Timer(1030, False),
        )

        # If no request limits are known, just nap
        self.assertEqual(
            rs.Service().next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1030, False),
        )

    @with_fake_event_loop
    async def test_service_next_delay_02(self):
        await asyncio.sleep(1000)

        # If there were no errors and some deferred, use an immediate delay

        success_count = 0
        deferred_costs = {'requests': 1}
        error_count = 0

        # No base delay, run immediately
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total='unlimited')},
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(None, True),
        )

        # Has delay, run immediately after delay
        self.assertAlmostEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6)},
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1011, True),
        )

    @with_fake_event_loop
    async def test_service_next_delay_03(self):
        await asyncio.sleep(1000)

        # If there were no errors or deferred, and some work was done
        # sucessfully, run immediately.

        success_count = 1
        deferred_costs = {'requests': 0}
        error_count = 0

        # No base delay, run immediately
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total='unlimited')}
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(None, True),
        )

        # Has delay, run immediately anyways
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6)}
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(None, True),
        )

    @with_fake_event_loop
    async def test_service_next_delay_04(self):
        await asyncio.sleep(1000)

        # If nothing was done, take a nap.

        success_count = 0
        deferred_costs = {'requests': 0}
        error_count = 0

        # No base delay, take a nap
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total='unlimited')}
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1030, False),
        )

        # Has delay, take a nap
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6)}
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1030, False),
        )

        # Has delay longer than naptime, use delay
        self.assertEqual(
            rs.Service(
                limits={'requests': rs.Limits(total=6, delay_factor=4)}
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1044, False),
        )

    @with_fake_event_loop
    async def test_service_next_delay_05(self):
        await asyncio.sleep(1000)

        # If there were no errors and some deferred, with multiple limits,
        # use an immediate delay with the highest value.

        success_count = 0
        deferred_costs = {'requests': 1, 'tokens': 2}
        error_count = 0

        # No base delay, run immediately
        self.assertEqual(
            rs.Service(
                limits={
                    'requests': rs.Limits(total='unlimited'),
                    'tokens': rs.Limits(total='unlimited'),
                },
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(None, True),
        )

        # Has delay, run immediately after delay
        self.assertAlmostEqual(
            rs.Service(
                limits={
                    'requests': rs.Limits(total=6),
                    'tokens': rs.Limits(total='unlimited'),
                },
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1011, True),
        )
        self.assertAlmostEqual(
            rs.Service(
                limits={
                    'requests': rs.Limits(total='unlimited'),
                    'tokens': rs.Limits(total=6),
                },
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1011, True),
        )
        self.assertAlmostEqual(
            rs.Service(
                limits={
                    'requests': rs.Limits(total=3),
                    'tokens': rs.Limits(total=6),
                },
            ).next_delay(
                success_count, deferred_costs, error_count, naptime=30,
            ),
            rs.Timer(1022, True),
        )

    def test_limits_update_total_01(self):
        # Check total takes the "latest" value
        self.assertEqual(
            rs.Limits(total=None).update_total(rs.Limits(total=None)),
            rs.Limits(total=None),
        )

        self.assertEqual(
            rs.Limits(total=None).update_total(rs.Limits(total=10)),
            rs.Limits(total=10),
        )

        self.assertEqual(
            rs.Limits(total=None).update_total(rs.Limits(total='unlimited')),
            rs.Limits(total='unlimited'),
        )

        self.assertEqual(
            rs.Limits(total=10).update_total(rs.Limits(total=None)),
            rs.Limits(total=10),
        )

        self.assertEqual(
            rs.Limits(total=10).update_total(rs.Limits(total=20)),
            rs.Limits(total=20),
        )

        self.assertEqual(
            rs.Limits(total=10).update_total(rs.Limits(total='unlimited')),
            rs.Limits(total='unlimited'),
        )

        self.assertEqual(
            rs.Limits(total='unlimited').update_total(rs.Limits(total=None)),
            rs.Limits(total='unlimited'),
        )

        self.assertEqual(
            rs.Limits(total='unlimited').update_total(rs.Limits(total='unlimited')),
            rs.Limits(total='unlimited'),
        )

        self.assertEqual(
            rs.Limits(total='unlimited').update_total(
                rs.Limits(total=10)
            ),
            rs.Limits(total=10),
        )

    def test_limits_update_remaining_01(self):
        # Check remaining takes the smallest available value
        self.assertEqual(
            rs.Limits(remaining=None).update_remaining(
                rs.Limits(remaining=None)
            ),
            rs.Limits(remaining=None),
        )

        self.assertEqual(
            rs.Limits(remaining=None).update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=10).update_remaining(
                rs.Limits(remaining=None)
            ),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=10).update_remaining(
                rs.Limits(remaining=20)
            ),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(remaining=20).update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(remaining=10),
        )

        # Check that a remaining value resets an unlimited total
        self.assertEqual(
            rs.Limits(total='unlimited', remaining=10).update_remaining(
                rs.Limits()
            ),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(total='unlimited').update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(remaining=10),
        )

        self.assertEqual(
            rs.Limits(total='unlimited', remaining=20).update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(remaining=10),
        )

        # Check that a remaining value does not reset a limited total
        self.assertEqual(
            rs.Limits(total=30, remaining=10).update_remaining(
                rs.Limits()
            ),
            rs.Limits(total=30, remaining=10),
        )

        self.assertEqual(
            rs.Limits(total=30).update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(total=30, remaining=10),
        )

        self.assertEqual(
            rs.Limits(total=30, remaining=20).update_remaining(
                rs.Limits(remaining=10)
            ),
            rs.Limits(total=30, remaining=10),
        )

    def test_limits_base_delay_01(self):
        # Unlimited limit has no delay.
        self.assertIsNone(
            rs.Limits(total='unlimited').base_delay(1, guess=60),
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
        limits = {'requests': rs.Limits(total='unlimited', delay_factor=1)}

        # All tasks return a valid result
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 0}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(True, report.updated_limits['requests'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)

        # No errors or deferred, delay factor is decreased, can't go below 1
        self.assertAlmostEqual(
            1, report.updated_limits['requests'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_02a(self):
        # Unlimited total
        limits = {'requests': rs.Limits(total='unlimited', delay_factor=2)}

        # A mix of successes and failures, no deferrals
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
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
                    _costs={'requests': 3}
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[TestResult(data=rs.Error('D', False))],
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0},
            finalize_target
        )

        self.assertEqual(2, report.success_count)
        self.assertEqual(1, report.unknown_error_count)
        self.assertEqual(['D'], report.known_error_messages)
        self.assertEqual({'requests': 0}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(True, report.updated_limits['requests'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)

        # Nothing was deferred, but there were errors, delay factor unchanged.
        self.assertAlmostEqual(
            2, report.updated_limits['requests'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_02b(self):
        # Unlimited total
        limits = {'requests': rs.Limits(total='unlimited', delay_factor=2)}

        # A mix of successes and failures, has unexpected deferrals
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[
                        TestResult(data=rs.Error('B', True)),
                        TestResult(data=rs.Error('B', True)),
                        TestResult(data=rs.Error('B', True)),
                        TestResult(data=rs.Error('B', True)),
                    ]
                ),
                TestParams(
                    _costs={'requests': 3}
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[TestResult(data=rs.Error('D', False))],
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0},
            finalize_target
        )

        self.assertEqual(1, report.success_count)
        self.assertEqual(1, report.unknown_error_count)
        self.assertEqual(['D'], report.known_error_messages)
        self.assertEqual({'requests': 2}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(True, report.updated_limits['requests'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)

        # Unexpected deferred requests, delay factor increased
        self.assertAlmostEqual(
            4, report.updated_limits['requests'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_03(self):
        # The total limit is finite
        limits = {'requests': rs.Limits(total=6, remaining=5, delay_factor=2)}

        # As many tasks as possible are run
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0},
            finalize_target
        )

        self.assertEqual(3, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 4}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(6, report.updated_limits['requests'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)

        # No unexpected deferred requests, delay factor unchanged
        self.assertAlmostEqual(
            2, report.updated_limits['requests'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_04(self):
        # The total limit is finite
        # But, a sufficient remaining limit is returned by some the result.
        limits = {'requests': rs.Limits(total=12, delay_factor=2)}

        # The first task returns an updated limit
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                            limits={'requests': rs.Limits(remaining=9)},
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 0}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(12, report.updated_limits['requests'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)

        # All succeed, delay factor reduced
        self.assertAlmostEqual(
            1.9, report.updated_limits['requests'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_05(self):
        # Two different unlimited totals
        limits = {
            'requests': rs.Limits(total='unlimited', delay_factor=2),
            'tokens': rs.Limits(total='unlimited', delay_factor=2),
        }

        # All tasks return a valid result
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1, 'tokens': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2, 'tokens': 2},
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 3, 'tokens': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4, 'tokens': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 0, 'tokens': 0}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests', 'tokens'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertTrue(6, report.updated_limits['requests'].total)
        self.assertTrue(20, report.updated_limits['tokens'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)
        self.assertIsNone(report.updated_limits['tokens'].remaining)

        # No errors or deferred, both delay factor decreased
        self.assertAlmostEqual(
            1.9, report.updated_limits['requests'].delay_factor
        )
        self.assertAlmostEqual(
            1.9, report.updated_limits['tokens'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_06(self):
        # Two different finite limits
        limits = {
            'requests': rs.Limits(total=6, remaining=6, delay_factor=2),
            'tokens': rs.Limits(total=20, remaining=20, delay_factor=3),
        }

        # As many tasks as possible are run
        finalize_target: dict[int, float] = {}

        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1, 'tokens': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2, 'tokens': 2},
                    _results=[
                        TestResult(
                            data=TestData(2),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 3, 'tokens': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4, 'tokens': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 5, 'tokens': 5},
                    _results=[
                        TestResult(
                            data=TestData(5),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0},
            finalize_target
        )

        self.assertEqual(3, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 9, 'tokens': 9}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests', 'tokens'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertEqual(6, report.updated_limits['requests'].total)
        self.assertEqual(20, report.updated_limits['tokens'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)
        self.assertIsNone(report.updated_limits['tokens'].remaining)

        # No unexpected deferred requests, delay factor unchanged
        self.assertAlmostEqual(
            2, report.updated_limits['requests'].delay_factor
        )
        self.assertAlmostEqual(
            3, report.updated_limits['tokens'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_07(self):
        # Two different finite limits
        limits = {
            'requests': rs.Limits(total=6, remaining=6, delay_factor=2),
            'tokens': rs.Limits(total=20, remaining=20, delay_factor=3),
        }

        # As many tasks as possible are run
        finalize_target: dict[int, float] = {}

        # An unexpected delay occurs, the delaying limit ('requests') is found
        report = await rs.execute_no_sleep(
            params=[
                TestParams(
                    _costs={'requests': 1, 'tokens': 1},
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 2, 'tokens': 2},
                    _results=[
                        TestResult(data=rs.Error('B', True)),
                        TestResult(data=rs.Error('B', True)),
                        TestResult(data=rs.Error('B', True)),
                        TestResult(
                            data=rs.Error('B', True),
                            limits={'requests': rs.Limits(remaining=1)},
                        ),
                    ]
                ),
                TestParams(
                    _costs={'requests': 3, 'tokens': 3},
                    _results=[
                        TestResult(
                            data=TestData(3),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 4, 'tokens': 4},
                    _results=[
                        TestResult(
                            data=TestData(4),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
                TestParams(
                    _costs={'requests': 5, 'tokens': 5},
                    _results=[
                        TestResult(
                            data=TestData(5),
                            finalize_target=finalize_target,
                        )
                    ]
                ),
            ],
            service=rs.Service(jitter=False, limits=limits),
        )

        self.assertEqual(
            {1: 0, 3: 0},
            finalize_target
        )

        self.assertEqual(2, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual({'requests': 11, 'tokens': 11}, report.deferred_costs)

        # Check limits
        self.assertEqual(
            {'requests', 'tokens'},
            set(report.updated_limits.keys())
        )

        # Total limit is unchanged
        self.assertEqual(6, report.updated_limits['requests'].total)
        self.assertEqual(20, report.updated_limits['tokens'].total)

        # Remaining limit is reset to None
        self.assertIsNone(report.updated_limits['requests'].remaining)
        self.assertIsNone(report.updated_limits['tokens'].remaining)

        # Deferred requests limit, delay factor increased
        # Tokens limit is ok, delay factor unchanged
        self.assertAlmostEqual(
            4, report.updated_limits['requests'].delay_factor
        )
        self.assertAlmostEqual(
            3, report.updated_limits['tokens'].delay_factor
        )

    @with_fake_event_loop
    async def test_execute_specified_01(self):
        # All tasks return a valid result
        results = await rs._execute_specified(
            params=[
                TestParams(
                    _costs={'requests': 1},
                    _results=[TestResult(data=TestData(1))],
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[TestResult(data=TestData(2))],
                ),
                TestParams(
                    _costs={'requests': 3},
                    _results=[TestResult(data=TestData(3))],
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[TestResult(data=TestData(4))],
                ),
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
                    _costs={'requests': 1},
                    _results=[TestResult(data=TestData(1))],
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[TestResult(data=rs.Error('B', True))],
                ),
                TestParams(
                    _costs={'requests': 3},
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[TestResult(data=rs.Error('D', False))],
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
                TestParams(
                    _costs={'requests': 1},
                    _results=[TestResult(data=TestData(1))],
                ),
                TestParams(
                    _costs={'requests': 2},
                    _results=[TestResult(data=TestData(2))],
                ),
                TestParams(
                    _costs={'requests': 3},
                    _results=[TestResult(data=TestData(3))],
                ),
                TestParams(
                    _costs={'requests': 4},
                    _results=[TestResult(data=TestData(4))],
                ),
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
