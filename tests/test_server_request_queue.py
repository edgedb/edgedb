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
from typing import Optional

import asyncio
import unittest

from edb.server.protocol import request_queue as rq
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

@dataclass(frozen=True)
class TestData:
    value: int


class TestResult(rq.Result[TestData]):

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
class TestParams(rq.Params[TestData]):

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


class TestTask(rq.Task[TestData]):

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

    def test_service_next_delay_01(self):
        # If there were errors, use a non-immediate delay

        success_count = 0
        deferred_count = 0
        error_count = 1

        # No base delay, use naptime
        self.assertEqual(
            rq.Service(request_limits=rq.Limits(total=True)).next_delay(
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
            rq.Service(
                request_limits=rq.Limits(total=6, delay_factor=2),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=60,
            ),
            (60, False),
        )

        # delay_max < delay*factor = 44 < naptime
        self.assertAlmostEqual(
            rq.Service(
                request_limits=rq.Limits(total=6, delay_factor=4),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=60,
            ),
            (60, False),
        )

        # naptime < delay*factor = 22 < delay_max
        self.assertAlmostEqual(
            rq.Service(
                request_limits=rq.Limits(total=6, delay_factor=2),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=10,
            ),
            (22, False),
        )

        # naptime < delay_max < delay*factor = 44
        self.assertAlmostEqual(
            rq.Service(
                request_limits=rq.Limits(total=6, delay_factor=4),
                delay_max=30,
            ).next_delay(
                success_count, deferred_count, error_count, naptime=10,
            ),
            (30, False),
        )

        # If no request limits are known, just nap
        self.assertEqual(
            rq.Service().next_delay(
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
            rq.Service(request_limits=rq.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (None, True),
        )

        # Has delay, run immediately after delay
        self.assertAlmostEqual(
            rq.Service(request_limits=rq.Limits(total=6)).next_delay(
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
            rq.Service(request_limits=rq.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (None, True),
        )

        # Has delay, run immediately anyways
        self.assertEqual(
            rq.Service(request_limits=rq.Limits(total=6)).next_delay(
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
            rq.Service(request_limits=rq.Limits(total=True)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

        # Has delay, take a nap
        self.assertEqual(
            rq.Service(request_limits=rq.Limits(total=6)).next_delay(
                success_count, deferred_count, error_count, naptime=30,
            ),
            (30, False),
        )

    def test_limits_update_01(self):
        # Check total takes the "latest" value
        self.assertEqual(
            rq.Limits(total=None).update(rq.Limits(total=None)),
            rq.Limits(total=None),
        )

        self.assertEqual(
            rq.Limits(total=None).update(rq.Limits(total=10)),
            rq.Limits(total=10),
        )

        self.assertEqual(
            rq.Limits(total=None).update(rq.Limits(total=True)),
            rq.Limits(total=True),
        )

        self.assertEqual(
            rq.Limits(total=10).update(rq.Limits(total=None)),
            rq.Limits(total=10),
        )

        self.assertEqual(
            rq.Limits(total=10).update(rq.Limits(total=20)),
            rq.Limits(total=20),
        )

        self.assertEqual(
            rq.Limits(total=10).update(rq.Limits(total=True)),
            rq.Limits(total=True),
        )

        self.assertEqual(
            rq.Limits(total=True).update(rq.Limits(total=None)),
            rq.Limits(total=True),
        )

        self.assertEqual(
            rq.Limits(total=True).update(rq.Limits(total=True)),
            rq.Limits(total=True),
        )

        self.assertEqual(
            rq.Limits(total=True).update(rq.Limits(total=10)),
            rq.Limits(total=10),
        )

        # Check remaining takes the smallest available value
        self.assertEqual(
            rq.Limits(remaining=None).update(rq.Limits(remaining=None)),
            rq.Limits(remaining=None),
        )

        self.assertEqual(
            rq.Limits(remaining=None).update(rq.Limits(remaining=10)),
            rq.Limits(remaining=10),
        )

        self.assertEqual(
            rq.Limits(remaining=10).update(rq.Limits(remaining=None)),
            rq.Limits(remaining=10),
        )

        self.assertEqual(
            rq.Limits(remaining=10).update(rq.Limits(remaining=20)),
            rq.Limits(remaining=10),
        )

        self.assertEqual(
            rq.Limits(remaining=20).update(rq.Limits(remaining=10)),
            rq.Limits(remaining=10),
        )

        # Check that a remaining value resets an unlimited total
        self.assertEqual(
            rq.Limits(total=True, remaining=10).update(rq.Limits()),
            rq.Limits(remaining=10),
        )

        self.assertEqual(
            rq.Limits(total=True).update(rq.Limits(remaining=10)),
            rq.Limits(remaining=10),
        )

        self.assertEqual(
            rq.Limits(total=True, remaining=20).update(rq.Limits(remaining=10)),
            rq.Limits(remaining=10),
        )
        # Check total takes the "latest" value

    def test_limits_base_delay_01(self):
        # Unlimited limit has no delay.
        self.assertIsNone(
            rq.Limits(total=True).base_delay(1, guess=60),
        )

        # If number of requests is less than remaining limit, no delay needed.
        self.assertIsNone(
            rq.Limits(remaining=4).base_delay(1, guess=60),
        )

        # If total limit exists, rate is based on limit.
        self.assertAlmostEqual(
            11,
            rq.Limits(total=6).base_delay(1, guess=60),
        )
        self.assertAlmostEqual(
            11,
            rq.Limits(total=6, remaining=4).base_delay(99, guess=60),
        )

        # Otherwise, just use the minimum guess
        self.assertAlmostEqual(
            20,
            rq.Limits().base_delay(1, guess=20),
        )
        self.assertAlmostEqual(
            20,
            rq.Limits(remaining=4).base_delay(99, guess=20),
        )

    @with_fake_event_loop
    async def test_execute_no_sleep_01(self):
        # Unlimited total
        request_limits = rq.Limits(total=True, delay_factor=2)

        # All tasks return a valid result
        finalize_target: dict[int, float] = {}

        report = await rq.execute_no_sleep(
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
            service=rq.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.deferred_requests)

        # Delay factor is decreased if no retries are needed
        self.assertEqual(1.9, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_02(self):
        # Unlimited total
        request_limits = rq.Limits(total=True)

        # A mix of successes and failures
        finalize_target: dict[int, float] = {}

        report = await rq.execute_no_sleep(
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
                        TestResult(data=rq.Error('B', True)),
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
                    _cost=4, _results=[TestResult(data=rq.Error('D', False))]
                ),
                TestParams(
                    _cost=2,
                    _results=[
                        # unsuccessful retry
                        TestResult(data=rq.Error('E', True)),
                        TestResult(data=rq.Error('E', True)),
                        TestResult(data=rq.Error('E', True)),
                        TestResult(data=rq.Error('E', True)),
                        TestResult(data=rq.Error('E', True)),
                    ]
                ),
            ],
            service=rq.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0},
            finalize_target
        )

        self.assertEqual(2, report.success_count)
        self.assertEqual(1, report.unknown_error_count)
        self.assertEqual(['D'], report.known_error_messages)
        self.assertEqual(1, report.deferred_requests)

        self.assertEqual(2, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_03(self):
        # The total limit is finite
        request_limits = rq.Limits(total=6)

        # Only the first task returns a valid result
        finalize_target: dict[int, float] = {}

        report = await rq.execute_no_sleep(
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
            service=rq.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0},
            finalize_target
        )

        self.assertEqual(1, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(3, report.deferred_requests)

        self.assertEqual(2, report.updated_limits.delay_factor)

    @with_fake_event_loop
    async def test_execute_no_sleep_04(self):
        # The total limit is finite
        # But, a sufficient remaining limit is returned by some the result.
        request_limits = rq.Limits(total=6)

        # Only the first task returns a valid result
        finalize_target: dict[int, float] = {}

        report = await rq.execute_no_sleep(
            params=[
                TestParams(
                    _cost=1,
                    _results=[
                        TestResult(
                            data=TestData(1),
                            finalize_target=finalize_target,
                            request_limits=rq.Limits(remaining=3),
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
            service=rq.Service(jitter=False, request_limits=request_limits),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(4, report.success_count)
        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.deferred_requests)

        self.assertEqual(1, report.updated_limits.delay_factor)
        # remaining stays None
        self.assertIsNone(report.updated_limits.remaining)

    @with_fake_event_loop
    async def test_execute_specified_01(self):
        # All tasks return a valid result
        results = await rq._execute_specified(
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
        results = await rq._execute_specified(
            params=[
                TestParams(
                    _cost=1, _results=[TestResult(data=TestData(1))]
                ),
                TestParams(
                    _cost=2, _results=[TestResult(data=rq.Error('B', True))]
                ),
                TestParams(
                    _cost=3
                ),
                TestParams(
                    _cost=4, _results=[TestResult(data=rq.Error('D', False))]
                ),
            ],
            indexes=[0, 1, 2, 3],
        )

        self.assertEqual(
            {
                0: TestResult(data=TestData(1)),
                1: TestResult(data=rq.Error('B', True)),
                3: TestResult(data=rq.Error('D', False)),
            },
            results,
        )

        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [0, 0, 0])

    @with_fake_event_loop
    async def test_execute_specified_03(self):
        # Run only some tasks
        results = await rq._execute_specified(
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
