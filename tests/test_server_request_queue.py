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

    @with_fake_event_loop
    async def test_execute_requests_01(self):
        # All tasks return a valid result
        finalize_target: dict[int, float] = {}

        report = await rq.execute_requests(
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
            ctx=rq.Context(jitter=False, request_limits=rq.Limits(total=True)),
        )

        self.assertEqual(
            {1: 0, 2: 0, 3: 0, 4: 0},
            finalize_target
        )

        self.assertEqual(0, report.unknown_error_count)
        self.assertEqual([], report.known_error_messages)
        self.assertEqual(0, report.remaining_retries)

    @with_fake_event_loop
    async def test_execute_requests_02(self):
        # A mix of successes and failures
        finalize_target: dict[int, float] = {}

        report = await rq.execute_requests(
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
            ctx=rq.Context(jitter=False, request_limits=rq.Limits(total=True)),
        )

        self.assertEqual(
            {1: 0, 2: 0},
            finalize_target
        )

        self.assertEqual(1, report.unknown_error_count)
        self.assertEqual(['D'], report.known_error_messages)
        self.assertEqual(1, report.remaining_retries)

    def test_choose_execution_strategy_01(self):
        params = [
            TestParams(_cost=1),
            TestParams(_cost=2),
            TestParams(_cost=3),
            TestParams(_cost=4),
        ]

        indexes = [0, 1, 2, 3]

        # If there is no rate limit, use _execute_all.
        self.assertEqual(
            rq._execute_all,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(total=True),
            ),
        )

        # If there are enough remaining requests to cover the task indexes,
        # use _execute_all.
        self.assertEqual(
            rq._execute_all,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(remaining=20),
            ),
        )

        self.assertEqual(
            rq._execute_all,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(remaining=10),
            ),
        )

        # If there are not enough remaining requests, and the rate limit is
        # known, use _execute_known_limit.
        self.assertEqual(
            rq._execute_known_limit,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(remaining=8, total=8),
            ),
        )

        self.assertEqual(
            rq._execute_known_limit,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(total=10),
            ),
        )

        # Otherwise, use _execute_guess_limit
        self.assertEqual(
            rq._execute_guess_limit,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(remaining=8, guess_delay=10),
            ),
        )

        self.assertEqual(
            rq._execute_guess_limit,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(guess_delay=10),
            ),
        )

        self.assertEqual(
            rq._execute_guess_limit,
            rq._choose_execution_strategy(
                params, indexes, rq.Limits(),
            ),
        )

    @with_fake_event_loop
    async def test_execute_all_01(self):
        # All tasks return a valid result
        results = await rq._execute_all(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[0, 1, 2, 3],
            limits=rq.Limits(total=True),
            ctx=rq.Context(jitter=False),
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
    async def test_execute_all_02(self):
        # A mix of successes and failures
        results = await rq._execute_all(
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
            limits=rq.Limits(total=True),
            ctx=rq.Context(jitter=False),
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
    async def test_execute_all_03(self):
        # Run only some tasks
        results = await rq._execute_all(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[1, 3],
            limits=rq.Limits(total=True),
            ctx=rq.Context(jitter=False),
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

    @with_fake_event_loop
    async def test_execute_known_limit_01(self):
        # All tasks return a valid result
        results = await rq._execute_known_limit(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[0, 1, 2, 3],
            limits=rq.Limits(total=6),
            ctx=rq.Context(jitter=False),
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

        # The ideal delay is 60s / 6 = 10s
        # With a 1.1 factor, the base delay is 11s
        #
        # The cumulative cost factor is [1, 3, 6, 10].
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [11, 33, 66, 110])

    @with_fake_event_loop
    async def test_execute_known_limit_02(self):
        # A mix of successes and failures
        results = await rq._execute_known_limit(
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
            limits=rq.Limits(total=6),
            ctx=rq.Context(jitter=False),
        )

        self.assertEqual(
            {
                0: TestResult(data=TestData(1)),
                1: TestResult(data=rq.Error('B', True)),
                3: TestResult(data=rq.Error('D', False)),
            },
            results,
        )

        # The ideal delay is 60s / 6 = 10s
        # With a 1.1 factor, the base delay is 11s
        #
        # The cumulative cost factor is [1, 3, 9, 17].
        #
        # The cost increment increases after index 1 because of a retry,
        # which causes the base delay to increase.
        #
        # The final value is lower than the expected 11*17=187 because the
        # delay is always capped at the delay max.
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [11, 33, 153])

    @with_fake_event_loop
    async def test_execute_known_limit_03(self):
        # Run only some tasks
        results = await rq._execute_known_limit(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[1, 3],
            limits=rq.Limits(total=6),
            ctx=rq.Context(jitter=False),
        )

        self.assertEqual(
            {
                1: TestResult(data=TestData(2)),
                3: TestResult(data=TestData(4)),
            },
            results,
        )

        # The ideal delay is 60s / 6 = 10s
        # With a 1.1 factor, the base delay is 11s
        #
        # The cumulative cost factor is [2, 6].
        #
        # Skipped indexes don't cause a delay
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [22, 66])

    @with_fake_event_loop
    async def test_execute_guess_limit_01(self):
        # All tasks return a valid result
        results = await rq._execute_guess_limit(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[0, 1, 2, 3],
            limits=rq.Limits(guess_delay=10),
            ctx=rq.Context(jitter=False),
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

        # The cumulative cost factor is [1, 3, 6, 10].
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [10, 30, 60, 100])

    @with_fake_event_loop
    async def test_execute_guess_limit_02(self):
        # A mix of successes and failures
        results = await rq._execute_guess_limit(
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
            limits=rq.Limits(guess_delay=10),
            ctx=rq.Context(jitter=False),
        )

        self.assertEqual(
            {
                0: TestResult(data=TestData(1)),
                1: TestResult(data=rq.Error('B', True)),
                3: TestResult(data=rq.Error('D', False)),
            },
            results,
        )

        # The cumulative cost factor is [1, 3, 9, 17].
        #
        # The cost increment increases after index 1 because of a retry,
        # which causes the base delay to increase.
        #
        # The final value is lower than the expected 10*17=170 because the
        # delay is always capped at the delay max.
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [10, 30, 150])

    @with_fake_event_loop
    async def test_execute_guess_limit_03(self):
        # Run only some tasks
        results = await rq._execute_guess_limit(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[1, 3],
            limits=rq.Limits(guess_delay=10),
            ctx=rq.Context(jitter=False),
        )

        self.assertEqual(
            {
                1: TestResult(data=TestData(2)),
                3: TestResult(data=TestData(4)),
            },
            results,
        )

        # The cumulative cost factor is [2, 6].
        #
        # Skipped indexes don't cause a delay
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [20, 60])

    @with_fake_event_loop
    async def test_execute_guess_limit_04(self):
        # Use the minimum guess delay if no other guess was provided
        results = await rq._execute_guess_limit(
            params=[
                TestParams(_cost=1, _results=[TestResult(data=TestData(1))]),
                TestParams(_cost=2, _results=[TestResult(data=TestData(2))]),
                TestParams(_cost=3, _results=[TestResult(data=TestData(3))]),
                TestParams(_cost=4, _results=[TestResult(data=TestData(4))]),
            ],
            indexes=[0, 1, 2, 3],
            limits=rq.Limits(),
            ctx=rq.Context(jitter=False, guess_delay_min=10),
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

        # The cumulative cost factor is [1, 3, 6, 10].
        times = sorted(r.time for r in results.values())
        self.assertEqual(times, [10, 30, 60, 100])
