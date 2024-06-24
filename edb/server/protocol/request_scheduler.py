#
# This source file is part of the EdgeDB open source project.
#
# Copyright MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Final,
    Generic,
    Iterable,
    Literal,
    Optional,
    Sequence,
    TypeVar,
)

import abc
import asyncio
import copy
import random

_T = TypeVar('_T')


def _default_service() -> Service:
    return Service()


@dataclass
class Scheduler(abc.ABC):
    """A scheduler for tasks which use some asynchronous service.

    Tasks may be generated at any time. The service may have some rate limit
    and may additionally, not always succeed in handling the request.

    This class checks whether any tasks exist and when they should be processed.
    """

    service: Service = field(default_factory=_default_service)

    # The next time to process requests
    ready_time: Optional[float] = None

    # Whether requests should be processed as soon as possible after the ready
    # time.
    # If this is True, but no ready time is specified, then this should be
    # processed right away.
    execute_immediately: bool = True

    @abc.abstractmethod
    async def get_task_params(
        self, context: Context,
    ) -> Optional[Sequence[Params]]:
        """Get parameters for the tasks to run."""
        raise NotImplementedError

    async def process(self, context: Context) -> bool:
        now = asyncio.get_running_loop().time()
        if (
            self.ready_time is not None
            and now < self.ready_time
        ):
            # Not ready yet
            return False

        try:
            task_params = await self.get_task_params(context)
        except Exception:
            task_params = None

        error_count = 0
        deferred_cost = 0
        success_count = 0

        if task_params is None:
            error_count = 1

        elif len(task_params) > 0:
            try:
                execution_task = asyncio.create_task(
                    execute_no_sleep(task_params, service=self.service)
                )
                await execution_task

                execution_report = execution_task.result()

            except Exception:
                execution_report = ExecutionReport(unknown_error_count=1)

            assert isinstance(execution_report, ExecutionReport)

            self.finalize(execution_report)

            # Cache limits for next time
            if execution_report.updated_limits is not None:
                if self.service.request_limits is not None:
                    self.service.request_limits.update(
                        execution_report.updated_limits
                    )
                    self.service.request_limits.delay_factor = (
                        execution_report.updated_limits.delay_factor
                    )

                else:
                    self.service.request_limits = (
                        execution_report.updated_limits
                    )

            # Update counts
            error_count = (
                len(execution_report.known_error_messages)
                + execution_report.unknown_error_count
            )

            deferred_cost = execution_report.deferred_cost
            success_count = execution_report.success_count

        # Update when this service should be processed again
        delay, execute_immediately = self.service.next_delay(
            success_count, deferred_cost, error_count, context.naptime
        )

        if delay is None:
            self.ready_time = None
        else:
            now = asyncio.get_running_loop().time()
            self.ready_time = now + delay
        self.execute_immediately = execute_immediately

        return True

    @abc.abstractmethod
    def finalize(self, execution_report: ExecutionReport) -> None:
        """Do any extra work after executing the tasks"""
        pass

    @staticmethod
    def get_combined_ready_time(
        schedulers: Sequence[Scheduler]
    ) -> float | bool:
        """Get the ready time of a group of schedulers.

        A True result means some scheduler should be processed immediately with
        no delay.

        A float result is the lowest delay among schedulers which should be
        executed immediately after their delays.

        A False result means no scheduler has an immediate delay.
        """
        if any(
            (
                scheduler.ready_time is None
                and scheduler.execute_immediately
            )
            for scheduler in schedulers
        ):
            # A provider needs to be processed again immediately
            return True

        immediate_ready_times = [
            scheduler.ready_time
            for scheduler in schedulers
            if (
                scheduler.ready_time is not None
                and scheduler.execute_immediately
            )
        ]
        if len(immediate_ready_times) > 0:
            # A provider needs to be processed again after some delay
            return min(immediate_ready_times)

        # Providers can be processed whenever, after their delay
        return False


@dataclass
class Context:
    """Additional information used to process a service's requests."""

    naptime: float


@dataclass
class Service:
    """Parameters for requests to a given service."""

    # Information about the service's rate limits
    request_limits: Optional[Limits] = None

    # The maximum number of times to retry tasks
    max_retry_count: Final[int] = 4
    # Whether to jitter the delay time if a retry error is produced
    jitter: Final[bool] = True
    # Initial guess for the delay
    guess_delay: Final[float] = 1.0
    # The upper bound for delays
    delay_max: Final[float] = 60.0

    def next_delay(
        self,
        success_count,
        deferred_cost,
        error_count,
        naptime: float
    ) -> tuple[Optional[float], bool]:
        """When should the service should be processed again.

        Returns a delay and whether the processing should happen as soon as the
        the delay elapses.

        Examples:
        (None, True) = execute immediately
        (None, False) = execute any time
        (10, True) = execute immediately after 10s
        (10, False) = execute any time after 10s
        """

        if self.request_limits is not None:
            base_delay = self.request_limits.base_delay(
                deferred_cost, guess=self.guess_delay,
            )
            if base_delay is None:
                delay = None

            else:
                # If delay_factor is very high, it may take quite a long time
                # for it to return to 1. A maximum delay prevents this service
                # from never getting checked.
                delay = min(
                    base_delay * self.request_limits.delay_factor,
                    self.delay_max,
                )

        else:
            # We have absolutely no information about the delay, assume naptime.
            delay = naptime

        if error_count > 0:
            # There was an error, wait before trying again.
            # Use the larger of delay or naptime.
            delay = max(delay, naptime) if delay is not None else naptime
            execute_immediately = False

        elif deferred_cost > 0:
            # There is some deferred work, apply the delay and run immediately.
            execute_immediately = True

        elif success_count > 0:
            # Some work was done successfully. Run again to ensure no more work
            # needs to be done.
            delay = None
            execute_immediately = True

        else:
            # No work left to do. Take a nap.
            delay = naptime
            execute_immediately = False

        return delay, execute_immediately


@dataclass
class ExecutionReport:
    """Information about the tasks after they are complete"""

    success_count: int = 0
    unknown_error_count: int = 0
    known_error_messages: list[str] = field(default_factory=list)
    deferred_cost: int = 0

    updated_limits: Optional[Limits] = None


@dataclass
class Limits:
    """Information about a service's rate limits."""

    # Total limit of a resource per minute for a service.
    # A True value represents a unlimited total
    total: Optional[int | Literal[True]] = None

    # Remaining resources before the limit is hit.
    remaining: Optional[int] = None

    # A delay factor to implement exponential backoff
    delay_factor: float = 1

    def base_delay(
        self,
        request_cost: int,
        *,
        guess: float,
    ) -> Optional[float]:
        if self.total is True:
            return None

        if self.remaining is not None and request_cost <= self.remaining:
            return None

        if self.total is not None:
            return 60.0 / self.total * 1.1  # 10% buffer just in case

        # guess the delay
        return guess

    def update(self, latest: Limits) -> Limits:
        """Update based on the latest information."""

        # The total will change rarely. Always take the latest value if
        # it exists.
        if latest.total is not None:
            self.total = latest.total

        # The remaining amount can fluctuate quite a bit, take the smallest
        # value available.
        if self.remaining is None:
            self.remaining = latest.remaining
        elif latest.remaining is not None:
            self.remaining = min(self.remaining, latest.remaining)

        if self.total is True and self.remaining:
            # If there is a remaining value, the total is not actually
            # unlimited.
            self.total = None

        return self


class Task(abc.ABC, Generic[_T]):
    """Represents an async request"""

    params: Params[_T]
    _inner: asyncio.Task

    def __init__(self, params: Params[_T]):
        self.params = params
        self._inner = asyncio.create_task(self.run())

    @abc.abstractmethod
    async def run(self) -> Optional[Result[_T]]:
        """Run the task and return a result."""
        raise NotImplementedError

    async def wait_result(self) -> None:
        """Wait for the request to complete."""
        await self._inner

    def get_result(self) -> Optional[Result[_T]]:
        """Get the result of the request."""
        task_result = self._inner.result()
        return task_result


class Params(abc.ABC, Generic[_T]):
    """The parameters of an async request task.

    These are used to generate tasks. They may be used to generate multiple
    tasks if the task fails and is re-tried.
    """

    @abc.abstractmethod
    def cost(self) -> int:
        """Expected cost to execute the task."""
        raise NotImplementedError

    @abc.abstractmethod
    def create_task(self) -> Task[_T]:
        """Create a task using the parameters."""
        raise NotImplementedError


@dataclass(frozen=True)
class Result(abc.ABC, Generic[_T]):
    """The result of an async request.

    Some tasks may include updated request limit information in their
    response.
    """

    data: _T | Error

    # Some services can return their request limits
    request_limits: Optional[Limits] = None

    async def finalize(self) -> None:
        """An optional finalize task to be run sequentially."""
        pass


@dataclass(frozen=True)
class Error:
    """Represents an error from an async request."""

    message: str

    # If there was an error, it may be possible to retry the request
    # Eg. 429 too many requests
    retry: bool


async def execute_no_sleep(
    params: Sequence[Params[_T]],
    *,
    service: Service,
) -> ExecutionReport:
    """Attempt to execute as many tasks as possible without sleeping."""
    report = ExecutionReport()

    # Set up request limits
    if service.request_limits is None:
        # If no other information is available, for the first attempt assume
        # there is no limit.
        request_limits = Limits(total=True)

    else:
        request_limits = copy.copy(service.request_limits)

    # If any tasks fail and can be retried, retry them up to a maximum number
    # of times.
    retry_count: int = 0
    pending_task_indexes: list[int] = list(range(len(params)))

    while pending_task_indexes and retry_count < service.max_retry_count:
        pending_cost = sum(
            params[i].cost()
            for i in pending_task_indexes
        )

        base_delay = request_limits.base_delay(
            pending_cost, guess=service.guess_delay,
        )

        active_task_indexes: list[int]
        inactive_task_indexes: list[int]
        if base_delay is None:
            # Try to execute all tasks.
            active_task_indexes = pending_task_indexes
            inactive_task_indexes = []

        elif retry_count == 0:
            # If there is any delay, only execute one task.
            # This may update the remaining limit, allowing the remaining tasks
            # to run.
            active_task_indexes = pending_task_indexes[:1]
            inactive_task_indexes = pending_task_indexes[1:]

        else:
            break

        results = await _execute_specified(
            params, active_task_indexes,
        )

        # Check task results
        retry_task_indexes: list[int] = []

        request_limits.remaining = None

        for task_index in active_task_indexes:
            if task_index not in results:
                report.unknown_error_count += 1
                continue

            task_result = results[task_index]

            if isinstance(task_result.data, Error):
                if task_result.data.retry:
                    # task can be retried
                    retry_task_indexes.append(task_index)

                else:
                    # error with message
                    report.known_error_messages.append(task_result.data.message)
            else:
                report.success_count += 1

            await task_result.finalize()

            if task_result.request_limits is not None:
                request_limits.update(task_result.request_limits)

        retry_count += 1
        pending_task_indexes = retry_task_indexes + inactive_task_indexes

    # Note how many retries were left
    report.deferred_cost = sum(
            params[i].cost()
            for i in pending_task_indexes
        )

    if report.deferred_cost != 0:
        # If there are deferred requests, gradually increase the delay factor
        request_limits.delay_factor *= (
            1 + random.random() if service.jitter else 2
        )

    elif (
        len(report.known_error_messages) == 0
        and report.unknown_error_count == 0
    ):
        # If there are no errors, gradually decrease the delay factor over time.
        request_limits.delay_factor = max(
            0.95 * request_limits.delay_factor,
            1,
        )

    # We don't know when the service will be called again, so just clear the
    # remaining value
    request_limits.remaining = None

    # Return the updated request limits limits
    report.updated_limits = request_limits

    return report


async def _execute_specified(
    params: Sequence[Params[_T]],
    indexes: Iterable[int],
) -> dict[int, Result[_T]]:
    # Send all requests at once.
    # We are confident that all requests can be handled right away.

    tasks: dict[int, Task[_T]] = {}

    for task_index in indexes:
        tasks[task_index] = params[task_index].create_task()

    results: dict[int, Result[_T]] = {}

    for task_index, task in tasks.items():
        await task.wait_result()

        task_result = task.get_result()
        if task_result is not None:
            results[task_index] = task_result

    return results
