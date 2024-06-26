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


@dataclass
class Timer:
    """Represents a time after which an action should be taken.

    Examples:
    (None, True) = execute immediately
    (None, False) = execute any time
    (10, True) = execute immediately after 10s
    (10, False) = execute any time after 10s
    """

    time: Optional[float] = None

    # Whether the action should be taken as soon as possible after the time.
    urgent: bool = True

    @staticmethod
    def create_delay(delay: Optional[float], urgent: bool) -> Timer:
        now = asyncio.get_running_loop().time()
        if delay is None:
            time = None
        else:
            now = asyncio.get_running_loop().time()
            time = now + delay

        return Timer(time, urgent)

    def is_ready(self) -> bool:
        now = asyncio.get_running_loop().time()
        return self.time is None or self.time <= now

    def is_ready_and_urgent(self) -> bool:
        return self.is_ready() and self.urgent

    def remaining_time(self, max_delay: float) -> float:
        """How long before this timer is ready in seconds."""
        if self.urgent:
            if self.time is None:
                return 0

            else:
                # 1ms extra, just in case
                now = asyncio.get_running_loop().time()
                delay = self.time - now + 0.001
                return min(max(0, delay), max_delay)

        else:
            # If not urgent, wait as long as possible
            return max_delay

    @staticmethod
    def combine(timers: Iterable[Timer]) -> Optional[Timer]:
        """Combine the timers to determine the when to take the next action.

        If the timers are (1, False), (2, False), (3, True), it may be wasteful
        to act at times [1, 2, 3].

        Instead, we would prefer to act only once, at time 3, since only the
        third action was urgent.
        """
        for target_urgency in [True, False]:
            if any(
                timer.time is None and timer.urgent == target_urgency
                for timer in timers
            ):
                # An action should be taken right away.
                return Timer(None, target_urgency)

            urgent_times = [
                timer.time
                for timer in timers
                if timer.time is not None and timer.urgent == target_urgency
            ]
            if len(urgent_times) > 0:
                # An action should be taken after some delay
                return Timer(min(urgent_times), target_urgency)

        # Nothing to do
        return None


def _default_service() -> Service:
    return Service()


def _default_delay_time() -> Timer:
    return Timer()


@dataclass
class Scheduler(abc.ABC, Generic[_T]):
    """A scheduler for requests to an asynchronous service.

    A Scheduler both generates requests and tracks when the service can be
    accessed.
    """

    service: Service = field(default_factory=_default_service)

    # The next time to process requests
    timer: Timer = field(default_factory=_default_delay_time)

    @abc.abstractmethod
    async def get_params(
        self, context: Context,
    ) -> Optional[Sequence[Params[_T]]]:
        """Get parameters for the requests to run."""
        raise NotImplementedError

    async def process(self, context: Context) -> bool:
        if not self.timer.is_ready():
            return False

        request_params: Optional[Sequence[Params[_T]]]
        try:
            request_params = await self.get_params(context)
        except Exception:
            request_params = None

        error_count = 0
        deferred_cost = 0
        success_count = 0

        if request_params is None:
            error_count = 1

        elif len(request_params) > 0:
            try:
                execution_report = await execute_no_sleep(
                    request_params, service=self.service,
                )

            except Exception:
                execution_report = ExecutionReport(unknown_error_count=1)

            assert isinstance(execution_report, ExecutionReport)

            self.finalize(execution_report)

            # Cache limits for next time
            if execution_report.updated_limits is not None:
                if self.service.request_limits is not None:
                    self.service.request_limits.update_total(
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
        self.timer = self.service.next_delay(
            success_count, deferred_cost, error_count, context.naptime
        )

        return True

    @abc.abstractmethod
    def finalize(self, execution_report: ExecutionReport) -> None:
        """An optional final step after executing requests"""
        pass


@dataclass
class Context:
    """Information passed to a Scheduler to process requests."""

    # If there is no work, the scheduler should take a nap.
    naptime: float


@dataclass
class ExecutionReport:
    """Information about the requests after they are complete"""

    success_count: int = 0
    unknown_error_count: int = 0
    known_error_messages: list[str] = field(default_factory=list)
    deferred_cost: int = 0

    # Some requests may report an update to the service's request limits.
    updated_limits: Optional[Limits] = None


@dataclass
class Service:
    """Information on how to access to a given service."""

    # Information about the service's rate limits
    request_limits: Optional[Limits] = None

    # The maximum number of times to retry requests
    max_retry_count: Final[int] = 4
    # Whether to jitter the delay time if a retry error is produced
    jitter: Final[bool] = True
    # Initial guess for the delay
    guess_delay: Final[float] = 1.0
    # The upper bound for delays
    delay_max: Final[float] = 60.0

    def next_delay(
        self,
        success_count: int,
        deferred_cost: int,
        error_count: int,
        naptime: float
    ) -> Timer:
        """When should the service should be processed again."""

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
            urgent = False

        elif deferred_cost > 0:
            # There is some deferred work, apply the delay and run immediately.
            urgent = True

        elif success_count > 0:
            # Some work was done successfully. Run again to ensure no more work
            # needs to be done.
            delay = None
            urgent = True

        else:
            # No work left to do, wait before trying again.
            # Use the larger of delay or naptime.
            delay = max(delay, naptime) if delay is not None else naptime
            urgent = False

        return Timer.create_delay(delay, urgent)


@dataclass
class Limits:
    """Information about a service's rate limits."""

    # Total limit of a resource per minute for a service.
    # A True value represents a unlimited total
    total: Optional[int | Literal[True]] = None

    # Remaining resources before the limit is hit.
    # It is assumed to be decreasing during a call to execute_no_sleep.
    #
    # This can be set by users before a call to Scheduler.process.
    # It will also be updated during execution if a responseincludes an updated
    # value.
    #
    # Finally, it is reset after requests are executed since we don't know when
    # the next call will be.
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

    def update_total(self, latest: Limits) -> Limits:
        """Update total based on the latest information.

        The total will change rarely. Always take the latest value if
        # it exists
        """
        if latest.total is not None:
            self.total = latest.total

        return self

    def update_remaining(self, latest: Limits) -> Limits:
        """Update remaining based on the latest information.

        The remaining amount is assumed to decreasing during a call to
        execute_no_sleep.
        """
        if self.remaining is None:
            self.remaining = latest.remaining
        elif latest.remaining is not None:
            self.remaining = min(self.remaining, latest.remaining)

        if self.total is True and self.remaining:
            # If there is a remaining value, the total is not actually
            # unlimited.
            self.total = None

        return self


class Request(abc.ABC, Generic[_T]):
    """Represents an async request"""

    params: Params[_T]
    _inner: asyncio.Task[Optional[Result[_T]]]

    def __init__(self, params: Params[_T]):
        self.params = params
        self._inner = asyncio.create_task(self.run())

    @abc.abstractmethod
    async def run(self) -> Optional[Result[_T]]:
        """Run the request and return a result."""
        raise NotImplementedError

    async def wait_result(self) -> None:
        """Wait for the request to complete."""
        await self._inner

    def get_result(self) -> Optional[Result[_T]]:
        """Get the result of the request."""
        result = self._inner.result()
        return result


class Params(abc.ABC, Generic[_T]):
    """The parameters of an async request.

    These are used to generate requests.

    A single Params instance may be used to generate multiple Request
    instances if it fails, but can be retried right away.
    """

    @abc.abstractmethod
    def cost(self) -> int:
        """Expected cost to execute the request."""
        raise NotImplementedError

    @abc.abstractmethod
    def create_request(self) -> Request[_T]:
        """Create a request with the parameters."""
        raise NotImplementedError


@dataclass(frozen=True)
class Result(abc.ABC, Generic[_T]):
    """The result of an async request."""

    data: _T | Error

    # Some services can return request limits along with their usual results.
    request_limits: Optional[Limits] = None

    async def finalize(self) -> None:
        """An optional finalize to be run sequentially."""
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
    """Attempt to execute as many requests as possible without sleeping."""
    report = ExecutionReport()

    # Set up request limits
    if service.request_limits is None:
        # If no other information is available, for the first attempt assume
        # there is no limit.
        request_limits = Limits(total=True)

    else:
        request_limits = copy.copy(service.request_limits)

    # If any requests fail and can be retried, retry them up to a maximum number
    # of times.
    retry_count: int = 0
    pending_request_indexes: list[int] = list(range(len(params)))

    while pending_request_indexes and retry_count < service.max_retry_count:
        pending_cost = sum(
            params[i].cost()
            for i in pending_request_indexes
        )

        base_delay = request_limits.base_delay(
            pending_cost, guess=service.guess_delay,
        )

        active_request_indexes: list[int]
        inactive_request_indexes: list[int]
        if base_delay is None:
            # Try to execute all requests.
            active_request_indexes = pending_request_indexes
            inactive_request_indexes = []

        elif retry_count == 0:
            # If there is any delay, only execute one request.
            # This may update the remaining limit, allowing the remaining
            # requests to run.
            active_request_indexes = pending_request_indexes[:1]
            inactive_request_indexes = pending_request_indexes[1:]

        else:
            break

        results = await _execute_specified(
            params, active_request_indexes,
        )

        # Check results
        retry_request_indexes: list[int] = []

        request_limits.remaining = None

        for request_index in active_request_indexes:
            if request_index not in results:
                report.unknown_error_count += 1
                continue

            result = results[request_index]

            if isinstance(result.data, Error):
                if result.data.retry:
                    # requests can be retried
                    retry_request_indexes.append(request_index)

                else:
                    # error with message
                    report.known_error_messages.append(result.data.message)
            else:
                report.success_count += 1

            await result.finalize()

            if result.request_limits is not None:
                request_limits.update_total(result.request_limits)
                request_limits.update_remaining(result.request_limits)

        retry_count += 1
        pending_request_indexes = (
            retry_request_indexes + inactive_request_indexes
        )

    # Note how many retries were left
    report.deferred_cost = sum(
        params[i].cost()
        for i in pending_request_indexes
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

    requests: dict[int, Request[_T]] = {}

    for request_index in indexes:
        requests[request_index] = params[request_index].create_request()

    results: dict[int, Result[_T]] = {}

    for request_index, request in requests.items():
        await request.wait_result()

        result = request.get_result()
        if result is not None:
            results[request_index] = result

    return results
