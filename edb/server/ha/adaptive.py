#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Optional

import asyncio
import enum
import logging
import os

from edb.server import metrics

from . import base


UNHEALTHY_MIN_TIME = int(os.getenv(
    'EDGEDB_SERVER_BACKEND_ADAPTIVE_HA_UNHEALTHY_MIN_TIME', 30
))
UNEXPECTED_DISCONNECTS_THRESHOLD = int(os.getenv(
    'EDGEDB_SERVER_BACKEND_ADAPTIVE_HA_DISCONNECT_PERCENT', 60
)) / 100

logger = logging.getLogger("edb.pgcluster")


class State(enum.Enum):
    HEALTHY = 1
    UNHEALTHY = 2
    FAILOVER = 3


class AdaptiveHASupport:
    # Adaptive HA support is used to detect HA backends that does not actively
    # send clear failover signals to EdgeDB. It can be enabled through command
    # line argument --enable-backend-adaptive-ha.
    #
    # This class evaluates the events on the backend connection pool into 3
    # states representing the status of the backend:
    #
    #   * Healthy - all is good
    #   * Unhealthy - a staging state before failover
    #   * Failover - backend failover is in process
    #
    # When entering Unhealthy state, we will start to count events for a
    # threshold; when reached, we'll switch to Failover state - that means we
    # will actively disconnect all backend connections and wait for sys_pgcon
    # to reconnect. In any of the 3 states, client connections will not be
    # dropped. Whether the clients could issue queries is irrelevant to the 3
    # states - `BackendUnavailableError` or `BackendInFailoverError` is only
    # raised if the sys_pgcon is broken. But even with that said,
    # `BackendUnavailableError` is only seen in Unhealthy (not always), and
    # Failover always means `BackendInFailoverError` for any queries.
    #
    # Rules of state switches:
    #
    # Unhealthy -> Healthy
    #   * Successfully connected to a non-hot-standby backend.
    #   * Data received from any pgcon (not implemented).
    #
    # Unhealthy -> Failover
    #   * More than 60% (UNEXPECTED_DISCONNECTS_THRESHOLD) of existing pgcons
    #     are "unexpectedly disconnected" (number of existing pgcons is
    #     captured at the moment we change to Unhealthy state, and maintained
    #     on "expected disconnects" too).
    #   * (and) In Unhealthy state for more than UNHEALTHY_MIN_TIME seconds.
    #   * (and) sys_pgcon is down.
    #   * (or) Postgres shutdown/hot-standby notification received.
    #
    # Healthy -> Unhealthy
    #   * Any unexpected disconnect.
    #   * (or) Failed to connect due to ConnectionError (not implemented).
    #   * (or) Last active time is greater than 10 seconds (depends on the
    #     sys_pgcon idle-poll interval) (not implemented).
    #
    # Healthy -> Failover
    #   * Postgres shutdown/hot-standby notification received.
    #
    # Failover -> Healthy
    #   * Successfully connected to a non-hot-standby backend.
    #   * (and) sys_pgcon is healthy.

    _state: State
    _unhealthy_timer_handle: Optional[asyncio.TimerHandle]

    def __init__(self, cluster_protocol: base.ClusterProtocol, tag: str):
        self._cluster_protocol = cluster_protocol
        self._state = State.UNHEALTHY
        self._pgcon_count = 0
        self._unexpected_disconnects = 0
        self._unhealthy_timer_handle = None
        self._sys_pgcon_healthy = False
        self._tag = tag

    def incr_metrics_counter(self, event: str, value: float = 1.0) -> None:
        metrics.ha_events_total.inc(value, f"adaptive://{self._tag}", event)

    def set_state_failover(self, *, call_on_switch_over=True):
        self._state = State.FAILOVER
        self._reset()
        if call_on_switch_over:
            logger.critical("adaptive: HA failover detected")
            self.incr_metrics_counter("failover")
            self._cluster_protocol.on_switch_over()

    def on_pgcon_broken(self, is_sys_pgcon: bool):
        if is_sys_pgcon:
            self._sys_pgcon_healthy = False
        if self._state == State.HEALTHY:
            self.incr_metrics_counter("unhealthy")
            self._state = State.UNHEALTHY
            self._unexpected_disconnects = 1
            self._unhealthy_timer_handle = (
                asyncio.get_running_loop().call_later(
                    UNHEALTHY_MIN_TIME, self._maybe_failover
                )
            )
            self._pgcon_count = max(
                self._cluster_protocol.get_active_pgcon_num(), 0
            ) + 1
            logger.warning(
                "adaptive: Backend HA cluster is unhealthy. "
                "Captured number of pgcons: %d",
                self._pgcon_count,
            )
        elif self._state == State.UNHEALTHY:
            self._unexpected_disconnects += 1
            if self._unhealthy_timer_handle is None:
                self._maybe_failover()

    def on_pgcon_lost(self):
        if self._state == State.UNHEALTHY:
            self._pgcon_count = max(1, self._pgcon_count - 1)
            logger.debug(
                "on_pgcon_lost: decreasing captured pgcon count to: %d",
                self._pgcon_count,
            )
            if self._unhealthy_timer_handle is None:
                self._maybe_failover()

    def on_pgcon_made(self, is_sys_pgcon: bool):
        if is_sys_pgcon:
            self._sys_pgcon_healthy = True
        if self._state == State.UNHEALTHY:
            self.incr_metrics_counter("healthy")
            self._state = State.HEALTHY
            logger.info("adaptive: Backend HA cluster is healthy")
            self._reset()
        elif self._state == State.FAILOVER:
            if self._sys_pgcon_healthy:
                self.incr_metrics_counter("healthy")
                self._state = State.HEALTHY
                logger.info(
                    "adaptive: Backend HA cluster has recovered from failover"
                )

    def _reset(self):
        self._pgcon_count = 0
        self._unexpected_disconnects = 0
        if self._unhealthy_timer_handle is not None:
            self._unhealthy_timer_handle.cancel()
            self._unhealthy_timer_handle = None

    def _maybe_failover(self):
        logger.debug(
            "_maybe_failover: unexpected disconnects: %d",
            self._unexpected_disconnects,
        )
        self._unhealthy_timer_handle = None
        if (
            self._unexpected_disconnects / self._pgcon_count
            >= UNEXPECTED_DISCONNECTS_THRESHOLD
        ) and not self._sys_pgcon_healthy:
            self.set_state_failover()
