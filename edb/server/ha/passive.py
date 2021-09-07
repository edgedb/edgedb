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

from typing import *

import asyncio
import enum
import logging

from . import base


UNHEALTHY_MIN_TIME = 30
UNEXPECTED_DISCONNECTS_THRESHOLD = 0.6

logger = logging.getLogger("edb.pgcluster")


class State(enum.Enum):
    HEALTHY = 1
    UNHEALTHY = 2
    FAILOVER = 3


class PassiveHASupport:
    _state: State
    _unhealthy_timer_handle: Optional[asyncio.TimerHandle]

    def __init__(self, cluster_protocol: base.ClusterProtocol):
        self._cluster_protocol = cluster_protocol
        self._state = State.UNHEALTHY
        self._pgcon_count = 0
        self._unexpected_disconnects = 0
        self._unhealthy_timer_handle = None
        self._sys_pgcon_healthy = False

    def set_state_failover(self):
        self._state = State.FAILOVER
        logger.critical("Passive HA failover detected")
        self._reset()
        self._cluster_protocol.on_switch_over()

    def on_pgcon_broken(self, is_sys_pgcon: bool):
        if is_sys_pgcon:
            self._sys_pgcon_healthy = False
        if self._state == State.HEALTHY:
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
                "Passive HA cluster is unhealthy. "
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
            self._state = State.HEALTHY
            logger.info("Passive HA cluster is healthy")
            self._reset()
        elif self._state == State.FAILOVER:
            if self._sys_pgcon_healthy:
                self._state = State.HEALTHY
                logger.info("Passive HA cluster has recovered from failover")

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
