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

from __future__ import annotations
from typing import Callable, Optional, Tuple

import urllib.parse

from edb.common import asyncwatcher
from edb.server import metrics


class ClusterProtocol:
    def on_switch_over(self):
        pass

    def get_active_pgcon_num(self) -> int:
        raise NotImplementedError()


class HABackend(asyncwatcher.AsyncWatcher):
    def __init__(self) -> None:
        super().__init__()
        self._failover_cb: Optional[Callable[[], None]] = None

    async def get_cluster_consensus(self) -> Tuple[str, int]:
        raise NotImplementedError

    def get_master_addr(self) -> Optional[Tuple[str, int]]:
        raise NotImplementedError

    def set_failover_callback(self, cb: Optional[Callable[[], None]]) -> None:
        self._failover_cb = cb

    @property
    def dsn(self) -> str:
        raise NotImplementedError

    def incr_metrics_counter(self, event: str, value: float = 1.0) -> None:
        metrics.ha_events_total.inc(value, self.dsn, event)


def get_backend(parsed_dsn: urllib.parse.ParseResult) -> Optional[HABackend]:
    backend, _, sub_scheme = parsed_dsn.scheme.partition("+")
    if backend == "stolon":
        from . import stolon

        return stolon.get_backend(sub_scheme, parsed_dsn)

    return None
