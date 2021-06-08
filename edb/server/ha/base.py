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

import urllib.parse
from typing import *


class ClusterProtocol:
    def on_switch_over(self, old_master, new_master):
        pass


class HABackend:
    async def get_cluster_consensus(self) -> Tuple[str, int]:
        raise NotImplementedError

    async def start_watching(
        self, cluster_protocol: Optional[ClusterProtocol] = None
    ) -> bool:
        raise NotImplementedError

    def stop_watching(self):
        raise NotImplementedError

    def get_master_addr(self) -> Optional[Tuple[str, int]]:
        raise NotImplementedError


class HAClusterInfo(NamedTuple):
    backend: str
    store: str
    host: Optional[str]
    port: Optional[int]
    name: str


def parse_ha_uri(uri: str) -> HAClusterInfo:
    parsed = urllib.parse.urlparse(uri)
    backend, _, store = parsed.scheme.partition("+")

    return HAClusterInfo(
        backend=backend,
        store=store,
        host=parsed.hostname,
        port=parsed.port,
        name=parsed.path.lstrip("/"),
    )
