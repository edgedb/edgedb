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
    def on_switch_over(self):
        pass

    def get_active_pgcon_num(self) -> int:
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


def get_backend(parsed_dsn: urllib.parse.ParseResult) -> Optional[HABackend]:
    backend, _, sub_scheme = parsed_dsn.scheme.partition("+")
    if backend == "stolon":
        from . import stolon

        return stolon.get_backend(sub_scheme, parsed_dsn)

    return None
