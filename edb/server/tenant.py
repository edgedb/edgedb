#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *

if TYPE_CHECKING:
    from . import pgcluster
    from . import server as edbserver


class Tenant:
    _server: edbserver.Server | None
    _cluster: pgcluster.BaseCluster

    def __init__(self, cluster: pgcluster.BaseCluster):
        self._server = None
        self._cluster = cluster

    def set_server(self, server: edbserver.Server) -> None:
        self._server = server
