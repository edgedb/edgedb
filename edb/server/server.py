#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from edb.common import taskgroup

from . import dbview


class Server:

    def __init__(self, *, loop, cluster, runstate_dir,
                 max_backend_connections):

        self._loop = loop

        self._serving = False

        self._cluster = cluster
        self._dbindex = dbview.DatabaseIndex()

        self._runstate_dir = runstate_dir
        self._max_backend_connections = max_backend_connections

        self._ports = []

    def add_port(self, portcls, **kwargs):
        if self._serving:
            raise RuntimeError(
                'cannot add new ports after start() call')

        self._ports.append(
            portcls(
                loop=self._loop,
                cluster=self._cluster,
                runstate_dir=self._runstate_dir,
                dbindex=self._dbindex,
                **kwargs))

    async def start(self):
        async with taskgroup.TaskGroup() as g:
            for port in self._ports:
                g.create_task(port.start())

        self._serving = True

    async def stop(self):
        async with taskgroup.TaskGroup() as g:
            for port in self._ports:
                g.create_task(port.stop())
