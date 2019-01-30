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


import os
import urllib.parse

from edb.common import taskgroup

from . import dbview


class Server:

    def __init__(self, *, loop, cluster, runstate_dir,
                 max_backend_connections):

        self._loop = loop

        self._serving = False

        self._cluster = cluster
        self._pg_addr = self._get_pgaddr()
        self._pg_data_dir = self._cluster.get_data_dir()

        self._dbindex = dbview.DatabaseIndex()

        self._runstate_dir = runstate_dir
        self._max_backend_connections = max_backend_connections

        self._ports = []

    def _get_pgaddr(self):
        pg_con_spec = self._cluster.get_connection_spec()
        if 'host' not in pg_con_spec and 'dsn' in pg_con_spec:
            parsed = urllib.parse.urlparse(pg_con_spec['dsn'])
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            host = query.get("host")[-1]
            port = query.get("port")[-1]
        else:
            host = pg_con_spec.get("host")
            port = pg_con_spec.get("port")

        return os.path.join(host, f'.s.PGSQL.{port}')

    def add_port(self, portcls, **kwargs):
        if self._serving:
            raise RuntimeError(
                'cannot add new ports after start() call')

        self._ports.append(
            portcls(
                loop=self._loop,
                pg_addr=self._pg_addr,
                pg_data_dir=self._pg_data_dir,
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
