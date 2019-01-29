#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

from edb.server import backend
from edb.server import baseport

from . import edgecon


class ManagementPort(baseport.Port):

    def __init__(self, nethost: str, netport: int, **kwargs):
        super().__init__(**kwargs)

        self._nethost = nethost
        self._netport = netport

        self._edgecon_id = 0

        self._backend_manager = None
        self._serving = False

        self._servers = []

    def new_view(self, *, dbname, user, query_cache):
        return self._dbindex.new_view(
            dbname, user=user, query_cache=query_cache)

    async def new_backend(self, *, dbname: str, dbver: int):
        return await self._backend_manager.new_backend(
            dbname=dbname, dbver=dbver)

    def new_edgecon_id(self):
        self._edgecon_id += 1
        return str(self._edgecon_id)

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        pg_con_spec = self._cluster.get_connection_spec()
        if 'host' not in pg_con_spec and 'dsn' in pg_con_spec:
            # XXX
            parsed = urllib.parse.urlparse(pg_con_spec['dsn'])
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            host = query.get("host")[-1]
            port = query.get("port")[-1]
        else:
            host = pg_con_spec.get("host")
            port = pg_con_spec.get("port")

        pgaddr = os.path.join(host, f'.s.PGSQL.{port}')

        self._backend_manager = backend.BackendManager(
            runstate_dir=self._runstate_dir,
            data_dir=self._cluster.get_data_dir(),
            pgaddr=pgaddr)
        await self._backend_manager.start()

        srv = await self._loop.create_server(
            lambda: edgecon.EdgeConnection(self),
            host=self._nethost, port=self._netport)

        self._servers.append(srv)

    async def stop(self):
        self._serving = False

        async with taskgroup.TaskGroup() as g:
            for srv in self._servers:
                srv.close()
                g.create_task(srv.wait_closed())

        await self._backend_manager.stop()

        self._backend_manager = None
