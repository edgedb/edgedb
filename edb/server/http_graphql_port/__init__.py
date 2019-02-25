#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import asyncio

from edb.common import taskgroup
from edb.server import baseport

from . import protocol


class HttpGraphQLPort(baseport.Port):

    def __init__(self, nethost: str, netport: int,
                 database: str,
                 user: str,
                 concurrency: int,
                 protocol: str,
                 **kwargs):
        super().__init__(**kwargs)

        if protocol != 'http+graphql':
            raise RuntimeError(f'unknown protocol {protocol!r}')
        if concurrency <= 0 or concurrency > 500:
            raise RuntimeError(
                f'concurrency must be greater than 0 and less than 500')

        self._compilers = asyncio.LifoQueue()
        self._pgcons = asyncio.LifoQueue()
        self._compilers_list = []
        self._pgcons_list = []

        self._nethost = nethost
        self._netport = netport

        self.database = database
        self.user = user
        self.concurrency = concurrency

        self._serving = False
        self._servers = []

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        dbver = self._dbindex.get_dbver(self.database)

        compilers = []
        pgcons = []

        async with taskgroup.TaskGroup() as g:
            for _ in range(self.concurrency):
                compilers.append(
                    g.create_task(self.get_server().new_compiler(
                        self.database, dbver)))
                pgcons.append(
                    g.create_task(self.get_server().new_pgcon(self.database)))

        for com_task in compilers:
            self._compilers.put_nowait(com_task.result())
            self._compilers_list.append(com_task.result())

        for con_task in pgcons:
            self._pgcons.put_nowait(con_task.result())
            self._pgcons_list.append(con_task.result())

        srv = await self._loop.create_server(
            lambda: protocol.Protocol(self._loop, self),
            host=self._nethost, port=self._netport)

        self._servers.append(srv)

    async def stop(self):
        self._serving = False

        try:
            async with taskgroup.TaskGroup() as g:
                for srv in self._servers:
                    srv.close()
                    g.create_task(srv.wait_closed())
        finally:
            async with taskgroup.TaskGroup() as g:
                for compiler in self._compilers_list:
                    g.create_task(compiler.close())

            for pgcon in self._pgcons_list:
                pgcon.terminate()
