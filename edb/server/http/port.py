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
from edb.server import cache
from edb.server import defines


class BaseHttpPort(baseport.Port):

    def __init__(self, nethost: str, netport: int,
                 database: str,
                 user: str,
                 concurrency: int,
                 protocol: str,
                 **kwargs):

        super().__init__(**kwargs)

        if protocol != self.get_proto_name():
            raise RuntimeError(f'unknown protocol {protocol!r}')
        if concurrency <= 0 or concurrency > defines.HTTP_PORT_MAX_CONCURRENCY:
            raise RuntimeError(
                f'concurrency must be greater than 0 and '
                f'less than {defines.HTTP_PORT_MAX_CONCURRENCY}')

        self._compilers = asyncio.LifoQueue()
        self._pgcons = asyncio.LifoQueue()
        self._compilers_list = []
        self._pgcons_list = []

        self._nethost = nethost
        self._netport = netport

        self.database = database
        self.user = user
        self.concurrency = concurrency

        self._servers = []
        self._query_cache = cache.StatementsCache(
            maxsize=defines.HTTP_PORT_QUERY_CACHE_SIZE)

    @property
    def compilers(self):
        return self._compilers

    @property
    def pgcons(self):
        return self._pgcons

    @classmethod
    def get_proto_name(cls):
        raise NotImplementedError

    def get_dbver(self):
        return self._dbindex.get_dbver(self.database)

    def get_compiler_worker_cls(self):
        raise NotImplementedError

    def get_compiler_worker_name(self):
        return f'compiler-{self._netport}'

    def build_protocol(self):
        raise NotImplementedError

    async def start(self):
        compilers = []
        pgcons = []

        await super().start()

        async with taskgroup.TaskGroup() as g:
            for _ in range(self.concurrency):
                compilers.append(
                    g.create_task(self.new_compiler(
                        self.database, self.get_dbver())))
                pgcons.append(
                    g.create_task(self.get_server().new_pgcon(self.database)))

        for com_task in compilers:
            self._compilers.put_nowait(com_task.result())
            self._compilers_list.append(com_task.result())

        for con_task in pgcons:
            self._pgcons.put_nowait(con_task.result())
            self._pgcons_list.append(con_task.result())

        srv = await self._loop.create_server(
            self.build_protocol,
            host=self._nethost, port=self._netport)

        self._servers.append(srv)

    async def stop(self):
        try:
            async with taskgroup.TaskGroup() as g:
                for srv in self._servers:
                    srv.close()
                    g.create_task(srv.wait_closed())
        finally:
            try:
                async with taskgroup.TaskGroup() as g:
                    for cmp in self._compilers_list:
                        g.create_task(cmp.close())

                for pgcon in self._pgcons_list:
                    pgcon.terminate()
            finally:
                await super().stop()
