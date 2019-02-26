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


import weakref

from edb.common import taskgroup
from edb.server import baseport
from edb.server import compiler

from . import edgecon


class Backend:

    def __init__(self, pgcon, compiler):
        self._pgcon = pgcon
        self._compiler = compiler

    @property
    def pgcon(self):
        return self._pgcon

    @property
    def compiler(self):
        return self._compiler

    async def close(self):
        self._pgcon.terminate()
        await self._compiler.close()


class ManagementPort(baseport.Port):

    def __init__(self, nethost: str, netport: int, **kwargs):
        super().__init__(**kwargs)

        self._nethost = nethost
        self._netport = netport

        self._edgecon_id = 0

        self._servers = []
        self._backends = weakref.WeakSet()

    def new_view(self, *, dbname, user, query_cache):
        return self._dbindex.new_view(
            dbname, user=user, query_cache=query_cache)

    def get_compiler_worker_cls(self):
        return compiler.Compiler

    def get_compiler_worker_name(self):
        return 'compiler-mng'

    async def new_backend(self, *, dbname: str, dbver: int):
        server = self.get_server()

        async with taskgroup.TaskGroup() as g:
            new_pgcon_task = g.create_task(server.new_pgcon(dbname))
            compiler_task = g.create_task(self.new_compiler(dbname, dbver))

        backend = Backend(
            new_pgcon_task.result(),
            compiler_task.result())

        self._backends.add(backend)
        return backend

    def new_edgecon_id(self):
        self._edgecon_id += 1
        return str(self._edgecon_id)

    async def start(self):
        await super().start()

        srv = await self._loop.create_server(
            lambda: edgecon.EdgeConnection(self),
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
                    for backend in self._backends:
                        g.create_task(backend.close())
            finally:
                await super().stop()
