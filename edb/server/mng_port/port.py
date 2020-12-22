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


from __future__ import annotations
from typing import *

import asyncio
import logging
import os
import os.path
import stat
import weakref

from edb.common import taskgroup
from edb.server import baseport
from edb.server import compiler

from . import edgecon  # type: ignore[attr-defined]


logger = logging.getLogger('edb.server')
log_metrics = logging.getLogger('edb.server.metrics')


class Backend:

    def __init__(self, compiler):
        self._compiler = compiler

    @property
    def compiler(self):
        return self._compiler

    async def close(self):
        await self._compiler.close()


class ManagementPort(baseport.Port):

    _servers: List[asyncio.AbstractServer]

    def __init__(
        self,
        *,
        nethost: str,
        netport: int,
        auto_shutdown: bool,
        max_protocol: Tuple[int, int],
        startup_script=None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._nethost = nethost
        self._netport = netport

        self._edgecon_id = 0
        self._num_connections = 0

        self._servers = []
        self._backends = weakref.WeakSet()  # type: ignore[var-annotated]

        self._auto_shutdown = auto_shutdown
        self._accepting = False
        self._max_protocol = max_protocol
        self._startup_script = startup_script

    def new_view(self, *, dbname, user, query_cache):
        return self._dbindex.new_view(
            dbname, user=user, query_cache=query_cache)

    def get_compiler_worker_cls(self):
        return compiler.Compiler

    def get_compiler_worker_name(self):
        return 'compiler-mng'

    async def new_backend(self, *, dbname: str, dbver: int):
        backend = Backend(await self.new_compiler(dbname, dbver))
        self._backends.add(backend)
        return backend

    def on_client_connected(self) -> str:
        self._edgecon_id += 1
        return str(self._edgecon_id)

    def on_client_authed(self):
        self._num_connections += 1
        self._report_connections()

    def on_client_disconnected(self):
        self._num_connections -= 1
        self._report_connections(action="close")
        if not self._num_connections and self._auto_shutdown:
            self._accepting = False
            raise SystemExit

    async def run_startup_script_and_exit(self):
        """Run the script specified in *startup_script* and exit immediately"""
        await super().start()

        try:
            await edgecon.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )
        finally:
            await super().stop()

    async def start(self):
        await super().start()

        if self._startup_script is not None:
            await edgecon.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )

        nethost = await self._fix_localhost(self._nethost, self._netport)

        tcp_srv = await self._loop.create_server(
            lambda: edgecon.EdgeConnection(self),
            host=nethost, port=self._netport)

        try:
            unix_sock_path = os.path.join(
                self._runstate_dir, f'.s.EDGEDB.{self._netport}')
            unix_srv = await self._loop.create_unix_server(
                lambda: edgecon.EdgeConnection(self),
                unix_sock_path)
        except Exception:
            tcp_srv.close()
            await tcp_srv.wait_closed()
            raise

        try:
            admin_unix_sock_path = os.path.join(
                self._runstate_dir, f'.s.EDGEDB.admin.{self._netport}')
            admin_unix_srv = await self._loop.create_unix_server(
                lambda: edgecon.EdgeConnection(self, external_auth=True),
                admin_unix_sock_path)
            os.chmod(admin_unix_sock_path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            tcp_srv.close()
            await tcp_srv.wait_closed()
            unix_srv.close()
            await unix_srv.wait_closed()
            raise

        self._servers.append(tcp_srv)
        if len(nethost) > 1:
            host_str = f"{{{', '.join(nethost)}}}"
        else:
            host_str = next(iter(nethost))
        logger.info('Serving on %s:%s', host_str, self._netport)
        self._servers.append(unix_srv)
        logger.info('Serving on %s', unix_sock_path)
        self._servers.append(admin_unix_srv)
        logger.info('Serving admin on %s', admin_unix_sock_path)

        self._accepting = True

    async def stop(self):
        self._accepting = False
        try:
            async with taskgroup.TaskGroup() as g:
                for srv in self._servers:
                    srv.close()
                    g.create_task(srv.wait_closed())
                self._servers.clear()
        finally:
            try:
                async with taskgroup.TaskGroup() as g:
                    for backend in self._backends:
                        g.create_task(backend.close())
                    self._backends.clear()
            finally:
                await super().stop()

    def _report_connections(self, *, action: str = "open"):
        action = action.capitalize()
        if not action.endswith("e"):
            action += "e"
        action += "d"
        log_metrics.info(
            "%s a connection with ID %d; open_count=%d",
            action,
            self._edgecon_id,
            self._num_connections,
        )
