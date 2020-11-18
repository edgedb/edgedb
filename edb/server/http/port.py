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


from __future__ import annotations

from typing import *

import asyncio
import logging

from edb.common import taskgroup
from edb.common import windowedsum

from edb.server import baseport
from edb.server import cache
from edb.server import defines

log_metrics = logging.getLogger('edb.server.metrics')


class BaseHttpPort(baseport.Port):

    def __init__(self, nethost: str, netport: int,
                 database: str,
                 user: str,
                 concurrency: int,
                 protocol: str,
                 **kwargs):

        super().__init__(**kwargs)
        self._compiler_pool_size = 0

        if protocol != self.get_proto_name():
            raise RuntimeError(f'unknown protocol {protocol!r}')
        if concurrency <= 0 or concurrency > defines.HTTP_PORT_MAX_CONCURRENCY:
            raise RuntimeError(
                f'concurrency must be greater than 0 and '
                f'less than {defines.HTTP_PORT_MAX_CONCURRENCY}')

        self._compilers: asyncio.LifoQueue[Any] = asyncio.LifoQueue()
        self._pgcons: asyncio.LifoQueue[Any] = asyncio.LifoQueue()
        self._compilers_list: List[Any] = []
        self._pgcons_list: List[Any] = []

        self._nethost = nethost
        self._netport = netport

        self.database = database
        self.user = user
        self.concurrency = concurrency
        self.last_minute_requests = windowedsum.WindowedSum()

        self._http_proto_server = None
        self._http_request_logger = None
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

        nethost = await self._fix_localhost(self._nethost, self._netport)
        self._http_proto_server = await self._loop.create_server(
            self.build_protocol,
            host=nethost, port=self._netport)
        self._http_request_logger = asyncio.create_task(
            self.request_stats_logger()
        )

    async def stop(self):
        try:
            srv = self._http_proto_server
            if srv is not None:
                self._http_proto_server = None
                srv.close()
                await srv.wait_closed()
        finally:
            try:
                async with taskgroup.TaskGroup() as g:
                    for cmp in self._compilers_list:
                        g.create_task(cmp.close())
                    self._compilers_list.clear()

                for pgcon in self._pgcons_list:
                    pgcon.terminate()
                self._pgcons_list.clear()
                if self._http_request_logger is not None:
                    self._http_request_logger.cancel()
                    await self._http_request_logger
            finally:
                await super().stop()

    async def request_stats_logger(self):
        last_seen = -1
        while True:
            current = int(self.last_minute_requests)
            if current != last_seen:
                log_metrics.info(
                    "HTTP requests for %s-%s in last minute: %d",
                    self.get_proto_name(),
                    self._netport,
                    current,
                )
                last_seen = current
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                return
