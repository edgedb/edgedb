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

import collections.abc
import socket

from edb.common import devmode
from edb.server import procpool


class Port:

    def __init__(self, *, server, loop,
                 pg_addr,
                 runstate_dir, internal_runstate_dir,
                 dbindex):

        self._server = server
        self._loop = loop
        self._pg_addr = pg_addr
        self._dbindex = dbindex
        self._runstate_dir = runstate_dir
        self._internal_runstate_dir = internal_runstate_dir

        self._devmode = devmode.is_in_dev_mode()

        self._compiler_manager = None
        self._serving = False

    def in_dev_mode(self):
        return self._devmode

    def get_loop(self):
        return self._loop

    def get_server(self):
        return self._server

    def get_compiler_worker_cls(self):
        raise NotImplementedError

    def get_compiler_worker_name(self):
        raise NotImplementedError

    async def new_compiler(self, dbname, dbver):
        compiler_worker = await self._compiler_manager.spawn_worker()
        try:
            await compiler_worker.call('connect', dbname, dbver)
        except Exception:
            await compiler_worker.close()
            raise
        return compiler_worker

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        self._compiler_manager = await procpool.create_manager(
            runstate_dir=self._internal_runstate_dir,
            worker_args=(self._pg_addr,),
            worker_cls=self.get_compiler_worker_cls(),
            name=self.get_compiler_worker_name(),
        )

    async def stop(self):
        if self._compiler_manager is not None:
            await self._compiler_manager.stop()
            self._compiler_manager = None
        self._compiler_manager = None
        self._serving = False

    async def _fix_localhost(self, host, port):
        # On many systems 'localhost' resolves to _both_ IPv4 and IPv6
        # addresses, even if the system is not capable of handling
        # IPv6 connections.  Due to the common nature of this issue
        # we explicitly disable the AF_INET6 component of 'localhost'.

        if (isinstance(host, str)
                or not isinstance(host, collections.abc.Iterable)):
            hosts = [host]
        else:
            hosts = list(host)

        try:
            idx = hosts.index('localhost')
        except ValueError:
            # No localhost, all good
            return hosts

        localhost = await self._loop.getaddrinfo(
            'localhost',
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            flags=socket.AI_PASSIVE,
            proto=0,
        )

        infos = [a for a in localhost if a[0] == socket.AF_INET]

        if not infos:
            # "localhost" did not resolve to an IPv4 address,
            # let create_server handle the situation.
            return hosts

        # Replace 'localhost' with explicitly resolved AF_INET addresses.
        hosts.pop(idx)
        for info in reversed(infos):
            addr, *_ = info[4]
            hosts.insert(idx, addr)

        return hosts
