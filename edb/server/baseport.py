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


from edb.common import devmode
from edb.server import procpool


class Port:

    def __init__(self, *, server, loop,
                 pg_addr, pg_data_dir, runstate_dir, dbindex):

        self._server = server
        self._loop = loop
        self._pg_addr = pg_addr
        self._pg_data_dir = pg_data_dir
        self._dbindex = dbindex
        self._runstate_dir = runstate_dir

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
            runstate_dir=self._runstate_dir,
            worker_args=(dict(host=self._pg_addr), self._pg_data_dir),
            worker_cls=self.get_compiler_worker_cls(),
            name=self.get_compiler_worker_name(),
        )

    async def stop(self):
        await self._compiler_manager.stop()
        self._compiler_manager = None
        self._serving = False
