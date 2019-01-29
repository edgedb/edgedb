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


import pathlib
import weakref

from edb.common import taskgroup

from edb.edgeql import parser as ql_parser

from edb.server import pgcon
from edb.server import procpool

from . import compiler
from . import stdschema


__all__ = ('BackendManager',)


class Backend:

    def __init__(self, pgcon, compiler, std_schema):
        self._pgcon = pgcon
        self._compiler = compiler
        self._std_schema = std_schema

    @property
    def std_schema(self):
        return self._std_schema

    @property
    def pgcon(self):
        return self._pgcon

    @property
    def compiler(self):
        return self._compiler

    async def close(self):
        self._pgcon.terminate()
        await self._compiler.close()


class BackendManager:

    def __init__(self, *, pgaddr, runstate_dir, data_dir):
        self._pgaddr = pgaddr
        self._runstate_dir = runstate_dir
        self._data_dir = data_dir

        self._backends = weakref.WeakSet()

        self._compiler_manager = None

    async def start(self):
        # Make sure that EdgeQL parser is preloaded; edgecon might use
        # it to restore config values.
        ql_parser.preload()
        # std schema is also needed to restore config values.
        self._std_schema = stdschema.load(pathlib.Path(self._data_dir))

        self._compiler_manager = await procpool.create_manager(
            runstate_dir=self._runstate_dir,
            name='edgedb-compiler',
            worker_cls=compiler.Compiler,
            worker_args=(dict(host=self._pgaddr), self._data_dir))

    async def stop(self):
        # TODO: Make a graceful version of this.
        try:
            async with taskgroup.TaskGroup() as g:
                for backend in self._backends:
                    g.create_task(backend.close())
        finally:
            await self._compiler_manager.stop()

    async def new_backend(self, *, dbname: str, dbver: int):
        async def new_compiler():
            compiler_worker = await self._compiler_manager.spawn_worker()
            try:
                await compiler_worker.call('connect', dbname, dbver)
            except Exception:
                await compiler_worker.close()
                raise
            return compiler_worker

        async with taskgroup.TaskGroup() as g:
            new_pgcon_task = g.create_task(
                pgcon.connect(self._pgaddr, dbname))

            compiler_task = g.create_task(new_compiler())

        backend = Backend(
            new_pgcon_task.result(),
            compiler_task.result(),
            self._std_schema)

        self._backends.add(backend)
        return backend
