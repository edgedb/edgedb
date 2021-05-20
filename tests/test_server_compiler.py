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
import contextlib
import os
import pickle
import signal
import subprocess
import sys
import tempfile

from edb.testbase import lang as tb
from edb.testbase import server as tbs
from edb.server import compiler as edbcompiler
from edb.server.compiler_pool import amsg
from edb.server.compiler_pool import pool


class TestServerCompiler(tb.BaseSchemaLoadTest):

    SCHEMA = '''
        type Foo {
            property bar -> str;
        }
    '''

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._std_schema = tb._load_std_schema()

    def test_server_compiler_compile_edgeql_script(self):
        compiler = tb.new_compiler()
        context = edbcompiler.new_compiler_context(
            user_schema=self.schema,
            modaliases={None: 'test'},
        )

        edbcompiler.compile_edgeql_script(
            compiler=compiler,
            ctx=context,
            eql='''
                SELECT Foo {
                    bar
                }
            ''',
        )


class ServerProtocol(amsg.ServerProtocol):
    def __init__(self):
        self.connected = asyncio.Queue()
        self.disconnected = asyncio.Queue()
        self.pids = set()

    def worker_connected(self, pid):
        self.connected.put_nowait(pid)
        self.pids.add(pid)

    def worker_disconnected(self, pid):
        self.disconnected.put_nowait(pid)
        self.pids.remove(pid)


class TestCompilerPool(tbs.TestCase):
    @contextlib.asynccontextmanager
    async def compiler_pool(self, num_proc):
        proto = ServerProtocol()

        with tempfile.TemporaryDirectory() as td:
            sock_name = f'{td}/compiler.sock'
            server = amsg.Server(sock_name, self.loop, proto)
            await server.start()
            try:
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", pool.WORKER_MOD,
                    "--sockname", sock_name,
                    "--numproc", str(num_proc),
                    env=pool._ENV,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                try:
                    yield server, proto, proc, sock_name
                finally:
                    try:
                        proc.terminate()
                        await proc.wait()
                    except ProcessLookupError:
                        pass
            finally:
                await server.stop()
                self.assertEqual(len(proto.pids), 0)

    async def check_pid(self, pid, server):
        conn = server.get_by_pid(pid)
        resp = await conn.request(pickle.dumps(('not_exist', ())))
        status, *data = pickle.loads(resp)
        self.assertEqual(status, 1)
        self.assertIsInstance(data[0], RuntimeError)

    async def test_server_compiler_pool_restart(self):
        pids = []
        async with self.compiler_pool(2) as (server, proto, proc, sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), 10)
            pid2 = await asyncio.wait_for(proto.connected.get(), 1)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            # Worker killed with SIGTERM shall be restarted
            os.kill(pid1, signal.SIGTERM)
            pid = await asyncio.wait_for(proto.disconnected.get(), 1)
            pids.append(pid)
            self.assertEqual(pid, pid1)
            pid3 = await asyncio.wait_for(proto.connected.get(), 1)
            self.assertNotIn(pid3, (pid1, pid2))
            await self.check_pid(pid3, server)

            # Worker killed with SIGKILL shall be restarted
            os.kill(pid2, signal.SIGKILL)
            pid = await asyncio.wait_for(proto.disconnected.get(), 1)
            pids.append(pid)
            self.assertEqual(pid, pid2)
            pid4 = await asyncio.wait_for(proto.connected.get(), 1)
            self.assertNotIn(pid4, (pid1, pid2, pid3))
            await self.check_pid(pid4, server)

            # Worker killed with SIGINT shall NOT be restarted
            os.kill(pid3, signal.SIGINT)
            pid = await asyncio.wait_for(proto.disconnected.get(), 1)
            pids.append(pid)
            self.assertEqual(pid, pid3)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(proto.connected.get(), 1)

        # The only remaining worker should be terminated on exit
        pid = await asyncio.wait_for(proto.disconnected.get(), 1)
        pids.append(pid)

        # Make sure all the workers are gone
        for pid in pids:
            with self.assertRaises(OSError):
                os.kill(pid, 0)

    async def test_server_compiler_pool_template_proc_exit(self):
        async with self.compiler_pool(2) as (server, proto, proc, sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), 10)
            pid2 = await asyncio.wait_for(proto.connected.get(), 1)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            proc.terminate()
            await proc.wait()

            pids = []
            for _ in range(2):
                pid = await asyncio.wait_for(proto.disconnected.get(), 1)
                pids.append(pid)

            self.assertIn(pid1, pids)
            self.assertIn(pid2, pids)

            # Make sure all the workers are gone
            for pid in pids:
                with self.assertRaises(OSError):
                    os.kill(pid, 0)

    async def test_server_compiler_pool_server_exit(self):
        async with self.compiler_pool(2) as (server, proto, proc, sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), 10)
            pid2 = await asyncio.wait_for(proto.connected.get(), 1)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            await server.stop()

            await asyncio.wait_for(proc.wait(), 1)

            pids = []
            for _ in range(2):
                pid = await asyncio.wait_for(proto.disconnected.get(), 1)
                pids.append(pid)

            self.assertIn(pid1, pids)
            self.assertIn(pid2, pids)

            # Make sure all the workers are gone
            for pid in pids:
                with self.assertRaises(OSError):
                    os.kill(pid, 0)

    async def test_server_compiler_pool_no_socket(self):
        async with self.compiler_pool(2) as (server, proto, proc, sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), 10)
            pid2 = await asyncio.wait_for(proto.connected.get(), 1)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            os.unlink(sn)
            os.kill(pid1, signal.SIGTERM)

            await asyncio.wait_for(proc.wait(), 1)

            pids = []
            while not proto.disconnected.empty():
                pids.append(proto.disconnected.get_nowait())

            # Make sure all the workers are gone
            for pid in pids:
                with self.assertRaises(OSError):
                    os.kill(pid, 0)
