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

from typing import Any

import asyncio
import contextlib
import os
import pickle
import signal
import subprocess
import sys
import tempfile
import time
import unittest.mock
import uuid

import immutables

from edb import edgeql
from edb import errors
from edb.ir import statypes
from edb.testbase import lang as tb
from edb.testbase import server as tbs
from edb.pgsql import params as pg_params
from edb.server import args as edbargs
from edb.server import compiler as edbcompiler
from edb.server.compiler import rpc
from edb.server import config
from edb.server.compiler_pool import amsg
from edb.server.compiler_pool import pool
from edb.server.dbview import dbview


SHORT_WAIT = 5
LONG_WAIT = 60


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

    def setUp(self):
        super().setUp()
        self.compiler = tb.new_compiler()

    def test_server_compiler_compile_edgeql_script(self):
        context = edbcompiler.new_compiler_context(
            compiler_state=self.compiler.state,
            user_schema=self.schema,
            modaliases={None: 'default'},
        )

        edbcompiler.compile_edgeql_script(
            ctx=context,
            eql='''
                SELECT Foo {
                    bar
                }
            ''',
        )

    def _test_compile_structured_config(
        self,
        values: dict[str, Any],
        *,
        source: str = "config file",
        **expected: Any,
    ) -> dict[str, config.SettingValue]:
        result = self.compiler.compile_structured_config(
            {"cfg::Config": values}, source=source, allow_nested=True
        )
        rv = dict(result["cfg::Config"])
        for name, setting in rv.items():
            self.assertEqual(setting.name, name)
            self.assertEqual(setting.scope, config.ConfigScope.INSTANCE)
            self.assertEqual(setting.source, source)
        self.assertDictEqual({k: v.value for k, v in rv.items()}, expected)
        return rv

    def composite_obj(self, _type_name, **values):
        return config.CompositeConfigType(
            self.compiler.state.config_spec.get_type_by_name(_type_name),
            **values,
        )

    def test_server_compiler_compile_structured_config_01(self):
        self._test_compile_structured_config(
            {
                "singleprop": "value",
                "memprop": 512,
                "durprop": "16 seconds",
                "enumprop": "One",
                "multiprop": ["v1", "v2", "v3"],
                "listen_port": 5,
                "sysobj": [
                    {
                        "name": "1",
                        "obj": {
                            "_tname": "cfg::Subclass1",
                            "name": "aa",
                            "sub1": "bb",
                        },
                    },
                    {
                        "name": "2",
                        "_tname": "cfg::TestInstanceConfigStatTypes",
                        "memprop": 128,
                    },
                ],
            },
            singleprop="value",
            memprop=statypes.ConfigMemory(512),
            durprop=statypes.Duration.from_microseconds(16 * 1_000_000),
            enumprop="One",
            multiprop=frozenset(["v1", "v2", "v3"]),
            listen_port=5,
            sysobj=frozenset([
                self.composite_obj(
                    "cfg::TestInstanceConfig",
                    name="1",
                    obj=self.composite_obj(
                        "cfg::Subclass1", name="aa", sub1="bb",
                    ),
                ),
                self.composite_obj(
                    "cfg::TestInstanceConfigStatTypes",
                    name="2",
                    memprop=statypes.ConfigMemory(128),
                ),
            ])
        )

    def test_server_compiler_compile_structured_config_02(self):
        self._test_compile_structured_config(
            {"singleprop": 42}, singleprop="42"
        )

    def test_server_compiler_compile_structured_config_03(self):
        self._test_compile_structured_config(
            {"singleprop": "{{'4' ++ <str>2}}"}, singleprop="42"
        )

    def test_server_compiler_compile_structured_config_04(self):
        with self.assertRaisesRegex(
            errors.ConfigurationError, "unsupported input type"
        ):
            self._test_compile_structured_config({"singleprop": ["1", "2"]})

    def test_server_compiler_compile_structured_config_05(self):
        with self.assertRaisesRegex(
            errors.ConfigurationError, "unsupported input type"
        ):
            self._test_compile_structured_config({"singleprop": {"a": "x"}})

    def test_server_compiler_compile_structured_config_06(self):
        self._test_compile_structured_config(
            {"listen_port": "8080"}, listen_port=8080
        )

    def test_server_compiler_compile_structured_config_07(self):
        self._test_compile_structured_config(
            {"multiprop": "single"}, multiprop=frozenset(["single"])
        )

    def test_server_compiler_compile_structured_config_08(self):
        with self.assertRaisesRegex(
            errors.ConfigurationError, "must be a sequence"
        ):
            self._test_compile_structured_config({"multiprop": {"a": 1}})

    def test_server_compiler_compile_structured_config_09(self):
        with self.assertRaisesRegex(
            errors.InvalidReferenceError, "has no member"
        ):
            self._test_compile_structured_config({"enumprop": "non_exist"})

    def test_server_compiler_compile_structured_config_10(self):
        with self.assertRaisesRegex(
            errors.ConfigurationError, "does not have field"
        ):
            self._test_compile_structured_config({"non_exist": 123})

    def test_server_compiler_compile_structured_config_11(self):
        with self.assertRaisesRegex(
            errors.ConfigurationError, "type of `_tname` must be str"
        ):
            self._test_compile_structured_config({"sysobj": [{"_tname": 123}]})

    def test_server_compiler_compile_structured_config_12(self):
        with self.assertRaisesRegex(
            errors.ConstraintViolationError,
            "name violates exclusivity constraint",
        ):
            self._test_compile_structured_config(
                {"sysobj": [{"name": "same"}, {"name": "same"}]}
            )


class ServerProtocol(amsg.ServerProtocol):
    def __init__(self):
        self.connected = asyncio.Queue()
        self.disconnected = asyncio.Queue()
        self.pids = set()

    def worker_connected(self, pid, version):
        self.connected.put_nowait(pid)
        self.pids.add(pid)

    def worker_disconnected(self, pid):
        self.disconnected.put_nowait(pid)
        self.pids.remove(pid)


class TestAmsg(tbs.TestCase):
    @contextlib.asynccontextmanager
    async def compiler_pool(self, num_proc):
        proto = ServerProtocol()

        with tempfile.TemporaryDirectory() as td:
            sock_name = f'{td}/compiler.sock'
            server = amsg.Server(sock_name, self.loop, proto)
            await server.start()
            try:
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m", pool.WORKER_PKG + pool.BaseLocalPool._worker_mod,
                    "--sockname", sock_name,
                    "--numproc", str(num_proc),
                    "--version-serial", "1",
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
        async with self.compiler_pool(2) as (server, proto, _proc, _sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), LONG_WAIT)
            pid2 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            # Worker killed with SIGTERM shall be restarted
            os.kill(pid1, signal.SIGTERM)
            pid = await asyncio.wait_for(proto.disconnected.get(), SHORT_WAIT)
            pids.append(pid)
            self.assertEqual(pid, pid1)
            pid3 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            self.assertNotIn(pid3, (pid1, pid2))
            await self.check_pid(pid3, server)

            # Worker killed with SIGKILL shall be restarted
            os.kill(pid2, signal.SIGKILL)
            pid = await asyncio.wait_for(proto.disconnected.get(), SHORT_WAIT)
            pids.append(pid)
            self.assertEqual(pid, pid2)
            pid4 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            self.assertNotIn(pid4, (pid1, pid2, pid3))
            await self.check_pid(pid4, server)

            # Worker killed with SIGINT shall NOT be restarted
            os.kill(pid3, signal.SIGINT)
            pid = await asyncio.wait_for(proto.disconnected.get(), SHORT_WAIT)
            pids.append(pid)
            self.assertEqual(pid, pid3)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)

        # The only remaining worker should be terminated on exit
        pid = await asyncio.wait_for(proto.disconnected.get(), SHORT_WAIT)
        pids.append(pid)

        # Make sure all the workers are gone
        for pid in pids:
            with self.assertRaises(OSError):
                os.kill(pid, 0)

    async def test_server_compiler_pool_template_proc_exit(self):
        async with self.compiler_pool(2) as (server, proto, proc, _sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), LONG_WAIT)
            pid2 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            # Make sure the template process is ready to listen to signals
            # by testing its restarting feature
            pids = []
            os.kill(pid1, signal.SIGTERM)
            pid = await asyncio.wait_for(proto.disconnected.get(), SHORT_WAIT)
            pids.append(pid)
            self.assertEqual(pid, pid1)
            pid3 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            self.assertNotIn(pid3, (pid1, pid2))
            await self.check_pid(pid3, server)

            # Kill the template process, it should kill all its children
            proc.terminate()
            await proc.wait()

            for _ in range(2):
                pid = await asyncio.wait_for(
                    proto.disconnected.get(), SHORT_WAIT)
                pids.append(pid)

            self.assertIn(pid1, pids)
            self.assertIn(pid2, pids)
            self.assertIn(pid3, pids)

            # Make sure all the workers are gone
            for pid in pids:
                with self.assertRaises(OSError):
                    os.kill(pid, 0)

    async def test_server_compiler_pool_server_exit(self):
        async with self.compiler_pool(2) as (server, proto, proc, _sn):
            # Make sure both compiler workers are up and ready
            pid1 = await asyncio.wait_for(proto.connected.get(), LONG_WAIT)
            pid2 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            await server.stop()

            await asyncio.wait_for(proc.wait(), SHORT_WAIT)

            pids = []
            for _ in range(2):
                pid = await asyncio.wait_for(
                    proto.disconnected.get(), SHORT_WAIT)
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
            pid1 = await asyncio.wait_for(proto.connected.get(), LONG_WAIT)
            pid2 = await asyncio.wait_for(proto.connected.get(), SHORT_WAIT)
            await self.check_pid(pid1, server)
            await self.check_pid(pid2, server)

            # Destroy the UNIX domain socket file
            os.unlink(sn)
            # Kill one worker, the template process will try to restart it
            os.kill(pid1, signal.SIGTERM)
            # But the new worker won't be able to connect to the UNIX socket,
            # the template process should abort in a reasonable time with
            # enough retries, depending on the number of CPU cores.
            await asyncio.wait_for(proc.wait(), 30)

            pids = []
            while not proto.disconnected.empty():
                pids.append(proto.disconnected.get_nowait())

            # Make sure all the workers are gone
            for pid in pids:
                with self.assertRaises(OSError):
                    os.kill(pid, 0)


class TestServerCompilerPool(tbs.TestCase):
    def _wait_pids(self, *pids, timeout=1):
        remaining = list(pids)

        start = time.monotonic()
        while pids and time.monotonic() - start < timeout:
            for pid in tuple(remaining):
                try:
                    os.kill(pid, 0)
                except OSError:
                    remaining.remove(pid)
            if remaining:
                time.sleep(0.1)
        return remaining

    def _kill_and_wait(self, *pids, sig=signal.SIGTERM, timeout=1):
        for pid in pids:
            os.kill(pid, sig)
        remaining = self._wait_pids(*pids, timeout=timeout)
        if remaining:
            raise TimeoutError(
                f"Failed to kill PID {remaining} with {sig} "
                f"in {timeout} second(s)"
            )

    async def _get_worker_pids(self, sd, least_num=2, timeout=15):
        rv = []
        start = time.monotonic()
        while time.monotonic() - start < timeout and len(rv) < least_num:
            await asyncio.sleep(timeout / 50)
            pool_info = sd.fetch_server_info()['compiler_pool']
            rv = pool_info['worker_pids']
        if len(rv) < least_num:
            raise TimeoutError(
                f"Not enough workers found in {timeout} second(s)"
            )
        return rv

    def _get_template_pid(self, sd):
        return sd.fetch_server_info()['compiler_pool']['template_pid']

    async def test_server_compiler_pool_with_server(self):
        async with tbs.start_edgedb_server(
            compiler_pool_size=2,
            compiler_pool_mode=edbargs.CompilerPoolMode.Fixed,
            http_endpoint_security=(
                edbargs.ServerEndpointSecurityMode.Optional),
        ) as sd:
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')
            pid1, pid2 = await self._get_worker_pids(sd)

            data = sd.fetch_metrics()
            self.assertRegex(
                data, r'\nedgedb_server_compiler_processes_current 2.0\n'
            )

            # Terminate one worker, the server is still OK
            self._kill_and_wait(pid1)
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')

            # Confirm that another worker is started
            pids = set(await self._get_worker_pids(sd))
            self.assertIn(pid2, pids)
            pids.remove(pid2)
            self.assertEqual(len(pids), 1)
            pid3 = pids.pop()

            # Kill both workers, the server would need some time to recover
            os.kill(pid2, signal.SIGKILL)
            os.kill(pid3, signal.SIGKILL)
            time.sleep(0.1)
            start = time.monotonic()
            while time.monotonic() - start < 10:
                try:
                    self.assertEqual(
                        sd.call_system_api('/server/status/ready'), 'OK'
                    )
                except AssertionError:
                    time.sleep(0.1)
                else:
                    break
            pids = set(await self._get_worker_pids(sd))
            self.assertNotIn(pid1, pids)
            self.assertNotIn(pid2, pids)
            self.assertNotIn(pid3, pids)

            # Kill one worker with SIGINT, it's not restarted
            self._kill_and_wait(pids.pop(), sig=signal.SIGINT, timeout=5)
            time.sleep(1)
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')
            self.assertSetEqual(
                set(await self._get_worker_pids(sd, least_num=1)), pids
            )
            pid4 = pids.pop()

            # Kill the template process, the server shouldn't be
            # impacted immediately
            tmpl_pid1 = self._get_template_pid(sd)
            os.kill(tmpl_pid1, signal.SIGKILL)
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')

            # When the new template process is started, it will spawn 2 new
            # workers, and the old pid4 will then be killed.
            self._wait_pids(pid4)
            tmpl_pid2 = self._get_template_pid(sd)
            self.assertIsNotNone(tmpl_pid2)
            self.assertNotEqual(tmpl_pid1, tmpl_pid2)
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')

            # Make sure everything works again
            start = time.monotonic()
            while time.monotonic() - start < 10:
                pids = await self._get_worker_pids(sd, timeout=10)
                if pid4 not in pids:
                    break
            self.assertNotIn(pid4, pids)
            self.assertEqual(len(pids), 2)
            self.assertEqual(sd.call_system_api('/server/status/ready'), 'OK')


class TestCompilerPool(tbs.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._std_schema = tb._load_std_schema()
        result = tb._load_reflection_schema()
        cls._refl_schema, _schema_class_layout = result
        assert _schema_class_layout is not None
        cls._schema_class_layout = _schema_class_layout

    async def _test_pool_disconnect_queue(self, pool_class):
        with tempfile.TemporaryDirectory() as td:
            pool_ = await pool.create_compiler_pool(
                runstate_dir=td,
                pool_size=2,
                backend_runtime_params=pg_params.get_default_runtime_params(),
                std_schema=self._std_schema,
                refl_schema=self._refl_schema,
                schema_class_layout=self._schema_class_layout,
                pool_class=pool_class,
                dbindex=dbview.DatabaseIndex(
                    unittest.mock.MagicMock(),
                    std_schema=self._std_schema,
                    global_schema_pickle=pickle.dumps(None, -1),
                    sys_config={},
                    default_sysconfig=immutables.Map(),
                    sys_config_spec=config.load_spec_from_schema(
                        self._std_schema),
                ),
            )
            try:
                w1 = await pool_._acquire_worker()
                w2 = await pool_._acquire_worker()
                with self.assertRaises(AttributeError):
                    await w1.call('nonexist')
                with self.assertRaises(AttributeError):
                    await w2.call('nonexist')
                pool_._release_worker(w1)
                pool_._release_worker(w2)

                pool_._ready_evt.clear()
                os.kill(w1.get_pid(), signal.SIGTERM)
                os.kill(w2.get_pid(), signal.SIGTERM)
                await asyncio.wait_for(pool_._ready_evt.wait(), LONG_WAIT)

                compiler = edbcompiler.new_compiler(
                    std_schema=self._std_schema,
                    reflection_schema=self._refl_schema,
                    schema_class_layout=self._schema_class_layout,
                )

                context = edbcompiler.new_compiler_context(
                    compiler_state=compiler.state,
                    user_schema=self._std_schema,
                    modaliases={None: 'default'},
                )

                orig_query = 'SELECT 123'
                cfg_ser = compiler.state.compilation_config_serializer
                request = rpc.CompilationRequest(
                    source=edgeql.Source.from_string(orig_query),
                    protocol_version=(1, 0),
                    schema_version=uuid.uuid4(),
                    compilation_config_serializer=cfg_ser,
                    implicit_limit=101,
                )

                await asyncio.gather(*(pool_.compile_in_tx(
                    None,
                    pickle.dumps(context.state.root_user_schema),
                    context.state.current_tx().id,
                    pickle.dumps(context.state),
                    0,
                    request.serialize(),
                    orig_query,
                ) for _ in range(4)))
            finally:
                await pool_.stop()

    async def test_server_compiler_pool_disconnect_queue_fixed(self):
        await self._test_pool_disconnect_queue(pool.FixedPool)

    async def test_server_compiler_pool_disconnect_queue_adaptive(self):
        await self._test_pool_disconnect_queue(pool.SimpleAdaptivePool)

    def test_server_compiler_rpc_hash_eq(self):
        compiler = edbcompiler.new_compiler(
            std_schema=self._std_schema,
            reflection_schema=self._refl_schema,
            schema_class_layout=self._schema_class_layout,
        )

        def test(source: edgeql.Source):
            cfg_ser = compiler.state.compilation_config_serializer
            request1 = rpc.CompilationRequest(
                source=source,
                protocol_version=(1, 0),
                schema_version=uuid.uuid4(),
                compilation_config_serializer=cfg_ser,
            )
            request2 = rpc.CompilationRequest.deserialize(
                request1.serialize(), "<unknown>", cfg_ser)
            self.assertEqual(hash(request1), hash(request2))
            self.assertEqual(request1, request2)

            # schema_version affects the cache_key, hence the hash.
            # But, it's not serialized so the 2 requests are still equal.
            # This makes request2 a new key as being used in dicts.
            request2.set_schema_version(uuid.uuid4())
            self.assertNotEqual(hash(request1), hash(request2))
            self.assertEqual(request1, request2)

        test(edgeql.Source.from_string("SELECT 42"))
        test(edgeql.NormalizedSource.from_string("SELECT 42"))
