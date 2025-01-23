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
from typing import Any, Tuple, Mapping, NamedTuple, Callable

import asyncio
import http
import http.client
import json
import os.path
import pathlib
import platform
import random
import signal
import subprocess
import ssl
import sys
import tempfile
import textwrap
import time
import unittest
import urllib.error
import urllib.request
import uuid

import edgedb

import edb
from edb import buildmeta
from edb import protocol
from edb.common import devmode
from edb.protocol import protocol as edb_protocol  # type: ignore
from edb.server import args, pgcluster
from edb.server import cluster as edbcluster
from edb.testbase import server as tb


class TestServerApi(tb.ClusterTestCase):
    async def test_server_healthchecks(self):
        with self.http_con() as http_con:
            _, _, status = self.http_con_request(
                http_con,
                path='/server/status/alive',
            )

            self.assertEqual(status, http.HTTPStatus.OK)

        with self.http_con() as http_con:
            _, _, status = self.http_con_request(
                http_con,
                path='/server/status/ready',
            )

            self.assertEqual(status, http.HTTPStatus.OK)


class TestServerOps(tb.TestCaseWithHttpClient, tb.CLITestCaseMixin):

    async def kill_process(self, proc: asyncio.subprocess.Process):
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=20)
        except TimeoutError:
            proc.kill()

    async def test_server_ops_auto_shutdown_after_zero(self):
        # Test that "edgedb-server" works as expected with the
        # following arguments:
        #
        # * "--port=auto"
        # * "--temp-dir"
        # * "--auto-shutdown-after=0"
        # * "--emit-server-status"

        async with tb.start_edgedb_server(
            auto_shutdown_after=0,
        ) as sd:

            con1 = await sd.connect()
            self.assertEqual(await con1.query_single('SELECT 1'), 1)

            con2 = await sd.connect()
            self.assertEqual(await con2.query_single('SELECT 1'), 1)

            await con1.aclose()

            self.assertEqual(await con2.query_single('SELECT 42'), 42)
            await con2.aclose()

            with self.assertRaises(
                    (ConnectionError, edgedb.ClientConnectionError)):
                # Since both con1 and con2 are now disconnected and
                # the cluster was started with an "--auto-shutdown-after=0"
                # option, we expect this connection to be rejected
                # and the cluster to be shutdown soon.
                await sd.connect(wait_until_available=0)

    async def test_server_ops_auto_shutdown_after_one(self):
        async with tb.start_edgedb_server(
            auto_shutdown_after=1,
        ) as sd:
            await asyncio.sleep(2)

            with self.assertRaises(
                    (ConnectionError, edgedb.ClientConnectionError)):
                await sd.connect(wait_until_available=0)

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "--bootstrap-command is not supported in multi-tenant mode",
    )
    async def test_server_ops_bootstrap_script(self) -> None:
        # Test that "edgedb-server" works as expected with the
        # following arguments:
        #
        # * "--bootstrap-only"
        # * "--bootstrap-command"

        cmd = [
            sys.executable, '-m', 'edb.server.main',
            '--port', 'auto',
            '--testmode',
            '--temp-dir',
            '--bootstrap-command=CREATE SUPERUSER ROLE test_bootstrap;',
            '--bootstrap-only',
            '--log-level=error',
            '--max-backend-connections', '10',
            '--tls-cert-mode=generate_self_signed',
        ]

        # Note: for debug comment "stderr=subprocess.PIPE".
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=240)
        except asyncio.TimeoutError:
            if proc.returncode is None:
                proc.terminate()
            raise
        else:
            self.assertTrue(
                proc.returncode == 0,
                f'server exited with code {proc.returncode}:\n'
                f'STDERR: {stderr.decode()}',
            )

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "--bootstrap-command is not supported in multi-tenant mode",
    )
    async def test_server_ops_bootstrap_script_server(self):
        # Test that "edgedb-server" works as expected with the
        # following arguments:
        #
        # * "--bootstrap-command"

        async with tb.start_edgedb_server(
            bootstrap_command='CREATE SUPERUSER ROLE test_bootstrap2 '
                              '{ SET password := "tbs2" };'
        ) as sd:
            con = await sd.connect(user='test_bootstrap2', password='tbs2')
            try:
                self.assertEqual(await con.query_single('SELECT 1'), 1)
            finally:
                await con.aclose()

    @unittest.skipIf(
        platform.system() == "Darwin",
        "https://github.com/edgedb/edgedb/issues/7789"
    )
    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "--background is not supported in multi-tenante mode"
    )
    async def test_server_ops_background(self) -> None:
        # Test that "edgedb-server" works as expected with the
        # following arguments:
        #
        # * "--background"
        debug = False

        status_fd, status_file = tempfile.mkstemp()
        os.close(status_fd)

        cmd = [
            sys.executable, '-m', 'edb.server.main',
            '--port', 'auto',
            '--testmode',
            '--temp-dir',
            '--log-level=debug',
            '--background',
            '--emit-server-status', status_file,
            '--tls-cert-mode=generate_self_signed',
            '--jose-key-mode=generate',
        ]

        proc = None

        def _read(filename: str) -> str:
            with open(filename, 'r') as f:
                while True:
                    result = f.readline()
                    if not result:
                        time.sleep(0.1)
                    else:
                        return result

        async def _waiter() -> Tuple[str, Mapping[str, Any]]:
            loop = asyncio.get_running_loop()
            line = await loop.run_in_executor(None, _read, status_file)
            status, _, dataline = line.partition('=')
            return status, json.loads(dataline)

        pid = None

        try:
            if debug:
                print(*cmd)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=None if debug else subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )

            status, data = await asyncio.wait_for(_waiter(), timeout=240)
            self.assertEqual(status, 'READY')
            self.assertIsNotNone(data.get('socket_dir'))
            self.assertIsNotNone(data.get('port'))
            pid = data.get('main_pid')
            self.assertIsNotNone(pid)

        finally:
            if proc and proc.returncode is None:
                await self.kill_process(proc)
            os.unlink(status_file)

            if pid is not None:
                os.kill(pid, signal.SIGTERM)

    async def test_server_ops_emit_server_status_to_file(self) -> None:
        debug = False

        status_fd, status_file = tempfile.mkstemp()
        os.close(status_fd)

        status_fd, status_file_2 = tempfile.mkstemp()
        os.close(status_fd)

        cmd = [
            sys.executable, '-m', 'edb.server.main',
            '--port', 'auto',
            '--testmode',
            '--log-level=debug',
            '--emit-server-status', status_file,
            '--emit-server-status', status_file_2,
            '--tls-cert-mode=generate_self_signed',
            '--jose-key-mode=generate',
        ]
        if "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" not in os.environ:
            cmd.extend([
                '--temp-dir',
                '--max-backend-connections', '10',
            ])

        proc = None

        def _read(filename: str) -> str:
            with open(filename, 'r') as f:
                while True:
                    result = f.readline()
                    if not result:
                        time.sleep(0.1)
                    else:
                        return result

        async def _waiter() -> Tuple[str, Mapping[str, Any]]:
            loop = asyncio.get_running_loop()
            lines = await asyncio.gather(
                loop.run_in_executor(None, _read, status_file),
                loop.run_in_executor(None, _read, status_file_2),
            )
            self.assertEqual(len(lines), 2)
            self.assertEqual(lines[0], lines[1])
            status, _, dataline = lines[0].partition('=')
            return status, json.loads(dataline)

        try:
            if debug:
                print(*cmd)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=None if debug else subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )

            status, data = await asyncio.wait_for(_waiter(), timeout=240)
            self.assertEqual(status, 'READY')
            self.assertIsNotNone(data.get('socket_dir'))
            self.assertIsNotNone(data.get('port'))
            self.assertIsNotNone(data.get('main_pid'))

        finally:
            if proc:
                await self.kill_process(proc)
            os.unlink(status_file)

    async def test_server_ops_generates_cert_to_specified_file(self):
        cert_fd, cert_file = tempfile.mkstemp()
        os.close(cert_fd)
        os.unlink(cert_file)

        key_fd, key_file = tempfile.mkstemp()
        os.close(key_fd)
        os.unlink(key_file)

        try:
            async with tb.start_edgedb_server(
                tls_cert_file=cert_file,
                tls_key_file=key_file,
            ) as sd:
                con = await sd.connect()
                try:
                    await con.query_single("SELECT 1")
                finally:
                    await con.aclose()

            key_file_path = pathlib.Path(key_file)
            cert_file_path = pathlib.Path(cert_file)

            self.assertTrue(key_file_path.exists())
            self.assertTrue(cert_file_path.exists())

            self.assertGreater(key_file_path.stat().st_size, 0)
            self.assertGreater(cert_file_path.stat().st_size, 0)

            # Check that the server works with the generated cert/key
            async with tb.start_edgedb_server(
                tls_cert_file=cert_file,
                tls_key_file=key_file,
                tls_cert_mode=args.ServerTlsCertMode.RequireFile,
            ) as sd:
                con = await sd.connect()
                try:
                    await con.query_single("SELECT 1")
                finally:
                    await con.aclose()

        finally:
            os.unlink(key_file)
            os.unlink(cert_file)

    async def test_server_ops_generates_cert_to_default_location(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Trust,
            ) as sd:
                con = await sd.connect()
                try:
                    await con.query_single("SELECT 1")
                finally:
                    await con.aclose()

            # Check that the server works with the generated cert/key
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                tls_cert_mode=args.ServerTlsCertMode.RequireFile,
                default_auth_method=args.ServerAuthMethod.Trust,
            ) as sd:
                con = await sd.connect()
                try:
                    await con.query_single("SELECT 1")
                finally:
                    await con.aclose()

    async def test_server_only_bootstraps_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Scram,
                bootstrap_command='ALTER ROLE admin SET password := "first";'
            ) as sd:
                con = await sd.connect(password='first')
                try:
                    await con.query_single('SELECT 1')
                finally:
                    await con.aclose()

        # The bootstrap command should not be run on subsequent server starts.
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Scram,
                bootstrap_command='ALTER ROLE admin SET password := "second";'
            ) as sd:
                con = await sd.connect(password='first')
                try:
                    await con.query_single('SELECT 1')
                finally:
                    await con.aclose()

    async def test_server_alter_role_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Scram,
                bootstrap_command='ALTER ROLE edgedb SET password := "first";'
            ) as sd:
                con = await sd.connect(password='first')
                try:
                    await con.query_single('SELECT 1')
                finally:
                    await con.aclose()

    async def test_server_ops_bogus_bind_addr_in_mix(self):
        async with tb.start_edgedb_server(
            bind_addrs=('host.invalid', '127.0.0.1',),
        ) as sd:
            con = await sd.connect()
            try:
                await con.query_single("SELECT 1")
            finally:
                await con.aclose()

    async def test_server_ops_bogus_bind_addr_only(self):
        with self.assertRaisesRegex(
            edbcluster.ClusterError,
            "could not create any listen sockets",
        ):
            async with tb.start_edgedb_server(
                bind_addrs=('host.invalid',),
            ) as sd:
                con = await sd.connect()
                try:
                    await con.query_single("SELECT 1")
                finally:
                    await con.aclose()

    async def test_server_ops_set_pg_max_connections(self):
        actual = random.randint(50, 100)
        async with tb.start_edgedb_server(
            max_allowed_connections=actual,
        ) as sd:
            con = await sd.connect()
            try:
                max_connections = await con.query_single(
                    'SELECT cfg::InstanceConfig.__pg_max_connections LIMIT 1'
                )  # TODO: remove LIMIT 1 after #2402
                self.assertEqual(int(max_connections), actual + 2)
            finally:
                await con.aclose()

    async def test_server_ops_detect_postgres_pool_size(self):
        actual = random.randint(50, 100)

        async def test(pgdata_path):
            async with tb.start_edgedb_server(
                max_allowed_connections=None,
                backend_dsn=f'postgres:///?user=postgres&host={pgdata_path}',
                reset_auth=True,
                runstate_dir=None if devmode.is_in_dev_mode() else pgdata_path,
            ) as sd:
                con = await sd.connect()
                try:
                    max_connections = await con.query_single(
                        '''
                        SELECT cfg::InstanceConfig.__pg_max_connections
                        LIMIT 1
                        '''
                    )  # TODO: remove LIMIT 1 after #2402
                    self.assertEqual(int(max_connections), actual)
                finally:
                    await con.aclose()

        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(
                td, max_connections=actual, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())
            await cluster.start()
            try:
                await test(td)
            finally:
                await cluster.stop()

    async def test_server_ops_postgres_multitenant_env(self):
        async def test(pgdata_path, tenant):
            async with tb.start_edgedb_server(
                reset_auth=True,
                backend_dsn=f'postgres:///?user=postgres&host={pgdata_path}',
                runstate_dir=None if devmode.is_in_dev_mode() else pgdata_path,
                env={"EDGEDB_SERVER_TENANT_ID": tenant},
            ) as sd:
                con = await sd.connect()
                try:
                    await con.execute(f'CREATE DATABASE {tenant}')
                    await con.execute(f'CREATE SUPERUSER ROLE {tenant}')
                    databases = await con.query('SELECT sys::Branch.name')
                    self.assertEqual(set(databases), {'main', tenant})
                    roles = await con.query('SELECT sys::Role.name')
                    self.assertEqual(set(roles), {'admin', tenant})
                finally:
                    await con.aclose()

        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())

            await cluster.start()
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(test(td, 'tenant1'))
                    tg.create_task(test(td, 'tenant2'))
            finally:
                await cluster.stop()

    async def test_server_ops_postgres_multitenant(self):
        async def test(pgdata_path, tenant):
            async with tb.start_edgedb_server(
                tenant_id=tenant,
                reset_auth=True,
                backend_dsn=f'postgres:///?user=postgres&host={pgdata_path}',
                runstate_dir=None if devmode.is_in_dev_mode() else pgdata_path,
            ) as sd:
                con = await sd.connect()
                try:
                    await con.execute(f'CREATE DATABASE {tenant}')
                    await con.execute(f'CREATE SUPERUSER ROLE {tenant}')
                    databases = await con.query('SELECT sys::Branch.name')
                    self.assertEqual(set(databases), {'main', tenant})
                    roles = await con.query('SELECT sys::Role.name')
                    self.assertEqual(set(roles), {'admin', tenant})
                finally:
                    await con.aclose()

        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())

            await cluster.start()
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(test(td, 'tenant1'))
                    tg.create_task(test(td, 'tenant2'))
            finally:
                await cluster.stop()

    async def test_server_ops_postgres_recovery(self):
        async def test(pgdata_path):
            async with tb.start_edgedb_server(
                backend_dsn=f'postgres:///?user=postgres&host={pgdata_path}',
                reset_auth=True,
                runstate_dir=None if devmode.is_in_dev_mode() else pgdata_path,
            ) as sd:
                con = await sd.connect()
                try:
                    val = await con.query_single('SELECT 123')
                    self.assertEqual(int(val), 123)

                    # stop the postgres
                    await cluster.stop()
                    with self.assertRaisesRegex(
                        edgedb.BackendUnavailableError,
                        'Postgres is not available',
                    ):
                        await con.query_single('SELECT 123+456')

                    # bring postgres back
                    await cluster.start()

                    # give the EdgeDB server some time to recover
                    async for tr in self.try_until_succeeds(
                        ignore=edgedb.BackendUnavailableError,
                        timeout=30,
                    ):
                        async with tr:
                            val = await con.query_single('SELECT 123+456')
                    self.assertEqual(int(val), 579)
                finally:
                    await con.aclose()

        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())
            await cluster.start()
            try:
                await test(td)
            finally:
                await cluster.stop()

    async def _test_server_ops_ignore_other_tenants(self, td, user):
        async with tb.start_edgedb_server(
            backend_dsn=f'postgres:///?user={user}&host={td}',
            runstate_dir=None if devmode.is_in_dev_mode() else td,
            reset_auth=True,
        ) as sd:
            con = await sd.connect()
            await con.aclose()

        async with tb.start_edgedb_server(
            backend_dsn=f'postgres:///?user=postgres&host={td}',
            runstate_dir=None if devmode.is_in_dev_mode() else td,
            reset_auth=True,
            ignore_other_tenants=True,
            env={'EDGEDB_SERVER_TENANT_ID': 'asdf'},
        ) as sd:
            con = await sd.connect()
            await con.aclose()

    async def test_server_ops_ignore_other_tenants(self):
        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())

            await cluster.start()
            try:
                await self._test_server_ops_ignore_other_tenants(
                    td, 'postgres'
                )
            finally:
                await cluster.stop()

    async def test_server_ops_ignore_other_tenants_single_role(self):
        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())
            cluster.add_hba_entry(
                type="local",
                database="all",
                user="single",
                auth_method="trust",
            )
            await cluster.start()
            conn = await cluster.connect(source_description="test_server_ops")
            setup = b"""\
                CREATE ROLE single WITH LOGIN CREATEDB;
                CREATE DATABASE single;
                REVOKE ALL ON DATABASE single FROM PUBLIC;
                GRANT CONNECT ON DATABASE single TO single;
                GRANT ALL ON DATABASE single TO single;\
            """
            for sql in setup.split(b'\n'):
                await conn.sql_execute(sql)
            await conn.close()
            try:
                await self._test_server_ops_ignore_other_tenants(td, 'single')
            finally:
                await cluster.stop()

    async def _test_connection(self, con):
        await con.send(
            protocol.Execute(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                command_text='SELECT 1',
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=protocol.OutputFormat.NONE,
                expected_cardinality=protocol.Cardinality.MANY,
                input_typedesc_id=b'\0' * 16,
                output_typedesc_id=b'\0' * 16,
                state_typedesc_id=b'\0' * 16,
                arguments=b'',
                state_data=b'',
            ),
            protocol.Sync(),
        )
        await con.recv_match(
            protocol.CommandComplete,
            status='SELECT'
        )
        await con.recv_match(
            protocol.ReadyForCommand,
            transaction_state=protocol.TransactionState.NOT_IN_TRANSACTION,
        )

    async def test_server_ops_cache_recompile_01(self):
        def measure_compilations(
            sd: tb._EdgeDBServerData
        ) -> Callable[[], float | int]:
            return lambda: tb.parse_metrics(sd.fetch_metrics()).get(
                'edgedb_server_edgeql_query_compilations_total'
                '{tenant="localtest",path="compiler"}'
            ) or 0

        def measure_sql_compilations(
            sd: tb._EdgeDBServerData
        ) -> Callable[[], float | int]:
            return lambda: tb.parse_metrics(sd.fetch_metrics()).get(
                'edgedb_server_sql_compilations_total'
                '{tenant="localtest"}'
            ) or 0

        with tempfile.TemporaryDirectory() as temp_dir:
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Trust,
                net_worker_mode='disabled',
            ) as sd:
                con = await sd.connect()
                try:
                    qry = 'select schema::Object { name }'

                    await con.query(qry)

                    # Querying a second time should hit the cache
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query(qry)

                    await con.query('''
                        create type X
                    ''')

                    # We should have recompiled the cache when we created
                    # the type, so doing the query shouldn't cause another
                    # compile!
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query(qry)
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query(qry)

                    # TODO: this does not behave the way I thing it should
                    # with self.assertChange(measure_compilations(sd), 1):
                    #     con_c = con.with_config(apply_access_policies=False)
                    #     await con_c.query(qry)

                    # Set the compilation timeout to 2ms.
                    #
                    # This should prevent recompilation from
                    # succeeding. If we ever make the compiler fast
                    # enough, we might need to change this :)
                    #
                    # We do 2ms instead of 1ms or something even smaller
                    # because uvloop's timer has ms granularity, and
                    # setting it to 2ms should typically ensure that it
                    # manages to start the compilation.
                    await con.execute(
                        "configure current database "
                        "set auto_rebuild_query_cache_timeout := "
                        "<duration>'2ms'"
                    )

                    await con.query('''
                        drop type X
                    ''')
                    await con.query('''
                        create global g: str;
                    ''')

                    with self.assertChange(measure_compilations(sd), 1):
                        await con.query(qry)

                    await con.execute(
                        "configure current database "
                        "reset auto_rebuild_query_cache_timeout"
                    )

                    # Now, a similar thing for SQL queries

                    with self.assertChange(measure_compilations(sd), 1):
                        await con.query_sql('select 1')

                    # cache hit
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query_sql('select 1')

                    # changing globals: cache hit
                    with self.assertChange(measure_compilations(sd), 0):
                        con_g = con.with_globals({'g': 'hello'})
                        await con_g.query_sql('select 1')

                    # normalization: pg_query_normalize is underwhelming
                    with self.assertChange(measure_compilations(sd), 1):
                        await con.query_sql('sElEct  1')

                    # constant extraction: cache hit
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query_sql('select 2')

                    # TODO: this does not behave the way I though it should
                    # changing certain config options: compiler call
                    # with self.assertChange(measure_compilations(sd), 1):
                    #     con_c = con.with_config(apply_access_policies=False)
                    #     await con_c.query_sql(qry_sql)

                finally:
                    await con.aclose()

                has_asyncpg = True
                try:
                    import asyncpg  # noqa
                except ImportError:
                    has_asyncpg = False

                if has_asyncpg:
                    scon = await sd.connect_pg()
                    try:
                        with self.assertChange(measure_sql_compilations(sd), 1):
                            await scon.fetch('select 1')

                        with self.assertChange(measure_sql_compilations(sd), 1):
                            await scon.fetch('select 1 + 1')

                        # cache hit
                        with self.assertChange(measure_sql_compilations(sd), 0):
                            await scon.fetch('select 1')

                        # cache hit because of query normalization
                        with self.assertChange(measure_sql_compilations(sd), 0):
                            await scon.fetch('select 2')

                        # TODO: better normalization
                        with self.assertChange(measure_sql_compilations(sd), 1):
                            await scon.fetch('sELEcT  1')

                        # cache hit, even after global has been changed
                        await scon.execute('SET "global default::g" to 1')
                        with self.assertChange(measure_sql_compilations(sd), 0):
                            await scon.execute('select 1')

                        # compiler call, because config was changed
                        await scon.execute('SET apply_access_policies_pg to 1')

                        with self.assertChange(measure_sql_compilations(sd), 1):
                            await scon.execute('select 1')
                    finally:
                        await scon.close()

            # Now restart the server to test the cache persistence.
            async with tb.start_edgedb_server(
                data_dir=temp_dir,
                default_auth_method=args.ServerAuthMethod.Trust,
                net_worker_mode='disabled',
            ) as sd:
                con = await sd.connect()
                try:
                    # It should hit the cache no problem.
                    with self.assertChange(measure_compilations(sd), 0):
                        await con.query(qry)

                finally:
                    await con.aclose()

    async def test_server_ops_schema_metrics_01(self):
        def _extkey(extension: str) -> str:
            return (
                f'edgedb_server_extension_used_branch_count_current'
                f'{{tenant="localtest",extension="{extension}"}}'
            )

        def _featkey(feature: str) -> str:
            return (
                f'edgedb_server_feature_used_branch_count_current'
                f'{{tenant="localtest",feature="{feature}"}}'
            )

        async with tb.start_edgedb_server(
            default_auth_method=args.ServerAuthMethod.Trust,
            net_worker_mode='disabled',
            force_new=True,
        ) as sd:
            con = await sd.connect()
            con2 = None
            try:
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_extkey('graphql'), 0), 0)
                self.assertEqual(metrics.get(_extkey('pg_trgm'), 0), 0)
                self.assertEqual(metrics.get(_featkey('function'), 0), 0)

                await con.execute('create extension graphql')
                await con.execute('create extension pg_trgm')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_extkey('graphql'), 0), 1)
                self.assertEqual(metrics.get(_extkey('pg_trgm'), 0), 1)
                # The innards of extensions shouldn't be counted in
                # feature use metrics. (pg_trgm has functions.)
                self.assertEqual(metrics.get(_featkey('function'), 0), 0)

                await con.execute('drop extension graphql')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_extkey('graphql'), 0), 0)
                self.assertEqual(metrics.get(_extkey('pg_trgm'), 0), 1)

                self.assertEqual(metrics.get(_extkey('global'), 0), 0)

                await con.execute('create global foo -> str;')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_featkey('global'), 0), 1)

                # Make sure it works after a transaction
                await con.execute('start transaction;')
                await con.execute('drop global foo;')
                await con.execute('commit;')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_featkey('global'), 0), 0)

                # And inside a script
                await con.execute('create global foo -> str; select 1;')
                await con.execute('create function asdf() -> int64 using (0)')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_featkey('global'), 0), 1)
                self.assertEqual(metrics.get(_featkey('function'), 0), 1)

                # Check in a second branch
                await con.execute('create empty branch b2')
                con2 = await sd.connect(database='b2')
                await con2.execute('create function asdf() -> int64 using (0)')
                await con2.execute('create extension graphql')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_extkey('graphql'), 0), 1)
                self.assertEqual(metrics.get(_featkey('global'), 0), 1)
                self.assertEqual(metrics.get(_featkey('function'), 0), 2)

                await con2.aclose()
                con2 = None

                # Dropping the other branch clears them out
                await con.execute('drop branch b2')
                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_extkey('graphql'), 0), 0)
                self.assertEqual(metrics.get(_featkey('function'), 0), 1)

                # More detailed testing

                await con.execute('create type Foo;')

                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_featkey('index'), 0), 0)
                self.assertEqual(metrics.get(_featkey('constraint'), 0), 0)

                await con.execute('''
                    alter type Foo create constraint expression on (true)
                ''')

                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(metrics.get(_featkey('index'), 0), 0)
                self.assertEqual(metrics.get(_featkey('constraint'), 0), 1)
                self.assertEqual(metrics.get(_featkey('constraint_expr'), 0), 1)
                self.assertEqual(
                    metrics.get(_featkey('multiple_inheritance'), 0), 0
                )
                self.assertEqual(metrics.get(_featkey('scalar'), 0), 0)
                self.assertEqual(metrics.get(_featkey('enum'), 0), 0)
                self.assertEqual(metrics.get(_featkey('link_property'), 0), 0)

                await con.execute('''
                    create type Bar;
                    create type Baz extending Foo, Bar;
                    create scalar type EnumType02 extending enum<foo, bar>;

                    create type Lol { create multi link foo -> Bar {
                        create property x -> str;
                    } };
                ''')

                metrics = tb.parse_metrics(sd.fetch_metrics())
                self.assertEqual(
                    metrics.get(_featkey('multiple_inheritance'), 0), 1
                )
                self.assertEqual(metrics.get(_featkey('scalar'), 0), 1)
                self.assertEqual(metrics.get(_featkey('enum'), 0), 1)
                self.assertEqual(metrics.get(_featkey('link_property'), 0), 1)

            finally:
                await con.aclose()
                if con2:
                    await con2.aclose()

    async def test_server_ops_downgrade_to_cleartext(self):
        async with tb.start_edgedb_server(
            binary_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            con = await sd.connect_test_protocol(
                user='edgedb',
                tls_security='insecure',
            )
            try:
                await self._test_connection(con)
            finally:
                await con.aclose()

    async def test_server_ops_no_cleartext(self):
        async with tb.start_edgedb_server(
            binary_endpoint_security=args.ServerEndpointSecurityMode.Tls,
            http_endpoint_security=args.ServerEndpointSecurityMode.Tls,
        ) as sd:
            con = http.client.HTTPConnection(sd.host, sd.port)
            con.connect()
            try:
                con.request(
                    'GET',
                    f'http://{sd.host}:{sd.port}/blah404'
                )
                resp = con.getresponse()
                self.assertEqual(resp.status, 301)
                resp_headers = {k.lower(): v.lower()
                                for k, v in resp.getheaders()}
                self.assertIn('location', resp_headers)
                self.assertTrue(
                    resp_headers['location'].startswith('https://'))

                self.assertIn('strict-transport-security', resp_headers)
                # By default we enforce HTTPS via HSTS on all routes.
                self.assertEqual(
                    resp_headers['strict-transport-security'],
                    'max-age=31536000')
            finally:
                con.close()

            with self.assertRaisesRegex(
                edgedb.BinaryProtocolError, "TLS Required"
            ):
                await sd.connect(test_no_tls=True)

            con = await edb_protocol.new_connection(
                user='edgedb',
                password=sd.password,
                host=sd.host,
                port=sd.port,
                tls_ca_file=sd.tls_cert_file,
            )
            try:
                await con.connect()
                await self._test_connection(con)
            finally:
                await con.aclose()

    async def test_server_ops_cleartext_http_allowed(self):
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:

            con = http.client.HTTPConnection(sd.host, sd.port)
            con.connect()
            try:
                con.request(
                    'GET',
                    f'http://{sd.host}:{sd.port}/blah404'
                )
                resp = con.getresponse()
                self.assertEqual(resp.status, 404)
            finally:
                con.close()

            tls_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=sd.tls_cert_file,
            )
            tls_context.check_hostname = False
            con = http.client.HTTPSConnection(
                sd.host, sd.port, context=tls_context)
            con.connect()
            try:
                con.request(
                    'GET',
                    f'http://{sd.host}:{sd.port}/blah404'
                )
                resp = con.getresponse()
                self.assertEqual(resp.status, 404)
                resp_headers = {k.lower(): v.lower()
                                for k, v in resp.getheaders()}

                self.assertIn('strict-transport-security', resp_headers)
                # When --allow-insecure-http-clients is passed, we set
                # max-age to 0, to let browsers know that it's safe
                # for the user to open http://
                self.assertEqual(
                    resp_headers['strict-transport-security'], 'max-age=0')
            finally:
                con.close()

            # Connect to let it autoshutdown; also test that
            # --allow-insecure-http-clients doesn't break binary
            # connections.
            con = await sd.connect_test_protocol()
            try:
                await self._test_connection(con)
            finally:
                await con.aclose()

    async def test_server_ops_mtls_http_transports(self):
        certs = pathlib.Path(__file__).parent / 'certs'
        client_ca_cert_file = certs / 'client_ca.cert.pem'
        client_ssl_cert_file = certs / 'client.cert.pem'
        client_ssl_key_file = certs / 'client.key.pem'
        async with tb.start_edgedb_server(
            tls_client_ca_file=client_ca_cert_file,
            security=args.ServerSecurityMode.Strict,
        ) as sd:
            def test(url):
                # Connection without the client cert fails
                tls_context = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH,
                    cafile=sd.tls_cert_file,
                )
                error_code = None
                try:
                    resp = urllib.request.urlopen(url, context=tls_context)
                except urllib.error.HTTPError as e:
                    error_code = e.code
                self.assertEqual(error_code, 401)

                # But once we load the client it will work
                tls_context.load_cert_chain(
                    client_ssl_cert_file,
                    client_ssl_key_file,
                )
                resp = urllib.request.urlopen(url, context=tls_context)
                self.assertEqual(resp.status, 200)

            test(f'https://{sd.host}:{sd.port}/metrics')
            test(f'https://{sd.host}:{sd.port}/server/status/alive')

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "--readiness-state-file is not allowed in multi-tenant mode",
    )
    async def test_server_ops_readiness(self):
        rf_no, rf_name = tempfile.mkstemp(text=True)
        rf = open(rf_no, "wt")

        try:
            print("not_ready", file=rf, flush=True)

            async with tb.start_edgedb_server(
                readiness_state_file=rf_name,
            ) as sd:
                # Initially not ready, but accepts connections
                await sd.connect()

                # Readiness check returns 503
                with self.http_con(server=sd) as http_con:
                    _, _, status = self.http_con_request(
                        http_con,
                        path='/server/status/ready',
                    )

                    self.assertEqual(
                        status, http.HTTPStatus.SERVICE_UNAVAILABLE)

                # It is alive though.
                with self.http_con(server=sd) as http_con:
                    _, _, status = self.http_con_request(
                        http_con,
                        path='/server/status/alive',
                    )

                    self.assertEqual(status, http.HTTPStatus.OK)

                # Make ready explicitly
                rf.seek(0)
                print("default", file=rf, flush=True)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        with self.http_con(server=sd) as http_con:
                            _, _, status = self.http_con_request(
                                http_con,
                                path='/server/status/ready',
                            )

                            self.assertEqual(status, http.HTTPStatus.OK)

                        conn = await sd.connect()
                        await conn.aclose()

                # Make not ready
                rf.seek(0)
                print("not_ready", file=rf, flush=True)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        conn = await sd.connect()
                        await conn.aclose()

                # Readiness check returns 503 once more
                with self.http_con(server=sd) as http_con:
                    _, _, status = self.http_con_request(
                        http_con,
                        path='/server/status/ready',
                    )

                    self.assertEqual(
                        status, http.HTTPStatus.SERVICE_UNAVAILABLE)

                # Make ready by removing the file
                rf.close()
                os.unlink(rf_name)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        with self.http_con(server=sd) as http_con:
                            _, _, status = self.http_con_request(
                                http_con,
                                path='/server/status/ready',
                            )

                            self.assertEqual(status, http.HTTPStatus.OK)

                        conn = await sd.connect()
                        await conn.aclose()

                # Re-create the file, the server should pick it up
                rf = open(rf_name, "w")
                print("not_ready", file=rf, flush=True)
                await sd.connect()
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        with self.http_con(server=sd) as http_con:
                            _, _, status = self.http_con_request(
                                http_con,
                                path='/server/status/ready',
                            )
                            self.assertEqual(
                                status, http.HTTPStatus.SERVICE_UNAVAILABLE)
        finally:
            if os.path.exists(rf_name):
                rf.close()
                os.unlink(rf_name)

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "--readiness-state-file is not allowed in multi-tenant mode",
    )
    async def test_server_ops_readonly(self):
        rf_no, rf_name = tempfile.mkstemp(text=True)
        rf = open(rf_no, "wt")

        try:
            print("default", file=rf, flush=True)

            async with tb.start_edgedb_server(
                readiness_state_file=rf_name,
            ) as sd:
                conn = await sd.connect()
                await conn.execute("create type A")
                await conn.execute("insert A")
                await conn.execute(
                    "configure current database "
                    "set allow_user_specified_id := true"
                )

                # Make read-only explicitly
                rf.seek(0)
                print("read_only", file=rf, flush=True)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        with self.assertRaisesRegex(
                            edgedb.DisabledCapabilityError,
                            "the server is currently in read-only mode",
                        ):
                            await conn.execute("insert A")

                with self.assertRaisesRegex(
                    edgedb.DisabledCapabilityError,
                    "the server is currently in read-only mode",
                ):
                    await conn.execute("create type B")
                with self.assertRaisesRegex(
                    edgedb.DisabledCapabilityError,
                    "the server is currently in read-only mode",
                ):
                    await conn.execute(
                        "configure current database "
                        "set allow_user_specified_id := false"
                    )

                # Clear read-only by removing the file
                rf.close()
                os.unlink(rf_name)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.AccessError, AssertionError),
                ):
                    async with tr:
                        await conn.execute("insert A")

                await conn.aclose()
        finally:
            if os.path.exists(rf_name):
                rf.close()
                os.unlink(rf_name)

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "covered in test_server_ops_multi_tenant",
    )
    async def test_server_ops_offline(self):
        rf_no, rf_name = tempfile.mkstemp(text=True)
        rf = open(rf_no, "wt")

        try:
            print("default", file=rf, flush=True)

            async with tb.start_edgedb_server(
                readiness_state_file=rf_name,
            ) as sd:
                conn = await sd.connect()
                await conn.execute("select 1")

                # Go offline
                rf.seek(0)
                print("offline", file=rf, flush=True)
                await asyncio.sleep(0.01)

                with self.assertRaises(
                    (edgedb.AvailabilityError, edgedb.ClientConnectionError),
                ):
                    await conn.execute("select 1")

                # Clear read-only by removing the file
                rf.close()
                os.unlink(rf_name)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(edgedb.ClientConnectionError,),
                ):
                    async with tr:
                        await conn.execute("select 1")

                await conn.aclose()
        finally:
            if os.path.exists(rf_name):
                rf.close()
                os.unlink(rf_name)

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "covered in test_server_ops_multi_tenant",
    )
    async def test_server_ops_blocked(self):
        rf_no, rf_name = tempfile.mkstemp(text=True)
        rf = open(rf_no, "wt")

        try:
            print("default", file=rf, flush=True)

            async with tb.start_edgedb_server(
                readiness_state_file=rf_name,
            ) as sd:
                conn = await sd.connect()
                await conn.execute("select 1")

                # Go blocked
                rf.seek(0)
                print("blocked:quota exceeded", file=rf, flush=True)
                await asyncio.sleep(0.01)

                with self.assertRaisesRegex(
                    edgedb.AvailabilityError,
                    "quota exceeded"
                ):
                    await conn.execute("select 1")

                await asyncio.sleep(0.01)
                self.assertTrue(conn.is_closed())

                # Clear read-only by removing the file
                rf.close()
                os.unlink(rf_name)
                await asyncio.sleep(0.05)
                async for tr in self.try_until_succeeds(
                    ignore=(
                        edgedb.AvailabilityError,
                    ),
                ):
                    async with tr:
                        await conn.execute("select 1")

                await conn.aclose()
        finally:
            if os.path.exists(rf_name):
                rf.close()
                os.unlink(rf_name)

    async def test_server_ops_restore_with_schema_signal(self):
        async def test(pgdata_path):
            backend_dsn = f'postgres:///?user=postgres&host={pgdata_path}'
            runstate_dir = None if devmode.is_in_dev_mode() else pgdata_path
            async with tb.start_edgedb_server(
                max_allowed_connections=None,
                backend_dsn=backend_dsn,
                reset_auth=True,
                runstate_dir=runstate_dir,
            ) as sd1:
                async with tb.start_edgedb_server(
                    max_allowed_connections=None,
                    backend_dsn=backend_dsn,
                    runstate_dir=runstate_dir,
                ) as sd2:
                    await self._test_server_ops_restore_with_schema_signal(
                        sd1, sd2
                    )

        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(
                td, max_connections=20, log_level='s'
            )
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())
            await cluster.start()
            try:
                await test(td)
            finally:
                await cluster.stop()

    async def _test_server_ops_restore_with_schema_signal(self, sd1, sd2):
        con = await sd1.connect()
        try:
            await con.execute("CREATE DATABASE restore_signal;")
            await con.execute("CREATE TYPE RestoreSignal;")
            await con.execute("INSERT RestoreSignal;")
        finally:
            await con.aclose()

        # Connect to the adjacent server first with the empty schema
        conn_args = sd1.get_connect_args()
        con = await sd2.connect(
            database="restore_signal",
            password=conn_args["password"],
        )

        try:
            # Dump and restore should trigger a re-introspection in sd2
            with tempfile.TemporaryDirectory() as f:
                fname = os.path.join(f, 'dump')
                await asyncio.to_thread(
                    self.run_cli_on_connection, conn_args, "dump", fname
                )
                await asyncio.to_thread(
                    self.run_cli_on_connection,
                    conn_args,
                    "-d",
                    "restore_signal",
                    "restore",
                    fname
                )

            # The re-introspection has a delay, but should eventually happen
            async for tr in self.try_until_succeeds(
                ignore=edgedb.InvalidReferenceError,
                timeout=30,
            ):
                async with tr:
                    rsids = await con.query("SELECT RestoreSignal;")
            self.assertTrue(rsids)
        finally:
            await con.aclose()

    async def _init_pg_cluster(self, path):
        cluster = await pgcluster.get_local_pg_cluster(path, log_level='s')
        cluster.update_connection_params(
            user='postgres',
            database='template1',
        )
        self.assertTrue(await cluster.ensure_initialized())
        await cluster.start()
        try:
            runstate_dir = None if devmode.is_in_dev_mode() else path
            async with tb.start_edgedb_server(
                runstate_dir=runstate_dir,
                backend_dsn=f'postgres:///?user=postgres&host={path}',
                reset_auth=True,
                auto_shutdown_after=1,
            ) as sd:
                connect_args = {
                    k: v
                    for k, v in sd.get_connect_args().items()
                    if k in {"user", "password"}
                }
        except Exception:
            await cluster.stop()
            raise
        return cluster, connect_args

    async def test_server_ops_multi_tenant(self):
        with (
            tempfile.TemporaryDirectory() as td1,
            tempfile.TemporaryDirectory() as td2,
            tempfile.NamedTemporaryFile("w+") as conf_file,
            tempfile.NamedTemporaryFile("w+") as rd1,
            tempfile.NamedTemporaryFile("w+") as rd2,
            tempfile.NamedTemporaryFile("w+") as cf1,
            tempfile.NamedTemporaryFile("w+") as cf2,
        ):
            fs = []
            conf = {}
            for i, td, rd, cf in [(1, td1, rd1, cf1), (2, td2, rd2, cf2)]:
                rd.file.write("default:ok")
                rd.file.flush()
                cf.write(textwrap.dedent(f"""
                    [[magic_smtp_config]]
                    _tname = "cfg::SMTPProviderConfig"
                    name = "provider:{i}"
                    sender = "sender@host{i}.com"
                """))
                cf.flush()
                fs.append(self.loop.create_task(self._init_pg_cluster(td)))
                conf[f"{i}.localhost"] = {
                    "instance-name": f"localtest{i}",
                    "backend-dsn": f'postgres:///?user=postgres&host={td}',
                    "max-backend-connections": 10,
                    "readiness-state-file": rd.name,
                    "config-file": cf.name,
                }
            await asyncio.wait(fs)
            cluster1, args1 = await fs[0]
            cluster2, args2 = await fs[1]
            args1["server_hostname"] = "1.localhost"
            args2["server_hostname"] = "2.localhost"
            try:
                json.dump(conf, conf_file.file)
                conf_file.file.flush()

                runstate_dir = None if devmode.is_in_dev_mode() else td1
                srv = tb.start_edgedb_server(
                    runstate_dir=runstate_dir,
                    multitenant_config=conf_file.name,
                    max_allowed_connections=None,
                    http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
                )
                async with srv as sd:
                    mtargs = MultiTenantArgs(
                        srv,
                        sd,
                        conf_file,
                        conf,
                        args1,
                        args2,
                        rd1,
                        rd2,
                        cf1,
                        cf2
                    )
                    test_prefix = '_test_server_ops_multi_tenant_'
                    tests = [s for s in dir(self) if s.startswith(test_prefix)]
                    for name in tests:
                        i = name.replace(test_prefix, '')
                        with self.subTest(name, i=i):
                            await getattr(self, name)(mtargs)
            finally:
                try:
                    await cluster1.stop()
                finally:
                    await cluster2.stop()

    async def _test_server_ops_multi_tenant_1(
        self, mtargs: MultiTenantArgs, **kwargs
    ):
        conn = await mtargs.sd.connect(**mtargs.args1, **kwargs)
        try:
            rv = await conn.query_single("select sys::get_instance_name()")
            self.assertEqual(rv, "localtest1")
        finally:
            await conn.aclose()

    async def _test_server_ops_multi_tenant_2(self, mtargs: MultiTenantArgs):
        conn = await mtargs.sd.connect(**mtargs.args2)
        try:
            rv = await conn.query_single("select sys::get_instance_name()")
            self.assertEqual(rv, "localtest2")
        finally:
            await conn.aclose()

    async def _test_server_ops_multi_tenant_3(self, mtargs: MultiTenantArgs):
        data = mtargs.sd.fetch_metrics()
        self.assertIn(
            '\nedgedb_server_mt_tenants_current 2.0\n', data
        )
        self.assertIn(
            '\nedgedb_server_mt_tenant_add_total'
            '{tenant="localtest1"} 1.0\n',
            data,
        )
        self.assertNotIn(
            '\nedgedb_server_mt_tenant_remove_total'
            '{tenant="localtest1"} 1.0\n',
            data,
        )

        conf1 = mtargs.conf.pop("1.localhost")
        mtargs.reload_server()

        async for tr in self.try_until_fails(
            wait_for=edgedb.AvailabilityError
        ):
            async with tr:
                await self._test_server_ops_multi_tenant_1(mtargs)

        await self._test_server_ops_multi_tenant_2(mtargs)

        data = mtargs.sd.fetch_metrics()
        self.assertIn(
            '\nedgedb_server_mt_tenants_current 1.0\n',
            data,
        )
        self.assertIn(
            '\nedgedb_server_mt_tenant_add_total'
            '{tenant="localtest1"} 1.0\n',
            data,
        )
        self.assertIn(
            '\nedgedb_server_mt_tenant_remove_total'
            '{tenant="localtest1"} 1.0\n',
            data,
        )

        mtargs.conf["1.localhost"] = conf1
        mtargs.reload_server()

        async for tr in self.try_until_succeeds(
            ignore=edgedb.AvailabilityError
        ):
            async with tr:
                await self._test_server_ops_multi_tenant_1(mtargs)

        await self._test_server_ops_multi_tenant_2(mtargs)

        data = mtargs.sd.fetch_metrics()
        self.assertIn(
            '\nedgedb_server_mt_tenants_current 2.0\n',
            data,
        )
        self.assertIn(
            '\nedgedb_server_mt_tenant_add_total'
            '{tenant="localtest1"} 2.0\n',
            data,
        )
        self.assertIn(
            '\nedgedb_server_mt_tenant_remove_total'
            '{tenant="localtest1"} 1.0\n',
            data,
        )

    async def _test_server_ops_multi_tenant_4(self, mtargs: MultiTenantArgs):
        mtargs.rd1.file.seek(0)
        mtargs.rd1.file.truncate(0)
        mtargs.rd1.file.write("offline:test")
        mtargs.rd1.file.flush()

        async for tr in self.try_until_fails(
            wait_for=edgedb.ClientConnectionClosedError
        ):
            async with tr:
                await self._test_server_ops_multi_tenant_1(
                    mtargs,
                    timeout=1,
                    wait_until_available=0,
                )

        await self._test_server_ops_multi_tenant_2(mtargs)

        mtargs.rd1.file.seek(0)
        mtargs.rd1.file.truncate(0)
        mtargs.rd1.file.write("default:ok")
        mtargs.rd1.file.flush()

        await self._test_server_ops_multi_tenant_1(mtargs)
        await self._test_server_ops_multi_tenant_2(mtargs)

    async def _test_server_ops_multi_tenant_5(self, mtargs: MultiTenantArgs):
        mtargs.rd1.file.seek(0)
        mtargs.rd1.file.truncate(0)
        mtargs.rd1.file.write("blocked:test")
        mtargs.rd1.file.flush()

        async for tr in self.try_until_fails(
            wait_for=edgedb.AvailabilityError
        ):
            async with tr:
                await self._test_server_ops_multi_tenant_1(
                    mtargs,
                    timeout=1,
                    wait_until_available=0,
                )

        await self._test_server_ops_multi_tenant_2(mtargs)

        mtargs.rd1.file.seek(0)
        mtargs.rd1.file.truncate(0)
        mtargs.rd1.file.write("default:ok")
        mtargs.rd1.file.flush()

        await self._test_server_ops_multi_tenant_1(mtargs)
        await self._test_server_ops_multi_tenant_2(mtargs)

    async def _test_server_ops_global_compile_cache(
        self, mtargs: MultiTenantArgs, ddl, i, **kwargs
    ):
        conn = await mtargs.sd.connect(**kwargs)
        try:
            await conn.execute(ddl)
            await conn.execute('create extension pgcrypto')
            await conn.execute('create extension auth')
            await conn.execute(f'''
                configure current database set
                ext::auth::AuthConfig::auth_signing_key := '{"a" * 32}';

                configure current database
                insert ext::auth::EmailPasswordProviderConfig {{
                    require_verification := false,
                }};

                configure current database set
                current_email_provider_name := 'provider:{i}';
            ''')
        finally:
            await conn.aclose()

        with self.http_con(
            mtargs.sd, server_hostname=kwargs['server_hostname']
        ) as http_con:
            async for tr in self.try_until_succeeds(ignore=AssertionError):
                async with tr:
                    _response, _, status = self.http_con_json_request(
                        http_con,
                        path=f"/db/{conn.dbname}/ext/auth/register",
                        body={
                            "provider": "builtin::local_emailpassword",
                            "challenge": str(uuid.uuid4()),
                            "email": "cache@example.com",
                            "password": "secret",
                        },
                    )
                    self.assertEqual(status, 201)

    async def _test_server_ops_multi_tenant_6(self, mtargs: MultiTenantArgs):
        # The 2 tenants has different user schema, make sure the auth queries
        # work fine: the first run caches the queries and the second uses them
        await self._test_server_ops_global_compile_cache(
            mtargs,
            "create type GlobalCache1 { create property name: str }",
            1,
            **mtargs.args1,
        )
        await self._test_server_ops_global_compile_cache(
            mtargs,
            "create type GlobalCache2 { create property active: bool }",
            2,
            **mtargs.args2,
        )

    async def _test_server_ops_multi_tenant_7(self, mtargs: MultiTenantArgs):
        self.assertEqual(
            (await mtargs.current_email_provider(1))["sender"],
            "sender@host1.com",
        )
        self.assertEqual(
            (await mtargs.current_email_provider(2))["sender"],
            "sender@host2.com",
        )

        mtargs.cf1.seek(0)
        mtargs.cf1.truncate(0)
        mtargs.cf1.write(textwrap.dedent("""
            [[magic_smtp_config]]
            _tname = "cfg::SMTPProviderConfig"
            name = "provider:1"
            sender = "updated@example.com"
        """))
        mtargs.cf1.flush()
        assert mtargs.srv.proc is not None
        mtargs.srv.proc.send_signal(signal.SIGHUP)

        async for tr in self.try_until_succeeds(ignore=AssertionError):
            async with tr:
                self.assertEqual(
                    (await mtargs.current_email_provider(1))["sender"],
                    "updated@example.com",
                )
                self.assertEqual(
                    (await mtargs.current_email_provider(2))["sender"],
                    "sender@host2.com",
                )

    async def _test_server_ops_multi_tenant_8(self, mtargs: MultiTenantArgs):
        # Start with 2 tenants
        data = mtargs.sd.fetch_metrics()
        self.assertIn(
            '\nedgedb_server_mt_tenants_current 2.0\n',
            data,
        )
        await self._test_server_ops_multi_tenant_1(mtargs)

        with tempfile.TemporaryDirectory() as td:
            # Test adding tenant with a non-existing jwt-sub-allowlist-file
            tf = tempfile.mktemp(dir=td)
            conf = mtargs.conf["1.localhost"].copy()
            conf.update({
                "instance-name": "localtest3",
                "jwt-sub-allowlist-file": tf,
            })
            mtargs.conf["3.localhost"] = conf
            mtargs.reload_server()

            # The tenant should not be ready at this moment, while the server
            # keeps retrying to add the tenant
            args3 = mtargs.args1.copy()
            args3["server_hostname"] = "3.localhost"
            with self.assertRaises(edgedb.AvailabilityError):
                async for tr in self.try_until_succeeds(
                    ignore=edgedb.AvailabilityError, timeout=3
                ):
                    async with tr:
                        conn = await mtargs.sd.connect(**args3)
                        await conn.aclose()

            # Though, the metrics should reflect the ongoing attempt
            data = mtargs.sd.fetch_metrics()
            self.assertIn(
                '\nedgedb_server_mt_tenant_add_total'
                '{tenant="localtest3"} 1.0\n',
                data,
            )
            self.assertIn(
                '\nedgedb_server_mt_tenants_current 2.0\n',
                data,
            )

            # Now, create the missing file and the tenant should be added
            with open(tf, "w") as f:
                f.write("\n")
            async for tr in self.try_until_succeeds(
                ignore=edgedb.AvailabilityError
            ):
                async with tr:
                    conn = await mtargs.sd.connect(**args3)
                    await conn.aclose()
            data = mtargs.sd.fetch_metrics()
            self.assertIn(
                '\nedgedb_server_mt_tenant_add_total'
                '{tenant="localtest3"} 1.0\n',
                data,
            )
            self.assertIn(
                '\nedgedb_server_mt_tenants_current 3.0\n',
                data,
            )


class MultiTenantArgs(NamedTuple):
    srv: tb._EdgeDBServer
    sd: tb._EdgeDBServerData
    conf_file: tempfile._TemporaryFileWrapper
    conf: dict[str, dict[str, Any]]
    args1: dict[str, str]
    args2: dict[str, str]
    rd1: tempfile._TemporaryFileWrapper
    rd2: tempfile._TemporaryFileWrapper
    cf1: tempfile._TemporaryFileWrapper
    cf2: tempfile._TemporaryFileWrapper
    dbnames: list[str | None] = [None, None]

    def reload_server(self):
        self.conf_file.file.seek(0)
        self.conf_file.file.truncate(0)
        json.dump(self.conf, self.conf_file.file)
        self.conf_file.file.flush()
        self.srv.proc.send_signal(signal.SIGHUP)

    def fetch_server_info(self, i):
        return self.sd.fetch_server_info()["tenants"][f"{i}.localhost"]

    async def current_email_provider(self, i):
        tenant_info = self.fetch_server_info(i)
        dbname = self.dbnames[i - 1]
        if dbname is None:
            conn = await self.sd.connect(**getattr(self, f"args{i}"))
            try:
                dbname = await conn.query_single("""\
                    select sys::get_current_branch()
                """)
                self.dbnames[i - 1] = dbname
            finally:
                await conn.aclose()
        return tenant_info["databases"][dbname]["current_email_provider"]


class TestPGExtensions(tb.TestCase):
    async def test_edb_stat_statements(self):
        ext_home = (
            pathlib.Path(edb.__file__).parent.parent / 'edb_stat_statements'
        ).resolve()
        if not ext_home.exists():
            raise unittest.SkipTest("no source of edb_stat_statements")
        with tempfile.TemporaryDirectory() as td:
            cluster = await pgcluster.get_local_pg_cluster(td, log_level='s')
            cluster.update_connection_params(
                user='postgres',
                database='template1',
            )
            self.assertTrue(await cluster.ensure_initialized())
            await cluster.start(server_settings={
                'edb_stat_statements.track_planning': 'false',
                'edb_stat_statements.track': 'dev',
                'max_prepared_transactions': '5',
            })
            try:
                pg_config = buildmeta.get_pg_config_path()
                env = os.environ.copy()
                params = cluster.get_pgaddr()
                env['PGHOST'] = params.host
                env['PGPORT'] = params.port
                env['PGUSER'] = params.user
                env['PGDATABASE'] = params.database
                subprocess.check_output([
                    'make',
                    f'PG_CONFIG={pg_config}',
                    'installcheck',
                ], cwd=str(ext_home), env=env, text=True)
            except subprocess.CalledProcessError as e:
                output = ext_home / "regression.out"
                if output.exists():
                    regression_out = output.read_text()
                else:
                    regression_out = ""
                raise AssertionError(f"{e}:\n{regression_out}") from e
            finally:
                await cluster.stop()
