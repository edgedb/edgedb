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

import asyncio
import os.path
import pathlib
import random
import subprocess
import sys
import tempfile

import asyncpg
import edgedb

from edb.common import devmode
from edb.server import pgcluster, pgconnparams, cluster as edgedb_cluster
from edb.testbase import server as tb


class TestServerOps(tb.TestCase):

    async def test_server_ops_temp_dir(self):
        # Test that "edgedb-server" works as expected with the
        # following arguments:
        #
        # * "--port=auto"
        # * "--temp-dir"
        # * "--auto-shutdown"
        # * "--echo-runtime-info"

        bootstrap_command = (
            r'CONFIGURE SYSTEM INSERT Auth '
            r'{ priority := 0, method := (INSERT Trust) }'
        )

        async with tb.start_edgedb_server(
                bootstrap_command=bootstrap_command,
                auto_shutdown=True) as sd:

            self.assertTrue(os.path.exists(sd.host))

            con1 = await edgedb.async_connect(
                user='edgedb', host=sd.host, port=sd.port)
            self.assertEqual(await con1.query_one('SELECT 1'), 1)

            con2 = await edgedb.async_connect(
                user='edgedb', host=sd.host, port=sd.port)
            self.assertEqual(await con2.query_one('SELECT 1'), 1)

            await con1.aclose()

            self.assertEqual(await con2.query_one('SELECT 42'), 42)
            await con2.aclose()

            with self.assertRaises(
                    (ConnectionError, edgedb.ClientConnectionError)):
                # Since both con1 and con2 are now disconnected and
                # the cluster was started with an "--auto-shutdown"
                # option, we expect this connection to be rejected
                # and the cluster to be shutdown soon.
                await edgedb.async_connect(
                    user='edgedb',
                    host=sd.host,
                    port=sd.port,
                    wait_until_available=0,
                )

    async def test_server_ops_bootstrap_script(self):
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
        ]

        # Note: for debug comment "stderr=subprocess.PIPE".
        proc: asyncio.Process = await asyncio.create_subprocess_exec(
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

            if stderr != b'':
                self.fail(
                    'Unexpected server error output:\n' + stderr.decode()
                )

    async def test_server_ops_set_pg_max_connections(self):
        bootstrap_command = (
            r'CONFIGURE SYSTEM INSERT Auth '
            r'{ priority := 0, method := (INSERT Trust) }'
        )
        actual = random.randint(50, 100)
        async with tb.start_edgedb_server(
            auto_shutdown=True,
            bootstrap_command=bootstrap_command,
            max_allowed_connections=actual,
        ) as sd:
            run_dir = sd.server_data['runstate_dir']
            port = 0
            for sock in pathlib.Path(run_dir).glob('.s.PGSQL.*'):
                if sock.is_socket():
                    port = int(sock.suffix[1:])
                    break

            con = await edgedb.async_connect(
                user='edgedb', host=sd.host, port=sd.port)
            self.assertEqual(await con.query_one('SELECT 1'), 1)

            try:

                conn = await asyncpg.connect(
                    host=run_dir, port=port, user='postgres')
                try:
                    max_connectiosn = await conn.fetchval(
                        "SELECT setting FROM pg_settings "
                        "WHERE name = 'max_connections'")
                    self.assertEqual(int(max_connectiosn), actual)
                finally:
                    await conn.close()

            finally:
                await con.aclose()

    def test_server_ops_detect_postgres_pool_size(self):
        port = edgedb_cluster.find_available_port()
        actual = random.randint(50, 100)

        async def test(pgdata_path):
            bootstrap_command = (
                r'CONFIGURE SYSTEM INSERT Auth '
                r'{ priority := 0, method := (INSERT Trust) }'
            )
            async with tb.start_edgedb_server(
                auto_shutdown=True,
                bootstrap_command=bootstrap_command,
                max_allowed_connections=None,
                postgres_dsn=f'postgres:///?user=postgres&port={port}&'
                             f'host={pgdata_path}',
                runstate_dir=None if devmode.is_in_dev_mode() else pgdata_path,
            ) as sd:
                con = await edgedb.async_connect(
                    user='edgedb', host=sd.host, port=sd.port)
                try:
                    max_connections = await con.query_one(
                        'SELECT cfg::SystemConfig.__pg_max_connections '
                        'LIMIT 1')  # TODO: remove LIMIT 1 after #2402
                    self.assertEqual(int(max_connections), actual)
                finally:
                    await con.aclose()

        with tempfile.TemporaryDirectory() as td:
            cluster = pgcluster.get_local_pg_cluster(td,
                                                     max_connections=actual)
            cluster.set_connection_params(
                pgconnparams.ConnectionParameters(
                    user='postgres',
                    database='template1',
                ),
            )
            self.assertTrue(cluster.ensure_initialized())
            cluster.start(port=port)
            try:
                self.loop.run_until_complete(test(td))
            finally:
                cluster.stop()
