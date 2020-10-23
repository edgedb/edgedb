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
import json
import os.path
import subprocess
import sys

import edgedb

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

        async def read_runtime_info(stdout: asyncio.StreamReader):
            while True:
                line = await stdout.readline()
                if not line:
                    raise RuntimeError("EdgeDB server terminated")
                if line.startswith(b'EDGEDB_SERVER_DATA:'):
                    break

            dataline = line.decode().split('EDGEDB_SERVER_DATA:', 1)[1]
            data = json.loads(dataline)
            return data

        bootstrap_command = (
            r'CONFIGURE SYSTEM INSERT Auth '
            r'{ priority := 0, method := (INSERT Trust) }'
        )

        cmd = [
            sys.executable, '-m', 'edb.server.main',
            '--port', 'auto',
            '--testmode',
            '--temp-dir',
            '--auto-shutdown',
            '--echo-runtime-info',
            f'--bootstrap-command={bootstrap_command}',
        ]

        # Note: for debug comment "stderr=subprocess.PIPE".
        proc: asyncio.Process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

        try:
            data = await asyncio.wait_for(
                read_runtime_info(proc.stdout),
                timeout=240,
            )

            runstate_dir = data['runstate_dir']
            port = data['port']

            self.assertTrue(os.path.exists(runstate_dir))

            con1 = await edgedb.async_connect(
                user='edgedb', host=runstate_dir, port=port)
            self.assertEqual(await con1.query_one('SELECT 1'), 1)

            con2 = await edgedb.async_connect(
                user='edgedb', host=runstate_dir, port=port)
            self.assertEqual(await con2.query_one('SELECT 1'), 1)

            await con1.aclose()

            self.assertEqual(await con2.query_one('SELECT 42'), 42)
            await con2.aclose()

            with self.assertRaises(ConnectionError):
                # Since both con1 and con2 are now disconnected and
                # the cluster was started with an "--auto-shutdown"
                # option, we expect this connection to be rejected
                # and the cluster to be shutdown soon.
                await edgedb.async_connect(
                    user='edgedb', host=runstate_dir, port=port)

            i = 600 * 5  # Give it up to 5 minutes to cleanup.
            while i > 0:
                if not os.path.exists(runstate_dir):
                    break
                else:
                    i -= 1
                    await asyncio.sleep(0.1)
            else:
                self.fail('temp directory was not cleaned up')

        finally:
            if proc.returncode is None:
                proc.terminate()
                await proc.wait()

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
            '--bootstrap-command=SELECT 1',
            '--bootstrap-only',
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
