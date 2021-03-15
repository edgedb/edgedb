#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
import errno
import os
import random
import socket
import subprocess
import sys
import tempfile
import time

import edgedb

from edb.common import devmode
from edb.edgeql import quote

from edb.server import buildmeta
from edb.server import defines as edgedb_defines

from . import pgcluster


def find_available_port(port_range=(49152, 65535), max_tries=1000):
    low, high = port_range

    port = low
    try_no = 0

    while try_no < max_tries:
        try_no += 1
        port = random.randint(low, high)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                continue
        finally:
            sock.close()

        break
    else:
        port = None

    return port


class ClusterError(Exception):
    pass


class BaseCluster:
    def __init__(self, runstate_dir, *, port=edgedb_defines.EDGEDB_PORT,
                 env=None, testmode=False, log_level=None):
        self._edgedb_cmd = [sys.executable, '-m', 'edb.server.main']

        if log_level:
            self._edgedb_cmd.extend(['--log-level', log_level])

        if devmode.is_in_dev_mode():
            self._edgedb_cmd.append('--devmode')

        if testmode:
            self._edgedb_cmd.append('--testmode')

        self._runstate_dir = runstate_dir
        self._edgedb_cmd.extend(['--runstate-dir', runstate_dir])
        self._pg_cluster = self._get_pg_cluster()
        self._pg_connect_args = {}
        self._daemon_process = None
        self._port = port
        self._effective_port = None
        self._env = env

    def _get_pg_cluster(self):
        raise NotImplementedError()

    def get_status(self):
        pg_status = self._pg_cluster.get_status()
        initially_stopped = pg_status == 'stopped'

        if initially_stopped:
            self._pg_cluster.start(port=find_available_port())
        elif pg_status == 'not-initialized':
            return 'not-initialized'

        conn = None
        loop = asyncio.new_event_loop()
        try:
            conn = loop.run_until_complete(
                self._pg_cluster.connect(
                    database='template1', timeout=5, **self._pg_connect_args))

            db_exists = loop.run_until_complete(
                self._edgedb_template_exists(conn))
        finally:
            if conn is not None:
                conn.terminate()
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            if initially_stopped:
                self._pg_cluster.stop()

        if initially_stopped:
            return 'stopped' if db_exists else 'not-initialized,stopped'
        else:
            return 'running' if db_exists else 'not-initialized,running'

    def get_connect_args(self):
        return {
            'host': 'localhost',
            'port': self._effective_port
        }

    async def async_connect(self, **kwargs):
        connect_args = self.get_connect_args().copy()
        connect_args.update(kwargs)

        return await edgedb.async_connect(**connect_args)

    def connect(self, **kwargs):
        connect_args = self.get_connect_args().copy()
        connect_args.update(kwargs)

        return edgedb.connect(**connect_args)

    def init(self, *, server_settings=None):
        cluster_status = self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError('cluster has already been initialized')

        self._init()

    def start(self, wait=60, **settings):
        port = settings.pop('port', None) or self._port
        if port == 'dynamic':
            port = find_available_port()

        self._effective_port = port

        extra_args = ['--{}={}'.format(k.replace('_', '-'), v)
                      for k, v in settings.items()]
        extra_args.append('--port={}'.format(self._effective_port))

        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        self._daemon_process = subprocess.Popen(
            self._edgedb_cmd + extra_args,
            stdout=sys.stdout, stderr=sys.stderr,
            env=env)

        self._test_connection()

    def stop(self, wait=60):
        if (self._daemon_process is not None and
                self._daemon_process.returncode is None):
            self._daemon_process.terminate()
            self._daemon_process.wait(wait)

    def destroy(self):
        self._pg_cluster.destroy()

    def _init(self):
        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        init = subprocess.run(
            self._edgedb_cmd + ['--bootstrap-only'],
            stdout=sys.stdout, stderr=sys.stderr,
            env=env)

        if init.returncode != 0:
            raise ClusterError(
                f'edgedb-server --bootstrap-only failed with '
                f'exit code {init.returncode}')

    async def _edgedb_template_exists(self, conn):
        st = await conn.prepare(
            '''
            SELECT True FROM pg_catalog.pg_database WHERE datname = $1
        ''')

        return await st.fetchval(edgedb_defines.EDGEDB_TEMPLATE_DB)

    def _test_connection(self, timeout=30):
        async def test(timeout):
            left = timeout
            while True:
                started = time.monotonic()
                try:
                    conn = await edgedb.async_connect(
                        host=str(self._runstate_dir),
                        port=self._effective_port,
                        database=edgedb_defines.EDGEDB_SUPERUSER_DB,
                        user=edgedb_defines.EDGEDB_SUPERUSER,
                        timeout=left,
                    )
                except (OSError, asyncio.TimeoutError,
                        edgedb.ClientConnectionError):
                    left -= (time.monotonic() - started)
                    if left > 0.05:
                        await asyncio.sleep(0.05)
                        left -= 0.05
                        continue
                    raise ClusterError(
                        f'could not connect to edgedb-server '
                        f'within {timeout} seconds')
                except edgedb.AuthenticationError:
                    return
                else:
                    await conn.aclose()
                    return

        asyncio.run(test(timeout))

    def _admin_query(self, query):
        conn_args = self.get_connect_args().copy()
        conn_args['host'] = str(self._runstate_dir)
        conn_args['admin'] = True
        conn_args['user'] = edgedb_defines.EDGEDB_SUPERUSER
        conn_args['database'] = edgedb_defines.EDGEDB_SUPERUSER_DB
        conn = self.connect(**conn_args)

        try:
            return conn.query(query)
        finally:
            conn.close()

    def set_superuser_password(self, password):
        self._admin_query(f'''
            ALTER ROLE {edgedb_defines.EDGEDB_SUPERUSER}
            SET password := {quote.quote_literal(password)}
        ''')

    def trust_local_connections(self):
        self._admin_query('''
            CONFIGURE SYSTEM INSERT Auth {
                priority := 0,
                method := (INSERT Trust),
            }
        ''')


class Cluster(BaseCluster):
    def __init__(
            self, data_dir, *,
            pg_superuser='postgres', port=edgedb_defines.EDGEDB_PORT,
            runstate_dir=None, env=None, testmode=False, log_level=None):
        self._data_dir = data_dir
        if runstate_dir is None:
            runstate_dir = buildmeta.get_runstate_path(self._data_dir)
        super().__init__(
            runstate_dir,
            port=port,
            env=env,
            testmode=testmode,
            log_level=log_level,
        )
        self._edgedb_cmd.extend(['-D', self._data_dir])
        self._pg_connect_args['user'] = pg_superuser

    def _get_pg_cluster(self):
        return pgcluster.get_local_pg_cluster(self._data_dir)

    def get_data_dir(self):
        return self._data_dir

    def init(self, *, server_settings=None):
        cluster_status = self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._data_dir))

        self._init()


class TempCluster(Cluster):
    def __init__(
            self, *, data_dir_suffix=None, data_dir_prefix=None,
            data_dir_parent=None, env=None, testmode=False, log_level=None):
        tempdir = tempfile.mkdtemp(
            suffix=data_dir_suffix, prefix=data_dir_prefix,
            dir=data_dir_parent)
        super().__init__(data_dir=tempdir, runstate_dir=tempdir, env=env,
                         testmode=testmode, log_level=log_level)


class RunningCluster(BaseCluster):
    def __init__(self, **conn_args):
        self.conn_args = conn_args

    def is_managed(self):
        return False

    def ensure_initialized(self):
        return False

    def get_connect_args(self):
        return dict(self.conn_args)

    def get_status(self):
        return 'running'

    def init(self, **settings):
        pass

    def start(self, wait=60, **settings):
        pass

    def stop(self, wait=60):
        pass

    def destroy(self):
        pass


class TempClusterWithRemotePg(BaseCluster):
    def __init__(self, postgres_dsn, *, data_dir_suffix=None,
                 data_dir_prefix=None, data_dir_parent=None,
                 env=None, testmode=False, log_level=None):
        runstate_dir = tempfile.mkdtemp(
            suffix=data_dir_suffix, prefix=data_dir_prefix,
            dir=data_dir_parent)
        self._pg_dsn = postgres_dsn
        super().__init__(
            runstate_dir, env=env, testmode=testmode, log_level=log_level
        )
        self._edgedb_cmd.extend(['--postgres-dsn', postgres_dsn])

    def _get_pg_cluster(self):
        return pgcluster.get_remote_pg_cluster(self._pg_dsn)
