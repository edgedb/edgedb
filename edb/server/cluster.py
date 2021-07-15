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
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import typing

import edgedb

from edb.common import devmode
from edb.edgeql import quote

from edb.server import buildmeta
from edb.server import defines as edgedb_defines

from . import pgcluster


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
        else:
            self._edgedb_cmd.append('--generate-self-signed-cert')

        if testmode:
            self._edgedb_cmd.append('--testmode')

        self._runstate_dir = runstate_dir
        self._edgedb_cmd.extend(['--runstate-dir', runstate_dir])
        self._pg_cluster = self._get_pg_cluster()
        self._pg_connect_args = {}
        self._daemon_process = None
        self._port = port
        self._effective_port = None
        self._tls_cert_file = None
        self._env = env

    def _get_pg_cluster(self):
        raise NotImplementedError()

    def get_status(self):
        pg_status = self._pg_cluster.get_status()
        initially_stopped = pg_status == 'stopped'

        if initially_stopped:
            self._pg_cluster.start()
        elif pg_status == 'not-initialized':
            return 'not-initialized'

        conn = None
        loop = asyncio.new_event_loop()
        try:
            conn = loop.run_until_complete(
                self._pg_cluster.connect(
                    timeout=5, **self._pg_connect_args))

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
            'port': self._effective_port,
            'tls_ca_file': self._tls_cert_file,
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

    def start(self, wait=60, *, port: int=None, **settings):
        if port is None:
            port = self._port

        if port == 0:
            cmd_port = 'auto'
        else:
            cmd_port = str(port)

        extra_args = ['--{}={}'.format(k.replace('_', '-'), v)
                      for k, v in settings.items()]
        extra_args.append(f'--port={cmd_port}')
        status_r = status_w = None
        if port == 0:
            status_r, status_w = socket.socketpair()
            extra_args.append(f'--emit-server-status=fd://{status_w.fileno()}')

        env: typing.Optional[dict]
        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        self._daemon_process = subprocess.Popen(
            self._edgedb_cmd + extra_args,
            env=env,
            text=True,
            pass_fds=(status_w.fileno(),) if status_w is not None else (),
        )

        if status_w is not None:
            status_w.close()

        self._wait_for_server(timeout=wait, status_sock=status_r)

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

    def _wait_for_server(self, timeout=30, status_sock=None):

        async def _read_server_status(stream: asyncio.StreamReader):
            while True:
                line = await stream.readline()
                if not line:
                    raise ClusterError("EdgeDB server terminated")
                if line.startswith(b'READY='):
                    break

            _, _, dataline = line.decode().partition('=')
            try:
                return json.loads(dataline)
            except Exception as e:
                raise ClusterError(
                    f"EdgeDB server returned invalid status line: "
                    f"{dataline!r} ({e})"
                )

        async def test():
            stat_reader, stat_writer = await asyncio.open_connection(
                sock=status_sock,
            )
            try:
                data = await asyncio.wait_for(
                    _read_server_status(stat_reader),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                raise ClusterError(
                    f'EdgeDB server did not initialize '
                    f'within {timeout} seconds'
                ) from None

            self._effective_port = data['port']
            self._tls_cert_file = data['tls_cert_file']
            stat_writer.close()

        left = timeout
        if status_sock is not None:
            started = time.monotonic()
            asyncio.run(test())
            left -= (time.monotonic() - started)

        if self._admin_query("SELECT ();", f"{min(1, int(left))}s"):
            raise ClusterError(
                f'could not connect to edgedb-server '
                f'within {timeout} seconds') from None

    def _admin_query(self, query, wait_until_available="0s"):
        return subprocess.call(
            [
                sys.executable,
                "-m",
                "edb.cli",
                "--host",
                str(self._runstate_dir),
                "--port",
                str(self._effective_port),
                "--admin",
                "--user",
                edgedb_defines.EDGEDB_SUPERUSER,
                "--database",
                edgedb_defines.EDGEDB_SUPERUSER_DB,
                "--wait-until-available",
                wait_until_available,
                "-c",
                query,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

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
        self._pg_connect_args['database'] = 'template1'

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
