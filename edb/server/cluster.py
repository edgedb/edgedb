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


import asyncio
import errno
import os
import pathlib
import random
import socket
import subprocess
import sys
import tempfile
import time

from asyncpg import cluster as pg_cluster

import edb
from edb import client as edgedb_client
from edb.client import connect_utils
from edb.server import defines as edgedb_defines

if sys.platform == 'linux':

    def ensure_dead_with_parent():
        import ctypes
        import signal

        try:
            PR_SET_PDEATHSIG = 1
            libc = ctypes.CDLL(ctypes.util.find_library('c'))
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
        except Exception as e:
            print(e)
else:

    def ensure_dead_with_parent():
        pass


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


def enable_dev_mode():
    os.environ['__EDGEDB_DEVMODE'] = '1'


def is_in_dev_mode() -> bool:
    devmode = os.environ.get('__EDGEDB_DEVMODE', '0')
    return devmode.lower() not in ('0', '', 'false')


def get_pg_config_path_from_build_meta() -> os.PathLike:
    try:
        from . import _buildmeta
        return pathlib.Path(_buildmeta.PG_CONFIG_PATH)
    except (ImportError, AttributeError):
        raise LookupError('could not find pg_config in build metadata') \
            from None


def get_pg_config_path() -> os.PathLike:
    if is_in_dev_mode():
        root = pathlib.Path(edb.server.__path__[0]).parent.parent
        pg_config = (root / 'build' / 'postgres' /
                     'install' / 'bin' / 'pg_config').resolve()
        if not pg_config.is_file():
            try:
                pg_config = get_pg_config_path_from_build_meta()
            except LookupError:
                pass

        if not pg_config.is_file():
            raise ClusterError('DEV mode: Could not find PostgreSQL build, '
                               'run `pip install -e .`')

    else:
        try:
            pg_config = get_pg_config_path_from_build_meta()
        except LookupError:
            raise ClusterError('could not find pg_config') from None

        if not pg_config.is_file():
            raise ClusterError(
                f'invalid pg_config path: {pg_config!r}: file does not exist '
                f'or is not a regular file')

    return pg_config


def get_pg_cluster(data_dir: os.PathLike) -> pg_cluster.Cluster:
    pg_config = get_pg_config_path()
    return pg_cluster.Cluster(data_dir=data_dir, pg_config_path=str(pg_config))


class ClusterError(Exception):
    pass


class Cluster:
    def __init__(
            self, data_dir_or_pg_cluster, *,
            pg_superuser='postgres', port=edgedb_defines.EDGEDB_PORT,
            env=None):
        if (isinstance(data_dir_or_pg_cluster, str) and
                (data_dir_or_pg_cluster.startswith('postgres://') or
                    data_dir_or_pg_cluster.startswith('postgres://'))):
            self._pg_dsn = data_dir_or_pg_cluster
            data_dir_or_pg_cluster = pg_cluster.RunningCluster(
                dsn=self._pg_dsn)
        else:
            self._pg_dsn = None

        self._edgedb_cmd = ['edgedb-server']

        if isinstance(data_dir_or_pg_cluster, pg_cluster.Cluster):
            self._data_dir = None
            self._location = None
            self._pg_cluster = data_dir_or_pg_cluster

            pg_conn_spec = dict(self._pg_cluster.get_connection_spec())
            pg_conn_spec['user'] = pg_superuser

            if self._pg_dsn is None:
                self._pg_dsn = connect_utils.render_dsn(
                    'postgres', pg_conn_spec)

            reduced_spec = {
                k: v for k, v in pg_conn_spec.items()
                if k in ('dsn', 'host', 'port')
            }
            self._location = connect_utils.render_dsn('postgres', reduced_spec)

            self._edgedb_cmd.extend(['-P', self._pg_dsn])
        else:
            self._data_dir = data_dir_or_pg_cluster
            self._location = self._data_dir
            self._pg_cluster = get_pg_cluster(self._data_dir)

            self._edgedb_cmd.extend(['-D', self._data_dir])

        self._pg_superuser = pg_superuser
        self._daemon_process = None
        self._port = port
        self._effective_port = None
        self._env = env

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
                    user=self._pg_superuser, database='template1', timeout=5,
                    loop=loop))

            db_exists = loop.run_until_complete(
                self._edgedb_template_exists(conn))
        finally:
            if conn is not None:
                conn.terminate()
            loop.run_until_complete(asyncio.sleep(0, loop=loop))
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

    async def connect(self, loop=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()

        connect_args = self.get_connect_args().copy()
        connect_args.update(kwargs)

        return await edgedb_client.connect(loop=loop, **connect_args)

    def init(self, *, server_settings={}):
        cluster_status = self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._location))

        self._init(self._pg_cluster)

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
            preexec_fn=ensure_dead_with_parent,
            env=env)

        self._test_connection()

    def stop(self, wait=60):
        if (
                self._daemon_process is not None and
                self._daemon_process.returncode is None):
            self._daemon_process.terminate()
            self._daemon_process.wait(wait)

    def destroy(self):
        self._pg_cluster.destroy()

    def _init(self, pg_connector):
        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        init = subprocess.run(
            self._edgedb_cmd + ['--bootstrap'],
            stdout=sys.stdout, stderr=sys.stderr,
            preexec_fn=ensure_dead_with_parent,
            env=env)

        if init.returncode != 0:
            raise ClusterError(
                f'edgedb-ctl init failed with exit code {init.returncode}')

    async def _edgedb_template_exists(self, conn):
        st = await conn.prepare(
            '''
            SELECT True FROM pg_catalog.pg_database WHERE datname = $1
        ''')

        return await st.fetchval(edgedb_defines.EDGEDB_TEMPLATE_DB)

    def _test_connection(self, timeout=60):
        self._connection_addr = None

        loop = asyncio.new_event_loop()

        pf = lambda: asyncio.Protocol()

        try:
            for i in range(timeout):
                try:
                    tr, pr = loop.run_until_complete(
                        loop.create_connection(
                            pf, host='localhost', port=self._effective_port))
                except (OSError, asyncio.TimeoutError):
                    time.sleep(1)
                    continue
                else:
                    tr.close()
                    loop.run_until_complete(asyncio.sleep(0, loop=loop))
                    break
            else:
                raise ClusterError(
                    f'could not connect to edgedb-server within {timeout}s')
        finally:
            loop.close()

        return 'running'


class TempCluster(Cluster):
    def __init__(
            self, *, data_dir_suffix=None, data_dir_prefix=None,
            data_dir_parent=None, env=None):
        self._data_dir = tempfile.mkdtemp(
            suffix=data_dir_suffix, prefix=data_dir_prefix,
            dir=data_dir_parent)
        super().__init__(self._data_dir, env=env)


class RunningCluster(Cluster):
    def __init__(self, **conn_args):
        self.conn_args = conn_args

    def is_managed(self):
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
