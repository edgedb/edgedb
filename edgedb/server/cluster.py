##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import asyncio
import errno
import os
import random
import socket
import subprocess
import sys
import tempfile
import time

from asyncpg import cluster as pg_cluster

from edgedb import client as edgedb_client
from edgedb.server import defines as edgedb_defines

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


class ClusterError(Exception):
    pass


class Cluster:
    def __init__(
            self, data_dir, *, pg_config_path=None, pg_superuser='postgres',
            port=edgedb_defines.EDGEDB_PORT, env=None):
        self._data_dir = data_dir
        self._pg_cluster = self._get_pg_cluster(data_dir, pg_config_path)
        self._pg_superuser = pg_superuser
        self._daemon_process = None
        self._port = port
        self._effective_port = None
        self._env = env

    def _get_pg_cluster(self, data_dir, pg_config_path):
        return pg_cluster.Cluster(data_dir, pg_config_path=pg_config_path)

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

    async def connect(self, loop=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()

        return await edgedb_client.connect(
            host='localhost', port=self._effective_port, loop=loop, **kwargs)

    def init(self, **settings):
        cluster_status = self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._data_dir))

        is_running = 'running' in cluster_status

        conn_args = {'port': find_available_port(), }

        server_settings = {
            'log_connections': 'yes',
            'log_statement': 'all',
            'log_disconnections': 'yes',
            'log_min_messages': 'WARNING',
            'client_min_messages': 'WARNING',
        }

        if cluster_status == 'not-initialized':
            self._pg_cluster.init(username=self._pg_superuser, locale='C')
            self._pg_cluster.start(
                server_settings=server_settings, **conn_args)

        elif not is_running:
            self._pg_cluster.start(
                server_settings=server_settings, **conn_args)

        try:
            self._init(self._pg_cluster)
        finally:
            if not is_running:
                self._pg_cluster.stop()

    def start(self, wait=60, **settings):
        port = settings.pop('port', None) or self._port
        if port == 'dynamic':
            port = find_available_port()

        self._effective_port = port

        extra_args = ['--{}={}'.format(k, v) for k, v in settings.items()]
        extra_args.append('--port={}'.format(self._effective_port))

        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        self._daemon_process = subprocess.Popen(
            ['edgedb-server', '-D', self._data_dir, *extra_args],
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
            ['edgedb-server', '-D', self._data_dir, '--bootstrap'],
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
            data_dir_parent=None, pg_config_path=None,
            env=None):
        self._data_dir = tempfile.mkdtemp(
            suffix=data_dir_suffix, prefix=data_dir_prefix,
            dir=data_dir_parent)
        super().__init__(self._data_dir, pg_config_path=pg_config_path,
                         env=env)
