# Copyright (C) 2016-present MagicStack Inc. and the EdgeDB authors.
# Copyright (C) 2016-present the asyncpg authors and contributors
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

"""PostgreSQL cluster management."""

from __future__ import annotations
from typing import *  # NoQA

import asyncio
import logging
import os
import os.path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time

import asyncpg
from asyncpg import serverversion

from edb.server import buildmeta
from edb.server import defines

from . import pgconnparams


logger = logging.getLogger(__name__)

_system = platform.uname().system

if _system == 'Windows':
    def platform_exe(name):
        if name.endswith('.exe'):
            return name
        return name + '.exe'
else:
    def platform_exe(name):
        return name


class ClusterError(Exception):
    pass


class BaseCluster:

    def __init__(self):
        self._connection_addr = None
        self._connection_params = None
        self._default_session_auth = None

    async def connect(self, loop=None, **kwargs):
        conn_info = self.get_connection_spec()
        conn_info.update(kwargs)
        conn = await asyncpg.connect(loop=loop, **conn_info)

        if (not kwargs.get('user')
                and self._default_session_auth
                and conn_info.get('user') != self._default_session_auth):
            # No explicit user given, and the default
            # SESSION AUTHORIZATION is different from the user
            # used to connect.
            await conn.execute(
                f'SET SESSION AUTHORIZATION {self._default_session_auth};'
            )

        return conn

    def get_superuser_role(self) -> Optional[str]:
        return None

    def get_connection_addr(self):
        status = self.get_status()
        if status != 'running':
            raise ClusterError('cluster is not running')

        return self._get_connection_addr()

    def set_default_session_authorization(self, rolename: str) -> None:
        self._default_session_auth = rolename

    def set_connection_params(self, params):
        self._connection_params = params

    def get_connection_params(self):
        return self._connection_params

    def get_connection_spec(self):
        conn_dict = {}
        addr = self.get_connection_addr()
        conn_dict['host'] = addr[0]
        conn_dict['port'] = addr[1]
        params = self.get_connection_params()
        for k in ('user', 'password', 'database', 'ssl', 'server_settings'):
            v = getattr(params, k)
            if v is not None:
                conn_dict[k] = v

        return conn_dict

    def _get_connection_addr(self):
        return self._connection_addr


class Cluster(BaseCluster):
    def __init__(self, data_dir, *, pg_config_path=None):
        super().__init__()
        self._data_dir = data_dir
        self._pg_config_path = pg_config_path
        self._pg_bin_dir = os.environ.get('PGINSTALLATION')
        self._pg_ctl = None
        self._daemon_pid = None
        self._daemon_process = None

    def get_pg_version(self):
        return self._pg_version

    def is_managed(self):
        return True

    def get_data_dir(self):
        return self._data_dir

    def get_status(self):
        if self._pg_ctl is None:
            self._init_env()

        process = subprocess.run(
            [self._pg_ctl, 'status', '-D', self._data_dir],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.stdout, process.stderr

        if (process.returncode == 4 or not os.path.exists(self._data_dir) or
                not os.listdir(self._data_dir)):
            return 'not-initialized'
        elif process.returncode == 3:
            return 'stopped'
        elif process.returncode == 0:
            r = re.match(r'.*PID\s?:\s+(\d+).*', stdout.decode())
            if not r:
                raise ClusterError(
                    'could not parse pg_ctl status output: {}'.format(
                        stdout.decode()))
            self._daemon_pid = int(r.group(1))
            return self._test_connection(timeout=0)
        else:
            raise ClusterError(
                'pg_ctl status exited with status {:d}: {}'.format(
                    process.returncode, stderr))

    def ensure_initialized(self, **settings):
        cluster_status = self.get_status()

        if cluster_status == 'not-initialized':
            logger.info(
                'Initializing database cluster in %s', self._data_dir)
            initdb_output = self.init(
                username='postgres', locale='C', encoding='UTF8')
            for line in initdb_output.splitlines():
                logger.debug('initdb: %s', line)
            self.reset_hba()
            self.add_hba_entry(
                type='local',
                database='all',
                user='postgres',
                auth_method='trust'
            )
            self.add_hba_entry(
                type='local',
                database='all',
                user=defines.EDGEDB_SUPERUSER,
                auth_method='trust'
            )

            return True
        else:
            return False

    def init(self, **settings):
        """Initialize cluster."""
        if self.get_status() != 'not-initialized':
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._data_dir))

        if settings:
            settings_args = ['--{}={}'.format(k, v)
                             for k, v in settings.items()]
            extra_args = ['-o'] + [' '.join(settings_args)]
        else:
            extra_args = []

        process = subprocess.run(
            [self._pg_ctl, 'init', '-D', self._data_dir] + extra_args,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output = process.stdout

        if process.returncode != 0:
            raise ClusterError(
                'pg_ctl init exited with status {:d}:\n{}'.format(
                    process.returncode, output.decode()))

        return output.decode()

    def start(self, wait=60, *, server_settings=None, port, **opts):
        """Start the cluster."""
        status = self.get_status()
        if status == 'running':
            return
        elif status == 'not-initialized':
            raise ClusterError(
                'cluster in {!r} has not been initialized'.format(
                    self._data_dir))

        extra_args = ['--{}={}'.format(k, v) for k, v in opts.items()]
        extra_args.append('--port={}'.format(port))

        start_settings = {
            'listen_addresses': '',  # we use Unix sockets
            'unix_socket_permissions': '0700',
            'unix_socket_directories': str(self._data_dir),
            # TODO: EdgeDB must manage/monitor all client connections and
            # have its own "max_connections".  We'll set this setting even
            # higher when we have that fully implemented.
            'max_connections': '500',
        }

        if server_settings:
            start_settings.update(server_settings)

        ssl_key = start_settings.get('ssl_key_file')
        if ssl_key:
            # Make sure server certificate key file has correct permissions.
            keyfile = os.path.join(self._data_dir, 'srvkey.pem')
            shutil.copy(ssl_key, keyfile)
            os.chmod(keyfile, 0o600)
            start_settings['ssl_key_file'] = keyfile

        for k, v in start_settings.items():
            extra_args.extend(['-c', '{}={}'.format(k, v)])

        if _system == 'Windows':
            # On Windows we have to use pg_ctl as direct execution
            # of postgres daemon under an Administrative account
            # is not permitted and there is no easy way to drop
            # privileges.
            if os.getenv('ASYNCPG_DEBUG_SERVER'):
                stdout = sys.stdout
            else:
                stdout = subprocess.DEVNULL

            process = subprocess.run(
                [self._pg_ctl, 'start', '-D', self._data_dir,
                 '-o', ' '.join(extra_args)],
                stdout=stdout, stderr=subprocess.STDOUT)

            if process.returncode != 0:
                if process.stderr:
                    stderr = ':\n{}'.format(process.stderr.decode())
                else:
                    stderr = ''
                raise ClusterError(
                    'pg_ctl start exited with status {:d}{}'.format(
                        process.returncode, stderr))
        else:
            if os.getenv('ASYNCPG_DEBUG_SERVER'):
                stdout = sys.stdout
            else:
                stdout = subprocess.DEVNULL

            self._daemon_process = \
                subprocess.Popen(
                    [self._postgres, '-D', self._data_dir, *extra_args],
                    stdout=stdout, stderr=subprocess.STDOUT)

            self._daemon_pid = self._daemon_process.pid

        self._test_connection(timeout=wait)

    def reload(self):
        """Reload server configuration."""
        status = self.get_status()
        if status != 'running':
            raise ClusterError('cannot reload: cluster is not running')

        process = subprocess.run(
            [self._pg_ctl, 'reload', '-D', self._data_dir],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stderr = process.stderr

        if process.returncode != 0:
            raise ClusterError(
                'pg_ctl stop exited with status {:d}: {}'.format(
                    process.returncode, stderr.decode()))

    def stop(self, wait=60):
        process = subprocess.run(
            [self._pg_ctl, 'stop', '-D', self._data_dir, '-t', str(wait),
             '-m', 'fast'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stderr = process.stderr

        if process.returncode != 0:
            raise ClusterError(
                'pg_ctl stop exited with status {:d}: {}'.format(
                    process.returncode, stderr.decode()))

        if (self._daemon_process is not None and
                self._daemon_process.returncode is None):
            self._daemon_process.kill()
            self._daemon_process.wait(wait)

    def destroy(self):
        status = self.get_status()
        if status == 'stopped' or status == 'not-initialized':
            shutil.rmtree(self._data_dir)
        else:
            raise ClusterError('cannot destroy {} cluster'.format(status))

    def reset_wal(self, *, oid=None, xid=None):
        status = self.get_status()
        if status == 'not-initialized':
            raise ClusterError(
                'cannot modify WAL status: cluster is not initialized')

        if status == 'running':
            raise ClusterError(
                'cannot modify WAL status: cluster is running')

        opts = []
        if oid is not None:
            opts.extend(['-o', str(oid)])
        if xid is not None:
            opts.extend(['-x', str(xid)])
        if not opts:
            return

        opts.append(self._data_dir)

        try:
            reset_wal = self._find_pg_binary('pg_resetwal')
        except ClusterError:
            reset_wal = self._find_pg_binary('pg_resetxlog')

        process = subprocess.run(
            [reset_wal] + opts,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stderr = process.stderr

        if process.returncode != 0:
            raise ClusterError(
                'pg_resetwal exited with status {:d}: {}'.format(
                    process.returncode, stderr.decode()))

    def reset_hba(self):
        """Remove all records from pg_hba.conf."""
        status = self.get_status()
        if status == 'not-initialized':
            raise ClusterError(
                'cannot modify HBA records: cluster is not initialized')

        pg_hba = os.path.join(self._data_dir, 'pg_hba.conf')

        try:
            with open(pg_hba, 'w'):
                pass
        except IOError as e:
            raise ClusterError(
                'cannot modify HBA records: {}'.format(e)) from e

    def add_hba_entry(self, *, type='host', database, user, address=None,
                      auth_method, auth_options=None):
        """Add a record to pg_hba.conf."""
        status = self.get_status()
        if status == 'not-initialized':
            raise ClusterError(
                'cannot modify HBA records: cluster is not initialized')

        if type not in {'local', 'host', 'hostssl', 'hostnossl'}:
            raise ValueError('invalid HBA record type: {!r}'.format(type))

        pg_hba = os.path.join(self._data_dir, 'pg_hba.conf')

        record = '{} {} {}'.format(type, database, user)

        if type != 'local':
            if address is None:
                raise ValueError(
                    '{!r} entry requires a valid address'.format(type))
            else:
                record += ' {}'.format(address)

        record += ' {}'.format(auth_method)

        if auth_options is not None:
            record += ' ' + ' '.join(
                '{}={}'.format(k, v) for k, v in auth_options)

        try:
            with open(pg_hba, 'a') as f:
                print(record, file=f)
        except IOError as e:
            raise ClusterError(
                'cannot modify HBA records: {}'.format(e)) from e

    def trust_local_connections(self):
        self.reset_hba()

        if _system != 'Windows':
            self.add_hba_entry(type='local', database='all',
                               user='all', auth_method='trust')
        self.add_hba_entry(type='host', address='127.0.0.1/32',
                           database='all', user='all',
                           auth_method='trust')
        self.add_hba_entry(type='host', address='::1/128',
                           database='all', user='all',
                           auth_method='trust')
        status = self.get_status()
        if status == 'running':
            self.reload()

    def trust_local_replication_by(self, user):
        if _system != 'Windows':
            self.add_hba_entry(type='local', database='replication',
                               user=user, auth_method='trust')
        self.add_hba_entry(type='host', address='127.0.0.1/32',
                           database='replication', user=user,
                           auth_method='trust')
        self.add_hba_entry(type='host', address='::1/128',
                           database='replication', user=user,
                           auth_method='trust')
        status = self.get_status()
        if status == 'running':
            self.reload()

    def _init_env(self):
        if not self._pg_bin_dir:
            pg_config = self._find_pg_config(self._pg_config_path)
            pg_config_data = self._run_pg_config(pg_config)

            self._pg_bin_dir = pg_config_data.get('bindir')
            if not self._pg_bin_dir:
                raise ClusterError(
                    'pg_config output did not provide the BINDIR value')

        self._pg_ctl = self._find_pg_binary('pg_ctl')
        self._postgres = self._find_pg_binary('postgres')
        self._pg_version = self._get_pg_version()

    def _get_connection_addr(self):
        if self._connection_addr is None:
            self._connection_addr = self._connection_addr_from_pidfile()

        return self._connection_addr

    def _connection_addr_from_pidfile(self):
        pidfile = os.path.join(self._data_dir, 'postmaster.pid')

        try:
            with open(pidfile, 'rt') as f:
                piddata = f.read()
        except FileNotFoundError:
            return None

        lines = piddata.splitlines()

        if len(lines) < 6:
            # A complete postgres pidfile is at least 6 lines
            return None

        pmpid = int(lines[0])
        if self._daemon_pid and pmpid != self._daemon_pid:
            # This might be an old pidfile left from previous postgres
            # daemon run.
            return None

        portnum = lines[3]
        sockdir = lines[4]
        hostaddr = lines[5]

        if sockdir:
            if sockdir[0] != '/':
                # Relative sockdir
                sockdir = os.path.normpath(
                    os.path.join(self._data_dir, sockdir))
            host_str = sockdir
        else:
            host_str = hostaddr

        if host_str == '*':
            host_str = 'localhost'
        elif host_str == '0.0.0.0':
            host_str = '127.0.0.1'
        elif host_str == '::':
            host_str = '::1'

        return (host_str, portnum)

    def _test_connection(self, timeout=60):
        self._connection_addr = None

        loop = asyncio.new_event_loop()

        try:
            for _ in range(timeout):
                if self._connection_addr is None:
                    conn_addr = self._get_connection_addr()
                    if conn_addr is None:
                        time.sleep(1)
                        continue

                try:
                    con = loop.run_until_complete(
                        asyncpg.connect(database='postgres',
                                        user='postgres',
                                        timeout=5,
                                        loop=loop,
                                        host=self._connection_addr[0],
                                        port=self._connection_addr[1]))
                except (OSError, asyncio.TimeoutError,
                        asyncpg.CannotConnectNowError,
                        asyncpg.PostgresConnectionError):
                    time.sleep(1)
                    continue
                except asyncpg.PostgresError:
                    # Any other error other than ServerNotReadyError or
                    # ConnectionError is interpreted to indicate the server is
                    # up.
                    break
                else:
                    loop.run_until_complete(con.close())
                    break
        finally:
            loop.close()

        return 'running'

    def _run_pg_config(self, pg_config_path):
        process = subprocess.run(
            pg_config_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.stdout, process.stderr

        if process.returncode != 0:
            raise ClusterError('pg_config exited with status {:d}: {}'.format(
                process.returncode, stderr))
        else:
            config = {}

            for line in stdout.splitlines():
                k, eq, v = line.decode('utf-8').partition('=')
                if eq:
                    config[k.strip().lower()] = v.strip()

            return config

    def _find_pg_config(self, pg_config_path):
        if pg_config_path is None:
            pg_install = os.environ.get('PGINSTALLATION')
            if pg_install:
                pg_config_path = platform_exe(
                    os.path.join(pg_install, 'pg_config'))
            else:
                pathenv = os.environ.get('PATH').split(os.pathsep)
                for path in pathenv:
                    pg_config_path = platform_exe(
                        os.path.join(path, 'pg_config'))
                    if os.path.exists(pg_config_path):
                        break
                else:
                    pg_config_path = None

        if not pg_config_path:
            raise ClusterError('could not find pg_config executable')

        if not os.path.isfile(pg_config_path):
            raise ClusterError('{!r} is not an executable'.format(
                pg_config_path))

        return pg_config_path

    def _find_pg_binary(self, binary):
        bpath = platform_exe(os.path.join(self._pg_bin_dir, binary))

        if not os.path.isfile(bpath):
            raise ClusterError(
                'could not find {} executable: '.format(binary) +
                '{!r} does not exist or is not a file'.format(bpath))

        return bpath

    def _get_pg_version(self):
        process = subprocess.run(
            [self._postgres, '--version'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.stdout, process.stderr

        if process.returncode != 0:
            raise ClusterError(
                'postgres --version exited with status {:d}: {}'.format(
                    process.returncode, stderr))

        version_string = stdout.decode('utf-8').strip(' \n')
        prefix = 'postgres (PostgreSQL) '
        if not version_string.startswith(prefix):
            raise ClusterError(
                'could not determine server version from {!r}'.format(
                    version_string))
        version_string = version_string[len(prefix):]

        return serverversion.split_server_version_string(version_string)


class TempCluster(Cluster):
    def __init__(self, *,
                 data_dir_suffix=None, data_dir_prefix=None,
                 data_dir_parent=None, pg_config_path=None):
        self._data_dir = tempfile.mkdtemp(suffix=data_dir_suffix,
                                          prefix=data_dir_prefix,
                                          dir=data_dir_parent)
        super().__init__(self._data_dir, pg_config_path=pg_config_path)


class RemoteCluster(BaseCluster):
    def __init__(self, addr, params):
        super().__init__()
        self._connection_addr = addr
        self._connection_params = params

    def ensure_initialized(self, **settings):
        return False

    def is_managed(self):
        return False

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

    def reset_hba(self):
        raise ClusterError('cannot modify HBA records of unmanaged cluster')

    def add_hba_entry(self, *, type='host', database, user, address=None,
                      auth_method, auth_options=None):
        raise ClusterError('cannot modify HBA records of unmanaged cluster')


class RDSCluster(RemoteCluster):

    def get_superuser_role(self) -> Optional[str]:
        return 'rds_superuser'


def get_local_pg_cluster(data_dir: os.PathLike) -> Cluster:
    pg_config = buildmeta.get_pg_config_path()
    return Cluster(data_dir=data_dir, pg_config_path=str(pg_config))


def get_remote_pg_cluster(dsn: str) -> RemoteCluster:
    addrs, params = pgconnparams.parse_dsn(dsn)
    if len(addrs) > 1:
        raise ValueError('multiple hosts in Postgres DSN are not supported')
    rcluster = RemoteCluster(addrs[0], params)

    loop = asyncio.new_event_loop()

    async def _is_rds():
        conn = await rcluster.connect()

        try:
            rds_super = await conn.fetch(
                "SELECT * FROM pg_roles WHERE rolname = 'rds_superuser'"
            )
        finally:
            await conn.close()

        return bool(rds_super)

    try:
        is_rds = loop.run_until_complete(_is_rds())
    finally:
        loop.close()

    if is_rds:
        return RDSCluster(addrs[0], params)
    else:
        return rcluster
