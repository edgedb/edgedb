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
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    Iterable,
    Mapping,
    Sequence,
    Coroutine,
    Unpack,
    Dict,
    List,
    cast,
    TYPE_CHECKING,
)

import asyncio
import copy
import functools
import hashlib
import json
import logging
import os
import os.path
import pathlib
import re
import shlex
import shutil
import signal
import struct
import textwrap
import urllib.parse

from edb import buildmeta
from edb import errors
from edb.common import supervisor
from edb.common import uuidgen

from edb.server import args as srvargs
from edb.server import defines
from edb.server import pgconnparams
from edb.server.ha import base as ha_base
from edb.pgsql import common as pgcommon
from edb.pgsql import params as pgparams

if TYPE_CHECKING:
    from edb.server import pgcon

logger = logging.getLogger('edb.pgcluster')
pg_dump_logger = logging.getLogger('pg_dump')
pg_restore_logger = logging.getLogger('pg_restore')
pg_ctl_logger = logging.getLogger('pg_ctl')
pg_config_logger = logging.getLogger('pg_config')
initdb_logger = logging.getLogger('initdb')
postgres_logger = logging.getLogger('postgres')

get_database_backend_name = pgcommon.get_database_backend_name
get_role_backend_name = pgcommon.get_role_backend_name

EDGEDB_SERVER_SETTINGS = {
    'client_encoding': 'utf-8',
    # DO NOT raise client_min_messages above NOTICE level
    # because server indirect block return machinery relies
    # on NoticeResponse as the data channel.
    'client_min_messages': 'NOTICE',
    'search_path': 'edgedb',
    'timezone': 'UTC',
    'intervalstyle': 'iso_8601',
    'jit': 'off',
    'default_transaction_isolation': 'serializable',
}


class ClusterError(Exception):
    pass


class PostgresPidFileNotReadyError(Exception):
    """Raised on an attempt to read non-existent or bad Postgres PID file"""


class BaseCluster:

    def __init__(
        self,
        *,
        instance_params: Optional[pgparams.BackendInstanceParams] = None,
    ) -> None:
        self._connection_addr: Optional[Tuple[str, int]] = None
        self._connection_params: pgconnparams.ConnectionParams = \
            pgconnparams.ConnectionParams(server_settings=EDGEDB_SERVER_SETTINGS)
        self._pg_config_data: Dict[str, str] = {}
        self._pg_bin_dir: Optional[pathlib.Path] = None
        if instance_params is None:
            self._instance_params = (
                pgparams.get_default_runtime_params().instance_params)
        else:
            self._instance_params = instance_params

    def get_db_name(self, db_name: str) -> str:
        if (
            not self._instance_params.capabilities
            & pgparams.BackendCapabilities.CREATE_DATABASE
        ):
            assert (
                db_name == defines.EDGEDB_SUPERUSER_DB
            ), f"db_name={db_name} is not allowed"
            rv = self.get_connection_params().database
            assert rv is not None
            return rv
        return get_database_backend_name(
            db_name,
            tenant_id=self._instance_params.tenant_id,
        )

    def get_role_name(self, role_name: str) -> str:
        if (
            not self._instance_params.capabilities
            & pgparams.BackendCapabilities.CREATE_ROLE
        ):
            assert (
                role_name == defines.EDGEDB_SUPERUSER
            ), f"role_name={role_name} is not allowed"
            rv = self.get_connection_params().user
            assert rv is not None
            return rv

        return get_database_backend_name(
            role_name,
            tenant_id=self._instance_params.tenant_id,
        )

    async def start(
        self,
        wait: int = 60,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
        **opts: Any,
    ) -> None:
        raise NotImplementedError

    async def stop(self, wait: int = 60) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    async def connect(self,
                      *,
                      source_description: str,
                      apply_init_script: bool = False,
                      **kwargs: Unpack[pgconnparams.CreateParamsKwargs]
    ) -> pgcon.PGConnection:
        """Connect to this cluster, with optional overriding parameters. If
        overriding parameters are specified, they are applied to a copy of the
        connection parameters before the connection takes place."""
        from edb.server import pgcon

        connection = copy.copy(self.get_connection_params())
        addr = self._get_connection_addr()
        assert addr is not None
        connection.update(hosts=[addr])
        connection.update(**kwargs)
        conn = await pgcon.pg_connect(
            connection,
            source_description=source_description,
            backend_params=self.get_runtime_params(),
            apply_init_script=apply_init_script,
        )
        return conn

    async def start_watching(
        self, failover_cb: Optional[Callable[[], None]] = None
    ) -> None:
        pass

    def stop_watching(self) -> None:
        pass

    def get_runtime_params(self) -> pgparams.BackendRuntimeParams:
        params = self.get_connection_params()
        login_role: Optional[str] = params.user
        sup_role = self.get_role_name(defines.EDGEDB_SUPERUSER)
        return pgparams.BackendRuntimeParams(
            instance_params=self._instance_params,
            session_authorization_role=(
                None if login_role == sup_role else login_role
            ),
        )

    def overwrite_capabilities(
        self, caps: pgparams.BackendCapabilities
    ) -> None:
        self._instance_params = self._instance_params._replace(
            capabilities=caps
        )

    def update_connection_params(
        self,
        **kwargs: Unpack[pgconnparams.CreateParamsKwargs],
    ) -> None:
        self._connection_params.update(**kwargs)

    def get_pgaddr(self) -> pgconnparams.ConnectionParams:
        assert self._connection_params is not None
        addr = self._get_connection_addr()
        assert addr is not None
        params = copy.copy(self._connection_params)
        params.update(hosts=[addr])
        return params

    def get_connection_params(
        self,
    ) -> pgconnparams.ConnectionParams:
        assert self._connection_params is not None
        return self._connection_params

    def _get_connection_addr(self) -> Optional[Tuple[str, int]]:
        return self._connection_addr

    def is_managed(self) -> bool:
        raise NotImplementedError

    async def get_status(self) -> str:
        raise NotImplementedError

    def _dump_restore_conn_args(
        self,
        dbname: str,
    ) -> tuple[list[str], dict[str, str]]:
        params = copy.copy(self.get_connection_params())
        addr = self._get_connection_addr()
        assert addr is not None
        params.update(database=dbname, hosts=[addr])

        args = [
            f'--dbname={params.database}',
            f'--host={params.host}',
            f'--port={params.port}',
            f'--username={params.user}',
        ]

        env = os.environ.copy()
        if params.password:
            env['PGPASSWORD'] = params.password

        return args, env

    async def dump_database(
        self,
        dbname: str,
        *,
        exclude_schemas: Iterable[str] = (),
        include_schemas: Iterable[str] = (),
        include_tables: Iterable[str] = (),
        include_extensions: Iterable[str] = (),
        schema_only: bool = False,
        dump_object_owners: bool = True,
        create_database: bool = False,
    ) -> bytes:
        status = await self.get_status()
        if status != 'running':
            raise ClusterError('cannot dump: cluster is not running')

        if self._pg_bin_dir is None:
            await self.lookup_postgres()
        pg_dump = self._find_pg_binary('pg_dump')

        conn_args, env = self._dump_restore_conn_args(dbname)

        args = [
            pg_dump,
            '--inserts',
            *conn_args,
        ]

        if not dump_object_owners:
            args.append('--no-owner')
        if schema_only:
            args.append('--schema-only')
        if create_database:
            args.append('--create')

        configs = [
            ('exclude-schema', exclude_schemas),
            ('schema', include_schemas),
            ('table', include_tables),
            ('extension', include_extensions),
        ]
        for flag, vals in configs:
            for val in vals:
                args.append(f'--{flag}={val}')

        stdout_lines, _, _ = await _run_logged_subprocess(
            args,
            logger=pg_dump_logger,
            log_stdout=False,
            env=env,
        )
        return b'\n'.join(stdout_lines)

    async def _copy_database(
        self,
        src_dbname: str,
        tgt_dbname: str,
        src_args: list[str],
        tgt_args: list[str],
    ) -> None:
        status = await self.get_status()
        if status != 'running':
            raise ClusterError('cannot dump: cluster is not running')

        if self._pg_bin_dir is None:
            await self.lookup_postgres()
        pg_dump = self._find_pg_binary('pg_dump')
        # We actually just use psql to restore, because it is more
        # tolerant of version differences.
        # TODO: Maybe use pg_restore when we know we match the backend version?
        pg_restore = self._find_pg_binary('psql')

        src_conn_args, src_env = self._dump_restore_conn_args(src_dbname)
        tgt_conn_args, _tgt_env = self._dump_restore_conn_args(tgt_dbname)

        dump_args = [
            pg_dump, '--verbose', *src_conn_args, *src_args
        ]
        restore_args = [
            pg_restore, *tgt_conn_args, *tgt_args
        ]

        rpipe, wpipe = os.pipe()
        wpipef = os.fdopen(wpipe, "wb")

        try:
            # N.B: uvloop will waitpid() on the child process even if we don't
            # actually await on it due to a later error.
            dump_p, dump_out_r, dump_err_r = await _start_logged_subprocess(
                dump_args,
                logger=pg_dump_logger,
                override_stdout=wpipef,
                log_stdout=False,
                capture_stdout=False,
                capture_stderr=False,
                env=src_env,
            )

            res_p, res_out_r, res_err_r = await _start_logged_subprocess(
                restore_args,
                logger=pg_restore_logger,
                stdin=rpipe,
                capture_stdout=False,
                capture_stderr=False,
                log_stdout=True,
                log_stderr=True,
                env=src_env,
            )
        finally:
            wpipef.close()
            os.close(rpipe)

        dump_exit_code, _, _, restore_exit_code, _, _ = await asyncio.gather(
            dump_p.wait(), dump_out_r, dump_err_r,
            res_p.wait(), res_out_r, res_err_r,
        )

        if dump_exit_code != 0 and dump_exit_code != -signal.SIGPIPE:
            raise errors.ExecutionError(
                f'branch failed: {dump_args[0]} exited with status '
                f'{dump_exit_code}'
            )
        if restore_exit_code != 0:
            raise errors.ExecutionError(
                f'branch failed: '
                f'{restore_args[0]} exited with status {restore_exit_code}'
            )

    def _find_pg_binary(self, binary: str) -> str:
        assert self._pg_bin_dir is not None
        bpath = self._pg_bin_dir / binary
        if not bpath.is_file():
            raise ClusterError(
                'could not find {} executable: '.format(binary) +
                '{!r} does not exist or is not a file'.format(bpath))

        return str(bpath)

    def _subprocess_error(
        self,
        name: str,
        exitcode: int,
        stderr: Optional[bytes],
    ) -> ClusterError:
        if stderr:
            return ClusterError(
                f'{name} exited with status {exitcode}:\n'
                + textwrap.indent(stderr.decode(), ' ' * 4),
            )
        else:
            return ClusterError(
                f'{name} exited with status {exitcode}',
            )

    async def lookup_postgres(self) -> None:
        self._pg_bin_dir = await get_pg_bin_dir()

    def get_client_id(self) -> int:
        return 0


class Cluster(BaseCluster):
    def __init__(
        self,
        data_dir: pathlib.Path,
        *,
        runstate_dir: Optional[pathlib.Path] = None,
        instance_params: Optional[pgparams.BackendInstanceParams] = None,
        log_level: str = 'i',
    ):
        super().__init__(instance_params=instance_params)
        self._data_dir = data_dir
        self._runstate_dir = (
            runstate_dir if runstate_dir is not None else data_dir)
        self._daemon_pid: Optional[int] = None
        self._daemon_process: Optional[asyncio.subprocess.Process] = None
        self._daemon_supervisor: Optional[supervisor.Supervisor] = None
        self._log_level = log_level

    def is_managed(self) -> bool:
        return True

    def get_data_dir(self) -> pathlib.Path:
        return self._data_dir

    def get_main_pid(self) -> Optional[int]:
        return self._daemon_pid

    async def get_status(self) -> str:
        stdout_lines, stderr_lines, exit_code = (
            await _run_logged_text_subprocess(
                [self._pg_ctl, 'status', '-D', str(self._data_dir)],
                logger=pg_ctl_logger,
                check=False,
            )
        )

        if (
            exit_code == 4
            or not os.path.exists(self._data_dir)
            or not os.listdir(self._data_dir)
        ):
            return 'not-initialized'
        elif exit_code == 3:
            return 'stopped'
        elif exit_code == 0:
            output = '\n'.join(stdout_lines)
            r = re.match(r'.*PID\s?:\s+(\d+).*', output)
            if not r:
                raise ClusterError(
                    f'could not parse pg_ctl status output: {output}')
            self._daemon_pid = int(r.group(1))
            if self._connection_addr is None:
                self._connection_addr = self._connection_addr_from_pidfile()
            return 'running'
        else:
            stderr_text = '\n'.join(stderr_lines)
            raise ClusterError(
                f'`pg_ctl status` exited with status {exit_code}:\n'
                + textwrap.indent(stderr_text, ' ' * 4),
            )

    async def ensure_initialized(self, **settings: Any) -> bool:
        cluster_status = await self.get_status()

        if cluster_status == 'not-initialized':
            logger.info(
                'Initializing database cluster in %s', self._data_dir)

            have_c_utf8 = self.get_runtime_params().has_c_utf8_locale
            await self.init(
                username='postgres',
                locale='C.UTF-8' if have_c_utf8 else 'en_US.UTF-8',
                lc_collate='C',
                encoding='UTF8',
            )
            self.reset_hba()
            self.add_hba_entry(
                type='local',
                database='all',
                user='postgres',
                auth_method='trust'
            )
            return True
        else:
            return False

    async def init(self, **settings: str) -> None:
        """Initialize cluster."""
        if await self.get_status() != 'not-initialized':
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._data_dir))

        if settings:
            settings_args = ['--{}={}'.format(k.replace('_', '-'), v)
                             for k, v in settings.items()]
            extra_args = ['-o'] + [' '.join(settings_args)]
        else:
            extra_args = []

        await _run_logged_subprocess(
            [self._pg_ctl, 'init', '-D', str(self._data_dir)] + extra_args,
            logger=initdb_logger,
        )

    async def start(
        self,
        wait: int = 60,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
        **opts: str,
    ) -> None:
        """Start the cluster."""
        status = await self.get_status()
        if status == 'running':
            return
        elif status == 'not-initialized':
            raise ClusterError(
                'cluster in {!r} has not been initialized'.format(
                    self._data_dir))

        extra_args = ['--{}={}'.format(k, v) for k, v in opts.items()]

        start_settings = {
            'listen_addresses': '',  # we use Unix sockets
            'unix_socket_permissions': '0700',
            'unix_socket_directories': str(self._runstate_dir),
            # here we are not setting superuser_reserved_connections because
            # we're using superuser only now (so all connections available),
            # and we don't support reserving connections for now
            'max_connections': str(self._instance_params.max_connections),
            # From Postgres docs:
            #
            #   You might need to raise this value if you have queries that
            #   touch many different tables in a single transaction, e.g.,
            #   query of a parent table with many children.
            #
            # EdgeDB queries might touch _lots_ of tables, especially in deep
            # inheritance hierarchies.  This is especially important in low
            # `max_connections` scenarios.
            'max_locks_per_transaction': 1024,
            'max_pred_locks_per_transaction': 1024,
            "shared_preload_libraries": ",".join(
                [
                    "edb_stat_statements",
                ]
            ),
            "edb_stat_statements.track_planning": "true",
        }

        if os.getenv('EDGEDB_DEBUG_PGSERVER'):
            start_settings['log_min_messages'] = 'info'
            start_settings['log_statement'] = 'all'
        else:
            log_level_map = {
                'd': 'INFO',
                'i': 'WARNING',  # NOTICE in Postgres is quite noisy
                'w': 'WARNING',
                'e': 'ERROR',
                's': 'PANIC',
            }
            start_settings['log_min_messages'] = log_level_map[self._log_level]
            start_settings['log_statement'] = 'none'
            start_settings['log_line_prefix'] = ''

        if server_settings:
            start_settings.update(server_settings)

        ssl_key = start_settings.get('ssl_key_file')
        if ssl_key:
            # Make sure server certificate key file has correct permissions.
            keyfile = os.path.join(self._data_dir, 'srvkey.pem')
            assert isinstance(ssl_key, str)
            shutil.copy(ssl_key, keyfile)
            os.chmod(keyfile, 0o600)
            start_settings['ssl_key_file'] = keyfile

        for k, v in start_settings.items():
            extra_args.extend(['-c', '{}={}'.format(k, v)])

        self._daemon_process, *loggers = await _start_logged_subprocess(
            [self._postgres, '-D', str(self._data_dir), *extra_args],
            capture_stdout=False,
            capture_stderr=False,
            logger=postgres_logger,
            log_processor=postgres_log_processor,
        )
        self._daemon_pid = self._daemon_process.pid

        sup = await supervisor.Supervisor.create(name="postgres loggers")
        for logger_coro in loggers:
            sup.create_task(logger_coro)
        self._daemon_supervisor = sup

        await self._test_connection(timeout=wait)

    async def reload(self) -> None:
        """Reload server configuration."""
        status = await self.get_status()
        if status != 'running':
            raise ClusterError('cannot reload: cluster is not running')

        await _run_logged_subprocess(
            [self._pg_ctl, 'reload', '-D', str(self._data_dir)],
            logger=pg_ctl_logger,
        )

    async def stop(self, wait: int = 60) -> None:
        await _run_logged_subprocess(
            [
                self._pg_ctl,
                'stop', '-D', str(self._data_dir),
                '-t', str(wait), '-m', 'fast'
            ],
            logger=pg_ctl_logger,
        )

        if (
            self._daemon_process is not None and
            self._daemon_process.returncode is None
        ):
            self._daemon_process.terminate()
            await asyncio.wait_for(self._daemon_process.wait(), timeout=wait)

        if self._daemon_supervisor is not None:
            await self._daemon_supervisor.cancel()
            self._daemon_supervisor = None

    def destroy(self) -> None:
        shutil.rmtree(self._data_dir)

    def reset_hba(self) -> None:
        """Remove all records from pg_hba.conf."""
        pg_hba = os.path.join(self._data_dir, 'pg_hba.conf')

        try:
            with open(pg_hba, 'w'):
                pass
        except IOError as e:
            raise ClusterError(
                'cannot modify HBA records: {}'.format(e)) from e

    def add_hba_entry(
        self,
        *,
        type: str = 'host',
        database: str,
        user: str,
        address: Optional[str] = None,
        auth_method: str,
        auth_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Add a record to pg_hba.conf."""
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
                '{}={}'.format(k, v) for k, v in auth_options.items())

        try:
            with open(pg_hba, 'a') as f:
                print(record, file=f)
        except IOError as e:
            raise ClusterError(
                'cannot modify HBA records: {}'.format(e)) from e

    async def trust_local_connections(self) -> None:
        self.reset_hba()

        self.add_hba_entry(type='local', database='all',
                           user='all', auth_method='trust')
        self.add_hba_entry(type='host', address='127.0.0.1/32',
                           database='all', user='all',
                           auth_method='trust')
        self.add_hba_entry(type='host', address='::1/128',
                           database='all', user='all',
                           auth_method='trust')
        status = await self.get_status()
        if status == 'running':
            await self.reload()

    async def lookup_postgres(self) -> None:
        await super().lookup_postgres()
        self._pg_ctl = self._find_pg_binary('pg_ctl')
        self._postgres = self._find_pg_binary('postgres')

    def _get_connection_addr(self) -> Tuple[str, int]:
        if self._connection_addr is None:
            self._connection_addr = self._connection_addr_from_pidfile()

        return self._connection_addr

    def _connection_addr_from_pidfile(self) -> Tuple[str, int]:
        pidfile = os.path.join(self._data_dir, 'postmaster.pid')

        try:
            with open(pidfile, 'rt') as f:
                piddata = f.read()
        except FileNotFoundError:
            raise PostgresPidFileNotReadyError

        lines = piddata.splitlines()

        if len(lines) < 6:
            # A complete postgres pidfile is at least 6 lines
            raise PostgresPidFileNotReadyError

        pmpid = int(lines[0])
        if self._daemon_pid and pmpid != self._daemon_pid:
            # This might be an old pidfile left from previous postgres
            # daemon run.
            raise PostgresPidFileNotReadyError

        portnum = int(lines[3])
        sockdir = lines[4]
        hostaddr = lines[5]

        if sockdir:
            if sockdir[0] != '/':
                # Relative sockdir
                sockdir = os.path.normpath(
                    os.path.join(self._data_dir, sockdir))
            host_str = sockdir
        elif hostaddr:
            host_str = hostaddr
        else:
            raise PostgresPidFileNotReadyError

        if host_str == '*':
            host_str = 'localhost'
        elif host_str == '0.0.0.0':
            host_str = '127.0.0.1'
        elif host_str == '::':
            host_str = '::1'

        return (host_str, portnum)

    async def _test_connection(self, timeout: int = 60) -> str:
        from edb.server import pgcon

        self._connection_addr = None
        connected = False

        params = pgconnparams.ConnectionParams(
            user="postgres",
            database="postgres")

        for n in range(timeout + 9):
            # pg usually comes up pretty quickly, but not so quickly
            # that we don't hit the wait case. Make our first several
            # waits pretty short, to shave almost a second off the
            # happy case.
            sleep_time = 1.0 if n >= 10 else 0.1

            try:
                conn_addr = self._get_connection_addr()
            except PostgresPidFileNotReadyError:
                try:
                    assert self._daemon_process is not None
                    code = await asyncio.wait_for(
                        self._daemon_process.wait(),
                        sleep_time
                    )
                except asyncio.TimeoutError:
                    # means that the postgres process is still alive
                    pass
                else:
                    # the postgres process has exited prematurely
                    raise ClusterError(f"The backend exited with {code}")

                continue

            try:
                params.update(hosts=[conn_addr])
                con = await asyncio.wait_for(
                    pgcon.pg_connect(
                        params,
                        source_description=f"{self.__class__}._test_connection",
                        backend_params=self.get_runtime_params(),
                        apply_init_script=False,
                    ),
                    timeout=5,
                )
            except (
                OSError,
                asyncio.TimeoutError,
                pgcon.BackendConnectionError,
            ) as e:
                if n % 10 == 0 and 0 < n < timeout + 9 - 1:
                    logger.error("cannot connect to the backend cluster:"
                                 " %s, retrying...", e)
                await asyncio.sleep(sleep_time)
                continue
            except pgcon.BackendError:
                # Any other error other than ServerNotReadyError or
                # ConnectionError is interpreted to indicate the server is
                # up.
                break
            else:
                connected = True
                con.terminate()
                break

        if connected:
            return 'running'
        else:
            return 'not-initialized'


class RemoteCluster(BaseCluster):
    def __init__(
        self,
        *,
        connection_addr: tuple[str, int],
        connection_params: pgconnparams.ConnectionParams,
        instance_params: Optional[pgparams.BackendInstanceParams] = None,
        ha_backend: Optional[ha_base.HABackend] = None,
    ):
        super().__init__(instance_params=instance_params)
        self._connection_params = connection_params
        self._connection_params.update(
            server_settings=EDGEDB_SERVER_SETTINGS
        )
        self._connection_addr = connection_addr
        self._ha_backend = ha_backend

    def _get_connection_addr(self) -> Optional[Tuple[str, int]]:
        if self._ha_backend is not None:
            return self._ha_backend.get_master_addr()
        return self._connection_addr

    async def ensure_initialized(self, **settings: Any) -> bool:
        return False

    def is_managed(self) -> bool:
        return False

    async def get_status(self) -> str:
        return 'running'

    def init(self, **settings: str) -> Optional[str]:
        pass

    async def start(
        self,
        wait: int = 60,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
        **opts: Any,
    ) -> None:
        pass

    async def stop(self, wait: int = 60) -> None:
        pass

    def destroy(self) -> None:
        pass

    def reset_hba(self) -> None:
        raise ClusterError('cannot modify HBA records of unmanaged cluster')

    def add_hba_entry(
        self,
        *,
        type: str = 'host',
        database: str,
        user: str,
        address: Optional[str] = None,
        auth_method: str,
        auth_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        raise ClusterError('cannot modify HBA records of unmanaged cluster')

    async def start_watching(
        self, failover_cb: Optional[Callable[[], None]] = None
    ) -> None:
        if self._ha_backend is not None:
            self._ha_backend.set_failover_callback(failover_cb)
            await self._ha_backend.start_watching()

    def stop_watching(self) -> None:
        if self._ha_backend is not None:
            self._ha_backend.stop_watching()

    @functools.cache
    def get_client_id(self) -> int:
        tenant_id = self._instance_params.tenant_id
        if self._ha_backend is not None:
            backend_dsn = self._ha_backend.dsn
        else:
            assert self._connection_addr is not None
            assert self._connection_params is not None
            host, port = self._connection_addr
            database = self._connection_params.database
            backend_dsn = f"postgres://{host}:{port}/{database}"
        data = f"{backend_dsn}|{tenant_id}".encode("utf-8")
        digest = hashlib.blake2b(data, digest_size=8).digest()
        rv: int = struct.unpack("q", digest)[0]
        return rv


async def get_pg_bin_dir() -> pathlib.Path:
    pg_config_data = await get_pg_config()
    pg_bin_dir = pg_config_data.get('bindir')
    if not pg_bin_dir:
        raise ClusterError(
            'pg_config output did not provide the BINDIR value')
    return pathlib.Path(pg_bin_dir)


async def get_pg_config() -> Dict[str, str]:
    stdout_lines, _, _ = await _run_logged_text_subprocess(
        [str(buildmeta.get_pg_config_path())],
        logger=pg_config_logger,
    )

    config = {}
    for line in stdout_lines:
        k, eq, v = line.partition('=')
        if eq:
            config[k.strip().lower()] = v.strip()

    return config


async def get_local_pg_cluster(
    data_dir: pathlib.Path,
    *,
    runstate_dir: Optional[pathlib.Path] = None,
    max_connections: Optional[int] = None,
    tenant_id: Optional[str] = None,
    log_level: Optional[str] = None,
) -> Cluster:
    if log_level is None:
        log_level = 'i'
    if tenant_id is None:
        tenant_id = buildmeta.get_default_tenant_id()
    instance_params = None
    if max_connections is not None:
        instance_params = pgparams.get_default_runtime_params(
            max_connections=max_connections,
            tenant_id=tenant_id,
        ).instance_params
    cluster = Cluster(
        data_dir=data_dir,
        runstate_dir=runstate_dir,
        instance_params=instance_params,
        log_level=log_level,
    )
    await cluster.lookup_postgres()
    return cluster


async def get_remote_pg_cluster(
    dsn: str,
    *,
    tenant_id: Optional[str] = None,
    specified_capabilities: Optional[srvargs.BackendCapabilitySets] = None,
) -> RemoteCluster:
    from edb.server import pgcon
    parsed = urllib.parse.urlparse(dsn)
    ha_backend = None

    if parsed.scheme not in {'postgresql', 'postgres'}:
        ha_backend = ha_base.get_backend(parsed)
        if ha_backend is None:
            raise ValueError(
                'invalid DSN: scheme is expected to be "postgresql", '
                '"postgres" or one of the supported HA backend, '
                'got {!r}'.format(parsed.scheme))

        addr = await ha_backend.get_cluster_consensus()
        dsn = 'postgresql://{}:{}'.format(*addr)

        if parsed.query:
            # Allow passing through Postgres connection parameters from the HA
            # backend DSN as "pg" prefixed query strings. For example, an HA
            # backend DSN with `?pgpassword=123` will result an actual backend
            # DSN with `?password=123`. They have higher priority than the `PG`
            # prefixed environment variables like `PGPASSWORD`.
            pq = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            query = {}
            for k, v in pq.items():
                if k.startswith("pg") and k not in ["pghost", "pgport"]:
                    if isinstance(v, list):
                        val = v[-1]
                    else:
                        val = cast(str, v)
                    query[k[2:]] = val
            if query:
                dsn += f"?{urllib.parse.urlencode(query)}"

    if tenant_id is None:
        t_id = buildmeta.get_default_tenant_id()
    else:
        t_id = tenant_id

    async def _get_cluster_type(
        conn: pgcon.PGConnection,
    ) -> Tuple[Type[RemoteCluster], Optional[str]]:
        managed_clouds = {
            'rds_superuser': RemoteCluster,    # Amazon RDS
            'cloudsqlsuperuser': RemoteCluster,    # GCP Cloud SQL
            'azure_pg_admin': RemoteCluster,    # Azure Postgres
        }

        managed_cloud_super = await conn.sql_fetch_val(
            b"""
                SELECT
                    rolname
                FROM
                    pg_roles
                WHERE
                    rolname IN (SELECT json_array_elements_text($1::json))
                LIMIT
                    1
            """,
            args=[json.dumps(list(managed_clouds)).encode("utf-8")],
        )

        if managed_cloud_super is not None:
            rolname = managed_cloud_super.decode("utf-8")
            return managed_clouds[rolname], rolname
        else:
            return RemoteCluster, None

    async def _detect_capabilities(
        conn: pgcon.PGConnection,
    ) -> pgparams.BackendCapabilities:
        from edb.server import pgcon
        from edb.server.pgcon import errors

        caps = pgparams.BackendCapabilities.NONE

        try:
            cur_cluster_name = await conn.sql_fetch_val(
                b"""
                SELECT
                    setting
                FROM
                    pg_file_settings
                WHERE
                    setting = 'cluster_name'
                    AND sourcefile = ((
                        SELECT setting
                        FROM pg_settings WHERE name = 'data_directory'
                    ) || '/postgresql.auto.conf')
                """,
            )
        except pgcon.BackendPrivilegeError:
            configfile_access = False
        else:
            try:
                await conn.sql_execute(b"""
                    ALTER SYSTEM SET cluster_name = 'edgedb-test'
                """)
            except pgcon.BackendPrivilegeError:
                configfile_access = False
            except pgcon.BackendError as e:
                # Stolon keeper symlinks postgresql.auto.conf to /dev/null
                # making ALTER SYSTEM fail with InternalServerError,
                # see https://github.com/sorintlab/stolon/pull/343
                if 'could not fsync file "postgresql.auto.conf"' in e.args[0]:
                    configfile_access = False
                else:
                    raise
            else:
                configfile_access = True

                if cur_cluster_name:
                    cn = pgcommon.quote_literal(
                        cur_cluster_name.decode("utf-8"))
                    await conn.sql_execute(
                        f"""
                        ALTER SYSTEM SET cluster_name = {cn}
                        """.encode("utf-8"),
                    )
                else:
                    await conn.sql_execute(
                        b"""
                        ALTER SYSTEM SET cluster_name = DEFAULT
                        """,
                    )

        if configfile_access:
            caps |= pgparams.BackendCapabilities.CONFIGFILE_ACCESS

        await conn.sql_execute(b"START TRANSACTION")
        rname = str(uuidgen.uuid1mc())

        try:
            await conn.sql_execute(
                f"CREATE ROLE {pgcommon.quote_ident(rname)} WITH SUPERUSER"
                .encode("utf-8"),
            )
        except pgcon.BackendPrivilegeError:
            can_make_superusers = False
        except pgcon.BackendError as e:
            if e.code_is(
                errors.ERROR_INTERNAL_ERROR
            ) and "not in permitted superuser list" in str(e):
                # DigitalOcean raises a custom error:
                # XX000: Role ... not in permitted superuser list
                can_make_superusers = False
            else:
                raise
        else:
            can_make_superusers = True
        finally:
            await conn.sql_execute(b"ROLLBACK")

        if can_make_superusers:
            caps |= pgparams.BackendCapabilities.SUPERUSER_ACCESS

        coll = await conn.sql_fetch_val(b"""
            SELECT collname FROM pg_collation
            WHERE lower(replace(collname, '-', '')) = 'c.utf8' LIMIT 1;
        """)

        if coll is not None:
            caps |= pgparams.BackendCapabilities.C_UTF8_LOCALE

        roles = json.loads(await conn.sql_fetch_val(
            b"""
            SELECT json_build_object(
                'rolcreaterole', rolcreaterole,
                'rolcreatedb', rolcreatedb
            )
            FROM pg_roles
            WHERE rolname = (SELECT current_user);
            """,
        ))

        if roles['rolcreaterole']:
            caps |= pgparams.BackendCapabilities.CREATE_ROLE
        if roles['rolcreatedb']:
            caps |= pgparams.BackendCapabilities.CREATE_DATABASE

        stats_ver = await conn.sql_fetch_val(b"""
            SELECT default_version FROM pg_available_extensions
            WHERE name = 'edb_stat_statements';
        """)
        if stats_ver in (b"1.0",):
            caps |= pgparams.BackendCapabilities.STAT_STATEMENTS

        return caps

    async def _get_pg_settings(
        conn: pgcon.PGConnection,
        name: str,
    ) -> str:
        return await conn.sql_fetch_val(  # type: ignore
            b"SELECT setting FROM pg_settings WHERE name = $1",
            args=[name.encode("utf-8")],
        )

    async def _get_reserved_connections(
        conn: pgcon.PGConnection,
    ) -> int:
        rv = int(
            await _get_pg_settings(conn, 'superuser_reserved_connections')
        )
        for name in [
            'rds.rds_superuser_reserved_connections',
        ]:
            value = await _get_pg_settings(conn, name)
            if value:
                rv += int(value)
        return rv

    probe_connection = pgconnparams.ConnectionParams(dsn=dsn)
    conn = await pgcon.pg_connect(
        probe_connection,
        source_description="remote cluster probe",
        backend_params=pgparams.get_default_runtime_params(),
        apply_init_script=False
    )
    params = conn.connection
    addr = conn.addr

    try:
        data = json.loads(await conn.sql_fetch_val(
            b"""
            SELECT json_build_object(
                'user', current_user,
                'dbname', current_database(),
                'connlimit', (
                    select rolconnlimit
                    from pg_roles
                    where rolname = current_user
                )
            )""",
        ))
        params.update(
            user=data["user"],
            database=data["dbname"]
        )
        cluster_type, superuser_name = await _get_cluster_type(conn)
        max_connections = data["connlimit"]
        pg_max_connections = await _get_pg_settings(conn, 'max_connections')
        if max_connections == -1 or not isinstance(max_connections, int):
            max_connections = pg_max_connections
        else:
            max_connections = min(max_connections, pg_max_connections)
        capabilities = await _detect_capabilities(conn)

        if (
            specified_capabilities is not None
            and specified_capabilities.must_be_absent
        ):
            disabled = []
            for cap in specified_capabilities.must_be_absent:
                if capabilities & cap:
                    capabilities &= ~cap
                    disabled.append(cap)
            if disabled:
                logger.info(
                    f"the following backend capabilities are explicitly "
                    f"disabled by server command line: "
                    f"{', '.join(str(cap.name) for cap in disabled)}"
                )

        if t_id != buildmeta.get_default_tenant_id():
            # GOTCHA: This tenant_id check cannot protect us from running
            # multiple EdgeDB servers using the default tenant_id with
            # different catalog versions on the same backend. However, that
            # would fail during bootstrap in single-role/database mode.
            if not capabilities & pgparams.BackendCapabilities.CREATE_ROLE:
                raise ClusterError(
                    "The remote backend doesn't support CREATE ROLE; "
                    "multi-tenancy is disabled."
                )
            if not capabilities & pgparams.BackendCapabilities.CREATE_DATABASE:
                raise ClusterError(
                    "The remote backend doesn't support CREATE DATABASE; "
                    "multi-tenancy is disabled."
                )

        pg_ver_string = conn.get_server_parameter_status("server_version")
        if pg_ver_string is None:
            raise ClusterError(
                "remote server did not report its version "
                "in ParameterStatus")

        if capabilities & pgparams.BackendCapabilities.CREATE_DATABASE:
            # If we can create databases, assume we're free to create
            # extensions in them as well.
            ext_schema = "edgedbext"
            existing_exts = {}
        else:
            ext_schema = (await conn.sql_fetch_val(
                b'''
                SELECT COALESCE(
                    (SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name = 'heroku_ext'),
                    'edgedbext')
                ''',
            )).decode("utf-8")

            existing_exts_data = await conn.sql_fetch(
                b"""
                SELECT
                    extname,
                    nspname
                FROM
                    pg_extension
                    INNER JOIN pg_namespace
                        ON (pg_extension.extnamespace = pg_namespace.oid)
                """
            )

            existing_exts = {
                r[0].decode("utf-8"): r[1].decode("utf-8")
                for r in existing_exts_data
            }

        instance_params = pgparams.BackendInstanceParams(
            capabilities=capabilities,
            version=buildmeta.parse_pg_version(pg_ver_string),
            base_superuser=superuser_name,
            max_connections=int(max_connections),
            reserved_connections=await _get_reserved_connections(conn),
            tenant_id=t_id,
            ext_schema=ext_schema,
            existing_exts=existing_exts,
        )
    finally:
        conn.terminate()

    return cluster_type(
        connection_addr=addr,
        connection_params=params,
        instance_params=instance_params,
        ha_backend=ha_backend,
    )


async def _run_logged_text_subprocess(
    args: Sequence[str],
    logger: logging.Logger,
    level: int = logging.DEBUG,
    check: bool = True,
    log_stdout: bool = True,
    timeout: Optional[float] = None,
    **kwargs: Any,
) -> Tuple[List[str], List[str], int]:
    stdout_lines, stderr_lines, exit_code = await _run_logged_subprocess(
        args,
        logger=logger,
        level=level,
        check=check,
        log_stdout=log_stdout,
        timeout=timeout,
        **kwargs,
    )

    return (
        [line.decode() for line in stdout_lines],
        [line.decode() for line in stderr_lines],
        exit_code,
    )


async def _run_logged_subprocess(
    args: Sequence[str],
    logger: logging.Logger,
    level: int = logging.DEBUG,
    check: bool = True,
    log_stdout: bool = True,
    log_stderr: bool = True,
    capture_stdout: bool = True,
    capture_stderr: bool = True,
    timeout: Optional[float] = None,
    stdin: Any = asyncio.subprocess.PIPE,
    **kwargs: Any,
) -> Tuple[List[bytes], List[bytes], int]:
    process, stdout_reader, stderr_reader = await _start_logged_subprocess(
        args,
        logger=logger,
        level=level,
        log_stdout=log_stdout,
        log_stderr=log_stderr,
        capture_stdout=capture_stdout,
        capture_stderr=capture_stderr,
        stdin=stdin,
        **kwargs,
    )

    if isinstance(stdin, int) and stdin >= 0:
        os.close(stdin)

    exit_code, stdout_lines, stderr_lines = await asyncio.wait_for(
        asyncio.gather(process.wait(), stdout_reader, stderr_reader),
        timeout=timeout,
    )

    if exit_code != 0 and check:
        stderr_text = b'\n'.join(stderr_lines).decode()
        raise ClusterError(
            f'{args[0]} exited with status {exit_code}:\n'
            + textwrap.indent(stderr_text, ' ' * 4),
        )
    else:
        return stdout_lines, stderr_lines, exit_code


async def _start_logged_subprocess(
    args: Sequence[str],
    *,
    logger: logging.Logger,
    level: int = logging.DEBUG,
    override_stdout: Any = None,
    override_stderr: Any = None,
    log_stdout: bool = True,
    log_stderr: bool = True,
    capture_stdout: bool = True,
    capture_stderr: bool = True,
    stdin: Any = asyncio.subprocess.PIPE,
    log_processor: Optional[Callable[[str], Tuple[str, int]]] = None,
    **kwargs: Any,
) -> Tuple[
    asyncio.subprocess.Process,
    Coroutine[Any, Any, List[bytes]],
    Coroutine[Any, Any, List[bytes]],
]:
    logger.log(
        level,
        f'running `{" ".join(shlex.quote(arg) for arg in args)}`'
    )

    process = await asyncio.create_subprocess_exec(
        *args,
        stdin=stdin,
        stdout=(
            override_stdout
            if override_stdout
            else asyncio.subprocess.PIPE
            if log_stdout or capture_stdout
            else asyncio.subprocess.DEVNULL
        ),
        stderr=(
            override_stderr
            if override_stderr
            else asyncio.subprocess.PIPE
            if log_stderr or capture_stderr
            else asyncio.subprocess.DEVNULL
        ),
        limit=2 ** 20,  # 1 MiB
        **kwargs,
    )

    if log_stderr or capture_stderr:
        assert override_stderr is None
        assert process.stderr is not None
        stderr_reader = _capture_and_log_subprocess_output(
            process.pid,
            process.stderr,
            logger,
            level,
            log_processor,
            capture_output=capture_stderr,
            log_output=log_stderr,
        )
    else:
        stderr_reader = _dummy()

    if log_stdout or capture_stdout:
        assert override_stdout is None
        assert process.stdout is not None
        stdout_reader = _capture_and_log_subprocess_output(
            process.pid,
            process.stdout,
            logger,
            level,
            log_processor,
            capture_output=capture_stdout,
            log_output=log_stdout,
        )
    else:
        stdout_reader = _dummy()

    return process, stdout_reader, stderr_reader


async def _capture_and_log_subprocess_output(
    pid: int,
    stream: asyncio.StreamReader,
    logger: logging.Logger,
    level: int,
    log_processor: Optional[Callable[[str], Tuple[str, int]]] = None,
    *,
    capture_output: bool,
    log_output: bool,
) -> List[bytes]:
    lines = []
    while not stream.at_eof():
        line = await _safe_readline(stream)
        if line or not stream.at_eof():
            line = line.rstrip(b'\n')
            if capture_output:
                lines.append(line)
            if log_output:
                log_line = line.decode()
                if log_processor is not None:
                    log_line, level = log_processor(log_line)
                logger.log(level, log_line, extra={"process": pid})
    return lines


async def _safe_readline(stream: asyncio.StreamReader) -> bytes:
    try:
        line = await stream.readline()
    except ValueError:
        line = b"<too long>"

    return line


async def _dummy() -> List[bytes]:
    return []


postgres_to_python_level_map = {
    "DEBUG5": logging.DEBUG,
    "DEBUG4": logging.DEBUG,
    "DEBUG3": logging.DEBUG,
    "DEBUG2": logging.DEBUG,
    "DEBUG1": logging.DEBUG,
    "INFO": logging.INFO,
    "NOTICE": logging.INFO,
    "LOG": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "PANIC": logging.CRITICAL,
}

postgres_log_re = re.compile(r'^(\w+):\s*(.*)$')

postgres_specific_msg_level_map = {
    "terminating connection due to administrator command": logging.INFO,
    "the database system is shutting down": logging.INFO,
}


def postgres_log_processor(msg: str) -> Tuple[str, int]:
    if m := postgres_log_re.match(msg):
        postgres_level = m.group(1)
        msg = m.group(2)
        level = postgres_specific_msg_level_map.get(
            msg,
            postgres_to_python_level_map.get(postgres_level, logging.INFO),
        )
    else:
        level = logging.INFO

    return msg, level
