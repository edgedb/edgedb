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
from typing import Any, Optional, Mapping, Dict, TYPE_CHECKING

import asyncio
import json
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

from jwcrypto import jwk

from edb import buildmeta
from edb.common import devmode
from edb.edgeql import quote

from edb.server import args as edgedb_args
from edb.server import defines as edgedb_defines
from edb.server import pgconnparams

from . import pgcluster


if TYPE_CHECKING:
    from edb.server import pgcon


class ClusterError(Exception):
    pass


class BaseCluster:
    def __init__(
        self,
        runstate_dir: pathlib.Path,
        *,
        port: int = edgedb_defines.EDGEDB_PORT,
        env: Optional[Mapping[str, str]] = None,
        testmode: bool = False,
        log_level: Optional[str] = None,
        security: Optional[
            edgedb_args.ServerSecurityMode
        ] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode
        ] = None,
        compiler_pool_mode: Optional[
            edgedb_args.CompilerPoolMode
        ] = None,
        net_worker_mode: Optional[
            edgedb_args.NetWorkerMode
        ] = None,
    ):
        self._edgedb_cmd = [sys.executable, '-m', 'edb.server.main']

        if "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" not in os.environ:
            self._edgedb_cmd.append('--instance-name=localtest')

        self._edgedb_cmd.append('--tls-cert-mode=generate_self_signed')
        self._edgedb_cmd.append('--jose-key-mode=generate')

        if log_level:
            self._edgedb_cmd.extend(['--log-level', log_level])

        compiler_addr = os.getenv('EDGEDB_TEST_REMOTE_COMPILER')
        if compiler_addr:
            compiler_pool_mode = edgedb_args.CompilerPoolMode.Remote
            self._edgedb_cmd.extend(
                [
                    '--compiler-pool-addr',
                    compiler_addr,
                ]
            )

        if devmode.is_in_dev_mode():
            self._edgedb_cmd.append('--devmode')

        if testmode:
            self._edgedb_cmd.append('--testmode')

        if security:
            self._edgedb_cmd.extend((
                '--security',
                str(security),
            ))

        if http_endpoint_security:
            self._edgedb_cmd.extend((
                '--http-endpoint-security',
                str(http_endpoint_security),
            ))

        if compiler_pool_mode is not None:
            self._edgedb_cmd.extend((
                '--compiler-pool-mode',
                str(compiler_pool_mode),
            ))

        if net_worker_mode is not None:
            self._edgedb_cmd.extend((
                '--net-worker-mode',
                str(net_worker_mode),
            ))

        self._log_level = log_level
        self._runstate_dir = runstate_dir
        self._edgedb_cmd.extend(['--runstate-dir', str(runstate_dir)])
        self._pg_cluster: Optional[pgcluster.BaseCluster] = None
        self._pg_connect_args: pgconnparams.CreateParamsKwargs = {}
        self._daemon_process: Optional[subprocess.Popen[str]] = None
        self._port = port
        self._effective_port = None
        self._tls_cert_file = None
        self._env = env

    async def _get_pg_cluster(self) -> pgcluster.BaseCluster:
        if self._pg_cluster is None:
            self._pg_cluster = await self._new_pg_cluster()
        return self._pg_cluster

    async def _new_pg_cluster(self) -> pgcluster.BaseCluster:
        raise NotImplementedError()

    async def get_status(self) -> str:
        pg_cluster = await self._get_pg_cluster()
        pg_status = await pg_cluster.get_status()
        initially_stopped = pg_status == 'stopped'

        if initially_stopped:
            await pg_cluster.start()
        elif pg_status == 'not-initialized':
            return 'not-initialized'

        conn = None
        try:
            conn = await pg_cluster.connect(
                source_description=f"{self.__class__.__name__}.get_status",
                **self._pg_connect_args,
            )

            db_exists = await self._edgedb_template_exists(conn)
        finally:
            if conn is not None:
                conn.terminate()
            await asyncio.sleep(0)
            if initially_stopped:
                await pg_cluster.stop()

        if initially_stopped:
            return 'stopped' if db_exists else 'not-initialized,stopped'
        else:
            return 'running' if db_exists else 'not-initialized,running'

    def get_connect_args(self) -> Dict[str, Any]:
        return {
            'host': 'localhost',
            'port': self._effective_port,
            'tls_ca_file': self._tls_cert_file,
        }

    async def init(
        self,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
    ) -> None:
        cluster_status = await self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError('cluster has already been initialized')

        self._init()

    async def start(
        self,
        wait: int=60,
        *,
        port: Optional[int] = None,
        **settings: Any,
    ) -> None:
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

        env: Optional[Dict[str, str]]
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

        try:
            await self._wait_for_server(timeout=wait, status_sock=status_r)
        except Exception:
            self.stop()
            raise

    def stop(self, wait: int = 60) -> None:
        if (self._daemon_process is not None and
                self._daemon_process.returncode is None):
            self._daemon_process.terminate()
            self._daemon_process.wait(wait)

    def destroy(self) -> None:
        if self._pg_cluster is not None:
            self._pg_cluster.destroy()

    def _init(self) -> None:
        env: Optional[Dict[str, str]]
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

    async def _edgedb_template_exists(
        self,
        conn: pgcon.PGConnection,
    ) -> bool:
        return await conn.sql_fetch_val(
            b"SELECT True FROM pg_catalog.pg_database WHERE datname = $1",
            args=[edgedb_defines.EDGEDB_TEMPLATE_DB.encode("utf-8")],
        ) is not None

    async def _wait_for_server(
        self,
        timeout: float = 30.0,
        status_sock: Optional[socket.socket] = None,
    ) -> None:

        async def _read_server_status(
            stream: asyncio.StreamReader,
        ) -> Dict[str, Any]:
            while True:
                line = await stream.readline()
                if not line:
                    raise ClusterError("Gel server terminated")
                if line.startswith(b'READY='):
                    break

            _, _, dataline = line.decode().partition('=')
            try:
                return json.loads(dataline)  # type: ignore
            except Exception as e:
                raise ClusterError(
                    f"Gel server returned invalid status line: "
                    f"{dataline!r} ({e})"
                )

        async def test() -> None:
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
                    f'Gel server did not initialize '
                    f'within {timeout} seconds'
                ) from None

            self._effective_port = data['port']
            self._tls_cert_file = data['tls_cert_file']
            stat_writer.close()

        left = timeout
        if status_sock is not None:
            started = time.monotonic()
            await test()
            left -= (time.monotonic() - started)
        if res := self._admin_query(
            "SELECT ();",
            f"{max(1, int(left))}s",
            check=False,
        ):
            raise ClusterError(
                f'could not connect to edgedb-server '
                f'within {timeout} seconds (exit code = {res})') from None

    def _admin_query(
        self,
        query: str,
        wait_until_available: str = "0s",
        check: bool=True,
    ) -> int:
        args = [
            "edgedb",
            "query",
            "--unix-path",
            str(os.path.abspath(self._runstate_dir)),
            "--port",
            str(self._effective_port),
            "--admin",
            "--user",
            edgedb_defines.EDGEDB_SUPERUSER,
            "--branch",
            edgedb_defines.EDGEDB_SUPERUSER_DB,
            "--wait-until-available",
            wait_until_available,
            query,
        ]
        res = subprocess.run(
            args=args,
            check=check,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        return res.returncode

    async def set_test_config(self) -> None:
        self._admin_query(f'''
            # Set session_idle_transaction_timeout to 5 minutes.
            CONFIGURE INSTANCE SET session_idle_transaction_timeout :=
                <duration>'5 minutes';
        ''')

    async def set_superuser_password(self, password: str) -> None:
        self._admin_query(f'''
            ALTER ROLE {edgedb_defines.EDGEDB_SUPERUSER}
            SET password := {quote.quote_literal(password)}
        ''')

    async def trust_local_connections(self) -> None:
        self._admin_query('''
            CONFIGURE INSTANCE INSERT Auth {
                priority := 0,
                method := (INSERT Trust),
            }
        ''')

    def has_create_database(self) -> bool:
        return True

    def has_create_role(self) -> bool:
        return True


class Cluster(BaseCluster):
    def __init__(
        self,
        data_dir: pathlib.Path,
        *,
        pg_superuser: str = 'postgres',
        port: int = edgedb_defines.EDGEDB_PORT,
        runstate_dir: Optional[pathlib.Path] = None,
        env: Optional[Mapping[str, str]] = None,
        testmode: bool = False,
        log_level: Optional[str] = None,
        security: Optional[
            edgedb_args.ServerSecurityMode
        ] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode
        ] = None,
        compiler_pool_mode: Optional[
            edgedb_args.CompilerPoolMode
        ] = None,
    ) -> None:
        self._data_dir = data_dir
        if runstate_dir is None:
            runstate_dir = buildmeta.get_runstate_path(self._data_dir)
        super().__init__(
            runstate_dir,
            port=port,
            env=env,
            testmode=testmode,
            log_level=log_level,
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )
        self._edgedb_cmd.extend(['-D', str(self._data_dir)])
        self._pg_connect_args['user'] = pg_superuser
        self._pg_connect_args['database'] = 'template1'
        self._jws_key: Optional[jwk.JWK] = None

    async def _new_pg_cluster(self) -> pgcluster.Cluster:
        return await pgcluster.get_local_pg_cluster(
            self._data_dir,
            runstate_dir=self._runstate_dir,
            log_level=self._log_level,
        )

    def get_data_dir(self) -> pathlib.Path:
        return self._data_dir

    async def init(
        self,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
    ) -> None:
        cluster_status = await self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._data_dir))

        self._init()


class TempCluster(Cluster):
    def __init__(
        self,
        *,
        data_dir_suffix: Optional[str] = None,
        data_dir_prefix: Optional[str] = None,
        data_dir_parent: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        testmode: bool = False,
        log_level: Optional[str] = None,
        security: Optional[
            edgedb_args.ServerSecurityMode
        ] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode
        ] = None,
        compiler_pool_mode: Optional[
            edgedb_args.CompilerPoolMode
        ] = None,
    ) -> None:
        tempdir = pathlib.Path(
            tempfile.mkdtemp(
                suffix=data_dir_suffix,
                prefix=data_dir_prefix,
                dir=data_dir_parent,
            ),
        )
        super().__init__(
            data_dir=tempdir,
            runstate_dir=tempdir,
            env=env,
            testmode=testmode,
            log_level=log_level,
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )


class RunningCluster(BaseCluster):
    def __init__(self, **conn_args: Any) -> None:
        self.conn_args = conn_args

    def is_managed(self) -> bool:
        return False

    def ensure_initialized(self) -> bool:
        return False

    def get_connect_args(self) -> Dict[str, Any]:
        return dict(self.conn_args)

    async def get_status(self) -> str:
        return 'running'

    async def init(
        self,
        *,
        server_settings: Optional[Mapping[str, str]] = None,
    ) -> None:
        pass

    async def start(
        self,
        wait: int=60,
        *,
        port: Optional[int] = None,
        **settings: Any,
    ) -> None:
        pass

    def stop(self, wait: int = 60) -> None:
        pass

    def destroy(self) -> None:
        pass

    def has_create_database(self) -> bool:
        return os.environ.get('EDGEDB_TEST_CASES_SET_UP') != 'inplace'

    def has_create_role(self) -> bool:
        return os.environ.get('EDGEDB_TEST_HAS_CREATE_ROLE') == 'True'


class TempClusterWithRemotePg(BaseCluster):
    def __init__(
        self,
        backend_dsn: str,
        *,
        data_dir_suffix: Optional[str] = None,
        data_dir_prefix: Optional[str] = None,
        data_dir_parent: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        testmode: bool = False,
        log_level: Optional[str] = None,
        security: Optional[
            edgedb_args.ServerSecurityMode
        ] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode
        ] = None,
        compiler_pool_mode: Optional[
            edgedb_args.CompilerPoolMode
        ] = None,
    ) -> None:
        runstate_dir = pathlib.Path(
            tempfile.mkdtemp(
                suffix=data_dir_suffix,
                prefix=data_dir_prefix,
                dir=data_dir_parent,
            ),
        )
        self._backend_dsn = backend_dsn
        mt = "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ
        if mt:
            compiler_pool_mode = edgedb_args.CompilerPoolMode.MultiTenant

        super().__init__(
            runstate_dir,
            env=env,
            testmode=testmode,
            log_level=log_level,
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )
        if not mt:
            self._edgedb_cmd.extend(['--backend-dsn', backend_dsn])

    async def _new_pg_cluster(self) -> pgcluster.BaseCluster:
        return await pgcluster.get_remote_pg_cluster(self._backend_dsn)

    def has_create_database(self) -> bool:
        if self._pg_cluster:
            return self._pg_cluster.get_runtime_params().has_create_database
        else:
            return super().has_create_database()

    def has_create_role(self) -> bool:
        if self._pg_cluster:
            return self._pg_cluster.get_runtime_params().has_create_role
        else:
            return super().has_create_role()
