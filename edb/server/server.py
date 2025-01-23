# mypy: check-untyped-defs

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
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Iterator,
    Mapping,
    Sequence,
    TYPE_CHECKING,
)

import asyncio
import collections
import ipaddress
import itertools
import json
import logging
import os
import pathlib
import pickle
import socket
import ssl
import stat
import time
import uuid

import immutables
from jwcrypto import jwk

from edb import buildmeta
from edb import errors

from edb.common import devmode
from edb.common import lru
from edb.common import secretkey
from edb.common import windowedsum
from edb.common.log import current_tenant

from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema

from edb.server import args as srvargs
from edb.server import cache
from edb.server import config
from edb.server import compiler_pool
from edb.server import daemon
from edb.server import defines
from edb.server import instdata
from edb.server import protocol
from edb.server import net_worker
from edb.server import tenant as edbtenant
from edb.server.protocol import binary  # type: ignore
from edb.server.protocol import pg_ext  # type: ignore
from edb.server.protocol import ui_ext  # type: ignore
from edb.server.protocol.auth_ext import pkce
from edb.server import metrics
from edb.server import pgcon

from edb.pgsql import patches as pg_patches

from . import compiler as edbcompiler
from .compiler import sertypes

if TYPE_CHECKING:
    import asyncio.base_events

    from edb.pgsql import params as pgparams

    from . import bootstrap


ADMIN_PLACEHOLDER = "<edgedb:admin>"
logger = logging.getLogger('edb.server')
log_metrics = logging.getLogger('edb.server.metrics')


class StartupError(Exception):
    pass


class BaseServer:
    _sys_queries: Mapping[str, bytes]
    _local_intro_query: bytes
    _global_intro_query: bytes
    _report_config_typedesc: dict[defines.ProtocolVersion, bytes]
    _use_monitor_fs: bool
    _file_watch_handles: list[asyncio.Handle]

    _std_schema: s_schema.Schema
    _refl_schema: s_schema.Schema
    _schema_class_layout: s_refl.SchemaClassLayout

    _servers: Mapping[str, asyncio.AbstractServer]

    _testmode: bool

    # We maintain an OrderedDict of all active client connections.
    # We use an OrderedDict because it allows to move keys to either
    # end of the dict. That's used to keep all active client connections
    # grouped at the right end of the dict. The idea is that we can then
    # have a periodically run coroutine to GC all inactive connections.
    # This should be more economical than maintaining a TimerHandle for
    # every open connection. Also, this way, we can react to the
    # `session_idle_timeout` config setting changed mid-flight.
    _binary_conns: collections.OrderedDict[binary.EdgeConnection, bool]
    _pgext_conns: dict[str, pg_ext.PgConnection]
    _idle_gc_handler: asyncio.TimerHandle | None = None
    _stmt_cache_size: int | None = None

    _compiler_pool: compiler_pool.AbstractPool | None
    compilation_config_serializer: sertypes.CompilationConfigSerializer
    _http_request_logger: asyncio.Task | None
    _auth_gc: asyncio.Task | None
    _net_worker_http: asyncio.Task | None
    _net_worker_http_gc: asyncio.Task | None

    def __init__(
        self,
        *,
        runstate_dir,
        internal_runstate_dir,
        compiler_pool_size,
        compiler_pool_mode: srvargs.CompilerPoolMode,
        compiler_pool_addr,
        nethosts,
        netport,
        listen_sockets: tuple[socket.socket, ...] = (),
        testmode: bool = False,
        daemonized: bool = False,
        pidfile_dir: Optional[pathlib.Path] = None,
        binary_endpoint_security: srvargs.ServerEndpointSecurityMode = (
            srvargs.ServerEndpointSecurityMode.Tls),
        http_endpoint_security: srvargs.ServerEndpointSecurityMode = (
            srvargs.ServerEndpointSecurityMode.Tls),
        auto_shutdown_after: float = -1,
        echo_runtime_info: bool = False,
        status_sinks: Sequence[Callable[[str], None]] = (),
        default_auth_method: srvargs.ServerAuthMethods = (
            srvargs.DEFAULT_AUTH_METHODS),
        admin_ui: bool = False,
        disable_dynamic_system_config: bool = False,
        compiler_state: edbcompiler.CompilerState,
        use_monitor_fs: bool = False,
        net_worker_mode: srvargs.NetWorkerMode = srvargs.NetWorkerMode.Default,
    ):
        self.__loop = asyncio.get_running_loop()
        self._use_monitor_fs = use_monitor_fs

        self._schema_class_layout = compiler_state.schema_class_layout
        self._config_settings = compiler_state.config_spec
        self._refl_schema = compiler_state.refl_schema
        self._std_schema = compiler_state.std_schema
        assert compiler_state.global_intro_query is not None
        self._global_intro_query = (
            compiler_state.global_intro_query.encode("utf-8"))
        assert compiler_state.local_intro_query is not None
        self._local_intro_query = (
            compiler_state.local_intro_query.encode("utf-8"))

        # Used to tag PG notifications to later disambiguate them.
        self._server_id = str(uuid.uuid4())

        self._daemonized = daemonized
        self._pidfile_dir = pidfile_dir
        self._runstate_dir = runstate_dir
        self._internal_runstate_dir = internal_runstate_dir
        self._compiler_pool = None
        self._compiler_pool_size = compiler_pool_size
        self._compiler_pool_mode = compiler_pool_mode
        self._compiler_pool_addr = compiler_pool_addr
        self._system_compile_cache = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE
        )
        self._system_compile_cache_locks: dict[Any, Any] = {}

        self._listen_sockets = listen_sockets
        if listen_sockets:
            nethosts = tuple(s.getsockname()[0] for s in listen_sockets)
            netport = listen_sockets[0].getsockname()[1]

        self._listen_hosts = nethosts
        self._listen_port = netport

        # Shutdown the server after the last management
        # connection has disconnected
        # and there have been no new connections for n seconds
        self._auto_shutdown_after = auto_shutdown_after
        self._auto_shutdown_handler: Any = None

        self._echo_runtime_info = echo_runtime_info
        self._status_sinks = status_sinks

        self._sys_queries = immutables.Map()

        self._devmode = devmode.is_in_dev_mode()
        self._testmode = testmode

        self._binary_proto_id_counter = 0
        self._binary_conns = collections.OrderedDict()
        self._pgext_conns = {}

        self._servers = {}

        self._http_query_cache = cache.StatementsCache(
            maxsize=defines.HTTP_PORT_QUERY_CACHE_SIZE)

        self._http_last_minute_requests = windowedsum.WindowedSum()
        self._http_request_logger = None
        self._auth_gc = None
        self._net_worker_http = None
        self._net_worker_http_gc = None
        self._net_worker_mode = net_worker_mode

        self._stop_evt = asyncio.Event()
        self._tls_cert_file: str | Any = None
        self._tls_cert_newly_generated = False
        self._sslctx: ssl.SSLContext | Any = None
        self._sslctx_pgext: ssl.SSLContext | Any = None

        self._jws_key: jwk.JWK | None = None
        self._jws_keys_newly_generated = False

        self._default_auth_method_spec = default_auth_method
        self._default_auth_methods = self._get_auth_method_types(
            default_auth_method)
        self._binary_endpoint_security = binary_endpoint_security
        self._http_endpoint_security = http_endpoint_security

        self._idle_gc_handler = None

        self._admin_ui = admin_ui

        self._file_watch_handles = []
        self._tls_certs_reload_retry_handle: Any | asyncio.TimerHandle = None

        self._disable_dynamic_system_config = disable_dynamic_system_config
        self._report_config_typedesc = {}

    def _get_auth_method_types(
        self,
        auth_methods_spec: srvargs.ServerAuthMethods,
    ) -> dict[srvargs.ServerConnTransport, list[config.CompositeConfigType]]:
        mapping = {}
        for transport, methods in auth_methods_spec.items():
            result = []
            for method in methods:
                auth_type = self.config_settings.get_type_by_name(
                    f'cfg::{method.value}'
                )
                result.append(auth_type())
            mapping[transport] = result

        return mapping

    async def _request_stats_logger(self):
        last_seen = -1
        while True:
            current = int(self._http_last_minute_requests)
            if current != last_seen:
                log_metrics.info(
                    "HTTP requests in last minute: %d",
                    current,
                )
                last_seen = current

            await asyncio.sleep(30)

    def get_server_id(self):
        return self._server_id

    def get_listen_hosts(self):
        return self._listen_hosts

    def get_listen_port(self):
        return self._listen_port

    def get_loop(self):
        return self.__loop

    def in_dev_mode(self):
        return self._devmode

    def in_test_mode(self):
        return self._testmode

    def is_admin_ui_enabled(self):
        return self._admin_ui

    def on_binary_client_created(self) -> str:
        self._binary_proto_id_counter += 1

        if self._auto_shutdown_handler:
            self._auto_shutdown_handler.cancel()
            self._auto_shutdown_handler = None

        return str(self._binary_proto_id_counter)

    def on_binary_client_connected(self, conn):
        self._binary_conns[conn] = True
        metrics.current_client_connections.inc(
            1.0, conn.get_tenant_label()
        )

    def on_binary_client_authed(self, conn):
        self._report_connections(event='opened')
        metrics.total_client_connections.inc(
            1.0, conn.get_tenant_label()
        )

    def on_binary_client_after_idling(self, conn):
        try:
            self._binary_conns.move_to_end(conn, last=True)
        except KeyError:
            # Shouldn't happen, but just in case some weird async twist
            # gets us here we don't want to crash the connection with
            # this error.
            metrics.background_errors.inc(
                1.0, conn.get_tenant_label(), 'client_after_idling'
            )

    def on_binary_client_disconnected(self, conn):
        self._binary_conns.pop(conn, None)
        self._report_connections(event="closed")
        metrics.current_client_connections.dec(
            1.0, conn.get_tenant_label()
        )
        self.maybe_auto_shutdown()

    def maybe_auto_shutdown(self):
        if (
            not self._binary_conns
            and self._auto_shutdown_after >= 0
            and self._auto_shutdown_handler is None
        ):
            self._auto_shutdown_handler = self.__loop.call_later(
                self._auto_shutdown_after, self.request_auto_shutdown)

    def _report_connections(self, *, event: str) -> None:
        log_metrics.info(
            "%s a connection; open_count=%d",
            event,
            len(self._binary_conns),
        )

    def on_pgext_client_connected(self, conn):
        self._pgext_conns[conn.get_id()] = conn

    def on_pgext_client_disconnected(self, conn):
        self._pgext_conns.pop(conn.get_id(), None)
        self.maybe_auto_shutdown()

    def cancel_pgext_connection(self, pid, secret):
        conn = self._pgext_conns.get(pid)
        if conn is not None:
            conn.cancel(secret)

    def monitor_fs(
        self,
        file_path: str | pathlib.Path,
        cb: Callable[[], None],
    ) -> Callable[[], None]:
        if not self._use_monitor_fs:
            return lambda: None

        if isinstance(file_path, str):
            path = pathlib.Path(file_path)
            path_str = file_path
        else:
            path = file_path
            path_str = str(file_path)
        handle = None
        parent_dir = path.parent

        def watch_dir(file_modified, _event):
            nonlocal handle
            if parent_dir / os.fsdecode(file_modified) == path:
                try:
                    new_handle = self.__loop._monitor_fs(  # type: ignore
                        path_str, callback)
                except FileNotFoundError:
                    pass
                else:
                    finalizer()
                    handle = new_handle
                    self._file_watch_handles.append(handle)
                    cb()

        def callback(_file_modified, _event):
            nonlocal handle
            # First, cancel the existing watcher and call cb() regardless of
            # what event it is. This is because macOS issues RENAME while Linux
            # issues CHANGE, and we don't have enough knowledge about renaming.
            # The idea here is to re-watch the file path after every event, so
            # that even if the file is recreated, we still watch the right one.
            finalizer()
            try:
                cb()
            finally:
                try:
                    # Then, see if we can directly re-watch the target path
                    handle = self.__loop._monitor_fs(  # type: ignore
                        path_str, callback)
                except FileNotFoundError:
                    # If not, watch the parent directory to wait for recreation
                    handle = self.__loop._monitor_fs(  # type: ignore
                        str(parent_dir), watch_dir)
                self._file_watch_handles.append(handle)

        # ... we depend on an event loop internal _monitor_fs
        handle = self.__loop._monitor_fs(path_str, callback)  # type: ignore

        def finalizer():
            try:
                self._file_watch_handles.remove(handle)
            except ValueError:
                # The server may have cleared _file_watch_handles before the
                # tenants do, so we can skip the double cancel here.
                pass
            else:
                handle.cancel()

        self._file_watch_handles.append(handle)

        return finalizer

    def _get_sys_config(self) -> Mapping[str, config.SettingValue]:
        raise NotImplementedError

    def config_lookup(
        self,
        name: str,
        *configs: Mapping[str, config.SettingValue],
    ) -> Any:
        return config.lookup(name, *configs, spec=self._config_settings)

    @property
    def config_settings(self) -> config.Spec:
        return self._config_settings

    async def init(self):
        if self.is_admin_ui_enabled():
            ui_ext.cache_assets()

        sys_config = self._get_sys_config()
        if not self._listen_hosts:
            self._listen_hosts = (
                self.config_lookup('listen_addresses', sys_config)
                or ('localhost',)
            )

        if self._listen_port is None:
            self._listen_port = (
                self.config_lookup('listen_port', sys_config)
                or defines.EDGEDB_PORT
            )

        self._stmt_cache_size = self.config_lookup(
            '_pg_prepared_statement_cache_size', sys_config
        )

        self.reinit_idle_gc_collector()

    def reinit_idle_gc_collector(self) -> float:
        if self._auto_shutdown_after >= 0:
            return -1

        if self._idle_gc_handler is not None:
            self._idle_gc_handler.cancel()
            self._idle_gc_handler = None

        session_idle_timeout = self.config_lookup(
            'session_idle_timeout', self._get_sys_config())

        timeout = session_idle_timeout.to_microseconds()
        timeout /= 1_000_000.0  # convert to seconds

        if timeout > 0:
            self._idle_gc_handler = self.__loop.call_later(
                timeout, self._idle_gc_collector)

        return timeout

    @property
    def stmt_cache_size(self) -> int | None:
        return self._stmt_cache_size

    @property
    def system_compile_cache(self):
        return self._system_compile_cache

    def request_stop_fe_conns(self, dbname: str) -> None:
        for conn in itertools.chain(
            self._binary_conns.keys(), self._pgext_conns.values()
        ):
            if conn.dbname == dbname:
                conn.request_stop()

    @property
    def system_compile_cache_locks(self):
        return self._system_compile_cache_locks

    def _idle_gc_collector(self):
        try:
            self._idle_gc_handler = None
            idle_timeout = self.reinit_idle_gc_collector()

            if idle_timeout <= 0:
                return

            now = time.monotonic()
            expiry_time = now - idle_timeout
            for conn in self._binary_conns:
                try:
                    if conn.is_idle(expiry_time):
                        label = conn.get_tenant_label()
                        metrics.idle_client_connections.inc(1.0, label)
                        current_tenant.set(label)
                        conn.close_for_idling()
                    elif conn.is_alive():
                        # We are sorting connections in
                        # 'on_binary_client_after_idling' to specifically
                        # enable this optimization. As soon as we find first
                        # non-idle active connection we're guaranteed
                        # to have traversed all of the potentially idling
                        # connections.
                        break
                except Exception:
                    metrics.background_errors.inc(
                        1.0, conn.get_tenant_label(), 'close_for_idling'
                    )
                    conn.abort()
        except Exception:
            metrics.background_errors.inc(
                1.0, 'system', 'idle_clients_collector'
            )
            raise

    def _get_backend_runtime_params(self) -> pgparams.BackendRuntimeParams:
        raise NotImplementedError

    def _get_compiler_args(self) -> dict[str, Any]:
        # Force Postgres version in BackendRuntimeParams to be the
        # minimal supported, because the compiler does not rely on
        # the version, and not pinning it would make the remote compiler
        # pool refuse connections from clients that have differing versions
        # of Postgres backing them.
        runtime_params = self._get_backend_runtime_params()
        min_ver = '.'.join(str(v) for v in defines.MIN_POSTGRES_VERSION)
        runtime_params = runtime_params._replace(
            instance_params=runtime_params.instance_params._replace(
                version=buildmeta.parse_pg_version(min_ver),
            ),
        )

        args = dict(
            pool_size=self._compiler_pool_size,
            pool_class=self._compiler_pool_mode.pool_class,
            runstate_dir=self._internal_runstate_dir,
            backend_runtime_params=runtime_params,
            std_schema=self._std_schema,
            refl_schema=self._refl_schema,
            schema_class_layout=self._schema_class_layout,
        )
        if self._compiler_pool_mode == srvargs.CompilerPoolMode.Remote:
            args['address'] = self._compiler_pool_addr
        return args

    async def _destroy_compiler_pool(self):
        if self._compiler_pool is not None:
            await self._compiler_pool.stop()
            self._compiler_pool = None

    def get_compiler_pool(self):
        return self._compiler_pool

    async def introspect_global_schema_json(
        self, conn: pgcon.PGConnection
    ) -> bytes:
        return await conn.sql_fetch_val(self._global_intro_query)

    def _parse_global_schema(
        self, json_data: Any
    ) -> s_schema.Schema:
        return s_refl.parse_into(
            base_schema=self._std_schema,
            schema=s_schema.EMPTY_SCHEMA,
            data=json_data,
            schema_class_layout=self._schema_class_layout,
        )

    async def introspect_global_schema(
        self, conn: pgcon.PGConnection
    ) -> s_schema.Schema:
        json_data = await self.introspect_global_schema_json(conn)
        return self._parse_global_schema(json_data)

    async def introspect_user_schema_json(
        self,
        conn: pgcon.PGConnection,
    ) -> bytes:
        return await conn.sql_fetch_val(self._local_intro_query)

    def _parse_user_schema(
        self,
        json_data: Any,
        global_schema: s_schema.Schema,
    ) -> s_schema.Schema:
        base_schema = s_schema.ChainedSchema(
            self._std_schema,
            s_schema.EMPTY_SCHEMA,
            global_schema,
        )

        return s_refl.parse_into(
            base_schema=base_schema,
            schema=s_schema.EMPTY_SCHEMA,
            data=json_data,
            schema_class_layout=self._schema_class_layout,
        )

    async def _introspect_user_schema(
        self,
        conn: pgcon.PGConnection,
        global_schema: s_schema.Schema,
    ) -> s_schema.Schema:
        json_data = await self.introspect_user_schema_json(conn)
        return self._parse_user_schema(json_data, global_schema)

    async def introspect_db_config(self, conn: pgcon.PGConnection) -> bytes:
        return await conn.sql_fetch_val(self.get_sys_query("dbconfig"))

    def _parse_db_config(
        self, db_config_json: bytes, user_schema: s_schema.Schema
    ) -> Mapping[str, config.SettingValue]:
        spec = config.ChainedSpec(
            self._config_settings,
            config.load_ext_spec_from_schema(
                user_schema,
                self.get_std_schema(),
            ),
        )

        return config.from_json(spec, db_config_json)

    async def get_dbnames(self, syscon):
        dbs_query = self.get_sys_query('listdbs')
        json_data = await syscon.sql_fetch_val(dbs_query)
        return json.loads(json_data)

    async def get_patch_count(self, conn: pgcon.PGConnection) -> int:
        """Get the number of applied patches."""
        num_patches = await instdata.get_instdata(
            conn, 'num_patches', 'json')
        res: int = json.loads(num_patches) if num_patches else 0
        return res

    async def _on_system_config_add(self, setting_name, value):
        # CONFIGURE INSTANCE INSERT ConfigObject;
        pass

    async def _on_system_config_rem(self, setting_name, value):
        # CONFIGURE INSTANCE RESET ConfigObject;
        pass

    async def _on_system_config_set(self, setting_name, value):
        # CONFIGURE INSTANCE SET setting_name := value;
        pass

    async def _on_system_config_reset(self, setting_name):
        # CONFIGURE INSTANCE RESET setting_name;
        pass

    def before_alter_system_config(self):
        if self._disable_dynamic_system_config:
            raise errors.ConfigurationError(
                "cannot change this configuration value in this instance"
            )

    async def _after_system_config_add(self, setting_name, value):
        # CONFIGURE INSTANCE INSERT ConfigObject;
        pass

    async def _after_system_config_rem(self, setting_name, value):
        # CONFIGURE INSTANCE RESET ConfigObject;
        pass

    async def _after_system_config_set(self, setting_name, value):
        # CONFIGURE INSTANCE SET setting_name := value;
        pass

    async def _after_system_config_reset(self, setting_name):
        # CONFIGURE INSTANCE RESET setting_name;
        pass

    async def _start_server(
        self,
        host: str,
        port: int,
        sock: Optional[socket.socket] = None,
    ) -> Optional[asyncio.base_events.Server]:
        proto_factory = lambda: protocol.HttpProtocol(
            self,
            self._sslctx,
            self._sslctx_pgext,
            binary_endpoint_security=self._binary_endpoint_security,
            http_endpoint_security=self._http_endpoint_security,
        )

        try:
            kwargs: dict[str, Any]
            if sock is not None:
                kwargs = {"sock": sock}
            else:
                kwargs = {"host": host, "port": port}
            return await self.__loop.create_server(proto_factory, **kwargs)
        except Exception as e:
            logger.warning(
                f"could not create listen socket for '{host}:{port}': {e}"
            )
            return None

    async def _start_admin_server(
        self,
        port: int,
    ) -> asyncio.base_events.Server:
        admin_unix_sock_path = os.path.join(
            self._runstate_dir, f'.s.GEL.admin.{port}')
        symlink = os.path.join(
            self._runstate_dir, f'.s.EDGEDB.admin.{port}')

        exists = False
        try:
            mode = os.lstat(symlink).st_mode
            if stat.S_ISSOCK(mode):
                os.unlink(symlink)
            else:
                exists = True
        except FileNotFoundError:
            pass
        if not exists:
            os.symlink(admin_unix_sock_path, symlink)

        assert len(admin_unix_sock_path) <= (
            defines.MAX_RUNSTATE_DIR_PATH
            + defines.MAX_UNIX_SOCKET_PATH_LENGTH
            + 1
        ), "admin Unix socket length exceeds maximum allowed"
        admin_unix_srv = await self.__loop.create_unix_server(
            lambda: binary.new_edge_connection(
                self, self._get_admin_tenant(), external_auth=True
            ),
            admin_unix_sock_path
        )
        os.chmod(admin_unix_sock_path, stat.S_IRUSR | stat.S_IWUSR)
        logger.info('Serving admin on %s', admin_unix_sock_path)
        return admin_unix_srv

    def _get_admin_tenant(self) -> edbtenant.Tenant:
        return self.get_default_tenant()

    async def _start_servers(
        self,
        hosts: tuple[str, ...],
        port: int,
        *,
        admin: bool = True,
        sockets: tuple[socket.socket, ...] = (),
    ):
        servers = {}
        if port == 0:
            # Automatic port selection requires us to start servers
            # sequentially until we get a working bound socket to ensure
            # consistent port value across all requested listen addresses.
            try:
                for host in hosts:
                    server = await self._start_server(host, port)
                    if server is not None:
                        if port == 0:
                            port = server.sockets[0].getsockname()[1]
                        servers[host] = server
            except Exception:
                await self._stop_servers(servers.values())
                raise
        else:
            start_tasks = {}
            try:
                async with asyncio.TaskGroup() as g:
                    if sockets:
                        for host, sock in zip(hosts, sockets):
                            start_tasks[host] = g.create_task(
                                self._start_server(host, port, sock=sock)
                            )
                    else:
                        for host in hosts:
                            start_tasks[host] = g.create_task(
                                self._start_server(host, port)
                            )
            except Exception:
                await self._stop_servers([
                    fut.result() for fut in start_tasks.values()
                    if (
                        fut.done()
                        and fut.exception() is None
                        and fut.result() is not None
                    )
                ])
                raise

            servers.update({
                host: srv
                for host, fut in start_tasks.items()
                if (srv := fut.result()) is not None
            })

        # Fail if none of the servers can be started, except when the admin
        # server on a UNIX domain socket will be started.
        if not servers and (not admin or port == 0):
            raise StartupError("could not create any listen sockets")

        addrs = []
        for tcp_srv in servers.values():
            for s in tcp_srv.sockets:
                addrs.append(s.getsockname())

        if len(addrs) > 1:
            if port:
                addr_str = f"{{{', '.join(addr[0] for addr in addrs)}}}:{port}"
            else:
                addr_str = f"""{{{', '.join(
                    f'{addr[0]}:{addr[1]}' for addr in addrs)}}}"""
        elif addrs:
            addr_str = f'{addrs[0][0]}:{addrs[0][1]}'
            port = addrs[0][1]
        else:
            addr_str = None

        if addr_str:
            logger.info('Serving on %s', addr_str)

        if admin and port:
            try:
                admin_unix_srv = await self._start_admin_server(port)
            except Exception:
                await self._stop_servers(servers.values())
                raise
            servers[ADMIN_PLACEHOLDER] = admin_unix_srv

        return servers, port, addrs

    def _sni_callback(self, sslobj, server_name, sslctx):
        # Match the given SNI for a pre-registered Tenant instance,
        # and temporarily store in memory indexed by sslobj for future
        # retrieval, see also retrieve_tenant() below.
        #
        # Used in multi-tenant server only. This method must not fail.
        pass

    def reload_tls(self, tls_cert_file, tls_key_file, client_ca_file):
        logger.info("loading TLS certificates")
        tls_password_needed = False
        if self._tls_certs_reload_retry_handle is not None:
            self._tls_certs_reload_retry_handle.cancel()
            self._tls_certs_reload_retry_handle = None

        def _tls_private_key_password():
            nonlocal tls_password_needed
            tls_password_needed = True
            return (
                os.environ.get('GEL_SERVER_TLS_PRIVATE_KEY_PASSWORD', '')
                or os.environ.get('EDGEDB_SERVER_TLS_PRIVATE_KEY_PASSWORD', '')
            )

        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sslctx_pgext = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            sslctx.load_cert_chain(
                tls_cert_file,
                tls_key_file,
                password=_tls_private_key_password,
            )
            sslctx_pgext.load_cert_chain(
                tls_cert_file,
                tls_key_file,
                password=_tls_private_key_password,
            )
        except ssl.SSLError as e:
            if e.library == "SSL" and e.errno == 9:  # ERR_LIB_PEM
                if tls_password_needed:
                    if _tls_private_key_password():
                        raise StartupError(
                            "Cannot load TLS certificates - it's likely that "
                            "the private key password is wrong."
                        ) from e
                    else:
                        raise StartupError(
                            "Cannot load TLS certificates - the private key "
                            "file is likely protected by a password. Specify "
                            "the password using environment variable: "
                            "GEL_SERVER_TLS_PRIVATE_KEY_PASSWORD"
                        ) from e
                elif tls_key_file is None:
                    raise StartupError(
                        "Cannot load TLS certificates - have you specified "
                        "the private key file using the `--tls-key-file` "
                        "command-line argument?"
                    ) from e
                else:
                    raise StartupError(
                        "Cannot load TLS certificates - please double check "
                        "if the specified certificate files are valid."
                    )
            elif e.library == "X509" and e.errno == 116:
                # X509 Error 116: X509_R_KEY_VALUES_MISMATCH
                raise StartupError(
                    "Cannot load TLS certificates - the private key doesn't "
                    "match the certificate."
                )

            raise StartupError(f"Cannot load TLS certificates - {e}") from e

        if client_ca_file is not None:
            try:
                sslctx.load_verify_locations(client_ca_file)
                sslctx_pgext.load_verify_locations(client_ca_file)
            except ssl.SSLError as e:
                raise StartupError(
                    f"Cannot load client CA certificates - {e}") from e
            sslctx.verify_mode = ssl.CERT_OPTIONAL
            sslctx_pgext.verify_mode = ssl.CERT_OPTIONAL

        sslctx.set_alpn_protocols(['edgedb-binary', 'http/1.1'])
        sslctx.sni_callback = self._sni_callback
        sslctx_pgext.sni_callback = self._sni_callback
        self._sslctx = sslctx
        self._sslctx_pgext = sslctx_pgext

    def init_tls(
        self,
        tls_cert_file,
        tls_key_file,
        tls_cert_newly_generated,
        client_ca_file,
    ):
        assert self._sslctx is self._sslctx_pgext is None
        self.reload_tls(tls_cert_file, tls_key_file, client_ca_file)

        self._tls_cert_file = str(tls_cert_file)
        self._tls_cert_newly_generated = tls_cert_newly_generated

        def reload_tls(retry=0):
            try:
                self.reload_tls(tls_cert_file, tls_key_file, client_ca_file)
            except (StartupError, FileNotFoundError) as e:
                if retry > defines._TLS_CERT_RELOAD_MAX_RETRIES:
                    logger.critical(str(e))
                    self.request_shutdown()
                else:
                    delay = defines._TLS_CERT_RELOAD_EXP_INTERVAL * 2 ** retry
                    logger.warning("%s; retrying in %.1f seconds.", e, delay)
                    self._tls_certs_reload_retry_handle = (
                        self.__loop.call_later(
                            delay,
                            reload_tls,
                            retry + 1,
                        )
                    )
            except Exception:
                logger.critical(
                    "error while reloading TLS certificate and/or key, "
                    "shutting down.",
                    exc_info=True,
                )
                self.request_shutdown()

        self.monitor_fs(tls_cert_file, reload_tls)
        if tls_cert_file != tls_key_file:
            self.monitor_fs(tls_key_file, reload_tls)
        if client_ca_file is not None:
            self.monitor_fs(client_ca_file, reload_tls)

    def start_watching_files(self):
        # TODO(fantix): include the monitor_fs() lines above
        pass

    def load_jwcrypto(self, jws_key_file: pathlib.Path) -> None:
        try:
            self._jws_key = secretkey.load_secret_key(jws_key_file)
        except secretkey.SecretKeyReadError as e:
            raise StartupError(e.args[0]) from e

    def init_jwcrypto(
        self,
        jws_key_file: pathlib.Path,
        jws_keys_newly_generated: bool,
    ) -> None:
        self.load_jwcrypto(jws_key_file)
        self._jws_keys_newly_generated = jws_keys_newly_generated

    def get_jws_key(self) -> jwk.JWK | None:
        return self._jws_key

    async def _stop_servers(self, servers):
        async with asyncio.TaskGroup() as g:
            for srv in servers:
                srv.close()
                g.create_task(srv.wait_closed())

    async def _before_start_servers(self) -> None:
        pass

    async def _after_start_servers(self) -> None:
        pass

    async def start(self):
        self._stop_evt.clear()

        self._http_request_logger = self.__loop.create_task(
            self._request_stats_logger()
        )

        self._compiler_pool = await compiler_pool.create_compiler_pool(
            **self._get_compiler_args()
        )
        self.compilation_config_serializer = (
            await self._compiler_pool.make_compilation_config_serializer()
        )

        await self._before_start_servers()
        self._servers, actual_port, listen_addrs = await self._start_servers(
            tuple((await _resolve_interfaces(self._listen_hosts))[0]),
            self._listen_port,
            sockets=self._listen_sockets,
        )
        self._listen_hosts = [addr[0] for addr in listen_addrs]
        self._listen_port = actual_port

        if self._daemonized:
            pidfile_dir = self._pidfile_dir
            if pidfile_dir is None:
                pidfile_dir = self._runstate_dir
            pidfile_path = pidfile_dir / f".s.EDGEDB.{actual_port}.lock"
            pidfile = daemon.PidFile(pidfile_path)
            pidfile.acquire()

        await self._after_start_servers()
        self._auth_gc = self.__loop.create_task(pkce.gc(self))
        if self._net_worker_mode is srvargs.NetWorkerMode.Default:
            self._net_worker_http = self.__loop.create_task(
                net_worker.http(self)
            )
            self._net_worker_http_gc = self.__loop.create_task(
                net_worker.gc(self)
            )

        if self._echo_runtime_info:
            ri = {
                "port": self._listen_port,
                "runstate_dir": str(self._runstate_dir),
                "tls_cert_file": self._tls_cert_file,
            }
            print(f'\nEDGEDB_SERVER_DATA:{json.dumps(ri)}\n', flush=True)

        status = self._get_status()
        status["listen_addrs"] = listen_addrs
        status_str = f'READY={json.dumps(status)}'
        for status_sink in self._status_sinks:
            status_sink(status_str)

        if self._auto_shutdown_after > 0:
            self._auto_shutdown_handler = self.__loop.call_later(
                self._auto_shutdown_after, self.request_auto_shutdown)

    def _get_status(self) -> dict[str, Any]:
        return {
            "port": self._listen_port,
            "socket_dir": str(self._runstate_dir),
            "main_pid": os.getpid(),
            "tls_cert_file": self._tls_cert_file,
            "tls_cert_newly_generated": self._tls_cert_newly_generated,
            "jws_keys_newly_generated": self._jws_keys_newly_generated,
        }

    def request_auto_shutdown(self):
        if self._auto_shutdown_after == 0:
            logger.info("shutting down server: all clients disconnected")
        else:
            logger.info(
                f"shutting down server: no clients connected in last"
                f" {self._auto_shutdown_after} seconds"
            )
        self.request_shutdown()

    def request_shutdown(self):
        self._stop_evt.set()

    async def stop(self):
        if self._idle_gc_handler is not None:
            self._idle_gc_handler.cancel()
            self._idle_gc_handler = None

        if self._http_request_logger is not None:
            self._http_request_logger.cancel()
        if self._auth_gc is not None:
            self._auth_gc.cancel()
        if self._net_worker_http is not None:
            self._net_worker_http.cancel()
        if self._net_worker_http_gc is not None:
            self._net_worker_http_gc.cancel()

        for handle in self._file_watch_handles:
            handle.cancel()
        self._file_watch_handles.clear()

        await self._stop_servers(self._servers.values())
        self._servers = {}

        # This should be done by tenant.stop(), but let's still do it again
        for conn in self._binary_conns:
            conn.request_stop()
        self._binary_conns.clear()

        for conn in self._pgext_conns.values():
            conn.request_stop()
        self._pgext_conns.clear()

    def request_frontend_stop(self, tenant: edbtenant.Tenant):
        dropped = []
        for conn in self._binary_conns:
            if conn.tenant is tenant:
                conn.request_stop()
                dropped.append(conn)
        for conn in dropped:
            self._binary_conns.pop(conn, None)

        dropped.clear()
        for conn in self._pgext_conns.values():
            if conn.tenant is tenant:
                conn.request_stop()
                dropped.append(conn)
        for conn in dropped:
            self._pgext_conns.pop(conn, None)

    async def serve_forever(self):
        await self._stop_evt.wait()

    def get_sys_query(self, key):
        return self._sys_queries[key]

    def get_debug_info(self):
        """Used to render the /server-info endpoint in dev/test modes.

        Some tests depend on the exact layout of the returned structure.
        """

        return dict(
            params=dict(
                dev_mode=self._devmode,
                test_mode=self._testmode,
                default_auth_methods=str(self._default_auth_method_spec),
                listen_hosts=self._listen_hosts,
                listen_port=self._listen_port,
            ),
            instance_config=config.debug_serialize_config(
                self._get_sys_config()),
            compiler_pool=(
                self._compiler_pool.get_debug_info()
                if self._compiler_pool
                else None
            ),
        )

    def get_report_config_typedesc(
        self,
    ) -> dict[defines.ProtocolVersion, bytes]:
        return self._report_config_typedesc

    def get_default_auth_methods(
        self, transport: srvargs.ServerConnTransport
    ) -> list[config.CompositeConfigType]:
        return self._default_auth_methods.get(transport, [])

    def get_std_schema(self) -> s_schema.Schema:
        return self._std_schema

    def retrieve_tenant(self, sslobj) -> edbtenant.Tenant | None:
        # After TLS handshake, the client connection would use this method to
        # retrieve the Tenant instance associated with the given SSLObject.
        #
        # This method must not fail. See also _sni_callback() above.
        return self.get_default_tenant()

    def get_default_tenant(self) -> edbtenant.Tenant:
        # The client connection must proceed on a Tenant instance. In cases:
        #   1. plain-text connection without TLS handshake
        #   2. TLS handshake didn't provide SNI
        #   3. SNI didn't match any Tenant (retrieve_tenant() returned None)
        # this method will be called for a "default" tenant to use.
        #
        # The caller must be ready to handle errors raised in this method, and
        # provide a decent error.
        raise NotImplementedError

    def iter_tenants(self) -> Iterator[edbtenant.Tenant]:
        raise NotImplementedError

    async def maybe_generate_pki(
        self, args: srvargs.ServerConfig, ss: BaseServer
    ) -> tuple[bool, bool]:
        tls_cert_newly_generated = False
        if args.tls_cert_mode is srvargs.ServerTlsCertMode.SelfSigned:
            assert args.tls_cert_file is not None
            if not args.tls_cert_file.exists():
                assert args.tls_key_file is not None
                logger.info(
                    f'generating self-signed TLS certificate '
                    f'in "{args.tls_cert_file}"'
                )
                secretkey.generate_tls_cert(
                    args.tls_cert_file,
                    args.tls_key_file,
                    ss.get_listen_hosts(),
                )
                tls_cert_newly_generated = True
        jws_keys_newly_generated = False
        if args.jose_key_mode is srvargs.JOSEKeyMode.Generate:
            assert args.jws_key_file is not None
            if not args.jws_key_file.exists():
                logger.info(
                    f'generating JOSE key pair in "{args.jws_key_file}"'
                )
                secretkey.generate_jwk(args.jws_key_file)
                jws_keys_newly_generated = True
        return tls_cert_newly_generated, jws_keys_newly_generated


class Server(BaseServer):
    _tenant: edbtenant.Tenant
    _startup_script: srvargs.StartupScript | None
    _new_instance: bool

    def __init__(
        self,
        *,
        tenant: edbtenant.Tenant,
        startup_script: srvargs.StartupScript | None = None,
        new_instance: bool,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._tenant = tenant
        self._startup_script = startup_script
        self._new_instance = new_instance

        tenant.set_server(self)

    def _get_sys_config(self) -> Mapping[str, config.SettingValue]:
        return self._tenant.get_sys_config()

    async def init(self) -> None:
        logger.debug("starting server init")
        await self._tenant.init_sys_pgcon()
        await self._load_instance_data()
        await self._maybe_patch()
        await self._tenant.init()
        await super().init()

    def get_default_tenant(self) -> edbtenant.Tenant:
        return self._tenant

    def iter_tenants(self) -> Iterator[edbtenant.Tenant]:
        yield self._tenant

    async def _get_patch_log(
        self, conn: pgcon.PGConnection, idx: int
    ) -> Optional[bootstrap.PatchEntry]:
        # We need to maintain a log in the system database of
        # patches that have been applied. This is so that if a
        # patch creates a new object, and then we succesfully
        # apply the patch to a user db but crash *before* applying
        # it to the system db, when we start up again and try
        # applying it to the system db, it is important that we
        # apply the same compiled version of the patch. If we
        # instead recompiled it, and it created new objects, those
        # objects might have a different id in the std schema and
        # in the actual user db.
        result = await instdata.get_instdata(
            conn, f'patch_log_{idx}', 'bin')
        if result:
            return pickle.loads(result)
        else:
            return None

    async def _prepare_patches(
        self, conn: pgcon.PGConnection
    ) -> dict[int, bootstrap.PatchEntry]:
        """Prepare all the patches"""
        num_patches = await self.get_patch_count(conn)

        if num_patches < len(pg_patches.PATCHES):
            logger.info("preparing patches for database upgrade")

        patches = {}
        patch_list = list(enumerate(pg_patches.PATCHES))
        for num, (kind, patch) in patch_list[num_patches:]:
            from . import bootstrap  # noqa: F402

            idx = num_patches + num
            if not (entry := await self._get_patch_log(conn, idx)):
                patch_info = await bootstrap.gather_patch_info(
                    num, kind, patch, conn
                )

                entry = bootstrap.prepare_patch(
                    num, kind, patch, self._std_schema, self._refl_schema,
                    self._schema_class_layout,
                    self._tenant.get_backend_runtime_params(),
                    patch_info=patch_info,
                )

                await bootstrap._store_static_bin_cache_conn(
                    conn, f'patch_log_{idx}', pickle.dumps(entry))

            patches[num] = entry
            _, _, updates, _ = entry
            if 'std_and_reflection_schema' in updates:
                self._std_schema, self._refl_schema = updates[
                    'std_and_reflection_schema']
                # +config patches might modify config_spec, which requires
                # a reload of it from the schema.
                if '+config' in kind:
                    config_spec = config.load_spec_from_schema(self._std_schema)
                    self._config_settings = config_spec

            if 'local_intro_query' in updates:
                self._local_intro_query = updates['local_intro_query']
            if 'global_intro_query' in updates:
                self._global_intro_query = updates['global_intro_query']
            if 'classlayout' in updates:
                self._schema_class_layout = updates['classlayout']
            if 'sysqueries' in updates:
                queries = json.loads(updates['sysqueries'])
                self._sys_queries = immutables.Map(
                    {k: q.encode() for k, q in queries.items()})
            if 'report_configs_typedesc' in updates:
                self._report_config_typedesc = (
                    updates['report_configs_typedesc'])

        return patches

    async def _maybe_apply_patches(
        self,
        dbname: str,
        conn: pgcon.PGConnection,
        patches: dict[int, bootstrap.PatchEntry],
        sys: bool=False,
    ) -> None:
        """Apply any un-applied patches to the database."""
        num_patches = await self.get_patch_count(conn)
        for num, (sql_b, syssql, keys, repair) in patches.items():
            if num_patches <= num:
                if sys:
                    sql_b += syssql
                logger.info("applying patch %d to database '%s'", num, dbname)
                sql = tuple(x.encode('utf-8') for x in sql_b)

                # If we are doing a user_ext update, we need to
                # actually run that against each user database.
                if keys.get('is_user_ext_update'):
                    from . import bootstrap

                    kind, patch = pg_patches.PATCHES[num]
                    patch_info = await bootstrap.gather_patch_info(
                        num, kind, patch, conn
                    )

                    # Reload the compiler state from this database in
                    # particular, so we can compiler from exactly the
                    # right state. (Since self._std_schema and the like might
                    # be further advanced.)
                    state = (await edbcompiler.new_compiler_from_pg(conn)).state

                    assert state.global_intro_query and state.local_intro_query
                    global_schema = self._parse_global_schema(
                        await conn.sql_fetch_val(
                            state.global_intro_query.encode('utf-8')),
                    )
                    user_schema = self._parse_user_schema(
                        await conn.sql_fetch_val(
                            state.local_intro_query.encode('utf-8')),
                        global_schema,
                    )

                    entry = bootstrap.prepare_patch(
                        num, kind, patch,
                        state.std_schema,
                        state.refl_schema,
                        state.schema_class_layout,
                        self._tenant.get_backend_runtime_params(),
                        patch_info=patch_info,
                        user_schema=user_schema,
                        global_schema=global_schema,
                    )

                    sql += tuple(x.encode('utf-8') for x in entry[0])

                # Only do repairs when they are the *last* pending
                # repair in the patch queue. We make sure that every
                # patch that changes the user schema is followed by a
                # repair, so this allows us to only ever have to do
                # repairs on up-to-date std schemas.
                last_repair = repair and not any(
                    patches[i][3] for i in range(num + 1, len(patches))
                )
                if last_repair:
                    from . import bootstrap

                    global_schema = await self.introspect_global_schema(conn)
                    user_schema = await self._introspect_user_schema(
                        conn, global_schema)
                    config_json = await self.introspect_db_config(conn)
                    db_config = self._parse_db_config(config_json, user_schema)
                    try:
                        logger.info("repairing database '%s'", dbname)
                        rep_sql = bootstrap.prepare_repair_patch(
                            self._std_schema,
                            self._refl_schema,
                            user_schema,
                            global_schema,
                            self._schema_class_layout,
                            self._tenant.get_backend_runtime_params(),
                            db_config,
                        )
                        sql += (rep_sql,)
                    except errors.EdgeDBError as e:
                        if isinstance(e, errors.InternalServerError):
                            raise
                        raise errors.SchemaError(
                            f'Could not repair schema inconsistencies in '
                            f'database "{dbname}". Probably the schema is '
                            f'no longer valid due to a bug fix.\n'
                            f'Downgrade to the last working version, fix '
                            f'the schema issue, and try again.'
                        ) from e

                if sql:
                    await conn.sql_execute(sql)
                logger.info(
                    "finished applying patch %d to database '%s'", num, dbname)

    async def _maybe_patch_db(
        self, dbname: str, patches: dict[int, bootstrap.PatchEntry], sem: Any
    ) -> None:
        logger.info("applying patches to database '%s'", dbname)

        try:
            async with sem:
                async with self._tenant.direct_pgcon(dbname) as conn:
                    await self._maybe_apply_patches(dbname, conn, patches)
        except Exception as e:
            if (
                isinstance(e, errors.EdgeDBError)
                and not isinstance(e, errors.InternalServerError)
            ):
                raise
            raise errors.InternalServerError(
                f'Could not apply patches for minor version upgrade to '
                f'database {dbname}'
            ) from e

    async def _maybe_patch(self) -> None:
        """Apply patches to all the databases"""

        async with self._tenant.use_sys_pgcon() as syscon:
            patches = await self._prepare_patches(syscon)
            if not patches:
                return

            dbnames = await self.get_dbnames(syscon)

        async with asyncio.TaskGroup() as g:
            # Cap the parallelism used when applying patches, to avoid
            # having huge numbers of in flight patches that make
            # little visible progress in the logs.
            sem = asyncio.Semaphore(16)

            # Patch all the databases
            for dbname in dbnames:
                if dbname != defines.EDGEDB_SYSTEM_DB:
                    g.create_task(
                        self._maybe_patch_db(dbname, patches, sem))

            # Patch the template db, so that any newly created databases
            # will have the patches.
            g.create_task(self._maybe_patch_db(
                defines.EDGEDB_TEMPLATE_DB, patches, sem))

        await self._tenant.ensure_database_not_connected(
            defines.EDGEDB_TEMPLATE_DB
        )

        # Patch the system db last. The system db needs to go last so
        # that it only gets updated if all of the other databases have
        # been succesfully patched. This is important, since we don't check
        # other databases for patches unless the system db is patched.
        #
        # Driving everything from the system db like this lets us
        # always use the correct schema when compiling patches.
        async with self._tenant.use_sys_pgcon() as syscon:
            await self._maybe_apply_patches(
                defines.EDGEDB_SYSTEM_DB, syscon, patches, sys=True)

    def _load_schema(self, result, version_key) -> s_schema.FlatSchema:
        res = pickle.loads(result[2:])
        if version_key != pg_patches.get_version_key(len(pg_patches.PATCHES)):
            res = s_schema.upgrade_schema(res)
        return res

    async def _load_instance_data(self):
        logger.info("loading instance data")
        async with self._tenant.use_sys_pgcon() as syscon:
            patch_count = await self.get_patch_count(syscon)
            version_key = pg_patches.get_version_key(patch_count)

            result = await instdata.get_instdata(
                syscon, f'sysqueries{version_key}', 'json')
            queries = json.loads(result)
            self._sys_queries = immutables.Map(
                {k: q.encode() for k, q in queries.items()})

            self._report_config_typedesc[(1, 0)] = (
                await instdata.get_instdata(
                    syscon,
                    f'report_configs_typedesc_1_0{version_key}',
                    'bin',
                )
            )

            self._report_config_typedesc[(2, 0)] = (
                await instdata.get_instdata(
                    syscon,
                    f'report_configs_typedesc_2_0{version_key}',
                    'bin',
                )
            )

    def _reload_stmt_cache_size(self):
        size = self.config_lookup(
            '_pg_prepared_statement_cache_size', self._get_sys_config()
        )
        self._stmt_cache_size = size
        self._tenant.set_stmt_cache_size(size)

    async def _restart_servers_new_addr(self, nethosts, netport):
        if not netport:
            raise RuntimeError('cannot restart without network port specified')
        nethosts, has_ipv4_wc, has_ipv6_wc = await _resolve_interfaces(
            nethosts
        )
        servers_to_stop = []
        servers_to_stop_early = []
        servers = {}
        if self._listen_port == netport:
            hosts_to_start = [
                host for host in nethosts if host not in self._servers
            ]
            for host, srv in self._servers.items():
                if host == ADMIN_PLACEHOLDER or host in nethosts:
                    servers[host] = srv
                elif host in ['::', '0.0.0.0']:
                    servers_to_stop_early.append(srv)
                else:
                    if has_ipv4_wc:
                        try:
                            ipaddress.IPv4Address(host)
                        except ValueError:
                            pass
                        else:
                            servers_to_stop_early.append(srv)
                            continue
                    if has_ipv6_wc:
                        try:
                            ipaddress.IPv6Address(host)
                        except ValueError:
                            pass
                        else:
                            servers_to_stop_early.append(srv)
                            continue
                    servers_to_stop.append(srv)
            admin = False
        else:
            hosts_to_start = nethosts
            servers_to_stop = list(self._servers.values())
            admin = True

        if servers_to_stop_early:
            await self._stop_servers_with_logging(servers_to_stop_early)

        if hosts_to_start:
            try:
                new_servers, *_ = await self._start_servers(
                    tuple(hosts_to_start),
                    netport,
                    admin=admin,
                )
                servers.update(new_servers)
            except StartupError:
                raise errors.ConfigurationError(
                    'Server updated its config but cannot serve on requested '
                    'address/port, please see server log for more information.'
                )
        self._servers = servers
        self._listen_hosts = [
            s.getsockname()[0]
            for host, tcp_srv in servers.items()
            if host != ADMIN_PLACEHOLDER
            for s in tcp_srv.sockets  # type: ignore
        ]
        self._listen_port = netport

        await self._stop_servers_with_logging(servers_to_stop)

    async def _stop_servers_with_logging(self, servers_to_stop):
        addrs = []
        unix_addr = None
        port = None
        for srv in servers_to_stop:
            for s in srv.sockets:
                addr = s.getsockname()
                if isinstance(addr, tuple):
                    addrs.append(addr[:2])
                    if port is None:
                        port = addr[1]
                    elif port != addr[1]:
                        port = 0
                else:
                    unix_addr = addr
        if len(addrs) > 1:
            if port:
                addr_str = f"{{{', '.join(addr[0] for addr in addrs)}}}:{port}"
            else:
                addr_str = f"{{{', '.join('%s:%d' % addr for addr in addrs)}}}"
        elif addrs:
            addr_str = "%s:%d" % addrs[0]
        else:
            addr_str = None
        if addr_str:
            logger.info('Stopping to serve on %s', addr_str)
        if unix_addr:
            logger.info('Stopping to serve admin on %s', unix_addr)

        await self._stop_servers(servers_to_stop)

    async def _on_system_config_set(self, setting_name, value):
        try:
            if setting_name == 'listen_addresses':
                await self._restart_servers_new_addr(value, self._listen_port)

            elif setting_name == 'listen_port':
                await self._restart_servers_new_addr(self._listen_hosts, value)

            elif setting_name == 'session_idle_timeout':
                self.reinit_idle_gc_collector()

            elif setting_name == '_pg_prepared_statement_cache_size':
                self._reload_stmt_cache_size()

            self._tenant.schedule_reported_config_if_needed(setting_name)
        except Exception:
            metrics.background_errors.inc(
                1.0, self._tenant.get_instance_name(), 'on_system_config_set'
            )
            raise

    async def _on_system_config_reset(self, setting_name):
        try:
            if setting_name == 'listen_addresses':
                cfg = self._get_sys_config()
                await self._restart_servers_new_addr(
                    self.config_lookup('listen_addresses', cfg)
                    or ('localhost',),
                    self._listen_port,
                )

            elif setting_name == 'listen_port':
                cfg = self._get_sys_config()
                await self._restart_servers_new_addr(
                    self._listen_hosts,
                    self.config_lookup('listen_port', cfg)
                    or defines.EDGEDB_PORT,
                )

            elif setting_name == 'session_idle_timeout':
                self.reinit_idle_gc_collector()

            elif setting_name == '_pg_prepared_statement_cache_size':
                self._reload_stmt_cache_size()

            self._tenant.schedule_reported_config_if_needed(setting_name)
        except Exception:
            metrics.background_errors.inc(
                1.0, self._tenant.get_instance_name(), 'on_system_config_reset'
            )
            raise

    async def _after_system_config_add(self, setting_name, value):
        try:
            if setting_name == 'auth':
                self._tenant.populate_sys_auth()
        except Exception:
            metrics.background_errors.inc(
                1.0,
                self._tenant.get_instance_name(),
                'after_system_config_add',
            )
            raise

    async def _after_system_config_rem(self, setting_name, value):
        try:
            if setting_name == 'auth':
                self._tenant.populate_sys_auth()
        except Exception:
            metrics.background_errors.inc(
                1.0,
                self._tenant.get_instance_name(),
                'after_system_config_rem',
            )
            raise

    async def run_startup_script_and_exit(self):
        """Run the script specified in *startup_script* and exit immediately"""
        if self._startup_script is None:
            raise AssertionError('startup script is not defined')
        self._compiler_pool = await compiler_pool.create_compiler_pool(
            **self._get_compiler_args()
        )
        self.compilation_config_serializer = (
            await self._compiler_pool.make_compilation_config_serializer()
        )
        try:
            await binary.run_script(
                server=self,
                tenant=self._tenant,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )
        finally:
            await self._destroy_compiler_pool()

    async def _before_start_servers(self) -> None:
        await self._tenant.start_accepting_new_tasks()
        if self._startup_script and self._new_instance:
            await binary.run_script(
                server=self,
                tenant=self._tenant,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )

    async def _after_start_servers(self) -> None:
        self._tenant.start_running()

    def _get_status(self) -> dict[str, Any]:
        status = super()._get_status()
        status["tenant_id"] = self._tenant.tenant_id
        return status

    def load_jwcrypto(self, jws_key_file: pathlib.Path) -> None:
        super().load_jwcrypto(jws_key_file)
        self._tenant.load_jwcrypto()

    def request_shutdown(self):
        self._tenant.stop_accepting_connections()
        super().request_shutdown()

    async def stop(self):
        try:
            self._tenant.stop()

            await super().stop()

            await self._tenant.wait_stopped()
            await self._destroy_compiler_pool()
        finally:
            self._tenant.terminate_sys_pgcon()

    def get_debug_info(self):
        parent = super().get_debug_info()
        child = self._tenant.get_debug_info()
        parent["params"].update(child["params"])
        child["params"] = parent["params"]
        parent.update(child)
        return parent

    def _get_backend_runtime_params(self) -> pgparams.BackendRuntimeParams:
        return self._tenant.get_backend_runtime_params()

    def _get_compiler_args(self) -> dict[str, Any]:
        rv = super()._get_compiler_args()
        rv.update(self._tenant.get_compiler_args())
        return rv

    def start_watching_files(self):
        super().start_watching_files()
        self._tenant.start_watching_files()


def _cleanup_wildcard_addrs(
    hosts: Sequence[str],
) -> tuple[list[str], list[str], bool, bool]:
    """Filter out conflicting addresses in presence of INADDR_ANY wildcards.

    Attempting to bind to 0.0.0.0 (or ::) _and_ a non-wildcard address will
    usually result in EADDRINUSE.  To avoid this, filter out all specific
    addresses if a wildcard is present in the *hosts* sequence.

    Returns a tuple: first element is the new list of hosts, second
    element is a list of rejected host addrs/names.
    """

    ipv4_hosts = set()
    ipv6_hosts = set()
    named_hosts = set()

    ipv4_wc = ipaddress.ip_address('0.0.0.0')
    ipv6_wc = ipaddress.ip_address('::')

    for host in hosts:
        if host == "*":
            ipv4_hosts.add(ipv4_wc)
            ipv6_hosts.add(ipv6_wc)
            continue

        try:
            ip = ipaddress.IPv4Address(host)
        except ValueError:
            pass
        else:
            ipv4_hosts.add(ip)
            continue

        try:
            ip6 = ipaddress.IPv6Address(host)
        except ValueError:
            pass
        else:
            ipv6_hosts.add(ip6)
            continue

        named_hosts.add(host)

    if not ipv4_hosts and not ipv6_hosts:
        return (list(hosts), [], False, False)

    if ipv4_wc not in ipv4_hosts and ipv6_wc not in ipv6_hosts:
        return (list(hosts), [], False, False)

    if ipv4_wc in ipv4_hosts and ipv6_wc in ipv6_hosts:
        return (
            ['0.0.0.0', '::'],
            [
                str(a) for a in
                ((named_hosts | ipv4_hosts | ipv6_hosts) - {ipv4_wc, ipv6_wc})
            ],
            True,
            True,
        )

    if ipv4_wc in ipv4_hosts:
        return (
            [str(a) for a in ({ipv4_wc} | ipv6_hosts)],
            [str(a) for a in ((named_hosts | ipv4_hosts) - {ipv4_wc})],
            True,
            False,
        )

    if ipv6_wc in ipv6_hosts:
        return (
            [str(a) for a in ({ipv6_wc} | ipv4_hosts)],
            [str(a) for a in ((named_hosts | ipv6_hosts) - {ipv6_wc})],
            False,
            True,
        )

    raise AssertionError('unreachable')


async def _resolve_host(host: str) -> list[str] | Exception:
    loop = asyncio.get_running_loop()
    try:
        addrinfo = await loop.getaddrinfo(
            None if host == '*' else host,
            0,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            flags=socket.AI_PASSIVE,
        )
    except Exception as e:
        return e
    else:
        return [addr[4][0] for addr in addrinfo]


async def _resolve_interfaces(
    hosts: Sequence[str],
) -> Tuple[Sequence[str], bool, bool]:

    async with asyncio.TaskGroup() as g:
        resolve_tasks = {
            host: g.create_task(_resolve_host(host))
            for host in hosts
        }

    addrs = []
    for host, fut in resolve_tasks.items():
        result = fut.result()
        if isinstance(result, Exception):
            logger.warning(
                f"could not translate host name {host!r} to address: {result}")
        else:
            addrs.extend(result)

    (
        clean_addrs, rejected_addrs, has_ipv4_wc, has_ipv6_wc
    ) = _cleanup_wildcard_addrs(addrs)

    if rejected_addrs:
        logger.warning(
            "wildcard addresses found in listen_addresses; " +
            "discarding the other addresses: " +
            ", ".join(repr(h) for h in rejected_addrs)
        )

    return clean_addrs, has_ipv4_wc, has_ipv6_wc
