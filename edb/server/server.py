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

import functools
from typing import *

import asyncio
import collections
import contextlib
import ipaddress
import json
import logging
import os
import pickle
import socket
import ssl
import stat
import struct
import sys
import time
import uuid

import immutables
from jwcrypto import jwk

from edb import errors

from edb.common import devmode
from edb.common import retryloop
from edb.common import taskgroup
from edb.common import windowedsum

from edb.schema import reflection as s_refl
from edb.schema import roles as s_role
from edb.schema import schema as s_schema

from edb.server import args as srvargs
from edb.server import cache
from edb.server import config
from edb.server import connpool
from edb.server import compiler_pool
from edb.server import defines
from edb.server import protocol
from edb.server.ha import base as ha_base
from edb.server.ha import adaptive as adaptive_ha
from edb.server.protocol import binary  # type: ignore
from edb.server import metrics
from edb.server import pgcon
from edb.server.pgcon import errors as pgcon_errors

from edb.pgsql import patches as pg_patches

from . import dbview

if TYPE_CHECKING:
    import asyncio.base_events
    import pathlib


ADMIN_PLACEHOLDER = "<edgedb:admin>"
logger = logging.getLogger('edb.server')
log_metrics = logging.getLogger('edb.server.metrics')


class RoleDescriptor(TypedDict):
    superuser: bool
    name: str
    password: str


class StartupError(Exception):
    pass


class Server(ha_base.ClusterProtocol):

    _sys_pgcon: Optional[pgcon.PGConnection]

    _roles: Mapping[str, RoleDescriptor]
    _instance_data: Mapping[str, str]
    _sys_queries: Mapping[str, str]
    _local_intro_query: bytes
    _global_intro_query: bytes
    _report_config_typedesc: bytes
    _report_config_data: bytes
    _tls_certs_watch_handles: list[asyncio.Handle]

    _std_schema: s_schema.Schema
    _refl_schema: s_schema.Schema
    _schema_class_layout: s_refl.SchemaTypeLayout

    _sys_pgcon_waiter: asyncio.Lock
    _servers: Mapping[str, asyncio.AbstractServer]

    _task_group: Optional[taskgroup.TaskGroup]
    _tasks: Set[asyncio.Task]
    _backend_adaptive_ha: Optional[adaptive_ha.AdaptiveHASupport]

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
    _idle_gc_handler: asyncio.TimerHandle | None = None
    _session_idle_timeout: int | None = None

    def __init__(
        self,
        *,
        cluster,
        runstate_dir,
        internal_runstate_dir,
        max_backend_connections,
        compiler_pool_size,
        compiler_pool_mode: srvargs.CompilerPoolMode,
        compiler_pool_addr,
        nethosts,
        netport,
        new_instance: bool,
        listen_sockets: tuple[socket.socket, ...] = (),
        testmode: bool = False,
        binary_endpoint_security: srvargs.ServerEndpointSecurityMode = (
            srvargs.ServerEndpointSecurityMode.Tls),
        http_endpoint_security: srvargs.ServerEndpointSecurityMode = (
            srvargs.ServerEndpointSecurityMode.Tls),
        auto_shutdown_after: float = -1,
        echo_runtime_info: bool = False,
        status_sinks: Sequence[Callable[[str], None]] = (),
        startup_script: Optional[srvargs.StartupScript] = None,
        backend_adaptive_ha: bool = False,
        default_auth_method: srvargs.ServerAuthMethods = (
            srvargs.DEFAULT_AUTH_METHODS),
        admin_ui: bool = False,
        instance_name: str,
    ):
        self.__loop = asyncio.get_running_loop()
        self._config_settings = config.get_settings()

        # Used to tag PG notifications to later disambiguate them.
        self._server_id = str(uuid.uuid4())

        # Increase-only counter to reject outdated attempts to connect
        self._ha_master_serial = 0

        self._serving = False
        self._initing = False
        self._accept_new_tasks = False
        self._tasks = set()

        self._cluster = cluster
        self._pg_addr = self._get_pgaddr()
        inst_params = cluster.get_runtime_params().instance_params
        self._tenant_id = inst_params.tenant_id

        # 1 connection is reserved for the system DB
        pool_capacity = max_backend_connections - 1
        self._pg_pool = connpool.Pool(
            connect=self._pg_connect,
            disconnect=self._pg_disconnect,
            max_capacity=pool_capacity,
        )
        self._pg_unavailable_msg = None

        # DB state will be initialized in init().
        self._dbindex = None

        self._runstate_dir = runstate_dir
        self._internal_runstate_dir = internal_runstate_dir
        self._max_backend_connections = max_backend_connections
        self._compiler_pool = None
        self._compiler_pool_size = compiler_pool_size
        self._compiler_pool_mode = compiler_pool_mode
        self._compiler_pool_addr = compiler_pool_addr
        self._suggested_client_pool_size = max(
            min(max_backend_connections,
                defines.MAX_SUGGESTED_CLIENT_POOL_SIZE),
            defines.MIN_SUGGESTED_CLIENT_POOL_SIZE
        )

        self._listen_sockets = listen_sockets
        if listen_sockets:
            nethosts = tuple(s.getsockname()[0] for s in listen_sockets)
            netport = listen_sockets[0].getsockname()[1]

        self._listen_hosts = nethosts
        self._listen_port = netport

        self._sys_auth: Tuple[Any, ...] = tuple()

        # Shutdown the server after the last management
        # connection has disconnected
        # and there have been no new connections for n seconds
        self._auto_shutdown_after = auto_shutdown_after
        self._auto_shutdown_handler = None

        self._echo_runtime_info = echo_runtime_info
        self._status_sinks = status_sinks

        self._startup_script = startup_script
        self._new_instance = new_instance

        self._instance_name = instance_name

        # Never use `self.__sys_pgcon` directly; get it via
        # `await self._acquire_sys_pgcon()`.
        self.__sys_pgcon = None

        self._roles = immutables.Map()
        self._instance_data = immutables.Map()
        self._sys_queries = immutables.Map()

        self._devmode = devmode.is_in_dev_mode()
        self._testmode = testmode

        self._binary_proto_id_counter = 0
        self._binary_conns = collections.OrderedDict()
        self._accepting_connections = False

        self._servers = {}

        self._http_query_cache = cache.StatementsCache(
            maxsize=defines.HTTP_PORT_QUERY_CACHE_SIZE)

        self._http_last_minute_requests = windowedsum.WindowedSum()
        self._http_request_logger = None

        self._task_group = None
        self._stop_evt = asyncio.Event()
        self._tls_cert_file = None
        self._tls_cert_newly_generated = False
        self._sslctx = None

        self._jws_key: jwk.JWK | None = None
        self._jwe_key: jwk.JWK | None = None
        self._jws_keys_newly_generated = False
        self._jwe_keys_newly_generated = False

        self._default_auth_method = default_auth_method
        self._binary_endpoint_security = binary_endpoint_security
        self._http_endpoint_security = http_endpoint_security
        if backend_adaptive_ha:
            self._backend_adaptive_ha = adaptive_ha.AdaptiveHASupport(self)
        else:
            self._backend_adaptive_ha = None

        self._idle_gc_handler = None
        self._session_idle_timeout = None

        self._admin_ui = admin_ui

        # A set of databases that should not accept new connections.
        self._block_new_connections: set[str] = set()

        self._tls_certs_watch_handles = []
        self._tls_certs_reload_retry_handle = None

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

    def get_listen_hosts(self):
        return self._listen_hosts

    def get_listen_port(self):
        return self._listen_port

    def get_loop(self):
        return self.__loop

    def get_tenant_id(self):
        return self._tenant_id

    def get_instance_name(self):
        return self._instance_name

    def in_dev_mode(self):
        return self._devmode

    def in_test_mode(self):
        return self._testmode

    def is_admin_ui_enabled(self):
        return self._admin_ui

    def get_pg_dbname(self, dbname: str) -> str:
        return self._cluster.get_db_name(dbname)

    def on_binary_client_created(self) -> str:
        self._binary_proto_id_counter += 1

        if self._auto_shutdown_handler:
            self._auto_shutdown_handler.cancel()
            self._auto_shutdown_handler = None

        return str(self._binary_proto_id_counter)

    def on_binary_client_connected(self, conn):
        self._binary_conns[conn] = True
        metrics.current_client_connections.inc()

    def on_binary_client_authed(self, conn):
        self._report_connections(event='opened')
        metrics.total_client_connections.inc()

    def on_binary_client_after_idling(self, conn):
        try:
            self._binary_conns.move_to_end(conn, last=True)
        except KeyError:
            # Shouldn't happen, but just in case some weird async twist
            # gets us here we don't want to crash the connection with
            # this error.
            metrics.background_errors.inc(1.0, 'client_after_idling')

    def on_binary_client_disconnected(self, conn):
        self._binary_conns.pop(conn, None)
        self._report_connections(event="closed")
        metrics.current_client_connections.dec()

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

    async def _pg_connect(self, dbname):
        ha_serial = self._ha_master_serial
        if self.get_backend_runtime_params().has_create_database:
            pg_dbname = self.get_pg_dbname(dbname)
        else:
            pg_dbname = self.get_pg_dbname(defines.EDGEDB_SUPERUSER_DB)
        started_at = time.monotonic()
        try:
            rv = await pgcon.connect(
                self._get_pgaddr(),
                pg_dbname,
                self.get_backend_runtime_params(),
            )
        except Exception:
            metrics.backend_connection_establishment_errors.inc()
            raise
        finally:
            metrics.backend_connection_establishment_latency.observe(
                time.monotonic() - started_at)
        if ha_serial == self._ha_master_serial:
            rv.set_server(self)
            if self._backend_adaptive_ha is not None:
                self._backend_adaptive_ha.on_pgcon_made(
                    dbname == defines.EDGEDB_SYSTEM_DB
                )
            metrics.total_backend_connections.inc()
            metrics.current_backend_connections.inc()
            return rv
        else:
            rv.terminate()
            raise ConnectionError("connected to outdated Postgres master")

    async def _pg_disconnect(self, conn):
        metrics.current_backend_connections.dec()
        conn.terminate()

    async def init(self):
        self._initing = True
        try:
            self.__sys_pgcon = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
            self._sys_pgcon_waiter = asyncio.Lock()
            self._sys_pgcon_ready_evt = asyncio.Event()
            self._sys_pgcon_reconnect_evt = asyncio.Event()

            await self._load_instance_data()
            await self._maybe_patch()

            global_schema = await self.introspect_global_schema()
            sys_config = await self.load_sys_config()
            await self.load_reported_config()

            self._dbindex = dbview.DatabaseIndex(
                self,
                std_schema=self._std_schema,
                global_schema=global_schema,
                sys_config=sys_config,
            )

            self._fetch_roles()
            await self._introspect_dbs()

            # Now, once all DBs have been introspected, start listening on
            # any notifications about schema/roles/etc changes.
            await self.__sys_pgcon.listen_for_sysevent()
            self.__sys_pgcon.mark_as_system_db()
            self._sys_pgcon_ready_evt.set()

            self._populate_sys_auth()

            if not self._listen_hosts:
                self._listen_hosts = (
                    config.lookup('listen_addresses', sys_config)
                    or ('localhost',)
                )

            if self._listen_port is None:
                self._listen_port = (
                    config.lookup('listen_port', sys_config)
                    or defines.EDGEDB_PORT
                )

            self._reinit_idle_gc_collector()

        finally:
            self._initing = False

    def _reinit_idle_gc_collector(self) -> float:
        if self._auto_shutdown_after >= 0:
            return -1

        if self._idle_gc_handler is not None:
            self._idle_gc_handler.cancel()
            self._idle_gc_handler = None

        assert self._dbindex is not None
        session_idle_timeout = config.lookup(
            'session_idle_timeout', self._dbindex.get_sys_config())

        timeout = session_idle_timeout.to_microseconds()
        timeout /= 1_000_000.0  # convert to seconds

        if timeout > 0:
            self._idle_gc_handler = self.__loop.call_later(
                timeout, self._idle_gc_collector)

        return timeout

    def _idle_gc_collector(self):
        try:
            self._idle_gc_handler = None
            idle_timeout = self._reinit_idle_gc_collector()

            if idle_timeout <= 0:
                return

            now = time.monotonic()
            expiry_time = now - idle_timeout
            for conn in self._binary_conns:
                try:
                    if conn.is_idle(expiry_time):
                        metrics.idle_client_connections.inc()
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
                    metrics.background_errors.inc(1.0, 'close_for_idling')
                    conn.abort()
        except Exception:
            metrics.background_errors.inc(1.0, 'idle_clients_collector')
            raise

    async def _create_compiler_pool(self):
        args = dict(
            pool_size=self._compiler_pool_size,
            pool_class=self._compiler_pool_mode.pool_class,
            dbindex=self._dbindex,
            runstate_dir=self._internal_runstate_dir,
            backend_runtime_params=self.get_backend_runtime_params(),
            std_schema=self._std_schema,
            refl_schema=self._refl_schema,
            schema_class_layout=self._schema_class_layout,
        )
        if self._compiler_pool_mode == srvargs.CompilerPoolMode.Remote:
            args['address'] = self._compiler_pool_addr
        self._compiler_pool = await compiler_pool.create_compiler_pool(**args)

    async def _destroy_compiler_pool(self):
        if self._compiler_pool is not None:
            await self._compiler_pool.stop()
            self._compiler_pool = None

    def _populate_sys_auth(self):
        cfg = self._dbindex.get_sys_config()
        auth = config.lookup('auth', cfg) or ()
        self._sys_auth = tuple(sorted(auth, key=lambda a: a.priority))

    def _get_pgaddr(self):
        return self._cluster.get_connection_spec()

    def get_compiler_pool(self):
        return self._compiler_pool

    def get_suggested_client_pool_size(self) -> int:
        return self._suggested_client_pool_size

    def get_db(self, *, dbname: str):
        assert self._dbindex is not None
        return self._dbindex.get_db(dbname)

    def maybe_get_db(self, *, dbname: str):
        assert self._dbindex is not None
        return self._dbindex.maybe_get_db(dbname)

    async def new_dbview(self, *, dbname, query_cache, protocol_version):
        db = self.get_db(dbname=dbname)
        await db.introspection()
        return self._dbindex.new_view(
            dbname, query_cache=query_cache, protocol_version=protocol_version
        )

    def remove_dbview(self, dbview):
        return self._dbindex.remove_view(dbview)

    def get_global_schema(self):
        return self._dbindex.get_global_schema()

    def get_compilation_system_config(self):
        return self._dbindex.get_compilation_system_config()

    async def acquire_pgcon(self, dbname):
        if self._pg_unavailable_msg is not None:
            raise errors.BackendUnavailableError(
                'Postgres is not available: ' + self._pg_unavailable_msg
            )

        for _ in range(self._pg_pool.max_capacity):
            conn = await self._pg_pool.acquire(dbname)
            if conn.is_healthy():
                return conn
            else:
                logger.warning('Acquired an unhealthy pgcon; discard now.')
                self._pg_pool.release(dbname, conn, discard=True)
        else:
            # This is unlikely to happen, but we defer to the caller to retry
            # when it does happen
            raise errors.BackendUnavailableError(
                'No healthy backend connection available at the moment, '
                'please try again.'
            )

    def release_pgcon(self, dbname, conn, *, discard=False):
        if not conn.is_healthy():
            if not discard:
                logger.warning('Released an unhealthy pgcon; discard now.')
            discard = True
        try:
            self._pg_pool.release(dbname, conn, discard=discard)
        except Exception:
            metrics.background_errors.inc(1.0, 'release_pgcon')
            raise

    async def load_sys_config(self):
        async with self._use_sys_pgcon() as syscon:
            query = self.get_sys_query('sysconfig')
            sys_config_json = await syscon.sql_fetch_val(query)

        return config.from_json(config.get_settings(), sys_config_json)

    async def reload_sys_config(self):
        cfg = await self.load_sys_config()
        self._dbindex.update_sys_config(cfg)
        self._reinit_idle_gc_collector()

    def schedule_reported_config_if_needed(self, setting_name):
        setting = self._config_settings[setting_name]
        if setting.report and self._accept_new_tasks:
            self.create_task(
                self.load_reported_config(), interruptable=True)

    def get_report_config_data(self) -> bytes:
        return self._report_config_data

    async def load_reported_config(self):
        syscon = await self._acquire_sys_pgcon()
        try:
            data = await syscon.sql_fetch_val(
                self.get_sys_query('report_configs'),
                use_prep_stmt=True,
            )
            self._report_config_data = (
                struct.pack('!L', len(self._report_config_typedesc)) +
                self._report_config_typedesc +
                struct.pack('!L', len(data)) +
                data
            )
        except Exception:
            metrics.background_errors.inc(1.0, 'load_reported_config')
            raise
        finally:
            self._release_sys_pgcon()

    async def introspect_global_schema(self, conn=None):
        intro_query = self._global_intro_query
        if conn is not None:
            json_data = await conn.sql_fetch_val(intro_query)
        else:
            async with self._use_sys_pgcon() as syscon:
                json_data = await syscon.sql_fetch_val(intro_query)

        return s_refl.parse_into(
            base_schema=self._std_schema,
            schema=s_schema.FlatSchema(),
            data=json_data,
            schema_class_layout=self._schema_class_layout,
        )

    async def _reintrospect_global_schema(self):
        if not self._initing and not self._serving:
            logger.warning(
                "global-schema-changes event received during shutdown; "
                "ignoring."
            )
            return
        new_global_schema = await self.introspect_global_schema()
        self._dbindex.update_global_schema(new_global_schema)
        self._fetch_roles()

    async def introspect_user_schema(self, conn):
        json_data = await conn.sql_fetch_val(self._local_intro_query)

        base_schema = s_schema.ChainedSchema(
            self._std_schema,
            s_schema.FlatSchema(),
            self.get_global_schema(),
        )

        return s_refl.parse_into(
            base_schema=base_schema,
            schema=s_schema.FlatSchema(),
            data=json_data,
            schema_class_layout=self._schema_class_layout,
        )

    async def _acquire_intro_pgcon(self, dbname):
        try:
            conn = await self.acquire_pgcon(dbname)
        except pgcon_errors.BackendError as e:
            if e.code_is(pgcon_errors.ERROR_INVALID_CATALOG_NAME):
                # database does not exist (anymore)
                logger.warning(
                    "Detected concurrently-dropped database %s; skipping.",
                    dbname,
                )
                if self._dbindex is not None and self._dbindex.has_db(dbname):
                    self._dbindex.unregister_db(dbname)
                return None
            else:
                raise
        return conn

    async def introspect_db(self, dbname):
        """Use this method to (re-)introspect a DB.

        If the DB is already registered in self._dbindex, its
        schema, config, etc. would simply be updated. If it's missing
        an entry for it would be created.

        All remote notifications of remote events should use this method
        to refresh the state. Even if the remote event was a simple config
        change, a lot of other events could happen before it was sent to us
        by a remote server and us receiving it. E.g. a DB could have been
        dropped and recreated again. It's safer to refresh the entire state
        than refreshing individual components of it. Besides, DDL and
        database-level config modifications are supposed to be rare events.
        """
        logger.info("introspecting database '%s'", dbname)

        conn = await self._acquire_intro_pgcon(dbname)
        if not conn:
            return

        try:
            user_schema = await self.introspect_user_schema(conn)

            reflection_cache_json = await conn.sql_fetch_val(
                b'''
                    SELECT json_agg(o.c)
                    FROM (
                        SELECT
                            json_build_object(
                                'eql_hash', t.eql_hash,
                                'argnames', array_to_json(t.argnames)
                            ) AS c
                        FROM
                            ROWS FROM(edgedb._get_cached_reflection())
                                AS t(eql_hash text, argnames text[])
                    ) AS o;
                ''',
            )

            reflection_cache = immutables.Map({
                r['eql_hash']: tuple(r['argnames'])
                for r in json.loads(reflection_cache_json)
            })

            backend_ids_json = await conn.sql_fetch_val(
                b'''
                SELECT
                    json_object_agg(
                        "id"::text,
                        "backend_id"
                    )::text
                FROM
                    edgedb."_SchemaType"
                ''',
            )
            backend_ids = json.loads(backend_ids_json)

            db_config = await self.introspect_db_config(conn)

            assert self._dbindex is not None
            self._dbindex.register_db(
                dbname,
                user_schema=user_schema,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
            )
        finally:
            self.release_pgcon(dbname, conn)

    async def introspect_db_config(self, conn):
        result = await conn.sql_fetch_val(self.get_sys_query('dbconfig'))
        return config.from_json(config.get_settings(), result)

    async def _early_introspect_db(self, dbname):
        """We need to always introspect the extensions for each database.

        Otherwise we won't know to accept connections for graphql or
        http, for example, until a native connection is made.
        """
        logger.info("introspecting extensions for database '%s'", dbname)

        conn = await self._acquire_intro_pgcon(dbname)
        if not conn:
            return

        try:
            extension_names_json = await conn.sql_fetch_val(
                b'''
                    SELECT json_agg(name) FROM edgedb."_SchemaExtension";
                ''',
            )
            if extension_names_json:
                extensions = set(json.loads(extension_names_json))
            else:
                extensions = set()

            assert self._dbindex is not None
            self._dbindex.register_db(
                dbname,
                user_schema=None,
                db_config=None,
                reflection_cache=None,
                backend_ids=None,
                extensions=extensions,
            )
        finally:
            self.release_pgcon(dbname, conn)

    async def get_dbnames(self, syscon):
        dbs_query = self.get_sys_query('listdbs')
        json_data = await syscon.sql_fetch_val(dbs_query)
        return json.loads(json_data)

    async def _introspect_dbs(self):
        async with self._use_sys_pgcon() as syscon:
            dbnames = await self.get_dbnames(syscon)

        async with taskgroup.TaskGroup(name='introspect DB extensions') as g:
            for dbname in dbnames:
                # There's a risk of the DB being dropped by another server
                # between us building the list of databases and loading
                # information about them.
                g.create_task(self._early_introspect_db(dbname))

    async def get_patch_count(self, conn):
        """Apply any un-applied patches to the database."""
        num_patches = await conn.sql_fetch_val(
            b'''
                SELECT json::json from edgedbinstdata.instdata
                WHERE key = 'num_patches';
            ''',
        )
        num_patches = json.loads(num_patches) if num_patches else 0
        return num_patches

    async def _prepare_patches(self, conn):
        """Prepare all the patches"""
        num_patches = await self.get_patch_count(conn)
        schema = self._std_schema

        patches = {}
        patch_list = list(enumerate(pg_patches.PATCHES))
        for num, (kind, patch) in patch_list[num_patches:]:
            from . import bootstrap
            sql, syssql, schema = bootstrap.prepare_patch(
                num, kind, patch, schema, self._refl_schema,
                self._schema_class_layout, self.get_backend_runtime_params())

            patches[num] = (sql, syssql, schema)

        return patches

    async def _maybe_apply_patches(self, dbname, conn, patches, sys=False):
        """Apply any un-applied patches to the database."""
        num_patches = await self.get_patch_count(conn)
        for num, (sql, syssql, _) in patches.items():
            if num_patches <= num:
                if sys:
                    sql += syssql
                logger.info("applying patch %d to database '%s'", num, dbname)
                sql = tuple(x.encode('utf-8') for x in sql)
                await conn.sql_fetch(sql)

    async def _maybe_patch_db(self, dbname, patches):
        logger.info("applying patches to database '%s'", dbname)

        if dbname != defines.EDGEDB_SYSTEM_DB:
            async with self._direct_pgcon(dbname) as conn:
                await self._maybe_apply_patches(dbname, conn, patches)

    async def _maybe_patch(self):
        """Apply patches to all the databases"""

        async with self._use_sys_pgcon() as syscon:
            patches = await self._prepare_patches(syscon)
            if not patches:
                return

            dbnames = await self.get_dbnames(syscon)

        async with taskgroup.TaskGroup(name='apply patches') as g:
            # Patch all the databases
            for dbname in dbnames:
                if dbname != defines.EDGEDB_SYSTEM_DB:
                    g.create_task(self._maybe_patch_db(dbname, patches))

            # Patch the template db, so that any newly created databases
            # will have the patches.
            g.create_task(self._maybe_patch_db(
                defines.EDGEDB_TEMPLATE_DB, patches))

        await self._ensure_database_not_connected(defines.EDGEDB_TEMPLATE_DB)

        # Patch the system db last. The system db needs to go last so
        # that it only gets updated if all of the other databases have
        # been succesfully patched. This is important, since we don't check
        # other databases for patches unless the system db is patched.
        #
        # Driving everything from the system db like this lets us
        # always use the correct schema when compiling patches.
        async with self._use_sys_pgcon() as syscon:
            await self._maybe_apply_patches(
                defines.EDGEDB_SYSTEM_DB, syscon, patches, sys=True)
        self._std_schema = patches[max(patches)][-1]

    def _fetch_roles(self):
        global_schema = self._dbindex.get_global_schema()

        roles = {}
        for role in global_schema.get_objects(type=s_role.Role):
            role_name = str(role.get_name(global_schema))
            roles[role_name] = {
                'name': role_name,
                'superuser': role.get_superuser(global_schema),
                'password': role.get_password(global_schema),
            }

        self._roles = immutables.Map(roles)

    async def _load_instance_data(self):
        async with self._use_sys_pgcon() as syscon:
            result = await syscon.sql_fetch_val(b'''\
                SELECT json::json FROM edgedbinstdata.instdata
                WHERE key = 'instancedata';
            ''')
            self._instance_data = immutables.Map(json.loads(result))

            result = await syscon.sql_fetch_val(b'''\
                SELECT json::json FROM edgedbinstdata.instdata
                WHERE key = 'sysqueries';
            ''')
            queries = json.loads(result)
            self._sys_queries = immutables.Map(
                {k: q.encode() for k, q in queries.items()})

            self._local_intro_query = await syscon.sql_fetch_val(b'''\
                SELECT text FROM edgedbinstdata.instdata
                WHERE key = 'local_intro_query';
            ''')

            self._global_intro_query = await syscon.sql_fetch_val(b'''\
                SELECT text FROM edgedbinstdata.instdata
                WHERE key = 'global_intro_query';
            ''')

            result = await syscon.sql_fetch_val(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'stdschema';
            ''')
            try:
                self._std_schema = pickle.loads(result[2:])
            except Exception as e:
                raise RuntimeError(
                    'could not load std schema pickle') from e

            result = await syscon.sql_fetch_val(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'reflschema';
            ''')
            try:
                self._refl_schema = pickle.loads(result[2:])
            except Exception as e:
                raise RuntimeError(
                    'could not load refl schema pickle') from e

            result = await syscon.sql_fetch_val(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'classlayout';
            ''')
            try:
                self._schema_class_layout = pickle.loads(result[2:])
            except Exception as e:
                raise RuntimeError(
                    'could not load schema class layout pickle') from e

            self._report_config_typedesc = await syscon.sql_fetch_val(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'report_configs_typedesc';
            ''')

    def get_roles(self):
        return self._roles

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
            servers_to_stop = self._servers.values()
            admin = True

        if servers_to_stop_early:
            await self._stop_servers_with_logging(servers_to_stop_early)

        if hosts_to_start:
            try:
                new_servers, *_ = await self._start_servers(
                    hosts_to_start,
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
        self._listen_hosts = nethosts
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

    async def _on_before_drop_db(
        self,
        dbname: str,
        current_dbname: str
    ) -> None:
        if current_dbname == dbname:
            raise errors.ExecutionError(
                f'cannot drop the currently open database {dbname!r}')

        await self._ensure_database_not_connected(dbname)

    async def _on_before_create_db_from_template(
        self,
        dbname: str,
        current_dbname: str
    ):
        if current_dbname == dbname:
            raise errors.ExecutionError(
                f'cannot create database using currently open database '
                f'{dbname!r} as a template database')

        await self._ensure_database_not_connected(dbname)

    async def _ensure_database_not_connected(self, dbname: str) -> None:
        if self._dbindex and self._dbindex.count_connections(dbname):
            # If there are open EdgeDB connections to the `dbname` DB
            # just raise the error Postgres would have raised itself.
            raise errors.ExecutionError(
                f'database {dbname!r} is being accessed by other users')
        else:
            self._block_new_connections.add(dbname)

            # Prune our inactive connections.
            await self._pg_pool.prune_inactive_connections(dbname)

            # Signal adjacent servers to prune their connections to this
            # database.
            await self._signal_sysevent(
                'ensure-database-not-used',
                dbname=dbname,
            )

            rloop = retryloop.RetryLoop(
                backoff=retryloop.exp_backoff(),
                timeout=10.0,
                ignore=errors.ExecutionError,
            )

            async for iteration in rloop:
                async with iteration:
                    await self._pg_ensure_database_not_connected(dbname)

    async def _pg_ensure_database_not_connected(self, dbname: str) -> None:
        async with self._use_sys_pgcon() as pgcon:
            conns = await pgcon.sql_fetch_col(
                b"""
                SELECT
                    pid
                FROM
                    pg_stat_activity
                WHERE
                    datname = $1
                """,
                args=[dbname.encode("utf-8")],
            )

        if conns:
            raise errors.ExecutionError(
                f'database {dbname!r} is being accessed by other users')

    def _allow_database_connections(self, dbname: str) -> None:
        self._block_new_connections.discard(dbname)

    def _on_after_drop_db(self, dbname: str):
        try:
            assert self._dbindex is not None
            if self._dbindex.has_db(dbname):
                self._dbindex.unregister_db(dbname)
            self._block_new_connections.discard(dbname)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_after_drop_db')
            raise

    async def _on_system_config_add(self, setting_name, value):
        # CONFIGURE INSTANCE INSERT ConfigObject;
        pass

    async def _on_system_config_rem(self, setting_name, value):
        # CONFIGURE INSTANCE RESET ConfigObject;
        pass

    async def _on_system_config_set(self, setting_name, value):
        # CONFIGURE INSTANCE SET setting_name := value;
        try:
            if setting_name == 'listen_addresses':
                await self._restart_servers_new_addr(value, self._listen_port)

            elif setting_name == 'listen_port':
                await self._restart_servers_new_addr(self._listen_hosts, value)

            elif setting_name == 'session_idle_timeout':
                self._reinit_idle_gc_collector()

            self.schedule_reported_config_if_needed(setting_name)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_system_config_set')
            raise

    async def _on_system_config_reset(self, setting_name):
        # CONFIGURE INSTANCE RESET setting_name;
        try:
            if setting_name == 'listen_addresses':
                await self._restart_servers_new_addr(
                    ('localhost',), self._listen_port)

            elif setting_name == 'listen_port':
                await self._restart_servers_new_addr(
                    self._listen_hosts, defines.EDGEDB_PORT)

            elif setting_name == 'session_idle_timeout':
                self._reinit_idle_gc_collector()

            self.schedule_reported_config_if_needed(setting_name)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_system_config_reset')
            raise

    async def _after_system_config_add(self, setting_name, value):
        # CONFIGURE INSTANCE INSERT ConfigObject;
        try:
            if setting_name == 'auth':
                self._populate_sys_auth()
        except Exception:
            metrics.background_errors.inc(1.0, 'after_system_config_add')
            raise

    async def _after_system_config_rem(self, setting_name, value):
        # CONFIGURE INSTANCE RESET ConfigObject;
        try:
            if setting_name == 'auth':
                self._populate_sys_auth()
        except Exception:
            metrics.background_errors.inc(1.0, 'after_system_config_rem')
            raise

    async def _after_system_config_set(self, setting_name, value):
        # CONFIGURE INSTANCE SET setting_name := value;
        pass

    async def _after_system_config_reset(self, setting_name):
        # CONFIGURE INSTANCE RESET setting_name;
        pass

    async def _acquire_sys_pgcon(self) -> pgcon.PGConnection:
        if not self._initing and not self._serving:
            raise RuntimeError("EdgeDB server is not serving.")

        await self._sys_pgcon_waiter.acquire()

        if not self._initing and not self._serving:
            self._sys_pgcon_waiter.release()
            raise RuntimeError("EdgeDB server is not serving.")

        if self.__sys_pgcon is None or not self.__sys_pgcon.is_healthy():
            conn, self.__sys_pgcon = self.__sys_pgcon, None
            if conn is not None:
                self._sys_pgcon_ready_evt.clear()
                conn.abort()
            # We depend on the reconnect on connection_lost() of __sys_pgcon
            await self._sys_pgcon_ready_evt.wait()
            if self.__sys_pgcon is None:
                self._sys_pgcon_waiter.release()
                raise RuntimeError("Cannot acquire pgcon to the system DB.")

        return self.__sys_pgcon

    def _release_sys_pgcon(self):
        self._sys_pgcon_waiter.release()

    @contextlib.asynccontextmanager
    async def _use_sys_pgcon(self):
        conn = await self._acquire_sys_pgcon()
        try:
            yield conn
        finally:
            self._release_sys_pgcon()

    @contextlib.asynccontextmanager
    async def _direct_pgcon(
        self,
        dbname: str,
    ) -> AsyncGenerator[pgcon.PGConnection, None]:
        conn = None
        try:
            conn = await self._pg_connect(dbname)
            yield conn
        finally:
            if conn is not None:
                await self._pg_disconnect(conn)

    async def _cancel_pgcon_operation(self, pgcon) -> bool:
        async with self._use_sys_pgcon() as syscon:
            if pgcon.idle:
                # pgcon could have received the query results while we
                # were acquiring a system connection to cancel it.
                return False

            if pgcon.is_cancelling():
                # Somehow the connection is already being cancelled and
                # we don't want to have to cancellations go in parallel.
                return False

            pgcon.start_pg_cancellation()
            try:
                # Returns True if the `pid` exists and it was able to send it a
                # SIGINT.  Will throw an exception if the privileges aren't
                # sufficient.
                result = await syscon.sql_fetch_val(
                    f'SELECT pg_cancel_backend({pgcon.backend_pid});'.encode(),
                )
            finally:
                pgcon.finish_pg_cancellation()

            return result == b'\x01'

    async def _cancel_and_discard_pgcon(self, pgcon, dbname) -> None:
        try:
            if self._serving:
                await self._cancel_pgcon_operation(pgcon)
        finally:
            self.release_pgcon(dbname, pgcon, discard=True)

    async def _signal_sysevent(self, event, **kwargs):
        try:
            if not self._initing and not self._serving:
                # This is very likely if we are doing
                # "run_startup_script_and_exit()", but is also possible if the
                # server was shut down with this coroutine as a background task
                # in flight.
                return

            pgcon = await self._acquire_sys_pgcon()
            try:
                await pgcon.signal_sysevent(event, **kwargs)
            finally:
                self._release_sys_pgcon()
        except Exception:
            metrics.background_errors.inc(1.0, 'signal_sysevent')
            raise

    def _on_remote_database_quarantine(self, dbname):
        if not self._accept_new_tasks:
            return

        # Block new connections to the database.
        self._block_new_connections.add(dbname)

        async def task():
            try:
                await self._pg_pool.prune_inactive_connections(dbname)
            except Exception:
                metrics.background_errors.inc(1.0, 'remote_db_quarantine')
                raise

        self.create_task(task(), interruptable=True)

    def _on_remote_ddl(self, dbname):
        if not self._accept_new_tasks:
            return

        # Triggered by a postgres notification event 'schema-changes'
        # on the __edgedb_sysevent__ channel
        async def task():
            try:
                await self.introspect_db(dbname)
            except Exception:
                metrics.background_errors.inc(1.0, 'on_remote_ddl')
                raise

        self.create_task(task(), interruptable=True)

    def _on_remote_database_changes(self):
        if not self._accept_new_tasks:
            return

        # Triggered by a postgres notification event 'database-changes'
        # on the __edgedb_sysevent__ channel
        async def task():
            async with self._use_sys_pgcon() as syscon:
                dbnames = set(await self.get_dbnames(syscon))

            tg = taskgroup.TaskGroup(name='new database introspection')
            async with tg as g:
                for dbname in dbnames:
                    if not self._dbindex.has_db(dbname):
                        g.create_task(self._early_introspect_db(dbname))

            for dbname in self._dbindex.iter_dbs():
                if dbname not in dbnames:
                    self._on_after_drop_db(dbname)

        self.create_task(task(), interruptable=True)

    def _on_remote_database_config_change(self, dbname):
        if not self._accept_new_tasks:
            return

        # Triggered by a postgres notification event 'database-config-changes'
        # on the __edgedb_sysevent__ channel
        async def task():
            try:
                await self.introspect_db(dbname)
            except Exception:
                metrics.background_errors.inc(
                    1.0, 'on_remote_database_config_change')
                raise

        self.create_task(task(), interruptable=True)

    def _on_local_database_config_change(self, dbname):
        if not self._accept_new_tasks:
            return

        # Triggered by DB Index.
        # It's easier and safer to just schedule full re-introspection
        # of the DB and update all components of it.
        async def task():
            try:
                await self.introspect_db(dbname)
            except Exception:
                metrics.background_errors.inc(
                    1.0, 'on_local_database_config_change')
                raise

        self.create_task(task(), interruptable=True)

    def _on_remote_system_config_change(self):
        if not self._accept_new_tasks:
            return

        # Triggered by a postgres notification event 'system-config-changes'
        # on the __edgedb_sysevent__ channel

        async def task():
            try:
                await self.reload_sys_config()
            except Exception:
                metrics.background_errors.inc(
                    1.0, 'on_remote_system_config_change')
                raise

        self.create_task(task(), interruptable=True)

    def _on_global_schema_change(self):
        if not self._accept_new_tasks:
            return

        async def task():
            try:
                await self._reintrospect_global_schema()
            except Exception:
                metrics.background_errors.inc(
                    1.0, 'on_global_schema_change')
                raise

        self.create_task(task(), interruptable=True)

    def _on_sys_pgcon_connection_lost(self, exc):
        try:
            if not self._serving:
                # The server is shutting down, release all events so that
                # the waiters if any could continue and exit
                self._sys_pgcon_ready_evt.set()
                self._sys_pgcon_reconnect_evt.set()
                return

            logger.error(
                "Connection to the system database is " +
                ("closed." if exc is None else f"broken! Reason: {exc}")
            )
            self.set_pg_unavailable_msg(
                "Connection is lost, please check server log for the reason."
            )
            self.__sys_pgcon = None
            self._sys_pgcon_ready_evt.clear()
            if self._accept_new_tasks:
                self.create_task(
                    self._reconnect_sys_pgcon(), interruptable=True
                )
            self._on_pgcon_broken(True)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_sys_pgcon_connection_lost')
            raise

    def _on_sys_pgcon_parameter_status_updated(self, name, value):
        try:
            if name == 'in_hot_standby' and value == 'on':
                # It is a strong evidence of failover if the sys_pgcon receives
                # a notification that in_hot_standby is turned on.
                self._on_sys_pgcon_failover_signal()
        except Exception:
            metrics.background_errors.inc(
                1.0, 'on_sys_pgcon_parameter_status_updated')
            raise

    def _on_sys_pgcon_failover_signal(self):
        if not self._serving:
            return
        try:
            if self._backend_adaptive_ha is not None:
                # Switch to FAILOVER if adaptive HA is enabled
                self._backend_adaptive_ha.set_state_failover()
            elif getattr(self._cluster, '_ha_backend', None) is None:
                # If the server is not using an HA backend, nor has enabled the
                # adaptive HA monitoring, we still tries to "switch over" by
                # disconnecting all pgcons if failover signal is received,
                # allowing reconnection to happen sooner.
                self.on_switch_over()
            # Else, the HA backend should take care of calling on_switch_over()
        except Exception:
            metrics.background_errors.inc(1.0, 'on_sys_pgcon_failover_signal')
            raise

    def _on_pgcon_broken(self, is_sys_pgcon=False):
        try:
            if self._backend_adaptive_ha:
                self._backend_adaptive_ha.on_pgcon_broken(is_sys_pgcon)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_pgcon_broken')
            raise

    def _on_pgcon_lost(self):
        try:
            if self._backend_adaptive_ha:
                self._backend_adaptive_ha.on_pgcon_lost()
        except Exception:
            metrics.background_errors.inc(1.0, 'on_pgcon_lost')
            raise

    async def _reconnect_sys_pgcon(self):
        try:
            conn = None
            while self._serving:
                try:
                    conn = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
                    break
                except (ConnectionError, TimeoutError):
                    # Keep retrying as far as:
                    #   1. The EdgeDB server is still serving,
                    #   2. We still cannot connect to the Postgres cluster, or
                    pass
                except pgcon_errors.BackendError as e:
                    #   3. The Postgres cluster is still starting up, or the
                    #      HA failover is still in progress
                    if not (
                        e.code_is(pgcon_errors.ERROR_FEATURE_NOT_SUPPORTED) or
                        e.code_is(pgcon_errors.ERROR_CANNOT_CONNECT_NOW) or
                        e.code_is(pgcon_errors.ERROR_READ_ONLY_SQL_TRANSACTION)
                    ):
                        # TODO: ERROR_FEATURE_NOT_SUPPORTED should be removed
                        # once PostgreSQL supports SERIALIZABLE in hot standbys
                        raise

                if self._serving:
                    try:
                        # Retry after INTERVAL seconds, unless the event is set
                        # and we can retry immediately after the event.
                        await asyncio.wait_for(
                            self._sys_pgcon_reconnect_evt.wait(),
                            defines.SYSTEM_DB_RECONNECT_INTERVAL,
                        )
                        # But the event can only skip one INTERVAL.
                        self._sys_pgcon_reconnect_evt.clear()
                    except asyncio.TimeoutError:
                        pass

            if not self._serving:
                if conn is not None:
                    conn.abort()
                return

            logger.info("Successfully reconnected to the system database.")
            self.__sys_pgcon = conn
            self.__sys_pgcon.mark_as_system_db()
            # This await is meant to be after mark_as_system_db() because we
            # need the pgcon to be able to trigger another reconnect if its
            # connection is lost during this await.
            await self.__sys_pgcon.listen_for_sysevent()
            self.set_pg_unavailable_msg(None)
        finally:
            self._sys_pgcon_ready_evt.set()

    async def run_startup_script_and_exit(self):
        """Run the script specified in *startup_script* and exit immediately"""
        if self._startup_script is None:
            raise AssertionError('startup script is not defined')
        await self._create_compiler_pool()
        try:
            await binary.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )
        finally:
            await self._destroy_compiler_pool()

    async def _start_server(
        self,
        host: str,
        port: int,
        sock: Optional[socket.socket] = None,
    ) -> Optional[asyncio.base_events.Server]:
        proto_factory = lambda: protocol.HttpProtocol(
            self,
            self._sslctx,
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
            self._runstate_dir, f'.s.EDGEDB.admin.{port}')
        assert len(admin_unix_sock_path) <= (
            defines.MAX_RUNSTATE_DIR_PATH
            + defines.MAX_UNIX_SOCKET_PATH_LENGTH
            + 1
        ), "admin Unix socket length exceeds maximum allowed"
        admin_unix_srv = await self.__loop.create_unix_server(
            lambda: binary.new_edge_connection(self, external_auth=True),
            admin_unix_sock_path
        )
        os.chmod(admin_unix_sock_path, stat.S_IRUSR | stat.S_IWUSR)
        logger.info('Serving admin on %s', admin_unix_sock_path)
        return admin_unix_srv

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
                async with taskgroup.TaskGroup() as g:
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
                host: fut.result()
                for host, fut in start_tasks.items()
                if fut.result() is not None
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

    def reload_tls(self, tls_cert_file, tls_key_file):
        logger.info("loading TLS certificates")
        tls_password_needed = False
        if self._tls_certs_reload_retry_handle is not None:
            self._tls_certs_reload_retry_handle.cancel()
            self._tls_certs_reload_retry_handle = None

        def _tls_private_key_password():
            nonlocal tls_password_needed
            tls_password_needed = True
            return os.environ.get('EDGEDB_SERVER_TLS_PRIVATE_KEY_PASSWORD', '')

        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            sslctx.load_cert_chain(
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
                            "EDGEDB_SERVER_TLS_PRIVATE_KEY_PASSWORD"
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

        sslctx.set_alpn_protocols(['edgedb-binary', 'http/1.1'])
        self._sslctx = sslctx

    def init_tls(
        self,
        tls_cert_file,
        tls_key_file,
        tls_cert_newly_generated,
    ):
        assert self._sslctx is None
        self.reload_tls(tls_cert_file, tls_key_file)

        self._tls_cert_file = str(tls_cert_file)
        self._tls_cert_newly_generated = tls_cert_newly_generated

        def reload_tls(_file_modified, _event, retry=0):
            try:
                self.reload_tls(tls_cert_file, tls_key_file)
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
                            _file_modified,
                            _event,
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

        self._tls_certs_watch_handles.append(
            self.__loop._monitor_fs(str(tls_cert_file), reload_tls)
        )
        if tls_cert_file != tls_key_file:
            self._tls_certs_watch_handles.append(
                self.__loop._monitor_fs(str(tls_key_file), reload_tls)
            )

    def load_jwcrypto(
        self,
        jws_key_file: pathlib.Path,
        jwe_key_file: pathlib.Path,
    ) -> None:
        try:
            with open(jws_key_file, 'rb') as kf:
                self._jws_key = jwk.JWK.from_pem(kf.read())
        except Exception as e:
            raise StartupError(f"cannot load JWS key: {e}") from e

        if (
            not self._jws_key.has_public
            or self._jws_key['kty'] not in {"RSA", "EC"}
        ):
            raise StartupError(
                f"the provided JWS key file does not "
                f"contain a valid RSA or EC public key")

        try:
            with open(jwe_key_file, 'rb') as kf:
                self._jwe_key = jwk.JWK.from_pem(kf.read())
        except Exception as e:
            raise StartupError(f"cannot load JWE key: {e}") from e

        if (
            not self._jwe_key.has_private
            or self._jwe_key['kty'] not in {"RSA", "EC"}
        ):
            raise StartupError(
                f"the provided JWE key file does not "
                f"contain a valid RSA or EC private key")

    def init_jwcrypto(
        self,
        jws_key_file: pathlib.Path,
        jwe_key_file: pathlib.Path,
        jws_keys_newly_generated: bool,
        jwe_keys_newly_generated: bool,
    ) -> None:
        self.load_jwcrypto(jws_key_file, jwe_key_file)
        self._jws_keys_newly_generated = jws_keys_newly_generated
        self._jwe_keys_newly_generated = jwe_keys_newly_generated

    def get_jws_key(self) -> jwk.JWK | None:
        return self._jws_key

    def get_jwe_key(self) -> jwk.JWK | None:
        return self._jwe_key

    async def _stop_servers(self, servers):
        async with taskgroup.TaskGroup() as g:
            for srv in servers:
                srv.close()
                g.create_task(srv.wait_closed())

    async def start(self):
        self._stop_evt.clear()
        assert self._task_group is None
        self._task_group = taskgroup.TaskGroup()
        await self._task_group.__aenter__()
        self._accept_new_tasks = True

        self._http_request_logger = self.create_task(
            self._request_stats_logger(), interruptable=True
        )

        await self._cluster.start_watching(self)
        await self._create_compiler_pool()

        if self._startup_script and self._new_instance:
            await binary.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )

        self._servers, actual_port, listen_addrs = await self._start_servers(
            (await _resolve_interfaces(self._listen_hosts))[0],
            self._listen_port,
            sockets=self._listen_sockets,
        )
        self._listen_hosts = listen_addrs
        self._listen_port = actual_port

        self._accepting_connections = True
        self._serving = True

        if self._echo_runtime_info:
            ri = {
                "port": self._listen_port,
                "runstate_dir": str(self._runstate_dir),
                "tls_cert_file": self._tls_cert_file,
            }
            print(f'\nEDGEDB_SERVER_DATA:{json.dumps(ri)}\n', flush=True)

        for status_sink in self._status_sinks:
            status = {
                "listen_addrs": listen_addrs,
                "port": self._listen_port,
                "socket_dir": str(self._runstate_dir),
                "main_pid": os.getpid(),
                "tenant_id": self._tenant_id,
                "tls_cert_file": self._tls_cert_file,
                "tls_cert_newly_generated": self._tls_cert_newly_generated,
                "jws_keys_newly_generated": self._jws_keys_newly_generated,
                "jwe_keys_newly_generated": self._jwe_keys_newly_generated,
            }
            status_sink(f'READY={json.dumps(status)}')

        if self._auto_shutdown_after > 0:
            self._auto_shutdown_handler = self.__loop.call_later(
                self._auto_shutdown_after, self.request_auto_shutdown)

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
        self._accepting_connections = False
        self._stop_evt.set()

    async def stop(self):
        try:
            self._serving = False
            self._accept_new_tasks = False

            if self._idle_gc_handler is not None:
                self._idle_gc_handler.cancel()
                self._idle_gc_handler = None

            self._cluster.stop_watching()
            if self._http_request_logger is not None:
                self._http_request_logger.cancel()

            for handle in self._tls_certs_watch_handles:
                handle.cancel()
            self._tls_certs_watch_handles.clear()

            await self._stop_servers(self._servers.values())
            self._servers = {}

            for conn in self._binary_conns:
                conn.stop()
            self._binary_conns.clear()

            if self._task_group is not None:
                tg = self._task_group
                self._task_group = None
                await tg.__aexit__(*sys.exc_info())

            await self._destroy_compiler_pool()

        finally:
            if self.__sys_pgcon is not None:
                self.__sys_pgcon.terminate()
                self.__sys_pgcon = None
            self._sys_pgcon_waiter = None

    def create_task(self, coro, *, interruptable):
        # Interruptable tasks are regular asyncio tasks that may be interrupted
        # randomly in the middle when the event loop stops; while tasks with
        # interruptable=False are always awaited before the server stops, so
        # that e.g. all finally blocks get a chance to execute in those tasks.
        # Therefore, it is an error trying to create a task while the server is
        # not expecting one, so always couple the call with an additional check
        if self._accept_new_tasks:
            if interruptable:
                rv = self.__loop.create_task(coro)
            else:
                rv = self._task_group.create_task(coro)

            # Keep a strong reference of the created Task
            self._tasks.add(rv)
            rv.add_done_callback(self._tasks.discard)

            return rv
        else:
            # Hint: add `if server._accept_new_tasks` before `.create_task()`
            raise RuntimeError("task cannot be created at this time")

    async def serve_forever(self):
        await self._stop_evt.wait()

    async def get_auth_method(
        self,
        user: str,
        transport: srvargs.ServerConnTransport,
    ) -> Any:
        authlist = self._sys_auth

        if authlist:
            for auth in authlist:
                match = (
                    (user in auth.user or '*' in auth.user)
                    and (
                        not auth.method.transports
                        or transport in auth.method.transports
                    )
                )

                if match:
                    return auth.method

        default_method = self._default_auth_method.get(transport)
        auth_type = config.get_settings().get_type_by_name(
            default_method.value)
        return auth_type()

    def is_database_connectable(self, dbname: str) -> bool:
        return (
            dbname != defines.EDGEDB_TEMPLATE_DB
            and dbname not in self._block_new_connections
        )

    def get_sys_query(self, key):
        return self._sys_queries[key]

    def get_instance_data(self, key):
        return self._instance_data[key]

    @functools.lru_cache
    def get_backend_runtime_params(self) -> Any:
        return self._cluster.get_runtime_params()

    def set_pg_unavailable_msg(self, msg):
        if msg is None or self._pg_unavailable_msg is None:
            self._pg_unavailable_msg = msg

    def on_switch_over(self):
        # Bumping this serial counter will "cancel" all pending connections
        # to the old master.
        self._ha_master_serial += 1

        if self._accept_new_tasks:
            self.create_task(
                self._pg_pool.prune_all_connections(), interruptable=True
            )

        if self.__sys_pgcon is None:
            # Assume a reconnect task is already running, now that we know the
            # new master is likely ready, let's just give the task a push.
            self._sys_pgcon_reconnect_evt.set()
        else:
            # Brutally close the sys_pgcon to the old master - this should
            # trigger a reconnect task.
            self.__sys_pgcon.abort()
        if self._backend_adaptive_ha is not None:
            # Switch to FAILOVER if adaptive HA is enabled
            self._backend_adaptive_ha.set_state_failover(
                call_on_switch_over=False
            )

    def get_active_pgcon_num(self) -> int:
        return (
            self._pg_pool.current_capacity - self._pg_pool.get_pending_conns()
        )

    def get_debug_info(self):
        """Used to render the /server-info endpoint in dev/test modes.

        Some tests depend on the exact layout of the returned structure.
        """

        def serialize_config(cfg):
            return {name: value.value for name, value in cfg.items()}

        obj = dict(
            params=dict(
                max_backend_connections=self._max_backend_connections,
                suggested_client_pool_size=self._suggested_client_pool_size,
                tenant_id=self._tenant_id,
                dev_mode=self._devmode,
                test_mode=self._testmode,
                default_auth_method=str(self._default_auth_method),
                listen_hosts=self._listen_hosts,
                listen_port=self._listen_port,
            ),
            instance_config=serialize_config(self._dbindex.get_sys_config()),
            user_roles=self._roles,
            pg_addr=self._pg_addr,
            pg_pool=self._pg_pool._build_snapshot(now=time.monotonic()),
            compiler_pool=dict(
                worker_pids=list(self._compiler_pool._workers.keys()),
                template_pid=self._compiler_pool.get_template_pid(),
            ),
        )

        dbs = {}
        for db in self._dbindex.iter_dbs():
            if db.name in defines.EDGEDB_SPECIAL_DBS:
                continue

            dbs[db.name] = dict(
                name=db.name,
                dbver=db.dbver,
                config=serialize_config(db.db_config),
                extensions=sorted(db.extensions),
                query_cache_size=db.get_query_cache_size(),
                connections=[
                    dict(
                        in_tx=view.in_tx(),
                        in_tx_error=view.in_tx_error(),
                        config=serialize_config(view.get_session_config()),
                        module_aliases=view.get_modaliases(),
                    )
                    for view in db.iter_views()
                ],
            )

        obj['databases'] = dbs

        return obj


def _cleanup_wildcard_addrs(
    hosts: Sequence[str]
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
    hosts: Sequence[str]
) -> Tuple[Sequence[str], bool, bool]:

    async with taskgroup.TaskGroup() as g:
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
