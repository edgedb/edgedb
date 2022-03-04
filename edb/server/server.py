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
import binascii
import collections
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

from edb import errors

from edb.common import devmode
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

from . import dbview


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

    _std_schema: s_schema.Schema
    _refl_schema: s_schema.Schema
    _schema_class_layout: s_refl.SchemaTypeLayout

    _sys_pgcon_waiter: asyncio.Lock
    _servers: Mapping[str, asyncio.AbstractServer]

    _task_group: Optional[taskgroup.TaskGroup]
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
        default_auth_method: srvargs.ServerAuthMethod,
        admin_ui: bool = False,
        instance_name: Optional[str] = None,
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

        if not self._binary_conns and self._auto_shutdown_after >= 0:

            def shutdown():
                self._accepting_connections = False
                self._stop_evt.set()

            self._auto_shutdown_handler = self.__loop.call_later(
                self._auto_shutdown_after, shutdown)

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

    def new_dbview(self, *, dbname, query_cache):
        return self._dbindex.new_view(
            dbname, query_cache=query_cache)

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

        for _ in range(self._pg_pool.max_capacity + 1):
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
        syscon = await self._acquire_sys_pgcon()
        try:
            query = self.get_sys_query('sysconfig')
            sys_config_json = await syscon.parse_execute_json(
                query,
                b'__backend_sysconfig',
                dbver=0,
                use_prep_stmt=True,
                args=(),
            )
        finally:
            self._release_sys_pgcon()

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
            data = await syscon.parse_execute_extract_single_data_frame(
                self.get_sys_query('report_configs'),
                b'__report_configs',
                dbver=0, use_prep_stmt=True, args=(),
            )
            self._report_config_data = (
                struct.pack('!L', len(self._report_config_typedesc)) +
                self._report_config_typedesc +
                data
            )
        except Exception:
            metrics.background_errors.inc(1.0, 'load_reported_config')
            raise
        finally:
            self._release_sys_pgcon()

    async def introspect_global_schema(self, conn=None):
        if conn is not None:
            json_data = await conn.parse_execute_json(
                self._global_intro_query, b'__global_intro_db',
                dbver=0, use_prep_stmt=True, args=(),
            )
        else:
            syscon = await self._acquire_sys_pgcon()
            try:
                json_data = await syscon.parse_execute_json(
                    self._global_intro_query, b'__global_intro_db',
                    dbver=0, use_prep_stmt=True, args=(),
                )
            finally:
                self._release_sys_pgcon()

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
        json_data = await conn.parse_execute_json(
            self._local_intro_query, b'__local_intro_db',
            dbver=0, use_prep_stmt=True, args=(),
        )

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
                return
            else:
                raise

        try:
            user_schema = await self.introspect_user_schema(conn)

            reflection_cache_json = await conn.parse_execute_json(
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
                b'__reflection_cache',
                dbver=0,
                use_prep_stmt=True,
                args=(),
            )

            reflection_cache = immutables.Map({
                r['eql_hash']: tuple(r['argnames'])
                for r in json.loads(reflection_cache_json)
            })

            backend_ids_json = await conn.parse_execute_json(
                b'''
                SELECT
                    json_object_agg(
                        "id"::text,
                        "backend_id"
                    )::text
                FROM
                    edgedb."_SchemaType"
                ''',
                b'__backend_ids_fetch',
                dbver=0,
                use_prep_stmt=True,
                args=(),
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
                refresh=True,
            )
        finally:
            self.release_pgcon(dbname, conn)

    async def introspect_db_config(self, conn):
        query = self.get_sys_query('dbconfig')
        result = await conn.parse_execute_json(
            query,
            b'__backend_dbconfig',
            dbver=0,
            use_prep_stmt=True,
            args=(),
        )
        return config.from_json(config.get_settings(), result)

    async def _introspect_dbs(self):
        syscon = await self._acquire_sys_pgcon()
        try:
            dbs_query = self.get_sys_query('listdbs')
            json_data = await syscon.parse_execute_json(
                dbs_query, b'__listdbs',
                dbver=0, use_prep_stmt=True, args=(),
            )
            dbnames = json.loads(json_data)
        finally:
            self._release_sys_pgcon()

        async with taskgroup.TaskGroup(name='introspect DBs') as g:
            for dbname in dbnames:
                # There's a risk of the DB being dropped by another server
                # between us building the list of databases and loading
                # information about them.
                g.create_task(self.introspect_db(dbname))

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
        syscon = await self._acquire_sys_pgcon()
        try:
            result = await syscon.simple_query(b'''\
                SELECT json FROM edgedbinstdata.instdata
                WHERE key = 'instancedata';
            ''', ignore_data=False)
            self._instance_data = immutables.Map(
                json.loads(result[0][0].decode('utf-8')))

            result = await syscon.simple_query(b'''\
                SELECT json FROM edgedbinstdata.instdata
                WHERE key = 'sysqueries';
            ''', ignore_data=False)
            queries = json.loads(result[0][0].decode('utf-8'))
            self._sys_queries = immutables.Map(
                {k: q.encode() for k, q in queries.items()})

            result = await syscon.simple_query(b'''\
                SELECT text FROM edgedbinstdata.instdata
                WHERE key = 'local_intro_query';
            ''', ignore_data=False)
            self._local_intro_query = result[0][0]

            result = await syscon.simple_query(b'''\
                SELECT text FROM edgedbinstdata.instdata
                WHERE key = 'global_intro_query';
            ''', ignore_data=False)
            self._global_intro_query = result[0][0]

            result = await syscon.simple_query(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'stdschema';
            ''', ignore_data=False)
            try:
                data = binascii.a2b_hex(result[0][0][2:])
                self._std_schema = pickle.loads(data)
            except Exception as e:
                raise RuntimeError(
                    'could not load std schema pickle') from e

            result = await syscon.simple_query(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'reflschema';
            ''', ignore_data=False)
            try:
                data = binascii.a2b_hex(result[0][0][2:])
                self._refl_schema = pickle.loads(data)
            except Exception as e:
                raise RuntimeError(
                    'could not load refl schema pickle') from e

            result = await syscon.simple_query(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'classlayout';
            ''', ignore_data=False)
            try:
                data = binascii.a2b_hex(result[0][0][2:])
                self._schema_class_layout = pickle.loads(data)
            except Exception as e:
                raise RuntimeError(
                    'could not load schema class layout pickle') from e

            result = await syscon.simple_query(b'''\
                SELECT bin FROM edgedbinstdata.instdata
                WHERE key = 'report_configs_typedesc';
            ''', ignore_data=False)
            try:
                data = binascii.a2b_hex(result[0][0][2:])
                assert data is not None
                self._report_config_typedesc = data
            except Exception as e:
                raise RuntimeError(
                    'could not load report config typedesc') from e

        finally:
            self._release_sys_pgcon()

    def get_roles(self):
        return self._roles

    async def _restart_servers_new_addr(self, nethosts, netport):
        if not netport:
            raise RuntimeError('cannot restart without network port specified')
        nethosts = await _resolve_interfaces(nethosts)
        servers_to_stop = []
        servers = {}
        if self._listen_port == netport:
            hosts_to_start = [
                host for host in nethosts if host not in self._servers
            ]
            for host, srv in self._servers.items():
                if host == ADMIN_PLACEHOLDER or host in nethosts:
                    servers[host] = srv
                else:
                    servers_to_stop.append(srv)
            admin = False
        else:
            hosts_to_start = nethosts
            servers_to_stop = self._servers.values()
            admin = True

        if hosts_to_start:
            new_servers, *_ = await self._start_servers(
                hosts_to_start, netport, admin
            )
            servers.update(new_servers)
        self._servers = servers
        self._listen_hosts = nethosts
        self._listen_port = netport

        addrs = []
        unix_addr = None
        port = None
        for srv in servers_to_stop:
            for s in srv.sockets:
                addr = s.getsockname()
                if isinstance(addr, tuple):
                    addrs.append(addr)
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

    async def _ensure_database_not_connected(self, dbname: str):
        assert self._dbindex is not None

        if self._dbindex.count_connections(dbname):
            # If there are open EdgeDB connections to the `dbname` DB
            # just raise the error Postgres would have raised itself.
            raise errors.ExecutionError(
                f'database {dbname!r} is being accessed by other users')
        else:
            # If, however, there are no open EdgeDB connections, prune
            # all non-active postgres connection to the `dbname` DB.
            await self._pg_pool.prune_inactive_connections(dbname)

    def _on_after_drop_db(self, dbname: str):
        try:
            assert self._dbindex is not None
            self._dbindex.unregister_db(dbname)
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

    async def _acquire_sys_pgcon(self):
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

    async def _cancel_pgcon_operation(self, pgcon) -> bool:
        syscon = await self._acquire_sys_pgcon()
        try:
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
                # SIGINT.  Will throw an exception if the priveleges aren't
                # sufficient.
                result = await syscon.simple_query(
                    f'SELECT pg_cancel_backend({pgcon.backend_pid});'.encode(),
                    ignore_data=False
                )
            finally:
                pgcon.finish_pg_cancellation()

            return result[0][0] == b't'
        finally:
            self._release_sys_pgcon()

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
            await binary.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )
        finally:
            await self._destroy_compiler_pool()

    async def _start_server(
        self, host: str, port: int
    ) -> Optional[asyncio.AbstractServer]:
        proto_factory = lambda: protocol.HttpProtocol(
            self,
            self._sslctx,
            binary_endpoint_security=self._binary_endpoint_security,
            http_endpoint_security=self._http_endpoint_security,
        )

        try:
            return await self.__loop.create_server(
                proto_factory, host=host, port=port)
        except Exception as e:
            logger.warning(
                f"could not create listen socket for '{host}:{port}': {e}"
            )
            return None

    async def _start_admin_server(self, port: int) -> asyncio.AbstractServer:
        admin_unix_sock_path = os.path.join(
            self._runstate_dir, f'.s.EDGEDB.admin.{port}')
        assert len(admin_unix_sock_path) <= (
            defines.MAX_RUNSTATE_DIR_PATH
            + defines.MAX_UNIX_SOCKET_PATH_LENGTH
            + 1
        ), "admin Unix socket length exceeds maximum allowed"
        admin_unix_srv = await self.__loop.create_unix_server(
            lambda: binary.EdgeConnection(self, external_auth=True),
            admin_unix_sock_path
        )
        os.chmod(admin_unix_sock_path, stat.S_IRUSR | stat.S_IWUSR)
        logger.info('Serving admin on %s', admin_unix_sock_path)
        return admin_unix_srv

    async def _start_servers(self, hosts, port, admin=True):
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

        if not servers:
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

    def init_tls(
        self,
        tls_cert_file,
        tls_key_file,
        tls_cert_newly_generated,
    ):
        assert self._sslctx is None
        tls_password_needed = False

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
        self._tls_cert_file = str(tls_cert_file)
        self._tls_cert_newly_generated = tls_cert_newly_generated

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
            await binary.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )

        self._servers, actual_port, listen_addrs = await self._start_servers(
            await _resolve_interfaces(self._listen_hosts),
            self._listen_port,
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
            }
            status_sink(f'READY={json.dumps(status)}')

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
                return self.__loop.create_task(coro)
            else:
                return self._task_group.create_task(coro)
        else:
            # Hint: add `if server._accept_new_tasks` before `.create_task()`
            raise RuntimeError("task cannot be created at this time")

    async def serve_forever(self):
        await self._stop_evt.wait()

    async def get_auth_method(self, user):
        authlist = self._sys_auth

        if authlist:
            for auth in authlist:
                match = (
                    (user in auth.user or '*' in auth.user)
                )

                if match:
                    return auth.method

        auth_type = config.get_settings().get_type_by_name(
            self._default_auth_method)
        return auth_type()

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
                default_auth_method=self._default_auth_method,
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
            dbs[db.name] = dict(
                name=db.name,
                dbver=db.dbver,
                config=serialize_config(db.db_config),
                extensions=list(db.extensions.keys()),
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
) -> tuple[list[str], list[str]]:
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
        return (list(hosts), [])

    if ipv4_wc not in ipv4_hosts and ipv6_wc not in ipv6_hosts:
        return (list(hosts), [])

    if ipv4_wc in ipv4_hosts and ipv6_wc in ipv6_hosts:
        return (
            ['0.0.0.0', '::'],
            [str(a) for a in
                ((named_hosts | ipv4_hosts | ipv6_hosts) - {ipv4_wc, ipv6_wc})]
        )

    if ipv4_wc in ipv4_hosts:
        return (
            [str(a) for a in ({ipv4_wc} | ipv6_hosts)],
            [str(a) for a in ((named_hosts | ipv4_hosts) - {ipv4_wc})]
        )

    if ipv6_wc in ipv6_hosts:
        return (
            [str(a) for a in ({ipv6_wc} | ipv4_hosts)],
            [str(a) for a in ((named_hosts | ipv6_hosts) - {ipv6_wc})]
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


async def _resolve_interfaces(hosts: Sequence[str]) -> Sequence[str]:

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

    clean_addrs, rejected_addrs = _cleanup_wildcard_addrs(addrs)

    if rejected_addrs:
        logger.warning(
            "wildcard addresses found in listen_addresses; " +
            "discarding the other addresses: " +
            ", ".join(repr(h) for h in rejected_addrs)
        )

    return clean_addrs
