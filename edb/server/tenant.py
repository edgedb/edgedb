#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *

import asyncio
import contextlib
import functools
import json
import logging
import struct
import time

import immutables

from edb import errors
from edb.common import retryloop
from edb.common import taskgroup
from edb.schema import reflection as s_refl
from edb.schema import roles as s_role
from edb.schema import schema as s_schema

from . import args as srvargs
from . import config
from . import connpool
from . import dbview
from . import defines
from . import metrics
from . import pgcon
from .ha import adaptive as adaptive_ha
from .ha import base as ha_base
from .pgcon import errors as pgcon_errors

if TYPE_CHECKING:
    from edb.pgsql import params as pgparams

    from . import pgcluster
    from . import server as edbserver
    from .config.ops import SettingsMap


logger = logging.getLogger('edb.server')


class RoleDescriptor(TypedDict):
    superuser: bool
    name: str
    password: str | None


class Tenant(ha_base.ClusterProtocol):
    _server: edbserver.Server
    _cluster: pgcluster.BaseCluster
    _tenant_id: str
    _instance_name: str
    _instance_data: Mapping[str, str]
    _dbindex: dbview.DatabaseIndex | None

    __sys_pgcon: pgcon.PGConnection | None
    _sys_pgcon_waiter: asyncio.Lock
    _sys_pgcon_ready_evt: asyncio.Event
    _sys_pgcon_reconnect_evt: asyncio.Event
    _max_backend_connections: int
    _suggested_client_pool_size: int
    _pg_pool: connpool.Pool
    _pg_unavailable_msg: str | None
    _stmt_cache_size: int | None

    _ha_master_serial: int
    _backend_adaptive_ha: adaptive_ha.AdaptiveHASupport | None

    # A set of databases that should not accept new connections.
    _block_new_connections: set[str]
    _report_config_data: dict[defines.ProtocolVersion, bytes]

    _roles: Mapping[str, RoleDescriptor]
    _sys_auth: Tuple[Any, ...]

    def __init__(
        self,
        cluster: pgcluster.BaseCluster,
        *,
        instance_name: str,
        max_backend_connections: int,
        backend_adaptive_ha: bool = False,
    ):
        self._cluster = cluster
        self._tenant_id = self.get_backend_runtime_params().tenant_id
        self._instance_name = instance_name
        self._instance_data = immutables.Map()

        # Never use `self.__sys_pgcon` directly; get it via
        # `async with self._use_sys_pgcon()`.
        self.__sys_pgcon = None

        # Increase-only counter to reject outdated attempts to connect
        self._ha_master_serial = 0
        if backend_adaptive_ha:
            self._backend_adaptive_ha = adaptive_ha.AdaptiveHASupport(self)
        else:
            self._backend_adaptive_ha = None

        self._max_backend_connections = max_backend_connections
        self._suggested_client_pool_size = max(
            min(max_backend_connections,
                defines.MAX_SUGGESTED_CLIENT_POOL_SIZE),
            defines.MIN_SUGGESTED_CLIENT_POOL_SIZE
        )
        self._pg_pool = connpool.Pool(
            connect=self._pg_connect,
            disconnect=self._pg_disconnect,
            # 1 connection is reserved for the system DB
            max_capacity=max_backend_connections - 1,
        )
        self._pg_unavailable_msg = None
        self._block_new_connections = set()
        self._report_config_data = {}
        self._stmt_cache_size = None

        # DB state will be initialized in Server.init().
        self._dbindex = None

        self._roles = immutables.Map()
        self._sys_auth = tuple()

    def set_server(self, server: edbserver.Server) -> None:
        self._server = server

    def on_switch_over(self):
        # Bumping this serial counter will "cancel" all pending connections
        # to the old master.
        self._ha_master_serial += 1

        if self._server._accept_new_tasks:
            self._server.create_task(
                self._pg_pool.prune_all_connections(),
                interruptable=True,
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

    @property
    def server(self) -> edbserver.Server:
        return self._server

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def max_backend_connections(self) -> int:
        return self._max_backend_connections

    @property
    def suggested_client_pool_size(self) -> int:
        return self._suggested_client_pool_size

    def get_pg_dbname(self, dbname: str) -> str:
        return self._cluster.get_db_name(dbname)

    def get_pgaddr(self) -> Dict[str, Any]:
        return self._cluster.get_connection_spec()

    @functools.lru_cache
    def get_backend_runtime_params(self) -> pgparams.BackendRuntimeParams:
        return self._cluster.get_runtime_params()

    def get_instance_name(self) -> str:
        return self._instance_name

    def get_instance_data(self, key: str) -> str:
        return self._instance_data[key]

    def get_report_config_data(
        self,
        protocol_version: defines.ProtocolVersion,
    ) -> bytes:
        if protocol_version >= (2, 0):
            return self._report_config_data[(2, 0)]
        else:
            return self._report_config_data[(1, 0)]

    def get_global_schema(self) -> s_schema.FlatSchema:
        assert self._dbindex is not None
        return self._dbindex.get_global_schema()

    def get_db(self, *, dbname: str) -> dbview.Database:
        assert self._dbindex is not None
        return self._dbindex.get_db(dbname)

    def maybe_get_db(self, *, dbname: str) -> dbview.Database:
        assert self._dbindex is not None
        return self._dbindex.maybe_get_db(dbname)

    def get_roles(self) -> Mapping[str, RoleDescriptor]:
        return self._roles

    def fetch_roles(self) -> None:
        global_schema = self.get_global_schema()

        roles = {}
        for role in global_schema.get_objects(type=s_role.Role):
            role_name = str(role.get_name(global_schema))
            roles[role_name] = RoleDescriptor(
                name=role_name,
                superuser=role.get_superuser(global_schema),
                password=role.get_password(global_schema),
            )

        self._roles = immutables.Map(roles)

    async def init(self) -> None:
        self.__sys_pgcon = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
        self._sys_pgcon_waiter = asyncio.Lock()
        self._sys_pgcon_ready_evt = asyncio.Event()
        self._sys_pgcon_reconnect_evt = asyncio.Event()

        async with self._use_sys_pgcon() as syscon:
            result = await syscon.sql_fetch_val(b'''\
                SELECT json::json FROM edgedbinstdata.instdata
                WHERE key = 'instancedata';
            ''')
            self._instance_data = immutables.Map(json.loads(result))

    async def start(self) -> None:
        global_schema = await self.introspect_global_schema()
        sys_config = await self._load_sys_config()
        default_sysconfig = await self._load_sys_config('sysconfig_default')
        await self._load_reported_config()

        self._dbindex = dbview.DatabaseIndex(
            self._server,
            self,
            std_schema=self._server._std_schema,
            global_schema=global_schema,
            sys_config=sys_config,
            default_sysconfig=default_sysconfig,
        )

        self.fetch_roles()
        await self._introspect_dbs()

        # Now, once all DBs have been introspected, start listening on
        # any notifications about schema/roles/etc changes.
        assert self.__sys_pgcon is not None
        await self.__sys_pgcon.listen_for_sysevent()
        self.__sys_pgcon.mark_as_system_db()
        self._sys_pgcon_ready_evt.set()

        self._populate_sys_auth()

        self._stmt_cache_size = config.lookup(
            '_pg_prepared_statement_cache_size', sys_config
        )

    def stop(self) -> None:
        if self.__sys_pgcon is not None:
            self.__sys_pgcon.terminate()
            self.__sys_pgcon = None
        del self._sys_pgcon_waiter

    async def _pg_connect(self, dbname: str) -> pgcon.PGConnection:
        ha_serial = self._ha_master_serial
        if self.get_backend_runtime_params().has_create_database:
            pg_dbname = self.get_pg_dbname(dbname)
        else:
            pg_dbname = self.get_pg_dbname(defines.EDGEDB_SUPERUSER_DB)
        started_at = time.monotonic()
        try:
            rv = await pgcon.connect(
                self.get_pgaddr(), pg_dbname, self.get_backend_runtime_params()
            )
            if self._stmt_cache_size is not None:
                rv.set_stmt_cache_size(self._stmt_cache_size)
        except Exception:
            metrics.backend_connection_establishment_errors.inc()
            raise
        finally:
            metrics.backend_connection_establishment_latency.observe(
                time.monotonic() - started_at)
        if ha_serial == self._ha_master_serial:
            rv.set_server(self._server)
            rv.set_tenant(self)
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

    async def _pg_disconnect(self, conn: pgcon.PGConnection) -> None:
        metrics.current_backend_connections.dec()
        conn.terminate()

    @contextlib.asynccontextmanager
    async def _use_sys_pgcon(self) -> AsyncGenerator[pgcon.PGConnection, None]:
        if not self._server._initing and not self._server._serving:
            raise RuntimeError("EdgeDB server is not serving.")

        await self._sys_pgcon_waiter.acquire()

        if not self._server._initing and not self._server._serving:
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

        try:
            yield self.__sys_pgcon
        finally:
            self._sys_pgcon_waiter.release()

    def on_sys_pgcon_parameter_status_updated(
        self,
        name: str,
        value: str,
    ) -> None:
        try:
            if name == 'in_hot_standby' and value == 'on':
                # It is a strong evidence of failover if the sys_pgcon receives
                # a notification that in_hot_standby is turned on.
                self.on_sys_pgcon_failover_signal()
        except Exception:
            metrics.background_errors.inc(
                1.0, 'on_sys_pgcon_parameter_status_updated')
            raise

    def on_sys_pgcon_failover_signal(self) -> None:
        if not self._server._serving:
            return
        try:
            if self._backend_adaptive_ha is not None:
                # Switch to FAILOVER if adaptive HA is enabled
                self._backend_adaptive_ha.set_state_failover()
            elif getattr(self._cluster, '_ha_backend', None) is None:
                # If the server is not using an HA backend, nor has enabled the
                # adaptive HA monitoring, we still try to "switch over" by
                # disconnecting all pgcons if failover signal is received,
                # allowing reconnection to happen sooner.
                self.on_switch_over()
            # Else, the HA backend should take care of calling on_switch_over()
        except Exception:
            metrics.background_errors.inc(1.0, 'on_sys_pgcon_failover_signal')
            raise

    def on_sys_pgcon_connection_lost(self, exc: Exception | None) -> None:
        try:
            if not self._server._serving:
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
            if self._server._accept_new_tasks:
                self._server.create_task(
                    self._reconnect_sys_pgcon(), interruptable=True
                )
            self.on_pgcon_broken(True)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_sys_pgcon_connection_lost')
            raise

    async def _reconnect_sys_pgcon(self) -> None:
        try:
            conn = None
            while self._server._serving:
                try:
                    conn = await self._pg_connect(
                        defines.EDGEDB_SYSTEM_DB
                    )
                    break
                except OSError:
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

                if self._server._serving:
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

            if not self._server._serving:
                if conn is not None:
                    conn.abort()
                return

            assert conn is not None
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

    def on_pgcon_broken(self, is_sys_pgcon: bool = False) -> None:
        try:
            if self._backend_adaptive_ha:
                self._backend_adaptive_ha.on_pgcon_broken(is_sys_pgcon)
        except Exception:
            metrics.background_errors.inc(1.0, 'on_pgcon_broken')
            raise

    def on_pgcon_lost(self) -> None:
        try:
            if self._backend_adaptive_ha:
                self._backend_adaptive_ha.on_pgcon_lost()
        except Exception:
            metrics.background_errors.inc(1.0, 'on_pgcon_lost')
            raise

    def set_pg_unavailable_msg(self, msg: str | None) -> None:
        if msg is None or self._pg_unavailable_msg is None:
            self._pg_unavailable_msg = msg

    async def acquire_pgcon(self, dbname: str) -> pgcon.PGConnection:
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

    def release_pgcon(
        self,
        dbname: str,
        conn: pgcon.PGConnection,
        *,
        discard: bool = False,
    ) -> None:
        if not conn.is_healthy():
            if not discard:
                logger.warning('Released an unhealthy pgcon; discard now.')
            discard = True
        try:
            self._pg_pool.release(dbname, conn, discard=discard)
        except Exception:
            metrics.background_errors.inc(1.0, 'release_pgcon')
            raise

    def allow_database_connections(self, dbname: str) -> None:
        self._block_new_connections.discard(dbname)

    def is_database_connectable(self, dbname: str) -> bool:
        return (
            dbname != defines.EDGEDB_TEMPLATE_DB
            and dbname not in self._block_new_connections
        )

    async def ensure_database_not_connected(self, dbname: str) -> None:
        if self._server._dbindex and self._server._dbindex.count_connections(
            dbname
        ):
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
            await self._server._signal_sysevent(
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

    async def _acquire_intro_pgcon(
        self, dbname: str
    ) -> pgcon.PGConnection | None:
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

    async def introspect_user_schema(
        self,
        conn: pgcon.PGConnection,
        global_schema: s_schema.FlatSchema | None = None,
    ) -> s_schema.FlatSchema:
        json_data = await conn.sql_fetch_val(self._server._local_intro_query)

        base_schema = s_schema.ChainedSchema(
            self._server._std_schema,
            s_schema.FlatSchema(),
            global_schema or self.get_global_schema(),
        )

        return s_refl.parse_into(
            base_schema=base_schema,
            schema=s_schema.FlatSchema(),
            data=json_data,
            schema_class_layout=self._server._schema_class_layout,
        )

    async def introspect_db_config(
        self, conn: pgcon.PGConnection
    ) -> SettingsMap:
        result = await conn.sql_fetch_val(
            self._server.get_sys_query('dbconfig')
        )
        return config.from_json(config.get_settings(), result)

    async def introspect_extensions(self, dbname: str) -> None:
        logger.info("introspecting extensions for database '%s'", dbname)
        conn = await self._acquire_intro_pgcon(dbname)
        if not conn:
            return

        try:
            extensions = await self._introspect_extensions(conn)
        finally:
            self.release_pgcon(dbname, conn)

        if self._dbindex is not None:
            db = self._dbindex.maybe_get_db(dbname)
            if db is not None:
                db.extensions = extensions

    async def _introspect_extensions(
        self, conn: pgcon.PGConnection
    ) -> set[str]:
        extension_names_json = await conn.sql_fetch_val(
            b'''
                SELECT json_agg(name) FROM edgedb."_SchemaExtension";
            ''',
        )
        if extension_names_json:
            extensions = set(json.loads(extension_names_json))
        else:
            extensions = set()

        return extensions

    async def introspect_db(self, dbname: str) -> None:
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
            extensions = await self._introspect_extensions(conn)

            assert self._dbindex is not None
            self._dbindex.register_db(
                dbname,
                user_schema=user_schema,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
                extensions=extensions,
            )
        finally:
            self.release_pgcon(dbname, conn)

    async def _early_introspect_db(self, dbname: str) -> None:
        """We need to always introspect the extensions for each database.

        Otherwise, we won't know to accept connections for graphql or
        http, for example, until a native connection is made.
        """
        logger.info("introspecting extensions for database '%s'", dbname)

        conn = await self._acquire_intro_pgcon(dbname)
        if not conn:
            return

        try:
            assert self._dbindex is not None
            if not self._dbindex.has_db(dbname):
                extensions = await self._introspect_extensions(conn)
                # Re-check in case we have a concurrent introspection task.
                if not self._dbindex.has_db(dbname):
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

    async def _introspect_dbs(self) -> None:
        async with self._use_sys_pgcon() as syscon:
            dbnames = await self._server.get_dbnames(syscon)

        async with taskgroup.TaskGroup(name='introspect DB extensions') as g:
            for dbname in dbnames:
                # There's a risk of the DB being dropped by another server
                # between us building the list of databases and loading
                # information about them.
                g.create_task(self._early_introspect_db(dbname))

    async def _load_reported_config(self) -> None:
        async with self._use_sys_pgcon() as syscon:
            try:
                data = await syscon.sql_fetch_val(
                    self._server.get_sys_query('report_configs'),
                    use_prep_stmt=True,
                )

                for (
                    protocol_ver,
                    typedesc,
                ) in self._server._report_config_typedesc.items():
                    self._report_config_data[protocol_ver] = (
                        struct.pack('!L', len(typedesc)) +
                        typedesc +
                        struct.pack('!L', len(data)) +
                        data
                    )
            except Exception:
                metrics.background_errors.inc(1.0, 'load_reported_config')
                raise

    async def _load_sys_config(
        self,
        query_name: str = "sysconfig",
    ) -> SettingsMap:
        async with self._use_sys_pgcon() as syscon:
            query = self._server.get_sys_query(query_name)
            sys_config_json = await syscon.sql_fetch_val(query)

        return config.from_json(config.get_settings(), sys_config_json)

    async def introspect_global_schema(
        self,
        conn: pgcon.PGConnection | None = None,
    ) -> s_schema.FlatSchema:
        intro_query = self._server._global_intro_query
        if conn is not None:
            json_data = await conn.sql_fetch_val(intro_query)
        else:
            async with self._use_sys_pgcon() as syscon:
                json_data = await syscon.sql_fetch_val(intro_query)

        return s_refl.parse_into(
            base_schema=self._server._std_schema,
            schema=s_schema.FlatSchema(),
            data=json_data,
            schema_class_layout=self._server._schema_class_layout,
        )

    async def _reintrospect_global_schema(self) -> None:
        if not self._server._initing and not self._server._serving:
            logger.warning(
                "global-schema-changes event received during shutdown; "
                "ignoring."
            )
            return
        new_global_schema = await self.introspect_global_schema()
        assert self._dbindex is not None
        self._dbindex.update_global_schema(new_global_schema)
        self.fetch_roles()

    def _populate_sys_auth(self) -> None:
        assert self._dbindex is not None
        cfg = self._dbindex.get_sys_config()
        auth = config.lookup('auth', cfg) or ()
        self._sys_auth = tuple(sorted(auth, key=lambda a: a.priority))

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

        default_method = self._server._default_auth_method.get(transport)
        auth_type = config.get_settings().get_type_by_name(
            default_method.value)
        return auth_type()

    async def new_dbview(
        self,
        *,
        dbname: str,
        query_cache: bool,
        protocol_version: defines.ProtocolVersion,
    ) -> dbview.DatabaseConnectionView:
        db = self.get_db(dbname=dbname)
        await db.introspection()
        assert self._dbindex is not None
        return self._dbindex.new_view(
            dbname, query_cache=query_cache, protocol_version=protocol_version
        )

    def remove_dbview(self, dbview_: dbview.DatabaseConnectionView) -> None:
        assert self._dbindex is not None
        return self._dbindex.remove_view(dbview_)

    def _reload_stmt_cache_size(self) -> None:
        assert self._dbindex is not None
        size = config.lookup(
            '_pg_prepared_statement_cache_size', self._dbindex.get_sys_config()
        )
        self._stmt_cache_size = size
        for conn in self._pg_pool.iterate_connections():
            conn.set_stmt_cache_size(size)

    def _schedule_reported_config_if_needed(self, setting_name: str) -> None:
        setting = config.get_settings()[setting_name]
        if setting.report and self._server._accept_new_tasks:
            self._server.create_task(
                self._load_reported_config(), interruptable=True)
