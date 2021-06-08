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
from typing import *

import asyncio
import binascii
import collections
import json
import logging
import os
import pickle
import socket
import stat
import sys
import uuid

import immutables

from edb import errors

from edb.common import devmode
from edb.common import taskgroup
from edb.common import windowedsum

from edb.schema import reflection as s_refl
from edb.schema import roles as s_role
from edb.schema import schema as s_schema

from edb.edgeql import parser as ql_parser

from edb.server import args as srvargs
from edb.server import cache
from edb.server import config
from edb.server import connpool
from edb.server import compiler_pool
from edb.server import defines
from edb.server import protocol
from edb.server.protocol import binary  # type: ignore
from edb.server import pgcon
from edb.server.pgcon import errors as pgcon_errors

from . import dbview


logger = logging.getLogger('edb.server')
log_metrics = logging.getLogger('edb.server.metrics')


class RoleDescriptor(TypedDict):
    superuser: bool
    name: str
    password: str


class Server:

    _sys_pgcon: Optional[pgcon.PGConnection]

    _roles: Mapping[str, RoleDescriptor]
    _instance_data: Mapping[str, str]
    _sys_queries: Mapping[str, str]
    _local_intro_query: bytes
    _global_intro_query: bytes

    _std_schema: s_schema.Schema
    _refl_schema: s_schema.Schema
    _schema_class_layout: s_refl.SchemaTypeLayout

    _sys_pgcon_waiter: asyncio.Lock
    _servers: List[asyncio.AbstractServer]

    _task_group: Optional[taskgroup.TaskGroup]
    _binary_conns: Set[binary.EdgeConnection]

    def __init__(
        self,
        *,
        cluster,
        runstate_dir,
        internal_runstate_dir,
        max_backend_connections,
        compiler_pool_size,
        nethost,
        netport,
        auto_shutdown_after: float = -1,
        echo_runtime_info: bool = False,
        status_sink: Optional[Callable[[str], None]] = None,
        startup_script: Optional[srvargs.StartupScript] = None,
    ):

        self._loop = asyncio.get_running_loop()

        # Used to tag PG notifications to later disambiguate them.
        self._server_id = str(uuid.uuid4())

        self._serving = False
        self._initing = False

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

        # DB state will be initialized in init().
        self._dbindex = None

        self._runstate_dir = runstate_dir
        self._internal_runstate_dir = internal_runstate_dir
        self._max_backend_connections = max_backend_connections
        self._compiler_pool = None
        self._compiler_pool_size = compiler_pool_size

        self._listen_host = nethost
        self._listen_port = netport

        self._sys_auth: Tuple[Any, ...] = tuple()

        # Shutdown the server after the last management
        # connection has disconnected
        # and there have been no new connections for n seconds
        self._auto_shutdown_after = auto_shutdown_after
        self._auto_shutdown_handler = None

        self._echo_runtime_info = echo_runtime_info
        self._status_sink = status_sink

        self._startup_script = startup_script

        # Never use `self.__sys_pgcon` directly; get it via
        # `await self._acquire_sys_pgcon()`.
        self.__sys_pgcon = None

        self._roles = immutables.Map()
        self._instance_data = immutables.Map()
        self._sys_queries = immutables.Map()

        self._devmode = devmode.is_in_dev_mode()

        self._binary_proto_id_counter = 0
        self._binary_conns = set()
        self._accepting_connections = False

        self._servers = []

        self._http_query_cache = cache.StatementsCache(
            maxsize=defines.HTTP_PORT_QUERY_CACHE_SIZE)

        self._http_last_minute_requests = windowedsum.WindowedSum()
        self._http_request_logger = None

        self._task_group = None
        self._stop_evt = asyncio.Event()

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

    def get_listen_port(self):
        return self._listen_port

    def get_loop(self):
        return self._loop

    def in_dev_mode(self):
        return self._devmode

    def get_pg_dbname(self, dbname: str) -> str:
        return self._cluster.get_db_name(dbname)

    def on_binary_client_connected(self) -> str:
        self._binary_proto_id_counter += 1

        if self._auto_shutdown_handler:
            self._auto_shutdown_handler.cancel()
            self._auto_shutdown_handler = None

        return str(self._binary_proto_id_counter)

    def on_binary_client_authed(self, conn):
        self._binary_conns.add(conn)
        self._report_connections(event='opened')

    def on_binary_client_disconnected(self, conn):
        self._binary_conns.discard(conn)
        self._report_connections(event="closed")

        if not self._binary_conns and self._auto_shutdown_after >= 0:

            def shutdown():
                self._accepting_connections = False
                self._stop_evt.set()

            self._auto_shutdown_handler = self._loop.call_later(
                self._auto_shutdown_after, shutdown)

    def _report_connections(self, *, event: str) -> None:
        log_metrics.info(
            "%s a connection; open_count=%d",
            event,
            len(self._binary_conns),
        )

    async def _pg_connect(self, dbname):
        pg_dbname = self.get_pg_dbname(dbname)
        return await pgcon.connect(
            self._get_pgaddr(), pg_dbname, self._tenant_id)

    async def _pg_disconnect(self, conn):
        conn.terminate()

    async def init(self):
        self._initing = True
        try:
            self.__sys_pgcon = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
            self._sys_pgcon_waiter = asyncio.Lock()

            await self._load_instance_data()

            global_schema = await self.introspect_global_schema()
            sys_config = await self.load_sys_config()

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
            await self.__sys_pgcon.set_server(self)

            self._populate_sys_auth()

            if not self._listen_host:
                self._listen_host = (
                    config.lookup('listen_addresses', sys_config)
                    or 'localhost'
                )

            if self._listen_port is None:
                self._listen_port = (
                    config.lookup('listen_port', sys_config)
                    or defines.EDGEDB_PORT
                )

            self._http_request_logger = asyncio.create_task(
                self._request_stats_logger()
            )

        finally:
            self._initing = False

    async def _create_compiler_pool(self):
        self._compiler_pool = await compiler_pool.create_compiler_pool(
            pool_size=self._compiler_pool_size,
            dbindex=self._dbindex,
            runstate_dir=self._internal_runstate_dir,
            backend_runtime_params=self.get_backend_runtime_params(),
            std_schema=self._std_schema,
            refl_schema=self._refl_schema,
            schema_class_layout=self._schema_class_layout,
        )

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

    def get_db(self, *, dbname: str):
        assert self._dbindex is not None
        return self._dbindex.get_db(dbname)

    def maybe_get_db(self, *, dbname: str):
        assert self._dbindex is not None
        return self._dbindex.maybe_get_db(dbname)

    def new_dbview(self, *, dbname, user, query_cache):
        return self._dbindex.new_view(
            dbname, user=user, query_cache=query_cache)

    def remove_dbview(self, dbview):
        return self._dbindex.remove_view(dbview)

    def get_global_schema(self):
        return self._dbindex.get_global_schema()

    def get_compilation_system_config(self):
        return self._dbindex.get_compilation_system_config()

    async def acquire_pgcon(self, dbname):
        return await self._pg_pool.acquire(dbname)

    def release_pgcon(self, dbname, conn, *, discard=False):
        if not conn.is_healthy_to_go_back_to_pool():
            # TODO: Add warning. This shouldn't happen.
            discard = True
        self._pg_pool.release(dbname, conn, discard=discard)

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

    async def introspect_db(
        self, dbname, *, refresh=False, skip_dropped=False
    ):
        try:
            conn = await self.acquire_pgcon(dbname)
        except pgcon_errors.BackendError as e:
            if skip_dropped and e.fields['C'] == '3D000':
                # 3D000 - INVALID CATALOG NAME, database does not exist
                logger.warning(
                    "Detected concurrently-dropped database %s; skipping.",
                    dbname,
                )
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

            self._dbindex.register_db(
                dbname,
                user_schema=user_schema,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
                refresh=refresh,
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
                g.create_task(self.introspect_db(dbname, skip_dropped=True))

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
        finally:
            self._release_sys_pgcon()

    def get_roles(self):
        return self._roles

    async def _restart_servers_new_addr(self, nethost, netport):
        if not netport:
            raise RuntimeError('cannot restart without network port specified')
        old_servers = self._servers
        self._servers, _ = await self._start_servers(nethost, netport)
        self._listen_host = nethost
        self._listen_port = netport
        await self._stop_servers(old_servers)

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
        assert self._dbindex is not None
        self._dbindex.unregister_db(dbname)

    async def _on_system_config_add(self, setting_name, value):
        # CONFIGURE SYSTEM INSERT ConfigObject;
        pass

    async def _on_system_config_rem(self, setting_name, value):
        # CONFIGURE SYSTEM RESET ConfigObject;
        pass

    async def _on_system_config_set(self, setting_name, value):
        # CONFIGURE SYSTEM SET setting_name := value;
        if setting_name == 'listen_addresses':
            await self._restart_servers_new_addr(value, self._listen_port)

        elif setting_name == 'listen_port':
            await self._restart_servers_new_addr(self._listen_host, value)

    async def _on_system_config_reset(self, setting_name):
        # CONFIGURE SYSTEM RESET setting_name;
        if setting_name == 'listen_addresses':
            await self._restart_servers_new_addr(
                'localhost', self._listen_port)

        elif setting_name == 'listen_port':
            await self._restart_servers_new_addr(
                self._listen_host, defines.EDGEDB_PORT)

    async def _after_system_config_add(self, setting_name, value):
        # CONFIGURE SYSTEM INSERT ConfigObject;
        if setting_name == 'auth':
            self._populate_sys_auth()

    async def _after_system_config_rem(self, setting_name, value):
        # CONFIGURE SYSTEM RESET ConfigObject;
        if setting_name == 'auth':
            self._populate_sys_auth()

    async def _after_system_config_set(self, setting_name, value):
        # CONFIGURE SYSTEM SET setting_name := value;
        pass

    async def _after_system_config_reset(self, setting_name):
        # CONFIGURE SYSTEM RESET setting_name;
        pass

    async def _acquire_sys_pgcon(self):
        if not self._initing and not self._serving:
            return None

        await self._sys_pgcon_waiter.acquire()

        if not self._initing and not self._serving:
            self._sys_pgcon_waiter.release()
            return None

        if (self.__sys_pgcon is None or
                not self.__sys_pgcon.is_healthy_to_go_back_to_pool()):
            self.__sys_pgcon.abort()
            self.__sys_pgcon = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
            await self.__sys_pgcon.set_server(self)

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
            await self._cancel_pgcon_operation(pgcon)
        finally:
            self.release_pgcon(dbname, pgcon, discard=True)

    async def _signal_sysevent(self, event, **kwargs):
        pgcon = await self._acquire_sys_pgcon()
        if pgcon is None:
            # No pgcon means that the server is going down.  This is very
            # likely if we are doing "run_startup_script_and_exit()",
            # but is also possible if the server was shut down with
            # this coroutine as a background task in flight.
            return

        try:
            await pgcon.signal_sysevent(event, **kwargs)
        finally:
            self._release_sys_pgcon()

    def _on_remote_ddl(self, dbname):
        # Triggered by a postgres notification event 'schema-changes'
        # on the __edgedb_sysevent__ channel
        self._loop.create_task(
            self.introspect_db(dbname, refresh=True)
        )

    def _on_remote_database_config_change(self, dbname):
        # Triggered by a postgres notification event 'database-config-changes'
        # on the __edgedb_sysevent__ channel
        pass

    def _on_remote_system_config_change(self):
        # Triggered by a postgres notification event 'ystem-config-changes'
        # on the __edgedb_sysevent__ channel
        pass

    def _on_global_schema_change(self):
        self._loop.create_task(self._reintrospect_global_schema())

    def _on_sys_pgcon_connection_lost(self):
        if not self._serving:
            # The server is shutting down.
            return
        self.__sys_pgcon = None

        async def reconnect():
            conn = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
            if self.__sys_pgcon is not None or not self._serving:
                conn.abort()
                return

            self.__sys_pgcon = conn
            await self.__sys_pgcon.set_server(self)

        self._loop.create_task(reconnect())

    async def run_startup_script_and_exit(self):
        """Run the script specified in *startup_script* and exit immediately"""
        if self._startup_script is None:
            raise AssertionError('startup script is not defined')
        await self._create_compiler_pool()
        try:
            ql_parser.preload()
            await binary.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )
        finally:
            await self._destroy_compiler_pool()

    async def _start_servers(self, host, port):
        nethost = await _fix_localhost(host)

        tcp_srv = await self._loop.create_server(
            lambda: protocol.HttpProtocol(self),
            host=nethost, port=port)

        if port == 0:
            port = tcp_srv.sockets[0].getsockname()[1]

        try:
            unix_sock_path = os.path.join(
                self._runstate_dir, f'.s.EDGEDB.{port}')
            unix_srv = await self._loop.create_unix_server(
                lambda: protocol.HttpProtocol(self),
                unix_sock_path)
        except Exception:
            tcp_srv.close()
            await tcp_srv.wait_closed()
            raise

        try:
            admin_unix_sock_path = os.path.join(
                self._runstate_dir, f'.s.EDGEDB.admin.{port}')
            admin_unix_srv = await self._loop.create_unix_server(
                lambda: protocol.HttpProtocol(self, external_auth=True),
                admin_unix_sock_path)
            os.chmod(admin_unix_sock_path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            tcp_srv.close()
            await tcp_srv.wait_closed()
            unix_srv.close()
            await unix_srv.wait_closed()
            raise

        servers = []

        servers.append(tcp_srv)
        if len(nethost) > 1:
            host_str = f"{{{', '.join(nethost)}}}"
        else:
            host_str = next(iter(nethost))

        logger.info('Serving on %s:%s', host_str, port)
        servers.append(unix_srv)
        logger.info('Serving on %s', unix_sock_path)
        servers.append(admin_unix_srv)
        logger.info('Serving admin on %s', admin_unix_sock_path)

        return servers, port

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

        await self._create_compiler_pool()

        # Make sure that EdgeQL parser is preloaded; edgecon might use
        # it to restore config values.
        ql_parser.preload()

        if self._startup_script:
            await binary.EdgeConnection.run_script(
                server=self,
                database=self._startup_script.database,
                user=self._startup_script.user,
                script=self._startup_script.text,
            )

        self._servers, actual_port = await self._start_servers(
            self._listen_host, self._listen_port)
        if self._listen_port == 0:
            self._listen_port = actual_port

        self._accepting_connections = True
        self._serving = True

        if self._echo_runtime_info:
            ri = {
                "port": self._listen_port,
                "runstate_dir": str(self._runstate_dir),
            }
            print(f'\nEDGEDB_SERVER_DATA:{json.dumps(ri)}\n', flush=True)

        if self._status_sink is not None:
            status = {
                "port": self._listen_port,
                "socket_dir": str(self._runstate_dir),
                "main_pid": os.getpid(),
                "tenant_id": self._tenant_id,
            }
            self._status_sink(f'READY={json.dumps(status)}')

    async def stop(self):
        try:
            self._serving = False

            if self._http_request_logger is not None:
                self._http_request_logger.cancel()

            await self._stop_servers(self._servers)
            self._servers = []

            for conn in self._binary_conns:
                conn.stop()
            self._binary_conns = set()

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

    def create_task(self, coro):
        if self._serving:
            return self._task_group.create_task(coro)

    async def serve_forever(self):
        await self._stop_evt.wait()

    async def get_auth_method(self, user):
        authlist = self._sys_auth

        if not authlist:
            default_method = 'SCRAM'
            return config.get_settings().get_type_by_name(default_method)()
        else:
            for auth in authlist:
                match = (
                    (user in auth.user or '*' in auth.user)
                )

                if match:
                    return auth.method

    def get_sys_query(self, key):
        return self._sys_queries[key]

    def get_instance_data(self, key):
        return self._instance_data[key]

    def get_backend_runtime_params(self) -> Any:
        return self._cluster.get_runtime_params()


async def _fix_localhost(host):
    # On many systems 'localhost' resolves to _both_ IPv4 and IPv6
    # addresses, even if the system is not capable of handling
    # IPv6 connections.  Due to the common nature of this issue
    # we explicitly disable the AF_INET6 component of 'localhost'.

    if (isinstance(host, str)
            or not isinstance(host, collections.abc.Iterable)):
        hosts = [host]
    else:
        hosts = list(host)

    try:
        idx = hosts.index('localhost')
    except ValueError:
        # No localhost, all good
        return hosts

    loop = asyncio.get_running_loop()
    localhost = await loop.getaddrinfo(
        'localhost',
        0,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        flags=socket.AI_PASSIVE,
        proto=0,
    )

    infos = [a for a in localhost if a[0] == socket.AF_INET]

    if not infos:
        # "localhost" did not resolve to an IPv4 address,
        # let create_server handle the situation.
        return hosts

    # Replace 'localhost' with explicitly resolved AF_INET addresses.
    hosts.pop(idx)
    for info in reversed(infos):
        addr, *_ = info[4]
        hosts.insert(idx, addr)

    return hosts
