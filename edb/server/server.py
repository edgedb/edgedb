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
import json
import logging

import immutables

from edb import errors

from edb.common import taskgroup

from edb.edgeql import parser as ql_parser

from edb.server import config
from edb.server import connpool
from edb.server import defines
from edb.server import http
from edb.server import http_edgeql_port
from edb.server import http_graphql_port
from edb.server import notebook_port
from edb.server import mng_port
from edb.server import pgcon

from . import baseport
from . import dbview


logger = logging.getLogger('edb.server')


class StartupScript(NamedTuple):

    text: str
    database: str
    user: str


class RoleDescriptor(TypedDict):
    is_superuser: bool
    name: str
    password: str


class Server:

    _ports: List[baseport.Port]
    _sys_conf_ports: Dict[config.ConfigType, baseport.Port]
    _sys_pgcon: Optional[pgcon.PGConnection]

    _roles: Mapping[str, RoleDescriptor]
    _instance_data: Mapping[str, str]
    _sys_queries: Mapping[str, str]

    def __init__(
        self,
        *,
        loop,
        cluster,
        runstate_dir,
        internal_runstate_dir,
        max_backend_connections,
        nethost,
        netport,
        auto_shutdown: bool=False,
        echo_runtime_info: bool = False,
        max_protocol: Tuple[int, int],
        startup_script: Optional[StartupScript] = None,
    ):

        self._loop = loop

        self._serving = False

        self._cluster = cluster
        self._pg_addr = self._get_pgaddr()

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

        self._mgmt_port = None
        self._mgmt_host_addr = nethost
        self._mgmt_port_no = netport
        self._mgmt_protocol_max = max_protocol

        self._ports = []
        self._sys_conf_ports = {}
        self._sys_auth: Tuple[Any, ...] = tuple()

        # Shutdown the server after the last management
        # connection has disconnected
        self._auto_shutdown = auto_shutdown

        self._echo_runtime_info = echo_runtime_info

        self._startup_script = startup_script

        # Never use `self.__sys_pgcon` directly; get it via
        # `await self._acquire_sys_pgcon()`.
        self.__sys_pgcon = None
        self._sys_pgcon_waiters = None

        self._roles = immutables.Map()
        self._instance_data = immutables.Map()
        self._sys_queries = immutables.Map()

    async def _pg_connect(self, dbname):
        return await pgcon.connect(self._get_pgaddr(), dbname)

    async def _pg_disconnect(self, conn):
        conn.terminate()

    async def init(self):
        self.__sys_pgcon = await self._pg_connect(defines.EDGEDB_SYSTEM_DB)
        await self.__sys_pgcon.set_server(self)
        self._sys_pgcon_waiters = asyncio.Queue()
        self._sys_pgcon_waiters.put_nowait(self.__sys_pgcon)

        await self._load_instance_data()
        await self._load_sys_queries()
        await self._fetch_roles()
        self._dbindex = await dbview.DatabaseIndex.init(self)

        self._populate_sys_auth()

        cfg = self._dbindex.get_sys_config()

        if not self._mgmt_host_addr:
            self._mgmt_host_addr = (
                config.lookup('listen_addresses', cfg) or 'localhost')

        if not self._mgmt_port_no:
            self._mgmt_port_no = (
                config.lookup('listen_port', cfg) or defines.EDGEDB_PORT)

        self._mgmt_port = self._new_port(
            mng_port.ManagementPort,
            nethost=self._mgmt_host_addr,
            netport=self._mgmt_port_no,
            auto_shutdown=self._auto_shutdown,
            max_protocol=self._mgmt_protocol_max,
            startup_script=self._startup_script,
        )

    def _populate_sys_auth(self):
        cfg = self._dbindex.get_sys_config()
        auth = config.lookup('auth', cfg) or ()
        self._sys_auth = tuple(sorted(auth, key=lambda a: a.priority))

    def _get_pgaddr(self):
        return self._cluster.get_connection_spec()

    async def acquire_pgcon(self, dbname):
        return await self._pg_pool.acquire(dbname)

    def release_pgcon(self, dbname, conn, *, discard=False):
        if not conn.is_connected() or conn.in_tx():
            discard = True
        self._pg_pool.release(dbname, conn, discard=discard)

    async def _fetch_roles(self):
        syscon = await self._acquire_sys_pgcon()
        try:
            role_query = self.get_sys_query('roles')
            json_data = await syscon.parse_execute_json(
                role_query, b'__sys_role',
                dbver=b'', use_prep_stmt=True, args=(),
            )
            roles = json.loads(json_data)
            self._roles = immutables.Map([(r['name'], r) for r in roles])
        finally:
            self._release_sys_pgcon()

    async def _load_instance_data(self):
        syscon = await self._acquire_sys_pgcon()
        try:
            result = await syscon.simple_query(b'''\
                SELECT json FROM edgedbinstdata.instdata
                WHERE key = 'instancedata';
            ''', ignore_data=False)
            self._instance_data = immutables.Map(
                json.loads(result[0][0].decode('utf-8')))
        finally:
            self._release_sys_pgcon()

    async def _load_sys_queries(self):
        syscon = await self._acquire_sys_pgcon()
        try:
            result = await syscon.simple_query(b'''\
                SELECT json FROM edgedbinstdata.instdata
                WHERE key = 'sysqueries';
            ''', ignore_data=False)
            queries = json.loads(result[0][0].decode('utf-8'))
            self._sys_queries = immutables.Map(
                {k: q.encode() for k, q in queries.items()})
        finally:
            self._release_sys_pgcon()

    def get_roles(self):
        return self._roles

    async def new_compiler(self, dbname, dbver):
        compiler_worker = await self._compiler_manager.spawn_worker()
        try:
            await compiler_worker.call('connect', dbname, dbver)
        except Exception:
            await compiler_worker.close()
            raise
        return compiler_worker

    def _new_port(self, portcls, **kwargs):
        return portcls(
            server=self,
            loop=self._loop,
            pg_addr=self._pg_addr,
            runstate_dir=self._runstate_dir,
            internal_runstate_dir=self._internal_runstate_dir,
            dbindex=self._dbindex,
            **kwargs,
        )

    async def _restart_mgmt_port(self, nethost, netport):
        await self._mgmt_port.stop()

        try:
            new_mgmt_port = self._new_port(
                mng_port.ManagementPort,
                nethost=nethost,
                netport=netport,
                auto_shutdown=self._auto_shutdown,
                max_protocol=self._mgmt_protocol_max,
            )
        except Exception:
            await self._mgmt_port.start()
            raise

        try:
            await new_mgmt_port.start()
        except Exception:
            try:
                await new_mgmt_port.stop()
            except Exception:
                logging.exception('could not stop the new server')
                pass
            await self._mgmt_port.start()
            raise
        else:
            self._mgmt_host_addr = nethost
            self._mgmt_port_no = netport
            self._mgmt_port = new_mgmt_port

    async def _start_portconf(self, portconf: Any, *,
                              suppress_errors=False):
        if portconf in self._sys_conf_ports:
            logging.info('port for config %r has been already started',
                         portconf)
            return

        port_cls: Type[http.BaseHttpPort]
        if portconf.protocol == 'graphql+http':
            port_cls = http_graphql_port.HttpGraphQLPort
        elif portconf.protocol == 'edgeql+http':
            port_cls = http_edgeql_port.HttpEdgeQLPort
        elif portconf.protocol == 'notebook':
            port_cls = notebook_port.NotebookPort
        else:
            raise errors.InvalidReferenceError(
                f'unknown protocol {portconf.protocol!r}')

        port = self._new_port(
            port_cls,
            netport=portconf.port,
            nethost=portconf.address,
            database=portconf.database,
            user=portconf.user,
            protocol=portconf.protocol,
            concurrency=portconf.concurrency)

        try:
            await port.start()
        except Exception as ex:
            await port.stop()
            if suppress_errors:
                logging.error(
                    'failed to start port for config: %r', portconf,
                    exc_info=True)
            else:
                raise ex
        else:
            logging.info('started port for config: %r', portconf)

        self._sys_conf_ports[portconf] = port
        return port

    async def _stop_portconf(self, portconf):
        if portconf not in self._sys_conf_ports:
            logging.warning('no port to stop for config: %r', portconf)
            return

        try:
            port = self._sys_conf_ports.pop(portconf)
            await port.stop()
        except Exception:
            logging.error(
                'failed to stop port for config: %r', portconf,
                exc_info=True)
        else:
            logging.info('stopped port for config: %r', portconf)

    async def _on_drop_db(self, dbname: str, current_dbname: str) -> None:
        assert self._dbindex is not None

        if current_dbname == dbname:
            raise errors.ExecutionError(
                f'cannot drop the currently open database {dbname!r}')

        if self._dbindex.count_connections(dbname):
            # If there are open EdgeDB connections to the `dbname` DB
            # just raise the error Postgres would have raised itself.
            raise errors.ExecutionError(
                f'database {dbname!r} is being accessed by other users')
        else:
            # If, however, there are no open EdgeDB connections, prune
            # all non-active postgres connection to the `dbname` DB.
            await self._pg_pool.prune_inactive_connections(dbname)

    async def _on_system_config_add(self, setting_name, value):
        # CONFIGURE SYSTEM INSERT ConfigObject;
        if setting_name == 'ports':
            await self._start_portconf(value)

    async def _on_system_config_rem(self, setting_name, value):
        # CONFIGURE SYSTEM RESET ConfigObject;
        if setting_name == 'ports':
            await self._stop_portconf(value)

    async def _on_system_config_set(self, setting_name, value):
        # CONFIGURE SYSTEM SET setting_name := value;
        if setting_name == 'listen_addresses':
            await self._restart_mgmt_port(value, self._mgmt_port_no)

        elif setting_name == 'listen_port':
            await self._restart_mgmt_port(self._mgmt_host_addr, value)

    async def _on_system_config_reset(self, setting_name):
        # CONFIGURE SYSTEM RESET setting_name;
        if setting_name == 'listen_addresses':
            await self._restart_mgmt_port(
                'localhost', self._mgmt_port_no)

        elif setting_name == 'listen_port':
            await self._restart_mgmt_port(
                self._mgmt_host_addr, defines.EDGEDB_PORT)

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
        if self._sys_pgcon_waiters is None:
            raise RuntimeError('invalid request to acquire a system pgcon')
        return await self._sys_pgcon_waiters.get()

    def _release_sys_pgcon(self):
        self._sys_pgcon_waiters.put_nowait(self.__sys_pgcon)

    async def _signal_sysevent(self, event, **kwargs):
        pgcon = await self._acquire_sys_pgcon()
        try:
            await pgcon.signal_sysevent(event, **kwargs)
        finally:
            self._release_sys_pgcon()

    def _on_remote_ddl(self, dbname, dbver):
        # Triggered by a postgres notification event 'schema-changes'
        # on the __edgedb_sysevent__ channel
        self._dbindex._on_remote_ddl(dbname, dbver)

    def _on_remote_database_config_change(self, dbname):
        # Triggered by a postgres notification event 'database-config-changes'
        # on the __edgedb_sysevent__ channel
        self._dbindex._on_remote_database_config_change(dbname)

    def _on_remote_system_config_change(self):
        # Triggered by a postgres notification event 'ystem-config-changes'
        # on the __edgedb_sysevent__ channel
        self._dbindex._on_remote_system_config_change()

    def _on_role_change(self):
        self._loop.create_task(self._fetch_roles())

    def add_port(self, portcls, **kwargs):
        if self._serving:
            raise RuntimeError(
                'cannot add new ports after start() call')

        port = self._new_port(portcls, **kwargs)
        self._ports.append(port)
        return port

    async def run_startup_script_and_exit(self):
        """Run the script specified in *startup_script* and exit immediately"""
        if self._startup_script is None:
            raise AssertionError('startup script is not defined')

        ql_parser.preload()
        await self._mgmt_port.run_startup_script_and_exit()
        return

    async def start(self):
        # Make sure that EdgeQL parser is preloaded; edgecon might use
        # it to restore config values.
        ql_parser.preload()

        async with taskgroup.TaskGroup() as g:
            g.create_task(self._mgmt_port.start())
            for port in self._ports:
                g.create_task(port.start())

        sys_config = self._dbindex.get_sys_config()
        ports = config.lookup('ports', sys_config)
        if ports:
            for portconf in ports:
                await self._start_portconf(portconf, suppress_errors=True)

        self._serving = True

        if self._echo_runtime_info:
            ri = {
                "port": self._mgmt_port_no,
                "runstate_dir": str(self._runstate_dir),
            }
            print(f'\nEDGEDB_SERVER_DATA:{json.dumps(ri)}\n', flush=True)

    async def stop(self):
        try:
            self._serving = False

            async with taskgroup.TaskGroup() as g:
                for port in self._ports:
                    g.create_task(port.stop())
                self._ports.clear()
                for port in self._sys_conf_ports.values():
                    g.create_task(port.stop())
                self._sys_conf_ports.clear()
                g.create_task(self._mgmt_port.stop())
                self._mgmt_port = None
        finally:
            pgcon = await self._acquire_sys_pgcon()
            self._sys_pgcon_waiters = None
            self.__sys_pgcon = None
            pgcon.terminate()

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
