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
from typing import *  # NoQA

import json
import logging

from edb import errors

from edb.common import taskgroup

from edb.edgeql import parser as ql_parser

from edb.server import config
from edb.server import defines
from edb.server import http_edgeql_port
from edb.server import http_graphql_port
from edb.server import mng_port
from edb.server import pgcon

from . import baseport
from . import dbview


logger = logging.getLogger('edb.server')


class Server:

    _ports: List[baseport.Port]
    _sys_conf_ports: Mapping[config.ConfigType, baseport.Port]

    def __init__(self, *, loop, cluster, runstate_dir,
                 internal_runstate_dir,
                 max_backend_connections,
                 nethost, netport,
                 auto_shutdown: bool=False,
                 echo_runtime_info: bool = False):

        self._loop = loop

        self._serving = False

        self._cluster = cluster
        self._pg_addr = self._get_pgaddr()

        # DB state will be initialized in init().
        self._dbindex = None

        self._runstate_dir = runstate_dir
        self._internal_runstate_dir = internal_runstate_dir
        self._max_backend_connections = max_backend_connections

        self._mgmt_port = None
        self._mgmt_host_addr = nethost
        self._mgmt_port_no = netport

        self._ports = []
        self._sys_conf_ports = {}
        self._sys_auth = tuple()

        # Shutdown the server after the last management
        # connection has disconnected
        self._auto_shutdown = auto_shutdown

        self._echo_runtime_info = echo_runtime_info

    async def init(self):
        self._dbindex = await dbview.DatabaseIndex.init(self)
        self._populate_sys_auth()

        cfg = self._dbindex.get_sys_config()

        if not self._mgmt_host_addr:
            self._mgmt_host_addr = cfg.get('listen_addresses') or 'localhost'

        if not self._mgmt_port_no:
            self._mgmt_port_no = cfg.get('listen_port', defines.EDGEDB_PORT)

        self._mgmt_port = self._new_port(
            mng_port.ManagementPort,
            nethost=self._mgmt_host_addr,
            netport=self._mgmt_port_no,
            auto_shutdown=self._auto_shutdown,
        )

    def _populate_sys_auth(self):
        self._sys_auth = tuple(sorted(
            self._dbindex.get_sys_config().get('auth', ()),
            key=lambda a: a.priority))

    def _get_pgaddr(self):
        return self._cluster.get_connection_spec()

    async def new_pgcon(self, dbname):
        return await pgcon.connect(self._get_pgaddr(), dbname)

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

    async def _start_portconf(self, portconf: config.ConfigType, *,
                              suppress_errors=False):
        if portconf in self._sys_conf_ports:
            logging.info('port for config %r has been already started',
                         portconf)
            return

        if portconf.protocol == 'graphql+http':
            port_cls = http_graphql_port.HttpGraphQLPort
        elif portconf.protocol == 'edgeql+http':
            port_cls = http_edgeql_port.HttpEdgeQLPort
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

    def add_port(self, portcls, **kwargs):
        if self._serving:
            raise RuntimeError(
                'cannot add new ports after start() call')

        port = self._new_port(portcls, **kwargs)
        self._ports.append(port)
        return port

    async def start(self):
        # Make sure that EdgeQL parser is preloaded; edgecon might use
        # it to restore config values.
        ql_parser.preload()

        async with taskgroup.TaskGroup() as g:
            g.create_task(self._mgmt_port.start())
            for port in self._ports:
                g.create_task(port.start())

        sys_config = self._dbindex.get_sys_config()
        if 'ports' in sys_config:
            for portconf in sys_config['ports']:
                await self._start_portconf(portconf, suppress_errors=True)

        self._serving = True

        if self._echo_runtime_info:
            ri = {
                "port": self._mgmt_port_no,
                "runstate_dir": str(self._runstate_dir),
            }
            print(f'\nEDGEDB_SERVER_DATA:{json.dumps(ri)}\n', flush=True)

    async def stop(self):
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

    async def get_auth_method(self, user, conn):
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

    async def get_sys_query(self, conn, key):
        return await self._dbindex.get_sys_query(conn, key)

    async def get_instance_data(self, conn, key):
        return await self._dbindex.get_instance_data(conn, key)
