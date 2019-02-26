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


import os
import logging
import urllib.parse

from edb import errors

from edb.common import taskgroup

from edb.edgeql import parser as ql_parser

from edb.server import config
from edb.server import http_edgeql_port
from edb.server import http_graphql_port
from edb.server import pgcon

from . import dbview


logger = logging.getLogger('edb.server')


class Server:

    def __init__(self, *, loop, cluster, runstate_dir,
                 max_backend_connections):

        self._loop = loop

        self._serving = False

        self._cluster = cluster
        self._pg_addr = self._get_pgaddr()
        self._pg_data_dir = self._cluster.get_data_dir()

        self._dbindex = dbview.DatabaseIndex(self)

        self._runstate_dir = runstate_dir
        self._max_backend_connections = max_backend_connections

        self._ports = []
        self._sys_conf_ports = {}

    def _get_pgaddr(self):
        pg_con_spec = self._cluster.get_connection_spec()
        if 'host' not in pg_con_spec and 'dsn' in pg_con_spec:
            parsed = urllib.parse.urlparse(pg_con_spec['dsn'])
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            host = query.get("host")[-1]
            port = query.get("port")[-1]
        else:
            host = pg_con_spec.get("host")
            port = pg_con_spec.get("port")

        return os.path.join(host, f'.s.PGSQL.{port}')

    async def new_pgcon(self, dbname):
        return await pgcon.connect(self._pg_addr, dbname)

    async def new_compiler(self, dbname, dbver):
        compiler_worker = await self._compiler_manager.spawn_worker()
        try:
            await compiler_worker.call('connect', dbname, dbver)
        except Exception:
            await compiler_worker.close()
            raise
        return compiler_worker

    async def _start_portconf(self, portconf: config.Port, *,
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

        port = port_cls(
            server=self,
            loop=self._loop,
            pg_addr=self._pg_addr,
            pg_data_dir=self._pg_data_dir,
            runstate_dir=self._runstate_dir,
            dbindex=self._dbindex,
            netport=portconf.port,
            nethost=portconf.address,
            database=portconf.database,
            user=portconf.user,
            protocol=portconf.protocol,
            concurrency=portconf.concurrency)

        try:
            await port.start()
        except Exception as ex:
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
        # SET SYSTEM CONFIG setting_name += value;
        if setting_name == 'ports':
            await self._start_portconf(value)

    async def _on_system_config_rem(self, setting_name, value):
        # SET SYSTEM CONFIG setting_name -= value;
        if setting_name == 'ports':
            await self._stop_portconf(value)

    async def _on_system_config_set(self, setting_name, value):
        # SET SYSTEM CONFIG setting_name := value;
        pass

    def get_datadir(self):
        return self._pg_data_dir

    def add_port(self, portcls, **kwargs):
        if self._serving:
            raise RuntimeError(
                'cannot add new ports after start() call')

        self._ports.append(
            portcls(
                server=self,
                loop=self._loop,
                pg_addr=self._pg_addr,
                pg_data_dir=self._pg_data_dir,
                runstate_dir=self._runstate_dir,
                dbindex=self._dbindex,
                **kwargs))

    async def start(self):
        # Make sure that EdgeQL parser is preloaded; edgecon might use
        # it to restore config values.
        ql_parser.preload()

        async with taskgroup.TaskGroup() as g:
            for port in self._ports:
                g.create_task(port.start())

        sys_config = self._dbindex.get_system_overrides()
        if 'ports' in sys_config:
            for portconf in sys_config['ports']:
                await self._start_portconf(portconf, suppress_errors=True)

        self._serving = True

    async def stop(self):
        async with taskgroup.TaskGroup() as g:
            for port in self._ports:
                g.create_task(port.stop())
            for port in self._sys_conf_ports.values():
                g.create_task(port.stop())
