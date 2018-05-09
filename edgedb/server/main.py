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


import asyncio
import getpass
import ipaddress
import logging
import os.path
import setproctitle
import signal
import sys

import click
from asyncpg import cluster as pg_cluster

from edgedb.lang.common import exceptions

from . import cluster as edgedb_cluster
from . import daemon
from . import defines
from . import logsetup


logger = logging.getLogger('edgedb.server')


def abort(msg, *args):
    logger.critical(msg, *args)
    sys.exit(1)


def terminate_server(server, loop):
    loop.stop()


def _init_cluster(cluster, args):
    loop = asyncio.get_event_loop()

    from edgedb.server import pgsql as backend

    bootstrap_args = {
        'default_database': (args['default_database'] or
                             args['default_database_user']),
        'default_database_user': args['default_database_user'],
    }

    loop.run_until_complete(backend.bootstrap(
        cluster, bootstrap_args, loop=loop))


def _run_server(cluster, args):
    loop = asyncio.get_event_loop()
    srv = None

    _init_cluster(cluster, args)

    from edgedb.server import protocol as edgedb_protocol

    def protocol_factory():
        return edgedb_protocol.Protocol(cluster, loop=loop)

    try:
        srv = loop.run_until_complete(
            loop.create_server(
                protocol_factory,
                host=args['bind_address'], port=args['port']))

        loop.add_signal_handler(signal.SIGTERM, terminate_server, srv, loop)
        logger.info('Serving on %s:%s', args['bind_address'], args['port'])
        loop.run_forever()

    except KeyboardInterrupt:
        logger.info('Shutting down.')
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        srv = None

    finally:
        if srv is not None:
            logger.info('Shutting down.')
            srv.close()


def run_server(args):
    logger.info('EdgeDB server starting.')

    pg_cluster_started_by_us = False

    if args['data_dir']:
        server_settings = {
            'log_connections': 'yes',
            'log_statement': 'all',
            'log_disconnections': 'yes',
            'log_min_messages': 'INFO',
        }

        if args['timezone']:
            server_settings['TimeZone'] = args['timezone']

        cluster = pg_cluster.Cluster(data_dir=args['data_dir'])
        cluster_status = cluster.get_status()

        if cluster_status == 'not-initialized':
            logger.info(
                'Initializing database cluster in %s', args['data_dir'])
            initdb_output = cluster.init(username='postgres')
            for line in initdb_output.splitlines():
                logger.debug('initdb: %s', line)
            cluster.reset_hba()
            cluster.add_hba_entry(
                type='local',
                database='all', user='all',
                auth_method='trust'
            )
            cluster.add_hba_entry(
                type='local', address=ipaddress.ip_network('127.0.0.0/24'),
                database='all', user='all',
                auth_method='trust'
            )

        cluster_status = cluster.get_status()

        if cluster_status == 'stopped':
            cluster.start(
                port=edgedb_cluster.find_available_port(),
                server_settings=server_settings)
            pg_cluster_started_by_us = True

        elif cluster_status != 'running':
            abort('Could not start database cluster in %s', args['data_dir'])

        cluster.override_connection_spec(
            user='postgres', database='template1')

    else:
        cluster = pg_cluster.RunningCluster(dsn=args['postgres'])

    if args['bootstrap']:
        _init_cluster(cluster, args)
    else:
        _run_server(cluster, args)

    if pg_cluster_started_by_us:
        cluster.stop()


@click.command('EdgeDB Server')
@click.option(
    '-D', '--data-dir', type=str, envvar='EDGEDB_DATADIR',
    help='database cluster directory')
@click.option(
    '-P', '--postgres', type=str,
    help='address of Postgres backend server in DSN format')
@click.option(
    '-l', '--log-level',
    help=('Logging level.  Possible values: (d)ebug, (i)nfo, (w)arn, '
          '(e)rror, (s)ilent'),
    default='i', envvar='EDGEDB_LOG_LEVEL')
@click.option(
    '--log-to',
    help=('send logs to DEST, where DEST can be a file name, "syslog", '
          'or "stderr"'),
    type=str, metavar='DEST', default='stderr')
@click.option(
    '--bootstrap', is_flag=True,
    help='bootstrap the database cluster and exit')
@click.option(
    '--default-database', type=str, default=getpass.getuser(),
    help='the name of the default database to create')
@click.option(
    '--default-database-user', type=str, default=getpass.getuser(),
    help='the name of the default database owner')
@click.option(
    '-I', '--bind-address', type=str, default='127.0.0.1',
    help='IP address to listen on', envvar='EDGEDB_BIND_ADDRESS')
@click.option(
    '-p', '--port', type=int, default=defines.EDGEDB_PORT,
    help='port to listen on')
@click.option(
    '-b', '--background', is_flag=True, help='daemonize')
@click.option(
    '--pidfile', type=str, default='/run/edgedb/',
    help='path to PID file directory')
@click.option(
    '--timezone', type=str,
    help='timezone for displaying and interpreting timestamps')
@click.option(
    '--daemon-user', type=int)
@click.option(
    '--daemon-group', type=int)
def main(**kwargs):
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    exceptions.install_excepthook()

    if kwargs['background']:
        daemon_opts = {'detach_process': True}
        pidfile = os.path.join(
            kwargs['pidfile'], '.s.EDGEDB.{}.lock'.format(kwargs['port']))
        daemon_opts['pidfile'] = pidfile
        if kwargs['daemon_user']:
            daemon_opts['uid'] = kwargs['daemon_user']
        if kwargs['daemon_group']:
            daemon_opts['gid'] = kwargs['daemon_group']
        with daemon.DaemonContext(**daemon_opts):
            setproctitle.setproctitle(
                'edgedb-server-{}'.format(kwargs['port']))
            run_server(kwargs)
    else:
        run_server(kwargs)


if __name__ == '__main__':
    main()
