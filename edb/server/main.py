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
import contextlib
import getpass
import logging
import os
import os.path
import setproctitle
import signal
import socket
import sys
import tempfile

import uvloop

import click
from asyncpg import cluster as pg_cluster

from edb.lang.common import devmode
from edb.lang.common import exceptions

from .. import server2

from . import cluster as edgedb_cluster
from . import daemon
from . import defines
from . import logsetup


logger = logging.getLogger('edb.server')
_server_initialized = False


def abort(msg, *args):
    logger.critical(msg, *args)
    sys.exit(1)


def terminate_server(server, loop):
    loop.stop()


@contextlib.contextmanager
def _runstate_dir(path):
    if path is None:
        path = '/run'
        if not os.path.isdir(path):
            path = '/var/run'
    if not os.path.isdir(path):
        abort(f'{path!r} is not a valid path; please point '
              f'--runstate-dir to a valid directory')
        return

    try:
        with tempfile.TemporaryDirectory(prefix='edgedb-', dir=path) as td:
            yield td
    except PermissionError as ex:
        if not devmode.is_in_dev_mode():
            abort(f'no permissions to write to {path!r} directory: {str(ex)}')
            return
        with tempfile.TemporaryDirectory(prefix='edgedb-') as td:
            yield td


def _init_cluster(cluster, args):
    loop = asyncio.get_event_loop()

    from edb.server import pgsql as backend

    bootstrap_args = {
        'default_database': (args['default_database'] or
                             args['default_database_user']),
        'default_database_user': args['default_database_user'],
    }

    loop.run_until_complete(backend.bootstrap(
        cluster, bootstrap_args))

    global _server_initialized
    _server_initialized = True


def _sd_notify(message):
    notify_socket = os.environ.get('NOTIFY_SOCKET')
    if not notify_socket:
        return

    if notify_socket[0] == '@':
        notify_socket = '\0' + notify_socket[1:]

    sd_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sd_sock.connect(notify_socket)

    try:
        sd_sock.sendall(message.encode())
    finally:
        sd_sock.close()


def _run_server(cluster, args, runstate_dir):
    loop = asyncio.get_event_loop()
    srv = None

    _init_cluster(cluster, args)

    from edb.server import protocol as edgedb_protocol

    def protocol_factory():
        return edgedb_protocol.Protocol(cluster, loop=loop)

    try:
        srv = loop.run_until_complete(
            loop.create_server(
                protocol_factory,
                host=args['bind_address'], port=args['port']))

        loop.add_signal_handler(signal.SIGTERM, terminate_server, srv, loop)
        logger.info('Serving on %s:%s', args['bind_address'], args['port'])

        # Notify systemd that we've started up.
        _sd_notify('READY=1')

        ss = server2.Server(
            loop=loop,
            cluster=cluster,
            runstate_dir=runstate_dir,
            max_backend_connections=args['max_backend_connections'])
        ss.add_binary_interface(args['bind_address'], args['port'] + 1)
        loop.run_until_complete(ss.start())
        logger.info(
            'Serving EDGE on %s:%s',
            args['bind_address'], args['port'] + 1)

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(ss.stop())

    except KeyboardInterrupt:
        logger.info('Shutting down.')
        _sd_notify('STOPPING=1')
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        srv = None

    finally:
        if srv is not None:
            logger.info('Shutting down.')
            srv.close()


def run_server(args):
    if devmode.is_in_dev_mode():
        logger.info('EdgeDB server starting in DEV mode.')
    else:
        logger.info('EdgeDB server starting.')

    pg_cluster_init_by_us = False
    pg_cluster_started_by_us = False

    try:
        if not args['postgres']:
            server_settings = {
                'log_connections': 'yes',
                'log_statement': 'all',
                'log_disconnections': 'yes',
                'log_min_messages': 'INFO',
                'client_min_messages': 'INFO',
                'listen_addresses': '',  # we use Unix sockets
            }

            if args['timezone']:
                server_settings['TimeZone'] = args['timezone']

            cluster = edgedb_cluster.get_pg_cluster(args['data_dir'])
            cluster_status = cluster.get_status()

            if cluster_status == 'not-initialized':
                logger.info(
                    'Initializing database cluster in %s', args['data_dir'])
                initdb_output = cluster.init(
                    username='postgres', locale='C', encoding='UTF8')
                for line in initdb_output.splitlines():
                    logger.debug('initdb: %s', line)
                cluster.reset_hba()
                cluster.add_hba_entry(
                    type='local',
                    database='all', user='all',
                    auth_method='trust'
                )
                pg_cluster_init_by_us = True

            cluster_status = cluster.get_status()

            if cluster_status == 'stopped':
                cluster.start(
                    port=edgedb_cluster.find_available_port(),
                    server_settings=server_settings)
                pg_cluster_started_by_us = True

            elif cluster_status != 'running':
                abort('Could not start database cluster in %s',
                      args['data_dir'])

            cluster.override_connection_spec(
                user='postgres', database='template1')

        else:
            cluster = pg_cluster.RunningCluster(dsn=args['postgres'])
            cluster._data_dir = args['data_dir']

        if args['bootstrap']:
            _init_cluster(cluster, args)
        else:
            with _runstate_dir(args['runstate_dir']) as rsdir:
                _run_server(cluster, args, rsdir)

    except BaseException:
        if pg_cluster_init_by_us and not _server_initialized:
            logger.warning('server bootstrap did not complete successfully, '
                           'removing the data directory')
            if cluster.get_status() == 'running':
                cluster.stop()
            cluster.destroy()
        raise

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
@click.option(
    '--runstate-dir', type=str, default=None,
    help=('directory where UNIX sockets will be created '
          '("/run" on Linux by default)'))
@click.option(
    '--max-backend-connections', type=int, default=100)
def main(**kwargs):
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    exceptions.install_excepthook()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

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


def main_dev():
    devmode.enable_dev_mode()
    main()
