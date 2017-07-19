##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import argparse
import asyncio
import ipaddress
import logging
import os.path
import setproctitle
import signal
import sys

from asyncpg import cluster as pg_cluster

from edgedb.lang.common import exceptions

from . import cluster as edgedb_cluster
from . import daemon
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

    loop.run_until_complete(backend.bootstrap(cluster, loop=loop))


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
                protocol_factory, host='localhost', port=args.port))

        loop.add_signal_handler(signal.SIGTERM, terminate_server, srv, loop)
        logger.info('Serving on %s:%s', 'localhost', args.port)
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

    if args.data_dir:
        server_settings = {
            'log_connections': 'yes',
            'log_statement': 'all',
            'log_disconnections': 'yes',
            'log_min_messages': 'INFO',
        }

        if args.timezone:
            server_settings['TimeZone'] = args.timezone

        cluster = pg_cluster.Cluster(data_dir=args.data_dir)
        cluster_status = cluster.get_status()

        if cluster_status == 'not-initialized':
            logger.info('Initializing database cluster in %s', args.data_dir)
            initdb_output = cluster.init(**{
                'username': 'postgres',
                'locale': 'en_US.utf8',
            })
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
            abort('Could not start database cluster in %s', args.data_dir)

        cluster.override_connection_spec(
            user='postgres', database='template1')

    else:
        cluster = pg_cluster.RunningCluster(dsn=args.postgres)

    if args.bootstrap:
        _init_cluster(cluster, args)
    else:
        _run_server(cluster, args)

    if pg_cluster_started_by_us:
        cluster.stop()


def main(argv=sys.argv[1:]):
    from edgedb.server import defines as edgedb_defines

    parser = argparse.ArgumentParser(description='EdgeDB Server')
    backend_info = parser.add_mutually_exclusive_group(required=True)
    backend_info.add_argument(
        '-D', '--data-dir', type=str, help='database cluster directory')
    backend_info.add_argument(
        '-P', '--postgres', type=str,
        help='address of Postgres backend server in DSN format')
    parser.add_argument(
        '-l', '--log-level', dest='log_level',
        help=('Logging level.  Possible values: (d)ebug, (i)nfo, (w)arn, '
              '(e)rror, (s)ilent'),
        default=os.environ.get('EDGEDB_LOG_LEVEL') or 'i')
    parser.add_argument(
        '--log-to',
        help=('send logs to DEST, where DEST can be a file name, "syslog", '
              'or "stderr"'),
        type=str, metavar='DEST', dest='log_destination',
        default='stderr')
    parser.add_argument(
        '--bootstrap-database', type=str, default='template1',
        help='name of PostgreSQL database to connect to for bootstrap')
    parser.add_argument(
        '--bootstrap', action='store_true',
        help='bootstrap the database cluster and exit')
    parser.add_argument(
        '-p', '--port', type=int, default=edgedb_defines.EDGEDB_PORT,
        help='port to listen on')
    parser.add_argument(
        '-b', '--background', action='store_true', help='daemonize')
    parser.add_argument(
        '--pidfile', type=str, default='/run/edgedb/',
        help='path to PID file directory')
    parser.add_argument(
        '--timezone', type=str,
        help='timezone for displaying and interpreting timestamps')
    parser.add_argument('--daemon-user', type=int)
    parser.add_argument('--daemon-group', type=int)

    args = parser.parse_args(argv)

    logsetup.setup_logging(args.log_level, args.log_destination)
    exceptions.install_excepthook()

    if args.background:
        daemon_opts = {'detach_process': True}
        pidfile = os.path.join(
            args.pidfile, '.s.EDGEDB.{}.lock'.format(args.port))
        daemon_opts['pidfile'] = pidfile
        if args.daemon_user:
            daemon_opts['uid'] = args.daemon_user
        if args.daemon_group:
            daemon_opts['gid'] = args.daemon_group
        with daemon.DaemonContext(**daemon_opts):
            setproctitle.setproctitle('edgedb-server-{}'.format(args.port))
            run_server(args)
    else:
        run_server(args)


if __name__ == '__main__':
    main()
