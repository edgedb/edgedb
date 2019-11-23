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

import asyncio
import contextlib
import getpass
import logging
import os
import os.path
import pathlib
import resource
import signal
import socket
import sys
import tempfile

import uvloop

import click
import setproctitle

from edb.common import devmode
from edb.common import exceptions

from . import buildmeta
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


def _ensure_runstate_dir(data_dir, runstate_dir):
    if runstate_dir is None:
        try:
            runstate_dir = buildmeta.get_runstate_path(data_dir)
        except buildmeta.MetadataError:
            abort(
                f'cannot determine the runstate directory location; '
                f'please use --runstate-dir to specify the correct location')

    runstate_dir = pathlib.Path(runstate_dir)

    if not runstate_dir.exists():
        if not runstate_dir.parent.exists():
            abort(
                f'cannot create the runstate directory: '
                f'{str(runstate_dir.parent)!r} does not exist; please use '
                f'--runstate-dir to specify the correct location')

        try:
            runstate_dir.mkdir()
        except PermissionError as ex:
            abort(
                f'cannot create the runstate directory: '
                f'{ex!s}; please use --runstate-dir to specify '
                f'the correct location')

    if not os.path.isdir(runstate_dir):
        abort(f'{str(runstate_dir)!r} is not a directory; please use '
              f'--runstate-dir to specify the correct location')

    return runstate_dir


@contextlib.contextmanager
def _internal_state_dir(runstate_dir):
    try:
        with tempfile.TemporaryDirectory(prefix='internal-',
                                         dir=runstate_dir) as td:
            yield td
    except PermissionError as ex:
        abort(f'cannot write to the runstate directory: '
              f'{ex!s}; please fix the permissions or use '
              f'--runstate-dir to specify the correct location')


def _init_cluster(cluster, args) -> bool:
    from edb.server import bootstrap

    bootstrap_args = {
        'default_database': (args['default_database'] or
                             args['default_database_user']),
        'default_database_user': args['default_database_user'],
        'testmode': args['testmode'],
        'insecure': args['insecure'],
    }

    need_restart = asyncio.run(bootstrap.bootstrap(cluster, bootstrap_args))

    global _server_initialized
    _server_initialized = True

    return need_restart


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


def _init_parsers():
    # Initialize all parsers, rebuilding grammars if
    # necessary.  Do it earlier than later so that we don't
    # end up in a situation where all our compiler processes
    # are building parsers in parallel.

    from edb.edgeql import parser as ql_parser

    ql_parser.preload()


def _run_server(cluster, args, runstate_dir, internal_runstate_dir):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Import here to make sure that most of imports happen
        # under coverage (if we're testing with it).  Otherwise
        # coverage will fail to detect that "import edb..." lines
        # actually were run.
        from . import server

        ss = server.Server(
            loop=loop,
            cluster=cluster,
            runstate_dir=runstate_dir,
            internal_runstate_dir=internal_runstate_dir,
            max_backend_connections=args['max_backend_connections'],
            nethost=args['bind_address'],
            netport=args['port'],
        )

        loop.run_until_complete(ss.init())

        try:
            loop.run_until_complete(ss.start())
        except Exception:
            loop.run_until_complete(ss.stop())
            raise

        loop.add_signal_handler(signal.SIGTERM, terminate_server, ss, loop)

        # Notify systemd that we've started up.
        _sd_notify('READY=1')

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(ss.stop())

    except KeyboardInterrupt:
        logger.info('Shutting down.')
        _sd_notify('STOPPING=1')


def run_server(args):
    ver = buildmeta.get_version()

    if devmode.is_in_dev_mode():
        logger.info(f'EdgeDB server ({ver}) starting in DEV mode.')
    else:
        logger.info(f'EdgeDB server ({ver}) starting.')

    _init_parsers()

    pg_cluster_init_by_us = False
    pg_cluster_started_by_us = False

    try:
        server_settings = {
            'log_connections': 'yes',
            'log_statement': 'all',
            'log_disconnections': 'yes',
            'log_min_messages': 'INFO',
            'client_min_messages': 'INFO',
            'listen_addresses': '',  # we use Unix sockets
            'unix_socket_permissions': '0700',
            # We always enforce UTC timezone:
            # * timestamptz is stored in UTC anyways;
            # * this makes the DB server more predictable.
            'TimeZone': 'UTC',
            'default_transaction_isolation': 'repeatable read',

            # TODO: EdgeDB must manage/monitor all client connections and
            # have its own "max_connections".  We'll set this setting even
            # higher when we have that fully implemented.
            'max_connections': '500',
        }

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
                database='all',
                user='postgres',
                auth_method='trust'
            )
            cluster.add_hba_entry(
                type='local',
                database='all',
                user=defines.EDGEDB_SUPERUSER,
                auth_method='trust'
            )
            pg_cluster_init_by_us = True

        cluster_status = cluster.get_status()
        data_dir = cluster.get_data_dir()

        if args['runstate_dir']:
            specified_runstate_dir = args['runstate_dir']
        elif args['bootstrap']:
            # When bootstrapping a new EdgeDB instance it is often necessary
            # to avoid using the main runstate dir due to lack of permissions,
            # possibility of conflict with another running instance, etc.
            # The --bootstrap mode is also often runs unattended, i.e.
            # as a post-install hook during package installation.
            specified_runstate_dir = data_dir
        else:
            specified_runstate_dir = None

        runstate_dir = _ensure_runstate_dir(data_dir, specified_runstate_dir)

        with _internal_state_dir(runstate_dir) as internal_runstate_dir:
            server_settings['unix_socket_directories'] = args['data_dir']

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

            need_cluster_restart = _init_cluster(cluster, args)

            if need_cluster_restart and pg_cluster_started_by_us:
                logger.info('Restarting server to reload configuration...')
                cluster_port = cluster.get_connection_spec()['port']
                cluster.stop()
                cluster.start(
                    port=cluster_port,
                    server_settings=server_settings)

            if not args['bootstrap']:
                _run_server(cluster, args, runstate_dir, internal_runstate_dir)

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


def bump_rlimit_nofile() -> None:
    try:
        fno_soft, fno_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except resource.error:
        logger.warning('could not read RLIMIT_NOFILE')
    else:
        if fno_soft < defines.EDGEDB_MIN_RLIMIT_NOFILE:
            try:
                resource.setrlimit(
                    resource.RLIMIT_NOFILE,
                    (min(defines.EDGEDB_MIN_RLIMIT_NOFILE, fno_hard),
                     fno_hard))
            except resource.error:
                logger.warning('could not set RLIMIT_NOFILE')


_server_options = [
    click.option(
        '-D', '--data-dir', type=str, envvar='EDGEDB_DATADIR',
        help='database cluster directory'),
    click.option(
        '-l', '--log-level',
        help=('Logging level.  Possible values: (d)ebug, (i)nfo, (w)arn, '
              '(e)rror, (s)ilent'),
        default='i', envvar='EDGEDB_LOG_LEVEL'),
    click.option(
        '--log-to',
        help=('send logs to DEST, where DEST can be a file name, "syslog", '
              'or "stderr"'),
        type=str, metavar='DEST', default='stderr'),
    click.option(
        '--bootstrap', is_flag=True,
        help='bootstrap the database cluster and exit'),
    click.option(
        '--default-database', type=str, default=getpass.getuser(),
        help='the name of the default database to create'),
    click.option(
        '--default-database-user', type=str, default=getpass.getuser(),
        help='the name of the default database owner'),
    click.option(
        '--devmode/--no-devmode',
        help='enable or disable the development mode',
        default=None),
    click.option(
        '--testmode/--no-testmode',
        help='enable or disable the test mode',
        default=False),
    click.option(
        '-I', '--bind-address', type=str, default=None,
        help='IP address to listen on', envvar='EDGEDB_BIND_ADDRESS'),
    click.option(
        '-p', '--port', type=int, default=None,
        help='port to listen on'),
    click.option(
        '-b', '--background', is_flag=True, help='daemonize'),
    click.option(
        '--pidfile', type=str, default='/run/edgedb/',
        help='path to PID file directory'),
    click.option(
        '--daemon-user', type=int),
    click.option(
        '--daemon-group', type=int),
    click.option(
        '--runstate-dir', type=str, default=None,
        help=('directory where UNIX sockets will be created '
              '("/run" on Linux by default)')),
    click.option(
        '--max-backend-connections', type=int, default=100),
]


def server_options(func):
    for option in reversed(_server_options):
        func = option(func)
    return func


def server_main(*, insecure=False, **kwargs):
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    exceptions.install_excepthook()

    bump_rlimit_nofile()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    if kwargs['devmode'] is not None:
        devmode.enable_dev_mode(kwargs['devmode'])

    if not kwargs['data_dir']:
        if devmode.is_in_dev_mode():
            kwargs['data_dir'] = os.path.expanduser('~/.edgedb')
        else:
            abort('Please specify the instance data directory '
                  'using the -D argument')

    kwargs['insecure'] = insecure

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
        with devmode.CoverageConfig.enable_coverage_if_requested():
            run_server(kwargs)


@click.command(
    'EdgeDB Server',
    context_settings=dict(help_option_names=['-h', '--help']))
@server_options
def main(**kwargs):
    server_main(**kwargs)


def main_dev():
    devmode.enable_dev_mode()
    main()


if __name__ == '__main__':
    main()
