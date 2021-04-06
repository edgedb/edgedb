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
import contextlib
import errno
import logging
import os
import os.path
import pathlib
import random
import resource
import signal
import socket
import sys
import tempfile
import typing
import warnings

import psutil
import uvloop

import click
import setproctitle

from edb.common import devmode
from edb.common import exceptions

from edb.server import defines as edgedb_defines

from . import buildmeta
from . import cluster as edgedb_cluster
from . import daemon
from . import defines
from . import logsetup
from . import pgconnparams
from . import pgcluster
from . import protocol


BYTES_OF_MEM_PER_CONN = 100 * 1024 * 1024  # 100MiB
NUM_RESERVED_CONNS_IN_TESTMODE = 15

logger = logging.getLogger('edb.server')
_server_initialized = False


def abort(msg, *args) -> NoReturn:
    logger.critical(msg, *args)
    sys.exit(1)


def terminate_server(server, loop):
    loop.stop()


@contextlib.contextmanager
def _ensure_runstate_dir(
    default_runstate_dir: pathlib.Path,
    specified_runstate_dir: Optional[pathlib.Path]
) -> Iterator[pathlib.Path]:
    temp_runstate_dir = None

    if specified_runstate_dir is None:
        if default_runstate_dir is None:
            temp_runstate_dir = tempfile.TemporaryDirectory(prefix='edbrun-')
            default_runstate_dir = temp_runstate_dir.name

        try:
            runstate_dir = buildmeta.get_runstate_path(default_runstate_dir)
        except buildmeta.MetadataError:
            abort(
                f'cannot determine the runstate directory location; '
                f'please use --runstate-dir to specify the correct location')
    else:
        runstate_dir = specified_runstate_dir

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

    try:
        yield runstate_dir
    finally:
        if temp_runstate_dir is not None:
            temp_runstate_dir.cleanup()


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


def _init_cluster(cluster, args: ServerConfig) -> bool:
    from edb.server import bootstrap

    bootstrap_args = {
        'default_database': (args.default_database or
                             args.default_database_user),
        'default_database_user': args.default_database_user,
        'testmode': args.testmode,
        'insecure': args.insecure,
        'bootstrap_script': args.bootstrap_script,
        'bootstrap_command': args.bootstrap_command,
    }

    need_restart = asyncio.run(
        bootstrap.ensure_bootstrapped(cluster, bootstrap_args)
    )

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


def _run_server(cluster, args: ServerConfig,
                runstate_dir, internal_runstate_dir):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Import here to make sure that most of imports happen
    # under coverage (if we're testing with it).  Otherwise
    # coverage will fail to detect that "import edb..." lines
    # actually were run.
    from . import server

    bootstrap_script_text: Optional[str]
    if args.bootstrap_script:
        with open(args.bootstrap_script) as f:
            bootstrap_script_text = f.read()
    elif args.bootstrap_command:
        bootstrap_script_text = args.bootstrap_command
    else:
        bootstrap_script_text = None

    if bootstrap_script_text is None:
        bootstrap_script = None
    else:
        bootstrap_script = server.StartupScript(
            text=bootstrap_script_text,
            database=(
                args.default_database or
                edgedb_defines.EDGEDB_SUPERUSER_DB
            ),
            user=(
                args.default_database_user or
                edgedb_defines.EDGEDB_SUPERUSER
            ),
        )

    ss = server.Server(
        loop=loop,
        cluster=cluster,
        runstate_dir=runstate_dir,
        internal_runstate_dir=internal_runstate_dir,
        max_backend_connections=args.max_backend_connections,
        compiler_pool_size=args.compiler_pool_size,
        nethost=args.bind_address,
        netport=args.port,
        auto_shutdown=args.auto_shutdown,
        echo_runtime_info=args.echo_runtime_info,
        startup_script=bootstrap_script,
    )

    loop.run_until_complete(ss.init())

    if args.bootstrap_only:
        loop.run_until_complete(ss.run_startup_script_and_exit())
    else:
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
            try:
                logger.info('Shutting down.')
                loop.run_until_complete(ss.stop())
            finally:
                _sd_notify('STOPPING=1')


def run_server(args: ServerConfig):
    ver = buildmeta.get_version()

    if devmode.is_in_dev_mode():
        logger.info(f'EdgeDB server ({ver}) starting in DEV mode.')
    else:
        logger.info(f'EdgeDB server ({ver}) starting.')

    _init_parsers()

    pg_cluster_init_by_us = False
    pg_cluster_started_by_us = False

    cluster: Union[pgcluster.Cluster, pgcluster.RemoteCluster]
    if args.data_dir:
        pg_max_connections = args.max_backend_connections
        if not pg_max_connections:
            max_conns = _compute_default_max_backend_connections()
            pg_max_connections = max_conns
            if args.testmode:
                max_conns = max(
                    1,
                    max_conns // 2,
                    max_conns - NUM_RESERVED_CONNS_IN_TESTMODE,
                )
                logger.info(f'Configuring Postgres max_connections='
                            f'{pg_max_connections} under test mode.')
            args = args._replace(max_backend_connections=max_conns)
            logger.info(f'Using {max_conns} max backend connections based on '
                        f'total memory.')

        cluster = pgcluster.get_local_pg_cluster(
            args.data_dir, max_connections=pg_max_connections)
        default_runstate_dir = cluster.get_data_dir()
        cluster.set_connection_params(
            pgconnparams.ConnectionParameters(
                user='postgres',
                database='template1',
            ),
        )
    elif args.postgres_dsn:
        cluster = pgcluster.get_remote_pg_cluster(args.postgres_dsn)

        instance_params = cluster.get_runtime_params().instance_params
        max_conns = (
            instance_params.max_connections -
            instance_params.reserved_connections)
        if not args.max_backend_connections:
            logger.info(f'Detected {max_conns} backend connections available.')
            if args.testmode:
                max_conns = max(
                    1,
                    max_conns // 2,
                    max_conns - NUM_RESERVED_CONNS_IN_TESTMODE,
                )
                logger.info(f'Using max_backend_connections={max_conns} '
                            f'under test mode.')
            args = args._replace(max_backend_connections=max_conns)
        elif args.max_backend_connections > max_conns:
            abort(f'--max-backend-connections is too large for this backend; '
                  f'detected maximum available NUM: {max_conns}')

        default_runstate_dir = None
    else:
        # This should have been checked by main() already,
        # but be extra careful.
        abort('Neither the data directory nor the remote Postgres DSN '
              'are specified')

    try:
        pg_cluster_init_by_us = cluster.ensure_initialized()

        cluster_status = cluster.get_status()

        specified_runstate_dir: Optional[pathlib.Path]
        if args.runstate_dir:
            specified_runstate_dir = args.runstate_dir
        elif args.bootstrap_only:
            # When bootstrapping a new EdgeDB instance it is often necessary
            # to avoid using the main runstate dir due to lack of permissions,
            # possibility of conflict with another running instance, etc.
            # The --bootstrap mode is also often runs unattended, i.e.
            # as a post-install hook during package installation.
            specified_runstate_dir = default_runstate_dir
        else:
            specified_runstate_dir = None

        runstate_dir_mgr = _ensure_runstate_dir(
            default_runstate_dir,
            specified_runstate_dir,
        )

        with runstate_dir_mgr as runstate_dir, \
                _internal_state_dir(runstate_dir) as internal_runstate_dir:

            if cluster_status == 'stopped':
                cluster.start(port=edgedb_cluster.find_available_port())
                pg_cluster_started_by_us = True

            elif cluster_status != 'running':
                abort('Could not start database cluster in %s',
                      args.data_dir)

            need_cluster_restart = _init_cluster(cluster, args)

            if need_cluster_restart and pg_cluster_started_by_us:
                logger.info('Restarting server to reload configuration...')
                cluster_port = cluster.get_connection_spec()['port']
                cluster.stop()
                cluster.start(port=cluster_port)

            if (
                not args.bootstrap_only
                or args.bootstrap_script
                or args.bootstrap_command
            ):
                if args.data_dir:
                    cluster.set_connection_params(
                        pgconnparams.ConnectionParameters(
                            user=defines.EDGEDB_SUPERUSER,
                            database=defines.EDGEDB_TEMPLATE_DB,
                        ),
                    )

                _run_server(cluster, args, runstate_dir, internal_runstate_dir)

    except BaseException:
        if pg_cluster_init_by_us and not _server_initialized:
            logger.warning('server bootstrap did not complete successfully, '
                           'removing the data directory')
            if cluster.get_status() == 'running':
                cluster.stop()
            cluster.destroy()
        raise

    finally:
        if args.temp_dir:
            if cluster.get_status() == 'running':
                cluster.stop()
            cluster.destroy()

        elif pg_cluster_started_by_us:
            cluster.stop()


class PathPath(click.Path):
    name = 'path'

    def convert(self, value, param, ctx):
        return pathlib.Path(super().convert(value, param, ctx)).absolute()


class PortType(click.ParamType):
    name = 'port'

    @staticmethod
    def find_available_port(port_range=(49152, 65535), max_tries=1000):
        low, high = port_range

        port = low
        try_no = 0

        while try_no < max_tries:
            try_no += 1
            port = random.randint(low, high)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(('localhost', port))
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    continue
                raise
            finally:
                sock.close()

            break
        else:
            port = None

        return port

    def convert(self, value, param, ctx):
        if value == 'auto':
            return self.find_available_port()

        try:
            return int(value, 10)
        except TypeError:
            self.fail(
                "expected string for int() conversion, got "
                f"{value!r} of type {type(value).__name__}",
                param,
                ctx,
            )
        except ValueError:
            self.fail(f"{value!r} is not a valid integer", param, ctx)


class ServerConfig(typing.NamedTuple):

    insecure: bool
    data_dir: pathlib.Path
    postgres_dsn: str
    log_level: str
    log_to: str
    bootstrap_only: bool
    bootstrap_command: str
    bootstrap_script: pathlib.Path
    default_database: Optional[str]
    default_database_user: Optional[str]
    devmode: bool
    testmode: bool
    bind_address: str
    port: int
    background: bool
    pidfile_dir: pathlib.Path
    daemon_user: str
    daemon_group: str
    runstate_dir: pathlib.Path
    max_backend_connections: Optional[int]
    compiler_pool_size: int
    echo_runtime_info: bool
    temp_dir: bool
    auto_shutdown: bool


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


def _get_runstate_dir_default() -> str:
    try:
        return buildmeta.get_build_metadata_value("RUNSTATE_DIR")
    except buildmeta.MetadataError:
        return '<data-dir>'


def _protocol_version(
    ctx: click.Context,
    param: click.Param,  # type: ignore[name-defined]
    value: str,
) -> Tuple[int, int]:
    try:
        minor, major = map(int, value.split('.'))
        ver = minor, major
        if ver < protocol.MIN_PROTOCOL or ver > protocol.CURRENT_PROTOCOL:
            raise ValueError()
    except ValueError:
        raise click.UsageError(
            f"protocol version must be in the form "
            f"MAJOR.MINOR, in the range of "
            f"{protocol.MIN_PROTOCOL[0]}.{protocol.MIN_PROTOCOL[1]} - "
            f"{protocol.CURRENT_PROTOCOL[0]}.{protocol.CURRENT_PROTOCOL[1]}")
    return ver


def _validate_max_backend_connections(ctx, param, value):
    if value is not None and value < defines.BACKEND_CONNECTIONS_MIN:
        raise click.BadParameter(
            f'the minimum number of backend connections '
            f'is {defines.BACKEND_CONNECTIONS_MIN}')
    return value


def _compute_default_max_backend_connections():
    total_mem = psutil.virtual_memory().total
    return max(int(total_mem / BYTES_OF_MEM_PER_CONN), 2)


def _validate_compiler_pool_size(ctx, param, value):
    if value < defines.BACKEND_COMPILER_POOL_SIZE_MIN:
        raise click.BadParameter(
            f'the minimum value for the compiler pool size option '
            f'is {defines.BACKEND_COMPILER_POOL_SIZE_MIN}')
    return value


_server_options = [
    click.option(
        '-D', '--data-dir', type=PathPath(), envvar='EDGEDB_DATADIR',
        help='database cluster directory'),
    click.option(
        '--postgres-dsn', type=str,
        help='DSN of a remote Postgres cluster, if using one'),
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
        '--bootstrap', is_flag=True, hidden=True,
        help='[DEPRECATED] bootstrap the database cluster and exit'),
    click.option(
        '--bootstrap-only', is_flag=True,
        help='bootstrap the database cluster and exit'),
    click.option(
        '--default-database', type=str, hidden=True,
        help='[DEPRECATED] the name of the default database to create'),
    click.option(
        '--default-database-user', type=str, hidden=True,
        help='[DEPRECATED] the name of the default database owner'),
    click.option(
        '--bootstrap-command', metavar="QUERIES",
        help='run the commands when initializing the database. '
             'Queries are executed by default user within default '
             'database. May be used with or without `--bootstrap-only`.'),
    click.option(
        '--bootstrap-script', type=PathPath(), metavar="PATH",
        help='run the script when initializing the database. '
             'Script run by default user within default database. '
             'May be used with or without `--bootstrap-only`.'),
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
        '-P', '--port', type=PortType(), default=None,
        help='port to listen on'),
    click.option(
        '-b', '--background', is_flag=True, help='daemonize'),
    click.option(
        '--pidfile-dir', type=PathPath(), default='/run/edgedb/',
        help='path to PID file directory'),
    click.option(
        '--daemon-user', type=int),
    click.option(
        '--daemon-group', type=int),
    click.option(
        '--runstate-dir', type=PathPath(), default=None,
        help=f'directory where UNIX sockets and other temporary '
             f'runtime files will be placed ({_get_runstate_dir_default()} '
             f'by default)'),
    click.option(
        '--max-backend-connections', type=int, metavar='NUM',
        help=f'The maximum NUM of connections this EdgeDB instance could make '
             f'to the backend PostgreSQL cluster. If not set, EdgeDB will '
             f'detect and calculate the NUM: RAM/100MiB='
             f'{_compute_default_max_backend_connections()} for local '
             f'Postgres or pg_settings.max_connections for remote Postgres, '
             f'minus the NUM of --reserved-pg-connections.',
        callback=_validate_max_backend_connections),
    click.option(
        '--compiler-pool-size', type=int,
        default=defines.BACKEND_COMPILER_POOL_SIZE_DEFAULT,
        callback=_validate_compiler_pool_size),
    click.option(
        '--echo-runtime-info', type=bool, default=False, is_flag=True,
        help='echo runtime info to stdout; the format is JSON, prefixed by ' +
             '"EDGEDB_SERVER_DATA:", ended with a new line'),
    click.option(
        '--temp-dir', type=bool, default=False, is_flag=True,
        help='create a temporary database cluster directory '
             'that will be automatically purged on server shutdown'),
    click.option(
        '--auto-shutdown', type=bool, default=False, is_flag=True,
        help='shutdown the server after the last management ' +
             'connection is closed'),
    click.option(
        '--version', is_flag=True,
        help='Show the version and exit.')
]


def server_options(func):
    for option in reversed(_server_options):
        func = option(func)
    return func


def server_main(*, insecure=False, bootstrap, **kwargs):
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    exceptions.install_excepthook()

    bump_rlimit_nofile()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    if kwargs['devmode'] is not None:
        devmode.enable_dev_mode(kwargs['devmode'])

    if bootstrap:
        warnings.warn(
            "Option `--bootstrap` is deprecated, use `--bootstrap-only`",
            DeprecationWarning,
        )
        kwargs['bootstrap_only'] = True

    if kwargs['default_database_user']:
        if kwargs['default_database_user'] == 'edgedb':
            warnings.warn(
                "Option `--default-database-user` is deprecated."
                " Role `edgedb` is always created and"
                " no role named after unix user is created any more.",
                DeprecationWarning,
            )
        else:
            warnings.warn(
                "Option `--default-database-user` is deprecated."
                " Please create the role explicitly.",
                DeprecationWarning,
            )

    if kwargs['default_database']:
        if kwargs['default_database'] == 'edgedb':
            warnings.warn(
                "Option `--default-database` is deprecated."
                " Database `edgedb` is always created and"
                " no database named after unix user is created any more.",
                DeprecationWarning,
            )
        else:
            warnings.warn(
                "Option `--default-database` is deprecated."
                " Please create the database explicitly.",
                DeprecationWarning,
            )

    if kwargs['temp_dir']:
        if kwargs['data_dir']:
            abort('--temp-dir is incompatible with --data-dir/-D')
        if kwargs['runstate_dir']:
            abort('--temp-dir is incompatible with --runstate-dir')
        if kwargs['postgres_dsn']:
            abort('--temp-dir is incompatible with --postgres-dsn')
        kwargs['data_dir'] = kwargs['runstate_dir'] = pathlib.Path(
            tempfile.mkdtemp())
    else:
        if not kwargs['data_dir']:
            if kwargs['postgres_dsn']:
                pass
            elif devmode.is_in_dev_mode():
                kwargs['data_dir'] = os.path.expanduser('~/.edgedb')
            else:
                abort('Please specify the instance data directory '
                      'using the -D argument or the address of a remote '
                      'PostgreSQL cluster using the --postgres-dsn argument')
        elif kwargs['postgres_dsn']:
            abort('The -D and --postgres-dsn options are mutually exclusive.')

    kwargs['insecure'] = insecure

    if kwargs['background']:
        daemon_opts = {'detach_process': True}
        pidfile = kwargs['pidfile_dir'] / f".s.EDGEDB.{kwargs['port']}.lock"
        daemon_opts['pidfile'] = pidfile
        if kwargs['daemon_user']:
            daemon_opts['uid'] = kwargs['daemon_user']
        if kwargs['daemon_group']:
            daemon_opts['gid'] = kwargs['daemon_group']
        with daemon.DaemonContext(**daemon_opts):
            # TODO: setproctitle should probably be moved to where
            # management port is initialized, as that's where we know
            # the actual network port we listen on.  At this point
            # "port" can be "None".
            setproctitle.setproctitle(
                f"edgedb-server-{kwargs['port']}")

            run_server(ServerConfig(**kwargs))
    else:
        with devmode.CoverageConfig.enable_coverage_if_requested():
            run_server(ServerConfig(**kwargs))


@click.command(
    'EdgeDB Server',
    context_settings=dict(help_option_names=['-h', '--help']))
@server_options
def main(version=False, **kwargs):
    if version:
        print(f"edgedb-server, version {buildmeta.get_version()}")
        sys.exit(0)
    server_main(**kwargs)


def main_dev():
    devmode.enable_dev_mode()
    main()


if __name__ == '__main__':
    main()
