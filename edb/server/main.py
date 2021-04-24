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

from . import args as srvargs
from . import buildmeta
from . import daemon
from . import defines
from . import logsetup
from . import pgconnparams
from . import pgcluster
from . import protocol


if TYPE_CHECKING:
    from . import server
else:
    # Import server lazily to make sure that most of imports happen
    # under coverage (if we're testing with it).  Otherwise
    # coverage will fail to detect that "import edb..." lines
    # actually were run.
    server = None


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
        with tempfile.TemporaryDirectory(prefix='edgedb-internal-',
                                         dir=runstate_dir) as td:
            yield td
    except PermissionError as ex:
        abort(f'cannot write to the runstate directory: '
              f'{ex!s}; please fix the permissions or use '
              f'--runstate-dir to specify the correct location')


def _init_cluster(cluster, args: srvargs.ServerConfig) -> bool:
    from edb.server import bootstrap

    need_restart = asyncio.run(bootstrap.ensure_bootstrapped(cluster, args))
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


def _run_server(
    cluster,
    args: srvargs.ServerConfig,
    runstate_dir,
    internal_runstate_dir,
    *,
    do_setproctitle: bool
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

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
        status_sink=args.status_sink,
        startup_script=args.startup_script,
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

        if do_setproctitle:
            setproctitle.setproctitle(
                f"edgedb-server-{ss.get_listen_port()}"
            )

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


def run_server(args: srvargs.ServerConfig, *, do_setproctitle: bool=False):

    from . import server as server_mod
    global server
    server = server_mod

    ver = buildmeta.get_version()

    if devmode.is_in_dev_mode():
        logger.info(f'EdgeDB server ({ver}) starting in DEV mode.')
    else:
        logger.info(f'EdgeDB server ({ver}) starting.')

    _init_parsers()

    pg_cluster_init_by_us = False
    pg_cluster_started_by_us = False

    if args.postgres_tenant_id is None:
        tenant_id = buildmeta.get_default_tenant_id()
    else:
        tenant_id = f'C{args.postgres_tenant_id}'

    cluster: Union[pgcluster.Cluster, pgcluster.RemoteCluster]
    if args.data_dir:
        pg_max_connections = args.max_backend_connections
        if not pg_max_connections:
            max_conns = srvargs.compute_default_max_backend_connections()
            pg_max_connections = max_conns
            if args.testmode:
                max_conns = srvargs.adjust_testmode_max_connections(max_conns)
                logger.info(f'Configuring Postgres max_connections='
                            f'{pg_max_connections} under test mode.')
            args = args._replace(max_backend_connections=max_conns)
            logger.info(f'Using {max_conns} max backend connections based on '
                        f'total memory.')

        cluster = pgcluster.get_local_pg_cluster(
            args.data_dir,
            max_connections=pg_max_connections,
            tenant_id=tenant_id,
        )
        default_runstate_dir = cluster.get_data_dir()
        cluster.set_connection_params(
            pgconnparams.ConnectionParameters(
                user='postgres',
                database='template1',
            ),
        )
    elif args.postgres_dsn:
        cluster = pgcluster.get_remote_pg_cluster(
            args.postgres_dsn,
            tenant_id=tenant_id,
        )

        instance_params = cluster.get_runtime_params().instance_params
        max_conns = (
            instance_params.max_connections -
            instance_params.reserved_connections)
        if not args.max_backend_connections:
            logger.info(f'Detected {max_conns} backend connections available.')
            if args.testmode:
                max_conns = srvargs.adjust_testmode_max_connections(max_conns)
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
                cluster.start()
                pg_cluster_started_by_us = True

            elif cluster_status != 'running':
                abort('Could not start database cluster in %s',
                      args.data_dir)

            need_cluster_restart = _init_cluster(cluster, args)

            if need_cluster_restart and pg_cluster_started_by_us:
                logger.info('Restarting server to reload configuration...')
                cluster.stop()
                cluster.start()

            if (
                not args.bootstrap_only
                or args.bootstrap_script
                or args.bootstrap_command
            ):
                if args.data_dir:
                    cluster.set_connection_params(
                        pgconnparams.ConnectionParameters(
                            user='postgres',
                            database=pgcluster.get_database_backend_name(
                                defines.EDGEDB_TEMPLATE_DB,
                                tenant_id=tenant_id,
                            ),
                        ),
                    )

                _run_server(
                    cluster, args, runstate_dir, internal_runstate_dir,
                    do_setproctitle=do_setproctitle,
                )

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


def server_main(*, insecure=False, **kwargs):
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    exceptions.install_excepthook()

    bump_rlimit_nofile()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    if kwargs['devmode'] is not None:
        devmode.enable_dev_mode(kwargs['devmode'])

    server_args = srvargs.parse_args(insecure=insecure, **kwargs)

    if kwargs['background']:
        daemon_opts = {'detach_process': True}
        pidfile = kwargs['pidfile_dir'] / f".s.EDGEDB.{kwargs['port']}.lock"
        daemon_opts['pidfile'] = pidfile
        if kwargs['daemon_user']:
            daemon_opts['uid'] = kwargs['daemon_user']
        if kwargs['daemon_group']:
            daemon_opts['gid'] = kwargs['daemon_group']
        with daemon.DaemonContext(**daemon_opts):
            run_server(server_args, setproctitle=True)
    else:
        with devmode.CoverageConfig.enable_coverage_if_requested():
            run_server(server_args)


@click.command(
    'EdgeDB Server',
    context_settings=dict(help_option_names=['-h', '--help']))
@srvargs.server_options
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
