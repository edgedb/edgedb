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
import dataclasses
import datetime
import logging
import os
import os.path
import pathlib
import resource
import signal
import sys
import tempfile
import uuid

import click
from jwcrypto import jwk
import setproctitle
import uvloop

from . import logsetup
logsetup.early_setup()

from edb import buildmeta
from edb.common import exceptions
from edb.common import devmode
from edb.common import signalctl

from . import args as srvargs
from . import daemon
from . import defines
from . import pgconnparams
from . import pgcluster
from . import service_manager


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


def abort(msg, *args, exit_code=1) -> NoReturn:
    logger.critical(msg, *args)
    sys.exit(exit_code)


@contextlib.contextmanager
def _ensure_runstate_dir(
    default_runstate_dir: Optional[pathlib.Path],
    specified_runstate_dir: Optional[pathlib.Path]
) -> Iterator[pathlib.Path]:
    temp_runstate_dir = None

    if specified_runstate_dir is None:
        if default_runstate_dir is None:
            temp_runstate_dir = tempfile.TemporaryDirectory(prefix='edbrun-')
            runstate_parent = pathlib.Path(temp_runstate_dir.name)
        else:
            runstate_parent = default_runstate_dir

        try:
            runstate_dir = buildmeta.get_runstate_path(runstate_parent)
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
        with tempfile.TemporaryDirectory(prefix="", dir=runstate_dir) as td:
            yield td
    except PermissionError as ex:
        abort(f'cannot write to the runstate directory: '
              f'{ex!s}; please fix the permissions or use '
              f'--runstate-dir to specify the correct location')


async def _init_cluster(cluster, args: srvargs.ServerConfig) -> bool:
    from edb.server import bootstrap

    need_restart = await bootstrap.ensure_bootstrapped(cluster, args)
    global _server_initialized
    _server_initialized = True

    return need_restart


def _init_parsers():
    # Initialize all parsers, rebuilding grammars if
    # necessary.  Do it earlier than later so that we don't
    # end up in a situation where all our compiler processes
    # are building parsers in parallel.

    from edb.edgeql import parser as ql_parser

    ql_parser.preload(allow_rebuild=devmode.is_in_dev_mode(), paralellize=True)


async def _run_server(
    cluster,
    args: srvargs.ServerConfig,
    runstate_dir,
    internal_runstate_dir,
    *,
    do_setproctitle: bool,
    new_instance: bool,
):

    sockets = service_manager.get_activation_listen_sockets()

    if sockets:
        logger.info("detected service manager socket activation")

    if fd_str := os.environ.get("EDGEDB_SERVER_EXTERNAL_LOCK_FD"):
        try:
            fd = int(fd_str)
        except ValueError:
            logger.info("Invalid EDGEDB_SERVER_EXTERNAL_LOCK_FD")
        else:
            os.set_inheritable(fd, False)

    with signalctl.SignalController(signal.SIGINT, signal.SIGTERM) as sc:
        ss = server.Server(
            cluster=cluster,
            runstate_dir=runstate_dir,
            internal_runstate_dir=internal_runstate_dir,
            max_backend_connections=args.max_backend_connections,
            compiler_pool_size=args.compiler_pool_size,
            compiler_pool_mode=args.compiler_pool_mode,
            compiler_pool_addr=args.compiler_pool_addr,
            nethosts=args.bind_addresses,
            netport=args.port,
            listen_sockets=tuple(s for ss in sockets.values() for s in ss),
            auto_shutdown_after=args.auto_shutdown_after,
            echo_runtime_info=args.echo_runtime_info,
            status_sinks=args.status_sinks,
            startup_script=args.startup_script,
            binary_endpoint_security=args.binary_endpoint_security,
            http_endpoint_security=args.http_endpoint_security,
            backend_adaptive_ha=args.backend_adaptive_ha,
            default_auth_method=args.default_auth_method,
            testmode=args.testmode,
            new_instance=new_instance,
            admin_ui=args.admin_ui,
            instance_name=args.instance_name,
        )
        await sc.wait_for(ss.init())

        tls_cert_newly_generated = False
        if args.tls_cert_mode is srvargs.ServerTlsCertMode.SelfSigned:
            assert args.tls_cert_file is not None
            if not args.tls_cert_file.exists():
                assert args.tls_key_file is not None
                generate_tls_cert(
                    args.tls_cert_file,
                    args.tls_key_file,
                    ss.get_listen_hosts(),
                )
                tls_cert_newly_generated = True

        jws_keys_newly_generated = False
        jwe_keys_newly_generated = False
        if args.jose_key_mode is srvargs.JOSEKeyMode.Generate:
            assert args.jws_key_file is not None
            assert args.jwe_key_file is not None
            if not args.jws_key_file.exists():
                generate_jwk(args.jws_key_file)
                jws_keys_newly_generated = True
            if not args.jwe_key_file.exists():
                generate_jwk(args.jwe_key_file)
                jwe_keys_newly_generated = True

        if args.bootstrap_only:
            if args.startup_script and new_instance:
                await sc.wait_for(ss.run_startup_script_and_exit())
            return

        ss.init_tls(
            args.tls_cert_file, args.tls_key_file, tls_cert_newly_generated)

        ss.init_jwcrypto(
            args.jws_key_file,
            args.jwe_key_file,
            jws_keys_newly_generated,
            jwe_keys_newly_generated,
        )

        def load_configuration(_signum):
            logger.info("reloading configuration")
            try:
                ss.reload_tls(args.tls_cert_file, args.tls_key_file)
                ss.load_jwcrypto(args.jws_key_file, args.jwe_key_file)
            except Exception:
                logger.critical(
                    "Unexpected error occurred during reload configuration; "
                    "shutting down.",
                    exc_info=True,
                )
                ss.request_shutdown()

        try:
            await sc.wait_for(ss.start())

            if do_setproctitle:
                setproctitle.setproctitle(
                    f"edgedb-server-{ss.get_listen_port()}"
                )

            # Notify systemd that we've started up.
            service_manager.sd_notify('READY=1')

            with signalctl.SignalController(signal.SIGHUP) as reload_ctl:
                reload_ctl.add_handler(load_configuration)
                try:
                    await sc.wait_for(ss.serve_forever())
                except signalctl.SignalError as e:
                    logger.info('Received signal: %s.', e.signo)
        finally:
            service_manager.sd_notify('STOPPING=1')
            logger.info('Shutting down.')
            await sc.wait_for(ss.stop())


def generate_tls_cert(
    tls_cert_file: pathlib.Path,
    tls_key_file: pathlib.Path,
    listen_hosts: Iterable[str]
) -> None:
    logger.info(f'generating self-signed TLS certificate in "{tls_cert_file}"')

    from cryptography import x509
    from cryptography.hazmat import backends
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509 import oid

    backend = backends.default_backend()
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=backend
    )
    subject = x509.Name(
        [x509.NameAttribute(oid.NameOID.COMMON_NAME, "EdgeDB Server")]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .public_key(private_key.public_key())
        .serial_number(int(uuid.uuid4()))
        .issuer_name(subject)
        .not_valid_before(
            datetime.datetime.today() - datetime.timedelta(days=1)
        )
        .not_valid_after(
            datetime.datetime.today() + datetime.timedelta(weeks=1000)
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(name) for name in listen_hosts
                    if name not in {'0.0.0.0', '::'}
                ]
            ),
            critical=False,
        )
        .sign(
            private_key=private_key,
            algorithm=hashes.SHA256(),
            backend=backend,
        )
    )
    with tls_cert_file.open("wb") as f:
        f.write(certificate.public_bytes(encoding=serialization.Encoding.PEM))
    tls_cert_file.chmod(0o644)
    with tls_key_file.open("wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    tls_key_file.chmod(0o600)


def generate_jwk(keys_file: pathlib.Path) -> None:
    logger.info(f'generating JOSE key pair in "{keys_file}"')

    key = jwk.JWK(generate='EC')
    with keys_file.open("wb") as f:
        f.write(key.export_to_pem(private_key=True, password=None))

    keys_file.chmod(0o600)


async def _get_local_pgcluster(
    args: srvargs.ServerConfig,
    runstate_dir: pathlib.Path,
    tenant_id: str,
) -> Tuple[pgcluster.Cluster, srvargs.ServerConfig]:
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

    cluster = await pgcluster.get_local_pg_cluster(
        args.data_dir,
        runstate_dir=runstate_dir,
        # Plus two below to account for system connections.
        max_connections=pg_max_connections + 2,
        tenant_id=tenant_id,
        log_level=args.log_level,
    )
    cluster.set_connection_params(
        pgconnparams.ConnectionParameters(
            user='postgres',
            database='template1',
            server_settings={
                "application_name": f'edgedb_instance_{args.instance_name}',
            }
        ),
    )
    return cluster, args


async def _get_remote_pgcluster(
    args: srvargs.ServerConfig,
    tenant_id: str,
) -> Tuple[pgcluster.RemoteCluster, srvargs.ServerConfig]:

    cluster = await pgcluster.get_remote_pg_cluster(
        args.backend_dsn,
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

    conn_params = cluster.get_connection_params()
    conn_params = dataclasses.replace(
        conn_params,
        server_settings=dict(
            conn_params.server_settings,
            application_name=f'edgedb_instance_{args.instance_name}',
        ),
    )
    cluster.set_connection_params(conn_params)

    return cluster, args


async def run_server(
    args: srvargs.ServerConfig,
    *,
    do_setproctitle: bool=False,
) -> None:
    from . import server as server_mod
    global server
    server = server_mod

    ver_meta = buildmeta.get_version_metadata()
    extras = []
    source = ""
    if build_date := ver_meta["build_date"]:
        nice_date = build_date.strftime("%Y-%m-%dT%H:%MZ")
        source += f" on {nice_date}"
    if ver_meta["scm_revision"]:
        source += f" from revision {ver_meta['scm_revision']}"
        if source_date := ver_meta["source_date"]:
            nice_date = source_date.strftime("%Y-%m-%dT%H:%MZ")
            source += f" ({nice_date})"
    if source:
        extras.append(f", built{source}")
    if ver_meta["target"]:
        extras.append(f"for {ver_meta['target']}")

    ver_line = buildmeta.get_version_string() + " ".join(extras)
    logger.info(f"starting EdgeDB server {ver_line}")
    logger.info(f'instance name: {args.instance_name!r}')
    if devmode.is_in_dev_mode():
        logger.info(f'development mode active')

    logger.debug(
        f"defaulting to the '{args.default_auth_method}' authentication method"
    )

    _init_parsers()

    pg_cluster_init_by_us = False

    if args.tenant_id is None:
        tenant_id = buildmeta.get_default_tenant_id()
    else:
        tenant_id = f'C{args.tenant_id}'

    cluster: Union[pgcluster.Cluster, pgcluster.RemoteCluster]
    default_runstate_dir: Optional[pathlib.Path]

    if args.data_dir:
        default_runstate_dir = args.data_dir
    else:
        default_runstate_dir = None

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

    with runstate_dir_mgr as runstate_dir:
        runstate_dir_str = str(runstate_dir)
        runstate_dir_str_len = len(
            runstate_dir_str.encode(
                sys.getfilesystemencoding(),
                errors=sys.getfilesystemencodeerrors(),
            ),
        )
        if runstate_dir_str_len > defines.MAX_RUNSTATE_DIR_PATH:
            abort(
                f'the length of the specified path for server run state '
                f'exceeds the maximum of {defines.MAX_RUNSTATE_DIR_PATH} '
                f'bytes: {runstate_dir_str!r} ({runstate_dir_str_len} bytes)',
                exit_code=11,
            )

        try:
            if args.data_dir:
                cluster, args = await _get_local_pgcluster(
                    args, runstate_dir, tenant_id)
            elif args.backend_dsn:
                cluster, args = await _get_remote_pgcluster(args, tenant_id)
            else:
                # This should have been checked by main() already,
                # but be extra careful.
                abort('neither the data directory nor the remote Postgres DSN '
                      'have been specified')
        except pgcluster.ClusterError as e:
            abort(str(e))

        try:
            pg_cluster_init_by_us = await cluster.ensure_initialized()
            cluster_status = await cluster.get_status()

            if isinstance(cluster, pgcluster.Cluster):
                if cluster_status == 'running':
                    # Refuse to start local instance on an occupied datadir,
                    # as it's very likely that Postgres was orphaned by an
                    # earlier unclean exit of EdgeDB.
                    main_pid = cluster.get_main_pid() or '<unknown>'
                    abort(
                        f'a PostgreSQL instance (PID {main_pid}) is already '
                        f'running in data directory "{args.data_dir}", please '
                        f'stop it to proceed'
                    )
                elif cluster_status == 'stopped':
                    await cluster.start()
                else:
                    abort('could not initialize data directory "%s"',
                          args.data_dir)
            else:
                # We expect the remote cluster to be running
                if cluster_status != "running":
                    abort('specified PostgreSQL instance is not running')

            need_cluster_restart = await _init_cluster(cluster, args)

            if need_cluster_restart:
                logger.info('Restarting server to reload configuration...')
                await cluster.stop()
                await cluster.start()

            if (
                not args.bootstrap_only
                or args.bootstrap_script
                or args.bootstrap_command
                or (
                    args.tls_cert_mode
                    is srvargs.ServerTlsCertMode.SelfSigned
                )
                or (
                    args.jose_key_mode
                    is srvargs.JOSEKeyMode.Generate
                )
            ):
                conn_params = cluster.get_connection_params()
                instance_name = args.instance_name
                conn_params = dataclasses.replace(
                    conn_params,
                    server_settings={
                        **conn_params.server_settings,
                        'application_name': f'edgedb_instance_{instance_name}',
                        'edgedb.instance_name': instance_name,
                        'edgedb.server_version': buildmeta.get_version_json(),
                    },
                )
                if args.data_dir:
                    conn_params.database = pgcluster.get_database_backend_name(
                        defines.EDGEDB_TEMPLATE_DB,
                        tenant_id=tenant_id,
                    )

                cluster.set_connection_params(conn_params)

                with _internal_state_dir(runstate_dir) as int_runstate_dir:
                    if (
                        args.tls_cert_file
                        and '<runstate>' in str(args.tls_cert_file)
                    ):
                        args = args._replace(
                            tls_cert_file=pathlib.Path(
                                str(args.tls_cert_file).replace(
                                    '<runstate>', int_runstate_dir)
                            ),
                            tls_key_file=pathlib.Path(
                                str(args.tls_key_file).replace(
                                    '<runstate>', int_runstate_dir)
                            )
                        )
                    if (
                        args.jws_key_file
                        and '<runstate>' in str(args.jws_key_file)
                    ):
                        args = args._replace(
                            jws_key_file=pathlib.Path(
                                str(args.jws_key_file).replace(
                                    '<runstate>', int_runstate_dir)
                            ),
                        )
                    if (
                        args.jwe_key_file
                        and '<runstate>' in str(args.jwe_key_file)
                    ):
                        args = args._replace(
                            jwe_key_file=pathlib.Path(
                                str(args.jwe_key_file).replace(
                                    '<runstate>', int_runstate_dir)
                            ),
                        )

                    await _run_server(
                        cluster,
                        args,
                        runstate_dir,
                        int_runstate_dir,
                        do_setproctitle=do_setproctitle,
                        new_instance=need_cluster_restart,
                    )

        except server.StartupError as e:
            abort(str(e))

        except BaseException:
            if pg_cluster_init_by_us and not _server_initialized:
                logger.warning(
                    'server bootstrap did not complete successfully, '
                    'removing the data directory')
                if await cluster.get_status() == 'running':
                    await cluster.stop()
                cluster.destroy()
            raise

        finally:
            if args.temp_dir:
                if await cluster.get_status() == 'running':
                    await cluster.stop()
                cluster.destroy()
            elif await cluster.get_status() == 'running':
                await cluster.stop()


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


def server_main(**kwargs):
    exceptions.install_excepthook()

    bump_rlimit_nofile()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    if kwargs['devmode'] is not None:
        devmode.enable_dev_mode(kwargs['devmode'])

    try:
        server_args = srvargs.parse_args(**kwargs)
    except srvargs.InvalidUsageError as e:
        abort(e.args[0], exit_code=e.args[1])

    if kwargs['background']:
        daemon_opts = {'detach_process': True}
        pidfile = kwargs['pidfile_dir'] / f".s.EDGEDB.{kwargs['port']}.lock"
        daemon_opts['pidfile'] = pidfile
        if kwargs['daemon_user']:
            daemon_opts['uid'] = kwargs['daemon_user']
        if kwargs['daemon_group']:
            daemon_opts['gid'] = kwargs['daemon_group']
        with daemon.DaemonContext(**daemon_opts):
            asyncio.run(run_server(server_args, setproctitle=True))
    else:
        with devmode.CoverageConfig.enable_coverage_if_requested():
            asyncio.run(run_server(server_args))


@click.group(
    'EdgeDB Server',
    invoke_without_command=True,
    context_settings=dict(help_option_names=['-h', '--help']))
@srvargs.server_options
@click.pass_context
def main(ctx, version=False, **kwargs):
    if kwargs.get('testmode') and 'EDGEDB_TEST_CATALOG_VERSION' in os.environ:
        buildmeta.EDGEDB_CATALOG_VERSION = int(
            os.environ['EDGEDB_TEST_CATALOG_VERSION']
        )
    if version:
        print(f"edgedb-server, version {buildmeta.get_version()}")
        sys.exit(0)
    logsetup.setup_logging(kwargs['log_level'], kwargs['log_to'])
    if ctx.invoked_subcommand is None:
        server_main(**kwargs)


@main.command(hidden=True)
@srvargs.compiler_options
def compiler(**kwargs):
    from edb.server.compiler_pool import server as compiler_server

    asyncio.run(compiler_server.server_main(**kwargs))


def main_dev():
    devmode.enable_dev_mode()
    main()


if __name__ == '__main__':
    main()
