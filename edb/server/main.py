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
from typing import (
    Any,
    Optional,
    Tuple,
    Union,
    Iterator,
    Mapping,
    Dict,
    NoReturn,
    TYPE_CHECKING,
)

from edb.common.log import early_setup
# ruff: noqa: E402
early_setup()

import asyncio
import contextlib
import json
import logging
import os
import os.path
import pathlib

import immutables
import resource
import signal
import sys
import tempfile

import click
import setproctitle
import uvloop

from edb import buildmeta
from edb import errors
from edb.ir import statypes
from edb.common import exceptions
from edb.common import devmode
from edb.common import signalctl
from edb.common import debug

from . import config
from . import args as srvargs
from . import compiler as edbcompiler
from . import daemon
from . import defines
from . import logsetup
from . import pgcluster
from . import service_manager


if TYPE_CHECKING:
    from . import server
    from edb.server import bootstrap
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
        try:
            runstate_dir.mkdir(parents=True)
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
def _internal_state_dir(
    runstate_dir: pathlib.Path, args: srvargs.ServerConfig
) -> Iterator[Tuple[str, srvargs.ServerConfig]]:
    try:
        with tempfile.TemporaryDirectory(prefix="", dir=runstate_dir) as td:
            if (
                args.tls_cert_file
                and '<runstate>' in str(args.tls_cert_file)
            ):
                args = args._replace(
                    tls_cert_file=pathlib.Path(
                        str(args.tls_cert_file).replace(
                            '<runstate>', td)
                    ),
                    tls_key_file=pathlib.Path(
                        str(args.tls_key_file).replace(
                            '<runstate>', td)
                    )
                )
            if (
                args.jws_key_file
                and '<runstate>' in str(args.jws_key_file)
            ):
                args = args._replace(
                    jws_key_file=pathlib.Path(
                        str(args.jws_key_file).replace(
                            '<runstate>', td)
                    ),
                )
            yield td, args
    except PermissionError as ex:
        abort(f'cannot write to the runstate directory: '
              f'{ex!s}; please fix the permissions or use '
              f'--runstate-dir to specify the correct location')


async def _init_cluster(
    cluster, args: srvargs.ServerConfig
) -> tuple[bool, edbcompiler.Compiler]:
    from edb.server import bootstrap

    new_instance = await bootstrap.ensure_bootstrapped(cluster, args)
    global _server_initialized
    _server_initialized = True

    return new_instance


def _init_parsers():
    # Initialize parsers that are used in the server process.
    from edb.edgeql import parser as ql_parser

    ql_parser.preload_spec()


async def _run_server(
    cluster,
    args: srvargs.ServerConfig,
    runstate_dir,
    internal_runstate_dir,
    *,
    do_setproctitle: bool,
    new_instance: bool,
    compiler: edbcompiler.Compiler,
    init_con_data: list[config.ConState],
):

    sockets = service_manager.get_activation_listen_sockets()

    if sockets:
        logger.info("detected service manager socket activation")

    with signalctl.SignalController(signal.SIGINT, signal.SIGTERM) as sc:
        from . import tenant as edbtenant

        # max_backend_connections should've been calculated already by now
        assert args.max_backend_connections is not None
        tenant = edbtenant.Tenant(
            cluster,
            instance_name=args.instance_name,
            max_backend_connections=args.max_backend_connections,
            backend_adaptive_ha=args.backend_adaptive_ha,
            extensions_dir=args.extensions_dir,
        )
        tenant.set_init_con_data(init_con_data)
        tenant.set_reloadable_files(
            readiness_state_file=args.readiness_state_file,
            jwt_sub_allowlist_file=args.jwt_sub_allowlist_file,
            jwt_revocation_list_file=args.jwt_revocation_list_file,
            config_file=args.config_file,
        )
        ss = server.Server(
            runstate_dir=runstate_dir,
            internal_runstate_dir=internal_runstate_dir,
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
            default_auth_method=args.default_auth_method,
            testmode=args.testmode,
            daemonized=args.background,
            pidfile_dir=args.pidfile_dir,
            new_instance=new_instance,
            admin_ui=args.admin_ui,
            disable_dynamic_system_config=args.disable_dynamic_system_config,
            compiler_state=compiler.state,
            tenant=tenant,
            use_monitor_fs=args.reload_config_files in [
                srvargs.ReloadTrigger.Default,
                srvargs.ReloadTrigger.FileSystemEvent,
            ],
            net_worker_mode=args.net_worker_mode,
        )
        magic_smtp = os.getenv('EDGEDB_MAGIC_SMTP_CONFIG')
        if magic_smtp:
            await tenant.load_sidechannel_configs(
                json.loads(magic_smtp), compiler=compiler
            )
        if args.config_file:
            await tenant.load_config_file(compiler)
        # This coroutine runs as long as the server,
        # and compiler(.state) is *heavy*, so make sure we don't
        # keep a reference to it.
        del compiler
        await sc.wait_for(ss.init())

        (
            tls_cert_newly_generated, jws_keys_newly_generated
        ) = await ss.maybe_generate_pki(args, ss)

        if args.bootstrap_only:
            if args.startup_script and new_instance:
                await sc.wait_for(ss.run_startup_script_and_exit())
            return

        ss.init_tls(
            args.tls_cert_file,
            args.tls_key_file,
            tls_cert_newly_generated,
            args.tls_client_ca_file,
        )

        ss.init_jwcrypto(args.jws_key_file, jws_keys_newly_generated)
        ss.start_watching_files()

        def load_configuration(_signum):
            if args.reload_config_files not in [
                srvargs.ReloadTrigger.Default,
                srvargs.ReloadTrigger.Signal,
            ]:
                logger.info(
                    "SIGHUP received, but reload on signal is disabled"
                )
                return

            logger.info("reloading configuration")
            try:
                if args.readiness_state_file:
                    tenant.reload_readiness_state()
                ss.reload_tls(
                    args.tls_cert_file,
                    args.tls_key_file,
                    args.tls_client_ca_file,
                )
                ss.load_jwcrypto(args.jws_key_file)
                tenant.reload_config_file.schedule()
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
                reload_ctl.add_handler(
                    load_configuration,
                    signals=(signal.SIGHUP,)
                )

                try:
                    await sc.wait_for(ss.serve_forever())
                except signalctl.SignalError as e:
                    logger.info('Received signal: %s.', e.signo)
        finally:
            service_manager.sd_notify('STOPPING=1')
            logger.info('Shutting down.')
            await sc.wait_for(ss.stop())


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
    cluster.update_connection_params(
        user='postgres',
        database='template1',
        server_settings={
            "application_name": f'edgedb_instance_{args.instance_name}',
        }
    )
    return cluster, args


async def _get_remote_pgcluster(
    args: srvargs.ServerConfig,
    tenant_id: str,
) -> Tuple[pgcluster.RemoteCluster, srvargs.ServerConfig]:

    cluster = await pgcluster.get_remote_pg_cluster(
        args.backend_dsn,
        tenant_id=tenant_id,
        specified_capabilities=args.backend_capability_sets,
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

    cluster.update_connection_params(server_settings={
        'application_name': f'edgedb_instance_{args.instance_name}'
    })

    return cluster, args


def _patch_stdlib_testmode(
    stdlib: bootstrap.StdlibBits
) -> bootstrap.StdlibBits:
    from edb import edgeql
    from edb.pgsql import delta as delta_cmds
    from edb.pgsql import params as pg_params
    from edb.edgeql import ast as qlast
    from edb.schema import ddl as s_ddl
    from edb.schema import delta as sd
    from edb.schema import schema as s_schema
    from edb.schema import std as s_std

    schema: s_schema.Schema = s_schema.ChainedSchema(
        s_schema.EMPTY_SCHEMA,
        stdlib.stdschema,
        stdlib.global_schema,
    )
    reflschema = stdlib.reflschema
    ctx = sd.CommandContext(
        stdmode=True,
        backend_runtime_params=pg_params.get_default_runtime_params(),
    )

    for modname in s_schema.TESTMODE_SOURCES:
        ddl_text = s_std.get_std_module_text(modname)
        for ddl_cmd in edgeql.parse_block(ddl_text):
            assert isinstance(ddl_cmd, qlast.DDLCommand)
            delta = s_ddl.delta_from_ddl(
                ddl_cmd, modaliases={}, schema=schema, stdmode=True
            )
            if not delta.canonical:
                sd.apply(delta, schema=schema)
            delta = delta_cmds.CommandMeta.adapt(delta)
            schema = sd.apply(delta, schema=schema, context=ctx)
            reflschema = delta.apply(reflschema, ctx)

    assert isinstance(schema, s_schema.ChainedSchema)
    return stdlib._replace(
        stdschema=schema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        reflschema=reflschema,
    )


async def run_server(
    args: srvargs.ServerConfig,
    *,
    do_setproctitle: bool=False,
    runstate_dir: pathlib.Path,
) -> None:
    from . import server as server_mod
    global server
    server = server_mod

    logsetup.setup_logging(args.log_level, args.log_to)

    logger.info(f"starting Gel server {buildmeta.get_version_line()}")
    if args.multitenant_config_file:
        logger.info("configured as a multitenant instance")
    else:
        logger.info(f'instance name: {args.instance_name!r}')
    if devmode.is_in_dev_mode():
        logger.info(f'development mode active')

    if fd_str := os.environ.get("EDGEDB_SERVER_EXTERNAL_LOCK_FD"):
        try:
            fd = int(fd_str)
        except ValueError:
            logger.info("Invalid EDGEDB_SERVER_EXTERNAL_LOCK_FD")
        else:
            os.set_inheritable(fd, False)
    if fd_str := os.environ.get("GEL_SERVER_EXTERNAL_LOCK_FD"):
        try:
            fd = int(fd_str)
        except ValueError:
            logger.info("Invalid GEL_SERVER_EXTERNAL_LOCK_FD")
        else:
            os.set_inheritable(fd, False)

    logger.debug(
        f"defaulting to the '{args.default_auth_method}' authentication method"
    )

    if debug.flags.pydebug_listen:
        import debugpy

        debugpy.listen(38782)

    _init_parsers()

    pg_cluster_init_by_us = False

    if args.tenant_id is None:
        tenant_id = buildmeta.get_default_tenant_id()
    else:
        tenant_id = f'C{args.tenant_id}'

    cluster: Union[pgcluster.Cluster, pgcluster.RemoteCluster]

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

    if args.multitenant_config_file:
        from edb.schema import reflection as s_refl
        from . import bootstrap
        from . import multitenant

        try:
            stdlib: bootstrap.StdlibBits | None
            stdlib = bootstrap.read_data_cache(
                bootstrap.STDLIB_CACHE_FILE_NAME, pickled=True
            )
            if stdlib is None:
                abort(
                    "Cannot run multi-tenant server "
                    "without pre-compiled standard library"
                )
            if args.testmode:
                # In multitenant mode, the server/compiler is started without a
                # backend and will be connected to many backends. That means we
                # cannot load the stdlib from a certain backend; instead, the
                # pre-compiled stdlib is always in use. This means that we need
                # to explicitly enable --testmode starting a multitenant server
                # in order to handle backends with test-mode schema properly.
                try:
                    stdlib = _patch_stdlib_testmode(stdlib)
                except errors.SchemaError:
                    # The pre-compiled standard library already has test-mode
                    # schema; ignore the patching error.
                    pass

            compiler = edbcompiler.new_compiler(
                stdlib.stdschema,
                stdlib.reflschema,
                stdlib.classlayout,
                config_spec=None,
            )
            reflection = s_refl.generate_structure(
                stdlib.reflschema, make_funcs=False,
            )
            (
                local_intro_sql, global_intro_sql
            ) = bootstrap.compile_intro_queries_stdlib(
                compiler=compiler,
                user_schema=stdlib.reflschema,
                reflection=reflection,
            )
            del reflection
            compiler_state = edbcompiler.CompilerState(
                std_schema=compiler.state.std_schema,
                refl_schema=compiler.state.refl_schema,
                schema_class_layout=stdlib.classlayout,
                backend_runtime_params=(
                    compiler.state.backend_runtime_params
                ),
                config_spec=compiler.state.config_spec,
                local_intro_query=local_intro_sql,
                global_intro_query=global_intro_sql,
            )
            del local_intro_sql, global_intro_sql
            (
                sys_queries,
                report_configs_typedesc_1_0,
                report_configs_typedesc_2_0,
            ) = bootstrap.compile_sys_queries(
                stdlib.reflschema,
                compiler,
                compiler_state.config_spec,
            )

            sys_config, backend_settings, init_con_data = (
                initialize_static_cfg(
                    args,
                    is_remote_cluster=True,
                    compiler=compiler,
                )
            )
            del compiler
            if backend_settings:
                abort(
                    'Static backend settings for remote backend are '
                    'not supported'
                )
            with _internal_state_dir(runstate_dir, args) as (
                int_runstate_dir,
                args,
            ):
                return await multitenant.run_server(
                    args,
                    sys_config=sys_config,
                    sys_queries={
                        key: sql.encode("utf-8")
                        for key, sql in sys_queries.items()
                    },
                    report_config_typedesc={
                        (1, 0): report_configs_typedesc_1_0,
                        (2, 0): report_configs_typedesc_2_0,
                    },
                    runstate_dir=runstate_dir,
                    internal_runstate_dir=int_runstate_dir,
                    do_setproctitle=do_setproctitle,
                    compiler_state=compiler_state,
                    init_con_data=init_con_data,
                )
        except server.StartupError as e:
            abort(str(e))

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
        logger.debug("postgres cluster status: %s", cluster_status)

        if isinstance(cluster, pgcluster.Cluster):
            is_local_cluster = True
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
            is_local_cluster = False
            if cluster_status != "running":
                abort('specified PostgreSQL instance is not running')

        logger.info("postgres cluster is running")

        if (
            args.inplace_upgrade_prepare
            or args.inplace_upgrade_finalize
            or args.inplace_upgrade_rollback
        ):
            from . import inplace_upgrade
            await inplace_upgrade.inplace_upgrade(cluster, args)
            return

        new_instance, compiler = await _init_cluster(cluster, args)

        _, backend_settings, init_con_data = initialize_static_cfg(
            args,
            is_remote_cluster=not is_local_cluster,
            compiler=compiler,
        )

        if is_local_cluster:
            if new_instance or backend_settings:
                logger.info('Restarting server to reload configuration...')
                await cluster.stop()
                await cluster.start(server_settings=backend_settings)
        elif backend_settings:
            abort(
                'Static backend settings for remote backend are not supported'
            )
        del backend_settings

        if (
            not args.bootstrap_only
            or args.bootstrap_command_file
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
            instance_name = args.instance_name
            database = pgcluster.get_database_backend_name(
                defines.EDGEDB_TEMPLATE_DB,
                tenant_id=tenant_id,
            ) if args.data_dir else None
            server_settings = {
                'application_name': f'edgedb_instance_{instance_name}',
                'edgedb.instance_name': instance_name,
                'edgedb.server_version': buildmeta.get_version_json(),
            }
            if database:
                cluster.update_connection_params(
                    database=database,
                    server_settings=server_settings
                )
            else:
                cluster.update_connection_params(
                    server_settings=server_settings
                )

            with _internal_state_dir(runstate_dir, args) as (
                int_runstate_dir,
                args,
            ):
                await _run_server(
                    cluster,
                    args,
                    runstate_dir,
                    int_runstate_dir,
                    do_setproctitle=do_setproctitle,
                    new_instance=new_instance,
                    compiler=compiler,
                    init_con_data=init_con_data,
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


def server_main(**kwargs: Any) -> None:
    exceptions.install_excepthook()

    bump_rlimit_nofile()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    if kwargs['devmode'] is not None:
        devmode.enable_dev_mode(kwargs['devmode'])

    try:
        server_args = srvargs.parse_args(**kwargs)
    except srvargs.InvalidUsageError as e:
        abort(e.args[0], exit_code=e.args[1])

    if server_args.data_dir:
        default_runstate_dir = server_args.data_dir
    else:
        default_runstate_dir = None

    specified_runstate_dir: Optional[pathlib.Path]
    if server_args.runstate_dir:
        specified_runstate_dir = server_args.runstate_dir
    elif server_args.bootstrap_only:
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
        if server_args.background:
            daemon_opts: dict[str, Any] = {'detach_process': True}
            if server_args.daemon_user:
                daemon_opts['uid'] = server_args.daemon_user
            if server_args.daemon_group:
                daemon_opts['gid'] = server_args.daemon_group
            with daemon.DaemonContext(**daemon_opts):
                asyncio.run(run_server(
                    server_args,
                    runstate_dir=runstate_dir,
                ))
        else:
            with devmode.CoverageConfig.enable_coverage_if_requested():
                asyncio.run(run_server(
                    server_args,
                    runstate_dir=runstate_dir,
                ))


@click.group(
    'Gel Server',
    invoke_without_command=True,
    context_settings=dict(help_option_names=['-h', '--help'])
)
@srvargs.server_options
@click.pass_context
def main(ctx, version=False, **kwargs):
    if kwargs.get('testmode') and 'GEL_TEST_CATALOG_VERSION' in os.environ:
        buildmeta.EDGEDB_CATALOG_VERSION = int(
            os.environ['GEL_TEST_CATALOG_VERSION']
        )
    elif kwargs.get('testmode') and 'EDGEDB_TEST_CATALOG_VERSION' in os.environ:
        buildmeta.EDGEDB_CATALOG_VERSION = int(
            os.environ['EDGEDB_TEST_CATALOG_VERSION']
        )
    if version:
        print(f"gel-server, version {buildmeta.get_version()}")
        sys.exit(0)
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


def initialize_static_cfg(
    args: srvargs.ServerConfig,
    is_remote_cluster: bool,
    compiler: edbcompiler.Compiler,
) -> Tuple[
    Mapping[str, config.SettingValue], Dict[str, str], list[config.ConState]
]:
    result = {}
    init_con_script_data: list[config.ConState] = []
    backend_settings = {}
    config_spec = compiler.state.config_spec
    sources = {
        config.ConStateType.command_line_argument: "command line argument",
        config.ConStateType.environment_variable: "environment variable",
    }

    def add_config_values(obj: dict[str, Any], source: config.ConStateType):
        settings = compiler.compile_structured_config(
            {"cfg::Config": obj}, source=sources[source]
        )["cfg::Config"]
        for name, value in settings.items():
            setting = config_spec[name]

            if is_remote_cluster:
                if setting.backend_setting and setting.requires_restart:
                    if source == config.ConStateType.command_line_argument:
                        where = "on command line"
                    else:
                        where = "as an environment variable"
                    raise server.StartupError(
                        f"Can't set config {name!r} {where} when using "
                        f"a remote Postgres cluster"
                    )
            init_con_script_data.append({
                "name": name,
                "value": config.value_to_json_value(setting, value.value),
                "type": source,
            })
            result[name] = value
            if setting.backend_setting:
                backend_val = value.value
                if isinstance(backend_val, statypes.ScalarType):
                    backend_val = backend_val.to_backend_str()
                backend_settings[setting.backend_setting] = str(backend_val)

    values: dict[str, Any] = {}
    translate_env = {
        "EDGEDB_SERVER_BIND_ADDRESS": "listen_addresses",
        "EDGEDB_SERVER_PORT": "listen_port",
        "GEL_SERVER_BIND_ADDRESS": "listen_addresses",
        "GEL_SERVER_PORT": "listen_port",
    }
    for name, value in os.environ.items():
        if cfg := translate_env.get(name):
            values[cfg] = value
        else:
            cfg = name.removeprefix("EDGEDB_SERVER_CONFIG_cfg::")
            if cfg != name:
                values[cfg] = value
            else:
                cfg = name.removeprefix("GEL_SERVER_CONFIG_cfg::")
                if cfg != name:
                    values[cfg] = value
    if values:
        add_config_values(values, config.ConStateType.environment_variable)

    values = {}
    if args.bind_addresses:
        values["listen_addresses"] = args.bind_addresses
    if args.port:
        values["listen_port"] = args.port
    if values:
        add_config_values(values, config.ConStateType.command_line_argument)

    return immutables.Map(result), backend_settings, init_con_script_data


if __name__ == '__main__':
    main()
