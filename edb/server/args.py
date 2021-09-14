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

import logging
import pathlib
import re
import warnings
import sys
import tempfile

import click
import psutil

from edb import buildmeta
from edb.common import devmode
from edb.schema import defines as schema_defines

from . import defines


MIB = 1024 * 1024
RAM_MIB_PER_CONN = 100
TLS_CERT_FILE_NAME = "edbtlscert.pem"
TLS_KEY_FILE_NAME = "edbprivkey.pem"


logger = logging.getLogger('edb.server')


def abort(msg, *args) -> NoReturn:
    logger.critical(msg, *args)
    sys.exit(1)


class StartupScript(NamedTuple):

    text: str
    database: str
    user: str


class ServerConfig(NamedTuple):

    data_dir: pathlib.Path
    backend_dsn: str
    backend_adaptive_ha: bool
    tenant_id: Optional[str]
    ignore_other_tenants: bool
    log_level: str
    log_to: str
    bootstrap_only: bool
    bootstrap_command: str
    bootstrap_script: pathlib.Path
    default_database: Optional[str]
    default_database_user: Optional[str]
    devmode: bool
    testmode: bool
    bind_addresses: tuple[str]
    port: int
    background: bool
    pidfile_dir: pathlib.Path
    daemon_user: str
    daemon_group: str
    runstate_dir: pathlib.Path
    max_backend_connections: Optional[int]
    compiler_pool_size: int
    echo_runtime_info: bool
    emit_server_status: str
    temp_dir: bool
    auto_shutdown_after: float

    startup_script: Optional[StartupScript]
    status_sink: Optional[Callable[[str], None]]

    tls_cert_file: Optional[pathlib.Path]
    tls_key_file: Optional[pathlib.Path]
    generate_self_signed_cert: bool

    default_auth_method: str
    allow_insecure_binary_clients: bool
    allow_insecure_http_clients: bool

    instance_name: Optional[str]


class PathPath(click.Path):
    name = 'path'

    def convert(self, value, param, ctx):
        return pathlib.Path(super().convert(value, param, ctx)).absolute()


class PortType(click.ParamType):
    name = 'port'

    def convert(self, value, param, ctx):
        if value == 'auto':
            return 0

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


def _get_runstate_dir_default() -> str:
    try:
        return buildmeta.get_build_metadata_value("RUNSTATE_DIR")
    except buildmeta.MetadataError:
        return '<data-dir>'


def _validate_max_backend_connections(ctx, param, value):
    if value is not None and value < defines.BACKEND_CONNECTIONS_MIN:
        raise click.BadParameter(
            f'the minimum number of backend connections '
            f'is {defines.BACKEND_CONNECTIONS_MIN}')
    return value


def compute_default_max_backend_connections() -> int:
    total_mem = psutil.virtual_memory().total
    total_mem_mb = total_mem // MIB
    if total_mem_mb <= 1024:
        return defines.BACKEND_CONNECTIONS_MIN
    else:
        return max(
            total_mem_mb // RAM_MIB_PER_CONN,
            defines.BACKEND_CONNECTIONS_MIN,
        )


def adjust_testmode_max_connections(max_conns):
    # Some test cases will start a second EdgeDB server (default
    # max_backend_connections=10), so we should reserve some backend
    # connections for that. This is ideally calculated upon the edb test -j
    # option, but that also depends on the total available memory. We are
    # hard-coding 15 reserved connections here for simplicity.
    return max(1, max_conns // 2, max_conns - 15)


def _validate_compiler_pool_size(ctx, param, value):
    if value < defines.BACKEND_COMPILER_POOL_SIZE_MIN:
        raise click.BadParameter(
            f'the minimum value for the compiler pool size option '
            f'is {defines.BACKEND_COMPILER_POOL_SIZE_MIN}')
    return value


def compute_default_compiler_pool_size() -> int:
    total_mem = psutil.virtual_memory().total
    total_mem_mb = total_mem // MIB
    if total_mem_mb <= 1024:
        return defines.BACKEND_COMPILER_POOL_SIZE_MIN
    else:
        return max(
            psutil.cpu_count(logical=False) or 0,
            defines.BACKEND_COMPILER_POOL_SIZE_MIN,
        )


def _validate_tenant_id(ctx, param, value):
    if value is not None:
        if len(value) > schema_defines.MAX_TENANT_ID_LENGTH:
            raise click.BadParameter(
                f'cannot be longer than'
                f' {schema_defines.MAX_TENANT_ID_LENGTH} characters')
        if not value.isalnum() or not value.isascii():
            raise click.BadParameter(
                f'contains invalid characters')

    return value


def _status_sink_file(path: str) -> Callable[[str], None]:
    def _writer(status: str) -> None:
        try:
            with open(path, 'a') as f:
                print(status, file=f, flush=True)
        except OSError as e:
            logger.warning(
                f'could not write server status to {path!r}: {e.strerror}')
        except Exception as e:
            logger.warning(
                f'could not write server status to {path!r}: {e}')

    return _writer


def _status_sink_fd(fileno: int) -> Callable[[str], None]:
    def _writer(status: str) -> None:
        try:
            with open(fileno, mode='a', closefd=False) as f:
                print(status, file=f, flush=True)
        except OSError as e:
            logger.warning(
                f'could not write server status to fd://{fileno!r}: '
                f'{e.strerror}')
        except Exception as e:
            logger.warning(
                f'could not write server status to fd://{fileno!r}: {e}')

    return _writer


_server_options = [
    click.option(
        '-D', '--data-dir', type=PathPath(),
        help='database cluster directory'),
    click.option(
        '--postgres-dsn', type=str, hidden=True,
        help='[DEPRECATED] DSN of a remote Postgres cluster, if using one'),
    click.option(
        '--backend-dsn', type=str,
        help='DSN of a remote backend cluster, if using one. '
             'Also supports HA clusters, for example: stolon+consul+http://'
             'localhost:8500/test_cluster'),
    click.option(
        '--enable-backend-adaptive-ha', 'backend_adaptive_ha', is_flag=True,
        help='If backend adaptive HA is enabled, the EdgeDB server will '
             'monitor the health of the backend cluster and shutdown all '
             'backend connections if threshold is reached, until reconnected '
             'again using the same DSN (HA should have updated the DNS '
             'value). Default is disabled.'),
    click.option(
        '--tenant-id',
        type=str,
        callback=_validate_tenant_id,
        help='Specifies the tenant ID of this server when hosting'
             ' multiple EdgeDB instances on one Postgres cluster.'
             ' Must be an alphanumeric ASCII string, maximum'
             f' {schema_defines.MAX_TENANT_ID_LENGTH} characters long.',
    ),
    click.option(
        '--ignore-other-tenants',
        is_flag=True,
        help='If set, the server will ignore the presence of another tenant '
             'in the database instance in single-tenant mode instead of '
             'exiting with a catalog incompatibility error.'
    ),
    click.option(
        '-l', '--log-level',
        default='i',
        type=click.Choice(
            ['debug', 'd', 'info', 'i', 'warn', 'w',
             'error', 'e', 'silent', 's'],
            case_sensitive=False,
        ),
        help=(
            'Logging level.  Possible values: (d)ebug, (i)nfo, (w)arn, '
            '(e)rror, (s)ilent'
        )),
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
        '-I', '--bind-address', type=str, multiple=True,
        help='IP addresses to listen on, specify multiple times for more than '
             'one address to listen on'),
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
             f'{compute_default_max_backend_connections()} for local '
             f'Postgres or pg_settings.max_connections for remote Postgres, '
             f'minus the NUM of --reserved-pg-connections.',
        callback=_validate_max_backend_connections),
    click.option(
        '--compiler-pool-size', type=int,
        default=compute_default_compiler_pool_size(),
        callback=_validate_compiler_pool_size),
    click.option(
        '--echo-runtime-info', type=bool, default=False, is_flag=True,
        help='[DEPREATED, use --emit-server-status] '
             'echo runtime info to stdout; the format is JSON, prefixed by '
             '"EDGEDB_SERVER_DATA:", ended with a new line'),
    click.option(
        '--emit-server-status', type=str, default=None, metavar='DEST',
        help='Instruct the server to emit changes in status to DEST, '
             'where DEST is a URI specifying a file (file://<path>), '
             'or a file descriptor (fd://<fileno>).  If the URI scheme '
             'is not specified, file:// is assumed.'),
    click.option(
        '--temp-dir', type=bool, default=False, is_flag=True,
        help='create a temporary database cluster directory '
             'that will be automatically purged on server shutdown'),
    click.option(
        '--auto-shutdown', type=bool, default=False, is_flag=True, hidden=True,
        help='shutdown the server after the last ' +
             'connection is closed'),
    click.option(
        '--auto-shutdown-after', type=float, default=-1.0,
        help='shutdown the server after the last connection has been closed '
             'for N seconds. N < 0 is treated as infinite.'),
    click.option(
        '--tls-cert-file',
        type=PathPath(),
        help='Specify a path to a single file in PEM format containing the '
             'TLS certificate to run the server, as well as any number of CA '
             'certificates needed to establish the certificate’s '
             'authenticity. If not present, the server will try to find '
             f'`{TLS_CERT_FILE_NAME}` in the --data-dir if set.'),
    click.option(
        '--tls-key-file', type=PathPath(),
        help='Specify a path to a file containing the private key. If not '
             f'present, the server will try to find `{TLS_KEY_FILE_NAME}` in '
             'the --data dir if set. If not found, the private key will be '
             'taken from --tls-cert-file as well. If the private key is '
             'protected by a password, specify it with the environment '
             'variable: EDGEDB_SERVER_TLS_PRIVATE_KEY_PASSWORD.'),
    click.option(
        '--generate-self-signed-cert', type=bool, default=False, is_flag=True,
        help='When set, a new self-signed certificate will be generated '
             'together with its private key if no cert is found in the data '
             'dir. The generated files will be stored in the data dir, or a '
             'temporary dir (deleted once the server is stopped) if there is '
             'no data dir. This option conflicts with --tls-cert-file and '
             '--tls-key-file, and defaults to True in dev mode.'),
    click.option(
        '--allow-insecure-binary-clients',
        envvar='EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS',
        type=bool, is_flag=True, hidden=True,
        help='Allow non-TLS client binary connections.'),
    click.option(
        '--allow-insecure-http-clients',
        envvar="EDGEDB_SERVER_ALLOW_INSECURE_HTTP_CLIENTS",
        type=bool, is_flag=True, hidden=True,
        help='Allow non-TLS client HTTP connections.'),
    click.option(
        "--default-auth-method",
        envvar="EDGEDB_SERVER_DEFAULT_AUTH_METHOD",
        type=click.Choice(
            ['SCRAM', 'Trust'],
            case_sensitive=True,
        ),
        help=(
            "The default authentication method to use when none is "
            "explicitly configured. Defaults to 'SCRAM'."
        ),
    ),
    click.option(
        '--instance-name',
        envvar="EDGEDB_SERVER_INSTANCE_NAME",
        type=str, default=None, hidden=True,
        help='Server instance name.'),
    click.option(
        '--version', is_flag=True,
        help='Show the version and exit.')
]


def server_options(func):
    for option in reversed(_server_options):
        func = option(func)
    return func


def parse_args(**kwargs: Any):
    kwargs['bind_addresses'] = kwargs.pop('bind_address')

    if kwargs['echo_runtime_info']:
        warnings.warn(
            "The `--echo-runtime-info` option is deprecated, use "
            "`--emit-server-status` instead.",
            DeprecationWarning,
        )

    if kwargs['bootstrap']:
        warnings.warn(
            "Option `--bootstrap` is deprecated, use `--bootstrap-only`",
            DeprecationWarning,
        )
        kwargs['bootstrap_only'] = True

    kwargs.pop('bootstrap', False)

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

    if kwargs['auto_shutdown']:
        warnings.warn(
            "The `--auto-shutdown` option is deprecated, use "
            "`--auto-shutdown-after` instead.",
            DeprecationWarning,
        )
        if kwargs['auto_shutdown_after'] < 0:
            kwargs['auto_shutdown_after'] = 0

    del kwargs['auto_shutdown']

    if kwargs['postgres_dsn']:
        warnings.warn(
            "The `--postgres-dsn` option is deprecated, use "
            "`--backend-dsn` instead.",
            DeprecationWarning,
        )
        if not kwargs['backend_dsn']:
            kwargs['backend_dsn'] = kwargs['postgres_dsn']

    del kwargs['postgres_dsn']

    if not kwargs['default_auth_method']:
        kwargs['default_auth_method'] = 'SCRAM'

    if kwargs['temp_dir']:
        if kwargs['data_dir']:
            abort('--temp-dir is incompatible with --data-dir/-D')
        if kwargs['runstate_dir']:
            abort('--temp-dir is incompatible with --runstate-dir')
        if kwargs['backend_dsn']:
            abort('--temp-dir is incompatible with --backend-dsn')
        kwargs['data_dir'] = kwargs['runstate_dir'] = pathlib.Path(
            tempfile.mkdtemp())
    else:
        if not kwargs['data_dir']:
            if kwargs['backend_dsn']:
                pass
            elif devmode.is_in_dev_mode():
                data_dir = devmode.get_dev_mode_data_dir()
                if not data_dir.parent.exists():
                    data_dir.parent.mkdir(exist_ok=True, parents=True)

                kwargs["data_dir"] = data_dir
            else:
                abort('Please specify the instance data directory '
                      'using the -D argument or the address of a remote '
                      'backend cluster using the --backend-dsn argument')
        elif kwargs['backend_dsn']:
            abort('The -D and --backend-dsn options are mutually exclusive.')

    if kwargs['tls_cert_file'] or kwargs['tls_key_file']:
        if tls_cert_file := kwargs['tls_cert_file']:
            if kwargs['generate_self_signed_cert']:
                abort("--tls-cert-file and --generate-self-signed-cert are "
                      "mutually exclusive.")
            tls_cert_file = tls_cert_file.resolve()
            if not tls_cert_file.exists():
                abort(f"File doesn't exist: --tls-cert-file={tls_cert_file}")
            kwargs['tls_cert_file'] = tls_cert_file
        elif kwargs['data_dir'] and (
            tls_cert_file := kwargs['data_dir'] / TLS_CERT_FILE_NAME
        ).exists():
            kwargs['tls_cert_file'] = tls_cert_file
        else:
            abort("Cannot find --tls-cert-file, but --tls-key-file is set")

        if tls_key_file := kwargs['tls_key_file']:
            if kwargs['generate_self_signed_cert']:
                abort("--tls-key-file and --generate-self-signed-cert are "
                      "mutually exclusive.")
            tls_key_file = tls_key_file.resolve()
            if not tls_key_file.exists():
                abort(f"File doesn't exist: --tls-key-file={tls_key_file}")
            kwargs['tls_key_file'] = tls_key_file
        elif kwargs['data_dir'] and (
            tls_key_file := kwargs['data_dir'] / TLS_KEY_FILE_NAME
        ).exists():
            kwargs['tls_key_file'] = tls_key_file
    else:
        if devmode.is_in_dev_mode():
            kwargs['generate_self_signed_cert'] = True
        if data_dir := kwargs['data_dir']:
            if (tls_cert_file := data_dir / TLS_CERT_FILE_NAME).exists():
                kwargs['tls_cert_file'] = tls_cert_file
                kwargs['generate_self_signed_cert'] = False
                if (tls_key_file := data_dir / TLS_KEY_FILE_NAME).exists():
                    kwargs['tls_key_file'] = tls_key_file
    if (
        not kwargs['generate_self_signed_cert']
        and not kwargs['tls_cert_file']
        and not kwargs['bootstrap_only']
    ):
        abort('Please specify a TLS certificate with --tls-cert-file.')

    if kwargs['log_level']:
        kwargs['log_level'] = kwargs['log_level'].lower()[0]

    bootstrap_script_text: Optional[str]
    if kwargs['bootstrap_script']:
        with open(kwargs['bootstrap_script']) as f:
            bootstrap_script_text = f.read()
    elif kwargs['bootstrap_command']:
        bootstrap_script_text = kwargs['bootstrap_command']
    else:
        bootstrap_script_text = None

    if bootstrap_script_text is None:
        startup_script = None
    else:
        startup_script = StartupScript(
            text=bootstrap_script_text,
            database=(
                kwargs['default_database'] or
                defines.EDGEDB_SUPERUSER_DB
            ),
            user=(
                kwargs['default_database_user'] or
                defines.EDGEDB_SUPERUSER
            ),
        )

    status_sink = None

    if status_sink_addr := kwargs['emit_server_status']:
        if status_sink_addr.startswith('file://'):
            status_sink = _status_sink_file(status_sink_addr[len('file://'):])
        elif status_sink_addr.startswith('fd://'):
            try:
                fileno = int(status_sink_addr[len('fd://'):])
            except ValueError:
                abort(
                    f'invalid file descriptor number in --emit-server-status: '
                    f'{status_sink_addr[len("fd://")]!r}'
                )

            status_sink = _status_sink_fd(fileno)
        elif m := re.match(r'(^\w+)://', status_sink_addr):
            abort(
                f'unsupported destination scheme in --emit-server-status: '
                f'{m.group(1)}'
            )
        else:
            # Assume it's a file.
            status_sink = _status_sink_file(status_sink_addr)

    return ServerConfig(
        startup_script=startup_script,
        status_sink=status_sink,
        **kwargs,
    )
