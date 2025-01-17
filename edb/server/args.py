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
    Callable,
    Optional,
    ItemsView,
    Mapping,
    List,
    NamedTuple,
    NoReturn,
    Sequence,
)

import logging
import os
import pathlib
import re
import warnings
import tempfile

import click
import psutil

from edb import buildmeta
from edb.common import devmode
from edb.common import enum
from edb.common import typeutils
from edb.schema import defines as schema_defines
from edb.pgsql import params as pgsql_params

from . import defines


MIB = 1024 * 1024
RAM_MIB_PER_CONN = 100
TLS_CERT_FILE_NAME = "edbtlscert.pem"
TLS_KEY_FILE_NAME = "edbprivkey.pem"
JWS_KEY_FILE_NAME = "edbjwskeys.pem"


logger = logging.getLogger('edb.server')


class InvalidUsageError(Exception):

    def __init__(self, msg: str, exit_code: int = 2) -> None:
        super().__init__(msg, exit_code)


def abort(msg: str, *, exit_code: int = 2) -> NoReturn:
    raise InvalidUsageError(msg, exit_code)


class StartupScript(NamedTuple):

    text: str
    database: str
    user: str


class ServerSecurityMode(enum.StrEnum):

    Strict = "strict"
    InsecureDevMode = "insecure_dev_mode"


class ServerEndpointSecurityMode(enum.StrEnum):

    Tls = "tls"
    Optional = "optional"


class ServerTlsCertMode(enum.StrEnum):

    RequireFile = "require_file"
    SelfSigned = "generate_self_signed"


class JOSEKeyMode(enum.StrEnum):

    RequireFile = "require_file"
    Generate = "generate"


class ReadinessState(enum.StrEnum):

    Default = "default"
    """Default state: serving normally"""

    NotReady = "not_ready"
    """/server/status/ready returns an error, but clients can still connect."""

    ReadOnly = "read_only"
    """Only read-only queries are allowed."""

    Offline = "offline"
    """Any existing connections are gracefully terminated and no new
    connections are accepted."""

    Blocked = "blocked"
    """Any existing connections are gracefully terminated and all
    new connections are accepted but are immediately terminated
    with a ServerBlockedError."""


class ServerAuthMethod(enum.StrEnum):

    Auto = "auto"
    Trust = "Trust"
    Scram = "SCRAM"
    JWT = "JWT"
    Password = "Password"
    mTLS = "mTLS"


class ServerConnTransport(enum.StrEnum):

    HTTP = "HTTP"
    TCP = "TCP"
    TCP_PG = "TCP_PG"
    SIMPLE_HTTP = "SIMPLE_HTTP"
    HTTP_METRICS = "HTTP_METRICS"
    HTTP_HEALTH = "HTTP_HEALTH"


class ReloadTrigger(enum.StrEnum):
    """
    Configure what triggers the reload of the following config files:
    1. TLS certificate and key (server config)
    2. JWS key (server config)
    3. Multi-tenant config file (server config)
    4. Readiness state (server or tenant config)
    5. JWT sub allowlist and revocation list (server or tenant config)
    6. The TOML config file (server or tenant config)
    """

    Default = "default"
    """By default, reload on both SIGHUP and fsevent."""

    Never = "never"
    """Disable the reload function."""

    Signal = "signal"
    """Only reload on SIGHUP."""

    FileSystemEvent = "fsevent"
    """Watch the files for changes and reload when it happens."""


class NetWorkerMode(enum.StrEnum):
    Default = "default"
    Disabled = "disabled"


class ServerAuthMethods:

    def __init__(
        self,
        methods: Mapping[ServerConnTransport, list[ServerAuthMethod]],
    ) -> None:
        self._methods = dict(methods)

    def get(self, transport: ServerConnTransport) -> list[ServerAuthMethod]:
        return self._methods[transport]

    def items(self) -> ItemsView[ServerConnTransport, list[ServerAuthMethod]]:
        return self._methods.items()

    def __str__(self):
        return ','.join(
            f'{t.lower()}:{'/'.join(m.lower() for m in mm)}'
            for t, mm in self._methods.items()
        )


DEFAULT_AUTH_METHODS = ServerAuthMethods({
    ServerConnTransport.TCP: [ServerAuthMethod.Scram],
    ServerConnTransport.TCP_PG: [ServerAuthMethod.Scram],
    ServerConnTransport.HTTP: [ServerAuthMethod.JWT],
    ServerConnTransport.SIMPLE_HTTP: [
        ServerAuthMethod.Password, ServerAuthMethod.JWT],
    ServerConnTransport.HTTP_METRICS: [ServerAuthMethod.Auto],
    ServerConnTransport.HTTP_HEALTH: [ServerAuthMethod.Auto],
})


class BackendCapabilitySets(NamedTuple):
    must_be_present: List[pgsql_params.BackendCapabilities]
    must_be_absent: List[pgsql_params.BackendCapabilities]


class CompilerPoolMode(enum.StrEnum):
    Default = "default"
    Fixed = "fixed"
    OnDemand = "on_demand"
    Remote = "remote"
    MultiTenant = "fixed_multi_tenant"

    def __init__(self, name):
        self.pool_class = None

    def assign_implementation(self, cls):
        # decorator function to link this enum with the actual implementation
        self.pool_class = cls
        return cls


class ServerConfig(NamedTuple):

    data_dir: pathlib.Path
    backend_dsn: str
    backend_adaptive_ha: bool
    tenant_id: Optional[str]
    ignore_other_tenants: bool
    multitenant_config_file: Optional[pathlib.Path]
    log_level: str
    log_to: str
    bootstrap_only: bool
    inplace_upgrade_prepare: Optional[pathlib.Path]
    inplace_upgrade_finalize: bool
    inplace_upgrade_rollback: bool
    bootstrap_command: str
    bootstrap_command_file: pathlib.Path
    default_branch: Optional[str]
    default_database: Optional[str]
    default_database_user: Optional[str]
    devmode: bool
    testmode: bool
    bind_addresses: list[str]
    port: int
    background: bool
    pidfile_dir: pathlib.Path
    daemon_user: str
    daemon_group: str
    runstate_dir: pathlib.Path
    extensions_dir: tuple[pathlib.Path, ...]
    max_backend_connections: Optional[int]
    compiler_pool_size: int
    compiler_pool_mode: CompilerPoolMode
    compiler_pool_addr: str
    compiler_pool_tenant_cache_size: int
    echo_runtime_info: bool
    emit_server_status: str
    temp_dir: bool
    auto_shutdown_after: float
    readiness_state_file: Optional[pathlib.Path]
    disable_dynamic_system_config: bool
    reload_config_files: ReloadTrigger
    net_worker_mode: NetWorkerMode
    config_file: Optional[pathlib.Path]

    startup_script: Optional[StartupScript]
    status_sinks: List[Callable[[str], None]]

    tls_cert_file: pathlib.Path
    tls_key_file: pathlib.Path
    tls_cert_mode: ServerTlsCertMode
    tls_client_ca_file: Optional[pathlib.Path]

    jws_key_file: pathlib.Path
    jose_key_mode: JOSEKeyMode
    jwt_sub_allowlist_file: Optional[pathlib.Path]
    jwt_revocation_list_file: Optional[pathlib.Path]

    default_auth_method: ServerAuthMethods
    security: ServerSecurityMode
    binary_endpoint_security: ServerEndpointSecurityMode
    http_endpoint_security: ServerEndpointSecurityMode

    instance_name: str

    backend_capability_sets: BackendCapabilitySets

    admin_ui: bool


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


class BackendCapabilitySet(click.ParamType):
    name = 'capability'

    def __init__(self):
        self.choices = {
            cap.name: cap
            for cap in pgsql_params.BackendCapabilities
            if cap.name != 'NONE'
        }

    def get_metavar(self, param):
        return " ".join(f'[[~]{cap}]' for cap in self.choices)

    def convert(self, value, param, ctx):
        must_be_present = []
        must_be_absent = []
        visited = set()
        for cap_str in value.split():
            try:
                if cap_str.startswith("~"):
                    cap = self.choices[cap_str[1:].upper()]
                    must_be_absent.append(cap)
                else:
                    cap = self.choices[cap_str.upper()]
                    must_be_present.append(cap)
                if cap in visited:
                    self.fail(f"duplicate capability: {cap_str}", param, ctx)
                else:
                    visited.add(cap)
            except KeyError:
                self.fail(
                    f"invalid capability: {cap_str}. "
                    f"(choose from {', '.join(self.choices)})",
                    param,
                    ctx,
                )
        return BackendCapabilitySets(
            must_be_present=must_be_present,
            must_be_absent=must_be_absent,
        )


class CompilerPoolModeChoice(click.Choice):
    def __init__(self):
        super().__init__(
            list(sorted(
                set(CompilerPoolMode.__members__.values())
                - {CompilerPoolMode.Remote}
            )),
        )

    def convert(self, value, param, ctx):
        if value == "remote":
            return CompilerPoolMode.Remote
        else:
            return super().convert(value, param, ctx)


def _get_runstate_dir_default() -> str:
    runstate_dir: Optional[str]

    try:
        runstate_dir = buildmeta.get_build_metadata_value("RUNSTATE_DIR")
    except buildmeta.MetadataError:
        runstate_dir = None

    if runstate_dir is None:
        runstate_dir = '<data-dir>'

    return runstate_dir


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
    if value is not None and value < defines.BACKEND_COMPILER_POOL_SIZE_MIN:
        raise click.BadParameter(
            f'the minimum value for the compiler pool size option '
            f'is {defines.BACKEND_COMPILER_POOL_SIZE_MIN}')
    return value


def _validate_host_port(ctx, param, value):
    if value is None:
        return None
    address = value.split(":", 1)
    if len(address) == 1:
        return address[0], defines.EDGEDB_REMOTE_COMPILER_PORT
    else:
        try:
            return address[0], int(address[1])
        except ValueError:
            raise click.BadParameter(f'port must be int: {address[1]}')


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


def _validate_default_auth_method(
    ctx: click.Context,
    param: click.Option | click.Parameter,
    value: Any,
) -> ServerAuthMethods | None:
    if value is None:
        return None

    methods = dict(DEFAULT_AUTH_METHODS.items())

    names = {v.lower(): v for v in ServerAuthMethod.__members__.values()}
    method = names.get(value.lower())
    if method is not None:
        # Single auth method value.
        #
        # HTTP does not support SCRAM, but for backward compatibility
        # if SCRAM is passed explicitly, default HTTP to JWT.
        if method in {ServerAuthMethod.Auto, ServerAuthMethod.Scram}:
            pass
        else:
            for t in methods:
                # HTTP_METRICS and HTTP_HEALTH support only mTLS, but for
                # backward compatibility, default them to `auto` if unsupported
                # method is passed explicitly.
                if t in (
                    ServerConnTransport.HTTP_METRICS,
                    ServerConnTransport.HTTP_HEALTH,
                ):
                    if method not in (
                        ServerAuthMethod.Trust,
                        ServerAuthMethod.mTLS,
                    ):
                        continue

                methods[t] = [method]
    elif "," not in value and ":" not in value:
        raise click.BadParameter(
            f"invalid authentication method: {value}, "
            f"supported values are: {', '.join(names)})"
        )
    else:
        # Per-transport configuration.
        transport_specs = value.split(",")
        transport_names = {
            v.lower(): v
            for v in ServerConnTransport.__members__.values()
        }
        for transport_spec in transport_specs:
            transport_spec = transport_spec.strip()
            transport_name, _, method_names = transport_spec.partition(':')
            if not method_names:
                raise click.BadParameter(
                    "format is <transport>:<method>[/method...][,...]")
            transport = transport_names.get(transport_name.lower())
            if not transport:
                raise click.BadParameter(
                    f"invalid connection transport: {transport_name}, "
                    f"supported values are: {', '.join(transport_names)})"
                )
            transport_methods = []
            for method_name in method_names.split('/'):
                method = names.get(method_name)
                if not method:
                    raise click.BadParameter(
                        f"invalid authentication method: {method_name}, "
                        f"supported values are: {', '.join(names)})"
                    )
                transport_methods.append(method)
            methods[transport] = transport_methods

    return ServerAuthMethods(methods)


def oxford_comma(els: Sequence[str]) -> str:
    '''Who gives a fuck?'''
    assert els
    if len(els) == 1:
        return els[0]
    elif len(els) == 2:
        return f'{els[0]} and {els[1]}'
    else:
        return f'{", ".join(els[:-1])}, and {els[-1]}'


class EnvvarResolver(click.Option):
    def resolve_envvar_value(self, ctx: click.Context):
        if self.envvar is None:
            return None

        if not isinstance(self.envvar, str):
            raise click.BadParameter(
                "expected a single envvar value but got multiple")

        file_var = f'{self.envvar}_FILE'
        alt_var = f'{self.envvar}_ENV'

        old_envvar = self.envvar.replace('GEL_', 'EDGEDB_')
        old_file_var = f'{old_envvar}_FILE'
        old_alt_var = f'{old_envvar}_ENV'

        vars_set = []
        for var, old_var in [
            (self.envvar, old_envvar),
            (file_var, old_file_var),
            (alt_var, old_alt_var),
        ]:
            if var in os.environ and old_var in os.environ:
                print(
                    f"Warning: both {var} and {old_var} are specified. "
                    f"{var} will take precedence."
                )
            if var in os.environ:
                vars_set.append(var)
            elif old_var in os.environ:
                vars_set.append(old_var)

        if len(vars_set) > 1:
            amt = "both" if len(vars_set) == 2 else "all"
            raise click.BadParameter(
                f'{oxford_comma(vars_set)} are exclusive, '
                f'but {amt} are set.'
            )

        var_val = os.environ.get(self.envvar) or os.environ.get(old_envvar)
        alt_var_val = os.environ.get(alt_var) or os.environ.get(old_alt_var)
        file_var_val = os.environ.get(file_var) or os.environ.get(old_file_var)

        if alt_var_val:
            var_val = os.environ.get(alt_var_val)

        if var_val:
            return var_val

        if file_var_val:
            try:
                with open(file_var_val, 'rt') as f:
                    return f.read()
            except Exception as e:
                raise click.BadParameter(
                    f'could not read the file specified by '
                    f'{file_var} ({file_var_val!r})') from e

        return None


server_options = typeutils.chain_decorators([
    click.option(
        '-D', '--data-dir', type=PathPath(),
        envvar="GEL_SERVER_DATADIR", cls=EnvvarResolver,
        help='database cluster directory'),
    click.option(
        '--postgres-dsn', type=str, hidden=True,
        help='[DEPRECATED] DSN of a remote Postgres cluster, if using one'),
    click.option(
        '--backend-dsn', type=str,
        envvar="GEL_SERVER_BACKEND_DSN", cls=EnvvarResolver,
        help='DSN of a remote backend cluster, if using one. '
             'Also supports HA clusters, for example: stolon+consul+http://'
             'localhost:8500/test_cluster'),
    click.option(
        '--enable-backend-adaptive-ha', 'backend_adaptive_ha', is_flag=True,
        help='If backend adaptive HA is enabled, the Gel server will '
             'monitor the health of the backend cluster and shutdown all '
             'backend connections if threshold is reached, until reconnected '
             'again using the same DSN (HA should have updated the DNS '
             'value). Default is disabled.'),
    click.option(
        '--tenant-id',
        type=str,
        callback=_validate_tenant_id,
        envvar="GEL_SERVER_TENANT_ID",
        cls=EnvvarResolver,
        help='Specifies the tenant ID of this server when hosting'
             ' multiple Gel instances on one Postgres cluster.'
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
        '--multitenant-config-file', type=PathPath(), metavar="PATH",
        envvar="GEL_SERVER_MULTITENANT_CONFIG_FILE",
        cls=EnvvarResolver,
        hidden=True,
        help='Start the server in multi-tenant mode, with reloadable tenants '
             'configured in the given file. Each tenant must have a unique '
             'SNI name as the key to route the traffic correctly, as well as '
             'a dedicated backend DSN to host the tenant data. See edb/server/'
             'multitenant.py for config file format. All tenants share the '
             'same compiler pool, thus the same stdlib. So if any of the '
             'backends contains test-mode schema, the server should be '
             'started with --testmode to handle them properly.',
    ),
    click.option(
        '-l', '--log-level',
        envvar="GEL_SERVER_LOG_LEVEL",
        cls=EnvvarResolver,
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
        envvar="GEL_SERVER_BOOTSTRAP_ONLY", cls=EnvvarResolver,
        help='bootstrap the database cluster and exit'),
    click.option(
        '--inplace-upgrade-prepare', type=PathPath(),
        envvar="GEL_SERVER_INPLACE_UPGRADE_PREPARE",
        cls=EnvvarResolver,
        help='try to do an in-place upgrade with the specified dump file'),
    click.option(
        '--inplace-upgrade-rollback', type=bool, is_flag=True,
        envvar="GEL_SERVER_INPLACE_UPGRADE_ROLLBACK",
        cls=EnvvarResolver,
        help='rollback a prepared upgrade'),
    click.option(
        '--inplace-upgrade-finalize', type=bool, is_flag=True,
        envvar="GEL_SERVER_INPLACE_UPGRADE_FINALIZE",
        cls=EnvvarResolver,
        help='finalize an in-place upgrade'),
    click.option(
        '--default-branch', type=str,
        help='the name of the default branch to create'),
    click.option(
        '--default-database', type=str, hidden=True,
        help='[DEPRECATED] the name of the default database to create'),
    click.option(
        '--default-database-user', type=str, hidden=True,
        help='[DEPRECATED] the name of the default database owner'),
    click.option(
        '--bootstrap-command', metavar="QUERIES",
        envvar="GEL_SERVER_BOOTSTRAP_COMMAND", cls=EnvvarResolver,
        help='run the commands when initializing the database. '
             'Queries are executed by default user within default '
             'database. May be used with or without `--bootstrap-only`.'),
    click.option(
        '--bootstrap-command-file', type=PathPath(), metavar="PATH",
        help='run the script when initializing the database. '
             'Script run by default user within default database. '
             'May be used with or without `--bootstrap-only`.'),
    click.option(
        '--bootstrap-script', type=PathPath(),
        help='[DEPRECATED] use --bootstrap-command-file instead.'),
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
        envvar="GEL_SERVER_BIND_ADDRESS", cls=EnvvarResolver,
        help='IP addresses to listen on, specify multiple times for more than '
             'one address to listen on'),
    click.option(
        '-P', '--port', type=PortType(), default=None,
        envvar="GEL_SERVER_PORT", cls=EnvvarResolver,
        help='port to listen on'),
    click.option(
        '-b', '--background', is_flag=True, help='daemonize'),
    click.option(
        '--pidfile-dir', type=PathPath(), default=None,
        help='path to PID file directory, defaults to --runstate-dir'),
    click.option(
        '--daemon-user', type=int),
    click.option(
        '--daemon-group', type=int),
    click.option(
        '--runstate-dir', type=PathPath(), default=None,
        envvar="GEL_SERVER_RUNSTATE_DIR",
        cls=EnvvarResolver,
        help=f'directory where UNIX sockets and other temporary '
             f'runtime files will be placed ({_get_runstate_dir_default()} '
             f'by default)'),
    click.option(
        '--extensions-dir', type=PathPath(), default=(), multiple=True,
        envvar="GEL_SERVER_EXTENSIONS_DIR",
        cls=EnvvarResolver,
        help=f'directory where third-party extension packages are loaded from'),
    click.option(
        '--max-backend-connections', type=int, metavar='NUM',
        envvar="GEL_SERVER_MAX_BACKEND_CONNECTIONS",
        cls=EnvvarResolver,
        help=f'The maximum NUM of connections this Gel instance could make '
             f'to the backend PostgreSQL cluster. If not set, Gel will '
             f'detect and calculate the NUM: RAM/100MiB='
             f'{compute_default_max_backend_connections()} for local '
             f'Postgres or pg_settings.max_connections for remote Postgres, '
             f'minus the NUM of --reserved-pg-connections.',
        callback=_validate_max_backend_connections),
    click.option(
        '--compiler-pool-size', type=int,
        envvar="GEL_SERVER_COMPILER_POOL_SIZE",
        cls=EnvvarResolver,
        callback=_validate_compiler_pool_size),
    click.option(
        '--compiler-pool-mode',
        type=CompilerPoolModeChoice(),
        default=CompilerPoolMode.Default.value,
        envvar="GEL_SERVER_COMPILER_POOL_MODE",
        cls=EnvvarResolver,
        help='Choose a mode for the compiler pool to scale. "fixed" means the '
             'pool will not scale and sticks to --compiler-pool-size, while '
             '"on_demand" means the pool will maintain at least 1 worker and '
             'automatically scale up (to --compiler-pool-size workers ) and '
             'down to the demand. Defaults to "fixed" in production mode and '
             '"on_demand" in development mode.',
    ),
    click.option(
        '--compiler-pool-addr',
        hidden=True,
        callback=_validate_host_port,
        envvar="GEL_SERVER_COMPILER_POOL_ADDR",
        cls=EnvvarResolver,
        help=f'Specify the host[:port] of the compiler pool to connect to, '
             f'only used if --compiler-pool-mode=remote. Default host is '
             f'localhost, port is {defines.EDGEDB_REMOTE_COMPILER_PORT}',
    ),
    click.option(
        "--compiler-pool-tenant-cache-size",
        hidden=True,
        type=int,
        default=100,
        envvar="GEL_SERVER_COMPILER_POOL_TENANT_CACHE_SIZE",
        cls=EnvvarResolver,
        help="Maximum number of tenants for which each compiler worker can "
             "cache their schemas, "
             "only used when --compiler-pool-mode=fixed_multi_tenant"
    ),
    click.option(
        '--echo-runtime-info', type=bool, default=False, is_flag=True,
        help='[DEPREATED, use --emit-server-status] '
             'echo runtime info to stdout; the format is JSON, prefixed by '
             '"EDGEDB_SERVER_DATA:", ended with a new line'),
    click.option(
        '--emit-server-status',
        type=str, default=None, metavar='DEST', multiple=True,
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
        '--auto-shutdown-after', type=float, default=-1.0, metavar='N',
        help='shutdown the server if no client connections were made in the '
             'last N seconds, if N = 0, shut down after the last client has '
             'disconnected, N < 0 (default) means no auto shutdown'),
    click.option(
        '--tls-cert-file',
        type=PathPath(),
        envvar="GEL_SERVER_TLS_CERT_FILE",
        cls=EnvvarResolver,
        help='Specifies a path to a file containing a server TLS certificate '
             'in PEM format, as well as possibly any number of CA '
             'certificates needed to establish the certificate '
             'authenticity.  If the file does not exist and the '
             '--tls-cert-mode option is set to "generate_self_signed", a '
             'self-signed certificate will be automatically created in '
             'the specified path.'),
    click.option(
        '--tls-key-file',
        type=PathPath(),
        envvar="GEL_SERVER_TLS_KEY_FILE",
        cls=EnvvarResolver,
        help='Specifies a path to a file containing the private key in PEM '
             'format.  If the file does not exist and the --tls-cert-mode '
             'option is set to "generate_self_signed", the private key will '
             'be automatically created in the specified path.'),
    click.option(
        '--tls-cert-mode',
        envvar="GEL_SERVER_TLS_CERT_MODE", cls=EnvvarResolver,
        type=click.Choice(
            ['default'] + list(ServerTlsCertMode.__members__.values()),
            case_sensitive=True,
        ),
        default='default',
        help='Specifies what to do when the TLS certificate and key are '
             'either not specified or are missing.  When set to '
             '"require_file", the TLS certificate and key must be specified '
             'in the --tls-cert-file and --tls-key-file options and both must '
             'exist.  When set to "generate_self_signed" a new self-signed '
             'certificate and private key will be generated and placed in the '
             'path specified by --tls-cert-file/--tls-key-file, if those are '
             'set, otherwise the generated certificate and key are stored as '
             f'`{TLS_CERT_FILE_NAME}` and `{TLS_KEY_FILE_NAME}` in the data '
             'directory, or, if the server is running with --backend-dsn, '
             'in a subdirectory of --runstate-dir.\n\nThe default is '
             '"require_file" when the --security option is set to "strict", '
             'and "generate_self_signed" when the --security option is set to '
             '"insecure_dev_mode"'),
    click.option(
        '--tls-client-ca-file',
        type=PathPath(),
        envvar='EDGEDB_SERVER_TLS_CLIENT_CA_FILE',
        cls=EnvvarResolver,
        help='Specifies a path to a file containing a TLS CA certificate to '
             'verify client certificates on demand. When set, the default '
             'authentication method of HTTP_METRICS(/metrics) and HTTP_HEALTH'
             '(/server/*) will also become "mTLS", unless explicitly set in '
             '--default-auth-method. Note, the protection of such HTTP '
             'endpoints is only complete if --http-endpoint-security is also '
             'set to `tls`, or they are still accessible in plaintext HTTP.'
    ),
    click.option(
        '--generate-self-signed-cert', type=bool, default=False, is_flag=True,
        help='DEPRECATED.\n\n'
             'Use --tls-cert-mode=generate_self_signed instead.'),
    click.option(
        '--binary-endpoint-security',
        envvar="GEL_SERVER_BINARY_ENDPOINT_SECURITY",
        cls=EnvvarResolver,
        type=click.Choice(
            ['default', 'tls', 'optional'],
            case_sensitive=True,
        ),
        default='default',
        help='Specifies the security mode of server binary endpoint. '
             'When set to `optional`, non-TLS connections are allowed. '
             'The default is `tls`.',
    ),
    click.option(
        '--http-endpoint-security',
        envvar="GEL_SERVER_HTTP_ENDPOINT_SECURITY",
        cls=EnvvarResolver,
        type=click.Choice(
            ['default', 'tls', 'optional'],
            case_sensitive=True,
        ),
        default='default',
        help='Specifies the security mode of server HTTP endpoint. '
             'When set to `optional`, non-TLS connections are allowed. '
             'The default is `tls`.',
    ),
    click.option(
        '--security',
        envvar="GEL_SERVER_SECURITY",
        cls=EnvvarResolver,
        type=click.Choice(
            ['default', 'strict', 'insecure_dev_mode'],
            case_sensitive=True,
        ),
        default='default',
        help=(
            'When set to `insecure_dev_mode`, sets the default '
            'authentication method to `Trust`, enables non-TLS '
            'client HTTP connections, and implies '
            '`--tls-cert-mode=generate_self_signed`.  The default is `strict`.'
        ),
    ),
    click.option(
        '--jws-key-file',
        type=PathPath(),
        envvar="GEL_SERVER_JWS_KEY_FILE",
        cls=EnvvarResolver,
        hidden=True,
        help='Specifies a path to a file containing a public key in PEM '
             'format used to verify JWT signatures. The file could also '
             'contain a private key to sign JWT for local testing.'),
    click.option(
        '--jwe-key-file',
        type=PathPath(),
        hidden=True,
        help='Deprecated: no longer in use.'),
    click.option(
        '--jose-key-mode',
        envvar="GEL_SERVER_JOSE_KEY_MODE", cls=EnvvarResolver,
        type=click.Choice(
            ['default'] + list(JOSEKeyMode.__members__.values()),
            case_sensitive=True,
        ),
        hidden=True,
        default='default',
        help='Specifies what to do when the JOSE keys are either not '
             'specified or are missing.  When set to "require_file", the JOSE '
             'keys must be specified in the --jws-key-file and the file must '
             'exist.  When set to "generate", a new key pair will be '
             'generated and placed in the path specified by --jws-key-file, '
             'if those are set, otherwise the generated key pairs are stored '
             f'as `{JWS_KEY_FILE_NAME}` in the data directory, or, if the '
             'server is running with --backend-dsn, in a subdirectory of '
             '--runstate-dir.\n\nThe default is "require_file" when the '
             '--security option is set to "strict", and "generate" when the '
             '--security option is set to "insecure_dev_mode"'),
    click.option(
        '--jwt-sub-allowlist-file',
        type=PathPath(),
        envvar="GEL_SERVER_JWT_SUB_ALLOWLIST_FILE",
        cls=EnvvarResolver,
        hidden=True,
        help='A file where the server can obtain a list of all JWT subjects '
             'that are allowed to access this instance. '
             'The file must contain one JWT "sub" claim value per line. '
             'Applies only to the JWT authentication method.'
    ),
    click.option(
        '--jwt-revocation-list-file',
        type=PathPath(),
        envvar="GEL_SERVER_JWT_REVOCATION_LIST_FILE",
        cls=EnvvarResolver,
        hidden=True,
        help='A file where the server can obtain a list of all JWT ids '
             'that are allowed to access this instance. '
             'The file must contain one JWT "jti" claim value per line. '
             'Applies only to the JWT authentication method.'
    ),
    click.option(
        "--default-auth-method",
        envvar="GEL_SERVER_DEFAULT_AUTH_METHOD", cls=EnvvarResolver,
        callback=_validate_default_auth_method,
        type=str,
        help=(
            "The default authentication method to use when none is "
            "explicitly configured. Defaults to 'auto', which means "
            "the SCRAM authentication method for TCP connections and "
            "the JWT authentication method for HTTP-tunneled connections."
        ),
    ),
    click.option(
        "--readiness-state-file",
        envvar="GEL_SERVER_READINESS_STATE_FILE",
        cls=EnvvarResolver,
        type=PathPath(),
        help=(
            "Path to a file containing the value for server readiness state. "
            "When it contains 'not_ready' (without quotes), the server will "
            "refuse connections and the '/server/status/ready' check will "
            "return a 503 status.  Every other value, including absense of "
            "file indicates that the server is in the 'ready' state and "
            "can server connections.  The file can be modified when the "
            "server is running."
        ),
    ),
    click.option(
        '--instance-name',
        envvar="GEL_SERVER_INSTANCE_NAME",
        cls=EnvvarResolver,
        type=str, default=None, hidden=True,
        help='Server instance name.'),
    click.option(
        '--backend-capabilities',
        envvar="GEL_SERVER_BACKEND_CAPABILITIES",
        cls=EnvvarResolver,
        type=BackendCapabilitySet(),
        help="A space-separated set of backend capabilities, which are "
             "required to be present, or absent if prefixed with ~. Gel "
             "will only start if the actual backend capabilities match the "
             "specified set. However if the backend was never bootstrapped, "
             "the capabilities prefixed with ~ will be *disabled permanently* "
             "in Gel as if the backend never had them."
    ),
    click.option(
        '--version', is_flag=True,
        help='Show the version and exit.'),
    click.option(
        '--admin-ui',
        envvar="GEL_SERVER_ADMIN_UI",
        cls=EnvvarResolver,
        type=click.Choice(
            ['default', 'enabled', 'disabled'],
            case_sensitive=True,
        ),
        default='default',
        help='Enable admin UI.'),
    click.option(
        '--disable-dynamic-system-config', is_flag=True,
        envvar="GEL_SERVER_DISABLE_DYNAMIC_SYSTEM_CONFIG",
        cls=EnvvarResolver,
        help="Disable dynamic configuration of system config values",
    ),
    click.option(
        "--reload-config-files",
        envvar="GEL_SERVER_RELOAD_CONFIG_FILES", cls=EnvvarResolver,
        type=click.Choice(
            list(ReloadTrigger.__members__.values()), case_sensitive=True
        ),
        hidden=True,
        default='default',
        help='Specifies when to reload the config files. See the docstring of '
             'ReloadTrigger for more information.',
    ),
    click.option(
        "--net-worker-mode",
        envvar="GEL_SERVER_NET_WORKER_MODE", cls=EnvvarResolver,
        type=click.Choice(
            list(NetWorkerMode.__members__.values()), case_sensitive=True
        ),
        hidden=True,
        default='default',
        help='Controls how the std::net workers work.',
    ),
    click.option(
        "--config-file", type=PathPath(), metavar="PATH",
        envvar="GEL_SERVER_CONFIG_FILE",
        cls=EnvvarResolver,
        help='Path to a TOML file to configure the server.',
        hidden=True,
    ),
])


compiler_options = typeutils.chain_decorators([
    click.option(
        "--pool-size",
        type=int,
        callback=_validate_compiler_pool_size,
        default=compute_default_compiler_pool_size(),
        help=f"Number of compiler worker processes. Defaults to "
             f"{compute_default_compiler_pool_size()}.",
    ),
    click.option(
        "--client-schema-cache-size",
        type=int,
        default=100,
        help="Number of client schemas each worker could cache at most. The "
             "compiler server is not affected by this setting, it keeps a "
             "pickled copy of the client schema of all active clients."
    ),
    click.option(
        '-I', '--listen-addresses', type=str, multiple=True,
        default=('localhost',),
        help='IP addresses to listen on, specify multiple times for more than '
             'one address to listen on. Default: localhost',
    ),
    click.option(
        '-P', '--listen-port', type=PortType(),
        help=f'Port to listen on. '
             f'Default: {defines.EDGEDB_REMOTE_COMPILER_PORT}',
    ),
    click.option(
        '--runstate-dir', type=PathPath(), default=None,
        help="Directory to store UNIX domain socket file for IPC, a temporary "
             "directory will be used if not specified.",
    ),
    click.option(
        '--metrics-port', type=PortType(),
        help=f'Port to listen on for metrics HTTP API.',
    ),
])


def parse_args(**kwargs: Any):
    kwargs['bind_addresses'] = kwargs.pop('bind_address')

    if kwargs['echo_runtime_info']:
        warnings.warn(
            "The `--echo-runtime-info` option is deprecated, use "
            "`--emit-server-status` instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    if kwargs['bootstrap']:
        warnings.warn(
            "Option `--bootstrap` is deprecated, use `--bootstrap-only`",
            DeprecationWarning,
            stacklevel=2,
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
                stacklevel=2,
            )
        else:
            warnings.warn(
                "Option `--default-database-user` is deprecated."
                " Please create the role explicitly.",
                DeprecationWarning,
                stacklevel=2,
            )

    if kwargs['default_database']:
        if kwargs['default_database'] == 'edgedb':
            warnings.warn(
                "Option `--default-database` is deprecated."
                " Database `edgedb` is always created and"
                " no database named after unix user is created any more.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            warnings.warn(
                "Option `--default-database` is deprecated."
                " Please create the database explicitly.",
                DeprecationWarning,
                stacklevel=2,
            )

    if kwargs['auto_shutdown']:
        warnings.warn(
            "The `--auto-shutdown` option is deprecated, use "
            "`--auto-shutdown-after` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if kwargs['auto_shutdown_after'] < 0:
            kwargs['auto_shutdown_after'] = 0

    del kwargs['auto_shutdown']

    if kwargs['postgres_dsn']:
        warnings.warn(
            "The `--postgres-dsn` option is deprecated, use "
            "`--backend-dsn` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not kwargs['backend_dsn']:
            kwargs['backend_dsn'] = kwargs['postgres_dsn']

    del kwargs['postgres_dsn']

    if kwargs['generate_self_signed_cert']:
        warnings.warn(
            "The `--generate-self-signed-cert` option is deprecated, use "
            "`--tls-cert-mode=generate_self_signed` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if kwargs['tls_cert_mode'] == 'default':
            kwargs['tls_cert_mode'] = 'generate_self_signed'

    del kwargs['generate_self_signed_cert']

    if os.environ.get('EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS') == "1":
        if kwargs['binary_endpoint_security'] == "tls":
            abort(
                "The value of deprecated "
                "EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS environment "
                "variable disagrees with --binary-endpoint-security"
            )
        else:
            if kwargs['binary_endpoint_security'] == "default":
                warnings.warn(
                    "EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS is "
                    "deprecated. Use EDGEDB_SERVER_BINARY_ENDPOINT_SECURITY "
                    "instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            kwargs['binary_endpoint_security'] = 'optional'

    if os.environ.get('EDGEDB_SERVER_ALLOW_INSECURE_HTTP_CLIENTS') == "1":
        if kwargs['http_endpoint_security'] == "tls":
            abort(
                "The value of deprecated "
                "EDGEDB_SERVER_ALLOW_INSECURE_HTTP_CLIENTS environment "
                "variable disagrees with --http-endpoint-security"
            )
        else:
            if kwargs['http_endpoint_security'] == "default":
                warnings.warn(
                    "EDGEDB_SERVER_ALLOW_INSECURE_BINARY_CLIENTS is "
                    "deprecated. Use EDGEDB_SERVER_BINARY_ENDPOINT_SECURITY "
                    "instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            kwargs['http_endpoint_security'] = 'optional'

    if kwargs['security'] == 'default':
        if devmode.is_in_dev_mode():
            kwargs['security'] = 'insecure_dev_mode'
        else:
            kwargs['security'] = 'strict'

    if kwargs['security'] == 'insecure_dev_mode':
        if kwargs['http_endpoint_security'] == 'default':
            kwargs['http_endpoint_security'] = 'optional'
        if not kwargs['default_auth_method']:
            kwargs['default_auth_method'] = ServerAuthMethods({
                t: [ServerAuthMethod.Trust]
                for t in ServerConnTransport.__members__.values()
            })
        if kwargs['tls_cert_mode'] == 'default':
            kwargs['tls_cert_mode'] = 'generate_self_signed'

    elif not kwargs['default_auth_method']:
        kwargs['default_auth_method'] = DEFAULT_AUTH_METHODS

    transport_methods = dict(kwargs['default_auth_method'].items())
    for transport in ServerConnTransport.__members__.values():
        methods = transport_methods[transport]
        if ServerAuthMethod.Auto in methods:
            pos = methods.index(ServerAuthMethod.Auto)
            if transport in (
                ServerConnTransport.HTTP_METRICS,
                ServerConnTransport.HTTP_HEALTH,
            ):
                if kwargs['tls_client_ca_file'] is None:
                    method = ServerAuthMethod.Trust
                else:
                    method = ServerAuthMethod.mTLS
                methods[pos] = method
            else:
                methods = (
                    methods[:pos]
                    + DEFAULT_AUTH_METHODS.get(transport)
                    + methods[pos + 1:]
                )
            transport_methods[transport] = [method]
        elif transport in (
            ServerConnTransport.HTTP_METRICS,
            ServerConnTransport.HTTP_HEALTH,
        ):
            if ServerAuthMethod.mTLS in methods:
                if kwargs['tls_client_ca_file'] is None:
                    abort('--tls-client-ca-file is required '
                          'for mTLS authentication')

            if not all(
                m is ServerAuthMethod.Trust or m is ServerAuthMethod.mTLS
                for m in methods
            ):
                abort(
                    f'--default-auth-method of {transport} can only be one '
                    f'of: {ServerAuthMethod.Trust}, {ServerAuthMethod.mTLS} '
                    f'or {ServerAuthMethod.Auto}'
                )
    kwargs['default_auth_method'] = ServerAuthMethods(transport_methods)

    if kwargs['binary_endpoint_security'] == 'default':
        kwargs['binary_endpoint_security'] = 'tls'

    if kwargs['http_endpoint_security'] == 'default':
        kwargs['http_endpoint_security'] = 'tls'

    if kwargs['tls_cert_mode'] == 'default':
        kwargs['tls_cert_mode'] = 'require_file'

    if kwargs['jose_key_mode'] == 'default':
        kwargs['jose_key_mode'] = 'generate'

    kwargs['security'] = ServerSecurityMode(kwargs['security'])
    kwargs['binary_endpoint_security'] = ServerEndpointSecurityMode(
        kwargs['binary_endpoint_security'])
    kwargs['http_endpoint_security'] = ServerEndpointSecurityMode(
        kwargs['http_endpoint_security'])
    kwargs['tls_cert_mode'] = ServerTlsCertMode(kwargs['tls_cert_mode'])
    kwargs['jose_key_mode'] = JOSEKeyMode(kwargs['jose_key_mode'])

    if kwargs['compiler_pool_mode'] == 'default':
        if kwargs['multitenant_config_file']:
            kwargs['compiler_pool_mode'] = 'fixed_multi_tenant'
        elif devmode.is_in_dev_mode():
            kwargs['compiler_pool_mode'] = 'on_demand'
        else:
            kwargs['compiler_pool_mode'] = 'fixed'
    kwargs['compiler_pool_mode'] = CompilerPoolMode(
        kwargs['compiler_pool_mode']
    )
    if kwargs['compiler_pool_size'] is None:
        if kwargs['compiler_pool_mode'] == CompilerPoolMode.Remote:
            # this reflects to a local semaphore to control concurrency,
            # 2 means this is a small EdgeDB instance that could only issue
            # at max 2 concurrent compile requests at a time.
            kwargs['compiler_pool_size'] = 2
        else:
            kwargs['compiler_pool_size'] = compute_default_compiler_pool_size()
    if kwargs['compiler_pool_mode'] == CompilerPoolMode.Remote:
        if kwargs['compiler_pool_addr'] is None:
            kwargs['compiler_pool_addr'] = (
                "localhost", defines.EDGEDB_REMOTE_COMPILER_PORT
            )
    elif kwargs['compiler_pool_addr'] is not None:
        abort('--compiler-pool-addr is only meaningful '
              'under --compiler-pool-mode=remote')

    if kwargs['temp_dir']:
        if kwargs['data_dir']:
            abort('--temp-dir is incompatible with --data-dir/-D')
        if kwargs['runstate_dir']:
            abort('--temp-dir is incompatible with --runstate-dir')
        if kwargs['backend_dsn']:
            abort('--temp-dir is incompatible with --backend-dsn')
        if kwargs['multitenant_config_file']:
            abort('--temp-dir is incompatible with --multitenant-config-file')
        kwargs['data_dir'] = kwargs['runstate_dir'] = pathlib.Path(
            tempfile.mkdtemp())
    else:
        if not kwargs['data_dir']:
            if kwargs['backend_dsn'] or kwargs['multitenant_config_file']:
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
        elif kwargs['multitenant_config_file']:
            abort('The -D and --multitenant-config-file options '
                  'are mutually exclusive.')

    if kwargs['tls_key_file'] and not kwargs['tls_cert_file']:
        abort('When --tls-key-file is set, --tls-cert-file must also be set.')

    if kwargs['tls_cert_file'] and not kwargs['tls_key_file']:
        abort('When --tls-cert-file is set, --tls-key-file must also be set.')

    self_signing = kwargs['tls_cert_mode'] is ServerTlsCertMode.SelfSigned

    if not kwargs['tls_cert_file']:
        if kwargs['data_dir']:
            tls_cert_file = kwargs['data_dir'] / TLS_CERT_FILE_NAME
            tls_key_file = kwargs['data_dir'] / TLS_KEY_FILE_NAME
        elif self_signing:
            tls_cert_file = pathlib.Path('<runstate>') / TLS_CERT_FILE_NAME
            tls_key_file = pathlib.Path('<runstate>') / TLS_KEY_FILE_NAME
        else:
            abort(
                "no TLS certificate specified and certificate auto-generation"
                " has not been requested; see help for --tls-cert-mode",
                exit_code=10,
            )
        kwargs['tls_cert_file'] = tls_cert_file
        kwargs['tls_key_file'] = tls_key_file

    if not kwargs['bootstrap_only'] and not self_signing:
        if not kwargs['tls_cert_file'].exists():
            abort(
                f"TLS certificate file \"{kwargs['tls_cert_file']}\""
                " does not exist and certificate auto-generation has not been"
                " requested; see help for --tls-cert-mode",
                exit_code=10,
            )

    if (
        kwargs['tls_cert_file'].exists()
        and not kwargs['tls_cert_file'].is_file()
    ):
        abort(
            f"TLS certificate file \"{kwargs['tls_cert_file']}\""
            " is not a regular file"
        )

    if (
        kwargs['tls_key_file'].exists()
        and not kwargs['tls_key_file'].is_file()
    ):
        abort(
            f"TLS private key file \"{kwargs['tls_key_file']}\""
            " is not a regular file"
        )

    generate_jose = kwargs['jose_key_mode'] is JOSEKeyMode.Generate

    if not kwargs['jws_key_file']:
        if kwargs['data_dir']:
            jws_key_file = kwargs['data_dir'] / JWS_KEY_FILE_NAME
        elif generate_jose:
            jws_key_file = pathlib.Path('<runstate>') / JWS_KEY_FILE_NAME
        else:
            abort(
                "no JWS key specified and JOSE keys auto-generation"
                " has not been requested; see help for --jose-key-mode",
                exit_code=11,
            )
        kwargs['jws_key_file'] = jws_key_file
    del kwargs['jwe_key_file']

    if not kwargs['bootstrap_only'] and not generate_jose:
        if not kwargs['jws_key_file'].exists():
            abort(
                f"JWS key file \"{kwargs['jws_key_file']}\" does not exist"
            )

    if (
        kwargs['jws_key_file'].exists() and
        not kwargs['jws_key_file'].is_file()
    ):
        abort(
            f"JWT key file \"{kwargs['jws_key_file']}\""
            " is not a regular file"
        )

    if kwargs['log_level']:
        kwargs['log_level'] = kwargs['log_level'].lower()[0]

    if kwargs['bootstrap_script']:
        if not kwargs['bootstrap_command_file']:
            warnings.warn(
                "The `--bootstrap-script` option is deprecated, use "
                "`--bootstrap-command-file` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs['bootstrap_command_file'] = kwargs['bootstrap_script']
        else:
            warnings.warn(
                "Both `--bootstrap-command-file` and `--bootstrap-script` "
                "were specified, but are mutually exclusive. "
                "Ignoring the deprecated `--bootstrap-script` option.",
                DeprecationWarning,
                stacklevel=2,
            )

    del kwargs['bootstrap_script']

    if kwargs['multitenant_config_file']:
        for name in (
            "tenant_id",
            "backend_dsn",
            "backend_adaptive_ha",
            "bootstrap_only",
            "inplace_upgrade",
            "bootstrap_command",
            "bootstrap_command_file",
            "instance_name",
            "max_backend_connections",
            "readiness_state_file",
            "jwt_sub_allowlist_file",
            "jwt_revocation_list_file",
            "config_file",
        ):
            if kwargs.get(name):
                opt = "--" + name.replace("_", "-")
                abort(f"The {opt} and --multitenant-config-file options "
                      f"are mutually exclusive.")
        if kwargs['compiler_pool_mode'] is not CompilerPoolMode.MultiTenant:
            abort("must use --compiler-pool-mode=fixed_multi_tenant "
                  "in multi-tenant mode")

    bootstrap_script_text: Optional[str]
    if kwargs['bootstrap_command_file']:
        with open(kwargs['bootstrap_command_file']) as f:
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
                kwargs['default_branch'] or
                kwargs['default_database'] or
                defines.EDGEDB_SUPERUSER_DB
            ),
            user=(
                kwargs['default_database_user'] or
                defines.EDGEDB_SUPERUSER
            ),
        )

    status_sinks = []

    if status_sink_addrs := kwargs['emit_server_status']:
        for status_sink_addr in status_sink_addrs:
            if status_sink_addr.startswith('file://'):
                status_sink = _status_sink_file(
                    status_sink_addr[len('file://'):])
            elif status_sink_addr.startswith('fd://'):
                fileno_str = status_sink_addr[len('fd://'):]
                try:
                    fileno = int(fileno_str)
                except ValueError:
                    abort(
                        f'invalid file descriptor number in '
                        f'--emit-server-status: {fileno_str!r}'
                    )

                status_sink = _status_sink_fd(fileno)
            elif m := re.match(r'^(\w+)://', status_sink_addr):
                abort(
                    f'unsupported destination scheme in --emit-server-status: '
                    f'{m.group(1)}'
                )
            else:
                # Assume it's a file.
                status_sink = _status_sink_file(status_sink_addr)

            status_sinks.append(status_sink)

    kwargs['backend_capability_sets'] = (
        kwargs.pop('backend_capabilities') or BackendCapabilitySets([], [])
    )

    if kwargs['admin_ui'] == 'default':
        if devmode.is_in_dev_mode():
            kwargs['admin_ui'] = 'enabled'
        else:
            kwargs['admin_ui'] = 'disabled'

    kwargs['admin_ui'] = kwargs['admin_ui'] == 'enabled'

    if not kwargs['instance_name']:
        if devmode.is_in_dev_mode():
            kwargs['instance_name'] = '_localdev'
        else:
            kwargs['instance_name'] = '_unknown'

    kwargs['reload_config_files'] = ReloadTrigger(
        kwargs['reload_config_files']
    )
    kwargs['net_worker_mode'] = NetWorkerMode(kwargs['net_worker_mode'])

    for disallowed, replacement in (
        (
            'EDGEDB_SERVER_CONFIG_cfg::listen_addresses',
            'GEL_SERVER_BIND_ADDRESS',
        ),
        (
            'EDGEDB_SERVER_CONFIG_cfg::listen_port',
            'GEL_SERVER_PORT',
        ),
        (
            'GEL_SERVER_CONFIG_cfg::listen_addresses',
            'GEL_SERVER_BIND_ADDRESS',
        ),
        (
            'GEL_SERVER_CONFIG_cfg::listen_port',
            'GEL_SERVER_PORT',
        ),
    ):
        if disallowed in os.environ:
            abort(f"{disallowed} is disallowed; use {replacement} instead")

    return ServerConfig(
        startup_script=startup_script,
        status_sinks=status_sinks,
        **kwargs,
    )
