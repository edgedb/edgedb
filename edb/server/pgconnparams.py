# Copyright (C) 2016-present MagicStack Inc. and the EdgeDB authors.
# Copyright (C) 2016-present the asyncpg authors and contributors
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


from __future__ import annotations
from typing import *

import dataclasses
import getpass
import os
import pathlib
import platform
import re
import ssl as ssl_module
import stat
import urllib.parse
import warnings


@dataclasses.dataclass
class ConnectionParameters:
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    ssl: Optional[ssl_module.SSLContext] = None
    ssl_is_advisory: Optional[bool] = None
    server_settings: Dict[str, str] = dataclasses.field(default_factory=dict)


_system = platform.uname().system


if _system == 'Windows':
    import ctypes.wintypes

    CSIDL_APPDATA = 0x001a
    PGPASSFILE = 'pgpass.conf'

    def get_pg_home_directory() -> Optional[pathlib.Path]:
        # We cannot simply use expanduser() as that returns the user's
        # home directory, whereas Postgres stores its config in
        # %AppData% on Windows.
        buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
        r = ctypes.windll.shell32.SHGetFolderPathW(  # type: ignore
            0, CSIDL_APPDATA, 0, 0, buf)
        if r:
            return None
        else:
            return pathlib.Path(buf.value) / 'postgresql'
else:
    PGPASSFILE = '.pgpass'

    def get_pg_home_directory() -> Optional[pathlib.Path]:
        return pathlib.Path.home()


def _read_password_file(passfile: pathlib.Path) -> List[Tuple[str, ...]]:

    passtab = []

    try:
        if not passfile.exists():
            return []

        if not passfile.is_file():
            warnings.warn(
                'password file {!r} is not a plain file'.format(passfile))

            return []

        if _system != 'Windows':
            if passfile.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
                warnings.warn(
                    'password file {!r} has group or world access; '
                    'permissions should be u=rw (0600) or less'.format(
                        passfile))

                return []

        with passfile.open('rt') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    # Skip empty lines and comments.
                    continue
                # Backslash escapes both itself and the colon,
                # which is a record separator.
                line = line.replace(R'\\', '\n')
                passtab.append(tuple(
                    p.replace('\n', R'\\')
                    for p in re.split(r'(?<!\\):', line, maxsplit=4)
                ))
    except IOError:
        pass

    return passtab


def _read_password_from_pgpass(
    *,
    passfile: Optional[pathlib.Path],
    hosts: List[str],
    ports: List[int],
    database: str,
    user: str,
) -> Optional[str]:
    """Parse the pgpass file and return the matching password.

    :return:
        Password string, if found, ``None`` otherwise.
    """

    if passfile is not None:
        passtab = _read_password_file(passfile)
        if not passtab:
            return None

    for host, port in zip(hosts, ports):
        if host.startswith('/'):
            # Unix sockets get normalized into 'localhost'
            host = 'localhost'

        for phost, pport, pdatabase, puser, ppassword in passtab:
            if phost != '*' and phost != host:
                continue
            if pport != '*' and pport != str(port):
                continue
            if pdatabase != '*' and pdatabase != database:
                continue
            if puser != '*' and puser != user:
                continue

            # Found a match.
            return ppassword

    return None


def _validate_port_spec(
    hosts: Collection[str],
    port: Union[int, List[int]],
) -> List[int]:
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise ValueError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
    else:
        port = [port for _ in range(len(hosts))]

    return port


def _parse_hostlist(
    hostlist: str,
    port: Union[int, List[int]],
    *,
    unquote: bool = False,
) -> Tuple[List[str], List[int]]:
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(',')
    else:
        hostspecs = [hostlist]

    hosts = []
    hostlist_ports = []
    result_port: List[int] = []

    if not port:
        portspec = os.environ.get('PGPORT')
        specified_port: Union[int, List[int]]
        if portspec:
            if ',' in portspec:
                specified_port = [int(p) for p in portspec.split(',')]
            else:
                specified_port = int(portspec)
        else:
            specified_port = 5432

        default_port = _validate_port_spec(hostspecs, specified_port)

    else:
        result_port = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        if not hostspec.startswith('/'):
            addr, _, hostspec_port = hostspec.partition(':')
        else:
            addr = hostspec
            hostspec_port = ''

        if unquote:
            addr = urllib.parse.unquote(addr)

        hosts.append(addr)
        if not port:
            if hostspec_port:
                if unquote:
                    hostspec_port = urllib.parse.unquote(hostspec_port)
                hostlist_ports.append(int(hostspec_port))
            else:
                hostlist_ports.append(default_port[i])

    if not result_port:
        result_port = hostlist_ports

    return hosts, result_port


def parse_dsn(
    dsn: str,
) -> Tuple[
    Tuple[Tuple[str, int], ...],
    ConnectionParameters,
]:
    # `auth_hosts` is the version of host information for the purposes
    # of reading the pgpass file.
    auth_hosts = None
    host: List[str] = []
    port: Union[int, List[int]] = []
    user = None
    password = None
    passfile = None
    database = None
    sslmode = None
    server_settings: Dict[str, str] = {}

    parsed = urllib.parse.urlparse(dsn)

    if parsed.scheme not in {'postgresql', 'postgres'}:
        raise ValueError(
            'invalid DSN: scheme is expected to be either '
            '"postgresql" or "postgres", got {!r}'.format(parsed.scheme))

    if parsed.netloc:
        if '@' in parsed.netloc:
            dsn_auth, _, dsn_hostspec = parsed.netloc.partition('@')
        else:
            dsn_hostspec = parsed.netloc
            dsn_auth = ''
    else:
        dsn_auth = dsn_hostspec = ''

    if dsn_auth:
        dsn_user, _, dsn_password = dsn_auth.partition(':')
    else:
        dsn_user = dsn_password = ''

    if dsn_hostspec:
        host, port = _parse_hostlist(dsn_hostspec, [], unquote=True)

    if parsed.path:
        dsn_database = parsed.path
        if dsn_database.startswith('/'):
            dsn_database = dsn_database[1:]
        database = urllib.parse.unquote(dsn_database)

    if dsn_user:
        user = urllib.parse.unquote(dsn_user)

    if dsn_password:
        password = urllib.parse.unquote(dsn_password)

    if parsed.query:
        query: Dict[str, str] = {}
        pq = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        for k, v in pq.items():
            if isinstance(v, list):
                query[k] = v[-1]
            else:
                query[k] = cast(str, v)

        if 'port' in query:
            val = query.pop('port')
            if not port and val:
                port = [int(p) for p in val.split(',')]

        if 'host' in query:
            val = query.pop('host')
            if not host and val:
                host, port = _parse_hostlist(val, port)

        if 'dbname' in query:
            val = query.pop('dbname')
            if database is None:
                database = val

        if 'database' in query:
            val = query.pop('database')
            if database is None:
                database = val

        if 'user' in query:
            val = query.pop('user')
            if user is None:
                user = val

        if 'password' in query:
            val = query.pop('password')
            if password is None:
                password = val

        if 'passfile' in query:
            passfile = query.pop('passfile')

        if 'sslmode' in query:
            sslmode = query.pop('sslmode')

        if query:
            server_settings = query

    if not host:
        hostspec = os.environ.get('PGHOST')
        if hostspec:
            host, port = _parse_hostlist(hostspec, port)

    if not host:
        auth_hosts = ['localhost']

        if _system == 'Windows':
            host = ['localhost']
        else:
            host = ['/run/postgresql', '/var/run/postgresql',
                    '/tmp', '/private/tmp', 'localhost']

    if auth_hosts is None:
        auth_hosts = host

    if not port:
        portspec = os.environ.get('PGPORT')
        if portspec:
            if ',' in portspec:
                port = [int(p) for p in portspec.split(',')]
            else:
                port = int(portspec)
        else:
            port = 5432

    elif isinstance(port, (list, tuple)):
        port = [int(p) for p in port]

    else:
        port = int(port)

    port = _validate_port_spec(host, port)

    if user is None:
        user = os.getenv('PGUSER')
        if not user:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('PGPASSWORD')

    if database is None:
        database = os.getenv('PGDATABASE')

    if database is None:
        database = user

    if user is None:
        raise ValueError(
            'could not determine user name to connect with')

    if database is None:
        raise ValueError(
            'could not determine database name to connect to')

    if password is None:
        if passfile is None:
            passfile = os.getenv('PGPASSFILE')

        if passfile is None:
            homedir = get_pg_home_directory()
            if not homedir:
                passfile_path = None
            else:
                passfile_path = homedir / PGPASSFILE
        else:
            passfile_path = pathlib.Path(passfile)

        if passfile_path is not None:
            password = _read_password_from_pgpass(
                hosts=auth_hosts, ports=port,
                database=database, user=user,
                passfile=passfile_path)

    addrs: List[Tuple[str, int]] = []
    for h, p in zip(host, port):
        addrs.append((h, p))

    if not addrs:
        raise ValueError(
            'could not determine the database address to connect to')

    if sslmode is None:
        sslmode = os.getenv('PGSSLMODE')

    # ssl_is_advisory is only allowed to come from the sslmode parameter.
    ssl_is_advisory = None
    if sslmode:
        SSLMODES = {
            'disable': 0,
            'allow': 1,
            'prefer': 2,
            'require': 3,
            'verify-ca': 4,
            'verify-full': 5,
        }
        try:
            sslmode_key = SSLMODES[sslmode]
        except KeyError:
            modes = ', '.join(SSLMODES.keys())
            raise ValueError(
                '`sslmode` parameter must be one of: {}'.format(modes))

        # sslmode 'allow' is currently handled as 'prefer' because we're
        # missing the "retry with SSL" behavior for 'allow', but do have the
        # "retry without SSL" behavior for 'prefer'.
        # Not changing 'allow' to 'prefer' here would be effectively the same
        # as changing 'allow' to 'disable'.
        if sslmode_key == SSLMODES['allow']:
            sslmode_key = SSLMODES['prefer']

        # docs at https://www.postgresql.org/docs/10/static/libpq-connect.html
        # Not implemented: sslcert & sslkey & sslrootcert & sslcrl params.
        if sslmode_key <= SSLMODES['allow']:
            ssl = None
            ssl_is_advisory = sslmode_key >= SSLMODES['allow']
        else:
            ssl = ssl_module.create_default_context()
            ssl.check_hostname = sslmode_key >= SSLMODES['verify-full']
            ssl.verify_mode = ssl_module.CERT_REQUIRED
            if sslmode_key <= SSLMODES['require']:
                ssl.verify_mode = ssl_module.CERT_NONE
            ssl_is_advisory = sslmode_key <= SSLMODES['prefer']
    else:
        ssl = None

    if ssl:
        for addr in addrs:
            if isinstance(addr, str):
                # UNIX socket
                raise ValueError(
                    '`ssl` parameter can only be enabled for TCP addresses, '
                    'got a UNIX socket path: {!r}'.format(addr))

    if server_settings is not None and (
            not isinstance(server_settings, dict) or
            not all(isinstance(k, str) for k in server_settings) or
            not all(isinstance(v, str) for v in server_settings.values())):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')

    params = ConnectionParameters(
        user=user,
        password=password,
        database=database,
        ssl=ssl,
        ssl_is_advisory=ssl_is_advisory,
        server_settings=server_settings,
    )

    return tuple(addrs), params
