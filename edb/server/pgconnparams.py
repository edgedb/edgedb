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
import enum
import getpass
import os
import pathlib
import platform
import re
import ssl as ssl_module
import stat
import sys
import urllib.parse
import warnings


class SSLMode(enum.IntEnum):
    disable = 0
    allow = 1
    prefer = 2
    require = 3
    verify_ca = 4
    verify_full = 5

    @classmethod
    def parse(cls, sslmode: Union[SSLMode, str]) -> SSLMode:
        if isinstance(sslmode, SSLMode):
            rv = sslmode
        else:
            rv = getattr(cls, sslmode.replace('-', '_'))
        return rv


@dataclasses.dataclass
class ConnectionParameters:
    user: str
    password: Optional[str] = None
    database: Optional[str] = None
    ssl: Optional[ssl_module.SSLContext] = None
    sslmode: Optional[SSLMode] = None
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
        if hostspec[0] == '/':
            # Unix socket
            addr = hostspec
            hostspec_port = ''
        elif hostspec[0] == '[':
            # IPv6 address
            m = re.match(r'(?:\[([^\]]+)\])(?::([0-9]+))?', hostspec)
            if m:
                addr = m.group(1)
                hostspec_port = m.group(2)
            else:
                raise ValueError(
                    f'invalid IPv6 address in the connection URI: {hostspec!r}'
                )
        else:
            # IPv4 address
            addr, _, hostspec_port = hostspec.partition(':')

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


def _parse_tls_version(tls_version: str) -> ssl_module.TLSVersion:
    if tls_version.startswith('SSL'):
        raise ValueError(
            f"Unsupported TLS version: {tls_version}"
        )
    try:
        return ssl_module.TLSVersion[tls_version.replace('.', '_')]
    except KeyError:
        raise ValueError(
            f"No such TLS version: {tls_version}"
        )


def _dot_postgresql_path(filename: str) -> str:
    return str((pathlib.Path.home() / '.postgresql' / filename).resolve())


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
    sslmode_str = None
    sslcert = None
    sslkey = None
    sslpassword = None
    sslrootcert = None
    sslcrl = None
    ssl_min_protocol_version = None
    ssl_max_protocol_version = None
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
            sslmode_str = query.pop('sslmode')

        if 'sslcert' in query:
            sslcert = query.pop('sslcert')

        if 'sslkey' in query:
            sslkey = query.pop('sslkey')

        if 'sslpassword' in query:
            sslpassword = query.pop('sslpassword')

        if 'sslrootcert' in query:
            sslrootcert = query.pop('sslrootcert')

        if 'sslcrl' in query:
            sslcrl = query.pop('sslcrl')

        if 'ssl_min_protocol_version' in query:
            ssl_min_protocol_version = query.pop('ssl_min_protocol_version')

        if 'ssl_max_protocol_version' in query:
            ssl_max_protocol_version = query.pop('ssl_max_protocol_version')

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
    have_tcp_addrs = False
    for h, p in zip(host, port):
        addrs.append((h, p))
        if not h.startswith('/'):
            have_tcp_addrs = True

    if not addrs:
        raise ValueError(
            'could not determine the database address to connect to')

    if sslmode_str is None:
        sslmode_str = os.getenv('PGSSLMODE')

    if sslmode_str is None and have_tcp_addrs:
        sslmode_str = 'prefer'

    if sslmode_str:
        try:
            sslmode = SSLMode.parse(sslmode_str)
        except AttributeError:
            modes = ', '.join(m.name.replace('_', '-') for m in SSLMode)
            raise ValueError(
                '`sslmode` parameter must be one of: {}'.format(modes))

        # docs at https://www.postgresql.org/docs/10/static/libpq-connect.html
        if sslmode < SSLMode.allow:
            ssl = None
        else:
            ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            ssl.check_hostname = sslmode >= SSLMode.verify_full
            if sslmode < SSLMode.require:
                ssl.verify_mode = ssl_module.CERT_NONE
            else:
                if sslrootcert is None:
                    sslrootcert = os.getenv('PGSSLROOTCERT')
                if sslrootcert:
                    ssl.load_verify_locations(cafile=sslrootcert)
                    ssl.verify_mode = ssl_module.CERT_REQUIRED
                else:
                    sslrootcert = _dot_postgresql_path('root.crt')
                    try:
                        ssl.load_verify_locations(cafile=sslrootcert)
                    except FileNotFoundError:
                        if sslmode > SSLMode.require:
                            raise ValueError(
                                f'root certificate file "{sslrootcert}" does '
                                f'not exist\nEither provide the file or '
                                f'change sslmode to disable server '
                                f'certificate verification.'
                            )
                        else:
                            # sslmode=require without sslrootcert won't verify
                            # the server certificate
                            ssl.verify_mode = ssl_module.CERT_NONE
                    else:
                        ssl.verify_mode = ssl_module.CERT_REQUIRED

                if sslcrl is None:
                    sslcrl = os.getenv('PGSSLCRL')
                if sslcrl:
                    ssl.load_verify_locations(cafile=sslcrl)
                    ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN
                else:
                    sslcrl = _dot_postgresql_path('root.crl')
                    try:
                        ssl.load_verify_locations(cafile=sslcrl)
                    except FileNotFoundError:
                        pass
                    else:
                        ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN

            if sslkey is None:
                sslkey = os.getenv('PGSSLKEY')
            if not sslkey:
                sslkey = _dot_postgresql_path('postgresql.key')
                if not os.path.exists(sslkey):
                    sslkey = None
            if sslcert is None:
                sslcert = os.getenv('PGSSLCERT')
            if sslcert:
                ssl.load_cert_chain(
                    sslcert, keyfile=sslkey, password=lambda: sslpassword or ''
                )
            else:
                sslcert = _dot_postgresql_path('postgresql.crt')
                try:
                    ssl.load_cert_chain(
                        sslcert,
                        keyfile=sslkey,
                        password=lambda: sslpassword or '',
                    )
                except FileNotFoundError:
                    pass

            # OpenSSL 1.1.1 keylog file
            if hasattr(ssl, 'keylog_filename'):
                keylogfile = os.environ.get('SSLKEYLOGFILE')
                if keylogfile and not sys.flags.ignore_environment:
                    setattr(ssl, 'keylog_filename', keylogfile)  # noqa

            if ssl_min_protocol_version is None:
                ssl_min_protocol_version = os.getenv('PGSSLMINPROTOCOLVERSION')
            if ssl_min_protocol_version:
                ssl.minimum_version = _parse_tls_version(
                    ssl_min_protocol_version
                )
            else:
                ssl.minimum_version = ssl_module.TLSVersion.TLSv1_2

            if ssl_max_protocol_version is None:
                ssl_max_protocol_version = os.getenv('PGSSLMAXPROTOCOLVERSION')
            if ssl_max_protocol_version:
                ssl.maximum_version = _parse_tls_version(
                    ssl_max_protocol_version
                )

    else:
        ssl = None
        sslmode = SSLMode.disable

    if ssl and not have_tcp_addrs:
        raise ValueError(
            '`ssl` parameter can only be enabled for TCP addresses, '
            'got a UNIX socket paths: {!r}'.format(addrs))

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
        sslmode=sslmode,
        server_settings=server_settings,
    )

    return tuple(addrs), params
