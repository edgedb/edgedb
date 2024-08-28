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
from typing import Optional, Tuple, Union, Dict, List

import dataclasses
import enum
import getpass
import pathlib
import platform
import ssl as ssl_module
import warnings
import edb.server._pg_rust


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
    connect_timeout: Optional[int] = None


_system = platform.uname().system


if _system == 'Windows':
    import ctypes.wintypes

    CSIDL_APPDATA = 0x001a

    def get_pg_home_directory() -> pathlib.Path:
        # We cannot simply use expanduser() as that returns the user's
        # home directory, whereas Postgres stores its config in
        # %AppData% on Windows.
        buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
        r = ctypes.windll.shell32.SHGetFolderPathW(  # type: ignore
            0, CSIDL_APPDATA, 0, 0, buf)
        if r:
            # Fall back to the home directory
            warnings.warn("Could not resolve %AppData%", stacklevel=2)
            return pathlib.Path.home()
        else:
            return pathlib.Path(buf.value) / 'postgresql'
else:
    def get_pg_home_directory() -> pathlib.Path:
        return pathlib.Path.home() / '.postgresql'


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


def parse_dsn(
    dsn: str,
) -> Tuple[
    Tuple[Tuple[str, int], ...],
    ConnectionParameters,
]:
    try:
        parsed, ssl_paths = edb.server._pg_rust.parse_dsn(getpass.getuser(),
                                               str(get_pg_home_directory()),
                                               dsn)
    except Exception as e:
        raise ValueError(f"{e.args[0]}") from e

    # Extract SSL configuration from the dict
    ssl = None
    sslmode = SSLMode.disable
    ssl_config = parsed['ssl']
    if 'Enable' in ssl_config:
        ssl_config = ssl_config['Enable']
        ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
        sslmode = SSLMode.parse(ssl_config[0].lower())
        ssl.check_hostname = sslmode >= SSLMode.verify_full
        ssl_config = ssl_config[1]
        if sslmode < SSLMode.require:
            ssl.verify_mode = ssl_module.CERT_NONE
        else:
            if ssl_paths['rootcert']:
                ssl.load_verify_locations(ssl_paths['rootcert'])
                ssl.verify_mode = ssl_module.CERT_REQUIRED
            else:
                if sslmode == SSLMode.require:
                    ssl.verify_mode = ssl_module.CERT_NONE
            if ssl_paths['crl']:
                ssl.load_verify_locations(ssl_paths['crl'])
                ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN
        if ssl_paths['key'] and ssl_paths['cert']:
            ssl.load_cert_chain(ssl_paths['cert'],
                                ssl_paths['key'],
                                ssl_config['password'] or '')
        if ssl_config['max_protocol_version']:
            ssl.maximum_version = _parse_tls_version(
                ssl_config['max_protocol_version'])
        if ssl_config['min_protocol_version']:
            ssl.minimum_version = _parse_tls_version(
                ssl_config['min_protocol_version'])
        # OpenSSL 1.1.1 keylog file
        if hasattr(ssl, 'keylog_filename'):
            if ssl_config['keylog_filename']:
                ssl.keylog_filename = ssl_config['keylog_filename']

    # Extract hosts from the dict
    addrs: List[Tuple[str, int]] = []
    for host in parsed['hosts']:
        if 'Hostname' in host:
            host, port = host['Hostname']
            addrs.append((host, port))
        elif 'IP' in host:
            ip, port, scope = host['IP']
            # Reconstruct the scope ID
            if scope:
                ip = f'{ip}%{scope}'
            addrs.append((ip, port))
        elif 'Path' in host:
            path, port = host['Path']
            addrs.append((path, port))
        elif 'Abstract' in host:
            path, port = host['Abstract']
            addrs.append((path, port))

    # Database/user/password/connect_timeout
    database: str = str(parsed['database']) or ''
    user: str = str(parsed['user']) or ''
    connect_timeout = parsed['connect_timeout']['secs'] \
        if parsed['connect_timeout'] else None

    # Extract password from the dict
    password: str | None = ""
    password_config = parsed['password']
    if 'Unspecified' in password_config:
        password = ''
    elif 'Specified' in password_config:
        password = password_config['Specified']

    params = ConnectionParameters(
        user=user,
        password=password,
        database=database,
        ssl=ssl,
        sslmode=sslmode,
        server_settings=parsed['server_settings'],
        connect_timeout=connect_timeout,
    )

    return tuple(addrs), params
