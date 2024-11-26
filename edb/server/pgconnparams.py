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
#

from __future__ import annotations
from typing import TypedDict, NotRequired, Optional, Unpack, Self, Any

import enum
import pathlib
import platform
import warnings

from edb.server._rust_native._pg_rust import PyConnectionParams

_system = platform.uname().system
if _system == 'Windows':
    import ctypes.wintypes

    CSIDL_APPDATA = 0x001A

    def get_pg_home_directory() -> pathlib.Path:
        # We cannot simply use expanduser() as that returns the user's
        # home directory, whereas Postgres stores its config in
        # %AppData% on Windows.
        buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
        r = ctypes.windll.shell32.SHGetFolderPathW(  # type: ignore
            0, CSIDL_APPDATA, 0, 0, buf
        )
        if r:
            # Fall back to the home directory
            warnings.warn("Could not resolve %AppData%", stacklevel=2)
            return pathlib.Path.home()
        else:
            return pathlib.Path(buf.value) / 'postgresql'

else:

    def get_pg_home_directory() -> pathlib.Path:
        return pathlib.Path.home() / '.postgresql'


class SSLMode(enum.IntEnum):
    disable = 0
    allow = 1
    prefer = 2
    require = 3
    verify_ca = 4
    verify_full = 5

    @classmethod
    def parse(cls, sslmode: str) -> Self:
        value: Self = getattr(cls, sslmode.replace('-', '_'))
        assert value is not None, f"Invalid SSL mode: {sslmode}"
        return value


class CreateParamsKwargs(TypedDict, total=False):
    dsn: NotRequired[str]
    hosts: NotRequired[Optional[list[tuple[str, int]]]]
    host: NotRequired[Optional[str]]
    user: NotRequired[Optional[str]]
    password: NotRequired[Optional[str]]
    database: NotRequired[Optional[str]]
    server_settings: NotRequired[Optional[dict[str, str]]]
    sslmode: NotRequired[Optional[SSLMode]]
    sslrootcert: NotRequired[Optional[str]]
    connect_timeout: NotRequired[Optional[int]]


class ConnectionParams:
    """
    A Python representation of the Rust connection parameters that are
    passed back during connection/parse.

    This class encapsulates the connection parameters used for establishing
    a connection to a PostgreSQL database.
    """

    _params: PyConnectionParams

    def __init__(self, **kwargs: Unpack[CreateParamsKwargs]) -> None:
        dsn = kwargs.pop("dsn", None)
        if dsn:
            self._params = PyConnectionParams(dsn)
        else:
            self._params = PyConnectionParams(None)
        self.update(**kwargs)

    @classmethod
    def _create(
        cls,
        params: dict[str, Any],
    ) -> Self:
        instance = super().__new__(cls)
        instance._params = params
        return instance

    def update(self, **kwargs: Unpack[CreateParamsKwargs]) -> None:
        if dsn := kwargs.pop('dsn', None):
            params = PyConnectionParams(dsn)
            for k, v in params.to_dict().items():
                self._params[k] = v
        if server_settings := kwargs.pop("server_settings", None):
            for k2, v2 in server_settings.items():
                self._params.update_server_settings(k2, v2)
        if host_specs := kwargs.pop("hosts", None):
            hosts, ports = zip(*host_specs)
            self._params['host'] = ','.join(hosts)
            self._params['port'] = ','.join(map(str, ports))
        if (ssl_mode := kwargs.pop("sslmode", None)) is not None:
            mode: SSLMode = ssl_mode
            self._params["sslmode"] = mode.name
        if connect_timeout := kwargs.pop("connect_timeout", None):
            self._params["connect_timeout"] = str(connect_timeout)
        for k, v in kwargs.items():
            if k == "database":
                k = "dbname"
            self._params[k] = v

    def clear_server_settings(self) -> None:
        self._params.clear_server_settings()

    def resolve(self) -> Self:
        return self._create(
            self._params.resolve("", str(get_pg_home_directory())),
        )

    def __copy__(self) -> Self:
        return self._create(self._params.clone())

    @property
    def hosts(self) -> Optional[list[tuple[dict[str, Any], int]]]:
        return self._params['hosts']  # type: ignore

    @property
    def host(self) -> Optional[str]:
        return self._params['host']  # type: ignore

    @property
    def port(self) -> Optional[int]:
        return self._params['port']  # type: ignore

    @property
    def user(self) -> Optional[str]:
        return self._params['user']  # type: ignore

    @property
    def password(self) -> Optional[str]:
        return self._params['password']  # type: ignore

    @property
    def database(self) -> Optional[str]:
        return self._params['dbname']  # type: ignore

    @property
    def connect_timeout(self) -> Optional[int]:
        connect_timeout = self._params['connect_timeout']
        return int(connect_timeout) if connect_timeout else None

    @property
    def sslmode(self) -> Optional[SSLMode]:
        sslmode = self._params['sslmode']
        return SSLMode.parse(sslmode) if sslmode is not None else None

    def to_dsn(self) -> str:
        dsn: str = self._params.to_dsn()
        return dsn

    @property
    def __dict__(self) -> dict[str, Any]:
        to_dict: dict[str, str] = self._params.to_dict()
        database = to_dict.pop('dbname', None)
        if database:
            to_dict['database'] = database
        return to_dict

    @__dict__.setter
    def __dict__(self, value: dict[str, Any]) -> None:
        new_params = self._params.__class__()
        try:
            for k, v in value.items():
                new_params[k] = v
            self._params = new_params
        except Exception:
            raise ValueError("Failed to update __dict__")

    def __repr__(self) -> Any:
        return self._params.__repr__()
