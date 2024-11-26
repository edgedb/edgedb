#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

"""
This module implements a Rust-based transport for PostgreSQL connections.

The PGRawConn class provides a high-level interface for establishing and
managing PostgreSQL connections using a Rust-implemented state machine. It
handles the complexities of connection establishment, including SSL negotiation
and authentication, while presenting a simple asyncio-like transport interface
to the caller.
"""

from __future__ import annotations
from typing import Optional, List, Tuple, Protocol, Callable, Dict, Any, TypeVar

import asyncio
import ssl as ssl_module
import socket
import warnings

from enum import Enum, auto
from dataclasses import dataclass

from edb.server._rust_native import _pg_rust as pgrust
from edb.server.pgconnparams import (
    ConnectionParams,
    SSLMode,
    get_pg_home_directory,
)

from . import errors as pgerror

TCP_KEEPIDLE = 24
TCP_KEEPINTVL = 2
TCP_KEEPCNT = 3
DEFAULT_CONNECT_TIMEOUT = 60


class ConnectionStateType(Enum):
    CONNECTING = 0
    SSL_CONNECTING = auto()
    AUTHENTICATING = auto()
    SYNCHRONIZING = auto()
    READY = auto()


class Authentication(Enum):
    NONE = 0
    TRUST = auto()
    PASSWORD = auto()
    MD5 = auto()
    SCRAM_SHA256 = auto()


@dataclass
class PGState:
    parameters: Dict[str, str]
    cancellation_key: Optional[Tuple[int, int]]
    auth: Optional[Authentication]
    server_error: Optional[list[tuple[str, str]]]
    ssl: bool


class ConnectionStateUpdate(Protocol):
    def send(self, message: memoryview) -> None: ...
    def upgrade(self) -> None: ...
    def parameter(self, name: str, value: str) -> None: ...
    def cancellation_key(self, pid: int, key: int) -> None: ...
    def state_changed(self, state: int) -> None: ...
    def auth(self, auth: int) -> None: ...


StateChangeCallback = Callable[[ConnectionStateType], None]


def _parse_tls_version(tls_version: str) -> ssl_module.TLSVersion:
    if tls_version.startswith('SSL'):
        raise ValueError(f"Unsupported TLS version: {tls_version}")
    try:
        return ssl_module.TLSVersion[tls_version.replace('.', '_')]
    except KeyError:
        raise ValueError(f"No such TLS version: {tls_version}")


def _create_ssl(ssl_config: Dict[str, Any]):
    sslmode = SSLMode.parse(ssl_config['sslmode'])
    ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
    ssl.check_hostname = sslmode >= SSLMode.verify_full
    if sslmode < SSLMode.require:
        ssl.verify_mode = ssl_module.CERT_NONE
    else:
        if ssl_config['sslrootcert']:
            ssl.load_verify_locations(ssl_config['sslrootcert'])
            ssl.verify_mode = ssl_module.CERT_REQUIRED
        else:
            if sslmode == SSLMode.require:
                ssl.verify_mode = ssl_module.CERT_NONE
        if ssl_config['sslcrl']:
            ssl.load_verify_locations(ssl_config['sslcrl'])
            ssl.verify_flags |= ssl_module.VERIFY_CRL_CHECK_CHAIN
    if ssl_config['sslkey'] and ssl_config['sslcert']:
        ssl.load_cert_chain(
            ssl_config['sslcert'],
            ssl_config['sslkey'],
            ssl_config['sslpassword'] or '',
        )
    if ssl_config['ssl_max_protocol_version']:
        ssl.maximum_version = _parse_tls_version(
            ssl_config['ssl_max_protocol_version']
        )
    if ssl_config['ssl_min_protocol_version']:
        ssl.minimum_version = _parse_tls_version(
            ssl_config['ssl_min_protocol_version']
        )
    # OpenSSL 1.1.1 keylog file
    if hasattr(ssl, 'keylog_filename'):
        if ssl_config['keylog_filename']:
            ssl.keylog_filename = ssl_config['keylog_filename']
    return ssl


class PGConnectionProtocol(asyncio.Protocol):
    """A protocol that manages the initial connection and authentication process
    for PostgreSQL.

    This protocol acts as an intermediary between the raw socket connection and
    the user's protocol.
    """

    def __init__(
        self,
        hostname: Optional[str],
        state: pgrust.PyConnectionState,
        pg_state: PGState,
        complete_callback: Callable[
            [asyncio.BaseTransport], Tuple[PGRawConn, asyncio.Protocol]
        ],
    ):
        self.state = state
        self.pg_state = pg_state
        self.ready_future: asyncio.Future = asyncio.Future()
        self._complete_callback = complete_callback
        self._host = hostname
        self._transport: Optional[asyncio.Transport] = None

    def data_received(self, data: bytes):
        if self.ready_future.done():
            return

        try:
            self.state.drive_message(memoryview(data))
            if self.state.is_ready():
                assert self._transport is not None
                self.ready_future.set_result(
                    self._complete_callback(self._transport)
                )
        except Exception as e:
            if not self.ready_future.done():
                self.ready_future.set_exception(ConnectionError(e))

    def connection_lost(self, exc):
        if self.ready_future.done():
            return
        if exc:
            self.ready_future.set_exception(exc)
        else:
            ex = pgerror.new(
                pgerror.ERROR_CONNECTION_FAILURE,
                "Unexpected connection error",
            )
            ex.__cause__ = exc
            self.ready_future.set_exception(ex)

    # This may be called multiple times if we upgrade to SSL
    def connection_made(self, transport) -> None:
        try:
            if self._transport is None:
                # Initial connection
                self._transport = transport
                self.state.update = self
                self.state.drive_initial()
            else:
                # Upgrade path
                self._transport = transport
        except Exception:
            pass
        return super().connection_made(transport)

    def send(self, message: memoryview) -> None:
        assert self._transport is not None
        self._transport.write(bytes(message))

    def upgrade(self) -> None:
        asyncio.create_task(self._upgrade_to_ssl())

    async def _upgrade_to_ssl(self):
        transport = self._transport
        assert transport is not None
        try:
            ssl_context = _create_ssl(self.state.config)
            loop = asyncio.get_running_loop()
            new_transport = await loop.start_tls(
                transport,
                self,
                ssl_context,
                server_side=False,
                ssl_handshake_timeout=None,
                server_hostname=self._host,
            )
            self._transport = new_transport
            self.state.drive_ssl_ready()
            self.pg_state.ssl = True
        except Exception as e:
            if not self.ready_future.done():
                self.ready_future.set_exception(e)
            transport.abort()

    def parameter(self, name: str, value: str) -> None:
        self.pg_state.parameters[name] = value

    def cancellation_key(self, pid: int, key: int) -> None:
        self.pg_state.cancellation_key = (pid, key)

    def state_changed(self, _: int) -> None:
        pass

    def auth(self, auth: int) -> None:
        self.pg_state.auth = Authentication(auth)

    def server_error(self, error: list[tuple[str, str]]) -> None:
        if not self.ready_future.done():
            self.ready_future.set_exception(
                pgerror.BackendConnectionError(fields=dict(error))
            )


class PGRawConn(asyncio.Transport):
    def __init__(
        self,
        source_description: Optional[str],
        connection: ConnectionParams,
        raw_transport: asyncio.Transport,
        pg_state: PGState,
        addr: tuple[str, int],
    ):
        super().__init__()
        self.source_description = source_description
        self.connection = connection
        self.raw_transport = raw_transport
        self._pg_state = pg_state
        self.addr = addr

    @property
    def state(self):
        return self._pg_state

    def write(self, data: bytes | bytearray | memoryview):
        self.raw_transport.write(data)

    def close(self):
        self.raw_transport.close()

    def is_closing(self):
        return self.raw_transport.is_closing()

    def get_extra_info(self, name: str, default=None):
        return self.raw_transport.get_extra_info(name, default)

    def pause_reading(self):
        self.raw_transport.pause_reading()

    def resume_reading(self):
        self.raw_transport.resume_reading()

    def is_reading(self):
        return self.raw_transport.is_reading()

    def abort(self):
        self.raw_transport.abort()

    def __repr__(self):
        params = ', '.join(
            f"{k}={v}" for k, v in self._pg_state.parameters.items()
        )
        auth_str = (
            f", auth={self._pg_state.auth.name}" if self._pg_state.auth else ""
        )
        source_str = (
            f", source={self.source_description}"
            if self.source_description
            else ""
        )
        raw_repr = repr(self.raw_transport)
        dsn = self.connection._params
        return (
            f"<PGRawConn: connected{auth_str}{source_str}, {params}, "
            f"dsn={dsn}, raw_connection={raw_repr}>"
        )

    def __del__(self):
        if not self.raw_transport.is_closing():
            warnings.warn(
                f"unclosed connection {repr(self)}",
                ResourceWarning,
                stacklevel=2,
            )
            self.raw_transport.abort()


async def _create_connection_to(
    protocol_factory: Callable[[Optional[str], str, int], PGConnectionProtocol],
    address_family: str,
    host: str | bytes,
    hostname: str,
    port: int,
) -> Tuple[asyncio.Transport, PGConnectionProtocol]:
    if address_family == "unix":
        t, protocol = await asyncio.get_running_loop().create_unix_connection(
            lambda: protocol_factory(None, hostname, port), path=host  # type: ignore
        )
        return (t, protocol)
    else:
        t, protocol = await asyncio.get_running_loop().create_connection(
            lambda: protocol_factory(hostname, hostname, port), str(host), port
        )
        _set_tcp_keepalive(t)
        return (t, protocol)


async def _create_connection(
    protocol_factory: Callable[[Optional[str], str, int], PGConnectionProtocol],
    connect_timeout: Optional[int],
    host_candidates: List[Tuple[str, str | bytes, str, int]],
) -> Tuple[asyncio.Transport, PGConnectionProtocol]:
    e = None
    for address_family, host, hostname, port in host_candidates:
        try:
            async with asyncio.timeout(
                connect_timeout if connect_timeout else DEFAULT_CONNECT_TIMEOUT
            ):
                return await _create_connection_to(
                    protocol_factory, address_family, host, hostname, port
                )
        except asyncio.TimeoutError as ex:
            raise pgerror.new(
                pgerror.ERROR_CONNECTION_FAILURE,
                "timed out connecting to backend",
            ) from ex
        except Exception as ex:
            e = ex
            continue
    raise ConnectionError(
        f"Failed to connect to any of the provided hosts: {host_candidates}"
    ) from e


def _set_tcp_keepalive(transport):
    # TCP keepalive was initially added here for special cases where idle
    # connections are dropped silently on GitHub Action running test suite
    # against AWS RDS. We are keeping the TCP keepalive for generic
    # Postgres connections as the kernel overhead is considered low, and
    # in certain cases it does save us some reconnection time.
    #
    # In case of high-availability Postgres, TCP keepalive is necessary to
    # disconnect from a failing master node, if no other failover information
    # is available.
    sock = transport.get_extra_info('socket')
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # TCP_KEEPIDLE: the time (in seconds) the connection needs to remain idle
    # before TCP starts sending keepalive probes. This is socket.TCP_KEEPIDLE
    # on Linux, and socket.TCP_KEEPALIVE on macOS from Python 3.10.
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEPIDLE)
    if hasattr(socket, 'TCP_KEEPALIVE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, TCP_KEEPIDLE)

    # TCP_KEEPINTVL: The time (in seconds) between individual keepalive probes.
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEPINTVL)

    # TCP_KEEPCNT: The maximum number of keepalive probes TCP should send
    # before dropping the connection.
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEPCNT)


P = TypeVar('P', bound=asyncio.Protocol)


def complete_connection_callback(
    host, port, source_description, state, protocol_factory, pg_state
) -> Callable[[asyncio.BaseTransport], Tuple[PGRawConn, asyncio.Protocol]]:
    def complete_connection(upgraded_transport):
        conn = PGRawConn(
            source_description,
            ConnectionParams._create(state.config),
            upgraded_transport,
            pg_state,
            (host, port),
        )

        # We've successfully upgraded the protocol at this point, and the remote
        # PG server is sitting in the idle state, waiting for us to send the
        # next message. We transition to the user protocol here, synthesizing
        # a `connection_made` event.
        user_protocol = protocol_factory()
        upgraded_transport.set_protocol(user_protocol)

        # Notify the user protocol of successful connection
        user_protocol.connection_made(conn)
        return conn, user_protocol

    return complete_connection


async def create_postgres_connection(
    dsn: str | ConnectionParams,
    protocol_factory: Callable[[], P],
    *,
    source_description: Optional[str] = None,
) -> Tuple[PGRawConn, P]:
    """
    Open a PostgreSQL connection to the address specified by the DSN or
    ConnectionParams, creating the user protocol from the protocol_factory.

    This method establishes the connection asynchronously. When successful, it
    returns a (PGRawConn, protocol) pair.
    """
    if isinstance(dsn, str):
        dsn = ConnectionParams(dsn=dsn)
    connect_timeout = dsn.connect_timeout
    try:
        state = pgrust.PyConnectionState(
            dsn._params, "postgres", str(get_pg_home_directory())
        )
    except Exception as e:
        raise ValueError(e)
    pg_state = PGState(
        parameters={},
        cancellation_key=None,
        auth=None,
        server_error=None,
        ssl=False,
    )

    # The PGConnectionProtocol will drive the PyConnectionState from network
    # bytes it receives, as well as driving the connection from the messages
    # from PyConnectionState.
    connect_protocol_factory = (
        lambda hostname, host, port: PGConnectionProtocol(
            hostname,
            state,
            pg_state,
            complete_connection_callback(
                host,
                port,
                source_description,
                state,
                protocol_factory,
                pg_state,
            ),
        )
    )

    # Create a transport to the backend based off the host candidates.
    host_candidates = await asyncio.get_running_loop().run_in_executor(
        executor=None, func=lambda: state.config.host_candidates
    )
    _, protocol = await _create_connection(
        connect_protocol_factory,
        connect_timeout,
        host_candidates,
    )

    conn, user_protocol = await protocol.ready_future
    return conn, user_protocol
