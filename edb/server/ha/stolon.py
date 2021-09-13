#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import asyncio
import base64
import functools
import json
import logging
import ssl
import urllib.parse
from typing import *

import httptools

from . import base

logger = logging.getLogger("edb.server")


class StolonBackend(base.HABackend):
    _master_addr: Optional[Tuple[str, int]]

    def __init__(self):
        self._waiter = None
        self._master_addr = None
        self._protocol = None
        self._watching = False
        self._cluster_protocol = None

    async def get_cluster_consensus(self) -> Tuple[str, int]:
        if self._master_addr is None:
            started_by_us = await self.start_watching()
            try:
                assert self._waiter is None
                loop = asyncio.get_running_loop()
                self._waiter = loop.create_future()
                await self._waiter
            finally:
                if started_by_us:
                    self.stop_watching()
        assert self._master_addr
        return self._master_addr

    async def start_watching(self, cluster_protocol=None):
        if cluster_protocol is not None:
            assert self._cluster_protocol is None
            self._cluster_protocol = cluster_protocol
        if not self._watching:
            self._watching = True
            try:
                self._protocol = await self._start_watching()
                return True
            except BaseException:
                self._watching = False
                raise
        return False

    def stop_watching(self):
        self._watching = False
        self._cluster_protocol = None
        protocol, self._protocol = self._protocol, None
        if protocol is not None:
            protocol.close()

    def get_master_addr(self):
        return self._master_addr

    async def _start_watching(self):
        raise NotImplementedError

    def on_cluster_data(self, data):
        cluster_status = data.get("cluster", {}).get("status", {})
        master_db = cluster_status.get("master")
        cluster_phase = cluster_status.get("phase")
        if cluster_phase != "normal":
            logger.debug("Stolon cluster phase: %r", cluster_phase)

        if not master_db:
            return

        master_status = (
            data.get("dbs", {}).get(master_db, {}).get("status", {})
        )
        master_healthy = master_status.get("healthy")
        if not master_healthy:
            logger.warning("Stolon reports unhealthy master Postgres.")
            return

        master_host = master_status.get("listenAddress")
        master_port = master_status.get("port")
        if not master_host or not master_port:
            return
        master_addr = master_host, int(master_port)
        if master_addr != self._master_addr:
            if self._master_addr is None:
                logger.info("Discovered master Postgres at %r", master_addr)
            else:
                logger.critical(
                    f"Switching over the master Postgres from %r to %r",
                    self._master_addr,
                    master_addr,
                )
                if self._cluster_protocol is not None:
                    self._cluster_protocol.set_state_failover()
            self._master_addr = master_addr

        if self._waiter is not None:
            if not self._waiter.done():
                self._waiter.set_result(None)
            self._waiter = None

    def connection_lost(self):
        self._protocol = None
        if self._watching:
            self.stop_watching()
            loop = asyncio.get_running_loop()
            loop.create_task(self.start_watching()).add_done_callback(
                self._start_watching_cb
            )

    def _start_watching_cb(self, fut: asyncio.Task):
        try:
            fut.result()
        except BaseException:
            raise


class ConsulProtocol(asyncio.Protocol):
    def __init__(self, consul_backend):
        self._consul_backend = consul_backend
        self._parser = httptools.HttpResponseParser(self)
        self._transport = None
        self._last_modify_index = None
        self._buffers = []

    def connection_made(self, transport):
        self._transport = transport
        self.request()

    def data_received(self, data: bytes):
        self._parser.feed_data(data)

    def connection_lost(self, exc):
        self._consul_backend.connection_lost()

    def on_status(self, status: bytes):
        if self._parser.get_status_code() != 200:
            logger.debug("Consul is returning non-200 responses")
            self._transport.close()

    def on_body(self, body: bytes):
        self._buffers.append(body)

    def on_message_complete(self):
        try:
            payload = json.loads(b"".join(self._buffers))[0]
            last_modify_index = payload["ModifyIndex"]
            cluster_data = json.loads(base64.b64decode(payload["Value"]))
            self._consul_backend.on_cluster_data(cluster_data)
            if self._last_modify_index != last_modify_index:
                self._last_modify_index = last_modify_index
                self.request()
        finally:
            self._buffers.clear()

    def request(self):
        uri = "/".join(
            (
                "/v1/kv/stolon/cluster",
                self._consul_backend._cluster_name,
                "clusterdata",
            )
        )
        if self._last_modify_index is not None:
            uri += f"?wait=0s&index={self._last_modify_index}"
        self._transport.write(
            f"GET {uri} HTTP/1.1\r\n"
            f"Host: {self._consul_backend._host}\r\n"
            f"\r\n".encode()
        )

    def close(self):
        self._transport.close()


class ConsulBackend(StolonBackend):
    def __init__(self, cluster_name, *, host="127.0.0.1", port=8500, ssl=None):
        super().__init__()
        self._cluster_name = cluster_name
        self._host = host
        self._port = port
        self._ssl = ssl

    async def _start_watching(self):
        loop = asyncio.get_running_loop()
        tr, pr = await loop.create_connection(
            functools.partial(ConsulProtocol, self),
            self._host,
            self._port,
            ssl=self._ssl,
        )
        return pr


def get_backend(
    sub_scheme: str, parsed_dsn: urllib.parse.ParseResult
) -> StolonBackend:
    name = parsed_dsn.path.lstrip("/")
    if not name:
        raise ValueError("Stolon requires cluster name in the URI as path.")

    cls = None
    storage, _, wire_protocol = sub_scheme.partition("+")
    if storage == "consul":
        cls = ConsulBackend
    if not cls:
        raise ValueError(f"{parsed_dsn.scheme} is not supported")
    if wire_protocol not in {"", "http", "https"}:
        raise ValueError(f"Wire protocol {wire_protocol} is not supported")

    args: Dict[str, Any] = {}
    if parsed_dsn.hostname:
        args["host"] = parsed_dsn.hostname
    if parsed_dsn.port:
        args["port"] = parsed_dsn.port
    if wire_protocol == "https":
        args["ssl"] = ssl.create_default_context()

    return cls(name, **args)
