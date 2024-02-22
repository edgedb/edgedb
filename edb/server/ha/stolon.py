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

from __future__ import annotations
from typing import Any, Optional, Tuple, Dict

import asyncio
import base64
import functools
import json
import logging
import os
import ssl
import urllib.parse

from edb.common import asyncwatcher
from edb.common import token_bucket
from edb.server import consul

from . import base

logger = logging.getLogger("edb.server")


class StolonBackend(base.HABackend):
    _master_addr: Optional[Tuple[str, int]]

    def __init__(self) -> None:
        super().__init__()
        self._master_addr = None

    async def get_cluster_consensus(self) -> Tuple[str, int]:
        if self._master_addr is None:
            started_by_us = await self.start_watching()
            try:
                assert self._waiter is None
                self._waiter = asyncio.get_running_loop().create_future()
                await self._waiter
            finally:
                if started_by_us:
                    self.stop_watching()
                    await self.wait_stopped_watching()
        assert self._master_addr
        return self._master_addr

    def get_master_addr(self) -> Optional[Tuple[str, int]]:
        return self._master_addr

    def _on_update(self, payload: bytes) -> None:
        try:
            data = json.loads(base64.b64decode(payload))
        except (TypeError, ValueError):
            logger.exception(f"could not decode Stolon cluster data")
            return

        # Successful Consul response, reset retry backoff
        self._retry_attempt = 0

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
                self._master_addr = master_addr
            else:
                logger.critical(
                    f"Switching over the master Postgres from %r to %r",
                    self._master_addr,
                    master_addr,
                )
                self._master_addr = master_addr
                if self._failover_cb is not None:
                    self.incr_metrics_counter("failover")
                    self._failover_cb()

        if self._waiter is not None:
            if not self._waiter.done():
                self._waiter.set_result(None)
            self._waiter = None


class StolonConsulBackend(StolonBackend):
    def __init__(
        self,
        cluster_name: str,
        *,
        host: str = "127.0.0.1",
        port: int = 8500,
        ssl: Optional[ssl.SSLContext] = None,
    ) -> None:
        super().__init__()
        self._cluster_name = cluster_name
        self._host = host
        self._port = port
        self._ssl = ssl

        # This means we can request for 10 consecutive requests immediately
        # after each response without delay, and then we're capped to 0.1
        # request(token) per second, or 1 request per 10 seconds.
        cap = float(os.environ.get("EDGEDB_SERVER_CONSUL_TOKEN_CAPACITY", 10))
        rate = float(os.environ.get("EDGEDB_SERVER_CONSUL_TOKEN_RATE", 0.1))
        self._token_bucket = token_bucket.TokenBucket(cap, rate)

    async def _start_watching(self) -> asyncwatcher.AsyncWatcherProtocol:
        _, pr = await asyncio.get_running_loop().create_connection(
            functools.partial(
                consul.ConsulKVProtocol,
                self,
                self._host,
                f"stolon/cluster/{self._cluster_name}/clusterdata",
            ),
            self._host,
            self._port,
            ssl=self._ssl,
        )
        return pr  # type: ignore [return-value]

    @functools.cached_property
    def dsn(self) -> str:
        proto = "http" if self._ssl is None else "https"
        return (
            f"stolon+consul+{proto}://"
            f"{self._host}:{self._port}/{self._cluster_name}"
        )

    def consume_tokens(self, tokens: int) -> float:
        return self._token_bucket.consume(tokens)


def get_backend(
    sub_scheme: str, parsed_dsn: urllib.parse.ParseResult
) -> StolonBackend:
    name = parsed_dsn.path.lstrip("/")
    if not name:
        raise ValueError("Stolon requires cluster name in the URI as path.")

    cls = None
    storage, _, wire_protocol = sub_scheme.partition("+")
    if storage == "consul":
        cls = StolonConsulBackend
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
