#
# This source file is part of the EdgeDB open source project.
#
# Copyright EdgeDB Inc. and the EdgeDB authors.
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
from typing import Optional

import asyncio
import json
import logging
import random
import urllib.parse

import httptools

from edb.common import asyncwatcher


logger = logging.getLogger("edb.server.consul")


class ConsulKVProtocol(asyncwatcher.AsyncWatcherProtocol):
    def __init__(
        self,
        watcher: asyncwatcher.AsyncWatcher,
        consul_host: str,
        key: str,
    ) -> None:
        assert not key.startswith("/"), "absolute path rewrites Consul KV URL"
        super().__init__(watcher)
        self._host = consul_host
        self._key = key
        self._watcher = watcher
        self._parser = httptools.HttpResponseParser(self)
        self._last_modify_index: Optional[str] = None
        self._buffers: list[bytes] = []

    def data_received(self, data: bytes) -> None:
        self._parser.feed_data(data)

    def on_status(self, status: bytes) -> None:
        status_code = self._parser.get_status_code()
        if status_code != 200:
            logger.debug(
                "Consul is returning non-200 responses: %s %r",
                status_code,
                status,
            )
            if self._transport is not None:
                self._transport.close()

    def on_body(self, body: bytes) -> None:
        self._buffers.append(body)

    def on_message_complete(self) -> None:
        try:
            code = self._parser.get_status_code()
            if code == 200:
                self._watcher.incr_metrics_counter("watch-update")
                payload = json.loads(b"".join(self._buffers))[0]
                last_modify_index = payload["ModifyIndex"]
                self._watcher.on_update(payload["Value"])
                if self._last_modify_index == last_modify_index:
                    self._watcher.incr_metrics_counter("watch-timeout")
                    self._last_modify_index = None
                else:
                    self._last_modify_index = last_modify_index
            else:
                self._watcher.incr_metrics_counter(f"watch-err-{code}")
            self.request()

        finally:
            self._buffers.clear()

    def request(self) -> None:
        delay = self._watcher.consume_tokens(1)
        if delay > 0:
            asyncio.get_running_loop().call_later(
                delay + random.random() * 0.1, self.request
            )
            return
        uri = urllib.parse.urljoin("/v1/kv/", self._key)
        if self._last_modify_index is not None:
            uri += f"?index={self._last_modify_index}"
        if self._transport is None or self._transport.is_closing():
            logger.error("cannot perform Consul request: connection is closed")
            return
        self._transport.write(
            f"GET {uri} HTTP/1.1\r\n"
            f"Host: {self._host}\r\n"
            f"\r\n".encode()
        )

    def close(self) -> None:
        if self._transport is not None and not self._transport.is_closing():
            self._transport.close()
            self._transport = None
