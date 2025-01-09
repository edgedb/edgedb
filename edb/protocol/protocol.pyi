#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any

from . import messages

class Connection:
    async def connect(self) -> None:
        ...

    async def execute(self, query: str, state_id: bytes, state: bytes) -> None:
        ...

    async def sync(self) -> bytes:
        ...

    async def recv(self) -> messages.ServerMessage:
        ...

    async def recv_match(
        self,
        msgcls: type[messages.ServerMessage],
        _ignore_msg: type[messages.ServerMessage] | None,
        **fields: Any,
    ) -> messages.ServerMessage:
        ...

    async def send(self, *msgs: messages.ClientMessage) -> None:
        ...

    async def aclose(self) -> None:
        ...
