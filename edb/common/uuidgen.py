#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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
from typing import *  # NoQA

import hashlib
import os
import uuid

from asyncpg.pgproto import pgproto


UUID: Type[uuid.UUID] = pgproto.UUID


def uuid1mc() -> uuid.UUID:
    """Generate a v1 UUID using a pseudo-random multicast node address."""

    # Note: cannot use pgproto.UUID since it's UUID v1
    node = int.from_bytes(os.urandom(6), byteorder='little') | (1 << 40)
    return UUID(uuid.uuid1(node=node).bytes)  # type: ignore


# type-ignores below because the first argument to uuid.UUID is a string
# called `hex` which is not something that pgproto.UUID supports.


def uuid4() -> uuid.UUID:
    """Generate a random UUID."""
    return UUID(os.urandom(16))  # type: ignore


def uuid5(namespace: uuid.UUID, name: str) -> uuid.UUID:
    """Generate a UUID from the SHA-1 hash of a namespace UUID and a name."""
    hash = hashlib.sha1(namespace.bytes + bytes(name, "utf-8")).digest()
    return UUID(hash[:16])  # type: ignore
