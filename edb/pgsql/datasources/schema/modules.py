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

import asyncpg
from typing import *


async def fetch(
        conn: asyncpg.connection.Connection, *,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                id,
                name,
                builtin
            FROM
                edgedb.module AS m
            WHERE
                ($1::text[] IS NULL
                 OR split_part(m.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(m.name, '::', 1) != all($2::text[]))
    """, modules, exclude_modules)
