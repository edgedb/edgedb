#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-2010 MagicStack Inc. and the EdgeDB authors.
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
        schema_pattern: str=None,
        sequence_pattern: str=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.oid                                 AS oid,
                c.relname                             AS name,
                ns.nspname                            AS schema
            FROM
                pg_class AS c
                INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
            WHERE
                --
                -- Limit the schema scope
                --
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                --
                -- Only specified sequences
                --
                ($2::text IS NULL OR c.relname LIKE $2::text) AND
                --
                -- And only actual sequences
                --
                c.relkind = 'S'
    """, schema_pattern, sequence_pattern)
