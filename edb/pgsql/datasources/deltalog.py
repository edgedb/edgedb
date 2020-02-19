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
        rev_id: str, offset: int=0) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                *
            FROM
                (WITH RECURSIVE deltalog("id", "offset", "parents") AS (
                        SELECT
                                log.id      AS "id",
                                0           AS "offset",
                                log.parents AS "parents"
                            FROM
                                edgedb.deltalog log
                            WHERE
                                log.id = $1
                      UNION ALL
                        SELECT
                                log.id            AS "id",
                                l."offset" + 1    AS "offset",
                                log.parents AS "parents"
                            FROM
                                edgedb.deltalog log,
                                deltalog l
                            WHERE
                                log.id = any(l.parents)
                                AND l."offset" < $2::int
                )
                SELECT "id", "offset" FROM deltalog) q
            WHERE
                "offset" = $2::int
    """, rev_id, offset)
