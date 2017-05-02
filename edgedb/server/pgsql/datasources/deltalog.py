##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncpg
import typing


async def fetch(
        conn: asyncpg.connection.Connection, *,
        rev_id: str, offset: int=0) -> typing.List[asyncpg.Record]:
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
