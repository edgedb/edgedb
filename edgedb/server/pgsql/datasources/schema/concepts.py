##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncpg
import typing


async def fetch(
        conn: asyncpg.connection.Connection,
        *, name: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.id AS id,
                c.name AS name,
                c.title AS title,
                c.description AS description,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final
            FROM
                edgedb.concept c
            WHERE
                ($1::text IS NULL OR c.name = $1::text) AND
                NOT c.is_virtual
    """, name)
