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
        schema_pattern: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                ns.oid                                AS oid,
                ns.nspname                            AS name
            FROM
                pg_namespace AS ns
            WHERE
                --
                -- Limit the schema scope
                --
                $1::text IS NULL OR ns.nspname LIKE $1
    """, schema_pattern)
