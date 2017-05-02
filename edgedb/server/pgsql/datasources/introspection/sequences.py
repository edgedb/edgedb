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
        schema_pattern: str=None,
        sequence_pattern: str=None) -> typing.List[asyncpg.Record]:
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
