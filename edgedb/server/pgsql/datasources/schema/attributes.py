##
# Copyright (c) 2013-present MagicStack Inc.
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
                a.id                    AS id,
                a.name                  AS name,
                a.title                 AS title,
                a.description           AS description,
                edgedb._resolve_type(a.type)
                                        AS type
            FROM
                edgedb.attribute a
            WHERE
                $1::text IS NULL OR a.name LIKE $1::text
    """, name)


async def fetch_values(
        conn: asyncpg.connection.Connection,
        *, name: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                a.id                        AS id,
                a.name                      AS name,
                edgedb._resolve_type_name(a.subject)
                                            AS subject_name,
                edgedb._resolve_type_name(a.attribute)
                                            AS attribute_name,
                a.value                     AS value
            FROM
                edgedb.AttributeValue a
            WHERE
                $1::text IS NULL OR a.name LIKE $1::text
    """, name)
