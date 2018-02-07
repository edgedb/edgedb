##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncpg
import typing


async def fetch(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.id AS id,
                c.name AS name,
                c.title AS title,
                c.description AS description,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final,
                c.view_type AS view_type,
                c.expr AS expr
            FROM
                edgedb.concept c
    """)


async def fetch_derived(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.id AS id,
                c.name AS name,
                edgedb._resolve_type_name(c.bases) AS bases,
                c.title AS title,
                c.description AS description,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final,
                c.view_type AS view_type,
                c.expr AS expr
            FROM
                edgedb.derivedconcept c
    """)
