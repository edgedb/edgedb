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
                l.id,
                edgedb._resolve_type_name(l.source) AS source,
                edgedb._resolve_type(l.target) AS target,
                edgedb._resolve_type_name(l.spectargets) AS spectargets,
                l.name AS name,
                edgedb._resolve_type_name(l.bases) AS bases,
                l.cardinality,
                l.required,
                l.computable,
                l.title,
                l.description,
                l.is_abstract,
                l.is_final,
                l.readonly,
                l.default
            FROM
                edgedb.link l
            ORDER BY
                l.name LIKE 'std::%' DESC, l.id, l.target NULLS FIRST
    """)


async def fetch_properties(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id                    AS id,
                p.name                  AS name,
                edgedb._resolve_type_name(p.bases)
                                        AS bases,
                p.title                 AS title,
                p.description           AS description,
                p.required              AS required,
                p.computable            AS computable,
                p.readonly              AS readonly,
                p.default               AS default,
                edgedb._resolve_type_name(p.source)
                                        AS source,
                edgedb._resolve_type(p.target)
                                        AS target
            FROM
                edgedb.LinkProperty p
            ORDER BY
                p.id, p.target NULLS FIRST
    """)
