##
# Copyright (c) 2016-present MagicStack Inc.
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
                i.id            AS id,
                i.name          AS name,
                i.expr          AS expr,
                edgedb._resolve_type_name(i.bases)
                                AS bases,
                edgedb._resolve_type_name(i.subject)
                                AS subject_name
            FROM
                edgedb.SourceIndex i
    """)
