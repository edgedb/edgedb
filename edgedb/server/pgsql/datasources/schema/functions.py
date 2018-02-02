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
                f.id AS id,
                f.name AS name,
                f.title AS title,
                f.description AS description,
                edgedb._resolve_type(f.paramtypes) AS paramtypes,
                f.paramnames,
                f.varparam,
                f.paramdefaults,
                f.paramkinds,
                f.aggregate,
                f.set_returning,
                f.language,
                f.code,
                f.from_function,
                f.initial_value,
                edgedb._resolve_type(f.returntype) AS returntype
            FROM
                edgedb.function f
            ORDER BY
                f.id
    """)
