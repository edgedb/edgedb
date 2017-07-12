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
                a.is_abstract           AS is_abstract,
                a.is_final              AS is_final,
                edgedb._resolve_type_name(a.bases)
                                        AS bases,
                a.expr                  AS expr,
                a.subjectexpr           AS subjectexpr,
                a.localfinalexpr        AS localfinalexpr,
                a.finalexpr             AS finalexpr,
                a.errmessage            AS errmessage,
                edgedb._resolve_type(a.paramtypes)
                                        AS paramtypes,
                a.args                  AS args,
                edgedb._resolve_type_name(a.subject)
                                        AS subject
            FROM
                edgedb.constraint a
            WHERE
                $1::text IS NULL OR a.name LIKE $1::text
    """, name)
