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
        domain_pattern: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                t.oid                                 AS oid,
                t.typname                             AS name,
                ns.nspname                            AS schema,

                CASE WHEN t.typbasetype != 0 THEN
                    format_type(t.typbasetype, t.typtypmod)
                ELSE
                    NULL
                END                                 AS basetype_full,

                CASE WHEN t.typbasetype != 0 THEN
                    format_type(t.typbasetype, NULL)
                ELSE
                    NULL
                END                                 AS basetype,

                bns.nspname                         AS basetype_schema,

                ARRAY(SELECT
                            pg_get_constraintdef(c.oid, true)
                        FROM
                            pg_constraint AS c
                        WHERE
                            c.contypid = t.oid
                        ORDER BY
                            c.oid)                  AS constraints,

                ARRAY(SELECT
                            c.conname
                        FROM
                            pg_constraint AS c
                        WHERE
                            c.contypid = t.oid
                        ORDER BY
                            c.oid)                  AS constraint_names,

                t.typdefault                        AS default
            FROM
                pg_type AS t
                INNER JOIN pg_namespace AS ns ON ns.oid = t.typnamespace
                LEFT JOIN pg_class AS c ON c.reltype = t.oid
                LEFT JOIN pg_type AS bt ON t.typbasetype = bt.oid
                LEFT JOIN pg_namespace AS bns ON bns.oid = bt.typnamespace

             WHERE
                ($1::text IS NULL OR ns.nspname LIKE $1::text)
                AND ($2::text IS NULL OR t.typname LIKE $2::text)
                --
                -- We're not interested in shell- or pseudotypes
                -- or arrays or table row types.
                --
                AND t.typisdefined AND t.typtype != 'p' AND
                    (t.typelem IS NULL OR t.typelem = 0) AND
                    (c.oid IS NULL OR c.relkind = 'c')
    """, schema_pattern, domain_pattern)
