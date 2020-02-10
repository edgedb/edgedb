#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import asyncpg
from typing import *


async def fetch_tables(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None,
        table_pattern: str=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.oid                                 AS oid,
                typ.oid                               AS typoid,
                c.relname                             AS name,
                ns.nspname                            AS schema,
                cmt.description                       AS comment
            FROM
                pg_class AS c
                INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
                INNER JOIN pg_type AS typ ON typ.typrelid = c.oid
                LEFT JOIN pg_description AS cmt
                    ON (cmt.objoid = c.oid AND cmt.objsubid = 0)
            WHERE
                --
                -- Limit the schema scope
                --
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                --
                -- Only specified tables
                --
                ($2::text IS NULL OR c.relname LIKE $2::text) AND
                --
                -- And only real tables
                --
                c.relkind = 'r'
    """, schema_pattern, table_pattern)


async def fetch_columns(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        include_inherited: bool=True) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                ns.nspname                              AS table_schema,
                c.relname                               AS table_name,
                a.attname                               AS column_name,
                t.typname                               AS column_type,
                a.atttypmod                             AS column_type_mod,
                tns.nspname                             AS column_type_schema,
                t.oid                                   AS column_type_oid,
                a.attnotnull                            AS column_required,
                pg_get_expr(def.adbin, c.oid, true)     AS column_default,
                a.attislocal                            AS column_is_local,
                a.attinhcount                           AS column_ancestors,
                cmt.description                         AS column_comment
            FROM
                pg_class c
                INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
                INNER JOIN pg_attribute a ON c.oid = a.attrelid
                INNER JOIN pg_type t ON a.atttypid = t.oid
                INNER JOIN pg_namespace tns ON t.typnamespace = tns.oid
                LEFT JOIN pg_attrdef def
                    ON (def.adrelid = c.oid AND def.adnum = a.attnum)
                LEFT JOIN pg_description cmt
                    ON (cmt.objoid = c.oid AND cmt.objsubid = a.attnum)
            WHERE
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                ($2::text IS NULL OR c.relname LIKE $2::text) AND
                a.attnum > 0 AND
                ($3::bool OR a.attislocal)
            ORDER BY
                a.attnum
    """, schema_pattern, table_pattern, include_inherited)


async def fetch_inheritance(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        max_depth: int=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                *
            FROM
                (WITH RECURSIVE
                    inheritance(oid, name, ns, depth, pos, path) AS (
                        SELECT
                                c.oid,
                                c.relname,
                                ns.nspname,
                                0,
                                0,
                                ARRAY[c.relname]
                            FROM
                                pg_class c
                                INNER JOIN pg_namespace ns
                                    ON c.relnamespace = ns.oid
                            WHERE
                                ($1::text IS NULL OR
                                    ns.nspname LIKE $1::text) AND
                                ($2::text IS NULL OR
                                    c.relname LIKE $2::text)

                        UNION ALL

                        SELECT
                                c.oid,
                                c.relname,
                                ns.nspname,
                                i.depth + 1,
                                pgi.inhseqno AS pos,
                                i.path || c.relname
                            FROM
                                pg_class c,
                                inheritance i,
                                pg_inherits pgi,
                                pg_namespace ns
                            WHERE
                                i.oid = pgi.inhrelid
                                AND c.oid = pgi.inhparent
                                AND ns.oid = c.relnamespace
                                AND ($3::int IS NULL OR i.depth < $3::int)
                )
                SELECT DISTINCT ON (ns, name)
                    ns, name, depth, pos FROM inheritance
                ) q
            ORDER BY
                depth, pos
    """, schema_pattern, table_pattern, max_depth)


async def fetch_descendants(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        max_depth: int=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                *
            FROM
                (WITH RECURSIVE
                    inheritance(oid, name, ns, depth, path) AS (
                        SELECT
                                c.oid,
                                c.relname,
                                ns.nspname,
                                0,
                                ARRAY[c.relname]
                            FROM
                                pg_class c
                                INNER JOIN pg_namespace ns
                                    ON c.relnamespace = ns.oid
                            WHERE
                                ($1::text IS NULL OR
                                    ns.nspname LIKE $1::text) AND
                                ($2::text IS NULL OR
                                    c.relname LIKE $2::text)

                        UNION ALL

                        SELECT
                                c.oid,
                                c.relname,
                                ns.nspname,
                                i.depth + 1,
                                i.path || c.relname
                            FROM
                                pg_class c,
                                inheritance i,
                                pg_inherits pgi,
                                pg_namespace ns
                            WHERE
                                i.oid = pgi.inhparent
                                AND c.oid = pgi.inhrelid
                                AND ns.oid = c.relnamespace
                                AND ($3::int IS NULL OR i.depth < $3::int)
                )
                SELECT DISTINCT ON (ns, name)
                    ns, name, depth FROM inheritance) q
            WHERE
                depth > 0
            ORDER BY
                depth
    """, schema_pattern, table_pattern, max_depth)


async def fetch_indexes(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        table_list: Optional[List[str]]=None,
        index_pattern: str=None, inheritable_only: bool=False,
        include_inherited: bool=False) -> List[asyncpg.Record]:

    qry = """
        SELECT
            i.table_name                            AS table_name,
            array_agg((
                i.index_name,
                i.index_is_unique,
                i.index_predicate,
                i.index_expression,
                i.index_columns,
                i.index_metadata
            ))                                      AS indexes
        FROM
            (SELECT
                *
             FROM
                (SELECT
                    ARRAY[ns.nspname, c.relname]    AS table_name,
                    ic.relname::text                AS index_name,
                    i.indisunique                   AS index_is_unique,
                    pg_get_expr(i.indpred, i.indrelid)::text
                                                    AS index_predicate,
                    pg_get_expr(i.indexprs, i.indrelid)::text
                                                    AS index_expression,

                    (SELECT
                        array_agg(ia.attname ORDER BY ia.attnum)
                     FROM
                        pg_attribute AS ia
                     WHERE
                        ia.attrelid = i.indexrelid
                        AND (ia.attnum IS NULL OR ia.attnum >= 1)
                    )                               AS index_columns,

                    edgedb.obj_metadata(i.indexrelid, 'pg_class')
                                                    AS index_metadata

                 FROM
                    pg_class AS c
                    INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
                    INNER JOIN pg_index AS i ON i.indrelid = c.oid
                    INNER JOIN pg_class AS ic ON i.indexrelid = ic.oid

                 WHERE
                    ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                    ($2::text IS NULL OR c.relname LIKE $2::text) AND
                    ($3::text[] IS NULL OR
                        ns.nspname || '.' || ic.relname = any($3::text[])) AND
                    ($4::text IS NULL OR ic.relname LIKE $4::text)
                ) AS q

             WHERE
                (NOT $5::bool OR
                    (index_metadata IS NOT NULL AND
                        (index_metadata->>'ddl:inherit')::bool))
                AND (
                    $6 OR
                    (
                        index_metadata IS NULL OR
                        NOT coalesce(
                            (index_metadata->>'ddl:inherited')::bool, false)
                    )
                )

            ) AS i

        GROUP BY
            i.table_name
    """

    return await conn.fetch(
        qry, schema_pattern, table_pattern, table_list, index_pattern,
        inheritable_only, include_inherited)


async def fetch_constraints(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        table_list: Optional[List[str]]=None,
        constraint_pattern: str=None,
        include_inherited: bool=False) -> List[asyncpg.Record]:

    qry = """
        SELECT
            c.table_name                        AS table_name,
            array_agg((
                c.constraint_id,
                c.constraint_type,
                c.constraint_name,
                c.constraint_expression,
                c.constraint_proc,
                c.constraint_description,
                c.constraint_columns
            ))                                  AS constraints
        FROM
            (SELECT
                ARRAY[ns.nspname, cc.relname]   AS table_name,
                c.oid::int                      AS constraint_id,
                c.contype::text                 AS constraint_type,
                c.conname::text                 AS constraint_name,

                -- Unique constraints have their expression
                -- stored in index catalog.
                -- Check constraints have their expression
                -- stored in constraint catalog.
                coalesce(
                    pg_get_expr(c.conbin, c.conrelid)::text,
                    pg_get_expr(ind.indexprs, ind.indrelid)::text
                )                               AS constraint_expression,

                (CASE
                    WHEN c.contype::text = 't' THEN
                        (SELECT
                            (nsp.nspname, p.proname)
                         FROM
                            pg_proc AS p
                            INNER JOIN pg_namespace AS nsp
                                    ON nsp.oid = p.pronamespace
                            INNER JOIN pg_trigger AS trg
                                    ON trg.tgfoid = p.oid
                         WHERE
                            trg.tgconstraint = c.oid
                         LIMIT 1)
                    ELSE
                        NULL
                END)                            AS constraint_proc,

                (CASE
                    WHEN c.contype::text = 't' THEN
                        (SELECT
                            cmt.description
                         FROM
                            pg_description AS cmt
                            INNER JOIN pg_trigger AS trg
                                    ON trg.oid = cmt.objoid
                         WHERE
                            trg.tgconstraint = c.oid
                            AND cmt.description IS NOT NULL
                            AND cmt.description != ''
                         LIMIT 1)
                    ELSE
                        (SELECT
                            cmt.description
                         FROM
                            pg_description AS cmt
                            INNER JOIN pg_constraint AS cmtc
                                    ON cmtc.oid = cmt.objoid
                         WHERE
                            cmtc.conname = c.conname
                            AND cmtc.coninhcount = 0
                            AND cmt.description IS NOT NULL
                            AND cmt.description != ''
                         LIMIT 1)
                END)                            AS constraint_description,

                (SELECT
                    array_agg(ia.attname ORDER BY ia.attnum)
                 FROM
                    pg_attribute AS ia
                 WHERE
                    ia.attrelid = c.conindid
                )                               AS constraint_columns

             FROM
                pg_constraint AS c
                INNER JOIN pg_class AS cc ON c.conrelid = cc.oid
                INNER JOIN pg_namespace AS ns ON ns.oid = cc.relnamespace
                LEFT JOIN pg_index AS ind ON ind.indexrelid = c.conindid
             WHERE
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                ($2::text IS NULL OR c.relname LIKE $2::text) AND
                ($3::text[] IS NULL OR
                    ns.nspname || '.' || cc.relname = any($3::text[])) AND
                ($4::text IS NULL OR cc.relname LIKE $4::text) AND
                ($5::bool OR c.coninhcount = 0)
            ) AS c

        GROUP BY
            table_name
    """

    return await conn.fetch(
        qry, schema_pattern, table_pattern, table_list, constraint_pattern,
        include_inherited)


async def fetch_triggers(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None, table_pattern: str=None,
        table_list: Optional[List[str]]=None,
        trigger_pattern: str=None,
        inheritable_only: bool=False) -> List[asyncpg.Record]:

    qry = """
        SELECT
            table_name                          AS table_name,
            array_agg((
                trg_id,
                trg_name,
                trg_proc,
                trg_constraint,
                trg_granularity,
                trg_timing,
                trg_events,
                trg_definition,
                trg_metadata
            ))                                  AS triggers
        FROM
            (SELECT
                *
             FROM
                (SELECT
                    (ns.nspname, tc.relname)                AS table_name,

                    t.oid::int                              AS trg_id,
                    t.tgname::text                          AS trg_name,

                    (SELECT
                        (nsp.nspname, p.proname)
                     FROM
                        pg_proc AS p
                        INNER JOIN pg_namespace AS nsp
                                ON nsp.oid = p.pronamespace
                     WHERE
                        t.tgfoid = p.oid
                    )                                       AS trg_proc,

                    t.tgconstraint                          AS trg_constraint,

                    (CASE
                        WHEN (t.tgtype & (1 << 0)) != 0 THEN 'row'
                        ELSE 'statement'
                    END)                                    AS trg_granularity,

                    (CASE
                        WHEN (t.tgtype & (1 << 1)) != 0 THEN 'before'
                        WHEN (t.tgtype & (1 << 6)) != 0 THEN 'instead'
                        ELSE 'after'
                    END)                                    AS trg_timing,

                    array_remove(ARRAY[
                        (CASE WHEN (t.tgtype & (1 << 2)) != 0 THEN 'insert'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 3)) != 0 THEN 'delete'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 4)) != 0 THEN 'update'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 5)) != 0 THEN 'truncate'
                         ELSE NULL END)
                    ]::text[], NULL)                        AS trg_events,

                    pg_get_triggerdef(t.oid)::text          AS trg_definition,

                    edgedb.obj_metadata(t.oid, 'pg_trigger') AS trg_metadata

                 FROM
                    pg_trigger AS t
                    INNER JOIN pg_class AS tc ON t.tgrelid = tc.oid
                    INNER JOIN pg_namespace AS ns ON ns.oid = tc.relnamespace

                 WHERE
                    ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                    ($2::text IS NULL OR tc.relname LIKE $2::text) AND
                    ($3::text[] IS NULL OR
                        ns.nspname || '.' || tc.relname = any($3::text[])) AND
                    ($4::text IS NULL OR t.tgname LIKE $4::text)
                ) AS q

             WHERE
                NOT $5::bool OR
                (trg_metadata IS NOT NULL AND
                    (trg_metadata->>'ddl:inherit')::bool)
            ) AS t

        GROUP BY
            table_name
    """

    return await conn.fetch(
        qry, schema_pattern, table_pattern, table_list, trigger_pattern,
        inheritable_only)
