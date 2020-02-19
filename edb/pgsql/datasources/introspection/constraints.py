#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


async def fetch(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None,
        constraint_pattern: str=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
            (CASE WHEN cd.typname IS NOT NULL THEN
                ARRAY[cdns.nspname, cd.typname]
             ELSE NULL END)                         AS domain_name,

            (CASE WHEN cc.relname IS NOT NULL THEN
                ARRAY[ccns.nspname, cc.relname]
             ELSE NULL END)                         AS table_name,

            c.oid::int                              AS constraint_id,
            c.contype::text                         AS constraint_type,
            c.conname::text                         AS constraint_name,

            -- Unique constraints have their expression stored
            -- in index catalog.
            -- Check constraints have their expression stored
            -- in constraint catalog.
            coalesce(
                pg_get_expr(c.conbin, c.conrelid)::text,
                pg_get_expr(ind.indexprs, ind.indrelid)::text
            )                                       AS constraint_expression,

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
            END)                                    AS constraint_proc,

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
            END)                                    AS constraint_description,

            (SELECT
                array_agg(ia.attname::text ORDER BY ia.attnum)
             FROM
                pg_attribute ia
             WHERE
                ia.attrelid = c.conindid
            )                                       AS constraint_columns

         FROM
            pg_constraint AS c
            INNER JOIN pg_namespace AS ns on c.connamespace = ns.oid

            LEFT JOIN pg_type AS cd ON c.contypid = cd.oid
            LEFT JOIN pg_namespace AS cdns ON cd.typnamespace = cdns.oid

            LEFT JOIN pg_class AS cc ON c.conrelid = cc.oid
            LEFT JOIN pg_namespace AS ccns ON cc.relnamespace = ccns.oid

            LEFT JOIN pg_index AS ind ON ind.indexrelid = c.conindid

         WHERE
            ($1::text IS NULL OR ns.nspname LIKE $1::text)
            AND ($2::text IS NULL OR c.conname LIKE $2::text)
    """, schema_pattern, constraint_pattern)
