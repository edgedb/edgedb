#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-2012 MagicStack Inc. and the EdgeDB authors.
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
        schema_pattern: str=None, type_pattern: str=None,
        include_arrays: bool=True) -> List[asyncpg.Record]:

    qry = """
        SELECT
                tp.oid                                AS oid,
                tp.typrelid                           AS typrelid,
                tp.typname                            AS name,
                ns.nspname                            AS schema,
                cmt.description                       AS comment
            FROM
                pg_type AS tp
                INNER JOIN pg_namespace AS ns
                    ON ns.oid = tp.typnamespace
                LEFT JOIN pg_description AS cmt
                    ON (cmt.objoid = tp.oid AND cmt.objsubid = 0)
            WHERE
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                ($2::text IS NULL OR tp.typname LIKE $2::text) AND
                ($3::bool OR tp.typcategory != 'A')
    """

    return await conn.fetch(qry, schema_pattern, type_pattern, include_arrays)


async def fetch_attributes(
        conn: asyncpg.connection.Connection, *,
        schema_pattern: str=None,
        type_pattern: str=None) -> List[asyncpg.Record]:

    qry = """
        SELECT
                ns.nspname                      AS type_schema,
                ct.typname                      AS type_name,
                a.attname                       AS attribute_name,
                t.typname                       AS attribute_type,
                format_type(t.oid, a.atttypmod) AS attribute_type_formatted,
                tns.nspname                     AS attribute_type_schema,
                coalesce(elemt.typrelid, t.typrelid)
                                                AS attribute_type_composite_id,
                t.typcategory = 'A'             AS attribute_type_is_array,
                a.attnotnull                    AS attribute_required,
                pg_get_expr(def.adbin, c.oid, true)
                                                AS attribute_default,
                a.attislocal                    AS attribute_is_local,
                a.attinhcount                   AS attribute_ancestors,
                cmt.description                 AS attribute_comment
            FROM
                pg_type ct
                INNER JOIN pg_namespace ns ON ct.typnamespace = ns.oid
                INNER JOIN pg_class c ON ct.typrelid = c.oid
                INNER JOIN pg_attribute a ON c.oid = a.attrelid
                INNER JOIN pg_type t ON a.atttypid = t.oid
                INNER JOIN pg_namespace tns ON t.typnamespace = tns.oid
                LEFT JOIN pg_attrdef def
                    ON def.adrelid = c.oid AND def.adnum = a.attnum
                LEFT JOIN pg_description cmt
                    ON (cmt.objoid = c.oid AND cmt.objsubid = a.attnum)
                LEFT JOIN pg_type elemt ON t.typelem = elemt.oid
            WHERE
                ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                ($2::text IS NULL OR ct.typname LIKE $2::text) AND
                a.attnum > 0
            ORDER BY
                a.attnum
    """

    return await conn.fetch(qry, schema_pattern, type_pattern)
