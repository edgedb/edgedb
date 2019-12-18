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
from typing import *  # NoQA


async def fetch_tuple_views(
        conn: asyncpg.connection.Connection,
        *,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
            c.id AS id,
            c.name AS name,
            c.expr_type AS expr_type,
            c.alias_is_persistent AS alias_is_persistent,
            c.expr AS expr,
            c.named AS named,
            c.element_types AS element_types
        FROM
            edgedb.TupleExprAlias c
        WHERE
            ($1::text[] IS NULL
                OR split_part(c.name, '::', 1) = any($1::text[]))
            AND ($2::text[] IS NULL
                    OR split_part(c.name, '::', 1) != all($2::text[]))
            AND expr_type IS NOT NULL
        ORDER BY
            c.id
    """, modules, exclude_modules)


async def fetch_array_views(
        conn: asyncpg.connection.Connection,
        *,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
            c.id AS id,
            c.name AS name,
            c.expr_type AS expr_type,
            c.alias_is_persistent AS alias_is_persistent,
            c.expr AS expr,
            c.element_type AS element_type,
            c.dimensions AS dimensions
        FROM
            edgedb.ArrayExprAlias c
        WHERE
            ($1::text[] IS NULL
                OR split_part(c.name, '::', 1) = any($1::text[]))
            AND ($2::text[] IS NULL
                    OR split_part(c.name, '::', 1) != all($2::text[]))
            AND expr_type IS NOT NULL
        ORDER BY
            c.id
    """, modules, exclude_modules)
