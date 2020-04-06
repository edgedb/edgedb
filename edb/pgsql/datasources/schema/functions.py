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


async def fetch(
        conn: asyncpg.connection.Connection,
        *,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                f.id AS id,
                f.name AS name,
                f.return_typemod,
                f.language,
                f.code,
                f.nativecode,
                f.from_function,
                f.from_expr,
                f.force_return_cast,
                f.sql_func_has_out_params,
                f.error_on_null_result,
                f.initial_value,
                f.session_only,
                f.volatility,
                f.return_type AS return_type,
                edgedb._resolve_type_name(f.params) AS params
            FROM
                edgedb.function f
            WHERE
                ($1::text[] IS NULL
                 OR split_part(f.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(f.name, '::', 1) != all($2::text[]))
            ORDER BY
                f.id
    """, modules, exclude_modules)


async def fetch_params(
        conn: asyncpg.connection.Connection, *,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id,
                p.name,
                p.num,
                p.default,
                p.type AS type,
                p.typemod,
                p.kind
            FROM
                edgedb.Parameter p
            WHERE
                ($1::text[] IS NULL
                 OR split_part(p.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(p.name, '::', 1) != all($2::text[]))
            ORDER BY
                p.id
    """, modules, exclude_modules)
