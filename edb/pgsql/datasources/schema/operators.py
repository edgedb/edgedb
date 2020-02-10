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
                o.id AS id,
                o.name AS name,
                o.is_abstract AS is_abstract,
                o.return_typemod,
                o.language,
                o.from_operator,
                o.from_function,
                o.from_expr,
                o.force_return_cast,
                o.code,
                o.recursive,
                o.operator_kind,
                o.volatility,
                o.return_type,
                o.commutator,
                o.negator,
                edgedb._resolve_type_name(o.params) AS params
            FROM
                edgedb.operator o
            WHERE
                ($1::text[] IS NULL
                 OR split_part(o.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(o.name, '::', 1) != all($2::text[]))
            ORDER BY
                o.id
    """, modules, exclude_modules)
