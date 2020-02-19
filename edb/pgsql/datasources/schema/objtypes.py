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
                c.id AS id,
                c.name AS name,
                edgedb._resolve_type_name(c.bases) AS bases,
                edgedb._resolve_type_name(c.ancestors) AS ancestors,
                edgedb._resolve_type_name(c.union_of) AS union_of,
                edgedb._resolve_type_name(c.intersection_of)
                    AS intersection_of,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final,
                c.expr_type AS expr_type,
                c.alias_is_persistent AS alias_is_persistent,
                c.expr AS expr,
                c.inherited_fields AS inherited_fields

            FROM
                edgedb.ObjectType c
            WHERE
                ($1::text[] IS NULL
                 OR split_part(c.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(c.name, '::', 1) != all($2::text[]))
    """, modules, exclude_modules)
