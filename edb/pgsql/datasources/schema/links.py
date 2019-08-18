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
import typing


async def fetch(
        conn: asyncpg.connection.Connection,
        *,
        modules=None,
        exclude_modules=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                l.id,
                edgedb._resolve_type_name(l.source) AS source,
                l.target AS target,
                l.name AS name,
                edgedb._resolve_type_name(l.bases) AS bases,
                edgedb._resolve_type_name(l.ancestors) AS ancestors,
                edgedb._resolve_type_name(l.derived_from) AS derived_from,
                l.cardinality,
                l.required,
                l.expr,
                l.is_abstract,
                l.is_final,
                l.is_local,
                l.is_derived,
                l.readonly,
                l.default,
                l.field_inh_map
            FROM
                edgedb.link l
            WHERE
                ($1::text[] IS NULL
                 OR split_part(l.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(l.name, '::', 1) != all($2::text[]))
            ORDER BY
                l.name LIKE 'std::%' DESC, l.id, l.target NULLS FIRST
    """, modules, exclude_modules)


async def fetch_properties(
        conn: asyncpg.connection.Connection,
        *,
        modules=None,
        exclude_modules=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id                    AS id,
                p.name                  AS name,
                edgedb._resolve_type_name(p.bases)
                                        AS bases,
                edgedb._resolve_type_name(p.ancestors)
                                        AS ancestors,
                edgedb._resolve_type_name(p.derived_from)
                                        AS derived_from,
                p.cardinality           AS cardinality,
                p.required              AS required,
                p.expr                  AS expr,
                p.readonly              AS readonly,
                p.default               AS default,
                edgedb._resolve_type_name(p.source)
                                        AS source,
                p.target                AS target,
                p.is_abstract           AS is_abstract,
                p.is_final              AS is_final,
                p.is_local              AS is_local,
                p.is_derived            AS is_derived,
                p.field_inh_map         AS field_inh_map
            FROM
                edgedb.Property p
            WHERE
                ($1::text[] IS NULL
                 OR split_part(p.name, '::', 1) = any($1::text[]))
                AND ($2::text[] IS NULL
                     OR split_part(p.name, '::', 1) != all($2::text[]))
            ORDER BY
                p.id, p.target NULLS FIRST
    """, modules, exclude_modules)
