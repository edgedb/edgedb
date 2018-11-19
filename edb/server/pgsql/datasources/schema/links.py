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


import asyncpg
import typing


async def fetch(
        conn: asyncpg.connection.Connection, *,
        modules=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                l.id,
                edgedb._resolve_type_name(l.source) AS source,
                edgedb._resolve_type(l.target) AS target,
                edgedb._resolve_type_name(l.spectargets) AS spectargets,
                l.name AS name,
                edgedb._resolve_type_name(l.bases) AS bases,
                edgedb._resolve_type_name(l.derived_from) AS derived_from,
                l.cardinality,
                l.required,
                l.computable,
                l.title,
                l.description,
                l.is_abstract,
                l.is_final,
                l.is_derived,
                l.readonly,
                l.default
            FROM
                edgedb.link l
            WHERE
                $1::text[] IS NULL
                OR split_part(l.name, '::', 1) = any($1::text[])
            ORDER BY
                l.name LIKE 'std::%' DESC, l.id, l.target NULLS FIRST
    """, modules)


async def fetch_properties(
        conn: asyncpg.connection.Connection, *,
        modules=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id                    AS id,
                p.name                  AS name,
                edgedb._resolve_type_name(p.bases)
                                        AS bases,
                edgedb._resolve_type_name(p.derived_from)
                                        AS derived_from,
                p.cardinality           AS cardinality,
                p.title                 AS title,
                p.description           AS description,
                p.required              AS required,
                p.computable            AS computable,
                p.readonly              AS readonly,
                p.default               AS default,
                edgedb._resolve_type_name(p.source)
                                        AS source,
                edgedb._resolve_type(p.target)
                                        AS target,

                p.is_abstract           AS is_abstract,
                p.is_final              AS is_final,
                p.is_derived            AS is_derived
            FROM
                edgedb.Property p
            WHERE
                $1::text[] IS NULL
                OR split_part(p.name, '::', 1) = any($1::text[])
            ORDER BY
                p.id, p.target NULLS FIRST
    """, modules)
