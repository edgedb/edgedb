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
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                l.id,
                edgedb._resolve_type_name(l.source) AS source,
                edgedb._resolve_type(l.target) AS target,
                edgedb._resolve_type_name(l.spectargets) AS spectargets,
                l.name AS name,
                edgedb._resolve_type_name(l.bases) AS bases,
                l.cardinality,
                l.required,
                l.computable,
                l.title,
                l.description,
                l.is_abstract,
                l.is_final,
                l.readonly,
                l.default
            FROM
                edgedb.link l
            ORDER BY
                l.name LIKE 'std::%' DESC, l.id, l.target NULLS FIRST
    """)


async def fetch_properties(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id                    AS id,
                p.name                  AS name,
                edgedb._resolve_type_name(p.bases)
                                        AS bases,
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
                                        AS target
            FROM
                edgedb.Property p
            ORDER BY
                p.id, p.target NULLS FIRST
    """)
