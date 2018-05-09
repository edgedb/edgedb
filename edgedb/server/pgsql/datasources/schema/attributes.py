#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2013-present MagicStack Inc. and the EdgeDB authors.
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
        conn: asyncpg.connection.Connection,
        *, name: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                a.id                    AS id,
                a.name                  AS name,
                a.title                 AS title,
                a.description           AS description,
                edgedb._resolve_type(a.type)
                                        AS type
            FROM
                edgedb.attribute a
            WHERE
                $1::text IS NULL OR a.name LIKE $1::text
    """, name)


async def fetch_values(
        conn: asyncpg.connection.Connection,
        *, name: str=None) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                a.id                        AS id,
                a.name                      AS name,
                edgedb._resolve_type_name(a.subject)
                                            AS subject_name,
                edgedb._resolve_type_name(a.attribute)
                                            AS attribute_name,
                a.value                     AS value
            FROM
                edgedb.AttributeValue a
            WHERE
                $1::text IS NULL OR a.name LIKE $1::text
    """, name)
