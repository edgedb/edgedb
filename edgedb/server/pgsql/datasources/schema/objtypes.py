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
                c.id AS id,
                c.name AS name,
                c.title AS title,
                c.description AS description,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final,
                c.view_type AS view_type,
                c.expr AS expr
            FROM
                edgedb.ObjectType c
    """)


async def fetch_derived(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                c.id AS id,
                c.name AS name,
                edgedb._resolve_type_name(c.bases) AS bases,
                c.title AS title,
                c.description AS description,
                c.is_abstract AS is_abstract,
                c.is_final AS is_final,
                c.view_type AS view_type,
                c.expr AS expr
            FROM
                edgedb.DerivedObjectType c
    """)
