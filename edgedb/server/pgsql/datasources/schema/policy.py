#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


async def fetch_actions(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                a.id           AS id,
                a.name         AS name,
                a.title        AS title,
                a.description  AS description
            FROM
                edgedb.action a
    """)


async def fetch_events(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                e.id           AS id,
                e.name         AS name,
                (SELECT array_agg(name) FROM edgedb.NamedObject
                 WHERE id = any(e.bases)) AS bases,
                e.title        AS title,
                e.description  AS description
            FROM
                edgedb.event e
    """)


async def fetch_policies(
        conn: asyncpg.connection.Connection) -> typing.List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                p.id                        AS id,
                p.name                      AS name,
                p.title                     AS title,
                p.description               AS description,
                edgedb._resolve_type_name(p.subject)
                                            AS subject,
                edgedb._resolve_type_name(p.event)
                                            AS event,
                edgedb._resolve_type_name(p.actions)
                                            AS actions
            FROM
                edgedb.policy p
    """)
