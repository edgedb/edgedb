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
                o.id AS id,
                o.name AS name,
                o.title AS title,
                o.description AS description,
                o.return_typemod,
                o.language,
                o.from_operator,
                o.operator_kind,
                edgedb._resolve_type(o.return_type) AS return_type,
                edgedb._resolve_type_name(o.commutator) AS commutator,

                (SELECT array_agg(
                    (p.pos,
                     p.name,
                     p.default,
                     edgedb._resolve_type(p.type),
                     p.typemod,
                     p.kind))

                    FROM
                        unnest(o.params) AS p
                ) AS params

            FROM
                edgedb.operator o
            ORDER BY
                o.id
    """)
