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
                f.id AS id,
                f.name AS name,
                f.title AS title,
                f.description AS description,
                edgedb._resolve_type(f.paramtypes) AS paramtypes,
                f.paramnames,
                f.varparam,
                f.paramdefaults,
                f.paramkinds,
                f.aggregate,
                f.set_returning,
                f.language,
                f.code,
                f.from_function,
                f.initial_value,
                edgedb._resolve_type(f.returntype) AS returntype
            FROM
                edgedb.function f
            ORDER BY
                f.id
    """)
