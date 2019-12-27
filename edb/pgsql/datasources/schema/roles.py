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
from typing import *  # NoQA


async def fetch(
        conn: asyncpg.connection.Connection) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                ((d.description)->>'id')::uuid AS id,
                a.rolname AS name,
                a.rolsuper AS is_superuser,
                a.rolcanlogin AS allow_login,
                (d.description)->>'password_hash' AS password,
                (
                    SELECT
                        array_agg(
                            ((md.description)->>'id')::uuid
                        ) FILTER (WHERE (md.description)->>'id' IS NOT NULL)
                    FROM
                        pg_auth_members m
                        INNER JOIN pg_roles ma ON m.roleid = ma.oid
                        CROSS JOIN LATERAL (
                            SELECT
                                edgedb.shobj_metadata(ma.oid, 'pg_authid')
                                    AS description
                        ) AS md
                    WHERE m.member = a.oid
                 )
                              AS bases
            FROM
                pg_roles AS a
                CROSS JOIN LATERAL (
                    SELECT
                        edgedb.shobj_metadata(a.oid, 'pg_authid')
                            AS description
                ) AS d
            WHERE
                (d.description)->>'id' IS NOT NULL
    """)
