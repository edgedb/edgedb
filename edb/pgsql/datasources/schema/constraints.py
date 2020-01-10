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


from __future__ import annotations

import asyncpg
from typing import *  # NoQA


async def fetch(
        conn: asyncpg.connection.Connection,
        *,
        name: str=None,
        modules=None,
        exclude_modules=None) -> List[asyncpg.Record]:
    return await conn.fetch("""
        SELECT
                a.id                    AS id,
                a.name                  AS name,
                a.is_abstract           AS is_abstract,
                a.is_final              AS is_final,
                a.is_local              AS is_local,
                a.delegated             AS delegated,
                edgedb._resolve_type_name(a.bases)
                                        AS bases,
                edgedb._resolve_type_name(a.ancestors)
                                        AS ancestors,
                a.expr                  AS expr,
                a.subjectexpr           AS subjectexpr,
                a.finalexpr             AS finalexpr,
                a.errmessage            AS errmessage,
                a.args                  AS args,
                edgedb._resolve_type_name(a.subject)
                                        AS subject,
                edgedb._resolve_type_name(a.params)
                                        AS params,
                a.return_type           AS return_type,
                a.return_typemod        AS return_typemod,
                a.inherited_fields           AS inherited_fields

            FROM
                edgedb.constraint a
            WHERE
                ($1::text IS NULL OR a.name LIKE $1::text)
                AND ($2::text[] IS NULL
                     OR split_part(a.name, '::', 1) = any($2::text[]))
                AND ($3::text[] IS NULL
                     OR split_part(a.name, '::', 1) != all($3::text[]))
    """, name, modules, exclude_modules)
