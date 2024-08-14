#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Any,
    TYPE_CHECKING,
)


from edb.pgsql import common as pg_common

if TYPE_CHECKING:
    from edb.pgsql import metaschema


async def get_instdata(
    backend_conn: metaschema.PGConnection,
    key: str,
    field: str,
) -> bytes | Any:
    if field == 'json':
        field = 'json::json'

    schema = pg_common.versioned_schema('edgedbinstdata')
    return await backend_conn.sql_fetch_val(
        f"""
        SELECT {field} FROM {schema}.instdata
        WHERE key = $1
        """.encode('utf-8'),
        args=[key.encode("utf-8")],
    )
