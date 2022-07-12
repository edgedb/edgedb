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

from typing import (
    Any,
    Dict,
    Optional,
)


from edb.pgsql import params as pg_params


async def connect(
    connargs: Dict[str, Any],
    dbname: str,
    backend_params: pg_params.BackendRuntimeParams,
):
    ...


class PGConnection:

    async def sql_execute(
        self,
        sql: bytes | tuple[bytes, ...],
        state: Optional[bytes] = None,
    ) -> None:
        ...

    async def sql_fetch(
        self,
        sql: bytes | tuple[bytes, ...],
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> list[tuple[bytes, ...]]:
        ...

    async def sql_fetch_val(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> bytes:
        ...

    async def sql_fetch_col(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
        use_prep_stmt: bool = False,
        state: Optional[bytes] = None,
    ) -> list[bytes]:
        ...
