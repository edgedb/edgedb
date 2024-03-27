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
    Callable,
    Dict,
    Optional,
)

from edb.pgsql import params as pg_params

class BackendError(Exception):

    def get_field(self, field: str) -> str | None:
        ...

    def code_is(self, code: str) -> bool:
        ...

class BackendConnectionError(BackendError):
    ...

class BackendPrivilegeError(BackendError):
    ...

class BackendCatalogNameError(BackendError):
    ...

async def connect(
    connargs: Dict[str, Any],
    dbname: str,
    backend_params: pg_params.BackendRuntimeParams,
    apply_init_script: bool = True,
) -> PGConnection:
    ...

def set_init_con_script_data(cfg: list[dict[str, Any]]):
    ...

class PGConnection:

    idle: bool
    backend_pid: int

    async def sql_execute(self, sql: bytes | tuple[bytes, ...]) -> None:
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

    def terminate(self) -> None:
        ...

    def add_log_listener(self, cb: Callable[[str, str], None]) -> None:
        ...

    def get_server_parameter_status(self, parameter: str) -> Optional[str]:
        ...

    def set_stmt_cache_size(self, size: int) -> None:
        ...

    def set_server(self, server: object) -> None:
        ...

    async def signal_sysevent(self, event: str, *, dbname: str) -> None:
        ...

    def abort(self) -> None:
        ...

    def is_healthy(self) -> bool:
        ...

    async def listen_for_sysevent(self) -> None:
        ...

    def mark_as_system_db(self) -> None:
        ...

    def set_tenant(self, tenant: Any) -> None:
        ...

    def is_cancelling(self) -> bool:
        ...

    def start_pg_cancellation(self) -> None:
        ...

    def finish_pg_cancellation(self) -> None:
        ...

SETUP_TEMP_TABLE_SCRIPT: str
SETUP_CONFIG_CACHE_SCRIPT: str
