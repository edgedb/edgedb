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
    Awaitable,
    Callable,
    Hashable,
    Iterator,
    Mapping,
    Optional,
    TypeAlias,
)

import uuid

import immutables

from edb.schema import schema as s_schema

from edb.server import config
from edb.server import pgcon
from edb.server import server
from edb.server import tenant
from edb.server.compiler import dbstate
from edb.server.compiler import sertypes

Config: TypeAlias = Mapping[str, config.SettingValue]

class CompiledQuery:
    query_unit_group: dbstate.QueryUnitGroup

class Database:
    name: str
    dbver: int
    db_config: Config
    extensions: set[str]
    user_config_spec: config.Spec

    @property
    def server(self) -> server.Server:
        ...

    @property
    def tenant(self) -> tenant.Tenant:
        ...

    def stop(self) -> None:
        ...

    async def monitor(
        self,
        worker: Callable[[], Awaitable[None]],
        name: str,
    ) -> None:
        ...

    async def cache_worker(self) -> None:
        ...

    async def cache_notifier(self) -> None:
        ...

    def start_stop_extensions(self) -> None:
        ...

    def cache_compiled_sql(
        self,
        key: Hashable,
        compiled: list[dbstate.SQLQueryUnit],
        schema_version: uuid.UUID,
    ) -> None:
        ...

    def lookup_compiled_sql(
        self,
        key: Hashable,
    ) -> Optional[list[dbstate.SQLQueryUnit]]:
        ...

    def set_state_serializer(
        self,
        protocol_version: tuple[int, int],
        serializer: sertypes.StateSerializer,
    ) -> None:
        pass

    def hydrate_cache(self, query_cache: list[tuple[bytes, ...]]) -> None:
        ...

    def clear_query_cache(self) -> None:
        ...

    def iter_views(self) -> Iterator[DatabaseConnectionView]:
        ...

    def get_query_cache_size(self) -> int:
        ...

    async def introspection(self) -> None:
        ...

    def lookup_config(self, name: str) -> Any:
        ...

    def is_introspected(self) -> bool:
        ...

class DatabaseConnectionView:
    def in_tx(self) -> bool:
        ...

    def in_tx_error(self) -> bool:
        ...

    def get_session_config(self) -> Config:
        ...

    def get_modaliases(self) -> Mapping[str | None, str]:
        ...

class DatabaseIndex:
    def __init__(
        self,
        tenant: tenant.Tenant,
        *,
        std_schema: s_schema.Schema,
        global_schema_pickle: bytes,
        sys_config: Config,
        default_sysconfig: Config,
        sys_config_spec: config.Spec,
    ) -> None:
        ...

    def count_connections(self, dbname: str) -> int:
        ...

    def get_sys_config(self) -> Config:
        ...

    def get_compilation_system_config(self) -> Config:
        ...

    def update_sys_config(self, sys_config: Config) -> None:
        ...

    def has_db(self, dbname: str) -> bool:
        ...

    def get_db(self, dbname) -> Database:
        ...

    def maybe_get_db(self, dbname) -> Optional[Database]:
        ...

    def get_global_schema_pickle(self) -> bytes:
        ...

    def update_global_schema(self, global_schema_pickle: bytes) -> None:
        ...

    def register_db(
        self,
        dbname: str,
        *,
        user_schema_pickle: Optional[bytes],
        schema_version: Optional[uuid.UUID],
        db_config: Optional[Config],
        reflection_cache: Optional[Mapping[str, tuple[str, ...]]],
        backend_ids: Optional[Mapping[str, tuple[int, str]]],
        extensions: Optional[set[str]],
        ext_config_settings: Optional[list[config.Setting]],
        early: bool = False,
        feature_used_metrics: Optional[Mapping[str, float]] = ...,
    ) -> Database:
        ...

    def unregister_db(self, dbname: str) -> None:
        ...

    def iter_dbs(self) -> Iterator[Database]:
        ...

    async def apply_system_config_op(
        self,
        conn: pgcon.PGConnection,
        op: config.Operation,
    ) -> None:
        ...

    def new_view(
        self,
        dbname: str,
        *,
        query_cache: bool,
        protocol_version: tuple[int, int],
    ) -> DatabaseConnectionView:
        ...

    def remove_view(
        self,
        view: DatabaseConnectionView,
    ) -> None:
        ...

    def invalidate_caches(self) -> None:
        ...

    def get_cached_compiler_args(
        self,
    ) -> tuple[immutables.Map, bytes, Config]:
        ...

    def lookup_config(self, name: str) -> Any:
        ...
