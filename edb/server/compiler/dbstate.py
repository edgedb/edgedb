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
from typing import *  # NoQA

import dataclasses
import enum
import time
import uuid

import immutables

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import delta as s_delta
from edb.schema import migrations as s_migrations
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema

from edb.server import config
from edb.server import defines

from edb.pgsql import codegen as pgcodegen

from . import enums
from . import sertypes


class TxAction(enum.IntEnum):

    START = 1
    COMMIT = 2
    ROLLBACK = 3

    DECLARE_SAVEPOINT = 4
    RELEASE_SAVEPOINT = 5
    ROLLBACK_TO_SAVEPOINT = 6


class MigrationAction(enum.IntEnum):

    START = 1
    POPULATE = 2
    DESCRIBE = 3
    ABORT = 4
    COMMIT = 5
    REJECT_PROPOSED = 6


@dataclasses.dataclass(frozen=True)
class BaseQuery:

    sql: Tuple[bytes, ...]

    @property
    def is_transactional(self) -> bool:
        return True


@dataclasses.dataclass(frozen=True)
class NullQuery(BaseQuery):

    sql: Tuple[bytes, ...] = tuple()
    is_transactional: bool = True
    has_dml: bool = False


@dataclasses.dataclass(frozen=True)
class Query(BaseQuery):

    sql_hash: bytes

    cardinality: enums.Cardinality

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes
    in_type_args: Optional[List[Param]] = None

    globals: Optional[list[tuple[str, bool]]] = None

    is_transactional: bool = True
    has_dml: bool = False
    single_unit: bool = False
    cacheable: bool = True
    is_explain: bool = False
    query_asts: Any = None
    append_rollback: bool = False


@dataclasses.dataclass(frozen=True)
class SimpleQuery(BaseQuery):

    sql: Tuple[bytes, ...]
    is_transactional: bool = True
    has_dml: bool = False
    single_unit: bool = False
    # XXX: Temporary hack, since SimpleQuery will die
    in_type_args: Optional[List[Param]] = None


@dataclasses.dataclass(frozen=True)
class SessionStateQuery(BaseQuery):

    config_scope: Optional[qltypes.ConfigScope] = None
    is_backend_setting: bool = False
    requires_restart: bool = False
    is_system_config: bool = False
    config_op: Optional[config.Operation] = None
    is_transactional: bool = True
    single_unit: bool = False
    globals: Optional[list[tuple[str, bool]]] = None

    in_type_data: Optional[bytes] = None
    in_type_id: Optional[bytes] = None
    in_type_args: Optional[List[Param]] = None


@dataclasses.dataclass(frozen=True)
class DDLQuery(BaseQuery):

    user_schema: s_schema.FlatSchema
    global_schema: Optional[s_schema.FlatSchema] = None
    cached_reflection: Any = None
    is_transactional: bool = True
    single_unit: bool = False
    create_db: Optional[str] = None
    drop_db: Optional[str] = None
    create_ext: Optional[str] = None
    drop_ext: Optional[str] = None
    create_db_template: Optional[str] = None
    has_role_ddl: bool = False
    ddl_stmt_id: Optional[str] = None
    config_ops: List[config.Operation] = (
        dataclasses.field(default_factory=list))


@dataclasses.dataclass(frozen=True)
class TxControlQuery(BaseQuery):

    action: TxAction
    cacheable: bool

    modaliases: Optional[immutables.Map[Optional[str], str]]
    is_transactional: bool = True
    single_unit: bool = False

    user_schema: Optional[s_schema.Schema] = None
    global_schema: Optional[s_schema.Schema] = None
    cached_reflection: Any = None

    sp_name: Optional[str] = None
    sp_id: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class MigrationControlQuery(BaseQuery):

    action: MigrationAction
    tx_action: Optional[TxAction]
    cacheable: bool

    modaliases: Optional[immutables.Map[Optional[str], str]]
    is_transactional: bool = True
    single_unit: bool = False

    user_schema: Optional[s_schema.FlatSchema] = None
    cached_reflection: Any = None
    ddl_stmt_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class MaintenanceQuery(BaseQuery):

    is_transactional: bool = True


@dataclasses.dataclass(frozen=True)
class Param:
    name: str
    required: bool
    array_type_id: Optional[uuid.UUID]
    outer_idx: Optional[int]
    sub_params: Optional[tuple[list[Optional[uuid.UUID]], tuple[Any, ...]]]


#############################


@dataclasses.dataclass
class QueryUnit:

    sql: Tuple[bytes, ...]

    # Status-line for the compiled command; returned to front-end
    # in a CommandComplete protocol message if the command is
    # executed successfully.  When a QueryUnit contains multiple
    # EdgeQL queries, the status reflects the last query in the unit.
    status: bytes

    # Output format of this query unit
    output_format: enums.OutputFormat = enums.OutputFormat.NONE

    # Set only for units that contain queries that can be cached
    # as prepared statements in Postgres.
    sql_hash: bytes = b''

    # True if all statments in *sql* can be executed inside a transaction.
    # If False, they will be executed separately.
    is_transactional: bool = True

    # Capabilities used in this query
    capabilities: enums.Capability = enums.Capability(0)

    # True if this unit contains SET commands.
    has_set: bool = False

    # True if this unit contains ALTER/DROP/CREATE ROLE commands.
    has_role_ddl: bool = False

    # If tx_id is set, it means that the unit
    # starts a new transaction.
    tx_id: Optional[int] = None

    # True if this unit is single 'COMMIT' command.
    # 'COMMIT' is always compiled to a separate QueryUnit.
    tx_commit: bool = False

    # True if this unit is single 'ROLLBACK' command.
    # 'ROLLBACK' is always compiled to a separate QueryUnit.
    tx_rollback: bool = False

    # True if this unit is single 'ROLLBACK TO SAVEPOINT' command.
    # 'ROLLBACK TO SAVEPOINT' is always compiled to a separate QueryUnit.
    tx_savepoint_rollback: bool = False
    tx_savepoint_declare: bool = False

    # True if this unit is `ABORT MIGRATION` command within a transaction,
    # that means abort_migration and tx_rollback cannot be both True
    tx_abort_migration: bool = False

    # For SAVEPOINT commands, the name and sp_id
    sp_name: Optional[str] = None
    sp_id: Optional[int] = None

    # True if it is safe to cache this unit.
    cacheable: bool = False

    # If non-None, contains a name of the DB that is about to be
    # created/deleted. If it's the former, the IO process needs to
    # introspect the new db. If it's the later, the server should
    # close all inactive unused pooled connections to it.
    create_db: Optional[str] = None
    drop_db: Optional[str] = None

    # If non-None, contains a name of the DB that will be used as
    # a template database to create the database. The server should
    # close all inactive unused pooled connections to the template db.
    create_db_template: Optional[str] = None

    # If non-None, contains name of created/deleted extension.
    create_ext: Optional[str] = None
    drop_ext: Optional[str] = None

    # If non-None, the DDL statement will emit data packets marked
    # with the indicated ID.
    ddl_stmt_id: Optional[str] = None

    # Cardinality of the result set.  Set to NO_RESULT if the
    # unit represents multiple queries compiled as one script.
    cardinality: enums.Cardinality = \
        enums.Cardinality.NO_RESULT

    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_data: bytes = sertypes.NULL_TYPE_DESC
    in_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_args: Optional[List[Param]] = None
    in_type_args_real_count: int = 0
    globals: Optional[list[tuple[str, bool]]] = None

    # Set only when this unit contains a CONFIGURE INSTANCE command.
    system_config: bool = False
    # Set only when this unit contains a CONFIGURE DATABASE command.
    database_config: bool = False
    # Set only when this unit contains a SET_GLOBAL command.
    set_global: bool = False
    # Whether any configuration change requires a server restart
    config_requires_restart: bool = False
    # Set only when this unit contains a CONFIGURE command which
    # alters a backend configuration setting.
    backend_config: bool = False
    # Set only when this unit contains a CONFIGURE command which
    # alters a system configuration setting.
    is_system_config: bool = False
    config_ops: List[config.Operation] = (
        dataclasses.field(default_factory=list))
    modaliases: Optional[immutables.Map[Optional[str], str]] = None

    # If present, represents the future schema state after
    # the command is run. The schema is pickled.
    user_schema: Optional[bytes] = None
    cached_reflection: Optional[bytes] = None

    # If present, represents the future global schema state
    # after the command is run. The schema is pickled.
    global_schema: Optional[bytes] = None

    is_explain: bool = False
    query_asts: Any = None
    append_rollback: bool = False

    @property
    def has_ddl(self) -> bool:
        return bool(self.capabilities & enums.Capability.DDL)

    @property
    def tx_control(self) -> bool:
        return (
            bool(self.tx_id)
            or self.tx_rollback
            or self.tx_commit
            or self.tx_savepoint_declare
            or self.tx_savepoint_rollback
        )


@dataclasses.dataclass
class QueryUnitGroup:

    # All capabilities used by any query units in this group
    capabilities: enums.Capability = enums.Capability(0)

    # True if it is safe to cache this unit.
    cacheable: bool = True

    # True if any query unit has transaction control commands, like COMMIT,
    # ROLLBACK, START TRANSACTION or SAVEPOINT-related commands
    tx_control: bool = False

    # Cardinality of the result set.  Set to NO_RESULT if the
    # unit group is not expected or desired to return data.
    cardinality: enums.Cardinality = enums.Cardinality.NO_RESULT

    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_data: bytes = sertypes.NULL_TYPE_DESC
    in_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_args: Optional[List[Param]] = None
    in_type_args_real_count: int = 0
    globals: Optional[list[tuple[str, bool]]] = None

    units: List[QueryUnit] = dataclasses.field(default_factory=list)

    def __iter__(self) -> Iterator[QueryUnit]:
        return iter(self.units)

    def __len__(self) -> int:
        return len(self.units)

    def __getitem__(self, item: int) -> QueryUnit:
        return self.units[item]

    def append(self, query_unit: QueryUnit) -> None:
        self.capabilities |= query_unit.capabilities

        if not query_unit.cacheable:
            self.cacheable = False

        if query_unit.tx_control:
            self.tx_control = True

        self.cardinality = query_unit.cardinality
        self.out_type_data = query_unit.out_type_data
        self.out_type_id = query_unit.out_type_id
        self.in_type_data = query_unit.in_type_data
        self.in_type_id = query_unit.in_type_id
        self.in_type_args = query_unit.in_type_args
        self.in_type_args_real_count = query_unit.in_type_args_real_count
        if query_unit.globals is not None:
            if self.globals is None:
                self.globals = []
            self.globals.extend(query_unit.globals)

        self.units.append(query_unit)


@dataclasses.dataclass(frozen=True, kw_only=True)
class PreparedStmtOpData:
    """Common prepared statement metadata"""

    stmt_name: str
    """Original statement name as passed by the frontend"""

    be_stmt_name: bytes = b""
    """Computed statement name as passed to the backend"""


@dataclasses.dataclass(frozen=True, kw_only=True)
class PrepareData(PreparedStmtOpData):
    """PREPARE statement data"""

    query: str
    """Translated query string"""
    translation_data: Optional[pgcodegen.TranslationData] = None
    """Translation source map"""


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExecuteData(PreparedStmtOpData):
    """EXECUTE statement data"""
    pass


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeallocateData(PreparedStmtOpData):
    """DEALLOCATE statement data"""
    pass


@dataclasses.dataclass(kw_only=True)
class SQLQueryUnit:
    query: str
    """Translated query text."""
    orig_query: str
    """Original query text before translation."""
    translation_data: Optional[pgcodegen.TranslationData] = None
    """Translation source map."""
    fe_settings: SQLSettings
    """Frontend-only settings effective during translation of this unit."""

    tx_action: Optional[TxAction] = None
    tx_chain: bool = False
    sp_name: Optional[str] = None

    prepare: Optional[PrepareData] = None
    execute: Optional[ExecuteData] = None
    deallocate: Optional[DeallocateData] = None

    set_vars: Optional[dict[Optional[str], Optional[str | list[str]]]] = None
    get_var: Optional[str] = None
    is_local: bool = False

    stmt_name: bytes = b""
    """Computed prepared statement name for this query."""

    frontend_only: bool = False
    """Whether the query is completely emulated outside of backend and so
    the response should be synthesized also."""

    command_tag: bytes = b""
    """If frontend_only is True, only issue CommandComplete with this tag."""


SQLSettings = immutables.Map[Optional[str], Optional[str | list[str]]]
DEFAULT_SQL_SETTINGS: SQLSettings = immutables.Map()
DEFAULT_SQL_FE_SETTINGS: SQLSettings = immutables.Map({
    "search_path": "public",
    "server_version": defines.PGEXT_POSTGRES_VERSION,
    "server_version_num": str(defines.PGEXT_POSTGRES_VERSION_NUM),
})


@dataclasses.dataclass
class SQLTransactionState:
    in_tx: bool
    settings: SQLSettings
    in_tx_settings: Optional[SQLSettings]
    in_tx_local_settings: Optional[SQLSettings]
    savepoints: list[tuple[str, SQLSettings, SQLSettings]]

    def current_fe_settings(self) -> SQLSettings:
        if self.in_tx:
            return self.in_tx_settings or DEFAULT_SQL_FE_SETTINGS
        else:
            return self.in_tx_local_settings or DEFAULT_SQL_FE_SETTINGS

    def get(self, name: str) -> Optional[str | list[str]]:
        if self.in_tx:
            assert self.in_tx_local_settings
            return self.in_tx_local_settings[name]
        else:
            return self.settings[name]

    def apply(self, query_unit: SQLQueryUnit) -> None:
        if query_unit.tx_action == TxAction.COMMIT:
            self.in_tx = False
            self.settings = self.in_tx_settings  # type: ignore
            self.in_tx_settings = None
            self.in_tx_local_settings = None
            self.savepoints.clear()
        elif query_unit.tx_action == TxAction.ROLLBACK:
            self.in_tx = False
            self.in_tx_settings = None
            self.in_tx_local_settings = None
            self.savepoints.clear()
        elif query_unit.tx_action == TxAction.DECLARE_SAVEPOINT:
            self.savepoints.append((
                query_unit.sp_name,
                self.in_tx_settings,
                self.in_tx_local_settings,
            ))  # type: ignore
        elif query_unit.tx_action == TxAction.ROLLBACK_TO_SAVEPOINT:
            while self.savepoints:
                sp_name, settings, local_settings = self.savepoints[-1]
                if query_unit.sp_name == sp_name:
                    self.in_tx_settings = settings
                    self.in_tx_local_settings = local_settings
                    break
                else:
                    self.savepoints.pop(0)
            else:
                raise errors.TransactionError(
                    f'savepoint "{query_unit.sp_name}" does not exist'
                )
        if not self.in_tx:
            # Always start an implicit transaction here, because in the
            # compiler, multiple apply() calls only happen in simple query,
            # and any query would start an implicit transaction. For example,
            # we need to support a single ROLLBACK without a matching BEGIN
            # rolling back an implicit transaction.
            self.in_tx = True
            self.in_tx_settings = self.settings
            self.in_tx_local_settings = self.settings
        if query_unit.frontend_only and query_unit.set_vars:
            for name, value in query_unit.set_vars.items():
                self.set(name, value, query_unit.is_local)

    def set(
        self, name: Optional[str], value: str | list[str] | None,
        is_local: bool
    ) -> None:
        def _set(attr_name: str) -> None:
            settings = getattr(self, attr_name)
            if value is None:
                if name in settings:
                    settings = settings.delete(name)
            else:
                settings = settings.set(name, value)
            setattr(self, attr_name, settings)

        if self.in_tx:
            _set("in_tx_local_settings")
            if not is_local:
                _set("in_tx_settings")
        elif not is_local:
            _set("settings")


#############################


class ProposedMigrationStep(NamedTuple):

    statements: Tuple[str, ...]
    confidence: float
    prompt: str
    prompt_id: str
    data_safe: bool
    required_user_input: tuple[dict[str, str], ...]
    # This isn't part of the output data, but is used to figure out
    # what to prohibit when something is rejected.
    operation_key: s_delta.CommandKey

    def to_json(self) -> Dict[str, Any]:
        return {
            'statements': [{'text': stmt} for stmt in self.statements],
            'confidence': self.confidence,
            'prompt': self.prompt,
            'prompt_id': self.prompt_id,
            'data_safe': self.data_safe,
            'required_user_input': list(self.required_user_input)
        }


class MigrationState(NamedTuple):

    parent_migration: Optional[s_migrations.Migration]
    initial_schema: s_schema.Schema
    initial_savepoint: Optional[str]
    target_schema: s_schema.Schema
    guidance: s_obj.DeltaGuidance
    accepted_cmds: Tuple[qlast.Base, ...]
    last_proposed: Optional[Tuple[ProposedMigrationStep, ...]]


class MigrationRewriteState(NamedTuple):

    initial_savepoint: Optional[str]
    target_schema: s_schema.Schema
    accepted_migrations: Tuple[qlast.CreateMigration, ...]


class TransactionState(NamedTuple):

    id: int
    name: Optional[str]
    user_schema: s_schema.FlatSchema
    global_schema: s_schema.FlatSchema
    modaliases: immutables.Map[Optional[str], str]
    session_config: immutables.Map[str, config.SettingValue]
    database_config: immutables.Map[str, config.SettingValue]
    system_config: immutables.Map[str, config.SettingValue]
    cached_reflection: immutables.Map[str, Tuple[str, ...]]
    tx: Transaction
    migration_state: Optional[MigrationState] = None
    migration_rewrite_state: Optional[MigrationRewriteState] = None


class Transaction:

    _savepoints: Dict[int, TransactionState]
    _constate: CompilerConnectionState

    def __init__(
        self,
        constate: CompilerConnectionState,
        *,
        user_schema: s_schema.FlatSchema,
        global_schema: s_schema.FlatSchema,
        modaliases: immutables.Map[Optional[str], str],
        session_config: immutables.Map[str, config.SettingValue],
        database_config: immutables.Map[str, config.SettingValue],
        system_config: immutables.Map[str, config.SettingValue],
        cached_reflection: immutables.Map[str, Tuple[str, ...]],
        implicit: bool = True,
    ) -> None:

        assert not isinstance(user_schema, s_schema.ChainedSchema)

        self._constate = constate

        self._id = constate._new_txid()
        self._implicit = implicit

        self._current = TransactionState(
            id=self._id,
            name=None,
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
            tx=self,
        )

        self._state0 = self._current
        self._savepoints = {}

    @property
    def id(self) -> int:
        return self._id

    def is_implicit(self) -> bool:
        return self._implicit

    def make_explicit(self) -> None:
        if self._implicit:
            self._implicit = False
        else:
            raise errors.TransactionError('already in explicit transaction')

    def declare_savepoint(self, name: str) -> int:
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        return self._declare_savepoint(name)

    def start_migration(self) -> str:
        name = str(uuid.uuid4())
        self._declare_savepoint(name)
        return name

    def _declare_savepoint(self, name: str) -> int:
        sp_id = self._constate._new_txid()
        sp_state = self._current._replace(id=sp_id, name=name)
        self._savepoints[sp_id] = sp_state
        self._constate._savepoints_log[sp_id] = sp_state
        return sp_id

    def rollback_to_savepoint(self, name: str) -> TransactionState:
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        return self._rollback_to_savepoint(name)

    def abort_migration(self, name: str) -> None:
        self._rollback_to_savepoint(name)

    def _rollback_to_savepoint(self, name: str) -> TransactionState:
        sp_ids_to_erase = []
        for sp in reversed(self._savepoints.values()):
            if sp.name == name:
                self._current = sp
                break

            sp_ids_to_erase.append(sp.id)
        else:
            raise errors.TransactionError(f'there is no {name!r} savepoint')

        for sp_id in sp_ids_to_erase:
            self._savepoints.pop(sp_id)

        return sp

    def release_savepoint(self, name: str) -> None:
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        self._release_savepoint(name)

    def commit_migration(self, name: str) -> None:
        self._release_savepoint(name)

    def _release_savepoint(self, name: str) -> None:
        sp_ids_to_erase = []
        for sp in reversed(self._savepoints.values()):
            sp_ids_to_erase.append(sp.id)

            if sp.name == name:
                break
        else:
            raise errors.TransactionError(f'there is no {name!r} savepoint')

        for sp_id in sp_ids_to_erase:
            self._savepoints.pop(sp_id)

    def get_schema(self, std_schema: s_schema.FlatSchema) -> s_schema.Schema:
        assert isinstance(std_schema, s_schema.FlatSchema)
        return s_schema.ChainedSchema(
            std_schema,
            self._current.user_schema,
            self._current.global_schema,
        )

    def get_user_schema(self) -> s_schema.FlatSchema:
        return self._current.user_schema

    def get_user_schema_if_updated(self) -> Optional[s_schema.FlatSchema]:
        if self._current.user_schema is self._state0.user_schema:
            return None
        else:
            return self._current.user_schema

    def get_global_schema(self) -> s_schema.FlatSchema:
        return self._current.global_schema

    def get_global_schema_if_updated(self) -> Optional[s_schema.FlatSchema]:
        if self._current.global_schema is self._state0.global_schema:
            return None
        else:
            return self._current.global_schema

    def get_modaliases(self) -> immutables.Map[Optional[str], str]:
        return self._current.modaliases

    def get_session_config(self) -> immutables.Map[str, config.SettingValue]:
        return self._current.session_config

    def get_database_config(self) -> immutables.Map[str, config.SettingValue]:
        return self._current.database_config

    def get_system_config(self) -> immutables.Map[str, config.SettingValue]:
        return self._current.system_config

    def get_cached_reflection_if_updated(self) -> Optional[
        immutables.Map[str, Tuple[str, ...]]
    ]:
        if self._current.cached_reflection == self._state0.cached_reflection:
            return None
        else:
            return self._current.cached_reflection

    def get_cached_reflection(self) -> immutables.Map[str, Tuple[str, ...]]:
        return self._current.cached_reflection

    def get_migration_state(self) -> Optional[MigrationState]:
        return self._current.migration_state

    def get_migration_rewrite_state(self) -> Optional[MigrationRewriteState]:
        return self._current.migration_rewrite_state

    def update_schema(self, new_schema: s_schema.Schema) -> None:
        assert isinstance(new_schema, s_schema.ChainedSchema)
        user_schema = new_schema.get_top_schema()
        assert isinstance(user_schema, s_schema.FlatSchema)
        global_schema = new_schema.get_global_schema()
        assert isinstance(global_schema, s_schema.FlatSchema)
        self._current = self._current._replace(
            user_schema=user_schema,
            global_schema=global_schema,
        )

    def update_modaliases(
        self, new_modaliases: immutables.Map[Optional[str], str]
    ) -> None:
        self._current = self._current._replace(modaliases=new_modaliases)

    def update_session_config(
        self, new_config: immutables.Map[str, config.SettingValue]
    ) -> None:
        self._current = self._current._replace(session_config=new_config)

    def update_database_config(
        self, new_config: immutables.Map[str, config.SettingValue]
    ) -> None:
        self._current = self._current._replace(database_config=new_config)

    def update_cached_reflection(
        self,
        new: immutables.Map[str, Tuple[str, ...]],
    ) -> None:
        self._current = self._current._replace(cached_reflection=new)

    def update_migration_state(
        self, mstate: Optional[MigrationState]
    ) -> None:
        self._current = self._current._replace(migration_state=mstate)

    def update_migration_rewrite_state(
        self, mrstate: Optional[MigrationRewriteState]
    ) -> None:
        self._current = self._current._replace(migration_rewrite_state=mrstate)


class CompilerConnectionState:

    __slots__ = ('_savepoints_log', '_current_tx', '_tx_count',)

    _savepoints_log: Dict[int, TransactionState]

    def __init__(
        self,
        *,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        modaliases: immutables.Map[Optional[str], str],
        session_config: immutables.Map[str, config.SettingValue],
        database_config: immutables.Map[str, config.SettingValue],
        system_config: immutables.Map[str, config.SettingValue],
        cached_reflection: immutables.Map[str, Tuple[str, ...]],
    ):
        self._tx_count = time.monotonic_ns()
        self._init_current_tx(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
        )
        self._savepoints_log = {}

    def _new_txid(self) -> int:
        self._tx_count += 1
        return self._tx_count

    def _init_current_tx(
        self,
        *,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        modaliases: immutables.Map[Optional[str], str],
        session_config: immutables.Map[str, config.SettingValue],
        database_config: immutables.Map[str, config.SettingValue],
        system_config: immutables.Map[str, config.SettingValue],
        cached_reflection: immutables.Map[str, Tuple[str, ...]],
    ) -> None:
        assert isinstance(user_schema, s_schema.FlatSchema)
        assert isinstance(global_schema, s_schema.FlatSchema)
        self._current_tx = Transaction(
            self,
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
        )

    def can_sync_to_savepoint(self, spid: int) -> bool:
        return spid in self._savepoints_log

    def sync_to_savepoint(self, spid: int) -> None:
        """Synchronize the compiler state with the current DB state."""

        if not self.can_sync_to_savepoint(spid):
            raise RuntimeError(f'failed to lookup savepoint with id={spid}')

        sp = self._savepoints_log[spid]
        self._current_tx = sp.tx
        self._current_tx._current = sp
        self._current_tx._id = spid

        # Cleanup all savepoints declared after the one we rolled back to
        # in the transaction we have now set as current.
        for id in tuple(self._current_tx._savepoints):
            if id > spid:
                self._current_tx._savepoints.pop(id)

        # Cleanup all savepoints declared after the one we rolled back to
        # in the global savepoints log.
        for id in tuple(self._savepoints_log):
            if id > spid:
                self._savepoints_log.pop(id)

    def current_tx(self) -> Transaction:
        return self._current_tx

    def start_tx(self) -> None:
        if self._current_tx.is_implicit():
            self._current_tx.make_explicit()
        else:
            raise errors.TransactionError('already in transaction')

    def rollback_tx(self) -> TransactionState:
        # Note that we might not be in a transaction as we allow
        # ROLLBACKs outside of transaction blocks (just like Postgres).

        prior_state = self._current_tx._state0

        self._init_current_tx(
            user_schema=prior_state.user_schema,
            global_schema=prior_state.global_schema,
            modaliases=prior_state.modaliases,
            session_config=prior_state.session_config,
            database_config=prior_state.database_config,
            system_config=prior_state.system_config,
            cached_reflection=prior_state.cached_reflection,
        )

        return prior_state

    def commit_tx(self) -> TransactionState:
        if self._current_tx.is_implicit():
            raise errors.TransactionError('cannot commit: not in transaction')

        latest_state = self._current_tx._current

        self._init_current_tx(
            user_schema=latest_state.user_schema,
            global_schema=latest_state.global_schema,
            modaliases=latest_state.modaliases,
            session_config=latest_state.session_config,
            database_config=latest_state.database_config,
            system_config=latest_state.system_config,
            cached_reflection=latest_state.cached_reflection,
        )

        return latest_state

    def sync_tx(self, txid: int) -> None:
        if self._current_tx.id == txid:
            return

        if self.can_sync_to_savepoint(txid):
            self.sync_to_savepoint(txid)
            return

        raise errors.InternalServerError(
            f'failed to lookup transaction or savepoint with id={txid}'
        )  # pragma: no cover
