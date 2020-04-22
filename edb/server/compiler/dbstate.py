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

import dataclasses
import enum
import time
from typing import *

import immutables

from edb import errors

from edb.schema import schema as s_schema
from edb.server import config

from . import enums
from . import sertypes


class TxAction(enum.IntEnum):

    START = 1
    COMMIT = 2
    ROLLBACK = 3

    DECLARE_SAVEPOINT = 4
    RELEASE_SAVEPOINT = 5
    ROLLBACK_TO_SAVEPOINT = 6


@dataclasses.dataclass(frozen=True)
class BaseQuery:

    sql: Tuple[bytes, ...]


@dataclasses.dataclass(frozen=True)
class Query(BaseQuery):

    sql_hash: bytes

    cardinality: enums.ResultCardinality

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes
    in_type_args: Optional[List[Param]] = None

    is_transactional: bool = True
    single_unit: bool = False


@dataclasses.dataclass(frozen=True)
class SimpleQuery(BaseQuery):

    sql: Tuple[bytes, ...]
    is_transactional: bool = True
    single_unit: bool = False


@dataclasses.dataclass(frozen=True)
class SessionStateQuery(BaseQuery):

    is_system_setting: bool = False
    is_backend_setting: bool = False
    requires_restart: bool = False
    config_op: Optional[config.Operation] = None
    is_transactional: bool = True
    single_unit: bool = False


@dataclasses.dataclass(frozen=True)
class DDLQuery(BaseQuery):

    new_types: FrozenSet[str] = frozenset()
    is_transactional: bool = True
    single_unit: bool = False


@dataclasses.dataclass(frozen=True)
class TxControlQuery(BaseQuery):

    action: TxAction
    cacheable: bool

    modaliases: Optional[immutables.Map]
    is_transactional: bool = True
    single_unit: bool = False


@dataclasses.dataclass(frozen=True)
class Param:
    name: str
    required: bool
    array_tid: Optional[int]


#############################


@dataclasses.dataclass
class QueryUnit:

    dbver: bytes

    sql: Tuple[bytes, ...]

    # Status-line for the compiled command; returned to front-end
    # in a CommandComplete protocol message if the command is
    # executed successfully.  When a QueryUnit contains multiple
    # EdgeQL queries, the status reflects the last query in the unit.
    status: bytes

    # Set only for units that contain queries that can be cached
    # as prepared statements in Postgres.
    sql_hash: bytes = b''

    # True if all statments in *sql* can be executed inside a transaction.
    # If False, they will be executed separately.
    is_transactional: bool = True

    # True if this unit contains DDL commands.
    has_ddl: bool = False

    # A set of ids of types added by this unit.
    new_types: FrozenSet[str] = frozenset()

    # True if this unit contains SET commands.
    has_set: bool = False

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

    # True if it is safe to cache this unit.
    cacheable: bool = False

    # Cardinality of the result set.  Set to NO_RESULT if the
    # unit represents multiple queries compiled as one script.
    cardinality: enums.ResultCardinality = \
        enums.ResultCardinality.NO_RESULT

    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID
    in_type_data: bytes = sertypes.EMPTY_TUPLE_DESC
    in_type_id: bytes = sertypes.EMPTY_TUPLE_ID
    in_type_args: Optional[List[Param]] = None

    # Set only when this unit contains a CONFIGURE SYSTEM command.
    system_config: bool = False
    config_requires_restart: bool = False
    # Set only when this unit contains a CONFIGURE command which
    # alters a backend configuration setting.
    backend_config: bool = False
    config_ops: List[config.Operation] = (
        dataclasses.field(default_factory=list))
    modaliases: Optional[immutables.Map] = None


#############################


class TransactionState(NamedTuple):

    id: int
    name: str
    schema: s_schema.Schema
    modaliases: immutables.Map
    config: immutables.Map


class Transaction:

    def __init__(self, constate,
                 schema: s_schema.Schema,
                 modaliases: immutables.Map,
                 config: immutables.Map, *,
                 implicit=True):

        self._constate = constate

        self._id = time.monotonic_ns()

        self._implicit = implicit

        self._stack = []

        # Save the very first state -- we can use it to rollback
        # the transaction completely.
        self._stack.append(
            TransactionState(
                id=self._id,
                name=None,
                schema=schema,
                modaliases=modaliases,
                config=config))

        # The top of the stack is the "current" state.
        self._stack.append(
            TransactionState(
                id=self._id,
                name=None,
                schema=schema,
                modaliases=modaliases,
                config=config))

    @property
    def id(self):
        return self._id

    def copy(self):
        tr = Transaction.__new__(Transaction)
        tr._id = self._id
        tr._constate = self._constate
        tr._implicit = self._implicit
        tr._stack = self._stack.copy()
        return tr

    def is_implicit(self):
        return self._implicit

    def make_explicit(self):
        if self._implicit:
            self._implicit = False
        else:
            raise errors.TransactionError('already in explicit transaction')

    def declare_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        sp_id = time.monotonic_ns()

        # Save the savepoint state so that we can rollback to it.
        self._stack.append(
            TransactionState(
                id=sp_id,
                name=name,
                schema=self.get_schema(),
                modaliases=self.get_modaliases(),
                config=self.get_session_config()))

        # The top of the stack is the "current" state.
        self._stack.append(
            TransactionState(
                id=sp_id,
                name=None,
                schema=self.get_schema(),
                modaliases=self.get_modaliases(),
                config=self.get_session_config()))

        copy = self.copy()
        self._constate._savepoints_log[sp_id] = copy

        return sp_id

    def rollback_to_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        new_stack = self._stack.copy()
        while new_stack:
            top_new_state = new_stack[-1]
            if top_new_state.name == name:
                self._stack = new_stack
                # Add a nameless copy of the savepoint's state -- new
                # "working" state.
                self._stack.append(self._stack[-1]._replace(name=None))
                return self._stack[-1]
            else:
                new_stack.pop()
        raise errors.TransactionError(f'there is no {name!r} savepoint')

    def release_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        new_stack = []
        released = False
        for st in reversed(self._stack):
            if not released and st.name == name:
                released = True
                continue
            else:
                new_stack.append(st)
        if not released:
            raise errors.TransactionError(f'there is no {name!r} savepoint')
        else:
            self._stack = new_stack[::-1]

    def get_schema(self) -> s_schema.Schema:
        return self._stack[-1].schema

    def get_modaliases(self) -> immutables.Map:
        return self._stack[-1].modaliases

    def get_session_config(self) -> immutables.Map:
        return self._stack[-1].config

    def update_schema(self, new_schema: s_schema.Schema):
        self._stack[-1] = self._stack[-1]._replace(schema=new_schema)

    def update_modaliases(self, new_modaliases: immutables.Map):
        self._stack[-1] = self._stack[-1]._replace(modaliases=new_modaliases)

    def update_session_config(self, new_config: immutables.Map):
        self._stack[-1] = self._stack[-1]._replace(config=new_config)


class CompilerConnectionState:

    _savepoints_log: Mapping[int, Transaction]

    __slots__ = ('_savepoints_log', '_dbver', '_current_tx', '_capability')

    def __init__(self, dbver: bytes,
                 schema: s_schema.Schema,
                 modaliases: immutables.Map,
                 config: immutables.Map,
                 capability: enums.Capability):
        self._dbver = dbver
        self._savepoints_log = {}
        self._init_current_tx(schema, modaliases, config)
        self._capability = capability

    def _init_current_tx(self, schema, modaliases, config):
        self._current_tx = Transaction(
            self, schema, modaliases, config)

    def can_rollback_to_savepoint(self, spid):
        return spid in self._savepoints_log

    def rollback_to_savepoint(self, spid):
        if spid not in self._savepoints_log:
            raise RuntimeError(
                f'failed to lookup savepoint with id={spid}')

        new_tx = self._savepoints_log[spid]
        # This is tricky -- the server now thinks that this *spid*
        # is the new ID *of the current transaction* (txid).
        #
        # (see DatabaseConnectionView.rollback_tx_to_savepoint())
        #
        # This is done this way to avoid one extra call to the compiler
        # process to infer the "proper" transaction ID; it's easier
        # to just say that in the case of failed transaction and
        # ROLLBACK TO SAVEPOINT the ID of transaction changes to that
        # of the recovered savepoint.
        new_tx._id = spid

        self._savepoints_log.clear()
        self._current_tx = new_tx

    @property
    def dbver(self):
        return self._dbver

    @property
    def capability(self):
        return self._capability

    def current_tx(self) -> Transaction:
        return self._current_tx

    def start_tx(self):
        if self._current_tx.is_implicit():
            self._current_tx.make_explicit()
        else:
            raise errors.TransactionError('already in transaction')

    def rollback_tx(self):
        # Note that we might not be in a transaction as we allow
        # ROLLBACKs outside of transaction blocks (just like Postgres).

        prior_state = self._current_tx._stack[0]

        self._init_current_tx(
            prior_state.schema,
            prior_state.modaliases,
            prior_state.config)

        return prior_state

    def commit_tx(self):
        if self._current_tx.is_implicit():
            raise errors.TransactionError('cannot commit: not in transaction')

        latest_state = self._current_tx._stack[-1]

        self._init_current_tx(
            latest_state.schema,
            latest_state.modaliases,
            latest_state.config)

        return latest_state
