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


import dataclasses
import enum
import time
import typing

import immutables

from edb import errors

from edb.schema import schema as s_schema

from . import sertypes


class TxAction(enum.IntEnum):

    START = 1
    COMMIT = 2
    ROLLBACK = 3

    DECLARE_SAVEPOINT = 4
    RELEASE_SAVEPOINT = 5
    ROLLBACK_TO_SAVEPOINT = 6


class BaseQuery:
    pass


@dataclasses.dataclass(frozen=True)
class Query(BaseQuery):

    sql: bytes
    sql_hash: bytes

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes


@dataclasses.dataclass(frozen=True)
class SimpleQuery(BaseQuery):

    sql: bytes


@dataclasses.dataclass(frozen=True)
class SessionStateQuery(BaseQuery):

    sql: bytes

    sess_set_modaliases: typing.Mapping[typing.Optional[str], str] = None
    sess_reset_modaliases: typing.Set[typing.Optional[str]] = None
    sess_set_config: typing.Mapping[str, typing.Union[str, bool]] = None
    sess_reset_config: typing.Set[str] = None


@dataclasses.dataclass(frozen=True)
class DDLQuery(BaseQuery):

    sql: bytes


@dataclasses.dataclass(frozen=True)
class TxControlQuery(BaseQuery):

    sql: bytes
    action: TxAction
    single_unit: bool
    cacheable: bool


#############################


@dataclasses.dataclass
class QueryUnit:

    dbver: int

    sql: bytes = b''

    # Set only for units that contain queries that can be cached
    # as prepared statements in Postgres.
    sql_hash: bytes = b''

    # True if this unit contains DDL commands.
    has_ddl: bool = False

    # True if this unit contains SET commands.
    has_set: bool = False

    # If tx_id is set, it means that the unit
    # starts a new transaction.
    tx_id: typing.Optional[int] = None

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

    ignore_out_data: bool = True
    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID
    in_type_data: bytes = sertypes.EMPTY_TUPLE_DESC
    in_type_id: bytes = sertypes.EMPTY_TUPLE_ID

    config: typing.Optional[immutables.Map] = None
    modaliases: typing.Optional[immutables.Map] = None


#############################


class TransactionState(typing.NamedTuple):

    id: int
    txid: int
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
        self._stack.append(
            TransactionState(
                id=self._id,
                txid=self._id,
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

        self._stack.append(
            TransactionState(
                id=sp_id,
                txid=self._id,
                name=name,
                schema=self.get_schema(),
                modaliases=self.get_modaliases(),
                config=self.get_config()))

        self._stack.append(
            TransactionState(
                id=sp_id,
                txid=self._id,
                name=None,
                schema=self.get_schema(),
                modaliases=self.get_modaliases(),
                config=self.get_config()))

        copy = self.copy()
        self._constate._savepoints_log[sp_id] = copy

        return sp_id

    def rollback_to_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        new_stack = self._stack.copy()
        while new_stack:
            last_state = new_stack[-1]
            if last_state.name == name:
                self._stack = new_stack
                return
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

    def get_config(self) -> immutables.Map:
        return self._stack[-1].config

    def update_schema(self, new_schema: s_schema.Schema):
        self._stack[-1] = self._stack[-1]._replace(schema=new_schema)

    def update_modaliases(self, new_modaliases: immutables.Map):
        self._stack[-1] = self._stack[-1]._replace(modaliases=new_modaliases)

    def update_config(self, new_config: immutables.Map):
        self._stack[-1] = self._stack[-1]._replace(config=new_config)


class CompilerConnectionState:

    _savepoints_log: typing.Mapping[int, Transaction]

    def __init__(self, dbver: int,
                 schema: s_schema.Schema,
                 modaliases: immutables.Map,
                 config: immutables.Map):
        self._dbver = dbver
        self._schema = schema
        self._modaliases = modaliases
        self._config = config
        self._init_current_tx()
        self._savepoints_log = {}

    def _init_current_tx(self):
        self._current_tx = Transaction(
            self, self._schema, self._modaliases, self._config)

    def can_rollback_to_savepoint(self, spid):
        return spid in self._savepoints_log

    def rollback_to_savepoint(self, spid):
        if spid not in self._savepoints_log:
            raise RuntimeError(
                f'failed to lookup savepoint with id={spid}')

        self._current_tx = self._savepoints_log[spid]

        for id in list(self._savepoints_log):
            if self._savepoints_log[id].id != self._current_tx.id:
                # Cleanup all savepoints that belong to transactions
                # *other than* the transaction we've just rollbacked inside.
                del self._savepoints_log[id]

    @property
    def dbver(self):
        return self._dbver

    def current_tx(self) -> Transaction:
        return self._current_tx

    def start_tx(self):
        if self._current_tx.is_implicit():
            self._current_tx.make_explicit()
        else:
            raise errors.TransactionError('already in transaction')

    def rollback_tx(self):
        if self._current_tx.is_implicit():
            raise errors.TransactionError(
                'cannot rollback: not in transaction')

        self._init_current_tx()

    def commit_tx(self):
        if self._current_tx.is_implicit():
            raise errors.TransactionError('cannot commit: not in transaction')

        self._schema = self._current_tx.get_schema()
        self._modaliases = self._current_tx.get_modaliases()
        self._config = self._current_tx.get_config()
        self._init_current_tx()
