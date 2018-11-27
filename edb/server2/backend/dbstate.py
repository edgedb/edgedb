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

from edb.lang.schema import schema as s_schema


class TxAction(enum.IntEnum):

    START = 1
    COMMIT = 2
    ROLLBACK = 3


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


#############################


@dataclasses.dataclass
class QueryUnit:

    dbver: int
    txid: typing.Optional[int]

    sql: bytes = b''
    sql_hash: bytes = b''

    has_ddl: bool = False

    commits_tx: bool = False
    rollbacks_tx: bool = False
    starts_tx: bool = False

    out_type_data: bytes = b''
    out_type_id: bytes = b''
    in_type_data: bytes = b''
    in_type_id: bytes = b''

    config: typing.Optional[immutables.Map] = None
    modaliases: typing.Optional[immutables.Map] = None

    def is_preparable(self):
        """Answers the question: can this query be prepared and cached?"""
        prep = bool(self.sql and self.sql_hash and self.out_type_data)

        assert not prep or (not self.config and
                            not self.modaliases and
                            not self.has_ddl and
                            not self.commits_tx and
                            not self.rollbacks_tx and
                            not self.starts_tx)
        return prep


#############################


@dataclasses.dataclass
class TransactionState:

    name: str
    schema: s_schema.Schema
    modaliases: immutables.Map
    config: immutables.Map


class Transaction:

    def __init__(self, schema: s_schema.Schema,
                 modaliases: immutables.Map,
                 config: immutables.Map, *,
                 implicit=True):

        self._id = time.monotonic_ns()

        self._implicit = implicit

        self._stack = []
        self._stack.append(
            TransactionState(
                name=None,
                schema=schema,
                modaliases=modaliases,
                config=config))

    @property
    def id(self):
        return self._id

    def is_implicit(self):
        return self._implicit

    def make_explicit(self):
        if self._implicit:
            self._implicit = False
        else:
            raise RuntimeError('already in explicit transaction')

    def make_savepoint(self, name: str):
        self._stack.append(
            TransactionState(
                name=name,
                schema=self.get_schema(),
                modaliases=self.get_modaliases(),
                config=self.get_config()))

    def restore_savepoint(self, name: str):
        new_stack = self._stack.copy()
        while new_stack:
            last_state = new_stack.pop()
            if last_state.name == name:
                self._stack = new_stack
                return
        raise RuntimeError(f'there is no {name!r} savepoint')

    def get_schema(self) -> s_schema.Schema:
        return self._stack[-1].schema

    def get_modaliases(self) -> immutables.Map:
        return self._stack[-1].modaliases

    def get_config(self) -> immutables.Map:
        return self._stack[-1].config

    def update_schema(self, new_schema: s_schema.Schema):
        self._stack[-1].schema = new_schema

    def update_modaliases(self, new_modaliases: immutables.Map):
        self._stack[-1].modaliases = new_modaliases

    def update_config(self, new_config: immutables.Map):
        self._stack[-1].config = new_config


class CompilerConnectionState:

    def __init__(self, dbver: int,
                 schema: s_schema.Schema,
                 modaliases: immutables.Map,
                 config: immutables.Map):
        self._dbver = dbver
        self._schema = schema
        self._modaliases = modaliases
        self._config = config
        self._init_current_tx()

    def _init_current_tx(self):
        self._current_tx = Transaction(
            self._schema, self._modaliases, self._config)

    @property
    def dbver(self):
        return self._dbver

    def current_tx(self) -> Transaction:
        return self._current_tx

    def start_tx(self):
        if self._current_tx.is_implicit():
            self._current_tx.make_explicit()
        else:
            raise RuntimeError('already in transaction')

    def rollback_tx(self):
        if self._current_tx.is_implicit():
            raise RuntimeError('cannot rollback: not in transaction')

        self._init_current_tx()

    def commit_tx(self):
        if self._current_tx.is_implicit():
            raise RuntimeError('cannot commit: not in transaction')

        self._schema = self._current_tx.get_schema()
        self._modaliases = self._current_tx.get_modaliases()
        self._config = self._current_tx.get_config()
        self._init_current_tx()
