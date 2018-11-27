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


import time
import typing

import immutables

from edb.server import defines
from edb.lang.common import lru

from . import dbstate


__all__ = ('DatabaseIndex', 'DatabaseConnectionView')


class Database:

    # Global LRU cache of compiled anonymous queries
    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnit]

    def __init__(self, name):
        self._name = name
        self._dbver = time.monotonic_ns()

        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

    def _signal_ddl(self):
        self._dbver = time.monotonic_ns()  # Advance the version
        self._invalidate_caches()

    def _invalidate_caches(self):
        self._eql_to_compiled.clear()

    def _cache_compiled_query(self, eql: bytes, json_mode: bool,
                              compiled: dbstate.QueryUnit):
        assert compiled.is_preparable()
        key = (eql, json_mode)
        existing = self._eql_to_compiled.get(key)
        if existing is not None and existing.dbver > compiled.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._eql_to_compiled[key] = compiled

    def _new_view(self, *, user):
        return DatabaseConnectionView(self, user=user)


class DatabaseConnectionView:

    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnit]

    def __init__(self, db: Database, *, user):
        self._db = db

        self._user = user

        self._config = immutables.Map()
        self._modaliases = immutables.Map({None: 'default'})

        # Whenever we are in a transaction that had executed a
        # DDL command, we use this cache for compiled queries.
        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self._new_tx_state()

    def _invalidate_local_cache(self):
        self._eql_to_compiled.clear()

    def _new_tx_state(self):
        self._txid = None
        self._in_tx = False
        self._in_tx_with_ddl = False
        self._tx_error = False

    def rollback(self):
        self._new_tx_state()

    @property
    def config(self):
        return self._config

    @property
    def modaliases(self):
        return self._modaliases

    @property
    def txid(self):
        return self._txid

    @property
    def in_tx(self):
        return self._in_tx

    @property
    def user(self):
        return self._user

    @property
    def dbver(self):
        return self._db._dbver

    @property
    def dbname(self):
        return self._db._name

    def cache_compiled_query(self, eql: bytes,
                             json_mode: bool,
                             compiled: dbstate.QueryUnit):
        if self._in_tx_with_ddl:
            self._eql_to_compiled[(eql, json_mode)] = compiled
        else:
            self._db._cache_compiled_query(eql, json_mode, compiled)

    def lookup_compiled_query(
            self, eql: bytes,
            json_mode: bool) -> typing.Optional[dbstate.QueryUnit]:

        compiled: dbstate.QueryUnit
        key = (eql, json_mode)

        if self._in_tx_with_ddl:
            compiled = self._eql_to_compiled.get(key)
        else:
            compiled = self._db._eql_to_compiled.get(key)
            if compiled is not None and compiled.dbver != self.dbver:
                compiled = None

        return compiled

    def tx_error(self):
        if self._in_tx:
            self._tx_error = True

    def start(self, qu: dbstate.QueryUnit):
        self._txid = qu.txid
        if qu.starts_tx:
            self._in_tx = True
            if qu.has_ddl:
                self._in_tx_with_ddl

    def on_error(self, qu: dbstate.QueryUnit):
        self.tx_error()

    def on_success(self, qu: dbstate.QueryUnit):
        if not self._in_tx and qu.has_ddl:
            self._db._signal_ddl()

        if qu.commits_tx:
            assert self._in_tx
            if self._in_tx_with_ddl:
                self._db._signal_ddl()
            self._new_tx_state()

        elif qu.rollbacks_tx:
            assert self._in_tx
            self._new_tx_state()

        if qu.config:
            self._config = qu.config

        if qu.modaliases:
            self._modaliases = qu.modaliases


class DatabaseIndex:

    def __init__(self):
        self._dbs = {}

    def new_view(self, dbname: str, *, user: str) -> DatabaseConnectionView:
        try:
            db = self._dbs[dbname]
        except KeyError:
            db = Database(dbname)
            self._dbs[dbname] = db

        return db._new_view(user=user)
