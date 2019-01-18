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

from edb import errors
from edb.server import defines
from edb.common import lru

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

    def _cache_compiled_query(self, key, compiled: dbstate.QueryUnit):
        assert compiled.cacheable

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
        self._modaliases = immutables.Map({None: defines.DEFAULT_MODULE_ALIAS})

        # Whenever we are in a transaction that had executed a
        # DDL command, we use this cache for compiled queries.
        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self._reset_tx_state()

    def _invalidate_local_cache(self):
        self._eql_to_compiled.clear()

    def _reset_tx_state(self):
        self._txid = None
        self._in_tx = False
        self._in_tx_with_ddl = False
        self._in_tx_with_set = False
        self._tx_error = False
        self._invalidate_local_cache()

    def rollback_tx_to_savepoint(self, spid, modaliases, config):
        self._tx_error = False
        # See also CompilerConnectionState.rollback_to_savepoint().
        self._txid = spid
        self._modaliases = modaliases
        self._config = config
        self._invalidate_local_cache()

    def recover_aliases_and_config(self, modaliases, config):
        assert not self._in_tx
        self._modaliases = modaliases
        self._config = config

    def abort_tx(self):
        if not self.in_tx():
            raise errors.InternalServerError('abort_tx(): not in transaction')
        self._reset_tx_state()

    @property
    def config(self):
        return self._config

    @property
    def modaliases(self):
        return self._modaliases

    @property
    def txid(self):
        return self._txid

    def in_tx(self):
        return self._in_tx

    def in_tx_error(self):
        return self._tx_error

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

        assert compiled.cacheable

        key = (eql, json_mode, self._modaliases, self._config)

        if self._in_tx_with_ddl:
            self._eql_to_compiled[key] = compiled
        else:
            self._db._cache_compiled_query(key, compiled)

    def lookup_compiled_query(
            self, eql: bytes,
            json_mode: bool) -> typing.Optional[dbstate.QueryUnit]:

        if self._tx_error:
            return None

        compiled: dbstate.QueryUnit
        key = (eql, json_mode, self._modaliases, self._config)

        if self._in_tx_with_ddl or self._in_tx_with_set:
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
        if self._tx_error:
            self.raise_in_tx_error()

        if qu.tx_id is not None:
            self._in_tx = True
            self._txid = qu.tx_id

        if self._in_tx and not self._txid:
            raise errors.InternalServerError('unset txid in transaction')

        if self._in_tx:
            if qu.has_ddl:
                self._in_tx_with_ddl = True
            if qu.has_set:
                self._in_tx_with_set = True

    def on_error(self, qu: dbstate.QueryUnit):
        self.tx_error()

    def on_success(self, qu: dbstate.QueryUnit):
        if qu.tx_savepoint_rollback:
            # Need to invalidate the cache in case there were
            # SET ALIAS/SET CONFIG or DDL commands.
            self._invalidate_local_cache()

        if not self._in_tx and qu.has_ddl:
            self._db._signal_ddl()

        if qu.config is not None:
            self._config = qu.config

        if qu.modaliases is not None:
            self._modaliases = qu.modaliases

        if qu.tx_commit:
            assert self._in_tx
            if self._in_tx_with_ddl:
                self._db._signal_ddl()
            self._reset_tx_state()

        elif qu.tx_rollback:
            assert self._in_tx
            self._reset_tx_state()

    @staticmethod
    def raise_in_tx_error():
        raise errors.TransactionError(
            'current transaction is aborted, '
            'commands ignored until end of transaction block')


class DatabaseIndex:

    def __init__(self):
        self._dbs = {}

    def _get_db(self, dbname):
        try:
            db = self._dbs[dbname]
        except KeyError:
            db = Database(dbname)
            self._dbs[dbname] = db
        return db

    def new_view(self, dbname: str, *, user: str) -> DatabaseConnectionView:
        db = self._get_db(dbname)
        return db._new_view(user=user)
