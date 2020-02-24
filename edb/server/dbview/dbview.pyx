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


import json
import os.path
import pickle
import typing

import immutables

from edb import errors
from edb.common import lru, uuidgen
from edb.server import defines, config
from edb.server.compiler import dbstate
from edb.pgsql import dbops


__all__ = ('DatabaseIndex', 'DatabaseConnectionView')


cdef class Database:

    # Global LRU cache of compiled anonymous queries
    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnit]

    def __init__(self, DatabaseIndex index, str name):
        self._name = name
        self._dbver = uuidgen.uuid1mc().bytes

        self._index = index

        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

    cdef _signal_ddl(self, new_dbver):
        if new_dbver is None:
            self._dbver = uuidgen.uuid1mc().bytes
        else:
            self._dbver = new_dbver
        self._invalidate_caches()

    cdef _invalidate_caches(self):
        self._eql_to_compiled.clear()

    cdef _cache_compiled_query(self, key, compiled: dbstate.QueryUnit):
        assert compiled.cacheable

        existing = self._eql_to_compiled.get(key)
        if existing is not None and existing.dbver == compiled.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._eql_to_compiled[key] = compiled

    cdef _new_view(self, user, query_cache):
        return DatabaseConnectionView(self, user=user, query_cache=query_cache)


cdef class DatabaseConnectionView:

    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnit]

    def __init__(self, db: Database, *, user, query_cache):
        self._db = db

        self._query_cache_enabled = query_cache

        self._user = user

        self._config = immutables.Map()
        self._in_tx_config = None

        self._modaliases = immutables.Map({None: defines.DEFAULT_MODULE_ALIAS})

        # Whenever we are in a transaction that had executed a
        # DDL command, we use this cache for compiled queries.
        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self._reset_tx_state()

    cdef _invalidate_local_cache(self):
        self._eql_to_compiled.clear()

    cdef _reset_tx_state(self):
        self._txid = None
        self._in_tx = False
        self._in_tx_config = None
        self._in_tx_with_ddl = False
        self._in_tx_with_set = False
        self._tx_error = False
        self._invalidate_local_cache()

    cdef on_remote_ddl(self, bytes new_dbver):
        """Called when a DDL operation was applied at another server."""
        if new_dbver != self._db._dbver:
            self._db._signal_ddl(new_dbver)

    cdef rollback_tx_to_savepoint(self, spid, modaliases, config):
        self._tx_error = False
        # See also CompilerConnectionState.rollback_to_savepoint().
        self._txid = spid
        self._modaliases = modaliases
        self.set_session_config(config)
        self._invalidate_local_cache()

    cdef recover_aliases_and_config(self, modaliases, config):
        assert not self._in_tx
        self._modaliases = modaliases
        self.set_session_config(config)

    cdef abort_tx(self):
        if not self.in_tx():
            raise errors.InternalServerError('abort_tx(): not in transaction')
        self._reset_tx_state()

    cdef get_session_config(self):
        if self._in_tx:
            return self._in_tx_config
        else:
            return self._config

    cdef set_session_config(self, new_conf):
        if self._in_tx:
            self._in_tx_config = new_conf
        else:
            self._config = new_conf

    property modaliases:
        def __get__(self):
            return self._modaliases

    property txid:
        def __get__(self):
            return self._txid

    property user:
        def __get__(self):
            return self._user

    property dbver:
        def __get__(self):
            return self._db._dbver

    property dbname:
        def __get__(self):
            return self._db._name

    cdef in_tx(self):
        return self._in_tx

    cdef in_tx_error(self):
        return self._tx_error

    cdef cache_compiled_query(self, bytes eql, object io_format,
                              bint expect_one, int implicit_limit, query_unit):

        assert query_unit.cacheable

        key = (eql, io_format, expect_one, implicit_limit,
               self._modaliases, self._config)

        if self._in_tx_with_ddl:
            self._eql_to_compiled[key] = query_unit
        else:
            self._db._cache_compiled_query(key, query_unit)

    cdef lookup_compiled_query(self, bytes eql, object io_format,
                               bint expect_one, int implicit_limit):
        if (self._tx_error or
                not self._query_cache_enabled or
                self._in_tx_with_ddl):
            return None

        key = (eql, io_format, expect_one, implicit_limit,
               self._modaliases, self._config)

        if self._in_tx_with_ddl or self._in_tx_with_set:
            query_unit = self._eql_to_compiled.get(key)
        else:
            query_unit = self._db._eql_to_compiled.get(key)
            if query_unit is not None and query_unit.dbver != self.dbver:
                query_unit = None

        return query_unit

    cdef tx_error(self):
        if self._in_tx:
            self._tx_error = True

    cdef start(self, query_unit):
        if self._tx_error:
            self.raise_in_tx_error()

        if query_unit.tx_id is not None:
            self._in_tx = True
            self._txid = query_unit.tx_id
            self._in_tx_config = self._config

        if self._in_tx and not self._txid:
            raise errors.InternalServerError('unset txid in transaction')

        if self._in_tx:
            if query_unit.has_ddl:
                self._in_tx_with_ddl = True
            if query_unit.has_set:
                self._in_tx_with_set = True

    cdef on_error(self, query_unit):
        self.tx_error()

    cdef on_success(self, query_unit):
        signal_ddl = False

        if query_unit.tx_savepoint_rollback:
            # Need to invalidate the cache in case there were
            # SET ALIAS or CONFIGURE or DDL commands.
            self._invalidate_local_cache()

        if not self._in_tx and query_unit.has_ddl:
            self._db._signal_ddl(None)
            signal_ddl = True

        if query_unit.modaliases is not None:
            self._modaliases = query_unit.modaliases

        if query_unit.tx_commit:
            if not self._in_tx:
                # This shouldn't happen because compiler has
                # checks around that.
                raise errors.InternalServerError(
                    '"commit" outside of a transaction')
            self._config = self._in_tx_config
            if self._in_tx_with_ddl:
                self._db._signal_ddl(None)
                signal_ddl = True
            self._reset_tx_state()

        elif query_unit.tx_rollback:
            # Note that we might not be in a transaction as we allow
            # ROLLBACKs outside of transaction blocks (just like Postgres).
            # TODO: That said, we should send a *warning* when a ROLLBACK
            # is executed outside of a tx.
            self._reset_tx_state()

        return signal_ddl

    async def apply_config_ops(self, conn, ops):
        for op in ops:
            if op.level is config.OpLevel.SYSTEM:
                await self._db._index.apply_system_config_op(conn, op)
            else:
                self.set_session_config(
                    op.apply(
                        config.get_settings(),
                        self.get_session_config()))

    @staticmethod
    def raise_in_tx_error():
        raise errors.TransactionError(
            'current transaction is aborted, '
            'commands ignored until end of transaction block')


cdef class DatabaseIndex:

    @classmethod
    async def init(cls, server) -> DatabaseIndex:
        state = cls(server)
        await state.reload_config()
        return state

    def __init__(self, server):
        self._dbs = {}

        self._server = server
        self._sys_queries = None
        self._instance_data = None
        self._sys_config = None

    async def get_sys_query(self, conn, key: str) -> bytes:
        if self._sys_queries is None:
            result = await conn.simple_query(
                b'SELECT edgedb.__syscache_sysqueries()',
                ignore_data=False,
            )
            queries = json.loads(result[0][0].decode('utf-8'))
            self._sys_queries = {k: q.encode() for k, q in queries.items()}

        return self._sys_queries[key]

    async def get_instance_data(self, conn, key: str) -> object:
        if self._instance_data is None:
            result = await conn.simple_query(
                b'SELECT edgedb.__syscache_instancedata()',
                ignore_data=False,
            )
            self._instance_data = json.loads(result[0][0].decode('utf-8'))

        return self._instance_data[key]

    async def reload_config(self):
        conn = await self._server.new_pgcon(defines.EDGEDB_SUPERUSER_DB)

        query = await self.get_sys_query(conn, 'config')
        try:
            result = await conn.simple_query(query, ignore_data=False)
        finally:
            conn.terminate()

        config_json = result[0][0].decode('utf-8')
        self._sys_config = config.from_json(config.get_settings(), config_json)

    def get_sys_config(self):
        return self._sys_config

    def get_dbver(self, dbname):
        db = self._get_db(dbname)
        return (<Database>db)._dbver

    def _get_db(self, dbname):
        try:
            db = self._dbs[dbname]
        except KeyError:
            db = Database(self, dbname)
            self._dbs[dbname] = db
        return db

    async def _save_system_overrides(self, conn):
        data = config.to_json(config.get_settings(), self._sys_config)
        block = dbops.PLTopBlock()
        dbops.UpdateMetadata(
            dbops.Database(name=defines.EDGEDB_TEMPLATE_DB),
            {'sysconfig': json.loads(data)},
        ).generate(block)
        await conn.simple_query(block.to_string().encode(), True)

    async def apply_system_config_op(self, conn, op):
        op_value = op.get_setting(config.get_settings())
        if op.opcode is not None:
            allow_missing = (
                op.opcode is config.OpCode.CONFIG_REM
                or op.opcode is config.OpCode.CONFIG_RESET
            )
            op_value = op.coerce_value(op_value, allow_missing=allow_missing)

        # _save_system_overrides *must* happen before
        # the callbacks below, because certain config changes
        # may cause the backend connection to drop.
        self._sys_config = op.apply(config.get_settings(), self._sys_config)
        await self._save_system_overrides(conn)

        if op.opcode is config.OpCode.CONFIG_ADD:
            await self._server._on_system_config_add(op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_REM:
            await self._server._on_system_config_rem(op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_SET:
            await self._server._on_system_config_set(op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_RESET:
            await self._server._on_system_config_reset(op.setting_name)
        else:
            raise errors.UnsupportedFeatureError(
                f'unsupported config operation: {op.opcode}')

        if op.opcode is config.OpCode.CONFIG_ADD:
            await self._server._after_system_config_add(
                op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_REM:
            await self._server._after_system_config_rem(
                op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_SET:
            await self._server._after_system_config_set(
                op.setting_name, op_value)
        elif op.opcode is config.OpCode.CONFIG_RESET:
            await self._server._after_system_config_reset(
                op.setting_name)

    def new_view(self, dbname: str, *, user: str, query_cache: bool):
        db = self._get_db(dbname)
        return (<Database>db)._new_view(user, query_cache)
