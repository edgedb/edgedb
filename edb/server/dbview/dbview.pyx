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
import weakref

import immutables

from edb import errors
from edb.common import lru, uuidgen
from edb.schema import extensions as s_ext
from edb.schema import schema as s_schema
from edb.server import defines, config
from edb.server.compiler import dbstate
from edb.pgsql import dbops


__all__ = ('DatabaseIndex', 'DatabaseConnectionView', 'SideEffects')

cdef DEFAULT_MODALIASES = immutables.Map({None: defines.DEFAULT_MODULE_ALIAS})
cdef DEFAULT_CONFIG = immutables.Map()
cdef DEFAULT_STATE = json.dumps([
    {"name": n or '', "value": v, "type": "A"}
    for n, v in DEFAULT_MODALIASES.items()
]).encode('utf-8')

cdef int VER_COUNTER = 0
cdef DICTDEFAULT = (None, None)


cdef next_dbver():
    global VER_COUNTER
    VER_COUNTER += 1
    return VER_COUNTER


cdef class Database:

    # Global LRU cache of compiled anonymous queries
    _eql_to_compiled: typing.Mapping[str, dbstate.QueryUnit]

    def __init__(
        self,
        DatabaseIndex index,
        str name,
        *,
        object user_schema,
        object db_config,
        object reflection_cache,
        object backend_ids
    ):
        self.name = name

        self.dbver = next_dbver()

        self._index = index
        self._views = weakref.WeakSet()

        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self.db_config = db_config
        self.user_schema = user_schema
        self.reflection_cache = reflection_cache
        self.backend_ids = backend_ids
        self.extensions = {
            ext.get_name(user_schema).name: ext
            for ext in user_schema.get_objects(type=s_ext.Extension)
        }

    cdef _set_and_signal_new_user_schema(
        self,
        new_schema,
        reflection_cache=None,
        backend_ids=None,
        db_config=None,
    ):
        if new_schema is None:
            raise AssertionError('new_schema is not supposed to be None')

        self.dbver = next_dbver()

        self.user_schema = new_schema

        self.extensions = {
            ext.get_name(new_schema).name: ext
            for ext in new_schema.get_objects(type=s_ext.Extension)
        }

        if backend_ids is not None:
            self.backend_ids = backend_ids
        if reflection_cache is not None:
            self.reflection_cache = reflection_cache
        if db_config is not None:
            self.db_config = db_config
        self._invalidate_caches()

    cdef _update_backend_ids(self, new_types):
        self.backend_ids.update(new_types)

    cdef _invalidate_caches(self):
        self._eql_to_compiled.clear()

    cdef _cache_compiled_query(self, key, compiled: dbstate.QueryUnit):
        assert compiled.cacheable

        existing, dbver = self._eql_to_compiled.get(key, DICTDEFAULT)
        if existing is not None and dbver == self.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._eql_to_compiled[key] = compiled, self.dbver

    cdef _new_view(self, user, query_cache):
        view = DatabaseConnectionView(self, user=user, query_cache=query_cache)
        self._views.add(view)
        return view

    cdef _remove_view(self, view):
        self._views.remove(view)


cdef class DatabaseConnectionView:

    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnit]

    def __init__(self, db: Database, *, user, query_cache):
        self._db = db

        self._query_cache_enabled = query_cache

        self._user = user

        self._modaliases = DEFAULT_MODALIASES
        self._config = DEFAULT_CONFIG
        self._db_config = db.db_config
        self._session_state_cache = None

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
        self._in_tx_db_config = None
        self._in_tx_modaliases = None
        self._in_tx_with_ddl = False
        self._in_tx_with_role_ddl = False
        self._in_tx_with_sysconfig = False
        self._in_tx_with_dbconfig = False
        self._in_tx_with_set = False
        self._in_tx_user_schema = None
        self._in_tx_user_schema_pickled = None
        self._in_tx_global_schema = None
        self._in_tx_global_schema_pickled = None
        self._in_tx_new_types = {}
        self._tx_error = False
        self._in_tx_dbver = 0
        self._invalidate_local_cache()

    cdef rollback_tx_to_savepoint(self, spid, modaliases, config):
        self._tx_error = False
        # See also CompilerConnectionState.rollback_to_savepoint().
        self._txid = spid
        self.set_modaliases(modaliases)
        self.set_session_config(config)
        self._invalidate_local_cache()

    cdef recover_aliases_and_config(self, modaliases, config):
        assert not self._in_tx
        self.set_modaliases(modaliases)
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

    cdef get_database_config(self):
        if self._in_tx:
            return self._in_tx_db_config
        else:
            return self._db_config

    cdef set_database_config(self, new_conf):
        if self._in_tx:
            self._in_tx_db_config = new_conf
        else:
            self._db_config = new_conf

    cdef get_system_config(self):
        return self._db._index.get_sys_config()

    cdef get_compilation_system_config(self):
        return self._db._index.get_compilation_system_config()

    cdef set_modaliases(self, new_aliases):
        if self._in_tx:
            self._in_tx_modaliases = new_aliases
        else:
            self._modaliases = new_aliases

    cdef get_modaliases(self):
        if self._in_tx:
            return self._in_tx_modaliases
        else:
            return self._modaliases

    def get_user_schema(self):
        if self._in_tx:
            if self._in_tx_user_schema_pickled:
                self._in_tx_user_schema = pickle.loads(
                    self._in_tx_user_schema_pickled)
                self._in_tx_user_schema_pickled = None
            return self._in_tx_user_schema
        else:
            return self._db.user_schema

    def get_global_schema(self):
        if self._in_tx:
            if self._in_tx_global_schema_pickled:
                self._in_tx_global_schema = pickle.loads(
                    self._in_tx_global_schema_pickled)
                self._in_tx_global_schema_pickled = None
            return self._in_tx_global_schema
        else:
            return self._db._index._global_schema

    def get_schema(self):
        user_schema = self.get_user_schema()
        return s_schema.ChainedSchema(
            self._db._index._std_schema,
            user_schema,
            self._db._index._global_schema,
        )

    def resolve_array_type_id(self, type_id):
        type_id = str(type_id)

        if self._in_tx:
            try:
                return int(self._in_tx_new_types[type_id])
            except KeyError:
                pass

        tid = self._db.backend_ids.get(type_id)
        if tid is None:
            raise RuntimeError(
                f'failed to resolve array OID for type {type_id}')
        return tid

    cdef bytes serialize_state(self):
        cdef list state
        if self._in_tx:
            raise errors.InternalServerError(
                'no need to serialize state while in transaction')
        if (
            self._config == DEFAULT_CONFIG and
            self._modaliases == DEFAULT_MODALIASES
        ):
            return DEFAULT_STATE

        if self._session_state_cache is not None:
            if (
                self._session_state_cache[0] == self._config and
                self._session_state_cache[1] == self._modaliases
            ):
                return self._session_state_cache[2]

        state = []
        for key, val in self._modaliases.items():
            state.append(
                {"name": key or '', "value": val, "type": "A"}
            )
        if self._config:
            settings = config.get_settings()
            for sval in self._config.values():
                setting = settings[sval.name]
                if setting.backend_setting:
                    # We don't store PostgreSQL config settings in
                    # the _edgecon_state temp table.
                    continue
                jval = config.value_to_json_value(setting, sval.value)
                state.append({"name": sval.name, "value": jval, "type": "C"})

        spec = json.dumps(state).encode('utf-8')
        self._session_state_cache = (self._config, self._modaliases, spec)
        return spec

    property txid:
        def __get__(self):
            return self._txid

    property user:
        def __get__(self):
            return self._user

    property dbname:
        def __get__(self):
            return self._db.name

    property reflection_cache:
        def __get__(self):
            return self._db.reflection_cache

    property dbver:
        def __get__(self):
            if self._in_tx and self._in_tx_dbver:
                return self._in_tx_dbver
            return self._db.dbver

    cdef in_tx(self):
        return self._in_tx

    cdef in_tx_error(self):
        return self._tx_error

    cdef cache_compiled_query(self, object key, object query_unit):
        assert query_unit.cacheable

        key = (key, self.get_modaliases(), self.get_session_config())

        if self._in_tx_with_ddl:
            self._eql_to_compiled[key] = query_unit
        else:
            self._db._cache_compiled_query(key, query_unit)

    cdef lookup_compiled_query(self, object key):
        if (self._tx_error or
                not self._query_cache_enabled or
                self._in_tx_with_ddl):
            return None

        key = (key, self.get_modaliases(), self.get_session_config())

        if self._in_tx_with_ddl or self._in_tx_with_set:
            query_unit = self._eql_to_compiled.get(key)
        else:
            query_unit, qu_dbver = self._db._eql_to_compiled.get(
                key, DICTDEFAULT)
            if query_unit is not None and qu_dbver != self._db.dbver:
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
            self._in_tx_db_config = self._db_config
            self._in_tx_modaliases = self._modaliases
            self._in_tx_user_schema = self._db.user_schema
            self._in_tx_global_schema = self._db._index._global_schema

        if self._in_tx and not self._txid:
            raise errors.InternalServerError('unset txid in transaction')

        if self._in_tx:
            if query_unit.has_ddl:
                self._in_tx_with_ddl = True
            if query_unit.system_config:
                self._in_tx_with_sysconfig = True
            if query_unit.database_config:
                self._in_tx_with_dbconfig = True
            if query_unit.has_set:
                self._in_tx_with_set = True
            if query_unit.has_role_ddl:
                self._in_tx_with_role_ddl = True
            if query_unit.user_schema is not None:
                self._in_tx_user_schema_pickled = query_unit.user_schema
                self._in_tx_user_schema = None
            if query_unit.global_schema is not None:
                self._in_tx_global_schema_pickled = query_unit.global_schema
                self._in_tx_global_schema = None

    cdef on_error(self, query_unit):
        self.tx_error()

    cdef on_success(self, query_unit, new_types):
        side_effects = 0

        if query_unit.tx_savepoint_rollback:
            # Need to invalidate the cache in case there were
            # SET ALIAS or CONFIGURE or DDL commands.
            self._invalidate_local_cache()

        if not self._in_tx:
            if new_types:
                self._db._update_backend_ids(new_types)
            if query_unit.user_schema is not None:
                self._in_tx_dbver = next_dbver()
                self._db._set_and_signal_new_user_schema(
                    pickle.loads(query_unit.user_schema),
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None
                )
                side_effects |= SideEffects.SchemaChanges
            if query_unit.system_config:
                side_effects |= SideEffects.SystemConfigChanges
            if query_unit.database_config:
                side_effects |= SideEffects.DatabaseConfigChanges
            if query_unit.global_schema is not None:
                side_effects |= SideEffects.GlobalSchemaChanges
                self._db._index.update_global_schema(
                    pickle.loads(query_unit.global_schema))
            if query_unit.has_role_ddl:
                side_effects |= SideEffects.RoleChanges
                self._db._index._server._fetch_roles()
        else:
            if new_types:
                self._in_tx_new_types.update(new_types)

        if query_unit.modaliases is not None:
            self.set_modaliases(query_unit.modaliases)

        if query_unit.tx_commit:
            if not self._in_tx:
                # This shouldn't happen because compiler has
                # checks around that.
                raise errors.InternalServerError(
                    '"commit" outside of a transaction')
            self._config = self._in_tx_config
            self._db_config = self._in_tx_db_config
            self._modaliases = self._in_tx_modaliases

            if self._in_tx_new_types:
                self._db._update_backend_ids(self._in_tx_new_types)
            if query_unit.user_schema is not None:
                self._db._set_and_signal_new_user_schema(
                    pickle.loads(query_unit.user_schema),
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None
                )
                side_effects |= SideEffects.SchemaChanges
            if self._in_tx_with_sysconfig:
                side_effects |= SideEffects.SystemConfigChanges
            if self._in_tx_with_dbconfig:
                side_effects |= SideEffects.DatabaseConfigChanges
            if query_unit.global_schema is not None:
                side_effects |= SideEffects.GlobalSchemaChanges
                self._db._index.update_global_schema(
                    pickle.loads(query_unit.global_schema))
                self._db._index._server._fetch_roles()
            if self._in_tx_with_role_ddl:
                side_effects |= SideEffects.RoleChanges

            self._reset_tx_state()

        elif query_unit.tx_rollback:
            # Note that we might not be in a transaction as we allow
            # ROLLBACKs outside of transaction blocks (just like Postgres).
            # TODO: That said, we should send a *warning* when a ROLLBACK
            # is executed outside of a tx.
            self._reset_tx_state()

        return side_effects

    async def apply_config_ops(self, conn, ops):
        settings = config.get_settings()

        for op in ops:
            setting = settings[op.setting_name]
            if setting.backend_setting:
                continue

            if op.scope is config.ConfigScope.SYSTEM:
                await self._db._index.apply_system_config_op(conn, op)
            elif op.scope is config.ConfigScope.DATABASE:
                self.set_database_config(
                    op.apply(settings, self.get_database_config()),
                )
            elif op.scope is config.ConfigScope.SESSION:
                self.set_session_config(
                    op.apply(settings, self.get_session_config()),
                )

    @staticmethod
    def raise_in_tx_error():
        raise errors.TransactionError(
            'current transaction is aborted, '
            'commands ignored until end of transaction block')


cdef class DatabaseIndex:

    def __init__(self, server, *, std_schema, global_schema, sys_config):
        self._dbs = {}
        self._server = server
        self._sys_config = sys_config
        self._comp_sys_config = config.get_compilation_config(sys_config)
        self._std_schema = std_schema
        self._global_schema = global_schema

    def count_connections(self, dbname: str):
        try:
            db = self._dbs[dbname]
        except KeyError:
            return 0

        return len((<Database>db)._views)

    def get_sys_config(self):
        return self._sys_config

    def get_compilation_system_config(self):
        return self._comp_sys_config

    def update_sys_config(self, sys_config):
        self._sys_config = sys_config
        self._comp_sys_config = config.get_compilation_config(sys_config)

    def get_db(self, dbname):
        try:
            return self._dbs[dbname]
        except KeyError:
            raise errors.UnknownDatabaseError(
                f'database {dbname!r} does not exist')

    def maybe_get_db(self, dbname):
        return self._dbs.get(dbname)

    def get_global_schema(self):
        return self._global_schema

    def update_global_schema(self, global_schema):
        self._global_schema = global_schema

    def register_db(
        self,
        dbname,
        *,
        user_schema,
        db_config,
        reflection_cache,
        backend_ids,
        refresh=False
    ):
        cdef Database db
        db = self._dbs.get(dbname)
        if db is not None:
            if not refresh:
                raise RuntimeError(
                    f'cannot register DB {dbname!r}: it is already registered')
            db._set_and_signal_new_user_schema(
                user_schema, reflection_cache, backend_ids)
        else:
            db = Database(
                self,
                dbname,
                user_schema=user_schema,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
            )
            self._dbs[dbname] = db

    def unregister_db(self, dbname):
        self._dbs.pop(dbname)

    def iter_dbs(self):
        return iter(self._dbs.values())

    async def _save_system_overrides(self, conn):
        data = config.to_json(
            config.get_settings(),
            self._sys_config,
            setting_filter=lambda v: v.source == 'system override',
            include_source=False,
        )
        block = dbops.PLTopBlock()
        dbops.UpdateMetadata(
            dbops.Database(name=defines.EDGEDB_SYSTEM_DB),
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
        db = self.get_db(dbname)
        return (<Database>db)._new_view(user, query_cache)

    def remove_view(self, view: DatabaseConnectionView):
        db = self.get_db(view.dbname)
        return (<Database>db)._remove_view(view)
