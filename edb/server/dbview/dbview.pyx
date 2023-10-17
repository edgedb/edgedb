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
    Optional,
)

import asyncio
import base64
import json
import os.path
import pickle
import struct
import time
import typing
import weakref

import immutables

from edb import errors
from edb.common import lru, uuidgen
from edb import edgeql
from edb.edgeql import qltypes
from edb.schema import schema as s_schema
from edb.server import compiler, defines, config, metrics
from edb.server.compiler import dbstate, enums, sertypes
from edb.pgsql import dbops
from edb.server.compiler_pool import state as compiler_state_mod

cimport cython

from edb.server.protocol.args_ser cimport (
    recode_global,
)

__all__ = (
    'DatabaseIndex',
    'DatabaseConnectionView',
    'SideEffects',
    'Database'
)

cdef DEFAULT_MODALIASES = immutables.Map({None: defines.DEFAULT_MODULE_ALIAS})
cdef DEFAULT_CONFIG = immutables.Map()
cdef DEFAULT_GLOBALS = immutables.Map()
cdef DEFAULT_STATE = json.dumps([]).encode('utf-8')

cdef INT32_PACKER = struct.Struct('!l').pack

cdef int VER_COUNTER = 0
cdef DICTDEFAULT = (None, None)


cdef next_dbver():
    global VER_COUNTER
    VER_COUNTER += 1
    return VER_COUNTER


@cython.final
cdef class QueryRequestInfo:

    def __cinit__(
        self,
        source: edgeql.Source,
        protocol_version: tuple,
        *,
        output_format: compiler.OutputFormat = compiler.OutputFormat.BINARY,
        input_format: compiler.InputFormat = compiler.InputFormat.BINARY,
        expect_one: bint = False,
        implicit_limit: int = 0,
        inline_typeids: bint = False,
        inline_typenames: bint = False,
        inline_objectids: bint = True,
        allow_capabilities: uint64_t = <uint64_t>compiler.Capability.ALL,
    ):
        self.source = source
        self.protocol_version = protocol_version
        self.output_format = output_format
        self.input_format = input_format
        self.expect_one = expect_one
        self.implicit_limit = implicit_limit
        self.inline_typeids = inline_typeids
        self.inline_typenames = inline_typenames
        self.inline_objectids = inline_objectids
        self.allow_capabilities = allow_capabilities

        self.cached_hash = hash((
            self.source.cache_key(),
            self.protocol_version,
            self.output_format,
            self.input_format,
            self.expect_one,
            self.implicit_limit,
            self.inline_typeids,
            self.inline_typenames,
            self.inline_objectids,
        ))

    def __hash__(self):
        return self.cached_hash

    def __eq__(self, other: QueryRequestInfo) -> bool:
        return (
            self.source.cache_key() == other.source.cache_key() and
            self.protocol_version == other.protocol_version and
            self.output_format == other.output_format and
            self.input_format == other.input_format and
            self.expect_one == other.expect_one and
            self.implicit_limit == other.implicit_limit and
            self.inline_typeids == other.inline_typeids and
            self.inline_typenames == other.inline_typenames and
            self.inline_objectids == other.inline_objectids
        )


@cython.final
cdef class CompiledQuery:

    def __init__(
        self,
        query_unit_group: dbstate.QueryUnitGroup,
        first_extra: Optional[int]=None,
        extra_counts=(),
        extra_blobs=()
    ):
        self.query_unit_group = query_unit_group
        self.first_extra = first_extra
        self.extra_counts = extra_counts
        self.extra_blobs = extra_blobs


cdef class Database:

    # Global LRU cache of compiled anonymous queries
    _eql_to_compiled: typing.Mapping[str, dbstate.QueryUnitGroup]

    def __init__(
        self,
        DatabaseIndex index,
        str name,
        *,
        bytes user_schema_pickle,
        object db_config,
        object reflection_cache,
        object backend_ids,
        object extensions,
        object ext_config_settings,
    ):
        self.name = name

        self.dbver = next_dbver()

        self._index = index
        self._views = weakref.WeakSet()
        self._state_serializers = {}

        self._introspection_lock = asyncio.Lock()

        self._eql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)
        self._sql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self.db_config = db_config
        self.user_schema_pickle = user_schema_pickle
        if ext_config_settings is not None:
            self.user_config_spec = config.FlatSpec(*ext_config_settings)
        self.reflection_cache = reflection_cache
        self.backend_ids = backend_ids
        self.extensions = extensions

    @property
    def server(self):
        return self._index._server

    @property
    def tenant(self):
        return self._index._tenant

    cdef schedule_config_update(self):
        self._index._tenant.on_local_database_config_change(self.name)

    cdef _set_and_signal_new_user_schema(
        self,
        new_schema_pickle,
        extensions,
        ext_config_settings,
        reflection_cache=None,
        backend_ids=None,
        db_config=None,
    ):
        if new_schema_pickle is None:
            raise AssertionError('new_schema is not supposed to be None')

        self.dbver = next_dbver()

        self.user_schema_pickle = new_schema_pickle
        self.extensions = extensions
        self.user_config_spec = config.FlatSpec(*ext_config_settings)

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
        self._sql_to_compiled.clear()
        self._index.invalidate_caches()

    cdef _cache_compiled_query(self, key, compiled: dbstate.QueryUnitGroup):
        assert compiled.cacheable

        existing, dbver = self._eql_to_compiled.get(key, DICTDEFAULT)
        if existing is not None and dbver == self.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._eql_to_compiled[key] = compiled, self.dbver

    def cache_compiled_sql(self, key, compiled: list[str]):
        existing, dbver = self._sql_to_compiled.get(key, DICTDEFAULT)
        if existing is not None and dbver == self.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._sql_to_compiled[key] = compiled, self.dbver

    def lookup_compiled_sql(self, key):
        rv, cached_dbver = self._sql_to_compiled.get(key, DICTDEFAULT)
        if rv is not None and cached_dbver != self.dbver:
            rv = None
        return rv

    cdef _new_view(self, query_cache, protocol_version):
        view = DatabaseConnectionView(
            self, query_cache=query_cache, protocol_version=protocol_version
        )
        self._views.add(view)
        return view

    cdef _remove_view(self, view):
        self._views.remove(view)

    cdef get_state_serializer(self, protocol_version):
        return self._state_serializers.get(protocol_version)

    cpdef set_state_serializer(self, protocol_version, serializer):
        old_serializer = self._state_serializers.get(protocol_version)
        if (
            old_serializer is None or
            old_serializer.type_id != serializer.type_id
        ):
            # also invalidate other protocol versions
            self._state_serializers = {protocol_version: serializer}
            return serializer
        else:
            return old_serializer

    def iter_views(self):
        yield from self._views

    def get_query_cache_size(self):
        return len(self._eql_to_compiled) + len(self._sql_to_compiled)

    async def introspection(self):
        if self.user_schema_pickle is None:
            async with self._introspection_lock:
                if self.user_schema_pickle is None:
                    await self.tenant.introspect_db(self.name)


cdef class DatabaseConnectionView:

    _eql_to_compiled: typing.Mapping[bytes, dbstate.QueryUnitGroup]

    def __init__(self, db: Database, *, query_cache, protocol_version):
        self._db = db

        self._query_cache_enabled = query_cache
        self._protocol_version = protocol_version

        self._modaliases = DEFAULT_MODALIASES
        self._config = DEFAULT_CONFIG
        self._globals = DEFAULT_GLOBALS
        self._session_state_db_cache = None
        self._session_state_cache = None
        self._state_serializer = None

        if db.name == defines.EDGEDB_SYSTEM_DB:
            # Make system database read-only.
            self._capability_mask = <uint64_t>(
                compiler.Capability.ALL
                & ~compiler.Capability.DDL
                & ~compiler.Capability.MODIFICATIONS
            )
        else:
            self._capability_mask = <uint64_t>compiler.Capability.ALL

        self._db_config_temp = None
        self._db_config_dbver = None

        self._last_comp_state = None
        self._last_comp_state_id = 0

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
        self._in_tx_globals = None
        self._in_tx_db_config = None
        self._in_tx_modaliases = None
        self._in_tx_savepoints = []
        self._in_tx_with_ddl = False
        self._in_tx_with_sysconfig = False
        self._in_tx_with_dbconfig = False
        self._in_tx_with_set = False
        self._in_tx_user_schema_pickle = None
        self._in_tx_global_schema_pickle = None
        self._in_tx_new_types = {}
        self._in_tx_user_config_spec = None
        self._in_tx_state_serializer = None
        self._tx_error = False
        self._in_tx_dbver = 0
        self._invalidate_local_cache()

    cdef clear_tx_error(self):
        self._tx_error = False

    cdef rollback_tx_to_savepoint(self, name):
        self._tx_error = False
        # See also CompilerConnectionState.rollback_to_savepoint().
        while self._in_tx_savepoints:
            if self._in_tx_savepoints[-1][0] == name:
                break
            else:
                self._in_tx_savepoints.pop()
        else:
            raise RuntimeError(
                f'savepoint {name} not found')

        _, spid, (
            modaliases, config, globals, state_serializer
        ) = self._in_tx_savepoints[-1]
        self._txid = spid
        self.set_modaliases(modaliases)
        self.set_session_config(config)
        self.set_globals(globals)
        self.set_state_serializer(state_serializer)
        self._invalidate_local_cache()

    cdef declare_savepoint(self, name, spid):
        state = (
            self.get_modaliases(),
            self.get_session_config(),
            self.get_globals(),
            self.get_state_serializer(),
        )
        self._in_tx_savepoints.append((name, spid, state))

    cdef recover_aliases_and_config(self, modaliases, config, globals):
        assert not self._in_tx
        self.set_modaliases(modaliases)
        self.set_session_config(config)
        self.set_globals(globals)

    cdef abort_tx(self):
        if not self.in_tx():
            raise errors.InternalServerError('abort_tx(): not in transaction')
        self._reset_tx_state()

    cpdef get_session_config(self):
        if self._in_tx:
            return self._in_tx_config
        else:
            return self._config

    cpdef get_globals(self):
        if self._in_tx:
            return self._in_tx_globals
        else:
            return self._globals

    cdef get_state_serializer(self):
        if self._in_tx:
            return self._in_tx_state_serializer
        else:
            if self._state_serializer is None:
                self._state_serializer = self._db.get_state_serializer(
                    self._protocol_version
                )
            return self._state_serializer

    cdef set_state_serializer(self, new_serializer):
        if self._in_tx:
            if (
                self._in_tx_state_serializer is None or
                self._in_tx_state_serializer.type_id != new_serializer.type_id
            ):
                self._in_tx_state_serializer = new_serializer
        else:
            # Use the same object as the database to avoid duplicate cache
            self._state_serializer = self._db.set_state_serializer(
                self._protocol_version, new_serializer
            )

    cdef get_user_config_spec(self):
        if self._in_tx:
            return self._in_tx_user_config_spec
        else:
            return self._db.user_config_spec

    cpdef get_config_spec(self):
        return config.ChainedSpec(
            self._db._index._sys_config_spec,
            self.get_user_config_spec(),
        )

    cdef set_session_config(self, new_conf):
        if self._in_tx:
            self._in_tx_config = new_conf
        else:
            self._config = new_conf

    cpdef set_globals(self, new_globals):
        if self._in_tx:
            self._in_tx_globals = new_globals
        else:
            self._globals = new_globals

    cpdef get_database_config(self):
        if self._in_tx:
            return self._in_tx_db_config
        else:
            if self._db_config_temp is not None:
                # See `set_database_config()` for an explanation on
                # *why* we do this.
                if self._db_config_dbver is self._db.dbver:
                    assert self._db_config_dbver is not None
                    return self._db_config_temp
                else:
                    self._db_config_temp = None
                    self._db_config_dbver = None

            return self._db.db_config

    cdef update_database_config(self):
        # Unfortunately it's unsafe to just synchronously
        # update `self._db.db_config` to a new config. What if two
        # connections are updating different DB config settings
        # concurrently?
        # The only way to avoid a race here is to always schedule
        # a full DB state sync every time there's a DB config change.
        self._db.schedule_config_update()

    cdef set_database_config(self, new_conf):
        if self._in_tx:
            self._in_tx_db_config = new_conf
        else:
            # The idea here is to save the new DB conf in a temporary
            # storage until the DB state is refreshed by a call to
            # `update_database_config()` from `on_success()`. This is to
            # make it possible to immediately use the updated DB config in
            # this session. This is still racy, but the probability of
            # a race is very low so we go for it (and races like this aren't
            # critical and resolve in time.)
            # Check out `get_database_config()` to see how this is used.
            self._db_config_temp = new_conf
            self._db_config_dbver = self._db.dbver

    cdef get_system_config(self):
        return self._db._index.get_sys_config()

    cpdef get_compilation_system_config(self):
        return self._db._index.get_compilation_system_config()

    cdef set_modaliases(self, new_aliases):
        if self._in_tx:
            self._in_tx_modaliases = new_aliases
        else:
            self._modaliases = new_aliases

    cpdef get_modaliases(self):
        if self._in_tx:
            return self._in_tx_modaliases
        else:
            return self._modaliases

    def get_user_schema_pickle(self):
        if self._in_tx:
            return self._in_tx_user_schema_pickle
        else:
            return self._db.user_schema_pickle

    def get_global_schema_pickle(self):
        if self._in_tx:
            return self._in_tx_global_schema_pickle
        else:
            return self._db._index._global_schema_pickle

    def resolve_backend_type_id(self, type_id):
        type_id = str(type_id)

        if self._in_tx:
            try:
                return int(self._in_tx_new_types[type_id])
            except KeyError:
                pass

        tid = self._db.backend_ids.get(type_id)
        if tid is None:
            raise RuntimeError(
                f'cannot resolve backend OID for type {type_id}')
        return tid

    cdef bytes serialize_state(self):
        cdef list state
        if self._in_tx:
            raise errors.InternalServerError(
                'no need to serialize state while in transaction')

        dbver = self._db.dbver
        if self._session_state_db_cache is not None:
            if self._session_state_db_cache[0] == (self._config, dbver):
                return self._session_state_db_cache[1]

        state = []
        if self._config and self._config != DEFAULT_CONFIG:
            settings = self.get_config_spec()
            for sval in self._config.values():
                setting = settings[sval.name]
                kind = 'B' if setting.backend_setting else 'C'
                jval = config.value_to_json_value(setting, sval.value)
                state.append({"name": sval.name, "value": jval, "type": kind})

        # Include the database version in the state so that we are forced
        # to clear the config cache on dbver changes.
        state.append(
            {"name": '__dbver__', "value": dbver, "type": 'C'})

        spec = json.dumps(state).encode('utf-8')
        self._session_state_db_cache = ((self._config, dbver), spec)
        return spec

    cdef bint is_state_desc_changed(self):
        # We may have executed a query, or COMMIT/ROLLBACK - just use the
        # serializer we preserved before. NOTE: the schema might have been
        # concurrently changed from other sessions, we should not reload
        # serializer from self._db here so that our state can be serialized
        # properly, and the Execute stays atomic.
        serializer = self.get_state_serializer()

        if self._command_state_serializer is not None:
            # If the resulting descriptor is the same as the input, return None
            if serializer.type_id == self._command_state_serializer.type_id:
                if self._in_tx:
                    # There's a case when DDL was executed but the state schema
                    # wasn't affected, so it's enough to keep just one copy.
                    self._in_tx_state_serializer = (
                        self._command_state_serializer
                    )
                return False

            # Update with the new serializer for upcoming encoding
            self._command_state_serializer = serializer

        return True

    cdef describe_state(self):
        return self.get_state_serializer().describe()

    cdef encode_state(self):
        modaliases = self.get_modaliases()
        session_config = self.get_session_config()
        globals_ = self.get_globals()

        if self._session_state_cache is None:
            if (
                session_config == DEFAULT_CONFIG and
                modaliases == DEFAULT_MODALIASES and
                globals_ == DEFAULT_GLOBALS
            ):
                return sertypes.NULL_TYPE_ID, b""

        serializer = self._command_state_serializer
        self._command_state_serializer = None
        if not self.in_tx():
            # After encode_state(), self._state_serializer is no longer used if
            # not in a transaction. So it should be cleared
            self._state_serializer = None

        if self._session_state_cache is not None:
            if (
                modaliases, session_config, globals_, serializer.type_id.bytes
            ) == self._session_state_cache[:4]:
                return sertypes.NULL_TYPE_ID, b""

        self._session_state_cache = None

        state = {}
        try:
            if modaliases[None] != defines.DEFAULT_MODULE_ALIAS:
                state['module'] = modaliases[None]
        except KeyError:
            pass
        else:
            modaliases = modaliases.delete(None)
        if modaliases:
            state['aliases'] = list(modaliases.items())
        if session_config:
            state['config'] = {k: v.value for k, v in session_config.items()}
        if globals_:
            state['globals'] = {k: v.value for k, v in globals_.items()}
        return serializer.type_id, serializer.encode(state)

    cdef decode_state(self, type_id, data):
        serializer = self.get_state_serializer()
        self._command_state_serializer = serializer

        if type_id == sertypes.NULL_TYPE_ID.bytes:
            self.set_modaliases(DEFAULT_MODALIASES)
            self.set_session_config(DEFAULT_CONFIG)
            self.set_globals(DEFAULT_GLOBALS)
            self._session_state_cache = None
            return

        if type_id != serializer.type_id.bytes:
            self._command_state_serializer = None
            raise errors.StateMismatchError(
                "Cannot decode state: type mismatch"
            )

        if self._session_state_cache is not None:
            if type_id == self._session_state_cache[3]:
                if data == self._session_state_cache[4]:
                    return

        state = serializer.decode(data)
        aliases = dict(state.get('aliases', []))
        aliases[None] = state.get('module', defines.DEFAULT_MODULE_ALIAS)
        aliases = immutables.Map(aliases)
        session_config = immutables.Map({
            k: config.SettingValue(
                name=k,
                value=v,
                source='session',
                scope=qltypes.ConfigScope.SESSION,
            ) for k, v in state.get('config', {}).items()
        })
        globals_ = immutables.Map({
            k: config.SettingValue(
                name=k,
                value=recode_global(self, v, serializer.get_global_type_rep(k)),
                source='global',
                scope=qltypes.ConfigScope.GLOBAL,
            ) for k, v in state.get('globals', {}).items()
        })
        self.set_modaliases(aliases)
        self.set_session_config(session_config)
        self.set_globals(globals_)
        self._session_state_cache = (
            aliases, session_config, globals_, type_id, data
        )

    property txid:
        def __get__(self):
            return self._txid

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

    @property
    def server(self):
        return self._db._index._server

    @property
    def tenant(self):
        return self._db._index._tenant

    cpdef in_tx(self):
        return self._in_tx

    cpdef in_tx_error(self):
        return self._tx_error

    cdef cache_compiled_query(self, object key, object query_unit_group):
        assert query_unit_group.cacheable

        key = (key, self.get_modaliases(), self.get_session_config())

        if self._in_tx_with_ddl:
            self._eql_to_compiled[key] = query_unit_group
        else:
            self._db._cache_compiled_query(key, query_unit_group)

    cdef lookup_compiled_query(self, object key):
        if (self._tx_error or
                not self._query_cache_enabled or
                self._in_tx_with_ddl):
            return None

        key = (key, self.get_modaliases(), self.get_session_config())

        if self._in_tx_with_ddl:
            query_unit_group = self._eql_to_compiled.get(key)
        else:
            query_unit_group, qu_dbver = self._db._eql_to_compiled.get(
                key, DICTDEFAULT)
            if query_unit_group is not None and qu_dbver != self._db.dbver:
                query_unit_group = None

        return query_unit_group

    cdef tx_error(self):
        if self._in_tx:
            self._tx_error = True

    cdef start(self, query_unit):
        if self._tx_error:
            self.raise_in_tx_error()

        if query_unit.tx_id is not None:
            self._txid = query_unit.tx_id
            self._start_tx()

        if self._in_tx and not self._txid:
            raise errors.InternalServerError('unset txid in transaction')

        if self._in_tx:
            self._apply_in_tx(query_unit)

    cdef _start_tx(self):
        state_serializer = self.get_state_serializer()
        self._in_tx = True
        self._in_tx_config = self._config
        self._in_tx_globals = self._globals
        self._in_tx_db_config = self._db.db_config
        self._in_tx_modaliases = self._modaliases
        self._in_tx_user_schema_pickle = self._db.user_schema_pickle
        self._in_tx_global_schema_pickle = \
            self._db._index._global_schema_pickle
        self._in_tx_user_config_spec = self._db.user_config_spec
        self._in_tx_state_serializer = state_serializer

    cdef _apply_in_tx(self, query_unit):
        if query_unit.has_ddl:
            self._in_tx_with_ddl = True
        if query_unit.system_config:
            self._in_tx_with_sysconfig = True
        if query_unit.database_config:
            self._in_tx_with_dbconfig = True
        if query_unit.has_set:
            self._in_tx_with_set = True
        if query_unit.user_schema is not None:
            self._in_tx_user_schema_pickle = query_unit.user_schema
            self._in_tx_user_config_spec = config.FlatSpec(
                *query_unit.ext_config_settings
            )
        if query_unit.global_schema is not None:
            self._in_tx_global_schema_pickle = query_unit.global_schema

    cdef start_implicit(self, query_unit):
        if self._tx_error:
            self.raise_in_tx_error()

        if not self._in_tx:
            self._start_tx()

        self._apply_in_tx(query_unit)

    cdef on_error(self):
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
                    query_unit.user_schema,
                    query_unit.extensions,
                    query_unit.ext_config_settings,
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None
                )
                side_effects |= SideEffects.SchemaChanges
            if query_unit.system_config:
                side_effects |= SideEffects.InstanceConfigChanges
            if query_unit.database_config:
                self.update_database_config()
                side_effects |= SideEffects.DatabaseConfigChanges
            if query_unit.create_db:
                side_effects |= SideEffects.DatabaseChanges
            if query_unit.drop_db:
                side_effects |= SideEffects.DatabaseChanges
            if query_unit.global_schema is not None:
                side_effects |= SideEffects.GlobalSchemaChanges
                self._db._index.update_global_schema(query_unit.global_schema)
                self._db.tenant.set_roles(query_unit.roles)
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
            self._modaliases = self._in_tx_modaliases
            self._globals = self._in_tx_globals

            if self._in_tx_new_types:
                self._db._update_backend_ids(self._in_tx_new_types)
            if query_unit.user_schema is not None:
                self._db._set_and_signal_new_user_schema(
                    query_unit.user_schema,
                    query_unit.extensions,
                    query_unit.ext_config_settings,
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None
                )
                side_effects |= SideEffects.SchemaChanges
            if self._in_tx_with_sysconfig:
                side_effects |= SideEffects.InstanceConfigChanges
            if self._in_tx_with_dbconfig:
                self._db_config_temp = self._in_tx_db_config
                self._db_config_dbver = self._db.dbver

                self.update_database_config()
                side_effects |= SideEffects.DatabaseConfigChanges
            if query_unit.global_schema is not None:
                side_effects |= SideEffects.GlobalSchemaChanges
                self._db._index.update_global_schema(query_unit.global_schema)
                self._db.tenant.set_roles(query_unit.roles)

            self._reset_tx_state()

        elif query_unit.tx_rollback:
            # Note that we might not be in a transaction as we allow
            # ROLLBACKs outside of transaction blocks (just like Postgres).
            # TODO: That said, we should send a *warning* when a ROLLBACK
            # is executed outside of a tx.
            self._reset_tx_state()

        return side_effects

    cdef commit_implicit_tx(
        self,
        user_schema,
        extensions,
        ext_config_settings,
        global_schema,
        roles,
        cached_reflection,
    ):
        assert self._in_tx
        side_effects = 0

        self._config = self._in_tx_config
        self._modaliases = self._in_tx_modaliases
        self._globals = self._in_tx_globals

        if self._in_tx_new_types:
            self._db._update_backend_ids(self._in_tx_new_types)
        if user_schema is not None:
            self._db._set_and_signal_new_user_schema(
                user_schema,
                extensions,
                ext_config_settings,
                pickle.loads(cached_reflection)
                    if cached_reflection is not None
                    else None
            )
            side_effects |= SideEffects.SchemaChanges
        if self._in_tx_with_sysconfig:
            side_effects |= SideEffects.InstanceConfigChanges
        if self._in_tx_with_dbconfig:
            self._db_config_temp = self._in_tx_db_config
            self._db_config_dbver = self._db.dbver

            self.update_database_config()
            side_effects |= SideEffects.DatabaseConfigChanges
        if global_schema is not None:
            side_effects |= SideEffects.GlobalSchemaChanges
            self._db._index.update_global_schema(global_schema)
            self._db.tenant.set_roles(roles)

        self._reset_tx_state()
        return side_effects

    async def apply_config_ops(self, conn, ops):
        settings = self.get_config_spec()

        for op in ops:
            if op.scope is config.ConfigScope.INSTANCE:
                await self._db._index.apply_system_config_op(conn, op)
            elif op.scope is config.ConfigScope.DATABASE:
                self.set_database_config(
                    op.apply(settings, self.get_database_config()),
                )
            elif op.scope is config.ConfigScope.SESSION:
                self.set_session_config(
                    op.apply(settings, self.get_session_config()),
                )
            elif op.scope is config.ConfigScope.GLOBAL:
                self.set_globals(
                    op.apply(settings, self.get_globals()),
                )

    @staticmethod
    def raise_in_tx_error():
        raise errors.TransactionError(
            'current transaction is aborted, '
            'commands ignored until end of transaction block'
        ) from None

    async def parse(
        self,
        query_req: QueryRequestInfo,
        cached_globally=False,
        use_metrics=True,
    ) -> CompiledQuery:
        source = query_req.source
        if cached_globally:
            # WARNING: only set cached_globally to True when the query is
            # strictly referring to only shared stable objects in user schema
            # or anything from std schema, for example:
            #     YES:  select ext::auth::UIConfig { ... }
            #     NO:   select default::User { ... }
            query_unit_group = (
                self.server.system_compile_cache.get(query_req)
                if self._query_cache_enabled
                else None
            )
        else:
            query_unit_group = self.lookup_compiled_query(query_req)
        cached = True
        if query_unit_group is None:
            # Cache miss; need to compile this query.
            cached = False

            try:
                query_unit_group = await self._compile(query_req)
            except (errors.EdgeQLSyntaxError, errors.InternalServerError):
                raise
            except errors.EdgeDBError:
                if self.in_tx_error():
                    # Because we are in an error state it is more reasonable
                    # to fail with TransactionError("commands ignored")
                    # rather than with a potentially more cryptic error.
                    # An exception from this rule are syntax errors and
                    # ISEs, because these could arise while the user is
                    # trying to properly rollback this failed transaction.
                    self.raise_in_tx_error()
                else:
                    raise

            self.check_capabilities(
                query_unit_group.capabilities,
                query_req.allow_capabilities,
                errors.DisabledCapabilityError,
                "disabled by the client",
            )

        if self.in_tx_error():
            # The current transaction is aborted, so we must fail
            # all commands except ROLLBACK or ROLLBACK TO SAVEPOINT.
            first = query_unit_group[0]
            if (
                not (
                    first.tx_rollback
                    or first.tx_savepoint_rollback
                    or first.tx_abort_migration
                ) or len(query_unit_group) > 1
            ):
                self.raise_in_tx_error()

        if not cached and query_unit_group.cacheable:
            if cached_globally:
                self.server.system_compile_cache[query_req] = query_unit_group
            else:
                self.cache_compiled_query(query_req, query_unit_group)

        if use_metrics:
            metrics.edgeql_query_compilations.inc(
                1.0,
                self.tenant.get_instance_name(),
                'cache' if cached else 'compiler',
            )

        return CompiledQuery(
            query_unit_group=query_unit_group,
            first_extra=source.first_extra(),
            extra_counts=source.extra_counts(),
            extra_blobs=source.extra_blobs(),
        )

    async def _compile(
        self,
        query_req: QueryRequestInfo,
    ) -> dbstate.QueryUnitGroup:
        compiler_pool = self._db._index._server.get_compiler_pool()

        started_at = time.monotonic()
        try:
            if self.in_tx():
                result = await compiler_pool.compile_in_tx(
                    self.txid,
                    self._last_comp_state,
                    self._last_comp_state_id,
                    query_req.source,
                    query_req.output_format,
                    query_req.expect_one,
                    query_req.implicit_limit,
                    query_req.inline_typeids,
                    query_req.inline_typenames,
                    self._protocol_version,
                    query_req.inline_objectids,
                    query_req.input_format is compiler.InputFormat.JSON,
                    self.in_tx_error(),
                )
            else:
                result = await compiler_pool.compile(
                    self.dbname,
                    self.get_user_schema_pickle(),
                    self.get_global_schema_pickle(),
                    self.reflection_cache,
                    self.get_database_config(),
                    self.get_compilation_system_config(),
                    query_req.source,
                    self.get_modaliases(),
                    self.get_session_config(),
                    query_req.output_format,
                    query_req.expect_one,
                    query_req.implicit_limit,
                    query_req.inline_typeids,
                    query_req.inline_typenames,
                    self._protocol_version,
                    query_req.inline_objectids,
                    query_req.input_format is compiler.InputFormat.JSON,
                    client_id=self.tenant.client_id,
                )
        finally:
            metrics.edgeql_query_compilation_duration.observe(
                time.monotonic() - started_at,
                self.tenant.get_instance_name(),
            )

        unit_group, self._last_comp_state, self._last_comp_state_id = result

        return unit_group

    cdef check_capabilities(
        self,
        query_capabilities,
        allowed_capabilities,
        error_constructor,
        reason,
    ):
        if query_capabilities & ~self._capability_mask:
            # _capability_mask is currently only used for system database
            raise query_capabilities.make_error(
                self._capability_mask,
                errors.UnsupportedCapabilityError,
                "system database is read-only",
            )

        if query_capabilities & ~allowed_capabilities:
            raise query_capabilities.make_error(
                allowed_capabilities,
                error_constructor,
                reason,
            )

        if self.tenant.is_readonly():
            if query_capabilities & enums.Capability.WRITE:
                readiness_reason = self.tenant.get_readiness_reason()
                msg = "the server is currently in read-only mode"
                if readiness_reason:
                    msg = f"{msg}: {readiness_reason}"
                raise query_capabilities.make_error(
                    ~enums.Capability.WRITE,
                    errors.DisabledCapabilityError,
                    msg,
                )

    async def reload_state_serializer(self):
        # This should only happen once when a different protocol version is
        # used after schema change, or non-current version of protocol is used
        # for the first time after database introspection.  Because such cases
        # are rare, we'd rather do it lazily here than enumerating all protocol
        # versions making several serializers in every schema change.
        compiler_pool = self._db._index._server.get_compiler_pool()
        state_serializer = await compiler_pool.make_state_serializer(
            self._protocol_version,
            self.get_user_schema_pickle(),
            self.get_global_schema_pickle(),
        )
        self.set_state_serializer(state_serializer)


cdef class DatabaseIndex:

    def __init__(
        self,
        tenant,
        *,
        std_schema,
        global_schema_pickle,
        sys_config,
        default_sysconfig,  # system config without system override
        sys_config_spec,
    ):
        self._dbs = {}
        self._server = tenant.server
        self._tenant = tenant
        self._std_schema = std_schema
        self._global_schema_pickle = global_schema_pickle
        self._default_sysconfig = default_sysconfig
        self._sys_config_spec = sys_config_spec
        self.update_sys_config(sys_config)
        self._cached_compiler_args = None

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
        cdef Database db
        for db in self._dbs.values():
            db.dbver = next_dbver()

        with self._default_sysconfig.mutate() as mm:
            mm.update(sys_config)
            sys_config = mm.finish()
        self._sys_config = sys_config
        self._comp_sys_config = config.get_compilation_config(
            sys_config, spec=self._sys_config_spec)
        self.invalidate_caches()

    def has_db(self, dbname):
        return dbname in self._dbs

    def get_db(self, dbname):
        try:
            return self._dbs[dbname]
        except KeyError:
            raise errors.UnknownDatabaseError(
                f'database {dbname!r} does not exist')

    def maybe_get_db(self, dbname):
        return self._dbs.get(dbname)

    def get_global_schema_pickle(self):
        return self._global_schema_pickle

    def update_global_schema(self, global_schema_pickle):
        self._global_schema_pickle = global_schema_pickle
        self.invalidate_caches()

    def register_db(
        self,
        dbname,
        *,
        user_schema_pickle,
        db_config,
        reflection_cache,
        backend_ids,
        extensions,
        ext_config_settings,
    ):
        cdef Database db
        db = self._dbs.get(dbname)
        if db is not None:
            db._set_and_signal_new_user_schema(
                user_schema_pickle,
                extensions,
                ext_config_settings,
                reflection_cache,
                backend_ids,
                db_config,
            )
        else:
            db = Database(
                self,
                dbname,
                user_schema_pickle=user_schema_pickle,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
                extensions=extensions,
                ext_config_settings=ext_config_settings,
            )
            self._dbs[dbname] = db
        return db

    def unregister_db(self, dbname):
        self._dbs.pop(dbname)

    def iter_dbs(self):
        return iter(self._dbs.values())

    async def _save_system_overrides(self, conn, spec):
        data = config.to_json(
            spec,
            self._sys_config,
            setting_filter=lambda v: v.source == 'system override',
            include_source=False,
        )
        block = dbops.PLTopBlock()
        metadata = {'sysconfig': json.loads(data)}
        if self._tenant.get_backend_runtime_params().has_create_database:
            dbops.UpdateMetadata(
                dbops.Database(
                    name=self._tenant.get_pg_dbname(defines.EDGEDB_SYSTEM_DB),
                ),
                metadata,
            ).generate(block)
        else:
            dbops.UpdateSingleDBMetadata(
                defines.EDGEDB_SYSTEM_DB, metadata
            ).generate(block)
        await conn.sql_execute(block.to_string().encode())

    async def apply_system_config_op(self, conn, op):
        spec = self._sys_config_spec

        op_value = op.get_setting(spec)
        if op.opcode is not None:
            allow_missing = (
                op.opcode is config.OpCode.CONFIG_REM
                or op.opcode is config.OpCode.CONFIG_RESET
            )
            op_value = op.coerce_value(
                spec, op_value, allow_missing=allow_missing)

        # _save_system_overrides *must* happen before
        # the callbacks below, because certain config changes
        # may cause the backend connection to drop.
        self.update_sys_config(
            op.apply(spec, self._sys_config)
        )

        await self._save_system_overrides(conn, spec)

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

    def new_view(self, dbname: str, *, query_cache: bool, protocol_version):
        db = self.get_db(dbname)
        return (<Database>db)._new_view(query_cache, protocol_version)

    def remove_view(self, view: DatabaseConnectionView):
        db = self.get_db(view.dbname)
        return (<Database>db)._remove_view(view)

    cdef invalidate_caches(self):
        self._cached_compiler_args = None

    def get_cached_compiler_args(self):
        if self._cached_compiler_args is None:
            dbs = immutables.Map()
            for db in self._dbs.values():
                dbs = dbs.set(
                    db.name,
                    compiler_state_mod.PickledDatabaseState(
                        user_schema_pickle=db.user_schema_pickle,
                        reflection_cache=db.reflection_cache,
                        database_config=db.db_config,
                    )
                )
            self._cached_compiler_args = (
                dbs, self._global_schema_pickle, self._comp_sys_config
            )
        return self._cached_compiler_args
