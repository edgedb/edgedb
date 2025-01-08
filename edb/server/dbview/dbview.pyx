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
import copy
import json
import logging
import os.path
import pickle
import struct
import time
import typing
import uuid
import weakref

import immutables

from edb import errors
from edb.common import debug, lru, uuidgen, asyncutil
from edb import edgeql
from edb.edgeql import qltypes
from edb.schema import schema as s_schema
from edb.schema import name as s_name
from edb.server import compiler, defines, config, metrics, pgcon
from edb.server.compiler import dbstate, enums, sertypes
from edb.server.protocol import execute
from edb.pgsql import dbops
from edb.server.compiler_pool import state as compiler_state_mod
from edb.server.pgcon import errors as pgerror

from edb.server.protocol import ai_ext

cimport cython

from edb.server.compiler cimport rpc
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
cdef object logger = logging.getLogger('edb.server')


cdef next_dbver():
    global VER_COUNTER
    VER_COUNTER += 1
    return VER_COUNTER



cdef enum CacheState:
    Pending = 0,
    Present,
    Evicted


@cython.final
cdef class CompiledQuery:

    def __init__(
        self,
        query_unit_group: dbstate.QueryUnitGroup,
        first_extra: Optional[int]=None,
        extra_counts=(),
        extra_blobs=(),
        extra_formatted_as_text: bool = False,
        extra_type_oids: Sequence[int] = (),
        request=None,
        recompiled_cache=None,
        use_pending_func_cache=False,
    ):
        self.query_unit_group = query_unit_group
        self.first_extra = first_extra
        self.extra_counts = extra_counts
        self.extra_blobs = extra_blobs
        self.extra_formatted_as_text = extra_formatted_as_text
        self.extra_type_oids = tuple(extra_type_oids)
        self.request = request
        self.recompiled_cache = recompiled_cache
        self.use_pending_func_cache = use_pending_func_cache

    cdef bytes make_query_prefix(self):
        data = {}
        if self.tag:
            data['tag'] = self.tag
        if data:
            data_bytes = json.dumps(data).encode(defines.EDGEDB_ENCODING)
            return b''.join([b'-- ', data_bytes, b'\n'])
        else:
            return b''


cdef class Database:

    # Global LRU cache of compiled queries
    _eql_to_compiled: stmt_cache.StatementsCache[uuid.UUID, dbstate.QueryUnitGroup]

    def __init__(
        self,
        DatabaseIndex index,
        str name,
        *,
        bytes user_schema_pickle,
        object schema_version,
        object db_config,
        object reflection_cache,
        object backend_ids,
        object extensions,
        object ext_config_settings,
        object feature_used_metrics,
    ):
        self.name = name

        self.schema_version = schema_version
        self.dbver = next_dbver()

        self._index = index
        self._views = weakref.WeakSet()
        self._state_serializers = {}

        self._introspection_lock = asyncio.Lock()

        self._eql_to_compiled = stmt_cache.StatementsCache(
            maxsize=defines._MAX_QUERIES_CACHE_DB)
        self._cache_locks = {}
        self._sql_to_compiled = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        # Tracks the active transactions and their creation sequence. The
        # sequence ID is incremental-only. ID 0 is reserved as a non-exist ID.
        self._tx_seq = 0  # most-recently used transaction sequence ID
        self._active_tx_list = {}  # name it "list" to emphasize the order

        # Also an ordered dict of func cache present in DB but still using
        # inline SQL due to active transactions.
        self._func_cache_gt_tx_seq = {}

        self.db_config = db_config
        self.user_schema_pickle = user_schema_pickle
        if ext_config_settings is not None:
            self.user_config_spec = config.FlatSpec(*ext_config_settings)
        self.reflection_cache = reflection_cache
        self.backend_ids = backend_ids
        if backend_ids is not None:
            self.backend_oid_to_id = {
                v[0]: k for k, v in backend_ids.items()
                if v[0] is not None
            }
        else:
            self.backend_oid_to_id = {}
        self.extensions = set()
        self._set_extensions(extensions)
        self._observe_auth_ext_config()

        self._feature_used_metrics = {}
        self._set_feature_used_metrics(feature_used_metrics)

        self._cache_worker_task = self._cache_queue = None
        self._cache_notify_task = self._cache_notify_queue = None
        self._cache_queue = asyncio.Queue()
        self._cache_worker_task = asyncio.create_task(
            self.monitor(self.cache_worker, 'cache_worker'))
        self._cache_notify_queue = asyncio.Queue()
        self._cache_notify_task = asyncio.create_task(
            self.monitor(self.cache_notifier, 'cache_notifier'))

    @property
    def server(self):
        return self._index._server

    @property
    def tenant(self):
        return self._index._tenant

    def stop(self):
        if self._cache_worker_task:
            self._cache_worker_task.cancel()
            self._cache_worker_task = None
        if self._cache_notify_task:
            self._cache_notify_task.cancel()
            self._cache_notify_task = None
        self._set_extensions(set())
        self._set_feature_used_metrics({})
        self.start_stop_extensions()

    async def monitor(self, worker, name):
        while True:
            try:
                await worker()
            except Exception as ex:
                debug.dump(ex)
                metrics.background_errors.inc(
                    1.0, self.tenant._instance_name, name
                )
                # Give things time to recover, since the likely
                # failure mode here is a failover or some such.
                await asyncio.sleep(0.1)

    async def cache_worker(self):
        while True:
            # First, handle any evictions
            keys = []
            while self._eql_to_compiled.needs_cleanup():
                query_req, unit_group = self._eql_to_compiled.cleanup_one()
                if len(unit_group) == 1 and unit_group.cache_state == 1:
                    keys.append(query_req.get_cache_key())
                    self._func_cache_gt_tx_seq.pop(query_req, None)
                unit_group.cache_state = CacheState.Evicted
            if keys:
                await self.tenant.evict_query_cache(self.name, keys)

            # Now, populate the cache
            # Empty the queue, for batching reasons.
            # N.B: This empty/get_nowait loop is safe because this is
            # an asyncio Queue. If it was threaded, it would be racy.
            ops = [await self._cache_queue.get()]
            while not self._cache_queue.empty():
                ops.append(self._cache_queue.get_nowait())
            # Filter ops for only what we need
            ops = [
                (query_req, units) for query_req, units in ops
                if len(units) == 1
                and units[0].cache_sql
                and units.cache_state == CacheState.Pending
            ]
            if not ops:
                continue

            g = execute.build_cache_persistence_units(ops)
            async with self.tenant.with_pgcon(self.name) as conn:
                try:
                    await g.execute(conn, self)
                except Exception as e:
                    logger.warning("Failed to persist function cache",
                                exc_info=True)
                    continue

            for query_req, units in ops:
                units.cache_state = CacheState.Present
                if self._active_tx_list:
                    # Any active tx would delay the time we flip to func cache
                    units.tx_seq_id = self._tx_seq
                    self._func_cache_gt_tx_seq[query_req] = units
                else:
                    units[0].maybe_use_func_cache()
                self._cache_notify_queue.put_nowait(str(units[0].cache_key))

    cdef inline uint64_t tx_seq_begin_tx(self):
        self._tx_seq += 1
        self._active_tx_list[self._tx_seq] = True
        return self._tx_seq

    cdef inline tx_seq_end_tx(self, uint64_t seq):
        # Remove the ending transaction from the active list
        if not self._active_tx_list.pop(seq, False):
            return

        # Stop early if we don't have func cache to activate
        if not self._func_cache_gt_tx_seq:
            return

        if self._active_tx_list:
            # Grab the seq ID of the oldest active transaction
            active_tx = next(iter(self._active_tx_list.keys()))
        else:
            # If all tx ended, we should just activate all pending func cache
            for units in self._func_cache_gt_tx_seq.values():
                units[0].maybe_use_func_cache()
            self._func_cache_gt_tx_seq.clear()
            return

        # Or else, keep activating func cache until the oldest active tx
        drops = []
        for query_req, units in self._func_cache_gt_tx_seq.items():
            if units.tx_seq_id < active_tx:
                units[0].maybe_use_func_cache()
                drops.append(query_req)
            else:
                break
        for query_req in drops:
            self._func_cache_gt_tx_seq.pop(query_req)

    async def cache_notifier(self):
        await asyncutil.debounce(
            lambda: self._cache_notify_queue.get(),
            lambda keys: self.tenant.signal_sysevent(
                'query-cache-changes',
                dbname=self.name,
                keys=keys,
            ),
            max_wait=1.0,
            delay_amt=0.2,
            # 100 keys will take up about 4000 bytes, which
            # fits in the 8000 allowed in events.
            max_batch_size=100,
        )

    cdef _set_extensions(self, extensions):
        # Update metrics about extension use
        tname = self.tenant.get_instance_name()
        for ext in self.extensions:
            if ext not in extensions:
                metrics.extension_used.dec(1, tname, ext)

        for ext in extensions:
            if ext not in self.extensions:
                metrics.extension_used.inc(1, tname, ext)

        self.extensions = extensions

    cdef _set_feature_used_metrics(self, feature_used_metrics):
        # Update metrics about feature use
        #
        # We store the old feature use metrics so that we can
        # incrementally update them after DDL without needing to look
        # at the other database branches
        if feature_used_metrics is None:
            return

        tname = self.tenant.get_instance_name()
        keys = self._feature_used_metrics.keys() | feature_used_metrics.keys()
        for key in keys:
            metrics.feature_used.inc(
                feature_used_metrics.get(key, 0.0)
                - self._feature_used_metrics.get(key, 0.0),
                tname,
                key,
            )

        self._feature_used_metrics = feature_used_metrics

    cdef _set_and_signal_new_user_schema(
        self,
        new_schema_pickle,
        schema_version,
        extensions,
        ext_config_settings,
        feature_used_metrics,
        reflection_cache=None,
        backend_ids=None,
        db_config=None,
        start_stop_extensions=True,
    ):
        if new_schema_pickle is None:
            raise AssertionError('new_schema is not supposed to be None')

        self.schema_version = schema_version
        self.dbver = next_dbver()

        self.user_schema_pickle = new_schema_pickle
        self._set_extensions(extensions)
        self.user_config_spec = config.FlatSpec(*ext_config_settings)

        self._set_feature_used_metrics(feature_used_metrics)

        if backend_ids is not None:
            self.backend_ids = backend_ids
            self.backend_oid_to_id = {
                v[0]: k for k, v in backend_ids.items()
                if v[0] is not None
            }
        if reflection_cache is not None:
            self.reflection_cache = reflection_cache
        if db_config is not None:
            self.db_config = db_config
            self._observe_auth_ext_config()
        self._invalidate_caches()
        if start_stop_extensions:
            self.start_stop_extensions()

    cpdef start_stop_extensions(self):
        if "ai" in self.extensions:
            ai_ext.start_extension(self.tenant, self.name)
        else:
            ai_ext.stop_extension(self.tenant, self.name)

    cdef _observe_auth_ext_config(self):
        key = "ext::auth::AuthConfig::providers"
        if (
            self.db_config is not None and
            self.user_config_spec is not None and
            key in self.user_config_spec
        ):
            providers = config.lookup(
                key,
                self.db_config,
                spec=self.user_config_spec,
            )
            metrics.auth_providers.set(
                len(providers),
                self.tenant.get_instance_name(),
                self.name,
            )

    cdef _update_backend_ids(self, new_types):
        self.backend_ids.update(new_types)
        self.backend_oid_to_id.update({
            v[0]: k for k, v in new_types.items()
            if v[0] is not None
        })

    cdef _invalidate_caches(self):
        self._sql_to_compiled.clear()
        self._index.invalidate_caches()

    cdef _cache_compiled_query(self, key, compiled: dbstate.QueryUnitGroup):
        # `dbver` must be the schema version `compiled` was compiled upon
        assert compiled.cacheable

        if key in self._eql_to_compiled:
            # We already have a cached query for the current user schema
            return

        self._eql_to_compiled[key] = compiled

        if self._cache_queue is not None:
            self._cache_queue.put_nowait((key, compiled))

    def cache_compiled_sql(self, key, compiled: list[str], schema_version):
        existing, ver = self._sql_to_compiled.get(key, DICTDEFAULT)
        if existing is not None and ver == self.schema_version:
            # We already have a cached query for a more recent DB version.
            return
        if not all(unit.cacheable for unit in compiled):
            return

        # Store the matching schema version, see also the comments at origin
        self._sql_to_compiled[key] = compiled, schema_version

    def lookup_compiled_sql(self, key):
        rv, cached_ver = self._sql_to_compiled.get(key, DICTDEFAULT)
        if rv is not None and cached_ver != self.schema_version:
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

    def hydrate_cache(self, query_cache):
        warning_count = 0
        for _, in_data, out_data in query_cache:
            try:
                query_req = rpc.CompilationRequest.deserialize(
                    in_data,
                    "<unknown>",
                    self.server.compilation_config_serializer,
                )

                if query_req not in self._eql_to_compiled:
                    unit = dbstate.QueryUnit.deserialize(out_data)
                    group = dbstate.QueryUnitGroup()
                    group.append(unit, serialize=False)
                    group.cache_state = CacheState.Present
                    if self._active_tx_list:
                        # Any active transaction would delay the time we flip
                        # to function cache
                        group.tx_seq_id = self._tx_seq
                        self._func_cache_gt_tx_seq[query_req] = group
                    else:
                        group[0].maybe_use_func_cache()
                    self._eql_to_compiled[query_req] = group
            except Exception as e:
                if warning_count < 0:
                    warning_count -= 1
                elif warning_count < 10:
                    logger.warning("skipping incompatible cache item: %s", e)
                    warning_count += 1
                else:
                    logger.warning(
                        "too many incompatible cache items, "
                        "skipping the following warnings"
                    )
                    warning_count = -warning_count - 1
        if warning_count < 0:
            logger.warning(
                "skipped %d incompatible cache items", -warning_count
            )

    def clear_query_cache(self):
        self._eql_to_compiled.clear()

    def iter_views(self):
        yield from self._views

    def get_query_cache_size(self):
        return len(self._eql_to_compiled) + len(self._sql_to_compiled)

    async def introspection(self):
        if self.user_schema_pickle is None:
            async with self._introspection_lock:
                if self.user_schema_pickle is None:
                    await self.tenant.introspect_db(self.name)

    def is_introspected(self):
        return self.user_schema_pickle is not None

    def lookup_config(self, name: str):
        spec = self._index._sys_config_spec
        if self.user_config_spec is not None:
            spec = config.ChainedSpec(spec, self.user_config_spec)
        return config.lookup(
            name,
            self.db_config or DEFAULT_CONFIG,
            self._index._sys_config,
            spec=spec,
        )


cdef class DatabaseConnectionView:

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

        self._last_comp_state = None
        self._last_comp_state_id = 0

        self._in_tx_seq = 0
        self._reset_tx_state()

    def __del__(self):
        # In any case if _reset_tx_state() is not called, remove self from
        # ACTIVE_TX_LIST to be safe
        self._db.tx_seq_end_tx(self._in_tx_seq)

    cdef _reset_tx_state(self):
        self._db.tx_seq_end_tx(self._in_tx_seq)
        self._in_tx_seq = 0
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
        self._in_tx_root_user_schema_pickle = None
        self._in_tx_user_schema_pickle = None
        self._in_tx_user_schema_version = None
        self._in_tx_global_schema_pickle = None
        self._in_tx_new_types = {}
        self._in_tx_user_config_spec = None
        self._in_tx_state_serializer = None
        self._tx_error = False
        self._in_tx_dbver = 0

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
            return self._db.db_config

    cdef set_database_config(self, new_conf):
        if self._in_tx:
            self._in_tx_db_config = new_conf
        else:
            # N.B: If we *aren't* in a transaction, we rely on calling
            # process_side_effects() promptly to introspect the new
            # state.
            # (We do it this way to avoid potential races between
            # multiple connections do db configs.)
            pass


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
                tinfo = self._in_tx_new_types[type_id]
            except KeyError:
                pass
            else:
                return int(tinfo[0])

        tinfo = self._db.backend_ids.get(type_id)
        if tinfo is None:
            raise RuntimeError(
                f'cannot resolve backend OID for type {type_id}')

        return int(tinfo[0])

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

    property schema_version:
        def __get__(self):
            if self._in_tx and self._in_tx_user_schema_version:
                return self._in_tx_user_schema_version
            return self._db.schema_version

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

        if self._tx_error or self._in_tx_with_ddl:
            return

        self._db._cache_compiled_query(key, query_unit_group)

    cdef lookup_compiled_query(self, object key):
        if (self._tx_error or
                not self._query_cache_enabled or
                self._in_tx_with_ddl):
            return None

        return self._db._eql_to_compiled.get(key, None)

    cdef tx_error(self):
        if self._in_tx:
            self._tx_error = True

    cdef start(self, query_unit):
        if self._tx_error:
            self.raise_in_tx_error()

        if query_unit.tx_id is not None:
            self._txid = query_unit.tx_id
            self.start_tx()

        if self._in_tx:
            self._apply_in_tx(query_unit)

    cdef start_tx(self):
        state_serializer = self.get_state_serializer()
        self._in_tx = True
        self._in_tx_config = self._config
        self._in_tx_globals = self._globals
        self._in_tx_db_config = self._db.db_config
        self._in_tx_modaliases = self._modaliases
        self._in_tx_root_user_schema_pickle = self._db.user_schema_pickle
        self._in_tx_user_schema_pickle = self._db.user_schema_pickle
        self._in_tx_user_schema_version = self._db.schema_version
        self._in_tx_global_schema_pickle = \
            self._db._index._global_schema_pickle
        self._in_tx_user_config_spec = self._db.user_config_spec
        self._in_tx_state_serializer = state_serializer
        self._in_tx_dbver = self._db.dbver
        self._in_tx_seq = self._db.tx_seq_begin_tx()

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
            self._in_tx_dbver = next_dbver()
            self._in_tx_user_schema_pickle = query_unit.user_schema
            self._in_tx_user_schema_version = query_unit.user_schema_version
            self._in_tx_user_config_spec = config.FlatSpec(
                *query_unit.ext_config_settings
            )
        if query_unit.global_schema is not None:
            self._in_tx_global_schema_pickle = query_unit.global_schema

    cdef start_implicit(self, query_unit):
        if self._tx_error:
            self.raise_in_tx_error()

        if not self._in_tx:
            self.start_tx()

        self._apply_in_tx(query_unit)

    cdef on_error(self):
        self.tx_error()

    cdef on_success(self, query_unit, new_types):
        side_effects = 0

        if not self._in_tx:
            if new_types:
                self._db._update_backend_ids(new_types)
            if query_unit.user_schema is not None:
                self._db._set_and_signal_new_user_schema(
                    query_unit.user_schema,
                    query_unit.user_schema_version,
                    query_unit.extensions,
                    query_unit.ext_config_settings,
                    query_unit.feature_used_metrics,
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None,
                )
                side_effects |= SideEffects.SchemaChanges
            if query_unit.system_config:
                side_effects |= SideEffects.InstanceConfigChanges
            if query_unit.database_config:
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
                    query_unit.user_schema_version,
                    query_unit.extensions,
                    query_unit.ext_config_settings,
                    query_unit.feature_used_metrics,  # XXX? does this get set?
                    pickle.loads(query_unit.cached_reflection)
                        if query_unit.cached_reflection is not None
                        else None,
                )
                side_effects |= SideEffects.SchemaChanges
            if self._in_tx_with_sysconfig:
                side_effects |= SideEffects.InstanceConfigChanges
            if self._in_tx_with_dbconfig:
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
        feature_used_metrics,
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
                self._in_tx_user_schema_version,
                extensions,
                ext_config_settings,
                feature_used_metrics,
                pickle.loads(cached_reflection)
                    if cached_reflection is not None
                    else None
            )
            side_effects |= SideEffects.SchemaChanges
        if self._in_tx_with_sysconfig:
            side_effects |= SideEffects.InstanceConfigChanges
        if self._in_tx_with_dbconfig:
            side_effects |= SideEffects.DatabaseConfigChanges
        if global_schema is not None:
            side_effects |= SideEffects.GlobalSchemaChanges
            self._db._index.update_global_schema(global_schema)
            self._db.tenant.set_roles(roles)

        self._reset_tx_state()
        return side_effects

    async def recompile_cached_queries(self, user_schema, schema_version):
        compiler_pool = self.server.get_compiler_pool()
        compile_concurrency = max(1, compiler_pool.get_size_hint() // 2)
        concurrency_control = asyncio.Semaphore(compile_concurrency)
        rv = []

        recompile_timeout = self.server.config_lookup(
            "auto_rebuild_query_cache_timeout",
            self.get_session_config(),
            self.get_database_config(),
            self.get_system_config(),
        )

        loop = asyncio.get_running_loop()
        t0 = loop.time()
        if recompile_timeout is not None:
            stop_time = t0 + recompile_timeout.to_microseconds() / 1e6
        else:
            stop_time = None

        async def recompile_request(query_req: rpc.CompilationRequest):
            async with concurrency_control:
                try:
                    if stop_time is not None and loop.time() > stop_time:
                        return

                    database_config = self.get_database_config()
                    system_config = self.get_compilation_system_config()
                    query_req = copy.copy(query_req)
                    query_req.set_schema_version(schema_version)
                    query_req.set_database_config(database_config)
                    query_req.set_system_config(system_config)
                    async with asyncio.timeout_at(stop_time):
                        unit_group, _, _ = await compiler_pool.compile(
                            self.dbname,
                            user_schema,
                            self.get_global_schema_pickle(),
                            self.reflection_cache,
                            database_config,
                            system_config,
                            query_req.serialize(),
                            "<unknown>",
                            client_id=self.tenant.client_id,
                        )
                except Exception:
                    # ignore cache entry that cannot be recompiled
                    pass
                else:
                    rv.append((query_req, unit_group))

        async with asyncio.TaskGroup() as g:
            req: rpc.CompilationRequest
            # Reversed so that we compile more recently used first.
            for req, grp in reversed(self._db._eql_to_compiled.items()):
                if (
                    len(grp) == 1
                    # Only recompile queries from the *latest* version,
                    # to avoid quadratic slowdown problems.
                    and req.schema_version == self.schema_version
                ):
                    g.create_task(recompile_request(req))

        return rv

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
        query_req: rpc.CompilationRequest,
        cached_globally: bint = False,
        use_metrics: bint = True,
        allow_capabilities: uint64_t = <uint64_t>compiler.Capability.ALL,
        pgcon: pgcon.PGConnection | None = None,
    ) -> CompiledQuery:
        query_unit_group = None
        if self._query_cache_enabled:
            if cached_globally:
                # WARNING: only set cached_globally to True when the query is
                # strictly referring to only shared stable objects in user
                # schema or anything from std schema, for example:
                #     YES:  select ext::auth::UIConfig { ... }
                #     NO:   select default::User { ... }
                query_unit_group = (
                    self.server.system_compile_cache.get(query_req)
                )
            else:
                query_unit_group = self.lookup_compiled_query(query_req)

            # Fast-path to skip all the locks if it's a cache HIT
            if query_unit_group is not None:
                return self.as_compiled(
                    query_req, query_unit_group, use_metrics)

        lock = None
        schema_version = self.schema_version

        # Lock on the query compilation to avoid other coroutines running
        # the same compile and waste computational resources
        if cached_globally:
            lock_table = self.server.system_compile_cache_locks
        else:
            lock_table = self._db._cache_locks
        while True:
            # We need a loop here because schema_version is a part of the key,
            # there could be a DDL while we're waiting for the lock.
            lock = lock_table.get(query_req)
            if lock is None:
                lock = asyncio.Lock()
                lock_table[query_req] = lock
            await lock.acquire()
            if self.schema_version == schema_version:
                break
            else:
                lock.release()
                if not lock._waiters:
                    del lock_table[query_req]
                schema_version = self.schema_version
                # Updating the schema_version will make query_req a new key
                query_req.set_schema_version(schema_version)

        try:
            # Check the cache again with the lock acquired
            if self._query_cache_enabled:
                if cached_globally:
                    query_unit_group = (
                        self.server.system_compile_cache.get(query_req)
                    )
                else:
                    query_unit_group = self.lookup_compiled_query(query_req)
                if query_unit_group is not None:
                    return self.as_compiled(
                        query_req, query_unit_group, use_metrics)

            try:
                query_unit_group = await self._compile(query_req)
            except (errors.EdgeQLSyntaxError, errors.InternalServerError):
                raise
            except errors.EdgeDBError:
                if self.in_tx_error():
                    # Because we are in an error state it's more reasonable
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
                allow_capabilities,
                errors.DisabledCapabilityError,
                "disabled by the client",
            )
            self._check_in_tx_error(query_unit_group)

            if query_req.input_language is enums.InputLanguage.SQL:
                if len(query_unit_group) > 1:
                    raise errors.UnsupportedFeatureError(
                        "multi-statement SQL scripts are not supported yet"
                    )

                if pgcon is None:
                    raise errors.InternalServerError(
                        "a valid backend connection is required to fully "
                        "compile a query in SQL mode",
                    )
                await self._amend_typedesc_in_sql(
                    query_req,
                    query_unit_group,
                    pgcon,
                )

            if self._query_cache_enabled and query_unit_group.cacheable:
                if cached_globally:
                    self.server.system_compile_cache[query_req] = (
                        query_unit_group
                    )
                else:
                    self.cache_compiled_query(query_req, query_unit_group)
        finally:
            if lock is not None:
                lock.release()
                if not lock._waiters:
                    del lock_table[query_req]

        recompiled_cache = None
        if (
            not self.in_tx()
            or len(query_unit_group) > 0
            and query_unit_group[0].tx_commit
        ):
            # Recompile all cached queries if:
            #  * Issued a DDL or committing a tx with DDL (recompilation
            #    before in-tx DDL needs to fix _in_tx_with_ddl caching 1st)
            #  * Config.auto_rebuild_query_cache is turned on
            #
            # Ideally we should compute the proper user_schema, database_config
            # and system_config for recompilation from server/compiler.py with
            # proper handling of config values. For now we just use the values
            # in the current dbview and not support certain marginal cases.
            user_schema = None
            user_schema_version = None
            for unit in query_unit_group:
                if unit.tx_rollback:
                    break
                if unit.user_schema:
                    user_schema = unit.user_schema
                    user_schema_version = unit.user_schema_version
            if user_schema and not self.server.config_lookup(
                "auto_rebuild_query_cache",
                self.get_session_config(),
                self.get_database_config(),
                self.get_system_config(),
            ):
                user_schema = None
            if user_schema:
                recompiled_cache = await self.recompile_cached_queries(
                    user_schema, user_schema_version
                )

        if use_metrics:
            metrics.edgeql_query_compilations.inc(
                1.0, self.tenant.get_instance_name(), 'compiler'
            )

        source = query_req.source
        return CompiledQuery(
            query_unit_group=query_unit_group,
            first_extra=source.first_extra(),
            extra_counts=source.extra_counts(),
            extra_blobs=source.extra_blobs(),
            extra_formatted_as_text=source.extra_formatted_as_text(),
            extra_type_oids=source.extra_type_oids(),
            request=query_req,
            recompiled_cache=recompiled_cache,
        )

    async def _amend_typedesc_in_sql(
        self,
        query_req: rpc.CompilationRequest,
        qug: dbstate.QueryUnitGroup,
        pgcon: pgcon.PGConnection,
    ) -> None:
        # The SQL QueryUnitGroup as initially returned from the compiler
        # is missing the input/output type descriptors because we currently
        # don't run static SQL type inference.  To mend that we ask Postgres
        # to infer the the result types (as an OID tuple) and then use
        # our OID -> scalar type mapping to construct an EdgeQL free shape with
        # corresponding properties which we then send to the compiler to
        # compute the type descriptors.
        to_describe = []

        desc_map = {}
        source = query_req.source
        first_extra = source.first_extra()
        num_injected_params = 0
        if qug.globals is not None:
            num_injected_params += len(qug.globals)
        if first_extra is not None:
            extra_type_oids = source.extra_type_oids()
            all_type_oids = [0] * first_extra + extra_type_oids
            num_injected_params += len(extra_type_oids)
        else:
            all_type_oids = []

        for i, query_unit in enumerate(qug):
            intro_sql = query_unit.introspection_sql
            if intro_sql is None:
                intro_sql = query_unit.sql
            try:
                param_desc, result_desc = await pgcon.sql_describe(
                    intro_sql, all_type_oids)
            except pgerror.BackendError as ex:
                ex._from_sql = True
                if 'P' in ex.fields:
                    ex.fields['P'] = str(
                        int(ex.fields['P']) - query_unit.sql_prefix_len
                    )
                if query_unit.source_map:
                    ex._source_map = query_unit.source_map

                raise

            result_types = []
            for col, toid in result_desc:
                edb_type_id = self._db.backend_oid_to_id.get(toid)
                if edb_type_id is None:
                    raise errors.UnsupportedFeatureError(
                        f"unsupported SQL type in column \"{col}\" "
                        f"with type OID {toid}"
                    )

                result_types.append((col, edb_type_id))
            params = []
            if num_injected_params:
                param_desc = param_desc[:-num_injected_params]
            for pi, toid in enumerate(param_desc):
                edb_type_id = self._db.backend_oid_to_id.get(toid)
                if edb_type_id is None:
                    raise errors.UnsupportedFeatureError(
                        f"unsupported type in SQL parameter ${pi} "
                        f"with type OID {toid}"
                    )

                params.append(edb_type_id)

            to_describe.append((params, result_types))
            desc_map[len(to_describe) - 1] = i

        if to_describe:
            desc_qug = await self._compile_sql_descriptors(
                query_req, to_describe)

            for i, desc_qu in enumerate(desc_qug):
                qu_i = desc_map[i]

                if query_req.output_format is not enums.OutputFormat.NONE:
                    qug[qu_i].out_type_data = desc_qu[1][0]
                    qug[qu_i].out_type_id = desc_qu[1][1]

                qug[qu_i].in_type_data = desc_qu[0][0]
                qug[qu_i].in_type_id = desc_qu[0][1]
                qug[qu_i].in_type_args = desc_qu[0][2]
                qug[qu_i].in_type_args_real_count = desc_qu[0][3]

            # XXX We don't support SQL scripts just yet, so for now
            # we can just copy the last QU's descriptors and
            # apply them to the whole group (IOW a group is really
            # a group of ONE now.)
            # In near future we'll need to properly implement arg
            # remap.
            if query_req.output_format is not enums.OutputFormat.NONE:
                qug.out_type_data = desc_qug[-1][1][0]
                qug.out_type_id = desc_qug[-1][1][1]

            qug.in_type_data = desc_qug[-1][0][0]
            qug.in_type_id = desc_qug[-1][0][1]
            qug.in_type_args = desc_qug[-1][0][2]
            qug.in_type_args_real_count = desc_qug[-1][0][3]

    cdef inline _check_in_tx_error(self, query_unit_group):
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

    cdef as_compiled(self, query_req, query_unit_group, bint use_metrics=True):
        cdef use_pending_func_cache = False
        if query_unit_group.cache_state == 1:
            use_pending_func_cache = (
                not self._in_tx_seq
                or self._in_tx_seq > query_unit_group.tx_seq_id
            )

        self._check_in_tx_error(query_unit_group)
        if use_metrics:
            metrics.edgeql_query_compilations.inc(
                1.0, self.tenant.get_instance_name(), 'cache'
            )

        source = query_req.source
        return CompiledQuery(
            query_unit_group=query_unit_group,
            first_extra=source.first_extra(),
            extra_counts=source.extra_counts(),
            extra_blobs=source.extra_blobs(),
            extra_formatted_as_text=source.extra_formatted_as_text(),
            extra_type_oids=source.extra_type_oids(),
            use_pending_func_cache=use_pending_func_cache,
        )

    async def _compile(
        self,
        query_req: rpc.CompilationRequest,
    ) -> dbstate.QueryUnitGroup:
        compiler_pool = self._db._index._server.get_compiler_pool()

        started_at = time.monotonic()
        try:
            if self.in_tx():
                result = await compiler_pool.compile_in_tx(
                    self.dbname,
                    self._in_tx_root_user_schema_pickle,
                    self.txid,
                    self._last_comp_state,
                    self._last_comp_state_id,
                    query_req.serialize(),
                    query_req.source.text(),
                    self.in_tx_error(),
                    client_id=self.tenant.client_id,
                )
            else:
                result = await compiler_pool.compile(
                    self.dbname,
                    self.get_user_schema_pickle(),
                    self.get_global_schema_pickle(),
                    self.reflection_cache,
                    self.get_database_config(),
                    self.get_compilation_system_config(),
                    query_req.serialize(),
                    query_req.source.text(),
                    client_id=self.tenant.client_id,
                )
        finally:
            metrics.edgeql_query_compilation_duration.observe(
                time.monotonic() - started_at,
                self.tenant.get_instance_name(),
            )
            metrics.query_compilation_duration.observe(
                time.monotonic() - started_at,
                self.tenant.get_instance_name(),
                "edgeql",
            )

        unit_group, self._last_comp_state, self._last_comp_state_id = result

        return unit_group

    async def _compile_sql_descriptors(
        self,
        query_req: rpc.CompilationRequest,
        types_in_out: defines.ProtocolVersion,
    ) -> dbstate.QueryUnitGroup:
        compiler_pool = self._db._index._server.get_compiler_pool()

        cfg_ser = self.server.compilation_config_serializer
        req = rpc.CompilationRequest(
            source=rpc.SQLParamsSource(types_in_out),
            protocol_version=query_req.protocol_version,
            schema_version=query_req.schema_version,
            input_language=enums.InputLanguage.SQL_PARAMS,
            compilation_config_serializer=cfg_ser,
        )

        return await self._compile(req)

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

        return sum(1 for dbv in (<Database>db)._views if not dbv.is_transient)

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
                f'database branch {dbname!r} does not exist')

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
        schema_version,
        db_config,
        reflection_cache,
        backend_ids,
        extensions,
        ext_config_settings,
        early=False,
        feature_used_metrics=None,
    ):
        cdef Database db
        db = self._dbs.get(dbname)
        if db is not None:
            db._set_and_signal_new_user_schema(
                user_schema_pickle,
                schema_version,
                extensions,
                ext_config_settings,
                feature_used_metrics,
                reflection_cache,
                backend_ids,
                db_config,
                not early,
            )
        else:
            db = Database(
                self,
                dbname,
                user_schema_pickle=user_schema_pickle,
                schema_version=schema_version,
                db_config=db_config,
                reflection_cache=reflection_cache,
                backend_ids=backend_ids,
                extensions=extensions,
                ext_config_settings=ext_config_settings,
                feature_used_metrics=feature_used_metrics,
            )
            self._dbs[dbname] = db
            if not early:
                db.start_stop_extensions()
        self.set_current_branches()
        return db

    def unregister_db(self, dbname):
        db = self._dbs.pop(dbname)
        db.stop()
        self.set_current_branches()

    cdef inline set_current_branches(self):
        metrics.current_branches.set(
            sum(
                1
                for dbname in self._dbs
                if dbname != defines.EDGEDB_SYSTEM_DB
            ),
            self._tenant.get_instance_name(),
        )

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

    def lookup_config(self, name: str):
        return config.lookup(
            name,
            self._sys_config,
            spec=self._sys_config_spec,
        )
