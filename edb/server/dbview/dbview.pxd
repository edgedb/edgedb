#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


cimport cython

from libc.stdint cimport uint64_t

from edb.server.cache cimport stmt_cache

cdef DEFAULT_STATE

cpdef enum SideEffects:

    SchemaChanges = 1 << 0
    DatabaseConfigChanges = 1 << 1
    InstanceConfigChanges = 1 << 2
    GlobalSchemaChanges = 1 << 3
    DatabaseChanges = 1 << 4


@cython.final
cdef class CompiledQuery:
    cdef public object query_unit_group
    cdef public object first_extra  # Optional[int]
    cdef public object extra_counts
    cdef public object extra_blobs
    cdef public bint extra_formatted_as_text
    cdef public object extra_type_oids
    cdef public object request
    cdef public object recompiled_cache
    cdef public bint use_pending_func_cache
    cdef public object tag

    cdef bytes make_query_prefix(self)


cdef class DatabaseIndex:
    cdef:
        dict _dbs
        object _server
        object _tenant
        object _sys_config
        object _comp_sys_config
        object _std_schema
        object _global_schema_pickle
        object _default_sysconfig
        object _sys_config_spec
        object _cached_compiler_args

    cdef invalidate_caches(self)
    cdef inline set_current_branches(self)


cdef class Database:

    cdef:
        stmt_cache.StatementsCache _eql_to_compiled
        object _cache_locks
        object _sql_to_compiled
        DatabaseIndex _index
        object _views
        object _introspection_lock
        object _state_serializers
        readonly object user_config_spec

        object _cache_worker_task
        object _cache_queue
        object _cache_notify_task
        object _cache_notify_queue

        uint64_t _tx_seq
        object _active_tx_list
        object _func_cache_gt_tx_seq

        readonly str name
        readonly object schema_version
        readonly object dbver
        readonly object db_config
        readonly bytes user_schema_pickle
        readonly object reflection_cache
        readonly object backend_ids
        readonly object backend_oid_to_id
        readonly object extensions
        readonly object _feature_used_metrics

    cdef _invalidate_caches(self)
    cdef _cache_compiled_query(self, key, compiled)
    cdef _new_view(self, query_cache, protocol_version)
    cdef _remove_view(self, view)
    cdef _observe_auth_ext_config(self)
    cdef _update_backend_ids(self, new_types)
    cdef _set_extensions(
        self,
        extensions,
    )
    cdef _set_feature_used_metrics(self, feature_used_metrics)
    cdef _set_and_signal_new_user_schema(
        self,
        new_schema_pickle,
        schema_version,
        extensions,
        ext_config_settings,
        feature_used_metrics,
        reflection_cache=?,
        backend_ids=?,
        db_config=?,
        start_stop_extensions=?,
    )
    cpdef start_stop_extensions(self)
    cdef get_state_serializer(self, protocol_version)
    cpdef set_state_serializer(self, protocol_version, serializer)
    cdef inline uint64_t tx_seq_begin_tx(self)
    cdef inline tx_seq_end_tx(self, uint64_t seq)


cdef class DatabaseConnectionView:

    cdef:
        Database _db
        bint _query_cache_enabled
        object _protocol_version
        public bint is_transient
        # transient dbviews won't cause an immediate error in
        # ensure_database_not_connected(..., close_frontend_conns=False),
        # which is usually called from `DROP BRANCH` or `CREATE ... FROM`.
        # Although, transient dbviews users should guarantee the transient use
        # of pgcons, because _pg_ensure_database_not_connected() may still time
        # out `DROP BRANCH` if the transient pgcon is not released soon enough.

        # State properties
        object _config
        object _in_tx_config

        object _globals
        object _in_tx_globals

        object _modaliases
        object _in_tx_modaliases

        object _state_serializer
        object _in_tx_state_serializer
        object _command_state_serializer

        tuple _session_state_db_cache
        tuple _session_state_cache

        object _txid
        object _in_tx_db_config
        object _in_tx_savepoints
        object _in_tx_root_user_schema_pickle
        object _in_tx_user_schema_pickle
        object _in_tx_user_schema_version
        object _in_tx_user_config_spec
        object _in_tx_global_schema_pickle
        object _in_tx_new_types
        int _in_tx_dbver
        bint _in_tx
        bint _in_tx_with_ddl
        bint _in_tx_with_sysconfig
        bint _in_tx_with_dbconfig
        bint _in_tx_with_set
        bint _tx_error
        uint64_t _in_tx_seq

        uint64_t _capability_mask

        object _last_comp_state
        int _last_comp_state_id

        object __weakref__

    cdef _reset_tx_state(self)
    cdef inline _check_in_tx_error(self, query_unit_group)

    cdef clear_tx_error(self)
    cdef rollback_tx_to_savepoint(self, name)
    cdef declare_savepoint(self, name, spid)
    cdef recover_aliases_and_config(self, modaliases, config, globals)
    cdef abort_tx(self)

    cpdef in_tx(self)
    cpdef in_tx_error(self)

    cdef cache_compiled_query(self, object key, object query_unit_group)
    cdef lookup_compiled_query(self, object key)
    cdef as_compiled(self, query_req, query_unit_group, bint use_metrics=?)

    cdef tx_error(self)

    cdef start(self, query_unit)
    cdef start_tx(self)
    cdef _apply_in_tx(self, query_unit)
    cdef start_implicit(self, query_unit)
    cdef on_error(self)
    cdef on_success(self, query_unit, new_types)
    cdef commit_implicit_tx(
        self,
        user_schema,
        extensions,
        ext_config_settings,
        global_schema,
        roles,
        cached_reflection,
        feature_used_metrics,
    )

    cdef get_user_config_spec(self)
    cpdef get_config_spec(self)

    cpdef get_session_config(self)
    cdef set_session_config(self, new_conf)

    cpdef get_globals(self)
    cpdef set_globals(self, new_globals)

    cdef get_state_serializer(self)
    cdef set_state_serializer(self, new_serializer)

    cpdef get_database_config(self)
    cdef set_database_config(self, new_conf)

    cdef get_system_config(self)
    cpdef get_compilation_system_config(self)

    cdef set_modaliases(self, new_aliases)
    cpdef get_modaliases(self)

    cdef bytes serialize_state(self)
    cdef bint is_state_desc_changed(self)
    cdef describe_state(self)
    cdef encode_state(self)
    cdef decode_state(self, type_id, data)

    cdef check_capabilities(
        self,
        query_capabilities,
        allowed_capabilities,
        error_constructor,
        reason,
    )
