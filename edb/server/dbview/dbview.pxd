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


cpdef enum SideEffects:

    SchemaChanges = 1 << 0
    DatabaseConfigChanges = 1 << 1
    SystemConfigChanges = 1 << 2
    RoleChanges = 1 << 3


cdef class DatabaseIndex:
    cdef:
        dict _dbs

        object _server

        object _sys_config


cdef class Database:

    cdef:
        str _name
        object _dbver
        object _eql_to_compiled
        DatabaseIndex _index
        object _views

    cdef _signal_ddl(self, new_dbver)
    cdef _invalidate_caches(self)
    cdef _cache_compiled_query(self, key, query_unit)
    cdef _new_view(self, user, query_cache)


cdef class DatabaseConnectionView:

    cdef:
        Database _db
        bint _query_cache_enabled
        object _user

        object _config
        object _modaliases
        object _in_tx_modaliases
        tuple _session_state_cache

        object _eql_to_compiled

        object _txid
        object _in_tx_config
        bint _in_tx
        bint _in_tx_with_ddl
        bint _in_tx_with_role_ddl
        bint _in_tx_with_sysconfig
        bint _in_tx_with_dbconfig
        bint _in_tx_with_set
        bint _tx_error

        object __weakref__

    cdef _invalidate_local_cache(self)
    cdef _reset_tx_state(self)

    cdef rollback_tx_to_savepoint(self, spid, modaliases, config)
    cdef recover_aliases_and_config(self, modaliases, config)
    cdef abort_tx(self)

    cdef in_tx(self)
    cdef in_tx_error(self)

    cdef cache_compiled_query(self, object key, object query_unit)
    cdef lookup_compiled_query(self, object key)

    cdef tx_error(self)

    cdef start(self, query_unit)
    cdef on_error(self, query_unit)
    cdef on_success(self, query_unit)

    cdef get_session_config(self)
    cdef set_session_config(self, new_conf)

    cdef set_modaliases(self, new_aliases)
    cdef get_modaliases(self)

    cdef bytes serialize_state(self)
