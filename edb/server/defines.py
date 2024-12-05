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


from __future__ import annotations
from typing import TypeAlias

import enum

from edb import buildmeta

from edb.common import enum as s_enum
from edb.schema import defines as s_def


EDGEDB_PORT = 5656
EDGEDB_REMOTE_COMPILER_PORT = 5660
EDGEDB_SUPERGROUP = 'edgedb_supergroup'
EDGEDB_SUPERUSER = s_def.EDGEDB_SUPERUSER
EDGEDB_OLD_SUPERUSER = s_def.EDGEDB_OLD_SUPERUSER
EDGEDB_TEMPLATE_DB = s_def.EDGEDB_TEMPLATE_DB
EDGEDB_OLD_DEFAULT_DB = 'edgedb'
EDGEDB_SUPERUSER_DB = 'main'
EDGEDB_SYSTEM_DB = s_def.EDGEDB_SYSTEM_DB
EDGEDB_ENCODING = 'utf-8'
EDGEDB_VISIBLE_METADATA_PREFIX = r'Gel metadata follows, do not modify.\n'

EDGEDB_SPECIAL_DBS = s_def.EDGEDB_SPECIAL_DBS

EDGEDB_CATALOG_VERSION = buildmeta.EDGEDB_CATALOG_VERSION
MIN_POSTGRES_VERSION = (14, 0)

# Resource limit on open FDs for the server process.
# By default, at least on macOS, the max number of open FDs
# is 256, which is low and can cause 'edb test' to hang.
# We try to bump the rlimit on server start if pemitted.
EDGEDB_MIN_RLIMIT_NOFILE = 2048

BACKEND_CONNECTIONS_MIN = 4
BACKEND_COMPILER_POOL_SIZE_MIN = 1

# The time in seconds to wait before restarting the template compiler process
# after it exits unexpectedly.
BACKEND_COMPILER_TEMPLATE_PROC_RESTART_INTERVAL = 1

_MAX_QUERIES_CACHE = 1000
_MAX_QUERIES_CACHE_DB = 1000

_QUERY_ROLLING_AVG_LEN = 10
_QUERIES_ROLLING_AVG_LEN = 300

DEFAULT_MODULE_ALIAS = 'default'

# The maximum length of a Unix socket relative to runstate dir.
# 21 is the length of the longest socket we might use, which
# is the admin socket (.s.EDGEDB.admin.xxxxx).
MAX_UNIX_SOCKET_PATH_LENGTH = 21

# 104 is the maximum Unix socket path length on BSD/Darwin, whereas
# Linux is constrained to 108.
MAX_RUNSTATE_DIR_PATH = 104 - MAX_UNIX_SOCKET_PATH_LENGTH - 1

HTTP_PORT_QUERY_CACHE_SIZE = 1000

# The time in seconds the Gel server shall wait between retries to connect
# to the system database after the connection was broken during runtime.
SYSTEM_DB_RECONNECT_INTERVAL = 1

ProtocolVersion: TypeAlias = tuple[int, int]

MIN_PROTOCOL: ProtocolVersion = (1, 0)
CURRENT_PROTOCOL: ProtocolVersion = (3, 0)

# Emulated PG binary protocol
POSTGRES_PROTOCOL: ProtocolVersion = (-3, 0)

MIN_SUGGESTED_CLIENT_POOL_SIZE = 10
MAX_SUGGESTED_CLIENT_POOL_SIZE = 100

_TLS_CERT_RELOAD_MAX_RETRIES = 5
_TLS_CERT_RELOAD_EXP_INTERVAL = 0.1

PGEXT_POSTGRES_VERSION = 13.9
PGEXT_POSTGRES_VERSION_NUM = 130009

# The time in seconds the Gel server will wait for a tenant to be gracefully
# shutdown when removed from a multi-tenant host.
MULTITENANT_TENANT_DESTROY_TIMEOUT = 30


class TxIsolationLevel(s_enum.StrEnum):
    RepeatableRead = 'REPEATABLE READ'
    Serializable = 'SERIALIZABLE'


# Mapping to the backend `edb_stat_statements.stmt_type` values,
# as well as `sys::QueryType` in edb/lib/sys.edgeql
class QueryType(enum.IntEnum):
    EdgeQL = 1
    SQL = 2
