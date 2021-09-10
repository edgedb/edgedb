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

from edb import buildmeta


EDGEDB_PORT = 5656
EDGEDB_SUPERGROUP = 'edgedb_supergroup'
EDGEDB_SUPERUSER = 'edgedb'
EDGEDB_TEMPLATE_DB = '__edgedbtpl__'
EDGEDB_SUPERUSER_DB = 'edgedb'
EDGEDB_SYSTEM_DB = '__edgedbsys__'
EDGEDB_ENCODING = 'utf-8'
EDGEDB_VISIBLE_METADATA_PREFIX = r'EdgeDB metadata follows, do not modify.\n'

EDGEDB_SPECIAL_DBS = {EDGEDB_TEMPLATE_DB, EDGEDB_SYSTEM_DB}

EDGEDB_CATALOG_VERSION = buildmeta.EDGEDB_CATALOG_VERSION

# Resource limit on open FDs for the server process.
# By default, at least on macOS, the max number of open FDs
# is 256, which is low and can cause 'edb test' to hang.
# We try to bump the rlimit on server start if pemitted.
EDGEDB_MIN_RLIMIT_NOFILE = 2048

BACKEND_CONNECTIONS_MIN = 4
BACKEND_COMPILER_POOL_SIZE_MIN = 1

_MAX_QUERIES_CACHE = 1000

_QUERY_ROLLING_AVG_LEN = 10
_QUERIES_ROLLING_AVG_LEN = 300

DEFAULT_MODULE_ALIAS = 'default'


HTTP_PORT_QUERY_CACHE_SIZE = 1000
HTTP_PORT_MAX_CONCURRENCY = 250  # XXX

# The time in seconds the EdgeDB server shall wait between retries to connect
# to the system database after the connection was broken during runtime.
SYSTEM_DB_RECONNECT_INTERVAL = 1

MIN_PROTOCOL = (0, 7)
CURRENT_PROTOCOL = (0, 12)
