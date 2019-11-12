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

EDGEDB_PORT = 5656
EDGEDB_SUPERUSER = 'edgedb'
EDGEDB_TEMPLATE_DB = 'edgedb0'
EDGEDB_SUPERUSER_DB = 'edgedb'
EDGEDB_ENCODING = 'utf-8'
EDGEDB_VISIBLE_METADATA_PREFIX = r'EdgeDB metadata follows, do not modify.\n'


_MAX_QUERIES_CACHE = 1000

_QUERY_ROLLING_AVG_LEN = 10
_QUERIES_ROLLING_AVG_LEN = 300

DEFAULT_MODULE_ALIAS = 'default'


HTTP_PORT_QUERY_CACHE_SIZE = 500
HTTP_PORT_MAX_CONCURRENCY = 250
