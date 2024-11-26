#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

# Maximum length of Postgres tenant ID.
MAX_TENANT_ID_LENGTH = 10

# Maximum length of names that are reflected 1:1 to Postgres:
MAX_NAME_LENGTH = 63 - MAX_TENANT_ID_LENGTH - 1 - 1
#                 ^                           ^   ^
#    max Postgres name len     tenant_id scheme   tenant_id separator

# Maximum number of arguments supported by SQL functions.
MAX_FUNC_ARG_COUNT = 100

EDGEDB_SUPERUSER = 'admin'
EDGEDB_OLD_SUPERUSER = 'edgedb'
EDGEDB_TEMPLATE_DB = '__edgedbtpl__'
EDGEDB_SYSTEM_DB = '__edgedbsys__'

EDGEDB_SPECIAL_DBS = {EDGEDB_TEMPLATE_DB, EDGEDB_SYSTEM_DB}
