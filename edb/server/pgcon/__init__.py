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

from .errors import (
    BackendError,
    BackendConnectionError,
    BackendPrivilegeError,
    BackendCatalogNameError,
)

from .pgcon import (
    PGConnection,
)
from .connect import (
    pg_connect,
    SETUP_TEMP_TABLE_SCRIPT,
    SETUP_CONFIG_CACHE_SCRIPT,
    RESET_STATIC_CFG_SCRIPT,
)

__all__ = (
    'pg_connect',
    'PGConnection',
    'BackendError',
    'BackendConnectionError',
    'BackendPrivilegeError',
    'BackendCatalogNameError',
    'SETUP_TEMP_TABLE_SCRIPT',
    'SETUP_CONFIG_CACHE_SCRIPT',
    'RESET_STATIC_CFG_SCRIPT'
)
