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

import enum

from edb.common import enum as strenum


class CompileStatementMode(enum.Enum):

    SKIP_FIRST = 'skip_first'
    ALL = 'all'
    SINGLE = 'single'


class ResultCardinality(strenum.StrEnum):

    # Cardinality is 1 or 0
    ONE = 'ONE'

    # Cardinality is >= 0
    MANY = 'MANY'

    # Cardinality isn't applicable for the query:
    # * the query is a command like CONFIGURE that
    #   does not return any data;
    # * the query is composed of multiple queries.
    NO_RESULT = 'NO_RESULT'


class Capability(enum.IntFlag):

    MODIFICATIONS     = 1 << 0    # noqa
    SESSION_CONFIG    = 1 << 1    # noqa
    TRANSACTION       = 1 << 2    # noqa
    DDL               = 1 << 3    # noqa
    PERSISTENT_CONFIG = 1 << 4    # noqa

    QUERY             = 1 << 32   # noqa
    SESSION_MODE      = 1 << 33   # noqa


# Exposed to client as SERVER_HEADER_CAPABILITIES
# and QUERY_OPT_ALLOW_CAPABILITIES
PUBLIC_CAPABILITIES = (
    Capability.DDL |
    Capability.TRANSACTION |
    Capability.SESSION_CONFIG |
    Capability.PERSISTENT_CONFIG |
    Capability.MODIFICATIONS
)

# Private to server (compiler and io process)
PRIVATE_CAPABILITIES = Capability.QUERY | Capability.SESSION_MODE


CAPABILITY_TITLES = {
    Capability.MODIFICATIONS: 'data modification queries',
    Capability.SESSION_CONFIG: 'session configuration queries',
    Capability.TRANSACTION: 'transaction control commands',
    Capability.DDL: 'DDL commands',
    Capability.PERSISTENT_CONFIG: 'configuration commands',
    Capability.QUERY: 'read-only queries',
    Capability.SESSION_MODE: 'session-mode functions',
}


class IoFormat(strenum.StrEnum):
    BINARY = 'BINARY'
    JSON = 'JSON'
    JSON_ELEMENTS = 'JSON_ELEMENTS'
    SCRIPT = 'SCRIPT'
