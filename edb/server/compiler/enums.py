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


class Capability(enum.Flag):

    DDL = enum.auto()
    TRANSACTION = enum.auto()
    SESSION = enum.auto()
    QUERY = enum.auto()

    ALL = DDL | TRANSACTION | SESSION | QUERY
