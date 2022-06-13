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
from typing import *

import enum

from edb.common import enum as strenum
from edb.protocol.enums import * # NoQA


class CompileStatementMode(enum.Enum):

    SKIP_FIRST = 'skip_first'
    ALL = 'all'
    SINGLE = 'single'


if TYPE_CHECKING:
    Error_T = TypeVar('Error_T')


class Capability(enum.IntFlag):

    MODIFICATIONS     = 1 << 0    # noqa
    SESSION_CONFIG    = 1 << 1    # noqa
    TRANSACTION       = 1 << 2    # noqa
    DDL               = 1 << 3    # noqa
    PERSISTENT_CONFIG = 1 << 4    # noqa

    def make_error(
        self,
        allowed: Capability,
        error_constructor: Callable[[str], Error_T],
    ) -> Error_T:
        for item in Capability:
            if item & allowed:
                continue
            if self & item:
                return error_constructor(
                    f"cannot execute {CAPABILITY_TITLES[item]}")
        raise AssertionError(
            f"extra capability not found in"
            f" {self} allowed {allowed}"
        )


CAPABILITY_TITLES = {
    Capability.MODIFICATIONS: 'data modification queries',
    Capability.SESSION_CONFIG: 'session configuration queries',
    Capability.TRANSACTION: 'transaction control commands',
    Capability.DDL: 'DDL commands',
    Capability.PERSISTENT_CONFIG: 'configuration commands',
}


class OutputFormat(strenum.StrEnum):
    BINARY = 'BINARY'
    JSON = 'JSON'
    JSON_ELEMENTS = 'JSON_ELEMENTS'
    NONE = 'NONE'
