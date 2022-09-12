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

from edb.edgeql import qltypes as ir


class Cardinality(enum.Enum):
    # Cardinality isn't applicable for the query:
    # * the query is a command like CONFIGURE that
    #   does not return any data;
    # * the query is composed of multiple queries.
    NO_RESULT = 0x6e

    # Cardinality is 1 or 0
    AT_MOST_ONE = 0x6f

    # Cardinality is 1
    ONE = 0x41

    # Cardinality is >= 0
    MANY = 0x6d

    # Cardinality is >= 1
    AT_LEAST_ONE = 0x4d

    @classmethod
    def from_ir_value(cls, card: ir.Cardinality) -> Cardinality:
        if card is ir.Cardinality.AT_MOST_ONE:
            return Cardinality.AT_MOST_ONE
        elif card is ir.Cardinality.ONE:
            return Cardinality.ONE
        elif card is ir.Cardinality.MANY:
            return Cardinality.MANY
        elif card is ir.Cardinality.AT_LEAST_ONE:
            return Cardinality.AT_LEAST_ONE
        else:
            raise ValueError(
                f"Cardinality.from_ir_value() got an invalid input: {card}"
            )
