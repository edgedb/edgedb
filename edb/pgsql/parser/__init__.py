#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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

from typing import (
    List,
)

import json

from edb.pgsql import ast as pgast

from . import ast_builder
from . import parser
from .parser import (
    Source,
    NormalizedSource,
    deserialize,
)


__all__ = (
    "parse",
    "Source",
    "NormalizedSource",
    "deserialize"
)


def parse(
    sql_query: str, propagate_spans: bool = False
) -> List[pgast.Query | pgast.Statement]:
    ast_json = parser.pg_parse(bytes(sql_query, encoding="UTF8"))

    return ast_builder.build_stmts(
        json.loads(ast_json),
        sql_query,
        propagate_spans,
    )
