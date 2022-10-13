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

from typing import *

import json

from edb.pgsql import ast as pgast

from .exceptions import PSqlUnsupportedError
from .parser import pg_parse
from .ast_builder import build_queries


def parse(sql_query: str) -> List[pgast.Query]:
    ast_json = pg_parse(bytes(sql_query, encoding="UTF8"))

    try:
        return build_queries(json.loads(ast_json), sql_query)
    except IndexError:
        raise PSqlUnsupportedError()
