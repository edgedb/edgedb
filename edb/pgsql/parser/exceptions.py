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

import re
from typing import Any, Optional


class PSqlParseError(Exception):
    pass


class PSqlSyntaxError(PSqlParseError):
    def __init__(self, message, lineno, cursorpos):
        self.message = message
        self.lineno = lineno
        self.cursorpos = cursorpos

    def __str__(self):
        return self.message


class PSqlUnsupportedError(PSqlParseError):
    def __init__(self, node: Optional[Any] = None, feat: Optional[str] = None):
        self.node = node
        self.location = None
        self.message = "not supported"
        if feat:
            self.message += f": {feat}"

    def __str__(self):
        return self.message


def get_node_name(name: str) -> str:
    """
    Given a node name (CreateTableStmt), this function tries to guess the SQL
    command text (CREATE TABLE).
    """

    name = name.removesuffix('Stmt').removesuffix('Expr')
    name = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
    return name.upper()
