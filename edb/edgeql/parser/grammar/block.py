#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import parsing

from .expressions import Nonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .statements import *  # NOQA
from .ddl import *  # NOQA
from .session import *  # NOQA
from .config import *  # NOQA


class SingleStatement(Nonterm):
    def reduce_Stmt(self, stmt):
        # Expressions
        self.val = stmt.val

    def reduce_DDLStmt(self, stmt):
        # Data definition commands
        self.val = stmt.val

    def reduce_SessionStmt(self, stmt):
        # Session-local utility commands
        self.val = stmt.val

    def reduce_ConfigStmt(self, stmt):
        # Configuration commands
        self.val = stmt.val


class StatementBlock(parsing.ListNonterm, element=SingleStatement,
                     separator=Semicolons):  # NOQA, Semicolons are from .ddl
    pass


class EdgeQLBlock(Nonterm):
    "%start"

    def reduce_StatementBlock_OptSemicolons_EOF(self, block, _semicolon, _eof):
        self.val = block.val

    def reduce_OptSemicolons_EOF(self, _semicolon, _eof):
        self.val = []
