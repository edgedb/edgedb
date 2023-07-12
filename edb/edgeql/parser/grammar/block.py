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
    @parsing.inline(0)
    def reduce_Stmt(self, _):
        # Expressions
        pass

    @parsing.inline(0)
    def reduce_DDLStmt(self, _):
        # Data definition commands
        pass

    @parsing.inline(0)
    def reduce_SessionStmt(self, _):
        # Session-local utility commands
        pass

    @parsing.inline(0)
    def reduce_ConfigStmt(self, _):
        # Configuration commands
        pass


class StatementBlock(parsing.ListNonterm, element=SingleStatement,
                     separator=Semicolons):  # NOQA, Semicolons are from .ddl
    pass


class EdgeQLBlock(Nonterm):
    "%start"

    @parsing.inline(0)
    def reduce_StatementBlock_OptSemicolons_EOF(self, _, _semicolon, _eof):
        pass

    def reduce_OptSemicolons_EOF(self, _semicolon, _eof):
        self.val = []
