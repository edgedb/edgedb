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


from edb.lang.edgeql import ast as qlast

from .expressions import Nonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class Stmt(Nonterm):
    def reduce_StartTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CommitTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_RollbackTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ExprStmt(self, *kids):
        self.val = kids[0].val


class StartTransactionStmt(Nonterm):
    def reduce_START_TRANSACTION(self, *kids):
        self.val = qlast.StartTransaction()


class CommitTransactionStmt(Nonterm):
    def reduce_COMMIT(self, *kids):
        self.val = qlast.CommitTransaction()


class RollbackTransactionStmt(Nonterm):
    def reduce_ROLLBACK(self, *kids):
        self.val = qlast.RollbackTransaction()
