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


from edb.edgeql import ast as qlast

from .expressions import Nonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class Stmt(Nonterm):
    def reduce_TransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_ExprStmt(self, *kids):
        self.val = kids[0].val


class TransactionStmt(Nonterm):
    def reduce_START_TRANSACTION(self, *kids):
        self.val = qlast.StartTransaction()

    def reduce_COMMIT(self, *kids):
        self.val = qlast.CommitTransaction()

    def reduce_ROLLBACK(self, *kids):
        self.val = qlast.RollbackTransaction()

    def reduce_DECLARE_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.DeclareSavepoint(name=kids[2].val)

    def reduce_ROLLBACK_TO_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.RollbackToSavepoint(name=kids[3].val)

    def reduce_RELEASE_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.ReleaseSavepoint(name=kids[2].val)
