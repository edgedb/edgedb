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

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from .expressions import Nonterm
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class ConfigScope(Nonterm):

    def reduce_SESSION(self, *kids):
        self.val = qltypes.ConfigScope.SESSION

    def reduce_CURRENT_DATABASE(self, *kids):
        self.val = qltypes.ConfigScope.DATABASE

    def reduce_SYSTEM(self, *kids):
        self.val = qltypes.ConfigScope.INSTANCE

    def reduce_INSTANCE(self, *kids):
        self.val = qltypes.ConfigScope.INSTANCE


class ConfigOp(Nonterm):

    def reduce_SET_NodeName_ASSIGN_Expr(self, *kids):
        self.val = qlast.ConfigSet(
            name=kids[1].val,
            expr=kids[3].val,
        )

    def reduce_INSERT_NodeName_Shape(self, *kids):
        self.val = qlast.ConfigInsert(
            name=kids[1].val,
            shape=kids[2].val,
        )

    def reduce_RESET_NodeName_OptFilterClause(self, *kids):
        self.val = qlast.ConfigReset(
            name=kids[1].val,
            where=kids[2].val,
        )


class ConfigStmt(Nonterm):

    def reduce_CONFIGURE_DATABASE_ConfigOp(self, *kids):
        raise errors.EdgeQLSyntaxError(
            f"'{kids[0].val} {kids[1].val}' is invalid syntax. Did you mean "
            f"'{kids[0].val} "
            f"{'current' if kids[1].val[0] == 'd' else 'CURRENT'} "
            f"{kids[1].val}'?",
            context=kids[1].context)

    def reduce_CONFIGURE_ConfigScope_ConfigOp(self, *kids):
        self.val = kids[2].val
        self.val.scope = kids[1].val
