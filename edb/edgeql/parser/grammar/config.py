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

    def reduce_SESSION(self, _):
        self.val = qltypes.ConfigScope.SESSION

    def reduce_CURRENT_DATABASE(self, _c, _d):
        self.val = qltypes.ConfigScope.DATABASE

    def reduce_CURRENT_BRANCH(self, _c, _d):
        self.val = qltypes.ConfigScope.DATABASE

    def reduce_SYSTEM(self, _):
        self.val = qltypes.ConfigScope.INSTANCE

    def reduce_INSTANCE(self, _):
        self.val = qltypes.ConfigScope.INSTANCE


class ConfigOp(Nonterm):
    val: qlast.ConfigOp

    def reduce_SET_NodeName_ASSIGN_Expr(self, _s, name, _a, expr):
        self.val = qlast.ConfigSet(
            name=name.val,
            expr=expr.val,
        )

    def reduce_INSERT_NodeName_Shape(self, _, name, shape):
        self.val = qlast.ConfigInsert(
            name=name.val,
            shape=shape.val,
        )

    def reduce_RESET_NodeName_OptFilterClause(self, _, name, where):
        self.val = qlast.ConfigReset(
            name=name.val,
            where=where.val,
        )


class ConfigStmt(Nonterm):

    def reduce_CONFIGURE_DATABASE_ConfigOp(self, configure, database, _config):
        raise errors.EdgeQLSyntaxError(
            f"'{configure.val} {database.val}' is invalid syntax. "
            f"Did you mean '{configure.val} "
            f"{'current' if database.val[0] == 'd' else 'CURRENT'} "
            f"{database.val}'?",
            span=database.span)

    def reduce_CONFIGURE_BRANCH_ConfigOp(self, configure, database, _config):
        raise errors.EdgeQLSyntaxError(
            f"'{configure.val} {database.val}' is invalid syntax. "
            f"Did you mean '{configure.val} "
            f"{'current' if database.val[0] == 'd' else 'CURRENT'} "
            f"{database.val}'?",
            span=database.span)

    def reduce_CONFIGURE_ConfigScope_ConfigOp(self, _, scope, op):
        self.val = op.val
        self.val.scope = scope.val

    def reduce_SET_GLOBAL_NodeName_ASSIGN_Expr(self, _s, _g, name, _a, expr):
        self.val = qlast.ConfigSet(
            name=name.val,
            expr=expr.val,
            scope=qltypes.ConfigScope.GLOBAL,
        )

    def reduce_RESET_GLOBAL_NodeName(self, _r, _g, name):
        self.val = qlast.ConfigReset(
            name=name.val,
            scope=qltypes.ConfigScope.GLOBAL,
        )
