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
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class ConfigTarget(Nonterm):

    def reduce_SESSION(self, *kids):
        self.val = False

    def reduce_SYSTEM(self, *kids):
        self.val = True


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

    def reduce_CONFIGURE_ConfigTarget_ConfigOp(self, *kids):
        self.val = kids[2].val
        self.val.system = kids[1].val
