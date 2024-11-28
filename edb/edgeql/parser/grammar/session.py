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

from edb.edgeql import ast as qlast

from .expressions import Nonterm
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class SessionStmt(Nonterm):
    val: qlast.Command

    @parsing.inline(0)
    def reduce_SetStmt(self, *kids):
        pass

    @parsing.inline(0)
    def reduce_ResetStmt(self, *kids):
        pass


class SetStmt(Nonterm):
    val: qlast.SessionSetAliasDecl

    def reduce_SET_ALIAS_Identifier_AS_MODULE_ModuleName(self, *kids):
        _, _, alias, _, _, module = kids
        self.val = qlast.SessionSetAliasDecl(
            decl=qlast.ModuleAliasDecl(
                module='::'.join(module.val), alias=alias.val
            )
        )

    def reduce_SET_MODULE_ModuleName(self, *kids):
        _, _, module = kids
        self.val = qlast.SessionSetAliasDecl(
            decl=qlast.ModuleAliasDecl(module='::'.join(module.val))
        )


class ResetStmt(Nonterm):
    val: qlast.SessionResetAliasDecl

    def reduce_RESET_ALIAS_Identifier(self, *kids):
        self.val = qlast.SessionResetAliasDecl(
            alias=kids[2].val)

    def reduce_RESET_MODULE(self, *kids):
        self.val = qlast.SessionResetModule()

    def reduce_RESET_ALIAS_STAR(self, *kids):
        self.val = qlast.SessionResetAllAliases()
