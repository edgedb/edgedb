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


from edgedb.lang.common.parsing import ListNonterm
from edgedb.lang.edgeql import ast as qlast

from . import tokens
from .expressions import Nonterm
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class SessionStmt(Nonterm):
    def reduce_SetStmt(self, *kids):
        self.val = kids[0].val


class SetDecl(Nonterm):
    def reduce_AliasDecl(self, *kids):
        self.val = kids[0].val


class SetDeclList(ListNonterm, element=SetDecl,
                  separator=tokens.T_COMMA):
    pass


class SetStmt(Nonterm):
    def reduce_SET_SetDeclList(self, *kids):
        self.val = qlast.SessionStateDecl(
            items=kids[1].val
        )
