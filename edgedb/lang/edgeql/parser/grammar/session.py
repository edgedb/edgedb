##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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
