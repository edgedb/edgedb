##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .expressions import Nonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .statements import *  # NOQA
from .ddl import *  # NOQA


class SingleStatement(Nonterm):
    def reduce_Stmt(self, *kids):
        self.val = kids[0].val

    def reduce_DDLStmt(self, *kids):
        self.val = kids[0].val


class StatementBlock(Nonterm):
    "%start"

    def reduce_StatementBlock_SEMICOLON(self, *kids):
        self.val = kids[0].val

    def reduce_StatementBlock_SingleStatement_SEMICOLON(self, *kids):
        self.val = kids[0].val + [kids[1].val]

    def reduce_empty(self):
        self.val = []
