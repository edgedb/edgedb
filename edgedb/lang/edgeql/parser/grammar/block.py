##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.parsing import ListNonterm

from .precedence import *
from .tokens import *
from .statements import *
from .ddl import *


class SingleStatement(Nonterm):
    def reduce_Stmt(self, expr):
        self.val = expr.val

    def reduce_DDLStmt(self, expr):
        self.val = expr.val

    def reduce_empty(self):
        self.val = None


class StatementList(ListNonterm, element=SingleStatement,
                    separator=T_SEMICOLON):
    pass


class StatementBlock(Nonterm):
    "%start"

    def reduce_StatementList_SEMICOLON(self, exprs, semicolon):
        self.val = exprs.val
