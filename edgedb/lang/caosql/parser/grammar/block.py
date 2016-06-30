##
# Copyright (c) 2008-2015 MagicStack Inc.
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


class StatementList(ListNonterm, element=SingleStatement):
    pass


class StatementBlock(Nonterm):
    "%start"

    def reduce_StatementList(self, exprs):
        self.val = exprs.val
