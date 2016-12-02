##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .statements import *  # NOQA
from .ddl import *  # NOQA


class SingleStatementOrExpression(Nonterm):
    "%start"

    def reduce_Stmt(self, expr):
        self.val = expr.val

    def reduce_DDLStmt(self, expr):
        self.val = expr.val

    def reduce_Expr(self, expr):
        self.val = expr.val
