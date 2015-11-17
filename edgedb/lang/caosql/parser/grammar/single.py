##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .precedence import *
from .tokens import *
from .statements import *


class SingleStatementOrExpression(Nonterm):
    "%start"

    def reduce_Stmt(self, expr):
        self.val = expr.val

    def reduce_Expr(self, expr):
        self.val = expr.val
