##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import parsing

from metamagic.caos.lang.caosql import ast as qlast

from .precedence import *
from .tokens import *
from .expressions import *


class Stmt(Nonterm):
    def reduce_SelectStmt(self, *kids):
        self.val = kids[0].val

    def reduce_InsertStmt(self, *kids):
        self.val = kids[0].val

    def reduce_UpdateStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DeleteStmt(self, *kids):
        self.val = kids[0].val


class SelectStmt(Nonterm):
    # SelectNoParens is used instead of SelectExpr to avoid
    # a conflict with a Stmt|Expr production, as Expr contains a
    # SelectWithParens production.  Additionally, this is consistent
    # with other statement productions in disallowing parentheses.
    #
    def reduce_SelectNoParens(self, *kids):
        self.val = kids[0].val


class InsertStmt(Nonterm):
    def reduce_InsertStmt(self, *kids):
        r"%reduce OptAliasBlock INSERT Path OptSelectPathSpec \
                  OptReturningClause"
        self.val = qlast.InsertQueryNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            pathspec=kids[3].val,
            targets=kids[4].val
        )


class UpdateStmt(Nonterm):
    def reduce_UpdateStmt(self, *kids):
        r"%reduce OptAliasBlock UPDATE Path SelectPathSpec \
                  OptWhereClause OptReturningClause"
        self.val = qlast.UpdateQueryNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            pathspec=kids[3].val,
            where=kids[4].val,
            targets=kids[5].val
        )


class SetClauseList(Nonterm):
    def reduce_SetClause(self, *kids):
        self.val = [kids[0].val]

    def reduce_SetClauseList_COMMA_SetClause(self, *kids):
        self.val = kids[0].val + [kids[2].val]


class SetClause(Nonterm):
    def reduce_SetTarget_EQUALS_Expr(self, *kids):
        self.val = qlast.UpdateExprNode(expr=kids[0].val, value=kids[2].val)


class SetTarget(Nonterm):
    def reduce_NodeName(self, *kids):
        self.val = kids[0].val


class DeleteStmt(Nonterm):
    def reduce_DeleteStmt(self, *kids):
        "%reduce OptAliasBlock DELETE Path OptWhereClause OptReturningClause"
        self.val = qlast.DeleteQueryNode(
            namespaces = kids[0].val[0],
            aliases = kids[0].val[1],
            subject = kids[2].val,
            where = kids[3].val,
            targets = kids[4].val
        )


class ReturningClause(Nonterm):
    def reduce_RETURNING_SelectTargetList(self, *kids):
        self.val = kids[1].val


class OptReturningClause(Nonterm):
    def reduce_ReturningClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None
