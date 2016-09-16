##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA


class Stmt(Nonterm):
    def reduce_StartTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_CommitTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_RollbackTransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_SelectStmt(self, *kids):
        self.val = kids[0].val

    def reduce_InsertStmt(self, *kids):
        self.val = kids[0].val

    def reduce_UpdateStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DeleteStmt(self, *kids):
        self.val = kids[0].val


class StartTransactionStmt(Nonterm):
    def reduce_START_TRANSACTION(self, *kids):
        self.val = qlast.StartTransactionNode()


class CommitTransactionStmt(Nonterm):
    def reduce_OptAliasBlock_COMMIT(self, *kids):
        self.val = qlast.CommitTransactionNode()


class RollbackTransactionStmt(Nonterm):
    def reduce_ROLLBACK(self, *kids):
        self.val = qlast.RollbackTransactionNode()


class SelectStmt(Nonterm):
    # SelectNoParens is used instead of SelectExpr to avoid
    # a conflict with a Stmt|Expr production, as Expr contains a
    # SelectWithParens production.  Additionally, this is consistent
    # with other statement productions in disallowing parentheses.
    #
    def reduce_SelectNoParens(self, *kids):
        self.val = kids[0].val


class InsertStmt(Nonterm):
    def reduce_InsertExpr(self, *kids):
        self.val = kids[0].val


class UpdateStmt(Nonterm):
    def reduce_UpdateExpr(self, *kids):
        self.val = kids[0].val


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
    def reduce_DeleteExpr(self, *kids):
        self.val = kids[0].val


class ReturningClause(Nonterm):
    def reduce_RETURNING_OptSingle_SelectTargetEl(self, *kids):
        # XXX: for historical reasons a list is expected here by the compiler
        #
        self.val = [kids[1].val, [kids[2].val]]


class OptReturningClause(Nonterm):
    def reduce_ReturningClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = [False, None]
