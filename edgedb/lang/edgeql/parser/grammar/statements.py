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
        # NOTE: OptAliasBlock is trying to avoid conflicts
        if kids[0].val:
            raise EdgeQLSyntaxError('Unexpected token: {}'.format(kids[1]),
                                    context=kids[1].context)
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


class DeleteStmt(Nonterm):
    def reduce_DeleteExpr(self, *kids):
        self.val = kids[0].val
