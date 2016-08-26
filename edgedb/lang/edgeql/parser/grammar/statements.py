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
    def reduce_OptAliasBlock_INSERT_Path_OptReturningClause(self, *kids):
        self.val = qlast.InsertQueryNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            targets=kids[3].val
        )

    def reduce_OptAliasBlock_INSERT_TypedShape_OptReturningClause(self, *kids):
        pathspec = kids[2].val.pathspec
        kids[2].val.pathspec = None
        self.val = qlast.InsertQueryNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            pathspec=pathspec,
            targets=kids[3].val
        )


class UpdateStmt(Nonterm):
    def reduce_UpdateStmt(self, *kids):
        r"%reduce OptAliasBlock UPDATE TypedShape \
                  OptWhereClause OptReturningClause"
        pathspec = kids[2].val.pathspec
        kids[2].val.pathspec = None
        self.val = qlast.UpdateQueryNode(
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            pathspec=pathspec,
            where=kids[3].val,
            targets=kids[4].val
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
            namespaces=kids[0].val[0],
            aliases=kids[0].val[1],
            subject=kids[2].val,
            where=kids[3].val,
            targets=kids[4].val
        )


class ReturningClause(Nonterm):
    def reduce_RETURNING_SelectTargetList(self, *kids):
        self.val = kids[1].val


class OptReturningClause(Nonterm):
    def reduce_ReturningClause(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = None
