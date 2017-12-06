##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from ...errors import EdgeQLSyntaxError
from .expressions import Nonterm
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

    def reduce_SetStmt(self, *kids):
        self.val = kids[0].val


class StartTransactionStmt(Nonterm):
    def reduce_START_TRANSACTION(self, *kids):
        self.val = qlast.StartTransaction()


class CommitTransactionStmt(Nonterm):
    def reduce_DDLAliasBlock_COMMIT(self, *kids):
        # NOTE: DDLAliasBlock is trying to avoid conflicts
        with_block = kids[0].val
        if with_block.aliases:
            raise EdgeQLSyntaxError(
                'WITH block not allowed for a transaction COMMIT',
                context=kids[0].context)
        self.val = qlast.CommitTransaction()


class RollbackTransactionStmt(Nonterm):
    def reduce_ROLLBACK(self, *kids):
        self.val = qlast.RollbackTransaction()
