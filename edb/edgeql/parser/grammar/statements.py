#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from .expressions import Nonterm, ListNonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA

from . import tokens


class Stmt(Nonterm):
    def reduce_TransactionStmt(self, *kids):
        self.val = kids[0].val

    def reduce_DescribeStmt(self, *kids):
        # DESCRIBE
        self.val = kids[0].val

    def reduce_ExprStmt(self, *kids):
        self.val = kids[0].val


class TransactionMode(Nonterm):
    def reduce_ISOLATION_SERIALIZABLE(self, *kids):
        self.val = (qltypes.TransactionIsolationLevel.SERIALIZABLE,
                    kids[0].context)

    def reduce_ISOLATION_REPEATABLE_READ(self, *kids):
        self.val = (qltypes.TransactionIsolationLevel.REPEATABLE_READ,
                    kids[0].context)

    def reduce_READ_WRITE(self, *kids):
        self.val = (qltypes.TransactionAccessMode.READ_WRITE,
                    kids[0].context)

    def reduce_READ_ONLY(self, *kids):
        self.val = (qltypes.TransactionAccessMode.READ_ONLY,
                    kids[0].context)

    def reduce_DEFERRABLE(self, *kids):
        self.val = (qltypes.TransactionDeferMode.DEFERRABLE,
                    kids[0].context)

    def reduce_NOT_DEFERRABLE(self, *kids):
        self.val = (qltypes.TransactionDeferMode.NOT_DEFERRABLE,
                    kids[0].context)


class TransactionModeList(ListNonterm, element=TransactionMode,
                          separator=tokens.T_COMMA):
    pass


class OptTransactionModeList(Nonterm):
    def reduce_TransactionModeList(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = []


class TransactionStmt(Nonterm):
    def reduce_START_TRANSACTION_OptTransactionModeList(self, *kids):
        modes = kids[2].val

        isolation = None
        access = None
        deferrable = None

        for mode, mode_ctx in modes:
            if isinstance(mode, qltypes.TransactionIsolationLevel):
                if isolation is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"only one isolation level can be specified",
                        context=mode_ctx)
                isolation = mode

            elif isinstance(mode, qltypes.TransactionAccessMode):
                if access is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"only one access mode can be specified",
                        context=mode_ctx)
                access = mode

            else:
                assert isinstance(mode, qltypes.TransactionDeferMode)
                if deferrable is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"deferrable mode can only be specified once",
                        context=mode_ctx)
                deferrable = mode

        self.val = qlast.StartTransaction(
            isolation=isolation, access=access, deferrable=deferrable)

    def reduce_COMMIT(self, *kids):
        self.val = qlast.CommitTransaction()

    def reduce_ROLLBACK(self, *kids):
        self.val = qlast.RollbackTransaction()

    def reduce_DECLARE_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.DeclareSavepoint(name=kids[2].val)

    def reduce_ROLLBACK_TO_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.RollbackToSavepoint(name=kids[3].val)

    def reduce_RELEASE_SAVEPOINT_Identifier(self, *kids):
        self.val = qlast.ReleaseSavepoint(name=kids[2].val)


class DescribeFormat(Nonterm):

    def reduce_empty(self, *kids):
        self.val = qltypes.DescribeLanguage.DDL

    def reduce_AS_DDL(self, *kids):
        self.val = qltypes.DescribeLanguage.DDL

    def reduce_AS_SDL(self, *kids):
        self.val = qltypes.DescribeLanguage.SDL

    def reduce_AS_TEXT(self, *kids):
        self.val = qltypes.DescribeLanguage.TEXT


class DescribeOption(Nonterm):

    def reduce_VERBOSE(self, *kids):
        self.val = qlast.Flag(name='VERBOSE', val=True)


class DescribeOptions(ListNonterm, element=DescribeOption):
    def _reduce_list(self, lst, el):
        self.val = qlast.Options(
            options={**lst.val.options, el.val.name: el.val})

    def _reduce_el(self, el):
        self.val = qlast.Options(options={el.val.name: el.val})


class OptDescribeOptions(Nonterm):

    def reduce_DescribeOptions(self, *kids):
        self.val = kids[0].val

    def reduce_empty(self, *kids):
        self.val = qlast.Options()


class DescribeStmt(Nonterm):

    def reduce_DESCRIBE_SCHEMA(self, *kids):
        """%reduce DESCRIBE SCHEMA DescribeFormat
                   OptDescribeOptions
        """
        self.val = qlast.DescribeStmt(
            language=kids[2].val,
            object=None,
            options=kids[3].val,
        )

    def reduce_DESCRIBE_SchemaItem(self, *kids):
        """%reduce DESCRIBE SchemaItem DescribeFormat
                   OptDescribeOptions
        """
        self.val = qlast.DescribeStmt(
            language=kids[2].val,
            object=kids[1].val,
            options=kids[3].val,
        )

    def reduce_DESCRIBE_OBJECT(self, *kids):
        """%reduce DESCRIBE OBJECT NodeName DescribeFormat
                   OptDescribeOptions
        """
        self.val = qlast.DescribeStmt(
            language=kids[3].val,
            object=kids[2].val,
            options=kids[4].val,
        )
