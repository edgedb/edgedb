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

import typing

from edb import errors
from edb.common import parsing

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from .expressions import Nonterm, ListNonterm
from .precedence import *  # NOQA
from .tokens import *  # NOQA
from .expressions import *  # NOQA

from . import tokens


class Stmt(Nonterm):
    val: qlast.Command

    @parsing.inline(0)
    def reduce_TransactionStmt(self, stmt):
        pass

    @parsing.inline(0)
    def reduce_DescribeStmt(self, stmt):
        # DESCRIBE
        pass

    @parsing.inline(0)
    def reduce_AnalyzeStmt(self, stmt):
        # ANALYZE
        pass

    @parsing.inline(0)
    def reduce_AdministerStmt(self, stmt):
        pass

    @parsing.inline(0)
    def reduce_ExprStmt(self, stmt):
        pass


class TransactionMode(Nonterm):

    def reduce_ISOLATION_SERIALIZABLE(self, *kids):
        self.val = (qltypes.TransactionIsolationLevel.SERIALIZABLE,
                    kids[0].span)

    def reduce_ISOLATION_REPEATABLE_READ(self, *kids):
        self.val = (qltypes.TransactionIsolationLevel.REPEATABLE_READ,
                    kids[0].span)

    def reduce_READ_WRITE(self, *kids):
        self.val = (qltypes.TransactionAccessMode.READ_WRITE,
                    kids[0].span)

    def reduce_READ_ONLY(self, *kids):
        self.val = (qltypes.TransactionAccessMode.READ_ONLY,
                    kids[0].span)

    def reduce_DEFERRABLE(self, *kids):
        self.val = (qltypes.TransactionDeferMode.DEFERRABLE,
                    kids[0].span)

    def reduce_NOT_DEFERRABLE(self, *kids):
        self.val = (qltypes.TransactionDeferMode.NOT_DEFERRABLE,
                    kids[0].span)


class TransactionModeList(ListNonterm, element=TransactionMode,
                          separator=tokens.T_COMMA):
    pass


class OptTransactionModeList(Nonterm):

    @parsing.inline(0)
    def reduce_TransactionModeList(self, *kids):
        pass

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
                        span=mode_ctx)
                isolation = mode

            elif isinstance(mode, qltypes.TransactionAccessMode):
                if access is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"only one access mode can be specified",
                        span=mode_ctx)
                access = mode

            else:
                assert isinstance(mode, qltypes.TransactionDeferMode)
                if deferrable is not None:
                    raise errors.EdgeQLSyntaxError(
                        f"deferrable mode can only be specified once",
                        span=mode_ctx)
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


class DescribeFmt(typing.NamedTuple):

    language: typing.Optional[qltypes.DescribeLanguage] = None
    options: typing.Optional[qlast.Options] = None


class DescribeFormat(Nonterm):
    val: DescribeFmt

    def reduce_empty(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.DDL,
            options=qlast.Options(),
        )

    def reduce_AS_DDL(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.DDL,
            options=qlast.Options(),
        )

    def reduce_AS_SDL(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.SDL,
            options=qlast.Options(),
        )

    def reduce_AS_JSON(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.JSON,
            options=qlast.Options(),
        )

    def reduce_AS_TEXT(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.TEXT,
            options=qlast.Options(),
        )

    def reduce_AS_TEXT_VERBOSE(self, *kids):
        self.val = DescribeFmt(
            language=qltypes.DescribeLanguage.TEXT,
            options=qlast.Options(
                options={'VERBOSE': qlast.OptionFlag(
                    name='VERBOSE', val=True, span=kids[2].span)}
            ),
        )


class DescribeStmt(Nonterm):
    val: qlast.DescribeStmt

    def reduce_DESCRIBE_SCHEMA(self, *kids):
        """%reduce DESCRIBE SCHEMA DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=qlast.DescribeGlobal.Schema,
            language=kids[2].val.language,
            options=kids[2].val.options,
        )

    def reduce_DESCRIBE_CURRENT_DATABASE_CONFIG(self, *kids):
        """%reduce DESCRIBE CURRENT DATABASE CONFIG DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=qlast.DescribeGlobal.DatabaseConfig,
            language=kids[4].val.language,
            options=kids[4].val.options,
        )

    def reduce_DESCRIBE_CURRENT_BRANCH_CONFIG(self, *kids):
        """%reduce DESCRIBE CURRENT BRANCH CONFIG DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=qlast.DescribeGlobal.DatabaseConfig,
            language=kids[4].val.language,
            options=kids[4].val.options,
        )

    def reduce_DESCRIBE_INSTANCE_CONFIG(self, *kids):
        """%reduce DESCRIBE INSTANCE CONFIG DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=qlast.DescribeGlobal.InstanceConfig,
            language=kids[3].val.language,
            options=kids[3].val.options,
        )

    def reduce_DESCRIBE_SYSTEM_CONFIG(self, *kids):
        """%reduce DESCRIBE SYSTEM CONFIG DescribeFormat"""
        return self.reduce_DESCRIBE_INSTANCE_CONFIG(*kids)

    def reduce_DESCRIBE_ROLES(self, *kids):
        """%reduce DESCRIBE ROLES DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=qlast.DescribeGlobal.Roles,
            language=kids[2].val.language,
            options=kids[2].val.options,
        )

    def reduce_DESCRIBE_SchemaItem(self, *kids):
        """%reduce DESCRIBE SchemaItem DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=kids[1].val,
            language=kids[2].val.language,
            options=kids[2].val.options,
        )

    def reduce_DESCRIBE_OBJECT(self, *kids):
        """%reduce DESCRIBE OBJECT NodeName DescribeFormat"""
        self.val = qlast.DescribeStmt(
            object=kids[2].val,
            language=kids[3].val.language,
            options=kids[3].val.options,
        )

    def reduce_DESCRIBE_CURRENT_MIGRATION(self, *kids):
        """%reduce DESCRIBE CURRENT MIGRATION DescribeFormat"""
        lang = kids[3].val.language
        if (
            lang is not qltypes.DescribeLanguage.DDL
            and lang is not qltypes.DescribeLanguage.JSON
        ):
            raise errors.InvalidSyntaxError(
                f'unexpected DESCRIBE format: {lang!r}',
                span=kids[3].span,
            )
        if kids[3].val.options:
            raise errors.InvalidSyntaxError(
                f'DESCRIBE CURRENT MIGRATION does not support options',
                span=kids[3].span,
            )

        self.val = qlast.DescribeCurrentMigration(
            language=lang,
        )


class AnalyzeStmt(Nonterm):
    val: qlast.ExplainStmt

    def reduce_ANALYZE_NamedTuple_ExprStmt(self, *kids):
        _, args, stmt = kids
        self.val = qlast.ExplainStmt(
            args=args.val,
            query=stmt.val,
        )

    def reduce_ANALYZE_ExprStmt(self, *kids):
        _, stmt = kids
        self.val = qlast.ExplainStmt(
            query=stmt.val,
        )


class AdministerStmt(Nonterm):
    val: qlast.AdministerStmt

    def reduce_ADMINISTER_FuncExpr(self, *kids):
        _, expr = kids
        self.val = qlast.AdministerStmt(expr=expr.val)
