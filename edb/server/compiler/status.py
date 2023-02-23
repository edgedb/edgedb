#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import functools

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes


@functools.singledispatch
def get_status(ql: qlast.Base) -> bytes:
    raise NotImplementedError(
        f'cannot get status for the {type(ql).__name__!r} AST node')


@get_status.register(qlast.CreateObject)
def _ddl_create(ql):
    return f'CREATE {ql.object_class}'.encode()


@get_status.register(qlast.AlterObject)
def _ddl_alter(ql):
    return f'ALTER {ql.object_class}'.encode()


@get_status.register(qlast.DropObject)
def _ddl_drop(ql):
    return f'DROP {ql.object_class}'.encode()


@get_status.register(qlast.StartMigration)
def _ddl_migr_start(ql):
    return b'START MIGRATION'


@get_status.register(qlast.CreateMigration)
def _ddl_migr_create(ql):
    return b'CREATE MIGRATION'


@get_status.register(qlast.CommitMigration)
def _ddl_migr_commit(ql):
    return b'COMMIT MIGRATION'


@get_status.register(qlast.DropMigration)
def _ddl_migr_drop(ql):
    return b'DROP MIGRATION'


@get_status.register(qlast.AlterMigration)
def _ddl_migr_alter(ql):
    return b'ALTER MIGRATION'


@get_status.register(qlast.AbortMigration)
def _ddl_migr_abort(ql):
    return b'ABORT MIGRATION'


@get_status.register(qlast.PopulateMigration)
def _ddl_migr_populate(ql):
    return b'POPULATE MIGRATION'


@get_status.register(qlast.DescribeCurrentMigration)
def _ddl_migr_describe_current(ql):
    return b'DESCRIBE CURRENT MIGRATION'


@get_status.register(qlast.AlterCurrentMigrationRejectProposed)
def _ddl_migr_alter_current(ql):
    return b'ALTER CURRENT MIGRATION'


@get_status.register(qlast.StartMigrationRewrite)
def _ddl_migr_rw_start(ql):
    return b'START MIGRATION REWRITE'


@get_status.register(qlast.CommitMigrationRewrite)
def _ddl_migr_rw_commit(ql):
    return b'COMMIT MIGRATION REWRITE'


@get_status.register(qlast.AbortMigrationRewrite)
def _ddl_migr_rw_abort(ql):
    return b'ABORT MIGRATION REWRITE'


@get_status.register(qlast.ResetSchema)
def _ddl_migr_reset_schema(ql):
    return b'RESET SCHEMA'


@get_status.register(qlast.SelectQuery)
@get_status.register(qlast.GroupQuery)
@get_status.register(qlast.ForQuery)
def _select(ql):
    return b'SELECT'


@get_status.register(qlast.InsertQuery)
def _insert(ql):
    return b'INSERT'


@get_status.register(qlast.UpdateQuery)
def _update(ql):
    return b'UPDATE'


@get_status.register(qlast.DeleteQuery)
def _delete(ql):
    return b'DELETE'


@get_status.register(qlast.StartTransaction)
def _tx_start(ql):
    return b'START TRANSACTION'


@get_status.register(qlast.CommitTransaction)
def _tx_commit(ql):
    return b'COMMIT TRANSACTION'


@get_status.register(qlast.RollbackTransaction)
def _tx_rollback(ql):
    return b'ROLLBACK TRANSACTION'


@get_status.register(qlast.DeclareSavepoint)
def _tx_sp_declare(ql):
    return b'DECLARE SAVEPOINT'


@get_status.register(qlast.RollbackToSavepoint)
def _tx_sp_rollback(ql):
    return b'ROLLBACK TO SAVEPOINT'


@get_status.register(qlast.ReleaseSavepoint)
def _tx_sp_release(ql):
    return b'RELEASE SAVEPOINT'


@get_status.register(qlast.SessionSetAliasDecl)
def _sess_set_alias(ql):
    return b'SET ALIAS'


@get_status.register(qlast.SessionResetAliasDecl)
@get_status.register(qlast.SessionResetModule)
@get_status.register(qlast.SessionResetAllAliases)
def _sess_reset_alias(ql):
    return b'RESET ALIAS'


@get_status.register(qlast.ConfigOp)
def _sess_set_config(ql):
    if ql.scope == qltypes.ConfigScope.GLOBAL:
        if isinstance(ql, qlast.ConfigSet):
            return b'SET GLOBAL'
        else:
            return b'RESET GLOBAL'
    else:
        return f'CONFIGURE {ql.scope}'.encode('ascii')


@get_status.register(qlast.DescribeStmt)
def _describe(ql):
    return f'DESCRIBE'.encode()


@get_status.register(qlast.Rename)
def _rename(ql):
    return f'RENAME'.encode()


@get_status.register(qlast.ExplainStmt)
def _explain(ql):
    return b'EXPLAIN'
