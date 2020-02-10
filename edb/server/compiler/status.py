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


@functools.singledispatch
def get_status(ql: qlast.Base) -> bytes:
    raise NotImplementedError(
        f'cannot get status for the {type(ql).__name__!r} AST node')


@get_status.register(qlast.CreateObject)
def _ddl_create(ql):
    return b'CREATE'


@get_status.register(qlast.AlterObject)
def _ddl_alter(ql):
    return b'ALTER'


@get_status.register(qlast.DropObject)
def _ddl_drop(ql):
    return b'DROP'


@get_status.register(qlast.CreateMigration)
def _ddl_migr_create(ql):
    return b'CREATE MIGRATION'


@get_status.register(qlast.GetMigration)
def _ddl_migr_get(ql):
    return b'GET MIGRATION'


@get_status.register(qlast.CommitMigration)
def _ddl_migr_commit(ql):
    return b'COMMIT MIGRATION'


@get_status.register(qlast.DropMigration)
def _ddl_migr_drop(ql):
    return b'DROP MIGRATION'


@get_status.register(qlast.AlterMigration)
def _ddl_migr_alter(ql):
    return b'ALTER MIGRATION'


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
    if ql.system:
        return b'CONFIGURE SYSTEM'
    else:
        return b'CONFIGURE SESSION'


@get_status.register(qlast.DescribeStmt)
def _describe(ql):
    return f'DESCRIBE'.encode()


@get_status.register(qlast.Rename)
def _rename(ql):
    return f'RENAME'.encode()
