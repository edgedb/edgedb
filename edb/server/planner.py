#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.schema import ddl as s_ddl

from edb.server.pgsql import compiler


class TransactionStatement:
    def __init__(self, qlnode):
        if isinstance(qlnode, qlast.StartTransaction):
            self.op = 'start'
        elif isinstance(qlnode, qlast.CommitTransaction):
            self.op = 'commit'
        elif isinstance(qlnode, qlast.RollbackTransaction):
            self.op = 'rollback'
        else:
            raise ValueError(
                'expecting transaction node, '
                'got {!r}'.format(qlnode))

    def __repr__(self):
        return '<{} {!r} at 0x{:x}>'.format(self.__name__, self.op, id(self))


def plan_statement(stmt, backend, flags={}, *, timer):
    schema = backend.schema
    modaliases = backend.modaliases

    if isinstance(stmt, qlast.Database):
        # CREATE/ALTER/DROP DATABASE
        return s_ddl.cmd_from_ddl(stmt, schema=schema, modaliases=modaliases)

    elif isinstance(stmt, qlast.Delta):
        # CREATE/APPLY MIGRATION
        return s_ddl.cmd_from_ddl(stmt, schema=schema, modaliases=modaliases)

    elif isinstance(stmt, qlast.DDL):
        # CREATE/DELETE/ALTER (FUNCTION, TYPE, etc)
        return s_ddl.delta_from_ddl(stmt, schema=schema, modaliases=modaliases)

    elif isinstance(stmt, qlast.Transaction):
        # BEGIN/COMMIT
        return TransactionStatement(stmt)

    elif isinstance(stmt, qlast.SessionStateDecl):
        # SET ...
        with timer.timeit('compile_eql_to_ir'):
            ir = ql_compiler.compile_ast_to_ir(
                stmt, schema=schema, modaliases=modaliases)

        return ir

    else:
        # Queries
        with timer.timeit('compile_eql_to_ir'):
            ir = ql_compiler.compile_ast_to_ir(
                stmt, schema=schema, modaliases=modaliases,
                implicit_id_in_shapes=False)

        return backend.compile(ir, output_format=compiler.OutputFormat.JSON,
                               timer=timer)
