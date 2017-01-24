##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import compiler as ql_compiler
from edgedb.lang.schema import ddl as s_ddl


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


def plan_statement(stmt, backend, flags={}):
    if isinstance(stmt, qlast.Database):
        # CREATE/ALTER/DROP DATABASE
        return s_ddl.cmd_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.Delta):
        # CREATE/APPLY MIGRATION
        return s_ddl.cmd_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.DDL):
        # CREATE/DELETE/ALTER (FUNCTION, CONCEPT, etc)
        return s_ddl.delta_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.Transaction):
        # BEGIN/COMMIT
        return TransactionStatement(stmt)

    else:
        # Queries
        ir = ql_compiler.compile_ast_to_ir(stmt, schema=backend.schema)
        return backend.compile(ir, output_format='json')
