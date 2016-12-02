##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.schema import ddl as s_ddl


class TransactionStatement:
    def __init__(self, qlnode):
        if isinstance(qlnode, qlast.StartTransactionNode):
            self.op = 'start'
        elif isinstance(qlnode, qlast.CommitTransactionNode):
            self.op = 'commit'
        elif isinstance(qlnode, qlast.RollbackTransactionNode):
            self.op = 'rollback'
        else:
            raise ValueError(
                'expecting transaction node, '
                'got {!r}'.format(qlnode))

    def __repr__(self):
        return '<{} {!r} at 0x{:x}>'.format(self.__name__, self.op, id(self))


def plan_statement(stmt, backend, flags={}):
    if isinstance(stmt, qlast.DatabaseNode):
        return s_ddl.cmd_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.DeltaNode):
        return s_ddl.cmd_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.DDLNode):
        return s_ddl.delta_from_ddl(stmt, schema=backend.schema)

    elif isinstance(stmt, qlast.TransactionNode):
        return TransactionStatement(stmt)

    else:
        ir = edgeql.compile_ast_to_ir(stmt, schema=backend.schema)
        return backend.compile(ir, output_format='json')
