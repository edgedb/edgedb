##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import caosql
from edgedb.lang.caosql import ast as qlast
from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as s_delta


def plan_statement(stmt, backend):
    if isinstance(stmt, qlast.DDLNode):
        ddl = caosql.deoptimize(stmt)
        alter_db = s_db.AlterDatabase()
        context = s_delta.CommandContext()

        with context(s_db.DatabaseCommandContext(alter_db)):
            cmd = s_delta.Command.from_ast(ddl, context=context)
            alter_db.add(cmd)

        return alter_db

    else:
        ir = caosql.compile_ast_to_ir(stmt, schema=backend.schema)
        query = backend.compile(ir, output_format='json')

        return query
