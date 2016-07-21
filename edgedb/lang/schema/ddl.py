##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import caosql
from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as s_delta


# The below must be imported here to make sure we have all
# necessary mappers from/to DDL AST.
#
from . import atoms  # NOQA
from . import attributes  # NOQA
from . import concepts  # NOQA
from . import constraints  # NOQA
from . import functions  # NOQA
from . import indexes  # NOQA
from . import links  # NOQA
from . import lproperties  # NOQA
from . import modules  # NOQA
from . import policy  # NOQA


def delta_from_ddl(stmts):
    alter_db = s_db.AlterDatabase()
    context = s_delta.CommandContext()

    if isinstance(stmts, caosql.ast.Base):
        stmts = [stmts]

    for stmt in stmts:
        ddl = caosql.deoptimize(stmt)

        with context(s_db.DatabaseCommandContext(alter_db)):
            cmd = s_delta.Command.from_ast(ddl, context=context)
            alter_db.add(cmd)

    return alter_db


def ddl_from_delta(delta):
    """Return DDL AST for a delta command tree."""
    return delta.get_ast()


def ddl_text_from_delta(delta):
    """Return DDL text for a delta command tree."""
    if isinstance(delta, s_db.AlterDatabase):
        commands = delta
    else:
        commands = [delta]

    text = []
    for command in commands:
        delta_ast = ddl_from_delta(command)
        text.append(caosql.generate_source(caosql.optimize(delta_ast)))

    return '\n'.join(text)
