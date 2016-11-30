##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import edgeql
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

from . import delta as sd


def cmd_from_ddl(stmt, *, context=None, schema):
    ddl = edgeql.deoptimize(stmt)
    cmd = s_delta.Command.from_ast(ddl, schema=schema, context=context)
    return cmd


def delta_from_ddl(stmts, *, schema):
    alter_db = s_db.AlterDatabase()
    context = s_delta.CommandContext()

    if isinstance(stmts, edgeql.ast.Base):
        stmts = [stmts]

    for stmt in stmts:
        with context(s_db.DatabaseCommandContext(alter_db)):
            alter_db.add(cmd_from_ddl(stmt, context=context, schema=schema))

    return alter_db


def ddl_from_delta(delta):
    """Return DDL AST for a delta command tree."""
    return delta.get_ast()


def ddl_text_from_delta_command(delta):
    """Return DDL text for a delta command tree."""
    if isinstance(delta, s_db.AlterDatabase):
        commands = delta
    else:
        commands = [delta]

    text = []
    for command in commands:
        delta_ast = ddl_from_delta(command)
        if delta_ast:
            stmt_text = edgeql.generate_source(edgeql.optimize(delta_ast))
            text.append(stmt_text + ';')

    return '\n'.join(text)


def ddl_text_from_delta(schema, delta):
    """Return DDL text for a delta object."""
    commands = []

    if delta.target is not None:
        diff = sd.delta_schemas(delta.target, schema)
        commands.extend(diff)

    if delta.commands:
        commands.extend(delta.commands)

    text = []
    for command in commands:
        cmd_text = ddl_text_from_delta_command(command)
        text.append(cmd_text)

    return '\n'.join(text)
