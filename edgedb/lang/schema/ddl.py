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


from edgedb.lang import edgeql
from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as s_delta


# The below must be imported here to make sure we have all
# necessary mappers from/to DDL AST.

from . import scalars  # NOQA
from . import attributes  # NOQA
from . import objtypes  # NOQA
from . import constraints  # NOQA
from . import functions  # NOQA
from . import indexes  # NOQA
from . import links  # NOQA
from . import lproperties  # NOQA
from . import modules  # NOQA
from . import policy  # NOQA
from . import views  # NOQA


def cmd_from_ddl(stmt, *, context=None, schema, modaliases):
    # expand module aliases (implicit and explicit)
    ddl = edgeql.deoptimize(stmt, strip_builtins=False)

    if context is None:
        context = s_delta.CommandContext()

    context.modaliases = modaliases
    context.schema = schema

    cmd = s_delta.Command.from_ast(ddl, schema=schema, context=context)
    return cmd


def delta_from_ddl(stmts, *, schema, modaliases):
    alter_db = s_db.AlterDatabase()
    context = s_delta.CommandContext()
    context.modaliases = modaliases
    context.schema = schema

    if isinstance(stmts, edgeql.ast.Base):
        stmts = [stmts]

    for stmt in stmts:
        with context(s_db.DatabaseCommandContext(alter_db)):
            alter_db.add(cmd_from_ddl(
                stmt, context=context, schema=schema, modaliases=modaliases))

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
            stmt_text = edgeql.generate_source(edgeql.optimize(
                delta_ast, strip_builtins=False))
            text.append(stmt_text + ';')

    return '\n'.join(text)


def ddl_text_from_delta(schema, delta):
    """Return DDL text for a delta object."""
    text = []
    for command in delta.commands:
        cmd_text = ddl_text_from_delta_command(command)
        text.append(cmd_text)

    return '\n'.join(text)
