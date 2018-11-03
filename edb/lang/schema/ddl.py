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


from edb.lang import edgeql
from edb.lang.schema import database as s_db
from edb.lang.schema import delta as s_delta


# The below must be imported here to make sure we have all
# necessary mappers from/to DDL AST.

from . import scalars  # NOQA
from . import attributes  # NOQA
from . import declarative as s_decl
from . import delta as sd
from . import objtypes  # NOQA
from . import constraints  # NOQA
from . import functions  # NOQA
from . import operators  # NOQA
from . import indexes  # NOQA
from . import links  # NOQA
from . import lproperties  # NOQA
from . import modules  # NOQA
from . import std  # NOQA
from . import views  # NOQA


def cmd_from_ddl(stmt, *, context=None, schema, modaliases,
                 testmode: bool=False):
    # expand module aliases (implicit and explicit)
    ddl = edgeql.deoptimize(stmt, strip_builtins=False)

    if context is None:
        context = s_delta.CommandContext()

    context.modaliases = modaliases
    context.schema = schema
    context.testmode = testmode

    cmd = s_delta.Command.from_ast(schema, ddl, context=context)
    return cmd


def compile_migration(cmd, target_schema, current_schema):

    declarations = cmd.get_attribute_value('target')
    if not declarations:
        return cmd

    target_schema = s_decl.load_module_declarations(target_schema, [
        (cmd.classname.module, declarations)
    ])

    stdmodules = std.STD_MODULES
    moditems = target_schema.get_objects(type=modules.Module)
    modnames = {m.get_name(target_schema) for m in moditems} - stdmodules
    if len(modnames) != 1:
        raise RuntimeError('unexpected delta module structure')

    modname = next(iter(modnames))

    diff = sd.delta_module(target_schema, current_schema, modname)
    migration = list(diff.get_subcommands())

    for op in cmd.get_subcommands(type=sd.AlterObjectProperty):
        if op.property == 'commands':
            op.new_value = migration + op.new_value
            break
    else:
        cmd.add(sd.AlterObjectProperty(
            property='commands',
            new_value=migration
        ))

    return cmd


def delta_from_ddl(stmts, *, schema, modaliases,
                   stdmode: bool=False, testmode: bool=False):
    alter_db = s_db.AlterDatabase()
    context = s_delta.CommandContext()
    context.modaliases = modaliases
    context.schema = schema
    context.stdmode = stdmode
    context.testmode = testmode

    if isinstance(stmts, edgeql.ast.Base):
        stmts = [stmts]

    for stmt in stmts:
        with context(s_db.DatabaseCommandContext(alter_db)):
            alter_db.add(cmd_from_ddl(
                stmt, context=context, schema=schema, modaliases=modaliases,
                testmode=testmode))

    return alter_db


def ddl_from_delta(schema, delta):
    """Return DDL AST for a delta command tree."""
    return delta.get_ast(schema, None)


def ddl_text_from_delta_command(schema, delta):
    """Return DDL text for a delta command tree."""
    if isinstance(delta, s_db.AlterDatabase):
        commands = delta
    else:
        commands = [delta]

    text = []
    for command in commands:
        delta_ast = ddl_from_delta(schema, command)
        if delta_ast:
            stmt_text = edgeql.generate_source(edgeql.optimize(
                delta_ast, strip_builtins=False))
            text.append(stmt_text + ';')

    return '\n'.join(text)


def ddl_text_from_delta(schema, delta):
    """Return DDL text for a delta object."""
    text = []
    for command in delta.get_commands(schema):
        cmd_text = ddl_text_from_delta_command(schema, command)
        text.append(cmd_text)

    return '\n'.join(text)
