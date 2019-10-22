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


from __future__ import annotations

from edb import edgeql

from edb.edgeql import declarative as s_decl

from . import delta as sd
from . import derivable
from . import objects as so
from . import ordering as s_ordering
from . import schema as s_schema

# The below must be imported here to make sure we have all
# necessary mappers from/to DDL AST.

from . import scalars  # NOQA
from . import annos  # NOQA
from . import casts  # NOQA
from . import expr as s_expr
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


def get_global_dep_order():
    return (
        annos.Annotation,
        functions.Function,
        constraints.Constraint,
        scalars.ScalarType,
        lproperties.Property,
        links.Link,
        objtypes.BaseObjectType,
    )


def delta_schemas(schema1, schema2):
    result = sd.DeltaRoot(canonical=True)

    my_modules = set(schema1.get_objects(type=modules.Module))
    other_modules = set(schema2.get_objects(type=modules.Module))

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules

    for added_module in added_modules:
        create = modules.CreateModule(classname=added_module)
        create.add(sd.AlterObjectProperty(property='name', old_value=None,
                                          new_value=added_module))
        result.add(create)

    for type in get_global_dep_order():
        new = schema1.get_objects(type=type)
        old = schema2.get_objects(type=type)

        if issubclass(type, derivable.DerivableObject):
            new = filter(lambda i: i.generic(schema1), new)
            old = filter(lambda i: i.generic(schema2), old)

        result.update(so.Object.delta_sets(
            old, new, old_schema=schema2, new_schema=schema1))

    result = s_ordering.linearize_delta(
        result, old_schema=schema2, new_schema=schema1)

    for dropped_module in dropped_modules:
        result.add(modules.DeleteModule(classname=dropped_module))

    return result


def delta_modules(schema1, schema2, modnames):
    from . import derivable

    result = sd.DeltaRoot(canonical=True)

    for type in get_global_dep_order():
        new = schema1.get_objects(modules=modnames, type=type)
        old = schema2.get_objects(modules=modnames, type=type)

        if issubclass(type, derivable.DerivableObject):
            new = filter(lambda i: i.generic(schema1), new)
            old = filter(lambda i: i.generic(schema2), old)

        result.update(so.Object.delta_sets(
            old, new, old_schema=schema2, new_schema=schema1))

    result = s_ordering.linearize_delta(
        result, old_schema=schema2, new_schema=schema1)

    return result


def cmd_from_ddl(stmt, *, context=None, schema, modaliases,
                 testmode: bool=False):
    ddl = s_expr.imprint_expr_context(stmt, modaliases)

    if context is None:
        context = sd.CommandContext(
            schema=schema, modaliases=modaliases, testmode=testmode)

    cmd = sd.Command.from_ast(schema, ddl, context=context)
    return cmd


def compile_migration(cmd, target_schema, current_schema):

    declarations = cmd.get_attribute_value('target')
    if not declarations:
        return cmd

    target_schema = apply_sdl(
        [(cmd.classname.module, declarations)],
        target_schema=target_schema,
        current_schema=current_schema)

    stdmodules = s_schema.STD_MODULES
    moditems = target_schema.get_objects(type=modules.Module)
    modnames = (
        {m.get_name(target_schema) for m in moditems}
        - stdmodules
    )

    if len(modnames - {'__derived__'}) > 1:
        raise RuntimeError('unexpected delta module structure')

    diff = delta_modules(target_schema, current_schema, modnames)
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


def apply_sdl(documents, *, target_schema, current_schema,
              stdmode: bool=False, testmode: bool=False):
    ddl_stmts = s_decl.sdl_to_ddl(current_schema, documents)
    context = sd.CommandContext(
        modaliases={},
        schema=target_schema,
        stdmode=stdmode,
        testmode=testmode,
        declarative=True,
    )

    for ddl_stmt in ddl_stmts:
        delta = sd.DeltaRoot()
        with context(sd.DeltaRootContext(schema=target_schema, op=delta)):
            cmd = cmd_from_ddl(
                ddl_stmt, schema=target_schema, modaliases={},
                context=context, testmode=testmode)
            delta.add(cmd)
            target_schema, _ = delta.apply(target_schema, context)
            context.schema = target_schema

    return target_schema


def apply_ddl(ddl_stmt, *, schema, modaliases,
              stdmode: bool=False, testmode: bool=False):
    schema, _ = _delta_from_ddl(ddl_stmt, schema=schema, modaliases=modaliases,
                                stdmode=stdmode, testmode=testmode)
    return schema


def delta_from_ddl(ddl_stmt, *, schema, modaliases,
                   stdmode: bool=False, testmode: bool=False):
    _, cmd = _delta_from_ddl(ddl_stmt, schema=schema, modaliases=modaliases,
                             stdmode=stdmode, testmode=testmode)
    return cmd


def _delta_from_ddl(ddl_stmt, *, schema, modaliases,
                    stdmode: bool=False, testmode: bool=False):
    delta = sd.DeltaRoot()
    context = sd.CommandContext(
        modaliases=modaliases,
        schema=schema,
        stdmode=stdmode,
        testmode=testmode,
    )

    with context(sd.DeltaRootContext(schema=schema, op=delta)):
        cmd = cmd_from_ddl(
            ddl_stmt, schema=schema, modaliases={},
            context=context, testmode=testmode)
        schema, _ = cmd.apply(schema, context)
        delta.add(cmd)

    delta.canonical = True
    return schema, delta


def ddl_from_delta(schema, context, delta):
    """Return DDL AST for a delta command tree."""
    return delta.get_ast(schema, context)


def ddl_text_from_migration(schema, migration):
    """Return DDL text for a migration object."""

    root = sd.DeltaRoot(canonical=True)
    root.update(migration.get_commands(schema))

    context = sd.CommandContext()
    schema, _ = root.apply(schema, context)

    context = sd.CommandContext()
    text = []
    for command in root.get_subcommands():
        with context(sd.DeltaRootContext(schema=schema, op=root)):
            delta_ast = ddl_from_delta(schema, context, command)
            if delta_ast:
                stmt_text = edgeql.generate_source(delta_ast)
                text.append(stmt_text + ';')

    return '\n'.join(text)
