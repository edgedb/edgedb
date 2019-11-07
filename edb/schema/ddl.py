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

from typing import *  # noqa

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
from . import migrations
from . import operators  # NOQA
from . import indexes  # NOQA
from . import links  # NOQA
from . import lproperties  # NOQA
from . import modules  # NOQA
from . import std  # NOQA
from . import types
from . import views  # NOQA


def get_global_dep_order():
    return (
        annos.Annotation,
        functions.Function,
        constraints.Constraint,
        scalars.ScalarType,
        # schema arrays and tuples are UnqualifiedObject
        types.SchemaArray,
        types.SchemaTuple,
        # views are treated separately because they are not UnqualifiedObject
        types.ArrayView,
        types.TupleView,
        lproperties.Property,
        links.Link,
        objtypes.BaseObjectType,
    )


def delta_schemas(
    schema1: s_schema.Schema,
    schema2: s_schema.Schema,
    *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    include_module_diff: bool=True,
    include_std_diff: bool=False,
    linearize_delta: bool=True,
) -> sd.DeltaRoot:
    """Return difference between *schema1* and *schema2*.

    The returned object is a delta tree that, when applied to
    *schema2* results in *schema1*.

    Args:
        included_modules:
            Optional list of modules to include in the delta.

        excluded_modules:
            Optional list of modules to exlude from the delta.
            Takes precedence over *included_modules*.
            NOTE: standard library modules are always excluded.

        included_items:
            Optional list of names of objects to include in the delta.

        excluded_items:
            Optional list of names of objects to exclude from the delta.
            Takes precedence over *included_items*.

        include_module_diff:
            Whether to include create/drop module operations
            in the delta diff.

        include_std_diff:
            Whether to include the standard library in the diff.

        linearize_delta:
            Whether the resulting diff should be properly ordered
            using the dependencies between objects.

    Returns:
        A :class:`schema.delta.DeltaRoot` instances representing
        the delta between *schema2* and *schema1*.
    """

    result = sd.DeltaRoot(canonical=True)

    my_modules = {m.get_name(schema1)
                  for m in schema1.get_objects(type=modules.Module)}

    other_modules = {m.get_name(schema2)
                     for m in schema2.get_objects(type=modules.Module)}

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules

    if excluded_modules is None:
        excluded_modules = set()
    else:
        excluded_modules = set(excluded_modules)

    if not include_std_diff:
        excluded_modules.update(s_schema.STD_MODULES)

    # __derived__ is ephemeral and should never be included
    excluded_modules.add('__derived__')

    if included_modules is not None:
        included_modules = set(included_modules)

        added_modules &= included_modules
        dropped_modules &= included_modules

    if excluded_modules:
        added_modules -= excluded_modules
        dropped_modules -= excluded_modules

    if included_items is not None:
        included_items = set(included_items)

    if excluded_items is not None:
        excluded_items = set(excluded_items)

    if include_module_diff:
        for added_module in added_modules:
            create = modules.CreateModule(classname=added_module)
            create.set_attribute_value('name', added_module)
            result.add(create)

    objects = sd.DeltaRoot(canonical=True)

    for sclass in get_global_dep_order():
        filters = []

        if issubclass(sclass, derivable.DerivableObject):
            filters.append(lambda schema, obj: obj.generic(schema))

        if issubclass(sclass, so.UnqualifiedObject):
            # UnqualifiedObjects (like anonymous tuples and arrays)
            # should not use an included_modules filter.
            new = schema1.get_objects(
                type=sclass,
                excluded_modules=excluded_modules,
                included_items=included_items,
                excluded_items=excluded_items,
                extra_filters=filters,
            )
            old = schema2.get_objects(
                type=sclass,
                excluded_modules=excluded_modules,
                included_items=included_items,
                excluded_items=excluded_items,
                extra_filters=filters,
            )
        else:
            new = schema1.get_objects(
                type=sclass,
                included_modules=included_modules,
                excluded_modules=excluded_modules,
                included_items=included_items,
                excluded_items=excluded_items,
                extra_filters=filters,
            )
            old = schema2.get_objects(
                type=sclass,
                included_modules=included_modules,
                excluded_modules=excluded_modules,
                included_items=included_items,
                excluded_items=excluded_items,
                extra_filters=filters,
            )

        objects.update(so.Object.delta_sets(
            old, new, old_schema=schema2, new_schema=schema1))

    if linearize_delta:
        objects = s_ordering.linearize_delta(
            objects, old_schema=schema2, new_schema=schema1)

    result.update(objects)

    if include_module_diff:
        for dropped_module in dropped_modules:
            result.add(modules.DeleteModule(
                classname=dropped_module.get_name(schema2)))

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

    diff = delta_schemas(target_schema, current_schema,
                         included_modules=modnames)
    cmd.set_attribute_value('delta', diff)

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


def _text_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool,
    descriptive_mode: bool = False,
    limit_ref_classes: Iterable[so.ObjectMeta] = tuple(),
    emit_oids: bool = False,
) -> str:

    context = sd.CommandContext(
        descriptive_mode=descriptive_mode,
        emit_oids=emit_oids,
    )
    text = []
    for command in delta.get_subcommands():
        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            delta_ast = command.get_ast(schema, context)
            if delta_ast:
                ql_classes = [
                    scls.get_ql_class() for scls in limit_ref_classes
                ]

                stmt_text = edgeql.generate_source(
                    delta_ast, sdlmode=sdlmode,
                    descmode=descriptive_mode,
                    limit_ref_classes=ql_classes,
                )
                text.append(stmt_text + ';')

    return '\n'.join(text)


def ddl_text_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    emit_oids: bool = False,
) -> str:
    """Return DDL text corresponding to a delta plan.

    Args:
        schema:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.
        emit_oids:
            Whether object ids should be included in the output.

    Returns:
        DDL text corresponding to *delta*.
    """
    return _text_from_delta(schema, delta, sdlmode=False, emit_oids=emit_oids)


def sdl_text_from_delta(schema: s_schema.Schema, delta: sd.DeltaRoot) -> str:
    """Return SDL text corresponding to a delta plan.

    Args:
        schema:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.

    Returns:
        SDL text corresponding to *delta*.
    """
    return _text_from_delta(schema, delta, sdlmode=True)


def descriptive_text_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    limit_ref_classes: Iterable[so.ObjectMeta]=tuple(),
) -> str:
    """Return descriptive text corresponding to a delta plan.

    Args:
        schema:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.
        limit_ref_classes:
            If specified, limit the output of referenced objects
            to the specified classes.

    Returns:
        Descriptive text corresponding to *delta*.
    """
    return _text_from_delta(
        schema, delta, sdlmode=True, descriptive_mode=True,
        limit_ref_classes=limit_ref_classes)


def ddl_text_from_migration(
    schema: s_schema.Schema,
    migration: migrations.Migration
) -> str:
    """Return DDL text corresponding to a migration.

    Args:
        schema:
            Unlike :func:`ddl_text_from_schema`, this is the schema
            to which the *migration* has **not** already been
            applied.
        migration:
            The migration object.

    Returns:
        DDL text corresponding to the delta in *migration*.
    """
    delta = migration.get_delta(schema)
    context = sd.CommandContext()
    migrated_schema, _ = delta.apply(schema, context)
    return ddl_text_from_delta(migrated_schema, delta)


def ddl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
    emit_oids: bool=False,
) -> str:
    if include_std_ddl:
        empty_schema = s_schema.Schema()
    else:
        empty_schema = std.load_std_schema()
    diff = delta_schemas(
        schema,
        empty_schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
    )
    return ddl_text_from_delta(schema, diff, emit_oids=emit_oids)


def sdl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
    emit_oids: bool=False,
) -> str:
    if include_std_ddl:
        empty_schema = s_schema.Schema()
    else:
        empty_schema = std.load_std_schema()
    diff = delta_schemas(
        schema,
        empty_schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
        linearize_delta=False,
    )
    return sdl_text_from_delta(schema, diff)


def descriptive_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
    emit_oids: bool=False,
) -> str:
    if include_std_ddl:
        empty_schema = s_schema.Schema()
    else:
        empty_schema = std.load_std_schema()
    diff = delta_schemas(
        schema,
        empty_schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
        linearize_delta=False,
    )
    return descriptive_text_from_delta(
        schema, diff, limit_ref_classes=included_ref_classes)
