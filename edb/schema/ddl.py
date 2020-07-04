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

from collections import defaultdict
from typing import *  # noqa

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.edgeql import declarative as s_decl

from . import delta as sd
from . import expr as s_expr
from . import migrations as s_migr
from . import modules as s_mod
from . import objects as so
from . import objtypes as s_objtypes
from . import ordering as s_ordering
from . import schema as s_schema


if TYPE_CHECKING:
    import uuid


def delta_schemas(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    schema_a_filters: Iterable[
        Callable[[s_schema.Schema, so.Object], bool]
    ] = (),
    schema_b_filters: Iterable[
        Callable[[s_schema.Schema, so.Object], bool]
    ] = (),
    include_module_diff: bool=True,
    include_std_diff: bool=False,
    include_derived_types: bool=True,
    include_migrations: bool=False,
    linearize_delta: bool=True,
) -> sd.DeltaRoot:
    """Return difference between *schema_a* and *schema_b*.

    The returned object is a delta tree that, when applied to
    *schema_a* results in *schema_b*.

    Args:
        schema_a:
            Schema to use as a starting state.  If ``None``,
            then a schema with only standard modules is assumed,
            unless *include_std_diff* is ``True``, in which case
            an entirely empty schema is assumed as a starting point.

        schema_b:
            Schema to use as the ending state.

        included_modules:
            Optional list of modules to include in the delta.

        excluded_modules:
            Optional list of modules to exlude from the delta.
            Takes precedence over *included_modules*.
            NOTE: standard library modules are always excluded,
            unless *include_std_diff* is ``True``.

        included_items:
            Optional list of names of objects to include in the delta.

        excluded_items:
            Optional list of names of objects to exclude from the delta.
            Takes precedence over *included_items*.

        schema_a_filters:
            Optional list of additional filters to place on *schema_a*.

        schema_b_filters:
            Optional list of additional filters to place on *schema_b*.

        include_module_diff:
            Whether to include create/drop module operations
            in the delta diff.

        include_std_diff:
            Whether to include the standard library in the diff.

        include_derived_types:
            Whether to include derived types, like unions, in the diff.

        include_migrations:
            Whether to include migrations in the diff.

        linearize_delta:
            Whether the resulting diff should be properly ordered
            using the dependencies between objects.

    Returns:
        A :class:`schema.delta.DeltaRoot` instances representing
        the delta between *schema_a* and *schema_b*.
    """

    result = sd.DeltaRoot(canonical=True)

    schema_a_filters = list(schema_a_filters)
    schema_b_filters = list(schema_b_filters)

    if schema_a is None:
        if include_std_diff:
            schema_a = s_schema.Schema()
        else:
            schema_a = schema_b

            def _filter(schema: s_schema.Schema, obj: so.Object) -> bool:
                return (
                    (isinstance(obj, so.QualifiedObject)
                        and (obj.get_name(schema).module
                             in s_schema.STD_MODULES))
                    or (isinstance(obj, s_mod.Module)
                        and obj.get_name(schema) in s_schema.STD_MODULES)
                )
            schema_a_filters.append(_filter)

    my_modules = {
        m.get_name(schema_b)
        for m in schema_b.get_objects(
            type=s_mod.Module,
            extra_filters=schema_b_filters,
        )
    }

    other_modules = {
        m.get_name(schema_a)
        for m in schema_a.get_objects(
            type=s_mod.Module,
            extra_filters=schema_a_filters,
        )
    }

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules

    if excluded_modules is None:
        excluded_modules = set()
    else:
        excluded_modules = set(excluded_modules)

    if not include_std_diff:
        excluded_modules.update(s_schema.STD_MODULES)

        def _filter(schema: s_schema.Schema, obj: so.Object) -> bool:
            return not obj.get_builtin(schema)

        schema_a_filters.append(_filter)
        schema_b_filters.append(_filter)

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
            create = s_mod.CreateModule(
                classname=added_module,
                if_not_exists=True,
            )
            create.set_attribute_value('name', added_module)
            create.set_attribute_value('builtin', False)
            result.add(create)

    objects = sd.DeltaRoot(canonical=True)
    context = so.ComparisonContext()

    schemaclasses = [
        schemacls
        for schemacls in so.ObjectMeta.get_schema_metaclasses()
        if (
            not issubclass(schemacls, (so.GlobalObject, s_mod.Module))
            and schemacls.get_ql_class() is not None
            and (
                include_migrations
                or not issubclass(schemacls, s_migr.Migration)
            )
        )
    ]

    for sclass in schemaclasses:
        filters: List[Callable[[s_schema.Schema, so.Object], bool]] = []

        if not issubclass(sclass, so.QualifiedObject):
            # UnqualifiedObjects (like anonymous tuples and arrays)
            # should not use an included_modules filter.
            incl_modules = None
        else:
            if issubclass(sclass, so.DerivableObject):
                def _only_generic(
                    schema: s_schema.Schema,
                    obj: so.Object,
                ) -> bool:
                    assert isinstance(obj, so.DerivableObject)
                    return obj.generic(schema)
                filters.append(_only_generic)
            incl_modules = included_modules

        new = schema_b.get_objects(
            type=sclass,
            included_modules=incl_modules,
            excluded_modules=excluded_modules,
            included_items=included_items,
            excluded_items=excluded_items,
            extra_filters=filters + schema_b_filters,
        )
        old = schema_a.get_objects(
            type=sclass,
            included_modules=incl_modules,
            excluded_modules=excluded_modules,
            included_items=included_items,
            excluded_items=excluded_items,
            extra_filters=filters + schema_a_filters,
        )

        objects.add(
            sd.delta_objects(
                old,
                new,
                old_schema=schema_a,
                new_schema=schema_b,
                context=context,
            )
        )

    if linearize_delta:
        objects = s_ordering.linearize_delta(
            objects, old_schema=schema_a, new_schema=schema_b)

    if include_derived_types:
        result.add(objects)
    else:
        for cmd in objects.get_subcommands():
            if isinstance(cmd, s_objtypes.ObjectTypeCommand):
                if isinstance(cmd, s_objtypes.DeleteObjectType):
                    relevant_schema = schema_a
                else:
                    relevant_schema = schema_b

                obj = cast(s_objtypes.ObjectType,
                           relevant_schema.get(cmd.classname))
                if obj.is_union_type(relevant_schema):
                    continue

            result.add(cmd)

    if include_module_diff:
        for dropped_module in dropped_modules:
            result.add(s_mod.DeleteModule(classname=dropped_module))

    return result


def cmd_from_ddl(
    stmt: qlast.DDLCommand,
    *,
    context: Optional[sd.CommandContext]=None,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    testmode: bool=False
) -> sd.Command:
    ddl = s_expr.imprint_expr_context(stmt, modaliases)
    assert isinstance(ddl, qlast.DDLCommand)

    if context is None:
        context = sd.CommandContext(
            schema=schema, modaliases=modaliases, testmode=testmode)

    return sd.compile_ddl(schema, ddl, context=context)


def apply_sdl(
    sdl_document: qlast.Schema,
    *,
    base_schema: s_schema.Schema,
    current_schema: s_schema.Schema,
    stdmode: bool = False,
    testmode: bool = False,
) -> s_schema.Schema:
    # group declarations by module
    documents: Dict[str, List[qlast.DDL]] = defaultdict(list)
    # initialize the "default" module
    documents['default'] = []
    for decl in sdl_document.declarations:
        # declarations are either in a module block or fully-qualified
        if isinstance(decl, qlast.ModuleDeclaration):
            documents[decl.name.name].extend(decl.declarations)
        else:
            documents[decl.name.module].append(decl)

    ddl_stmts = s_decl.sdl_to_ddl(current_schema, documents)
    context = sd.CommandContext(
        modaliases={},
        schema=base_schema,
        stdmode=stdmode,
        testmode=testmode,
        declarative=True,
    )

    target_schema = base_schema
    for ddl_stmt in ddl_stmts:
        delta = sd.DeltaRoot()
        with context(sd.DeltaRootContext(schema=target_schema, op=delta)):
            cmd = cmd_from_ddl(
                ddl_stmt, schema=target_schema, modaliases={},
                context=context, testmode=testmode)

            delta.add(cmd)
            target_schema = delta.apply(target_schema, context)
            context.schema = target_schema

    return target_schema


def apply_ddl(
    ddl_stmt: qlast.DDLCommand,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    stdmode: bool=False,
    testmode: bool=False,
) -> s_schema.Schema:
    schema, _ = _delta_from_ddl(ddl_stmt, schema=schema, modaliases=modaliases,
                                stdmode=stdmode, testmode=testmode)
    return schema


def apply_ddl_script(
    ddl_text: str,
    *,
    schema: s_schema.Schema,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    stdmode: bool = False,
    testmode: bool = False,
) -> s_schema.Schema:

    schema, _ = apply_ddl_script_ex(
        ddl_text,
        schema=schema,
        modaliases=modaliases,
        stdmode=stdmode,
        testmode=testmode,
    )

    return schema


def apply_ddl_script_ex(
    ddl_text: str,
    *,
    schema: s_schema.Schema,
    modaliases: Optional[Mapping[Optional[str], str]] = None,
    stdmode: bool = False,
    testmode: bool = False,
) -> Tuple[s_schema.Schema, sd.DeltaRoot]:

    delta = sd.DeltaRoot()

    if modaliases is None:
        modaliases = {}

    for ddl_stmt in edgeql.parse_block(ddl_text):
        schema, cmd = _delta_from_ddl(
            ddl_stmt,
            schema=schema,
            modaliases=modaliases,
            stdmode=stdmode,
            testmode=testmode,
        )

        delta.add(cmd)

    return schema, delta


def delta_from_ddl(
    ddl_stmt: qlast.DDLCommand,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    stdmode: bool=False,
    testmode: bool=False,
    schema_object_ids: Optional[
        Mapping[Tuple[str, Optional[str]], uuid.UUID]
    ]=None,
) -> sd.DeltaRoot:
    _, cmd = _delta_from_ddl(ddl_stmt, schema=schema, modaliases=modaliases,
                             stdmode=stdmode, testmode=testmode,
                             schema_object_ids=schema_object_ids)
    return cmd


def _delta_from_ddl(
    ddl_stmt: qlast.DDLCommand,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    stdmode: bool=False,
    testmode: bool=False,
    schema_object_ids: Optional[
        Mapping[Tuple[str, Optional[str]], uuid.UUID]
    ]=None,
) -> Tuple[s_schema.Schema, sd.DeltaRoot]:
    delta = sd.DeltaRoot()
    context = sd.CommandContext(
        modaliases=modaliases,
        schema=schema,
        stdmode=stdmode,
        testmode=testmode,
        schema_object_ids=schema_object_ids,
    )

    with context(sd.DeltaRootContext(schema=schema, op=delta)):
        cmd = cmd_from_ddl(
            ddl_stmt,
            schema=schema,
            modaliases=modaliases,
            context=context,
            testmode=testmode,
        )
        schema = cmd.apply(schema, context)
        delta.add(cmd)

    delta.canonical = True
    return schema, delta


def ddlast_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    descriptive_mode: bool = False,
) -> Tuple[qlast.DDLOperation, ...]:

    context = sd.CommandContext(
        descriptive_mode=descriptive_mode,
        declarative=sdlmode,
    )

    stmts = []
    for command in delta.get_subcommands():
        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            ql_ast = command.get_ast(schema, context)
            if ql_ast:
                stmts.append(ql_ast)

    return tuple(stmts)


def statements_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    descriptive_mode: bool = False,
    limit_ref_classes: Iterable[so.ObjectMeta] = tuple(),
) -> Tuple[str, ...]:

    stmts = ddlast_from_delta(
        schema,
        delta,
        sdlmode=sdlmode,
        descriptive_mode=descriptive_mode,
    )

    ql_classes_src = {
        scls.get_ql_class() for scls in limit_ref_classes
    }

    ql_classes = {q for q in ql_classes_src if q is not None}

    text = []
    for stmt_ast in stmts:
        stmt_text = edgeql.generate_source(
            stmt_ast,
            sdlmode=sdlmode,
            descmode=descriptive_mode,
            limit_ref_classes=ql_classes,
        )
        text.append(stmt_text + ';')

    return tuple(text)


def text_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    descriptive_mode: bool = False,
    limit_ref_classes: Iterable[so.ObjectMeta] = tuple(),
) -> str:
    stmts = statements_from_delta(
        schema,
        delta,
        sdlmode=sdlmode,
        descriptive_mode=descriptive_mode,
        limit_ref_classes=limit_ref_classes,
    )
    return '\n'.join(stmts)


def ddl_text_from_delta(
    schema: s_schema.Schema,
    delta: sd.DeltaRoot,
) -> str:
    """Return DDL text corresponding to a delta plan.

    Args:
        schema:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.

    Returns:
        DDL text corresponding to *delta*.
    """
    return text_from_delta(schema, delta, sdlmode=False)


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
    return text_from_delta(schema, delta, sdlmode=True)


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
    return text_from_delta(
        schema,
        delta,
        sdlmode=True,
        descriptive_mode=True,
        limit_ref_classes=limit_ref_classes,
    )


def ddl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
) -> str:
    diff = delta_schemas(
        schema_a=None,
        schema_b=schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
        include_derived_types=False,
    )
    return ddl_text_from_delta(schema, diff)


def sdl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[str]]=None,
    excluded_modules: Optional[Iterable[str]]=None,
    included_items: Optional[Iterable[str]]=None,
    excluded_items: Optional[Iterable[str]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
) -> str:
    diff = delta_schemas(
        schema_a=None,
        schema_b=schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
        include_derived_types=False,
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
    include_derived_types: bool=False,
) -> str:
    diff = delta_schemas(
        schema_a=None,
        schema_b=schema,
        included_modules=included_modules,
        excluded_modules=excluded_modules,
        included_items=included_items,
        excluded_items=excluded_items,
        include_module_diff=include_module_ddl,
        include_std_diff=include_std_ddl,
        include_derived_types=False,
        linearize_delta=False,
    )
    return descriptive_text_from_delta(
        schema, diff, limit_ref_classes=included_ref_classes)
