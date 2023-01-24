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
from typing import *

from collections import defaultdict
import itertools

from edb import edgeql
from edb.common import uuidgen
from edb.edgeql import ast as qlast
from edb.edgeql import declarative as s_decl
from edb.server import defines

from . import delta as sd
from . import expr as s_expr
from . import functions as s_func
from . import migrations as s_migr
from . import modules as s_mod
from . import name as sn
from . import objects as so
from . import objtypes as s_objtypes
from . import ordering as s_ordering
from . import pseudo as s_pseudo
from . import schema as s_schema
from . import types as s_types
from . import version as s_ver


if TYPE_CHECKING:
    import uuid

    from edb.common import verutils


def delta_schemas(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    *,
    included_modules: Optional[Iterable[sn.Name]]=None,
    excluded_modules: Optional[Iterable[sn.Name]]=None,
    included_items: Optional[Iterable[sn.Name]]=None,
    excluded_items: Optional[Iterable[sn.Name]]=None,
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
    descriptive_mode: bool=False,
    generate_prompts: bool=False,
    guidance: Optional[so.DeltaGuidance]=None,
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

        descriptive_mode:
            DESCRIBE AS TEXT mode.

        generate_prompts:
            Whether to generate prompts that can be used in
            DESCRIBE MIGRATION.

        guidance:
            Optional explicit guidance to schema diff.

    Returns:
        A :class:`schema.delta.DeltaRoot` instances representing
        the delta between *schema_a* and *schema_b*.
    """

    result = sd.DeltaRoot(canonical=True)

    schema_a_filters = list(schema_a_filters)
    schema_b_filters = list(schema_b_filters)
    context = so.ComparisonContext(
        generate_prompts=generate_prompts,
        descriptive_mode=descriptive_mode,
        guidance=guidance,
    )

    if schema_a is None:
        if include_std_diff:
            schema_a = s_schema.FlatSchema()
        else:
            schema_a = schema_b

            def _filter(schema: s_schema.Schema, obj: so.Object) -> bool:
                return (
                    (
                        isinstance(obj, so.QualifiedObject)
                        and (
                            obj.get_name(schema).get_module_name()
                            in s_schema.STD_MODULES
                        )
                    ) or (
                        isinstance(obj, s_mod.Module)
                        and obj.get_name(schema) in s_schema.STD_MODULES
                    )
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
    excluded_modules.add(sn.UnqualName('__derived__'))

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
        for added_module in sorted(added_modules):
            if (
                guidance is None
                or (
                    (s_mod.Module, added_module)
                    not in guidance.banned_creations
                )
            ):
                mod = schema_b.get_global(s_mod.Module, added_module, None)
                assert mod is not None
                create = mod.as_create_delta(
                    schema=schema_b,
                    context=context,
                )
                assert isinstance(create, sd.CreateObject)
                create.if_not_exists = True
                # We currently fully assume that modules are created
                # or deleted and never renamed.  This is fine, because module
                # objects are never actually referenced directly, only by
                # the virtue of being the leading part of a fully-qualified
                # name.
                create.set_annotation('confidence', 1.0)

                result.add(create)

    excluded_classes = (
        so.GlobalObject,
        s_mod.Module,
        s_func.Parameter,
        s_pseudo.PseudoType,
    )

    schemaclasses = [
        schemacls
        for schemacls in so.ObjectMeta.get_schema_metaclasses()
        if (
            not issubclass(schemacls, excluded_classes)
            and not schemacls.is_abstract()
            and (
                include_migrations
                or not issubclass(schemacls, s_migr.Migration)
            )
        )
    ]

    assert not context.renames
    # We retry performing the diff until we stop finding new renames
    # and deletions. This allows us to be agnostic to the order that
    # we process schemaclasses.
    old_count = -1, -1
    while old_count != (len(context.renames), len(context.deletions)):
        old_count = len(context.renames), len(context.deletions)

        objects = sd.DeltaRoot(canonical=True)

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
                        return (
                            obj.generic(schema)
                            or (
                                isinstance(obj, s_types.Type)
                                and obj.get_from_global(schema)
                            )
                        )
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
                    sclass=sclass,
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
        # Process dropped modules in *reverse* sorted order, so that
        # `foo::bar` gets dropped before `foo`.
        for dropped_module in reversed(sorted(dropped_modules)):
            if (
                guidance is None
                or (
                    (s_mod.Module, dropped_module)
                    not in guidance.banned_deletions
                )
            ):
                mod = schema_a.get_global(s_mod.Module, dropped_module, None)
                assert mod is not None
                dropped = mod.as_delete_delta(
                    schema=schema_a,
                    context=context,
                )
                dropped.set_annotation('confidence', 1.0)

                result.add(dropped)

    return result


def cmd_from_ddl(
    stmt: qlast.DDLOperation,
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

    res = sd.compile_ddl(schema, ddl, context=context)
    context.early_renames.clear()
    return res


def apply_sdl(
    sdl_document: qlast.Schema,
    *,
    base_schema: s_schema.Schema,
    current_schema: s_schema.Schema,
    stdmode: bool = False,
    testmode: bool = False,
    allow_dml_in_functions: bool=False,
) -> s_schema.Schema:
    # group declarations by module
    documents: Dict[str, List[qlast.DDL]] = defaultdict(list)
    # initialize the "default" module
    documents[defines.DEFAULT_MODULE_ALIAS] = []
    extensions = {}
    futures = {}

    def collect(
        decl: qlast.NamedDDL | qlast.ModuleDeclaration,
        module: Optional[str],
    ) -> None:
        # declarations are either in a module block or fully-qualified
        if isinstance(decl, qlast.ModuleDeclaration):
            new_mod = (
                f'{module}::{decl.name.name}' if module else decl.name.name)
            # make sure the new one is present
            documents.setdefault(new_mod, [])
            for sdecl in decl.declarations:
                collect(sdecl, new_mod)
        elif isinstance(decl, qlast.CreateExtension):
            assert not module
            extensions[decl.name.name] = decl
        elif isinstance(decl, qlast.CreateFuture):
            assert not module
            futures[decl.name.name] = decl
        else:
            assert isinstance(decl, qlast.NamedDDL)
            assert module or decl.name.module is not None
            if decl.name.module is None:
                assert module
                name = module
            else:
                name = (
                    f'{module}::{decl.name.name}'
                    if module else decl.name.module)

            documents[name].append(decl)

    for decl in sdl_document.declarations:
        collect(decl, None)

    ddl_stmts = s_decl.sdl_to_ddl(current_schema, documents)
    context = sd.CommandContext(
        modaliases={},
        schema=base_schema,
        stdmode=stdmode,
        testmode=testmode,
        declarative=True,
        allow_dml_in_functions=allow_dml_in_functions,
    )

    target_schema = base_schema
    chained = itertools.chain(
        extensions.values(), futures.values(), ddl_stmts)
    for ddl_stmt in chained:
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
    internal_schema_mode: bool = False,
    testmode: bool = False,
) -> Tuple[s_schema.Schema, sd.DeltaRoot]:

    delta = sd.DeltaRoot()

    if modaliases is None:
        modaliases = {}

    for ddl_stmt in edgeql.parse_block(ddl_text):
        if not isinstance(ddl_stmt, qlast.DDLCommand):
            raise AssertionError(f'expected DDLCommand node, got {ddl_stmt!r}')
        schema, cmd = _delta_from_ddl(
            ddl_stmt,
            schema=schema,
            modaliases=modaliases,
            stdmode=stdmode,
            internal_schema_mode=internal_schema_mode,
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
    allow_dml_in_functions: bool=False,
    schema_object_ids: Optional[
        Mapping[Tuple[sn.Name, Optional[str]], uuid.UUID]
    ]=None,
    compat_ver: Optional[verutils.Version] = None,
) -> sd.DeltaRoot:
    _, cmd = _delta_from_ddl(
        ddl_stmt,
        schema=schema,
        modaliases=modaliases,
        stdmode=stdmode,
        testmode=testmode,
        allow_dml_in_functions=allow_dml_in_functions,
        schema_object_ids=schema_object_ids,
        compat_ver=compat_ver,
    )
    return cmd


def _delta_from_ddl(
    ddl_stmt: qlast.DDLCommand,
    *,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    stdmode: bool=False,
    internal_schema_mode: bool=False,
    testmode: bool=False,
    allow_dml_in_functions: bool=False,
    schema_object_ids: Optional[
        Mapping[Tuple[sn.Name, Optional[str]], uuid.UUID]
    ]=None,
    compat_ver: Optional[verutils.Version] = None,
) -> Tuple[s_schema.Schema, sd.DeltaRoot]:
    delta = sd.DeltaRoot()
    context = sd.CommandContext(
        modaliases=modaliases,
        schema=schema,
        stdmode=stdmode,
        internal_schema_mode=internal_schema_mode,
        testmode=testmode,
        allow_dml_in_functions=allow_dml_in_functions,
        schema_object_ids=schema_object_ids,
        compat_ver=compat_ver,
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

        if not stdmode:
            if not isinstance(
                cmd,
                (sd.GlobalObjectCommand, sd.ExternalObjectCommand),
            ):
                ver = schema.get_global(
                    s_ver.SchemaVersion, '__schema_version__')
                ver_cmd = ver.init_delta_command(schema, sd.AlterObject)
                ver_cmd.set_attribute_value('version', uuidgen.uuid1mc())
                schema = ver_cmd.apply(schema, context)
                delta.add(ver_cmd)
            elif not isinstance(cmd, sd.ExternalObjectCommand):
                gver = schema.get_global(
                    s_ver.GlobalSchemaVersion, '__global_schema_version__')
                g_ver_cmd = gver.init_delta_command(schema, sd.AlterObject)
                g_ver_cmd.set_attribute_value('version', uuidgen.uuid1mc())
                schema = g_ver_cmd.apply(schema, context)
                delta.add(g_ver_cmd)

        delta.add(cmd)

    delta.canonical = True
    return schema, delta


def ddlast_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    testmode: bool = False,
    descriptive_mode: bool = False,
) -> Dict[qlast.DDLOperation, sd.Command]:
    context = sd.CommandContext(
        descriptive_mode=descriptive_mode,
        declarative=sdlmode,
        testmode=testmode,
    )

    if schema_a is None:
        schema = schema_b
        update_schema = False
    else:
        schema = schema_a
        update_schema = True

    stmts = {}
    for command in delta.get_subcommands():
        with context(sd.DeltaRootContext(schema=schema, op=delta)):
            # The reason we do this instead of just directly using the new
            # schema is to populate the renames field of the context.
            # We do this one part at a time to avoid referring to things
            # that have not been renamed yet.
            # XXX: Is this fine-grained enough, though?
            if update_schema:
                schema = command.apply(schema, context)

            ql_ast = command.get_ast(schema, context)
            if ql_ast:
                stmts[ql_ast] = command

    return stmts


def statements_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    descriptive_mode: bool = False,
    # Used for backwards compatibility with older migration text.
    uppercase: bool = False,
    limit_ref_classes: Iterable[so.ObjectMeta] = tuple(),
) -> Tuple[Tuple[str, qlast.DDLOperation, sd.Command], ...]:

    stmts = ddlast_from_delta(
        schema_a,
        schema_b,
        delta,
        sdlmode=sdlmode,
        descriptive_mode=descriptive_mode,
    )

    ql_classes_src = {
        scls.get_ql_class() for scls in limit_ref_classes
    }

    ql_classes = {q for q in ql_classes_src if q is not None}

    # If we're generating SDL and it includes modules, try to nest the
    # module contents in the actual modules.
    processed: List[Tuple[qlast.DDLOperation, sd.Command]] = []
    unqualified: List[Tuple[qlast.DDLOperation, sd.Command]] = []
    modules = dict()
    for stmt_ast, cmd in stmts.items():
        if sdlmode:
            if isinstance(stmt_ast, qlast.CreateModule):
                # Record the module stubs.
                modules[stmt_ast.name.name] = stmt_ast
                stmt_ast.commands = []
                processed.append((stmt_ast, cmd))

            elif (
                modules
                and not isinstance(stmt_ast, qlast.UnqualifiedObjectCommand)
            ):
                # This SDL included creation of modules, so we will try to
                # nest the declarations in them.
                assert isinstance(stmt_ast, qlast.CreateObject)
                assert stmt_ast.name.module is not None
                module = modules[stmt_ast.name.module]
                module.commands.append(stmt_ast)
                # Strip the module from the object name, since we nest
                # them in a module already.
                stmt_ast.name.module = None

            elif isinstance(stmt_ast, qlast.UnqualifiedObjectCommand):
                unqualified.append((stmt_ast, cmd))

            else:
                processed.append((stmt_ast, cmd))

        else:
            processed.append((stmt_ast, cmd))

    text = []
    for stmt_ast, cmd in itertools.chain(unqualified, processed):
        stmt_text = edgeql.generate_source(
            stmt_ast,
            sdlmode=sdlmode,
            descmode=descriptive_mode,
            limit_ref_classes=ql_classes,
            uppercase=uppercase,
        )
        text.append((stmt_text + ';', stmt_ast, cmd))

    return tuple(text)


def text_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    sdlmode: bool = False,
    descriptive_mode: bool = False,
    limit_ref_classes: Iterable[so.ObjectMeta] = tuple(),
) -> str:
    stmts = statements_from_delta(
        schema_a,
        schema_b,
        delta,
        sdlmode=sdlmode,
        descriptive_mode=descriptive_mode,
        limit_ref_classes=limit_ref_classes,
    )
    return '\n'.join(text for text, _, _ in stmts)


def ddl_text_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
) -> str:
    """Return DDL text corresponding to a delta plan.

    Args:
        schema_a:
            The original schema (or None if starting from empty/std)
        schema_b:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.

    Returns:
        DDL text corresponding to *delta*.
    """
    return text_from_delta(schema_a, schema_b, delta, sdlmode=False)


def sdl_text_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
) -> str:
    """Return SDL text corresponding to a delta plan.

    Args:
        schema_a:
            The original schema (or None if starting from empty/std)
        schema_b:
            The schema to which the *delta* has **already** been
            applied.
        delta:
            The delta plan.

    Returns:
        SDL text corresponding to *delta*.
    """
    return text_from_delta(schema_a, schema_b, delta, sdlmode=True)


def descriptive_text_from_delta(
    schema_a: Optional[s_schema.Schema],
    schema_b: s_schema.Schema,
    delta: sd.DeltaRoot,
    *,
    limit_ref_classes: Iterable[so.ObjectMeta]=tuple(),
) -> str:
    """Return descriptive text corresponding to a delta plan.

    Args:
        schema_a:
            The original schema (or None if starting from empty/std)
        schema_b:
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
        schema_a,
        schema_b,
        delta,
        sdlmode=True,
        descriptive_mode=True,
        limit_ref_classes=limit_ref_classes,
    )


def ddl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[sn.Name]]=None,
    excluded_modules: Optional[Iterable[sn.Name]]=None,
    included_items: Optional[Iterable[sn.Name]]=None,
    excluded_items: Optional[Iterable[sn.Name]]=None,
    included_ref_classes: Iterable[so.ObjectMeta]=tuple(),
    include_module_ddl: bool=True,
    include_std_ddl: bool=False,
    include_migrations: bool=False,
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
        include_migrations=include_migrations,
    )
    return ddl_text_from_delta(None, schema, diff)


def sdl_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[sn.Name]]=None,
    excluded_modules: Optional[Iterable[sn.Name]]=None,
    included_items: Optional[Iterable[sn.Name]]=None,
    excluded_items: Optional[Iterable[sn.Name]]=None,
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
    return sdl_text_from_delta(None, schema, diff)


def descriptive_text_from_schema(
    schema: s_schema.Schema, *,
    included_modules: Optional[Iterable[sn.Name]]=None,
    excluded_modules: Optional[Iterable[sn.Name]]=None,
    included_items: Optional[Iterable[sn.Name]]=None,
    excluded_items: Optional[Iterable[sn.Name]]=None,
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
        descriptive_mode=True,
    )
    return descriptive_text_from_delta(
        None, schema, diff, limit_ref_classes=included_ref_classes)
