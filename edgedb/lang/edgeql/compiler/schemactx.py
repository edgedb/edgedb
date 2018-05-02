##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler schema helpers."""


import typing

from edgedb.lang.common import parsing

from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import nodes as s_nodes
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors as qlerrors

from . import context


def get_schema_object(
        name: typing.Union[str, qlast.ObjectRef],
        module: typing.Optional[str]=None, *,
        item_types: typing.Optional[typing.List[s_obj.ObjectMeta]],
        ctx: context.ContextLevel,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_obj.Object:

    if isinstance(name, qlast.ObjectRef):
        if srcctx is None:
            srcctx = name.context
        module = name.module
        name = name.name

    if module:
        name = sn.Name(name=name, module=module)

    if not module:
        result = ctx.aliased_views.get(name)
        if result is not None:
            return result

    try:
        scls = ctx.schema.get(name=name, module_aliases=ctx.modaliases,
                              type=item_types)

    except s_err.ItemNotFoundError as e:
        qlerror = qlerrors.EdgeQLError(e.args[0], context=srcctx)
        s_utils.enrich_schema_lookup_error(
            qlerror, name, modaliases=ctx.modaliases, schema=ctx.schema,
            item_types=item_types)

        raise qlerror

    except s_err.SchemaError as e:
        raise qlerrors.EdgeQLError(e.args[0], context=srcctx)

    result = ctx.aliased_views.get(scls.name)
    if result is None:
        result = scls

    return result


def get_schema_type(
        name: typing.Union[str, qlast.ObjectRef],
        module: typing.Optional[str]=None, *,
        ctx: context.ContextLevel,
        item_types: typing.Optional[typing.List[s_obj.ObjectMeta]]=None,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_types.Type:
    if item_types is None:
        item_types = (s_types.Type,)
    return get_schema_object(name, module, item_types=item_types,
                             ctx=ctx, srcctx=srcctx)


def get_schema_ptr(
        name: typing.Union[str, qlast.ObjectRef],
        module: typing.Optional[str]=None, *,
        ctx: context.ContextLevel,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_types.Type:
    return get_schema_object(
        name, module, item_types=(s_pointers.Pointer,), ctx=ctx, srcctx=srcctx
    )


def resolve_schema_name(
        name: str, module: str, *,
        ctx: context.ContextLevel) -> sn.Name:
    schema_module = ctx.modaliases.get(module)
    if schema_module is None:
        return None
    else:
        return sn.Name(name=name, module=schema_module)


def derive_view(
        scls: s_obj.Object, source: typing.Optional[s_nodes.Node]=None,
        target: typing.Optional[s_nodes.Node]=None,
        *qualifiers,
        derived_name: typing.Optional[sn.SchemaName]=None,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        is_insert: bool=False,
        is_update: bool=False,
        add_to_schema: bool=True,
        ctx: context.ContextLevel) -> s_obj.Object:
    if source is None:
        source = scls

    if derived_name is None:
        if not derived_name_quals:
            derived_name_quals = (ctx.aliases.get('view'),)

        if ctx.derived_target_module:
            derived_sname = scls.get_specialized_name(
                scls.shortname, *derived_name_quals)

            derived_name = sn.SchemaName(
                module=ctx.derived_target_module, name=derived_sname)
        elif source is scls:
            derived_sname = scls.get_specialized_name(
                scls.shortname, *derived_name_quals)

            derived_name = sn.SchemaName(
                module='__view__', name=derived_sname)

    if scls.generic():
        derived = scls.derive(
            ctx.schema, source, target, *qualifiers, name=derived_name,
            mark_derived=True)
    else:
        # If this is already a derived class, reuse its name,
        # so that the correct storage relations are used in DML.
        if derived_name is None:
            derived_name = scls.name

        derived = scls.derive_copy(
            ctx.schema, source, target, *qualifiers, name=derived_name,
            attrs=dict(bases=[scls]), mark_derived=True)

        if isinstance(derived, s_sources.Source):
            for pn, ptr in derived.own_pointers.items():
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls.pointers[pn]
                computable_data = ctx.source_map.get(src_ptr)
                if computable_data is not None:
                    ctx.source_map[ptr] = computable_data

    if isinstance(derived, s_types.Type):
        if is_insert:
            vtype = s_types.ViewType.Insert
        elif is_update:
            vtype = s_types.ViewType.Update
        else:
            vtype = s_types.ViewType.Select
        derived.view_type = vtype

    if (add_to_schema and not isinstance(derived, s_types.Collection) and
            ctx.schema.get(derived.name, None) is None):
        ctx.schema.add(derived)

    if isinstance(derived, s_types.Type):
        ctx.view_nodes[derived.name] = derived

    return derived
