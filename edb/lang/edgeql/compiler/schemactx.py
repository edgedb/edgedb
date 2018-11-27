#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


"""EdgeQL compiler schema helpers."""


import typing

from edb import errors

from edb.lang.common import parsing

from edb.lang.schema import abc as s_abc
from edb.lang.schema import name as sn
from edb.lang.schema import nodes as s_nodes
from edb.lang.schema import objects as s_obj
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import pseudo as s_pseudo
from edb.lang.schema import sources as s_sources
from edb.lang.schema import types as s_types
from edb.lang.schema import utils as s_utils

from edb.lang.edgeql import ast as qlast

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
    elif isinstance(name, qlast.AnyType):
        return s_pseudo.Any.create()

    if module:
        name = sn.Name(name=name, module=module)

    if not module:
        result = ctx.aliased_views.get(name)
        if result is not None:
            return result

    try:
        stype = ctx.env.schema.get(
            name=name, module_aliases=ctx.modaliases,
            type=item_types)

    except errors.QueryError as e:
        s_utils.enrich_schema_lookup_error(
            e, name, modaliases=ctx.modaliases, schema=ctx.env.schema,
            item_types=item_types)
        raise

    result = ctx.aliased_views.get(stype.get_name(ctx.env.schema))
    if result is None:
        result = stype

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
        stype: s_obj.Object, source: typing.Optional[s_nodes.Node]=None,
        target: typing.Optional[s_nodes.Node]=None,
        *qualifiers,
        derived_name: typing.Optional[sn.SchemaName]=None,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        derived_name_base: typing.Optional[str]=None,
        is_insert: bool=False,
        is_update: bool=False,
        attrs: typing.Optional[dict]=None,
        ctx: context.ContextLevel) -> s_obj.Object:
    if source is None:
        source = stype

    if derived_name is None and (ctx.derived_target_module or source is stype):
        derived_name = derive_view_name(
            stype=stype, derived_name_quals=derived_name_quals,
            derived_name_base=derived_name_base, ctx=ctx)

    if isinstance(stype, s_abc.Type):
        if is_insert:
            vtype = s_types.ViewType.Insert
        elif is_update:
            vtype = s_types.ViewType.Update
        else:
            vtype = s_types.ViewType.Select

        if attrs is None:
            attrs = {}
        else:
            attrs = dict(attrs)

        attrs['view_type'] = vtype

    if isinstance(stype, s_abc.Collection):
        ctx.env.schema, derived = stype.derive_subtype(
            ctx.env.schema, name=derived_name)
    else:
        if stype.get_name(ctx.env.schema) == derived_name:
            qualifiers = list(qualifiers)
            qualifiers.append(ctx.aliases.get('d'))

        ctx.env.schema, derived = stype.derive(
            ctx.env.schema, source, target, *qualifiers,
            name=derived_name, mark_derived=True, attrs=attrs)

        if not stype.generic(ctx.env.schema):
            if isinstance(derived, s_sources.Source):
                scls_pointers = stype.get_pointers(ctx.env.schema)
                derived_own_pointers = derived.get_own_pointers(ctx.env.schema)

                for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                    # This is a view of a view.  Make sure query-level
                    # computable expressions for pointers are carried over.
                    src_ptr = scls_pointers.get(ctx.env.schema, pn)
                    computable_data = ctx.source_map.get(src_ptr)
                    if computable_data is not None:
                        ctx.source_map[ptr] = computable_data

    if isinstance(derived, s_abc.Type):
        ctx.view_nodes[derived.get_name(ctx.env.schema)] = derived

    return derived


def derive_view_name(
        stype: s_obj.Object,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        derived_name_base: typing.Optional[str]=None, *,
        ctx: context.ContextLevel) -> sn.Name:

    if not derived_name_quals:
        derived_name_quals = (ctx.aliases.get('view'),)

    if not derived_name_base:
        derived_name_base = stype.get_shortname(ctx.env.schema)

    if ctx.derived_target_module:
        derived_name_module = ctx.derived_target_module
    else:
        derived_name_module = '__derived__'

    derived_sname = sn.get_specialized_name(
        derived_name_base, *derived_name_quals)

    return sn.SchemaName(module=derived_name_module, name=derived_sname)
