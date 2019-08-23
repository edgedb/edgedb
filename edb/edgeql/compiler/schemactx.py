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


from __future__ import annotations

import typing

from edb import errors

from edb.common import parsing

from edb.schema import abc as s_abc
from edb.schema import derivable as s_der
from edb.schema import inheriting as s_inh
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast

from . import context


def get_schema_object(
        name: typing.Union[str, qlast.BaseObjectRef],
        module: typing.Optional[str]=None, *,
        item_types: typing.Optional[typing.Sequence[s_obj.ObjectMeta]],
        ctx: context.ContextLevel,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_obj.Object:

    if isinstance(name, qlast.ObjectRef):
        if srcctx is None:
            srcctx = name.context
        module = name.module
        name = name.name
    elif isinstance(name, qlast.AnyType):
        return s_pseudo.Any.instance
    elif isinstance(name, qlast.AnyTuple):
        return s_pseudo.AnyTuple.instance

    if module:
        name = sn.Name(name=name, module=module)

    elif isinstance(name, str):
        result = ctx.aliased_views.get(name)
        if result is not None:
            return result

    try:
        stype = ctx.env.get_track_schema_object(
            name=name, modaliases=ctx.modaliases, type=item_types)

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
        name: typing.Union[str, qlast.BaseObjectRef],
        module: typing.Optional[str]=None, *,
        ctx: context.ContextLevel,
        item_types: typing.Optional[typing.Sequence[s_obj.ObjectMeta]]=None,
        srcctx: typing.Optional[parsing.ParserContext] = None) -> s_types.Type:
    if item_types is None:
        item_types = (s_types.Type,)
    obj = get_schema_object(name, module, item_types=item_types,
                            ctx=ctx, srcctx=srcctx)
    assert isinstance(obj, s_types.Type)
    return obj


def resolve_schema_name(
        name: str, module: str, *,
        ctx: context.ContextLevel) -> typing.Optional[sn.Name]:
    schema_module = ctx.modaliases.get(module)
    if schema_module is None:
        return None
    else:
        return sn.Name(name=name, module=schema_module)


def derive_view(
        stype: s_types.Type, *,
        derived_name: typing.Optional[sn.SchemaName]=None,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        derived_name_base: typing.Optional[str]=None,
        preserve_shape: bool=False,
        preserve_path_id: bool=False,
        is_insert: bool=False,
        is_update: bool=False,
        inheritance_merge: bool=True,
        attrs: typing.Optional[dict]=None,
        ctx: context.ContextLevel) -> s_types.Type:

    if derived_name is None:
        derived_name = derive_view_name(
            stype=stype, derived_name_quals=derived_name_quals,
            derived_name_base=derived_name_base, ctx=ctx)

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

    derived: s_types.Type

    if isinstance(stype, s_abc.Collection):
        ctx.env.schema, derived = stype.derive_subtype(
            ctx.env.schema, name=derived_name)

    elif isinstance(stype, s_inh.InheritingObject):
        qualifiers: typing.Tuple[str, ...] = ()
        if stype.get_name(ctx.env.schema) == derived_name:
            qualifiers = (ctx.aliases.get('d'),)

        ctx.env.schema, derived = stype.derive(
            ctx.env.schema,
            stype,
            *qualifiers,
            name=derived_name,
            inheritance_merge=inheritance_merge,
            refdict_whitelist={'pointers'},
            mark_derived=True,
            preserve_path_id=preserve_path_id,
            attrs=attrs)

        if (not stype.generic(ctx.env.schema)
                and isinstance(derived, s_sources.Source)):
            scls_pointers = stype.get_pointers(ctx.env.schema)
            derived_own_pointers = derived.get_pointers(ctx.env.schema)

            for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls_pointers.get(ctx.env.schema, pn)
                computable_data = ctx.source_map.get(src_ptr)
                if computable_data is not None:
                    ctx.source_map[ptr] = computable_data

    ctx.view_nodes[derived.get_name(ctx.env.schema)] = derived

    if preserve_shape and stype in ctx.env.view_shapes:
        ctx.env.view_shapes[derived] = ctx.env.view_shapes[stype]

    return derived


def derive_ptr(
        ptr: s_pointers.Pointer,
        source: s_sources.Source,
        target: typing.Optional[s_types.Type]=None,
        *qualifiers,
        derived_name: typing.Optional[sn.SchemaName]=None,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        derived_name_base: typing.Optional[str]=None,
        preserve_shape: bool=False,
        preserve_path_id: bool=False,
        is_insert: bool=False,
        is_update: bool=False,
        inheritance_merge: bool=True,
        attrs: typing.Optional[dict]=None,
        ctx: context.ContextLevel) -> s_pointers.Pointer:

    if derived_name is None and ctx.derived_target_module:
        derived_name = derive_view_name(
            stype=ptr, derived_name_quals=derived_name_quals,
            derived_name_base=derived_name_base, ctx=ctx)

    if ptr.get_name(ctx.env.schema) == derived_name:
        qualifiers = qualifiers + (ctx.aliases.get('d'),)

    ctx.env.schema, derived = ptr.derive(
        ctx.env.schema,
        source,
        target,
        *qualifiers,
        name=derived_name,
        inheritance_merge=inheritance_merge,
        refdict_whitelist={'pointers'},
        mark_derived=True,
        preserve_path_id=preserve_path_id,
        attrs=attrs)

    if not ptr.generic(ctx.env.schema):
        if isinstance(derived, s_sources.Source):
            scls_pointers = ptr.get_pointers(ctx.env.schema)
            derived_own_pointers = derived.get_pointers(ctx.env.schema)

            for pn, ptr in derived_own_pointers.items(ctx.env.schema):
                # This is a view of a view.  Make sure query-level
                # computable expressions for pointers are carried over.
                src_ptr = scls_pointers.get(ctx.env.schema, pn)
                computable_data = ctx.source_map.get(src_ptr)
                if computable_data is not None:
                    ctx.source_map[ptr] = computable_data

    if preserve_shape and ptr in ctx.env.view_shapes:
        ctx.env.view_shapes[derived] = ctx.env.view_shapes[ptr]

    return derived


def derive_view_name(
        stype: s_obj.Object,
        derived_name_quals: typing.Optional[typing.Sequence[str]]=(),
        derived_name_base: typing.Optional[str]=None, *,
        ctx: context.ContextLevel) -> sn.Name:

    if not derived_name_quals:
        derived_name_quals = (ctx.aliases.get('view'),)

    if ctx.derived_target_module:
        derived_name_module = ctx.derived_target_module
    else:
        derived_name_module = '__derived__'

    return s_der.derive_name(
        ctx.env.schema, *derived_name_quals,
        module=derived_name_module,
        derived_name_base=derived_name_base,
        parent=stype,
    )
