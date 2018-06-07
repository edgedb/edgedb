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


"""EdgeQL compiler statement-level context management."""


import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import name as s_name
from edb.lang.schema import objects as s_obj
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import schema as s_schema
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import setgen


def init_context(
        *,
        schema: s_schema.Schema,
        arg_types: typing.Optional[typing.Iterable[s_obj.Object]]=None,
        modaliases: typing.Optional[typing.Dict[str, str]]=None,
        anchors: typing.Optional[typing.Dict[str, s_obj.Object]]=None,
        security_context: typing.Optional[str]=None,
        derived_target_module: typing.Optional[str]=None,
        result_view_name: typing.Optional[str]=None,
        implicit_id_in_shapes: bool=True) -> \
        context.ContextLevel:
    stack = context.CompilerContext()
    ctx = stack.current
    ctx.schema = schema.get_overlay(extra=ctx.view_nodes)

    if modaliases:
        ctx.modaliases.update(modaliases)

    if arg_types:
        ctx.arguments.update(arg_types)

    if anchors:
        populate_anchors(anchors, ctx=ctx)

    ctx.derived_target_module = derived_target_module
    ctx.toplevel_result_view_name = result_view_name
    ctx.implicit_id_in_shapes = implicit_id_in_shapes

    return ctx


def fini_expression(
        ir: irast.Base, *,
        ctx: context.ContextLevel) -> irast.Command:
    for ir_set in ctx.all_sets:
        if ir_set.path_id.namespace:
            ir_set.path_id = ir_set.path_id.strip_weak_namespaces()

    if isinstance(ir, irast.Command):
        # IR is already a Command
        return ir

    if ctx.path_scope is not None:
        # Simple expressions have no scope.
        for node in ctx.path_scope.get_all_path_nodes(include_subpaths=True):
            if node.path_id.namespace:
                node.path_id = node.path_id.strip_weak_namespaces()

        cardinality = pathctx.infer_cardinality(ir, ctx=ctx)
    else:
        cardinality = irast.Cardinality.ONE

    result = irast.Statement(
        expr=ir,
        params=ctx.arguments,
        views=ctx.view_nodes,
        source_map=ctx.source_map,
        scope_tree=ctx.path_scope,
        cardinality=cardinality,
        view_shapes=ctx.class_shapes,
    )
    irutils.infer_type(result, schema=ctx.schema)
    return result


def populate_anchors(
        anchors: typing.Dict[str, s_obj.Object], *,
        ctx: context.ContextLevel) -> None:

    for anchor, scls in anchors.items():
        if isinstance(scls, s_types.Type):
            step = setgen.class_set(scls, ctx=ctx)
            step.anchor = anchor
            step.show_as_anchor = anchor

        elif (isinstance(scls, s_pointers.Pointer) and
                not scls.is_link_property()):
            if scls.source:
                path = setgen.extend_path(
                    setgen.class_set(scls.source, ctx=ctx), scls,
                    s_pointers.PointerDirection.Outbound,
                    scls.target, ctx=ctx)
            else:
                Object = ctx.schema.get('std::Object')

                ptrcls = scls.get_derived(
                    ctx.schema, Object, Object,
                    mark_derived=True, add_to_schema=False)

                path = setgen.extend_path(
                    setgen.class_set(Object, ctx=ctx), ptrcls,
                    s_pointers.PointerDirection.Outbound,
                    ptrcls.target, ctx=ctx)

            step = path
            step.anchor = anchor
            step.show_as_anchor = anchor

        elif isinstance(scls, s_pointers.Pointer) and scls.is_link_property():
            if scls.source.source:
                path = setgen.extend_path(
                    setgen.class_set(scls.source.source, ctx=ctx),
                    scls.source,
                    s_pointers.PointerDirection.Outbound,
                    scls.source.target, ctx=ctx)
            else:
                Object = ctx.schema.get('std::Object')
                ptrcls = scls.source.get_derived(
                    ctx.schema, Object, Object,
                    mark_derived=True, add_to_schema=False)
                path = setgen.extend_path(
                    setgen.class_set(Object, ctx=ctx), ptrcls,
                    s_pointers.PointerDirection.Outbound,
                    ptrcls.target, ctx=ctx)

            step = setgen.extend_path(
                path, scls,
                s_pointers.PointerDirection.Outbound,
                scls.target, ctx=ctx)

            step.anchor = anchor
            step.show_as_anchor = anchor

        else:
            step = scls

        ctx.anchors[anchor] = step


def declare_view(
        expr: qlast.Base, alias: str, *,
        fully_detached: bool=False,
        ctx: context.ContextLevel) -> irast.Set:
    expr = astutils.ensure_qlstmt(expr)

    with ctx.newscope(temporary=True, fenced=True) as subctx:
        if not fully_detached:
            # Detach the view namespace and record the prefix
            # in the parent statement's fence node.
            subctx.path_id_namespace = (
                subctx.path_id_namespace +
                (irast.WeakNamespace(ctx.aliases.get('ns')),))
            ctx.path_scope.namespaces.add(subctx.path_id_namespace[-1])

        if isinstance(alias, s_name.SchemaName):
            basename = alias
        else:
            basename = s_name.SchemaName(module='__view__', name=alias)

        if ctx.stmt is not None:
            subctx.stmt = ctx.stmt.parent_stmt

        view_name = s_name.SchemaName(
            module='_',
            name=s_obj.NamedObject.get_specialized_name(
                basename,
                ctx.aliases.get('w')
            )
        )
        subctx.toplevel_result_view_name = view_name

        view_set = dispatch.compile(expr, ctx=subctx)
        # The view path id _itself_ should not be in the nested
        # namespace.
        view_set.path_id = view_set.path_id.replace_namespace(
            ctx.path_id_namespace)
        ctx.aliased_views[alias] = view_set.scls
        ctx.path_scope_map[view_set] = subctx.path_scope

    return view_set


def declare_view_from_schema(
        viewcls: s_obj.Object, *,
        ctx: context.ContextLevel) -> irast.Set:
    vc = ctx.view_class_map.get(viewcls)
    if vc is not None:
        return vc

    with ctx.detached() as subctx:
        view_expr = qlparser.parse(viewcls.expr)
        declare_view(view_expr, alias=viewcls.name,
                     fully_detached=True, ctx=subctx)

        vc = subctx.aliased_views[viewcls.name]
        ctx.view_class_map[viewcls] = vc
        ctx.source_map.update(subctx.source_map)
        ctx.aliased_views[viewcls.name] = subctx.aliased_views[viewcls.name]
        ctx.view_nodes[vc.name] = vc
        ctx.view_sets[vc] = subctx.view_sets[vc]

    return vc
