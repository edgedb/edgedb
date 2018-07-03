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


import functools
import typing

from edb.lang.edgeql import errors

from edb.lang.ir import ast as irast
from edb.lang.ir import inference as irinference
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
        with ctx.newscope(fenced=True) as subctx:
            populate_anchors(anchors, ctx=subctx)

    ctx.derived_target_module = derived_target_module
    ctx.toplevel_result_view_name = result_view_name
    ctx.implicit_id_in_shapes = implicit_id_in_shapes

    return ctx


def fini_expression(
        ir: irast.Base, *,
        ctx: context.ContextLevel) -> irast.Command:
    # Run delayed work callbacks.
    for cb in ctx.completion_work:
        cb(ctx=ctx)
    ctx.completion_work.clear()

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

        cardinality = irinference.infer_cardinality(
            ir, scope_tree=ctx.path_scope, schema=ctx.schema)
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


def compile_anchor(
        name: str, anchor: typing.Union[qlast.Expr, s_obj.Object], *,
        ctx: context.ContextLevel) -> qlast.Expr:

    show_as_anchor = True

    if isinstance(anchor, s_types.Type):
        step = setgen.class_set(anchor, ctx=ctx)

    elif (isinstance(anchor, s_pointers.Pointer) and
            not anchor.is_link_property()):
        if anchor.source:
            path = setgen.extend_path(
                setgen.class_set(anchor.source, ctx=ctx), anchor,
                s_pointers.PointerDirection.Outbound,
                anchor.target, ctx=ctx)
        else:
            Object = ctx.schema.get('std::Object')

            ptrcls = anchor.get_derived(
                ctx.schema, Object, Object,
                mark_derived=True, add_to_schema=False)

            path = setgen.extend_path(
                setgen.class_set(Object, ctx=ctx), ptrcls,
                s_pointers.PointerDirection.Outbound,
                ptrcls.target, ctx=ctx)

        step = path

    elif isinstance(anchor, s_pointers.Pointer) and anchor.is_link_property():
        if anchor.source.source:
            path = setgen.extend_path(
                setgen.class_set(anchor.source.source, ctx=ctx),
                anchor.source,
                s_pointers.PointerDirection.Outbound,
                anchor.source.target, ctx=ctx)
        else:
            Object = ctx.schema.get('std::Object')
            ptrcls = anchor.source.get_derived(
                ctx.schema, Object, Object,
                mark_derived=True, add_to_schema=False)
            path = setgen.extend_path(
                setgen.class_set(Object, ctx=ctx), ptrcls,
                s_pointers.PointerDirection.Outbound,
                ptrcls.target, ctx=ctx)

        step = setgen.extend_path(
            path, anchor,
            s_pointers.PointerDirection.Outbound,
            anchor.target, ctx=ctx)

    elif isinstance(anchor, qlast.SubExpr):
        with ctx.new() as subctx:
            if anchor.anchors:
                subctx.anchors = {}
                populate_anchors(anchor.anchors, ctx=subctx)

            step = compile_anchor(name, anchor.expr, ctx=subctx)
            if name in anchor.anchors:
                show_as_anchor = False
                step.anchor = None
                step.show_as_anchor = None

    elif isinstance(anchor, qlast.Base):
        step = setgen.ensure_set(dispatch.compile(anchor, ctx=ctx), ctx=ctx)

    else:
        raise RuntimeError(f'unexpected anchor value: {anchor!r}')

    if show_as_anchor:
        step.anchor = name
        step.show_as_anchor = name

    return step


def populate_anchors(
        anchors: typing.Dict[str, s_obj.Object], *,
        ctx: context.ContextLevel) -> None:

    for name, val in anchors.items():
        ctx.anchors[name] = compile_anchor(name, val, ctx=ctx)


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
            module=ctx.derived_target_module or '_',
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


def infer_pointer_cardinality(
        *,
        ptrcls: s_pointers.Pointer,
        irexpr: irast.Expr,
        ctx: context.ContextLevel) -> None:

    scope = pathctx.get_set_scope(ir_set=irexpr, ctx=ctx)
    if scope is None:
        scope = ctx.path_scope
    inferred_cardinality = irinference.infer_cardinality(
        irexpr, scope_tree=scope, schema=ctx.schema)

    if inferred_cardinality == irast.Cardinality.MANY:
        ptrcls.cardinality = s_pointers.PointerCardinality.ManyToMany
    else:
        ptrcls.cardinality = s_pointers.PointerCardinality.ManyToOne

    _update_cardinality_in_derived(ptrcls, ctx=ctx)


def _update_cardinality_in_derived(
        ptrcls: s_pointers.Pointer, *,
        ctx: context.ContextLevel) -> None:

    children = ctx.pointer_derivation_map.get(ptrcls)
    if children:
        for child in children:
            child.cardinality = ptrcls.cardinality
            _update_cardinality_in_derived(child, ctx=ctx)


def get_pointer_cardinality_later(
        *,
        ptrcls: s_pointers.Pointer,
        irexpr: irast.Expr,
        ctx: context.ContextLevel) -> None:
    ctx.pending_cardinality.add(ptrcls)
    at_stmt_fini(
        functools.partial(
            infer_pointer_cardinality,
            ptrcls=ptrcls, irexpr=irexpr),
        ctx=ctx)


def enforce_singleton_now(
        irexpr: irast.Base, *,
        ctx: context.ContextLevel) -> None:
    scope = pathctx.get_set_scope(ir_set=irexpr, ctx=ctx)
    if scope is None:
        scope = ctx.path_scope
    cardinality = irinference.infer_cardinality(
        irexpr, scope_tree=scope, schema=ctx.schema)
    if cardinality != irast.Cardinality.ONE:
        raise errors.EdgeQLError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=irexpr.context)


def enforce_singleton(
        irexpr: irast.Base, *,
        ctx: context.ContextLevel) -> None:
    if not ctx.path_scope_is_temp:
        # We cannot reliably defer cardinality inference operations
        # because the current scope is temporary and will not be
        # accessible when the scheduled inference will run.
        at_stmt_fini(
            functools.partial(
                enforce_singleton_now,
                irexpr=irexpr
            ),
            ctx=ctx
        )


def at_stmt_fini(
        cb: typing.Callable, *,
        ctx: context.ContextLevel) -> None:
    ctx.completion_work.append(cb)
