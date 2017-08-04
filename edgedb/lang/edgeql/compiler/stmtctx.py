##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler statement-level context management."""


import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import views as s_views

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import parser as qlparser

from . import context
from . import dispatch
from . import pathctx
from . import setgen


def init_context(
        *,
        schema: s_schema.Schema,
        arg_types: typing.Optional[typing.Iterable[s_obj.Class]]=None,
        modaliases: typing.Optional[typing.Iterable[str]]=None,
        anchors: typing.Optional[typing.Dict[str, s_obj.Class]]=None,
        security_context: typing.Optional[str]=None,
        derived_target_module: typing.Optional[str]=None) -> \
        context.ContextLevel:
    stack = context.CompilerContext()
    ctx = stack.current
    ctx.schema = schema

    if modaliases:
        ctx.namespaces.update(modaliases)

    if arg_types:
        ctx.arguments.update(arg_types)

    if anchors:
        populate_anchors(anchors, ctx=ctx)

    ctx.derived_target_module = derived_target_module

    return ctx


def populate_anchors(
        anchors: typing.Dict[str, s_obj.Class], *,
        ctx: context.ContextLevel) -> None:

    for anchor, scls in anchors.items():
        if isinstance(scls, s_obj.NodeClass):
            step = setgen.class_set(scls, ctx=ctx)
            step.anchor = anchor
            step.show_as_anchor = anchor

        elif isinstance(scls, s_links.Link):
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

        elif isinstance(scls, s_lprops.LinkProperty):
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
        ctx: context.ContextLevel) -> irast.Set:
    if not isinstance(expr, qlast.Statement):
        expr = qlast.SelectQuery(result=expr)

    with ctx.new() as subctx:
        subctx.stmt = ctx.stmt.parent_stmt
        substmt = dispatch.compile(expr, ctx=subctx)

    if irutils.is_subquery_set(substmt):
        substmt = substmt.expr

    result_type = irutils.infer_type(substmt, ctx.schema)

    view_name = sn.Name(module='__view__', name=alias)
    if isinstance(result_type, s_concepts.Concept):
        c = result_type.__class__(name=view_name, bases=[result_type])
        c.acquire_ancestor_inheritance(ctx.schema)
    else:
        c = s_views.View(name=view_name)

    path_id = irast.PathId(c)

    if isinstance(substmt.result, irast.Set):
        real_path_id = substmt.result.path_id
    else:
        real_path_id = irast.PathId(result_type)

    substmt.main_stmt = ctx.stmt
    substmt.parent_stmt = ctx.stmt.parent_stmt
    substmt_set = irast.Set(
        path_id=path_id,
        real_path_id=real_path_id,
        scls=result_type,
        expr=substmt
    )

    ctx.sets[substmt_set.path_id] = substmt_set
    ctx.substmts[(alias, None)] = substmt_set
    ctx.stmt.substmts.append(substmt_set)
    return substmt_set


def declare_view_from_schema(
        viewcls: s_obj.Class, *, ctx: context.ContextLevel) -> irast.Set:

    alias = viewcls.mangle_name(viewcls.name)
    key = (alias, None)
    if key in ctx.substmts:
        return ctx.substmts[key]
    else:
        view_expr = qlparser.parse(viewcls.expr)
        return declare_view(view_expr, alias, ctx=ctx)


def declare_aliased_set(
        expr: irast.Base, alias: typing.Optional[str]=None, *,
        ctx: context.ContextLevel) -> irast.Set:
    ir_set = setgen.ensure_set(expr, ctx=ctx)

    if alias is not None:
        key = (alias, None)
    elif not isinstance(ir_set.scls, s_obj.Collection):
        rptr = getattr(expr, 'rptr', None)
        if rptr is not None:
            key = (rptr.ptrcls.shortname, rptr.source.path_id)
        else:
            key = (ir_set.path_id[0].name, None)
    else:
        key = None

    if alias is not None:
        restype = irutils.infer_type(ir_set, ctx.schema)
        if (not isinstance(restype, s_concepts.Concept) and
                len(ir_set.path_id) == 1):
            view_name = sn.Name(module='__aliased__', name=alias)
            c = s_views.View(name=view_name)
            ir_set.path_id = irast.PathId(c)

    if key is not None:
        ctx.substmts[key] = ir_set

    pathctx.register_path_scope(ir_set.path_id, ctx=ctx)

    return ir_set
