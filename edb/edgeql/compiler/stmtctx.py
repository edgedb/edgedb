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


from __future__ import annotations

from typing import *

from edb import errors

from edb.ir import ast as irast

from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import inference
from . import options as coptions
from . import pathctx
from . import setgen
from . import schemactx


def init_context(
    *,
    schema: s_schema.Schema,
    options: coptions.CompilerOptions,
) -> context.ContextLevel:

    if not schema.get_global(s_mod.Module, '__derived__', None):
        schema, _ = s_mod.Module.create_in_schema(schema, name='__derived__')
    env = context.Environment(
        schema=schema,
        path_scope=irast.new_scope_tree(),
        options=options,
    )
    ctx = context.ContextLevel(None, context.ContextSwitchMode.NEW, env=env)
    _ = context.CompilerContext(initial=ctx)

    if options.singletons:
        # The caller wants us to treat these type references
        # as singletons for the purposes of the overall expression
        # cardinality inference, so we set up the scope tree in
        # the necessary fashion.
        for singleton in options.singletons:
            path_id = pathctx.get_path_id(singleton, ctx=ctx)
            ctx.env.path_scope.attach_path(path_id, context=None)

        ctx.path_scope = ctx.env.path_scope.attach_fence()

    ctx.modaliases.update(options.modaliases)

    if options.anchors:
        with ctx.newscope(fenced=True) as subctx:
            populate_anchors(options.anchors, ctx=subctx)

    if options.path_prefix_anchor is not None:
        path_prefix = options.anchors[options.path_prefix_anchor]
        assert isinstance(path_prefix, s_types.Type)
        ctx.partial_path_prefix = setgen.class_set(path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = options.path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = options.path_prefix_anchor

    ctx.derived_target_module = options.derived_target_module
    ctx.toplevel_result_view_name = options.result_view_name
    ctx.implicit_id_in_shapes = options.implicit_id_in_shapes
    ctx.implicit_tid_in_shapes = options.implicit_tid_in_shapes
    ctx.implicit_limit = options.implicit_limit

    return ctx


def fini_expression(
    ir: irast.Base,
    *,
    ctx: context.ContextLevel,
) -> irast.Command:

    if (
        isinstance(ir, irast.Set)
        and pathctx.get_set_scope(ir, ctx=ctx) is None
    ):
        ir = setgen.scoped_set(ir, ctx=ctx)

    cardinality = qltypes.Cardinality.AT_MOST_ONE
    if ctx.path_scope is not None:
        # Simple expressions have no scope.
        cardinality = inference.infer_cardinality(
            ir,
            scope_tree=ctx.path_scope,
            ctx=inference.make_ctx(env=ctx.env),
        )

    # Strip weak namespaces
    for ir_set in ctx.env.set_types:
        if ir_set.path_id.namespace:
            ir_set.path_id = ir_set.path_id.strip_weak_namespaces()

    if ctx.path_scope is not None:
        for node in ctx.path_scope.path_descendants:
            if node.path_id.namespace:
                node.path_id = node.path_id.strip_weak_namespaces()

    if isinstance(ir, irast.Command):
        if isinstance(ir, irast.ConfigCommand):
            ir.scope_tree = ctx.path_scope
        # IR is already a Command
        return ir

    volatility = inference.infer_volatility(ir, env=ctx.env)

    if ctx.env.options.schema_view_mode:
        for view in ctx.view_nodes.values():
            if view.is_collection():
                continue

            assert isinstance(view, s_types.InheritingType)
            _elide_derived_ancestors(view, ctx=ctx)

            if not isinstance(view, s_sources.Source):
                continue

            view_own_pointers = view.get_pointers(ctx.env.schema)
            for vptr in view_own_pointers.objects(ctx.env.schema):
                _elide_derived_ancestors(vptr, ctx=ctx)
                ctx.env.schema = vptr.set_field_value(
                    ctx.env.schema,
                    'is_from_alias',
                    True,
                )

                tgt = vptr.get_target(ctx.env.schema)
                assert tgt is not None

                if (tgt.is_union_type(ctx.env.schema)
                        and tgt.get_is_opaque_union(ctx.env.schema)):
                    # Opaque unions should manifest as std::BaseObject
                    # in schema views.
                    ctx.env.schema = vptr.set_target(
                        ctx.env.schema,
                        ctx.env.schema.get(
                            'std::BaseObject', type=s_types.Type),
                    )

                if not hasattr(vptr, 'get_pointers'):
                    continue
                # type ignore below, because we check above if vptr has
                # a get_pointers attribute
                vptr_own_pointers = vptr.get_pointers(  # type: ignore
                    ctx.env.schema
                )
                for vlprop in vptr_own_pointers.objects(ctx.env.schema):
                    _elide_derived_ancestors(vlprop, ctx=ctx)
                    ctx.env.schema = vlprop.set_field_value(
                        ctx.env.schema,
                        'is_from_alias',
                        True,
                    )

    expr_type = inference.infer_type(ir, ctx.env)

    in_polymorphic_func = (
        ctx.env.options.func_params is not None and
        ctx.env.options.func_params.has_polymorphic(ctx.env.schema)
    )

    if (
        not in_polymorphic_func
        and not ctx.env.options.allow_generic_type_output
    ):
        anytype = expr_type.find_any(ctx.env.schema)
        if anytype is not None:
            raise errors.QueryError(
                'expression returns value of indeterminate type',
                hint='Consider using an explicit type cast.',
                context=ctx.env.type_origins.get(anytype))

    if ctx.must_use_views:
        alias, srcctx = next(iter(ctx.must_use_views.values()))
        raise errors.QueryError(
            f'unused alias definition: {alias!r}',
            context=srcctx,
        )

    result = irast.Statement(
        expr=ir,
        params=list(ctx.env.query_parameters.values()),
        views=ctx.view_nodes,
        source_map=ctx.source_map,
        scope_tree=ctx.path_scope,
        cardinality=cardinality,
        volatility=volatility,
        stype=expr_type,
        view_shapes={
            src: [ptr for ptr, _ in ptrs]
            for src, ptrs in ctx.env.view_shapes.items()
        },
        view_shapes_metadata=ctx.env.view_shapes_metadata,
        schema=ctx.env.schema,
        schema_refs=frozenset(
            ctx.env.schema_refs - ctx.env.created_schema_objects),
        schema_ref_exprs=ctx.env.schema_ref_exprs,
        new_coll_types=frozenset(
            t for t in ctx.env.created_schema_objects
            if isinstance(t, s_types.Collection) and t != expr_type
        ),
        type_rewrites={s.typeref.id: s for s in ctx.type_rewrites.values()},
    )
    return result


def _elide_derived_ancestors(
    obj: Union[s_types.InheritingType, s_pointers.Pointer], *,
    ctx: context.ContextLevel
) -> None:
    """Collapse references to derived objects in bases.

    When compiling a schema view expression, make sure we don't
    expose any ephemeral derived objects, as these wouldn't be
    present in the schema outside of the compilation context.
    """

    pbase = obj.get_bases(ctx.env.schema).first(ctx.env.schema)
    if pbase.get_is_derived(ctx.env.schema):
        pbase = pbase.get_nearest_non_derived_parent(ctx.env.schema)
        ctx.env.schema = obj.set_field_value(
            ctx.env.schema,
            'bases',
            s_obj.ObjectList.create(ctx.env.schema, [pbase]),
        )

        ctx.env.schema = obj.set_field_value(
            ctx.env.schema,
            'ancestors',
            s_obj.compute_ancestors(ctx.env.schema, obj)
        )


def compile_anchor(
    name: str,
    anchor: Union[qlast.Expr, irast.Base, s_obj.Object],
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    show_as_anchor = True

    if isinstance(anchor, s_types.Type):
        step = setgen.class_set(anchor, ctx=ctx)

    elif (isinstance(anchor, s_pointers.Pointer) and
            not anchor.is_link_property(ctx.env.schema)):
        src = anchor.get_source(ctx.env.schema)
        if src is not None:
            assert isinstance(src, s_objtypes.ObjectType)
            path = setgen.extend_path(
                setgen.class_set(src, ctx=ctx),
                anchor,
                s_pointers.PointerDirection.Outbound,
                ctx=ctx,
            )
        else:
            ptrcls = schemactx.derive_dummy_ptr(anchor, ctx=ctx)
            src = ptrcls.get_source(ctx.env.schema)
            assert isinstance(src, s_types.Type)
            path = setgen.extend_path(
                setgen.class_set(src, ctx=ctx),
                ptrcls,
                s_pointers.PointerDirection.Outbound,
                ctx=ctx)

        step = path

    elif (isinstance(anchor, s_pointers.Pointer) and
            anchor.is_link_property(ctx.env.schema)):

        anchor_source = anchor.get_source(ctx.env.schema)
        assert isinstance(anchor_source, s_pointers.Pointer)
        anchor_source_source = anchor_source.get_source(ctx.env.schema)

        if anchor_source_source:
            assert isinstance(anchor_source_source, s_objtypes.ObjectType)
            path = setgen.extend_path(
                setgen.class_set(anchor_source_source, ctx=ctx),
                anchor_source,
                s_pointers.PointerDirection.Outbound,
                ctx=ctx,
            )
        else:
            ptrcls = schemactx.derive_dummy_ptr(anchor_source, ctx=ctx)
            src = ptrcls.get_source(ctx.env.schema)
            assert isinstance(src, s_types.Type)
            path = setgen.extend_path(
                setgen.class_set(src, ctx=ctx),
                ptrcls,
                s_pointers.PointerDirection.Outbound,
                ctx=ctx)

        step = setgen.extend_path(
            path,
            anchor,
            s_pointers.PointerDirection.Outbound,
            ctx=ctx)

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

    elif isinstance(anchor, irast.Parameter):
        step = setgen.ensure_set(anchor, ctx=ctx)

    else:
        raise RuntimeError(f'unexpected anchor value: {anchor!r}')

    if show_as_anchor:
        step.anchor = name
        step.show_as_anchor = name

    return step


def populate_anchors(
    anchors: Mapping[str, Any],
    *,
    ctx: context.ContextLevel,
) -> None:

    for name, val in anchors.items():
        ctx.anchors[name] = compile_anchor(name, val, ctx=ctx)


def declare_view(
    expr: qlast.Expr,
    alias: str,
    *,
    fully_detached: bool=False,
    must_be_used: bool=False,
    path_id_namespace: Optional[FrozenSet[str]]=None,
    ctx: context.ContextLevel,
) -> irast.Set:

    pinned_pid_ns = path_id_namespace

    with ctx.newscope(temporary=True, fenced=True) as subctx:
        if path_id_namespace is not None:
            subctx.path_id_namespace = path_id_namespace

        if not fully_detached:
            cached_view_set = ctx.expr_view_cache.get((expr, alias))
            # Detach the view namespace and record the prefix
            # in the parent statement's fence node.
            view_path_id_ns = irast.WeakNamespace(ctx.aliases.get('ns'))
            subctx.path_id_namespace |= {view_path_id_ns}
            ctx.path_scope.add_namespaces({view_path_id_ns})
        else:
            cached_view_set = None

        if ctx.stmt is not None:
            subctx.stmt = ctx.stmt.parent_stmt

        if cached_view_set is not None:
            subctx.view_scls = setgen.get_set_type(cached_view_set, ctx=ctx)
            view_name = s_name.SchemaName(
                subctx.view_scls.get_name(ctx.env.schema))
        else:
            if isinstance(alias, s_name.SchemaName):
                basename = alias
            else:
                basename = s_name.SchemaName(module='__derived__', name=alias)

            view_name = s_name.SchemaName(
                module=ctx.derived_target_module or '__derived__',
                name=s_name.get_specialized_name(
                    basename,
                    ctx.aliases.get('w')
                )
            )

        subctx.toplevel_result_view_name = view_name

        view_set = dispatch.compile(astutils.ensure_qlstmt(expr), ctx=subctx)
        assert isinstance(view_set, irast.Set)

        ctx.path_scope_map[view_set] = context.ScopeInfo(
            path_scope=subctx.path_scope,
            pinned_path_id_ns=pinned_pid_ns,
        )

        if not fully_detached:
            # The view path id _itself_ should not be in the nested namespace.
            # The fully_detached case should be handled by the caller.
            if path_id_namespace is None:
                path_id_namespace = ctx.path_id_namespace
            view_set.path_id = view_set.path_id.replace_namespace(
                path_id_namespace)

        view_type = setgen.get_set_type(view_set, ctx=ctx)
        ctx.aliased_views[alias] = view_type
        ctx.expr_view_cache[expr, alias] = view_set

        if must_be_used:
            ctx.must_use_views[view_type] = (alias, expr.context)

    return view_set


def declare_view_from_schema(
        viewcls: s_types.Type, *,
        ctx: context.ContextLevel) -> s_types.Type:
    vc = ctx.env.schema_view_cache.get(viewcls)
    if vc is not None:
        return vc

    with ctx.detached() as subctx:
        subctx.expr_exposed = False
        view_expr = viewcls.get_expr(ctx.env.schema)
        assert view_expr is not None
        view_ql = qlparser.parse(view_expr.text)
        viewcls_name = viewcls.get_name(ctx.env.schema)
        view_set = declare_view(view_ql, alias=viewcls_name,
                                fully_detached=True, ctx=subctx)
        # The view path id _itself_ should not be in the nested namespace.
        view_set.path_id = view_set.path_id.replace_namespace(
            ctx.path_id_namespace)

        vc = subctx.aliased_views[viewcls_name]
        assert vc is not None
        ctx.env.schema_view_cache[viewcls] = vc
        ctx.source_map.update(subctx.source_map)
        ctx.aliased_views[viewcls_name] = subctx.aliased_views[viewcls_name]
        ctx.view_nodes[vc.get_name(ctx.env.schema)] = vc
        ctx.view_sets[vc] = subctx.view_sets[vc]

    return vc
