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

from typing import (
    Any,
    Optional,
    Union,
    Mapping,
    Sequence,
    Dict,
    List,
    FrozenSet,
)

import copy

from edb import errors

from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.ir import typeutils as irtyputils

from edb.schema import constraints as s_constr
from edb.schema import futures as s_futures
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import rewrites as s_rewrites
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast

from edb.common.ast import visitor as ast_visitor
from edb.common import ordered
from edb.common.typeutils import not_none

from . import astutils
from . import context
from . import dispatch
from . import eta_expand
from . import group
from . import inference
from . import options as coptions
from . import pathctx
from . import setgen
from . import viewgen
from . import schemactx
from . import triggers
from . import tuple_args
from . import typegen


def init_context(
    *,
    schema: s_schema.Schema,
    options: coptions.CompilerOptions,
    inlining_context: Optional[context.ContextLevel] = None,
) -> context.ContextLevel:

    if not schema.get_global(s_mod.Module, '__derived__', None):
        schema, _ = s_mod.Module.create_in_schema(
            schema,
            name=s_name.UnqualName('__derived__'),
        )

    if inlining_context:
        env = copy.copy(inlining_context.env)
        env.options = options
        env.path_scope = inlining_context.path_scope
        env.alias_result_view_name = options.result_view_name
        env.query_parameters = {}
        env.script_params = {}

        ctx = context.ContextLevel(
            inlining_context, mode=context.ContextSwitchMode.DETACHED
        )
        ctx.env = env

    else:
        env = context.Environment(
            schema=schema,
            options=options,
            alias_result_view_name=options.result_view_name,
        )
        ctx = context.ContextLevel(None, context.ContextSwitchMode.NEW, env=env)
    _ = context.CompilerContext(initial=ctx)

    if options.singletons:
        # The caller wants us to treat these type and pointer
        # references as singletons for the purposes of the overall
        # expression cardinality inference, so we set up the scope
        # tree in the necessary fashion.
        had_optional = False
        for singleton_ent in options.singletons:
            singleton, optional = (
                singleton_ent if isinstance(singleton_ent, tuple)
                else (singleton_ent, False)
            )
            had_optional |= optional
            path_id = compile_anchor('__', singleton, ctx=ctx).path_id
            ctx.env.path_scope.attach_path(
                path_id, optional=optional, span=None, ctx=ctx
            )
            if not optional:
                ctx.env.singletons.append(path_id)
            ctx.iterator_path_ids |= {path_id}

        # If we installed any optional singletons, run the rest of the
        # compilation under a fence to protect them.
        if had_optional:
            ctx.path_scope = ctx.path_scope.attach_fence()

    for orig, remapped in options.type_remaps.items():
        rset = compile_anchor('__', remapped, ctx=ctx)
        ctx.view_sets[orig] = rset
        ctx.env.path_scope_map[rset] = context.ScopeInfo(
            path_scope=ctx.path_scope, binding_kind=None
        )

    ctx.modaliases.update(options.modaliases)

    if options.anchors:
        with ctx.newscope(fenced=True) as subctx:
            populate_anchors(options.anchors, ctx=subctx)

    if options.path_prefix_anchor is not None:
        path_prefix = options.anchors[options.path_prefix_anchor]
        ctx.partial_path_prefix = compile_anchor(
            options.path_prefix_anchor, path_prefix, ctx=ctx)
        ctx.partial_path_prefix.anchor = options.path_prefix_anchor
        ctx.partial_path_prefix.show_as_anchor = options.path_prefix_anchor

    if options.detached:
        ctx.path_id_namespace = frozenset({ctx.aliases.get('ns')})

    if options.schema_object_context is s_rewrites.Rewrite:
        assert ctx.partial_path_prefix
        typ = setgen.get_set_type(ctx.partial_path_prefix, ctx=ctx)
        assert isinstance(typ, s_objtypes.ObjectType)
        ctx.active_rewrites |= {typ, *typ.descendants(ctx.env.schema)}

    ctx.derived_target_module = options.derived_target_module
    ctx.toplevel_result_view_name = options.result_view_name
    ctx.implicit_id_in_shapes = options.implicit_id_in_shapes
    ctx.implicit_tid_in_shapes = options.implicit_tid_in_shapes
    ctx.implicit_tname_in_shapes = options.implicit_tname_in_shapes
    ctx.implicit_limit = options.implicit_limit
    ctx.expr_exposed = context.Exposure.EXPOSED

    # Resolve simple_scoping/warn_old_scoping configs.
    # options specifies the value in the configuration system;
    # if that is None, we rely on the presence of the future.
    simple_scoping = options.simple_scoping
    if simple_scoping is None:
        simple_scoping = s_futures.future_enabled(
            ctx.env.schema, 'simple_scoping'
        )
    warn_old_scoping = options.warn_old_scoping
    if warn_old_scoping is None:
        warn_old_scoping = s_futures.future_enabled(
            ctx.env.schema, 'warn_old_scoping'
        )

    ctx.no_factoring = simple_scoping
    ctx.warn_factoring = warn_old_scoping

    return ctx


def fini_expression(
    ir: irast.Set, *, ctx: context.ContextLevel
) -> irast.Statement | irast.ConfigCommand:

    ctx.path_scope = ctx.env.path_scope

    ir = eta_expand.eta_expand_ir(ir, toplevel=True, ctx=ctx)

    if (
        isinstance(ir, irast.Set)
        and pathctx.get_set_scope(ir, ctx=ctx) is None
    ):
        ir = setgen.scoped_set(ir, ctx=ctx)

    # Compile any triggers that were triggered by the query
    ir_triggers = triggers.compile_triggers(ctx=ctx)

    # Collect all of the expressions stored in various side sets
    # that can make it into the output, so that we can make sure
    # to catch them all in our fixups and analyses.
    # IMPORTANT: Any new expressions that are sent to the backend
    # but don't appear in `ir` must be added here.
    extra_exprs = []
    extra_exprs += [
        rw for rw in ctx.env.type_rewrites.values()
        if isinstance(rw, irast.Set)
    ]
    extra_exprs += [
        p.sub_params.decoder_ir for p in ctx.env.query_parameters.values()
        if p.sub_params and p.sub_params.decoder_ir
    ]
    extra_exprs += [trigger.expr for stage in ir_triggers for trigger in stage]

    all_exprs = [ir] + extra_exprs

    # exprs_to_clear collects sets where we should never need to use
    # their expr in pgsql compilation, so we strip it out to make this
    # more evident in debug output. We have to do the clearing at the
    # end, because multiplicity/cardinality inference needs to be able
    # to look through those pointers.
    exprs_to_clear = _fixup_materialized_sets(all_exprs, ctx=ctx)
    for expr in all_exprs:
        exprs_to_clear.extend(_find_visible_binding_refs(expr, ctx=ctx))

    # The inference context object will be shared between
    # cardinality and multiplicity inferrers.
    inf_ctx = inference.make_ctx(env=ctx.env)
    cardinality = inference.infer_cardinality(
        ir, scope_tree=ctx.path_scope, ctx=inf_ctx
    )
    multiplicity = inference.infer_multiplicity(
        ir, scope_tree=ctx.path_scope, ctx=inf_ctx
    )

    for extra in extra_exprs:
        inference.infer_cardinality(
            extra, scope_tree=ctx.path_scope, ctx=inf_ctx)
        inference.infer_multiplicity(
            extra, scope_tree=ctx.path_scope, ctx=inf_ctx)

    # Fix up weak namespaces
    _rewrite_weak_namespaces(all_exprs, ctx)

    ctx.path_scope.validate_unique_ids()

    # Collect query parameters
    params = collect_params(ctx)

    # ConfigSet and ConfigReset don't like being part of a Set, so bail early
    if isinstance(ir.expr, (irast.ConfigSet, irast.ConfigReset)):
        ir.expr.scope_tree = ctx.path_scope
        ir.expr.globals = list(ctx.env.query_globals.values())
        ir.expr.params = params
        ir.expr.schema = ctx.env.schema

        return ir.expr

    volatility = inference.infer_volatility(ir, env=ctx.env)
    expr_type = setgen.get_set_type(ir, ctx=ctx)

    in_polymorphic_func = (
        ctx.env.options.func_params is not None and
        ctx.env.options.func_params.has_polymorphic(ctx.env.schema)
    )
    if (
        not in_polymorphic_func
        and not ctx.env.options.allow_generic_type_output
    ):
        anytype = expr_type.find_generic(ctx.env.schema)
        if anytype is not None:
            raise errors.QueryError(
                'expression returns value of indeterminate type',
                hint='Consider using an explicit type cast.',
                span=ctx.env.type_origins.get(anytype))

    # Clear out exprs that we decided to omit from the IR
    for ir_set in exprs_to_clear:
        new = (
            irast.MaterializedExpr(typeref=ir_set.typeref)
            if ir_set.is_materialized_ref
            else irast.VisibleBindingExpr(typeref=ir_set.typeref)
        )
        if isinstance(ir_set.expr, irast.Pointer):
            ir_set.expr.expr = new
        else:
            ir_set.expr = new

    # Analyze GROUP statements to find aggregates that can be optimized
    group.infer_group_aggregates(all_exprs, ctx=ctx)

    # If we are producing a schema view, clean up the result types
    if ctx.env.options.schema_view_mode:
        _fixup_schema_view(ctx=ctx)

    result = irast.Statement(
        expr=ir,
        params=params,
        globals=list(ctx.env.query_globals.values()),
        views=ctx.view_nodes,
        scope_tree=ctx.env.path_scope,
        cardinality=cardinality,
        volatility=volatility,
        multiplicity=multiplicity.own,
        stype=expr_type,
        view_shapes={
            src: [ptr for ptr, op in ptrs if op != qlast.ShapeOp.MATERIALIZE]
            for src, ptrs in ctx.env.view_shapes.items()
            if isinstance(src, s_obj.Object)
        },
        view_shapes_metadata=ctx.env.view_shapes_metadata,
        schema=ctx.env.schema,
        schema_refs=frozenset(
            {
                r
                for r in ctx.env.schema_refs
                # filter out newly derived objects
                if ctx.env.orig_schema.has_object(r.id)
            }
        ),
        schema_ref_exprs=ctx.env.schema_ref_exprs,
        type_rewrites={
            (typ.id, not skip_subtypes): s
            for (typ, skip_subtypes), s in ctx.env.type_rewrites.items()
            if isinstance(s, irast.Set)},
        dml_exprs=ctx.env.dml_exprs,
        singletons=ctx.env.singletons,
        triggers=ir_triggers,
        warnings=tuple(ctx.env.warnings),
    )
    return result


def collect_params(ctx: context.ContextLevel) -> List[irast.Param]:
    lparams = [
        p for p in ctx.env.query_parameters.values() if not p.is_sub_param
    ]
    if ctx.env.script_params:
        script_ordering = {k: i for i, k in enumerate(ctx.env.script_params)}
        lparams.sort(key=lambda x: script_ordering[x.name])

    params = []
    # Now flatten it out, including all sub_params, making sure subparams
    # appear in the right order.
    for p in lparams:
        params.append(p)
        if p.sub_params:
            params.extend(p.sub_params.params)
    return params


def _fixup_materialized_sets(
    irs: Sequence[irast.Base], *, ctx: context.ContextLevel
) -> List[irast.Set]:
    # Make sure that all materialized sets have their views compiled
    skips = {'materialized_sets'}
    children = []
    for ir in irs:
        children += ast_visitor.find_children(
            ir, irast.Stmt, extra_skips=skips)

    to_clear = []
    for stmt in ordered.OrderedSet(children):
        if not stmt.materialized_sets:
            continue
        for key in list(stmt.materialized_sets):
            mat_set = stmt.materialized_sets[key]
            assert not mat_set.finalized

            if len(mat_set.uses) <= 1:
                del stmt.materialized_sets[key]
                continue

            ir_set = mat_set.materialized
            assert ir_set.path_scope_id is not None
            new_scope = ctx.env.scope_tree_nodes[ir_set.path_scope_id]
            parent = not_none(new_scope.parent)

            good_reason = False
            for x in mat_set.reason:
                if isinstance(x, irast.MaterializeVolatile):
                    good_reason = True
                elif isinstance(x, irast.MaterializeVisible):
                    reason_scope = ctx.env.scope_tree_nodes[x.path_scope_id]
                    reason_parent = not_none(reason_scope.parent)

                    # If any of the bindings that the set uses are
                    # *visible* at the definition point and *not
                    # visible* from at least one use point, we need to
                    # materialize, to make sure that the use site sees
                    # the same value for the binding as the definition
                    # point. If it's not visible, then it's just being
                    # used internally and we don't need any special
                    # work.
                    use_scopes = [
                        ctx.env.scope_tree_nodes.get(x.path_scope_id)
                        if x.path_scope_id is not None
                        else None
                        for x in mat_set.use_sets
                    ]
                    for b, _ in x.sets:
                        if (
                            reason_parent.is_visible(b, allow_group=True)
                        ) and not all(
                            use_scope and use_scope.parent
                            and use_scope.parent.is_visible(
                                b, allow_group=True)
                            for use_scope in use_scopes
                        ):
                            good_reason = True
                            break

            if not good_reason:
                del stmt.materialized_sets[key]
                continue

            # Compile the view shapes in the set
            with ctx.new() as subctx:
                subctx.implicit_tid_in_shapes = False
                subctx.implicit_tname_in_shapes = False
                subctx.path_scope = new_scope
                subctx.path_scope = parent.attach_fence()
                viewgen.late_compile_view_shapes(ir_set, ctx=subctx)

            for use_set in mat_set.use_sets:
                if use_set != mat_set.materialized:
                    use_set.is_materialized_ref = True
                    # XXX: Deleting it on linkprops breaks a bunch of
                    # linkprop related DML...
                    if not use_set.path_id.is_linkprop_path():
                        to_clear.append(use_set)

            assert (
                not any(use.src_path() for use in mat_set.uses)
                or isinstance(mat_set.materialized.expr, irast.Pointer)
            ), f"materialized ptr {mat_set.uses} missing pointer"
            mat_set.finalized = True

    return to_clear


def _find_visible_binding_refs(
    ir: irast.Base, *, ctx: context.ContextLevel
) -> List[irast.Set]:
    children = ast_visitor.find_children(
        ir, irast.Set, lambda n: n.is_visible_binding_ref)
    return children


def _try_namespace_fix(
    scope: irast.ScopeTreeNode,
    path_id: irast.PathId,
) -> irast.PathId:
    for prefix in path_id.iter_prefixes():
        replacement = scope.find_visible(prefix, allow_group=True)
        if (
            replacement and replacement.path_id
            and prefix != replacement.path_id
        ):
            new = irtyputils.replace_pathid_prefix(
                path_id, prefix, replacement.path_id)

            return new

    return path_id


def _rewrite_weak_namespaces(
    irs: Sequence[irast.Base], ctx: context.ContextLevel
) -> None:
    """Rewrite weak namespaces in path ids to be usable by the backend.

    Weak namespaces in path ids in the frontend are "relative", and
    their interpretation depends on the current scope tree node and
    the namespaces on the parent nodes. The IR->pgsql compiler does
    not do this sort of interpretation, and needs path IDs that are
    "absolute".

    To accomplish this, we go through all the path ids and rewrite
    them: using the scope tree, we try to find the binding of the path
    ID (using a prefix if necessary) and drop all namespace parts that
    don't appear in the binding.
    """

    tree = ctx.path_scope

    for node in tree.strict_descendants:
        if node.path_id:
            node.path_id = _try_namespace_fix(node, node.path_id)

    scopes = irutils.find_path_scopes(irs)

    for ir_set in ctx.env.set_types:
        path_scope_id: Optional[int] = scopes.get(ir_set)
        if path_scope_id is not None:
            # Some entries in set_types are from compiling views
            # in temporary scopes, so we need to just skip those.
            if scope := ctx.env.scope_tree_nodes.get(path_scope_id):
                ir_set.path_id = _try_namespace_fix(scope, ir_set.path_id)


def _fixup_schema_view(*, ctx: context.ContextLevel) -> None:
    """Finalize schema view types for inclusion in the real schema.

    This includes setting from_alias flags and collapsing opaque
    unions to BaseObject.
    """
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
                'from_alias',
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

            if not isinstance(vptr, s_sources.Source):
                continue

            vptr_own_pointers = vptr.get_pointers(ctx.env.schema)
            for vlprop in vptr_own_pointers.objects(ctx.env.schema):
                _elide_derived_ancestors(vlprop, ctx=ctx)
                ctx.env.schema = vlprop.set_field_value(
                    ctx.env.schema,
                    'from_alias',
                    True,
                )


def _get_nearest_non_source_derived_parent(
    obj: s_obj.DerivableInheritingObjectT, ctx: context.ContextLevel
) -> s_obj.DerivableInheritingObjectT:
    """Find the nearest ancestor of obj whose "root source" is not derived"""
    schema = ctx.env.schema
    while (
        (src := s_pointers.get_root_source(obj, schema))
        and isinstance(src, s_obj.DerivableInheritingObject)
        and src.get_is_derived(schema)
    ):
        obj = obj.get_bases(schema).first(schema)
    return obj


def _elide_derived_ancestors(
    obj: Union[s_types.InheritingType, s_pointers.Pointer],
    *,
    ctx: context.ContextLevel,
) -> None:
    """Collapse references to derived objects in bases.

    When compiling a schema view expression, make sure we don't
    expose any ephemeral derived objects, as these wouldn't be
    present in the schema outside of the compilation context.
    """

    pbase = obj.get_bases(ctx.env.schema).first(ctx.env.schema)
    new_pbase = _get_nearest_non_source_derived_parent(pbase, ctx)
    if pbase != new_pbase:
        ctx.env.schema = obj.set_field_value(
            ctx.env.schema,
            'bases',
            s_obj.ObjectList.create(ctx.env.schema, [new_pbase]),
        )

        ctx.env.schema = obj.set_field_value(
            ctx.env.schema,
            'ancestors',
            s_obj.compute_ancestors(ctx.env.schema, obj)
        )


def compile_anchor(
    name: str,
    anchor: Union[qlast.Expr, irast.Base, s_obj.Object, irast.PathId],
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    show_as_anchor = True

    if isinstance(anchor, s_types.Type):
        # Anchors should not receive type rewrites; we are already
        # evaluating in their context.
        ctx.env.type_rewrites[anchor, False] = None
        step = setgen.class_set(anchor, ctx=ctx)

    elif (isinstance(anchor, s_pointers.Pointer) and
            not anchor.is_link_property(ctx.env.schema)):
        src = anchor.get_source(ctx.env.schema)
        if src is not None:
            assert isinstance(src, s_objtypes.ObjectType)
            ctx.env.type_rewrites[src, False] = None
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
            ctx.env.type_rewrites[src, False] = None
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

    elif isinstance(anchor, qlast.Base):
        step = dispatch.compile(anchor, ctx=ctx)

    elif isinstance(anchor, irast.Parameter):
        step = setgen.ensure_set(anchor, ctx=ctx)

    elif isinstance(anchor, irast.PathId):
        stype = typegen.type_from_typeref(anchor.target, env=ctx.env)
        step = setgen.class_set(
            stype, path_id=anchor, ignore_rewrites=True, ctx=ctx)

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
    alias: s_name.Name,
    *,
    factoring_fence: bool=False,
    fully_detached: bool=False,
    binding_kind: irast.BindingKind,
    path_id_namespace: Optional[FrozenSet[str]]=None,
    ctx: context.ContextLevel,
) -> irast.Set:

    pinned_pid_ns = path_id_namespace

    with ctx.newscope(fenced=True) as subctx:
        if factoring_fence:
            subctx.path_scope.factoring_fence = True
            subctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)

        if path_id_namespace is not None:
            subctx.path_id_namespace = path_id_namespace

        if not fully_detached:
            cached_view_set = ctx.env.expr_view_cache.get((expr, alias))
            # Detach the view namespace and record the prefix
            # in the parent statement's fence node.
            view_path_id_ns = {ctx.aliases.get('ns')}
            # if view_path_id_ns == {'ns~3'}:
            #     view_path_id_ns = set()
            subctx.path_id_namespace |= view_path_id_ns
            ctx.path_scope.add_namespaces(view_path_id_ns)
        else:
            cached_view_set = None

        if ctx.stmt is not None:
            subctx.stmt = ctx.stmt.parent_stmt

        if cached_view_set is not None:
            subctx.view_scls = setgen.get_set_type(cached_view_set, ctx=ctx)
            view_name = subctx.view_scls.get_name(ctx.env.schema)
            assert isinstance(view_name, s_name.QualName)
        else:
            if (
                isinstance(alias, s_name.QualName)
                and subctx.env.options.schema_view_mode
            ):
                view_name = alias
                subctx.recompiling_schema_alias = True
            else:
                view_name = s_name.QualName(
                    module=ctx.derived_target_module or '__derived__',
                    name=s_name.get_specialized_name(
                        alias,
                        ctx.aliases.get('w')
                    )
                )

        subctx.toplevel_result_view_name = view_name

        view_set = dispatch.compile(astutils.ensure_ql_query(expr), ctx=subctx)
        assert isinstance(view_set, irast.Set)

        ctx.env.path_scope_map[view_set] = context.ScopeInfo(
            path_scope=subctx.path_scope,
            pinned_path_id_ns=pinned_pid_ns,
            binding_kind=binding_kind,
        )

        if not fully_detached:
            # The view path id _itself_ should not be in the nested namespace.
            # The fully_detached case should be handled by the caller.
            if path_id_namespace is None:
                path_id_namespace = ctx.path_id_namespace
            view_set.path_id = view_set.path_id.replace_namespace(
                path_id_namespace)

        ctx.aliased_views[alias] = view_set
        ctx.env.expr_view_cache[expr, alias] = view_set

    return view_set


def _declare_view_from_schema(
    viewcls: s_types.Type, *, ctx: context.ContextLevel
) -> tuple[s_types.Type, irast.Set]:
    # We need to include "security context" things (currently just
    # access policy state) in the cache key, here.
    #
    # FIXME: Could we do better? Sometimes we might compute a single
    # global twice now, as a result of this. It should be possible to
    # make some decisions based on whether the alias actually does
    # touch any access policies...
    key = viewcls, ctx.get_security_context()
    e = ctx.env.schema_view_cache.get(key)
    if e is not None:
        return e

    # N.B: This takes a context, which we need to use to create a
    # subcontext to compile in, but it should avoid depending on the
    # context, because of the cache.
    with ctx.detached() as subctx:
        subctx.schema_factoring()
        subctx.current_schema_views += (viewcls,)
        subctx.expr_exposed = context.Exposure.UNEXPOSED
        view_expr: s_expr.Expression | None = viewcls.get_expr(ctx.env.schema)
        assert view_expr is not None
        view_ql = view_expr.parse()
        viewcls_name = viewcls.get_name(ctx.env.schema)
        assert isinstance(view_ql, qlast.Expr), 'expected qlast.Expr'
        view_set = declare_view(
            view_ql,
            alias=viewcls_name,
            binding_kind=irast.BindingKind.Schema,
            fully_detached=True,
            ctx=subctx,
        )
        # The view path id _itself_ should not be in the nested namespace.
        view_set.path_id = view_set.path_id.replace_namespace(frozenset())
        view_set.is_schema_alias = True

        vs = subctx.aliased_views[viewcls_name]
        assert vs is not None
        vc = setgen.get_set_type(vs, ctx=ctx)
        ctx.env.schema_view_cache[key] = vc, view_set

    return vc, view_set


def declare_view_from_schema(
    viewcls: s_types.Type, *, ctx: context.ContextLevel
) -> s_types.Type:
    vc, view_set = _declare_view_from_schema(viewcls, ctx=ctx)

    viewcls_name = viewcls.get_name(ctx.env.schema)

    ctx.aliased_views[viewcls_name] = view_set
    ctx.view_nodes[vc.get_name(ctx.env.schema)] = vc
    ctx.view_sets[vc] = view_set

    return vc


def check_params(params: Dict[str, irast.Param]) -> None:
    first_argname = next(iter(params))
    for param in params.values():
        # FIXME: context?
        if param.name.isdecimal() != first_argname.isdecimal():
            raise errors.QueryError(
                f'cannot combine positional and named parameters '
                f'in the same query')

    if first_argname.isdecimal():
        args_decnames = {int(arg) for arg in params}
        args_tpl = set(range(len(params)))
        if args_decnames != args_tpl:
            missing_args = args_tpl - args_decnames
            missing_args_repr = ', '.join(f'${a}' for a in missing_args)
            raise errors.QueryError(
                f'missing {missing_args_repr} positional argument'
                f'{"s" if len(missing_args) > 1 else ""}')


def throw_on_shaped_param(
    param: qlast.Parameter, shape: qlast.Shape, ctx: context.ContextLevel
) -> None:
    raise errors.QueryError(
        f'cannot apply a shape to the parameter',
        hint='Consider adding parentheses around the parameter and type cast',
        span=shape.span
    )


def throw_on_loose_param(
    param: qlast.Parameter, ctx: context.ContextLevel
) -> None:
    if ctx.env.options.func_params is not None:
        if ctx.env.options.schema_object_context is s_constr.Constraint:
            raise errors.InvalidConstraintDefinitionError(
                f'dollar-prefixed "$parameters" cannot be used here',
                span=param.span)
        else:
            raise errors.InvalidFunctionDefinitionError(
                f'dollar-prefixed "$parameters" cannot be used here',
                span=param.span)
    raise errors.QueryError(
        f'missing a type cast before the parameter',
        span=param.span)


def preprocess_script(
    stmts: List[qlast.Base], *, ctx: context.ContextLevel
) -> irast.ScriptInfo:
    """Extract parameters from all statements in a script.

    Doing this in advance makes it easy to check that they have
    consistent types.
    """
    params_lists = [
        astutils.find_parameters(stmt, ctx.modaliases)
        for stmt in stmts
    ]

    if loose_params := [
        loose for params in params_lists
        for loose in params.loose_params
    ]:
        throw_on_loose_param(loose_params[0], ctx)

    if shaped_params := [
        shaped for params in params_lists
        for shaped in params.shaped_params
    ]:
        throw_on_shaped_param(shaped_params[0][0], shaped_params[0][1], ctx)

    casts = [
        cast for params in params_lists for cast in params.cast_params
    ]
    params = {}
    for cast, modaliases in casts:
        assert isinstance(cast.expr, qlast.Parameter)
        name = cast.expr.name
        if name in params:
            continue
        with ctx.new() as mctx:
            mctx.modaliases = modaliases
            target_stype = typegen.ql_typeexpr_to_type(cast.type, ctx=mctx)

        # for ObjectType parameters, we inject intermediate cast to uuid,
        # so parameter is uuid and then cast to ObjectType
        if target_stype.is_object_type():
            uuid_cast = qlast.TypeCast(
                type=qlast.TypeName(maintype=qlast.ObjectRef(name='uuid')),
                expr=cast.expr,
                cardinality_mod=cast.cardinality_mod,
            )
            cast.expr = uuid_cast
            cast = cast.expr

            with ctx.new() as mctx:
                mctx.modaliases = modaliases
                target_stype = typegen.ql_typeexpr_to_type(cast.type, ctx=mctx)

        target_typeref = typegen.type_to_typeref(target_stype, env=ctx.env)
        required = cast.cardinality_mod != qlast.CardinalityModifier.Optional

        sub_params = tuple_args.create_sub_params(
            name, required, typeref=target_typeref, pt=target_stype, ctx=ctx)
        params[name] = irast.Param(
            name=name,
            required=required,
            schema_type=target_stype,
            ir_type=target_typeref,
            sub_params=sub_params,
        )

    if params:
        check_params(params)

        def _arg_key(k: tuple[str, object]) -> int:
            name = k[0]
            arg_prefix = '__edb_arg_'
            # Positional arguments should just be sorted numerically,
            # while for named arguments, injected args should be sorted and
            # need to come after normal ones. Normal named arguments can have
            # any order.
            if name.isdecimal():
                return int(name)
            elif name.startswith(arg_prefix):
                return int(k[0][len(arg_prefix):])
            else:
                return -1

        params = dict(sorted(params.items(), key=_arg_key))

    return irast.ScriptInfo(params=params, schema=ctx.env.schema)
