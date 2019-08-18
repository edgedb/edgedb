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

import functools
import typing

from edb import errors

from edb.common import parsing

from edb.ir import ast as irast

from edb.schema import abc as s_abc
from edb.schema import functions as s_func
from edb.schema import inheriting as s_inh
from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen


def init_context(
        *,
        schema: s_schema.Schema,
        func_params: typing.Optional[s_func.FuncParameterList]=None,
        parent_object_type: typing.Optional[s_obj.ObjectMeta]=None,
        modaliases: typing.Optional[typing.Dict[str, str]]=None,
        anchors: typing.Optional[typing.Dict[str, s_obj.Object]]=None,
        singletons: typing.Optional[typing.Iterable[s_types.Type]]=None,
        security_context: typing.Optional[str]=None,
        derived_target_module: typing.Optional[str]=None,
        result_view_name: typing.Optional[str]=None,
        schema_view_mode: bool=False,
        disable_constant_folding: bool=False,
        allow_generic_type_output: bool=False,
        allow_abstract_operators: bool=False,
        implicit_id_in_shapes: bool=False,
        implicit_tid_in_shapes: bool=False,
        json_parameters: bool=False,
        session_mode: bool=False) -> \
        context.ContextLevel:
    stack = context.CompilerContext()
    ctx = stack.current
    if not schema.get_global(s_mod.Module, '__derived__', None):
        schema, _ = s_mod.Module.create_in_schema(schema, name='__derived__')
    ctx.env = context.Environment(
        schema=schema,
        path_scope=irast.new_scope_tree(),
        constant_folding=not disable_constant_folding,
        func_params=func_params,
        parent_object_type=parent_object_type,
        schema_view_mode=schema_view_mode,
        json_parameters=json_parameters,
        session_mode=session_mode,
        allow_abstract_operators=allow_abstract_operators,
        allow_generic_type_output=allow_generic_type_output)

    if singletons:
        # The caller wants us to treat these type references
        # as singletons for the purposes of the overall expression
        # cardinality inference, so we set up the scope tree in
        # the necessary fashion.
        for singleton in singletons:
            path_id = pathctx.get_path_id(singleton, ctx=ctx)
            ctx.env.path_scope.attach_path(path_id)

        ctx.path_scope = ctx.env.path_scope.attach_fence()

    if modaliases:
        ctx.modaliases.update(modaliases)

    if anchors:
        with ctx.newscope(fenced=True) as subctx:
            populate_anchors(anchors, ctx=subctx)

    ctx.derived_target_module = derived_target_module
    ctx.toplevel_result_view_name = result_view_name
    ctx.implicit_id_in_shapes = implicit_id_in_shapes
    ctx.implicit_tid_in_shapes = implicit_tid_in_shapes

    return ctx


def fini_expression(
        ir: irast.Base, *,
        ctx: context.ContextLevel) -> irast.Command:
    # Run delayed work callbacks.
    for cb in ctx.completion_work:
        cb(ctx=ctx)
    ctx.completion_work.clear()

    for ir_set in ctx.env.set_types:
        if ir_set.path_id.namespace:
            ir_set.path_id = ir_set.path_id.strip_weak_namespaces()

    if isinstance(ir, irast.Command):
        if isinstance(ir, irast.ConfigCommand):
            ir.scope_tree = ctx.path_scope
        # IR is already a Command
        return ir

    if ctx.path_scope is not None:
        # Simple expressions have no scope.
        for node in ctx.path_scope.get_all_path_nodes(include_subpaths=True):
            if node.path_id.namespace:
                node.path_id = node.path_id.strip_weak_namespaces()

        cardinality = inference.infer_cardinality(
            ir, scope_tree=ctx.path_scope, env=ctx.env)
    else:
        cardinality = qltypes.Cardinality.ONE

    if ctx.env.schema_view_mode:
        for view in ctx.view_nodes.values():
            derived_from = view.get_derived_from(ctx.env.schema)
            if (derived_from is not None
                    and derived_from.get_derived_from(ctx.env.schema)
                    is not None):
                ctx.env.schema = view.set_field_value(
                    ctx.env.schema,
                    'derived_from',
                    derived_from.get_nearest_non_derived_parent(
                        ctx.env.schema,
                    )
                )

            base = view.get_bases(ctx.env.schema).first(ctx.env.schema)
            if base.get_derived_from(ctx.env.schema) is not None:
                base = base.get_nearest_non_derived_parent(ctx.env.schema)
                ctx.env.schema = view.set_field_value(
                    ctx.env.schema,
                    'bases',
                    s_obj.ObjectList.create(ctx.env.schema, [base]),
                )

                ctx.env.schema = view.set_field_value(
                    ctx.env.schema,
                    'ancestors',
                    s_inh.compute_ancestors(ctx.env.schema, view)
                )

            if not hasattr(view, 'get_pointers'):  # duck check
                continue

            view_own_pointers = view.get_pointers(ctx.env.schema)
            for vptr in view_own_pointers.objects(ctx.env.schema):
                ctx.env.schema = vptr.set_field_value(
                    ctx.env.schema,
                    'target',
                    vptr.get_target(ctx.env.schema).material_type(
                        ctx.env.schema))

                derived_from = vptr.get_derived_from(ctx.env.schema)
                if derived_from is not None:
                    if (derived_from.get_derived_from(ctx.env.schema)
                            is not None):
                        ctx.env.schema = vptr.set_field_value(
                            ctx.env.schema,
                            'derived_from',
                            derived_from.get_nearest_non_derived_parent(
                                ctx.env.schema,
                            )
                        )
                    elif derived_from.get_union_of(ctx.env.schema):
                        ctx.env.schema = vptr.set_field_value(
                            ctx.env.schema,
                            'derived_from',
                            None,
                        )

                if not hasattr(vptr, 'get_pointers'):
                    continue

                vptr_own_pointers = vptr.get_pointers(ctx.env.schema)
                for vlprop in vptr_own_pointers.objects(ctx.env.schema):
                    vlprop_target = vlprop.get_target(ctx.env.schema)
                    ctx.env.schema = vlprop.set_field_value(
                        ctx.env.schema,
                        'target',
                        vlprop_target.material_type(ctx.env.schema))

    expr_type = inference.infer_type(ir, ctx.env)

    in_polymorphic_func = (
        ctx.env.func_params is not None and
        ctx.env.func_params.has_polymorphic(ctx.env.schema)
    )

    if not in_polymorphic_func and not ctx.env.allow_generic_type_output:
        anytype = expr_type.find_any(ctx.env.schema)
        if anytype is not None:
            raise errors.QueryError(
                'expression returns value of indeterminate type',
                hint='Consider using an explicit type cast.',
                context=ctx.env.type_origins.get(anytype))

    result = irast.Statement(
        expr=ir,
        params=ctx.env.query_parameters,
        views=ctx.view_nodes,
        source_map=ctx.source_map,
        scope_tree=ctx.path_scope,
        cardinality=cardinality,
        stype=expr_type,
        view_shapes=ctx.env.view_shapes,
        view_shapes_metadata=ctx.env.view_shapes_metadata,
        schema=ctx.env.schema,
        schema_refs=frozenset(ctx.env.schema_refs),
    )
    return result


def _derive_dummy_ptr(ptr, *, ctx: context.ContextLevel):
    stdobj = ctx.env.schema.get('std::Object')
    derived_obj_name = stdobj.get_derived_name(
        ctx.env.schema, stdobj, module='__derived__')
    derived_obj = ctx.env.schema.get(derived_obj_name, None)
    if derived_obj is None:
        ctx.env.schema, derived_obj = stdobj.derive(
            ctx.env.schema, stdobj, name=derived_obj_name)

    derived_name = ptr.get_derived_name(
        ctx.env.schema, derived_obj)

    derived = ctx.env.schema.get(derived_name, None)
    if derived is None:
        ctx.env.schema, derived = ptr.derive(
            ctx.env.schema,
            derived_obj,
            derived_obj,
            attrs={
                'cardinality': qltypes.Cardinality.MANY,
            },
            name=derived_name,
            mark_derived=True)

    return derived


def compile_anchor(
        name: str, anchor: typing.Union[qlast.Expr, s_obj.Object], *,
        ctx: context.ContextLevel) -> qlast.Expr:

    show_as_anchor = True

    if isinstance(anchor, s_abc.Type):
        step = setgen.class_set(anchor, ctx=ctx)

    elif (isinstance(anchor, s_pointers.Pointer) and
            not anchor.is_link_property(ctx.env.schema)):
        if anchor.get_source(ctx.env.schema):
            path = setgen.extend_path(
                setgen.class_set(anchor.get_source(ctx.env.schema), ctx=ctx),
                anchor,
                s_pointers.PointerDirection.Outbound,
                anchor.get_target(ctx.env.schema),
                ctx=ctx)
        else:
            ptrcls = _derive_dummy_ptr(anchor, ctx=ctx)
            path = setgen.extend_path(
                setgen.class_set(ptrcls.get_source(ctx.env.schema), ctx=ctx),
                ptrcls,
                s_pointers.PointerDirection.Outbound,
                ptrcls.get_target(ctx.env.schema),
                ctx=ctx)

        step = path

    elif (isinstance(anchor, s_pointers.Pointer) and
            anchor.is_link_property(ctx.env.schema)):

        anchor_source = anchor.get_source(ctx.env.schema)
        anchor_source_source = anchor_source.get_source(ctx.env.schema)

        if anchor_source_source:
            path = setgen.extend_path(
                setgen.class_set(anchor_source_source, ctx=ctx),
                anchor_source,
                s_pointers.PointerDirection.Outbound,
                anchor.get_source(ctx.env.schema).get_target(ctx.env.schema),
                ctx=ctx)
        else:
            ptrcls = _derive_dummy_ptr(anchor_source, ctx=ctx)
            path = setgen.extend_path(
                setgen.class_set(ptrcls.get_source(ctx.env.schema), ctx=ctx),
                ptrcls,
                s_pointers.PointerDirection.Outbound,
                ptrcls.get_target(ctx.env.schema),
                ctx=ctx)

        step = setgen.extend_path(
            path,
            anchor,
            s_pointers.PointerDirection.Outbound,
            anchor.get_target(ctx.env.schema),
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
        anchors: typing.Dict[str, s_obj.Object], *,
        ctx: context.ContextLevel) -> None:

    for name, val in anchors.items():
        ctx.anchors[name] = compile_anchor(name, val, ctx=ctx)


def declare_view(
        expr: qlast.Base, alias: str, *,
        fully_detached: bool=False,
        temporary_scope: bool=True,
        ctx: context.ContextLevel) -> irast.Set:

    with ctx.newscope(temporary=temporary_scope, fenced=True) as subctx:
        if not fully_detached:
            cached_view_set = ctx.expr_view_cache.get((expr, alias))
            # Detach the view namespace and record the prefix
            # in the parent statement's fence node.
            view_path_id_ns = irast.WeakNamespace(ctx.aliases.get('ns'))
            subctx.path_id_namespace |= {view_path_id_ns}
            ctx.path_scope.add_namespaces((view_path_id_ns,))
        else:
            cached_view_set = None

        if ctx.stmt is not None:
            subctx.stmt = ctx.stmt.parent_stmt

        if cached_view_set is not None:
            subctx.view_scls = setgen.get_set_type(cached_view_set, ctx=ctx)
            view_name = subctx.view_scls.get_name(ctx.env.schema)
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

        if not fully_detached:
            # The view path id _itself_ should not be in the nested namespace.
            # The fully_detached case should be handled by the caller.
            view_set.path_id = view_set.path_id.replace_namespace(
                ctx.path_id_namespace)

        ctx.aliased_views[alias] = setgen.get_set_type(view_set, ctx=ctx)
        ctx.path_scope_map[view_set] = subctx.path_scope
        ctx.expr_view_cache[expr, alias] = view_set

    return view_set


def declare_view_from_schema(
        viewcls: s_obj.Object, *,
        ctx: context.ContextLevel) -> irast.Set:
    vc = ctx.env.schema_view_cache.get(viewcls)
    if vc is not None:
        return vc

    with ctx.detached() as subctx:
        subctx.expr_exposed = False
        view_expr = qlparser.parse(viewcls.get_expr(ctx.env.schema).text)
        viewcls_name = viewcls.get_name(ctx.env.schema)
        view_set = declare_view(view_expr, alias=viewcls_name,
                                fully_detached=True, ctx=subctx)
        # The view path id _itself_ should not be in the nested namespace.
        view_set.path_id = view_set.path_id.replace_namespace(
            ctx.path_id_namespace)

        vc = subctx.aliased_views[viewcls_name]
        ctx.env.schema_view_cache[viewcls] = vc
        ctx.source_map.update(subctx.source_map)
        ctx.aliased_views[viewcls_name] = subctx.aliased_views[viewcls_name]
        ctx.view_nodes[vc.get_name(ctx.env.schema)] = vc
        ctx.view_sets[vc] = subctx.view_sets[vc]

    return vc


def infer_expr_cardinality(
        *,
        irexpr: irast.Expr,
        ctx: context.ContextLevel) -> None:

    scope = pathctx.get_set_scope(ir_set=irexpr, ctx=ctx)
    if scope is None:
        scope = ctx.path_scope
    return inference.infer_cardinality(irexpr, scope_tree=scope, env=ctx.env)


def _infer_pointer_cardinality(
        *,
        ptrcls: s_pointers.Pointer,
        irexpr: irast.Expr,
        specified_card: typing.Optional[qltypes.Cardinality] = None,
        source_ctx: typing.Optional[parsing.ParserContext] = None,
        ctx: context.ContextLevel) -> None:

    inferred_card = infer_expr_cardinality(irexpr=irexpr, ctx=ctx)
    if specified_card is None or inferred_card is specified_card:
        ptr_card = inferred_card
    else:
        if specified_card is qltypes.Cardinality.MANY:
            # Explicit many foo := <expr>, just take it.
            ptr_card = specified_card
        else:
            # Specified cardinality is ONE, but we inferred MANY, this
            # is an error.
            raise errors.QueryError(
                f'possibly more than one element returned by an '
                f'expression for a computable '
                f'{ptrcls.get_verbosename(ctx.env.schema)} '
                f"declared as 'single'",
                context=source_ctx
            )

    ctx.env.schema = ptrcls.set_field_value(
        ctx.env.schema, 'cardinality', ptr_card)
    _update_cardinality_in_derived(ptrcls, ctx=ctx)
    _update_cardinality_callbacks(ptrcls, ctx=ctx)


def _update_cardinality_in_derived(
        ptrcls: s_pointers.Pointer, *,
        ctx: context.ContextLevel) -> None:

    children = ctx.pointer_derivation_map.get(ptrcls)
    if children:
        ptrcls_cardinality = ptrcls.get_cardinality(ctx.env.schema)
        for child in children:
            ctx.env.schema = child.set_field_value(
                ctx.env.schema, 'cardinality', ptrcls_cardinality)
            _update_cardinality_in_derived(child, ctx=ctx)
            _update_cardinality_callbacks(child, ctx=ctx)


def _update_cardinality_callbacks(
        ptrcls: s_pointers.Pointer, *,
        ctx: context.ContextLevel) -> None:

    pending = ctx.pending_cardinality.get(ptrcls)
    if pending:
        for cb in pending.callbacks:
            cb(ptrcls, ctx=ctx)


def pend_pointer_cardinality_inference(
        *,
        ptrcls: s_pointers.Pointer,
        specified_card: typing.Optional[qltypes.Cardinality] = None,
        from_parent: bool=False,
        source_ctx: typing.Optional[parsing.ParserContext] = None,
        ctx: context.ContextLevel) -> None:

    existing = ctx.pending_cardinality.get(ptrcls)
    if existing is not None:
        if (existing.specified_cardinality != specified_card
                or existing.from_parent != from_parent):
            raise errors.InternalServerError(
                f'cardinality inference for {ptrcls.get_name(ctx.env.schema)} '
                f'is scheduled multiple times with different context'
            )
    else:
        ctx.pending_cardinality[ptrcls] = context.PendingCardinality(
            specified_cardinality=specified_card,
            source_ctx=source_ctx,
            from_parent=from_parent,
            callbacks=[],
        )


def once_pointer_cardinality_is_inferred(
        ptrcls: s_pointers.Pointer,
        cb: typing.Callable, *,
        ctx: context.ContextLevel) -> None:

    pending = ctx.pending_cardinality.get(ptrcls)
    if pending is None:
        raise errors.InternalServerError(
            f'{ptrcls.get_name(ctx.env.schema)!r} is not pending '
            f'the cardinality inference')

    pending.callbacks.append(cb)


def get_pointer_cardinality_later(
        *,
        ptrcls: s_pointers.Pointer,
        irexpr: irast.Expr,
        specified_card: typing.Optional[qltypes.Cardinality] = None,
        source_ctx: typing.Optional[parsing.ParserContext] = None,
        ctx: context.ContextLevel) -> None:

    at_stmt_fini(
        functools.partial(
            _infer_pointer_cardinality,
            ptrcls=ptrcls,
            irexpr=irexpr,
            specified_card=specified_card,
            source_ctx=source_ctx),
        ctx=ctx)


def get_expr_cardinality_later(
        *,
        target: irast.Expr,
        field: str,
        irexpr: irast.Expr,
        ctx: context.ContextLevel) -> None:

    def cb(irexpr, ctx):
        card = infer_expr_cardinality(irexpr=irexpr, ctx=ctx)
        setattr(target, field, card)

    at_stmt_fini(
        functools.partial(cb, irexpr=irexpr),
        ctx=ctx)


def ensure_ptrref_cardinality(
        ptrcls: s_pointers.PointerLike,
        ptrref: irast.BasePointerRef, *,
        ctx: context.ContextLevel) -> None:

    if ptrcls.get_cardinality(ctx.env.schema) is None:
        # The cardinality of the pointer is not yet, known,
        # schedule an update of the PointerRef when it
        # becomes available
        def _update_ref_cardinality(ptrcls, *, ctx):
            if ptrcls.singular(ctx.env.schema, ptrref.direction):
                ptrref.dir_cardinality = qltypes.Cardinality.ONE
            else:
                ptrref.dir_cardinality = qltypes.Cardinality.MANY
            ptrref.out_cardinality = ptrcls.get_cardinality(ctx.env.schema)

        once_pointer_cardinality_is_inferred(
            ptrcls, _update_ref_cardinality, ctx=ctx)


def enforce_singleton_now(
        irexpr: irast.Base, *,
        ctx: context.ContextLevel) -> None:
    scope = pathctx.get_set_scope(ir_set=irexpr, ctx=ctx)
    if scope is None:
        scope = ctx.path_scope
    cardinality = inference.infer_cardinality(
        irexpr, scope_tree=scope, env=ctx.env)

    if cardinality != qltypes.Cardinality.ONE:
        raise errors.QueryError(
            'possibly more than one element returned by an expression '
            'where only singletons are allowed',
            context=irexpr.context)


def enforce_singleton(
        irexpr: irast.Base, *,
        ctx: context.ContextLevel) -> None:

    if not ctx.defining_view:
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


def enforce_pointer_cardinality(
        ptrcls,
        irexpr: irast.Base, *,
        ctx: context.ContextLevel) -> None:

    if not ctx.defining_view:
        def _enforce_singleton(ctx):
            if ptrcls.singular(ctx.env.schema):
                enforce_singleton_now(irexpr, ctx=ctx)

        at_stmt_fini(_enforce_singleton, ctx=ctx)


def at_stmt_fini(
        cb: typing.Callable, *,
        ctx: context.ContextLevel) -> None:
    ctx.completion_work.append(cb)
