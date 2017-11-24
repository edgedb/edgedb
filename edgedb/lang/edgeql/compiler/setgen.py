##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL set compilation functions."""


import typing

from edgedb.lang.common import parsing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_linkprops
from edgedb.lang.schema import nodes as s_nodes
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import utils as s_utils
from edgedb.lang.schema import views as s_views

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors
from edgedb.lang.edgeql import parser as qlparser

from . import context
from . import dispatch
from . import pathctx
from . import schemactx
from . import stmtctx
from . import typegen


PtrDir = s_pointers.PointerDirection


def compile_path(expr: qlast.Path, *, ctx: context.ContextLevel) -> irast.Set:
    anchors = ctx.anchors

    path_tip = None

    if expr.partial:
        if ctx.result_path_steps:
            expr.steps = ctx.result_path_steps + expr.steps
        else:
            raise errors.EdgeQLError('could not resolve partial path ',
                                     context=expr.context)

    for i, step in enumerate(expr.steps):
        if isinstance(step, qlast.Self):
            # 'self' can only appear as the starting path label
            # syntactically and is a known anchor
            path_tip = anchors.get(step.__class__)

        elif isinstance(step, qlast.Subject):
            # '__subject__' can only appear as the starting path label
            # syntactically and is a known anchor
            path_tip = anchors.get(step.__class__)

        elif isinstance(step, qlast.ClassRef):
            if i > 0:
                raise RuntimeError(
                    'unexpected ClassRef as a non-first path item')

            refnode = None

            if not step.module:
                # Check if the starting path label is a known anchor
                refnode = anchors.get(step.name)

            if refnode is None:
                # Finally, check if the stating path label is
                # a query defined as a view.
                if path_tip is not None:
                    src_path_id = path_tip.path_id
                else:
                    src_path_id = None

                if not step.module:
                    refnode = ctx.substmts.get((step.name, src_path_id))

                if refnode is None:
                    schema_name = schemactx.resolve_schema_name(
                        step.name, step.module, ctx=ctx)
                    refnode = ctx.substmts.get((schema_name, src_path_id))

            if refnode is not None:
                path_tip = refnode
            else:
                # Starting path label.  Must be a valid reference to an
                # existing Concept class, as aliases and path variables
                # have been checked above.
                scls = schemactx.get_schema_object(step, ctx=ctx)
                if isinstance(scls, s_views.View):
                    path_tip = stmtctx.declare_view_from_schema(scls, ctx=ctx)
                else:
                    path_tip = class_set(scls, ctx=ctx)

            mapped = ctx.view_map.get(path_tip.path_id)
            if mapped is not None:
                path_tip = irast.Set(
                    path_id=mapped.path_id, scls=mapped.scls)

        elif isinstance(step, qlast.Ptr):
            # Pointer traversal step
            ptr_expr = step
            ptr_target = None

            direction = (ptr_expr.direction or
                         s_pointers.PointerDirection.Outbound)
            if ptr_expr.target:
                # ... link [IS Target]
                ptr_target = schemactx.get_schema_object(
                    ptr_expr.target, ctx=ctx)
                if not isinstance(ptr_target, s_concepts.Concept):
                    raise errors.EdgeQLError(
                        f'invalid type filter operand: {ptr_target.name} '
                        f'is not a concept',
                        context=ptr_expr.target.context)

            ptr_name = (ptr_expr.ptr.module, ptr_expr.ptr.name)

            if ptr_expr.type == 'property':
                # Link property reference; the source is the
                # link immediately preceding this step in the path.
                source = path_tip.rptr.ptrcls
            else:
                source = path_tip.scls

            path_tip, _ = path_step(
                path_tip, source, ptr_name, direction, ptr_target,
                source_context=step.context, ctx=ctx)

        else:
            # Arbitrary expression
            if i > 0:
                raise RuntimeError(
                    'unexpected expression as a non-first path item')

            expr = dispatch.compile(step, ctx=ctx)
            if isinstance(expr, irast.Set):
                path_tip = expr
            else:
                path_tip = generated_set(expr, ctx=ctx)

    pathctx.register_path_in_scope(path_tip.path_id, ctx=ctx)

    return path_tip


def path_step(
        path_tip: irast.Set, source: s_sources.Source,
        ptr_name: typing.Tuple[str, str],
        direction: PtrDir,
        ptr_target: s_nodes.Node,
        source_context: parsing.ParserContext, *,
        ctx: context.ContextLevel) \
        -> typing.Tuple[irast.Set, s_pointers.Pointer]:

    if isinstance(source, s_obj.Tuple):
        if ptr_name[0] is not None:
            el_name = '::'.join(ptr_name)
        else:
            el_name = ptr_name[1]

        if el_name in source.element_types:
            path_id = irutils.tuple_indirection_path_id(
                path_tip.path_id, el_name,
                source.element_types[el_name])
            expr = irast.TupleIndirection(
                expr=path_tip, name=el_name, path_id=path_id,
                context=source_context)
        else:
            raise errors.EdgeQLReferenceError(
                f'{el_name} is not a member of a struct')

        tuple_ind = generated_set(expr, ctx=ctx)
        return tuple_ind, None

    else:
        # Check if the tip of the path has an associated shape.
        # This would be the case for paths on views.
        ptrcls = None
        shape_el = None
        view_source = None
        view_set = None

        if irutils.is_view_set(path_tip):
            view_set = irutils.get_subquery_shape(path_tip)

        if view_set is None:
            view_set = path_tip

        # Search for the pointer in the shape associated with
        # the tip of the path, i.e. a view.
        for shape_el in view_set.shape:
            shape_ptrcls = shape_el.rptr.ptrcls
            shape_pn = shape_ptrcls.shortname

            if ((ptr_name[0] and ptr_name == shape_pn.as_tuple()) or
                    ptr_name[1] == shape_pn.name):
                # Found a match!
                ptrcls = shape_ptrcls
                if shape_el.expr is not None:
                    view_source = shape_el
                break

        if ptrcls is None:
            # Try to resolve a pointer using the schema.
            ptrcls = resolve_ptr(
                source, ptr_name, direction, target=ptr_target, ctx=ctx)

        target = ptrcls.get_far_endpoint(direction)
        if isinstance(ptrcls, s_linkprops.LinkProperty):
            target_path_id = path_tip.path_id.ptr_path().extend(
                ptrcls, direction, target)
        else:
            target_path_id = path_tip.path_id.extend(
                ptrcls, direction, target)

        if (view_source is None or shape_el.path_id != target_path_id or
                path_tip.expr is not None):
            orig_path_tip = path_tip
            path_tip = extend_path(
                path_tip, ptrcls, direction, target, ctx=ctx)

            if view_source is not None:
                try:
                    viewctx, ql_source = ctx.source_map[view_source]
                except KeyError:
                    raise RuntimeError(
                        f'cannot find QL source for view shape '
                        f'reference {view_source!r}') from None

                with ctx.new() as subctx:
                    subctx.namespaces = viewctx.namespaces.copy()
                    subctx.substmts = viewctx.substmts.copy()
                    stmtctx.remove_aliased_set(view_set, ctx=subctx)

                    subctx.view_map = ctx.view_map.new_child()
                    subctx.view_map[view_set.path_id] = orig_path_tip

                    expr_set = dispatch.compile(ql_source, ctx=subctx)
                    path_tip.expr = expr_set.expr
        else:
            path_tip = shape_el

        if (isinstance(target, s_concepts.Concept) and
                target.is_virtual and
                ptr_target is not None):
            path_tip = class_indirection_set(
                path_tip, ptr_target, optional=False, ctx=ctx)

        return path_tip, ptrcls


def resolve_ptr(
        near_endpoint: irast.Set,
        ptr_name: typing.Tuple[str, str],
        direction: s_pointers.PointerDirection,
        target: typing.Optional[s_nodes.Node]=None, *,
        ctx: context.ContextLevel) -> s_pointers.Pointer:
    ptr_module, ptr_nqname = ptr_name

    if ptr_module:
        pointer = schemactx.get_schema_object(
            name=ptr_nqname, module=ptr_module, ctx=ctx)
        pointer_name = pointer.name
    else:
        pointer_name = ptr_nqname

    ptr = None

    if isinstance(near_endpoint, s_sources.Source):
        ptr = near_endpoint.resolve_pointer(
            ctx.schema,
            pointer_name,
            direction=direction,
            look_in_children=False,
            include_inherited=True,
            far_endpoint=target)
    else:
        if direction == s_pointers.PointerDirection.Outbound:
            bptr = schemactx.get_schema_object(pointer_name, ctx=ctx)
            schema_cls = ctx.schema.get('schema::Atom')
            if bptr.shortname == 'std::__class__':
                ptr = bptr.derive(ctx.schema, near_endpoint, schema_cls)

    if not ptr:
        if isinstance(near_endpoint, s_links.Link):
            path = f'({near_endpoint.shortname})@({pointer_name})'
        else:
            path = f'({near_endpoint.name}).{direction}({pointer_name})'

        if target:
            path += f'[IS {target.name}]'

        raise errors.EdgeQLReferenceError(
            f'{path} does not resolve to any known path')

    return ptr


def extend_path(
        source_set: irast.Set,
        ptrcls: s_pointers.Pointer,
        direction: PtrDir=PtrDir.Outbound,
        target: typing.Optional[s_nodes.Node]=None, *,
        ctx: context.ContextLevel) -> irast.Set:
    """Return a Set node representing the new path tip."""
    if target is None:
        target = ptrcls.get_far_endpoint(direction)

    if isinstance(ptrcls, s_linkprops.LinkProperty):
        path_id = source_set.path_id.ptr_path().extend(
            ptrcls, direction, target)
    elif direction != s_pointers.PointerDirection.Inbound:
        source = ptrcls.get_near_endpoint(direction)
        if not source_set.scls.issubclass(source):
            # Polymorphic link reference
            source_set = class_indirection_set(
                source_set, source, optional=True, ctx=ctx)

        path_id = source_set.path_id.extend(ptrcls, direction, target)
    else:
        path_id = source_set.path_id.extend(ptrcls, direction, target)

    target_set = irast.Set()
    target_set.scls = target
    target_set.path_id = path_id

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        ptrcls=ptrcls,
        direction=direction
    )

    target_set.rptr = ptr

    if ptrcls.is_pure_computable():
        target_set = computable_ptr_set(ptr, ctx=ctx)

    return target_set


def class_indirection_set(
        source_set: irast.Set,
        target_scls: s_nodes.Node, *,
        optional: bool,
        ctx: context.ContextLevel) -> irast.Set:

    poly_set = irast.Set()
    poly_set.scls = target_scls
    rptr = source_set.rptr
    if rptr is not None and not rptr.ptrcls.singular(rptr.direction):
        cardinality = s_links.LinkMapping.ManyToMany
    else:
        cardinality = s_links.LinkMapping.ManyToOne
    poly_set.path_id = irutils.type_indirection_path_id(
        source_set.path_id, target_scls, optional=optional,
        cardinality=cardinality)

    ptr = irast.Pointer(
        source=source_set,
        target=poly_set,
        ptrcls=poly_set.path_id.rptr(),
        direction=poly_set.path_id.rptr_dir()
    )

    poly_set.rptr = ptr

    return poly_set


def class_set(
        scls: s_nodes.Node, *, ctx: context.ContextLevel) -> irast.Set:
    path_id = pathctx.get_path_id(scls, ctx=ctx)
    return irast.Set(path_id=path_id, scls=scls)


def generated_set(
        expr: irast.Base, path_id: typing.Optional[irast.PathId]=None, *,
        typehint: typing.Optional[s_obj.NodeClass]=None,
        ctx: context.ContextLevel) -> irast.Set:
    alias = ctx.aliases.get('expr')
    if typehint is not None:
        ql_typeref = s_utils.typeref_to_ast(typehint)
        ir_typeref = typegen.ql_typeref_to_ir_typeref(ql_typeref, ctx=ctx)
    else:
        ir_typeref = None

    ir_set = irutils.new_expression_set(
        expr, ctx.schema, path_id, alias=alias, typehint=ir_typeref)

    return ir_set


def scoped_set(
        expr: irast.Base, *,
        typehint: typing.Optional[s_obj.NodeClass]=None,
        ctx: context.ContextLevel) -> irast.Set:
    ir_set = ensure_set(expr, typehint=typehint, ctx=ctx)
    if ir_set.path_scope is None:
        ir_set.path_scope = ctx.path_scope
    return ir_set


def ensure_set(
        expr: irast.Base, *,
        typehint: typing.Optional[s_obj.NodeClass]=None,
        ctx: context.ContextLevel) -> irast.Set:
    if not isinstance(expr, irast.Set):
        expr = generated_set(expr, typehint=typehint, ctx=ctx)

    if (isinstance(expr, irast.EmptySet) and expr.scls is None and
            typehint is not None):
        irutils.amend_empty_set_type(expr, typehint, schema=ctx.schema)

    if typehint is not None and not expr.scls.issubclass(typehint):
        raise errors.EdgeQLError(
            f'expecting expression of type {typehint.name}, '
            f'got {expr.scls.name}',
            context=expr.context
        )
    return expr


def ensure_stmt(expr: irast.Base, *, ctx: context.ContextLevel) -> irast.Stmt:
    if not isinstance(expr, irast.Stmt):
        expr = irast.SelectStmt(
            result=ensure_set(expr, ctx=ctx)
        )
    return expr


def computable_ptr_set(
        rptr: irast.Pointer, *,
        ctx: context.ContextLevel) -> irast.Set:
    """Return ir.Set for a pointer defined as a computable."""
    ptrcls = rptr.ptrcls
    if not ptrcls.default:
        raise ValueError(f'{ptrcls.shortname!r} is not a computable pointer')

    if isinstance(ptrcls.default, s_expr.ExpressionText):
        default_expr = qlparser.parse(ptrcls.default)
    else:
        default_expr = qlast.Constant(value=ptrcls.default)

    # Must use an entirely separate context, as the computable
    # expression is totally independent from the surrounding query.
    subctx = stmtctx.init_context(schema=ctx.schema)
    # subctx.anchors['self'] = rptr.source
    subctx.anchors[qlast.Self] = rptr.source

    subctx.path_id_namespace = subctx.aliases.get('ns')
    subctx.aliases = ctx.aliases
    subctx.path_scope = ctx.path_scope.add_fence()

    comp_ir = dispatch.compile(default_expr, ctx=subctx)
    target_class = irutils.infer_type(comp_ir, schema=subctx.schema)
    comp_ir_set = scoped_set(comp_ir, ctx=subctx)

    substmt = irast.SelectStmt(result=comp_ir_set)

    path_id = rptr.source.path_id.extend(
        ptrcls, s_pointers.PointerDirection.Outbound, target_class)
    s = generated_set(substmt, path_id=path_id, ctx=ctx)
    s.rptr = rptr

    return s
