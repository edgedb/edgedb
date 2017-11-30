##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL shape compilation functions."""


import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import nodes as s_nodes
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors

from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common import parsing

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import pathctx
from . import schemactx
from . import setgen


def compile_shape(
        source_expr: irast.Set,
        shapespec: typing.List[qlast.ShapeElement],
        *,
        rptr: typing.Optional[irast.Pointer]=None,
        require_expressions: bool=False,
        include_implicit: bool=True,
        _visited=None,
        _recurse=True,
        ctx: context.CompilerContext) -> irast.Set:
    """Build a shaped Set node given shape spec."""
    if _visited is None:
        _visited = {}
    else:
        _visited = _visited.copy()

    scls = source_expr.scls

    elements = []
    precompiled_exprs = {}

    if include_implicit:
        if isinstance(scls, s_concepts.Concept):
            implicit_ptrs = (sn.Name('std::id'),)

            implicit_shape_els = []

            for pn in implicit_ptrs:
                shape_el = qlast.ShapeElement(
                    expr=qlast.Path(steps=[
                        qlast.Ptr(
                            ptr=qlast.ClassRef(
                                name=pn.name,
                                module=pn.module
                            )
                        )
                    ])
                )

                implicit_shape_els.append(shape_el)

            shapespec = implicit_shape_els + list(shapespec)

        elif source_expr.rptr is not None or rptr is not None:
            target_prop = sn.Name('std::target')
            target_el = qlast.ShapeElement(
                expr=qlast.Path(steps=[
                    qlast.Ptr(
                        ptr=qlast.ClassRef(
                            name=target_prop.name,
                            module=target_prop.module
                        ),
                        type='property'
                    )
                ])
            )
            precompiled_exprs[target_el] = source_expr
            shapespec = [target_el] + list(shapespec)

    for shape_el in shapespec:
        with ctx.newfence() as elctx:
            el = compile_shape_el(
                source_expr, shape_el,
                scls=scls,
                rptr=rptr,
                require_expressions=require_expressions,
                include_implicit=include_implicit,
                precompiled_compexpr=precompiled_exprs.get(shape_el),
                _visited=_visited,
                _recurse=_recurse,
                ctx=elctx)
            el = setgen.scoped_set(el, ctx=elctx)

        # Record element may be none if ptrcls target is non-atomic
        # and recursion has been prohibited on this level to prevent
        # infinite looping.
        if el is not None:
            elements.append(el)

    source_expr.shape = elements

    return source_expr


def compile_shape_el(
        source_expr: irast.Set,
        shape_el: qlast.ShapeElement,
        *,
        rptr: irast.Pointer,
        scls: s_nodes.Node,
        require_expressions: bool=False,
        include_implicit: bool=True,
        precompiled_compexpr: typing.Optional[irast.Base]=None,
        _visited=None, _recurse=True,
        ctx: context.CompilerContext) -> irast.Set:
    ctx.result_path_steps += shape_el.expr.steps

    steps = shape_el.expr.steps
    ptrsource = scls

    if len(steps) == 2:
        # Pointers may be qualified by the explicit source
        # class, which is equivalent to Expr[IS Type].
        ptrsource = schemactx.get_schema_object(steps[0], ctx=ctx)
        lexpr = steps[1]
    elif len(steps) == 1:
        lexpr = steps[0]

    ptrname = (lexpr.ptr.module, lexpr.ptr.name)
    is_linkprop = lexpr.type == 'property'

    if is_linkprop:
        if rptr is None:
            raise errors.EdgeQLError(
                'invalid reference to link property '
                'in top level shape', context=lexpr.context)

        ptrsource = rptr.ptrcls

    ptr_direction = \
        lexpr.direction or s_pointers.PointerDirection.Outbound

    if shape_el.compexpr is not None or precompiled_compexpr is not None:
        # The shape element is defined as a computable expression.
        targetstep = compile_shape_compexpr(
            source_expr, shape_el, ptrname, ptrsource, ptr_direction,
            is_linkprop, rptr, precompiled_compexpr, lexpr.context, ctx=ctx)

        ptrcls = targetstep.rptr.ptrcls

    else:
        if lexpr.target is not None:
            ptr_target = schemactx.get_schema_object(lexpr.target, ctx=ctx)
        else:
            ptr_target = None

        targetstep, ptrcls = setgen.path_step(
            source_expr, ptrsource, ptrname, ptr_direction, ptr_target,
            source_context=shape_el.context, ctx=ctx)

        ctx.singletons.add(targetstep.path_id)

    pathctx.register_path_in_scope(targetstep.path_id, ctx=ctx)

    if shape_el.recurse:
        if shape_el.recurse_limit is not None:
            recurse = dispatch.compile(shape_el.recurse_limit, ctx=ctx)
        else:
            # XXX - temp hack
            recurse = _process_unlimited_recursion(ctx=ctx)
    else:
        recurse = None

    ptr_node = targetstep.rptr

    if _recurse and shape_el.elements:
        if isinstance(ctx.stmt, irast.InsertStmt):
            el = compile_insert_nested_shape(
                targetstep, shape_el.elements, ctx=ctx)
        elif isinstance(ctx.stmt, irast.UpdateStmt):
            el = compile_update_nested_shape(
                targetstep, shape_el.elements, ctx=ctx)
        else:
            el = compile_shape(
                targetstep,
                shape_el.elements or [],
                rptr=ptr_node,
                _visited=_visited,
                _recurse=True,
                require_expressions=require_expressions,
                include_implicit=include_implicit,
                ctx=ctx)
    else:
        el = targetstep

    where = clauses.compile_where_clause(shape_el.where, ctx=ctx)
    orderby = clauses.compile_orderby_clause(shape_el.orderby, ctx=ctx)
    offset = clauses.compile_limit_offset_clause(shape_el.offset, ctx=ctx)
    limit = clauses.compile_limit_offset_clause(shape_el.limit, ctx=ctx)

    if where is not None or orderby or offset is not None or limit is not None:
        substmt = irast.SelectStmt(
            result=el,
            where=where,
            orderby=orderby,
            offset=offset,
            limit=limit,
        )

        if recurse is not None:
            substmt.recurse_ptr = ptr_node
            substmt.recurse_depth = recurse

        wrapper = setgen.generated_set(substmt, path_id=el.path_id, ctx=ctx)
        wrapper.rptr = ptr_node
        wrapper.shape = el.shape
        el.shape = []
        el = wrapper

    return el


def compile_shape_compexpr(
        source_expr: irast.Set,
        shape_el: qlast.ShapeElement,
        ptrname: typing.Tuple[str, str],
        ptrsource: s_sources.Source,
        ptr_direction: s_pointers.PointerDirection,
        is_linkprop: bool,
        rptr: irast.Pointer,
        precompiled_compexpr: irast.Base,
        source_ctx: parsing, *,
        ctx: context.ContextLevel) -> irast.Set:
    schema = ctx.schema

    if is_linkprop:
        ptr_metacls = s_lprops.LinkProperty
    else:
        ptr_metacls = s_links.Link

    if ptrname[0]:
        pointer_name = sn.SchemaName(
            module=ptrname[0], name=ptrname[1])
    else:
        pointer_name = ptrname[1]

    if isinstance(ptrsource, s_atoms.Atom):
        raise errors.EdgeQLError(
            'atoms cannot have computable pointers', context=source_ctx)

    ptrcls = ptrsource.resolve_pointer(
        ctx.schema,
        pointer_name,
        direction=ptr_direction,
        look_in_children=False,
        include_inherited=True)

    if precompiled_compexpr is not None:
        compexpr = precompiled_compexpr
        qlexpr = None
    else:
        with ctx.new() as shape_expr_ctx:
            # Put current pointer class in context, so
            # that references to link properties in sub-SELECT
            # can be resolved.  This is necessary for proper
            # evaluation of link properties on computable links,
            # most importantly, in INSERT/UPDATE context.
            shape_expr_ctx.toplevel_shape_rptr = irast.Pointer(
                source=source_expr,
                ptrcls=ptrcls,
                direction=ptr_direction
            )
            qlexpr = astutils.ensure_qlstmt(shape_el.compexpr)
            compexpr = dispatch.compile(qlexpr, ctx=shape_expr_ctx)

    target_class = irutils.infer_type(compexpr, schema)
    if target_class is None:
        msg = 'cannot determine expression result type'
        raise errors.EdgeQLError(msg, context=source_ctx)

    if ptrcls is None:
        if (isinstance(ctx.stmt, irast.MutatingStmt) and
                ctx.clause != 'result'):
            raise errors.EdgeQLError(
                'reference to unknown pointer',
                context=source_ctx)

        ptr_module = (
            ptrname[0] or
            ctx.derived_target_module or
            ptrsource.name.module
        )

        ptrcls = ptr_metacls(
            name=sn.SchemaName(
                module=ptr_module,
                name=ptrname[1]),
        ).derive(schema, ptrsource, target_class)

        if isinstance(shape_el.compexpr, qlast.Statement):
            if shape_el.compexpr.cardinality == '1':
                ptrcls.mapping = s_links.LinkMapping.ManyToOne
            else:
                ptrcls.mapping = s_links.LinkMapping.ManyToMany
        else:
            cardinality = irinference.infer_cardinality(
                compexpr, ctx.singletons, ctx.schema)
            if cardinality == 1:
                ptrcls.mapping = s_links.LinkMapping.ManyToOne
            else:
                ptrcls.mapping = s_links.LinkMapping.ManyToMany

    compexpr = compexpr.expr

    if is_linkprop:
        path_id = rptr.source.path_id.extend(
            rptr.ptrcls, rptr.direction, source_expr.scls
        ).ptr_path().extend(
            ptrcls, ptr_direction, target_class
        )
    else:
        path_id = source_expr.path_id.extend(
            ptrcls, ptr_direction, target_class)

    targetstep = irast.Set(
        path_id=path_id,
        scls=target_class,
        expr=compexpr
    )

    ctx.singletons.add(targetstep.path_id)
    if qlexpr is not None:
        ctx.source_map[targetstep] = (ctx, qlexpr)

    targetstep.rptr = irast.Pointer(
        source=source_expr,
        target=targetstep,
        ptrcls=ptrcls,
        direction=ptr_direction
    )

    if ptrcls.shortname == 'std::__class__':
        msg = 'cannot assign to __class__'
        raise errors.EdgeQLError(msg, context=source_ctx)

    if (isinstance(ctx.stmt, irast.MutatingStmt) and
            ctx.clause != 'result'):
        if (isinstance(ptrcls.target, s_concepts.Concept) and
                not target_class.issubclass(ptrcls.target) and
                target_class.name != 'std::Object'):
            # Validate that the insert/update expression is
            # of the correct class.  Make an exception for
            # expressions returning std::Object, as the
            # GraphQL translator relies on that to support
            # insert-by-object-id.  XXX: remove this
            # exemption once support for class casts is added
            # to DML.
            lname = f'{ptrsource.name}.{ptrcls.shortname.name}'
            expected = [repr(str(ptrcls.target.name))]
            raise edgedb_error.InvalidPointerTargetError(
                f'invalid target for link {str(lname)!r}: '
                f'{str(target_class.name)!r} (expecting '
                f'{" or ".join(expected)})'
            )

    return targetstep


def compile_insert_nested_shape(
        targetstep: irast.Set,
        elements: typing.Iterable[qlast.ShapeElement], *,
        ctx: context.ContextLevel) -> irast.Set:
    mutation_shape = []
    for subel in elements or []:
        is_prop = (
            isinstance(subel.expr.steps[0], qlast.Ptr) and
            subel.expr.steps[0].type == 'property'
        )
        if not is_prop:
            mutation_shape.append(subel)

    ptr_node = targetstep.rptr

    ret_set = targetstep.__copy__()

    el = compile_shape(
        targetstep,
        mutation_shape,
        rptr=ptr_node,
        _recurse=True,
        require_expressions=True,
        include_implicit=False,
        ctx=ctx)

    returning_shape = []
    for subel in elements or []:
        is_prop = (
            isinstance(subel.expr.steps[0], qlast.Ptr) and
            subel.expr.steps[0].type == 'property'
        )
        if is_prop:
            returning_shape.append(subel)

    substmt = irast.InsertStmt(
        subject=el,
        result=compile_shape(
            ret_set,
            returning_shape,
            rptr=ptr_node,
            include_implicit=True,
            ctx=ctx
        ),
    )

    result = setgen.generated_set(substmt, ctx=ctx)
    result.rptr = ptr_node
    return result


def compile_update_nested_shape(
        targetstep: irast.Set,
        elements: typing.Iterable[qlast.ShapeElement], *,
        ctx: context.ContextLevel) -> irast.Set:
    for subel in elements or []:
        is_prop = (
            isinstance(subel.expr.steps[0], qlast.Ptr) and
            subel.expr.steps[0].type == 'property'
        )
        if not is_prop:
            raise errors.EdgeQLError(
                'only references to link properties are allowed '
                'in nested UPDATE shapes', context=subel.context)

    ptr_node = targetstep.rptr

    el = compile_shape(
        targetstep,
        elements,
        rptr=ptr_node,
        _recurse=True,
        require_expressions=True,
        include_implicit=False,
        ctx=ctx)

    substmt = irast.SelectStmt(
        result=el,
    )

    result = setgen.generated_set(substmt, ctx=ctx)
    result.rptr = ptr_node
    return result


def _process_unlimited_recursion(*, ctx):
    type = s_types.normalize_type((0).__class__, ctx.schema)
    return irast.Constant(value=0, index=None, type=type)
