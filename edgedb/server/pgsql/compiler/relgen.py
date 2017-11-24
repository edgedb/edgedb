##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compiler functions to generate SQL relations for IR sets."""


import contextlib
import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import dispatch
from . import output
from . import pathctx
from . import relctx


class SetRVars:
    __slots__ = ('main', 'new', 'aspect',)

    def __init__(self, main, new, aspect='value'):
        self.main = main
        self.new = new
        self.aspect = aspect


def get_set_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    """Return a BaseRangeVar for a given IR Set.

    @param ir_set: IR Set node.
    """
    path_id = ir_set.path_id
    scope_stmt = None
    rvar = None

    scope_stmt = relctx.maybe_get_scope_stmt(path_id, ctx=ctx)

    if scope_stmt is pathctx.scope_mask:
        scope_stmt = None
    elif scope_stmt is not None:
        rvar = pathctx.maybe_get_path_rvar(
            scope_stmt, ir_set.path_id, aspect='value', env=ctx.env)
    else:
        rvar = relctx.maybe_get_path_rvar(
            ctx.rel, path_id, aspect='value', ctx=ctx)

    if rvar is not None:
        pathctx.put_path_rvar_if_not_exists(
            ctx.rel, path_id, rvar, aspect='value', env=ctx.env)
        return rvar

    with contextlib.ExitStack() as cstack:
        if scope_stmt is not None:
            newctx = cstack.enter_context(ctx.new())
            newctx.rel = scope_stmt
        else:
            newctx = ctx

        subctx = cstack.enter_context(newctx.subrel())

        stmt = subctx.rel
        # If ir.Set compilation needs to produce a subquery,
        # make sure it uses the current subrel.  This makes it
        # possible to set up the path scope here and don't worry
        # about it later.
        subctx.pending_query = stmt

        if scope_stmt is None:
            scope_stmt = ctx.rel
            subctx.path_scope[path_id] = scope_stmt

        if scope_stmt.nonempty:
            stmt.nonempty = True

        if ir_set.path_scope is not None:
            relctx.update_scope(ir_set, stmt, ctx=subctx)

        stmt.name = ctx.env.aliases.get(get_set_rel_alias(ir_set))

        if irutils.is_subquery_set(ir_set):
            rvars = process_set_as_subquery(ir_set, stmt, ctx=subctx)

        elif irutils.is_set_membership_expr(ir_set.expr):
            # A [NOT] IN B expression.
            rvars = process_set_as_membership_expr(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set, irast.EmptySet):
            rvars = process_set_as_empty(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.SetOp):
            # Set operation: UNION
            rvars = process_set_as_setop(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.Coalesce):
            # Expr ?? Expr
            rvars = process_set_as_coalesce(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.EquivalenceOp):
            # Expr ?= Expr
            rvars = process_set_as_equivalence(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.Tuple):
            # Named tuple
            rvars = process_set_as_tuple(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.TupleIndirection):
            # Named tuple indirection.
            rvars = process_set_as_tuple_indirection(
                ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.FunctionCall):
            if ir_set.expr.func.aggregate:
                # Call to an aggregate function.
                rvars = process_set_as_agg_expr(ir_set, stmt, ctx=subctx)
            else:
                # Regular function call.
                rvars = process_set_as_func_expr(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.ExistPred):
            # EXISTS(), which is a special kind of an aggregate.
            rvars = process_set_as_exists_expr(ir_set, stmt, ctx=subctx)

        elif ir_set.expr is not None:
            # All other expressions.
            rvars = process_set_as_expr(ir_set, stmt, ctx=subctx)

        elif ir_set.rptr is not None:
            rvars = process_set_as_path(ir_set, stmt, ctx=subctx)

        else:
            rvars = process_set_as_root(ir_set, stmt, ctx=subctx)

        for rvar, pid, aspect in rvars.new:
            if not pathctx.maybe_get_path_rvar(
                    scope_stmt, pid, aspect=aspect, env=ctx.env):
                relctx.include_rvar(scope_stmt, rvar, path_id=pid,
                                    aspect=aspect, ctx=ctx)

        main_rvar = rvars.main
        pathctx.put_path_rvar_if_not_exists(
            ctx.rel, path_id, main_rvar, aspect=rvars.aspect, env=ctx.env)

    return main_rvar


def set_as_subquery(
        ir_set: irast.Set, *,
        aggregate: bool=False, as_value: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # Compile *ir_set* into a subquery as follows:
    #     (
    #         SELECT <set_rel>.v
    #         FROM <set_rel>
    #     )
    # If *aggregate* is True, then the return value will
    # be aggregated into an array.
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        dispatch.compile(ir_set, ctx=subctx)

        if as_value:
            var = pathctx.get_path_value_var(
                rel=wrapper, path_id=ir_set.path_id, env=ctx.env)
            value = output.output_as_value(var, ctx=ctx)
            wrapper.target_list = [
                pgast.ResTarget(val=value)
            ]
        else:
            if output.in_serialization_ctx(ctx):
                pathctx.get_path_serialized_output(
                    rel=wrapper, path_id=ir_set.path_id, env=ctx.env)
            else:
                if ir_set.path_id.is_concept_path():
                    aspect = 'identity'
                else:
                    aspect = 'value'

                pathctx.get_path_output(
                    rel=wrapper, path_id=ir_set.path_id,
                    aspect=aspect, env=ctx.env)

    result = wrapper

    if aggregate:
        rptr = ir_set.rptr
        if not rptr.ptrcls.singular(rptr.direction):
            result = set_to_array(
                ir_set=ir_set, query=wrapper, ctx=ctx)

    return result


def set_to_array(
        ir_set: irast.Set, query: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    """Convert a set query into an array."""
    if output.in_serialization_ctx(ctx):
        output_ref = pathctx.get_path_serialized_output(
            rel=query, path_id=ir_set.path_id, env=ctx.env)
    else:
        output_ref = pathctx.get_path_value_output(
            rel=query, path_id=ir_set.path_id, env=ctx.env)

    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('aggw')
        )
    )

    val = dbobj.get_rvar_fieldref(subrvar, output_ref)
    val = output.serialize_expr_if_needed(ctx, val)

    result = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.FuncCall(
                    name=('array_agg',),
                    args=[val],
                )
            )
        ],
        from_clause=[
            subrvar
        ]
    )

    return result


def get_set_rel_alias(ir_set: irast.Set) -> str:
    if ir_set.rptr is not None and ir_set.rptr.source.scls is not None:
        alias_hint = '{}_{}'.format(
            ir_set.rptr.source.scls.name.name,
            ir_set.rptr.ptrcls.shortname.name
        )
    else:
        if isinstance(ir_set.scls, s_obj.Collection):
            alias_hint = ir_set.scls.schema_name
        else:
            alias_hint = ir_set.scls.name.name

    return alias_hint


def process_set_as_root(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    rvar = relctx.new_root_rvar(ir_set, ctx=ctx)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_empty(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    rvar = relctx.new_empty_rvar(ir_set, ctx=ctx)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_link_property_ref(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    ir_source = ir_set.rptr.source
    src_rvar = get_set_rvar(ir_source, ctx=ctx)
    rvars = []

    ptr_info = pg_types.get_pointer_storage_info(
        ir_set.rptr.ptrcls, resolve_type=False, link_bias=False)

    if ptr_info.table_type == 'concept':
        # This is a singleton link property stored in source rel,
        # e.g. @target
        return SetRVars(main=src_rvar, new=[])

    with ctx.new() as newctx:
        link_path_id = ir_set.path_id.src_path()
        source_scope_stmt = relctx.get_scope_stmt(
            ir_source.path_id, ctx=newctx)

        link_rvar = pathctx.maybe_get_path_rvar(
            source_scope_stmt, link_path_id, aspect='value', env=ctx.env)

        if link_rvar is None:
            link_rvar = relctx.new_pointer_rvar(
                ir_source.rptr, src_rvar=src_rvar, link_bias=True, ctx=newctx)
            rvars.append((link_rvar, link_path_id, 'value'))

            if isinstance(ir_set.scls, s_concepts.Concept):
                target_rvar = relctx.new_root_rvar(ir_source, ctx=newctx)
                rvars.append((target_rvar, ir_set.path_id, 'identity'))
        else:
            target_rvar = pathctx.maybe_get_path_rvar(
                source_scope_stmt, link_path_id.tgt_path(),
                aspect='value', env=ctx.env)
            if target_rvar is None:
                target_rvar = relctx.new_root_rvar(ir_source, ctx=newctx)
                rvars.append((target_rvar, link_path_id.tgt_path(), 'value'))

    return SetRVars(main=link_rvar, new=rvars)


def process_set_as_path(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    rptr = ir_set.rptr
    ptrcls = rptr.ptrcls
    ir_source = rptr.source

    rvars = []

    # Path is a __class__ reference of a homogeneous set,
    # e.g {1, 2}.__class__.
    is_static_clsref = (isinstance(ir_source.scls, s_atoms.Atom) and
                        ptrcls.shortname == 'std::__class__')
    if is_static_clsref:
        main_rvar = relctx.new_static_class_rvar(ir_set, ctx=ctx)
        rvars.append((main_rvar, ir_set.path_id, 'value'))
        return SetRVars(main=main_rvar, new=rvars)

    if ir_set.path_id.is_type_indirection_path():
        stmt.nonempty = ptrcls.optional
        get_set_rvar(ir_source, ctx=ctx)
        poly_rvar = relctx.new_poly_rvar(ir_set, nullable=True, ctx=ctx)
        relctx.include_rvar(stmt, poly_rvar, ir_set.path_id, ctx=ctx)

        sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
        return SetRVars(
            main=sub_rvar, new=[(sub_rvar, ir_set.path_id, 'value')])

    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=False)

    # Path is a link property.
    is_linkprop = isinstance(ptrcls, s_lprops.LinkProperty)
    # Path is a reference to a relationship stored in the source table.
    is_inline_ref = ptr_info.table_type == 'concept'
    is_atom_ref = not isinstance(ptrcls.target, s_concepts.Concept)
    is_inline_atom_ref = is_inline_ref and is_atom_ref
    source_is_visible = ctx.scope_tree.is_visible(ir_source.path_id)
    semi_join = (
        not source_is_visible and
        ir_source.path_id not in ctx.disable_semi_join and
        not (is_linkprop or is_inline_atom_ref)
    )

    if semi_join:
        with ctx.subrel() as srcctx:
            srcctx.expr_exposed = False
            src_rvar = get_set_rvar(ir_source, ctx=srcctx)
            set_rvar = relctx.semi_join(stmt, ir_set, src_rvar, ctx=srcctx)
            rvars.append((set_rvar, ir_set.path_id, 'value'))

    elif not source_is_visible:
        # If the source path is not visible in the current scope,
        # it means that there are no other paths sharing this path prefix
        # in this scope.  In such cases the path is represented by a subquery
        # rather than a simple set of ranges.
        with ctx.new() as subctx:
            if is_linkprop:
                subctx.disable_semi_join.add(ir_source.path_id)
                subctx.unique_paths.add(ir_source.path_id)
            src_rvar = get_set_rvar(ir_source, ctx=subctx)

    else:
        src_rvar = get_set_rvar(ir_source, ctx=ctx)

    # Path is a reference to a link property.
    if is_linkprop:
        srvars = process_set_as_link_property_ref(ir_set, stmt, ctx=ctx)
        main_rvar = srvars.main
        rvars.extend(srvars.new)

    elif is_inline_atom_ref:
        main_rvar = relctx.ensure_value_rvar(ir_source, stmt, ctx=ctx)

    elif not semi_join:
        # Link range.
        map_rvar = relctx.new_pointer_rvar(
            ir_set.rptr, src_rvar=src_rvar, ctx=ctx)

        rvars.append((map_rvar, ir_set.path_id.ptr_path(), 'value'))

        # Target set range.
        if isinstance(ir_set.scls, s_concepts.Concept):
            set_rvar = relctx.new_root_rvar(ir_set, ctx=ctx)
            main_rvar = set_rvar
            if ir_source.path_id not in ctx.unique_paths:
                set_rvar.query.is_distinct = False
            rvars.append((set_rvar, ir_set.path_id, 'value'))
        else:
            set_rvar = None
            main_rvar = map_rvar

    if not source_is_visible:
        for rvar, path_id, aspect in rvars:
            relctx.include_rvar(stmt, rvar, path_id=path_id,
                                aspect=aspect, ctx=ctx)

        main_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
        rvars = [(main_rvar, ir_set.path_id, 'value')]

    return SetRVars(main=main_rvar, new=rvars)


def process_set_as_subquery(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    if ir_set.rptr is not None:
        ir_source = ir_set.rptr.source
        get_set_rvar(ir_set.rptr.source, ctx=ctx)

    with ctx.new() as newctx:
        inner_set = ir_set.expr.result
        outer_id = ir_set.path_id
        inner_id = inner_set.path_id

        if inner_id != outer_id:
            ctx.rel.view_path_id_map[outer_id] = inner_id
            newctx.expr_exposed = False

        if ir_set.rptr is not None:
            # This is a computable pointer.  In order to ensure that
            # the volatile functions in the pointer expression are called
            # the necessary number of times, we must inject a
            # "volatility reference" into function expressions.
            # The volatility_ref is the identity of the pointer source.
            newctx.volatility_ref = relctx.maybe_get_path_var(
                stmt, ir_source.path_id, aspect='identity', ctx=ctx)

        dispatch.compile(ir_set.expr, ctx=newctx)

        if isinstance(ir_set.expr, irast.DeleteStmt):
            # DeleteStmt innards are fenced, including the subject path,
            # because it pretends to be a view.
            newctx.path_scope[ir_set.expr.result.path_id] = \
                newctx.path_scope[ir_set.path_id]

    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=sub_rvar, new=[(sub_rvar, ir_set.path_id, 'value')])


def process_set_as_membership_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    # A [NOT] IN B is transformed into
    # SELECT [NOT] bool_or(val(A) = val(B)) FOR A CROSS JOIN B
    # bool_or is used instead of an IN sublink because it is necessary
    # to partition `B` properly considering the path scope.
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left_expr = dispatch.compile(expr.left, ctx=newctx)

        with newctx.subrel() as subctx:
            right_expr = dispatch.compile(expr.right, ctx=subctx)

            if right_expr.nullable:
                op = 'IS NOT DISTINCT FROM'
            else:
                op = ast.ops.EQ

            check_expr = astutils.new_binop(left_expr, right_expr, op=op)
            check_expr = pgast.FuncCall(
                name=('bool_or',), args=[check_expr])

            if expr.op == ast.ops.NOT_IN:
                check_expr = astutils.new_unop(
                    ast.ops.NOT, check_expr)

            wrapper = subctx.rel
            sub_rvar = relctx.new_rel_rvar(ir_set, wrapper, ctx=subctx)

            pathctx.put_path_value_var(
                wrapper, ir_set.path_id, check_expr, env=ctx.env)

    relctx.include_rvar(stmt, sub_rvar, ctx=ctx)
    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=sub_rvar, new=[(sub_rvar, ir_set.path_id, 'value')])


def process_set_as_setop(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False

        with newctx.subrel() as scopectx:
            larg = scopectx.rel
            larg.view_path_id_map[ir_set.path_id] = expr.left.path_id
            dispatch.compile(expr.left, ctx=scopectx)

        with newctx.subrel() as scopectx:
            rarg = scopectx.rel
            rarg.view_path_id_map[ir_set.path_id] = expr.right.path_id
            dispatch.compile(expr.right, ctx=scopectx)

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        # There are only two binary set operators possible coming from IR:
        # UNION and UNION_ALL
        subqry.op = pgast.UNION

        # We cannot simply rely on Postgres to produce a distinct set,
        # as the unioned relations will have distinct output columns.
        # Instead, we always use UNION ALL and then wrap the result
        # with SELECT DISTINCT ON.
        subqry.all = True
        subqry.larg = larg
        subqry.rarg = rarg

        union_rvar = dbobj.rvar_for_rel(subqry, lateral=True, env=subctx.env)
        relctx.include_rvar(stmt, union_rvar, ir_set.path_id, ctx=subctx)

    # UNIONs of concepts _always_ produce distinct sets.
    distinct = (
        expr.op != irast.UNION_ALL or
        isinstance(irutils.infer_type(expr.left, ctx.env.schema),
                   s_concepts.Concept)
    )

    if distinct:
        aspect = 'identity' if ir_set.path_id.is_concept_path() else 'value'

        value_var = pathctx.get_rvar_path_var(
            union_rvar, ir_set.path_id, aspect=aspect, env=ctx.env)

        stmt.distinct_clause = pathctx.get_rvar_output_var_as_col_list(
            union_rvar, value_var, aspect=aspect, env=ctx.env)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_coalesce(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    # Coalesce must not be killed by an empty set resulting in
    # either argument.
    stmt.nonempty = True

    expr = ir_set.expr

    dispatch.compile(expr.left, ctx=ctx)
    dispatch.compile(expr.right, ctx=ctx)

    set_expr = pgast.CoalesceExpr(args=[
        pathctx.get_path_value_var(stmt, expr.left.path_id, env=ctx.env),
        pathctx.get_path_value_var(stmt, expr.right.path_id, env=ctx.env),
    ])

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_equivalence(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    # A ?= B is trivially translated to A IS NOT DISTINCT FROM B,
    # but we need to make sure that neither A nor B are empty rels.
    # We also rely on the proper scope branch structure to make the
    # below "nonempty" effective.
    stmt.nonempty = True

    expr = ir_set.expr

    dispatch.compile(expr.left, ctx=ctx)
    dispatch.compile(expr.right, ctx=ctx)

    if expr.op == irast.NEQUIVALENT:
        op = 'IS DISTINCT FROM'
    else:
        op = 'IS NOT DISTINCT FROM'

    set_expr = astutils.new_binop(
        lexpr=pathctx.get_path_value_var(
            stmt, expr.left.path_id, env=ctx.env),
        rexpr=pathctx.get_path_value_var(
            stmt, expr.right.path_id, env=ctx.env),
        op=op
    )

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_tuple(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as subctx:
        elements = []

        for element in expr.elements:
            path_id = irutils.tuple_indirection_path_id(
                ir_set.path_id, element.name,
                ir_set.scls.element_types[element.name]
            )

            el_val = dispatch.compile(element.val, ctx=subctx)
            aspect = 'identity' if path_id.is_concept_path() else 'value'
            pathctx.put_path_var(stmt, path_id, el_val,
                                 aspect=aspect, env=ctx.env)
            elements.append(pgast.TupleElement(path_id=path_id, aspect=aspect))

        set_expr = pgast.TupleVar(elements=elements, named=expr.named)

    relctx.ensure_bond_for_expr(ir_set, stmt, ctx=ctx)
    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_tuple_indirection(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> typing.List[pgast.BaseRangeVar]:
    tuple_set = ir_set.expr.expr
    aspect = 'identity' if ir_set.path_id.is_concept_path() else 'value'

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        rvar = relctx.maybe_get_path_rvar(
            subctx.rel, tuple_set.path_id, aspect=aspect, ctx=subctx)
        if rvar is None:
            dispatch.compile(tuple_set, ctx=subctx)
            rvar = relctx.get_path_rvar(
                subctx.rel, tuple_set.path_id, aspect=aspect, ctx=subctx)

    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, aspect)],
                    aspect=aspect)


def process_set_as_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    set_expr = dispatch.compile(ir_set.expr, ctx=ctx)

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, 'value')])


def process_set_as_func_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.new() as newctx:
        newctx.rel = stmt
        newctx.expr_exposed = False

        expr = ir_set.expr
        funcobj = expr.func

        args = []

        for ir_arg in ir_set.expr.args:
            arg_ref = dispatch.compile(ir_arg, ctx=newctx)
            args.append(arg_ref)

        with_ordinality = False

        if funcobj.shortname == 'std::array_unpack':
            name = ('unnest',)
        elif funcobj.shortname == 'std::array_enumerate':
            name = ('unnest',)
            with_ordinality = True
        elif funcobj.from_function:
            name = (funcobj.from_function,)
        else:
            name = (
                common.edgedb_module_name_to_schema_name(
                    funcobj.shortname.module),
                common.edgedb_name_to_pg_name(
                    funcobj.shortname.name)
            )

        set_expr = pgast.FuncCall(
            name=name, args=args, with_ordinality=with_ordinality)

    if ir_set.path_id.is_concept_path():
        aspect = 'identity'
    else:
        aspect = 'value'

    if funcobj.set_returning:
        rtype = funcobj.returntype

        if isinstance(funcobj.returntype, s_obj.Tuple):
            colnames = [name for name in rtype.element_types]
        else:
            colnames = [ctx.env.aliases.get('v')]

        func_rvar = pgast.RangeFunction(
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get('f'),
                colnames=colnames),
            lateral=True,
            functions=[set_expr])

        stmt.from_clause.append(func_rvar)

        if len(colnames) == 1:
            set_expr = dbobj.get_column(func_rvar, colnames[0])
        else:
            set_expr = pgast.TupleVar(
                elements=[
                    pgast.TupleElement(
                        path_id=irutils.tuple_indirection_path_id(
                            ir_set.path_id, n, rtype.element_types[n],
                        ),
                        name=n,
                        val=dbobj.get_column(func_rvar, n),
                        aspect=aspect
                    )
                    for n in colnames
                ],
                named=rtype.named
            )

            if funcobj.shortname == 'std::array_enumerate':
                # Patch index colref to (colref - 1) to make
                # enumerate indexes start from 0.
                set_expr.elements[1].val = pgast.Expr(
                    kind=pgast.ExprKind.OP,
                    name='-',
                    lexpr=set_expr.elements[1].val,
                    rexpr=pgast.Constant(val=1))

            for element in set_expr.elements:
                pathctx.put_path_value_var(
                    stmt, element.path_id, element.val, env=ctx.env)

    if ctx.volatility_ref is not None:
        # Apply the volatility reference.
        # See the comment in process_set_as_subquery().
        # XXX: check if the function is actually volatile.
        volatility_source = pgast.SelectStmt(
            values=[pgast.ImplicitRowExpr(args=[ctx.volatility_ref])]
        )
        volatility_rvar = dbobj.rvar_for_rel(volatility_source, env=ctx.env)
        relctx.rel_join(stmt, volatility_rvar, ctx=ctx)

    pathctx.put_path_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, aspect=aspect, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=rvar, new=[(rvar, ir_set.path_id, aspect)],
                    aspect=aspect)


def process_set_as_agg_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.newscope() as newctx:
        expr = ir_set.expr
        funcobj = expr.func
        agg_filter = None
        agg_sort = []

        if ctx.group_by_rels:
            for (path_id, s_path_id), group_rel in ctx.group_by_rels.items():
                group_rvar = dbobj.rvar_for_rel(group_rel, env=ctx.env)
                relctx.include_rvar(stmt, group_rvar, path_id, ctx=ctx)
                ref = pathctx.get_path_identity_var(stmt, path_id, env=ctx.env)
                stmt.group_clause.append(ref)
                newctx.path_scope[s_path_id] = stmt

        with newctx.new() as argctx:
            # We want array_agg() (and similar) to do the right
            # thing with respect to output format, so, barring
            # the (unacceptable) hardcoding of function names,
            # check if the aggregate accepts a single argument
            # of std::any to determine serialized input safety.
            serialization_safe = (
                any(irutils.is_polymorphic_type(p)
                    for p in funcobj.paramtypes) and
                irutils.is_polymorphic_type(funcobj.returntype)
            )

            if not serialization_safe:
                argctx.expr_exposed = False

            args = []

            for i, ir_arg in enumerate(ir_set.expr.args):
                arg_is_visible = argctx.scope_tree.parent.is_visible(
                    ir_arg.path_id)
                arg_ref = dispatch.compile(ir_arg, ctx=argctx)

                if isinstance(arg_ref, pgast.TupleVar):
                    # tuple
                    arg_ref = output.serialize_expr_if_needed(argctx, arg_ref)

                if arg_is_visible:
                    # If the argument set is visible above us, we
                    # are aggregating a singleton set, potentially on
                    # the same query level, as the source set.
                    # Postgre doesn't like aggregates on the same query
                    # level, so wrap the arg ref into a VALUES range.
                    wrapper = pgast.SelectStmt(
                        values=[pgast.ImplicitRowExpr(args=[arg_ref])]
                    )
                    colname = argctx.env.aliases.get('a')
                    wrapper_rvar = dbobj.rvar_for_rel(
                        wrapper, lateral=True, colnames=[colname],
                        env=argctx.env)
                    relctx.include_rvar(stmt, wrapper_rvar, ctx=argctx)
                    arg_ref = dbobj.get_column(wrapper_rvar, colname)

                if (not expr.agg_sort and i == 0 and
                        irutils.is_subquery_set(ir_arg)):
                    # If the first argument of the aggregate
                    # is a SELECT or GROUP with an ORDER BY clause,
                    # we move the ordering conditions to the aggregate
                    # call to make sure the ordering is as expected.
                    substmt = ir_arg.expr
                    if isinstance(substmt, irast.GroupStmt):
                        substmt = substmt.result.expr

                    if (isinstance(substmt, irast.SelectStmt) and
                            substmt.orderby):
                        qrvar = pathctx.get_path_rvar(
                            stmt, ir_arg.path_id,
                            aspect='value', env=argctx.env)
                        query = qrvar.query

                        for i, sortref in enumerate(query.sort_clause):
                            alias = argctx.env.aliases.get(f's{i}')

                            query.target_list.append(
                                pgast.ResTarget(
                                    val=sortref.node,
                                    name=alias
                                )
                            )

                            agg_sort.append(
                                pgast.SortBy(
                                    node=dbobj.get_column(qrvar, alias),
                                    dir=sortref.dir,
                                    nulls=sortref.nulls))

                        query.sort_clause = []

                if (isinstance(ir_arg.scls, s_atoms.Atom) and
                        ir_arg.scls.bases):
                    # Cast atom refs to the base type in aggregate
                    # expressions, since PostgreSQL does not create array
                    # types for custom domains and will fail to process a
                    # query with custom domains appearing as array
                    # elements.
                    pgtype = pg_types.pg_type_from_object(
                        ctx.env.schema, ir_arg.scls, topbase=True)
                    pgtype = pgast.TypeName(name=pgtype)
                    arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                args.append(arg_ref)

        if expr.agg_filter:
            agg_filter = dispatch.compile(expr.agg_filter, ctx=newctx)

        for arg in args:
            if arg.nullable:
                agg_filter = astutils.extend_binop(
                    agg_filter, pgast.NullTest(arg=arg, negated=True))

        if expr.agg_sort:
            with newctx.new() as sortctx:
                for sortexpr in expr.agg_sort:
                    _sortexpr = dispatch.compile(sortexpr.expr, ctx=sortctx)
                    agg_sort.append(
                        pgast.SortBy(
                            node=_sortexpr, dir=sortexpr.direction,
                            nulls=sortexpr.nones_order))

        if funcobj.from_function:
            name = (funcobj.from_function,)
        else:
            name = (
                common.edgedb_module_name_to_schema_name(
                    funcobj.shortname.module),
                common.edgedb_name_to_pg_name(
                    funcobj.shortname.name)
            )

        set_expr = pgast.FuncCall(
            name=name, args=args,
            agg_order=agg_sort, agg_filter=agg_filter,
            agg_distinct=(
                expr.agg_set_modifier == irast.SetModifier.DISTINCT))

    if expr.initial_value is not None:
        if newctx.expr_exposed and serialization_safe:
            # Serialization has changed the output type.
            with newctx.new() as ivctx:
                ivctx.expr_exposed = True
                iv = dispatch.compile(expr.initial_value, ctx=ivctx)
                iv = output.serialize_expr_if_needed(ctx, iv)
                set_expr = output.serialize_expr_if_needed(ctx, set_expr)
        else:
            iv = dispatch.compile(expr.initial_value, ctx=newctx)

        pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
        pathctx.get_path_value_output(stmt, ir_set.path_id, env=ctx.env)

        with ctx.subrel() as subctx:
            wrapper = subctx.rel
            set_expr = pgast.CoalesceExpr(args=[stmt, iv])

            pathctx.put_path_value_var(
                wrapper, ir_set.path_id, set_expr, env=ctx.env)
            stmt = wrapper

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=sub_rvar, new=[(sub_rvar, ir_set.path_id, 'value')])


def process_set_as_exists_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        subctx.expr_exposed = False
        ir_expr = ir_set.expr.expr
        set_ref = dispatch.compile(ir_expr, ctx=subctx)

        pathctx.put_path_value_var(
            wrapper, ir_set.path_id, set_ref, env=ctx.env)
        pathctx.get_path_value_output(
            wrapper, ir_set.path_id, env=ctx.env)

        wrapper.where_clause = astutils.extend_binop(
            wrapper.where_clause, pgast.NullTest(arg=set_ref, negated=True))

        set_expr = pgast.SubLink(
            type=pgast.SubLinkType.EXISTS,
            expr=wrapper
        )

        if ir_set.expr.negated:
            set_expr = astutils.new_unop(ast.ops.NOT, set_expr)

    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return SetRVars(main=sub_rvar, new=[(sub_rvar, ir_set.path_id, 'value')])
