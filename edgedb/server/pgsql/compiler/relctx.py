##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compiler routines managing relation ranges and scope."""


import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts

from edgedb.server.pgsql import ast as pgast

from . import astutils
from . import context
from . import dbobj
from . import pathctx


def pull_path_namespace(
        *, target: pgast.Query, source: pgast.BaseRangeVar,
        replace_bonds: bool=True, ctx: context.CompilerContext):

    squery = source.query
    if astutils.is_set_op_query(squery):
        # Set op query
        source_qs = [squery, squery.larg, squery.rarg]
    else:
        source_qs = [squery]

    for source_q in source_qs:
        outputs = {o[0] for o in source_q.path_outputs}
        s_paths = set(source_q.path_rvar_map) | outputs
        for path_id in s_paths:
            path_id = pathctx.reverse_map_path_id(
                path_id, target.view_path_id_map)
            if path_id not in target.path_rvar_map or replace_bonds:
                pathctx.put_path_rvar(ctx.env, target, path_id, source)

        for path_id in source_q.path_bonds:
            if path_id in target.path_bonds and not replace_bonds:
                if path_id[-1].name.name == 'Group~2':
                    print('not replacing', path_id)
                continue

            orig_path_id = path_id
            path_id = pathctx.reverse_map_path_id(
                path_id, target.view_path_id_map)

            if (not path_id.is_in_scope(ctx.stmt_path_scope) and
                    not orig_path_id.is_in_scope(ctx.stmt_path_scope)):
                if path_id[-1].name.name == 'Group~2':
                    print('skipping', path_id)
                continue

            pathctx.put_path_bond(target, path_id)

            bond = pathctx.LazyPathVarRef(
                pathctx.get_rvar_path_var, ctx.env, source, path_id)

            ctx.path_bonds[path_id] = bond
            ctx.path_bonds_by_stmt[ctx.stmt][path_id] = bond


def apply_path_bond_injections(
        stmt: pgast.Query, *, ctx: context.CompilerContext) -> bool:

    if ctx.expr_injected_path_bond is not None:
        # Inject an explicitly provided path bond.  This is necessary
        # to ensure the correct output of rels that compute view
        # expressions that do not contain relevant path bonds themselves.
        alias = ctx.genalias(hint='b')
        bond_ref = ctx.expr_injected_path_bond['ref']
        bond_path_id = ctx.expr_injected_path_bond['path_id']

        ex_ref = pathctx.maybe_get_path_var(ctx.env, stmt, bond_path_id)
        if ex_ref is None:
            stmt.target_list.append(
                pgast.ResTarget(val=bond_ref, name=alias)
            )
            # Register this bond as output just in case.
            # BUT, do not add it to path_bonds.
            pathctx.put_path_output(ctx.env, stmt, bond_path_id, alias)
            pathctx.put_path_bond(stmt, bond_path_id)

        return True
    else:
        return False


def include_range(
        stmt: pgast.Query, rel: pgast.Query, join_type: str='inner',
        lateral: bool=False, replace_bonds: bool=True, *,
        ctx: context.CompilerContext) -> pgast.BaseRangeVar:
    """Ensure the *rel* is present in the from_clause of *stmt*.

    :param stmt:
        The statement to include *rel* in.

    :param rel:
        The relation node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param lateral:
        Whether *rel* should be joined laterally.

    :param replace_bonds:
        Whether the path bonds in *stmt* should be replaced.

    :return:
        RangeVar or RangeSubselect representing the *rel* in the
        context of current rel.
    """
    rvar = ctx.subquery_map[stmt].get(rel)
    if rvar is None:
        # The rel has not been recorded as a sub-relation of this rel,
        # make it so.
        rvar = dbobj.rvar_for_rel(ctx.env, rel, lateral=lateral)
        pathctx.rel_join(ctx.env, stmt, rvar, type=join_type)

        ctx.subquery_map[stmt][rel] = rvar

    # Make sure that the path namespace of *cte* is mapped
    # onto the path namespace of *stmt*.
    pull_path_namespace(
        target=stmt, source=rvar, replace_bonds=replace_bonds, ctx=ctx)

    return rvar


def get_root_rvar(
        ir_set: irast.Set, stmt: pgast.Query, nullable: bool=False,
        set_rvar: pgast.BaseRangeVar=None, *,
        ctx: context.CompilerContext) -> pgast.BaseRangeVar:
    if not isinstance(ir_set.scls, s_concepts.Concept):
        return None

    if set_rvar is None:
        set_rvar = dbobj.range_for_set(ctx.env, ir_set)
        set_rvar.nullable = nullable
        set_rvar.path_bonds.add(ir_set.path_id)

    pathctx.put_path_rvar(ctx.env, stmt, ir_set.path_id, set_rvar)

    if ir_set.path_id in ctx.stmt_path_scope:
        pathctx.put_path_bond(stmt, ir_set.path_id)

    return set_rvar


def ensure_correct_rvar_for_expr(
        ir_set: irast.Set, stmt: pgast.Query, set_expr: pgast.Base, *,
        ctx: context.CompilerContext):
    restarget = pgast.ResTarget(val=set_expr, name='v')

    if isinstance(ir_set.scls, s_concepts.Concept):
        root_rvar = get_root_rvar(ir_set, stmt, ctx=ctx)
        subqry = pgast.SelectStmt(
            target_list=[restarget]
        )
        pathctx.put_path_output(ctx.env, subqry, ir_set, restarget.name,
                                raw=True)
        include_range(stmt, subqry, lateral=True, ctx=ctx)
        pathctx.rel_join(ctx.env, stmt, root_rvar)
        pathctx.put_path_rvar(ctx.env, stmt, ir_set.path_id, root_rvar)
    else:
        stmt.target_list.append(restarget)
        pathctx.put_path_output(ctx.env, stmt, ir_set, restarget.name)


def enforce_path_scope(
        query: pgast.Query,
        path_bonds: typing.Dict[irast.PathId, pathctx.LazyPathVarRef], *,
        ctx: context.CompilerContext):
    cond = pathctx.full_inner_bond_condition(ctx.env, query, path_bonds)
    if cond is not None:
        query.where_clause = astutils.extend_binop(
            query.where_clause, cond)


def put_parent_range_scope(
        ir_set: irast.Set, rvar: pgast.BaseRangeVar, grouped: bool=False, *,
        ctx: context.CompilerContext):
    ir_set = irutils.get_canonical_set(ir_set)
    if ir_set not in ctx.computed_node_rels:
        ctx.computed_node_rels[ir_set] = rvar, grouped


def get_parent_range_scope(
        ir_set: irast.Set, *,
        ctx: context.CompilerContext) \
        -> typing.Tuple[pgast.BaseRangeVar, bool]:
    ir_set = irutils.get_canonical_set(ir_set)
    return ctx.computed_node_rels.get(ir_set)


def get_ctemap_key(
        ir_set: irast.Set, *, lax: typing.Optional[bool]=None,
        ctx: context.CompilerContext) -> tuple:
    ir_set = irutils.get_canonical_set(ir_set)

    if ir_set.rptr is not None and ir_set.expr is None:
        if lax is None:
            lax = bool(ctx.lax_paths)
        key = (ir_set, lax)
    else:
        key = (ir_set, False)

    return key


def put_set_cte(
        ir_set: irast.Set, cte: pgast.BaseRelation, *,
        lax: typing.Optional[bool]=None,
        ctx: context.CompilerContext) -> pgast.BaseRelation:
    key = get_ctemap_key(ir_set, lax=lax, ctx=ctx)
    ctx.ctemap[key] = cte
    ctx.ctemap_by_stmt[ctx.stmt][key] = cte
    return cte


def get_set_cte(
        ir_set: irast.Set, *, lax: typing.Optional[bool]=None,
        ctx: context.CompilerContext) -> typing.Optional[pgast.BaseRelation]:
    key = get_ctemap_key(ir_set, lax=lax, ctx=ctx)
    return ctx.ctemap.get(key)


def pop_prefix_ctes(
        prefix: irast.PathId, *, lax: typing.Optional[bool]=None,
        ctx: context.CompilerContextLevel) -> None:
    if lax is None:
        lax = bool(ctx.lax_paths)
    for key in list(ctx.ctemap):
        ir_set = key[0]
        if key[1] == lax and ir_set.path_id.startswith(prefix):
            ctx.ctemap.pop(key)


def replace_set_cte_subtree(
        ir_set: irast.Set, cte: pgast.BaseRelation, *,
        lax: typing.Optional[bool]=None,
        ctx: context.CompilerContext) -> pgast.BaseRelation:
    pop_prefix_ctes(ir_set.path_id, lax=lax, ctx=ctx)
    return put_set_cte(ir_set, cte, lax=lax, ctx=ctx)
