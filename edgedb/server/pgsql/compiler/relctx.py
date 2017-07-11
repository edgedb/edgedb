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
        replace_bonds: bool=True, ctx: context.CompilerContextLevel):

    squery = source.query
    if astutils.is_set_op_query(squery):
        # Set op query
        source_qs = [squery, squery.larg, squery.rarg]
    else:
        source_qs = [squery]

    for source_q in source_qs:
        outputs = {o[0] for o in source_q.path_outputs}
        ns = {o[0] for o in source_q.path_namespace}
        s_paths = set(source_q.path_rvar_map) | outputs | ns
        for path_id in s_paths:
            path_id = pathctx.reverse_map_path_id(
                path_id, target.view_path_id_map)
            if path_id not in target.path_rvar_map or replace_bonds:
                pathctx.put_path_rvar(ctx.env, target, path_id, source)

        for path_id in source_q.path_bonds:
            if path_id in target.path_bonds and not replace_bonds:
                continue

            orig_path_id = path_id
            path_id = pathctx.reverse_map_path_id(
                path_id, target.view_path_id_map)

            if (not path_id.is_in_scope(ctx.stmt_path_scope) and
                    not orig_path_id.is_in_scope(ctx.stmt_path_scope)):
                continue

            pathctx.put_path_bond(target, path_id)

            bond = pathctx.LazyPathVarRef(
                pathctx.get_rvar_path_identity_var, ctx.env, source, path_id)

            ctx.path_bonds[path_id] = bond
            ctx.path_bonds_by_stmt[ctx.stmt][path_id] = bond


def apply_path_bond_injections(
        stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> typing.Optional[irast.PathId]:

    if ctx.expr_injected_path_bond is not None:
        # Inject an explicitly provided path bond.  This is necessary
        # to ensure the correct output of rels that compute view
        # expressions that do not contain relevant path bonds themselves.
        bond_ref = ctx.expr_injected_path_bond['ref']
        bond_path_id = ctx.expr_injected_path_bond['path_id']

        ex_ref = pathctx.maybe_get_path_identity_var(
            stmt, bond_path_id, env=ctx.env)
        if ex_ref is None:
            pathctx.put_path_identity_var(
                stmt, bond_path_id, bond_ref, env=ctx.env)
            pathctx.put_path_bond(stmt, bond_path_id)
        return bond_path_id
    else:
        return None


def include_rvar(
        stmt: pgast.Query, rvar: pgast.BaseRangeVar, join_type: str='inner',
        replace_bonds: bool=True, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    """Ensure the *rvar* is present in the from_clause of *stmt*.

    :param stmt:
        The statement to include *rel* in.

    :param rvar:
        The range var node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param replace_bonds:
        Whether the path bonds in *stmt* should be replaced.
    """
    pathctx.rel_join(ctx.env, stmt, rvar, type=join_type)
    # Make sure that the path namespace of *cte* is mapped
    # onto the path namespace of *stmt*.
    pull_path_namespace(
        target=stmt, source=rvar, replace_bonds=replace_bonds, ctx=ctx)

    return rvar


def include_range(
        stmt: pgast.Query, rel: pgast.Query, join_type: str='inner',
        lateral: bool=False, replace_bonds: bool=True, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
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
        path_id: typing.Optional[irast.PathId]=None,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    if not isinstance(ir_set.scls, s_concepts.Concept):
        return None

    if path_id is None:
        path_id = ir_set.path_id

    if set_rvar is None:
        set_rvar = dbobj.range_for_set(ctx.env, ir_set)
        set_rvar.nullable = nullable
        set_rvar.path_bonds.add(path_id)

    pathctx.put_path_rvar(ctx.env, stmt, path_id, set_rvar)

    if path_id in ctx.stmt_path_scope:
        pathctx.put_path_bond(stmt, path_id)

    return set_rvar


def ensure_correct_rvar_for_expr(
        ir_set: irast.Set, stmt: pgast.Query, set_expr: pgast.Base, *,
        ctx: context.CompilerContextLevel):

    if isinstance(ir_set.scls, s_concepts.Concept):
        root_rvar = get_root_rvar(ir_set, stmt, path_id=ir_set.path_id,
                                  ctx=ctx)

        subqry = pgast.SelectStmt()
        pathctx.put_path_identity_var(subqry, ir_set.path_id,
                                      set_expr, env=ctx.env)
        include_range(stmt, subqry, lateral=True, ctx=ctx)

        pathctx.rel_join(ctx.env, stmt, root_rvar)
        pathctx.put_path_rvar(ctx.env, stmt, ir_set.path_id, root_rvar)
    else:
        pathctx.put_path_value_var_if_not_exists(
            stmt, ir_set.path_id, set_expr, env=ctx.env)


def ensure_bond_for_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, type='int',
        ctx: context.CompilerContextLevel) -> None:
    rt = irutils.infer_type(ir_set, ctx.env.schema)
    if isinstance(rt, s_concepts.Concept):
        # Concepts have inherent identity
        return

    ensure_transient_identity_for_set(ir_set, stmt, type=type, ctx=ctx)


def ensure_transient_identity_for_set(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel, type='int') -> None:

    if type == 'uuid':
        id_expr = pgast.FuncCall(
            name=('uuid_generate_v1mc',),
            args=[],
        )
    else:
        id_expr = pgast.FuncCall(
            name=('row_number',),
            args=[],
            over=pgast.WindowDef()
        )

    pathctx.put_path_identity_var(stmt, ir_set.path_id,
                                  id_expr, force=True, env=ctx.env)
    pathctx.put_path_bond(stmt, ir_set.path_id)


def enforce_path_scope(
        query: pgast.Query,
        path_bonds: typing.Dict[irast.PathId, pathctx.LazyPathVarRef], *,
        ctx: context.CompilerContextLevel):
    cond = pathctx.full_inner_bond_condition(ctx.env, query, path_bonds)
    if cond is not None:
        query.where_clause = astutils.extend_binop(
            query.where_clause, cond)


def put_parent_range_scope(
        ir_set: irast.Set, rvar: pgast.BaseRangeVar, grouped: bool=False, *,
        force: bool=False, ctx: context.CompilerContextLevel):
    ir_set = irutils.get_canonical_set(ir_set)
    if ir_set not in ctx.computed_node_rels or force:
        ctx.computed_node_rels[ir_set] = rvar, grouped


def get_parent_range_scope(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) \
        -> typing.Tuple[pgast.BaseRangeVar, bool]:
    ir_set = irutils.get_canonical_set(ir_set)
    return ctx.computed_node_rels.get(ir_set)


def get_ctemap_key(
        ir_set: irast.Set, *, lax: typing.Optional[bool]=None,
        extrakey: typing.Optional[object]=None,
        ctx: context.CompilerContextLevel) -> tuple:
    ir_set = irutils.get_canonical_set(ir_set)

    if ctx.query is not ctx.toplevel_stmt:
        # Consider parent scope only in qubqueries.
        extrakey = get_parent_range_scope(ir_set, ctx=ctx)
    else:
        extrakey = None

    if ir_set.rptr is not None and ir_set.expr is None:
        if lax is None:
            lax = bool(ctx.lax_paths)
        key = (ir_set, lax, extrakey)
    else:
        key = (ir_set, False, extrakey)

    return key


def put_set_cte(
        ir_set: irast.Set, cte: pgast.BaseRelation, *,
        lax: typing.Optional[bool]=None,
        ctx: context.CompilerContextLevel) -> pgast.BaseRelation:
    key = get_ctemap_key(ir_set, lax=lax, ctx=ctx)
    ctx.ctemap[key] = cte
    ctx.ctemap_by_stmt[ctx.stmt][key] = cte
    return cte


def get_set_cte(
        ir_set: irast.Set, *, lax: typing.Optional[bool]=None,
        ctx: context.CompilerContextLevel) -> \
        typing.Optional[pgast.BaseRelation]:
    key = get_ctemap_key(ir_set, lax=lax, ctx=ctx)
    cte = ctx.ctemap.get(key)
    if cte is None and key[-1] is None:
        cte = ctx.viewmap.get(ir_set)

    return cte


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
        recursive: bool=True,
        ctx: context.CompilerContextLevel) -> None:
    pop_prefix_ctes(ir_set.path_id, lax=lax, ctx=ctx)
    while ir_set is not None:
        put_set_cte(ir_set, cte, lax=lax, ctx=ctx)
        if ir_set.rptr is not None and recursive:
            ir_set = ir_set.rptr.source
        else:
            ir_set = None
