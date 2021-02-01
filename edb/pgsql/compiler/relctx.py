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


"""Compiler routines managing relation ranges and scope."""


from __future__ import annotations
from typing import *

import uuid

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import pointers as s_pointers
from edb.schema import name as sn

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pg_types

from . import astutils
from . import context
from . import dispatch
from . import pathctx


def init_toplevel_query(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    ctx.toplevel_stmt = ctx.stmt = ctx.rel = pgast.SelectStmt()
    update_scope(ir_set, ctx.rel, ctx=ctx)
    ctx.pending_query = ctx.rel


def pull_path_namespace(
        *, target: pgast.Query, source: pgast.PathRangeVar,
        replace_bonds: bool=True, ctx: context.CompilerContextLevel) -> None:

    squery = source.query
    source_qs: List[pgast.BaseRelation]

    if astutils.is_set_op_query(squery):
        # Set op query
        squery = cast(pgast.SelectStmt, squery)
        source_qs = [squery, squery.larg, squery.rarg]
    else:
        source_qs = [squery]

    for source_q in source_qs:
        s_paths: Set[Tuple[irast.PathId, str]] = set()
        if hasattr(source_q, 'path_outputs'):
            s_paths.update(source_q.path_outputs)
        if hasattr(source_q, 'path_namespace'):
            s_paths.update(source_q.path_namespace)
        if isinstance(source_q, pgast.Query):
            s_paths.update(source_q.path_rvar_map)

        view_path_id_map = getattr(source_q, 'view_path_id_map', {})

        for path_id, aspect in s_paths:
            path_id = pathctx.reverse_map_path_id(path_id, view_path_id_map)

            if path_id in squery.path_id_mask:
                continue

            rvar = pathctx.maybe_get_path_rvar(
                target, path_id, aspect=aspect, env=ctx.env)
            if rvar is None:
                pathctx.put_path_rvar(
                    target, path_id, source, aspect=aspect, env=ctx.env)


def find_rvar(
        stmt: pgast.Query, *,
        source_stmt: Optional[pgast.Query]=None,
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> \
        Optional[pgast.PathRangeVar]:
    """Find an existing range var for a given *path_id* in stmt hierarchy.

    If a range var is visible in a given SQL scope denoted by *stmt*, or,
    optionally, *source_stmt*, record it on *stmt* for future reference.

    :param stmt:
        The statement to ensure range var visibility in.

    :param source_stmt:
        An optional statement object which is used as the starting SQL scope
        for range var search.  If not specified, *stmt* is used as the
        starting scope.

    :param path_id:
        The path ID of the range var being searched.

    :param ctx:
        Compiler context.

    :return:
        A range var instance if found, ``None`` otherwise.
    """

    if source_stmt is None:
        source_stmt = stmt

    rvar = maybe_get_path_rvar(source_stmt, path_id=path_id,
                               aspect='value', ctx=ctx)
    if rvar is not None:
        pathctx.put_path_rvar_if_not_exists(
            stmt, path_id, rvar, aspect='value', env=ctx.env)

        src_rvar = maybe_get_path_rvar(source_stmt, path_id=path_id,
                                       aspect='source', ctx=ctx)

        if src_rvar is not None:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, src_rvar, aspect='source', env=ctx.env)

    return rvar


def include_rvar(
        stmt: pgast.SelectStmt,
        rvar: pgast.PathRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        pull_namespace: bool=True,
        aspects: Optional[Iterable[str]]=None,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Ensure that *rvar* is visible in *stmt* as a value/source aspect.

    :param stmt:
        The statement to include *rel* in.

    :param rvar:
        The range var node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param aspect:
        The reference aspect of the range var.

    :param ctx:
        Compiler context.
    """
    if aspects is None:
        if path_id.is_objtype_path():
            aspects = ('source', 'value')
        else:
            aspects = ('value',)

    return include_specific_rvar(
        stmt, rvar=rvar, path_id=path_id,
        overwrite_path_rvar=overwrite_path_rvar,
        pull_namespace=pull_namespace,
        aspects=aspects,
        ctx=ctx)


def include_specific_rvar(
        stmt: pgast.SelectStmt,
        rvar: pgast.PathRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        pull_namespace: bool=True,
        aspects: Iterable[str]=('value',),
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Make the *aspect* of *path_id* visible in *stmt* as *rvar*.

    :param stmt:
        The statement to include *rel* in.

    :param rvar:
        The range var node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param aspect:
        The reference aspect of the range var.

    :param ctx:
        Compiler context.
    """

    if not has_rvar(stmt, rvar, ctx=ctx):
        if not (
            ctx.env.external_rvars
            and has_external_rvar(path_id, aspects, ctx=ctx)
        ):
            rel_join(stmt, rvar, ctx=ctx)
        # Make sure that the path namespace of *rvar* is mapped
        # onto the path namespace of *stmt*.
        if pull_namespace:
            pull_path_namespace(target=stmt, source=rvar, ctx=ctx)

    for aspect in aspects:
        if overwrite_path_rvar:
            pathctx.put_path_rvar(
                stmt, path_id, rvar, aspect=aspect, env=ctx.env)
        else:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, rvar, aspect=aspect, env=ctx.env)

        scopes = [ctx.scope_tree]
        parent_scope = ctx.scope_tree.parent
        if parent_scope is not None:
            scopes.append(parent_scope)

        if not any(scope.path_id == path_id or
                   scope.find_child(path_id) for scope in scopes):
            stmt.path_id_mask.add(path_id)

    return rvar


def has_rvar(
        stmt: pgast.Query, rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> bool:

    curstmt: Optional[pgast.Query] = stmt

    while curstmt is not None:
        if pathctx.has_rvar(curstmt, rvar, env=ctx.env):
            return True
        curstmt = ctx.rel_hierarchy.get(curstmt)

    return False


def has_external_rvar(
    path_id: irast.PathId,
    aspects: Iterable[str],
    *,
    ctx: context.CompilerContextLevel,
) -> bool:
    return (
        bool(ctx.env.external_rvars)
        and all(
            (path_id, aspect) in ctx.env.external_rvars
            for aspect in aspects
        )
    )


def _maybe_get_path_rvar(
    stmt: pgast.Query,
    path_id: irast.PathId,
    *,
    aspect: str,
    ctx: context.CompilerContextLevel,
) -> Optional[Tuple[pgast.PathRangeVar, irast.PathId]]:
    qry: Optional[pgast.Query] = stmt
    while qry is not None:
        rvar = pathctx.maybe_get_path_rvar(
            qry, path_id, aspect=aspect, env=ctx.env)
        if rvar is not None:
            if qry is not stmt:
                # Cache the rvar reference.
                pathctx.put_path_rvar(stmt, path_id, rvar, aspect=aspect,
                                      env=ctx.env)
            return rvar, path_id
        if qry.view_path_id_map:
            path_id = pathctx.reverse_map_path_id(
                path_id, qry.view_path_id_map)
        qry = ctx.rel_hierarchy.get(qry)

    return None


def _get_path_rvar(
    stmt: pgast.Query,
    path_id: irast.PathId,
    *,
    aspect: str,
    ctx: context.CompilerContextLevel,
) -> Tuple[pgast.PathRangeVar, irast.PathId]:
    result = _maybe_get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    if result is None:
        raise LookupError(f'there is no range var for {path_id} in {stmt}')
    else:
        return result


def get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    return _get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)[0]


def get_path_var(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    var = pathctx.maybe_get_path_var(
        stmt, path_id=path_id, aspect=aspect, env=ctx.env)
    if var is not None:
        return var
    else:
        rvar, path_id = _get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
        return pathctx.get_rvar_path_var(
            rvar, path_id, aspect=aspect, env=ctx.env)


def maybe_get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel
) -> Optional[pgast.PathRangeVar]:
    result = _maybe_get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    return result[0] if result is not None else None


def maybe_get_path_var(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel
) -> Optional[pgast.OutputVar]:
    result = _maybe_get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    if result is None:
        return None
    else:
        try:
            return pathctx.get_rvar_path_var(
                result[0], result[1], aspect=aspect, env=ctx.env)
        except LookupError:
            return None


def new_empty_rvar(
        ir_set: irast.EmptySet, *,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    nullrel = pgast.NullRelation(path_id=ir_set.path_id)
    rvar = rvar_for_rel(nullrel, ctx=ctx)
    pathctx.put_rvar_path_bond(rvar, ir_set.path_id)
    return rvar


def new_primitive_rvar(
    ir_set: irast.Set,
    *,
    path_id: irast.PathId,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    if not ir_set.path_id.is_objtype_path():
        raise ValueError('cannot create root rvar for non-object path')

    typeref = ir_set.typeref
    dml_source = irutils.get_nearest_dml_stmt(ir_set)
    set_rvar = range_for_typeref(
        typeref, path_id, dml_source=dml_source, ctx=ctx)
    pathctx.put_rvar_path_bond(set_rvar, path_id)

    if ir_set.rptr is not None:
        ptr_ref_map: Dict[uuid.UUID, irast.BasePointerRef] = {}
        p: irast.BasePointerRef

        rptrref = ir_set.rptr.ptrref
        if isinstance(rptrref, irast.TypeIntersectionPointerRef):
            if rptrref.rptr_specialization:
                for p in rptrref.rptr_specialization:
                    ptr_ref_map[p.dir_target.id] = p

            src_set = ir_set.rptr.source
            if src_set.rptr is not None:
                src_rptrref = src_set.rptr.ptrref
                if src_rptrref.union_components:
                    for p in src_rptrref.union_components:
                        ptr_ref_map[p.dir_target.id] = p
                else:
                    ptr_ref_map[src_rptrref.dir_target.id] = src_rptrref
                rptrref = src_rptrref
            else:
                ptr_ref_map[rptrref.dir_target.id] = rptrref
        else:
            if rptrref.union_components:
                for p in rptrref.union_components:
                    ptr_ref_map[p.dir_target.id] = p
            else:
                ptr_ref_map[rptrref.dir_target.id] = rptrref

        if (
            set_rvar.typeref is not None
            and (narrow_rptrref := ptr_ref_map.get(set_rvar.typeref.id))
        ):
            rptrref = narrow_rptrref

        ptr_info = pg_types.get_ptrref_storage_info(
            rptrref, resolve_type=False, link_bias=False)

        if ptr_info.table_type == 'ObjectType' and rptrref.is_inbound:
            # Inline link
            prefix_path_id = path_id.src_path()
            assert prefix_path_id is not None, 'expected a path'
            rref = pgast.ColumnRef(
                name=[ptr_info.column_name],
                nullable=not rptrref.required)
            pathctx.put_rvar_path_bond(set_rvar, prefix_path_id)
            pathctx.put_rvar_path_output(
                set_rvar, prefix_path_id,
                aspect='identity', var=rref, env=ctx.env)

            if astutils.is_set_op_query(set_rvar.query):
                assert isinstance(set_rvar.query, pgast.SelectStmt)

                def _pull_col(comp_qry: pgast.Query) -> None:
                    rvar = pathctx.get_path_rvar(
                        comp_qry, path_id, aspect='source', env=ctx.env)
                    typeref = rvar.typeref
                    assert typeref is not None
                    comp_ptrref = ptr_ref_map[typeref.id]
                    comp_pi = pg_types.get_ptrref_storage_info(
                        comp_ptrref, resolve_type=False, link_bias=False)

                    comp_qry.target_list.append(
                        pgast.ResTarget(
                            val=pgast.ColumnRef(name=[comp_pi.column_name]),
                            name=ptr_info.column_name,
                        )
                    )

                astutils.for_each_query_in_set(
                    set_rvar.query,
                    _pull_col,
                )
            elif isinstance(set_rvar, pgast.RangeSubselect):
                rvar_path_var = pathctx.maybe_get_path_rvar(
                    set_rvar.query,
                    path_id=path_id,
                    aspect='identity',
                    env=ctx.env,
                )

                if isinstance(rvar_path_var, pgast.IntersectionRangeVar):
                    for comp_rvar in rvar_path_var.component_rvars:
                        if comp_rvar.typeref is None:
                            continue
                        comp_ptrref = ptr_ref_map.get(comp_rvar.typeref.id)
                        if comp_ptrref is None:
                            continue
                        comp_pi = pg_types.get_ptrref_storage_info(
                            comp_ptrref, resolve_type=False)

                        set_rvar.query.target_list.append(
                            pgast.ResTarget(
                                val=pgast.ColumnRef(
                                    name=[
                                        comp_rvar.alias.aliasname,
                                        comp_pi.column_name,
                                    ]
                                ),
                                name=ptr_info.column_name,
                            )
                        )

    return set_rvar


def new_root_rvar(
    ir_set: irast.Set,
    *,
    path_id: Optional[irast.PathId] = None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    if path_id is None:
        path_id = ir_set.path_id

    narrowing = ctx.intersection_narrowing.get(ir_set)
    if narrowing is not None:
        return new_primitive_rvar(narrowing, path_id=path_id, ctx=ctx)
    else:
        return new_primitive_rvar(ir_set, path_id=path_id, ctx=ctx)


def new_pointer_rvar(
        ir_ptr: irast.Pointer, *,
        link_bias: bool=False,
        src_rvar: pgast.PathRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:

    ptrref = ir_ptr.ptrref

    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=link_bias)

    if ptr_info.table_type == 'ObjectType':
        # Inline link
        return _new_inline_pointer_rvar(
            ir_ptr, ptr_info=ptr_info,
            src_rvar=src_rvar, ctx=ctx)
    else:
        return _new_mapped_pointer_rvar(ir_ptr, ctx=ctx)


def _new_inline_pointer_rvar(
        ir_ptr: irast.Pointer, *,
        lateral: bool=True,
        ptr_info: pg_types.PointerStorageInfo,
        src_rvar: pgast.PathRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    ptr_rel = pgast.SelectStmt()
    ptr_rvar = rvar_for_rel(ptr_rel, lateral=lateral, ctx=ctx)
    ptr_rvar.query.path_id = ir_ptr.target.path_id.ptr_path()

    is_inbound = ir_ptr.direction == s_pointers.PointerDirection.Inbound

    if is_inbound:
        far_pid = ir_ptr.source.path_id
    else:
        far_pid = ir_ptr.target.path_id

    far_ref = pathctx.get_rvar_path_identity_var(
        src_rvar, far_pid, env=ctx.env)

    pathctx.put_rvar_path_bond(ptr_rvar, far_pid)
    pathctx.put_path_identity_var(ptr_rel, far_pid, var=far_ref, env=ctx.env)

    return ptr_rvar


def _new_mapped_pointer_rvar(
        ir_ptr: irast.Pointer, *,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    ptrref = ir_ptr.ptrref
    dml_source = irutils.get_nearest_dml_stmt(ir_ptr.source)
    ptr_rvar = range_for_pointer(ir_ptr, dml_source=dml_source, ctx=ctx)
    src_col = 'source'

    source_ref = pgast.ColumnRef(name=[src_col], nullable=False)

    if (irtyputils.is_object(ptrref.out_target)
            and not irtyputils.is_computable_ptrref(ptrref)):
        tgt_ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, link_bias=True, resolve_type=False)
        tgt_col = tgt_ptr_info.column_name
    else:
        tgt_col = 'target'

    target_ref = pgast.ColumnRef(
        name=[tgt_col],
        nullable=not ptrref.required)

    # Set up references according to the link direction.
    if ir_ptr.direction == s_pointers.PointerDirection.Inbound:
        near_ref = target_ref
        far_ref = source_ref
    else:
        near_ref = source_ref
        far_ref = target_ref

    src_pid = ir_ptr.source.path_id
    tgt_pid = ir_ptr.target.path_id
    ptr_pid = tgt_pid.ptr_path()

    ptr_rvar.query.path_id = ptr_pid
    pathctx.put_rvar_path_bond(ptr_rvar, src_pid)
    pathctx.put_rvar_path_output(ptr_rvar, src_pid, aspect='identity',
                                 var=near_ref, env=ctx.env)
    pathctx.put_rvar_path_output(ptr_rvar, src_pid, aspect='value',
                                 var=near_ref, env=ctx.env)
    pathctx.put_rvar_path_output(ptr_rvar, tgt_pid, aspect='value',
                                 var=far_ref, env=ctx.env)

    if tgt_pid.is_objtype_path():
        pathctx.put_rvar_path_bond(ptr_rvar, tgt_pid)
        pathctx.put_rvar_path_output(ptr_rvar, tgt_pid, aspect='identity',
                                     var=far_ref, env=ctx.env)

    return ptr_rvar


def new_rel_rvar(
        ir_set: irast.Set, stmt: pgast.Query, *,
        lateral: bool=True,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    if irutils.is_scalar_view_set(ir_set):
        ensure_bond_for_expr(ir_set, stmt, ctx=ctx)

    return rvar_for_rel(stmt, typeref=ir_set.typeref, lateral=lateral, ctx=ctx)


def semi_join(
        stmt: pgast.SelectStmt,
        ir_set: irast.Set, src_rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Join an IR Set using semi-join."""
    rptr = ir_set.rptr
    assert rptr is not None

    # Target set range.
    set_rvar = new_root_rvar(ir_set, ctx=ctx)

    ptrref = rptr.ptrref
    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False)

    if ptr_info.table_type == 'ObjectType':
        if irtyputils.is_inbound_ptrref(ptrref):
            far_pid = ir_set.path_id.src_path()
            assert far_pid is not None
        else:
            far_pid = ir_set.path_id
    else:
        far_pid = ir_set.path_id
        # Link range.
        map_rvar = new_pointer_rvar(rptr, src_rvar=src_rvar, ctx=ctx)
        include_rvar(
            ctx.rel, map_rvar,
            path_id=ir_set.path_id.ptr_path(), ctx=ctx)

    tgt_ref = pathctx.get_rvar_path_identity_var(
        set_rvar, far_pid, env=ctx.env)

    pathctx.get_path_identity_output(
        ctx.rel, far_pid, env=ctx.env)

    cond = astutils.new_binop(tgt_ref, ctx.rel, 'IN')
    stmt.where_clause = astutils.extend_binop(
        stmt.where_clause, cond)

    return set_rvar


def ensure_bond_for_expr(
        ir_set: irast.Set, stmt: pgast.BaseRelation, *, type: str='int',
        ctx: context.CompilerContextLevel) -> None:
    if ir_set.path_id.is_objtype_path():
        # ObjectTypes have inherent identity
        return

    ensure_transient_identity_for_path(
        ir_set.path_id, stmt, type=type, ctx=ctx)


def apply_volatility_ref(
        stmt: pgast.SelectStmt, *,
        ctx: context.CompilerContextLevel) -> None:
    for ref in ctx.volatility_ref:
        # Apply the volatility reference.
        # See the comment in process_set_as_subquery().
        stmt.where_clause = astutils.extend_binop(
            stmt.where_clause,
            pgast.NullTest(
                arg=ref(),
                negated=True,
            )
        )


def ensure_transient_identity_for_path(
        path_id: irast.PathId, stmt: pgast.BaseRelation, *,
        ctx: context.CompilerContextLevel, type: str='int') -> None:

    if type == 'uuid':
        id_expr = pgast.FuncCall(
            name=('edgedbext', 'uuid_generate_v1mc',),
            args=[],
        )
    else:
        id_expr = pgast.FuncCall(
            name=('row_number',),
            args=[],
            over=pgast.WindowDef()
        )

    pathctx.put_path_identity_var(stmt, path_id,
                                  id_expr, force=True, env=ctx.env)
    pathctx.put_path_bond(stmt, path_id)

    if isinstance(stmt, pgast.SelectStmt):
        apply_volatility_ref(stmt, ctx=ctx)


def get_scope(
    ir_set: irast.Set, *,
    ctx: context.CompilerContextLevel,
) -> Optional[irast.ScopeTreeNode]:

    result: Optional[irast.ScopeTreeNode] = None

    if ir_set.path_scope_id is not None:
        result = ctx.scope_tree.root.find_by_unique_id(ir_set.path_scope_id)

    return result


def update_scope(
        ir_set: irast.Set, stmt: pgast.SelectStmt, *,
        ctx: context.CompilerContextLevel) -> None:

    scope_tree = get_scope(ir_set, ctx=ctx)
    if scope_tree is None:
        return

    ctx.scope_tree = scope_tree
    ctx.path_scope = ctx.path_scope.new_child()

    for p in scope_tree.path_children:
        assert p.path_id is not None
        ctx.path_scope[p.path_id] = stmt

    if isinstance(ir_set.expr, irast.Stmt):
        iterators = irutils.get_iterator_sets(ir_set.expr)
        iter_paths = {it.path_id for it in iterators}
    else:
        iter_paths = set()

    for child_path in scope_tree.get_all_paths():
        parent_scope = scope_tree.parent
        if ((parent_scope is None or
                not parent_scope.is_visible(child_path)) and
                child_path not in iter_paths):
            stmt.path_id_mask.add(child_path)


def maybe_get_scope_stmt(
    path_id: irast.PathId,
    *,
    ctx: context.CompilerContextLevel,
) -> Optional[pgast.SelectStmt]:
    stmt = ctx.path_scope.get(path_id)
    if stmt is None and path_id.is_ptr_path():
        stmt = ctx.path_scope.get(path_id.tgt_path())
    return stmt


def get_scope_stmt(
    path_id: irast.PathId,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.SelectStmt:
    stmt = maybe_get_scope_stmt(path_id, ctx=ctx)
    if stmt is None:
        raise LookupError(f'cannot find scope statement for {path_id}')
    else:
        return stmt


def rel_join(
        query: pgast.SelectStmt, right_rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> None:
    condition = None

    for path_id in right_rvar.query.path_scope:
        lref = maybe_get_path_var(query, path_id, aspect='identity', ctx=ctx)
        if lref is None:
            lref = maybe_get_path_var(query, path_id, aspect='value', ctx=ctx)
        if lref is None:
            continue

        rref = pathctx.get_rvar_path_identity_var(
            right_rvar, path_id, env=ctx.env)

        assert isinstance(lref, pgast.ColumnRef)
        assert isinstance(rref, pgast.ColumnRef)
        path_cond = astutils.join_condition(lref, rref)
        condition = astutils.extend_binop(condition, path_cond)

    if condition is None:
        join_type = 'cross'
    else:
        join_type = 'inner'

    if not query.from_clause:
        query.from_clause.append(right_rvar)
        if condition is not None:
            query.where_clause = astutils.extend_binop(
                query.where_clause, condition)
    else:
        larg = query.from_clause[0]
        rarg = right_rvar

        query.from_clause[0] = pgast.JoinExpr(
            type=join_type, larg=larg, rarg=rarg, quals=condition)


def range_for_material_objtype(
    typeref: irast.TypeRef,
    path_id: irast.PathId,
    *,
    for_mutation: bool=False,
    include_overlays: bool=True,
    include_descendants: bool=True,
    dml_source: Optional[irast.MutatingStmt]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    env = ctx.env

    if typeref.material_type is not None:
        typeref = typeref.material_type

    relation: Union[pgast.Relation, pgast.CommonTableExpr]

    if (
        (rewrite := ctx.env.type_rewrites.get(typeref.id)) is not None
        and typeref.id not in ctx.pending_type_ctes
        and not for_mutation
    ):

        if (type_cte := ctx.type_ctes.get(typeref.id)) is None:
            with ctx.newrel() as sctx:
                sctx.pending_type_ctes.add(typeref.id)
                sctx.pending_query = sctx.rel
                dispatch.visit(rewrite, ctx=sctx)
                type_cte = pgast.CommonTableExpr(
                    name=ctx.env.aliases.get('t'),
                    query=sctx.rel,
                    materialized=False,
                )
                ctx.type_ctes[typeref.id] = type_cte

        with ctx.subrel() as sctx:
            cte_rvar = pgast.RelRangeVar(
                relation=type_cte,
                typeref=typeref,
                alias=pgast.Alias(aliasname=env.aliases.get('t'))
            )
            pathctx.put_path_id_map(sctx.rel, path_id, rewrite.path_id)
            include_rvar(sctx.rel, cte_rvar, rewrite.path_id, ctx=sctx)
            rvar = rvar_for_rel(sctx.rel, typeref=typeref, ctx=sctx)
    else:
        assert isinstance(typeref.name_hint, sn.QualName)

        table_schema_name, table_name = common.get_objtype_backend_name(
            typeref.id,
            typeref.name_hint.module,
            aspect=(
                'table' if for_mutation or not include_descendants else
                'inhview'
            ),
            catenate=False,
        )

        if typeref.name_hint.module in {'cfg', 'sys'}:
            # Redirect all queries to schema tables to edgedbss
            table_schema_name = 'edgedbss'

        relation = pgast.Relation(
            schemaname=table_schema_name,
            name=table_name,
            path_id=path_id,
        )

        rvar = pgast.RelRangeVar(
            relation=relation,
            typeref=typeref,
            include_inherited=include_descendants,
            alias=pgast.Alias(
                aliasname=env.aliases.get(typeref.name_hint.name)
            )
        )

    overlays = get_type_rel_overlays(typeref, dml_source=dml_source, ctx=ctx)
    external_rvar = ctx.env.external_rvars.get((path_id, 'source'))
    if external_rvar is not None:
        if overlays:
            raise AssertionError('cannot mix external and internal overlays')
        return external_rvar

    if overlays and include_overlays:
        set_ops = []

        qry = pgast.SelectStmt()
        qry.from_clause.append(rvar)
        pathctx.put_path_value_rvar(qry, path_id, rvar, env=env)
        if path_id.is_objtype_path():
            pathctx.put_path_source_rvar(qry, path_id, rvar, env=env)
        pathctx.put_path_bond(qry, path_id)

        set_ops.append(('union', qry))

        for op, cte, cte_path_id in overlays:
            rvar = pgast.RelRangeVar(
                relation=cte,
                typeref=typeref,
                alias=pgast.Alias(
                    aliasname=env.aliases.get(hint=cte.name)
                )
            )

            qry = pgast.SelectStmt(
                from_clause=[rvar],
            )

            pathctx.put_path_value_rvar(qry, cte_path_id, rvar, env=env)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry, cte_path_id, rvar, env=env)
            pathctx.put_path_bond(qry, cte_path_id)
            pathctx.put_path_id_map(qry, path_id, cte_path_id)

            qry_rvar = pgast.RangeSubselect(
                subquery=qry,
                alias=pgast.Alias(
                    aliasname=env.aliases.get(hint=cte.name)
                )
            )

            qry2 = pgast.SelectStmt(
                from_clause=[qry_rvar]
            )
            pathctx.put_path_value_rvar(qry2, path_id, qry_rvar, env=env)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry2, path_id, qry_rvar, env=env)
            pathctx.put_path_bond(qry2, path_id)

            if op == 'replace':
                op = 'union'
                set_ops = []
            set_ops.append((op, qry2))

        rvar = range_from_queryset(
            set_ops, typeref.name_hint, path_id=path_id, ctx=ctx)

    return rvar


def range_for_typeref(
    typeref: irast.TypeRef,
    path_id: irast.PathId,
    *,
    for_mutation: bool=False,
    include_descendants: bool=True,
    dml_source: Optional[irast.MutatingStmt]=None,
    common_parent: bool=False,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    if typeref.common_parent is not None and common_parent:
        rvar = range_for_material_objtype(
            typeref.common_parent,
            path_id,
            include_descendants=include_descendants,
            for_mutation=for_mutation,
            dml_source=dml_source,
            ctx=ctx,
        )

    elif typeref.union:
        # Union object types are represented as a UNION of selects
        # from their children, which is, for most purposes, equivalent
        # to SELECTing from a parent table.
        set_ops = []

        for child in typeref.union:
            c_rvar = range_for_typeref(
                child,
                path_id=path_id,
                include_descendants=not typeref.union_is_concrete,
                for_mutation=for_mutation,
                dml_source=dml_source,
                ctx=ctx,
            )

            qry = pgast.SelectStmt(
                from_clause=[c_rvar],
            )

            pathctx.put_path_value_rvar(qry, path_id, c_rvar, env=ctx.env)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry, path_id, c_rvar, env=ctx.env)

            pathctx.put_path_bond(qry, path_id)

            set_ops.append(('union', qry))

        rvar = range_from_queryset(set_ops, typeref.name_hint, ctx=ctx)

    elif typeref.intersection:
        wrapper = pgast.SelectStmt()
        component_rvars = []
        for component in typeref.intersection:
            component_rvar = range_for_typeref(
                component,
                path_id=path_id,
                for_mutation=for_mutation,
                dml_source=dml_source,
                ctx=ctx,
            )
            pathctx.put_rvar_path_bond(component_rvar, path_id)
            component_rvars.append(component_rvar)
            include_rvar(wrapper, component_rvar, path_id, ctx=ctx)

        int_rvar = pgast.IntersectionRangeVar(component_rvars=component_rvars)
        for aspect in ('source', 'value'):
            pathctx.put_path_rvar(
                wrapper, path_id, int_rvar, aspect=aspect, env=ctx.env
            )

        pathctx.put_path_bond(wrapper, path_id)
        rvar = rvar_for_rel(wrapper, ctx=ctx)

    else:
        rvar = range_for_material_objtype(
            typeref,
            path_id,
            include_descendants=include_descendants,
            for_mutation=for_mutation,
            dml_source=dml_source,
            ctx=ctx,
        )

    rvar.query.path_id = path_id

    return rvar


def wrap_set_op_query(
    qry: pgast.SelectStmt, *,
    ctx: context.CompilerContextLevel
) -> pgast.SelectStmt:
    if astutils.is_set_op_query(qry):
        rvar = rvar_for_rel(qry, ctx=ctx)
        qry = pgast.SelectStmt(from_clause=[rvar])
        pull_path_namespace(target=qry, source=rvar, ctx=ctx)
    return qry


def anti_join(
    lhs: pgast.SelectStmt, rhs: pgast.SelectStmt,
    path_id: Optional[irast.PathId], *,
    ctx: context.CompilerContextLevel,
) -> None:
    """Filter elements out of the LHS that appear on the RHS"""

    if path_id:
        # grab the identity from the LHS and do an
        # anti-join against the RHS.
        src_ref = pathctx.get_path_identity_var(
            lhs, path_id=path_id, env=ctx.env)
        pathctx.get_path_identity_output(
            rhs, path_id=path_id, env=ctx.env)
        cond_expr: pgast.BaseExpr = astutils.new_binop(
            src_ref, rhs, 'NOT IN')
    else:
        # No path we care about. Just check existance.
        cond_expr = pgast.SubLink(
            type=pgast.SubLinkType.NOT_EXISTS, expr=rhs)
    lhs.where_clause = astutils.extend_binop(
        lhs.where_clause, cond_expr)


def range_from_queryset(
    set_ops: Sequence[Tuple[str, pgast.SelectStmt]],
    objname: sn.Name,
    *,
    path_id: Optional[irast.PathId]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    rvar: pgast.PathRangeVar

    if len(set_ops) > 1:
        # More than one class table, generate a UNION/EXCEPT clause.
        qry = set_ops[0][1]

        for op, rarg in set_ops[1:]:
            if op == 'filter':
                qry = wrap_set_op_query(qry, ctx=ctx)
                anti_join(qry, rarg, path_id, ctx=ctx)
            else:
                qry = pgast.SelectStmt(
                    op=op,
                    all=True,
                    larg=qry,
                    rarg=rarg,
                )

        rvar = pgast.RangeSubselect(
            subquery=qry,
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get(objname.name),
            )
        )

    else:
        # Just one class table, so return it directly
        from_rvar = set_ops[0][1].from_clause[0]
        assert isinstance(from_rvar, pgast.PathRangeVar)
        rvar = from_rvar

    return rvar


def table_from_ptrref(
    ptrref: irast.PointerRef,
    *,
    include_descendants: bool = True,
    for_mutation: bool = False,
    ctx: context.CompilerContextLevel,
) -> pgast.RelRangeVar:
    """Return a Table corresponding to a given Link."""
    table_schema_name, table_name = common.get_pointer_backend_name(
        ptrref.id,
        ptrref.name.module,
        aspect=(
            'table' if for_mutation or not include_descendants else 'inhview'
        ),
        catenate=False,
    )

    if ptrref.name.module in {'cfg', 'sys'}:
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name, name=table_name)

    # Pseudo pointers (tuple and type intersection) have no schema id.
    sobj_id = ptrref.id if isinstance(ptrref, irast.PointerRef) else None
    rvar = pgast.RelRangeVar(
        schema_object_id=sobj_id,
        relation=relation,
        include_inherited=include_descendants,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get(ptrref.shortname.name)
        )
    )

    return rvar


def range_for_ptrref(
    ptrref: irast.BasePointerRef, *,
    dml_source: Optional[irast.MutatingStmt]=None,
    for_mutation: bool=False,
    only_self: bool=False,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    """"Return a Range subclass corresponding to a given ptr step.

    The return value may potentially be a UNION of all tables
    corresponding to a set of specialized links computed from the given
    `ptrref` taking source inheritance into account.
    """
    tgt_col = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=True).column_name

    cols = [
        'source',
        tgt_col
    ]

    set_ops = []

    if ptrref.union_components:
        refs = ptrref.union_components
        if only_self and len(refs) > 1:
            raise errors.InternalServerError(
                'unexpected union link'
            )
    else:
        refs = {ptrref}
        assert isinstance(ptrref, irast.PointerRef), \
            "expected regular PointerRef"
        overlays = get_ptr_rel_overlays(
            ptrref, dml_source=dml_source, ctx=ctx)

    for src_ptrref in refs:
        assert isinstance(src_ptrref, irast.PointerRef), \
            "expected regular PointerRef"
        table = table_from_ptrref(
            src_ptrref,
            include_descendants=not ptrref.union_is_concrete,
            for_mutation=for_mutation,
            ctx=ctx,
        )

        qry = pgast.SelectStmt()
        qry.from_clause.append(table)

        # Make sure all property references are pulled up properly
        for colname in cols:
            selexpr = pgast.ColumnRef(
                name=[table.alias.aliasname, colname])
            qry.target_list.append(
                pgast.ResTarget(val=selexpr, name=colname))

        set_ops.append(('union', qry))

        overlays = get_ptr_rel_overlays(
            src_ptrref, dml_source=dml_source, ctx=ctx)
        if overlays and not for_mutation:
            for op, cte in overlays:
                rvar = pgast.RelRangeVar(
                    relation=cte,
                    alias=pgast.Alias(
                        aliasname=ctx.env.aliases.get(cte.name)
                    )
                )

                qry = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            val=pgast.ColumnRef(
                                name=[col]
                            )
                        )
                        for col in cols
                    ],
                    from_clause=[rvar],
                )
                set_ops.append((op, qry))

    return range_from_queryset(set_ops, ptrref.shortname, ctx=ctx)


def range_for_pointer(
    pointer: irast.Pointer,
    *,
    dml_source: Optional[irast.MutatingStmt] = None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    path_id = pointer.target.path_id.ptr_path()
    external_rvar = ctx.env.external_rvars.get((path_id, 'source'))
    if external_rvar is not None:
        return external_rvar

    ptrref = pointer.ptrref
    if ptrref.material_ptr is not None:
        ptrref = ptrref.material_ptr

    return range_for_ptrref(ptrref, dml_source=dml_source, ctx=ctx)


def rvar_for_rel(
    rel: Union[pgast.BaseRelation, pgast.CommonTableExpr],
    *,
    typeref: Optional[irast.TypeRef] = None,
    lateral: bool = False,
    colnames: Optional[List[str]] = None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    rvar: pgast.PathRangeVar

    if colnames is None:
        colnames = []

    if isinstance(rel, pgast.Query):
        alias = ctx.env.aliases.get(rel.name or 'q')

        rvar = pgast.RangeSubselect(
            subquery=rel,
            alias=pgast.Alias(aliasname=alias, colnames=colnames),
            lateral=lateral,
            typeref=typeref,
        )
    else:
        alias = ctx.env.aliases.get(rel.name)

        rvar = pgast.RelRangeVar(
            relation=rel,
            alias=pgast.Alias(aliasname=alias, colnames=colnames),
            typeref=typeref,
        )

    return rvar


def _add_type_rel_overlay(
        typeid: uuid.UUID,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:
    entry = (op, rel, path_id)
    if dml_stmts:
        for dml_stmt in dml_stmts:
            overlays = ctx.type_rel_overlays[dml_stmt][typeid]
            if entry not in overlays:
                overlays.append(entry)
    else:
        overlays = ctx.type_rel_overlays[None][typeid]
        if entry not in overlays:
            overlays.append(entry)


def add_type_rel_overlay(
        typeref: irast.TypeRef,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:
    if typeref.material_type is not None:
        typeref = typeref.material_type

    objs = [typeref]
    if typeref.ancestors:
        objs.extend(typeref.ancestors)
    for obj in objs:
        _add_type_rel_overlay(
            obj.id, op, rel,
            dml_stmts=dml_stmts, path_id=path_id, ctx=ctx)


def get_type_rel_overlays(
    typeref: irast.TypeRef,
    *,
    dml_source: Optional[irast.MutatingStmt]=None,
    ctx: context.CompilerContextLevel,
) -> List[
    Tuple[
        str,
        Union[pgast.BaseRelation, pgast.CommonTableExpr],
        irast.PathId,
    ]
]:
    if typeref.material_type is not None:
        typeref = typeref.material_type

    return ctx.type_rel_overlays[dml_source][typeref.id]


def reuse_type_rel_overlays(
    *,
    dml_stmts: Iterable[irast.MutatingStmt] = (),
    dml_source: irast.MutatingStmt,
    ctx: context.CompilerContextLevel,
) -> None:
    """Update type rel overlays when a DML statement is reused.

    When a WITH bound DML is used, we need to add it (and all of its
    nested overlays) as an overlay for all the enclosing DML
    statements.
    """
    ref_overlays = ctx.type_rel_overlays[dml_source]
    for tid, overlays in ref_overlays.items():
        for op, rel, path_id in overlays:
            _add_type_rel_overlay(
                tid, op, rel, dml_stmts=dml_stmts, path_id=path_id, ctx=ctx
            )
    ptr_overlays = ctx.ptr_rel_overlays[dml_source]
    for ptr_name, poverlays in ptr_overlays.items():
        for op, rel in poverlays:
            _add_ptr_rel_overlay(
                ptr_name, op, rel, dml_stmts=dml_stmts, ctx=ctx
            )


def _add_ptr_rel_overlay(
        ptrref_name: str,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        ctx: context.CompilerContextLevel) -> None:

    if dml_stmts:
        for dml_stmt in dml_stmts:
            overlays = ctx.ptr_rel_overlays[dml_stmt][ptrref_name]
            overlays.append((op, rel))
    else:
        overlays = ctx.ptr_rel_overlays[None][ptrref_name]
        overlays.append((op, rel))


def add_ptr_rel_overlay(
        ptrref: irast.PointerRef,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        ctx: context.CompilerContextLevel) -> None:
    _add_ptr_rel_overlay(
        ptrref.shortname.name, op, rel, dml_stmts=dml_stmts, ctx=ctx)


def get_ptr_rel_overlays(
    ptrref: irast.PointerRef, *,
    dml_source: Optional[irast.MutatingStmt]=None,
    ctx: context.CompilerContextLevel,
) -> List[
    Tuple[
        str,
        Union[pgast.BaseRelation, pgast.CommonTableExpr],
    ]
]:
    return ctx.ptr_rel_overlays[dml_source][ptrref.shortname.name]
