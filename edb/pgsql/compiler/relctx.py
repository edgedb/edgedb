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

from edb.edgeql import qltypes
from edb.edgeql import ast as qlast

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
from . import output
from . import pathctx


def init_toplevel_query(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    ctx.toplevel_stmt = ctx.stmt = ctx.rel
    update_scope(ir_set, ctx.rel, ctx=ctx)
    ctx.pending_query = ctx.rel


def _pull_path_namespace(
        *, target: pgast.Query, source: pgast.PathRangeVar,
        flavor: str='normal',
        replace_bonds: bool=True, ctx: context.CompilerContextLevel) -> None:

    squery = source.query
    source_qs: List[pgast.BaseRelation]

    if astutils.is_set_op_query(squery):
        # Set op query
        assert squery.larg and squery.rarg
        source_qs = [squery, squery.larg, squery.rarg]
    else:
        source_qs = [squery]

    for source_q in source_qs:
        s_paths: Set[Tuple[irast.PathId, str]] = set()
        if flavor == 'normal':
            if hasattr(source_q, 'path_outputs'):
                s_paths.update(source_q.path_outputs)
            if hasattr(source_q, 'path_namespace'):
                s_paths.update(source_q.path_namespace)
            if isinstance(source_q, pgast.Query):
                s_paths.update(source_q.path_rvar_map)
        elif flavor == 'packed':
            if hasattr(source_q, 'packed_path_outputs'):
                if source_q.packed_path_outputs:
                    s_paths.update(source_q.packed_path_outputs)
            if isinstance(source_q, pgast.Query):
                if source_q.path_packed_rvar_map:
                    s_paths.update(source_q.path_packed_rvar_map)
        else:
            raise AssertionError(f'unexpected flavor "{flavor}"')

        view_path_id_map = getattr(source_q, 'view_path_id_map', {})

        for path_id, aspect in s_paths:
            orig_path_id = path_id
            if flavor != 'packed':
                path_id = pathctx.reverse_map_path_id(
                    path_id, view_path_id_map)

            # Skip pulling paths that match the path_id_mask before or after
            # doing path id mapping. We need to look at before as well
            # to prevent paths leaking out under a different name.
            if flavor != 'packed' and (
                path_id in squery.path_id_mask
                or orig_path_id in squery.path_id_mask
            ):
                continue

            rvar = pathctx.maybe_get_path_rvar(
                target, path_id, aspect=aspect, flavor=flavor, env=ctx.env)
            if rvar is None:
                pathctx.put_path_rvar(
                    target, path_id, source, aspect=aspect, flavor=flavor,
                    env=ctx.env)


def pull_path_namespace(
        *, target: pgast.Query, source: pgast.PathRangeVar,
        replace_bonds: bool=True, ctx: context.CompilerContextLevel) -> None:
    for flavor in ('normal', 'packed'):
        _pull_path_namespace(target=target, source=source, flavor=flavor,
                             replace_bonds=replace_bonds, ctx=ctx)


def find_rvar(
        stmt: pgast.Query, *,
        flavor: str='normal',
        source_stmt: Optional[pgast.Query]=None,
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> \
        Optional[pgast.PathRangeVar]:
    """Find an existing range var for a given *path_id* in stmt hierarchy.

    If a range var is visible in a given SQL scope denoted by *stmt*, or,
    optionally, *source_stmt*, record it on *stmt* for future reference.

    :param stmt:
        The statement to ensure range var visibility in.

    :param flavor:
        Whether to look for normal rvars or packed rvars

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
                               aspect='value', flavor=flavor, ctx=ctx)
    if rvar is not None:
        pathctx.put_path_rvar_if_not_exists(
            stmt, path_id, rvar, aspect='value', flavor=flavor, env=ctx.env)

        src_rvar = maybe_get_path_rvar(source_stmt, path_id=path_id,
                                       aspect='source', flavor=flavor, ctx=ctx)

        if src_rvar is not None:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, src_rvar,
                aspect='source', flavor=flavor, env=ctx.env)

    return rvar


def include_rvar(
        stmt: pgast.SelectStmt,
        rvar: pgast.PathRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        pull_namespace: bool=True,
        update_mask: bool=True,
        flavor: str='normal',
        aspects: Optional[Tuple[str, ...] | AbstractSet[str]]=None,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Ensure that *rvar* is visible in *stmt* as a value/source aspect.

    :param stmt:
        The statement to include *rel* in.

    :param rvar:
        The range var node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param flavor:
        Whether this is a normal or packed rvar

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
        update_mask=update_mask,
        flavor=flavor,
        aspects=aspects,
        ctx=ctx)


def include_specific_rvar(
        stmt: pgast.SelectStmt,
        rvar: pgast.PathRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        pull_namespace: bool=True,
        update_mask: bool=True,
        flavor: str='normal',
        aspects: Iterable[str]=('value',),
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Make the *aspect* of *path_id* visible in *stmt* as *rvar*.

    :param stmt:
        The statement to include *rel* in.

    :param rvar:
        The range var node to join.

    :param join_type:
        JOIN type to use when including *rel*.

    :param flavor:
        Whether this is a normal or packed rvar

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
                stmt, path_id, rvar, flavor=flavor, aspect=aspect, env=ctx.env)
        else:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, rvar, flavor=flavor, aspect=aspect, env=ctx.env)

    if update_mask:
        scopes = [ctx.scope_tree]
        parent_scope = ctx.scope_tree.parent
        if parent_scope is not None:
            scopes.append(parent_scope)

        if not any(scope.path_id == path_id or
                   scope.find_child(path_id) for scope in scopes):
            pathctx.put_path_id_mask(stmt, path_id)

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
    flavor: str='normal',
    aspect: str,
    ctx: context.CompilerContextLevel,
) -> Optional[Tuple[pgast.PathRangeVar, irast.PathId]]:
    qry: Optional[pgast.Query] = stmt
    while qry is not None:
        rvar = pathctx.maybe_get_path_rvar(
            qry, path_id, aspect=aspect, flavor=flavor, env=ctx.env)
        if rvar is not None:
            if qry is not stmt:
                # Cache the rvar reference.
                pathctx.put_path_rvar(stmt, path_id, rvar,
                                      flavor=flavor, aspect=aspect,
                                      env=ctx.env)
            return rvar, path_id
        qry = ctx.rel_hierarchy.get(qry)

    return None


def _get_path_rvar(
    stmt: pgast.Query,
    path_id: irast.PathId,
    *,
    flavor: str='normal',
    aspect: str,
    ctx: context.CompilerContextLevel,
) -> Tuple[pgast.PathRangeVar, irast.PathId]:
    result = _maybe_get_path_rvar(
        stmt, path_id, flavor=flavor, aspect=aspect, ctx=ctx)
    if result is None:
        raise LookupError(f'there is no range var for {path_id} in {stmt}')
    else:
        return result


def get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        flavor: str='normal',
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    return _get_path_rvar(
        stmt, path_id, flavor=flavor, aspect=aspect, ctx=ctx)[0]


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
        flavor: str='normal',
        aspect: str, ctx: context.CompilerContextLevel
) -> Optional[pgast.PathRangeVar]:
    result = _maybe_get_path_rvar(stmt, path_id,
                                  aspect=aspect, flavor=flavor, ctx=ctx)
    return result[0] if result is not None else None


def maybe_get_path_var(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel
) -> Optional[pgast.BaseExpr]:
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
    lateral: bool,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    if not ir_set.path_id.is_objtype_path():
        raise ValueError('cannot create root rvar for non-object path')

    typeref = ir_set.typeref
    dml_source = irutils.get_nearest_dml_stmt(ir_set)
    set_rvar = range_for_typeref(
        typeref, path_id, lateral=lateral, dml_source=dml_source, ctx=ctx)
    pathctx.put_rvar_path_bond(set_rvar, path_id)

    rptr = ir_set.rptr
    if rptr is not None:
        if (isinstance(rptr.ptrref, irast.TypeIntersectionPointerRef)
                and rptr.source.rptr):
            rptr = rptr.source.rptr

        # If the set comes from an backlink, and the link is stored inline,
        # we want to output the source path.
        if (
            rptr.is_inbound
            and (
                rptrref := irtyputils.maybe_find_actual_ptrref(
                    set_rvar.typeref, rptr.ptrref) or rptr.ptrref
                if set_rvar.typeref else rptr.ptrref
            ) and (
                ptr_info := pg_types.get_ptrref_storage_info(
                    rptrref, resolve_type=False, link_bias=False,
                    allow_missing=True)
            ) and ptr_info.table_type == 'ObjectType'
        ):
            # Inline link
            prefix_path_id = path_id.src_path()
            assert prefix_path_id is not None, 'expected a path'

            flipped_id = path_id.extend(ptrref=rptrref)
            rref = pathctx.get_path_output(
                set_rvar.query, flipped_id, aspect='identity', env=ctx.env)

            pathctx.put_rvar_path_bond(set_rvar, prefix_path_id)
            pathctx.put_rvar_path_output(
                set_rvar, prefix_path_id,
                aspect='identity', var=rref, env=ctx.env)

    return set_rvar


def new_root_rvar(
    ir_set: irast.Set,
    *,
    lateral: bool = False,
    path_id: Optional[irast.PathId] = None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    if path_id is None:
        path_id = ir_set.path_id

    narrowing = ctx.intersection_narrowing.get(ir_set)
    if narrowing is not None:
        ir_set = narrowing
    return new_primitive_rvar(
        ir_set, lateral=lateral, path_id=path_id, ctx=ctx)


def new_pointer_rvar(
        ir_ptr: irast.Pointer, *,
        link_bias: bool=False,
        src_rvar: pgast.PathRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:

    ptrref = ir_ptr.ptrref

    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=link_bias, allow_missing=True)

    if ptr_info and ptr_info.table_type == 'ObjectType':
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


def is_pointer_rvar(
    rvar: pgast.PathRangeVar,
    *,
    ctx: context.CompilerContextLevel,
) -> bool:
    return rvar.query.path_id is not None and rvar.query.path_id.is_ptr_path()


def new_rel_rvar(
        ir_set: irast.Set, stmt: pgast.Query, *,
        lateral: bool=True,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    return rvar_for_rel(stmt, typeref=ir_set.typeref, lateral=lateral, ctx=ctx)


def semi_join(
        stmt: pgast.SelectStmt,
        ir_set: irast.Set, src_rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Join an IR Set using semi-join."""
    rptr = ir_set.rptr
    assert rptr is not None

    # Target set range.
    set_rvar = new_root_rvar(ir_set, lateral=True, ctx=ctx)

    ptrref = rptr.ptrref
    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, allow_missing=True)

    if ptr_info and ptr_info.table_type == 'ObjectType':
        if rptr.is_inbound:
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
    ir_set: irast.Set,
    stmt: pgast.BaseRelation,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    if ir_set.path_id.is_objtype_path():
        # ObjectTypes have inherent identity
        return

    ensure_transient_identity_for_path(ir_set.path_id, stmt, ctx=ctx)


def apply_volatility_ref(
        stmt: pgast.SelectStmt, *,
        ctx: context.CompilerContextLevel) -> None:
    for ref in ctx.volatility_ref:
        # Apply the volatility reference.
        # See the comment in process_set_as_subquery().
        arg = ref(stmt, ctx)
        if not arg:
            continue
        stmt.where_clause = astutils.extend_binop(
            stmt.where_clause,
            pgast.NullTest(
                arg=arg,
                negated=True,
            )
        )


def ensure_transient_identity_for_path(
    path_id: irast.PathId,
    stmt: pgast.BaseRelation,
    *,
    ctx: context.CompilerContextLevel,
) -> None:

    id_expr = pgast.FuncCall(
        name=('edgedbext', 'uuid_generate_v4',),
        args=[],
    )

    pathctx.put_path_identity_var(
        stmt, path_id, id_expr, force=True, env=ctx.env)
    pathctx.put_path_bond(stmt, path_id)

    if isinstance(stmt, pgast.SelectStmt):
        apply_volatility_ref(stmt, ctx=ctx)


def get_scope(
    ir_set: irast.Set, *,
    ctx: context.CompilerContextLevel,
) -> Optional[irast.ScopeTreeNode]:

    result: Optional[irast.ScopeTreeNode] = None

    if ir_set.path_scope_id is not None:
        result = ctx.env.scope_tree_nodes.get(ir_set.path_scope_id)

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

    for child_path in scope_tree.get_all_paths():
        parent_scope = scope_tree.parent
        if (parent_scope is None or
                not parent_scope.is_visible(child_path)):
            pathctx.put_path_id_mask(stmt, child_path)


def maybe_get_scope_stmt(
    path_id: irast.PathId,
    *,
    ctx: context.CompilerContextLevel,
) -> Optional[pgast.SelectStmt]:
    stmt = ctx.path_scope.get(path_id)
    if stmt is None and path_id.is_ptr_path():
        stmt = ctx.path_scope.get(path_id.tgt_path())
    return stmt


def set_to_array(
        path_id: irast.PathId, query: pgast.Query, *,
        materializing: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    """Collapse a set into an array."""
    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('aggw')
        )
    )

    result = pgast.SelectStmt()
    include_rvar(result, subrvar, path_id=path_id, ctx=ctx)

    val: Optional[pgast.BaseExpr] = (
        pathctx.maybe_get_path_serialized_var(
            result, path_id, env=ctx.env)
    )

    if val is None:
        value_var = pathctx.get_path_value_var(
            result, path_id, env=ctx.env)
        val = output.serialize_expr(
            value_var, path_id=path_id, env=ctx.env)
        pathctx.put_path_serialized_var(
            result, path_id, val, force=True, env=ctx.env)

    if isinstance(val, pgast.TupleVarBase):
        val = output.serialize_expr(
            val, path_id=path_id, env=ctx.env)

    pg_type = output.get_pg_type(path_id.target, ctx=ctx)
    orig_val = val

    if (path_id.is_array_path()
            and ctx.env.output_format is context.OutputFormat.NATIVE):
        # We cannot aggregate arrays straight away, as
        # they be of different length, so we have to
        # encase each element into a record.
        val = pgast.RowExpr(args=[val], ser_safe=val.ser_safe)
        pg_type = ('record',)

    array_agg = pgast.FuncCall(
        name=('array_agg',),
        args=[val],
        agg_filter=(
            astutils.new_binop(orig_val, pgast.NullConstant(),
                               'IS DISTINCT FROM')
            if orig_val.nullable else None
        ),
        ser_safe=val.ser_safe,
    )

    agg_expr = pgast.CoalesceExpr(
        args=[
            array_agg,
            pgast.TypeCast(
                arg=pgast.ArrayExpr(elements=[]),
                type_name=pgast.TypeName(name=pg_type, array_bounds=[-1])
            )
        ],
        ser_safe=array_agg.ser_safe,
        nullable=False,
    )

    result.target_list = [
        pgast.ResTarget(
            name=ctx.env.aliases.get('v'),
            val=agg_expr,
            ser_safe=agg_expr.ser_safe,
        )
    ]

    return result


class UnpackElement(NamedTuple):
    path_id: irast.PathId
    colname: str
    packed: bool
    multi: bool
    ref: Optional[pgast.BaseExpr]


def unpack_rvar(
        stmt: pgast.SelectStmt, path_id: irast.PathId, *,
        packed_rvar: pgast.PathRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    ref = pathctx.get_rvar_path_var(
        packed_rvar, path_id, aspect='value', flavor='packed', env=ctx.env)
    return unpack_var(stmt, path_id, ref=ref, ctx=ctx)


def unpack_var(
        stmt: pgast.SelectStmt, path_id: irast.PathId, *,
        ref: pgast.OutputVar,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:

    qry = pgast.SelectStmt()

    view_tvars: List[Tuple[irast.PathId, pgast.TupleVarBase, bool]] = []
    els = []
    ctr = 0

    def walk(ref: pgast.BaseExpr, path_id: irast.PathId, multi: bool) -> None:
        nonlocal ctr

        coldeflist = []
        alias = ctx.env.aliases.get('unpack')
        simple = False
        if irtyputils.is_tuple(path_id.target):
            els.append(UnpackElement(
                path_id, alias, packed=False, multi=False, ref=None
            ))

            orig_view_count = len(view_tvars)
            tuple_tvar_elements = []
            for i, st in enumerate(path_id.target.subtypes):
                colname = f'_t{ctr}'
                ctr += 1

                typ = pg_types.pg_type_from_ir_typeref(st)
                if st.id in ctx.env.materialized_views:
                    typ = ('record',)

                # Construct a path_id for the element
                el_name = sn.QualName('__tuple__', st.element_name or str(i))
                el_ref = irast.TupleIndirectionPointerRef(
                    name=el_name, shortname=el_name, path_id_name=el_name,
                    out_source=path_id.target,
                    out_target=st,
                    out_cardinality=qltypes.Cardinality.ONE,
                )
                el_path_id = path_id.extend(ptrref=el_ref)

                el_var = (
                    astutils.tuple_getattr(
                        pgast.ColumnRef(name=[alias]),
                        path_id.target, el_name.name)
                    if irtyputils.is_persistent_tuple(path_id.target)
                    else pgast.ColumnRef(name=[colname])
                )
                walk(el_var, el_path_id, multi=False)

                tuple_tvar_elements.append(
                    pgast.TupleElementBase(
                        path_id=el_path_id, name=el_name.name
                    )
                )

                coldeflist.append(
                    pgast.ColumnDef(
                        name=colname,
                        typename=pgast.TypeName(name=typ)
                    )
                )

            if len(view_tvars) > orig_view_count:
                tuple_tvar = pgast.TupleVarBase(
                    elements=tuple_tvar_elements,
                    typeref=path_id.target,
                    named=any(
                        st.element_name for st in path_id.target.subtypes),
                )
                view_tvars.append((path_id, tuple_tvar, True))

            if irtyputils.is_persistent_tuple(path_id.target):
                coldeflist = []

        elif irtyputils.is_array(path_id.target) and multi:
            # TODO: materialized arrays of tuples and arrays are really
            # quite broken
            coldeflist = [
                pgast.ColumnDef(
                    name='q',
                    typename=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(
                            path_id.target)
                    )
                )
            ]

            els.append(UnpackElement(
                path_id, coldeflist[0].name,
                packed=False, multi=False, ref=None
            ))

        elif path_id.target.id in ctx.env.materialized_views:
            view_tuple = ctx.env.materialized_views[path_id.target.id]

            vpath_ids = []
            id_idx = None
            for el, _ in view_tuple.shape:
                src_path, el_ptrref = el.path_id.src_path(), el.path_id.rptr()
                assert src_path and el_ptrref

                # We want to graft the ptrref for this element onto
                # the main path_id. To get the right one (that will
                # match code that wants to consume this), we need to
                # find the ptrref that matches the path_id view type.
                ptrref = irtyputils.maybe_find_actual_ptrref(
                    path_id.target, el_ptrref, material=False)
                if not ptrref:
                    # A missing ptrref should mean that this computable isn't
                    # actually used, so we don't need to worry too hard.
                    ptrref = el_ptrref
                el_id = path_id.ptr_path().extend(ptrref=ptrref)

                assert el.rptr
                card = el.rptr.ptrref.dir_cardinality(el.rptr.direction)
                is_singleton = card.is_single() and not card.can_be_zero()
                must_pack = not is_singleton

                if (rptr_name := el_id.rptr_name()) and rptr_name.name == 'id':
                    id_idx = len(els)

                colname = f'_t{ctr}'
                ctr += 1

                typ = pg_types.pg_type_from_ir_typeref(el_id.target)
                if el_id.target.id in ctx.env.materialized_views:
                    typ = ('record',)
                    must_pack = True

                if not is_singleton:
                    # Arrays get wrapped in a record before they can be put
                    # in another array
                    if el_id.is_array_path():
                        typ = ('record',)
                        must_pack = True
                    typ = pg_types.pg_type_array(typ)

                coldeflist.append(
                    pgast.ColumnDef(
                        name=colname,
                        typename=pgast.TypeName(name=typ),
                    )
                )
                els.append(UnpackElement(
                    el_id, colname,
                    packed=must_pack, multi=not is_singleton, ref=None
                ))

                vpath_ids.append(el_id)

            if id_idx is not None:
                els.append(UnpackElement(
                    path_id, els[id_idx].colname,
                    multi=False, packed=False, ref=None,
                ))

            view_tvars.append((path_id, pgast.TupleVarBase(
                elements=[
                    pgast.TupleElementBase(
                        path_id=pid,
                        name=astutils.tuple_element_for_shape_el(
                            el, ctx=ctx).name,
                    )
                    for (el, op), pid in zip(view_tuple.shape, vpath_ids)
                    if op != qlast.ShapeOp.MATERIALIZE or ctx.materializing
                ],
                typeref=path_id.target,
                named=True,
            ), False))

        else:
            coldeflist = []
            simple = not multi
            els.append(UnpackElement(
                path_id, alias, multi=False, packed=False,
                ref=ref if simple else None,
            ))

        if not simple:
            if not multi:
                # Sigh, have to wrap in an array so we can unpack.
                ref = pgast.ArrayExpr(elements=[ref])

            qry.from_clause.insert(
                0,
                pgast.RangeFunction(
                    alias=pgast.Alias(
                        aliasname=alias,
                    ),
                    is_rowsfrom=True,
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[ref],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            )

    ########################

    walk(ref, path_id, ref.is_packed_multi)

    rvar = rvar_for_rel(qry, lateral=True, ctx=ctx)
    include_rvar(stmt, rvar, path_id=path_id, aspects=('value',), ctx=ctx)

    for el in els:
        el_id = el.path_id
        cur_ref = el.ref or pgast.ColumnRef(name=[el.colname])

        for aspect in ('value', 'serialized'):
            pathctx.put_path_var(
                qry, el_id, cur_ref, aspect=aspect, env=ctx.env,
            )

        if not el.packed:
            pathctx.put_path_rvar(
                stmt, el_id, rvar, aspect='value', env=ctx.env)

            pathctx.put_path_rvar(
                ctx.rel, el_id, rvar, aspect='value', env=ctx.env)
        else:
            cref = pathctx.get_path_output(
                qry, el_id, aspect='value', env=ctx.env)
            cref = cref.replace(is_packed_multi=el.multi)

            pathctx.put_path_packed_output(qry, el_id, val=cref)

            pathctx.put_path_rvar(
                stmt, el_id, rvar, flavor='packed', aspect='value', env=ctx.env
            )

    # When we're producing an exposed shape, we need to rewrite the
    # serialized shape.
    # We also need to rewrite tuples that contain such shapes!
    # What a pain!
    #
    # We *also* need to rewrite tuple values, so that we don't consider
    # serialized materialized objects as part of the value of the tuple
    for view_path_id, view_tvar, is_tuple in view_tvars:
        if not view_tvar.elements:
            continue

        rewrite_aspects = []
        if ctx.expr_exposed and not is_tuple:
            rewrite_aspects.append('serialized')
        if is_tuple:
            rewrite_aspects.append('value')

        # Reserialize links if we are producing final output
        if (
            ctx.expr_exposed and not ctx.materializing and not is_tuple
        ):
            for tel in view_tvar.elements:
                el = [x for x in els if x.path_id == tel.path_id][0]
                if not el.packed:
                    continue
                reqry = reserialize_object(el, tel, ctx=ctx)
                pathctx.put_path_var(
                    qry, tel.path_id, reqry, aspect='serialized',
                    env=ctx.env, force=True
                )

        for aspect in rewrite_aspects:
            tv = pathctx.fix_tuple(qry, view_tvar, aspect=aspect, env=ctx.env)
            sval = (
                output.output_as_value(tv, env=ctx.env)
                if aspect == 'value' else
                output.serialize_expr(tv, path_id=path_id, env=ctx.env)
            )
            pathctx.put_path_var(
                qry, view_path_id, sval, aspect=aspect, env=ctx.env, force=True
            )
            pathctx.put_path_rvar(
                ctx.rel, view_path_id, rvar, aspect=aspect, env=ctx.env
            )

    return rvar


def reserialize_object(
        el: UnpackElement, tel: pgast.TupleElementBase,
        *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    tref = pgast.ColumnRef(name=[el.colname], is_packed_multi=el.multi)

    with ctx.subrel() as subctx:
        sub_rvar = unpack_var(subctx.rel, tel.path_id, ref=tref, ctx=subctx)
    reqry = sub_rvar.query
    assert isinstance(reqry, pgast.Query)
    rptr = tel.path_id.rptr()
    pathctx.get_path_serialized_output(reqry, tel.path_id, env=ctx.env)
    assert rptr
    if rptr.out_cardinality.is_multi():
        with ctx.subrel() as subctx:
            reqry = set_to_array(
                path_id=tel.path_id, query=reqry, ctx=subctx)
    return reqry


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
    query: pgast.SelectStmt,
    right_rvar: pgast.PathRangeVar,
    *,
    ctx: context.CompilerContextLevel,
) -> None:

    if (
        isinstance(right_rvar, pgast.RangeSubselect)
        and astutils.is_set_op_query(right_rvar.subquery)
        and right_rvar.tag == "overlay-stack"
        and all(astutils.for_each_query_in_set(
            right_rvar.subquery, lambda q: isinstance(q, pgast.SelectStmt)))
        and not is_pointer_rvar(right_rvar, ctx=ctx)
    ):
        # Unfortunately Postgres sometimes produces a very bad plan
        # when we join a UNION which is not a trivial Append, most notably
        # those produced by DML overlays.  To work around this we push
        # the JOIN condition into the WHERE clause of each UNION component.
        # While this is likely not harmful (and possibly beneficial) for
        # all kinds of UNIONs, we restrict this optimization to overlay
        # UNIONs only to limit the possibility of breakage as not all
        # UNIONs are guaranteed to have correct path namespace and
        # translation maps set up.
        _lateral_union_join(query, right_rvar, ctx=ctx)
    else:
        _plain_join(query, right_rvar, ctx=ctx)


def _plain_join(
    query: pgast.SelectStmt,
    right_rvar: pgast.PathRangeVar,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
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


def _lateral_union_join(
    query: pgast.SelectStmt,
    right_rvar: pgast.RangeSubselect,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    def _inject_filter(component: pgast.Query) -> None:
        condition = None

        for path_id in right_rvar.query.path_scope:
            lref = maybe_get_path_var(
                query, path_id, aspect='identity', ctx=ctx)
            if lref is None:
                lref = maybe_get_path_var(
                    query, path_id, aspect='value', ctx=ctx)
            if lref is None:
                continue

            rref = pathctx.get_path_identity_var(
                component, path_id, env=ctx.env)

            assert isinstance(lref, pgast.ColumnRef)
            assert isinstance(rref, pgast.ColumnRef)
            path_cond = astutils.join_condition(lref, rref)
            condition = astutils.extend_binop(condition, path_cond)

        if condition is not None:
            assert isinstance(component, pgast.SelectStmt)
            component.where_clause = astutils.extend_binop(
                component.where_clause, condition)

    astutils.for_each_query_in_set(right_rvar.subquery, _inject_filter)
    if not query.from_clause:
        query.from_clause.append(right_rvar)
    else:
        larg = query.from_clause[0]
        rarg = right_rvar

        query.from_clause[0] = pgast.JoinExpr(
            type='cross', larg=larg, rarg=rarg)


def range_for_material_objtype(
    typeref: irast.TypeRef,
    path_id: irast.PathId,
    *,
    for_mutation: bool=False,
    lateral: bool=False,
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
            include_rvar(
                sctx.rel, cte_rvar, rewrite.path_id, pull_namespace=False,
                ctx=sctx,
            )
            rvar = rvar_for_rel(
                sctx.rel, lateral=lateral, typeref=typeref, ctx=sctx)
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
            rvar = rvar_for_rel(cte, typeref=typeref, ctx=ctx)

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
                    aliasname=env.aliases.get(hint=cte.name or '')
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
            set_ops,
            typeref.name_hint,
            lateral=lateral,
            path_id=path_id,
            tag='overlay-stack',
            ctx=ctx,
        )

    return rvar


def range_for_typeref(
    typeref: irast.TypeRef,
    path_id: irast.PathId,
    *,
    lateral: bool=False,
    for_mutation: bool=False,
    include_descendants: bool=True,
    dml_source: Optional[irast.MutatingStmt]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    if typeref.union:
        # Union object types are represented as a UNION of selects
        # from their children, which is, for most purposes, equivalent
        # to SELECTing from a parent table.
        set_ops = []

        # Concrete unions might have view type elements with duplicate
        # material types, and we need to filter those out.
        seen = set()
        for child in typeref.union:
            mat_child = child.material_type or child
            if mat_child.id in seen:
                assert typeref.union_is_concrete
                continue
            seen.add(mat_child.id)

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

        rvar = range_from_queryset(
            set_ops,
            typeref.name_hint,
            lateral=lateral,
            typeref=typeref,
            ctx=ctx,
        )

    elif typeref.intersection:
        wrapper = pgast.SelectStmt()
        component_rvars = []
        for component in typeref.intersection:
            component_rvar = range_for_typeref(
                component,
                lateral=True,
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
        rvar = rvar_for_rel(wrapper, lateral=lateral, ctx=ctx)

    else:
        rvar = range_for_material_objtype(
            typeref,
            path_id,
            lateral=lateral,
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
        nqry = pgast.SelectStmt(from_clause=[rvar])
        nqry.target_list = [
            pgast.ResTarget(
                name=col.name,
                val=pgast.ColumnRef(
                    name=[rvar.alias.aliasname, col.name],
                )
            )
            for col in astutils.get_leftmost_query(qry).target_list
            if col.name
        ]

        pull_path_namespace(target=nqry, source=rvar, ctx=ctx)
        qry = nqry
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
    prep_filter: Callable[
        [pgast.SelectStmt, pgast.SelectStmt], None]=lambda a, b: None,
    path_id: Optional[irast.PathId]=None,
    lateral: bool=False,
    typeref: Optional[irast.TypeRef]=None,
    tag: Optional[str]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    rvar: pgast.PathRangeVar

    if len(set_ops) > 1:
        # More than one class table, generate a UNION/EXCEPT clause.
        qry = set_ops[0][1]

        for op, rarg in set_ops[1:]:
            if op == 'filter':
                qry = wrap_set_op_query(qry, ctx=ctx)
                prep_filter(qry, rarg)
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
            lateral=lateral,
            tag=tag,
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get(objname.name),
            ),
            typeref=typeref,
        )

    else:
        # Just one class table, so return it directly
        from_rvar = set_ops[0][1].from_clause[0]
        assert isinstance(from_rvar, pgast.PathRangeVar)
        rvar = from_rvar

    return rvar


def table_from_ptrref(
    ptrref: irast.PointerRef,
    ptr_info: pg_types.PointerStorageInfo,
    *,
    include_descendants: bool = True,
    for_mutation: bool = False,
    ctx: context.CompilerContextLevel,
) -> pgast.RelRangeVar:
    """Return a Table corresponding to a given Link."""

    aspect = 'table' if for_mutation or not include_descendants else 'inhview'
    table_schema_name, table_name = common.update_aspect(
        ptr_info.table_name, aspect
    )

    if ptrref.name.module in {'cfg', 'sys'}:
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name, name=table_name)

    # Pseudo pointers (tuple and type intersection) have no schema id.
    sobj_id = ptrref.id if isinstance(ptrref, irast.PointerRef) else None
    typeref = ptrref.out_source if ptrref else None
    rvar = pgast.RelRangeVar(
        schema_object_id=sobj_id,
        typeref=typeref,
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
    path_id: Optional[irast.PathId]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    """"Return a Range subclass corresponding to a given ptr step.

    The return value may potentially be a UNION of all tables
    corresponding to a set of specialized links computed from the given
    `ptrref` taking source inheritance into account.
    """

    output_cols = ('source', 'target')

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

        # Most references to inline links are dispatched to a separate
        # code path (_new_inline_pointer_rvar) by new_pointer_rvar,
        # but when we have union pointers, some might be inline.  We
        # always use the link table if it exists (because this range
        # needs to contain any link properties, for one reason.)
        ptr_info = pg_types.get_ptrref_storage_info(
            src_ptrref, resolve_type=False, link_bias=True,
        )
        if not ptr_info:
            assert ptrref.union_components
            ptr_info = pg_types.get_ptrref_storage_info(
                src_ptrref, resolve_type=False, link_bias=False,
            )

        cols = [
            'source' if ptr_info.table_type == 'link' else 'id',
            ptr_info.column_name,
        ]

        table = table_from_ptrref(
            src_ptrref,
            ptr_info,
            include_descendants=not ptrref.union_is_concrete,
            for_mutation=for_mutation,
            ctx=ctx,
        )

        qry = pgast.SelectStmt()
        qry.from_clause.append(table)

        # Make sure all property references are pulled up properly
        for colname, output_colname in zip(cols, output_cols):
            selexpr = pgast.ColumnRef(
                name=[table.alias.aliasname, colname])
            qry.target_list.append(
                pgast.ResTarget(val=selexpr, name=output_colname))

        set_ops.append(('union', qry))

        overlays = get_ptr_rel_overlays(
            src_ptrref, dml_source=dml_source, ctx=ctx)
        if overlays and not for_mutation:
            # We need the identity var for semi_join to work and
            # the source rvar so that linkprops can be found here.
            if path_id:
                target_ref = qry.target_list[1].val
                pathctx.put_path_identity_var(
                    qry, path_id, var=target_ref, env=ctx.env)
                pathctx.put_path_source_rvar(
                    qry, path_id, table, env=ctx.env)

            for op, cte, cte_path_id in overlays:
                rvar = rvar_for_rel(cte, ctx=ctx)

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
                # Set up identity var, source rvar for reasons discussed above
                if path_id:
                    target_ref = pgast.ColumnRef(
                        name=[rvar.alias.aliasname, cols[1]])
                    pathctx.put_path_identity_var(
                        qry, cte_path_id, var=target_ref, env=ctx.env)
                    pathctx.put_path_source_rvar(
                        qry, cte_path_id, rvar, env=ctx.env)
                    pathctx.put_path_id_map(qry, path_id, cte_path_id)

                set_ops.append((op, qry))

    def prep_filter(larg: pgast.SelectStmt, rarg: pgast.SelectStmt) -> None:
        # Set up the proper join on the source field and clear the target list
        # of the rhs of a filter overlay.
        assert isinstance(larg.target_list[0].val, pgast.ColumnRef)
        assert isinstance(rarg.target_list[0].val, pgast.ColumnRef)
        rarg.where_clause = astutils.join_condition(
            larg.target_list[0].val, rarg.target_list[0].val)
        rarg.target_list.clear()

    return range_from_queryset(
        set_ops, ptrref.shortname,
        prep_filter=prep_filter, path_id=path_id, ctx=ctx)


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

    return range_for_ptrref(
        ptrref, dml_source=dml_source, path_id=path_id, ctx=ctx)


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
        alias = ctx.env.aliases.get(rel.name or '')

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
        stop_ref: Optional[irast.TypeRef]=None,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:
    typeref = typeref.real_material_type
    objs = [typeref]
    if typeref.ancestors:
        objs.extend(typeref.ancestors)

    for obj in objs:
        if stop_ref and (
            obj == stop_ref or
            (stop_ref.ancestors and obj in stop_ref.ancestors)
        ):
            continue
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
    for (obj, ptr), poverlays in ptr_overlays.items():
        for op, rel, path_id in poverlays:
            _add_ptr_rel_overlay(
                obj, ptr, op, rel, path_id=path_id, dml_stmts=dml_stmts,
                ctx=ctx
            )


def _add_ptr_rel_overlay(
        typeid: uuid.UUID,
        ptrref_name: str,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:

    entry = (op, rel, path_id)
    if dml_stmts:
        for dml_stmt in dml_stmts:
            overlays = ctx.ptr_rel_overlays[dml_stmt][typeid, ptrref_name]
            if entry not in overlays:
                overlays.append(entry)
    else:
        overlays = ctx.ptr_rel_overlays[None][typeid, ptrref_name]
        if entry not in overlays:
            overlays.append(entry)


def add_ptr_rel_overlay(
        ptrref: irast.PointerRef,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:

    typeref = ptrref.out_source.real_material_type
    objs = [typeref]
    if typeref.ancestors:
        objs.extend(typeref.ancestors)

    for obj in objs:
        _add_ptr_rel_overlay(
            obj.id, ptrref.shortname.name, op, rel, path_id=path_id,
            dml_stmts=dml_stmts,
            ctx=ctx)


def get_ptr_rel_overlays(
    ptrref: irast.PointerRef, *,
    dml_source: Optional[irast.MutatingStmt]=None,
    ctx: context.CompilerContextLevel,
) -> List[
    Tuple[
        str,
        Union[pgast.BaseRelation, pgast.CommonTableExpr],
        irast.PathId,
    ]
]:
    typeref = ptrref.out_source.real_material_type
    return ctx.ptr_rel_overlays[dml_source][typeref.id, ptrref.shortname.name]
