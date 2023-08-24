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

import immutables as immu

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
                target, path_id, aspect=aspect, flavor=flavor
            )
            if rvar is None or flavor == 'packed':
                pathctx.put_path_rvar(
                    target, path_id, source, aspect=aspect, flavor=flavor
                )


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
            stmt, path_id, rvar, aspect='value', flavor=flavor
        )

        src_rvar = maybe_get_path_rvar(source_stmt, path_id=path_id,
                                       aspect='source', flavor=flavor, ctx=ctx)

        if src_rvar is not None:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, src_rvar, aspect='source', flavor=flavor
            )

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
        aspects = ('value',)
        if path_id.is_objtype_path():
            if isinstance(rvar, pgast.RangeSubselect):
                if pathctx.has_path_aspect(
                    rvar.query, path_id, aspect='source'
                ):
                    aspects += ('source',)
            else:
                aspects += ('source',)

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
        rel_join(stmt, rvar, ctx=ctx)
        # Make sure that the path namespace of *rvar* is mapped
        # onto the path namespace of *stmt*.
        if pull_namespace:
            pull_path_namespace(target=stmt, source=rvar, ctx=ctx)

    for aspect in aspects:
        if overwrite_path_rvar:
            pathctx.put_path_rvar(
                stmt, path_id, rvar, flavor=flavor, aspect=aspect
            )
        else:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, rvar, flavor=flavor, aspect=aspect
            )

    if update_mask:
        scopes = [ctx.scope_tree]
        parent_scope = ctx.scope_tree.parent
        if parent_scope is not None:
            scopes.append(parent_scope)

        tpath_id = path_id.tgt_path()
        if not any(scope.path_id == tpath_id or
                   scope.find_child(tpath_id) for scope in scopes):
            pathctx.put_path_id_mask(stmt, path_id)

    return rvar


def has_rvar(
        stmt: pgast.Query, rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> bool:

    curstmt: Optional[pgast.Query] = stmt

    if ctx.env.external_rvars and has_external_rvar(rvar, ctx=ctx):
        return True

    while curstmt is not None:
        if pathctx.has_rvar(curstmt, rvar):
            return True
        curstmt = ctx.rel_hierarchy.get(curstmt)

    return False


def has_external_rvar(
    rvar: pgast.PathRangeVar,
    *,
    ctx: context.CompilerContextLevel,
) -> bool:
    return rvar in ctx.env.external_rvars.values()


def _maybe_get_path_rvar(
    stmt: pgast.Query,
    path_id: irast.PathId,
    *,
    flavor: str='normal',
    aspect: str,
    ctx: context.CompilerContextLevel,
) -> Optional[Tuple[pgast.PathRangeVar, irast.PathId]]:
    rvar = ctx.env.external_rvars.get((path_id, aspect))
    if rvar:
        return rvar, path_id

    qry: Optional[pgast.Query] = stmt
    while qry is not None:
        rvar = pathctx.maybe_get_path_rvar(
            qry, path_id, aspect=aspect, flavor=flavor
        )
        if rvar is not None:
            if qry is not stmt:
                # Cache the rvar reference.
                pathctx.put_path_rvar(
                    stmt, path_id, rvar, flavor=flavor, aspect=aspect
                )
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
    nullrel = pgast.NullRelation(
        path_id=ir_set.path_id, type_or_ptr_ref=ir_set.typeref)
    rvar = rvar_for_rel(nullrel, ctx=ctx)
    pathctx.put_rvar_path_bond(rvar, ir_set.path_id)
    return rvar


def new_free_object_rvar(
    typeref: irast.TypeRef,
    path_id: irast.PathId,
    *,
    lateral: bool=False,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    """Create a fake source rel for a free object

    We generate fake IDs for free objects. The only thing other than ids
    that need to come from a free object is __type__, which we inject
    in a special case way in pathctx.get_path_var.

    We also have a special case in relgen.ensure_source_rvar to reuse an
    existing value rvar instead of creating a new root rvar.

    (We inject __type__ in get_path_var instead of injecting it here because
    we don't have the pathid for it available to us here and because it
    allows ensure_source_rvar to simply reuse a value rvar.)

    """
    with ctx.subrel() as subctx:
        qry = subctx.rel

        id_expr = pgast.FuncCall(
            name=('edgedb', 'uuid_generate_v4'),
            args=[],
        )

        pathctx.put_path_identity_var(qry, path_id, id_expr)
        pathctx.put_path_value_var(qry, path_id, id_expr)
        apply_volatility_ref(qry, ctx=subctx)

    return rvar_for_rel(qry, typeref=typeref, lateral=lateral, ctx=ctx)


def deep_copy_primitive_rvar_path_var(
    orig_id: irast.PathId, new_id: irast.PathId,
    rvar: pgast.PathRangeVar, *,
    env: context.Environment
) -> None:
    """Copy one identity path to another in a primitive rvar.

    The trickiness here is because primitive rvars might have an
    overlay stack, which means if they are joined on, it might be
    using _lateral_union_join, which requires every component of
    the union to have all the path bonds.
    """

    if isinstance(rvar, pgast.RangeSubselect):
        for component in astutils.each_query_in_set(rvar.query):
            rref = pathctx.get_path_var(
                component, orig_id, aspect='identity', env=env
            )
            pathctx.put_path_var(component, new_id, rref, aspect='identity')
    else:
        rref = pathctx.get_path_output(
            rvar.query, orig_id, aspect='identity', env=env
        )
        pathctx.put_rvar_path_output(rvar, new_id, aspect='identity', var=rref)


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
        typeref, path_id, lateral=lateral, dml_source=dml_source,
        include_descendants=not ir_set.skip_subtypes,
        ignore_rewrites=ir_set.ignore_rewrites, ctx=ctx)
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

            # Unfortunately we can't necessarily just install the
            # prefix path id path---the rvar from range_from_typeref
            # might be a DML overlay, which means joins on it will try
            # to use _lateral_union_join; this means that all of the
            # path bonds need to be valid on each *subquery*, so we
            # need to set them up in each subquery.
            deep_copy_primitive_rvar_path_var(
                flipped_id, prefix_path_id, set_rvar, env=ctx.env)
            pathctx.put_rvar_path_bond(set_rvar, prefix_path_id)

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

    if irtyputils.is_free_object(path_id.target):
        return new_free_object_rvar(
            path_id.target, path_id, lateral=lateral, ctx=ctx)

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
    pathctx.put_path_identity_var(ptr_rel, far_pid, var=far_ref)

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
    if (
        ir_ptr.direction == s_pointers.PointerDirection.Inbound
        or ptrref.computed_backlink
    ):
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
    pathctx.put_rvar_path_output(
        ptr_rvar, src_pid, aspect='identity', var=near_ref
    )
    pathctx.put_rvar_path_output(
        ptr_rvar, src_pid, aspect='value', var=near_ref
    )
    pathctx.put_rvar_path_output(
        ptr_rvar, tgt_pid, aspect='value', var=far_ref
    )

    if tgt_pid.is_objtype_path():
        pathctx.put_rvar_path_bond(ptr_rvar, tgt_pid)
        pathctx.put_rvar_path_output(
            ptr_rvar, tgt_pid, aspect='identity', var=far_ref
        )

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
        name=('edgedb', 'uuid_generate_v4'),
        args=[],
    )

    pathctx.put_path_identity_var(stmt, path_id, id_expr, force=True)
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
    """Update the scope of an ir set to be a pg stmt.

    If ir_set has a scope node associated with it, update path_scope
    so that any paths bound in that scope will be compiled in the
    context of stmt.

    This, combined with maybe_get_scope_stmt, is the mechanism by
    which the scope tree influences the shape of the output query.
    """

    scope_tree = get_scope(ir_set, ctx=ctx)
    if scope_tree is None:
        return

    ctx.scope_tree = scope_tree
    ctx.path_scope = ctx.path_scope.new_child()

    # Register paths in the current scope to be compiled as a subrel
    # of stmt.
    for p in scope_tree.path_children:
        assert p.path_id is not None
        ctx.path_scope[p.path_id] = stmt

    # Mark any paths under the scope tree as masked, so that they
    # won't get picked up by pull_path_namespace.
    for child_path in scope_tree.get_all_paths():
        pathctx.put_path_id_mask(stmt, child_path)

    # If this is an optional scope node, we need to be certain that
    # we don't leak out any paths that collide with a visible non-optional
    # path.
    # See test_edgeql_optional_leakage_01 for one case where this comes up.
    #
    # FIXME: I actually think we ought to be able to mask off visible
    # paths in *most* cases, but when I tried it I ran into trouble
    # with some DML linkprop cases (probably easy to fix) and a number
    # of materialization cases (possibly hard to fix), so I'm going
    # with a more conservative approach.
    if scope_tree.optional:
        # Since compilation is done, anything visible to us *will* be
        # up on the spine. Anything tucked away under a node must have
        # been pulled up.
        for anc in scope_tree.ancestors:
            for direct_child in anc.path_children:
                if not direct_child.optional:
                    pathctx.put_path_id_mask(stmt, direct_child.path_id)


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
        for_group_by: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    """Collapse a set into an array."""
    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('aggw')
        )
    )

    result = pgast.SelectStmt()
    aspects = pathctx.list_path_aspects(subrvar.query, path_id)
    include_rvar(result, subrvar, path_id=path_id, aspects=aspects, ctx=ctx)

    val: Optional[pgast.BaseExpr] = (
        pathctx.maybe_get_path_serialized_var(
            result, path_id, env=ctx.env)
    )

    if val is None:
        value_var = pathctx.get_path_value_var(result, path_id, env=ctx.env)
        val = output.serialize_expr(value_var, path_id=path_id, env=ctx.env)
        pathctx.put_path_serialized_var(result, path_id, val, force=True)

    if isinstance(val, pgast.TupleVarBase):
        val = output.serialize_expr(
            val, path_id=path_id, env=ctx.env)

    pg_type = output.get_pg_type(path_id.target, ctx=ctx)

    agg_filter_safe = True

    if for_group_by:
        # When doing this as part of a GROUP, the stuff being aggregated
        # needs to actually appear *inside* of the aggregate call...
        result.target_list = [pgast.ResTarget(val=val, ser_safe=val.ser_safe)]
        val = result
        try_collapse = astutils.collapse_query(val)
        if isinstance(try_collapse, pgast.ColumnRef):
            val = try_collapse
        else:
            agg_filter_safe = False

        result = pgast.SelectStmt()

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
            if orig_val.nullable and agg_filter_safe else None
        ),
        ser_safe=val.ser_safe,
    )

    # If this is for a group by, and the body isn't just a column ref,
    # then we need to remove NULLs after the fact.
    if orig_val.nullable and not agg_filter_safe:
        array_agg = pgast.FuncCall(
            name=('array_remove',),
            args=[array_agg, pgast.NullConstant()]
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
                    name=el_name, shortname=el_name,
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

                el_id = path_id.ptr_path().extend(ptrref=el_ptrref)

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
            pathctx.put_path_var(qry, el_id, cur_ref, aspect=aspect)

        if not el.packed:
            pathctx.put_path_rvar(stmt, el_id, rvar, aspect='value')

            pathctx.put_path_rvar(ctx.rel, el_id, rvar, aspect='value')
        else:
            cref = pathctx.get_path_output(
                qry, el_id, aspect='value', env=ctx.env)
            cref = cref.replace(is_packed_multi=el.multi)

            pathctx.put_path_packed_output(qry, el_id, val=cref)

            pathctx.put_path_rvar(
                stmt, el_id, rvar, flavor='packed', aspect='value'
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
                    qry, tel.path_id, reqry, aspect='serialized', force=True
                )

        for aspect in rewrite_aspects:
            tv = pathctx.fix_tuple(qry, view_tvar, aspect=aspect, env=ctx.env)
            sval = (
                output.output_as_value(tv, env=ctx.env)
                if aspect == 'value' else
                output.serialize_expr(tv, path_id=path_id, env=ctx.env)
            )
            pathctx.put_path_var(
                qry, view_path_id, sval, aspect=aspect, force=True
            )
            pathctx.put_path_rvar(ctx.rel, view_path_id, rvar, aspect=aspect)

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
        and all(isinstance(q, pgast.SelectStmt)
                for q in astutils.each_query_in_set(right_rvar.subquery))
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
    # Inject the filter into every subquery
    for component in astutils.each_query_in_set(right_rvar.subquery):
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

    # Do the actual join
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
    ignore_rewrites: bool=False,
    dml_source: Optional[irast.MutatingLikeStmt]=None,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    env = ctx.env

    if typeref.material_type is not None:
        typeref = typeref.material_type

    relation: Union[pgast.Relation, pgast.CommonTableExpr]

    assert isinstance(typeref.name_hint, sn.QualName)

    dml_source_key = dml_source if ctx.trigger_mode else None
    rw_key = (typeref.id, include_descendants)
    key = rw_key + (dml_source_key,)
    if (
        not ignore_rewrites
        and (rewrite := ctx.env.type_rewrites.get(rw_key)) is not None
        and rw_key not in ctx.pending_type_ctes
        and not for_mutation
    ):
        # Don't include overlays in the normal way in trigger mode
        # when a type cte is used, because we bake the overlays into
        # the cte instead (and so including them normally could union
        # back in things that we have filtered out).
        # We *don't* do this for __old__ and __new__; __old__ because
        # we don't want overlays at all and __new__ because we want the
        # overlays to apply after policies.
        trigger_mode = (
            ctx.trigger_mode
            and not isinstance(dml_source, irast.TriggerAnchor)
        )
        if trigger_mode:
            include_overlays = False

        type_rel: pgast.BaseRelation | pgast.CommonTableExpr
        if (type_cte := ctx.type_ctes.get(key)) is None:
            with ctx.newrel() as sctx:
                sctx.pending_type_ctes.add(rw_key)
                sctx.pending_query = sctx.rel
                sctx.volatility_ref = ()
                # Normally we want to compile type rewrites without
                # polluting them with any sort of overlays, but when
                # compiling triggers, we recompile all of the type
                # rewrites *to include* overlays, so that we can't peek
                # at all newly created objects that we can't see
                if not trigger_mode:
                    sctx.rel_overlays = context.RelOverlays()

                dispatch.visit(rewrite, ctx=sctx)
                # If we are expanding inhviews, we also expand type
                # rewrites, so don't populate type_ctes. The normal
                # case is to stick it in a CTE and cache that, though.
                if ctx.env.expand_inhviews:
                    type_rel = sctx.rel
                else:
                    type_cte = pgast.CommonTableExpr(
                        name=ctx.env.aliases.get('t'),
                        query=sctx.rel,
                        materialized=False,
                    )
                    ctx.type_ctes[key] = type_cte
                    type_rel = type_cte
        else:
            type_rel = type_cte

        with ctx.subrel() as sctx:
            cte_rvar = rvar_for_rel(
                type_rel,
                typeref=typeref,
                alias=env.aliases.get('t'),
                ctx=ctx,
            )
            pathctx.put_path_id_map(sctx.rel, path_id, rewrite.path_id)
            include_rvar(
                sctx.rel, cte_rvar, rewrite.path_id, pull_namespace=False,
                ctx=sctx,
            )
            rvar = rvar_for_rel(
                sctx.rel, lateral=lateral, typeref=typeref, ctx=sctx)

    # When we are compiling a query for EXPLAIN, expand out type references
    # to an explicit union of all the types, rather than relying on the
    # inheritance views. This allows postgres to actually give us back the
    # alias names that we use for relations, which we use to track which
    # parts of the query are being referred to.
    elif (
        ctx.env.expand_inhviews
        and include_descendants
        and not for_mutation
        and typeref.children is not None

        # HACK: This is a workaround for #4491
        and typeref.name_hint.module not in {'cfg', 'sys'}
    ):
        ops = []
        typerefs = [typeref, *irtyputils.get_typeref_descendants(typeref)]
        all_abstract = all(subref.is_abstract for subref in typerefs)
        for subref in typerefs:
            if subref.is_abstract and not all_abstract:
                continue
            rvar = range_for_material_objtype(
                subref, path_id, lateral=lateral,
                include_descendants=False,
                include_overlays=False,
                ignore_rewrites=ignore_rewrites,  # XXX: Is this right?
                ctx=ctx,
            )
            qry = pgast.SelectStmt(from_clause=[rvar])
            sub_path_id = path_id
            pathctx.put_path_value_rvar(qry, sub_path_id, rvar)
            pathctx.put_path_source_rvar(qry, sub_path_id, rvar)

            ops.append(('union', qry))

        rvar = range_from_queryset(
            ops,
            typeref.name_hint,
            lateral=lateral,
            path_id=path_id,
            typeref=typeref,
            tag='expanded-inhview',
            ctx=ctx,
        )

    else:

        table_schema_name, table_name = common.get_objtype_backend_name(
            typeref.id,
            typeref.name_hint.module,
            aspect=(
                'table' if for_mutation or not include_descendants else
                'inhview'
            ),
            catenate=False,
        )

        relation = pgast.Relation(
            schemaname=table_schema_name,
            name=table_name,
            path_id=path_id,
            type_or_ptr_ref=typeref,
        )

        rvar = pgast.RelRangeVar(
            relation=relation,
            typeref=typeref,
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
        pathctx.put_path_value_rvar(qry, path_id, rvar)
        if path_id.is_objtype_path():
            pathctx.put_path_source_rvar(qry, path_id, rvar)
        pathctx.put_path_bond(qry, path_id)

        set_ops.append(('union', qry))

        for op, cte, cte_path_id in overlays:
            rvar = rvar_for_rel(cte, typeref=typeref, ctx=ctx)

            qry = pgast.SelectStmt(
                from_clause=[rvar],
            )

            pathctx.put_path_value_rvar(qry, cte_path_id, rvar)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry, cte_path_id, rvar)
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
            pathctx.put_path_value_rvar(qry2, path_id, qry_rvar)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry2, path_id, qry_rvar)
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
            typeref=typeref,
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
    ignore_rewrites: bool=False,
    dml_source: Optional[irast.MutatingLikeStmt]=None,
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

            pathctx.put_path_value_rvar(qry, path_id, c_rvar)
            if path_id.is_objtype_path():
                pathctx.put_path_source_rvar(qry, path_id, c_rvar)

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
            pathctx.put_path_rvar(wrapper, path_id, int_rvar, aspect=aspect)

        pathctx.put_path_bond(wrapper, path_id)
        rvar = rvar_for_rel(wrapper, lateral=lateral, typeref=typeref, ctx=ctx)

    else:
        rvar = range_for_material_objtype(
            typeref,
            path_id,
            lateral=lateral,
            include_descendants=include_descendants,
            ignore_rewrites=ignore_rewrites,
            include_overlays=not for_mutation,
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
        cond_expr = pgast.SubLink(operator="NOT EXISTS", expr=rhs)
    lhs.where_clause = astutils.extend_binop(lhs.where_clause, cond_expr)


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

    typeref = ptrref.out_source if ptrref else None
    relation = pgast.Relation(
        schemaname=table_schema_name,
        name=table_name,
        type_or_ptr_ref=ptrref,
    )

    # Pseudo pointers (tuple and type intersection) have no schema id.
    sobj_id = ptrref.id if isinstance(ptrref, irast.PointerRef) else None
    rvar = pgast.RelRangeVar(
        schema_object_id=sobj_id,
        typeref=typeref,
        relation=relation,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get(ptrref.shortname.name)
        )
    )

    return rvar


def range_for_ptrref(
    ptrref: irast.BasePointerRef, *,
    dml_source: Optional[irast.MutatingLikeStmt]=None,
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
    elif ptrref.intersection_components:
        # This is a little funky, but in an intersection, the pointer
        # needs to appear in *all* of the tables, so we just pick any
        # one of them.
        refs = {next(iter((ptrref.intersection_components)))}
    elif ptrref.computed_backlink:
        refs = {ptrref.computed_backlink}
    else:
        refs = {ptrref}
        assert isinstance(ptrref, irast.PointerRef), \
            "expected regular PointerRef"
        overlays = get_ptr_rel_overlays(
            ptrref, dml_source=dml_source, ctx=ctx)

    include_descendants = not ptrref.union_is_concrete

    assert isinstance(ptrref.out_source.name_hint, sn.QualName)
    # expand_inhviews helps support EXPLAIN. see
    # range_for_material_objtype for details.
    lrefs: List[irast.BasePointerRef]
    if (
        ctx.env.expand_inhviews
        and include_descendants
        and not for_mutation

        # HACK: This is a workaround for #4491
        and ptrref.out_source.name_hint.module not in {'sys', 'cfg'}
    ):
        include_descendants = False
        lrefs = []
        for ref in list(refs):
            lrefs.extend(ref.descendants())
            lrefs.append(ref)
        concrete_lrefs = [
            ref for ref in lrefs if not ref.out_source.is_abstract
        ]
        # If there aren't any concrete types, we still need to
        # generate *something*, so just do all the abstract ones.
        if concrete_lrefs:
            lrefs = concrete_lrefs
    else:
        lrefs = list(refs)

    for src_ptrref in lrefs:
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
            include_descendants=include_descendants,
            for_mutation=for_mutation,
            ctx=ctx,
        )
        table.query.path_id = path_id

        qry = pgast.SelectStmt()
        qry.from_clause.append(table)

        # Make sure all property references are pulled up properly
        for colname, output_colname in zip(cols, output_cols):
            selexpr = pgast.ColumnRef(
                name=[table.alias.aliasname, colname])
            qry.target_list.append(
                pgast.ResTarget(val=selexpr, name=output_colname))

        set_ops.append(('union', qry))

        # We need the identity var for semi_join to work and
        # the source rvar so that linkprops can be found here.
        if path_id:
            target_ref = qry.target_list[1].val
            pathctx.put_path_identity_var(qry, path_id, var=target_ref)
            pathctx.put_path_source_rvar(qry, path_id, table)

        # Only fire off the overlays at the end of each expanded inhview.
        # This only matters when we are doing expand_inhviews, and prevents
        # us from repeating the overlays many times in that case.
        if src_ptrref in refs and not for_mutation:
            overlays = get_ptr_rel_overlays(
                src_ptrref, dml_source=dml_source, ctx=ctx)

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
                        qry, cte_path_id, var=target_ref
                    )
                    pathctx.put_path_source_rvar(qry, cte_path_id, rvar)
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
    dml_source: Optional[irast.MutatingLikeStmt] = None,
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
    alias: Optional[str] = None,
    typeref: Optional[irast.TypeRef] = None,
    lateral: bool = False,
    colnames: Optional[List[str]] = None,
    ctx: Optional[context.CompilerContextLevel] = None,
    env: Optional[context.Environment] = None,
) -> pgast.PathRangeVar:
    if ctx:
        env = ctx.env
    assert env

    rvar: pgast.PathRangeVar

    if colnames is None:
        colnames = []

    if isinstance(rel, pgast.Query):
        alias = alias or env.aliases.get(rel.name or 'q')

        rvar = pgast.RangeSubselect(
            subquery=rel,
            alias=pgast.Alias(aliasname=alias, colnames=colnames),
            lateral=lateral,
            typeref=typeref,
        )
    else:
        alias = alias or env.aliases.get(rel.name or '')

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
        dml_stmts: Iterable[irast.MutatingLikeStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:
    entry = (op, rel, path_id)
    dml_stmts2 = dml_stmts if dml_stmts else (None,)
    # If there is a "global" overlay, and there is none for the
    # current statements, use it as the base. This is important for
    # not losing track of the global environment in triggers.
    root = ctx.rel_overlays.type.get(None, immu.Map())
    for dml_stmt in dml_stmts2:
        ds_overlays = ctx.rel_overlays.type.get(dml_stmt, root)
        overlays = ds_overlays.get(typeid, ())
        if entry not in overlays:
            ds_overlays = ds_overlays.set(typeid, overlays + (entry,))
            ctx.rel_overlays.type = (
                ctx.rel_overlays.type.set(dml_stmt, ds_overlays))


def add_type_rel_overlay(
        typeref: irast.TypeRef,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        stop_ref: Optional[irast.TypeRef]=None,
        dml_stmts: Iterable[irast.MutatingLikeStmt] = (),
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
    dml_source: Optional[irast.MutatingLikeStmt]=None,
    ctx: context.CompilerContextLevel,
) -> tuple[context.OverlayEntry, ...]:
    if typeref.material_type is not None:
        typeref = typeref.material_type

    if dml_source not in ctx.rel_overlays.type:
        return ()
    else:
        return ctx.rel_overlays.type[dml_source].get(typeref.id, ())


def reuse_type_rel_overlays(
    *,
    dml_stmts: Iterable[irast.MutatingLikeStmt] = (),
    dml_source: irast.MutatingLikeStmt,
    ctx: context.CompilerContextLevel,
) -> None:
    """Update type rel overlays when a DML statement is reused.

    When a WITH bound DML is used, we need to add it (and all of its
    nested overlays) as an overlay for all the enclosing DML
    statements.
    """
    ref_overlays = ctx.rel_overlays.type.get(dml_source, immu.Map())
    for tid, overlays in ref_overlays.items():
        for op, rel, path_id in overlays:
            _add_type_rel_overlay(
                tid, op, rel, dml_stmts=dml_stmts, path_id=path_id, ctx=ctx
            )
    ptr_overlays = ctx.rel_overlays.ptr.get(dml_source, immu.Map())
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
        dml_stmts: Iterable[irast.MutatingLikeStmt] = (),
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> None:

    entry = (op, rel, path_id)
    dml_stmts2 = dml_stmts if dml_stmts else (None,)
    key = typeid, ptrref_name
    # If there is a "global" overlay, and there is none for the
    # current statements, use it as the base. This is important for
    # not losing track of the global environment in triggers.
    root = ctx.rel_overlays.ptr.get(None, immu.Map())
    for dml_stmt in dml_stmts2:
        ds_overlays = ctx.rel_overlays.ptr.get(dml_stmt, root)
        overlays = ds_overlays.get(key, ())
        if entry not in overlays:
            ds_overlays = ds_overlays.set(key, overlays + (entry,))
            ctx.rel_overlays.ptr = (
                ctx.rel_overlays.ptr.set(dml_stmt, ds_overlays))


def add_ptr_rel_overlay(
        ptrref: irast.PointerRef,
        op: str,
        rel: Union[pgast.BaseRelation, pgast.CommonTableExpr], *,
        dml_stmts: Iterable[irast.MutatingLikeStmt] = (),
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
    dml_source: Optional[irast.MutatingLikeStmt]=None,
    ctx: context.CompilerContextLevel,
) -> tuple[context.OverlayEntry, ...]:
    typeref = ptrref.out_source.real_material_type
    if dml_source not in ctx.rel_overlays.ptr:
        return ()
    else:
        key = typeref.id, ptrref.shortname.name
        return ctx.rel_overlays.ptr[dml_source].get(key, ())
