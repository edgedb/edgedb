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


import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import links as s_links
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import common
from edb.server.pgsql import types as pg_types

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
        s_paths = set()
        if hasattr(source_q, 'value_scope'):
            s_paths.update((p, 'value') for p in source_q.value_scope)
        if hasattr(source_q, 'path_outputs'):
            s_paths.update(source_q.path_outputs)
        if hasattr(source_q, 'path_namespace'):
            s_paths.update(source_q.path_namespace)
        if hasattr(source_q, 'path_rvar_map'):
            s_paths.update(source_q.path_rvar_map)

        view_path_id_map = getattr(source_q, 'view_path_id_map', {})

        for path_id, aspect in s_paths:
            path_id = pathctx.reverse_map_path_id(path_id, view_path_id_map)

            if path_id in source.query.path_id_mask:
                continue

            rvar = maybe_get_path_rvar(target, path_id, aspect=aspect, ctx=ctx)
            if rvar is None:
                pathctx.put_path_rvar(
                    target, path_id, source, aspect=aspect, env=ctx.env)


def find_rvar(
        stmt: pgast.Query, *,
        source_stmt: typing.Optional[pgast.Query]=None,
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> \
        typing.Optional[pgast.BaseRangeVar]:
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
        stmt: pgast.Query, rvar: pgast.BaseRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
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
    if path_id.is_objtype_path():
        aspects = ['source', 'value']
    else:
        aspects = ['value']

    return include_specific_rvar(
        stmt, rvar=rvar, path_id=path_id,
        overwrite_path_rvar=overwrite_path_rvar,
        aspects=aspects, ctx=ctx)


def include_specific_rvar(
        stmt: pgast.Query, rvar: pgast.BaseRangeVar,
        path_id: irast.PathId, *,
        overwrite_path_rvar: bool=False,
        aspects: typing.Iterable[str]=('value',),
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
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
        rel_join(stmt, rvar, ctx=ctx)
        # Make sure that the path namespace of *cte* is mapped
        # onto the path namespace of *stmt*.
        pull_path_namespace(target=stmt, source=rvar, ctx=ctx)

    for aspect in aspects:
        if overwrite_path_rvar:
            pathctx.put_path_rvar(
                stmt, path_id, rvar, aspect=aspect, env=ctx.env)
        else:
            pathctx.put_path_rvar_if_not_exists(
                stmt, path_id, rvar, aspect=aspect, env=ctx.env)

    return rvar


def has_rvar(
        stmt: pgast.Query, rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContextLevel) -> bool:
    while stmt is not None:
        if pathctx.has_rvar(stmt, rvar, env=ctx.env):
            return True
        stmt = ctx.rel_hierarchy.get(stmt)
    return False


def _get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    qry = stmt
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

    raise LookupError(
        f'there is no range var for {path_id} in {stmt}')


def get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    rvar, _ = _get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    return rvar


def get_path_var(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.OutputVar:
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
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    try:
        return get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    except LookupError:
        return None


def maybe_get_path_var(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, ctx: context.CompilerContextLevel) -> pgast.OutputVar:
    try:
        rvar, path_id = _get_path_rvar(stmt, path_id, aspect=aspect, ctx=ctx)
    except LookupError:
        return None
    else:
        try:
            return pathctx.get_rvar_path_var(
                rvar, path_id, aspect=aspect, env=ctx.env)
        except LookupError:
            return None


def new_empty_rvar(
        ir_set: irast.EmptySet, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    nullrel = pgast.NullRelation(path_id=ir_set.path_id)
    rvar = dbobj.rvar_for_rel(nullrel, env=ctx.env)
    pathctx.put_rvar_path_bond(rvar, ir_set.path_id)
    rvar.value_scope.add(ir_set.path_id)
    return rvar


def new_root_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    if not isinstance(ir_set.scls, s_objtypes.ObjectType):
        raise ValueError('cannot create root rvar for non-object path')

    set_rvar = dbobj.range_for_set(ir_set, env=ctx.env)
    pathctx.put_rvar_path_bond(set_rvar, ir_set.path_id)
    set_rvar.value_scope.add(ir_set.path_id)

    if ir_set.rptr and ir_set.rptr.is_inbound:
        ptrcls = ir_set.rptr.ptrcls
        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)

        if ptr_info.table_type == 'ObjectType':
            # Inline link
            rref = dbobj.get_column(
                None, ptr_info.column_name,
                nullable=not ptrcls.get_required(ctx.env.schema))
            pathctx.put_rvar_path_bond(
                set_rvar, ir_set.path_id.src_path())
            pathctx.put_rvar_path_output(
                set_rvar, ir_set.path_id.src_path(),
                aspect='identity', var=rref, env=ctx.env)

    return set_rvar


def new_poly_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:

    rvar = new_root_rvar(ir_set, ctx=ctx)
    pathctx.put_rvar_path_bond(rvar, ir_set.path_id.src_path())
    return rvar


def new_pointer_rvar(
        ir_ptr: irast.Pointer, *,
        link_bias: bool=False,
        src_rvar: pgast.BaseRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:

    ptrcls = ir_ptr.ptrcls

    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=link_bias)

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
        src_rvar: pgast.BaseRangeVar,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    ptr_rel = pgast.SelectStmt()
    ptr_rvar = dbobj.rvar_for_rel(ptr_rel, lateral=lateral, env=ctx.env)
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
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    ptrcls = ir_ptr.ptrcls
    ptr_rvar = dbobj.range_for_pointer(ir_ptr, env=ctx.env)
    # Set up references according to the link direction.
    if isinstance(ptrcls, s_links.Link):
        # XXX: fix this once Properties are Sources
        src_ptr_info = pg_types.get_pointer_storage_info(
            ptrcls.getptr(ctx.env.schema, 'std::source'), resolve_type=False)
        src_col = src_ptr_info.column_name
    else:
        src_col = common.edgedb_name_to_pg_name('std::source')

    source_ref = dbobj.get_column(None, src_col, nullable=False)

    if isinstance(ptrcls, s_links.Link):
        # XXX: fix this once Properties are Sources
        tgt_ptr_info = pg_types.get_pointer_storage_info(
            ptrcls.getptr(ctx.env.schema, 'std::target'), resolve_type=False)
        tgt_col = tgt_ptr_info.column_name
    else:
        tgt_col = common.edgedb_name_to_pg_name('std::target')

    target_ref = dbobj.get_column(
        None, tgt_col,
        nullable=not ptrcls.get_required(ctx.env.schema))

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
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    if irutils.is_scalar_view_set(ir_set, schema=ctx.env.schema):
        ensure_bond_for_expr(ir_set, stmt, ctx=ctx)

    return dbobj.rvar_for_rel(stmt, lateral=lateral, env=ctx.env)


def new_static_class_rvar(
        ir_set: irast.Set, *,
        lateral: bool=True,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    set_rvar = new_root_rvar(ir_set, ctx=ctx)
    clsname = pgast.StringConstant(
        val=ir_set.rptr.source.scls.material_type(ctx.env.schema).get_name(
            ctx.env.schema))
    nameref = dbobj.get_column(
        set_rvar, common.edgedb_name_to_pg_name('schema::name'),
        nullable=False)
    condition = astutils.new_binop(nameref, clsname, op='=')
    substmt = pgast.SelectStmt()
    include_rvar(substmt, set_rvar, ir_set.path_id, ctx=ctx)
    substmt.where_clause = astutils.extend_binop(
        substmt.where_clause, condition)
    return new_rel_rvar(ir_set, substmt, ctx=ctx)


def semi_join(
        stmt: pgast.Query, ir_set: irast.Set, src_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    """Join an IR Set using semi-join."""
    rptr = ir_set.rptr
    ptrcls = rptr.ptrcls
    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=False)
    is_inline_ref = ptr_info.table_type == 'ObjectType'

    # Target set range.
    set_rvar = new_root_rvar(ir_set, ctx=ctx)

    # Link range.
    map_rvar = new_pointer_rvar(rptr, src_rvar=src_rvar, ctx=ctx)

    # Target identity in the target range.
    if rptr.is_inbound and is_inline_ref:
        tgt_pid = ir_set.path_id.extend(ptrcls, schema=ctx.env.schema)
    else:
        tgt_pid = ir_set.path_id

    tgt_ref = pathctx.get_rvar_path_identity_var(
        set_rvar, tgt_pid, env=ctx.env)

    include_rvar(
        ctx.rel, map_rvar,
        path_id=ir_set.path_id.ptr_path(), ctx=ctx)

    pathctx.get_path_identity_output(ctx.rel, ir_set.path_id, env=ctx.env)

    cond = astutils.new_binop(tgt_ref, ctx.rel, 'IN')
    stmt.where_clause = astutils.extend_binop(
        stmt.where_clause, cond)

    return set_rvar


def ensure_source_rvar(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) \
        -> pgast.BaseRangeVar:

    rvar = maybe_get_path_rvar(stmt, ir_set.path_id, aspect='source', ctx=ctx)
    if rvar is None:
        scope_stmt = maybe_get_scope_stmt(ir_set.path_id, ctx=ctx)
        if scope_stmt is None:
            scope_stmt = ctx.rel
        rvar = new_root_rvar(ir_set, ctx=ctx)
        include_rvar(scope_stmt, rvar, path_id=ir_set.path_id, ctx=ctx)

    return rvar


def ensure_bond_for_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, type='int',
        ctx: context.CompilerContextLevel) -> None:
    if ir_set.path_id.is_objtype_path():
        # ObjectTypes have inherent identity
        return

    ensure_transient_identity_for_set(ir_set, stmt, type=type, ctx=ctx)


def ensure_transient_identity_for_set(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel, type='int') -> None:

    if type == 'uuid':
        id_expr = pgast.FuncCall(
            name=('edgedb', 'uuid_generate_v1mc',),
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


def get_scope(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> \
        typing.Optional[irast.ScopeTreeNode]:
    if ir_set.path_scope_id is None:
        return None
    else:
        return ctx.scope_tree.root.find_by_unique_id(ir_set.path_scope_id)


def update_scope(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:

    scope_tree = get_scope(ir_set, ctx=ctx)
    if scope_tree is None:
        return

    ctx.scope_tree = scope_tree
    ctx.path_scope = ctx.path_scope.new_child()
    ctx.path_scope.update({p.path_id: stmt for p in scope_tree.path_children})

    if (isinstance(ir_set.expr, irast.Stmt) and
            ir_set.expr.iterator_stmt is not None):
        iter_path_id = ir_set.expr.iterator_stmt.path_id
    else:
        iter_path_id = None

    for child_path in scope_tree.get_all_paths():
        parent_scope = scope_tree.parent
        if ((parent_scope is None or
                not parent_scope.is_visible(child_path)) and
                child_path != iter_path_id):
            stmt.path_id_mask.add(child_path)


def get_scope_stmt(
        path_id: irast.PathId, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    stmt = ctx.path_scope.get(path_id)
    if stmt is None and path_id.is_ptr_path():
        stmt = ctx.path_scope.get(path_id.tgt_path())
    if stmt is None:
        raise LookupError(f'cannot find scope statement for {path_id}')
    return stmt


def maybe_get_scope_stmt(
        path_id: irast.PathId, *,
        ctx: context.CompilerContextLevel) -> typing.Optional[pgast.Query]:
    try:
        return get_scope_stmt(path_id, ctx=ctx)
    except LookupError:
        return None


def rel_join(
        query: pgast.Query, right_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContextLevel) -> None:
    condition = None

    for path_id in right_rvar.path_scope:
        lref = maybe_get_path_var(query, path_id, aspect='identity', ctx=ctx)
        if lref is None:
            lref = maybe_get_path_var(query, path_id, aspect='value', ctx=ctx)
        if lref is None:
            continue

        rref = pathctx.get_rvar_path_identity_var(
            right_rvar, path_id, env=ctx.env)

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
        if join_type == 'left':
            right_rvar.nullable = True

    if not right_rvar.is_distinct:
        query.is_distinct = False
