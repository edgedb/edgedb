##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Helpers to manage statement path contexts."""


import functools
import typing

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj


class LazyPathVarRef:
    def __init__(self, getter, env, source, path_id, *,
                 grouped=False, weak=False):
        self.env = env
        self.path_id = path_id
        self.source = source
        self.getter = getter
        self.grouped = grouped
        self.weak = weak
        self._ref = None

    def get(self):
        if self._ref is None:
            ref = self.getter(self.env, self.source, self.path_id)
            if self.grouped or self.weak:
                ref = pgast.ColumnRef(
                    name=ref.name,
                    nullable=ref.nullable,
                    grouped=self.grouped,
                    weak=self.weak
                )
            self._ref = ref

        return self._ref

    def __repr__(self):
        return f'<LazyPathVarRef {self.path_id} source={self.source!r}>'


def get_id_path_id(
        schema: s_schema.Schema,
        path_id: irast.PathId) -> irast.PathId:
    """For PathId representing an object, return (PathId).(std::id)."""
    assert isinstance(path_id[-1], s_concepts.Concept)
    return path_id.extend(
        schema.get('std::id'),
        s_pointers.PointerDirection.Outbound,
        schema.get('std::uuid'))


def get_canonical_path_id(
        schema: s_schema.Schema,
        path_id: irast.PathId) -> irast.PathId:
    """For a path id (PathId).(std::id) return PathId."""
    rptr = path_id.rptr(schema)
    if (rptr is not None and
            path_id.rptr_dir() == s_pointers.PointerDirection.Outbound and
            rptr.shortname == 'std::id'):
        return irast.PathId(path_id[:-2])
    else:
        return path_id


def proper_path_id(
        schema: s_schema.Schema,
        path_id: typing.Union[irast.PathId, irast.Set]) -> irast.PathId:
    """For a path id or Set return return its identity path id."""
    if isinstance(path_id, irast.Set):
        ir_set = path_id
        path_id = ir_set.path_id
        if not path_id:
            path_id = irast.PathId([ir_set.scls])

    if isinstance(path_id[-1], s_concepts.Concept):
        path_id = get_id_path_id(schema, path_id)

    return path_id


def map_path_id(
        path_id: irast.PathId,
        path_id_map: typing.Dict[irast.PathId, irast.PathId]) -> irast.PathId:
    for outer_id, inner_id in path_id_map.items():
        new_path_id = path_id.replace_prefix(outer_id, inner_id)
        if new_path_id != path_id:
            path_id = new_path_id
            break

    return path_id


def reverse_map_path_id(
        path_id: irast.PathId,
        path_id_map: typing.Dict[irast.PathId, irast.PathId]) -> irast.PathId:
    for outer_id, inner_id in path_id_map.items():
        new_path_id = path_id.replace_prefix(inner_id, outer_id)
        if new_path_id != path_id:
            path_id = new_path_id
            break

    return path_id


def get_path_var(
        env: context.Environment, rel: pgast.Query,
        path_id: irast.PathId, *, raw: bool=True) -> pgast.ColumnRef:
    """Return ColumnRef for a given *path_id* in a given *rel*."""
    if isinstance(rel, pgast.CommonTableExpr):
        rel = rel.query

    if isinstance(path_id[-1], s_concepts.Concept):
        path_id = get_id_path_id(env.schema, path_id)

    if (path_id, raw) in rel.path_namespace:
        return rel.path_namespace[path_id, raw]

    if rel.as_type:
        # Relation represents the result of a type filter ([IS Type]).
        near_endpoint = rel.as_type[0]
    else:
        near_endpoint = None

    ptrcls = path_id.rptr(env.schema, near_endpoint)
    if ptrcls is not None:
        ptrname = ptrcls.shortname
        alias = common.edgedb_name_to_pg_name(ptrname)
    else:
        ptrname = None
        alias = common.edgedb_name_to_pg_name(path_id[-1].name)

    if astutils.is_set_op_query(rel):
        alias = env.aliases.get(alias)

        cb = functools.partial(
            get_path_output_or_null,
            env=env,
            path_id=path_id,
            alias=alias,
            raw=raw)

        astutils.for_each_query_in_set(rel, cb)
        put_path_output(env, rel, path_id, alias, raw=raw)
        return dbobj.get_column(None, alias)

    ptr_info = parent_ptr_info = parent_ptrcls = parent_dir = None
    if ptrcls is not None:
        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)

        parent_ptrcls = irast.PathId(path_id[:-2]).rptr(env.schema)
        if parent_ptrcls is not None:
            parent_ptr_info = pg_types.get_pointer_storage_info(
                parent_ptrcls, resolve_type=False, link_bias=False)

            parent_dir = path_id[:-2].rptr_dir()

    if isinstance(ptrcls, s_lprops.LinkProperty):
        if ptr_info.table_type == 'link':
            # This is a regular linkprop, step back to link rvar
            src_path_id = path_id[:-3]
        else:
            # This is a link prop that is stored in source rel,
            # step back to link source rvar.
            src_path_id = path_id[:-4]
    else:
        if ptrcls is not None:
            if (parent_ptr_info is not None and
                    parent_ptr_info.table_type == 'concept' and
                    ptrname == 'std::id' and
                    parent_dir == s_pointers.PointerDirection.Outbound):
                # Link to object with target stored directly in
                # source table.
                src_path_id = path_id[:-4]
                ptr_info = parent_ptr_info
            else:
                # Regular atomic link, step back to link source rvar.
                src_path_id = path_id[:-2]
        else:
            if len(path_id) > 1:
                # Couldn't resolve a specific pointer for path_id,
                # assume this is a valid path with type filter.
                src_path_id = path_id[:-2]
            else:
                # This is an atomic set derived from an expression.
                src_path_id = path_id

    rel_rvar = rel.path_rvar_map.get(path_id)
    if rel_rvar is None:
        rel_rvar = rel.path_rvar_map.get(src_path_id)
        if rel_rvar is None:
            raise LookupError(
                f'cannot find source range for '
                f'path {src_path_id} in {rel}')

    colname = None

    if should_recurse_into_rvar(rel_rvar):
        source_rel = rel_rvar.query

        drilldown_path_id = map_path_id(path_id, rel.view_path_id_map)

        if source_rel in env.root_rels:
            assert len(source_rel.path_bonds) == 1
            if not drilldown_path_id.is_concept_path():
                outer_path_id = drilldown_path_id.src_path()
            else:
                outer_path_id = drilldown_path_id

            path_id_map = {
                outer_path_id: next(iter(source_rel.path_bonds))
            }

            drilldown_path_id = map_path_id(
                drilldown_path_id, path_id_map)

        colname = get_path_output(env, source_rel, drilldown_path_id, raw=raw)

    else:
        if not isinstance(ptrcls, s_lprops.LinkProperty):
            path_src = path_id[-3]
            ptr_src = ptrcls.source
            src_path_id = irast.PathId(path_id[:-2])
            if path_src != ptr_src and not path_src.issubclass(ptr_src):
                poly_rvar = dbobj.range_for_concept(env, ptr_src, src_path_id)
                poly_rvar.nullable = True
                poly_rvar.path_bonds.add(src_path_id)
                rel_join(env, rel, poly_rvar, type='left')

                rel_rvar = poly_rvar

        colname = ptr_info.column_name

    fieldref = dbobj.get_column(rel_rvar, colname)

    canonical_path_id = get_canonical_path_id(env.schema, path_id)

    rel.path_namespace[path_id, raw] = fieldref
    rel.path_namespace[canonical_path_id, raw] = fieldref

    return fieldref


def should_recurse_into_rvar(
        rvar: pgast.BaseRangeVar) -> bool:
    return (
        isinstance(rvar, pgast.RangeSubselect) or
        isinstance(rvar.relation, pgast.CommonTableExpr)
    )


def maybe_get_path_var(
        env: context.Environment, rel: pgast.Query,
        path_id: irast.PathId) -> typing.Optional[pgast.ColumnRef]:
    try:
        return get_path_var(env, rel, path_id)
    except LookupError:
        return None


def put_path_bond(
        stmt: pgast.Query, path_id: irast.PathId):
    if isinstance(path_id[-1], s_concepts.Concept):
        # Only Concept paths form bonds.
        stmt.path_bonds.add(path_id)


def get_path_output_alias(
        env: context.Environment, path_id: irast.PathId) -> str:
    rptr = path_id.rptr(env.schema)
    if rptr is not None:
        ptrname = rptr.shortname
        alias = env.aliases.get(ptrname.name)
    elif isinstance(path_id[-1], s_obj.Collection):
        alias = env.aliases.get(path_id[-1].schema_name)
    else:
        alias = env.aliases.get(path_id[-1].name.name)

    return alias


def get_rvar_path_var(
        env: context.Environment, rvar: pgast.BaseRangeVar,
        path_id: irast.PathId, raw: bool=True):
    path_id = proper_path_id(env.schema, path_id)

    if isinstance(rvar.query, pgast.Relation):
        if isinstance(path_id[-1], s_concepts.Concept):
            path_id = get_id_path_id(env.schema, path_id)
        ptr = path_id.rptr(env.schema)
        name = common.edgedb_name_to_pg_name(ptr.shortname)
    else:
        name = get_path_output(env, rvar.query, path_id, raw=raw)

    return dbobj.get_column(rvar, name)


def put_path_rvar(
        env: context.Environment, stmt: pgast.Query, path_id: irast.PathId,
        rvar: pgast.BaseRangeVar):
    assert path_id
    path_id = get_canonical_path_id(
        env.schema, proper_path_id(env.schema, path_id))
    stmt.path_rvar_map[path_id] = rvar


def get_path_rvar(
        env: context.Environment, stmt: pgast.Query,
        path_id: irast.PathId) -> pgast.BaseRangeVar:
    path_id = get_canonical_path_id(
        env.schema, proper_path_id(env.schema, path_id))
    return stmt.path_rvar_map[path_id]


def get_path_output(
        env: context.Environment, rel: pgast.Query, path_id: irast.PathId, *,
        alias: str=None, raw: bool=False) -> str:

    path_id = proper_path_id(env.schema, path_id)

    result = rel.path_outputs.get((path_id, raw))
    if result is not None:
        return result

    ref = get_path_var(env, rel, path_id, raw=raw)
    set_op = getattr(rel, 'op', None)
    if set_op is not None:
        alias = ref.name[0]

    if alias is None:
        alias = get_path_output_alias(env, path_id)

    if set_op is None:
        restarget = pgast.ResTarget(name=alias, val=ref)
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)

    put_path_output(env, rel, path_id, alias, raw=raw)

    return alias


def get_path_output_or_null(
        rel: pgast.Query, path_id: irast.PathId,
        alias: str, *, raw: bool=False, env: context.Environment) -> str:
    try:
        alias = get_path_output(env, rel, path_id, alias=alias, raw=raw)
    except LookupError:
        restarget = pgast.ResTarget(
            name=alias,
            val=pgast.Constant(val=None))
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)

    return alias


def put_path_output(
        env: context.Environment, stmt: pgast.Query, path_id: irast.PathId,
        alias: str, *, raw: bool=False):
    path_id = proper_path_id(env.schema, path_id)
    canonical_path_id = get_canonical_path_id(env.schema, path_id)
    stmt.path_outputs[path_id, raw] = alias
    stmt.path_outputs[canonical_path_id, raw] = alias
    if not isinstance(canonical_path_id[-1], s_concepts.Concept) and not raw:
        stmt.path_outputs[path_id, True] = alias
        stmt.path_outputs[canonical_path_id, True] = alias


def full_inner_bond_condition(
        env: context.Environment,
        query: pgast.Query,
        parent_path_bonds: typing.Dict[irast.PathId, LazyPathVarRef]):
    condition = None

    for path_id in query.path_bonds:
        rptr = path_id.rptr(env.schema)
        if rptr and rptr.singular(path_id.rptr_dir()):
            continue

        rref = parent_path_bonds.get(path_id)
        if rref is None:
            continue

        rref = rref.get()

        lref = get_path_var(env, query, path_id)

        if rref.grouped:
            op = '='
            rref = pgast.SubLink(
                type=pgast.SubLinkType.ANY,
                expr=pgast.FuncCall(
                    name=('array_agg',),
                    args=[rref]
                )
            )
        else:
            if lref.nullable or rref.nullable:
                op = 'IS NOT DISTINCT FROM'
            else:
                op = '='

        path_cond = astutils.new_binop(lref, rref, op=op)
        condition = astutils.extend_binop(condition, path_cond)

    return condition


def full_outer_bond_condition(
        env: context.Environment, query: pgast.Query,
        right_rvar: pgast.BaseRangeVar) -> typing.Optional[pgast.Expr]:
    condition = None

    for path_id in right_rvar.path_bonds:
        rptr = path_id.rptr(env.schema)
        if rptr and rptr.singular(path_id.rptr_dir()):
            continue

        lref = maybe_get_path_var(env, query, path_id)
        if lref is None:
            continue

        rref = get_rvar_path_var(env, right_rvar, path_id)

        if lref.nullable or rref.nullable:
            op = 'IS NOT DISTINCT FROM'
        else:
            op = '='

        path_cond = astutils.new_binop(lref, rref, op=op)
        condition = astutils.extend_binop(condition, path_cond)

    return condition


def rel_join(
        env: context.Environment,
        query: pgast.Query, right_rvar: pgast.BaseRangeVar,
        type: str='inner', front: bool=False):
    if not query.from_clause:
        query.from_clause.append(right_rvar)
        return

    condition = full_outer_bond_condition(env, query, right_rvar)

    if type == 'where':
        # A "where" JOIN is equivalent to an INNER join with
        # its condition moved to a WHERE clause.
        if condition is not None:
            query.where_clause = astutils.extend_binop(
                query.where_clause, condition)

        if front:
            query.from_clause.insert(0, right_rvar)
        else:
            query.from_clause.append(right_rvar)
    else:
        if condition is None:
            type = 'cross'

        if front:
            larg = right_rvar
            rarg = query.from_clause[0]
        else:
            larg = query.from_clause[0]
            rarg = right_rvar

        query.from_clause[0] = pgast.JoinExpr(
            type=type, larg=larg, rarg=rarg, quals=condition)
        if type == 'left':
            right_rvar.nullable = True


def join_mapping_rel(
        env: context.Environment, *,
        stmt: pgast.Query, set_rvar: pgast.BaseRangeVar,
        ir_set: irast.Set, map_join_type: str='inner', semi: bool=False):
    link = ir_set.rptr
    if isinstance(link.ptrcls, s_lprops.LinkProperty):
        link = link.source.rptr
        link_path_id = ir_set.path_id[:-3]
    else:
        link_path_id = ir_set.path_id[:-1]

    try:
        # The same link map must not be joined more than once,
        # otherwise the cardinality of the result set will be wrong.
        #
        map_rvar = stmt.path_rvar_map[link_path_id]
        map_join = stmt.ptr_join_map[link_path_id]
    except KeyError:
        map_rvar = dbobj.range_for_pointer(env, link)
        map_join = None
        if map_join_type == 'left':
            map_rvar.nullable = True

    # Set up references according to link direction
    #
    src_col = common.edgedb_name_to_pg_name('std::source')
    source_ref = dbobj.get_column(map_rvar, src_col)

    tgt_col = common.edgedb_name_to_pg_name('std::target')
    target_ref = dbobj.get_column(map_rvar, tgt_col)

    valent_bond = get_path_var(env, stmt, link.source.path_id)
    forward_bond = astutils.new_binop(valent_bond, source_ref, op='=')
    backward_bond = astutils.new_binop(valent_bond, target_ref, op='=')

    if link.direction == s_pointers.PointerDirection.Inbound:
        map_join_cond = backward_bond
        far_ref = source_ref
    else:
        map_join_cond = forward_bond
        far_ref = target_ref

    if map_join is None:
        # Join link relation to source relation
        #
        map_join = pgast.JoinExpr(
            larg=stmt.from_clause[0],
            rarg=map_rvar,
            type=map_join_type,
            quals=map_join_cond
        )

        if not semi:
            put_path_rvar(env, stmt, link_path_id, map_rvar)
            stmt.ptr_join_map[link_path_id] = map_join

    if isinstance(ir_set.scls, s_concepts.Concept):
        if map_join_type == 'left':
            set_rvar.nullable = True

        target_range_bond = get_rvar_path_var(
            env, set_rvar, ir_set.path_id)

        if link.direction == s_pointers.PointerDirection.Inbound:
            map_tgt_ref = source_ref
        else:
            map_tgt_ref = target_ref

        if semi:
            source_sq = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=map_tgt_ref,
                        name=env.aliases.get('t')
                    )
                ],
                from_clause=[
                    map_join
                ]
            )

            cond = astutils.new_binop(
                target_range_bond,
                source_sq,
                'IN'
            )

            stmt.where_clause = astutils.extend_binop(stmt.where_clause, cond)

            map_join = set_rvar

        else:
            # Join the target relation.
            cond_expr = astutils.new_binop(
                map_tgt_ref, target_range_bond, op='=')

            pre_map_join = map_join.copy()
            new_map_join = pgast.JoinExpr(
                type=map_join_type,
                larg=pre_map_join,
                rarg=set_rvar,
                quals=cond_expr)
            map_join.copyfrom(new_map_join)

    stmt.from_clause[:] = [map_join]

    return map_rvar, far_ref


def join_class_rel(
        env: context.Environment, *,
        stmt: pgast.Query, set_rvar: pgast.BaseRangeVar,
        ir_set: irast.Set):
    fromexpr = stmt.from_clause[0]

    nref = dbobj.get_column(
        set_rvar, common.edgedb_name_to_pg_name('schema::name'))

    val = pgast.Constant(
        val=ir_set.rptr.source.scls.name
    )

    cond_expr = astutils.new_binop(nref, val, op='=')

    stmt.from_clause[0] = pgast.JoinExpr(
        type='inner',
        larg=fromexpr,
        rarg=set_rvar,
        quals=cond_expr)


def join_inline_rel(
        env: context.Environment, *,
        stmt: pgast.Query, set_rvar: pgast.BaseRangeVar,
        ir_set: irast.Set, back_id_col: str,
        join_type='inner'):
    if ir_set.rptr.direction == s_pointers.PointerDirection.Inbound:
        id_col = back_id_col
        src_ref = get_path_var(env, stmt, ir_set.rptr.source.path_id)
    else:
        id_col = common.edgedb_name_to_pg_name('std::id')
        src_ref = get_path_var(env, stmt, ir_set.path_id)

    tgt_ref = dbobj.get_column(set_rvar, id_col)

    fromexpr = stmt.from_clause[0]

    cond_expr = astutils.new_binop(src_ref, tgt_ref, op='=')

    stmt.from_clause[0] = pgast.JoinExpr(
        type=join_type,
        larg=fromexpr,
        rarg=set_rvar,
        quals=cond_expr)
