##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Helpers to manage statement path contexts."""


import functools
import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import schema as s_schema
from edgedb.lang.schema import sources as s_sources  # NOQA

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import output


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
            ref = self.getter(self.source, self.path_id, env=self.env)
            if self.grouped or self.weak:
                ref = pgast.ColumnRef(
                    name=ref.name,
                    nullable=ref.nullable,
                    optional=ref.optional,
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
    source: s_sources.Source = path_id[-1]
    assert isinstance(source, s_concepts.Concept)
    return path_id.extend(
        source.resolve_pointer(schema, 'std::id'),
        s_pointers.PointerDirection.Outbound,
        schema.get('std::uuid'))


def get_canonical_path_id(
        schema: s_schema.Schema,
        path_id: irast.PathId) -> irast.PathId:
    """For a path id (PathId).(std::id) return PathId."""
    rptr = path_id.rptr()
    if (rptr is not None and
            path_id.rptr_dir() == s_pointers.PointerDirection.Outbound and
            rptr.shortname == 'std::id'):
        return path_id[:-2]
    else:
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
        path_id: irast.PathId, *, aspect: str) -> pgast.OutputVar:
    """Return ColumnRef for a given *path_id* in a given *rel*."""
    if isinstance(rel, pgast.CommonTableExpr):
        rel = rel.query

    if (path_id, aspect) in rel.path_namespace:
        return rel.path_namespace[path_id, aspect]

    if isinstance(path_id[-1], s_concepts.Concept):
        ptr_path_id = get_id_path_id(env.schema, path_id)
    else:
        ptr_path_id = path_id

    ptrcls = ptr_path_id.rptr()
    if ptrcls is not None:
        ptrname = ptrcls.shortname
    else:
        ptrname = None

    if astutils.is_set_op_query(rel):
        cb = functools.partial(
            get_path_output_or_null,
            env=env,
            path_id=path_id,
            aspect=aspect)

        outputs = astutils.for_each_query_in_set(rel, cb)

        first = None
        optional = False
        all_null = True

        for colref, is_null in outputs:
            if first is None:
                first = colref
            if is_null:
                optional = True
            else:
                all_null = False

        if all_null:
            raise LookupError(
                f'cannot find refs for '
                f'path {path_id} {aspect} in {rel}')

        fieldref = dbobj.get_rvar_fieldref(None, first)
        # Path vars produced by UNION expressions can be "optional",
        # i.e the record is accepted as-is when such var is NULL.
        # This is necessary to correctly join heterogeneous UNIONs.
        fieldref.optional = optional
        rel.path_namespace[path_id, aspect] = fieldref
        return fieldref

    ptr_info = parent_ptr_info = parent_ptrcls = parent_dir = None
    if ptrcls is not None:
        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)

        parent_ptrcls = ptr_path_id[:-2].rptr()
        if parent_ptrcls is not None:
            parent_ptr_info = pg_types.get_pointer_storage_info(
                parent_ptrcls, resolve_type=False, link_bias=False)

            parent_dir = ptr_path_id[:-2].rptr_dir()

    if isinstance(ptrcls, s_lprops.LinkProperty):
        if ptr_info.table_type == 'link':
            # This is a regular linkprop, step back to link rvar
            src_path_id = ptr_path_id[:-3]
        else:
            # This is a link prop that is stored in source rel,
            # step back to link source rvar.
            src_path_id = ptr_path_id[:-4]
    else:
        if ptrcls is not None:
            if (parent_ptr_info is not None and
                    parent_ptr_info.table_type == 'concept' and
                    ptrname == 'std::id' and
                    parent_dir == s_pointers.PointerDirection.Outbound):
                # Link to object with target stored directly in
                # source table.
                src_path_id = ptr_path_id[:-4]
                ptr_info = parent_ptr_info
            elif ptr_info.table_type == 'link':
                # *-to-many atom link.
                src_path_id = ptr_path_id[:-1]
            else:
                # Regular atomic link, step back to link source rvar.
                src_path_id = ptr_path_id[:-2]
        else:
            if len(path_id) > 1:
                # Couldn't resolve a specific pointer for path_id,
                # assume this is a valid path with type filter.
                src_path_id = ptr_path_id[:-2]
            else:
                # This is an atomic set derived from an expression.
                src_path_id = ptr_path_id

    rel_rvar = rel.path_rvar_map.get(src_path_id)
    if rel_rvar is None:
        rel_rvar = rel.path_rvar_map.get(path_id)
        if rel_rvar is None:
            raise LookupError(
                f'cannot find source range for '
                f'path {src_path_id} {aspect} in {rel}')

    if should_recurse_into_rvar(rel_rvar):
        source_rel = rel_rvar.query

        drilldown_path_id = map_path_id(path_id, rel.view_path_id_map)

        if source_rel in env.root_rels and len(source_rel.path_scope) == 1:
            if not drilldown_path_id.is_concept_path() and ptrcls is not None:
                outer_path_id = drilldown_path_id.src_path()
            else:
                outer_path_id = drilldown_path_id

            path_id_map = {
                outer_path_id: next(iter(source_rel.path_scope))
            }

            drilldown_path_id = map_path_id(
                drilldown_path_id, path_id_map)

        outvar = get_path_output(
            source_rel, drilldown_path_id, aspect=aspect, env=env)

    elif ptrcls is None:
        # At this point we cannot continue without a resolved pointer.
        # This is actually fine for leafs in UNION operations.
        raise LookupError(
            f'{path_id} does not exist for {rel}'
        )

    else:
        outvar = pgast.ColumnRef(name=[ptr_info.column_name])

    if is_relation_rvar(rel_rvar) and aspect not in {'identity', 'value'}:
        raise LookupError(
            f'{path_id} {aspect} is not defined in {rel}'
        )

    fieldref = dbobj.get_rvar_fieldref(rel_rvar, outvar)
    rel.path_namespace[path_id, aspect] = fieldref
    return fieldref


def get_path_identity_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_var(env, rel, path_id, aspect='identity')


def get_path_value_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_var(env, rel, path_id, aspect='value')


def should_recurse_into_rvar(
        rvar: pgast.BaseRangeVar) -> bool:
    return (
        isinstance(rvar, pgast.RangeSubselect) or
        (isinstance(rvar, pgast.RangeVar) and
            isinstance(rvar.relation, pgast.CommonTableExpr))
    )


def is_relation_rvar(
        rvar: pgast.BaseRangeVar) -> bool:
    return (
        isinstance(rvar, pgast.RangeVar) and
        isinstance(rvar.relation, pgast.Relation)
    )


def maybe_get_path_var(
        rel: pgast.Query, path_id: irast.PathId, *, aspect: str,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(env, rel, path_id, aspect=aspect)
    except LookupError:
        return None


def maybe_get_path_identity_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(env, rel, path_id, aspect='identity')
    except LookupError:
        return None


def maybe_get_path_value_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(env, rel, path_id, aspect='value')
    except LookupError:
        return None


def maybe_get_path_serialized_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(env, rel, path_id, aspect='serialized')
    except LookupError:
        return None


def put_path_var(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        aspect: str, force: bool=False, env: context.Environment) -> None:
    if (path_id, aspect) in rel.path_namespace and not force:
        raise KeyError(
            f'{aspect} of {path_id} is already present in {rel}')
    rel.path_namespace[path_id, aspect] = var


def put_path_identity_var(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        force: bool=False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='identity', force=force, env=env)


def put_path_value_var(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        force=False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='value', force=force, env=env)


def put_path_serialized_var(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        force=False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='serialized', force=force, env=env)


def put_path_value_var_if_not_exists(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        force=False, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect='value', force=force, env=env)
    except KeyError:
        pass


def put_path_bond(
        stmt: pgast.Query, path_id: irast.PathId):
    stmt.path_scope.add(path_id)


def get_path_output_alias(
        env: context.Environment, path_id: irast.PathId) -> str:
    rptr = path_id.rptr()
    if rptr is not None:
        ptrname = rptr.shortname
        alias = env.aliases.get(ptrname.name)
    elif isinstance(path_id[-1], s_obj.Collection):
        alias = env.aliases.get(path_id[-1].schema_name)
    else:
        alias = env.aliases.get(path_id[-1].name.name)

    return alias


def get_rvar_path_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> pgast.OutputVar:
    """Return ColumnRef for a given *path_id* in a given *range var*."""
    if isinstance(rvar.query, pgast.Relation):
        # Range is a regular table.
        if isinstance(path_id[-1], s_concepts.Concept):
            path_id = get_id_path_id(env.schema, path_id)
        ptr = path_id.rptr()
        name = common.edgedb_name_to_pg_name(ptr.shortname)
        outvar = pgast.ColumnRef(name=[name])
    else:
        # Range is another query.
        outvar = get_path_output(rvar.query, path_id, aspect=aspect, env=env)

    return dbobj.get_rvar_fieldref(rvar, outvar)


def get_rvar_path_identity_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    return get_rvar_path_var(rvar, path_id, aspect='identity', env=env)


def get_rvar_path_value_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    return get_rvar_path_var(rvar, path_id, aspect='value', env=env)


def put_path_rvar(
        env: context.Environment, stmt: pgast.Query, path_id: irast.PathId,
        rvar: pgast.BaseRangeVar):
    assert path_id
    stmt.path_rvar_map[path_id] = rvar


def maybe_get_path_rvar(
        env: context.Environment, stmt: pgast.Query,
        path_id: irast.PathId) -> typing.Optional[pgast.BaseRangeVar]:
    return stmt.path_rvar_map.get(path_id)


def _same_expr(expr1, expr2):
    if (isinstance(expr1, pgast.ColumnRef) and
            isinstance(expr2, pgast.ColumnRef)):
        return expr1.name == expr2.name
    else:
        return expr1 == expr2


def find_path_output(
        env: context.Environment, rel: pgast.Query,
        path_id: irast.PathId, ref: pgast.Base) -> str:
    for key, other_ref in rel.path_namespace.items():
        if _same_expr(other_ref, ref):
            return rel.path_outputs.get(key)


def get_path_output(
        rel: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> pgast.OutputVar:

    result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    ref = get_path_var(env, rel, path_id, aspect=aspect)

    other_output = find_path_output(env, rel, path_id, ref)
    if other_output is not None:
        rel.path_outputs[path_id, aspect] = other_output
        return other_output

    if isinstance(ref, pgast.TupleVar):
        elements = []
        for el in ref.elements:
            el_path_id = reverse_map_path_id(
                el.path_id, rel.view_path_id_map)
            element = get_path_value_output(rel, el_path_id, env=env)
            elements.append(pgast.TupleElement(
                path_id=el_path_id, name=element))
        result = pgast.TupleVar(elements=elements, named=ref.named)

    else:
        if astutils.is_set_op_query(rel):
            assert isinstance(ref, pgast.ColumnRef)
            result = dbobj.get_column(None, ref)
        else:
            alias = get_path_output_alias(env, path_id)

            restarget = pgast.ResTarget(name=alias, val=ref)
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

            if isinstance(ref, pgast.ColumnRef):
                nullable = ref.nullable
                optional = ref.optional
            else:
                nullable = None
                optional = None

            result = pgast.ColumnRef(
                name=[alias], nullable=nullable, optional=optional)

    rel.path_outputs[path_id, aspect] = result
    return result


def get_path_identity_output(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_output(rel, path_id, aspect='identity', env=env)


def get_path_value_output(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_output(rel, path_id, aspect='value', env=env)


def get_path_serialized_output(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    # Serialized output is a special case, we don't
    # want this behaviour to be recursive, so it
    # must be kept outside of get_path_output() generic.
    aspect = 'serialized'

    result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    ref = maybe_get_path_serialized_var(rel, path_id, env=env)

    if ref is None:
        ref = get_path_var(env, rel, path_id, aspect='value')

    if isinstance(ref, pgast.TupleVar):
        elements = []
        for el in ref.elements:
            el_path_id = reverse_map_path_id(
                el.path_id, rel.view_path_id_map)
            element = pgast.TupleElement(
                path_id=el_path_id,
                name=el.name,
                val=get_path_value_var(rel, el_path_id, env=env)
            )
            elements.append(element)

        r_expr = pgast.TupleVar(elements=elements, named=ref.named)

        alias = env.aliases.get('s')
        val = output.serialize_expr(r_expr, env=env)
        restarget = pgast.ResTarget(name=alias, val=val)
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)
    else:
        ref = output.serialize_expr(ref, env=env)
        alias = get_path_output_alias(env, path_id)

        restarget = pgast.ResTarget(name=alias, val=ref)
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)

    result = pgast.ColumnRef(name=[alias])
    rel.path_outputs[path_id, aspect] = result
    return result


def get_path_output_or_null(
        rel: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> \
        typing.Tuple[pgast.OutputVar, bool]:
    try:
        ref = get_path_output(rel, path_id, aspect=aspect, env=env)
        is_null = False
    except LookupError:
        alias = env.aliases.get('null')
        restarget = pgast.ResTarget(
            name=alias,
            val=pgast.Constant(val=None))
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)
        is_null = True
        ref = pgast.ColumnRef(name=[alias], nullable=True)

    return ref, is_null


def bond_condition(lref: pgast.ColumnRef, rref: pgast.ColumnRef) -> pgast.Base:
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

    if lref.optional:
        opt_cond = pgast.NullTest(arg=lref)
        path_cond = astutils.extend_binop(
            path_cond, opt_cond, op=ast.ops.OR)

    if rref.optional:
        opt_cond = pgast.NullTest(arg=rref)
        path_cond = astutils.extend_binop(
            path_cond, opt_cond, op=ast.ops.OR)

    return path_cond


def full_inner_bond_condition(
        env: context.Environment,
        query: pgast.Query,
        parent_path_scope_refs: typing.Dict[irast.PathId, LazyPathVarRef]):
    condition = None

    for path_id in query.path_scope:
        rptr = path_id.rptr()
        if rptr and rptr.singular(path_id.rptr_dir()):
            continue

        rref = parent_path_scope_refs.get(path_id)
        if rref is None:
            continue

        rref = rref.get()
        lref = get_path_identity_var(query, path_id, env=env)

        path_cond = bond_condition(lref, rref)
        condition = astutils.extend_binop(condition, path_cond)

    return condition


def full_outer_bond_condition(
        env: context.Environment, query: pgast.Query,
        right_rvar: pgast.BaseRangeVar,
        allow_implicit: bool=True) -> typing.Optional[pgast.Expr]:
    condition = None

    for path_id in right_rvar.path_scope:
        rptr = path_id.rptr()
        if rptr and rptr.singular(path_id.rptr_dir()) and allow_implicit:
            continue

        lref = maybe_get_path_identity_var(query, path_id, env=env)
        if lref is None:
            continue

        rref = get_rvar_path_identity_var(right_rvar, path_id, env=env)

        path_cond = bond_condition(lref, rref)
        condition = astutils.extend_binop(condition, path_cond)

    return condition


def rel_join(
        env: context.Environment,
        query: pgast.Query, right_rvar: pgast.BaseRangeVar,
        type: str='inner', front: bool=False, allow_implicit_bond=True):
    if not query.from_clause:
        query.from_clause.append(right_rvar)
        return

    condition = full_outer_bond_condition(env, query, right_rvar,
                                          allow_implicit=allow_implicit_bond)

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

    valent_bond = get_path_identity_var(stmt, link.source.path_id, env=env)
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

        target_range_bond = get_rvar_path_identity_var(
            set_rvar, ir_set.path_id, env=env)

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

    if stmt.from_clause:
        fromexpr = stmt.from_clause[0]
    else:
        fromexpr = None

    nref = dbobj.get_column(
        set_rvar, common.edgedb_name_to_pg_name('schema::name'))

    val = pgast.Constant(
        val=ir_set.rptr.source.scls.name
    )

    cond_expr = astutils.new_binop(nref, val, op='=')

    if fromexpr is not None:
        stmt.from_clause[0] = pgast.JoinExpr(
            type='inner',
            larg=fromexpr,
            rarg=set_rvar,
            quals=cond_expr)
    else:
        stmt.from_clause = [set_rvar]
        stmt.where_clause = astutils.extend_binop(
            stmt.where_clause, cond_expr)


def join_inline_rel(
        env: context.Environment, *,
        stmt: pgast.Query, set_rvar: pgast.BaseRangeVar,
        src_ref: pgast.ColumnRef, tgt_ref: pgast.ColumnRef,
        join_type='inner'):
    fromexpr = stmt.from_clause[0]

    cond_expr = astutils.new_binop(src_ref, tgt_ref, op='=')

    stmt.from_clause[0] = pgast.JoinExpr(
        type=join_type,
        larg=fromexpr,
        rarg=set_rvar,
        quals=cond_expr)
