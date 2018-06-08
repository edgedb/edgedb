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


"""Helpers to manage statement path contexts."""


import functools
import typing

from edb.lang.common import enum as s_enum

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import output


class PathAspect(s_enum.StrEnum):
    IDENTITY = 'identity'
    VALUE = 'value'
    SERIALIZED = 'serialized'


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
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> pgast.OutputVar:
    """Return an OutputVar for a given *path_id* in a given *rel*."""
    if isinstance(rel, pgast.CommonTableExpr):
        rel = rel.query

    if rel.view_path_id_map:
        path_id = map_path_id(path_id, rel.view_path_id_map)

    if (path_id, aspect) in rel.path_namespace:
        return rel.path_namespace[path_id, aspect]

    ptrcls = path_id.rptr()
    if ptrcls is not None:
        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)
        ptr_dir = path_id.rptr_dir()
        is_inbound = ptr_dir == s_pointers.PointerDirection.Inbound
        if is_inbound:
            src_path_id = path_id
        else:
            src_path_id = path_id.src_path()
            # Value references to std::id link are identical to
            # the identity reference of its source.
            if ptrcls.is_id_pointer() and aspect == 'value':
                if (src_path_id, 'identity') in rel.path_namespace:
                    return rel.path_namespace[src_path_id, 'identity']

            # Default object value is its identity.
            # NOTE: the value may be an explicit tuple, if the
            #       set had an explicit shape.
            if path_id.is_objtype_path() and aspect == 'value':
                if (path_id, 'identity') in rel.path_namespace:
                    return rel.path_namespace[path_id, 'identity']

    else:
        ptr_info = None
        src_path_id = None
        ptr_dir = None

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
        nullable = False

        for colref, is_null in outputs:
            if colref.nullable:
                nullable = True
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

        # Path vars produced by UNION expressions can be "optional",
        # i.e the record is accepted as-is when such var is NULL.
        # This is necessary to correctly join heterogeneous UNIONs.
        fieldref = dbobj.get_rvar_fieldref(
            None, first, optional=optional, nullable=optional or nullable)
        put_path_var(rel, path_id, fieldref, aspect=aspect, env=env)
        return fieldref

    if ptrcls is None:
        if len(path_id) == 1:
            # This is an scalar set derived from an expression.
            src_path_id = path_id

    elif ptrcls.is_link_property():
        if ptr_info.table_type != 'link' and not is_inbound:
            # This is a link prop that is stored in source rel,
            # step back to link source rvar.
            src_path_id = path_id[:-4]

    elif ptr_info.table_type != 'ObjectType' and not is_inbound:
        # Ref is in the mapping rvar.
        src_path_id = path_id.ptr_path()

    rel_rvar = maybe_get_path_rvar(rel, path_id, aspect=aspect, env=env)
    if rel_rvar is None:
        if src_path_id.is_objtype_path() and aspect == 'identity':
            src_aspect = 'value'
        else:
            src_aspect = aspect

        if isinstance(src_path_id.rptr(), irutils.TupleIndirectionLink):
            rel_rvar = maybe_get_path_rvar(
                rel, src_path_id, aspect=src_aspect, env=env)

            if rel_rvar is None:
                rel_rvar = get_path_rvar(
                    rel, src_path_id.src_path(), aspect=src_aspect, env=env)
        else:
            rel_rvar = get_path_rvar(
                rel, src_path_id, aspect=src_aspect, env=env)

    source_rel = rel_rvar.query

    drilldown_path_id = map_path_id(path_id, rel.view_path_id_map)

    if source_rel in env.root_rels and len(source_rel.path_scope) == 1:
        if not drilldown_path_id.is_objtype_path() and ptrcls is not None:
            outer_path_id = drilldown_path_id.src_path()
        else:
            outer_path_id = drilldown_path_id

        path_id_map = {
            outer_path_id: next(iter(source_rel.path_scope))
        }

        drilldown_path_id = map_path_id(
            drilldown_path_id, path_id_map)

    outvar = get_path_output(
        source_rel, drilldown_path_id, ptr_info=ptr_info,
        aspect=aspect, env=env)

    if is_relation_rvar(rel_rvar) and aspect not in {'identity', 'value'}:
        raise LookupError(
            f'{path_id} {aspect} is not defined in {rel}'
        )

    fieldref = dbobj.get_rvar_fieldref(rel_rvar, outvar)
    put_path_var(rel, path_id, fieldref, aspect=aspect, env=env)
    return fieldref


def get_path_identity_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_var(rel, path_id, aspect='identity', env=env)


def get_path_value_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_path_var(rel, path_id, aspect='value', env=env)


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
        return get_path_var(rel, path_id, aspect=aspect, env=env)
    except LookupError:
        return None


def maybe_get_path_identity_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(rel, path_id, aspect='identity', env=env)
    except LookupError:
        return None


def maybe_get_path_value_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(rel, path_id, aspect='value', env=env)
    except LookupError:
        return None


def maybe_get_path_serialized_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_var(rel, path_id, aspect='serialized', env=env)
    except LookupError:
        return None


def put_path_var(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        aspect: str, force: bool=False, env: context.Environment) -> None:
    if (path_id, aspect) in rel.path_namespace and not force:
        raise KeyError(
            f'{aspect} of {path_id} is already present in {rel}')
    rel.path_namespace[path_id, aspect] = var


def put_path_var_if_not_exists(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        aspect: str, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect=aspect, env=env)
    except KeyError:
        pass


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


def put_path_serialized_var_if_not_exists(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.Base, *,
        force=False, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect='serialized',
                     force=force, env=env)
    except KeyError:
        pass


def put_path_bond(
        stmt: pgast.Query, path_id: irast.PathId):
    stmt.path_scope.add(path_id)


def get_path_output_alias(
        path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> str:
    rptr = path_id.rptr()
    if rptr is not None:
        ptrname = rptr.shortname
        alias_base = ptrname.name
    elif isinstance(path_id[-1], s_types.Collection):
        alias_base = path_id[-1].schema_name
    else:
        alias_base = path_id[-1].name.name

    return env.aliases.get(f'{alias_base}_{aspect}')


def get_rvar_path_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> pgast.OutputVar:
    """Return ColumnRef for a given *path_id* in a given *range var*."""

    if (path_id, aspect) in rvar.path_outputs:
        outvar = rvar.path_outputs[path_id, aspect]
    elif isinstance(rvar.query, pgast.Relation):
        outvar = _get_rel_path_output(rvar.query, path_id, aspect=aspect,
                                      env=env)
    else:
        # Range is another query.
        outvar = get_path_output(rvar.query, path_id, aspect=aspect, env=env)

    return dbobj.get_rvar_fieldref(rvar, outvar)


def put_rvar_path_output(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, aspect: str,
        var: pgast.OutputVar, *, env: context.Environment) -> None:
    rvar.path_outputs[path_id, aspect] = var


def get_rvar_path_identity_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    return get_rvar_path_var(rvar, path_id, aspect='identity', env=env)


def maybe_get_rvar_path_identity_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    try:
        return get_rvar_path_var(rvar, path_id, aspect='identity', env=env)
    except LookupError:
        return None


def get_rvar_path_value_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    return get_rvar_path_var(rvar, path_id, aspect='value', env=env)


def maybe_get_rvar_path_value_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, *,
        env: context.Environment):
    try:
        return get_rvar_path_var(rvar, path_id, aspect='value', env=env)
    except LookupError:
        return None


def get_rvar_output_var_as_col_list(
        rvar: pgast.BaseRangeVar, outvar: pgast.OutputVar, aspect: str, *,
        env: context.Environment) -> typing.List[pgast.ColumnRef]:
    if isinstance(outvar, pgast.ColumnRef):
        cols = [outvar]
    else:
        cols = []
        for el in outvar.elements:
            col = get_rvar_path_var(rvar, el.path_id, aspect=aspect, env=env)
            cols.append(col)

    return cols


def put_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.BaseRangeVar, *,
        aspect: str, env: context.Environment) -> None:
    assert isinstance(path_id, irast.PathId)
    stmt.path_rvar_map[path_id, aspect] = rvar

    # Normally, masked paths (i.e paths that are only behind a fence below),
    # will not be exposed in a query namespace.  However, when the masked
    # path in the *main* path of a set, it must still be exposed, but no
    # further than the immediate parent query.
    if path_id in rvar.query.path_id_mask:
        stmt.path_id_mask.add(path_id)


def put_path_value_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.BaseRangeVar, *,
        env: context.Environment) -> None:
    put_path_rvar(stmt, path_id, rvar, aspect='value', env=env)


def has_rvar(
        stmt: pgast.Query, rvar: pgast.BaseRangeVar, *,
        env: context.Environment) -> bool:
    return rvar in set(stmt.path_rvar_map.values())


def put_path_rvar_if_not_exists(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.BaseRangeVar, *,
        aspect: str, env: context.Environment) -> None:
    if (path_id, aspect) not in stmt.path_rvar_map:
        put_path_rvar(stmt, path_id, rvar, aspect=aspect, env=env)


def get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> pgast.BaseRangeVar:
    rvar = stmt.path_rvar_map.get((path_id, aspect))
    if rvar is None:
        if aspect == 'identity':
            rvar = stmt.path_rvar_map.get((path_id, 'value'))
        if rvar is None:
            raise LookupError(
                f'there is no range var for {path_id} {aspect} in {stmt}')
    return rvar


def maybe_get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *, aspect: str,
        env: context.Environment) -> typing.Optional[pgast.BaseRangeVar]:
    try:
        return get_path_rvar(stmt, path_id, aspect=aspect, env=env)
    except LookupError:
        return None


def _same_expr(expr1, expr2):
    if (isinstance(expr1, pgast.ColumnRef) and
            isinstance(expr2, pgast.ColumnRef)):
        return expr1.name == expr2.name
    else:
        return expr1 == expr2


def _get_rel_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    if path_id.is_objtype_path():
        if aspect == 'value':
            aspect = 'identity'

        if aspect != 'identity':
            raise LookupError(
                f'invalid request for non-scalar path {path_id} {aspect}')

        if (path_id == rel.path_id or
                (rel.path_id.is_type_indirection_path() and
                 path_id == rel.path_id.src_path())):
            path_id = irutils.get_id_path_id(path_id, schema=env.schema)
    else:
        if aspect == 'identity':
            raise LookupError(
                f'invalid request for scalar path {path_id} {aspect}')

    if path_id.rptr_dir() != s_pointers.PointerDirection.Outbound:
        raise LookupError(
            f'{path_id} is an inbound pointer and cannot be resolved '
            f'on a base relation')

    ptrcls = path_id.rptr()

    if ptrcls is None:
        raise ValueError(
            f'could not resolve trailing pointer class for {path_id}')

    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=False)

    result = pgast.ColumnRef(name=[ptr_info.column_name],
                             nullable=rel.nullable or not ptrcls.required)
    rel.path_outputs[path_id, aspect] = result
    return result


def find_path_output(
        rel: pgast.Query, path_id: irast.PathId, ref: pgast.Base, *,
        env: context.Environment) -> str:
    for key, other_ref in rel.path_namespace.items():
        if _same_expr(other_ref, ref) and key in rel.path_outputs:
            return rel.path_outputs.get(key)


def get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    view_path_id_map = getattr(rel, 'view_path_id_map', None)
    if view_path_id_map:
        path_id = map_path_id(path_id, view_path_id_map)

    result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    if isinstance(rel, pgast.Relation):
        return _get_rel_path_output(rel, path_id, aspect=aspect,
                                    ptr_info=ptr_info, env=env)
    else:
        ref = get_path_var(rel, path_id, aspect=aspect, env=env)

    other_output = find_path_output(rel, path_id, ref, env=env)
    if other_output is not None:
        rel.path_outputs[path_id, aspect] = other_output
        return other_output

    if isinstance(ref, pgast.TupleVar):
        elements = []
        for el in ref.elements:
            el_path_id = reverse_map_path_id(
                el.path_id, rel.view_path_id_map)
            element = get_path_output(
                rel, el_path_id, aspect=aspect, env=env)
            elements.append(pgast.TupleElement(
                path_id=el_path_id, name=element))
        result = pgast.TupleVar(elements=elements, named=ref.named)

    else:
        if astutils.is_set_op_query(rel):
            assert isinstance(ref, pgast.ColumnRef)
            result = dbobj.get_column(None, ref)
        else:
            alias = get_path_output_alias(path_id, aspect, env=env)

            restarget = pgast.ResTarget(name=alias, val=ref)
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

            if isinstance(ref, pgast.ColumnRef):
                nullable = ref.nullable
                optional = ref.optional
            else:
                nullable = rel.nullable
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


def get_path_serialized_or_value_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:

    ref = maybe_get_path_serialized_var(rel, path_id, env=env)
    if ref is None:
        ref = get_path_value_var(rel, path_id, env=env)
    return ref


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

    ref = get_path_serialized_or_value_var(rel, path_id, env=env)

    ref = output.serialize_expr(ref, env=env)
    alias = get_path_output_alias(path_id, aspect, env=env)

    restarget = pgast.ResTarget(name=alias, val=ref)
    if hasattr(rel, 'returning_list'):
        rel.returning_list.append(restarget)
    else:
        rel.target_list.append(restarget)

    result = pgast.ColumnRef(name=[alias], nullable=ref.nullable)
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
