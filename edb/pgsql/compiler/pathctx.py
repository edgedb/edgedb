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

from edb.common import enum as s_enum

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import pointers as s_pointers

from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import output


class PathAspect(s_enum.StrEnum):
    IDENTITY = 'identity'
    VALUE = 'value'
    SOURCE = 'source'
    SERIALIZED = 'serialized'


# A mapping of more specific aspect -> less specific aspect for objects
OBJECT_ASPECT_SPECIFICITY_MAP = {
    PathAspect.IDENTITY: PathAspect.VALUE,
    PathAspect.VALUE: PathAspect.SOURCE,
    PathAspect.SERIALIZED: PathAspect.SOURCE,
}

# A mapping of more specific aspect -> less specific aspect for primitives
PRIMITIVE_ASPECT_SPECIFICITY_MAP = {
    PathAspect.SERIALIZED: PathAspect.VALUE,
}


def get_less_specific_aspect(path_id: irast.PathId, aspect: str):
    if path_id.is_objtype_path():
        mapping = OBJECT_ASPECT_SPECIFICITY_MAP
    else:
        mapping = PRIMITIVE_ASPECT_SPECIFICITY_MAP

    return mapping.get(aspect)


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

    # Check if we already have a var, before remapping the path_id.
    # This is useful for serialized aspect disambiguation in tuples,
    # since process_set_as_tuple() records serialized vars with
    # original path_id.
    if (path_id, aspect) in rel.path_namespace:
        return rel.path_namespace[path_id, aspect]

    if rel.view_path_id_map:
        path_id = map_path_id(path_id, rel.view_path_id_map)

    if (path_id, aspect) in rel.path_namespace:
        return rel.path_namespace[path_id, aspect]

    ptrref = path_id.rptr()
    is_type_indirection = path_id.is_type_indirection_path()
    if ptrref is not None and not is_type_indirection:
        ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, resolve_type=False, link_bias=False)
        ptr_dir = path_id.rptr_dir()
        is_inbound = ptr_dir == s_pointers.PointerDirection.Inbound
        if is_inbound:
            src_path_id = path_id
        else:
            src_path_id = path_id.src_path()
            if irtyputils.is_id_ptrref(ptrref):
                # When there is a reference to the id property of
                # an object which is linked to by a link stored
                # inline, we want to route the reference to the
                # inline attribute.  For example,
                # Foo.__type__.id gets resolved to the Foo.__type__
                # column.
                src_rptr = src_path_id.rptr()
                pid = src_path_id
                while pid.is_type_indirection_path():
                    # Skip type indirection step.
                    src_pid = pid.src_path()
                    if src_pid is not None:
                        src_rptr = src_pid.rptr()
                        pid = src_pid
                    else:
                        break

                if src_rptr is not None:
                    src_ptr_info = pg_types.get_ptrref_storage_info(
                        src_rptr, resolve_type=False, link_bias=False)
                    if src_ptr_info.table_type == 'ObjectType':
                        src_path_id = src_path_id.src_path()
                        ptr_info = src_ptr_info

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
        var = dbobj.strip_output_var(
            first, optional=optional, nullable=optional or nullable)
        put_path_var(rel, path_id, var, aspect=aspect, env=env)
        return var

    if ptrref is None:
        if len(path_id) == 1:
            # This is an scalar set derived from an expression.
            src_path_id = path_id

    elif ptrref.parent_ptr is not None:
        if ptr_info.table_type != 'link' and not is_inbound:
            # This is a link prop that is stored in source rel,
            # step back to link source rvar.
            src_path_id = path_id.src_path().src_path()

    elif (is_type_indirection or
            (ptr_info.table_type != 'ObjectType' and not is_inbound)):
        # Ref is in the mapping rvar.
        src_path_id = path_id.ptr_path()

    rel_rvar = maybe_get_path_rvar(rel, path_id, aspect=aspect, env=env)

    if rel_rvar is None:
        alt_aspect = get_less_specific_aspect(path_id, aspect)
        if alt_aspect is not None:
            rel_rvar = maybe_get_path_rvar(
                rel, path_id, aspect=alt_aspect, env=env)
    else:
        alt_aspect = None

    if rel_rvar is None:
        if src_path_id.is_objtype_path():
            if aspect == 'identity':
                src_aspect = 'value'
            else:
                src_aspect = 'source'
        else:
            src_aspect = aspect

        if src_path_id.is_tuple_path():
            rel_rvar = maybe_get_path_rvar(
                rel, src_path_id, aspect=src_aspect, env=env)

            if rel_rvar is None:
                rel_rvar = maybe_get_path_rvar(
                    rel, src_path_id.src_path(), aspect=src_aspect, env=env)
        else:
            rel_rvar = maybe_get_path_rvar(
                rel, src_path_id, aspect=src_aspect, env=env)

        if (rel_rvar is None
                and src_aspect != 'source' and path_id != src_path_id):
            rel_rvar = maybe_get_path_rvar(
                rel, src_path_id, aspect='source', env=env)

    if rel_rvar is None and alt_aspect is not None:
        # There is no source range var for the requested aspect,
        # check if there is a cached var with less specificity.
        var = rel.path_namespace.get((path_id, alt_aspect))
        if var is not None:
            put_path_var(rel, path_id, var, aspect=aspect, env=env)
            return var

    if rel_rvar is None:
        raise LookupError(
            f'there is no range var for '
            f'{src_path_id} {src_aspect} in {rel}')

    source_rel = rel_rvar.query

    drilldown_path_id = map_path_id(path_id, rel.view_path_id_map)

    if source_rel in env.root_rels and len(source_rel.path_scope) == 1:
        if not drilldown_path_id.is_objtype_path() and ptrref is not None:
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

    var = dbobj.get_rvar_var(rel_rvar, outvar)
    put_path_var(rel, path_id, var, aspect=aspect, env=env)

    if isinstance(var, pgast.TupleVar):
        for element in var.elements:
            put_path_var_if_not_exists(rel, element.path_id, element.val,
                                       aspect=aspect, env=env)

    return var


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
        is_terminal_relation(rvar.relation)
    )


def is_terminal_relation(
        rel: pgast.BaseRelation) -> bool:
    return isinstance(rel, (pgast.Relation, pgast.NullRelation))


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
        stmt: pgast.Query, path_id: irast.PathId) -> None:
    stmt.path_scope.add(path_id)


def put_rvar_path_bond(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId) -> None:
    put_path_bond(rvar.query, path_id)


def get_path_output_alias(
        path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> str:
    rptr = path_id.rptr()
    if rptr is not None:
        alias_base = rptr.shortname.name
    elif path_id.is_collection_path():
        alias_base = path_id.target.collection
    else:
        _, _, alias_base = path_id.target_name_hint.rpartition('::')

    return env.aliases.get(f'{alias_base}_{aspect}')


def get_rvar_path_var(
        rvar: pgast.BaseRangeVar, path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> pgast.OutputVar:
    """Return ColumnRef for a given *path_id* in a given *range var*."""

    if (path_id, aspect) in rvar.path_outputs:
        outvar = rvar.path_outputs[path_id, aspect]
    elif is_relation_rvar(rvar):
        outvar = _get_rel_path_output(rvar.query, path_id, aspect=aspect,
                                      env=env)
    else:
        # Range is another query.
        outvar = get_path_output(rvar.query, path_id, aspect=aspect, env=env)

    return dbobj.get_rvar_var(rvar, outvar)


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
    if hasattr(rvar, 'query') and path_id in rvar.query.path_id_mask:
        stmt.path_id_mask.add(path_id)


def put_path_value_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.BaseRangeVar, *,
        env: context.Environment) -> None:
    put_path_rvar(stmt, path_id, rvar, aspect='value', env=env)


def put_path_source_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.BaseRangeVar, *,
        env: context.Environment) -> None:
    put_path_rvar(stmt, path_id, rvar, aspect='source', env=env)


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


def maybe_get_path_value_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> typing.Optional[pgast.BaseRangeVar]:
    return maybe_get_path_rvar(stmt, path_id, aspect='value', env=env)


def _same_expr(expr1, expr2):
    if (isinstance(expr1, pgast.ColumnRef) and
            isinstance(expr2, pgast.ColumnRef)):
        return expr1.name == expr2.name
    else:
        return expr1 == expr2


def _put_path_output_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, aspect: str,
        var: pgast.OutputVar, *, env: context.Environment) -> None:

    rel.path_outputs[path_id, aspect] = var


def _get_rel_object_id_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    var = rel.path_outputs.get((path_id, aspect))
    if var is not None:
        return var

    if isinstance(rel, pgast.NullRelation):
        name = env.aliases.get('id')

        val = pgast.TypeCast(
            arg=pgast.NullConstant(),
            type_name=pgast.TypeName(
                name=('uuid',),
            )
        )

        rel.target_list.append(pgast.ResTarget(name=name, val=val))
        result = pgast.ColumnRef(name=[name], nullable=True)

    else:
        result = pgast.ColumnRef(name=['id'], nullable=False)

    _put_path_output_var(rel, path_id, aspect, result, env=env)

    return result


def _get_rel_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    if path_id.is_objtype_path():
        if aspect == 'identity':
            aspect = 'value'

        if aspect != 'value':
            raise LookupError(
                f'invalid request for non-scalar path {path_id} {aspect}')

        if (path_id == rel.path_id or
                (rel.path_id.is_type_indirection_path() and
                 path_id == rel.path_id.src_path())):

            return _get_rel_object_id_output(
                rel, path_id, aspect=aspect, env=env)
    else:
        if aspect == 'identity':
            raise LookupError(
                f'invalid request for scalar path {path_id} {aspect}')

        elif aspect == 'serialized':
            aspect = 'value'

    var = rel.path_outputs.get((path_id, aspect))
    if var is not None:
        return var

    ptrref = path_id.rptr()
    rptr_dir = path_id.rptr_dir()

    if (rptr_dir is not None and
            rptr_dir != s_pointers.PointerDirection.Outbound):
        raise LookupError(
            f'{path_id} is an inbound pointer and cannot be resolved '
            f'on a base relation')

    if isinstance(rel, pgast.NullRelation):
        if ptrref is not None:
            target = ptrref.out_target
        else:
            target = path_id.target

        pg_type = pg_types.pg_type_from_ir_typeref(target)

        if ptr_info is not None:
            name = env.aliases.get(ptr_info.column_name)
        else:
            name = env.aliases.get('v')

        val = pgast.TypeCast(
            arg=pgast.NullConstant(),
            type_name=pgast.TypeName(
                name=pg_type,
            )
        )

        rel.target_list.append(pgast.ResTarget(name=name, val=val))
        result = pgast.ColumnRef(name=[name], nullable=True)
    else:
        if ptrref is None:
            raise ValueError(
                f'could not resolve trailing pointer class for {path_id}')

        if ptr_info is None:
            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=False)

        result = pgast.ColumnRef(
            name=[ptr_info.column_name],
            nullable=not ptrref.required)
    _put_path_output_var(rel, path_id, aspect, result, env=env)
    return result


def find_path_output(
        rel: pgast.Query, path_id: irast.PathId, ref: pgast.Base, *,
        env: context.Environment) -> str:
    if isinstance(ref, pgast.TupleVar):
        return None

    for key, other_ref in rel.path_namespace.items():
        if _same_expr(other_ref, ref) and key in rel.path_outputs:
            return rel.path_outputs.get(key)


def get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, allow_nullable: bool=True,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    view_path_id_map = getattr(rel, 'view_path_id_map', None)
    if view_path_id_map:
        path_id = map_path_id(path_id, view_path_id_map)

    return _get_path_output(rel, path_id=path_id, aspect=aspect,
                            ptr_info=ptr_info, allow_nullable=allow_nullable,
                            env=env)


def _get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, allow_nullable: bool=True,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    rptr = path_id.rptr()
    if rptr is not None and irtyputils.is_id_ptrref(rptr):
        # A value reference to Object.id is the same as a value
        # reference to the Object itself.
        src_path_id = path_id.src_path()
        id_output = rel.path_outputs.get((src_path_id, 'value'))
        if id_output is not None:
            _put_path_output_var(rel, path_id, aspect, id_output, env=env)
            return id_output

    if is_terminal_relation(rel):
        return _get_rel_path_output(rel, path_id, aspect=aspect,
                                    ptr_info=ptr_info, env=env)
    else:
        ref = get_path_var(rel, path_id, aspect=aspect, env=env)

    other_output = find_path_output(rel, path_id, ref, env=env)
    if other_output is not None:
        _put_path_output_var(rel, path_id, aspect, other_output, env=env)
        return other_output

    if isinstance(ref, pgast.TupleVar):
        elements = []
        for el in ref.elements:
            el_path_id = reverse_map_path_id(
                el.path_id, rel.view_path_id_map)

            try:
                # Similarly to get_path_var(), check for outer path_id
                # first for tuple serialized var disambiguation.
                element = _get_path_output(
                    rel, el_path_id, aspect=aspect,
                    allow_nullable=False, env=env)
            except LookupError:
                element = get_path_output(
                    rel, el_path_id, aspect=aspect,
                    allow_nullable=False, env=env)

            elements.append(pgast.TupleElement(
                path_id=el_path_id, name=element))

        result = pgast.TupleVar(elements=elements, named=ref.named)

    else:
        if astutils.is_set_op_query(rel):
            result = dbobj.strip_output_var(ref)
        else:
            alias = get_path_output_alias(path_id, aspect, env=env)

            restarget = pgast.ResTarget(
                name=alias, val=ref, ser_safe=getattr(ref, 'ser_safe', False))
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

            nullable = is_nullable(ref, env=env)

            if isinstance(ref, pgast.ColumnRef):
                optional = ref.optional
            else:
                optional = None

            if nullable and not allow_nullable:
                var = get_path_var(rel, path_id, aspect=aspect, env=env)
                rel.where_clause = astutils.extend_binop(
                    rel.where_clause,
                    pgast.NullTest(arg=var, negated=True)
                )
                nullable = False

            result = pgast.ColumnRef(
                name=[alias], nullable=nullable, optional=optional)

    _put_path_output_var(rel, path_id, aspect, result, env=env)
    if (aspect == 'identity' and path_id.is_objtype_path()
            and (path_id, 'value') not in rel.path_outputs):
        _put_path_output_var(rel, path_id, 'value', result, env=env)

    return result


def maybe_get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: typing.Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> typing.Optional[pgast.OutputVar]:
    try:
        return get_path_output(rel, path_id=path_id, aspect=aspect,
                               ptr_info=ptr_info, env=env)
    except LookupError:
        return None


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

    ref = output.serialize_expr(ref, path_id=path_id, env=env)
    alias = get_path_output_alias(path_id, aspect, env=env)

    restarget = pgast.ResTarget(name=alias, val=ref, ser_safe=True)
    if hasattr(rel, 'returning_list'):
        rel.returning_list.append(restarget)
    else:
        rel.target_list.append(restarget)

    result = pgast.ColumnRef(
        name=[alias], nullable=ref.nullable, ser_safe=True)

    _put_path_output_var(rel, path_id, aspect, result, env=env)

    return result


def get_path_output_or_null(
        rel: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> \
        typing.Tuple[pgast.OutputVar, bool]:

    path_id = map_path_id(path_id, rel.view_path_id_map)

    ref = maybe_get_path_output(rel, path_id, aspect=aspect, env=env)
    if ref is not None:
        return ref, False

    alt_aspect = get_less_specific_aspect(path_id, aspect)
    if alt_aspect is not None:
        ref = maybe_get_path_output(rel, path_id, aspect=alt_aspect, env=env)
        if ref is not None:
            _put_path_output_var(rel, path_id, aspect, ref, env=env)
            return ref, False

    alias = env.aliases.get('null')
    restarget = pgast.ResTarget(
        name=alias,
        val=pgast.NullConstant())

    if hasattr(rel, 'returning_list'):
        rel.returning_list.append(restarget)
    else:
        rel.target_list.append(restarget)

    ref = pgast.ColumnRef(name=[alias], nullable=True)
    _put_path_output_var(rel, path_id, aspect, ref, env=env)

    return ref, True


def is_nullable(
        expr: pgast.Base, *,
        env: context.Environment) -> bool:
    try:
        return expr.nullable
    except AttributeError:
        if isinstance(expr, pgast.Query):
            tl_len = len(expr.target_list)
            if tl_len != 1:
                raise RuntimeError(
                    f'subquery used as a value returns {tl_len} columns')

            return is_nullable(expr.target_list[0].val, env=env)
        else:
            raise
