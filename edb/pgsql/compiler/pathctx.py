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


from __future__ import annotations

import functools
from typing import *

from edb.common import enum as s_enum

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import pointers as s_pointers

from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import astutils
from . import context
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


def get_less_specific_aspect(
    path_id: irast.PathId,
    aspect: str,
) -> Optional[str]:
    if path_id.is_objtype_path():
        mapping = OBJECT_ASPECT_SPECIFICITY_MAP
    else:
        mapping = PRIMITIVE_ASPECT_SPECIFICITY_MAP

    less_specific_aspect = mapping.get(PathAspect(aspect))
    if less_specific_aspect is not None:
        return str(less_specific_aspect)
    else:
        return None


def map_path_id(
        path_id: irast.PathId,
        path_id_map: Dict[irast.PathId, irast.PathId]) -> irast.PathId:

    sorted_map = sorted(
        path_id_map.items(), key=lambda kv: len(kv[0]), reverse=True)

    for outer_id, inner_id in sorted_map:
        new_path_id = irtyputils.replace_pathid_prefix(
            path_id, outer_id, inner_id, permissive_ptr_path=True)
        if new_path_id != path_id:
            path_id = new_path_id
            break

    return path_id


def reverse_map_path_id(
        path_id: irast.PathId,
        path_id_map: Dict[irast.PathId, irast.PathId]) -> irast.PathId:
    for outer_id, inner_id in path_id_map.items():
        new_path_id = irtyputils.replace_pathid_prefix(
            path_id, inner_id, outer_id)
        if new_path_id != path_id:
            path_id = new_path_id
            break

    return path_id


def put_path_id_mask(
    stmt: pgast.EdgeQLPathInfo, path_id: irast.PathId
) -> None:
    stmt.path_id_mask.add(path_id)


def put_path_id_map(
    rel: pgast.Query,
    outer_path_id: irast.PathId,
    inner_path_id: irast.PathId,
) -> None:
    inner_path_id = map_path_id(inner_path_id, rel.view_path_id_map)
    rel.view_path_id_map[outer_path_id] = inner_path_id


def get_path_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> pgast.BaseExpr:
    """Return a value expression for a given *path_id* in a given *rel*."""
    if isinstance(rel, pgast.CommonTableExpr):
        rel = rel.query

    if flavor == 'normal':
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
    elif flavor == 'packed':
        if (
            rel.packed_path_namespace
            and (path_id, aspect) in rel.packed_path_namespace
        ):
            return rel.packed_path_namespace[path_id, aspect]

    if astutils.is_set_op_query(rel):
        return _get_path_var_in_setop(
            rel, path_id, aspect=aspect, flavor=flavor, env=env)

    ptrref = path_id.rptr()
    ptrref_dir = path_id.rptr_dir()
    is_type_intersection = path_id.is_type_intersection_path()

    src_path_id: Optional[irast.PathId] = None
    if ptrref is not None and not is_type_intersection:
        ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, resolve_type=False, link_bias=False, allow_missing=True)
        ptr_dir = path_id.rptr_dir()
        is_inbound = ptr_dir == s_pointers.PointerDirection.Inbound
        if is_inbound:
            src_path_id = path_id
        else:
            src_path_id = path_id.src_path()
            assert src_path_id is not None
            src_rptr = src_path_id.rptr()
            if (
                irtyputils.is_id_ptrref(ptrref)
                and (
                    src_rptr is None
                    or ptrref_dir is not s_pointers.PointerDirection.Inbound
                )
            ):
                # When there is a reference to the id property of
                # an object which is linked to by a link stored
                # inline, we want to route the reference to the
                # inline attribute.  For example,
                # Foo.__type__.id gets resolved to the Foo.__type__
                # column.  This can only be done if Foo is visible
                # in scope, and Foo.__type__ is not a computable.
                pid = src_path_id
                while pid.is_type_intersection_path():
                    # Skip type intersection step(s).
                    src_pid = pid.src_path()
                    if src_pid is not None:
                        src_rptr = src_pid.rptr()
                        pid = src_pid
                    else:
                        break

                if (src_rptr is not None
                        and not irtyputils.is_computable_ptrref(src_rptr)
                        and env.ptrref_source_visibility.get(src_rptr)):
                    src_ptr_info = pg_types.get_ptrref_storage_info(
                        src_rptr, resolve_type=False, link_bias=False,
                        allow_missing=True)
                    if (src_ptr_info
                            and src_ptr_info.table_type == 'ObjectType'):
                        src_path_id = src_path_id.src_path()
                        ptr_info = src_ptr_info

    else:
        ptr_info = None
        ptr_dir = None

    var: Optional[pgast.BaseExpr]

    if ptrref is None:
        if len(path_id) == 1:
            # This is an scalar set derived from an expression.
            src_path_id = path_id

    elif ptrref.source_ptr is not None:
        if ptr_info and ptr_info.table_type != 'link' and not is_inbound:
            # This is a link prop that is stored in source rel,
            # step back to link source rvar.
            _prefix_pid = path_id.src_path()
            assert _prefix_pid is not None
            src_path_id = _prefix_pid.src_path()

    elif is_type_intersection:
        src_path_id = path_id

    assert src_path_id is not None

    # Find which rvar will have path_id as an output
    src_aspect, rel_rvar, found_path_var = _find_rel_rvar(
        rel, path_id, src_path_id, aspect=aspect, flavor=flavor, env=env)

    if found_path_var:
        return found_path_var

    if rel_rvar is None:
        raise LookupError(
            f'there is no range var for '
            f'{src_path_id} {src_aspect} in {rel}')

    if isinstance(rel_rvar, pgast.IntersectionRangeVar):
        if (
            (path_id.is_objtype_path() and src_path_id == path_id)
            or (ptrref is not None and irtyputils.is_id_ptrref(ptrref))
        ):
            rel_rvar = rel_rvar.component_rvars[-1]
        else:
            # Intersection rvars are basically JOINs of the relevant
            # parts of the type intersection, and so we need to make
            # sure we pick the correct component relation of that JOIN.
            rel_rvar = _find_rvar_in_intersection_by_typeref(
                path_id,
                rel_rvar.component_rvars,
            )

    source_rel = rel_rvar.query

    if isinstance(ptrref, irast.PointerRef) and rel_rvar.typeref is not None:
        assert ptrref_dir
        actual_ptrref = irtyputils.maybe_find_actual_ptrref(
            rel_rvar.typeref, ptrref, dir=ptrref_dir)

        if actual_ptrref is not None:
            ptr_info = pg_types.get_ptrref_storage_info(
                actual_ptrref, resolve_type=False, link_bias=False)

    outvar = get_path_output(
        source_rel, path_id, ptr_info=ptr_info,
        aspect=aspect, flavor=flavor, env=env)

    var = astutils.get_rvar_var(rel_rvar, outvar)
    put_path_var(rel, path_id, var, aspect=aspect, flavor=flavor, env=env)

    if isinstance(var, pgast.TupleVar):
        for element in var.elements:
            put_path_var_if_not_exists(rel, element.path_id, element.val,
                                       flavor=flavor,
                                       aspect=aspect, env=env)

    return var


def _find_rel_rvar(
    rel: pgast.Query, path_id: irast.PathId, src_path_id: irast.PathId, *,
    aspect: str, flavor: str, env: context.Environment,
) -> Tuple[str, Optional[pgast.PathRangeVar], Optional[pgast.BaseExpr]]:
    """Rummage around rel looking for an appropriate rvar for path_id.

    Somewhat unfortunately, some checks to find the actual path var
    (in a particular tuple case) need to occur in the middle of the
    rvar rel search, so we can also find the actual path var in passing.
    """
    src_aspect = aspect
    rel_rvar = maybe_get_path_rvar(
        rel, path_id, aspect=aspect, flavor=flavor, env=env)

    if rel_rvar is None:
        alt_aspect = get_less_specific_aspect(path_id, aspect)
        if alt_aspect is not None:
            rel_rvar = maybe_get_path_rvar(
                rel, path_id, aspect=alt_aspect, env=env)
    else:
        alt_aspect = None

    if rel_rvar is None:
        if flavor == 'packed':
            src_aspect = aspect
        elif src_path_id.is_objtype_path():
            src_aspect = 'source'
        else:
            src_aspect = aspect

        if src_path_id.is_tuple_path():
            if src_aspect == 'identity':
                src_aspect = 'value'

            if (var := _find_in_output_tuple(
                    rel, path_id, src_aspect, env=env)):
                return src_aspect, None, var

            rel_rvar = maybe_get_path_rvar(
                rel, src_path_id, aspect=src_aspect, env=env)

            if rel_rvar is None:
                _src_path_id_prefix = src_path_id.src_path()
                if _src_path_id_prefix is not None:
                    rel_rvar = maybe_get_path_rvar(
                        rel, _src_path_id_prefix, aspect=src_aspect, env=env)
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
        assert flavor == 'normal'
        var = rel.path_namespace.get((path_id, alt_aspect))
        if var is not None:
            put_path_var(rel, path_id, var, aspect=aspect, env=env)
            return src_aspect, None, var

    return src_aspect, rel_rvar, None


def _get_path_var_in_setop(
    rel: pgast.Query, path_id: irast.PathId, *,
    aspect: str, flavor: str, env: context.Environment,
) -> pgast.BaseExpr:
    test_vals = []
    if aspect in ('value', 'serialized'):
        test_cb = functools.partial(
            maybe_get_path_var, env=env, path_id=path_id, aspect=aspect)
        test_vals = astutils.for_each_query_in_set(rel, test_cb)

    # In order to ensure output balance, we only want to output
    # a TupleVar if *every* subquery outputs a TupleVar.
    # If some but not all output TupleVars, we need to fix up
    # the output TupleVars by outputting them as a real tuple.
    # This is needed for cases like `(Foo.bar UNION (1,2))`.
    if (
        any(isinstance(x, pgast.TupleVarBase) for x in test_vals)
        and not all(isinstance(x, pgast.TupleVarBase) for x in test_vals)
    ):
        def fixup(subrel: pgast.Query) -> None:
            cur = get_path_var_and_fix_tuple(
                subrel, env=env, path_id=path_id, aspect=aspect)
            assert flavor == 'normal'
            if isinstance(cur, pgast.TupleVarBase):
                new = output.output_as_value(cur, env=env)
                new_path_id = map_path_id(path_id, subrel.view_path_id_map)
                put_path_var(
                    subrel, new_path_id, new,
                    force=True, env=env, aspect=aspect)

        astutils.for_each_query_in_set(rel, fixup)

    # We disable the find_path_output optimization when doing
    # UNIONs to avoid situations where they have different numbers
    # of columns.
    cb = functools.partial(
        get_path_output_or_null,
        env=env,
        disable_output_fusion=True,
        path_id=path_id,
        aspect=aspect,
        flavor=flavor)

    outputs = astutils.for_each_query_in_set(rel, cb)
    counts = astutils.for_each_query_in_set(
        rel, lambda x: len(x.target_list))
    assert counts == [counts[0]] * len(counts)

    first: Optional[pgast.OutputVar] = None
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

    # Fail if no subquery had the path or, for scalar identity paths,
    # if any did not have it.
    #
    # We need to do this for scalar identity because scalar identity
    # is only used for volatility refs, and it is OK if looking it up
    # fails, because we create a backup volatility ref---but it is
    # *not* OK for it to succeed and produce NULL in some cases.
    if all_null or (
        aspect == 'identity' and optional and not path_id.is_objtype_path()
    ):
        # If *none* of the subqueries had it, we have to remove them all
        # before erroring, lest a future call see them and decide
        # they really exist.
        def _clear(subrel: pgast.Query) -> None:
            assert flavor == 'normal'
            new_path_id = map_path_id(path_id, subrel.view_path_id_map)
            del subrel.path_outputs[new_path_id, aspect]
            subrel.target_list.pop()

        astutils.for_each_query_in_set(rel, _clear)
        raise LookupError(
            f'cannot find refs for '
            f'path {path_id} {aspect} in {rel}')

    if first is None:
        raise AssertionError(
            f'union did not produce any outputs')

    # Path vars produced by UNION expressions can be "optional",
    # i.e the record is accepted as-is when such var is NULL.
    # This is necessary to correctly join heterogeneous UNIONs.
    var = astutils.strip_output_var(
        first, optional=optional, nullable=optional or nullable)
    put_path_var(rel, path_id, var, aspect=aspect, flavor=flavor, env=env)
    return var


def _find_rvar_in_intersection_by_typeref(
    path_id: irast.PathId,
    component_rvars: Sequence[pgast.PathRangeVar],
) -> pgast.PathRangeVar:

    assert component_rvars

    pid_rptr = path_id.rptr()
    if pid_rptr is not None:
        if pid_rptr.material_ptr is not None:
            pid_rptr = pid_rptr.material_ptr
        tref = pid_rptr.out_source
    else:
        tref = path_id.target

    for component_rvar in component_rvars:
        if (
            component_rvar.typeref is not None
            and irtyputils.type_contains(tref, component_rvar.typeref)
        ):
            rel_rvar = component_rvar
            break
    else:
        raise AssertionError(
            f'no rvar in intersection matches path id {path_id}'
        )

    return rel_rvar


def _find_in_output_tuple(
        rel: pgast.Query,
        path_id: irast.PathId,
        aspect: str,
        env: context.Environment) -> Optional[pgast.BaseExpr]:
    """Try indirecting a source tuple already present as an output.

    Normally tuple indirections are handled by
    process_set_as_tuple_indirection, but UNIONing an explicit tuple with a
    tuple coming from a base relation (like `(Foo.bar UNION (1,2)).0`)
    can lead to us looking for a tuple path in relations that only have
    the actual full tuple.
    (See test_edgeql_coalesce_tuple_{08,09}).

    We handle this by checking whether some prefix of the tuple path
    is present in the path_outputs.

    This is sufficient because the relevant cases are all caused by
    set ops, and the "fixup" done in set op cases ensures that the
    tuple will be already present.
    """

    steps = []
    src_path_id = path_id.src_path()
    ptrref = path_id.rptr()
    while (
        src_path_id
        and src_path_id.is_tuple_path()
        and isinstance(ptrref, irast.TupleIndirectionPointerRef)
    ):
        steps.append((ptrref.shortname.name, src_path_id))

        if (
            (var := rel.path_namespace.get((src_path_id, aspect)))
            and not isinstance(var, pgast.TupleVarBase)
        ):
            for name, src in reversed(steps):
                var = astutils.tuple_getattr(var, src.target, name)
            put_path_var(rel, path_id, var, aspect=aspect, env=env)
            return var

        ptrref = src_path_id.rptr()
        src_path_id = src_path_id.src_path()

    return None


def get_path_identity_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.BaseExpr:
    return get_path_var(rel, path_id, aspect='identity', env=env)


def get_path_value_var(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.BaseExpr:
    return get_path_var(rel, path_id, aspect='value', env=env)


def is_relation_rvar(
        rvar: pgast.BaseRangeVar) -> bool:
    return (
        isinstance(rvar, pgast.RelRangeVar) and
        is_terminal_relation(rvar.query)
    )


def is_terminal_relation(
        rel: pgast.BaseRelation) -> bool:
    return isinstance(rel, (pgast.Relation, pgast.NullRelation))


def is_values_relation(
        rel: pgast.BaseRelation) -> bool:
    return bool(getattr(rel, 'values', None))


def maybe_get_path_var(
        rel: pgast.Query, path_id: irast.PathId, *, aspect: str,
        env: context.Environment) -> Optional[pgast.BaseExpr]:
    try:
        return get_path_var(rel, path_id, aspect=aspect, env=env)
    except LookupError:
        return None


def maybe_get_path_identity_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.BaseExpr]:
    try:
        return get_path_var(rel, path_id, aspect='identity', env=env)
    except LookupError:
        return None


def maybe_get_path_value_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.BaseExpr]:
    try:
        return get_path_var(rel, path_id, aspect='value', env=env)
    except LookupError:
        return None


def maybe_get_path_serialized_var(
        rel: pgast.Query,
        path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.BaseExpr]:
    try:
        return get_path_var(rel, path_id, aspect='serialized', env=env)
    except LookupError:
        return None


def put_path_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        aspect: str, flavor: str='normal', force: bool=False,
        env: context.Environment) -> None:
    if flavor == 'packed':
        if rel.packed_path_namespace is None:
            rel.packed_path_namespace = {}
        path_namespace = rel.packed_path_namespace
    else:
        path_namespace = rel.path_namespace

    if (path_id, aspect) in path_namespace and not force:
        raise KeyError(
            f'{aspect} of {path_id} is already present in {rel}')
    path_namespace[path_id, aspect] = var


def put_path_var_if_not_exists(
        rel: pgast.Query, path_id: irast.PathId, var: pgast.BaseExpr, *,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect=aspect, flavor=flavor, env=env)
    except KeyError:
        pass


def put_path_identity_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        force: bool=False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='identity', force=force, env=env)


def put_path_value_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        force: bool = False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='value', force=force, env=env)


def put_path_serialized_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        force: bool = False, env: context.Environment) -> None:
    put_path_var(rel, path_id, var, aspect='serialized', force=force, env=env)


def put_path_value_var_if_not_exists(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        force: bool = False, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect='value', force=force, env=env)
    except KeyError:
        pass


def put_path_serialized_var_if_not_exists(
        rel: pgast.BaseRelation, path_id: irast.PathId, var: pgast.BaseExpr, *,
        force: bool = False, env: context.Environment) -> None:
    try:
        put_path_var(rel, path_id, var, aspect='serialized',
                     force=force, env=env)
    except KeyError:
        pass


def put_path_bond(
        stmt: pgast.BaseRelation, path_id: irast.PathId) -> None:
    stmt.path_scope.add(path_id)


def put_rvar_path_bond(
        rvar: pgast.PathRangeVar, path_id: irast.PathId) -> None:
    put_path_bond(rvar.query, path_id)


def get_path_output_alias(
        path_id: irast.PathId, aspect: str, *,
        env: context.Environment) -> str:
    rptr = path_id.rptr()
    if rptr is not None:
        alias_base = rptr.shortname.name
    elif path_id.is_collection_path():
        assert path_id.target.collection is not None
        alias_base = path_id.target.collection
    else:
        alias_base = path_id.target_name_hint.name

    return env.aliases.get(f'{alias_base}_{aspect}')


def get_rvar_path_var(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, aspect: str, *,
        flavor: str='normal',
        env: context.Environment) -> pgast.OutputVar:
    """Return ColumnRef for a given *path_id* in a given *range var*."""

    if (path_id, aspect) in (outs := rvar.query.get_path_outputs(flavor)):
        outvar = outs[path_id, aspect]
    elif is_relation_rvar(rvar):
        assert flavor == 'normal'
        ptr_si: Optional[pg_types.PointerStorageInfo]
        if (
            (rptr := path_id.rptr()) is not None
            and rvar.typeref is not None
            and rvar.query.path_id
            and rvar.query.path_id != path_id
            and (not rvar.query.path_id.is_type_intersection_path()
                 or rvar.query.path_id.src_path() != path_id)
        ):
            ptrref_dir = path_id.rptr_dir()
            assert ptrref_dir
            actual_rptr = irtyputils.maybe_find_actual_ptrref(
                rvar.typeref, rptr, dir=ptrref_dir
            )
            if actual_rptr is not None:
                ptr_si = pg_types.get_ptrref_storage_info(actual_rptr)
            else:
                ptr_si = None
        else:
            ptr_si = None

        outvar = _get_rel_path_output(
            rvar.query, path_id, ptr_info=ptr_si,
            aspect=aspect, flavor=flavor, env=env)
    else:
        # Range is another query.
        outvar = get_path_output(
            rvar.query, path_id, aspect=aspect, flavor=flavor, env=env)

    return astutils.get_rvar_var(rvar, outvar)


def put_rvar_path_output(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, aspect: str,
        var: pgast.OutputVar, *, env: context.Environment) -> None:
    _put_path_output_var(rvar.query, path_id, aspect, var, env=env)


def get_rvar_path_identity_var(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_rvar_path_var(rvar, path_id, aspect='identity', env=env)


def maybe_get_rvar_path_identity_var(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.OutputVar]:
    try:
        return get_rvar_path_var(rvar, path_id, aspect='identity', env=env)
    except LookupError:
        return None


def get_rvar_path_value_var(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    return get_rvar_path_var(rvar, path_id, aspect='value', env=env)


def maybe_get_rvar_path_value_var(
        rvar: pgast.PathRangeVar, path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.OutputVar]:
    try:
        return get_rvar_path_var(rvar, path_id, aspect='value', env=env)
    except LookupError:
        return None


def get_rvar_output_var_as_col_list(
        rvar: pgast.PathRangeVar, outvar: pgast.OutputVar, aspect: str, *,
        env: context.Environment) -> List[pgast.OutputVar]:

    cols: List[pgast.OutputVar]

    if isinstance(outvar, pgast.ColumnRef):
        cols = [outvar]
    elif isinstance(outvar, pgast.TupleVarBase):
        cols = []
        for el in outvar.elements:
            col = get_rvar_path_var(rvar, el.path_id, aspect=aspect, env=env)
            cols.extend(get_rvar_output_var_as_col_list(
                rvar, col, aspect=aspect, env=env))
    else:
        raise RuntimeError(f'unexpected OutputVar: {outvar!r}')

    return cols


def put_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.PathRangeVar, *,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> None:
    assert isinstance(path_id, irast.PathId)
    stmt.get_rvar_map(flavor)[path_id, aspect] = rvar

    # Normally, masked paths (i.e paths that are only behind a fence below),
    # will not be exposed in a query namespace.  However, when the masked
    # path in the *main* path of a set, it must still be exposed, but no
    # further than the immediate parent query.
    try:
        query = rvar.query
    except NotImplementedError:
        pass
    else:
        if path_id in query.path_id_mask:
            put_path_id_mask(stmt, path_id)


def put_path_value_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.PathRangeVar, *,
        flavor: str='normal',
        env: context.Environment) -> None:
    put_path_rvar(stmt, path_id, rvar, aspect='value', flavor=flavor, env=env)


def put_path_source_rvar(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.PathRangeVar, *,
        flavor: str='normal',
        env: context.Environment) -> None:
    put_path_rvar(stmt, path_id, rvar, aspect='source', flavor=flavor, env=env)


def has_rvar(
        stmt: pgast.Query, rvar: pgast.PathRangeVar, *,
        env: context.Environment) -> bool:
    return any(
        rvar in set(stmt.get_rvar_map(flavor).values())
        for flavor in ('normal', 'packed')
    )


def put_path_rvar_if_not_exists(
        stmt: pgast.Query, path_id: irast.PathId, rvar: pgast.PathRangeVar, *,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> None:
    if (path_id, aspect) not in stmt.get_rvar_map(flavor):
        put_path_rvar(
            stmt, path_id, rvar, aspect=aspect, flavor=flavor, env=env)


def get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> pgast.PathRangeVar:
    rvar = maybe_get_path_rvar(
        stmt, path_id, aspect=aspect, flavor=flavor, env=env)
    if rvar is None:
        raise LookupError(
            f'there is no range var for {path_id} {aspect} in {stmt}')
    return rvar


def maybe_get_path_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *, aspect: str,
        flavor: str='normal',
        env: context.Environment) -> Optional[pgast.PathRangeVar]:
    rvar = env.external_rvars.get((path_id, aspect))
    path_rvar_map = stmt.maybe_get_rvar_map(flavor)
    if path_rvar_map is not None:
        if rvar is None and path_rvar_map:
            rvar = path_rvar_map.get((path_id, aspect))
        if rvar is None and aspect == 'identity':
            rvar = path_rvar_map.get((path_id, 'value'))
    return rvar


def list_path_aspects(
        stmt: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> Set[str]:

    path_id = map_path_id(path_id, stmt.view_path_id_map)

    aspects = set()

    for rvar_path_id, aspect in stmt.path_rvar_map:
        if path_id == rvar_path_id:
            aspects.add(aspect)

    for ns_path_id, aspect in stmt.path_namespace:
        if path_id == ns_path_id:
            aspects.add(aspect)

    for ns_path_id, aspect in stmt.path_outputs:
        if path_id == ns_path_id:
            aspects.add(aspect)

    return aspects


def maybe_get_path_value_rvar(
        stmt: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> Optional[pgast.BaseRangeVar]:
    return maybe_get_path_rvar(stmt, path_id, aspect='value', env=env)


def _same_expr(expr1: pgast.BaseExpr, expr2: pgast.BaseExpr) -> bool:
    if (isinstance(expr1, pgast.ColumnRef) and
            isinstance(expr2, pgast.ColumnRef)):
        return expr1.name == expr2.name
    else:
        return expr1 == expr2


def put_path_packed_output(
        rel: pgast.EdgeQLPathInfo, path_id: irast.PathId,
        val: pgast.OutputVar, aspect: str='value') -> None:
    if rel.packed_path_outputs is None:
        rel.packed_path_outputs = {}
    rel.packed_path_outputs[path_id, aspect] = val


def _put_path_output_var(
        rel: pgast.BaseRelation, path_id: irast.PathId, aspect: str,
        var: pgast.OutputVar, *, flavor: str='normal',
        env: context.Environment) -> None:
    if flavor == 'packed':
        put_path_packed_output(rel, path_id, var, aspect)
    else:
        rel.path_outputs[path_id, aspect] = var


def _get_rel_object_id_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str,
        ptr_info: Optional[pg_types.PointerStorageInfo]=None,
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
        flavor: str,
        ptr_info: Optional[pg_types.PointerStorageInfo]=None,
        env: context.Environment) -> pgast.OutputVar:

    if path_id.is_objtype_path():
        if aspect == 'identity':
            aspect = 'value'

        if aspect != 'value':
            raise LookupError(
                f'invalid request for non-scalar path {path_id} {aspect}')

        if (path_id == rel.path_id or
                (rel.path_id and
                 rel.path_id.is_type_intersection_path() and
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

        assert not ptrref.is_computable

        if ptr_info is None:
            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=False)

        result = pgast.ColumnRef(
            name=[ptr_info.column_name],
            nullable=not ptrref.required)
    _put_path_output_var(rel, path_id, aspect, result, flavor=flavor, env=env)
    return result


def find_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, ref: pgast.BaseExpr, *,
        env: context.Environment) -> Optional[pgast.OutputVar]:
    if isinstance(ref, pgast.TupleVarBase):
        return None

    for key, other_ref in rel.path_namespace.items():
        if _same_expr(other_ref, ref) and key in rel.path_outputs:
            return rel.path_outputs.get(key)
    else:
        return None


def get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, allow_nullable: bool=True,
        disable_output_fusion: bool=False,
        ptr_info: Optional[pg_types.PointerStorageInfo]=None,
        flavor: str='normal',
        env: context.Environment) -> pgast.OutputVar:

    if isinstance(rel, pgast.Query) and flavor == 'normal':
        path_id = map_path_id(path_id, rel.view_path_id_map)

    return _get_path_output(rel, path_id=path_id, aspect=aspect,
                            disable_output_fusion=disable_output_fusion,
                            ptr_info=ptr_info, allow_nullable=allow_nullable,
                            flavor=flavor,
                            env=env)


def _get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, allow_nullable: bool=True,
        disable_output_fusion: bool=False,
        ptr_info: Optional[pg_types.PointerStorageInfo]=None,
        flavor: str,
        env: context.Environment) -> pgast.OutputVar:

    if flavor == 'packed':
        result = (rel.packed_path_outputs.get((path_id, aspect))
                  if rel.packed_path_outputs else None)
    else:
        result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    ref: pgast.BaseExpr
    alias = None
    rptr = path_id.rptr()
    if rptr is not None and irtyputils.is_id_ptrref(rptr) and not (
        (src_path_id := path_id.src_path())
        and (src_rptr := src_path_id.rptr())
        and (
            src_rptr.is_computable
            or src_rptr.out_cardinality.is_multi()
        )
    ):
        # A value reference to Object.id is the same as a value
        # reference to the Object itself. (Though we want to only
        # apply this in the cases that process_set_as_path does this
        # optimization, which means not for multi props.)
        src_path_id = path_id.src_path()
        assert src_path_id is not None
        id_output = maybe_get_path_output(rel, src_path_id,
                                          aspect='value',
                                          allow_nullable=allow_nullable,
                                          ptr_info=ptr_info, env=env)
        if id_output is not None:
            _put_path_output_var(rel, path_id, aspect, id_output, env=env)
            return id_output

    if is_terminal_relation(rel):
        return _get_rel_path_output(rel, path_id, aspect=aspect, flavor=flavor,
                                    ptr_info=ptr_info, env=env)

    assert isinstance(rel, pgast.Query)
    if is_values_relation(rel) and aspect != 'identity':
        # The VALUES() construct seems to always expose its
        # value as "column1".
        alias = 'column1'
        ref = pgast.ColumnRef(name=[alias])
    else:
        ref = get_path_var(rel, path_id, aspect=aspect, flavor=flavor, env=env)

    # As an optimization, look to see if the same expression is being
    # output on a different aspect. This can save us needing to do the
    # work twice in the query.
    other_output = find_path_output(rel, path_id, ref, env=env)
    if other_output is not None and not disable_output_fusion:
        _put_path_output_var(
            rel, path_id, aspect, other_output, flavor=flavor, env=env)
        return other_output

    if isinstance(ref, pgast.TupleVarBase):
        elements = []
        for el in ref.elements:
            el_path_id = reverse_map_path_id(
                el.path_id, rel.view_path_id_map)

            try:
                # Similarly to get_path_var(), check for outer path_id
                # first for tuple serialized var disambiguation.
                element = _get_path_output(
                    rel, el_path_id, aspect=aspect,
                    disable_output_fusion=disable_output_fusion,
                    flavor=flavor,
                    allow_nullable=allow_nullable, env=env)
            except LookupError:
                element = get_path_output(
                    rel, el_path_id, aspect=aspect,
                    disable_output_fusion=disable_output_fusion,
                    flavor=flavor,
                    allow_nullable=allow_nullable, env=env)

            elements.append(pgast.TupleElementBase(
                path_id=el_path_id, name=element))

        result = pgast.TupleVarBase(
            elements=elements,
            named=ref.named,
            typeref=ref.typeref,
            is_packed_multi=ref.is_packed_multi,
        )

    else:
        if astutils.is_set_op_query(rel):
            assert isinstance(ref, pgast.OutputVar)
            result = astutils.strip_output_var(ref)
        else:
            assert isinstance(rel, pgast.ReturningQuery), \
                "expected ReturningQuery"

            if alias is None:
                alias = get_path_output_alias(path_id, aspect, env=env)

            restarget = pgast.ResTarget(
                name=alias, val=ref, ser_safe=getattr(ref, 'ser_safe', False))
            rel.target_list.append(restarget)

            nullable = is_nullable(ref, env=env)

            optional = None
            is_packed_multi = False
            if isinstance(ref, pgast.ColumnRef):
                optional = ref.optional
                is_packed_multi = ref.is_packed_multi

            if nullable and not allow_nullable:
                assert isinstance(rel, pgast.SelectStmt), \
                    "expected SelectStmt"
                var = get_path_var(rel, path_id, aspect=aspect, env=env)
                rel.where_clause = astutils.extend_binop(
                    rel.where_clause,
                    pgast.NullTest(arg=var, negated=True)
                )
                nullable = False

            result = pgast.ColumnRef(
                name=[alias], nullable=nullable, optional=optional,
                is_packed_multi=is_packed_multi)

    _put_path_output_var(rel, path_id, aspect, result, flavor=flavor, env=env)
    if (path_id.is_objtype_path()
            and not isinstance(result, pgast.TupleVarBase)):
        equiv_aspect = None
        if aspect == 'identity':
            equiv_aspect = 'value'
        elif aspect == 'value':
            equiv_aspect = 'identity'

        if (equiv_aspect is not None
                and (path_id, equiv_aspect) not in rel.path_outputs):
            _put_path_output_var(
                rel, path_id, equiv_aspect, result, flavor=flavor, env=env)

    return result


def maybe_get_path_output(
        rel: pgast.BaseRelation, path_id: irast.PathId, *,
        aspect: str, allow_nullable: bool=True,
        disable_output_fusion: bool=False,
        ptr_info: Optional[pg_types.PointerStorageInfo]=None,
        flavor: str='normal',
        env: context.Environment) -> Optional[pgast.OutputVar]:
    try:
        return get_path_output(rel, path_id=path_id, aspect=aspect,
                               allow_nullable=allow_nullable,
                               disable_output_fusion=disable_output_fusion,
                               flavor=flavor,
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
        env: context.Environment) -> pgast.BaseExpr:

    ref = maybe_get_path_serialized_var(rel, path_id, env=env)
    if ref is None:
        ref = get_path_value_var(rel, path_id, env=env)
    return ref


def fix_tuple(
        rel: pgast.Query, ref: pgast.BaseExpr, *,
        aspect: str, output: bool=False,
        env: context.Environment) -> pgast.BaseExpr:

    if (
        isinstance(ref, pgast.TupleVarBase)
        and not isinstance(ref, pgast.TupleVar)
    ):
        elements = []

        for el in ref.elements:
            assert el.path_id is not None
            val = _get_and_fix_tuple(
                rel, el.path_id, aspect=aspect, output=output, env=env)
            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        ref = pgast.TupleVar(
            elements,
            named=ref.named,
            typeref=ref.typeref,
        )

    return ref


def _get_and_fix_tuple(
        rel: pgast.Query, path_id: irast.PathId, *,
        output: bool=False,
        aspect: str, env: context.Environment) -> pgast.BaseExpr:

    ref = (
        get_path_output(rel, path_id, aspect=aspect, env=env)
        if output else
        get_path_var(rel, path_id, aspect=aspect, env=env)
    )
    return fix_tuple(rel, ref, aspect=aspect, output=output, env=env)


def get_path_var_and_fix_tuple(
        rel: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> pgast.BaseExpr:
    return _get_and_fix_tuple(
        rel, path_id, output=False, aspect=aspect, env=env)


def get_path_output_and_fix_tuple(
        rel: pgast.Query, path_id: irast.PathId, *,
        aspect: str, env: context.Environment) -> pgast.BaseExpr:
    return _get_and_fix_tuple(
        rel, path_id, output=True, aspect=aspect, env=env)


def get_path_serialized_output(
        rel: pgast.Query, path_id: irast.PathId, *,
        env: context.Environment) -> pgast.OutputVar:
    # Serialized output is a special case, we don't
    # want this behaviour to be recursive, so it
    # must be kept outside of get_path_output() generic.
    aspect = 'serialized'

    path_id = map_path_id(path_id, rel.view_path_id_map)
    result = rel.path_outputs.get((path_id, aspect))
    if result is not None:
        return result

    ref = get_path_serialized_or_value_var(rel, path_id, env=env)

    if (
        isinstance(ref, pgast.TupleVarBase)
        and not isinstance(ref, pgast.TupleVar)
    ):
        elements = []

        for el in ref.elements:
            assert el.path_id is not None
            val = get_path_serialized_or_value_var(rel, el.path_id, env=env)
            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        ref = pgast.TupleVar(
            elements,
            named=ref.named,
            typeref=ref.typeref,
        )

    refexpr = output.serialize_expr(ref, path_id=path_id, env=env)
    alias = get_path_output_alias(path_id, aspect, env=env)

    restarget = pgast.ResTarget(name=alias, val=refexpr, ser_safe=True)
    rel.target_list.append(restarget)

    result = pgast.ColumnRef(
        name=[alias], nullable=refexpr.nullable, ser_safe=True)

    _put_path_output_var(rel, path_id, aspect, result, env=env)

    return result


def get_path_output_or_null(
        rel: pgast.Query, path_id: irast.PathId, *,
        disable_output_fusion: bool=False,
        flavor: str='normal',
        aspect: str, env: context.Environment) -> \
        Tuple[pgast.OutputVar, bool]:

    path_id = map_path_id(path_id, rel.view_path_id_map)

    ref = maybe_get_path_output(
        rel, path_id,
        disable_output_fusion=disable_output_fusion,
        flavor=flavor,
        aspect=aspect, env=env)
    if ref is not None:
        return ref, False

    alt_aspect = get_less_specific_aspect(path_id, aspect)
    if alt_aspect is not None and flavor == 'normal':
        # If disable_output_fusion is true, we need to be careful
        # to not reuse an existing column
        if disable_output_fusion:
            preexisting = rel.path_outputs.pop((path_id, alt_aspect), None)
        ref = maybe_get_path_output(
            rel, path_id,
            disable_output_fusion=disable_output_fusion,
            aspect=alt_aspect, env=env)
        if disable_output_fusion:
            # Put back the path_output to whatever it was before
            if not preexisting:
                rel.path_outputs.pop((path_id, alt_aspect), None)
            else:
                rel.path_outputs[(path_id, alt_aspect)] = preexisting

        if ref is not None:
            _put_path_output_var(rel, path_id, aspect, ref, env=env)
            return ref, False

    alias = env.aliases.get('null')
    restarget = pgast.ResTarget(
        name=alias,
        val=pgast.NullConstant())

    rel.target_list.append(restarget)

    ref = pgast.ColumnRef(name=[alias], nullable=True)
    _put_path_output_var(rel, path_id, aspect, ref, flavor=flavor, env=env)

    return ref, True


def is_nullable(
        expr: pgast.BaseExpr, *,
        env: context.Environment) -> Optional[bool]:
    try:
        return expr.nullable
    except AttributeError:
        if isinstance(expr, pgast.ReturningQuery):
            tl_len = len(expr.target_list)
            if tl_len != 1:
                raise RuntimeError(
                    f'subquery used as a value returns {tl_len} columns')

            return is_nullable(expr.target_list[0].val, env=env)
        else:
            raise
