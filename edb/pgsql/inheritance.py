from __future__ import annotations
from typing import Optional, Tuple, AbstractSet, Dict, List, Iterator

from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import pointers as s_pointers
from edb.schema import sources as s_sources
from edb.schema import schema as s_schema

from edb.ir import typeutils as irtyputils

from . import ast as pgast
from . import types
from . import common


def get_inheritance_view(
    schema: s_schema.Schema,
    obj: s_sources.Source | s_pointers.Pointer,
    exclude_children: AbstractSet[
        s_sources.Source | s_pointers.Pointer
    ] = frozenset(),
    exclude_ptrs: AbstractSet[s_pointers.Pointer] = frozenset(),
) -> pgast.SelectStmt:
    ptrs: Dict[sn.UnqualName, Tuple[list[str], Tuple[str, ...]]] = {}

    if isinstance(obj, s_sources.Source):
        pointers = list(obj.get_pointers(schema).items(schema))
        # Sort by UUID timestamp for stable VIEW column order.
        pointers.sort(key=lambda p: p[1].id.time)

        for ptrname, ptr in pointers:
            if ptr in exclude_ptrs:
                continue
            if ptr.is_pure_computable(schema):
                continue
            ptr_stor_info = types.get_pointer_storage_info(
                ptr,
                link_bias=isinstance(obj, s_links.Link),
                schema=schema,
            )
            if (
                isinstance(obj, s_links.Link)
                or ptr_stor_info.table_type == 'ObjectType'
            ):
                ptrs[ptrname] = (
                    [ptr_stor_info.column_name],
                    ptr_stor_info.column_type,
                )

                shortname = ptr.get_shortname(schema).name
                if shortname != ptr_stor_info.column_name:
                    ptrs[ptrname][0].append(common.quote_ident(shortname))

        for name, alias, type in obj.get_addon_columns(schema):
            ptrs[sn.UnqualName(name)] = ([alias], type)

    else:
        # MULTI PROPERTY
        ptrs[sn.UnqualName('source')] = (['source'], ('uuid',))
        lp_info = types.get_pointer_storage_info(
            obj,
            link_bias=True,
            schema=schema,
        )
        ptrs[sn.UnqualName('target')] = (['target'], lp_info.column_type)

    descendants = [
        child
        for child in obj.descendants(schema)
        if types.has_table(child, schema) and child not in exclude_children
        # XXX: Exclude sys/cfg tables from non sys/cfg views. This
        # probably isn't *really* what we want to do, but until we
        # figure that out, do *something* so that DDL isn't
        # excruciatingly slow because of the cost of explicit id
        # checks. See #5168.
        and (
            not irtyputils.is_cfg_view(child, schema)
            or irtyputils.is_cfg_view(obj, schema)
        )
    ]

    # Hackily force 'source' to appear in abstract links. We need
    # source present in the code we generate to enforce newly
    # created exclusive constraints across types.
    if (
        ptrs
        and isinstance(obj, s_links.Link)
        and sn.UnqualName('source') not in ptrs
        and obj.is_non_concrete(schema)
    ):
        ptrs[sn.UnqualName('source')] = (['source'], ('uuid',))

    components = []
    components.append(_get_select_from(schema, obj, ptrs))
    components.extend(
        _get_select_from(schema, child, ptrs) for child in descendants
    )

    return _union_all(filter(None, components))


def _union_all(components: Iterator[pgast.SelectStmt]) -> pgast.SelectStmt:
    query = next(components)
    for component in components:
        query = pgast.SelectStmt(
            larg=query,
            op='UNION',
            all=True,
            rarg=component,
        )
    return query


def _get_select_from(
    schema: s_schema.Schema,
    obj: s_sources.Source | s_pointers.Pointer,
    ptr_names: Dict[sn.UnqualName, Tuple[list[str], Tuple[str, ...]]],
) -> Optional[pgast.SelectStmt]:
    schema_name, table_name = common.get_backend_name(
        schema,
        obj,
        catenate=False,
        aspect='table',
    )
    # the name of the rel var of the object table within the select query
    table_rvar_name = table_name

    target_list: List[pgast.ResTarget] = []

    system_cols = ['tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid']
    for sys_col_name in system_cols:
        val: pgast.BaseExpr
        if not irtyputils.is_cfg_view(obj, schema):
            val = pgast.ColumnRef(name=(table_rvar_name, sys_col_name))
        else:
            val = pgast.NullConstant()
        target_list.append(pgast.ResTarget(name=sys_col_name, val=val))

    if isinstance(obj, s_sources.Source):
        ptrs = dict(obj.get_pointers(schema).items(schema))

        for ptr_name, (aliases, pg_type) in ptr_names.items():
            ptr = ptrs.get(ptr_name)

            if ptr_name == sn.UnqualName('__type__'):
                # __type__ is special cased: since it is uniquely
                # determined by the type, we directly insert it
                # into the views instead of storing it (to save space)
                val = pgast.TypeCast(
                    arg=pgast.StringConstant(val=str(obj.id)),
                    type_name=pgast.TypeName(name=('uuid',)),
                )

            elif ptr is not None:
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr,
                    link_bias=isinstance(obj, s_links.Link),
                    schema=schema,
                )
                if ptr_stor_info.column_type != pg_type:
                    return None
                val = pgast.ColumnRef(
                    name=(table_rvar_name, ptr_stor_info.column_name)
                )

            elif ptr_name == sn.UnqualName('source'):
                val = pgast.TypeCast(
                    arg=pgast.NullConstant(),
                    type_name=pgast.TypeName(name=('uuid',)),
                )

            elif ptr_name == sn.UnqualName('__fts_document__') or (
                ptr_name.name.startswith('__ext_ai_')
                and ptr_name.name.endswith('__')
            ):
                # an addon column
                val = pgast.ColumnRef(name=(table_rvar_name, ptr_name.name))

            else:
                return None

            for alias in aliases:
                target_list.append(pgast.ResTarget(name=alias, val=val))

    else:
        for ptr_name, (aliases, _) in ptr_names.items():
            for alias in aliases:
                target_list.append(
                    pgast.ResTarget(
                        name=alias,
                        val=pgast.ColumnRef(
                            name=(table_rvar_name, str(ptr_name)),
                        ),
                    )
                )

    return pgast.SelectStmt(
        from_clause=[
            pgast.RelRangeVar(
                alias=pgast.Alias(aliasname=table_rvar_name),
                relation=pgast.Relation(
                    schemaname=schema_name,
                    name=table_name,
                ),
            )
        ],
        target_list=target_list,
    )
