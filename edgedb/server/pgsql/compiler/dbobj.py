##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common

from . import context


def range_for_material_concept(
        env: context.Environment,
        concept: s_concepts.Concept,
        path_id: irast.PathId,
        include_overlays: bool=True) -> pgast.BaseRangeVar:

    from . import pathctx  # XXX: fix cycle

    table_schema_name, table_name = common.concept_name_to_table_name(
        concept.name, catenate=False)

    if concept.name.module == 'schema':
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name,
        relname=table_name
    )

    rvar = pgast.RangeVar(
        relation=relation,
        alias=pgast.Alias(
            aliasname=env.aliases.get(concept.name.name)
        )
    )

    overlays = env.rel_overlays.get(concept)
    if overlays and include_overlays:
        set_ops = []

        qry = pgast.SelectStmt()
        qry.from_clause.append(rvar)
        pathctx.put_path_rvar(env, qry, path_id, rvar)
        qry.path_scope.add(path_id)

        set_ops.append(('union', qry))

        for op, cte in overlays:
            rvar = pgast.RangeVar(
                relation=cte,
                alias=pgast.Alias(
                    aliasname=env.aliases.get(hint=cte.name)
                )
            )

            qry = pgast.SelectStmt(
                from_clause=[rvar],
            )

            pathctx.put_path_rvar(env, qry, path_id, rvar)
            qry.path_scope.add(path_id)

            if op == 'replace':
                op = 'union'
                set_ops = []

            set_ops.append((op, qry))

        rvar = range_from_queryset(env, set_ops, concept)

    return rvar


def range_for_concept(
        env: context.Environment,
        concept: s_concepts.Concept,
        path_id: irast.PathId, *,
        include_overlays: bool=True) -> pgast.BaseRangeVar:
    from . import pathctx  # XXX: fix cycle

    if not concept.is_virtual:
        rvar = range_for_material_concept(
            env, concept, path_id, include_overlays=include_overlays)
    else:
        # Virtual concepts are represented as a UNION of selects
        # from their children, which is, for most purposes, equivalent
        # to SELECTing from a parent table.
        children = frozenset(concept.children(env.schema))

        set_ops = []

        for child in children:
            c_rvar = range_for_concept(
                env, child, path_id=path_id,
                include_overlays=include_overlays)

            qry = pgast.SelectStmt(
                from_clause=[c_rvar],
            )

            pathctx.put_path_rvar(env, qry, path_id, c_rvar)
            qry.path_scope.add(path_id)

            set_ops.append(('union', qry))

        rvar = range_from_queryset(env, set_ops, concept)

    return rvar


def range_for_set(
        env: context.Environment,
        ir_set: irast.Set, *,
        include_overlays: bool=True) -> pgast.BaseRangeVar:
    rvar = range_for_concept(
        env, ir_set.scls, ir_set.path_id, include_overlays=include_overlays)

    return rvar


def table_from_ptrcls(
        env: context.Environment,
        ptrcls: s_links.Link) -> pgast.RangeVar:
    """Return a Table corresponding to a given Link."""
    table_schema_name, table_name = common.get_table_name(
        ptrcls, catenate=False)

    pname = ptrcls.shortname

    if pname.module == 'schema':
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name, relname=table_name)

    rvar = pgast.RangeVar(
        relation=relation,
        alias=pgast.Alias(
            aliasname=env.aliases.get(pname.name)
        )
    )

    return rvar


def range_for_ptrcls(
        env: context.Environment,
        ptrcls: s_links.Link, direction: s_pointers.PointerDirection, *,
        include_overlays: bool=True) -> pgast.BaseRangeVar:
    """"Return a Range subclass corresponding to a given ptr step.

    If `ptrcls` is a generic link, then a simple RangeVar is returned,
    otherwise the return value may potentially be a UNION of all tables
    corresponding to a set of specialized links computed from the given
    `ptrcls` taking source inheritance into account.
    """
    linkname = ptrcls.shortname
    endpoint = ptrcls.source

    cols = [
        'std::source',
        'std::target'
    ]

    schema = env.schema

    set_ops = []

    ptrclses = set()

    for source in {endpoint} | set(endpoint.descendants(schema)):
        # Sift through the descendants to see who has this link
        try:
            src_ptrcls = source.pointers[linkname]
        except KeyError:
            # This source has no such link, skip it
            continue
        else:
            if src_ptrcls in ptrclses:
                # Seen this link already
                continue
            ptrclses.add(src_ptrcls)

        table = table_from_ptrcls(env, src_ptrcls)

        qry = pgast.SelectStmt()
        qry.from_clause.append(table)
        qry.rptr_rvar = table

        # Make sure all property references are pulled up properly
        for colname in cols:
            selexpr = pgast.ColumnRef(
                name=[table.alias.aliasname, colname])
            qry.target_list.append(
                pgast.ResTarget(val=selexpr, name=colname))

        set_ops.append(('union', qry))

        overlays = env.rel_overlays.get(src_ptrcls)
        if overlays and include_overlays:
            for op, cte in overlays:
                rvar = pgast.RangeVar(
                    relation=cte,
                    alias=pgast.Alias(
                        aliasname=env.aliases.get(cte.name)
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

    rvar = range_from_queryset(env, set_ops, ptrcls)
    return rvar


def range_for_pointer(
        env: context.Environment,
        pointer: s_links.Link) -> pgast.BaseRangeVar:
    return range_for_ptrcls(env, pointer.ptrcls, pointer.direction)


def range_from_queryset(
        env: context.Environment,
        set_ops: typing.Sequence[typing.Tuple[str, pgast.BaseRelation]],
        scls: s_obj.Class) -> pgast.BaseRangeVar:
    if len(set_ops) > 1:
        # More than one class table, generate a UNION/EXCEPT clause.
        qry = pgast.SelectStmt(
            all=True,
            larg=set_ops[0][1]
        )

        for op, rarg in set_ops[1:]:
            qry.op, qry.rarg = op, rarg
            qry = pgast.SelectStmt(
                all=True,
                larg=qry
            )

        qry = qry.larg

        rvar = pgast.RangeSubselect(
            subquery=qry,
            alias=pgast.Alias(
                aliasname=env.aliases.get(scls.shortname.name)
            )
        )

    else:
        # Just one class table, so return it directly
        rvar = set_ops[0][1].from_clause[0]

    return rvar


def get_column(
        rvar: pgast.BaseRangeVar,
        colspec: typing.Union[pgast.ColumnRef, str], *,
        grouped: bool=False, optional: bool=False) -> pgast.ColumnRef:

    if isinstance(colspec, pgast.ColumnRef):
        colname = colspec.name[-1]
        nullable = colspec.nullable
        optional = colspec.optional
        grouped = colspec.grouped
    else:
        colname = colspec
        nullable = rvar.nullable if rvar is not None else False
        optional = optional
        grouped = grouped

    if rvar is None:
        name = [colname]
    else:
        name = [rvar.alias.aliasname, colname]

    return pgast.ColumnRef(name=name, nullable=nullable,
                           grouped=grouped, optional=optional)


def rvar_for_rel(
        env: context.Environment, rel: pgast.Query,
        lateral: bool=False) -> pgast.BaseRangeVar:
    if isinstance(rel, pgast.Query):
        rvar = pgast.RangeSubselect(
            subquery=rel,
            alias=pgast.Alias(
                aliasname=env.aliases.get('q')
            ),
            lateral=lateral
        )
    else:
        rvar = pgast.RangeVar(
            relation=rel,
            alias=pgast.Alias(
                aliasname=env.aliases.get(rel.name)
            )
        )

    return rvar


def get_rvar_fieldref(
        rvar: typing.Optional[pgast.BaseRangeVar],
        colname: typing.Union[str, pgast.TupleVar]) -> \
        typing.Union[pgast.ColumnRef, pgast.TupleVar]:

    if isinstance(colname, pgast.TupleVar):
        elements = []

        for el in colname.elements:
            val = get_rvar_fieldref(rvar, el.name)
            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        fieldref = pgast.TupleVar(elements, named=colname.named)
    else:
        fieldref = get_column(rvar, colname)

    return fieldref


def add_rel_overlay(
        scls: s_concepts.Concept, op: str, rel: pgast.BaseRelation, *,
        env: context.Environment) -> None:
    overlays = env.rel_overlays[scls]
    overlays.append((op, rel))
