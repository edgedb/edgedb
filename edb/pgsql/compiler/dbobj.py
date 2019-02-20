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


import typing

from edb.ir import ast as irast

from edb.schema import name as sn

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pgtypes

from . import astutils
from . import context


def range_for_material_objtype(
        typeref: irast.TypeRef,
        path_id: irast.PathId, *,
        include_overlays: bool=True,
        env: context.Environment) -> pgast.BaseRangeVar:

    from . import pathctx  # XXX: fix cycle

    if typeref.material_type is not None:
        typeref = typeref.material_type

    table_schema_name, table_name = common.get_objtype_backend_name(
        typeref.id, typeref.module_id, catenate=False)

    if typeref.name_hint.module in {'schema', 'sys'}:
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name,
        name=table_name,
        path_id=path_id,
    )

    rvar = pgast.RangeVar(
        relation=relation,
        alias=pgast.Alias(
            aliasname=env.aliases.get(typeref.name_hint.name)
        )
    )

    overlays = env.rel_overlays.get(str(typeref.id))
    if overlays and include_overlays:
        set_ops = []

        qry = pgast.SelectStmt()
        qry.from_clause.append(rvar)
        pathctx.put_path_value_rvar(qry, path_id, rvar, env=env)
        pathctx.put_path_bond(qry, path_id)

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

            pathctx.put_path_value_rvar(qry, path_id, rvar, env=env)
            pathctx.put_path_bond(qry, path_id)

            if op == 'replace':
                op = 'union'
                set_ops = []

            set_ops.append((op, qry))

        rvar = range_from_queryset(set_ops, typeref.name_hint, env=env)

    return rvar


def range_for_typeref(
        typeref: irast.TypeRef,
        path_id: irast.PathId, *,
        include_overlays: bool=True,
        env: context.Environment) -> pgast.BaseRangeVar:
    from . import pathctx  # XXX: fix cycle

    if not typeref.children:
        rvar = range_for_material_objtype(
            typeref, path_id, include_overlays=include_overlays, env=env)
    else:
        # Union object types are represented as a UNION of selects
        # from their children, which is, for most purposes, equivalent
        # to SELECTing from a parent table.
        set_ops = []

        for child in typeref.children:
            c_rvar = range_for_typeref(
                child, path_id=path_id,
                include_overlays=include_overlays, env=env)

            qry = pgast.SelectStmt(
                from_clause=[c_rvar],
            )

            pathctx.put_path_value_rvar(qry, path_id, c_rvar, env=env)
            pathctx.put_path_bond(qry, path_id)

            set_ops.append(('union', qry))

        rvar = range_from_queryset(set_ops, typeref.name_hint, env=env)

    rvar.query.path_id = path_id

    return rvar


def range_for_set(
        ir_set: irast.Set, *,
        include_overlays: bool=True,
        env: context.Environment) -> pgast.BaseRangeVar:
    return range_for_typeref(
        ir_set.typeref, ir_set.path_id,
        include_overlays=include_overlays, env=env)


def table_from_ptrref(
        ptrref: irast.PointerRef, *,
        env: context.Environment) -> pgast.RangeVar:
    """Return a Table corresponding to a given Link."""
    table_schema_name, table_name = common.get_pointer_backend_name(
        ptrref.id, ptrref.module_id, catenate=False)

    if ptrref.shortname.module in {'schema', 'sys'}:
        # Redirect all queries to schema tables to edgedbss
        table_schema_name = 'edgedbss'

    relation = pgast.Relation(
        schemaname=table_schema_name, name=table_name)

    rvar = pgast.RangeVar(
        relation=relation,
        alias=pgast.Alias(
            aliasname=env.aliases.get(ptrref.shortname.name)
        )
    )

    return rvar


def range_for_ptrref(
        ptrref: irast.BasePointerRef, *,
        include_overlays: bool=True,
        env: context.Environment) -> pgast.BaseRangeVar:
    """"Return a Range subclass corresponding to a given ptr step.

    The return value may potentially be a UNION of all tables
    corresponding to a set of specialized links computed from the given
    `ptrref` taking source inheritance into account.
    """
    tgt_col = pgtypes.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=True).column_name

    cols = [
        'source',
        tgt_col
    ]

    set_ops = []

    for src_ptrref in {ptrref} | ptrref.descendants:
        table = table_from_ptrref(src_ptrref, env=env)

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

        overlays = env.rel_overlays.get(src_ptrref.shortname)
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

    rvar = range_from_queryset(set_ops, ptrref.shortname, env=env)
    return rvar


def range_for_pointer(
        pointer: irast.Pointer, *,
        env: context.Environment) -> pgast.BaseRangeVar:
    ptrref = pointer.ptrref
    if ptrref.derived_from_ptr is not None:
        ptrref = ptrref.derived_from_ptr

    return range_for_ptrref(ptrref, env=env)


def range_from_queryset(
        set_ops: typing.Sequence[typing.Tuple[str, pgast.BaseRelation]],
        objname: sn.Name, *,
        env: context.Environment) -> pgast.BaseRangeVar:
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
                aliasname=env.aliases.get(objname.name),
            )
        )

    else:
        # Just one class table, so return it directly
        rvar = set_ops[0][1].from_clause[0]

    return rvar


def find_column_in_subselect_rvar(rvar: pgast.BaseRangeVar, name: str) -> int:
    # Range over a subquery, we can inspect the output list
    # of the subquery.  If the subquery is a UNION (or EXCEPT),
    # we take the leftmost non-setop query.
    subquery = astutils.get_leftmost_query(rvar.subquery)
    for i, rt in enumerate(subquery.target_list):
        if rt.name == name:
            return i

    raise RuntimeError(f'cannot find {name!r} in {rvar} output')


def get_column(
        rvar: pgast.BaseRangeVar,
        colspec: typing.Union[str, pgast.ColumnRef], *,
        nullable: bool=None) -> pgast.ColumnRef:

    if isinstance(colspec, pgast.ColumnRef):
        colname = colspec.name[-1]
    else:
        colname = colspec

    ser_safe = False

    if nullable is None:
        if isinstance(rvar, pgast.RangeVar):
            # Range over a relation, we cannot infer nullability in
            # this context, so assume it's true.
            nullable = True

        elif isinstance(rvar, pgast.RangeSubselect):
            col_idx = find_column_in_subselect_rvar(rvar, colname)
            if astutils.is_set_op_query(rvar.subquery):
                nullables = []
                ser_safes = []
                astutils.for_each_query_in_set(
                    rvar.subquery,
                    lambda q:
                        (nullables.append(q.target_list[col_idx].nullable),
                         ser_safes.append(q.target_list[col_idx].ser_safe))
                )
                nullable = any(nullables)
                ser_safe = all(ser_safes)
            else:
                rt = rvar.subquery.target_list[col_idx]
                nullable = rt.nullable
                ser_safe = rt.ser_safe

        elif isinstance(rvar, pgast.RangeFunction):
            # Range over a function.
            # TODO: look into the possibility of inspecting coldeflist.
            nullable = True

        elif isinstance(rvar, pgast.JoinExpr):
            raise RuntimeError(
                f'cannot find {colname!r} in unexpected {rvar!r} range var')

    name = [rvar.alias.aliasname, colname]

    return pgast.ColumnRef(name=name, nullable=nullable, ser_safe=ser_safe)


def rvar_for_rel(
        rel: pgast.BaseRelation, *,
        lateral: bool=False, colnames: typing.List[str]=[],
        env: context.Environment) -> pgast.BaseRangeVar:
    if isinstance(rel, pgast.Query):
        alias = env.aliases.get(rel.name or 'q')

        rvar = pgast.RangeSubselect(
            subquery=rel,
            alias=pgast.Alias(aliasname=alias, colnames=colnames),
            lateral=lateral,
        )
    else:
        alias = env.aliases.get(rel.name)

        rvar = pgast.RangeVar(
            relation=rel,
            alias=pgast.Alias(aliasname=alias, colnames=colnames)
        )

    return rvar


def get_rvar_var(
        rvar: pgast.BaseRangeVar,
        var: pgast.OutputVar) -> pgast.OutputVar:

    assert isinstance(var, pgast.OutputVar)

    if isinstance(var, pgast.TupleVar):
        elements = []

        for el in var.elements:
            val = get_rvar_var(rvar, el.name)
            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        fieldref = pgast.TupleVar(elements, named=var.named)
    else:
        fieldref = get_column(rvar, var)

    return fieldref


def strip_output_var(
        var: pgast.OutputVar, *,
        optional: typing.Optional[bool]=None,
        nullable: typing.Optional[bool]=None) -> pgast.OutputVar:

    if isinstance(var, pgast.TupleVar):
        elements = []

        for el in var.elements:
            if isinstance(el.name, str):
                val = pgast.ColumnRef(name=[el.name])
            else:
                val = strip_output_var(el.name)

            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        result = pgast.TupleVar(elements, named=var.named)
    else:
        result = pgast.ColumnRef(
            name=[var.name[-1]],
            optional=optional if optional is not None else var.optional,
            nullable=nullable if nullable is not None else var.nullable,
        )

    return result


def add_rel_overlay(
        typeid: str, op: str, rel: pgast.BaseRelation, *,
        env: context.Environment) -> None:
    overlays = env.rel_overlays[typeid]
    overlays.append((op, rel))


def cte_for_query(
        rel: pgast.Query, *,
        env: context.Environment) -> pgast.CommonTableExpr:
    return pgast.CommonTableExpr(
        query=rel,
        alias=pgast.Alias(
            aliasname=env.aliases.get(rel.name)
        )
    )
