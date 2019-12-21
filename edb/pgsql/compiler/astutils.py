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


"""Context-agnostic SQL AST utilities."""


from __future__ import annotations

from typing import *  # NoQA

from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import context


def tuple_element_for_shape_el(
    shape_el: irast.Set,
    value: Optional[pgast.BaseExpr],
    *,
    ctx: context.CompilerContextLevel
) -> pgast.TupleElementBase:
    if shape_el.path_id.is_type_intersection_path():
        rptr = shape_el.rptr.source.rptr
    else:
        rptr = shape_el.rptr
    ptrref = rptr.ptrref
    ptrname = ptrref.shortname

    if value is not None:
        return pgast.TupleElement(
            path_id=shape_el.path_id,
            name=ptrname,
            val=value,
        )
    else:
        return pgast.TupleElementBase(
            path_id=shape_el.path_id,
            name=ptrname,
        )


def tuple_getattr(
    tuple_val: pgast.BaseExpr,
    tuple_typeref: irast.TypeRef,
    attr: str,
) -> pgast.BaseExpr:

    ttypes = []
    pgtypes = []
    for i, st in enumerate(tuple_typeref.subtypes):
        pgtype = pg_types.pg_type_from_ir_typeref(st)
        pgtypes.append(pgtype)

        if st.element_name:
            ttypes.append(st.element_name)
        else:
            ttypes.append(str(i))

    index = ttypes.index(attr)

    set_expr: pgast.BaseExpr

    if tuple_typeref.in_schema:
        set_expr = pgast.Indirection(
            arg=tuple_val,
            indirection=[
                pgast.ColumnRef(
                    name=[attr],
                ),
            ],
        )
    else:
        set_expr = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=[str(index)],
                    ),
                ),
            ],
            from_clause=[
                pgast.RangeFunction(
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[
                                pgast.ArrayExpr(
                                    elements=[tuple_val],
                                )
                            ],
                            coldeflist=[
                                pgast.ColumnDef(
                                    name=str(i),
                                    typename=pgast.TypeName(
                                        name=t
                                    )
                                )
                                for i, t in enumerate(pgtypes)
                            ]
                        )
                    ]
                )
            ]
        )

    return set_expr


def is_null_const(expr: pgast.BaseExpr) -> bool:
    if isinstance(expr, pgast.TypeCast):
        expr = expr.arg
    return isinstance(expr, pgast.NullConstant)


def is_set_op_query(query: pgast.BaseExpr) -> bool:
    return (
        isinstance(query, pgast.SelectStmt)
        and query.op is not None
    )


def get_leftmost_query(query: pgast.Query) -> pgast.Query:
    result = query
    while is_set_op_query(result):
        result = cast(pgast.SelectStmt, result)
        result = result.larg
    return result


def for_each_query_in_set(
    qry: pgast.Query,
    cb: Callable[[pgast.Query], Any],
) -> List[Any]:
    if is_set_op_query(qry):
        qry = cast(pgast.SelectStmt, qry)
        result = for_each_query_in_set(qry.larg, cb)
        result.extend(for_each_query_in_set(qry.rarg, cb))
    else:
        result = [cb(qry)]

    return result


def new_binop(
    lexpr: pgast.BaseExpr,
    rexpr: pgast.BaseExpr,
    op: str,
) -> pgast.Expr:
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        lexpr=lexpr,
        rexpr=rexpr
    )


def extend_binop(
    binop: Optional[pgast.BaseExpr],
    *exprs: pgast.BaseExpr,
    op: str = 'AND',
) -> pgast.BaseExpr:
    exprlist = list(exprs)
    result: pgast.BaseExpr

    if binop is None:
        result = exprlist.pop(0)
    else:
        result = binop

    for expr in exprlist:
        if expr is not None and expr is not result:
            result = new_binop(lexpr=result, op=op, rexpr=expr)

    return result


def new_unop(op: str, expr: pgast.BaseExpr) -> pgast.Expr:
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        rexpr=expr
    )


def join_condition(
    lref: pgast.ColumnRef,
    rref: pgast.ColumnRef,
) -> pgast.BaseExpr:
    path_cond: pgast.BaseExpr = new_binop(lref, rref, op='=')

    if lref.optional:
        opt_cond = pgast.NullTest(arg=lref)
        path_cond = extend_binop(path_cond, opt_cond, op='OR')

    if rref.optional:
        opt_cond = pgast.NullTest(arg=rref)
        path_cond = extend_binop(path_cond, opt_cond, op='OR')

    return path_cond


def safe_array_expr(
    elements: List[pgast.BaseExpr],
    *,
    ser_safe: bool = False,
) -> pgast.BaseExpr:
    result: pgast.BaseExpr = pgast.ArrayExpr(
        elements=elements,
        ser_safe=ser_safe,
    )
    if any(el.nullable for el in elements):
        result = pgast.FuncCall(
            name=('edgedb', '_nullif_array_nulls'),
            args=[result],
            ser_safe=ser_safe,
        )
    return result


def find_column_in_subselect_rvar(
    rvar: pgast.RangeSubselect,
    name: str,
) -> int:
    # Range over a subquery, we can inspect the output list
    # of the subquery.  If the subquery is a UNION (or EXCEPT),
    # we take the leftmost non-setop query.
    subquery = get_leftmost_query(rvar.subquery)
    for i, rt in enumerate(subquery.target_list):
        if rt.name == name:
            return i

    raise RuntimeError(f'cannot find {name!r} in {rvar} output')


def get_column(
        rvar: pgast.BaseRangeVar,
        colspec: Union[str, pgast.ColumnRef], *,
        nullable: bool=None) -> pgast.ColumnRef:

    if isinstance(colspec, pgast.ColumnRef):
        colname = colspec.name[-1]
    else:
        colname = colspec

    assert isinstance(colname, str)

    ser_safe = False

    if nullable is None:
        if isinstance(rvar, pgast.RelRangeVar):
            # Range over a relation, we cannot infer nullability in
            # this context, so assume it's true.
            nullable = True

        elif isinstance(rvar, pgast.RangeSubselect):
            col_idx = find_column_in_subselect_rvar(rvar, colname)
            if is_set_op_query(rvar.subquery):
                nullables = []
                ser_safes = []

                def _cb(q: pgast.Query) -> None:
                    nullables.append(q.target_list[col_idx].nullable)
                    ser_safes.append(q.target_list[col_idx].ser_safe)

                for_each_query_in_set(rvar.subquery, _cb)
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


def get_rvar_var(
        rvar: pgast.BaseRangeVar,
        var: pgast.OutputVar) -> pgast.OutputVar:

    fieldref: pgast.OutputVar

    if isinstance(var, pgast.TupleVarBase):
        elements = []

        for el in var.elements:
            assert isinstance(el.name, pgast.OutputVar)
            val = get_rvar_var(rvar, el.name)
            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el.name, val=val))

        fieldref = pgast.TupleVar(elements, named=var.named)

    elif isinstance(var, pgast.ColumnRef):
        fieldref = get_column(rvar, var)

    else:
        raise AssertionError(f'unexpected OutputVar subclass: {var!r}')

    return fieldref


def strip_output_var(
        var: pgast.OutputVar, *,
        optional: Optional[bool]=None,
        nullable: Optional[bool]=None) -> pgast.OutputVar:

    result: pgast.OutputVar

    if isinstance(var, pgast.TupleVarBase):
        elements = []

        for el in var.elements:
            val: pgast.OutputVar
            el_name = el.name

            if isinstance(el_name, str):
                val = pgast.ColumnRef(name=[el_name])
            elif isinstance(el_name, pgast.OutputVar):
                val = strip_output_var(el_name)
            else:
                raise AssertionError(
                    f'unexpected tuple element class: {el_name!r}')

            elements.append(
                pgast.TupleElement(
                    path_id=el.path_id, name=el_name, val=val))

        result = pgast.TupleVar(elements, named=var.named)

    elif isinstance(var, pgast.ColumnRef):
        result = pgast.ColumnRef(
            name=[var.name[-1]],
            optional=optional if optional is not None else var.optional,
            nullable=nullable if nullable is not None else var.nullable,
        )

    else:
        raise AssertionError(f'unexpected OutputVar subclass: {var!r}')

    return result
