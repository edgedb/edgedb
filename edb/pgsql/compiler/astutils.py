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


def tuple_element_for_shape_el(shape_el, value, *, ctx):
    if shape_el.path_id.is_type_indirection_path():
        rptr = shape_el.rptr.source.rptr
    else:
        rptr = shape_el.rptr
    ptrref = rptr.ptrref
    ptrname = ptrref.shortname

    return pgast.TupleElement(
        path_id=shape_el.path_id,
        name=ptrname,
        val=value,
    )


def tuple_getattr(tuple_val, tuple_typeref, attr):
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


def is_null_const(expr):
    if isinstance(expr, pgast.TypeCast):
        expr = expr.arg
    return isinstance(expr, pgast.NullConstant)


def is_set_op_query(query):
    return (
        isinstance(query, pgast.SelectStmt)
        and query.op is not None
    )


def get_leftmost_query(query):
    result = query
    while is_set_op_query(result):
        result = result.larg
    return result


def for_each_query_in_set(qry, cb):
    if qry.op:
        result = for_each_query_in_set(qry.larg, cb)
        result.extend(for_each_query_in_set(qry.rarg, cb))
    else:
        result = [cb(qry)]

    return result


def new_binop(lexpr, rexpr, op):
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        lexpr=lexpr,
        rexpr=rexpr
    )


def extend_binop(binop, *exprs, op='AND'):
    exprs = list(exprs)
    binop = binop or exprs.pop(0)

    for expr in exprs:
        if expr is not None and expr is not binop:
            binop = new_binop(lexpr=binop, op=op, rexpr=expr)

    return binop


def new_unop(op, expr):
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=op,
        rexpr=expr
    )


def join_condition(lref: pgast.ColumnRef, rref: pgast.ColumnRef) -> pgast.Base:
    path_cond = new_binop(lref, rref, op='=')

    if lref.optional:
        opt_cond = pgast.NullTest(arg=lref)
        path_cond = extend_binop(path_cond, opt_cond, op='OR')

    if rref.optional:
        opt_cond = pgast.NullTest(arg=rref)
        path_cond = extend_binop(path_cond, opt_cond, op='OR')

    return path_cond


def safe_array_expr(
    elements: List[pgast.BaseExpr],
    **kwargs,
) -> pgast.BaseExpr:
    result: pgast.BaseExpr = pgast.ArrayExpr(elements=elements, **kwargs)
    if any(el.nullable for el in elements):
        result = pgast.FuncCall(
            name=('edgedb', '_nullif_array_nulls'),
            args=[result],
            **kwargs,
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
                for_each_query_in_set(
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


def get_rvar_var(
        rvar: pgast.BaseRangeVar,
        var: pgast.OutputVar) -> pgast.OutputVar:

    assert isinstance(var, pgast.OutputVar)

    if isinstance(var, pgast.TupleVarBase):
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
        optional: Optional[bool]=None,
        nullable: Optional[bool]=None) -> pgast.OutputVar:

    if isinstance(var, pgast.TupleVarBase):
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
