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


"""Compilation helpers for output formatting and serialization."""

from __future__ import annotations
from typing import Optional, Tuple, Union, Sequence, List

import itertools

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import casts as s_casts
from edb.schema import defines as s_defs
from edb.schema import name as sn

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pgtypes

from . import astutils
from . import context


_JSON_FORMATS = {context.OutputFormat.JSON, context.OutputFormat.JSON_ELEMENTS}


def _get_json_func(
    name: str,
    *,
    output_format: Optional[context.OutputFormat] = None,
    env: context.Environment,
) -> Tuple[str, ...]:

    if output_format is None:
        output_format = env.output_format

    if output_format in _JSON_FORMATS:
        prefix_suffix = 'json'
    else:
        prefix_suffix = 'jsonb'

    if name == 'to':
        return (f'{name}_{prefix_suffix}',)
    else:
        return (f'{prefix_suffix}_{name}',)


def _build_json(
    name: str,
    args: Sequence[pgast.BaseExpr],
    *,
    null_safe: bool = False,
    ser_safe: bool = False,
    nullable: Optional[bool] = None,
    env: context.Environment,
) -> pgast.BaseExpr:
    # PostgreSQL has a limit on the maximum number of arguments
    # passed to a function call, so we must chop input into chunks
    # if the argument count is greater then the limit.

    if len(args) > s_defs.MAX_FUNC_ARG_COUNT:
        json_func = _get_json_func(
            name,
            output_format=context.OutputFormat.JSONB,
            env=env,
        )

        chunk_iters = [iter(args)] * s_defs.MAX_FUNC_ARG_COUNT
        chunks = list(itertools.zip_longest(*chunk_iters, fillvalue=None))
        if len(args) != len(chunks) * s_defs.MAX_FUNC_ARG_COUNT:
            chunks[-1] = tuple(filter(None, chunks[-1]))

        result: pgast.BaseExpr = pgast.FuncCall(
            name=json_func,
            args=list(chunks[0]),
            null_safe=null_safe,
            ser_safe=ser_safe,
            nullable=nullable,
        )

        for chunk in chunks[1:]:
            fc = pgast.FuncCall(
                name=json_func,
                args=list(chunk),
                null_safe=null_safe,
                ser_safe=ser_safe,
                nullable=nullable,
            )

            result = astutils.new_binop(
                lexpr=result,
                rexpr=fc,
                op='||',
            )

        if env.output_format in _JSON_FORMATS:
            result = pgast.TypeCast(
                arg=result,
                type_name=pgast.TypeName(
                    name=('json',)
                )
            )

        return result

    else:
        json_func = _get_json_func(name, env=env)

        return pgast.FuncCall(
            name=json_func,
            args=args,
            null_safe=null_safe,
            ser_safe=ser_safe,
            nullable=nullable,
        )


def coll_as_json_object(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    if irtyputils.is_tuple(styperef):
        return tuple_as_json_object(expr, styperef=styperef, env=env)
    elif irtyputils.is_array(styperef):
        return array_as_json_object(expr, styperef=styperef, env=env)
    else:
        raise RuntimeError(f'{styperef!r} is not a collection')


def array_as_json_object(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    el_type = styperef.subtypes[0]

    is_tuple = irtyputils.is_tuple(el_type)
    # Tuples/ranges/scalars with custom casts need underlying casts to be done
    if (
        is_tuple
        or irtyputils.is_range(el_type)
        or irtyputils.is_multirange(el_type)
        or el_type.real_base_type.needs_custom_json_cast
    ):
        coldeflist = []

        out_alias = env.aliases.get('q')

        val: pgast.BaseExpr
        if is_tuple:
            json_args: List[pgast.BaseExpr] = []
            is_named = any(st.element_name for st in el_type.subtypes)
            for i, st in enumerate(el_type.subtypes):
                if is_named:
                    colname = st.element_name
                    assert colname
                    json_args.append(pgast.StringConstant(val=colname))
                else:
                    colname = str(i)

                val = pgast.ColumnRef(name=[colname])
                val = serialize_expr_to_json(
                    val, styperef=st, nested=True, env=env)

                json_args.append(val)

                if not irtyputils.is_persistent_tuple(el_type):
                    # Column definition list is only allowed for functions
                    # returning "record", i.e. an anonymous tuple, which
                    # would not be the case for schema-persistent tuple types.
                    coldeflist.append(
                        pgast.ColumnDef(
                            name=colname,
                            typename=pgast.TypeName(
                                name=pgtypes.pg_type_from_ir_typeref(st)
                            )
                        )
                    )

            json_func = 'build_object' if is_named else 'build_array'
            agg_arg = _build_json(json_func, json_args, env=env)

            needs_unnest = bool(el_type.subtypes)
        else:
            val = pgast.ColumnRef(name=[out_alias])
            agg_arg = serialize_expr_to_json(
                val, styperef=el_type, nested=True, env=env)
            needs_unnest = True

        return pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.CoalesceExpr(
                        args=[
                            pgast.FuncCall(
                                name=_get_json_func('agg', env=env),
                                args=[agg_arg],
                            ),
                            pgast.StringConstant(val='[]'),
                        ]
                    ),
                    ser_safe=True,
                )
            ],
            from_clause=[
                pgast.RangeFunction(
                    alias=pgast.Alias(aliasname=out_alias),
                    is_rowsfrom=True,
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[expr],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            ] if needs_unnest else [],
        )
    else:
        return pgast.FuncCall(
            name=_get_json_func('to', env=env), args=[expr],
            null_safe=True, ser_safe=True)


def tuple_as_json_object(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    if any(st.element_name for st in styperef.subtypes):
        return named_tuple_as_json_object(expr, styperef=styperef, env=env)
    else:
        return unnamed_tuple_as_json_object(expr, styperef=styperef, env=env)


def unnamed_tuple_as_json_object(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    vals: List[pgast.BaseExpr] = []

    if irtyputils.is_persistent_tuple(styperef):
        for el_idx, el_type in enumerate(styperef.subtypes):
            val: pgast.BaseExpr = pgast.Indirection(
                arg=expr,
                indirection=[pgast.RecordIndirectionOp(name=str(el_idx))],
            )
            val = serialize_expr_to_json(
                val, styperef=el_type, nested=True, env=env)
            vals.append(val)

        obj = _build_json(
            'build_array',
            args=vals,
            null_safe=True,
            ser_safe=True,
            nullable=expr.nullable,
            env=env,
        )

    else:
        coldeflist = []

        for el_idx, el_type in enumerate(styperef.subtypes):

            coldeflist.append(pgast.ColumnDef(
                name=str(el_idx),
                typename=pgast.TypeName(
                    name=pgtypes.pg_type_from_ir_typeref(el_type),
                ),
            ))

            val = pgast.ColumnRef(name=[str(el_idx)])

            val = serialize_expr_to_json(
                val, styperef=el_type, nested=True, env=env)

            vals.append(val)

        obj = _build_json(
            'build_array',
            args=vals,
            null_safe=True,
            ser_safe=True,
            nullable=expr.nullable,
            env=env,
        )

        obj = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=obj,
                ),
            ],
            from_clause=[
                pgast.RangeFunction(
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[
                                pgast.ArrayExpr(
                                    elements=[expr],
                                )
                            ],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            ] if styperef.subtypes else []
        )

    if expr.nullable:
        obj = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=obj)],
            where_clause=pgast.NullTest(arg=expr, negated=True)
        )
    return obj


def named_tuple_as_json_object(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    keyvals: List[pgast.BaseExpr] = []

    if irtyputils.is_persistent_tuple(styperef):
        for el_type in styperef.subtypes:
            assert el_type.element_name
            keyvals.append(pgast.StringConstant(val=el_type.element_name))
            val: pgast.BaseExpr = pgast.Indirection(
                arg=expr,
                indirection=[
                    pgast.RecordIndirectionOp(
                        name=el_type.element_name
                    )
                ]
            )
            val = serialize_expr_to_json(
                val, styperef=el_type, nested=True, env=env)
            keyvals.append(val)

        obj = _build_json(
            'build_object',
            args=keyvals,
            null_safe=True,
            ser_safe=True,
            nullable=expr.nullable,
            env=env,
        )

    else:
        coldeflist = []

        for el_type in styperef.subtypes:
            assert el_type.element_name
            keyvals.append(pgast.StringConstant(val=el_type.element_name))

            coldeflist.append(pgast.ColumnDef(
                name=el_type.element_name,
                typename=pgast.TypeName(
                    name=pgtypes.pg_type_from_ir_typeref(el_type),
                ),
            ))

            val = pgast.ColumnRef(name=[el_type.element_name])

            val = serialize_expr_to_json(
                val, styperef=el_type, nested=True, env=env)

            keyvals.append(val)

        obj = _build_json(
            'build_object',
            args=keyvals,
            null_safe=True,
            ser_safe=True,
            nullable=expr.nullable,
            env=env,
        )

        obj = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=obj,
                ),
            ],
            from_clause=[
                pgast.RangeFunction(
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[
                                pgast.ArrayExpr(
                                    elements=[expr],
                                )
                            ],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            ] if styperef.subtypes else []
        )

    if expr.nullable:
        obj = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=obj)],
            where_clause=pgast.NullTest(arg=expr, negated=True)
        )
    return obj


def tuple_var_as_json_object(
    tvar: pgast.TupleVar,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:

    if not tvar.named:
        vals = [
            serialize_expr(t.val, path_id=t.path_id, nested=True, env=env)
            for t in tvar.elements
        ]

        return _build_json(
            'build_array',
            args=vals,
            null_safe=True,
            ser_safe=True,
            nullable=tvar.nullable,
            env=env,
        )
    else:
        keyvals: List[pgast.BaseExpr] = []

        for element in tvar.elements:
            rptr = element.path_id.rptr()
            assert rptr is not None
            name = rptr.shortname.name
            if rptr.source_ptr is not None:
                name = '@' + name
            keyvals.append(pgast.StringConstant(val=name))
            val = serialize_expr(
                element.val, path_id=element.path_id, nested=True, env=env)
            keyvals.append(val)

        return _build_json(
            'build_object',
            args=keyvals,
            null_safe=True,
            ser_safe=True,
            nullable=tvar.nullable,
            env=env,
        )


def in_serialization_ctx(ctx: context.CompilerContextLevel) -> bool:
    return ctx.expr_exposed is None or ctx.expr_exposed


def serialize_custom_tuple(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    """Serialize a tuple that needs custom serialization for a component"""
    vals: List[pgast.BaseExpr] = []

    obj: pgast.BaseExpr

    if irtyputils.is_persistent_tuple(styperef):
        for el_idx, el_type in enumerate(styperef.subtypes):
            val: pgast.BaseExpr = pgast.Indirection(
                arg=expr,
                indirection=[
                    pgast.RecordIndirectionOp(name=str(el_idx)),
                ],
            )
            val = output_as_value(
                val, ser_typeref=el_type, env=env)
            vals.append(val)

        obj = _row(vals)

    else:
        coldeflist = []

        for el_idx, el_type in enumerate(styperef.subtypes):

            coldeflist.append(pgast.ColumnDef(
                name=str(el_idx),
                typename=pgast.TypeName(
                    name=pgtypes.pg_type_from_ir_typeref(el_type),
                ),
            ))

            val = pgast.ColumnRef(name=[str(el_idx)])

            val = output_as_value(
                val, ser_typeref=el_type, env=env)

            vals.append(val)

        obj = _row(vals)

        obj = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=obj,
                ),
            ],
            from_clause=[
                pgast.RangeFunction(
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[
                                pgast.ArrayExpr(
                                    elements=[expr],
                                )
                            ],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            ] if styperef.subtypes else []
        )

    if expr.nullable:
        obj = pgast.SelectStmt(
            target_list=[pgast.ResTarget(val=obj)],
            where_clause=pgast.NullTest(arg=expr, negated=True)
        )
    return obj


def serialize_custom_array(
    expr: pgast.BaseExpr,
    *,
    styperef: irast.TypeRef,
    env: context.Environment,
) -> pgast.BaseExpr:
    """Serialize an array that needs custom serialization for a component"""
    el_type = styperef.subtypes[0]
    is_tuple = irtyputils.is_tuple(el_type)

    if is_tuple:
        coldeflist = []

        out_alias = env.aliases.get('q')

        val: pgast.BaseExpr
        args: List[pgast.BaseExpr] = []
        is_named = any(st.element_name for st in el_type.subtypes)
        for i, st in enumerate(el_type.subtypes):
            if is_named:
                colname = st.element_name
                assert colname
                args.append(pgast.StringConstant(val=colname))
            else:
                colname = str(i)

            val = pgast.ColumnRef(name=[colname])
            val = output_as_value(val, ser_typeref=st, env=env)

            args.append(val)

            if not irtyputils.is_persistent_tuple(el_type):
                # Column definition list is only allowed for functions
                # returning "record", i.e. an anonymous tuple, which
                # would not be the case for schema-persistent tuple types.
                coldeflist.append(
                    pgast.ColumnDef(
                        name=colname,
                        typename=pgast.TypeName(
                            name=pgtypes.pg_type_from_ir_typeref(st)
                        )
                    )
                )

        agg_arg: pgast.BaseExpr = _row(args)

        return pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.CoalesceExpr(
                        args=[
                            pgast.FuncCall(
                                name=('array_agg',),
                                args=[agg_arg],
                            ),
                            pgast.TypeCast(
                                arg=pgast.ArrayExpr(elements=[]),
                                type_name=pgast.TypeName(name=('record[]',)),
                            ),
                        ]
                    ),
                    ser_safe=True,
                )
            ],
            from_clause=[
                pgast.RangeFunction(
                    alias=pgast.Alias(aliasname=out_alias),
                    is_rowsfrom=True,
                    functions=[
                        pgast.FuncCall(
                            name=('unnest',),
                            args=[expr],
                            coldeflist=coldeflist,
                        )
                    ]
                )
            ]
        )
    else:
        el_sql_type = el_type.real_base_type.custom_sql_serialization
        return pgast.TypeCast(
            arg=expr,
            type_name=pgast.TypeName(name=(f'{el_sql_type}[]',)),
        )


def _row(
    args: list[pgast.BaseExpr]
) -> Union[pgast.ImplicitRowExpr, pgast.RowExpr]:
    if len(args) > 1:
        return pgast.ImplicitRowExpr(args=args)
    else:
        return pgast.RowExpr(args=args)


def output_as_value(
        expr: pgast.BaseExpr, *,
        ser_typeref: Optional[irast.TypeRef] = None,
        env: context.Environment) -> pgast.BaseExpr:
    """Format an expression as a proper value.

    Normally this just means packing TupleVars into real expressions,
    but if ser_typeref is provided, we also will do binary serialization.

    In particular, certain types actually need to be serialized as text or
    or some other format, and we handle that here.
    """

    needs_custom_serialization = ser_typeref and (
        irtyputils.needs_custom_serialization(ser_typeref))

    val = expr

    if isinstance(expr, pgast.TupleVar):
        if (
            env.output_format is context.OutputFormat.NATIVE_INTERNAL
            and len(expr.elements) == 1
            and (path_id := (el0 := expr.elements[0]).path_id) is not None
            and (rptr_name := path_id.rptr_name()) is not None
            and (rptr_name.name == 'id')
        ):
            # This is is a special mode whereby bare refs to objects
            # are serialized to UUID values.
            return output_as_value(el0.val, env=env)

        ser_typerefs = [
            ser_typeref.subtypes[i]
            if ser_typeref and ser_typeref.subtypes else None
            for i in range(len(expr.elements))
        ]
        val = _row([
            output_as_value(e.val, ser_typeref=ser_typerefs[i], env=env)
            for i, e in enumerate(expr.elements)
        ])

        if (expr.typeref is not None
                and not needs_custom_serialization
                and not env.singleton_mode
                and irtyputils.is_persistent_tuple(expr.typeref)):
            pg_type = pgtypes.pg_type_from_ir_typeref(expr.typeref)
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(
                    name=pg_type,
                ),
            )

    elif (needs_custom_serialization and not expr.ser_safe):
        assert ser_typeref is not None
        if irtyputils.is_array(ser_typeref):
            return serialize_custom_array(expr, styperef=ser_typeref, env=env)
        elif irtyputils.is_tuple(ser_typeref):
            return serialize_custom_tuple(expr, styperef=ser_typeref, env=env)
        else:
            el_sql_type = ser_typeref.real_base_type.custom_sql_serialization
            assert el_sql_type is not None
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=(el_sql_type,)),
            )

    return val


def add_null_test(expr: pgast.BaseExpr, query: pgast.SelectStmt) -> None:
    if not expr.nullable:
        return

    while isinstance(expr, pgast.TupleVar) and expr.elements:
        expr = expr.elements[0].val

    query.where_clause = astutils.extend_binop(
        query.where_clause,
        pgast.NullTest(arg=expr, negated=True)
    )


def serialize_expr_if_needed(
        expr: pgast.BaseExpr, *,
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    if in_serialization_ctx(ctx):
        val = serialize_expr(expr, path_id=path_id, env=ctx.env)
    else:
        val = expr

    return val


def serialize_expr_to_json(
        expr: pgast.BaseExpr, *,
        styperef: irast.TypeRef,
        nested: bool=False,
        env: context.Environment) -> pgast.BaseExpr:

    val: pgast.BaseExpr

    if isinstance(expr, pgast.TupleVar):
        val = tuple_var_as_json_object(expr, styperef=styperef, env=env)

    elif isinstance(expr, (pgast.RowExpr, pgast.ImplicitRowExpr)):
        val = _build_json(
            'build_array',
            args=expr.args,
            null_safe=True,
            ser_safe=True,
            env=env,
        )

    elif irtyputils.is_range(styperef) and not expr.ser_safe:
        val = pgast.FuncCall(
            # Use the actual generic helper for converting anyrange to jsonb
            name=common.maybe_versioned_name(
                ('edgedb', 'range_to_jsonb'),
                versioned=env.versioned_stdlib,
            ),
            args=[expr], null_safe=True, ser_safe=True)
        if env.output_format in _JSON_FORMATS:
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=('json',))
            )

    elif irtyputils.is_multirange(styperef) and not expr.ser_safe:
        val = pgast.FuncCall(
            # Use the actual generic helper for converting anymultirange to
            # jsonb
            name=common.maybe_versioned_name(
                ('edgedb', 'multirange_to_jsonb'),
                versioned=env.versioned_stdlib,
            ),
            args=[expr], null_safe=True, ser_safe=True)
        if env.output_format in _JSON_FORMATS:
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=('json',))
            )

    elif irtyputils.is_collection(styperef) and not expr.ser_safe:
        val = coll_as_json_object(expr, styperef=styperef, env=env)

    elif (
        styperef.real_base_type.needs_custom_json_cast
        and not expr.ser_safe
    ):
        base = styperef.real_base_type
        cast_name = s_casts.get_cast_fullname_from_names(
            base.orig_name_hint or base.name_hint,
            sn.QualName('std', 'json'),
        )
        val = pgast.FuncCall(
            name=common.get_cast_backend_name(
                cast_name, aspect='function', versioned=env.versioned_stdlib
            ),
            args=[expr], null_safe=True, ser_safe=True)
        if env.output_format in _JSON_FORMATS:
            val = pgast.TypeCast(
                arg=val,
                type_name=pgast.TypeName(name=('json',))
            )

    elif not nested:
        val = pgast.FuncCall(
            name=_get_json_func('to', env=env),
            args=[expr], null_safe=True, ser_safe=True)

    else:
        val = expr

    return val


def serialize_expr(
        expr: pgast.BaseExpr, *,
        path_id: irast.PathId,
        nested: bool=False,
        env: context.Environment) -> pgast.BaseExpr:

    if env.output_format in (context.OutputFormat.JSON,
                             context.OutputFormat.JSON_ELEMENTS,
                             context.OutputFormat.JSONB):
        val = serialize_expr_to_json(
            expr, styperef=path_id.target, nested=nested, env=env)

    elif env.output_format in (context.OutputFormat.NATIVE,
                               context.OutputFormat.NATIVE_INTERNAL,
                               context.OutputFormat.NONE):
        val = output_as_value(expr, ser_typeref=path_id.target, env=env)

    else:
        raise RuntimeError(f'unexpected output format: {env.output_format!r}')

    return val


def get_pg_type(
        typeref: irast.TypeRef, *,
        ctx: context.CompilerContextLevel) -> Tuple[str, ...]:

    if in_serialization_ctx(ctx):
        if ctx.env.output_format is context.OutputFormat.JSONB:
            return ('jsonb',)
        elif ctx.env.output_format in _JSON_FORMATS:
            return ('json',)
        elif irtyputils.is_object(typeref):
            return ('record',)
        else:
            return pgtypes.pg_type_from_ir_typeref(typeref)

    else:
        return pgtypes.pg_type_from_ir_typeref(typeref)


def aggregate_json_output(
        stmt: pgast.SelectStmt,
        ir_set: irast.Set, *,
        env: context.Environment) -> pgast.SelectStmt:

    subrvar = pgast.RangeSubselect(
        subquery=stmt,
        alias=pgast.Alias(
            aliasname=env.aliases.get('aggw')
        )
    )

    stmt_res = stmt.target_list[0]

    if stmt_res.name is None:
        stmt_res = stmt.target_list[0] = pgast.ResTarget(
            name=env.aliases.get('v'),
            val=stmt_res.val,
        )
        assert stmt_res.name is not None

    new_val = pgast.CoalesceExpr(
        args=[
            pgast.FuncCall(
                name=_get_json_func('agg', env=env),
                args=[pgast.ColumnRef(name=[stmt_res.name])]
            ),
            pgast.StringConstant(val='[]')
        ]
    )

    result = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=new_val
            )
        ],

        from_clause=[
            subrvar
        ]
    )

    result.ctes = stmt.ctes
    stmt.ctes = []

    return result


def wrap_script_stmt(
    stmt: pgast.SelectStmt,
    *,
    suppress_all_output: bool = False,
    env: context.Environment,
) -> pgast.SelectStmt:

    subrvar = pgast.RangeSubselect(
        subquery=stmt,
        alias=pgast.Alias(
            aliasname=env.aliases.get('aggw')
        )
    )

    stmt_res = stmt.target_list[0]

    if stmt_res.name is None:
        stmt_res = stmt.target_list[0] = pgast.ResTarget(
            name=env.aliases.get('v'),
            val=stmt_res.val,
        )
        assert stmt_res.name is not None

    count_val = pgast.FuncCall(
        name=('count',),
        args=[pgast.ColumnRef(name=[stmt_res.name])]
    )

    result = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=count_val,
                name=stmt_res.name,
            ),
        ],

        from_clause=[
            subrvar,
        ]
    )

    if suppress_all_output:
        subrvar = pgast.RangeSubselect(
            subquery=result,
            alias=pgast.Alias(
                aliasname=env.aliases.get('q')
            )
        )

        result = pgast.SelectStmt(
            target_list=[],
            from_clause=[
                subrvar,
            ],
            where_clause=pgast.NullTest(
                arg=pgast.ColumnRef(
                    name=[subrvar.alias.aliasname, stmt_res.name],
                ),
            ),
        )

    result.ctes = stmt.ctes
    stmt.ctes = []

    return result


def top_output_as_value(
        stmt: pgast.SelectStmt,
        ir_set: irast.Set, *,
        env: context.Environment) -> pgast.SelectStmt:
    """Finalize output serialization on the top level."""

    if (env.output_format is context.OutputFormat.JSON and
            not env.expected_cardinality_one):
        # For JSON we just want to aggregate the whole thing
        # into a JSON array.
        return aggregate_json_output(stmt, ir_set, env=env)

    elif (
        env.explicit_top_cast is not None
        and (
            env.output_format is context.OutputFormat.NATIVE
            or env.output_format is context.OutputFormat.NATIVE_INTERNAL
        )
    ):
        typecast = pgast.TypeCast(
            arg=stmt.target_list[0].val,
            type_name=pgast.TypeName(
                name=pgtypes.pg_type_from_ir_typeref(
                    env.explicit_top_cast,
                    persistent_tuples=True,
                ),
            ),
        )

        stmt.target_list[0] = pgast.ResTarget(
            name=env.aliases.get('v'),
            val=typecast,
        )

        return stmt

    elif env.output_format is context.OutputFormat.NONE:
        return wrap_script_stmt(stmt, env=env)

    else:
        # JSON_ELEMENTS and BINARY don't require any wrapping
        return stmt
