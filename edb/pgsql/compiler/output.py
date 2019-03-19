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

import typing

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.pgsql import ast as pgast
from edb.pgsql import types as pgtypes

from . import context


def _get_json_func(name: str, *,
                   env: context.Environment) -> typing.Tuple[str, ...]:
    if env.output_format is context.OutputFormat.JSON:
        prefix_suffix = 'json'
    else:
        prefix_suffix = 'jsonb'

    if name == 'to':
        return (f'{name}_{prefix_suffix}',)
    else:
        return (f'{prefix_suffix}_{name}',)


def coll_as_json_object(expr, *, styperef, env):
    if irtyputils.is_tuple(styperef):
        return tuple_as_json_object(expr, styperef=styperef, env=env)
    elif irtyputils.is_array(styperef):
        return array_as_json_object(expr, styperef=styperef, env=env)
    else:
        raise RuntimeError(f'{styperef!r} is not a collection')


def array_as_json_object(expr, *, styperef, env):
    el_type = styperef.subtypes[0]

    if irtyputils.is_tuple(el_type):
        coldeflist = []
        json_args = []
        is_named = any(st.element_name for st in el_type.subtypes)

        for i, st in enumerate(el_type.subtypes):
            if is_named:
                colname = env.aliases.get(st.element_name)
                json_args.append(pgast.StringConstant(val=st.element_name))
            else:
                colname = env.aliases.get(str(i))

            val = pgast.ColumnRef(name=[colname])
            if irtyputils.is_collection(st):
                val = coll_as_json_object(val, styperef=st, env=env)

            json_args.append(val)

            coldeflist.append(
                pgast.ColumnDef(
                    name=colname,
                    typename=pgast.TypeName(
                        name=pgtypes.pg_type_from_ir_typeref(st)
                    )
                )
            )

        if is_named:
            json_func = _get_json_func('build_object', env=env)
        else:
            json_func = _get_json_func('build_array', env=env)

        return pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.CoalesceExpr(
                        args=[
                            pgast.FuncCall(
                                name=_get_json_func('agg', env=env),
                                args=[
                                    pgast.FuncCall(
                                        name=json_func,
                                        args=json_args,
                                    )
                                ]
                            ),
                            pgast.StringConstant(val='[]'),
                        ]
                    ),
                    ser_safe=True,
                )
            ],
            from_clause=[
                pgast.RangeFunction(
                    alias=pgast.Alias(
                        aliasname=env.aliases.get('q'),
                    ),
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
        return pgast.FuncCall(
            name=_get_json_func('to', env=env), args=[expr],
            null_safe=True, ser_safe=True)


def tuple_as_json_object(expr, *, styperef, env):
    if any(st.element_name for st in styperef.subtypes):
        return named_tuple_as_json_object(expr, styperef=styperef, env=env)
    else:
        return unnamed_tuple_as_json_object(expr, styperef=styperef, env=env)


def unnamed_tuple_as_json_object(expr, *, styperef, env):
    vals = []
    for el_idx, el_type in enumerate(styperef.subtypes):
        type_sentinel = pgast.TypeCast(
            arg=pgast.NullConstant(),
            type_name=pgast.TypeName(
                name=pgtypes.pg_type_from_ir_typeref(el_type)
            )
        )

        val = pgast.FuncCall(
            name=('edgedb', 'row_getattr_by_num'),
            args=[
                expr,
                pgast.NumericConstant(val=str(el_idx + 1)),
                type_sentinel
            ])

        if irtyputils.is_collection(el_type):
            val = coll_as_json_object(val, styperef=el_type, env=env)

        vals.append(val)

    return pgast.FuncCall(
        name=('edgedb',) + _get_json_func('row_to_array', env=env),
        args=[expr], null_safe=True, ser_safe=True, nullable=expr.nullable)


def named_tuple_as_json_object(expr, *, styperef, env):
    keyvals = []
    for el_idx, el_type in enumerate(styperef.subtypes):
        keyvals.append(pgast.StringConstant(val=el_type.element_name))

        type_sentinel = pgast.TypeCast(
            arg=pgast.NullConstant(),
            type_name=pgast.TypeName(
                name=pgtypes.pg_type_from_ir_typeref(el_type)
            )
        )

        val = pgast.FuncCall(
            name=('edgedb', 'row_getattr_by_num'),
            args=[
                expr,
                pgast.NumericConstant(val=str(el_idx + 1)),
                type_sentinel
            ])

        if irtyputils.is_collection(el_type):
            val = coll_as_json_object(val, styperef=el_type, env=env)

        keyvals.append(val)

    return pgast.FuncCall(
        name=_get_json_func('build_object', env=env),
        args=keyvals, null_safe=True, ser_safe=True, nullable=expr.nullable)


def tuple_var_as_json_object(tvar, *, path_id, env):
    if not tvar.named:
        return pgast.FuncCall(
            name=_get_json_func('build_array', env=env),
            args=[
                serialize_expr(t.val, path_id=t.path_id, nested=True, env=env)
                for t in tvar.elements
            ],
            null_safe=True, ser_safe=True, nullable=tvar.nullable)
    else:
        keyvals = []

        for element in tvar.elements:
            rptr = element.path_id.rptr()
            name = rptr.shortname.name
            if rptr.parent_ptr is not None:
                name = '@' + name
            keyvals.append(pgast.StringConstant(val=name))
            val = serialize_expr(
                element.val, path_id=element.path_id, nested=True, env=env)
            keyvals.append(val)

        return pgast.FuncCall(
            name=_get_json_func('build_object', env=env),
            args=keyvals, null_safe=True, ser_safe=True,
            nullable=tvar.nullable)


def in_serialization_ctx(ctx: context.CompilerContextLevel) -> bool:
    return ctx.expr_exposed is None or ctx.expr_exposed


def output_as_value(
        expr: pgast.Base, *,
        env: context.Environment) -> pgast.Base:

    if isinstance(expr, pgast.TupleVar):
        if len(expr.elements) > 1:
            RowCls = pgast.ImplicitRowExpr
        else:
            RowCls = pgast.RowExpr

        val = RowCls(args=[
            output_as_value(e.val, env=env) for e in expr.elements
        ])
    else:
        val = expr

    return val


def serialize_expr_if_needed(
        expr: pgast.Base, *,
        path_id: irast.PathId,
        ctx: context.CompilerContextLevel) -> pgast.Base:
    if in_serialization_ctx(ctx):
        val = serialize_expr(expr, path_id=path_id, env=ctx.env)
    else:
        val = expr

    return val


def serialize_expr_to_json(
        expr: pgast.Base, *,
        path_id: irast.PathId,
        nested: bool=False,
        env: context.Environment) -> pgast.Base:

    if isinstance(expr, pgast.TupleVar):
        val = tuple_var_as_json_object(expr, path_id=path_id, env=env)

    elif isinstance(expr, (pgast.RowExpr, pgast.ImplicitRowExpr)):
        val = pgast.FuncCall(
            name=_get_json_func('build_array', env=env),
            args=expr.args, null_safe=True, ser_safe=True,)

    elif path_id.is_collection_path() and not expr.ser_safe:
        val = coll_as_json_object(expr, styperef=path_id.target, env=env)

    elif not nested:
        val = pgast.FuncCall(
            name=_get_json_func('to', env=env),
            args=[expr], null_safe=True, ser_safe=True)

    else:
        val = expr

    return val


def serialize_expr(
        expr: pgast.Base, *,
        path_id: irast.PathId,
        nested: bool=False,
        env: context.Environment) -> pgast.Base:

    if env.output_format in (context.OutputFormat.JSON,
                             context.OutputFormat.JSONB):
        val = serialize_expr_to_json(
            expr, path_id=path_id, nested=nested, env=env)

    elif env.output_format == context.OutputFormat.NATIVE:
        val = output_as_value(expr, env=env)

    else:
        raise RuntimeError(f'unexpected output format: {env.output_format!r}')

    return val


def get_pg_type(
        typeref: irast.TypeRef, *,
        ctx: context.CompilerContextLevel) -> typing.Tuple[str]:

    if in_serialization_ctx(ctx):
        if ctx.env.output_format is context.OutputFormat.JSONB:
            return ('jsonb',)
        elif ctx.env.output_format is context.OutputFormat.JSON:
            return ('json',)
        elif irtyputils.is_object(typeref):
            return ('record',)
        else:
            return pgtypes.pg_type_from_ir_typeref(typeref)

    else:
        return pgtypes.pg_type_from_ir_typeref(typeref)


def top_output_as_value(
        stmt: pgast.Query, *,
        env: context.Environment) -> pgast.Query:
    """Finalize output serialization on the top level."""

    if (env.output_format is context.OutputFormat.JSON and
            not env.expected_cardinality_one):
        # For JSON we just want to aggregate the whole thing
        # into a JSON array.
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

        new_val = pgast.FuncCall(
            name=_get_json_func('agg', env=env),
            args=[pgast.ColumnRef(name=[stmt_res.name])]
        )

        new_val = pgast.CoalesceExpr(
            args=[
                new_val,
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
        result.argnames = stmt.argnames
        stmt.ctes = []

        return result

    else:
        return stmt
