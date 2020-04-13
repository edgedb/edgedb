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


"""Compilation handlers for non-statement expressions."""

from __future__ import annotations

from typing import *

from edb import errors

from edb.edgeql import qltypes as ql_ft

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pg_types

from . import astutils
from . import config
from . import context
from . import dispatch
from . import expr as expr_compiler  # NOQA
from . import output
from . import pathctx
from . import relgen
from . import shapecomp


@dispatch.compile.register(irast.Set)
def compile_Set(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    if ctx.singleton_mode:
        return _compile_set_in_singleton_mode(ir_set, ctx=ctx)

    is_toplevel = ctx.toplevel_stmt is context.NO_STMT

    _compile_set_impl(ir_set, ctx=ctx)

    if is_toplevel:
        if isinstance(ir_set.expr, irast.ConfigCommand):
            return config.top_output_as_config_op(
                ir_set, ctx.rel, env=ctx.env)
        else:
            return output.top_output_as_value(ctx.rel, ir_set, env=ctx.env)
    else:
        value = pathctx.get_path_value_var(
            ctx.rel, ir_set.path_id, env=ctx.env)

        return output.output_as_value(value, env=ctx.env)


@dispatch.visit.register(irast.Set)
def visit_Set(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    if ctx.singleton_mode:
        _compile_set_in_singleton_mode(ir_set, ctx=ctx)

    _compile_set_impl(ir_set, ctx=ctx)


def _compile_set_impl(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    is_toplevel = ctx.toplevel_stmt is context.NO_STMT

    if isinstance(ir_set.expr, irast.BaseConstant):
        # Avoid creating needlessly complicated constructs for
        # constant expressions.  Besides being an optimization,
        # this helps in GROUP BY queries.
        value = dispatch.compile(ir_set.expr, ctx=ctx)
        if is_toplevel:
            ctx.rel = ctx.toplevel_stmt = pgast.SelectStmt()
        pathctx.put_path_value_var(ctx.rel, ir_set.path_id, value, env=ctx.env)
        if (output.in_serialization_ctx(ctx) and ir_set.shape
                and not ctx.env.ignore_object_shapes):
            _compile_shape(ir_set, shape=ir_set.shape, ctx=ctx)

    elif ir_set.path_scope_id is not None and not is_toplevel:
        # This Set is behind a scope fence, so compute it
        # in a fenced context.
        with ctx.newscope() as scopectx:
            _compile_set(ir_set, ctx=scopectx)

    else:
        # All other sets.
        _compile_set(ir_set, ctx=ctx)


@dispatch.compile.register(irast.Parameter)
def compile_Parameter(
        expr: irast.Parameter, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    result: pgast.BaseParamRef
    is_decimal: bool = expr.name.isdecimal()

    if not is_decimal and ctx.env.use_named_params:
        result = pgast.NamedParamRef(name=expr.name)
    else:
        try:
            index = ctx.argmap[expr.name]
        except KeyError:
            if is_decimal:
                index = int(expr.name) + 1
            else:
                index = len(ctx.argmap) + 1
            ctx.argmap[expr.name] = index
        result = pgast.ParamRef(number=index)

    return pgast.TypeCast(
        arg=result,
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_ir_typeref(expr.typeref)
        )
    )


@dispatch.compile.register(irast.StringConstant)
def compile_StringConstant(
        expr: irast.StringConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    return pgast.TypeCast(
        arg=pgast.StringConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_ir_typeref(expr.typeref)
        )
    )


@dispatch.compile.register(irast.BytesConstant)
def compile_BytesConstant(
        expr: irast.StringConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    return pgast.ByteaConstant(val=expr.value)


@dispatch.compile.register(irast.FloatConstant)
@dispatch.compile.register(irast.DecimalConstant)
@dispatch.compile.register(irast.BigintConstant)
@dispatch.compile.register(irast.IntegerConstant)
def compile_FloatConstant(
        expr: irast.BaseConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    return pgast.TypeCast(
        arg=pgast.NumericConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_ir_typeref(expr.typeref)
        )
    )


@dispatch.compile.register(irast.BooleanConstant)
def compile_BooleanConstant(
        expr: irast.BooleanConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    return pgast.TypeCast(
        arg=pgast.BooleanConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_ir_typeref(expr.typeref)
        )
    )


@dispatch.compile.register(irast.TypeCast)
def compile_TypeCast(
        expr: irast.TypeCast, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    pg_expr = dispatch.compile(expr.expr, ctx=ctx)

    if expr.sql_cast:
        # Use explicit SQL cast.

        pg_type = pg_types.pg_type_from_ir_typeref(expr.to_type)
        return pgast.TypeCast(
            arg=pg_expr,
            type_name=pgast.TypeName(
                name=pg_type
            )
        )

    elif expr.sql_function or expr.sql_expr:
        # Cast implemented as a function.

        if expr.sql_expr:
            func_name = common.get_cast_backend_name(
                expr.cast_name, expr.cast_module_id, aspect='function')
        else:
            func_name = tuple(expr.sql_function.split('.'))

        return pgast.FuncCall(
            name=func_name,
            args=[pg_expr],
        )

    else:
        raise RuntimeError('cast not supported')


@dispatch.compile.register(irast.IndexIndirection)
def compile_IndexIndirection(
        expr: irast.IndexIndirection, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    # Handle Expr[Index], where Expr may be std::str, array<T> or
    # std::json. For strings we translate this into substr calls.
    # Arrays use the native index access. JSON is handled by using the
    # `->` accessor. Additionally, in all of the above cases a
    # boundary-check is performed on the index and an exception is
    # potentially raised.

    # line, column and filename are captured here to be used with the
    # error message
    srcctx = pgast.StringConstant(
        val=irutils.get_source_context_as_json(expr.index,
                                               errors.InvalidValueError))

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)
        index = dispatch.compile(expr.index, ctx=subctx)

    result = pgast.FuncCall(
        name=('edgedb', '_index'),
        args=[subj, index, srcctx]
    )

    return result


@dispatch.compile.register(irast.SliceIndirection)
def compile_SliceIndirection(
        expr: irast.SliceIndirection, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    # Handle Expr[Index], where Expr may be std::str, array<T> or
    # std::json. For strings we translate this into substr calls.
    # Arrays use the native slice syntax. JSON is handled by a
    # combination of unnesting aggregation and array slicing.
    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)
        if expr.start is None:
            start = pgast.NullConstant()
        else:
            start = dispatch.compile(expr.start, ctx=subctx)
        if expr.stop is None:
            stop = pgast.NullConstant()
        else:
            stop = dispatch.compile(expr.stop, ctx=subctx)

    result = pgast.FuncCall(
        name=('edgedb', '_slice'),
        args=[subj, start, stop]
    )

    return result


@dispatch.compile.register(irast.OperatorCall)
def compile_OperatorCall(
        expr: irast.OperatorCall, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    if (expr.func_shortname == 'std::IF'
            and expr.args[0].cardinality.is_single()
            and expr.args[2].cardinality.is_single()):
        if_expr, condition, else_expr = (a.expr for a in expr.args)
        return pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=dispatch.compile(condition, ctx=ctx),
                    result=dispatch.compile(if_expr, ctx=ctx))
            ],
            defresult=dispatch.compile(else_expr, ctx=ctx))

    if expr.typemod is ql_ft.TypeModifier.SET_OF:
        raise RuntimeError(
            f'set returning operator {expr.func_shortname!r} is not supported '
            f'in simple expressions')

    args = [dispatch.compile(a.expr, ctx=ctx) for a in expr.args]
    return compile_operator(expr, args, ctx=ctx)


def compile_operator(
        expr: irast.OperatorCall,
        args: Sequence[pgast.BaseExpr], *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    lexpr = rexpr = None
    if expr.operator_kind is ql_ft.OperatorKind.INFIX:
        lexpr, rexpr = args
    elif expr.operator_kind is ql_ft.OperatorKind.PREFIX:
        rexpr = args[0]
    elif expr.operator_kind is ql_ft.OperatorKind.POSTFIX:
        lexpr = args[0]
    else:
        raise RuntimeError(f'unexpected operator kind: {expr.operator_kind!r}')

    if ((expr.func_shortname in {'std::=', 'std::!='}
            or expr.origin_name in {'std::=', 'std::!='})
            and expr.args[0].expr.typeref is not None
            and irtyputils.is_object(expr.args[0].expr.typeref)
            and expr.args[1].expr.typeref is not None
            and irtyputils.is_object(expr.args[1].expr.typeref)):
        if expr.func_shortname == 'std::=' or expr.origin_name == 'std::=':
            sql_oper = '='
        else:
            sql_oper = '!='

    elif expr.sql_operator:
        sql_oper = expr.sql_operator[0]
        if len(expr.sql_operator) > 1:
            # Explicit operand types given in FROM SQL OPERATOR
            if lexpr is not None:
                lexpr = pgast.TypeCast(
                    arg=lexpr,
                    type_name=pgast.TypeName(
                        name=(expr.sql_operator[1],)
                    )
                )

            if rexpr is not None:
                rexpr_qry = None

                if (isinstance(rexpr, pgast.SubLink)
                        and isinstance(rexpr.expr, pgast.SelectStmt)):
                    rexpr_qry = rexpr.expr
                elif isinstance(rexpr, pgast.SelectStmt):
                    rexpr_qry = rexpr

                if rexpr_qry is not None:
                    # Handle cases like foo <op> ANY (SELECT) and
                    # foo <OP> (SELECT).
                    rexpr_qry.target_list[0] = pgast.ResTarget(
                        name=rexpr_qry.target_list[0].name,
                        val=pgast.TypeCast(
                            arg=rexpr_qry.target_list[0].val,
                            type_name=pgast.TypeName(
                                name=(expr.sql_operator[2],)
                            )
                        )
                    )
                else:
                    rexpr = pgast.TypeCast(
                        arg=rexpr,
                        type_name=pgast.TypeName(
                            name=(expr.sql_operator[2],)
                        )
                    )

    elif expr.origin_name is not None:
        sql_oper = common.get_operator_backend_name(
            expr.origin_name, expr.origin_module_id)[1]

    else:
        sql_oper = common.get_operator_backend_name(
            expr.func_shortname, expr.func_module_id)[1]

    result: pgast.BaseExpr = pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=sql_oper,
        lexpr=lexpr,
        rexpr=rexpr,
    )

    if expr.force_return_cast:
        # The underlying operator has a return value type
        # different from that of the EdgeQL operator declaration,
        # so we need to make an explicit cast here.
        result = pgast.TypeCast(
            arg=result,
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(expr.typeref)
            )
        )

    return result


@dispatch.compile.register(irast.TypeCheckOp)
def compile_TypeCheckOp(
        expr: irast.TypeCheckOp, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left = dispatch.compile(expr.left, ctx=newctx)
        negated = expr.op == 'IS NOT'

        result: pgast.BaseExpr

        if expr.result is not None:
            result = pgast.BooleanConstant(
                val='false' if not expr.result or negated else 'true')
        else:
            right: pgast.BaseExpr

            if expr.right.union:
                right = pgast.ArrayExpr(
                    elements=[
                        dispatch.compile(c, ctx=newctx)
                        for c in expr.right.union
                    ]
                )
            else:
                right = dispatch.compile(expr.right, ctx=newctx)

            result = pgast.FuncCall(
                name=('edgedb', 'issubclass'),
                args=[left, right])

            if negated:
                result = astutils.new_unop('NOT', result)

    return result


@dispatch.compile.register(irast.Array)
def compile_Array(
        expr: irast.Array, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    return relgen.build_array_expr(expr, elements, ctx=ctx)


@dispatch.compile.register(irast.Tuple)
def compile_Tuple(
        expr: irast.Tuple, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    ttype = expr.typeref
    ttypes = {}
    for i, st in enumerate(ttype.subtypes):
        if st.element_name:
            ttypes[st.element_name] = st
        else:
            ttypes[str(i)] = st
    telems = list(ttypes)

    elements = []

    for i, e in enumerate(expr.elements):
        telem = telems[i]
        ttype = ttypes[telem]
        val = dispatch.compile(e.val, ctx=ctx)
        elements.append(pgast.TupleElement(path_id=e.path_id, val=val))

    result = pgast.TupleVar(elements=elements, typeref=ttype)

    return output.output_as_value(result, env=ctx.env)


@dispatch.compile.register(irast.TypeRef)
def compile_TypeRef(
        expr: irast.TypeRef, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    if expr.collection:
        raise NotImplementedError()
    else:
        result = pgast.TypeCast(
            arg=pgast.StringConstant(val=str(expr.id)),
            type_name=pgast.TypeName(
                name=('uuid',)
            )
        )

    return result


@dispatch.compile.register(irast.FunctionCall)
def compile_FunctionCall(
        expr: irast.FunctionCall, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    if expr.typemod is ql_ft.TypeModifier.SET_OF:
        raise RuntimeError(
            'set returning functions are not supported in simple expressions')

    args = [dispatch.compile(a.expr, ctx=ctx) for a in expr.args]

    if expr.has_empty_variadic and expr.variadic_param_type is not None:
        var = pgast.TypeCast(
            arg=pgast.ArrayExpr(elements=[]),
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(expr.variadic_param_type)
            )
        )

        args.append(pgast.VariadicArgument(expr=var))

    if expr.func_sql_function:
        # The name might contain a "." if it's one of our
        # metaschema helpers.
        name = tuple(expr.func_sql_function.split('.', 1))
    else:
        name = common.get_function_backend_name(expr.func_shortname,
                                                expr.func_module_id)

    result: pgast.BaseExpr = pgast.FuncCall(name=name, args=args)

    if expr.force_return_cast:
        # The underlying function has a return value type
        # different from that of the EdgeQL function declaration,
        # so we need to make an explicit cast here.
        result = pgast.TypeCast(
            arg=result,
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(expr.typeref)
            )
        )

    return result


def _tuple_to_row_expr(
        tuple_set: irast.Set, *,
        ctx: context.CompilerContextLevel,
) -> Union[pgast.ImplicitRowExpr, pgast.RowExpr]:
    tuple_val = dispatch.compile(tuple_set, ctx=ctx)
    if not isinstance(tuple_val, (pgast.RowExpr, pgast.ImplicitRowExpr)):
        raise RuntimeError('tuple compilation unexpectedly did '
                           'not return a RowExpr or ImplicitRowExpr')
    return tuple_val


def _compile_set(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    relgen.get_set_rvar(ir_set, ctx=ctx)

    if (output.in_serialization_ctx(ctx) and ir_set.shape
            and not ctx.env.ignore_object_shapes):
        _compile_shape(ir_set, shape=ir_set.shape, ctx=ctx)


def _compile_shape(
        ir_set: irast.Set, shape: List[irast.Set], *,
        ctx: context.CompilerContextLevel) -> pgast.TupleVar:

    result = shapecomp.compile_shape(ir_set, shape, ctx=ctx)

    for element in result.elements:
        # The ref might have already been added by the nested shape
        # processing, so add it conditionally.
        pathctx.put_path_serialized_var_if_not_exists(
            ctx.rel, element.path_id, element.val, env=ctx.env)

    ser_elements = []
    for el in result.elements:
        ser_val = pathctx.get_path_serialized_or_value_var(
            ctx.rel, el.path_id, env=ctx.env)
        ser_elements.append(pgast.TupleElement(
            path_id=el.path_id,
            name=el.name,
            val=ser_val
        ))

    ser_result = pgast.TupleVar(elements=ser_elements, named=True)
    sval = output.serialize_expr(
        ser_result, path_id=ir_set.path_id, env=ctx.env)
    pathctx.put_path_serialized_var(
        ctx.rel, ir_set.path_id, sval, force=True, env=ctx.env)

    return result


def _compile_set_in_singleton_mode(
        node: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    if isinstance(node, irast.EmptySet):
        return pgast.NullConstant()
    elif node.expr is not None:
        return dispatch.compile(node.expr, ctx=ctx)
    else:
        if node.rptr:
            ptrref = node.rptr.ptrref
            source = node.rptr.source

            if ptrref.source_ptr is None and source.rptr is not None:
                raise RuntimeError(
                    'unexpectedly long path in simple expr')

            ptr_stor_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False)

            colref = pgast.ColumnRef(name=[ptr_stor_info.column_name])
        elif irtyputils.is_scalar(node.typeref):
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(str(node.typeref.id))
                ]
            )
        else:
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(str(node.typeref.id))
                ]
            )

        return colref
