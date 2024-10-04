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

from typing import Optional, Tuple, Union, Sequence

from edb import errors

from edb.edgeql import qltypes as ql_ft
from edb.edgeql import ast as qlast

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
from . import enums as pgce
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
        return dispatch.compile(ir_set.expr, ctx=ctx)

    is_toplevel = ctx.toplevel_stmt is context.NO_STMT

    _compile_set_impl(ir_set, ctx=ctx)

    if is_toplevel:
        if isinstance(ir_set.expr, irast.ConfigCommand):
            return config.top_output_as_config_op(
                ir_set, ctx.rel, env=ctx.env)
        else:
            pathctx.get_path_serialized_output(
                ctx.rel, ir_set.path_id, env=ctx.env)
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
        dispatch.compile(ir_set.expr, ctx=ctx)

    _compile_set_impl(ir_set, ctx=ctx)


def _compile_set_impl(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    is_toplevel = ctx.toplevel_stmt is context.NO_STMT

    if isinstance(ir_set.expr, (irast.BaseConstant, irast.Parameter)):
        # Avoid creating needlessly complicated constructs for
        # constant expressions.  Besides being an optimization,
        # this helps in GROUP BY queries.
        value = dispatch.compile(ir_set.expr, ctx=ctx)
        if is_toplevel:
            ctx.rel = ctx.toplevel_stmt = pgast.SelectStmt()
        pathctx.put_path_value_var_if_not_exists(
            ctx.rel, ir_set.path_id, value)
        if (output.in_serialization_ctx(ctx) and ir_set.shape
                and not ctx.env.ignore_object_shapes):
            _compile_shape(ir_set, ir_set.shape, ctx=ctx)

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

    result: pgast.BaseExpr
    is_decimal: bool = expr.name.isdecimal()

    params = [p for p in ctx.env.query_params if p.name == expr.name]
    param = params[0] if params else None

    if not is_decimal and ctx.env.named_param_prefix is not None:
        result = pgast.ColumnRef(
            name=ctx.env.named_param_prefix + (expr.name,),
            nullable=not expr.required,
        )
    elif param and param.sub_params:
        return relgen.process_encoded_param(param, ctx=ctx)
    else:
        index = ctx.argmap[expr.name].index
        result = pgast.ParamRef(number=index, nullable=not expr.required)

    if irtyputils.needs_custom_serialization(expr.typeref):
        if irtyputils.is_array(expr.typeref):
            subt = expr.typeref.subtypes[0]
            el_sql_type = subt.real_base_type.custom_sql_serialization
            # Arrays of text encoded types need to come in as the custom type
            result = pgast.TypeCast(
                arg=result,
                type_name=pgast.TypeName(name=(f'{el_sql_type}[]',)),
            )
        else:
            el_sql_type = expr.typeref.real_base_type.custom_sql_serialization
            assert el_sql_type is not None
            result = pgast.TypeCast(
                arg=result,
                type_name=pgast.TypeName(name=(el_sql_type,)),
            )

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
    expr: irast.BytesConstant, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:

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
        arg=pgast.BooleanConstant(val=expr.value.lower() == 'true'),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_ir_typeref(expr.typeref)
        )
    )


@dispatch.compile.register(irast.TypeCast)
def compile_TypeCast(
        expr: irast.TypeCast, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    pg_expr = dispatch.compile(expr.expr, ctx=ctx)

    detail: Optional[pgast.StringConstant] = None
    if expr.error_message_context is not None:
        detail = pgast.StringConstant(
            val=(
                '{"error_message_context": "'
                + expr.error_message_context
                + '"}'
            )
        )

    if expr.sql_cast:
        # Use explicit SQL cast.

        pg_type = pg_types.pg_type_from_ir_typeref(expr.to_type)
        res: pgast.BaseExpr = pgast.TypeCast(
            arg=pg_expr,
            type_name=pgast.TypeName(
                name=pg_type
            )
        )

    elif expr.sql_expr:
        # Cast implemented as a function.
        assert expr.cast_name

        func_name = common.get_cast_backend_name(
            expr.cast_name, aspect="function",
            versioned=ctx.env.versioned_stdlib,
        )

        args = [pg_expr]
        if detail is not None:
            args.append(detail)
        res = pgast.FuncCall(
            name=func_name,
            args=args,
        )

    elif expr.sql_function:
        res = pgast.FuncCall(
            name=tuple(expr.sql_function.split(".")),
            args=[pg_expr],
        )

    else:
        raise errors.UnsupportedFeatureError('cast not supported')

    if expr.cardinality_mod is qlast.CardinalityModifier.Required:
        args = [
            res,
            pgast.StringConstant(
                val='invalid_parameter_value',
            ),
            pgast.StringConstant(
                val='invalid null value in cast',
            ),
        ]
        if detail is not None:
            args.append(detail)
        res = pgast.FuncCall(
            name=astutils.edgedb_func('raise_on_null', ctx=ctx),
            args=args
        )

    return res


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
    span = pgast.StringConstant(
        val=irutils.get_span_as_json(
            expr.index, errors.InvalidValueError
        )
    )

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)
        index = dispatch.compile(expr.index, ctx=subctx)

    result = pgast.FuncCall(
        name=astutils.edgedb_func('_index', ctx=ctx),
        args=[subj, index, span]
    )

    return result


@dispatch.compile.register(irast.SliceIndirection)
def compile_SliceIndirection(
    expr: irast.SliceIndirection, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    # Handle Expr[Index], where Expr may be std::str, array<T> or
    # std::json. For strings we translate this into substr calls.
    # Arrays use the native slice syntax. JSON is handled by a
    # combination of unnesting aggregation and array slicing.
    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)

        if expr.start is None:
            start: pgast.BaseExpr = pgast.LiteralExpr(expr="0")
        else:
            start = dispatch.compile(expr.start, ctx=subctx)

        if expr.stop is None:
            stop: pgast.BaseExpr = pgast.LiteralExpr(expr=str(2**31 - 1))
        else:
            stop = dispatch.compile(expr.stop, ctx=subctx)

        typ = expr.expr.typeref
        inline_array_slicing = irtyputils.is_array(typ) and any(
            irtyputils.is_tuple(st) for st in typ.subtypes
        )

        if inline_array_slicing:
            return _inline_array_slicing(subj, start, stop, ctx=ctx)
        else:
            return pgast.FuncCall(
                name=astutils.edgedb_func('_slice', ctx=ctx),
                args=[subj, start, stop]
            )


def _inline_array_slicing(
    subj: pgast.BaseExpr, start: pgast.BaseExpr, stop: pgast.BaseExpr,
    ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    return pgast.Indirection(
        arg=subj,
        indirection=[
            pgast.Slice(
                lidx=pgast.FuncCall(
                    name=astutils.edgedb_func(
                        '_normalize_array_slice_index', ctx=ctx),
                    args=[
                        start,
                        pgast.FuncCall(
                            name=("cardinality",), args=[subj]
                        ),
                    ],
                ),
                ridx=astutils.new_binop(
                    lexpr=pgast.FuncCall(
                        name=astutils.edgedb_func(
                            '_normalize_array_slice_index', ctx=ctx),
                        args=[
                            stop,
                            pgast.FuncCall(
                                name=("cardinality",), args=[subj]
                            ),
                        ],
                    ),
                    op="-",
                    rexpr=pgast.LiteralExpr(expr="1"),
                ),
            )
        ],
    )


def _compile_call_args(
    expr: irast.Call, *,
    ctx: context.CompilerContextLevel
) -> tuple[list[pgast.BaseExpr], list[pgast.BaseExpr]]:
    args = []
    maybe_null = []
    if isinstance(expr, irast.FunctionCall) and expr.global_args:
        args += [dispatch.compile(arg, ctx=ctx) for arg in expr.global_args]
    for ir_arg in expr.args.values():
        ref = dispatch.compile(ir_arg.expr, ctx=ctx)
        args.append(ref)
        if (
            not expr.impl_is_strict
            and ir_arg.cardinality.can_be_zero()
            and ref.nullable
            and ir_arg.param_typemod == ql_ft.TypeModifier.SingletonType
        ):
            maybe_null.append(ref)
    return args, maybe_null


def _wrap_call(
    expr: pgast.BaseExpr, maybe_nulls: list[pgast.BaseExpr], *,
    ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    # If necessary, use CASE to filter out NULLs while calling a
    # non-strict function.
    if maybe_nulls:
        tests = [pgast.NullTest(arg=arg, negated=True) for arg in maybe_nulls]
        expr = pgast.CaseExpr(
            args=[pgast.CaseWhen(
                expr=astutils.extend_binop(None, *tests, op='AND'),
                result=expr,
            )]
        )
    return expr


@dispatch.compile.register(irast.OperatorCall)
def compile_OperatorCall(
        expr: irast.OperatorCall, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    if (str(expr.func_shortname) == 'std::IF'
            and expr.args[0].cardinality.is_single()
            and expr.args[2].cardinality.is_single()):
        if_expr, condition, else_expr = (a.expr for a in expr.args.values())
        return pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=dispatch.compile(condition, ctx=ctx),
                    result=dispatch.compile(if_expr, ctx=ctx))
            ],
            defresult=dispatch.compile(else_expr, ctx=ctx))
    elif (str(expr.func_shortname) == 'std::??'
            and expr.args[0].cardinality.is_single()
            and expr.args[1].cardinality.is_single()):
        l_expr, r_expr = (a.expr for a in expr.args.values())
        return pgast.CoalesceExpr(
            args=[
                dispatch.compile(l_expr, ctx=ctx),
                dispatch.compile(r_expr, ctx=ctx),
            ],
        )
    elif irutils.is_singleton_set_of_call(expr):
        pass
    elif irutils.returns_set_of(expr):
        raise errors.UnsupportedFeatureError(
            f"set returning operator '{expr.func_shortname}' is not supported "
            f"in singleton expressions")
    elif irutils.has_set_of_param(expr):
        raise errors.UnsupportedFeatureError(
            f"aggregate operator '{expr.func_shortname}' is not supported "
            f"in singleton expressions")

    args, maybe_null = _compile_call_args(expr, ctx=ctx)
    return _wrap_call(
        compile_operator(expr, args, ctx=ctx), maybe_null, ctx=ctx)


def compile_operator(
        expr: irast.OperatorCall,
        args: Sequence[pgast.BaseExpr], *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    lexpr = rexpr = None
    result: Optional[pgast.BaseExpr] = None

    if expr.operator_kind is ql_ft.OperatorKind.Infix:
        lexpr, rexpr = args
    elif expr.operator_kind is ql_ft.OperatorKind.Prefix:
        rexpr = args[0]
    elif expr.operator_kind is ql_ft.OperatorKind.Postfix:
        lexpr = args[0]
    else:
        raise RuntimeError(f'unexpected operator kind: {expr.operator_kind!r}')

    str_func_name = str(expr.func_shortname)
    if ((str_func_name in {'std::=', 'std::!='}
            or str(expr.origin_name) in {'std::=', 'std::!='})
            and expr.args[0].expr.typeref is not None
            and irtyputils.is_object(expr.args[0].expr.typeref)
            and expr.args[1].expr.typeref is not None
            and irtyputils.is_object(expr.args[1].expr.typeref)):
        if str_func_name == 'std::=' or str(expr.origin_name) == 'std::=':
            sql_oper = '='
        else:
            sql_oper = '!='

    elif str_func_name == 'std::EXISTS':
        assert rexpr
        result = pgast.NullTest(arg=rexpr, negated=True)

    elif expr.func_shortname in common.operator_map:
        sql_oper = common.operator_map[expr.func_shortname]

    elif expr.sql_operator:
        sql_oper = expr.sql_operator[0]
        if len(expr.sql_operator) > 1:
            # Explicit operand types given in FROM SQL OPERATOR
            lexpr, rexpr = _cast_operands(lexpr, rexpr, expr.sql_operator[1:])

    elif expr.origin_name is not None:
        sql_oper = common.get_operator_backend_name(
            expr.origin_name)[1]

    else:
        if expr.sql_function:
            sql_func, *cast_types = expr.sql_function

            func_name = common.maybe_versioned_name(
                tuple(sql_func.split('.', 1)),
                versioned=(
                    ctx.env.versioned_stdlib
                    and expr.func_shortname.get_root_module_name().name != 'ext'
                ),
            )

            if cast_types:
                # Explicit operand types given in FROM SQL FUNCTION
                lexpr, rexpr = _cast_operands(lexpr, rexpr, cast_types)
        else:
            func_name = common.get_operator_backend_name(
                expr.func_shortname, aspect='function',
                versioned=ctx.env.versioned_stdlib)

        args = []
        if lexpr is not None:
            args.append(lexpr)
        if rexpr is not None:
            args.append(rexpr)

        result = pgast.FuncCall(name=func_name, args=args)

    # If result was not already computed, it's going to be a generic Expr.
    if result is None:
        result = pgast.Expr(
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


def _cast_operands(
    lexpr: Optional[pgast.BaseExpr],
    rexpr: Optional[pgast.BaseExpr],
    sql_types: Sequence[str],
) -> Tuple[Optional[pgast.BaseExpr], Optional[pgast.BaseExpr]]:

    if lexpr is not None:
        lexpr = pgast.TypeCast(
            arg=lexpr,
            type_name=pgast.TypeName(
                name=(sql_types[0],)
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
                        name=(sql_types[1],)
                    )
                )
            )
        else:
            rexpr = pgast.TypeCast(
                arg=rexpr,
                type_name=pgast.TypeName(
                    name=(sql_types[1],)
                )
            )

    return lexpr, rexpr


def get_func_call_backend_name(
    expr: irast.FunctionCall, *,
    ctx: context.CompilerContextLevel
) -> Tuple[str, ...]:
    if expr.func_sql_function:
        # The name might contain a "." if it's one of our
        # metaschema helpers.
        func_name = common.maybe_versioned_name(
            tuple(expr.func_sql_function.split('.', 1)),
            versioned=(
                ctx.env.versioned_stdlib
                and expr.func_shortname.get_root_module_name().name != 'ext'
            ),
        )
    else:
        func_name = common.get_function_backend_name(
            expr.func_shortname, expr.backend_name,
            versioned=ctx.env.versioned_stdlib)
    return func_name


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
                val=(expr.result and not negated)
            )
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
                name=astutils.edgedb_func('issubclass', ctx=ctx),
                args=[left, right])

            if negated:
                result = astutils.new_unop('NOT', result)

    return result


@dispatch.compile.register(irast.ConstantSet)
def compile_ConstantSet(
        expr: irast.ConstantSet, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    raise errors.UnsupportedFeatureError(
        "Constant sets not allowed in singleton mode",
        hint="Are you passing a set into a variadic function?")


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
        assert e.path_id
        elements.append(pgast.TupleElement(path_id=e.path_id, val=val))

    result = pgast.TupleVar(elements=elements, typeref=ttype)

    return output.output_as_value(result, env=ctx.env)


@dispatch.compile.register(irast.TypeRef)
def compile_TypeRef(
        expr: irast.TypeRef, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    return astutils.compile_typeref(expr)


@dispatch.compile.register(irast.TypeIntrospection)
def compile_TypeIntrospection(
        expr: irast.TypeIntrospection, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    return astutils.compile_typeref(expr.output_typeref)


@dispatch.compile.register(irast.FunctionCall)
def compile_FunctionCall(
        expr: irast.FunctionCall, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    fname = str(expr.func_shortname)
    if sfunc := relgen._SIMPLE_SPECIAL_FUNCTIONS.get(fname):
        return sfunc(expr, ctx=ctx)

    if expr.func_sql_expr:
        raise errors.UnsupportedFeatureError(
            f'unimplemented function for singleton mode: {fname}'
        )

    if irutils.is_singleton_set_of_call(expr):
        pass
    elif irutils.returns_set_of(expr):
        raise errors.UnsupportedFeatureError(
            'set returning functions are not supported in simple expressions')
    elif irutils.has_set_of_param(expr):
        raise errors.UnsupportedFeatureError(
            f"aggregate function '{expr.func_shortname}' is not supported "
            f"in singleton expressions")

    args, maybe_null = _compile_call_args(expr, ctx=ctx)

    if expr.has_empty_variadic and expr.variadic_param_type is not None:
        var = pgast.TypeCast(
            arg=pgast.ArrayExpr(elements=[]),
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(expr.variadic_param_type)
            )
        )

        args.append(pgast.VariadicArgument(expr=var))

    name = get_func_call_backend_name(expr, ctx=ctx)

    result: pgast.BaseExpr = pgast.FuncCall(name=name, args=args)

    result = _wrap_call(result, maybe_null, ctx=ctx)

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
        _compile_shape(ir_set, ir_set.shape, ctx=ctx)
    elif ir_set.shape and ir_set in ctx.shapes_needed_by_dml:
        # If this shape is needed for DML purposes (because it is
        # populating link properties), compile it and populate its
        # elements as *values*, so that process_link_values can pick
        # them up and insert them.
        shape_tuple = shapecomp.compile_shape(ir_set, ir_set.shape, ctx=ctx)
        for element in shape_tuple.elements:
            pathctx.put_path_var_if_not_exists(
                ctx.rel,
                element.path_id,
                element.val,
                aspect=pgce.PathAspect.VALUE,
            )


def _compile_shape(
        ir_set: irast.Set,
        shape: Sequence[Tuple[irast.SetE[irast.Pointer], qlast.ShapeOp]],
        *,
        ctx: context.CompilerContextLevel) -> pgast.TupleVar:

    result = shapecomp.compile_shape(ir_set, shape, ctx=ctx)

    for element in result.elements:
        # We want to force, because the path id might already exist
        # serialized with a different shape, and we need ours to be
        # visible. (Anything needing the old one needs to have pulled
        # it already: see the "unfortunate hack" in
        # process_set_as_tuple.)
        pathctx.put_path_serialized_var(
            ctx.rel, element.path_id, element.val, force=True
        )

    # When we compile a shape during materialization, stash the
    # set away so we can consume it in unpack_rvar.
    if (
        ctx.materializing
        and ir_set.typeref.id not in ctx.env.materialized_views
    ):
        ctx.env.materialized_views[ir_set.typeref.id] = ir_set

    ser_elements = []
    for el in result.elements:
        ser_val = pathctx.get_path_serialized_or_value_var(
            ctx.rel, el.path_id, env=ctx.env)
        ser_elements.append(pgast.TupleElement(
            path_id=el.path_id,
            name=el.name,
            val=ser_val
        ))
        # Don't let the elements themselves leak out, since they may
        # be wrapped in arrays.
        pathctx.put_path_id_mask(ctx.rel, el.path_id)

    ser_result = pgast.TupleVar(elements=ser_elements, named=True)
    sval = output.serialize_expr(
        ser_result, path_id=ir_set.path_id, env=ctx.env
    )
    pathctx.put_path_serialized_var(ctx.rel, ir_set.path_id, sval, force=True)

    return result


@dispatch.compile.register
def compile_EmptySet(
    expr: irast.EmptySet, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    return pgast.NullConstant()


@dispatch.compile.register
def compile_TypeRoot(
    expr: irast.TypeRoot, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    name = [common.edgedb_name_to_pg_name(str(expr.typeref.id))]
    if irtyputils.is_object(expr.typeref):
        name.append('id')

    return pgast.ColumnRef(name=name)


@dispatch.compile.register
def compile_Pointer(
    rptr: irast.Pointer, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    assert ctx.singleton_mode

    if rptr.expr:
        return dispatch.compile(rptr.expr, ctx=ctx)

    ptrref = rptr.ptrref
    source = rptr.source

    if ptrref.source_ptr is None and isinstance(source.expr, irast.Pointer):
        raise errors.UnsupportedFeatureError(
            'unexpectedly long path in simple expr')

    # In most cases, we don't need to reference the rvar (since there
    # will be only one in scope), but sometimes we do (for example NEW
    # in trigger functions).
    rvar_name = []
    if src := ctx.env.external_rvars.get(
        (source.path_id, pgce.PathAspect.SOURCE)
    ):
        rvar_name = [src.alias.aliasname]

    # compile column name
    ptr_stor_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False)

    colref = pgast.ColumnRef(
        name=rvar_name + [ptr_stor_info.column_name],
        nullable=rptr.dir_cardinality.can_be_zero())

    return colref


@dispatch.compile.register
def compile_TupleIndirectionPointer(
    rptr: irast.TupleIndirectionPointer, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    tuple_val = dispatch.compile(rptr.source, ctx=ctx)
    set_expr = astutils.tuple_getattr(
        tuple_val,
        rptr.source.typeref,
        rptr.ptrref.shortname.name,
    )
    return set_expr


@dispatch.compile.register(irast.FTSDocument)
def compile_FTSDocument(
    expr: irast.FTSDocument, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    return pgast.FTSDocument(
        text=dispatch.compile(expr.text, ctx=ctx),
        language=dispatch.compile(expr.language, ctx=ctx),
        language_domain=expr.language_domain,
        weight=expr.weight,
    )
