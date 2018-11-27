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

import typing

from edb import errors

from edb.lang.edgeql import functypes as ql_ft

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import scalars as s_scalars

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import common
from edb.server.pgsql import types as pg_types

from . import astutils
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
        ctx: context.CompilerContextLevel) -> pgast.Base:

    if ctx.env.singleton_mode:
        return _compile_set_in_singleton_mode(ir_set, ctx=ctx)

    is_toplevel = ctx.toplevel_stmt is None

    _compile_set_impl(ir_set, ctx=ctx)

    if is_toplevel:
        return output.top_output_as_value(ctx.rel, env=ctx.env)
    else:
        value = pathctx.get_path_value_var(
            ctx.rel, ir_set.path_id, env=ctx.env)

        return output.output_as_value(value, env=ctx.env)


@dispatch.visit.register(irast.Set)
def visit_Set(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    if ctx.env.singleton_mode:
        return _compile_set_in_singleton_mode(ir_set, ctx=ctx)

    _compile_set_impl(ir_set, ctx=ctx)


def _compile_set_impl(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    is_toplevel = ctx.toplevel_stmt is None

    if isinstance(ir_set.expr, irast.BaseConstant):
        # Avoid creating needlessly complicated constructs for
        # constant expressions.  Besides being an optimization,
        # this helps in GROUP BY queries.
        value = dispatch.compile(ir_set.expr, ctx=ctx)
        pathctx.put_path_value_var(ctx.rel, ir_set.path_id, value, env=ctx.env)
        if output.in_serialization_ctx(ctx) and ir_set.shape:
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
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    if expr.name.isdecimal():
        index = int(expr.name) + 1
        result = pgast.ParamRef(number=index)
    else:
        if ctx.env.use_named_params:
            result = pgast.NamedParamRef(name=expr.name)
        else:
            if expr.name in ctx.argmap:
                index = ctx.argmap[expr.name]
            else:
                index = len(ctx.argmap) + 1
                ctx.argmap[expr.name] = index

            result = pgast.ParamRef(number=index)

    return pgast.TypeCast(
        arg=result,
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.RawStringConstant)
def compile_RawStringConstant(
        expr: irast.RawStringConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.TypeCast(
        arg=pgast.StringConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.StringConstant)
def compile_StringConstant(
        expr: irast.StringConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.TypeCast(
        arg=pgast.EscapedStringConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.BytesConstant)
def compile_BytesConstant(
        expr: irast.StringConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.ByteaConstant(val=expr.value)


@dispatch.compile.register(irast.IntegerConstant)
def compile_IntegerConstant(
        expr: irast.IntegerConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.TypeCast(
        arg=pgast.NumericConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.FloatConstant)
def compile_FloatConstant(
        expr: irast.FloatConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.TypeCast(
        arg=pgast.NumericConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.BooleanConstant)
def compile_BooleanConstant(
        expr: irast.BooleanConstant, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    return pgast.TypeCast(
        arg=pgast.BooleanConstant(val=expr.value),
        type_name=pgast.TypeName(
            name=pg_types.pg_type_from_object(
                ctx.env.schema, expr.stype)
        )
    )


@dispatch.compile.register(irast.TypeCast)
def compile_TypeCast(
        expr: irast.TypeCast, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    pg_expr = dispatch.compile(expr.expr, ctx=ctx)

    if expr.sql_cast:
        # Use explicit SQL cast.

        to_type = irutils.typeref_to_type(ctx.env.schema, expr.to_type)
        pg_type = pg_types.pg_type_from_object(ctx.env.schema, to_type)
        return pgast.TypeCast(
            arg=pg_expr,
            type_name=pgast.TypeName(
                name=pg_type
            )
        )

    elif expr.sql_function or expr.sql_expr:
        # Cast implemented as a function.

        if expr.sql_expr:
            func_name = common.get_backend_cast_name(
                expr.cast_name, aspect='function')
        else:
            func_name = (expr.sql_function,)

        return pgast.FuncCall(
            name=func_name,
            args=[pg_expr],
        )

    else:
        raise RuntimeError('cast not supported')


@dispatch.compile.register(irast.IndexIndirection)
def compile_IndexIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
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

    # If the index is some integer, cast it into int, because there's
    # no backend function that handles indexes larger than int.
    index_t = expr.index.stype
    int_t = ctx.env.schema.get('std::anyint')
    if index_t.issubclass(ctx.env.schema, int_t):
        index = pgast.TypeCast(
            arg=index,
            type_name=pgast.TypeName(
                name=('int',)
            )
        )

    result = pgast.FuncCall(
        name=('edgedb', '_index'),
        args=[subj, index, srcctx]
    )

    return result


@dispatch.compile.register(irast.SliceIndirection)
def compile_SliceIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
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

    # any integer indexes must be upcast into int to fit the helper
    # function signature
    start = pgast.TypeCast(
        arg=start,
        type_name=pgast.TypeName(
            name=('int',)
        )
    )
    stop = pgast.TypeCast(
        arg=stop,
        type_name=pgast.TypeName(
            name=('int',)
        )
    )

    result = pgast.FuncCall(
        name=('edgedb', '_slice'),
        args=[subj, start, stop]
    )

    return result


@dispatch.compile.register(irast.OperatorCall)
def compile_OperatorCall(
        expr: irast.OperatorCall, *,
        ctx: context.CompilerContextLevel) -> pgast.Expr:

    if expr.typemod is ql_ft.TypeModifier.SET_OF:
        raise RuntimeError(
            f'set returning operator {expr.func_shortname!r} is not supported '
            f'in simple expressions')

    args = [dispatch.compile(a, ctx=ctx) for a in expr.args]
    if expr.operator_kind is ql_ft.OperatorKind.INFIX:
        lexpr, rexpr = args
    elif expr.operator_kind is ql_ft.OperatorKind.PREFIX:
        rexpr = args[0]
        lexpr = None
    elif expr.operator_kind is ql_ft.OperatorKind.POSTFIX:
        lexpr = args[0]
        rexpr = None
    else:
        raise RuntimeError(f'unexpected operator kind: {expr.operator_kind!r}')

    if expr.sql_operator:
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
                rexpr = pgast.TypeCast(
                    arg=rexpr,
                    type_name=pgast.TypeName(
                        name=(expr.sql_operator[2],)
                    )
                )
    else:
        sql_oper = common.get_backend_operator_name(expr.func_shortname)[1]

    result = pgast.Expr(
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
                name=pg_types.pg_type_from_object(
                    ctx.env.schema, expr.stype)
            )
        )

    return result


@dispatch.compile.register(irast.TypeCheckOp)
def compile_TypeCheckOp(
        expr: irast.TypeCheckOp, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left = dispatch.compile(expr.left, ctx=newctx)
        right = dispatch.compile(expr.right, ctx=newctx)

    result = pgast.FuncCall(
        name=('edgedb', 'issubclass'),
        args=[left, right])

    if expr.op == 'IS NOT':
        result = astutils.new_unop('NOT', result)

    return result


@dispatch.compile.register(irast.IfElseExpr)
def compile_IfElseExpr(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        return pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=dispatch.compile(expr.condition, ctx=subctx),
                    result=dispatch.compile(expr.if_expr, ctx=subctx))
            ],
            defresult=dispatch.compile(expr.else_expr, ctx=subctx))


@dispatch.compile.register(irast.Array)
def compile_Array(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    array = astutils.safe_array_expr(elements)

    if irutils.is_empty_array_expr(expr):
        return pgast.TypeCast(
            arg=array,
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_object(
                    ctx.env.schema, expr.stype)
            )
        )
    else:
        return array


@dispatch.compile.register(irast.TupleIndirection)
def compile_TupleIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    for se in expr.expr.expr.elements:
        if se.name == expr.name:
            return dispatch.compile(se.val, ctx=ctx)

    raise ValueError(f'no tuple element with name {expr.name}')


@dispatch.compile.register(irast.Tuple)
def compile_Tuple(
        expr: irast.Tuple, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    ttype = expr.stype
    ttypes = ttype.element_types
    telems = list(ttypes)

    path_id = irast.PathId.from_type(ctx.env.schema, ttype)

    elements = []

    for i, e in enumerate(expr.elements):
        telem = telems[i]
        ttype = ttypes[telem]
        el_path_id = irutils.tuple_indirection_path_id(
            path_id, telem, ttype, schema=ctx.env.schema)
        val = dispatch.compile(e.val, ctx=ctx)
        elements.append(pgast.TupleElement(path_id=el_path_id, val=val))

    result = pgast.TupleVar(elements=elements)

    return output.output_as_value(result, env=ctx.env)


@dispatch.compile.register(irast.TypeRef)
def compile_TypeRef(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    if expr.subtypes:
        raise NotImplementedError()
    else:
        result = pgast.FuncCall(
            name=('edgedb', '_resolve_type_id'),
            args=[pgast.StringConstant(val=expr.maintype)],
        )

    return result


@dispatch.compile.register(irast.FunctionCall)
def compile_FunctionCall(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:

    if expr.typemod is ql_ft.TypeModifier.SET_OF:
        raise RuntimeError(
            'set returning functions are not supported in simple expressions')

    args = [dispatch.compile(a, ctx=ctx) for a in expr.args]

    if expr.has_empty_variadic:
        var = pgast.TypeCast(
            arg=pgast.ArrayExpr(elements=[]),
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_object(
                    ctx.env.schema, expr.variadic_param_type)
            )
        )

        args.append(pgast.VariadicArgument(expr=var))

    if expr.func_sql_function:
        name = (expr.func_sql_function,)
    else:
        name = common.schema_name_to_pg_name(expr.func_shortname)

    result = pgast.FuncCall(name=name, args=args)

    if expr.force_return_cast:
        # The underlying function has a return value type
        # different from that of the EdgeQL function declaration,
        # so we need to make an explicit cast here.
        result = pgast.TypeCast(
            arg=result,
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_object(
                    ctx.env.schema, expr.stype)
            )
        )

    return result


@dispatch.compile.register(irast.Coalesce)
def compile_Coalesce(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        pg_args = [dispatch.compile(a, ctx=subctx) for a in expr.args]
    return pgast.FuncCall(name=('coalesce',), args=pg_args)


def _tuple_to_row_expr(
        tuple_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.ImplicitRowExpr:
    tuple_val = dispatch.compile(tuple_set, ctx=ctx)
    if not isinstance(tuple_val, (pgast.RowExpr, pgast.ImplicitRowExpr)):
        raise RuntimeError('tuple compilation unexpectedly did '
                           'not return a RowExpr or ImplicitRowExpr')
    return tuple_val


def _compile_set(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    relgen.get_set_rvar(ir_set, ctx=ctx)

    if output.in_serialization_ctx(ctx) and ir_set.shape:
        _compile_shape(ir_set, shape=ir_set.shape, ctx=ctx)


def _compile_shape(
        ir_set: irast.Set, shape: typing.List[irast.Set], *,
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
        node: irast.Set, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    if isinstance(node, irast.EmptySet):
        return pgast.NullConstant()
    elif node.expr is not None:
        return dispatch.compile(node.expr, ctx=ctx)
    else:
        if node.rptr:
            ptrcls = node.rptr.ptrcls
            source = node.rptr.source

            if not ptrcls.is_link_property(ctx.env.schema):
                if source.rptr:
                    raise RuntimeError(
                        'unexpectedly long path in simple expr')

            ptr_stor_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.env.schema, resolve_type=False)

            colref = pgast.ColumnRef(name=[ptr_stor_info.column_name])
        elif isinstance(node.stype, s_scalars.ScalarType):
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(
                        node.stype.get_name(ctx.env.schema))
                ]
            )
        else:
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(
                        node.stype.get_name(ctx.env.schema))
                ]
            )

        return colref
