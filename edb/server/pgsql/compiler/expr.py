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

from edb.lang.common import ast

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import objects as s_obj
from edb.lang.schema import types as s_types

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
from . import typecomp


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

    if isinstance(ir_set.expr, irast.Constant):
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
    if expr.name.isnumeric():
        index = int(expr.name) + 1
    else:
        if expr.name in ctx.argmap:
            index = ctx.argmap[expr.name]
        else:
            index = len(ctx.argmap) + 1
            ctx.argmap[expr.name] = index

    result = pgast.ParamRef(number=index)
    return typecomp.cast(
        result, source_type=expr.type, target_type=expr.type,
        force=True, env=ctx.env)


@dispatch.compile.register(irast.Constant)
def compile_Constant(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    result = pgast.Constant(val=expr.value)
    result = typecomp.cast(
        result, source_type=expr.type, target_type=expr.type,
        force=True, env=ctx.env)
    return result


@dispatch.compile.register(irast.TypeCast)
def compile_TypeCast(
        expr: irast.TypeCast, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:
    pg_expr = dispatch.compile(expr.expr, ctx=ctx)

    target_type = _infer_type(expr, ctx=ctx)

    if (isinstance(expr.expr, irast.EmptySet) or
            (isinstance(expr.expr, irast.Array) and
                not expr.expr.elements)):

        return typecomp.cast(
            pg_expr, source_type=target_type,
            target_type=target_type, force=True, env=ctx.env)

    else:
        source_type = _infer_type(expr.expr, ctx=ctx)
        return typecomp.cast(
            pg_expr, source_type=source_type, target_type=target_type,
            env=ctx.env)


@dispatch.compile.register(irast.IndexIndirection)
def compile_IndexIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    # Handle Expr[Index], where Expr may be std::str or array<T>.
    # For strings we translate this into substr calls, whereas
    # for arrays the native slice syntax is used.
    is_string = False
    arg_type = _infer_type(expr.expr, ctx=ctx)

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)
        index = dispatch.compile(expr.index, ctx=subctx)

    if isinstance(arg_type, s_scalars.ScalarType):
        b = arg_type.get_topmost_concrete_base()
        is_string = b.name == 'std::str'

    one = pgast.Constant(val=1)
    zero = pgast.Constant(val=0)

    when_cond = astutils.new_binop(
        lexpr=index, rexpr=zero, op=ast.ops.LT)

    index_plus_one = astutils.new_binop(
        lexpr=index, op=ast.ops.ADD, rexpr=one)

    if is_string:
        upper_bound = pgast.FuncCall(
            name=('char_length',), args=[subj])
    else:
        upper_bound = pgast.FuncCall(
            name=('array_upper',), args=[subj, one])

    neg_off = astutils.new_binop(
        lexpr=upper_bound, rexpr=index_plus_one, op=ast.ops.ADD)

    when_expr = pgast.CaseWhen(
        expr=when_cond, result=neg_off)

    index = pgast.CaseExpr(
        args=[when_expr], defresult=index_plus_one)

    if is_string:
        index = pgast.TypeCast(
            arg=index,
            type_name=pgast.TypeName(
                name=('int',)
            )
        )
        result = pgast.FuncCall(
            name=('substr',),
            args=[subj, index, one]
        )
    else:
        indirection = pgast.Indices(ridx=index)
        result = pgast.Indirection(
            arg=subj, indirection=[indirection])

    return result


@dispatch.compile.register(irast.SliceIndirection)
def compile_SliceIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    # Handle Expr[Start:End], where Expr may be std::str or array<T>.
    # For strings we translate this into substr calls, whereas
    # for arrays the native slice syntax is used.
    with ctx.new() as subctx:
        subctx.expr_exposed = False
        subj = dispatch.compile(expr.expr, ctx=subctx)
        start = dispatch.compile(expr.start, ctx=subctx)
        stop = dispatch.compile(expr.stop, ctx=subctx)

    one = pgast.Constant(val=1)
    zero = pgast.Constant(val=0)

    is_string = False
    arg_type = _infer_type(expr.expr, ctx=ctx)

    if isinstance(arg_type, s_scalars.ScalarType):
        b = arg_type.get_topmost_concrete_base()
        is_string = b.name == 'std::str'

    if is_string:
        upper_bound = pgast.FuncCall(
            name=('char_length',), args=[subj])
    else:
        upper_bound = pgast.FuncCall(
            name=('array_upper',), args=[subj, one])

    if astutils.is_null_const(start):
        lower = one
    else:
        lower = start

        when_cond = astutils.new_binop(
            lexpr=lower, rexpr=zero, op=ast.ops.LT)
        lower_plus_one = astutils.new_binop(
            lexpr=lower, rexpr=one, op=ast.ops.ADD)

        neg_off = astutils.new_binop(
            lexpr=upper_bound, rexpr=lower_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)
        lower = pgast.CaseExpr(
            args=[when_expr], defresult=lower_plus_one)

    if astutils.is_null_const(stop):
        upper = upper_bound
    else:
        upper = stop

        when_cond = astutils.new_binop(
            lexpr=upper, rexpr=zero, op=ast.ops.LT)

        neg_off = astutils.new_binop(
            lexpr=upper_bound, rexpr=upper, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)
        upper = pgast.CaseExpr(
            args=[when_expr], defresult=upper)

    if is_string:
        lower = pgast.TypeCast(
            arg=lower,
            type_name=pgast.TypeName(
                name=('int',)
            )
        )

        args = [subj, lower]

        if upper is not upper_bound:
            for_length = astutils.new_binop(
                lexpr=upper, op=ast.ops.SUB, rexpr=lower)
            for_length = astutils.new_binop(
                lexpr=for_length, op=ast.ops.ADD, rexpr=one)

            for_length = pgast.TypeCast(
                arg=for_length,
                type_name=pgast.TypeName(
                    name=('int',)
                )
            )
            args.append(for_length)

        result = pgast.FuncCall(name=('substr',), args=args)

    else:
        indirection = pgast.Indices(
            lidx=lower, ridx=upper)
        result = pgast.Indirection(
            arg=subj, indirection=[indirection])

    return result


@dispatch.compile.register(irast.BinOp)
def compile_BinOp(
        expr: irast.Base, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        op = expr.op
        is_bool_op = op in {ast.ops.AND, ast.ops.OR}
        left = dispatch.compile(expr.left, ctx=newctx)
        right = dispatch.compile(expr.right, ctx=newctx)

    if not isinstance(expr.left, irast.EmptySet):
        left_type = _infer_type(expr.left, ctx=ctx)
    else:
        left_type = None

    if not isinstance(expr.right, irast.EmptySet):
        right_type = _infer_type(expr.right, ctx=ctx)
    else:
        right_type = None

    if (not isinstance(expr.left, irast.EmptySet) and
            not isinstance(expr.right, irast.EmptySet)):
        left_pg_type = pg_types.pg_type_from_object(
            ctx.env.schema, left_type, True)

        right_pg_type = pg_types.pg_type_from_object(
            ctx.env.schema, right_type, True)

        if (left_pg_type in {('text',), ('varchar',)} and
                right_pg_type in {('text',), ('varchar',)} and
                op == ast.ops.ADD):
            op = '||'

    if isinstance(left_type, s_types.Tuple):
        left = _tuple_to_row_expr(expr.left, ctx=newctx)
        left_count = len(left.args)
    else:
        left_count = 0

    if isinstance(right_type, s_types.Tuple):
        right = _tuple_to_row_expr(expr.right, ctx=newctx)
        right_count = len(right.args)
    else:
        right_count = 0

    if left_count != right_count:
        # Postgres does not allow comparing rows with
        # unequal number of entries, but we want to allow
        # this.  Fortunately, we know that such comparison is
        # always False.
        result = pgast.Constant(val=False)
    else:
        if is_bool_op:
            # Transform logical operators to force
            # the correct behaviour with respect to NULLs.
            # See the OrFilterFunction comment for details.
            if ctx.clause == 'where':
                if expr.op == ast.ops.OR:
                    result = pgast.FuncCall(
                        name=('edgedb', '_or'),
                        args=[left, right]
                    )
                else:
                    # For the purposes of the WHERE clause,
                    # AND operator works correctly, as
                    # it will either return NULL or FALSE,
                    # which both will disqualify the row.
                    result = astutils.new_binop(left, right, op=op)
            else:
                # For expressions outside WHERE, we
                # always want the result to be NULL
                # if either operand is NULL.
                bitop = '&' if expr.op == ast.ops.AND else '|'
                bitcond = astutils.new_binop(
                    lexpr=pgast.TypeCast(
                        arg=left,
                        type_name=pgast.TypeName(
                            name=('int',)
                        )
                    ),
                    rexpr=pgast.TypeCast(
                        arg=right,
                        type_name=pgast.TypeName(
                            name=('int',)
                        )
                    ),
                    op=bitop
                )
                bitcond = pgast.TypeCast(
                    arg=bitcond,
                    type_name=pgast.TypeName(
                        name=('bool',)
                    )
                )
                result = bitcond
        else:
            result = astutils.new_binop(left, right, op=op)

    return result


@dispatch.compile.register(irast.UnaryOp)
def compile_UnaryOp(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    with ctx.new() as subctx:
        subctx.expr_exposed = False
        operand = dispatch.compile(expr.expr, ctx=subctx)
    return pgast.Expr(name=expr.op, rexpr=operand, kind=pgast.ExprKind.OP)


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

    if expr.op == ast.ops.IS_NOT:
        result = astutils.new_unop(ast.ops.NOT, result)

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
    return astutils.safe_array_expr(elements)


@dispatch.compile.register(irast.TupleIndirection)
def compile_TupleIndirection(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    for se in expr.expr.expr.elements:
        if se.name == expr.name:
            return dispatch.compile(se.val, ctx=ctx)

    raise ValueError(f'no tuple element with name {expr.name}')


@dispatch.compile.register(irast.Tuple)
def compile_Tuple(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    ttype = _infer_type(expr, ctx=ctx)
    ttypes = ttype.element_types
    telems = list(ttypes)

    path_id = irast.PathId(ttype)

    elements = []

    for i, e in enumerate(expr.elements):
        telem = telems[i]
        ttype = ttypes[telem]
        el_path_id = irutils.tuple_indirection_path_id(path_id, telem, ttype)
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
            args=[pgast.Constant(val=expr.maintype)],
        )

    return result


@dispatch.compile.register(irast.FunctionCall)
def compile_FunctionCall(
        expr: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    funcobj = expr.func

    if funcobj.aggregate:
        raise RuntimeError(
            'aggregate functions are not supported in simple expressions')

    if funcobj.set_returning:
        raise RuntimeError(
            'set returning functions are not supported in simple expressions')

    args = [dispatch.compile(a, ctx=ctx) for a in expr.args]

    if funcobj.from_function:
        name = (funcobj.from_function,)
    else:
        name = (
            common.edgedb_module_name_to_schema_name(
                funcobj.shortname.module),
            common.edgedb_name_to_pg_name(
                funcobj.shortname.name)
        )

    result = pgast.FuncCall(name=name, args=args)

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
        pathctx.put_path_var_if_not_exists(
            ctx.rel, element.path_id, element.val, aspect='serialized',
            env=ctx.env)

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
        return pgast.Constant(value=None)
    elif node.expr is not None:
        return dispatch.compile(node.expr, ctx=ctx)
    else:
        if node.rptr:
            ptrcls = node.rptr.ptrcls
            source = node.rptr.source

            if not ptrcls.is_link_property():
                if source.rptr:
                    raise RuntimeError(
                        'unexpectedly long path in simple expr')

            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(ptrcls.shortname)
                ]
            )
        elif isinstance(node.scls, s_scalars.ScalarType):
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(node.scls.name)
                ]
            )
        else:
            colref = pgast.ColumnRef(
                name=[
                    common.edgedb_name_to_pg_name(node.scls.name)
                ]
            )

        return colref


def _infer_type(
        expr: irast.Base, *,
        ctx: context.CompilerContextLevel) -> s_obj.Object:
    return irutils.infer_type(expr, schema=ctx.env.schema)
