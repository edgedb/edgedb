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


"""EdgeQL non-statement expression compilation functions."""


from __future__ import annotations

import typing

from edb import errors

from edb.edgeql import qltypes as ft

from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import constraints as s_constr
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from edb.edgeql import ast as qlast

from . import casts
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen
from . import typegen

from . import func  # NOQA


@dispatch.compile.register(qlast._Optional)
def compile__Optional(
        expr: qlast._Optional, *, ctx: context.ContextLevel) -> irast.Set:

    result = setgen.ensure_set(
        dispatch.compile(expr.expr, ctx=ctx),
        ctx=ctx)

    pathctx.register_set_in_scope(result, ctx=ctx)
    pathctx.mark_path_as_optional(result.path_id, ctx=ctx)

    return result


@dispatch.compile.register(qlast.Path)
def compile_Path(
        expr: qlast.Path, *, ctx: context.ContextLevel) -> irast.Set:
    return setgen.compile_path(expr, ctx=ctx)


@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:

    op_node = func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.left, expr.right], ctx=ctx)

    if ctx.env.options.constant_folding:
        op_node.expr = typing.cast(irast.OperatorCall, op_node.expr)
        folded = try_fold_binop(op_node.expr, ctx=ctx)
        if folded is not None:
            return folded

    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.IsOp)
def compile_IsOp(
        expr: qlast.IsOp, *, ctx: context.ContextLevel) -> irast.Set:
    op_node = compile_type_check_op(expr, ctx=ctx)
    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.Parameter)
def compile_Parameter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.env.options.func_params is not None:
        if ctx.env.options.schema_object_context is s_constr.Constraint:
            raise errors.InvalidConstraintDefinitionError(
                f'dollar-prefixed "$parameters" cannot be used here',
                context=expr.context)
        else:
            raise errors.InvalidFunctionDefinitionError(
                f'dollar-prefixed "$parameters" cannot be used here',
                context=expr.context)

    raise errors.QueryError(
        f'missing a type cast before the parameter',
        context=expr.context)


@dispatch.compile.register(qlast.DetachedExpr)
def compile_DetachedExpr(
    expr: qlast.DetachedExpr,
    *,
    ctx: context.ContextLevel,
) -> typing.Union[irast.Set, irast.Expr]:
    with ctx.detached() as subctx:
        return dispatch.compile(expr.expr, ctx=subctx)


@dispatch.compile.register(qlast.Set)
def compile_Set(
    expr: qlast.Set,
    *,
    ctx: context.ContextLevel
) -> typing.Union[irast.Set, irast.Expr]:
    # after flattening the set may still end up with 0 or 1 element,
    # which are treated as a special case
    elements = flatten_set(expr)

    if elements:
        if len(elements) == 1:
            # From the scope perspective, single-element set
            # literals are equivalent to a binary UNION with
            # an empty set, not to the element.
            with ctx.newscope(fenced=True) as scopectx:
                ir_set = dispatch.compile(elements[0], ctx=scopectx)
                return setgen.scoped_set(ir_set, ctx=scopectx)
        else:
            # a set literal is just sugar for a UNION
            op = 'UNION'

            bigunion = qlast.BinOp(
                left=elements[0],
                right=elements[1],
                op=op
            )
            for el in elements[2:]:
                bigunion = qlast.BinOp(
                    left=bigunion,
                    right=el,
                    op=op
                )
            return dispatch.compile(bigunion, ctx=ctx)
    else:
        return setgen.new_empty_set(
            alias=ctx.aliases.get('e'),
            ctx=ctx,
            srcctx=expr.context,
        )


@dispatch.compile.register(qlast.BaseConstant)
def compile_BaseConstant(
        expr: qlast.BaseConstant, *, ctx: context.ContextLevel) -> irast.Set:
    value = expr.value

    node_cls: typing.Type[irast.BaseConstant]

    if isinstance(expr, qlast.StringConstant):
        std_type = 'std::str'
        node_cls = irast.StringConstant
    elif isinstance(expr, qlast.IntegerConstant):
        int_value = int(expr.value)
        if expr.is_negative:
            int_value = -int_value
            value = f'-{value}'
        # If integer value is out of int64 bounds, use decimal
        std_type = 'std::int64'
        node_cls = irast.IntegerConstant
    elif isinstance(expr, qlast.FloatConstant):
        if expr.is_negative:
            value = f'-{value}'
        std_type = 'std::float64'
        node_cls = irast.FloatConstant
    elif isinstance(expr, qlast.DecimalConstant):
        assert value[-1] == 'n'
        value = value[:-1]
        if expr.is_negative:
            value = f'-{value}'
        std_type = 'std::decimal'
        node_cls = irast.DecimalConstant
    elif isinstance(expr, qlast.BigintConstant):
        assert value[-1] == 'n'
        value = value[:-1]
        if expr.is_negative:
            value = f'-{value}'
        std_type = 'std::bigint'
        node_cls = irast.BigintConstant
    elif isinstance(expr, qlast.BooleanConstant):
        std_type = 'std::bool'
        node_cls = irast.BooleanConstant
    elif isinstance(expr, qlast.BytesConstant):
        std_type = 'std::bytes'
        node_cls = irast.BytesConstant
    else:
        raise RuntimeError(f'unexpected constant type: {type(expr)}')

    ct = typegen.type_to_typeref(
        ctx.env.get_track_schema_type(std_type),
        env=ctx.env,
    )
    return setgen.ensure_set(node_cls(value=value, typeref=ct), ctx=ctx)


def try_fold_binop(
        opcall: irast.OperatorCall, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    try:
        const = ireval.evaluate(opcall, schema=ctx.env.schema)
    except ireval.UnsupportedExpressionError:
        anyreal = typing.cast(s_scalars.ScalarType,
                              ctx.env.schema.get('std::anyreal'))

        if (opcall.func_shortname in ('std::+', 'std::*') and
                opcall.operator_kind is ft.OperatorKind.INFIX and
                all(setgen.get_set_type(a.expr, ctx=ctx).issubclass(
                    ctx.env.schema, anyreal)
                    for a in opcall.args)):
            return try_fold_associative_binop(opcall, ctx=ctx)
        else:
            return None
    else:
        return setgen.ensure_set(const, ctx=ctx)


def try_fold_associative_binop(
        opcall: irast.OperatorCall, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:

    # Let's check if we have (CONST + (OTHER_CONST + X))
    # tree, which can be optimized to ((CONST + OTHER_CONST) + X)

    op = opcall.func_shortname
    my_const = opcall.args[0].expr
    other_binop = opcall.args[1].expr
    folded = None

    if isinstance(other_binop.expr, irast.BaseConstant):
        my_const, other_binop = other_binop, my_const

    if (isinstance(my_const.expr, irast.BaseConstant) and
            isinstance(other_binop.expr, irast.OperatorCall) and
            other_binop.expr.func_shortname == op and
            other_binop.expr.operator_kind is ft.OperatorKind.INFIX):

        other_const = other_binop.expr.args[0].expr
        other_binop_node = other_binop.expr.args[1].expr

        if isinstance(other_binop_node.expr, irast.BaseConstant):
            other_binop_node, other_const = \
                other_const, other_binop_node

        if isinstance(other_const.expr, irast.BaseConstant):
            try:
                new_const = ireval.evaluate(
                    irast.OperatorCall(
                        args=[
                            irast.CallArg(
                                expr=other_const,
                            ),
                            irast.CallArg(
                                expr=my_const,
                            ),
                        ],
                        func_module_id=opcall.func_module_id,
                        func_shortname=op,
                        func_polymorphic=opcall.func_polymorphic,
                        func_sql_function=opcall.func_sql_function,
                        sql_operator=opcall.sql_operator,
                        force_return_cast=opcall.force_return_cast,
                        operator_kind=opcall.operator_kind,
                        params_typemods=opcall.params_typemods,
                        context=opcall.context,
                        typeref=opcall.typeref,
                        typemod=opcall.typemod,
                    ),
                    schema=ctx.env.schema,
                )
            except ireval.UnsupportedExpressionError:
                pass
            else:
                folded_binop = irast.OperatorCall(
                    args=[
                        irast.CallArg(
                            expr=setgen.ensure_set(new_const, ctx=ctx),
                        ),
                        irast.CallArg(
                            expr=other_binop_node,
                        ),
                    ],
                    func_module_id=opcall.func_module_id,
                    func_shortname=op,
                    func_polymorphic=opcall.func_polymorphic,
                    func_sql_function=opcall.func_sql_function,
                    sql_operator=opcall.sql_operator,
                    force_return_cast=opcall.force_return_cast,
                    operator_kind=opcall.operator_kind,
                    params_typemods=opcall.params_typemods,
                    context=opcall.context,
                    typeref=opcall.typeref,
                    typemod=opcall.typemod,
                )

                folded = setgen.ensure_set(folded_binop, ctx=ctx)

    return folded


@dispatch.compile.register(qlast.NamedTuple)
def compile_NamedTuple(
        expr: qlast.NamedTuple, *, ctx: context.ContextLevel) -> irast.Set:

    elements = []
    for el in expr.elements:
        element = irast.TupleElement(
            name=el.name.name,
            val=setgen.ensure_set(dispatch.compile(el.val, ctx=ctx), ctx=ctx)
        )
        elements.append(element)

    return setgen.new_tuple_set(elements, named=True, ctx=ctx)


@dispatch.compile.register(qlast.Tuple)
def compile_Tuple(
        expr: qlast.Tuple, *, ctx: context.ContextLevel) -> irast.Set:

    elements = []
    for i, el in enumerate(expr.elements):
        element = irast.TupleElement(
            name=str(i),
            val=setgen.ensure_set(dispatch.compile(el, ctx=ctx), ctx=ctx)
        )
        elements.append(element)

    return setgen.new_tuple_set(elements, named=False, ctx=ctx)


@dispatch.compile.register(qlast.Array)
def compile_Array(
        expr: qlast.Array, *, ctx: context.ContextLevel) -> irast.Set:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    # check that none of the elements are themselves arrays
    for el, expr_el in zip(elements, expr.elements):
        if isinstance(inference.infer_type(el, ctx.env), s_abc.Array):
            raise errors.QueryError(
                f'nested arrays are not supported',
                context=expr_el.context)

    return setgen.new_array_set(elements, ctx=ctx, srcctx=expr.context)


@dispatch.compile.register(qlast.IfElse)
def compile_IfElse(
        expr: qlast.IfElse, *, ctx: context.ContextLevel) -> irast.Set:

    op_node = func.compile_operator(
        expr, op_name='std::IF',
        qlargs=[expr.if_expr, expr.condition, expr.else_expr], ctx=ctx)

    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.UnaryOp)
def compile_UnaryOp(
        expr: qlast.UnaryOp, *, ctx: context.ContextLevel) -> irast.Set:

    result = func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.operand], ctx=ctx)

    try:
        result = setgen.ensure_set(
            ireval.evaluate(result, schema=ctx.env.schema),
            ctx=ctx,
        )
    except ireval.UnsupportedExpressionError:
        pass

    return result


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
        expr: qlast.TypeCast, *, ctx: context.ContextLevel) -> irast.Set:
    target_typeref = typegen.ql_typeexpr_to_ir_typeref(expr.type, ctx=ctx)
    ir_expr: irast.Base

    is_parameter = isinstance(expr.expr, qlast.Parameter)
    if not is_parameter and expr.modifier:
        raise errors.QueryError(
            'cardinality modifiers REQUIRED and OPTIONAL only allowed '
            'on paremeters',
            context=expr.context)

    if (isinstance(expr.expr, qlast.Array) and not expr.expr.elements and
            irtyputils.is_array(target_typeref)):
        ir_expr = irast.Array()

    elif isinstance(expr.expr, qlast.Parameter):
        pt = typegen.ql_typeexpr_to_type(expr.type, ctx=ctx)

        if (
            (pt.is_tuple(ctx.env.schema) or pt.is_anytuple(ctx.env.schema))
            and not ctx.env.options.func_params
        ):
            raise errors.QueryError(
                'cannot pass tuples as query parameters',
                context=expr.expr.context,
            )

        if (
            isinstance(pt, s_types.Collection)
            and pt.contains_array_of_tuples(ctx.env.schema)
            and not ctx.env.options.func_params
        ):
            raise errors.QueryError(
                'cannot pass collections with tuple elements'
                ' as query parameters',
                context=expr.expr.context,
            )

        param_name = expr.expr.name
        if expr.modifier:
            if expr.modifier == qlast.CardinalityModifier.Optional:
                required = False
            elif expr.modifier == qlast.CardinalityModifier.Required:
                required = True
            else:
                raise NotImplementedError(
                    f"cardinality modifier {expr.modifier}")
        else:
            required = True

        if ctx.env.options.json_parameters:
            if param_name.isdecimal():
                raise errors.QueryError(
                    'queries compiled to accept JSON parameters do not '
                    'accept positional parameters',
                    context=expr.expr.context)

            typeref = typegen.type_to_typeref(
                ctx.env.get_track_schema_type('std::json'),
                env=ctx.env,
            )

            param = casts.compile_cast(
                irast.Parameter(
                    typeref=typeref,
                    name=param_name,
                    required=required,
                    context=expr.expr.context,
                ),
                pt,
                srcctx=expr.expr.context,
                ctx=ctx,
            )

        else:
            typeref = typegen.type_to_typeref(pt, env=ctx.env)
            param = setgen.ensure_set(
                irast.Parameter(
                    typeref=typeref,
                    name=param_name,
                    required=required,
                    context=expr.expr.context,
                ),
                ctx=ctx,
            )

        if param_name not in ctx.env.query_parameters:
            if ctx.env.query_parameters:
                first_key: str = next(iter(ctx.env.query_parameters))
                if first_key.isdecimal():
                    if not param_name.isdecimal():
                        raise errors.QueryError(
                            f'cannot combine positional and named parameters '
                            f'in the same query',
                            context=expr.expr.context)
                else:
                    if param_name.isdecimal():
                        raise errors.QueryError(
                            f'expected a named argument',
                            context=expr.expr.context)
            ctx.env.query_parameters[param_name] = irast.Param(
                name=param_name,
                required=required,
                schema_type=pt,
                ir_type=typeref,
            )
        else:
            param_first_type = ctx.env.query_parameters[param_name].schema_type
            if not param_first_type.explicitly_castable_to(pt, ctx.env.schema):
                raise errors.QueryError(
                    f'cannot cast '
                    f'{param_first_type.get_displayname(ctx.env.schema)} to '
                    f'{pt.get_displayname(ctx.env.schema)}',
                    context=expr.expr.context)

        return param

    else:
        with ctx.new() as subctx:
            # We use "exposed" mode in case this is a type of a cast
            # that wants view shapes, e.g. a std::json cast.  We do
            # this wholesale to support tuple and array casts without
            # having to analyze the target type (which is cumbersome
            # in QL AST).
            subctx.expr_exposed = True
            ir_expr = dispatch.compile(expr.expr, ctx=subctx)

    new_stype = typegen.ql_typeexpr_to_type(expr.type, ctx=ctx)
    return casts.compile_cast(
        ir_expr, new_stype, ctx=ctx, srcctx=expr.expr.context)


@dispatch.compile.register(qlast.Introspect)
def compile_Introspect(
        expr: qlast.Introspect, *, ctx: context.ContextLevel) -> irast.Set:

    typeref = typegen.ql_typeexpr_to_ir_typeref(expr.type, ctx=ctx)
    if typeref.material_type and not irtyputils.is_object(typeref):
        typeref = typeref.material_type
    if typeref.is_opaque_union:
        typeref = typegen.type_to_typeref(
            typing.cast(
                s_objtypes.ObjectType,
                ctx.env.schema.get('std::BaseObject'),
            ),
            env=ctx.env,
        )

    if irtyputils.is_view(typeref):
        raise errors.QueryError(
            f'cannot introspect transient type variant',
            context=expr.type.context)
    if irtyputils.is_collection(typeref):
        raise errors.QueryError(
            f'cannot introspect collection types',
            context=expr.type.context)
    if irtyputils.is_generic(typeref):
        raise errors.QueryError(
            f'cannot introspect generic types',
            context=expr.type.context)

    return setgen.ensure_set(irast.TypeIntrospection(typeref=typeref), ctx=ctx)


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
        expr: qlast.Indirection, *, ctx: context.ContextLevel) -> irast.Set:
    node = dispatch.compile(expr.arg, ctx=ctx)
    for indirection_el in expr.indirection:
        if isinstance(indirection_el, qlast.Index):
            idx = dispatch.compile(indirection_el.index, ctx=ctx)
            idx.context = indirection_el.index.context
            node = irast.IndexIndirection(expr=node, index=idx,
                                          context=expr.context)

        elif isinstance(indirection_el, qlast.Slice):
            start: typing.Optional[irast.Base]
            stop: typing.Optional[irast.Base]

            if indirection_el.start:
                start = dispatch.compile(indirection_el.start, ctx=ctx)
            else:
                start = None

            if indirection_el.stop:
                stop = dispatch.compile(indirection_el.stop, ctx=ctx)
            else:
                stop = None

            node = irast.SliceIndirection(
                expr=node, start=start, stop=stop)
        else:
            raise ValueError('unexpected indirection node: '
                             '{!r}'.format(indirection_el))

    return setgen.ensure_set(node, ctx=ctx)


def compile_type_check_op(
        expr: qlast.IsOp, *, ctx: context.ContextLevel) -> irast.TypeCheckOp:
    # <Expr> IS <TypeExpr>
    left = setgen.ensure_set(dispatch.compile(expr.left, ctx=ctx), ctx=ctx)
    ltype = setgen.get_set_type(left, ctx=ctx)
    typeref = typegen.ql_typeexpr_to_ir_typeref(expr.right, ctx=ctx)

    if ltype.is_object_type():
        left = setgen.ptr_step_set(
            left, source=ltype, ptr_name='__type__',
            source_context=expr.context, ctx=ctx)
        pathctx.register_set_in_scope(left, ctx=ctx)
        result = None
    else:
        if (ltype.is_collection()
                and typing.cast(s_types.Collection, ltype).contains_object(
                    ctx.env.schema)):
            raise errors.QueryError(
                f'type checks on non-primitive collections are not supported'
            )

        ctx.env.schema, test_type = (
            irtyputils.ir_typeref_to_type(ctx.env.schema, typeref)
        )
        result = ltype.issubclass(ctx.env.schema, test_type)

    return irast.TypeCheckOp(
        left=left, right=typeref, op=expr.op, result=result)


def flatten_set(expr: qlast.Set) -> typing.List[qlast.Expr]:
    elements = []
    for el in expr.elements:
        if isinstance(el, qlast.Set):
            elements.extend(flatten_set(el))
        else:
            elements.append(el)

    return elements
