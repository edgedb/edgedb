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


import typing

from edb.lang.common import parsing

from edb.lang.edgeql import functypes as ft

from edb.lang.ir import ast as irast
from edb.lang.ir import staeval as ireval
from edb.lang.ir import utils as irutils

from edb.lang.schema import abc as s_abc
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import utils as s_utils

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors

from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen
from . import schemactx
from . import stmtctx
from . import typegen
from . import viewgen

from . import func  # NOQA


@dispatch.compile.register(qlast._Optional)
def compile__Optional(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    result = setgen.ensure_set(
        dispatch.compile(expr.expr, ctx=ctx),
        ctx=ctx)

    pathctx.register_set_in_scope(result, ctx=ctx)
    pathctx.mark_path_as_optional(result.path_id, ctx=ctx)

    return result


@dispatch.compile.register(qlast.Path)
def compile_Path(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    return setgen.compile_path(expr, ctx=ctx)


@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    if expr.op == 'UNION':
        op_node = compile_set_op(expr, ctx=ctx)
    else:
        op_node = func.compile_operator(
            expr, op_name=expr.op, qlargs=[expr.left, expr.right], ctx=ctx)

        folded = try_fold_binop(op_node.expr, ctx=ctx)
        if folded is not None:
            return folded

    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.IsOp)
def compile_IsOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    op_node = compile_type_check_op(expr, ctx=ctx)
    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.Parameter)
def compile_Parameter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    if ctx.func is not None:
        raise errors.EdgeQLError(
            f'"$parameters" cannot not be used in functions',
            context=expr.context)

    return setgen.ensure_set(
        irast.Parameter(stype=None, name=expr.name), ctx=ctx)


@dispatch.compile.register(qlast.DetachedExpr)
def compile_DetachedExpr(
        expr: qlast.DetachedExpr, *, ctx: context.ContextLevel):
    with ctx.detached() as subctx:
        return dispatch.compile(expr.expr, ctx=subctx)


@dispatch.compile.register(qlast.Set)
def compile_Set(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    if expr.elements:
        if len(expr.elements) == 1:
            # From the scope perspective, single-element set
            # literals are equivalent to a binary UNION with
            # an empty set, not to the element.
            with ctx.newscope(fenced=True) as scopectx:
                ir_set = dispatch.compile(expr.elements[0], ctx=scopectx)
                return setgen.scoped_set(ir_set, ctx=scopectx)
        else:
            elements = flatten_set(expr)
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
        return irutils.new_empty_set(ctx.env.schema,
                                     alias=ctx.aliases.get('e'))


@dispatch.compile.register(qlast.BaseConstant)
def compile_BaseConstant(
        expr: qlast.BaseConstant, *, ctx: context.ContextLevel) -> irast.Base:
    value = expr.value

    if isinstance(expr, qlast.StringConstant):
        std_type = 'std::str'
        node_cls = irast.StringConstant
    elif isinstance(expr, qlast.RawStringConstant):
        std_type = 'std::str'
        node_cls = irast.RawStringConstant
    elif isinstance(expr, qlast.IntegerConstant):
        int_value = int(expr.value)
        if expr.is_negative:
            int_value = -int_value
            value = f'-{value}'
        # If integer value is out of int64 bounds, use decimal
        if -2 ** 63 <= int_value < 2 ** 63:
            std_type = 'std::int64'
        else:
            std_type = 'std::decimal'
        node_cls = irast.IntegerConstant
    elif isinstance(expr, qlast.FloatConstant):
        if expr.is_negative:
            value = f'-{value}'
        std_type = 'std::float64'
        node_cls = irast.FloatConstant
    elif isinstance(expr, qlast.BooleanConstant):
        std_type = 'std::bool'
        node_cls = irast.BooleanConstant
    elif isinstance(expr, qlast.BytesConstant):
        std_type = 'std::bytes'
        node_cls = irast.BytesConstant
    else:
        raise RuntimeError(f'unexpected constant type: {type(expr)}')

    ct = ctx.env.schema.get(std_type)
    return setgen.generated_set(node_cls(value=value, stype=ct), ctx=ctx)


def try_fold_binop(
        opcall: irast.OperatorCall, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    try:
        const = ireval.evaluate(opcall, schema=ctx.env.schema)
    except ireval.UnsupportedExpressionError:
        anyreal = ctx.env.schema.get('std::anyreal')

        if (opcall.func_shortname in ('std::+', 'std::*') and
                opcall.operator_kind is ft.OperatorKind.INFIX and
                all(a.stype.issubclass(ctx.env.schema, anyreal)
                    for a in opcall.args)):
            return try_fold_associative_binop(opcall, ctx=ctx)
    else:
        return setgen.ensure_set(const, ctx=ctx)


def try_fold_associative_binop(
        opcall: irast.OperatorCall, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:

    # Let's check if we have (CONST + (OTHER_CONST + X))
    # tree, which can be optimized to ((CONST + OTHER_CONST) + X)

    op = opcall.func_shortname
    my_const = opcall.args[0]
    other_binop = opcall.args[1]
    folded = None

    if isinstance(other_binop.expr, irast.BaseConstant):
        my_const, other_binop = other_binop, my_const

    if (isinstance(my_const.expr, irast.BaseConstant) and
            isinstance(other_binop.expr, irast.OperatorCall) and
            other_binop.expr.func_shortname == op and
            other_binop.expr.operator_kind is ft.OperatorKind.INFIX):

        other_const = other_binop.expr.args[0]
        other_binop_node = other_binop.expr.args[1]

        if isinstance(other_binop_node.expr, irast.BaseConstant):
            other_binop_node, other_const = \
                other_const, other_binop_node

        if isinstance(other_const.expr, irast.BaseConstant):
            try:
                new_const = ireval.evaluate(
                    irast.OperatorCall(
                        args=[other_const, my_const],
                        func_shortname=op,
                        func_polymorphic=opcall.func_polymorphic,
                        func_sql_function=opcall.func_sql_function,
                        sql_operator=opcall.sql_operator,
                        operator_kind=opcall.operator_kind,
                        params_typemods=opcall.params_typemods,
                        context=opcall.context,
                        stype=opcall.stype,
                        typemod=opcall.typemod,
                    ),
                    schema=ctx.env.schema,
                )
            except ireval.UnsupportedExpressionError:
                pass
            else:
                folded_binop = irast.OperatorCall(
                    args=[
                        setgen.ensure_set(new_const, ctx=ctx),
                        other_binop_node
                    ],
                    func_shortname=op,
                    func_polymorphic=opcall.func_polymorphic,
                    func_sql_function=opcall.func_sql_function,
                    sql_operator=opcall.sql_operator,
                    operator_kind=opcall.operator_kind,
                    params_typemods=opcall.params_typemods,
                    context=opcall.context,
                    stype=opcall.stype,
                    typemod=opcall.typemod,
                )

                folded = setgen.ensure_set(folded_binop, ctx=ctx)

    return folded


@dispatch.compile.register(qlast.EmptyCollection)
def compile_EmptyCollection(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    raise errors.EdgeQLError(
        f'could not determine type of empty array',
        context=expr.context)


@dispatch.compile.register(qlast.TupleElement)
def compile_TupleElement(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    name = expr.name.name
    if expr.name.module:
        name = f'{expr.name.module}::{name}'

    val = setgen.ensure_set(dispatch.compile(expr.val, ctx=ctx), ctx=ctx)

    element = irast.TupleElement(
        name=name,
        val=val,
    )

    return element


def make_tuple(
        elements: typing.List[irast.TupleElement], *,
        named: bool,
        ctx: context.ContextLevel) -> irast.Tuple:

    tup = irast.Tuple(elements=elements, named=named)
    tup.stype = inference.infer_type(tup, env=ctx.env)
    return tup


@dispatch.compile.register(qlast.NamedTuple)
def compile_NamedTuple(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    tup = make_tuple(elements, named=True, ctx=ctx)
    return setgen.generated_set(tup, ctx=ctx)


@dispatch.compile.register(qlast.Tuple)
def compile_Tuple(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = []

    for i, el in enumerate(expr.elements):
        element = irast.TupleElement(
            name=str(i),
            val=dispatch.compile(el, ctx=ctx)
        )
        elements.append(element)

    tup = make_tuple(elements, named=False, ctx=ctx)
    return setgen.generated_set(tup, ctx=ctx)


@dispatch.compile.register(qlast.Array)
def compile_Array(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    # check that none of the elements are themselves arrays
    for el, expr_el in zip(elements, expr.elements):
        if isinstance(inference.infer_type(el, ctx.env), s_abc.Array):
            raise errors.EdgeQLError(
                f'nested arrays are not supported',
                context=expr_el.context)
    return setgen.generated_set(irast.Array(elements=elements), ctx=ctx)


@dispatch.compile.register(qlast.IfElse)
def compile_IfElse(
        expr: qlast.IfElse, *, ctx: context.ContextLevel) -> irast.Base:

    condition = setgen.ensure_set(
        dispatch.compile(expr.condition, ctx=ctx), ctx=ctx)

    ql_if_expr = expr.if_expr
    ql_else_expr = expr.else_expr

    with ctx.newscope(fenced=True) as scopectx:
        if_expr = setgen.scoped_set(
            dispatch.compile(ql_if_expr, ctx=scopectx),
            ctx=scopectx)

    with ctx.newscope(fenced=True) as scopectx:
        else_expr = setgen.scoped_set(
            dispatch.compile(ql_else_expr, ctx=scopectx),
            ctx=scopectx)

    if_expr_type = inference.infer_type(if_expr, ctx.env)
    else_expr_type = inference.infer_type(else_expr, ctx.env)

    result = s_utils.get_class_nearest_common_ancestor(
        ctx.env.schema, [if_expr_type, else_expr_type])

    if result is None:
        raise errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.get_name(ctx.env.schema),
                else_expr_type.get_name(ctx.env.schema)),
            context=expr.context)

    ifelse = irast.IfElseExpr(
        if_expr=if_expr,
        else_expr=else_expr,
        condition=condition)

    stmtctx.get_expr_cardinality_later(
        target=ifelse, field='if_expr_card', irexpr=if_expr, ctx=ctx)
    stmtctx.get_expr_cardinality_later(
        target=ifelse, field='else_expr_card', irexpr=else_expr, ctx=ctx)

    return setgen.generated_set(
        ifelse,
        ctx=ctx
    )


@dispatch.compile.register(qlast.UnaryOp)
def compile_UnaryOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:

    result = func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.operand], ctx=ctx)

    try:
        result = ireval.evaluate(result, schema=ctx.env.schema)
    except ireval.UnsupportedExpressionError:
        pass

    return setgen.generated_set(result, ctx=ctx)


@dispatch.compile.register(qlast.Coalesce)
def compile_Coalesce(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    if all(isinstance(a, qlast.Set) and not a.elements for a in expr.args):
        return irutils.new_empty_set(ctx.env.schema,
                                     alias=ctx.aliases.get('e'))

    # Due to the construction of relgen, the (unfenced) subscope
    # below is necessary to shield LHS paths from the outer query
    # to prevent path binding which may break OPTIONAL.
    with ctx.newscope() as newctx:
        leftmost_arg = larg = setgen.ensure_set(
            dispatch.compile(expr.args[0], ctx=newctx), ctx=newctx)

        for rarg_ql in expr.args[1:]:
            with newctx.new() as nestedscopectx:
                with nestedscopectx.newscope(fenced=True) as fencectx:
                    rarg = setgen.scoped_set(
                        dispatch.compile(rarg_ql, ctx=fencectx),
                        force_reassign=True, ctx=fencectx)

                coalesce = irast.Coalesce(left=larg, right=rarg)
                larg = setgen.generated_set(coalesce, ctx=nestedscopectx)

            stmtctx.get_expr_cardinality_later(
                target=coalesce, field='right_card', irexpr=rarg, ctx=ctx)

        # Make sure any empty set types are properly resolved
        # before entering them into the scope tree.
        inference.infer_type(larg, env=ctx.env)

        pathctx.register_set_in_scope(leftmost_arg, ctx=ctx)
        pathctx.mark_path_as_optional(leftmost_arg.path_id, ctx=ctx)
        pathctx.assign_set_scope(leftmost_arg, newctx.path_scope, ctx=ctx)

    return larg


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    target_typeref = typegen.ql_typeref_to_ir_typeref(expr.type, ctx=ctx)

    if (isinstance(expr.expr, qlast.EmptyCollection) and
            target_typeref.maintype == 'array'):
        ir_expr = irast.Array()
    else:
        with ctx.new() as subctx:
            # We use "exposed" mode in case this is a type of a cast
            # that wants view shapes, e.g. a std::json cast.  We do
            # this wholesale to support tuple and array casts without
            # having to analyze the target type (which is cumbersome
            # in QL AST).
            subctx.expr_exposed = True
            ir_expr = dispatch.compile(expr.expr, ctx=subctx)

    return setgen.ensure_set(
        _cast_expr(expr.type, ir_expr, ctx=ctx,
                   source_context=expr.expr.context),
        ctx=ctx
    )


def _cast_expr(
        ql_type: qlast.TypeName, ir_expr: irast.Base, *,
        source_context: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.Base:
    try:
        orig_type = inference.infer_type(ir_expr, ctx.env)
    except errors.EdgeQLError:
        # It is possible that the source expression is unresolved
        # if the expr is an empty set (or a coalesce of empty sets).
        orig_type = None

    new_type = typegen.ql_typeref_to_type(ql_type, ctx=ctx)
    new_typeref = typegen.ql_typeref_to_ir_typeref(ql_type, ctx=ctx)
    json_t = ctx.env.schema.get('std::json')

    if isinstance(orig_type, s_abc.Tuple):
        if new_type.issubclass(ctx.env.schema, json_t):
            # Casting to std::json involves casting each tuple
            # element and also keeping the cast around the whole tuple.
            # This is to trigger the downstream logic of casting
            # objects (in elements of the tuple).
            elements = []
            for i, n in enumerate(orig_type.element_types):
                val = setgen.generated_set(
                    irast.TupleIndirection(
                        expr=ir_expr,
                        name=n
                    ),
                    ctx=ctx
                )
                val.path_id = irutils.tuple_indirection_path_id(
                    ir_expr.path_id, n, orig_type.element_types[n],
                    schema=ctx.env.schema)

                val_type = inference.infer_type(val, ctx.env)
                # Element cast
                val = _cast_expr(ql_type, val, ctx=ctx,
                                 source_context=source_context)

                elements.append(irast.TupleElement(name=n, val=val))

            new_tuple = setgen.ensure_set(
                make_tuple(elements, named=orig_type.named, ctx=ctx),
                ctx=ctx
            )

            return setgen.ensure_set(
                irast.TypeCast(expr=new_tuple, type=new_typeref), ctx=ctx)

        else:
            # For tuple-to-tuple casts we generate a new tuple
            # to simplify things on sqlgen side.
            if not isinstance(new_type, s_abc.Tuple):
                raise errors.EdgeQLError(
                    f'cannot cast tuple to '
                    f'{new_type.get_name(ctx.env.schema)}',
                    context=source_context)

            if len(orig_type.element_types) != len(new_type.element_types):
                raise errors.EdgeQLError(
                    f'cannot cast to {new_type.get_name(ctx.env.schema)}: '
                    f'number of elements is not the same',
                    context=source_context)

            new_names = list(new_type.element_types)

            elements = []
            for i, n in enumerate(orig_type.element_types):
                val = setgen.generated_set(
                    irast.TupleIndirection(
                        expr=ir_expr,
                        name=n
                    ),
                    ctx=ctx
                )
                val.path_id = irutils.tuple_indirection_path_id(
                    ir_expr.path_id, n, orig_type.element_types[n],
                    schema=ctx.env.schema)

                val_type = inference.infer_type(val, ctx.env)
                new_el_name = new_names[i]
                if val_type != new_type.element_types[new_el_name]:
                    # Element cast
                    val = _cast_expr(ql_type.subtypes[i], val, ctx=ctx,
                                     source_context=source_context)

                elements.append(irast.TupleElement(name=new_el_name, val=val))

            return make_tuple(named=new_type.named, elements=elements, ctx=ctx)

    elif isinstance(ir_expr, irast.EmptySet):
        # For the common case of casting an empty set, we simply
        # generate a new EmptySet node of the requested type.
        return irutils.new_empty_set(ctx.env.schema, stype=new_type,
                                     alias=ir_expr.path_id.target_name.name)

    elif (isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.Array)):
        if new_type.issubclass(ctx.env.schema, json_t):
            el_type = ql_type
        elif not isinstance(new_type, s_abc.Array):
            raise errors.EdgeQLError(
                f'cannot cast array to {new_type.get_name(ctx.env.schema)}',
                context=source_context)
        else:
            el_type = ql_type.subtypes[0]

        casted_els = []
        for el in ir_expr.expr.elements:
            el = _cast_expr(el_type, el, ctx=ctx,
                            source_context=source_context)
            casted_els.append(el)

        ir_expr.expr = irast.Array(elements=casted_els)
        return setgen.ensure_set(
            irast.TypeCast(expr=ir_expr, type=new_typeref), ctx=ctx)

    else:
        if (new_type.issubclass(ctx.env.schema, json_t) and
                ir_expr.path_id.is_objtype_path()):
            # JSON casts of objects are special: we want the full shape
            # and not just an identity.
            viewgen.compile_view_shapes(ir_expr, ctx=ctx)

        return setgen.ensure_set(
            irast.TypeCast(expr=ir_expr, type=new_typeref), ctx=ctx)


@dispatch.compile.register(qlast.TypeFilter)
def compile_TypeFilter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    # Expr[IS Type] expressions.
    with ctx.new() as scopectx:
        arg = setgen.ensure_set(
            dispatch.compile(expr.expr, ctx=scopectx),
            ctx=scopectx)

    arg_type = inference.infer_type(arg, ctx.env)
    if not isinstance(arg_type, s_objtypes.ObjectType):
        raise errors.EdgeQLError(
            f'invalid type filter operand: '
            f'{arg_type.get_name(ctx.env.schema)} '
            f'is not an object type',
            context=expr.expr.context)

    typ = schemactx.get_schema_type(expr.type.maintype, ctx=ctx)
    if not isinstance(typ, s_objtypes.ObjectType):
        raise errors.EdgeQLError(
            f'invalid type filter operand: '
            f'{typ.get_name(ctx.env.schema)} is not an object type',
            context=expr.type.context)

    return setgen.class_indirection_set(arg, typ, optional=False, ctx=ctx)


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    node = dispatch.compile(expr.arg, ctx=ctx)
    for indirection_el in expr.indirection:
        if isinstance(indirection_el, qlast.Index):
            idx = dispatch.compile(indirection_el.index, ctx=ctx)
            idx.context = indirection_el.index.context
            node = irast.IndexIndirection(expr=node, index=idx,
                                          context=expr.context)

        elif isinstance(indirection_el, qlast.Slice):
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
    left = dispatch.compile(expr.left, ctx=ctx)
    ltype = inference.infer_type(left, ctx.env)
    left = setgen.ptr_step_set(
        left, source=ltype, ptr_name=('std', '__type__'),
        direction=s_pointers.PointerDirection.Outbound,
        source_context=expr.context, ctx=ctx)

    pathctx.register_set_in_scope(left, ctx=ctx)

    right = typegen.ql_typeref_to_ir_typeref(expr.right, ctx=ctx)
    return irast.TypeCheckOp(left=left, right=right, op=expr.op)


def compile_set_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:

    with ctx.newscope(fenced=True) as scopectx:
        left = setgen.scoped_set(
            dispatch.compile(expr.left, ctx=scopectx),
            ctx=scopectx)

    with ctx.newscope(fenced=True) as scopectx:
        right = setgen.scoped_set(
            dispatch.compile(expr.right, ctx=scopectx),
            ctx=scopectx)

    left_type = inference.infer_type(left, ctx.env)
    right_type = inference.infer_type(right, ctx.env)

    if left_type != right_type and isinstance(left_type, s_abc.Collection):
        common_type = left_type.find_common_implicitly_castable_type(
            right_type, ctx.env.schema)

        if common_type is None:
            raise errors.EdgeQLError(
                f'could not determine type of a set',
                context=expr.context)

        if left_type != common_type:
            left = setgen.ensure_set(
                _cast_expr(typegen.type_to_ql_typeref(common_type, ctx=ctx),
                           left, ctx=ctx, source_context=expr.context),
                ctx=ctx
            )

        if right_type != common_type:
            right = setgen.ensure_set(
                _cast_expr(typegen.type_to_ql_typeref(common_type, ctx=ctx),
                           right, ctx=ctx, source_context=expr.context),
                ctx=ctx
            )

    setop = irast.SetOp(left=left, right=right, op=expr.op)

    stmtctx.get_expr_cardinality_later(
        target=setop, field='left_card', irexpr=left, ctx=ctx)
    stmtctx.get_expr_cardinality_later(
        target=setop, field='right_card', irexpr=right, ctx=ctx)

    return setgen.ensure_set(setop, ctx=ctx)


def flatten_set(expr: qlast.Set) -> typing.List[qlast.Expr]:
    elements = []
    for el in expr.elements:
        if isinstance(el, qlast.Set):
            elements.extend(flatten_set(el))
        else:
            elements.append(el)

    return elements
