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

from edb.lang.common import ast
from edb.lang.common import parsing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import basetypes as s_basetypes
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types
from edb.lang.schema import utils as s_utils

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import setgen
from . import schemactx
from . import typegen

from . import func  # NOQA


@dispatch.compile.register(qlast.Path)
def compile_Path(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    return setgen.compile_path(expr, ctx=ctx)


@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    try_folding = True

    if isinstance(expr.op, ast.ops.TypeCheckOperator):
        op_node = compile_type_check_op(expr, ctx=ctx)
    elif isinstance(expr.op, qlast.SetOperator):
        op_node = compile_set_op(expr, ctx=ctx)
        try_folding = False
    elif isinstance(expr.op, qlast.EquivalenceOperator):
        op_node = compile_equivalence_op(expr, ctx=ctx)
    elif isinstance(expr.op, ast.ops.MembershipOperator):
        op_node = compile_membership_op(expr, ctx=ctx)
        try_folding = False
    else:
        left = dispatch.compile(expr.left, ctx=ctx)
        right = dispatch.compile(expr.right, ctx=ctx)
        op_node = irast.BinOp(left=left, right=right, op=expr.op)

    if try_folding:
        folded = try_fold_binop(op_node, ctx=ctx)
        if folded is not None:
            return folded

    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register(qlast.Parameter)
def compile_Parameter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    pt = ctx.arguments.get(expr.name)
    if pt is not None and not isinstance(pt, s_types.Type):
        pt = s_basetypes.normalize_type(pt, ctx.schema)

    return setgen.ensure_set(irast.Parameter(type=pt, name=expr.name), ctx=ctx)


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
            op = qlast.UNION

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
        return irutils.new_empty_set(ctx.schema, alias=ctx.aliases.get('e'))


@dispatch.compile.register(qlast.Constant)
def compile_Constant(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    ct = s_basetypes.normalize_type(expr.value.__class__, ctx.schema)
    return setgen.generated_set(
        irast.Constant(value=expr.value, type=ct), ctx=ctx)


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


@dispatch.compile.register(qlast.NamedTuple)
def compile_NamedTuple(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    return setgen.generated_set(
        irast.Tuple(elements=elements, named=True), ctx=ctx)


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

    return setgen.generated_set(irast.Tuple(elements=elements), ctx=ctx)


@dispatch.compile.register(qlast.Array)
def compile_Array(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    return setgen.generated_set(irast.Array(elements=elements), ctx=ctx)


@dispatch.compile.register(qlast.IfElse)
def compile_IfElse(
        expr: qlast.IfElse, *, ctx: context.ContextLevel) -> irast.Base:

    condition = setgen.ensure_set(
        dispatch.compile(expr.condition, ctx=ctx), ctx=ctx)

    ql_if_expr = expr.if_expr
    ql_else_expr = expr.else_expr

    with ctx.newscope(fenced=True) as scopectx:
        if_expr = dispatch.compile(ql_if_expr, ctx=scopectx)

    with ctx.newscope(fenced=True) as scopectx:
        else_expr = dispatch.compile(ql_else_expr, ctx=scopectx)

    if_expr_type = irutils.infer_type(if_expr, ctx.schema)
    else_expr_type = irutils.infer_type(else_expr, ctx.schema)

    result = s_utils.get_class_nearest_common_ancestor(
        [if_expr_type, else_expr_type])

    if result is None:
        raise errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.name, else_expr_type.name),
            context=expr.context)

    return setgen.generated_set(
        irast.IfElseExpr(
            if_expr=if_expr, else_expr=else_expr, condition=condition),
        ctx=ctx
    )


@dispatch.compile.register(qlast.UnaryOp)
def compile_UnaryOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Set:
    if expr.op == qlast.DISTINCT:
        return compile_distinct_op(expr, ctx=ctx)

    operand = dispatch.compile(expr.operand, ctx=ctx)
    if astutils.is_exists_expr_set(operand):
        operand.expr.negated = not operand.expr.negated
        return operand

    unop = irast.UnaryOp(expr=operand, op=expr.op)
    result_type = irutils.infer_type(unop, ctx.schema)

    real_t = ctx.schema.get('std::anyreal')

    if (isinstance(operand.expr, irast.Constant) and
            result_type.issubclass(real_t)):
        # Fold the operation to constant if possible
        if expr.op == ast.ops.UMINUS:
            return setgen.ensure_set(
                irast.Constant(value=-operand.expr.value, type=result_type),
                ctx=ctx)
        elif expr.op == ast.ops.UPLUS:
            return operand

    return setgen.generated_set(unop, ctx=ctx)


@dispatch.compile.register(qlast.ExistsPredicate)
def compile_ExistsPredicate(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new() as exctx:
        with exctx.newscope(fenced=True) as opctx:
            operand = setgen.scoped_set(
                dispatch.compile(expr.expr, ctx=opctx), ctx=opctx)

        return setgen.generated_set(
            irast.ExistPred(expr=operand), ctx=exctx)


@dispatch.compile.register(qlast.Coalesce)
def compile_Coalesce(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    if all(isinstance(a, qlast.Set) and not a.elements for a in expr.args):
        return irutils.new_empty_set(ctx.schema, alias=ctx.aliases.get('e'))

    with ctx.newscope() as newctx:
        leftmost_arg = larg = setgen.ensure_set(
            dispatch.compile(expr.args[0], ctx=newctx), ctx=newctx)

        for rarg_ql in expr.args[1:]:
            with newctx.new() as nestedscopectx:
                with nestedscopectx.newscope(fenced=True) as fencectx:
                    rarg = setgen.scoped_set(
                        dispatch.compile(rarg_ql, ctx=fencectx), ctx=fencectx)

                coalesce = irast.Coalesce(left=larg, right=rarg)
                larg = setgen.generated_set(coalesce, ctx=nestedscopectx)

        # Make sure any empty set types are properly resolved
        # before entering them into the scope tree.
        irutils.infer_type(larg, schema=ctx.schema)

        pathctx.register_set_in_scope(leftmost_arg, ctx=ctx)
        pathctx.mark_path_as_optional(leftmost_arg.path_id, ctx=ctx)

    return larg


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    maintype = expr.type.maintype

    if (isinstance(expr.expr, qlast.EmptyCollection) and
            maintype.name == 'array'):
        ir_expr = irast.Array()
    else:
        ir_expr = dispatch.compile(expr.expr, ctx=ctx)

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
        orig_type = irutils.infer_type(ir_expr, ctx.schema)
    except errors.EdgeQLError:
        # It is possible that the source expression is unresolved
        # if the expr is an empty set (or a coalesce of empty sets).
        orig_type = None

    if isinstance(orig_type, s_types.Tuple):
        # For tuple-to-tuple casts we generate a new tuple
        # to simplify things on sqlgen side.
        new_type = typegen.ql_typeref_to_type(ql_type, ctx=ctx)
        if not isinstance(new_type, s_types.Tuple):
            raise errors.EdgeQLError(
                f'cannot cast tuple to {new_type.name}',
                context=source_context)

        if len(orig_type.element_types) != len(new_type.element_types):
            raise errors.EdgeQLError(
                f'cannot cast to {new_type.name}: '
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
                ir_expr.path_id, n, orig_type.element_types[n])

            val_type = irutils.infer_type(val, ctx.schema)
            new_el_name = new_names[i]
            if val_type != new_type.element_types[new_el_name]:
                # Element cast
                val = _cast_expr(ql_type.subtypes[i], val, ctx=ctx,
                                 source_context=source_context)

            elements.append(irast.TupleElement(name=new_el_name, val=val))

        return irast.Tuple(named=new_type.named, elements=elements)

    elif isinstance(ir_expr, irast.EmptySet):
        # For the common case of casting an empty set, we simply
        # generate a new EmptySet node of the requested type.
        scls = typegen.ql_typeref_to_type(ql_type, ctx=ctx)
        return irutils.new_empty_set(ctx.schema, scls=scls,
                                     alias=ir_expr.path_id[-1].name.name)

    else:
        typ = typegen.ql_typeref_to_ir_typeref(ql_type, ctx=ctx)
        return setgen.ensure_set(
            irast.TypeCast(expr=ir_expr, type=typ), ctx=ctx)


@dispatch.compile.register(qlast.TypeFilter)
def compile_TypeFilter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    # Expr[IS Type] expressions.
    with ctx.new() as scopectx:
        arg = setgen.ensure_set(
            dispatch.compile(expr.expr, ctx=scopectx),
            ctx=scopectx)

    arg_type = irutils.infer_type(arg, ctx.schema)
    if not isinstance(arg_type, s_objtypes.ObjectType):
        raise errors.EdgeQLError(
            f'invalid type filter operand: {arg_type.name} '
            f'is not an object type',
            context=expr.expr.context)

    typ = schemactx.get_schema_type(expr.type.maintype, ctx=ctx)
    if not isinstance(typ, s_objtypes.ObjectType):
        raise errors.EdgeQLError(
            f'invalid type filter operand: {typ.name} is not an object type',
            context=expr.type.context)

    return setgen.class_indirection_set(arg, typ, optional=False, ctx=ctx)


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    node = dispatch.compile(expr.arg, ctx=ctx)
    int_type = schemactx.get_schema_type('std::int64', ctx=ctx)
    for indirection_el in expr.indirection:
        if isinstance(indirection_el, qlast.Index):
            idx = dispatch.compile(indirection_el.index, ctx=ctx)
            node = irast.IndexIndirection(expr=node, index=idx)

        elif isinstance(indirection_el, qlast.Slice):
            if indirection_el.start:
                start = dispatch.compile(indirection_el.start, ctx=ctx)
            else:
                start = irast.Constant(value=None, type=int_type)

            if indirection_el.stop:
                stop = dispatch.compile(indirection_el.stop, ctx=ctx)
            else:
                stop = irast.Constant(value=None, type=int_type)

            node = irast.SliceIndirection(
                expr=node, start=start, stop=stop)
        else:
            raise ValueError('unexpected indirection node: '
                             '{!r}'.format(indirection_el))

    return node


def try_fold_arithmetic_binop(
        op: ast.ops.Operator, left: irast.Set, right: irast.Set, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    """Try folding an arithmetic expr into a constant."""
    schema = ctx.schema

    real_t = schema.get('std::anyreal')
    float_t = schema.get('std::anyfloat')
    int_t = schema.get('std::anyint')

    left_type = irutils.infer_type(left, schema)
    right_type = irutils.infer_type(right, schema)

    if not left_type.issubclass(real_t) or not right_type.issubclass(real_t):
        return

    result_type = left_type
    if right_type.issubclass(float_t):
        result_type = right_type

    left = left.expr
    right = right.expr

    if op == ast.ops.ADD:
        value = left.value + right.value
    elif op == ast.ops.SUB:
        value = left.value - right.value
    elif op == ast.ops.MUL:
        value = left.value * right.value
    elif op == ast.ops.DIV:
        if left_type.issubclass(int_t) and right_type.issubclass(int_t):
            value = left.value // right.value
        else:
            value = left.value / right.value
    elif op == ast.ops.POW:
        value = left.value ** right.value
    elif op == ast.ops.MOD:
        value = left.value % right.value
    else:
        value = None

    if value is not None:
        return setgen.ensure_set(
            irast.Constant(value=value, type=result_type), ctx=ctx)


def try_fold_comparison_binop(
        op: ast.ops.Operator, left: irast.Set, right: irast.Set, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    """Try folding a comparison expr into a constant."""
    left = left.expr
    right = right.expr

    if op == ast.ops.EQ:
        value = left.value == right.value
    elif op == ast.ops.NE:
        value = left.value != right.value
    elif op == ast.ops.GT:
        value = left.value > right.value
    elif op == ast.ops.GE:
        value = left.value >= right.value
    elif op == ast.ops.LT:
        value = left.value < right.value
    elif op == ast.ops.LE:
        value = left.value <= right.value
    else:
        value = None

    if value is not None:
        return setgen.ensure_set(
            irast.Constant(value=value, type=ctx.schema.get('std::bool')),
            ctx=ctx)


def try_fold_binop(
        binop: irast.BinOp, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Set]:
    """Try folding a binary operator expression."""
    schema = ctx.schema
    real_t = schema.get('std::anyreal')

    result_type = irutils.infer_type(binop, schema)
    folded = None

    left = binop.left
    right = binop.right
    op = binop.op

    if (isinstance(left.expr, irast.Constant) and
            isinstance(right.expr, irast.Constant)):
        # Left and right nodes are constants.
        if isinstance(op, ast.ops.ComparisonOperator):
            folded = try_fold_comparison_binop(op, left, right, ctx=ctx)

        elif result_type.issubclass(real_t):
            folded = try_fold_arithmetic_binop(op, left, right, ctx=ctx)

    elif op in {ast.ops.ADD, ast.ops.MUL}:
        # Let's check if we have (CONST + (OTHER_CONST + X))
        # tree, which can be optimized to ((CONST + OTHER_CONST) + X)

        my_const = left
        other_binop = right
        if isinstance(right.expr, irast.Constant):
            my_const, other_binop = other_binop, my_const

        if (isinstance(my_const.expr, irast.Constant) and
                isinstance(other_binop.expr, irast.BinOp) and
                other_binop.expr.op == op):

            other_const = other_binop.expr.left
            other_binop_node = other_binop.expr.right
            if isinstance(other_binop_node.expr, irast.Constant):
                other_binop_node, other_const = \
                    other_const, other_binop_node

            if isinstance(other_const.expr, irast.Constant):
                new_const = try_fold_arithmetic_binop(
                    op, other_const, my_const, ctx=ctx)

                if new_const is not None:
                    folded_binop = irast.BinOp(
                        left=new_const,
                        right=other_binop_node,
                        op=op)
                    folded = setgen.ensure_set(folded_binop, ctx=ctx)

    return folded


def compile_type_check_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.TypeCheckOp:
    # <Expr> IS <Type>
    left = dispatch.compile(expr.left, ctx=ctx)
    with ctx.new() as subctx:
        subctx.path_as_type = True
        right = dispatch.compile(expr.right, ctx=subctx)

    ltype = irutils.infer_type(left, ctx.schema)
    left = setgen.ptr_step_set(
        left, source=ltype, ptr_name=('std', '__type__'),
        direction=s_pointers.PointerDirection.Outbound,
        source_context=expr.context, ctx=ctx)

    pathctx.register_set_in_scope(left, ctx=ctx)

    right = typegen.process_type_ref_expr(right)

    return irast.TypeCheckOp(left=left, right=right, op=expr.op)


def _compile_set_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:

    with ctx.newscope(fenced=True) as scopectx:
        left = setgen.scoped_set(
            dispatch.compile(expr.left, ctx=scopectx),
            ctx=scopectx)

    with ctx.newscope(fenced=True) as scopectx:
        right = setgen.scoped_set(
            dispatch.compile(expr.right, ctx=scopectx),
            ctx=scopectx)

    return setgen.ensure_set(
        irast.SetOp(left=left, right=right, op=expr.op), ctx=ctx)


def compile_set_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:
    # UNION
    return _compile_set_op(expr, ctx=ctx)


def compile_distinct_op(
        expr: qlast.UnaryOp, *, ctx: context.ContextLevel) -> irast.DistinctOp:
    # DISTINCT(SET OF any A) -> SET OF any
    with ctx.newscope(fenced=True) as scopectx:
        operand = setgen.scoped_set(
            dispatch.compile(expr.operand, ctx=scopectx), ctx=scopectx)
    return setgen.generated_set(irast.DistinctOp(expr=operand), ctx=ctx)


def compile_equivalence_op(
        expr: qlast.BinOp, *,
        ctx: context.ContextLevel) -> irast.EquivalenceOp:
    # A ?= B ≣ EQUIV(OPTIONAL any A, OPTIONAL any B) -> std::bool
    # Definition:
    #   | {a = b | ∀ (a, b) ∈ A ⨯ B}, iff A != ∅ ∧ B != ∅
    #   | {True}, iff A = B = ∅
    #   | {False}, iff A != ∅ ∧ B = ∅
    #   | {False}, iff A = ∅ ∧ B != ∅
    #
    # A ?!= B ≣ NEQUIV(OPTIONAL any A, OPTIONAL any B) -> std::bool
    # Definition:
    #   | {a != b | ∀ (a, b) ∈ A ⨯ B}, iff A != ∅ ∧ B != ∅
    #   | {False}, iff A = B = ∅
    #   | {True}, iff A != ∅ ∧ B = ∅
    #   | {True}, iff A = ∅ ∧ B != ∅
    left = setgen.ensure_set(dispatch.compile(expr.left, ctx=ctx), ctx=ctx)
    right = setgen.ensure_set(dispatch.compile(expr.right, ctx=ctx), ctx=ctx)
    result = irast.EquivalenceOp(left=left, right=right, op=expr.op)

    # Make sure any empty set types are properly resolved
    # before entering them into the scope tree.
    irutils.infer_type(result, schema=ctx.schema)

    pathctx.register_set_in_scope(left, ctx=ctx)
    pathctx.mark_path_as_optional(left.path_id, ctx=ctx)
    pathctx.register_set_in_scope(right, ctx=ctx)
    pathctx.mark_path_as_optional(right.path_id, ctx=ctx)

    return result


def compile_membership_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Base:
    left = dispatch.compile(expr.left, ctx=ctx)
    with ctx.newscope(fenced=True) as scopectx:
        # [NOT] IN is an aggregate, so we need to put a scope fence.
        right = setgen.scoped_set(
            dispatch.compile(expr.right, ctx=scopectx), ctx=scopectx)

    op_node = irast.BinOp(left=left, right=right, op=expr.op)
    return setgen.generated_set(op_node, ctx=ctx)


def flatten_set(expr: qlast.Set) -> typing.List[qlast.Expr]:
    elements = []
    for el in expr.elements:
        if isinstance(el, qlast.Set):
            elements.extend(flatten_set(el))
        else:
            elements.append(el)

    return elements
