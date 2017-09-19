##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL non-statement expression compilation functions."""


import typing

from edgedb.lang.common import ast
from edgedb.lang.common import parsing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors

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
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    return setgen.compile_path(expr, ctx=ctx)


@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
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

    if not isinstance(op_node, irast.Set):
        return setgen.generated_set(op_node, ctx=ctx)
    else:
        return op_node


@dispatch.compile.register(qlast.Parameter)
def compile_Parameter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    pt = ctx.arguments.get(expr.name)
    if pt is not None and not isinstance(pt, s_obj.NodeClass):
        pt = s_types.normalize_type(pt, ctx.schema)

    return irast.Parameter(type=pt, name=expr.name)


@dispatch.compile.register(qlast.Set)
def compile_Set(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    if expr.elements:
        if len(expr.elements) == 1:
            return dispatch.compile(expr.elements[0], ctx=ctx)
        else:
            # FIXME: this is sugar for a UNION (need to change to UNION ALL)
            elements = flatten_set(expr)
            bigunion = qlast.BinOp(
                left=elements[0],
                right=elements[1],
                op=qlast.UNION
            )
            for el in elements[2:]:
                bigunion = qlast.BinOp(
                    left=bigunion,
                    right=el,
                    op=qlast.UNION
                )
            return dispatch.compile(bigunion, ctx=ctx)
    else:
        return irast.EmptySet()


@dispatch.compile.register(qlast.Constant)
def compile_Constant(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    ct = s_types.normalize_type(expr.value.__class__, ctx.schema)
    return irast.Constant(value=expr.value, type=ct)


@dispatch.compile.register(qlast.EmptyCollection)
def compile_EmptyCollection(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    raise errors.EdgeQLError(
        f'could not determine type of empty collection',
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


@dispatch.compile.register(qlast.Mapping)
def compile_Mapping(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    keys = [dispatch.compile(k, ctx=ctx) for k in expr.keys]
    values = [dispatch.compile(v, ctx=ctx) for v in expr.values]
    return irast.Mapping(keys=keys, values=values)


@dispatch.compile.register(qlast.Array)
def compile_Array(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    return irast.Array(elements=elements)


@dispatch.compile.register(qlast.IfElse)
def compile_IfElse(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    ifelse_op = compile_ifelse(expr.condition, expr.if_expr,
                               expr.else_expr, expr.context, ctx=ctx)
    return setgen.generated_set(ifelse_op, ctx=ctx)


@dispatch.compile.register(qlast.UnaryOp)
def compile_UnaryOp(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    operand = dispatch.compile(expr.operand, ctx=ctx)

    if astutils.is_exists_expr_set(operand):
        operand.expr.negated = not operand.expr.negated
        return operand

    unop = irast.UnaryOp(expr=operand, op=expr.op)
    result_type = irutils.infer_type(unop, ctx.schema)

    if (isinstance(operand, irast.Constant) and
            result_type.name in {'std::int', 'std::float'}):
        # Fold the operation to constant if possible
        if expr.op == ast.ops.UMINUS:
            return irast.Constant(value=-operand.value, type=result_type)
        elif expr.op == ast.ops.UPLUS:
            return operand

    return setgen.generated_set(unop, ctx=ctx)


@dispatch.compile.register(qlast.ExistsPredicate)
def compile_ExistsPredicate(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new_traced_scope() as aggctx:
        # EXISTS is a special aggregate, so we need to put a scope
        # fence for the same reasons we do for aggregates.
        operand = dispatch.compile(expr.expr, ctx=aggctx)
        if irutils.is_strictly_subquery_set(operand):
            operand = operand.expr
        ir_set = setgen.generated_set(
            irast.ExistPred(expr=operand), ctx=aggctx)

        ir_set.path_scope = frozenset(aggctx.traced_path_scope)

    return ir_set


@dispatch.compile.register(qlast.Coalesce)
def compile_Coalesce(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    if all(isinstance(a, qlast.Set) and not a.elements for a in expr.args):
        return irast.EmptySet()

    args = [dispatch.compile(a, ctx=ctx) for a in expr.args]
    return irast.Coalesce(args=args)


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    maintype = expr.type.maintype

    if (isinstance(expr.expr, qlast.EmptyCollection) and
            maintype.name in ('array', 'map')):
        if maintype.name == 'array':
            ir_expr = irast.Array()
        elif maintype.name == 'map':
            ir_expr = irast.Mapping()
    else:
        ir_expr = dispatch.compile(expr.expr, ctx=ctx)

    return _cast_expr(expr.type, ir_expr, ctx=ctx,
                      source_context=expr.expr.context)


def _cast_expr(
        ql_type: qlast.TypeName, ir_expr: irast.Base, *,
        source_context: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.Base:
    try:
        orig_type = irutils.infer_type(ir_expr, ctx.schema)
    except errors.EdgeQLError:
        # It is possible that the source expression is unresolved
        # if the expr is EMPTY (or a coalesce of EMPTY).
        orig_type = None

    if isinstance(orig_type, s_obj.Tuple):
        # For tuple-to-tuple casts we generate a new tuple
        # to simplify things on sqlgen side.
        new_type = typegen.ql_typeref_to_type(ql_type, ctx=ctx)
        if not isinstance(new_type, s_obj.Tuple):
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

    else:
        typ = typegen.ql_typeref_to_ir_typeref(ql_type, ctx=ctx)
        return irast.TypeCast(expr=ir_expr, type=typ)


@dispatch.compile.register(qlast.TypeFilter)
def compile_TypeFilter(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    # Expr[IS Type] expressions,
    arg = dispatch.compile(expr.expr, ctx=ctx)
    arg_type = irutils.infer_type(arg, ctx.schema)
    if not isinstance(arg_type, s_concepts.Concept):
        raise errors.EdgeQLError(
            f'invalid type filter operand: {arg_type.name} '
            f'is not a concept',
            context=expr.expr.context)

    typ = schemactx.get_schema_object(expr.type.maintype, ctx=ctx)
    if not isinstance(typ, s_concepts.Concept):
        raise errors.EdgeQLError(
            f'invalid type filter operand: {typ.name} is not a concept',
            context=expr.type.context)

    return setgen.generated_set(
        irast.TypeFilter(
            expr=arg,
            type=irast.TypeRef(
                maintype=typ.name
            )
        ),
        ctx=ctx
    )


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    node = dispatch.compile(expr.arg, ctx=ctx)
    int_type = schemactx.get_schema_object('std::int', ctx=ctx)
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
        op: ast.ops.Operator, left: irast.BinOp, right: irast.BinOp, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Constant]:
    """Try folding an arithmetic expr into a constant."""
    left_type = irutils.infer_type(left, ctx.schema)
    right_type = irutils.infer_type(right, ctx.schema)

    if (left_type.name not in {'std::int', 'std::float'} or
            right_type.name not in {'std::int', 'std::float'}):
        return

    result_type = left_type
    if right_type.name == 'std::float':
        result_type = right_type

    if op == ast.ops.ADD:
        value = left.value + right.value
    elif op == ast.ops.SUB:
        value = left.value - right.value
    elif op == ast.ops.MUL:
        value = left.value * right.value
    elif op == ast.ops.DIV:
        if left_type.name == right_type.name == 'std::int':
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
        return irast.Constant(value=value, type=result_type)


def try_fold_binop(
        binop: irast.BinOp, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.Base]:
    """Try folding a binary operator expression."""
    result_type = irutils.infer_type(binop, ctx.schema)
    folded = None

    left = binop.left
    if isinstance(left, irast.Set) and left.expr is not None:
        left = left.expr
    right = binop.right
    if isinstance(right, irast.Set) and right.expr is not None:
        right = right.expr
    op = binop.op

    if (isinstance(left, irast.Constant) and
            isinstance(right, irast.Constant) and
            result_type.name in {'std::int', 'std::float'}):

        # Left and right nodes are constants.
        folded = try_fold_arithmetic_binop(op, left, right, ctx=ctx)

    elif op in {ast.ops.ADD, ast.ops.MUL}:
        # Let's check if we have (CONST + (OTHER_CONST + X))
        # tree, which can be optimized to ((CONST + OTHER_CONST) + X)

        my_const = left
        other_binop = right
        if isinstance(right, irast.Constant):
            my_const, other_binop = other_binop, my_const

        if (isinstance(my_const, irast.Constant) and
                isinstance(other_binop, irast.BinOp) and
                other_binop.op == op):

            other_const = other_binop.left
            other_binop_node = other_binop.right
            if isinstance(other_binop_node, irast.Constant):
                other_binop_node, other_const = \
                    other_const, other_binop_node

            if isinstance(other_const, irast.Constant):
                new_const = try_fold_arithmetic_binop(
                    op, other_const, my_const, ctx=ctx)

                if new_const is not None:
                    folded = irast.BinOp(
                        left=new_const,
                        right=other_binop_node,
                        op=op)

    return folded


def compile_type_check_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.BinOp:
    # <Expr> IS <Type>
    left = dispatch.compile(expr.left, ctx=ctx)
    with ctx.new() as subctx:
        subctx.path_as_type = True
        right = dispatch.compile(expr.right, ctx=subctx)

    ltype = irutils.infer_type(left, ctx.schema)
    left, _ = setgen.path_step(
        left, ltype, ('std', '__class__'),
        s_pointers.PointerDirection.Outbound, None,
        expr.context, ctx=ctx)

    right = typegen.process_type_ref_expr(right)

    return irast.BinOp(left=left, right=right, op=expr.op)


def compile_set_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.SetOp:
    # UNION

    left_ql = astutils.ensure_qlstmt(expr.left)
    right_ql = astutils.ensure_qlstmt(expr.right)

    left = dispatch.compile(left_ql, ctx=ctx)
    right = dispatch.compile(right_ql, ctx=ctx)

    result = irast.SetOp(left=left.expr, right=right.expr, op=expr.op)
    rtype = irutils.infer_type(result, ctx.schema)
    path_id = pathctx.get_path_id(rtype, ctx=ctx)
    pathctx.register_path_scope(path_id, ctx=ctx)

    return result


def compile_equivalence_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Base:
    #
    # a ?= b is defined as:
    #   a = b IF EXISTS a AND EXISTS b ELSE EXISTS a = EXISTS b
    # a ?!= b is defined as:
    #   a != b IF EXISTS a AND EXISTS b ELSE EXISTS a != EXISTS b
    #
    op = ast.ops.EQ if expr.op == qlast.EQUIVALENT else ast.ops.NE

    ex_left = qlast.ExistsPredicate(expr=expr.left)
    ex_right = qlast.ExistsPredicate(expr=expr.right)

    condition = qlast.BinOp(
        left=ex_left,
        right=ex_right,
        op=ast.ops.AND
    )

    if_expr = qlast.BinOp(
        left=expr.left,
        right=expr.right,
        op=op
    )

    else_expr = qlast.BinOp(
        left=ex_left,
        right=ex_right,
        op=op
    )

    return compile_ifelse(
        condition, if_expr, else_expr, expr.context, ctx=ctx)


def compile_ifelse(
        condition: qlast.Base,
        if_expr: qlast.Base, else_expr: qlast.Base,
        src_context: parsing.ParserContext, *,
        ctx: context.ContextLevel) -> irast.Base:
    if_expr = astutils.ensure_qlstmt(if_expr)
    if_expr.where = astutils.extend_qlbinop(
        if_expr.where, condition)

    not_condition = qlast.UnaryOp(operand=condition, op=ast.ops.NOT)
    else_expr = astutils.ensure_qlstmt(else_expr)
    else_expr.where = astutils.extend_qlbinop(
        else_expr.where, not_condition)

    if_expr = dispatch.compile(if_expr, ctx=ctx)
    else_expr = dispatch.compile(else_expr, ctx=ctx)

    if_expr_type = irutils.infer_type(if_expr, ctx.schema)
    else_expr_type = irutils.infer_type(else_expr, ctx.schema)

    result = s_utils.get_class_nearest_common_ancestor(
        [if_expr_type, else_expr_type])

    if result is None:
        raise errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.name, else_expr_type.name),
            context=src_context)

    return irast.SetOp(left=if_expr.expr, right=else_expr.expr,
                       op=qlast.UNION, exclusive=True)


def compile_membership_op(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new_traced_scope() as scopectx:
        # [NOT] IN is a set function, so we need to put a scope
        # fence.
        left = dispatch.compile(expr.left, ctx=scopectx)
        right = dispatch.compile(expr.right, ctx=scopectx)
        op_node = irast.BinOp(left=left, right=right, op=expr.op)
        ir_set = setgen.ensure_set(op_node, ctx=scopectx)
        ir_set.path_scope = frozenset(scopectx.traced_path_scope)

    return ir_set


def flatten_set(expr: qlast.Set) -> typing.List[qlast.Expr]:
    elements = []
    for el in expr.elements:
        if isinstance(el, qlast.Set):
            elements.extend(flatten_set(el))
        else:
            elements.append(el)

    return elements
