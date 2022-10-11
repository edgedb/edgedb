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

from typing import *

from edb import errors

from edb.common import context as ctx_utils
from edb.edgeql import qltypes as ft

from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import constraints as s_constr
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.edgeql import ast as qlast

from . import astutils
from . import casts
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen
from . import stmt
from . import tuple_args
from . import typegen

from . import func  # NOQA


@dispatch.compile.register(qlast._Optional)
def compile__Optional(
        expr: qlast._Optional, *, ctx: context.ContextLevel) -> irast.Set:

    result = dispatch.compile(expr.expr, ctx=ctx)

    pathctx.register_set_in_scope(result, optional=True, ctx=ctx)

    return result


@dispatch.compile.register(qlast.Path)
def compile_Path(
        expr: qlast.Path, *, ctx: context.ContextLevel) -> irast.Set:
    return stmt.maybe_add_view(setgen.compile_path(expr, ctx=ctx), ctx=ctx)


def _balance(
    elements: Sequence[qlast.Expr],
    ctor: Callable[
        [qlast.Expr, qlast.Expr, Optional[ctx_utils.ParserContext]],
        qlast.Expr
    ],
    context: Optional[ctx_utils.ParserContext],
) -> qlast.Expr:
    mid = len(elements) // 2
    ls, rs = elements[:mid], elements[mid:]
    ls_context = rs_context = None
    if len(ls) > 1 and ls[0].context and ls[-1].context:
        ls_context = ctx_utils.merge_context([
            ls[0].context, ls[-1].context])
    if len(rs) > 1 and rs[0].context and rs[-1].context:
        rs_context = ctx_utils.merge_context([
            rs[0].context, rs[-1].context])

    return ctor(
        (
            _balance(ls, ctor, ls_context)
            if len(ls) > 1 else ls[0]
        ),
        (
            _balance(rs, ctor, rs_context)
            if len(rs) > 1 else rs[0]
        ),
        context,
    )


REBALANCED_OPS = {'UNION'}
REBALANCE_THRESHOLD = 10


@dispatch.compile.register(qlast.SetConstructorOp)
@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(
        expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:
    # Rebalance some associative operations to avoid deeply nested ASTs
    if expr.op in REBALANCED_OPS and not expr.rebalanced:
        elements = collect_binop(expr)
        # Don't bother rebalancing small groups
        if len(elements) >= REBALANCE_THRESHOLD:
            balanced = _balance(
                elements,
                lambda l, r, c: qlast.BinOp(
                    left=l, right=r, op=expr.op, rebalanced=True, context=c),
                expr.context
            )
            return dispatch.compile(balanced, ctx=ctx)

    op_node = func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.left, expr.right], ctx=ctx)

    if ctx.env.options.constant_folding:
        op_node.expr = cast(irast.OperatorCall, op_node.expr)
        folded = try_fold_binop(op_node.expr, ctx=ctx)
        if folded is not None:
            return folded

    return op_node


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
) -> irast.Set:
    with ctx.detached() as subctx:
        ir = dispatch.compile(expr.expr, ctx=subctx)
    # Wrap the result in another set, so that the inner namespace
    # doesn't leak out into any shapes (since computable computation
    # will pull namespaces from the source path_ids.)
    return setgen.ensure_set(setgen.ensure_stmt(ir, ctx=ctx), ctx=ctx)


@dispatch.compile.register(qlast.Set)
def compile_Set(
    expr: qlast.Set,
    *,
    ctx: context.ContextLevel
) -> irast.Set:
    # after flattening the set may still end up with 0 or 1 element,
    # which are treated as a special case
    elements = flatten_set(expr)

    if elements:
        if len(elements) == 1:
            # From the scope perspective, single-element set
            # literals are equivalent to a binary UNION with
            # an empty set, not to the element.
            return dispatch.compile(
                astutils.ensure_qlstmt(elements[0]), ctx=ctx)
        else:
            # Turn it into a tree of UNIONs so we only blow up the nesting
            # depth logarithmically.
            # TODO: Introduce an N-ary operation that handles the whole thing?
            bigunion = _balance(
                elements,
                lambda l, r, c: qlast.SetConstructorOp(
                    left=l, right=r, rebalanced=True, context=c),
                expr.context
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

    node_cls: Type[irast.BaseConstant]

    if isinstance(expr, qlast.StringConstant):
        std_type = sn.QualName('std', 'str')
        node_cls = irast.StringConstant
    elif isinstance(expr, qlast.IntegerConstant):
        value = value.replace("_", "")
        int_value = int(value)
        if expr.is_negative:
            int_value = -int_value
            value = f'-{value}'
        # If integer value is out of int64 bounds, use decimal
        std_type = sn.QualName('std', 'int64')
        node_cls = irast.IntegerConstant
    elif isinstance(expr, qlast.FloatConstant):
        value = value.replace("_", "")
        if expr.is_negative:
            value = f'-{value}'
        std_type = sn.QualName('std', 'float64')
        node_cls = irast.FloatConstant
    elif isinstance(expr, qlast.DecimalConstant):
        assert value[-1] == 'n'
        value = value[:-1].replace("_", "")
        if expr.is_negative:
            value = f'-{value}'
        std_type = sn.QualName('std', 'decimal')
        node_cls = irast.DecimalConstant
    elif isinstance(expr, qlast.BigintConstant):
        assert value[-1] == 'n'
        value = value[:-1].replace("_", "")
        if expr.is_negative:
            value = f'-{value}'
        std_type = sn.QualName('std', 'bigint')
        node_cls = irast.BigintConstant
    elif isinstance(expr, qlast.BooleanConstant):
        std_type = sn.QualName('std', 'bool')
        node_cls = irast.BooleanConstant
    elif isinstance(expr, qlast.BytesConstant):
        std_type = sn.QualName('std', 'bytes')
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
        ctx: context.ContextLevel) -> Optional[irast.Set]:
    try:
        const = ireval.evaluate(opcall, schema=ctx.env.schema)
    except ireval.UnsupportedExpressionError:
        anyreal = cast(s_scalars.ScalarType,
                       ctx.env.schema.get('std::anyreal'))

        if (str(opcall.func_shortname) in ('std::+', 'std::*') and
                opcall.operator_kind is ft.OperatorKind.Infix and
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
        ctx: context.ContextLevel) -> Optional[irast.Set]:

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
            other_binop.expr.operator_kind is ft.OperatorKind.Infix):

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
                        func_shortname=op,
                        func_polymorphic=opcall.func_polymorphic,
                        sql_function=opcall.sql_function,
                        func_sql_expr=opcall.func_sql_expr,
                        sql_operator=opcall.sql_operator,
                        force_return_cast=opcall.force_return_cast,
                        operator_kind=opcall.operator_kind,
                        params_typemods=opcall.params_typemods,
                        context=opcall.context,
                        typeref=opcall.typeref,
                        typemod=opcall.typemod,
                        tuple_path_ids=opcall.tuple_path_ids,
                        volatility=opcall.volatility,
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
                    func_shortname=op,
                    func_polymorphic=opcall.func_polymorphic,
                    sql_function=opcall.sql_function,
                    func_sql_expr=opcall.func_sql_expr,
                    sql_operator=opcall.sql_operator,
                    force_return_cast=opcall.force_return_cast,
                    operator_kind=opcall.operator_kind,
                    params_typemods=opcall.params_typemods,
                    context=opcall.context,
                    typeref=opcall.typeref,
                    typemod=opcall.typemod,
                    tuple_path_ids=opcall.tuple_path_ids,
                    volatility=opcall.volatility,
                )

                folded = setgen.ensure_set(folded_binop, ctx=ctx)

    return folded


@dispatch.compile.register(qlast.NamedTuple)
def compile_NamedTuple(
        expr: qlast.NamedTuple, *, ctx: context.ContextLevel) -> irast.Set:

    names = set()
    elements = []
    for el in expr.elements:
        name = el.name.name
        if name in names:
            raise errors.QueryError(
                f"named tuple has duplicate field '{name}'",
                context=el.context)
        names.add(name)

        element = irast.TupleElement(
            name=name,
            val=dispatch.compile(el.val, ctx=ctx),
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
            val=dispatch.compile(el, ctx=ctx),
        )
        elements.append(element)

    return setgen.new_tuple_set(elements, named=False, ctx=ctx)


@dispatch.compile.register(qlast.Array)
def compile_Array(
        expr: qlast.Array, *, ctx: context.ContextLevel) -> irast.Set:
    elements = [
        dispatch.compile(e, ctx=ctx)
        for e in expr.elements
    ]
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

    return func.compile_operator(
        expr, op_name='std::IF',
        qlargs=[expr.if_expr, expr.condition, expr.else_expr], ctx=ctx)


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


@dispatch.compile.register(qlast.GlobalExpr)
def compile_GlobalExpr(
        expr: qlast.GlobalExpr, *, ctx: context.ContextLevel) -> irast.Set:
    glob = ctx.env.get_track_schema_object(
        s_utils.ast_ref_to_name(expr.name), expr.name,
        modaliases=ctx.modaliases, type=s_globals.Global)
    assert isinstance(glob, s_globals.Global)

    if glob.is_computable(ctx.env.schema):
        obj_ref = s_utils.name_to_ast_ref(
            glob.get_target(ctx.env.schema).get_name(ctx.env.schema))
        # Wrap the reference in a subquery so that it does not get
        # factored out or go directly into the scope tree.
        qry = qlast.SelectQuery(result=qlast.Path(steps=[obj_ref]))
        return dispatch.compile(qry, ctx=ctx)

    objctx = ctx.env.options.schema_object_context
    if objctx in (s_constr.Constraint, s_indexes.Index):
        typname = objctx.get_schema_class_displayname()
        raise errors.SchemaDefinitionError(
            f'global variables cannot be referenced from {typname}',
            context=expr.context)

    default = glob.get_default(ctx.env.schema)

    param_set: qlast.Expr | irast.Set
    present_set: qlast.Expr | irast.Set | None
    if (
        ctx.env.options.func_params is None
        and not ctx.env.options.json_parameters
    ):
        param_set, present_set = setgen.get_global_param_sets(glob, ctx=ctx)
    else:
        param_set, present_set = setgen.get_func_global_param_sets(
            glob, ctx=ctx)

    if default and not present_set:
        # If we have a default value and the global is required,
        # then we can use the param being {} as a signal to use
        # the default.
        with ctx.new() as subctx:
            subctx.anchors = subctx.anchors.copy()
            main_param = subctx.maybe_create_anchor(param_set, 'glob')
            param_set = func.compile_operator(
                expr, op_name='std::??',
                qlargs=[main_param, default.qlast], ctx=subctx)
    elif default and present_set:
        # ... but if {} is a valid value for the global, we need to
        # stick in an extra parameter to indicate whether to use
        # the default.
        with ctx.new() as subctx:
            subctx.anchors = subctx.anchors.copy()
            main_param = subctx.maybe_create_anchor(param_set, 'glob')

            present_param = subctx.maybe_create_anchor(present_set, 'present')

            param_set = func.compile_operator(
                expr, op_name='std::IF',
                qlargs=[main_param, present_param, default.qlast], ctx=subctx)
    elif not isinstance(param_set, irast.Set):
        param_set = dispatch.compile(param_set, ctx=ctx)

    return param_set


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
        expr: qlast.TypeCast, *, ctx: context.ContextLevel) -> irast.Set:
    target_stype = typegen.ql_typeexpr_to_type(expr.type, ctx=ctx)
    target_typeref = typegen.type_to_typeref(target_stype, env=ctx.env)
    ir_expr: Union[irast.Set, irast.Expr]

    if (isinstance(expr.expr, qlast.Array) and not expr.expr.elements and
            irtyputils.is_array(target_typeref)):
        ir_expr = irast.Array(elements=[], typeref=target_typeref)

    elif isinstance(expr.expr, qlast.Parameter):
        pt = typegen.ql_typeexpr_to_type(expr.type, ctx=ctx)

        param_name = expr.expr.name
        if expr.cardinality_mod:
            if expr.cardinality_mod == qlast.CardinalityModifier.Optional:
                required = False
            elif expr.cardinality_mod == qlast.CardinalityModifier.Required:
                required = True
            else:
                raise NotImplementedError(
                    f"cardinality modifier {expr.cardinality_mod}")
        else:
            required = True

        if ctx.env.options.json_parameters:
            if param_name.isdecimal():
                raise errors.QueryError(
                    'queries compiled to accept JSON parameters do not '
                    'accept positional parameters',
                    context=expr.expr.context)

            typeref = typegen.type_to_typeref(
                ctx.env.get_track_schema_type(sn.QualName('std', 'json')),
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

        if ex_param := ctx.env.script_params.get(param_name):
            # N.B. Accessing the schema_type from the param is unreliable
            ctx.env.schema, param_first_type = irtyputils.ir_typeref_to_type(
                ctx.env.schema, ex_param.ir_type)
            if param_first_type != pt:
                raise errors.QueryError(
                    f'parameter type '
                    f'{pt.get_displayname(ctx.env.schema)} '
                    f'does not match original type '
                    f'{param_first_type.get_displayname(ctx.env.schema)}',
                    context=expr.expr.context)

        if param_name not in ctx.env.query_parameters:
            sub_params = None
            if ex_param and ex_param.sub_params:
                sub_params = tuple_args.finish_sub_params(
                    ex_param.sub_params, ctx=ctx)

            ctx.env.query_parameters[param_name] = irast.Param(
                name=param_name,
                required=required,
                schema_type=pt,
                ir_type=typeref,
                sub_params=sub_params,
            )

        return param

    else:
        with ctx.new() as subctx:
            if target_stype.contains_json(subctx.env.schema):
                # JSON wants type shapes and acts as an output sink.
                subctx.expr_exposed = context.Exposure.EXPOSED
                subctx.inhibit_implicit_limit = True
                subctx.implicit_id_in_shapes = False
                subctx.implicit_tid_in_shapes = False
                subctx.implicit_tname_in_shapes = False
            ir_expr = dispatch.compile(expr.expr, ctx=subctx)

    res = casts.compile_cast(
        ir_expr, target_stype, cardinality_mod=expr.cardinality_mod,
        ctx=ctx, srcctx=expr.expr.context)
    return stmt.maybe_add_view(res, ctx=ctx)


@dispatch.compile.register(qlast.Introspect)
def compile_Introspect(
        expr: qlast.Introspect, *, ctx: context.ContextLevel) -> irast.Set:

    typeref = typegen.ql_typeexpr_to_ir_typeref(expr.type, ctx=ctx)
    if typeref.material_type and not irtyputils.is_object(typeref):
        typeref = typeref.material_type
    if typeref.is_opaque_union:
        typeref = typegen.type_to_typeref(
            cast(
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

    ir = setgen.ensure_set(irast.TypeIntrospection(typeref=typeref), ctx=ctx)
    return stmt.maybe_add_view(ir, ctx=ctx)


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
    expr: qlast.Indirection, *, ctx: context.ContextLevel
) -> irast.Set:
    node: Union[irast.Set, irast.Expr] = dispatch.compile(expr.arg, ctx=ctx)
    for indirection_el in expr.indirection:
        if isinstance(indirection_el, qlast.Index):
            idx = dispatch.compile(indirection_el.index, ctx=ctx)
            idx.context = indirection_el.index.context
            node = irast.IndexIndirection(
                expr=node, index=idx, context=expr.context
            )

        elif isinstance(indirection_el, qlast.Slice):
            start: Optional[irast.Base]
            stop: Optional[irast.Base]

            if indirection_el.start:
                start = dispatch.compile(indirection_el.start, ctx=ctx)
            else:
                start = None

            if indirection_el.stop:
                stop = dispatch.compile(indirection_el.stop, ctx=ctx)
            else:
                stop = None

            node_set = setgen.ensure_set(node, ctx=ctx)
            node = irast.SliceIndirection(
                expr=node_set, start=start, stop=stop
            )
        else:
            raise ValueError(
                'unexpected indirection node: ' '{!r}'.format(indirection_el)
            )

    return setgen.ensure_set(node, ctx=ctx)


def compile_type_check_op(
        expr: qlast.IsOp, *, ctx: context.ContextLevel) -> irast.TypeCheckOp:
    # <Expr> IS <TypeExpr>
    left = dispatch.compile(expr.left, ctx=ctx)
    ltype = setgen.get_set_type(left, ctx=ctx)
    typeref = typegen.ql_typeexpr_to_ir_typeref(expr.right, ctx=ctx)

    if ltype.is_object_type():
        left = setgen.ptr_step_set(
            left, expr=None, source=ltype, ptr_name='__type__',
            source_context=expr.context, ctx=ctx)
        pathctx.register_set_in_scope(left, ctx=ctx)
        result = None
    else:
        if (ltype.is_collection()
                and cast(s_types.Collection, ltype).contains_object(
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


def flatten_set(expr: qlast.Set) -> List[qlast.Expr]:
    elements = []
    for el in expr.elements:
        if isinstance(el, qlast.Set):
            elements.extend(flatten_set(el))
        else:
            elements.append(el)

    return elements


def collect_binop(expr: qlast.BinOp) -> List[qlast.Expr]:
    elements = []

    stack = [expr.right, expr.left]
    while stack:
        el = stack.pop()
        if isinstance(el, qlast.BinOp) and el.op == expr.op:
            stack.extend([el.right, el.left])
        else:
            elements.append(el)

    return elements
