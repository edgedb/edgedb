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

from typing import Callable, Optional, Type, Union, Sequence, List, cast

from edb import errors

from edb.common import parsing
from edb.common import span as edb_span

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import abc as s_abc
from edb.schema import constraints as s_constr
from edb.schema import functions as s_func
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pseudo as s_pseudo
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types
from edb.schema import utils as s_utils
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast

from . import astutils
from . import casts
from . import context
from . import dispatch
from . import pathctx
from . import setgen
from . import stmt
from . import tuple_args
from . import typegen

from . import func  # NOQA


@dispatch.compile.register(qlast.OptionalExpr)
def compile__Optional(
    expr: qlast.OptionalExpr, *, ctx: context.ContextLevel
) -> irast.Set:

    result = dispatch.compile(expr.expr, ctx=ctx)

    pathctx.register_set_in_scope(result, optional=True, ctx=ctx)

    return result


@dispatch.compile.register(qlast.Path)
def compile_Path(expr: qlast.Path, *, ctx: context.ContextLevel) -> irast.Set:
    if ctx.no_factoring and not expr.allow_factoring:
        return dispatch.compile(
            qlast.SelectQuery(
                result=expr.replace(allow_factoring=True),
                implicit=True,
            ),
            ctx=ctx,
        )

    if ctx.warn_factoring and not expr.allow_factoring:
        with ctx.newscope(fenced=False) as subctx:
            subctx.path_scope.warn = True
            return dispatch.compile(
                expr.replace(allow_factoring=True), ctx=subctx
            )

    return stmt.maybe_add_view(setgen.compile_path(expr, ctx=ctx), ctx=ctx)


def _balance(
    elements: Sequence[qlast.Expr],
    ctor: Callable[
        [qlast.Expr, qlast.Expr, Optional[qlast.Span]],
        qlast.Expr
    ],
    span: Optional[qlast.Span],
) -> qlast.Expr:
    mid = len(elements) // 2
    ls, rs = elements[:mid], elements[mid:]
    ls_span = rs_span = None
    if len(ls) > 1 and ls[0].span and ls[-1].span:
        ls_span = edb_span.merge_spans([
            ls[0].span, ls[-1].span
        ])
    if len(rs) > 1 and rs[0].span and rs[-1].span:
        rs_span = edb_span.merge_spans([
            rs[0].span, rs[-1].span])

    return ctor(
        (
            _balance(ls, ctor, ls_span)
            if len(ls) > 1 else ls[0]
        ),
        (
            _balance(rs, ctor, rs_span)
            if len(rs) > 1 else rs[0]
        ),
        span,
    )


REBALANCED_OPS = {'UNION'}
REBALANCE_THRESHOLD = 10


@dispatch.compile.register(qlast.BinOp)
def compile_BinOp(expr: qlast.BinOp, *, ctx: context.ContextLevel) -> irast.Set:
    # Rebalance some associative operations to avoid deeply nested ASTs
    if expr.op in REBALANCED_OPS and not expr.rebalanced:
        elements = collect_binop(expr)
        # Don't bother rebalancing small groups
        if len(elements) >= REBALANCE_THRESHOLD:
            balanced = _balance(
                elements,
                lambda l, r, s: qlast.BinOp(
                    left=l, right=r, op=expr.op, rebalanced=True, span=s
                ),
                expr.span
            )
            return dispatch.compile(balanced, ctx=ctx)

    if expr.op == '??' and astutils.contains_dml(expr.right, ctx=ctx):
        return _compile_dml_coalesce(expr, ctx=ctx)

    return func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.left, expr.right], ctx=ctx
    )


@dispatch.compile.register(qlast.IsOp)
def compile_IsOp(expr: qlast.IsOp, *, ctx: context.ContextLevel) -> irast.Set:
    op_node = compile_type_check_op(expr, ctx=ctx)
    return setgen.ensure_set(op_node, ctx=ctx)


@dispatch.compile.register
def compile_StrInterp(
    expr: qlast.StrInterp, *, ctx: context.ContextLevel
) -> irast.Set:
    strs: list[qlast.Expr] = []
    strs.append(qlast.Constant.string(expr.prefix))

    str_type = qlast.TypeName(
        maintype=qlast.ObjectRef(module='__std__', name='str')
    )
    for fragment in expr.interpolations:
        strs.append(qlast.TypeCast(
            expr=fragment.expr, type=str_type
        ))
        strs.append(qlast.Constant.string(fragment.suffix))

    call = qlast.FunctionCall(
        func=('__std__', 'array_join'),
        args=[qlast.Array(elements=strs), qlast.Constant.string('')],
    )

    return dispatch.compile(call, ctx=ctx)


@dispatch.compile.register(qlast.DetachedExpr)
def compile_DetachedExpr(
    expr: qlast.DetachedExpr,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    with ctx.detached() as subctx:
        if expr.preserve_path_prefix:
            subctx.partial_path_prefix = ctx.partial_path_prefix

        ir = dispatch.compile(expr.expr, ctx=subctx)
    # Wrap the result in another set, so that the inner namespace
    # doesn't leak out into any shapes (since computable computation
    # will pull namespaces from the source path_ids.)
    return setgen.ensure_set(setgen.ensure_stmt(ir, ctx=ctx), ctx=ctx)


@dispatch.compile.register(qlast.Set)
def compile_Set(expr: qlast.Set, *, ctx: context.ContextLevel) -> irast.Set:
    # after flattening the set may still end up with 0 or 1 element,
    # which are treated as a special case
    elements = flatten_set(expr)

    if elements:
        if len(elements) == 1:
            # From the scope perspective, single-element set
            # literals are equivalent to a binary UNION with
            # an empty set, not to the element.
            return dispatch.compile(
                astutils.ensure_ql_query(elements[0]), ctx=ctx
            )
        else:
            # Turn it into a tree of UNIONs so we only blow up the nesting
            # depth logarithmically.
            # TODO: Introduce an N-ary operation that handles the whole thing?
            bigunion = _balance(
                elements,
                lambda l, r, s: qlast.BinOp(
                    left=l, op='UNION', right=r,
                    rebalanced=True, set_constructor=True, span=s
                ),
                expr.span
            )
            res = dispatch.compile(bigunion, ctx=ctx)
            if cres := try_constant_set(res):
                res = setgen.ensure_set(cres, ctx=ctx)
            return res
    else:
        return setgen.new_empty_set(
            alias=ctx.aliases.get('e'),
            ctx=ctx,
            span=expr.span,
        )


@dispatch.compile.register(qlast.Constant)
def compile_Constant(
    expr: qlast.Constant, *, ctx: context.ContextLevel
) -> irast.Set:
    value = expr.value

    node_cls: Type[irast.BaseConstant]

    if expr.kind == qlast.ConstantKind.STRING:
        std_type = sn.QualName('std', 'str')
        node_cls = irast.StringConstant
    elif expr.kind == qlast.ConstantKind.INTEGER:
        value = value.replace("_", "")
        std_type = sn.QualName('std', 'int64')
        node_cls = irast.IntegerConstant
    elif expr.kind == qlast.ConstantKind.FLOAT:
        value = value.replace("_", "")
        std_type = sn.QualName('std', 'float64')
        node_cls = irast.FloatConstant
    elif expr.kind == qlast.ConstantKind.DECIMAL:
        assert value[-1] == 'n'
        value = value[:-1].replace("_", "")
        std_type = sn.QualName('std', 'decimal')
        node_cls = irast.DecimalConstant
    elif expr.kind == qlast.ConstantKind.BIGINT:
        assert value[-1] == 'n'
        value = value[:-1].replace("_", "")
        std_type = sn.QualName('std', 'bigint')
        node_cls = irast.BigintConstant
    elif expr.kind == qlast.ConstantKind.BOOLEAN:
        std_type = sn.QualName('std', 'bool')
        node_cls = irast.BooleanConstant
    else:
        raise RuntimeError(f'unexpected constant type: {expr.kind}')

    ct = typegen.type_to_typeref(
        ctx.env.get_schema_type_and_track(std_type),
        env=ctx.env,
    )
    return setgen.ensure_set(node_cls(value=value, typeref=ct), ctx=ctx)


@dispatch.compile.register(qlast.BytesConstant)
def compile_BytesConstant(
    expr: qlast.BytesConstant, *, ctx: context.ContextLevel
) -> irast.Set:
    std_type = sn.QualName('std', 'bytes')

    ct = typegen.type_to_typeref(
        ctx.env.get_schema_type_and_track(std_type),
        env=ctx.env,
    )
    return setgen.ensure_set(
        irast.BytesConstant(value=expr.value, typeref=ct), ctx=ctx
    )


@dispatch.compile.register(qlast.NamedTuple)
def compile_NamedTuple(
    expr: qlast.NamedTuple, *, ctx: context.ContextLevel
) -> irast.Set:

    names = set()
    elements = []
    for el in expr.elements:
        name = el.name.name
        if name in names:
            raise errors.QueryError(
                f"named tuple has duplicate field '{name}'",
                span=el.span)
        names.add(name)

        element = irast.TupleElement(
            name=name,
            val=dispatch.compile(el.val, ctx=ctx),
        )
        elements.append(element)

    return setgen.new_tuple_set(elements, named=True, ctx=ctx)


@dispatch.compile.register(qlast.Tuple)
def compile_Tuple(expr: qlast.Tuple, *, ctx: context.ContextLevel) -> irast.Set:

    elements = []
    for i, el in enumerate(expr.elements):
        element = irast.TupleElement(
            name=str(i),
            val=dispatch.compile(el, ctx=ctx),
        )
        elements.append(element)

    return setgen.new_tuple_set(elements, named=False, ctx=ctx)


@dispatch.compile.register(qlast.Array)
def compile_Array(expr: qlast.Array, *, ctx: context.ContextLevel) -> irast.Set:
    elements = [dispatch.compile(e, ctx=ctx) for e in expr.elements]
    # check that none of the elements are themselves arrays
    for el, expr_el in zip(elements, expr.elements):
        if isinstance(setgen.get_set_type(el, ctx=ctx), s_abc.Array):
            raise errors.QueryError(
                f'nested arrays are not supported',
                span=expr_el.span)

    return setgen.new_array_set(elements, ctx=ctx, span=expr.span)


def _move_fenced_anchor(ir: irast.Set, *, ctx: context.ContextLevel) -> None:
    """Move the scope tree of a fenced anchor to its use site.

    For technical reasons, in _compile_dml_coalesce and
    _compile_dml_ifelse, we compile the expressions normally and then
    extract the subcomponents and use them as anchors inside a
    desugared expression.

    Because of this, the resultant scope tree does not have the right
    shape, since the scope trees of the fenced subexpressions aren't
    nested inside the trees of the FOR loops. This results in subtle
    problems in backend compilation, since the loop iterators are not
    visible to the loop bodies.

    Fix the trees by finding the scope tree associated with a set used
    as an anchor, finding where that anchor was used, and moving the
    scope tree there.
    """
    match ir.expr:
        case irast.SelectStmt(result=irast.SetE(path_scope_id=int(id))):
            node = next(iter(
                x for x in ctx.path_scope.root.descendants
                if x.unique_id == id
            ))
            target = ctx.path_scope.find_descendant(ir.path_id)
            assert target and target.parent
            node.remove()
            target.parent.attach_child(node)


def _compile_dml_coalesce(
    expr: qlast.BinOp, *, ctx: context.ContextLevel
) -> irast.Set:
    """Transform a coalesce that contains DML into FOR loops

    The basic approach is to extract the pieces from the ?? and
    rewrite them into:
        for optional x in (LHS) union (
          {
            x,
            (for _ in (select () filter not exists x) union (RHS)),
          }
        )

    Optional for is needed because the LHS needs to be bound in a for
    in order to get put in a CTE and only executed once, but the RHS
    needs to be dependent on the LHS being empty.
    """
    with ctx.newscope(fenced=False) as subctx:
        # We have to compile it under a factoring fence to prevent
        # correlation with outside things. We can't just rely on the
        # factoring fences inserted when compiling the FORs, since we
        # are going to need to explicitly exempt the iterator
        # expression from that.
        subctx.path_scope.factoring_fence = True
        subctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)

        ir = func.compile_operator(
            expr, op_name=expr.op, qlargs=[expr.left, expr.right], ctx=subctx)

        # Extract the IR parts from the ??
        # Note that lhs_ir will be unfenced while rhs_ir
        # will have been compiled under fences.
        match ir.expr:
            case irast.OperatorCall(args={
                0: irast.CallArg(expr=lhs_ir),
                1: irast.CallArg(expr=rhs_ir),
            }):
                pass
            case _:
                raise AssertionError('malformed DML ??')

        subctx.anchors = subctx.anchors.copy()

        alias = ctx.aliases.get('_coalesce_x')
        cond_path = qlast.Path(
            steps=[qlast.ObjectRef(name=alias)],
        )

        rhs_b = qlast.ForQuery(
            iterator_alias=ctx.aliases.get('_coalesce_dummy'),
            iterator=qlast.SelectQuery(
                result=qlast.Tuple(elements=[]),
                where=qlast.UnaryOp(
                    op='NOT',
                    operand=qlast.UnaryOp(op='EXISTS', operand=cond_path),
                ),
            ),
            result=subctx.create_anchor(rhs_ir, check_dml=True),
        )

        full = qlast.ForQuery(
            iterator_alias=alias,
            iterator=subctx.create_anchor(lhs_ir, 'b'),
            result=qlast.Set(elements=[cond_path, rhs_b]),
            optional=True,
            from_desugaring=True,
        )

        subctx.iterator_path_ids |= {lhs_ir.path_id}
        res = dispatch.compile(full, ctx=subctx)
        # Indicate that the original ?? code should determine the
        # cardinality/multiplicity.
        assert isinstance(res.expr, irast.SelectStmt)
        res.expr.card_inference_override = ir

        _move_fenced_anchor(rhs_ir, ctx=subctx)

        return res


def _compile_dml_ifelse(
    expr: qlast.IfElse, *, ctx: context.ContextLevel
) -> irast.Set:
    """Transform an IF/ELSE that contains DML into FOR loops

    The basic approach is to extract the pieces from the if/then/else and
    rewrite them into:
        for b in COND union (
          {
            (for _ in (select () filter b) union (IF_BRANCH)),
            (for _ in (select () filter not b) union (ELSE_BRANCH)),
          }
        )
    """

    with ctx.newscope(fenced=False) as subctx:
        # We have to compile it under a factoring fence to prevent
        # correlation with outside things. We can't just rely on the
        # factoring fences inserted when compiling the FORs, since we
        # are going to need to explicitly exempt the iterator
        # expression from that.
        subctx.path_scope.factoring_fence = True
        subctx.path_scope.factoring_allowlist.update(ctx.iterator_path_ids)

        ir = func.compile_operator(
            expr, op_name='std::IF',
            qlargs=[expr.if_expr, expr.condition, expr.else_expr], ctx=subctx)

        # Extract the IR parts from the IF/THEN/ELSE
        # Note that cond_ir will be unfenced while if_ir and else_ir
        # will have been compiled under fences.
        match ir.expr:
            case irast.OperatorCall(args={
                0: irast.CallArg(expr=if_ir),
                1: irast.CallArg(expr=cond_ir),
                2: irast.CallArg(expr=else_ir),
            }):
                pass
            case _:
                raise AssertionError('malformed DML IF/ELSE')

        subctx.anchors = subctx.anchors.copy()

        alias = ctx.aliases.get('_ifelse_b')
        cond_path = qlast.Path(
            steps=[qlast.ObjectRef(name=alias)],
        )

        els: list[qlast.Expr] = []

        if not isinstance(irutils.unwrap_set(if_ir).expr, irast.EmptySet):
            if_b = qlast.ForQuery(
                iterator_alias=ctx.aliases.get('_ifelse_true_dummy'),
                iterator=qlast.SelectQuery(
                    result=qlast.Tuple(elements=[]),
                    where=cond_path,
                ),
                result=subctx.create_anchor(if_ir, check_dml=True),
            )
            els.append(if_b)

        if not isinstance(irutils.unwrap_set(else_ir).expr, irast.EmptySet):
            else_b = qlast.ForQuery(
                iterator_alias=ctx.aliases.get('_ifelse_false_dummy'),
                iterator=qlast.SelectQuery(
                    result=qlast.Tuple(elements=[]),
                    where=qlast.UnaryOp(op='NOT', operand=cond_path),
                ),
                result=subctx.create_anchor(else_ir, check_dml=True),
            )
            els.append(else_b)

        # If we are warning on factoring, double wrap it.
        if ctx.warn_factoring:
            cond_ir = setgen.ensure_set(
                setgen.ensure_stmt(cond_ir, ctx=ctx), ctx=ctx)
        full = qlast.ForQuery(
            iterator_alias=alias,
            iterator=subctx.create_anchor(cond_ir, 'b'),
            result=qlast.Set(elements=els) if len(els) != 1 else els[0],
        )

        subctx.iterator_path_ids |= {cond_ir.path_id}
        res = dispatch.compile(full, ctx=subctx)
        # Indicate that the original IF/ELSE code should determine the
        # cardinality/multiplicity.
        assert isinstance(res.expr, irast.SelectStmt)
        res.expr.card_inference_override = ir

        _move_fenced_anchor(if_ir, ctx=subctx)
        _move_fenced_anchor(else_ir, ctx=subctx)

        return res


@dispatch.compile.register(qlast.IfElse)
def compile_IfElse(
    expr: qlast.IfElse, *, ctx: context.ContextLevel
) -> irast.Set:

    if (
        astutils.contains_dml(expr.if_expr, ctx=ctx)
        or astutils.contains_dml(expr.else_expr, ctx=ctx)
    ):
        return _compile_dml_ifelse(expr, ctx=ctx)

    res = func.compile_operator(
        expr, op_name='std::IF',
        qlargs=[expr.if_expr, expr.condition, expr.else_expr], ctx=ctx)

    return res


@dispatch.compile.register(qlast.UnaryOp)
def compile_UnaryOp(
    expr: qlast.UnaryOp, *, ctx: context.ContextLevel
) -> irast.Set:

    return func.compile_operator(
        expr, op_name=expr.op, qlargs=[expr.operand], ctx=ctx)


@dispatch.compile.register(qlast.GlobalExpr)
def compile_GlobalExpr(
    expr: qlast.GlobalExpr, *, ctx: context.ContextLevel
) -> irast.Set:
    glob = ctx.env.get_schema_object_and_track(
        s_utils.ast_ref_to_name(expr.name), expr.name,
        modaliases=ctx.modaliases, type=s_globals.Global)
    assert isinstance(glob, s_globals.Global)

    if glob.is_computable(ctx.env.schema):
        obj_ref = s_utils.name_to_ast_ref(
            glob.get_target(ctx.env.schema).get_name(ctx.env.schema))
        # Wrap the reference in a subquery so that it does not get
        # factored out or go directly into the scope tree.
        qry = qlast.SelectQuery(result=qlast.Path(steps=[obj_ref]))
        target = dispatch.compile(qry, ctx=ctx)

        # If the global is single, use type_rewrites to make sure it
        # is computed only once in the SQL query.
        if glob.get_cardinality(ctx.env.schema).is_single():
            key = (setgen.get_set_type(target, ctx=ctx), False)
            if not ctx.env.type_rewrites.get(key):
                with ctx.detached() as dctx:
                    # The official rewrite needs to be in a detached
                    # scope to avoid collisions; this won't really
                    # recompile the whole thing, it will hit a cache
                    # of the view.
                    ctx.env.type_rewrites[key] = dispatch.compile(qry, ctx=dctx)
            rewrite_target = ctx.env.type_rewrites[key]

            # We need to have the set with expr=None, so that the rewrite
            # will be applied, but we also need to wrap it with a
            # card_inference_override so that we use the real cardinality
            # instead of assuming it is MANY.
            assert isinstance(rewrite_target, irast.Set)
            target = setgen.new_set_from_set(
                target,
                expr=irast.TypeRoot(
                    typeref=target.typeref, is_cached_global=True
                ),
                ctx=ctx,
            )
            wrap = irast.SelectStmt(
                result=target,
                card_inference_override=rewrite_target,
            )
            target = setgen.new_set_from_set(target, expr=wrap, ctx=ctx)

        return target

    default: Optional[s_expr.Expression] = glob.get_default(ctx.env.schema)

    # If we are compiling with globals suppressed but still allowed, always
    # treat it as being empty.
    if ctx.env.options.make_globals_empty:
        if default:
            return dispatch.compile(default.parse(), ctx=ctx)
        else:
            return setgen.new_empty_set(
                stype=glob.get_target(ctx.env.schema), ctx=ctx)

    objctx = ctx.env.options.schema_object_context
    if objctx in (s_constr.Constraint, s_indexes.Index):
        typname = objctx.get_schema_class_displayname()
        raise errors.SchemaDefinitionError(
            f'global variables cannot be referenced from {typname}',
            span=expr.span)

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
                qlargs=[main_param, default.parse()], ctx=subctx)
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
                qlargs=[main_param, present_param, default.parse()], ctx=subctx)
    elif not isinstance(param_set, irast.Set):
        param_set = dispatch.compile(param_set, ctx=ctx)

    return param_set


@dispatch.compile.register(qlast.TypeCast)
def compile_TypeCast(
    expr: qlast.TypeCast, *, ctx: context.ContextLevel
) -> irast.Set:
    try:
        target_stype = typegen.ql_typeexpr_to_type(expr.type, ctx=ctx)
    except errors.InvalidReferenceError as e:
        if (
            e.hint is None
            and isinstance(expr.type, qlast.TypeName)
            and isinstance(expr.type.maintype, qlast.ObjectRef)
        ):
            s_utils.enrich_schema_lookup_error(
                e,
                s_utils.ast_ref_to_name(expr.type.maintype),
                modaliases=ctx.modaliases,
                schema=ctx.env.schema,
                suggestion_limit=1,
                item_type=s_func.Function,
                hint_text='did you mean to call'
            )
        raise

    ir_expr: Union[irast.Set, irast.Expr]

    if isinstance(expr.expr, qlast.Parameter):
        if (
            # generic types not explicitly allowed
            not ctx.env.options.allow_generic_type_output and
            # not compiling a function which hadles its own generic types
            ctx.env.options.func_name is None and
            target_stype.is_polymorphic(ctx.env.schema)
        ):
            raise errors.QueryError(
                f'parameter cannot be a generic type '
                f'{target_stype.get_displayname(ctx.env.schema)!r}',
                hint="Please ensure you don't use generic "
                     '"any" types or abstract scalars.',
                span=expr.span)

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
                    span=expr.expr.span)

            typeref = typegen.type_to_typeref(
                ctx.env.get_schema_type_and_track(sn.QualName('std', 'json')),
                env=ctx.env,
            )

            param = casts.compile_cast(
                irast.Parameter(
                    typeref=typeref,
                    name=param_name,
                    required=required,
                    span=expr.expr.span,
                ),
                pt,
                span=expr.expr.span,
                ctx=ctx,
                cardinality_mod=expr.cardinality_mod,
            )

        else:
            typeref = typegen.type_to_typeref(pt, env=ctx.env)
            param = setgen.ensure_set(
                irast.Parameter(
                    typeref=typeref,
                    name=param_name,
                    required=required,
                    span=expr.expr.span,
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
                    span=expr.expr.span)

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

    with ctx.new() as subctx:
        if target_stype.contains_json(subctx.env.schema):
            # JSON wants type shapes and acts as an output sink.
            subctx.expr_exposed = context.Exposure.EXPOSED
            subctx.implicit_limit = 0
            subctx.implicit_id_in_shapes = False
            subctx.implicit_tid_in_shapes = False
            subctx.implicit_tname_in_shapes = False

        ir_expr = dispatch.compile(expr.expr, ctx=subctx)
        orig_stype = setgen.get_set_type(ir_expr, ctx=ctx)

        use_message_context = False
        if target_stype.is_collection() and subctx.collection_cast_info is None:
            subctx.collection_cast_info = context.CollectionCastInfo(
                from_type=orig_stype,
                to_type=target_stype,
                path_elements=[]
            )

            use_message_context = (
                orig_stype.is_array() and target_stype.is_array()
                or (
                    orig_stype.is_tuple(ctx.env.schema)
                    and target_stype.is_tuple(ctx.env.schema)
                )
            )

        try:
            res = casts.compile_cast(
                ir_expr,
                target_stype,
                cardinality_mod=expr.cardinality_mod,
                ctx=subctx,
                span=expr.span,
            )

        except errors.QueryError as e:
            if (
                (message_context := casts.cast_message_context(subctx))
                and use_message_context
            ):
                e.args = (
                    (message_context + e.args[0],)
                    + e.args[1:]
                )
            raise e

    return stmt.maybe_add_view(res, ctx=ctx)


def _infer_type_introspection(
    typeref: irast.TypeRef,
    env: context.Environment,
    span: Optional[parsing.Span]=None,
) -> s_types.Type:
    if irtyputils.is_scalar(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::ScalarType'))
    elif irtyputils.is_object(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::ObjectType'))
    elif irtyputils.is_array(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::Array'))
    elif irtyputils.is_tuple(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::Tuple'))
    elif irtyputils.is_range(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::Range'))
    elif irtyputils.is_multirange(typeref):
        return cast(s_objtypes.ObjectType,
                    env.schema.get('schema::MultiRange'))
    else:
        raise errors.QueryError(
            'unexpected type in INTROSPECT', span=span)


@dispatch.compile.register(qlast.Introspect)
def compile_Introspect(
    expr: qlast.Introspect, *, ctx: context.ContextLevel
) -> irast.Set:

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
            span=expr.type.span)
    if irtyputils.is_collection(typeref):
        raise errors.QueryError(
            f'cannot introspect collection types',
            span=expr.type.span)
    if irtyputils.is_generic(typeref):
        raise errors.QueryError(
            f'cannot introspect generic types',
            span=expr.type.span)

    result_typeref = typegen.type_to_typeref(
        _infer_type_introspection(typeref, ctx.env, expr.span), env=ctx.env
    )
    ir = setgen.ensure_set(
        irast.TypeIntrospection(output_typeref=typeref, typeref=result_typeref),
        ctx=ctx,
    )
    return stmt.maybe_add_view(ir, ctx=ctx)


def _infer_index_type(
    expr: irast.Set | irast.Expr,
    index: irast.Set,
    *, ctx: context.ContextLevel,
) -> s_types.Type:
    env = ctx.env
    node_type = setgen.get_expr_type(expr, ctx=ctx)
    index_type = setgen.get_set_type(index, ctx=ctx)

    str_t = env.schema.get('std::str', type=s_scalars.ScalarType)
    bytes_t = env.schema.get('std::bytes', type=s_scalars.ScalarType)
    int_t = env.schema.get('std::int64', type=s_scalars.ScalarType)
    json_t = env.schema.get('std::json', type=s_scalars.ScalarType)

    result: s_types.Type

    if node_type.issubclass(env.schema, str_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index string by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                span=index.span)

        result = str_t

    elif node_type.issubclass(env.schema, bytes_t):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index bytes by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                span=index.span)

        result = bytes_t

    elif node_type.issubclass(env.schema, json_t):

        if not (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema)):

            raise errors.QueryError(
                f'cannot index json by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} or '
                f'{str_t.get_displayname(env.schema)} was expected',
                span=index.span)

        result = json_t

    elif isinstance(node_type, s_types.Array):

        if not index_type.implicitly_castable_to(int_t, env.schema):
            raise errors.QueryError(
                f'cannot index array by '
                f'{index_type.get_displayname(env.schema)}, '
                f'{int_t.get_displayname(env.schema)} was expected',
                span=index.span)

        result = node_type.get_subtypes(env.schema)[0]

    elif (node_type.is_any(env.schema) or
            (node_type.is_scalar() and
                str(node_type.get_name(env.schema)) == 'std::anyscalar') and
            (index_type.implicitly_castable_to(int_t, env.schema) or
                index_type.implicitly_castable_to(str_t, env.schema))):
        result = s_pseudo.PseudoType.get(env.schema, 'anytype')

    else:
        raise errors.QueryError(
            f'index indirection cannot be applied to '
            f'{node_type.get_verbosename(env.schema)}',
            span=expr.span)

    return result


def _infer_slice_type(
    expr: irast.Set,
    start: Optional[irast.Set],
    stop: Optional[irast.Set],
    *, ctx: context.ContextLevel,
) -> s_types.Type:
    env = ctx.env
    node_type = setgen.get_set_type(expr, ctx=ctx)

    str_t = env.schema.get('std::str', type=s_scalars.ScalarType)
    int_t = env.schema.get('std::int64', type=s_scalars.ScalarType)
    json_t = env.schema.get('std::json', type=s_scalars.ScalarType)
    bytes_t = env.schema.get('std::bytes', type=s_scalars.ScalarType)

    if node_type.issubclass(env.schema, str_t):
        base_name = 'string'
    elif node_type.issubclass(env.schema, json_t):
        base_name = 'JSON array'
    elif node_type.issubclass(env.schema, bytes_t):
        base_name = 'bytes'
    elif isinstance(node_type, s_abc.Array):
        base_name = 'array'
    elif node_type.is_any(env.schema):
        base_name = 'anytype'
    else:
        # the base type is not valid
        raise errors.QueryError(
            f'{node_type.get_verbosename(env.schema)} cannot be sliced',
            span=expr.span)

    for index in [start, stop]:
        if index is not None:
            index_type = setgen.get_set_type(index, ctx=ctx)

            if not index_type.implicitly_castable_to(int_t, env.schema):
                raise errors.QueryError(
                    f'cannot slice {base_name} by '
                    f'{index_type.get_displayname(env.schema)}, '
                    f'{int_t.get_displayname(env.schema)} was expected',
                    span=index.span)

    return node_type


@dispatch.compile.register(qlast.Indirection)
def compile_Indirection(
    expr: qlast.Indirection, *, ctx: context.ContextLevel
) -> irast.Set:
    node: Union[irast.Set, irast.Expr] = dispatch.compile(expr.arg, ctx=ctx)
    for indirection_el in expr.indirection:
        if isinstance(indirection_el, qlast.Index):
            idx = dispatch.compile(indirection_el.index, ctx=ctx)
            idx.span = indirection_el.index.span
            typeref = typegen.type_to_typeref(
                _infer_index_type(node, idx, ctx=ctx), env=ctx.env
            )

            node = irast.IndexIndirection(
                expr=node, index=idx, typeref=typeref, span=expr.span
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
            typeref = typegen.type_to_typeref(
                _infer_slice_type(node_set, start, stop, ctx=ctx), env=ctx.env
            )
            node = irast.SliceIndirection(
                expr=node_set, start=start, stop=stop, typeref=typeref,
            )
        else:
            raise ValueError(
                'unexpected indirection node: ' '{!r}'.format(indirection_el)
            )

    return setgen.ensure_set(node, ctx=ctx)


def compile_type_check_op(
    expr: qlast.IsOp, *, ctx: context.ContextLevel
) -> irast.TypeCheckOp:
    # <Expr> IS <TypeExpr>
    left = dispatch.compile(expr.left, ctx=ctx)
    ltype = setgen.get_set_type(left, ctx=ctx)
    typeref = typegen.ql_typeexpr_to_ir_typeref(expr.right, ctx=ctx)

    if ltype.is_object_type() and not ltype.is_free_object_type(ctx.env.schema):
        left = setgen.ptr_step_set(
            left, expr=None, source=ltype, ptr_name='__type__',
            span=expr.span, ctx=ctx
        )
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

    output_typeref = typegen.type_to_typeref(
        ctx.env.schema.get('std::bool', type=s_types.Type),
        env=ctx.env,
    )

    return irast.TypeCheckOp(
        left=left, right=typeref, op=expr.op, result=result,
        typeref=output_typeref)


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


def try_constant_set(expr: irast.Base) -> Optional[irast.ConstantSet]:
    elements = []

    stack: list[Optional[irast.Base]] = [expr]
    while stack:
        el = stack.pop()
        if isinstance(el, irast.Set):
            stack.append(el.expr)
        elif (
            isinstance(el, irast.OperatorCall)
            and str(el.func_shortname) == 'std::UNION'
        ):
            stack.extend([el.args[1].expr.expr, el.args[0].expr.expr])
        elif el and irutils.is_trivial_select(el):
            stack.append(el.result)
        elif isinstance(el, (irast.BaseConstant, irast.Parameter)):
            elements.append(el)
        else:
            return None

    if elements:
        return irast.ConstantSet(
            elements=tuple(elements), typeref=elements[0].typeref
        )
    else:
        return None
