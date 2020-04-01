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


"""EdgeQL compiler routines for function calls and operators."""


from __future__ import annotations
from typing import *

from edb import errors

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.schema import constraints as s_constr
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import operators as s_oper
from edb.schema import scalars as s_scalars
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft
from edb.edgeql import parser as qlparser

from . import astutils
from . import casts
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import polyres
from . import schemactx
from . import setgen
from . import stmtctx
from . import typegen

if TYPE_CHECKING:
    import uuid


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.FunctionCall, *, ctx: context.ContextLevel) -> irast.Set:

    env = ctx.env

    if isinstance(expr.func, str):
        if (ctx.env.func_params is not None
                and ctx.env.func_params.get_by_name(env.schema, expr.func)):
            raise errors.QueryError(
                f'parameter `{expr.func}` is not callable',
                context=expr.context)

        funcname = expr.func
    else:
        funcname = sn.Name(expr.func[1], expr.func[0])

    funcs = env.schema.get_functions(funcname, module_aliases=ctx.modaliases)

    if funcs is None:
        raise errors.QueryError(
            f'could not resolve function name {funcname}',
            context=expr.context)

    in_polymorphic_func = (
        ctx.env.func_params is not None and
        ctx.env.func_params.has_polymorphic(env.schema)
    )

    in_abstract_constraint = (
        in_polymorphic_func and
        ctx.env.parent_object_type is s_constr.Constraint
    )

    args, kwargs = compile_call_args(expr, funcname, ctx=ctx)
    matched = polyres.find_callable(funcs, args=args, kwargs=kwargs, ctx=ctx)
    if not matched:
        raise errors.QueryError(
            f'could not find a function variant {funcname}',
            context=expr.context)
    elif len(matched) > 1:
        if in_abstract_constraint:
            matched_call = matched[0]
        else:
            raise errors.QueryError(
                f'function {funcname} is not unique',
                context=expr.context)
    else:
        matched_call = matched[0]

    func = matched_call.func
    assert isinstance(func, s_func.Function)
    func_name = func.get_shortname(env.schema)

    if not ctx.env.session_mode and func.get_session_only(env.schema):
        raise errors.QueryError(
            f'{func_name}() cannot be called in a non-session context',
            context=expr.context)

    matched_func_params = func.get_params(env.schema)
    variadic_param = matched_func_params.find_variadic(env.schema)
    variadic_param_type = None
    if variadic_param is not None:
        variadic_param_type = typegen.type_to_typeref(
            variadic_param.get_type(env.schema),
            env=env,
        )

    matched_func_ret_type = func.get_return_type(env.schema)
    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_func_params.objects(env.schema)) and
        matched_func_ret_type.is_polymorphic(env.schema)
    )

    matched_func_initial_value = func.get_initial_value(env.schema)

    final_args, params_typemods = finalize_args(
        matched_call,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    if not in_abstract_constraint:
        # We cannot add strong references to functions from
        # abstract constraints, since we cannot know which
        # form of the function is actually used.
        env.schema_refs.add(func)

    func_initial_value: Optional[irast.Set]

    if matched_func_initial_value is not None:
        iv_ql = qlast.TypeCast(
            expr=qlparser.parse_fragment(matched_func_initial_value.text),
            type=typegen.type_to_ql_typeref(matched_call.return_type, ctx=ctx),
        )
        func_initial_value = setgen.ensure_set(
            dispatch.compile(iv_ql, ctx=ctx),
            ctx=ctx,
        )
    else:
        func_initial_value = None

    rtype = matched_call.return_type
    path_id = pathctx.get_expression_path_id(rtype, ctx=ctx)

    if rtype.is_tuple():
        rtype = cast(s_types.Tuple, rtype)
        tuple_path_ids = []
        nested_path_ids = []
        for n, st in rtype.iter_subtypes(ctx.env.schema):
            elem_path_id = pathctx.get_tuple_indirection_path_id(
                path_id, n, st, ctx=ctx).strip_weak_namespaces()

            if isinstance(st, s_types.Tuple):
                nested_path_ids.append([
                    pathctx.get_tuple_indirection_path_id(
                        elem_path_id, nn, sst, ctx=ctx).strip_weak_namespaces()
                    for nn, sst in st.iter_subtypes(ctx.env.schema)
                ])

            tuple_path_ids.append(elem_path_id)
        for nested in nested_path_ids:
            tuple_path_ids.extend(nested)
    else:
        tuple_path_ids = []

    fcall = irast.FunctionCall(
        args=final_args,
        func_module_id=env.schema.get_global(
            s_mod.Module, func_name.module).id,
        func_shortname=func_name,
        func_polymorphic=is_polymorphic,
        func_sql_function=func.get_from_function(env.schema),
        force_return_cast=func.get_force_return_cast(env.schema),
        session_only=func.get_session_only(env.schema),
        volatility=func.get_volatility(env.schema),
        sql_func_has_out_params=func.get_sql_func_has_out_params(env.schema),
        error_on_null_result=func.get_error_on_null_result(env.schema),
        params_typemods=params_typemods,
        context=expr.context,
        typeref=typegen.type_to_typeref(
            rtype, env=env,
        ),
        typemod=matched_call.func.get_return_typemod(env.schema),
        has_empty_variadic=matched_call.has_empty_variadic,
        variadic_param_type=variadic_param_type,
        func_initial_value=func_initial_value,
        tuple_path_ids=tuple_path_ids,
    )

    return setgen.ensure_set(fcall, typehint=rtype, path_id=path_id, ctx=ctx)


#: A dictionary of conditional callables and the indices
#: of the arguments that are evaluated conditionally.
CONDITIONAL_OPS = {
    'std::IF': {0, 2},
    'std::??': {1},
}


def compile_operator(
        qlexpr: qlast.Base, op_name: str, qlargs: List[qlast.Base], *,
        ctx: context.ContextLevel) -> irast.Set:

    env = ctx.env
    schema = env.schema
    opers = schema.get_operators(op_name, module_aliases=ctx.modaliases)

    if opers is None:
        raise errors.QueryError(
            f'no operator matches the given name and argument types',
            context=qlexpr.context)

    fq_op_name = next(iter(opers)).get_shortname(ctx.env.schema)
    conditional_args = CONDITIONAL_OPS.get(fq_op_name)

    args = []
    for ai, qlarg in enumerate(qlargs):
        with ctx.newscope(fenced=True) as fencectx:
            # We put on a SET OF fence preemptively in case this is
            # a SET OF arg, which we don't know yet due to polymorphic
            # matching.  We will remove it if necessary in `finalize_args()`.
            if conditional_args and ai in conditional_args:
                fencectx.in_conditional = qlexpr.context

            arg_ir = setgen.ensure_set(
                dispatch.compile(qlarg, ctx=fencectx),
                ctx=fencectx)

            arg_ir = setgen.scoped_set(
                setgen.ensure_stmt(arg_ir, ctx=fencectx),
                ctx=fencectx)

        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of operand '
                f'#{ai} of {op_name}',
                context=qlarg.context)

        args.append((arg_type, arg_ir))

    # Check if the operator is a derived operator, and if so,
    # find the origins.
    origin_op = opers[0].get_derivative_of(env.schema)
    derivative_op: Optional[s_oper.Operator]
    if origin_op is not None:
        # If this is a derived operator, there should be
        # exactly one form of it.  This is enforced at the DDL
        # level, but check again to be sure.
        if len(opers) > 1:
            raise errors.InternalServerError(
                f'more than one derived operator of the same name: {op_name}',
                context=qlarg.context)

        derivative_op = opers[0]
        opers = schema.get_operators(origin_op)
        if not opers:
            raise errors.InternalServerError(
                f'cannot find the origin operator for {op_name}',
                context=qlarg.context)
        actual_typemods = [
            param.get_typemod(schema)
            for param in derivative_op.get_params(schema).objects(schema)
        ]
    else:
        derivative_op = None
        actual_typemods = []

    matched = None
    # Some 2-operand operators are special when their operands are
    # arrays or tuples.
    if len(args) == 2:
        coll_opers = None
        # If both of the args are arrays or tuples, potentially
        # compile the operator for them differently than for other
        # combinations.
        if args[0][0].is_tuple() and args[1][0].is_tuple():
            # Out of the candidate operators, find the ones that
            # correspond to tuples.
            coll_opers = [op for op in opers
                          if all(param.get_type(schema).is_tuple() for param
                                 in op.get_params(schema).objects(schema))]

        elif args[0][0].is_array() and args[1][0].is_array():
            # Out of the candidate operators, find the ones that
            # correspond to arrays.
            coll_opers = [op for op in opers
                          if all(param.get_type(schema).is_array() for param
                                 in op.get_params(schema).objects(schema))]

        # Proceed only if we have a special case of collection operators.
        if coll_opers:
            # Then check if they are recursive (i.e. validation must be
            # done recursively for the subtypes). We rely on the fact that
            # it is forbidden to define an operator that has both
            # recursive and non-recursive versions.
            if not coll_opers[0].get_recursive(schema):
                # The operator is non-recursive, so regular processing
                # is needed.
                matched = polyres.find_callable(
                    coll_opers, args=args, kwargs={}, ctx=ctx)

            else:
                # The recursive operators are usually defined as
                # being polymorphic on all parameters, and so this has
                # a side-effect of forcing both operands to be of
                # the same type (via casting) before the operator is
                # applied.  This might seem suboptmial, since there might
                # be a more specific operator for the types of the
                # elements, but the current version of Postgres
                # actually requires tuples and arrays to be of the
                # same type in comparison, so this behavior is actually
                # what we want.
                matched = polyres.find_callable(
                    coll_opers,
                    args=args,
                    kwargs={},
                    ctx=ctx,
                )

                # Now that we have an operator, we need to validate that it
                # can be applied to the tuple or array elements.
                submatched = validate_recursive_operator(
                    opers, args[0], args[1], ctx=ctx)

                if len(submatched) != 1:
                    # This is an error. We want the error message to
                    # reflect whether no matches were found or too
                    # many, so we preserve the submatches found for
                    # this purpose.
                    matched = submatched

    # No special handling match was necessary, find a normal match.
    if matched is None:
        matched = polyres.find_callable(opers, args=args, kwargs={}, ctx=ctx)

    in_polymorphic_func = (
        ctx.env.func_params is not None and
        ctx.env.func_params.has_polymorphic(env.schema)
    )

    in_abstract_constraint = (
        in_polymorphic_func and
        ctx.env.parent_object_type is s_constr.Constraint
    )

    if not in_polymorphic_func:
        matched = [call for call in matched
                   if not call.func.get_is_abstract(env.schema)]

    if len(matched) == 1:
        matched_call = matched[0]
    else:
        if len(args) == 2:
            ltype = schemactx.get_material_type(args[0][0], ctx=ctx)
            rtype = schemactx.get_material_type(args[1][0], ctx=ctx)

            types = (
                f'{ltype.get_displayname(env.schema)!r} and '
                f'{rtype.get_displayname(env.schema)!r}')
        else:
            types = ', '.join(
                repr(
                    schemactx.get_material_type(
                        a[0], ctx=ctx).get_displayname(env.schema)
                ) for a in args
            )

        if not matched:
            hint = ('Consider using an explicit type cast or a conversion '
                    'function.')

            if op_name == 'std::IF':
                hint = (f"The IF and ELSE result clauses must be of "
                        f"compatible types, while the condition clause must "
                        f"be 'std::bool'. {hint}")
            elif op_name == '+':
                str_t = cast(s_scalars.ScalarType,
                             env.schema.get('std::str'))
                bytes_t = cast(s_scalars.ScalarType,
                               env.schema.get('std::bytes'))
                if (
                    (ltype.issubclass(env.schema, str_t) and
                        rtype.issubclass(env.schema, str_t)) or
                    (ltype.issubclass(env.schema, bytes_t) and
                        rtype.issubclass(env.schema, bytes_t)) or
                    (ltype.is_array() and rtype.is_array())
                ):
                    hint = 'Consider using the "++" operator for concatenation'

            raise errors.QueryError(
                f'operator {str(op_name)!r} cannot be applied to '
                f'operands of type {types}',
                hint=hint,
                context=qlexpr.context)
        elif len(matched) > 1:
            if in_abstract_constraint:
                matched_call = matched[0]
            else:
                detail = ', '.join(
                    f'`{m.func.get_verbosename(ctx.env.schema)}`'
                    for m in matched
                )
                raise errors.QueryError(
                    f'operator {str(op_name)!r} is ambiguous for '
                    f'operands of type {types}',
                    hint=f'Possible variants: {detail}.',
                    context=qlexpr.context)

    oper = matched_call.func
    assert isinstance(oper, s_oper.Operator)
    env.schema_refs.add(oper)
    oper_name = oper.get_shortname(env.schema)

    matched_params = oper.get_params(env.schema)
    rtype = matched_call.return_type

    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_params.objects(env.schema)) and
        rtype.is_polymorphic(env.schema)
    )

    final_args, params_typemods = finalize_args(
        matched_call,
        actual_typemods=actual_typemods,
        is_polymorphic=is_polymorphic,
        ctx=ctx,
    )

    if oper_name in {'std::UNION', 'std::IF'} and rtype.is_object_type():
        # Special case for the UNION and IF operators, instead of common
        # parent type, we return a union type.
        if oper_name == 'std::UNION':
            larg, rarg = (a.expr for a in final_args)
        else:
            larg, _, rarg = (a.expr for a in final_args)

        left_type = schemactx.get_material_type(
            setgen.get_set_type(larg, ctx=ctx),
            ctx=ctx,
        )
        right_type = schemactx.get_material_type(
            setgen.get_set_type(rarg, ctx=ctx),
            ctx=ctx,
        )

        if left_type.issubclass(env.schema, right_type):
            rtype = right_type
        elif right_type.issubclass(env.schema, left_type):
            rtype = left_type
        else:
            assert isinstance(left_type, s_types.InheritingType)
            assert isinstance(right_type, s_types.InheritingType)
            rtype = schemactx.get_union_type([left_type, right_type], ctx=ctx)

    from_op = oper.get_from_operator(env.schema)
    sql_operator = None
    if (from_op is not None and oper.get_code(env.schema) is None and
            oper.get_from_function(env.schema) is None and
            not in_polymorphic_func):
        sql_operator = tuple(from_op)

    origin_name: Optional[sn.SchemaName]
    origin_module_id: Optional[uuid.UUID]
    if derivative_op is not None:
        origin_name = oper_name
        origin_module_id = env.schema.get_global(
            s_mod.Module, origin_name.module).id
        oper_name = derivative_op.get_shortname(env.schema)
    else:
        origin_name = None
        origin_module_id = None

    node = irast.OperatorCall(
        args=final_args,
        func_module_id=env.schema.get_global(
            s_mod.Module, oper_name.module).id,
        func_shortname=oper_name,
        func_polymorphic=is_polymorphic,
        origin_name=origin_name,
        origin_module_id=origin_module_id,
        func_sql_function=oper.get_from_function(env.schema),
        sql_operator=sql_operator,
        force_return_cast=oper.get_force_return_cast(env.schema),
        volatility=oper.get_volatility(env.schema),
        operator_kind=oper.get_operator_kind(env.schema),
        params_typemods=params_typemods,
        context=qlexpr.context,
        typeref=typegen.type_to_typeref(rtype, env=env),
        typemod=oper.get_return_typemod(env.schema),
    )

    return setgen.ensure_set(node, typehint=rtype, ctx=ctx)


def validate_recursive_operator(
        opers: Iterable[s_func.CallableObject],
        larg: Tuple[s_types.Type, irast.Set],
        rarg: Tuple[s_types.Type, irast.Set], *,
        ctx: context.ContextLevel) -> List[polyres.BoundCall]:

    matched: List[polyres.BoundCall] = []

    # if larg and rarg are tuples or arrays, recurse into their subtypes
    if (larg[0].is_tuple() and rarg[0].is_tuple() or
            larg[0].is_array() and rarg[0].is_array()):
        assert isinstance(larg[0], s_types.Collection)
        assert isinstance(rarg[0], s_types.Collection)
        for rsub, lsub in zip(larg[0].get_subtypes(ctx.env.schema),
                              rarg[0].get_subtypes(ctx.env.schema)):
            matched = validate_recursive_operator(
                opers, (lsub, larg[1]), (rsub, rarg[1]), ctx=ctx)
            if len(matched) != 1:
                # this is an error already
                break

    else:
        # we just have a pair of non-containers to compare
        matched = polyres.find_callable(
            opers, args=[larg, rarg], kwargs={}, ctx=ctx)

    return matched


def compile_call_arg(arg_ql: qlast.Expr, *,
                     ctx: context.ContextLevel) -> irast.Set:
    with ctx.new() as argctx:
        # We put on a SET OF fence preemptively in case this is
        # a SET OF arg, which we don't know yet due to polymorphic
        # matching.  We will remove it if necessary in `finalize_args()`.
        # Similarly, delay the decision to inject the implicit limit to
        # `finalize_args()`.
        arg_ql = astutils.ensure_qlstmt(arg_ql)
        argctx.inhibit_implicit_limit = True
        return setgen.ensure_set(
            dispatch.compile(arg_ql, ctx=argctx),
            ctx=argctx,
        )


def compile_call_args(
        expr: qlast.FunctionCall, funcname: str, *,
        ctx: context.ContextLevel) \
        -> Tuple[
            List[Tuple[s_types.Type, irast.Set]],
            Dict[str, Tuple[s_types.Type, irast.Set]]]:

    args = []
    kwargs = {}

    for ai, arg in enumerate(expr.args):
        arg_ir = compile_call_arg(arg, ctx=ctx)
        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of positional argument '
                f'#{ai} of function {funcname}',
                context=arg.context)

        args.append((arg_type, arg_ir))

    for aname, arg in expr.kwargs.items():
        arg_ir = compile_call_arg(arg, ctx=ctx)

        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.QueryError(
                f'could not resolve the type of named argument '
                f'${aname} of function {funcname}',
                context=arg.context)

        kwargs[aname] = (arg_type, arg_ir)

    return args, kwargs


def finalize_args(
    bound_call: polyres.BoundCall, *,
    actual_typemods: Sequence[ft.TypeModifier] = (),
    is_polymorphic: bool = False,
    ctx: context.ContextLevel,
) -> Tuple[List[irast.CallArg], List[ft.TypeModifier]]:

    args: List[irast.CallArg] = []
    typemods = []

    for i, barg in enumerate(bound_call.args):
        param = barg.param
        arg = barg.val
        if param is None:
            # defaults bitmask
            args.append(irast.CallArg(expr=arg))
            typemods.append(ft.TypeModifier.SINGLETON)
            continue

        if actual_typemods:
            param_mod = actual_typemods[i]
        else:
            param_mod = param.get_typemod(ctx.env.schema)

        typemods.append(param_mod)

        if param_mod is not ft.TypeModifier.SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            param_shortname = param.get_parameter_name(ctx.env.schema)

            # Arg was wrapped for scope fencing purposes,
            # but that fence has been removed above, so unwrap it.
            orig_arg = arg
            arg = irutils.unwrap_set(arg)

            if (param_mod is ft.TypeModifier.OPTIONAL or
                    param_shortname in bound_call.null_args):

                if arg_scope is not None:
                    # Due to the construction of relgen, the (unfenced)
                    # subscope is necessary to shield LHS paths from the outer
                    # query to prevent path binding which may break OPTIONAL.
                    branch = arg_scope.unfence()

                pathctx.register_set_in_scope(arg, ctx=ctx)
                pathctx.mark_path_as_optional(arg.path_id, ctx=ctx)

                if arg_scope is not None:
                    pathctx.assign_set_scope(arg, branch, ctx=ctx)

            elif arg_scope is not None:
                arg_scope.collapse()
                if arg is orig_arg:
                    pathctx.assign_set_scope(arg, None, ctx=ctx)
        else:
            if (is_polymorphic
                    and ctx.expr_exposed
                    and ctx.implicit_limit
                    and isinstance(arg.expr, irast.SelectStmt)
                    and arg.expr.limit is None):
                arg.expr.limit = setgen.ensure_set(
                    dispatch.compile(
                        qlast.IntegerConstant(value=str(ctx.implicit_limit)),
                        ctx=ctx,
                    ),
                    ctx=ctx,
                )

        paramtype = barg.param_type
        param_kind = param.get_kind(ctx.env.schema)
        if param_kind is ft.ParameterKind.VARIADIC:
            # For variadic params, paramtype would be array<T>,
            # and we need T to cast the arguments.
            assert isinstance(paramtype, s_types.Array)
            paramtype = list(paramtype.get_subtypes(ctx.env.schema))[0]

        # Check if we need to cast the argument value before passing
        # it to the callable.
        compatible = schemactx.is_type_compatible(
            paramtype,
            barg.valtype,
            ctx=ctx,
        )

        if not compatible:
            # The callable form was chosen via an implicit cast,
            # cast the arguments so that the backend has no
            # wiggle room to apply its own (potentially different)
            # casting.
            arg = casts.compile_cast(
                arg, paramtype, srcctx=None, ctx=ctx)

        if param_mod is not ft.TypeModifier.SET_OF:
            call_arg = irast.CallArg(expr=arg, cardinality=ft.Cardinality.ONE)
        else:
            call_arg = irast.CallArg(expr=arg, cardinality=None)
            stmtctx.get_expr_cardinality_later(
                target=call_arg, field='cardinality', irexpr=arg, ctx=ctx)

        args.append(call_arg)

    return args, typemods
