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


import typing

from edb import errors

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import name as sn
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import functypes as ft
from edb.lang.edgeql import parser as qlparser

from . import astutils
from . import cast
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import polyres
from . import setgen
from . import typegen


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:

    env = ctx.env

    if isinstance(expr.func, str):
        if ctx.func is not None:
            ctx_func_params = ctx.func.get_params(env.schema)
            if ctx_func_params.get_by_name(env.schema, expr.func):
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

    args, kwargs = compile_call_args(expr, funcname, ctx=ctx)
    matched = polyres.find_callable(funcs, args=args, kwargs=kwargs, ctx=ctx)
    if not matched:
        raise errors.QueryError(
            f'could not find a function variant {funcname}',
            context=expr.context)
    elif len(matched) > 1:
        raise errors.QueryError(
            f'function {funcname} is not unique',
            context=expr.context)
    else:
        matched_call = matched[0]

    args, params_typemods = finalize_args(matched_call, ctx=ctx)

    matched_func_params = matched_call.func.get_params(env.schema)
    variadic_param = matched_func_params.find_variadic(env.schema)
    variadic_param_type = None
    if variadic_param is not None:
        variadic_param_type = variadic_param.get_type(env.schema)

    matched_func_ret_type = matched_call.func.get_return_type(env.schema)
    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_func_params.objects(env.schema)) and
        matched_func_ret_type.is_polymorphic(env.schema)
    )

    matched_func_initial_value = matched_call.func.get_initial_value(
        env.schema)

    func = matched_call.func

    node = irast.FunctionCall(
        args=args,
        func_shortname=func.get_shortname(env.schema),
        func_polymorphic=is_polymorphic,
        func_sql_function=func.get_from_function(env.schema),
        force_return_cast=func.get_force_return_cast(env.schema),
        params_typemods=params_typemods,
        context=expr.context,
        stype=matched_call.return_type,
        typemod=matched_call.func.get_return_typemod(env.schema),
        has_empty_variadic=matched_call.has_empty_variadic,
        variadic_param_type=variadic_param_type,
    )

    if matched_func_initial_value is not None:
        rtype = inference.infer_type(node, env=ctx.env)
        iv_ql = qlast.TypeCast(
            expr=qlparser.parse_fragment(matched_func_initial_value),
            type=typegen.type_to_ql_typeref(rtype, ctx=ctx)
        )
        node.func_initial_value = dispatch.compile(iv_ql, ctx=ctx)

    return setgen.ensure_set(node, typehint=matched_call.return_type, ctx=ctx)


def compile_operator(
        qlexpr: qlast.Base, op_name: str, qlargs: typing.List[qlast.Base], *,
        ctx: context.ContextLevel) -> irast.OperatorCall:

    env = ctx.env
    opers = env.schema.get_operators(op_name, module_aliases=ctx.modaliases)

    if opers is None:
        raise errors.QueryError(
            f'no operator matches the given name and argument types',
            context=qlexpr.context)

    args = []
    for ai, qlarg in enumerate(qlargs):
        with ctx.newscope(fenced=True) as fencectx:
            # We put on a SET OF fence preemptively in case this is
            # a SET OF arg, which we don't know yet due to polymorphic
            # matching.  We will remove it if necessary in `finalize_args()`.
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

    matched = polyres.find_callable(opers, args=args, kwargs={}, ctx=ctx)
    if len(matched) == 1:
        matched_call = matched[0]
    else:
        if len(args) == 2:
            ltype = args[0][0].material_type(env.schema)
            rtype = args[1][0].material_type(env.schema)

            types = (
                f'{ltype.get_displayname(env.schema)!r} and '
                f'{rtype.get_displayname(env.schema)!r}')
        else:
            types = ', '.join(
                repr(
                    a[0].material_type(env.schema).get_displayname(env.schema)
                ) for a in args
            )

        if not matched:
            raise errors.QueryError(
                f'operator {str(op_name)!r} cannot be applied to '
                f'operands of type {types}',
                context=qlexpr.context)
        elif len(matched) > 1:
            detail = ', '.join(
                f'`{m.func.get_display_signature(ctx.env.schema)}`'
                for m in matched
            )
            raise errors.QueryError(
                f'operator {str(op_name)!r} is ambiguous for '
                f'operands of type {types}',
                hint=f'Possible variants: {detail}.',
                context=qlexpr.context)

    args, params_typemods = finalize_args(matched_call, ctx=ctx)

    oper = matched_call.func

    matched_params = oper.get_params(env.schema)
    matched_ret_type = oper.get_return_type(env.schema)
    is_polymorphic = (
        any(p.get_type(env.schema).is_polymorphic(env.schema)
            for p in matched_params.objects(env.schema)) and
        matched_ret_type.is_polymorphic(env.schema)
    )

    in_polymorphic_func = (
        ctx.func is not None and
        ctx.func.get_params(env.schema).has_polymorphic(env.schema)
    )

    from_op = oper.get_from_operator(env.schema)
    if (from_op is not None and oper.get_code(env.schema) is None and
            oper.get_from_function(env.schema) is None and
            not in_polymorphic_func):
        sql_operator = tuple(from_op)
    else:
        sql_operator = None

    node = irast.OperatorCall(
        args=args,
        func_shortname=oper.get_shortname(env.schema),
        func_polymorphic=is_polymorphic,
        func_sql_function=oper.get_from_function(env.schema),
        sql_operator=sql_operator,
        force_return_cast=oper.get_force_return_cast(env.schema),
        operator_kind=oper.get_operator_kind(env.schema),
        params_typemods=params_typemods,
        context=qlexpr.context,
        stype=matched_call.return_type,
        typemod=oper.get_return_typemod(env.schema),
    )

    return setgen.ensure_set(node, typehint=matched_call.return_type, ctx=ctx)


def compile_call_arg(arg: qlast.FuncArg, *,
                     ctx: context.ContextLevel) -> irast.Base:
    arg_ql = arg.arg

    if arg.sort or arg.filter:
        arg_ql = astutils.ensure_qlstmt(arg_ql)
        if arg.filter:
            arg_ql.where = astutils.extend_qlbinop(arg_ql.where, arg.filter)

        if arg.sort:
            arg_ql.orderby = arg.sort + arg_ql.orderby

    with ctx.newscope(fenced=True) as fencectx:
        # We put on a SET OF fence preemptively in case this is
        # a SET OF arg, which we don't know yet due to polymorphic
        # matching.  We will remove it if necessary in `finalize_args()`.
        arg_ir = setgen.ensure_set(
            dispatch.compile(arg_ql, ctx=fencectx),
            ctx=fencectx)

        return setgen.scoped_set(
            setgen.ensure_stmt(arg_ir, ctx=fencectx),
            ctx=fencectx)


def compile_call_args(
        expr: qlast.FunctionCall, funcname: sn.Name, *,
        ctx: context.ContextLevel) \
        -> typing.Tuple[
            typing.List[typing.Tuple[s_types.Type, irast.Base]],
            typing.Dict[str, typing.Tuple[s_types.Type, irast.Base]]]:

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


def finalize_args(bound_call: polyres.BoundCall, *,
                  ctx: context.ContextLevel) -> typing.List[irast.Base]:

    args = []
    typemods = []

    for barg in bound_call.args:
        param = barg.param
        arg = barg.val
        if param is None:
            # defaults bitmask
            args.append(arg)
            typemods.append(ft.TypeModifier.SINGLETON)
            continue

        param_mod = param.get_typemod(ctx.env.schema)
        typemods.append(param_mod)

        if param_mod is not ft.TypeModifier.SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            param_shortname = param.get_shortname(ctx.env.schema)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)

            # Arg was wrapped for scope fencing purposes,
            # but that fence has been removed above, so unwrap it.
            arg = irutils.unwrap_set(arg)

            if (param_mod is ft.TypeModifier.OPTIONAL or
                    param_shortname in bound_call.null_args):

                pathctx.register_set_in_scope(arg, ctx=ctx)
                pathctx.mark_path_as_optional(arg.path_id, ctx=ctx)

        paramtype = param.get_type(ctx.env.schema)
        param_kind = param.get_kind(ctx.env.schema)
        if param_kind is ft.ParameterKind.VARIADIC:
            # For variadic params, paramtype would be array<T>,
            # and we need T to cast the arguments.
            paramtype = list(paramtype.get_subtypes())[0]

        if (not barg.valtype.issubclass(ctx.env.schema, paramtype)
                and not paramtype.is_polymorphic(ctx.env.schema)):
            # The callable form was chosen via an implicit cast,
            # cast the arguments so that the backend has no
            # wiggle room to apply its own (potentially different)
            # casting.
            arg = cast.compile_cast(
                arg, paramtype, srcctx=None, ctx=ctx)

        args.append(arg)

    return args, typemods
