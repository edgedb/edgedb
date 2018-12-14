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

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import functions as s_func
from edb.lang.schema import name as sn
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors
from edb.lang.edgeql import functypes as ft
from edb.lang.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import setgen
from . import typegen


class BoundArg(typing.NamedTuple):

    param: typing.Optional[s_func.Parameter]
    val: typing.Optional[irast.Base]
    valtype: s_types.Type
    cast_distance: int


class BoundCall(typing.NamedTuple):

    func: s_func.CallableObject
    args: typing.List[BoundArg]
    null_args: typing.Set[str]
    return_type: typing.Optional[s_types.Type]
    has_empty_variadic: bool


_VARIADIC = ft.ParameterKind.VARIADIC
_NAMED_ONLY = ft.ParameterKind.NAMED_ONLY
_POSITIONAL = ft.ParameterKind.POSITIONAL

_SET_OF = ft.TypeModifier.SET_OF
_OPTIONAL = ft.TypeModifier.OPTIONAL
_SINGLETON = ft.TypeModifier.SINGLETON

_NO_MATCH = BoundCall(None, [], frozenset(), None, False)


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:

    env = ctx.env

    if isinstance(expr.func, str):
        if ctx.func is not None:
            ctx_func_params = ctx.func.get_params(env.schema)
            if ctx_func_params.get_by_name(env.schema, expr.func):
                raise errors.EdgeQLError(
                    f'parameter `{expr.func}` is not callable',
                    context=expr.context)

        funcname = expr.func
    else:
        funcname = sn.Name(expr.func[1], expr.func[0])

    funcs = env.schema.get_functions(funcname, module_aliases=ctx.modaliases)

    if funcs is None:
        raise errors.EdgeQLError(
            f'could not resolve function name {funcname}',
            context=expr.context)

    args, kwargs = compile_call_args(expr, funcname, ctx=ctx)
    matched = find_callable(funcs, args=args, kwargs=kwargs, ctx=ctx)
    if not matched:
        raise errors.EdgeQLError(
            f'could not find a function variant {funcname}',
            context=expr.context)
    elif len(matched) > 1:
        raise errors.EdgeQLError(
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
        raise errors.EdgeQLError(
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
            raise errors.EdgeQLError(
                f'could not resolve the type of operand '
                f'#{ai} of {op_name}',
                context=qlarg.context)

        args.append((arg_type, arg_ir))

    matched = find_callable(opers, args=args, kwargs={}, ctx=ctx)
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
            raise errors.EdgeQLError(
                f'operator {str(op_name)!r} cannot be applied to '
                f'operands of type {types}',
                context=qlexpr.context)
        elif len(matched) > 1:
            detail = ', '.join(
                f'`{m.func.get_display_signature(ctx.env.schema)}`'
                for m in matched
            )
            raise errors.EdgeQLError(
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


def find_callable(
        candidates: typing.Iterable[s_func.CallableObject], *,
        args: typing.List[typing.Tuple[s_types.Type, irast.Base]],
        kwargs: typing.Dict[str, typing.Tuple[s_types.Type, irast.Base]],
        ctx: context.ContextLevel) -> typing.List[BoundCall]:

    implicit_cast_distance = None
    matched = []

    for candidate in candidates:
        call = try_bind_call_args(args, kwargs, candidate, ctx=ctx)
        if call is _NO_MATCH:
            continue

        total_cd = sum(barg.cast_distance for barg in call.args)

        if implicit_cast_distance is None:
            implicit_cast_distance = total_cd
            matched.append(call)
        elif implicit_cast_distance == total_cd:
            matched.append(call)
        elif implicit_cast_distance > total_cd:
            implicit_cast_distance = total_cd
            matched = [call]

    if len(matched) <= 1:
        # Unabiguios resolution
        return matched

    else:
        # Ambiguous resolution, try to disambiguate by
        # checking for total type distance.
        type_dist = None
        remaining = []

        for call in matched:
            call_type_dist = 0

            for barg in call.args:
                if barg.param is None:
                    # Skip injected bitmask argument.
                    continue

                paramtype = barg.param.get_type(ctx.env.schema)
                arg_type_dist = barg.valtype.get_common_parent_type_distance(
                    paramtype, ctx.env.schema)
                call_type_dist += arg_type_dist

            if type_dist is None:
                type_dist = call_type_dist
                remaining.append(call)
            elif type_dist == call_type_dist:
                remaining.append(call)
            elif type_dist > call_type_dist:
                type_dist = call_type_dist
                remaining = [call]

        return remaining


def try_bind_call_args(
        args: typing.List[typing.Tuple[s_types.Type, irast.Base]],
        kwargs: typing.Dict[str, typing.Tuple[s_types.Type, irast.Base]],
        func: s_func.CallableObject, *,
        ctx: context.ContextLevel) -> BoundCall:

    def _get_cast_distance(arg, arg_type, param_type) -> int:
        nonlocal resolved_poly_base_type

        if in_polymorphic_func:
            # Compiling a body of a polymorphic function.

            if arg_type.is_polymorphic(schema):
                if param_type.is_polymorphic(schema):
                    if arg_type.test_polymorphic(schema, param_type):
                        return 0
                    else:
                        return -1
                else:
                    if arg_type.resolve_polymorphic(schema, param_type):
                        return 0
                    else:
                        return -1

        else:
            if arg_type.is_polymorphic(schema):
                raise errors.EdgeQLError(
                    f'a polymorphic argument in a non-polymorphic function',
                    context=arg.context)

        if param_type.is_polymorphic(schema):
            if not arg_type.test_polymorphic(schema, param_type):
                return -1

            resolved = param_type.resolve_polymorphic(schema, arg_type)
            if resolved is None:
                return -1

            if resolved_poly_base_type is None:
                resolved_poly_base_type = resolved

            if resolved_poly_base_type == resolved:
                return 0

            ct = resolved_poly_base_type.find_common_implicitly_castable_type(
                resolved, ctx.env.schema)

            return 0 if ct is not None else -1

        if arg_type.issubclass(schema, param_type):
            return 0

        return arg_type.get_implicit_cast_distance(param_type, schema)

    schema = ctx.env.schema

    in_polymorphic_func = (
        ctx.func is not None and
        ctx.func.get_params(schema).has_polymorphic(schema)
    )

    has_empty_variadic = False
    resolved_poly_base_type = None
    no_args_call = not args and not kwargs
    has_inlined_defaults = func.has_inlined_defaults(schema)

    func_params = func.get_params(schema)

    if not func_params:
        if no_args_call:
            # Match: `func` is a function without parameters
            # being called with no arguments.
            args = []
            if has_inlined_defaults:
                bytes_t = schema.get('std::bytes')
                argval = setgen.ensure_set(
                    irast.BytesConstant(value='\x00', stype=bytes_t),
                    typehint=bytes_t,
                    ctx=ctx)
                args = [BoundArg(None, argval, bytes_t, 0)]
            return BoundCall(
                func, args, set(),
                func.get_return_type(schema),
                False)
        else:
            # No match: `func` is a function without parameters
            # being called with some arguments.
            return _NO_MATCH

    pg_params = s_func.PgParams.from_params(schema, func_params)
    named_only = func_params.find_named_only(schema)

    if no_args_call and pg_params.has_param_wo_default:
        # A call without arguments and there is at least
        # one parameter without default.
        return _NO_MATCH

    bound_param_args = []

    params = pg_params.params
    nparams = len(params)
    nargs = len(args)
    has_missing_args = False

    ai = 0
    pi = 0
    matched_kwargs = 0

    # Bind NAMED ONLY arguments (they are compiled as first set of arguments).
    while True:
        if pi >= nparams:
            break

        param = params[pi]
        if param.get_kind(schema) is not _NAMED_ONLY:
            break

        pi += 1

        param_shortname = param.get_shortname(schema)
        if param_shortname in kwargs:
            matched_kwargs += 1

            arg_type, arg_val = kwargs[param_shortname]
            cd = _get_cast_distance(arg_val, arg_type, param.get_type(schema))
            if cd < 0:
                return _NO_MATCH

            bound_param_args.append(
                BoundArg(param, arg_val, arg_type, cd))

        else:
            if param.get_default(schema) is None:
                # required named parameter without default and
                # without a matching argument
                return _NO_MATCH

            has_missing_args = True
            bound_param_args.append(
                BoundArg(param, None, param.get_type(schema), 0))

    if matched_kwargs != len(kwargs):
        # extra kwargs?
        return _NO_MATCH

    # Bind POSITIONAL arguments (compiled to go after NAMED ONLY arguments).
    while True:
        if ai < nargs:
            arg_type, arg_val = args[ai]
            ai += 1

            if pi >= nparams:
                # too many positional arguments
                return _NO_MATCH
            param = params[pi]
            param_kind = param.get_kind(schema)
            pi += 1

            if param_kind is _NAMED_ONLY:
                # impossible condition
                raise RuntimeError('unprocessed NAMED ONLY parameter')

            if param_kind is _VARIADIC:
                var_type = param.get_type(schema).get_subtypes()[0]
                cd = _get_cast_distance(arg_val, arg_type, var_type)
                if cd < 0:
                    return _NO_MATCH

                bound_param_args.append(
                    BoundArg(param, arg_val, arg_type, cd))

                for arg_type, arg_val in args[ai:]:
                    cd = _get_cast_distance(arg_val, arg_type, var_type)
                    if cd < 0:
                        return _NO_MATCH

                    bound_param_args.append(
                        BoundArg(param, arg_val, arg_type, cd))

                break

            cd = _get_cast_distance(arg_val, arg_type, param.get_type(schema))
            if cd < 0:
                return _NO_MATCH

            bound_param_args.append(
                BoundArg(param, arg_val, arg_type, cd))

        else:
            break

    # Handle yet unprocessed POSITIONAL & VARIADIC arguments.
    for pi in range(pi, nparams):
        param = params[pi]
        param_kind = param.get_kind(schema)

        if param_kind is _POSITIONAL:
            if param.get_default(schema) is None:
                # required positional parameter that we don't have a
                # positional argument for.
                return _NO_MATCH

            has_missing_args = True
            param_type = param.get_type(schema)
            bound_param_args.append(
                BoundArg(param, None, param_type, 0))

        elif param_kind is _VARIADIC:
            has_empty_variadic = True

        elif param_kind is _NAMED_ONLY:
            # impossible condition
            raise RuntimeError('unprocessed NAMED ONLY parameter')

    # Populate defaults.
    defaults_mask = 0
    null_args = set()
    if has_missing_args:
        if has_inlined_defaults or named_only:
            for i in range(len(bound_param_args)):
                barg = bound_param_args[i]
                if barg.val is not None:
                    continue

                param = barg.param
                param_shortname = param.get_shortname(schema)
                null_args.add(param_shortname)

                defaults_mask |= 1 << i

                if not has_inlined_defaults:
                    ql_default = param.get_ql_default(schema)
                    default = dispatch.compile(ql_default, ctx=ctx)

                empty_default = (
                    has_inlined_defaults or
                    irutils.is_empty(default)
                )

                param_type = param.get_type(schema)

                if empty_default:
                    default_type = None

                    if param_type.is_any():
                        if resolved_poly_base_type is None:
                            raise errors.EdgeQLError(
                                f'could not resolve "anytype" type for the '
                                f'${param_shortname} parameter')
                        else:
                            default_type = resolved_poly_base_type
                    else:
                        default_type = param_type

                else:
                    default_type = param_type

                if has_inlined_defaults:
                    default = irutils.new_empty_set(
                        schema,
                        stype=default_type,
                        alias=param_shortname)

                default = setgen.ensure_set(
                    default,
                    typehint=default_type,
                    ctx=ctx)

                bound_param_args[i] = BoundArg(
                    param,
                    default,
                    barg.valtype,
                    barg.cast_distance,
                )

        else:
            bound_param_args = [
                barg for barg in bound_param_args if barg.val is not None]

    if has_inlined_defaults:
        # If we are compiling an EdgeQL function, inject the defaults
        # bit-mask as a first argument.
        bytes_t = schema.get('std::bytes')
        bm = defaults_mask.to_bytes(nparams // 8 + 1, 'little')
        bm_set = setgen.ensure_set(
            irast.BytesConstant(value=bm.decode('ascii'), stype=bytes_t),
            typehint=bytes_t, ctx=ctx)
        bound_param_args.insert(0, BoundArg(None, bm_set, bytes_t, 0))

    return_type = func.get_return_type(schema)
    if return_type.is_polymorphic(schema):
        if resolved_poly_base_type is not None:
            return_type = return_type.to_nonpolymorphic(
                schema, resolved_poly_base_type)
        elif not in_polymorphic_func:
            return _NO_MATCH

    return BoundCall(
        func, bound_param_args, null_args, return_type, has_empty_variadic)


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
            raise errors.EdgeQLError(
                f'could not resolve the type of positional argument '
                f'#{ai} of function {funcname}',
                context=arg.context)

        args.append((arg_type, arg_ir))

    for aname, arg in expr.kwargs.items():
        arg_ir = compile_call_arg(arg, ctx=ctx)

        arg_type = inference.infer_type(arg_ir, ctx.env)
        if arg_type is None:
            raise errors.EdgeQLError(
                f'could not resolve the type of named argument '
                f'${aname} of function {funcname}',
                context=arg.context)

        kwargs[aname] = (arg_type, arg_ir)

    return args, kwargs


def finalize_args(bound_call: BoundCall, *,
                  ctx: context.ContextLevel) -> typing.List[irast.Base]:

    args = []
    typemods = []

    for barg in bound_call.args:
        param = barg.param
        arg = barg.val
        if param is None:
            # defaults bitmask
            args.append(arg)
            typemods.append(_SINGLETON)
            continue

        param_mod = param.get_typemod(ctx.env.schema)
        typemods.append(param_mod)

        if param_mod is not _SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            param_shortname = param.get_shortname(ctx.env.schema)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)

            # Arg was wrapped for scope fencing purposes,
            # but that fence has been removed above, so unwrap it.
            arg = irutils.unwrap_set(arg)

            if (param_mod is _OPTIONAL or
                    param_shortname in bound_call.null_args):

                pathctx.register_set_in_scope(arg, ctx=ctx)
                pathctx.mark_path_as_optional(arg.path_id, ctx=ctx)

        args.append(arg)

    return args, typemods
