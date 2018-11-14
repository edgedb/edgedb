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


"""EdgeQL routines for function call compilation."""


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
from . import pathctx
from . import setgen
from . import typegen


class BoundCall(typing.NamedTuple):

    func: s_func.Function
    args: typing.Iterable[typing.Tuple[s_func.Parameter, irast.Base]]
    null_args: typing.Set[str]
    return_type: s_types.Type
    used_implicit_casts: bool
    has_empty_variadic: bool


_VARIADIC = ft.ParameterKind.VARIADIC
_NAMED_ONLY = ft.ParameterKind.NAMED_ONLY
_POSITIONAL = ft.ParameterKind.POSITIONAL

_SET_OF = ft.TypeModifier.SET_OF
_OPTIONAL = ft.TypeModifier.OPTIONAL
_SINGLETON = ft.TypeModifier.SINGLETON

_NO_MATCH = BoundCall(None, (), frozenset(), None, False, False)


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new() as fctx:
        if isinstance(expr.func, str):
            if ctx.func is not None:
                ctx_func_params = ctx.func.get_params(ctx.schema)
                if ctx_func_params.get_by_name(expr.func):
                    raise errors.EdgeQLError(
                        f'parameter `{expr.func}` is not callable',
                        context=expr.context)

            funcname = expr.func
        else:
            funcname = sn.Name(expr.func[1], expr.func[0])

        funcs = fctx.schema.get_functions(
            funcname, module_aliases=fctx.modaliases)

        if funcs is None:
            raise errors.EdgeQLError(
                f'could not resolve function name {funcname}',
                context=expr.context)

        fctx.in_func_call = True
        args, kwargs = compile_call_args(expr, funcname, ctx=fctx)

        fatal_array_check = len(funcs) == 1
        matched_call = _NO_MATCH

        for func in funcs:
            call = try_bind_func_args(
                args, kwargs, funcname, func,
                fatal_array_check=fatal_array_check,
                ctx=ctx)

            if call is _NO_MATCH:
                continue

            if matched_call is _NO_MATCH:
                matched_call = call
            else:
                if (matched_call.used_implicit_casts and
                        not call.used_implicit_casts):
                    matched_call = call

                if not args and not kwargs:
                    raise errors.EdgeQLError(
                        f'function {funcname} is not unique',
                        context=expr.context)

        if matched_call is _NO_MATCH:
            raise errors.EdgeQLError(
                f'could not find a function variant {funcname}',
                context=expr.context)

        args, params_typemods = finalize_args(matched_call, ctx=ctx)

        variadic_param_type = None
        matched_func_params = matched_call.func.get_params(ctx.schema)
        if matched_func_params.variadic is not None:
            variadic_param_type = matched_func_params.variadic.type

        matched_func_ret_type = matched_call.func.get_return_type(ctx.schema)
        is_polymorphic = (
            any(p.type.is_polymorphic(ctx.schema)
                for p in matched_func_params) and
            matched_func_ret_type.is_polymorphic(ctx.schema)
        )

        node = irast.FunctionCall(
            args=args,
            func_shortname=func.shortname,
            func_polymorphic=is_polymorphic,
            func_sql_function=func.from_function,
            params_typemods=params_typemods,
            context=expr.context,
            type=matched_call.return_type,
            typemod=matched_call.func.get_return_typemod(ctx.schema),
            has_empty_variadic=matched_call.has_empty_variadic,
            variadic_param_type=variadic_param_type,
        )

        if matched_call.func.initial_value is not None:
            rtype = irutils.infer_type(node, fctx.schema)
            iv_ql = qlast.TypeCast(
                expr=qlparser.parse_fragment(matched_call.func.initial_value),
                type=typegen.type_to_ql_typeref(rtype, ctx=ctx)
            )
            node.func_initial_value = dispatch.compile(iv_ql, ctx=fctx)

    return setgen.ensure_set(node, typehint=matched_call.return_type, ctx=ctx)


def try_bind_func_args(
        args: typing.List[typing.Tuple[s_types.Type, irast.Base]],
        kwargs: typing.Dict[str, typing.Tuple[s_types.Type, irast.Base]],
        funcname: sn.Name,
        func: s_func.Function,
        fatal_array_check: bool = False, *,
        ctx: context.ContextLevel) -> BoundCall:

    def _check_type(arg, arg_type, param_type):
        nonlocal used_implicit_cast
        nonlocal resolved_poly_base_type

        if in_polymorphic_func:
            # Compiling a body of a polymorphic function.

            if arg_type.is_polymorphic(ctx.schema):
                if param_type.is_polymorphic(ctx.schema):
                    return arg_type.test_polymorphic(ctx.schema, param_type)

                arg_poly = arg_type.resolve_polymorphic(ctx.schema, param_type)
                return arg_poly is not None

        else:
            if arg_type.is_polymorphic(ctx.schema):
                raise errors.EdgeQLError(
                    f'a polymorphic argument in a non-polymorphic function',
                    context=arg.context)

        if param_type.is_polymorphic(ctx.schema):
            if not arg_type.test_polymorphic(ctx.schema, param_type):
                return False

            resolved = param_type.resolve_polymorphic(ctx.schema, arg_type)
            if resolved is None:
                return False

            if resolved_poly_base_type is None:
                resolved_poly_base_type = resolved

            return resolved_poly_base_type == resolved

        if arg_type.issubclass(ctx.schema, param_type):
            return True

        if arg_type.implicitly_castable_to(param_type, ctx.schema):
            used_implicit_cast = True
            return True

        return False

    in_polymorphic_func = (
        ctx.func is not None and
        ctx.func.get_params(ctx.schema).has_polymorphic(ctx.schema)
    )

    has_empty_variadic = False
    used_implicit_cast = False
    resolved_poly_base_type = None
    no_args_call = not args and not kwargs
    has_inlined_defaults = func.has_inlined_defaults(ctx.schema)

    func_params = func.get_params(ctx.schema)

    if not func_params:
        if no_args_call:
            # Match: `func` is a function without parameters
            # being called with no arguments.
            args = []
            if has_inlined_defaults:
                bytes_t = ctx.schema.get('std::bytes')
                args = [
                    setgen.ensure_set(
                        irast.BytesConstant(value='\x00', type=bytes_t),
                        typehint=bytes_t,
                        ctx=ctx)
                ]
            return BoundCall(
                func, args, set(),
                func.get_return_type(ctx.schema),
                False, False)
        else:
            # No match: `func` is a function without parameters
            # being called with some arguments.
            return _NO_MATCH

    pg_params = func_params.as_pg_params()

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
        if param.kind is not _NAMED_ONLY:
            break

        pi += 1

        if param.shortname in kwargs:
            matched_kwargs += 1

            arg_type, arg_val = kwargs[param.shortname]
            if not _check_type(arg_val, arg_type, param.type):
                return _NO_MATCH

            bound_param_args.append((param, arg_val))

        else:
            if param.default is None:
                # required named parameter without default and
                # without a matching argument
                return _NO_MATCH

            has_missing_args = True
            bound_param_args.append((param, None))

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
            pi += 1

            if param.kind is _NAMED_ONLY:
                # impossible condition
                raise RuntimeError('unprocessed NAMED ONLY parameter')

            if param.kind is _VARIADIC:
                var_type = param.type.get_subtypes()[0]
                if not _check_type(arg_val, arg_type, var_type):
                    return _NO_MATCH

                bound_param_args.append((param, arg_val))

                for arg_type, arg_val in args[ai:]:
                    if not _check_type(arg_val, arg_type, var_type):
                        return _NO_MATCH

                    bound_param_args.append((param, arg_val))

                break

            if not _check_type(arg_val, arg_type, param.type):
                return _NO_MATCH

            bound_param_args.append((param, arg_val))

        else:
            break

    # Handle yet unprocessed POSITIONAL & VARIADIC arguments.
    for pi in range(pi, nparams):
        param = params[pi]

        if param.kind is _POSITIONAL:
            if param.default is None:
                # required positional parameter that we don't have a
                # positional argument for.
                return _NO_MATCH

            has_missing_args = True
            bound_param_args.append((param, None))

        elif param.kind is _VARIADIC:
            has_empty_variadic = True

        elif param.kind is _NAMED_ONLY:
            # impossible condition
            raise RuntimeError('unprocessed NAMED ONLY parameter')

    # Populate defaults.
    defaults_mask = 0
    null_args = set()
    if has_missing_args:
        if has_inlined_defaults or func_params.named_only:
            for i in range(len(bound_param_args)):
                param, val = bound_param_args[i]
                if val is not None:
                    continue

                null_args.add(param.shortname)

                defaults_mask |= 1 << i

                if not has_inlined_defaults:
                    ql_default = param.get_ql_default()
                    default = dispatch.compile(ql_default, ctx=ctx)

                empty_default = (
                    has_inlined_defaults or
                    irutils.is_empty(default)
                )

                if empty_default:
                    default_type = None

                    if param.type.is_any():
                        if resolved_poly_base_type is None:
                            raise errors.EdgeQLError(
                                f'could not resolve "anytype" type for the '
                                f'${param.shortname} parameter')
                        else:
                            default_type = resolved_poly_base_type
                    else:
                        default_type = param.type

                else:
                    default_type = param.type

                if has_inlined_defaults:
                    default = irutils.new_empty_set(
                        ctx.schema,
                        scls=default_type,
                        alias=param.shortname)

                default = setgen.ensure_set(
                    default,
                    typehint=default_type,
                    ctx=ctx)

                bound_param_args[i] = (
                    param,
                    default
                )

        else:
            bound_param_args = [
                (param, val)
                for (param, val) in bound_param_args
                if val is not None
            ]

    if has_inlined_defaults:
        # If we are compiling an EdgeQL function, inject the defaults
        # bit-mask as a first argument.
        bytes_t = ctx.schema.get('std::bytes')
        bm = defaults_mask.to_bytes(nparams // 8 + 1, 'little')
        bound_param_args.insert(
            0,
            (None, setgen.ensure_set(
                irast.BytesConstant(value=bm.decode('ascii'), type=bytes_t),
                typehint=bytes_t, ctx=ctx)))

    return_type = func.get_return_type(ctx.schema)
    if return_type.is_polymorphic(ctx.schema):
        if resolved_poly_base_type is None:
            return _NO_MATCH
        return_type = return_type.to_nonpolymorphic(
            ctx.schema, resolved_poly_base_type)

    return BoundCall(
        func, bound_param_args, null_args, return_type,
        used_implicit_cast, has_empty_variadic)


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
        return setgen.scoped_set(
            dispatch.compile(arg_ql, ctx=fencectx),
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

        arg_type = irutils.infer_type(arg_ir, ctx.schema)
        if arg_type is None:
            raise errors.EdgeQLError(
                f'could not resolve the type of positional argument '
                f'#{ai} of function {funcname}',
                context=arg.context)

        args.append((arg_type, arg_ir))

    for aname, arg in expr.kwargs.items():
        arg_ir = compile_call_arg(arg, ctx=ctx)

        arg_type = irutils.infer_type(arg_ir, ctx.schema)
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

    for param, arg in bound_call.args:
        if param is None:
            # defaults bitmask
            args.append(arg)
            typemods.append(_SINGLETON)
            continue

        param_mod = param.typemod
        typemods.append(param_mod)

        if param_mod is not _SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)
            if (param_mod is _OPTIONAL or
                    param.shortname in bound_call.null_args):
                pathctx.register_set_in_scope(arg, ctx=ctx)
                pathctx.mark_path_as_optional(arg.path_id, ctx=ctx)

        args.append(arg)

    return args, typemods
