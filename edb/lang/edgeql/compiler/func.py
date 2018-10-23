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


_VARIADIC = ft.ParameterKind.VARIADIC
_NAMED_ONLY = ft.ParameterKind.NAMED_ONLY
_POSITIONAL = ft.ParameterKind.POSITIONAL

_SET_OF = ft.TypeModifier.SET_OF
_OPTIONAL = ft.TypeModifier.OPTIONAL
_SINGLETON = ft.TypeModifier.SINGLETON


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new() as fctx:
        if isinstance(expr.func, str):
            if ctx.func is not None and ctx.func.params.get_by_name(expr.func):
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
        matched_func = None
        matched_args = None
        matched_args_implicit_cast = False

        for func in funcs:
            _bound_args, _used_impl_cast = try_bind_func_args(
                args, kwargs, funcname, func,
                fatal_array_check=fatal_array_check,
                ctx=ctx)

            if _bound_args is not None:
                if matched_args is None:
                    matched_func = func
                    matched_args = _bound_args
                    matched_args_implicit_cast = _used_impl_cast
                else:
                    if matched_args_implicit_cast and not _used_impl_cast:
                        matched_func = func
                        matched_args = _bound_args
                        matched_args_implicit_cast = False

                    if not args and not kwargs:
                        raise errors.EdgeQLError(
                            f'function {funcname} is not unique',
                            context=expr.context)

        if matched_func is None:
            raise errors.EdgeQLError(
                f'could not find a function variant {funcname}',
                context=expr.context)

        node = irast.FunctionCall(func=matched_func,
                                  args=matched_args,
                                  context=expr.context)

        if matched_func.initial_value is not None:
            rtype = irutils.infer_type(node, fctx.schema)
            iv_ql = qlast.TypeCast(
                expr=qlparser.parse_fragment(matched_func.initial_value),
                type=typegen.type_to_ql_typeref(rtype)
            )
            node.initial_value = dispatch.compile(iv_ql, ctx=fctx)

    ir_set = setgen.ensure_set(node, ctx=ctx)
    return ir_set


def try_bind_func_args(
        args: typing.List[typing.Tuple[s_types.Type, irast.Base]],
        kwargs: typing.Dict[str, typing.Tuple[s_types.Type, irast.Base]],
        funcname: sn.Name,
        func: s_func.Function,
        fatal_array_check: bool = False, *,
        ctx: context.ContextLevel) -> typing.List[irast.Base]:

    def _check_type(arg_type, param_type):
        nonlocal used_implicit_cast

        if arg_type.issubclass(param_type):
            return True

        if arg_type.implicitly_castable_to(param_type, ctx.schema):
            used_implicit_cast = True
            return True

        return False

    def _check_any(param, arg, arg_type) -> bool:
        nonlocal any_concrete_type

        if param.type.name == 'std::any':
            return_type = func.return_type
            if (isinstance(return_type, s_types.Array) and
                    return_type.get_subtypes()[0].name == 'std::any' and
                    isinstance(arg_type, s_types.Array)):

                if fatal_array_check:
                    raise errors.EdgeQLError(
                        f'function {funcname!r} returning '
                        f'{return_type.name} cannot '
                        f'take {arg_type.name} as a polymorphic argument',
                        context=arg.context)
                else:
                    return False

            if any_concrete_type is None:
                any_concrete_type = arg_type

        if (isinstance(param.type, s_types.Array) and
                param.type.element_type.name == 'std::any' and
                any_concrete_type is None):
            any_concrete_type = arg_type.element_type

        return True

    used_implicit_cast = False
    any_concrete_type = None
    no_args_call = not args and not kwargs

    if not func.params:
        if no_args_call:
            # Match: `func` is a function without parameters
            # being called with no arguments.
            return [], False
        else:
            # No match: `func` is a function without parameters
            # being called with some arguments.
            return None, False

    pg_params = func.params.as_pg_params()

    if no_args_call and pg_params.has_param_wo_default:
        # A call without arguments and there is at least
        # one parameter without default.
        return None, False

    bound_param_args = []

    params = pg_params.params
    nparams = len(params)
    nargs = len(args)
    populate_defaults = False

    ai = 0
    pi = 0

    while True:
        if ai < nargs:
            arg_type, arg_val = args[ai]
            ai += 1

            if pi >= nparams:
                # too many positional arguments
                return None, False
            param = params[pi]
            pi += 1

            if param.kind is _NAMED_ONLY:
                # too many positional arguments
                return None, False

            if param.kind is _VARIADIC:
                var_type = param.type.get_subtypes()[0]
                if not _check_type(arg_type, var_type):
                    return None, False
                vals = [arg_val]
                for arg_type, arg_val in args[ai:]:
                    if not _check_type(arg_type, var_type):
                        return None, False
                    vals.append(arg_val)

                bound_param_args.append((
                    param,
                    setgen.ensure_set(
                        irast.Array(elements=vals),
                        typehint=param.type,
                        ctx=ctx)
                ))
                break

            if not _check_type(arg_type, param.type):
                return None, False
            if not _check_any(param, arg_val, arg_type):
                return None, False

            bound_param_args.append((param, arg_val))

        else:
            break

    matched_kwargs = 0
    for pi in range(pi, nparams):
        param = params[pi]

        if param.kind is _POSITIONAL:
            if param.default is None:
                # required positional parameter that we don't have a
                # positional argument for.
                return None, False

            populate_defaults = True
            bound_param_args.append((param, None))

        elif param.kind is _VARIADIC:
            bound_param_args.append((
                param,
                setgen.ensure_set(
                    irast.Array(elements=[]),
                    typehint=param.type,
                    ctx=ctx)
            ))

        elif param.kind is _NAMED_ONLY:
            if param.name in kwargs:
                matched_kwargs += 1

                arg_type, arg_val = kwargs[param.name]
                if not _check_type(arg_type, param.type):
                    return None, False
                if not _check_any(param, arg_val, arg_type):
                    return None, False

                bound_param_args.append((param, arg_val))

            else:
                if param.default is None:
                    # required named parameter without default and
                    # without a matching argument
                    return None, False

                populate_defaults = True
                bound_param_args.append((param, None))

    if matched_kwargs != len(kwargs):
        # extra kwargs?
        return None, False

    if populate_defaults:
        for i in range(len(bound_param_args)):
            param, val = bound_param_args[i]
            if val is not None:
                continue

            default = param.get_ir_default(schema=ctx.schema)

            if irutils.is_empty(default):
                default_type = None

                if param.type.name == 'std::any':
                    if any_concrete_type is None:
                        raise errors.EdgeQLError(
                            f'could not resolve std::any type for the '
                            f'${param.name} parameter')
                    else:
                        default_type = any_concrete_type
                else:
                    default_type = param.type

                default = setgen.ensure_set(
                    default,
                    typehint=default_type,
                    ctx=ctx)

            else:
                default = setgen.ensure_set(
                    default,
                    typehint=param.type,
                    ctx=ctx)

            bound_param_args[i] = (
                param,
                default
            )

    bound_args = []
    for param, arg in bound_param_args:
        param_mod = param.typemod
        if param_mod is not _SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)
            if param_mod is _OPTIONAL:
                pathctx.register_set_in_scope(arg, ctx=ctx)
                pathctx.mark_path_as_optional(arg.path_id, ctx=ctx)
        bound_args.append(arg)

    return bound_args, used_implicit_cast


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
        # matching.
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
