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


"""EdgeQL compiler routines for polymorphic call resolution."""


import typing

from edb import errors

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import functions as s_func
from edb.lang.schema import types as s_types

from edb.lang.edgeql import functypes as ft

from . import context
from . import dispatch
from . import setgen


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
                raise errors.QueryError(
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
                            raise errors.QueryError(
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
