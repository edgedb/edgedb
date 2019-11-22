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


from __future__ import annotations

from typing import *  # NoQA

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.schema import functions as s_func
from edb.schema import types as s_types

from edb.edgeql import qltypes as ft

from . import context
from . import dispatch
from . import setgen


class BoundArg(NamedTuple):

    param: Optional[s_func.Parameter]
    param_type: s_types.Type
    val: irast.Set
    valtype: s_types.Type
    cast_distance: int


class MissingArg(NamedTuple):

    param: Optional[s_func.Parameter]
    param_type: s_types.Type


class BoundCall(NamedTuple):

    func: s_func.CallableLike
    args: List[BoundArg]
    null_args: Set[str]
    return_type: s_types.Type
    has_empty_variadic: bool


_VARIADIC = ft.ParameterKind.VARIADIC
_NAMED_ONLY = ft.ParameterKind.NAMED_ONLY
_POSITIONAL = ft.ParameterKind.POSITIONAL

_SET_OF = ft.TypeModifier.SET_OF
_OPTIONAL = ft.TypeModifier.OPTIONAL
_SINGLETON = ft.TypeModifier.SINGLETON


def find_callable(
        candidates: Iterable[s_func.CallableLike], *,
        args: Sequence[Tuple[s_types.Type, irast.Set]],
        kwargs: Mapping[str, Tuple[s_types.Type, irast.Set]],
        ctx: context.ContextLevel) -> List[BoundCall]:

    implicit_cast_distance = None
    matched = []

    for candidate in candidates:
        call = try_bind_call_args(args, kwargs, candidate, ctx=ctx)
        if call is None:
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
        args: Sequence[Tuple[s_types.Type, irast.Set]],
        kwargs: Mapping[str, Tuple[s_types.Type, irast.Set]],
        func: s_func.CallableLike, *,
        ctx: context.ContextLevel) -> Optional[BoundCall]:

    return_type = func.get_return_type(ctx.env.schema)
    is_abstract = func.get_is_abstract(ctx.env.schema)
    resolved_poly_base_type: Optional[s_types.Type] = None

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

        if param_type.is_polymorphic(schema):
            if not arg_type.test_polymorphic(schema, param_type):
                return -1

            resolved = param_type.resolve_polymorphic(schema, arg_type)
            if resolved is None:
                return -1

            if resolved_poly_base_type is None:
                resolved_poly_base_type = resolved

            if resolved_poly_base_type == resolved:
                return s_types.MAX_TYPE_DISTANCE if is_abstract else 0

            ct = resolved_poly_base_type.find_common_implicitly_castable_type(
                resolved, ctx.env.schema)

            if ct is not None:
                # If we found a common implicitly castable type, we
                # refine our resolved_poly_base_type to be that as the
                # more general case.
                resolved_poly_base_type = ct
                return s_types.MAX_TYPE_DISTANCE if is_abstract else 0
            else:
                return -1

        if arg_type.issubclass(schema, param_type):
            return 0

        return arg_type.get_implicit_cast_distance(param_type, schema)

    schema = ctx.env.schema

    in_polymorphic_func = (
        ctx.env.func_params is not None and
        ctx.env.func_params.has_polymorphic(schema)
    )

    has_empty_variadic = False
    no_args_call = not args and not kwargs
    has_inlined_defaults = func.has_inlined_defaults(schema)

    func_params = func.get_params(schema)

    if not func_params:
        if no_args_call:
            # Match: `func` is a function without parameters
            # being called with no arguments.
            bargs: List[BoundArg] = []
            if has_inlined_defaults:
                bytes_t = ctx.env.get_track_schema_type('std::bytes')
                argval = setgen.ensure_set(
                    irast.BytesConstant(
                        value=b'\x00',
                        typeref=irtyputils.type_to_typeref(schema, bytes_t)),
                    typehint=bytes_t,
                    ctx=ctx)
                bargs = [BoundArg(None, bytes_t, argval, bytes_t, 0)]
            return BoundCall(
                func, bargs, set(),
                return_type, False)
        else:
            # No match: `func` is a function without parameters
            # being called with some arguments.
            return None

    pg_params = s_func.PgParams.from_params(schema, func_params)
    named_only = func_params.find_named_only(schema)

    if no_args_call and pg_params.has_param_wo_default:
        # A call without arguments and there is at least
        # one parameter without default.
        return None

    bound_args_prep: List[Union[MissingArg, BoundArg]] = []

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
        param_type = param.get_type(schema)
        if param_shortname in kwargs:
            matched_kwargs += 1

            arg_type, arg_val = kwargs[param_shortname]
            cd = _get_cast_distance(arg_val, arg_type, param_type)
            if cd < 0:
                return None

            bound_args_prep.append(
                BoundArg(param, param_type, arg_val, arg_type, cd))

        else:
            if param.get_default(schema) is None:
                # required named parameter without default and
                # without a matching argument
                return None

            has_missing_args = True
            bound_args_prep.append(MissingArg(param, param_type))

    if matched_kwargs != len(kwargs):
        # extra kwargs?
        return None

    # Bind POSITIONAL arguments (compiled to go after NAMED ONLY arguments).
    while True:
        if ai < nargs:
            arg_type, arg_val = args[ai]
            ai += 1

            if pi >= nparams:
                # too many positional arguments
                return None
            param = params[pi]
            param_type = param.get_type(schema)
            param_kind = param.get_kind(schema)
            pi += 1

            if param_kind is _NAMED_ONLY:
                # impossible condition
                raise RuntimeError('unprocessed NAMED ONLY parameter')

            if param_kind is _VARIADIC:
                param_type = cast(s_types.Array, param_type)
                var_type = param_type.get_subtypes(schema)[0]
                cd = _get_cast_distance(arg_val, arg_type, var_type)
                if cd < 0:
                    return None

                bound_args_prep.append(
                    BoundArg(param, param_type, arg_val, arg_type, cd))

                for arg_type, arg_val in args[ai:]:
                    cd = _get_cast_distance(arg_val, arg_type, var_type)
                    if cd < 0:
                        return None

                    bound_args_prep.append(
                        BoundArg(param, param_type, arg_val, arg_type, cd))

                break

            cd = _get_cast_distance(arg_val, arg_type, param_type)
            if cd < 0:
                return None

            bound_args_prep.append(
                BoundArg(param, param_type, arg_val, arg_type, cd))

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
                return None

            has_missing_args = True
            param_type = param.get_type(schema)
            bound_args_prep.append(MissingArg(param, param_type))

        elif param_kind is _VARIADIC:
            has_empty_variadic = True

        elif param_kind is _NAMED_ONLY:
            # impossible condition
            raise RuntimeError('unprocessed NAMED ONLY parameter')

    # Populate defaults.
    defaults_mask = 0
    null_args: Set[str] = set()
    bound_param_args: List[BoundArg] = []
    if has_missing_args:
        if has_inlined_defaults or named_only:
            for i, barg in enumerate(bound_args_prep):
                if isinstance(barg, BoundArg):
                    bound_param_args.append(barg)
                    continue
                if barg.param is None:
                    # Shouldn't be possible; the code above takes care of this.
                    raise RuntimeError(
                        f'failed to resolve the parameter for the arg #{i}')

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
                    default = setgen.new_empty_set(
                        stype=default_type,
                        alias=param_shortname,
                        ctx=ctx)

                default = setgen.ensure_set(
                    default,
                    typehint=default_type,
                    ctx=ctx)

                bound_param_args.append(
                    BoundArg(
                        param,
                        param_type,
                        default,
                        param_type,
                        0,
                    )
                )

        else:
            bound_param_args = [
                barg for barg in bound_args_prep if isinstance(barg, BoundArg)
            ]
    else:
        bound_param_args = cast(List[BoundArg], bound_args_prep)

    if has_inlined_defaults:
        # If we are compiling an EdgeQL function, inject the defaults
        # bit-mask as a first argument.
        bytes_t = ctx.env.get_track_schema_type('std::bytes')
        bm = defaults_mask.to_bytes(nparams // 8 + 1, 'little')
        bm_set = setgen.ensure_set(
            irast.BytesConstant(
                value=bm,
                typeref=irtyputils.type_to_typeref(ctx.env.schema, bytes_t)),
            typehint=bytes_t, ctx=ctx)
        bound_param_args.insert(0, BoundArg(None, bytes_t, bm_set, bytes_t, 0))

    if return_type.is_polymorphic(schema):
        if resolved_poly_base_type is not None:
            return_type = return_type.to_nonpolymorphic(
                schema, resolved_poly_base_type)
        elif not in_polymorphic_func:
            return None

    # resolved_poly_base_type may be legitimately None within
    # bodies of polymorphic functions
    if resolved_poly_base_type is not None:
        for i, barg in enumerate(bound_param_args):
            if barg.param_type.is_polymorphic(schema):
                bound_param_args[i] = BoundArg(
                    barg.param,
                    barg.param_type.to_nonpolymorphic(
                        schema, resolved_poly_base_type),
                    barg.val,
                    barg.valtype,
                    barg.cast_distance,
                )

    return BoundCall(
        func, bound_param_args, null_args, return_type, has_empty_variadic)
