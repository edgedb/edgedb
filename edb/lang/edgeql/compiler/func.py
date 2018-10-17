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


import itertools
import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import functions as s_func
from edb.lang.schema import name as sn
from edb.lang.schema import objects as s_obj
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import errors
from edb.lang.edgeql import parser as qlparser

from . import astutils
from . import context
from . import dispatch
from . import pathctx
from . import setgen
from . import typegen


@dispatch.compile.register(qlast.FunctionCall)
def compile_FunctionCall(
        expr: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    with ctx.new() as fctx:
        if isinstance(expr.func, str):
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
        args, kwargs, arg_types = process_func_args(expr, funcname, ctx=fctx)

        fatal_array_check = len(funcs) == 1
        for funcobj in funcs:
            if check_function(expr, funcname, funcobj, arg_types,
                              fatal_array_check=fatal_array_check):
                break
        else:
            raise errors.EdgeQLError(
                f'could not find a function variant {funcname}',
                context=expr.context)

        fixup_param_scope(funcobj, args, kwargs, ctx=fctx)

        node = irast.FunctionCall(func=funcobj, args=args, kwargs=kwargs)

        if funcobj.initial_value is not None:
            rtype = irutils.infer_type(node, fctx.schema)
            iv_ql = qlast.TypeCast(
                expr=qlparser.parse_fragment(funcobj.initial_value),
                type=typegen.type_to_ql_typeref(rtype)
            )
            node.initial_value = dispatch.compile(iv_ql, ctx=fctx)

    ir_set = setgen.ensure_set(node, ctx=ctx)
    return ir_set


def check_function(
        expr: qlast.FunctionCall,
        funcname: sn.Name,
        func: s_func.Function,
        arg_types: typing.Iterable[s_obj.Object],
        fatal_array_check: bool = False) -> bool:
    if not func.paramtypes:
        if not arg_types:
            # Match: `func` is a function without parameters
            # being called with no arguments.
            return True
        else:
            # No match: `func` is a function without parameters
            # being called with some arguments.
            return False

    if not arg_types:
        # Call without arguments
        for pi, (pd, pk) in enumerate(zip(func.paramdefaults,
                                          func.paramkinds)):
            if pd is None and pk is not irast.ParameterKind.VARIADIC:
                # There is at least one non-variadic parameter
                # without default; hence this function cannot
                # be called without arguments.
                return False
        return True

    try:
        varparam = func.paramkinds.index(irast.ParameterKind.VARIADIC)
    except ValueError:
        varparam = None

    for pn, pt, pd, at in itertools.zip_longest(func.paramnames,
                                                func.paramtypes,
                                                func.paramdefaults,
                                                arg_types):
        if pt is None:
            # We have more arguments than parameters.
            if varparam is not None:
                # Function has a variadic parameter
                # (which must be the last one).
                pt = func.paramtypes[varparam]
            else:
                # No variadic parameter, hence no match.
                return False

        elif at is None:
            # We have fewer arguments than parameters.
            if pd is None:
                return False
        else:
            # We have both types for the parameter and for
            # the argument; check if they are compatible.
            if not at.issubclass(pt):
                return False

            rt = func.return_type
            # If the parameter type is 'any', the return type is
            # 'array<any>', and the argument is also an 'array', then
            # this is an invalid function invocation.
            if (pt.name == 'std::any' and
                    isinstance(rt, s_types.Array) and
                    rt.get_subtypes()[0].name == 'std::any' and
                    isinstance(at, s_types.Array)):

                if fatal_array_check:
                    raise errors.EdgeQLError(
                        f'function {funcname!r} returning {rt.name} cannot '
                        f'take {at.name} as a polymorphic argument',
                        context=expr.context)
                else:
                    return False

    # Match, the `func` passed all checks.
    return True


def process_func_args(
        expr: qlast.FunctionCall, funcname: sn.Name, *,
        ctx: context.ContextLevel) \
        -> typing.Tuple[
            typing.List[irast.Base],            # args
            typing.Dict[str, irast.Base],       # kwargs
            typing.List[s_types.Type]]:    # arg_types
    args = []
    kwargs = {}
    arg_types = []

    for ai, a in enumerate(expr.args):
        arg_ql = a.arg

        if a.sort or a.filter:
            arg_ql = astutils.ensure_qlstmt(arg_ql)
            if a.filter:
                arg_ql.where = astutils.extend_qlbinop(arg_ql.where, a.filter)

            if a.sort:
                arg_ql.orderby = a.sort + arg_ql.orderby

        with ctx.newscope(fenced=True) as fencectx:
            # We put on a SET OF fence preemptively in case this is
            # a SET OF arg, which we don't know yet due to polymorphic
            # matching.
            arg = setgen.scoped_set(
                dispatch.compile(arg_ql, ctx=fencectx),
                ctx=fencectx)

        if a.name:
            kwargs[a.name] = arg
            aname = a.name
        else:
            args.append(arg)
            aname = ai

        arg_type = irutils.infer_type(arg, ctx.schema)
        if arg_type is None:
            raise errors.EdgeQLError(
                f'could not resolve the type of argument '
                f'${aname} of function {funcname}',
                context=a.context)
        arg_types.append(arg_type)

    return args, kwargs, arg_types


def fixup_param_scope(
        func: s_func.Function,
        args: typing.List[irast.Set],
        kwargs: typing.Dict[str, irast.Set], *,
        ctx: context.ContextLevel) -> None:

    varparam_mod = None

    for i, arg in enumerate(args):
        if varparam_mod is not None:
            param_mod = varparam_mod
        else:
            param_mod = func.paramtypemods[i]
            param_kind = func.paramkinds[i]
            if param_kind is irast.ParameterKind.VARIADIC:
                varparam_mod = param_mod
        if param_mod != qlast.TypeModifier.SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)

    for name, arg in kwargs.items():
        i = func.paramnames.index(name)
        param_mod = func.paramtypemods[i]
        if param_mod != qlast.TypeModifier.SET_OF:
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                arg_scope.collapse()
                pathctx.assign_set_scope(arg, None, ctx=ctx)
