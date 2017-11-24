##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL routines for function call compilation."""


import itertools
import typing

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import functions as s_func
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors
from edgedb.lang.edgeql import parser as qlparser

from . import context
from . import dispatch
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
            funcname, module_aliases=fctx.namespaces)

        if funcs is None:
            raise errors.EdgeQLError(
                f'could not resolve function name {funcname}',
                context=expr.context)

        fctx.in_func_call = True

        is_agg = any(f.aggregate for f in funcs)
        if is_agg:
            fctx.in_aggregate = True

            # FIXME: a stop-gap solution
            if (len(expr.args) == 1 and
                isinstance(expr.args[0], qlast.UnaryOp) and
                    expr.args[0].op == qlast.DISTINCT):

                expr = qlast.FunctionCall(
                    func=expr.func,
                    args=[expr.args[0].operand],
                    agg_set_modifier=qlast.AggDISTINCT,
                    agg_filter=expr.agg_filter,
                    agg_sort=expr.agg_sort
                )
            else:
                expr = qlast.FunctionCall(
                    func=expr.func,
                    args=expr.args,
                    agg_set_modifier=qlast.AggALL,
                    agg_filter=expr.agg_filter,
                    agg_sort=expr.agg_sort
                )

        path_scope = None
        agg_sort = []
        agg_filter = None
        partition = []
        window = False

        if is_agg:
            # When processing calls to aggregate functions,
            # we do not want to affect the statement-wide path scope,
            # so put a newfence barrier here.  Store the scope
            # obtained by processing the agg call in the resulting
            # IR Set.
            with fctx.newfence() as scope_ctx:
                scope_ctx.group_paths.clear()

                args, kwargs, arg_types = \
                    process_func_args(expr, funcname, ctx=scope_ctx)

                if expr.agg_sort:
                    agg_sort = [
                        irast.SortExpr(
                            expr=dispatch.compile(e.path, ctx=scope_ctx),
                            direction=e.direction) for e in expr.agg_sort
                    ]

                elif expr.window:
                    if expr.window.orderby:
                        agg_sort = [
                            irast.SortExpr(
                                expr=dispatch.compile(e.path, ctx=scope_ctx),
                                direction=e.direction)
                            for e in expr.window.orderby
                        ]

                    if expr.window.partition:
                        for partition_expr in expr.window.partition:
                            partition_expr = dispatch.compile(
                                partition_expr, ctx=scope_ctx)
                            partition.append(partition_expr)

                    window = True

                if expr.agg_filter:
                    agg_filter = dispatch.compile(
                        expr.agg_filter, ctx=scope_ctx)

                path_scope = scope_ctx.path_scope

        else:
            args, kwargs, arg_types = \
                process_func_args(expr, funcname, ctx=fctx)

        for funcobj in funcs:
            if check_function(funcobj, arg_types):
                break
        else:
            raise errors.EdgeQLError(
                f'could not find a function variant {funcname}',
                context=expr.context)

        node = irast.FunctionCall(
            func=funcobj, args=args, kwargs=kwargs,
            window=window, partition=partition,
            agg_sort=agg_sort, agg_filter=agg_filter,
            agg_set_modifier=expr.agg_set_modifier)

        if funcobj.initial_value is not None:
            rtype = irutils.infer_type(node, fctx.schema)
            iv_ql = qlast.TypeCast(
                expr=qlparser.parse_fragment(funcobj.initial_value),
                type=typegen.type_to_ql_typeref(rtype)
            )
            node.initial_value = dispatch.compile(iv_ql, ctx=fctx)

    ir_set = setgen.generated_set(node, ctx=ctx)
    ir_set.path_scope = path_scope

    return ir_set


def check_function(
        func: s_func.Function,
        arg_types: typing.Iterable[s_obj.Class]) -> bool:
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
        for pi, pd in enumerate(func.paramdefaults, 1):
            if pd is None and pi != func.varparam:
                # There is at least one non-variadic parameter
                # without default; hence this function cannot
                # be called without arguments.
                return False
        return True

    for pt, pd, at in itertools.zip_longest(func.paramtypes,
                                            func.paramdefaults,
                                            arg_types):
        if pt is None:
            # We have more arguments than parameters.
            if func.varparam is not None:
                # Function has a variadic parameter
                # (which must be the last one).
                pt = func.paramtypes[func.varparam - 1]  # varparam is +1
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

    # Match, the `func` passed all checks.
    return True


def process_func_args(
        expr: qlast.FunctionCall, funcname: sn.Name, *,
        ctx: context.ContextLevel) \
        -> typing.Tuple[
            typing.List[irast.Base],        # args
            typing.Dict[str, irast.Base],   # kwargs
            typing.List[s_obj.NodeClass]]:  # arg_types
    args = []
    kwargs = {}
    arg_types = []

    for ai, a in enumerate(expr.args):
        if isinstance(a, qlast.NamedArg):
            arg = setgen.ensure_set(dispatch.compile(a.arg, ctx=ctx), ctx=ctx)
            kwargs[a.name] = arg
            aname = a.name
        else:
            arg = setgen.ensure_set(dispatch.compile(a, ctx=ctx), ctx=ctx)
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
