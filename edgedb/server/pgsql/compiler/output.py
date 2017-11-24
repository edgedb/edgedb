##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compilation helpers for output formatting and serialization."""


from edgedb.server.pgsql import ast as pgast

from . import context


def tuple_var_as_json_object(tvar, *, env):
    if not tvar.named:
        return pgast.FuncCall(
            name=('jsonb_build_array',),
            args=[t.val for t in tvar.elements],
            null_safe=True, nullable=tvar.nullable)
    else:
        keyvals = []

        if hasattr(tvar.elements[0].name, 'is_linkprop'):
            # This is a shape attribute map, use a specialized version.
            for element in tvar.elements:
                key = element.name
                if key.is_linkprop:
                    key = '@' + key.name
                else:
                    key = key.name
                keyvals.append(pgast.Constant(val=key))
                if isinstance(element.val, pgast.TupleVar):
                    val = serialize_expr(element.val, env=env)
                else:
                    val = element.val
                keyvals.append(val)
        else:
            for element in tvar.elements:
                name = element.path_id.rptr_name()
                if name is None:
                    name = element.path_id[-1].name.name
                else:
                    name = name.name
                keyvals.append(pgast.Constant(val=name))
                if isinstance(element.val, pgast.TupleVar):
                    val = serialize_expr(element.val, env=env)
                else:
                    val = element.val
                keyvals.append(val)

        return pgast.FuncCall(
            name=('jsonb_build_object',),
            args=keyvals, null_safe=True, nullable=tvar.nullable)


def in_serialization_ctx(
        ctx: context.CompilerContextLevel) -> bool:
    return (
        (ctx.expr_exposed is None or ctx.expr_exposed) and
        ctx.env.output_format == context.OutputFormat.JSON
    )


def output_as_value(
        expr: pgast.Base, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    if isinstance(expr, pgast.TupleVar):
        if in_serialization_ctx(ctx):
            val = serialize_expr(expr, env=ctx.env)
        else:
            val = pgast.ImplicitRowExpr(args=[e.val for e in expr.elements])
    else:
        val = expr

    return val


def serialize_expr_if_needed(
        ctx: context.CompilerContextLevel, expr: pgast.Base) -> pgast.Base:
    if in_serialization_ctx(ctx):
        val = serialize_expr(expr, env=ctx.env)
    else:
        val = expr

    return val


def serialize_expr(
        expr: pgast.Base, *, env: context.Environment) -> pgast.Base:
    if env.output_format == context.OutputFormat.JSON:
        if isinstance(expr, pgast.TupleVar):
            val = tuple_var_as_json_object(expr, env=env)
        elif isinstance(expr, pgast.ImplicitRowExpr):
            val = pgast.FuncCall(
                name=('jsonb_build_array',), args=expr.args,
                null_safe=True)
        else:
            val = pgast.FuncCall(
                name=('to_jsonb',), args=[expr], null_safe=True)
    else:
        val = expr

    return val
