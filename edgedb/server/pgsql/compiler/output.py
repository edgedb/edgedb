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


"""Compilation helpers for output formatting and serialization."""


from edgedb.server.pgsql import ast as pgast

from . import context


def tuple_var_as_json_object(tvar, *, env):
    if not tvar.named:
        return pgast.FuncCall(
            name=('jsonb_build_array',),
            args=[serialize_expr(t.val, nested=True, env=env)
                  for t in tvar.elements],
            null_safe=True, nullable=tvar.nullable)
    else:
        keyvals = []

        for element in tvar.elements:
            rptr = element.path_id.rptr()
            if rptr is None:
                name = element.path_id[-1].name.name
            else:
                name = rptr.shortname.name
                if rptr.is_link_property():
                    name = '@' + name
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
        env: context.Environment) -> pgast.Base:

    if isinstance(expr, pgast.TupleVar):
        val = pgast.ImplicitRowExpr(args=[e.val for e in expr.elements])
    else:
        val = expr

    return val


def serialize_expr_if_needed(
        expr: pgast.Base, *,
        ctx: context.CompilerContextLevel) -> pgast.Base:
    if in_serialization_ctx(ctx):
        val = serialize_expr(expr, env=ctx.env)
    else:
        val = expr

    return val


def serialize_expr(
        expr: pgast.Base, *,
        nested: bool=False,
        env: context.Environment) -> pgast.Base:
    if env.output_format == context.OutputFormat.JSON:
        if isinstance(expr, pgast.TupleVar):
            val = tuple_var_as_json_object(expr, env=env)
        elif isinstance(expr, pgast.ImplicitRowExpr):
            val = pgast.FuncCall(
                name=('jsonb_build_array',), args=expr.args,
                null_safe=True)
        elif not nested:
            val = pgast.FuncCall(
                name=('to_jsonb',), args=[expr], null_safe=True)
        else:
            val = expr
    else:
        val = expr

    return val
