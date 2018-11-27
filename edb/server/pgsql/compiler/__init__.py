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


import typing

from edb import errors

from edb.lang.common import debug
from edb.lang.common import exceptions as edgedb_error

from edb.lang.schema import schema as s_schema

from edb.lang.ir import ast as irast

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import codegen as pgcodegen

from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

from . import context
from . import dispatch

from .context import OutputFormat  # NOQA


def compile_ir_to_sql_tree(
        ir_expr: irast.Base, *,
        schema: s_schema.Schema,
        output_format: typing.Optional[OutputFormat]=None,
        ignore_shapes: bool=False,
        singleton_mode: bool=False,
        use_named_params: bool=False) -> pgast.Base:
    try:
        # Transform to sql tree
        ctx_stack = context.CompilerContext()
        ctx = ctx_stack.current
        expr_is_stmt = isinstance(ir_expr, irast.Statement)
        if expr_is_stmt:
            ctx.scope_map = ir_expr.scope_map
            ctx.scope_tree = ir_expr.scope_tree
            ir_expr = ir_expr.expr
        ctx.env = context.Environment(
            schema=schema, output_format=output_format,
            singleton_mode=singleton_mode,
            use_named_params=use_named_params)
        if ignore_shapes:
            ctx.expr_exposed = False
        qtree = dispatch.compile(ir_expr, ctx=ctx)

    except Exception as e:  # pragma: no cover
        try:
            args = [e.args[0]]
        except (AttributeError, IndexError):
            args = []
        raise errors.InternalServerError(*args) from e

    return qtree


def compile_ir_to_sql(
        ir_expr: irast.Base, *,
        schema: s_schema.Schema,
        output_format: typing.Optional[OutputFormat]=None,
        ignore_shapes: bool=False,
        timer=None,
        use_named_params: bool=False,
        pretty: bool=True) -> typing.Tuple[str, typing.Dict[str, int]]:

    if timer is None:
        qtree = compile_ir_to_sql_tree(
            ir_expr, schema=schema, output_format=output_format,
            ignore_shapes=ignore_shapes,
            use_named_params=use_named_params)
    else:
        with timer.timeit('compile_ir_to_sql'):
            qtree = compile_ir_to_sql_tree(
                ir_expr, schema=schema, output_format=output_format,
                ignore_shapes=ignore_shapes,
                use_named_params=use_named_params)

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL Tree')
        debug.dump(qtree, schema=schema)

    argmap = qtree.argnames

    # Generate query text
    if timer is None:
        codegen = _run_codegen(qtree, pretty=pretty)
    else:
        with timer.timeit('compile_ir_to_sql'):
            codegen = _run_codegen(qtree, pretty=pretty)

    sql_text = ''.join(codegen.result)

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL')
        debug.dump_code(sql_text, lexer='sql')

    return sql_text, argmap


def _run_codegen(qtree, *, pretty=True):
    codegen = pgcodegen.SQLSourceGenerator(pretty=pretty)
    try:
        codegen.visit(qtree)
    except pgcodegen.SQLSourceGeneratorError as e:  # pragma: no cover
        ctx = pgcodegen.SQLSourceGeneratorContext(
            qtree, codegen.result)
        edgedb_error.add_context(e, ctx)
        raise
    except Exception as e:  # pragma: no cover
        ctx = pgcodegen.SQLSourceGeneratorContext(
            qtree, codegen.result)
        err = pgcodegen.SQLSourceGeneratorError(
            'error while generating SQL source')
        edgedb_error.add_context(err, ctx)
        raise err from e

    return codegen
