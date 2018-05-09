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

from edgedb.lang.common import debug
from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast as irast

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import codegen as pgcodegen

from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

from . import context
from . import dispatch
from . import errors

from .context import OutputFormat  # NOQA


def compile_ir_to_sql_tree(
        ir_expr: irast.Base, *,
        schema, backend=None,
        output_format: typing.Optional[OutputFormat]=None,
        ignore_shapes: bool=False,
        singleton_mode: bool=False) -> pgast.Base:
    try:
        # Transform to sql tree
        ctx_stack = context.CompilerContext()
        ctx = ctx_stack.current
        expr_is_stmt = isinstance(ir_expr, irast.Statement)
        if expr_is_stmt:
            views = ir_expr.views
            ctx.scope_map = ir_expr.scope_map
            ctx.scope_tree = ir_expr.scope_tree
            ir_expr = ir_expr.expr
        else:
            views = {}
        ctx.env = context.Environment(
            schema=schema, output_format=output_format,
            backend=backend, singleton_mode=singleton_mode,
            views=views)
        if ignore_shapes:
            ctx.expr_exposed = False
        qtree = dispatch.compile(ir_expr, ctx=ctx)

    except Exception as e:  # pragma: no cover
        try:
            args = [e.args[0]]
        except (AttributeError, IndexError):
            args = []
        err = errors.IRCompilerInternalError(*args)
        err_ctx = errors.IRCompilerErrorContext(tree=ir_expr)
        edgedb_error.replace_context(err, err_ctx)
        raise err from e

    return qtree


def compile_ir_to_sql(
        ir_expr: irast.Base, *,
        schema, backend=None,
        output_format: typing.Optional[OutputFormat]=None,
        ignore_shapes: bool=False, timer=None):

    if timer is None:
        qtree = compile_ir_to_sql_tree(
            ir_expr, schema=schema, backend=backend,
            output_format=output_format, ignore_shapes=ignore_shapes)
    else:
        with timer.timeit('compile_ir_to_sql'):
            qtree = compile_ir_to_sql_tree(
                ir_expr, schema=schema, backend=backend,
                output_format=output_format, ignore_shapes=ignore_shapes)

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL Tree')
        debug.dump(qtree)

    argmap = qtree.argnames

    # Generate query text
    if timer is None:
        codegen = _run_codegen(qtree)
    else:
        with timer.timeit('compile_ir_to_sql'):
            codegen = _run_codegen(qtree)

    qchunks = codegen.result
    arg_index = codegen.param_index

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL')
        debug.dump_code(''.join(qchunks), lexer='sql')

    return qchunks, argmap, arg_index, type(qtree), tuple()


def _run_codegen(qtree):
    codegen = pgcodegen.SQLSourceGenerator()
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
