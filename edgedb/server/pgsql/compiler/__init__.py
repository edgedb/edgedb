##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common import debug
from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast as irast

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import codegen as pgcodegen
from edgedb.server.pgsql import optimizer as pgopt

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
        ctx.env = context.Environment(
            schema=schema, output_format=output_format,
            backend=backend, singleton_mode=singleton_mode)
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
        ignore_shapes: bool=False, optimize: bool=False, timer=None):

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

    if optimize:
        if debug.flags.edgeql_optimize:  # pragma: no cover
            debug.header('SQL Tree before optimization')
            debug.dump(qtree, _ast_include_meta=False)

            codegen = _run_codegen(qtree)
            qchunks = codegen.result
            debug.header('SQL before optimization')
            debug.dump_code(''.join(qchunks), lexer='sql')

        opt = pgopt.Optimizer()
        if timer is None:
            qtree = opt.optimize(qtree)
        else:
            with timer.timeit('optimize'):
                qtree = opt.optimize(qtree)

    argmap = qtree.argnames

    # Generate query text
    if timer is None:
        codegen = _run_codegen(qtree)
    else:
        with timer.timeit('compile_ir_to_sql'):
            codegen = _run_codegen(qtree)

    qchunks = codegen.result
    arg_index = codegen.param_index

    if (debug.flags.edgeql_compile or
            debug.flags.edgeql_optimize):  # pragma: no cover
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
