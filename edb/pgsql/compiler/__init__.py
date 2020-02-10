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


from __future__ import annotations

from typing import *

from edb import errors

from edb.common import debug
from edb.common import exceptions as edgedb_error

from edb.ir import ast as irast

from edb.pgsql import ast as pgast
from edb.pgsql import codegen as pgcodegen

from . import config as _config_compiler  # NOQA
from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

from . import context
from . import dispatch

from .context import OutputFormat  # NOQA


def compile_ir_to_sql_tree(
        ir_expr: irast.Base, *,
        output_format: Optional[OutputFormat]=None,
        ignore_shapes: bool=False,
        explicit_top_cast: Optional[irast.TypeRef]=None,
        singleton_mode: bool=False,
        use_named_params: bool=False,
        expected_cardinality_one: bool=False) -> pgast.Base:
    try:
        # Transform to sql tree
        env = context.Environment(
            output_format=output_format,
            expected_cardinality_one=expected_cardinality_one,
            use_named_params=use_named_params,
            ignore_object_shapes=ignore_shapes,
            explicit_top_cast=explicit_top_cast)

        if isinstance(ir_expr, irast.Statement):
            scope_tree = ir_expr.scope_tree
            ir_expr = ir_expr.expr
        elif isinstance(ir_expr, irast.ConfigCommand):
            scope_tree = ir_expr.scope_tree
        else:
            scope_tree = irast.new_scope_tree()

        ctx = context.CompilerContextLevel(
            None,
            context.ContextSwitchMode.TRANSPARENT,
            env=env,
            scope_tree=scope_tree,
        )

        _ = context.CompilerContext(initial=ctx)
        ctx.singleton_mode = singleton_mode
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
        output_format: Optional[OutputFormat]=None,
        ignore_shapes: bool=False,
        explicit_top_cast: Optional[irast.TypeRef]=None,
        use_named_params: bool=False,
        expected_cardinality_one: bool=False,
        pretty: bool=True) -> Tuple[str, Dict[str, int]]:

    qtree = compile_ir_to_sql_tree(
        ir_expr,
        output_format=output_format,
        ignore_shapes=ignore_shapes,
        explicit_top_cast=explicit_top_cast,
        use_named_params=use_named_params,
        expected_cardinality_one=expected_cardinality_one)

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL Tree')
        debug.dump(qtree)

    assert isinstance(qtree, pgast.Query), "expected instance of ast.Query"
    argmap = qtree.argnames

    # Generate query text
    codegen = _run_codegen(qtree, pretty=pretty)
    sql_text = ''.join(codegen.result)

    if debug.flags.edgeql_compile:  # pragma: no cover
        debug.header('SQL')
        debug.dump_code(sql_text, lexer='sql')

    return sql_text, argmap


def _run_codegen(
    qtree: pgast.Base,
    *,
    pretty: bool=True,
) -> pgcodegen.SQLSourceGenerator:
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
