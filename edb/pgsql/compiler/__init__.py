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
from edb.pgsql import params as pgparams

from . import config as _config_compiler  # NOQA
from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

from . import clauses
from . import context
from . import dispatch

from .context import OutputFormat as OutputFormat # NOQA


def compile_ir_to_sql_tree(
    ir_expr: irast.Base,
    *,
    output_format: Optional[OutputFormat] = None,
    ignore_shapes: bool = False,
    explicit_top_cast: Optional[irast.TypeRef] = None,
    singleton_mode: bool = False,
    use_named_params: bool = False,
    expected_cardinality_one: bool = False,
    external_rvars: Optional[
        Mapping[Tuple[irast.PathId, str], pgast.PathRangeVar]
    ] = None,
    backend_runtime_params: Optional[pgparams.BackendRuntimeParams]=None,
) -> pgast.Base:
    try:
        # Transform to sql tree
        query_params = []
        type_rewrites = {}

        singletons = []
        if isinstance(ir_expr, irast.Statement):
            scope_tree = ir_expr.scope_tree
            query_params = list(ir_expr.params)
            type_rewrites = ir_expr.type_rewrites
            singletons = ir_expr.singletons
            ir_expr = ir_expr.expr
        elif isinstance(ir_expr, irast.ConfigCommand):
            assert ir_expr.scope_tree
            scope_tree = ir_expr.scope_tree
        else:
            scope_tree = irast.new_scope_tree()

        scope_tree_nodes = {
            node.unique_id: node for node in scope_tree.descendants
            if node.unique_id is not None
        }

        if backend_runtime_params is None:
            backend_runtime_params = pgparams.get_default_runtime_params()

        env = context.Environment(
            output_format=output_format,
            expected_cardinality_one=expected_cardinality_one,
            use_named_params=use_named_params,
            query_params=query_params,
            type_rewrites=type_rewrites,
            ignore_object_shapes=ignore_shapes,
            explicit_top_cast=explicit_top_cast,
            singleton_mode=singleton_mode,
            scope_tree_nodes=scope_tree_nodes,
            external_rvars=external_rvars,
            backend_runtime_params=backend_runtime_params,
        )

        ctx = context.CompilerContextLevel(
            None,
            context.ContextSwitchMode.TRANSPARENT,
            env=env,
            scope_tree=scope_tree,
        )
        ctx.rel = pgast.SelectStmt()

        _ = context.CompilerContext(initial=ctx)

        ctx.singleton_mode = singleton_mode
        ctx.expr_exposed = True
        for sing in singletons:
            ctx.path_scope[sing] = ctx.rel
        qtree = dispatch.compile(ir_expr, ctx=ctx)
        if isinstance(ir_expr, irast.Set) and not singleton_mode:
            assert isinstance(qtree, pgast.Query)
            clauses.fini_toplevel(qtree, ctx)

    except errors.EdgeDBError:
        # Don't wrap propertly typed EdgeDB errors into
        # InternalServerError; raise them as is.
        raise

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
    singleton_mode: bool=False,
    use_named_params: bool=False,
    expected_cardinality_one: bool=False,
    pretty: bool=True,
    backend_runtime_params: Optional[pgparams.BackendRuntimeParams]=None,
) -> Tuple[str, Dict[str, pgast.Param]]:

    qtree = compile_ir_to_sql_tree(
        ir_expr,
        output_format=output_format,
        ignore_shapes=ignore_shapes,
        explicit_top_cast=explicit_top_cast,
        singleton_mode=singleton_mode,
        use_named_params=use_named_params,
        expected_cardinality_one=expected_cardinality_one,
        backend_runtime_params=backend_runtime_params,
    )

    if (  # pragma: no cover
        debug.flags.edgeql_compile or debug.flags.edgeql_compile_sql_ast
    ):
        debug.header('SQL Tree')
        debug.dump(qtree)

    if isinstance(qtree, pgast.Query) and qtree.argnames:
        argmap = qtree.argnames
    else:
        argmap = {}

    # Generate query text
    codegen = _run_codegen(qtree, pretty=pretty)
    sql_text = ''.join(codegen.result)

    if (  # pragma: no cover
        debug.flags.edgeql_compile or debug.flags.edgeql_compile_sql_text
    ):
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


def new_external_rvar(
    *,
    rel_name: Tuple[str, ...],
    path_id: irast.PathId,
    outputs: Mapping[Tuple[irast.PathId, Tuple[str, ...]], str],
) -> pgast.RelRangeVar:
    """Construct a ``RangeVar`` instance given a relation name and a path id.

    Given an optionally-qualified relation name *rel_name* and a *path_id*,
    return a ``RangeVar`` instance over the specified relation that is
    then assumed to represent the *path_id* binding.

    This is useful in situations where it is necessary to "prime" the compiler
    with a list of external relations that exist in a larger SQL expression
    that _this_ expression is being embedded into.

    The *outputs* mapping optionally specifies a set of outputs in the
    resulting range var as a ``(path_id, tuple-of-aspects): attribute name``
    mapping.
    """
    if len(rel_name) == 1:
        table_name = rel_name[0]
        schema_name = None
    elif len(rel_name) == 2:
        schema_name, table_name = rel_name
    else:
        raise AssertionError(f'unexpected rvar name: {rel_name}')

    rel = pgast.Relation(
        name=table_name,
        schemaname=schema_name,
        path_id=path_id,
    )

    alias = pgast.Alias(aliasname=table_name)

    if not path_id.is_ptr_path():
        rvar = pgast.RelRangeVar(
            relation=rel, typeref=path_id.target, alias=alias)
    else:
        rvar = pgast.RelRangeVar(
            relation=rel, alias=alias)

    for (output_pid, output_aspects), colname in outputs.items():
        var = pgast.ColumnRef(name=[colname])
        for aspect in output_aspects:
            rel.path_outputs[output_pid, aspect] = var

    return rvar
