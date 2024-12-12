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

from typing import Optional, Tuple, Mapping, Dict, List, TYPE_CHECKING
from dataclasses import dataclass

from edb import errors

from edb.ir import ast as irast

from edb.pgsql import ast as pgast
from edb.pgsql import params as pgparams
from edb.pgsql import types as pgtypes

from . import config as _config_compiler  # NOQA
from . import expr as _expr_compiler  # NOQA
from . import stmt as _stmt_compiler  # NOQA

from . import clauses
from . import context
from . import dispatch
from . import dml
from . import pathctx
from . import aliases

from .context import OutputFormat as OutputFormat # NOQA

if TYPE_CHECKING:
    import enums as pgce


@dataclass(kw_only=True, slots=True, repr=False, eq=False, frozen=True)
class CompileResult:
    ast: pgast.Base

    env: context.Environment

    argmap: Dict[str, pgast.Param]

    detached_params: Optional[List[Tuple[str, ...]]] = None


def compile_ir_to_sql_tree(
    ir_expr: irast.Base,
    *,
    output_format: Optional[OutputFormat] = None,
    ignore_shapes: bool = False,
    explicit_top_cast: Optional[irast.TypeRef] = None,
    singleton_mode: bool = False,
    named_param_prefix: Optional[tuple[str, ...]] = None,
    expected_cardinality_one: bool = False,
    is_explain: bool = False,
    external_rvars: Optional[
        Mapping[Tuple[irast.PathId, pgce.PathAspect], pgast.PathRangeVar]
    ] = None,
    external_rels: Optional[
        Mapping[
            irast.PathId,
            Tuple[
                pgast.BaseRelation | pgast.CommonTableExpr,
                Tuple[pgce.PathAspect, ...]
            ],
        ]
    ] = None,
    backend_runtime_params: Optional[pgparams.BackendRuntimeParams]=None,
    detach_params: bool = False,
    alias_generator: Optional[aliases.AliasGenerator] = None,
    versioned_stdlib: bool = True,
    # HACK?
    versioned_singleton: bool = False,
) -> CompileResult:
    if singleton_mode and not versioned_singleton:
        versioned_stdlib = False

    try:
        # Transform to sql tree
        query_params = []
        query_globals = []
        type_rewrites = {}
        triggers: tuple[tuple[irast.Trigger, ...], ...] = ()

        singletons = []
        if isinstance(ir_expr, irast.Statement):
            scope_tree = ir_expr.scope_tree
            query_params = list(ir_expr.params)
            query_globals = list(ir_expr.globals)
            type_rewrites = ir_expr.type_rewrites
            singletons = ir_expr.singletons
            triggers = ir_expr.triggers
            ir_expr = ir_expr.expr
        elif isinstance(ir_expr, irast.ConfigCommand):
            assert ir_expr.scope_tree
            scope_tree = ir_expr.scope_tree
            query_params = list(ir_expr.params)
            if ir_expr.globals:
                query_globals = list(ir_expr.globals)
        else:
            scope_tree = irast.new_scope_tree()

        scope_tree_nodes = {
            node.unique_id: node for node in scope_tree.descendants
            if node.unique_id is not None
        }

        if backend_runtime_params is None:
            backend_runtime_params = pgparams.get_default_runtime_params()

        env = context.Environment(
            alias_generator=alias_generator,
            output_format=output_format,
            expected_cardinality_one=expected_cardinality_one,
            named_param_prefix=named_param_prefix,
            query_params=list(tuple(query_params) + tuple(query_globals)),
            type_rewrites=type_rewrites,
            ignore_object_shapes=ignore_shapes,
            explicit_top_cast=explicit_top_cast,
            is_explain=is_explain,
            singleton_mode=singleton_mode,
            scope_tree_nodes=scope_tree_nodes,
            external_rvars=external_rvars,
            backend_runtime_params=backend_runtime_params,
            versioned_stdlib=versioned_stdlib,
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
        if external_rels:
            ctx.external_rels = external_rels
        clauses.populate_argmap(query_params, query_globals, ctx=ctx)

        qtree = dispatch.compile(ir_expr, ctx=ctx)
        dml.compile_triggers(triggers, qtree, ctx=ctx)

        if not singleton_mode:
            if isinstance(ir_expr, irast.Set):
                assert isinstance(qtree, pgast.Query)
                clauses.fini_toplevel(qtree, ctx)

            elif isinstance(qtree, pgast.Query):
                # Other types of expressions may compile to queries which may
                # use inheritance CTEs. Ensure they are added here.
                clauses.insert_ctes(qtree, ctx)

        if detach_params:
            detached_params_idx = {
                ctx.argmap[param.name].index: (
                    pgtypes.pg_type_from_ir_typeref(
                        param.ir_type.base_type or param.ir_type,
                        # Needs serialized=True so types without their own
                        # binary encodings (like postgis::box2d) get mapped
                        # to the real underlying type.
                        serialized=True,
                    )
                )
                for param in ctx.env.query_params
                if not param.sub_params
            }
        else:
            detached_params_idx = {}
        detached_params = [p for _, p in sorted(detached_params_idx.items())]

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

    return CompileResult(
        ast=qtree, env=env, argmap=ctx.argmap, detached_params=detached_params
    )


def new_external_rvar(
    *,
    rel_name: Tuple[str, ...],
    path_id: irast.PathId,
    outputs: Mapping[Tuple[irast.PathId, Tuple[pgce.PathAspect, ...]], str],
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
    rel = new_external_rel(rel_name=rel_name, path_id=path_id)
    assert rel.name

    alias = pgast.Alias(aliasname=rel.name)

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


def new_external_rvar_as_subquery(
    *,
    rel_name: tuple[str, ...],
    path_id: irast.PathId,
    aspects: tuple[pgce.PathAspect, ...],
) -> pgast.SelectStmt:
    rvar = new_external_rvar(
        rel_name=rel_name,
        path_id=path_id,
        outputs={},
    )
    qry = pgast.SelectStmt(
        from_clause=[rvar],
    )
    for aspect in aspects:
        pathctx.put_path_rvar(qry, path_id, rvar, aspect=aspect)
    return qry


def new_external_rel(
    *,
    rel_name: Tuple[str, ...],
    path_id: irast.PathId,
) -> pgast.Relation:
    if len(rel_name) == 1:
        table_name = rel_name[0]
        schema_name = None
    elif len(rel_name) == 2:
        schema_name, table_name = rel_name
    else:
        raise AssertionError(f'unexpected rvar name: {rel_name}')

    return pgast.Relation(
        name=table_name,
        schemaname=schema_name,
        path_id=path_id,
    )
