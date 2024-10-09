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

#
# Backend support for ext::ai::index
#
# The index adds the following hidden attribute to the object type relation
#
#    __ext_ai_{idx_id}_embedding__ vector(<index_embedding_dimensions>)
#
# The data in the attribute gets populated by an external indexing process,
# hence the ext::ai::index is currently always deferred.  If a given object
# record is yet unindexed, the attribute value would be NULL and the entry
# will be picked up in the work queue view (see below).
#
# To invalidate embeddings on changes of data referenced in the index
# expression changes, a simple trigger is also added, which resets the
# value of the embedding attribute back to NULL.
#
# The index is currently always deferred, hence the unindexed data
# needs to be exposed conveniently to an external indexer.  We do
# this here by creating the following internal SQL views:
#
# Enumeration of embedding models currently used in ext::ai::index()
# declarations in the current schema:
#
#   CREATE VIEW edgedbext.ai_active_embedding_models(
#     id,       -- generated unique id as int64 (could be used for locking)
#     name,     -- model name as specified in the ext::ai::model_name anno
#     provider, -- provider name as specified in the ext::ai::provider_name
#   )
#
# For each active model in the above view the following views are also
# generated:
#
#   CREATE VIEW edgedbext."ai_pending_embeddings_{model_name}"(
#     id,          -- Object ID
#     text,        -- Indexed text document (result of index expr eval)
#     target_rel,  -- SQL relation containing the embedding data
#     target_attr, -- Column in the above relation containing embedding data
#     target_dims_shortening -- If the embedding model produces more dimensions
#                            -- than the underlying index can handle, this
#                            -- would be the maximum dimensions supported by
#                            -- the index.  Embedding model must support
#                            -- vector shortening (e.g OpenAI
#                            -- embedding-text-3- models).
#  )
#
# The above view is a UNION of SELECTs over object relations, where each
# UNION element is roughly this:
#
#   SELECT (
#     Object.id,
#     eval(get_index_expr(Object, 'ext::ai::index')),
#   )
#   WHERE
#     eval(get_index_except_expr(Object, 'ext::ai::index')) IS NOT TRUE
#     AND Object.__ext_ai_{idx_id}_embedding__  IS NULL
#

from __future__ import annotations
from typing import (
    cast,
    Optional,
)

import collections
import dataclasses
import hashlib
import struct
import textwrap

from edb.schema import expr as s_expr
from edb.schema import indexes as s_indexes
from edb.schema import types as s_types
from edb.schema import schema as s_schema
from edb.schema import delta as sd
from edb.schema import name as sn
from edb.schema import properties as s_props

from edb.ir import ast as irast

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from . import codegen
from . import common
from . import dbops
from . import compiler
from . import types
from . import ast as pgast

from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .compiler import astutils
from .compiler import enums as pgce


ai_index_base_name = sn.QualName("ext::ai", "index")


def get_ext_ai_pre_restore_script(
    schema: s_schema.Schema,
) -> str:
    # We helpfully populate ext::ai::ChatPrompt with a starter prompt
    # in the extension setup script.
    # Unfortunately, this means that before user data is restored, we need
    # to delete those objects, or there will be a constraint error.
    return '''
        delete {ext::ai::ChatPrompt, ext::ai::ChatPromptMessage}
    '''


def create_ext_ai_index(
    index: s_indexes.Index,
    predicate_src: Optional[str],
    sql_kwarg_exprs: dict[str, str],
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    subject = index.get_subject(schema)
    assert isinstance(subject, s_indexes.IndexableSubject)

    effective, has_overridden = s_indexes.get_effective_object_index(
        schema, subject, ai_index_base_name
    )

    if index != effective:
        return dbops.CommandGroup()

    # When creating an index on a child that already has an ext::ai index
    # inherited from the parent, we don't need to create the index, but just
    # update the populating expressions.
    if has_overridden:
        return _refresh_ai_embeddings(
            index,
            options,
            schema,
            context,
        )
    else:
        return _create_ai_embeddings(
            index,
            predicate_src,
            sql_kwarg_exprs,
            options,
            schema,
            context,
        )


def delete_ext_ai_index(
    index: s_indexes.Index,
    drop_index: dbops.Command,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    orig_schema: s_schema.Schema,
    context: sd.CommandContext,
) -> tuple[dbops.Command, dbops.Command]:
    subject = index.get_subject(orig_schema)
    assert isinstance(subject, s_indexes.IndexableSubject)

    effective, _ = s_indexes.get_effective_object_index(
        schema, subject, ai_index_base_name
    )

    if not effective:
        return _delete_ai_embeddings(
            index, drop_index, schema, orig_schema, context)
    else:
        # effective index remains: don't drop the embeddings
        return dbops.CommandGroup(), dbops.CommandGroup()


def _compile_ai_embeddings_source_view_expr(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
) -> pgast.SelectStmt:
    # Compile a view returning a set of (id, text-to-embed) tuples
    # roughly as the following pseudo-QL
    #
    # SELECT (
    #   Object.id,
    #   eval(get_index_expr(Object, 'ext::ai::index')),
    # )
    # WHERE
    #   eval(get_index_except_expr(Object, 'ext::ai::index')) IS NOT TRUE
    #   AND Object.embedding_column IS NULL
    index_sexpr: Optional[s_expr.Expression] = index.get_expr(schema)
    assert index_sexpr
    ql = qlast.SelectQuery(
        result=qlast.Tuple(
            elements=[
                qlast.Path(
                    steps=[qlast.Ptr(name="id")],
                    partial=True,
                ),
                index_sexpr.parse(),
            ],
        ),
    )

    my_options = dataclasses.replace(options, singletons=frozenset())
    ir = qlcompiler.compile_ast_to_ir(
        ql,
        schema=schema,
        options=my_options,
    )
    assert isinstance(ir, irast.Statement)

    subject = index.get_subject(schema)
    assert isinstance(subject, s_types.Type)
    subject_id = irast.PathId.from_type(schema, subject, env=None)

    idx_id = _get_index_root_id(schema, index)
    table_name = common.get_index_table_backend_name(index, schema)
    aspects = (
        pgce.PathAspect.IDENTITY,
        pgce.PathAspect.VALUE,
        pgce.PathAspect.SOURCE
    )
    qry = compiler.new_external_rvar_as_subquery(
        rel_name=table_name,
        path_id=subject_id,
        aspects=aspects,
    )
    qry.where_clause = astutils.extend_binop(
        qry.where_clause,
        pgast.NullTest(
            arg=pgast.ColumnRef(
                name=(f"__ext_ai_{idx_id}_embedding__",),
            ),
        )
    )

    except_expr = index.get_except_expr(schema)
    if except_expr:
        except_expr = except_expr.ensure_compiled(
            schema=schema,
            options=options,
            context=None,
        )
        assert except_expr.irast
        except_res = compiler.compile_ir_to_sql_tree(
            except_expr.irast.expr, singleton_mode=True)
        assert isinstance(except_res.ast, pgast.BaseExpr)
        qry.where_clause = astutils.extend_binop(
            qry.where_clause,
            pgast.Expr(
                lexpr=except_res.ast,
                name="IS NOT",
                rexpr=pgast.BooleanConstant(val=True),
            ),
        )

    sql_res = compiler.compile_ir_to_sql_tree(
        ir,
        output_format=compiler.OutputFormat.NATIVE_INTERNAL,
        external_rels={
            subject_id: (qry, aspects),
        },
    )
    expr = sql_res.ast
    assert isinstance(expr, pgast.SelectStmt)

    return expr


def _create_ai_embeddings(
    index: s_indexes.Index,
    predicate_src: Optional[str],
    sql_kwarg_exprs: dict[str, str],
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    return _pg_create_ai_embeddings(
        index,
        options,
        predicate_src,
        sql_kwarg_exprs,
        schema,
        context,
    )


def _refresh_ai_embeddings(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    ops = dbops.CommandGroup()
    table_name = common.get_index_table_backend_name(index, schema)
    ops.add_command(
        _pg_drop_trigger(index, table_name, schema))

    idx_id = _get_index_root_id(schema, index)
    ops.add_command(dbops.Query(textwrap.dedent(f"""\
        UPDATE {common.qname(*table_name)}
        SET __ext_ai_{idx_id}_embedding__ = NULL
        WHERE __ext_ai_{idx_id}_embedding__ IS NOT NULL
    """)))

    ops.add_command(
        _pg_create_trigger(index, table_name, schema))
    ops.add_command(
        _pg_create_ai_embeddings_source_view(
            index, options, schema, context))
    return ops


def _delete_ai_embeddings(
    index: s_indexes.Index,
    drop_index: dbops.Command,
    schema: s_schema.Schema,
    orig_schema: s_schema.Schema,
    context: sd.CommandContext,
) -> tuple[dbops.Command, dbops.Command]:
    return _pg_delete_ai_embeddings(
        index, drop_index, schema, orig_schema, context
    )


# --- pgvector ---


def _pg_create_ai_embeddings(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    predicate_src: Optional[str],
    sql_kwarg_exprs: dict[str, str],
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    # Create:
    # * the "__ext_ai_{idx_id}_embedding__" vector attribute;
    # * pgvector index on the above;
    # * the embedding attribute invalidation trigger
    # * a component view for the "ai_pending_embeddings_{model_name}" union
    ops = dbops.CommandGroup()

    table_name = common.get_index_table_backend_name(index, schema)

    with_clause = {}
    kwargs = index.get_concrete_kwargs(schema)
    index_params_expr = kwargs.get("index_parameters")
    if index_params_expr is not None:
        index_params = index_params_expr.assert_compiled().as_python_value()
        with_clause["m"] = index_params["m"]
        with_clause["ef_construction"] = index_params["ef_construction"]

    dimensions = index.must_get_json_annotation(
        schema,
        sn.QualName("ext::ai", "embedding_dimensions"),
        int,
    )

    idx_id = _get_index_root_id(schema, index)

    alter_table = dbops.AlterTable(table_name)

    # The attribute
    alter_table.add_operation(
        dbops.AlterTableAddColumn(
            dbops.Column(
                name=f'__ext_ai_{idx_id}_embedding__',
                type=f'edgedb.vector({dimensions})',
                required=False,
            )
        )
    )

    ops.add_command(alter_table)

    # Also create a constant partial index on outdated entries
    # so that we use an index scan and not a seq scan when
    # picking out pending embeddings.
    outdated_idx_name = common.get_index_table_backend_name(
        index, schema, aspect="extaiselidx")

    ops.add_command(
        dbops.CreateIndex(
            dbops.Index(
                name=outdated_idx_name[1],
                table_name=table_name,
                exprs=["(1)"],
                predicate=(
                    f'__ext_ai_{idx_id}_embedding__ IS NULL'),
                unique=False,
                metadata={
                    'code': '(__col__)',
                },
            ),
        ),
    )

    df_expr = kwargs.get("distance_function")
    if df_expr is not None:
        df = df_expr.assert_compiled().as_python_value()
    else:
        df = "Cosine"

    match df:
        case "Cosine":
            opclass = "vector_cosine_ops"
        case "InnerProduct":
            opclass = "vector_ip_ops"
        case "L2":
            opclass = "vector_l2_ops"
        case _:
            raise RuntimeError(f"unsupported distance_function: {df}")

    # The main similarity (a.k.a distance) search index.
    module_name = index.get_name(schema).module
    index_name = common.get_index_backend_name(
        index.id, module_name, catenate=False, aspect=f'{dimensions}_index'
    )

    pg_index = dbops.Index(
        name=index_name[1],
        table_name=table_name,
        exprs=[f"__ext_ai_{idx_id}_embedding__"],
        with_clause=with_clause,
        unique=False,
        predicate=predicate_src,
        metadata={
            'schemaname': str(index.get_name(schema)),
            'kwargs': sql_kwarg_exprs,
            'code': f'hnsw (__col__ {opclass})',
            'dimensions': str(dimensions),
            'distance_function': str(df),
        },
    )
    ops.add_command(dbops.CreateIndex(pg_index))

    # The invalidation trigger
    ops.add_command(_pg_create_trigger(index, table_name, schema))

    # The component view for the "ai_pending_embeddings_{model_name}" union
    ops.add_command(
        _pg_create_ai_embeddings_source_view(index, options, schema, context))

    return ops


def _get_dep_cols(
    index: s_indexes.Index,
    schema: s_schema.Schema,
) -> list[str]:
    index_expr = index.get_expr(schema)
    assert index_expr is not None
    dep_cols = []
    assert index_expr.refs is not None
    for obj in index_expr.refs.objects(schema):
        if isinstance(obj, s_props.Property):
            ptrinfo = types.get_pointer_storage_info(obj, schema=schema)
            dep_cols.append(ptrinfo.column_name)

    return dep_cols


def _pg_delete_ai_embeddings(
    index: s_indexes.Index,
    drop_index: dbops.Command,
    schema: s_schema.Schema,
    orig_schema: s_schema.Schema,
    context: sd.CommandContext,
) -> tuple[dbops.Command, dbops.Command]:
    table_name = common.get_index_table_backend_name(index, orig_schema)
    idx_id = _get_index_root_id(orig_schema, index)

    table_ops = dbops.CommandGroup()

    ops = dbops.CommandGroup()
    ops.add_command(drop_index)

    # Drop the invalidation trigger
    ops.add_command(_pg_drop_trigger(index, table_name, orig_schema))
    # Drop component view for the "ai_pending_embeddings_{model_name}" union
    ops.add_command(_pg_drop_ai_embeddings_source_view(
        index, schema, orig_schema, context))

    # When the ObjectType is being deleted, we don't drop the index,
    # as it will get dropped with the parent table.
    # The same goes for __ext_ai_{idx_id}_embedding__.
    source_drop = isinstance(drop_index, dbops.NoOpCommand)
    if not source_drop:
        table_name = common.get_index_table_backend_name(index, orig_schema)

        dimensions = index.must_get_json_annotation(
            orig_schema,
            sn.QualName("ext::ai", "embedding_dimensions"),
            int,
        )

        alter_table = dbops.AlterTable(table_name)

        alter_table.add_operation(
            dbops.AlterTableDropColumn(
                dbops.Column(
                    name=f'__ext_ai_{idx_id}_embedding__',
                    type=('edgedb', f'vector({dimensions})'),
                )
            )
        )

        table_ops.add_command(alter_table)

    return ops, table_ops


def _pg_create_trigger(
    index: s_indexes.Index,
    table_name: tuple[str, str],
    schema: s_schema.Schema,
) -> dbops.Command:
    dep_cols = _get_dep_cols(index, schema)

    # Create a trigger that resets the __ext_ai_{idx_id}_embedding__ to
    # NULL whenever data referenced in the ext::ai::index expression gets
    # modified (TODO: the selective approach could also be used on
    # std::fts::index)
    ops = dbops.CommandGroup()
    idx_id = _get_index_root_id(schema, index)

    # create update function
    func_name = _pg_update_func_name(table_name, idx_id)
    function = dbops.Function(
        name=func_name,
        text=f"""
        BEGIN
            NEW."__ext_ai_{idx_id}_embedding__" := NULL;
            RETURN NEW;
        END;
        """,
        volatility='immutable',
        returns='trigger',
        language='plpgsql',
    )
    ops.add_command(dbops.CreateFunction(function))

    conditions = []
    for dep_col in dep_cols:
        dep_col = qi(dep_col)
        conditions.append(f'OLD.{dep_col} IS DISTINCT FROM NEW.{dep_col}')

    trigger_name = _pg_trigger_name(table_name[1], idx_id)
    trigger = dbops.Trigger(
        name=trigger_name,
        table_name=table_name,
        events=('update',),
        timing=dbops.TriggerTiming.Before,
        procedure=func_name,
        condition=' OR '.join(conditions),
    )
    ops.add_command(dbops.CreateTrigger(trigger))
    return ops


def _pg_drop_trigger(
    index: s_indexes.Index,
    table_name: tuple[str, str],
    schema: s_schema.Schema,
    override_id: Optional[str] = None,
) -> dbops.Command:
    idx_id = override_id or _get_index_root_id(schema, index)
    ops = dbops.CommandGroup()

    ops.add_command(
        dbops.DropTrigger(
            dbops.Trigger(
                _pg_trigger_name(table_name[1], idx_id),
                table_name=table_name,
                events=(),
                procedure='',
            )
        )
    )

    ops.add_command(
        dbops.DropFunction(
            _pg_update_func_name(table_name, idx_id),
            (),
        )
    )
    return ops


def pg_rebuild_all_pending_embeddings_views(
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    def flt(schema: s_schema.Schema, index: s_indexes.Index) -> bool:
        return (
            index.get_subject(schema) is not None
            and s_indexes.is_ext_ai_index(schema, index)
        )

    all_ai_indexes = schema.get_objects(
        type=s_indexes.Index,
        extra_filters=(flt,),
    )

    all_models = s_indexes.get_defined_ext_ai_embedding_models(schema)

    used_models = collections.defaultdict(list)
    for other_index in all_ai_indexes:
        if context.is_deleting(other_index):
            continue

        tabname = common.get_index_table_backend_name(
            other_index, schema, aspect="extaiview")
        model_name = other_index.must_get_annotation(
            schema, sn.QualName("ext::ai", "model_name"))
        used_models[model_name].append(f"SELECT * FROM {q(*tabname)}")

    model_providers = {}

    for model_name, model_stype in all_models.items():
        views = used_models.get(model_name)

        if views:
            query = " UNION ALL ".join(views)
        else:
            query = textwrap.dedent("""\
                SELECT
                    NULL::uuid    AS "id",
                    NULL::text    AS "text",
                    NULL::text    AS "target_rel",
                    NULL::text    AS "target_attr",
                    NULL::int     AS "target_dims_shortening",
                    NULL::boolean AS "truncate_to_max"
                WHERE
                    FALSE
            """)

        view = dbops.View(
            name=(
                "edgedbext",
                common.edgedb_name_to_pg_name(
                    f"ai_pending_embeddings_{model_name}"
                ),
            ),
            query=query,
        )
        ops.add_command(dbops.CreateView(view, or_replace=True))

        provider = model_stype.must_get_annotation(
            schema, sn.QualName("ext::ai", "model_provider"))
        model_providers[model_name] = provider

    if used_models:
        bits = []
        for model_name in used_models:
            mnhash = hashlib.blake2b(model_name.encode("utf-8"), digest_size=8)
            model_id: int = struct.unpack("q", mnhash.digest())[0]

            provider = model_providers[model_name]
            bits.append(textwrap.dedent(f"""\
                SELECT
                    {model_id}::bigint AS id,
                    {ql(model_name)} AS name,
                    {ql(provider)} AS provider
            """))
        used_sql = " UNION ALL ".join(bits)
    else:
        used_sql = textwrap.dedent("""\
            SELECT
                NULL::bigint AS id,
                NULL::text AS name,
                NULL::text AS provider
            WHERE
                FALSE
        """)

    ops.add_command(dbops.CreateView(
        view=dbops.View(
            name=("edgedbext", "ai_active_embedding_models"),
            query=used_sql,
        ),
        or_replace=True,
    ))

    return ops


def pg_drop_all_pending_embeddings_views(
    schema: s_schema.Schema,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    all_models = s_indexes.get_defined_ext_ai_embedding_models(schema)
    for model_name in all_models:
        view_name = (
            "edgedbext",
            common.edgedb_name_to_pg_name(
                f"ai_pending_embeddings_{model_name}"
            ),
        )
        ops.add_command(dbops.DropView(view_name, conditional=True))

    ops.add_command(
        dbops.DropView(("edgedbext", "ai_active_embedding_models")))

    return ops


def _pg_create_ai_embeddings_source_view(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
    *,
    rebuild_all: bool=True,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    expr = _compile_ai_embeddings_source_view_expr(index, options, schema)
    view_name = common.get_index_table_backend_name(
        index, schema, aspect="extaiview")

    idx_id = _get_index_root_id(schema, index)
    target_col = f"__ext_ai_{idx_id}_embedding__"
    index_dimensions = index.must_get_json_annotation(
        schema,
        sn.QualName("ext::ai", "embedding_dimensions"),
        int,
    )
    model_dimensions = index.must_get_json_annotation(
        schema,
        sn.QualName("ext::ai", "embedding_model_max_output_dimensions"),
        int,
    )

    if index_dimensions < model_dimensions:
        target_dims_shortening = str(index_dimensions)
    else:
        target_dims_shortening = "NULL"

    kwargs = index.get_concrete_kwargs(schema)
    truncate_to_max_arg = kwargs.get("truncate_to_max")
    if truncate_to_max_arg is not None:
        truncate_to_max = cast(
            bool,
            truncate_to_max_arg.assert_compiled().as_python_value()
        )
    else:
        truncate_to_max = False

    table_name = common.get_index_table_backend_name(index, schema)
    expr_sql = codegen.generate_source(expr)
    document_sql = textwrap.dedent(f"""\
        SELECT
            (q.val).f1 AS "id",
            (q.val).f2 AS "text",
            {ql(q(*table_name))} AS "target_rel",
            {ql(qi(target_col))} AS "target_attr",
            {target_dims_shortening}::int AS "target_dims_shortening",
            {truncate_to_max}::boolean AS "truncate_to_max"
        FROM
            ({expr_sql}) AS q(val)
    """)

    view = dbops.View(name=view_name, query=document_sql)
    ops.add_command(dbops.CreateView(view, or_replace=True))
    if rebuild_all:
        ops.add_command(
            pg_rebuild_all_pending_embeddings_views(schema, context))

    return ops


def _pg_drop_ai_embeddings_source_view(
    index: s_indexes.Index,
    schema: s_schema.Schema,
    orig_schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    ops.add_command(pg_rebuild_all_pending_embeddings_views(
        schema, context
    ))

    view_name = common.get_index_table_backend_name(
        index, orig_schema, aspect="extaiview")
    ops.add_command(dbops.DropView(view_name))

    return ops


def _pg_update_func_name(
    tbl_name: tuple[str, str],
    idx_id: str,
) -> tuple[str, ...]:
    return (
        tbl_name[0],
        common.edgedb_name_to_pg_name(tbl_name[1] + f'_extai_{idx_id}_upd'),
    )


def _pg_trigger_name(
    tbl_name: str,
    idx_id: str,
) -> str:
    return common.edgedb_name_to_pg_name(tbl_name + f'_extai_{idx_id}_trg')


def _get_index_root_id(
    schema: s_schema.Schema,
    index: s_indexes.Index,
) -> str:
    return s_indexes.get_ai_index_id(schema, index)
