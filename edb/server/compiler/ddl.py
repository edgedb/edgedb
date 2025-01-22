#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Optional, Tuple, Dict, List, FrozenSet

import dataclasses
import json
import textwrap

from edb import errors

from edb import edgeql
from edb.common import debug
from edb.common import ast
from edb.common import uuidgen

from edb.edgeql import ast as qlast
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote


from edb.schema import annos as s_annos
from edb.schema import constraints as s_constraints
from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import expraliases as s_expraliases
from edb.schema import functions as s_func
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import migrations as s_migrations
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import policies as s_policies
from edb.schema import pointers as s_pointers
from edb.schema import properties as s_properties
from edb.schema import rewrites as s_rewrites
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import triggers as s_triggers
from edb.schema import utils as s_utils
from edb.schema import version as s_ver

from edb.pgsql import common as pg_common
from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops

from . import dbstate
from . import compiler


NIL_QUERY = b"SELECT LIMIT 0"


def compile_and_apply_ddl_stmt(
    ctx: compiler.CompileContext,
    stmt: qlast.DDLCommand,
    source: Optional[edgeql.Source] = None,
) -> dbstate.DDLQuery:
    query, _ = _compile_and_apply_ddl_stmt(ctx, stmt, source)
    return query


def _compile_and_apply_ddl_stmt(
    ctx: compiler.CompileContext,
    stmt: qlast.DDLCommand,
    source: Optional[edgeql.Source] = None,
) -> tuple[dbstate.DDLQuery, Optional[pg_dbops.SQLBlock]]:
    if isinstance(stmt, qlast.GlobalObjectCommand):
        ctx._assert_not_in_migration_block(stmt)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    mstate = current_tx.get_migration_state()
    if (
        mstate is None
        and not ctx.bootstrap_mode
        and ctx.log_ddl_as_migrations
        and not isinstance(
            stmt,
            (
                qlast.CreateMigration,
                qlast.GlobalObjectCommand,
                qlast.DropMigration,
            ),
        )
    ):
        allow_bare_ddl = compiler._get_config_val(ctx, 'allow_bare_ddl')
        if allow_bare_ddl != "AlwaysAllow":
            raise errors.QueryError(
                "bare DDL statements are not allowed on this database branch",
                hint="Use the migration commands instead.",
                details=(
                    f"The `allow_bare_ddl` configuration variable "
                    f"is set to {str(allow_bare_ddl)!r}.  The "
                    f"`edgedb migrate` command normally sets this "
                    f"to avoid accidental schema changes outside of "
                    f"the migration flow."
                ),
                span=stmt.span,
            )
        cm = qlast.CreateMigration(  # type: ignore
            body=qlast.NestedQLBlock(
                commands=[stmt],
            ),
            commands=[
                qlast.SetField(
                    name='generated_by',
                    value=qlast.Path(
                        steps=[
                            qlast.ObjectRef(
                                name='MigrationGeneratedBy', module='schema'
                            ),
                            qlast.Ptr(name='DDLStatement'),
                        ]
                    ),
                )
            ],
        )
        return _compile_and_apply_ddl_stmt(ctx, cm)

    assert isinstance(stmt, qlast.DDLCommand)
    new_schema, delta = s_ddl.delta_and_schema_from_ddl(
        stmt,
        schema=schema,
        modaliases=current_tx.get_modaliases(),
        **_get_delta_context_args(ctx),
    )

    if debug.flags.delta_plan:
        debug.header('Canonical Delta Plan')
        debug.dump(delta, schema=schema)

    if mstate := current_tx.get_migration_state():
        mstate = mstate._replace(
            accepted_cmds=mstate.accepted_cmds + (stmt,),
        )

        last_proposed = mstate.last_proposed
        if last_proposed:
            if last_proposed[0].required_user_input or last_proposed[
                0
            ].prompt_id.startswith("Rename"):
                # Cannot auto-apply the proposed DDL
                # if user input is required.
                # Also skip auto-applying for renames, since
                # renames often force a bunch of rethinking.
                mstate = mstate._replace(last_proposed=None)
            else:
                proposed_stmts = last_proposed[0].statements
                ddl_script = '\n'.join(proposed_stmts)

                if source and source.text() == ddl_script:
                    # The client has confirmed the proposed migration step,
                    # advance the proposed script.
                    mstate = mstate._replace(
                        last_proposed=last_proposed[1:],
                    )
                else:
                    # The client replied with a statement that does not
                    # match what was proposed, reset the proposed script
                    # to force script regeneration on next DESCRIBE.
                    mstate = mstate._replace(last_proposed=None)

        current_tx.update_migration_state(mstate)
        current_tx.update_schema(new_schema)

        query = dbstate.DDLQuery(
            sql=NIL_QUERY,
            user_schema=current_tx.get_user_schema(),
            is_transactional=True,
            warnings=tuple(delta.warnings),
            feature_used_metrics=None,
        )

        return query, None

    store_migration_sdl = compiler._get_config_val(ctx, 'store_migration_sdl')
    if (
        isinstance(stmt, qlast.CreateMigration)
        and store_migration_sdl == 'AlwaysStore'
    ):
        stmt.target_sdl = s_ddl.sdl_text_from_schema(new_schema)

    # If we are in a migration rewrite, we also don't actually
    # apply the DDL, just record it. (The DDL also needs to be a
    # CreateMigration.)
    if mrstate := current_tx.get_migration_rewrite_state():
        if not isinstance(stmt, qlast.CreateMigration):
            # This will always fail, and gives us the error we need
            ctx._assert_not_in_migration_rewrite_block(stmt)
            # Tell this to the type checker
            raise AssertionError()

        mrstate = mrstate._replace(
            accepted_migrations=(mrstate.accepted_migrations + (stmt,))
        )
        current_tx.update_migration_rewrite_state(mrstate)

        current_tx.update_schema(new_schema)

        query = dbstate.DDLQuery(
            sql=NIL_QUERY,
            user_schema=current_tx.get_user_schema(),
            is_transactional=True,
            warnings=tuple(delta.warnings),
            feature_used_metrics=None,
        )

        return query, None

    # Apply and adapt delta, build native delta plan, which
    # will also update the schema.
    block, new_types, config_ops = _process_delta(ctx, delta)

    ddl_stmt_id: Optional[str] = None
    is_transactional = block.is_transactional()
    if not is_transactional:
        if not isinstance(stmt, qlast.DatabaseCommand):
            raise AssertionError(
                f"unexpected non-transaction DDL command type: {stmt}")
        sql_stmts = block.get_statements()
        sql = sql_stmts[0].encode("utf-8")
        db_op_trailer = tuple(stmt.encode("utf-8") for stmt in sql_stmts[1:])
    else:
        if new_types:
            # Inject a query returning backend OIDs for the newly
            # created types.
            ddl_stmt_id = str(uuidgen.uuid1mc())
            new_type_ids = [
                f'{pg_common.quote_literal(tid)}::uuid' for tid in new_types
            ]
            # Return newly-added type id mapping via the indirect
            # return channel (see PGConnection.last_indirect_return)
            new_types_sql = textwrap.dedent(f"""\
                PERFORM edgedb.indirect_return(
                    json_build_object(
                        'ddl_stmt_id',
                        {pg_common.quote_literal(ddl_stmt_id)},
                        'new_types',
                        (SELECT
                            json_object_agg(
                                "id"::text,
                                json_build_array("backend_id", "name")
                            )
                            FROM
                            edgedb_VER."_SchemaType"
                            WHERE
                                "id" = any(ARRAY[
                                    {', '.join(new_type_ids)}
                                ])
                        )
                    )::text
                )"""
            )

            block.add_command(pg_dbops.Query(text=new_types_sql).code())

        sql = block.to_string().encode('utf-8')
        db_op_trailer = ()

    create_db = None
    drop_db = None
    drop_db_reset_connections = False
    create_db_template = None
    create_db_mode = None
    if isinstance(stmt, qlast.DropDatabase):
        drop_db = stmt.name.name
        drop_db_reset_connections = stmt.force
    elif isinstance(stmt, qlast.CreateDatabase):
        create_db = stmt.name.name
        create_db_template = stmt.template.name if stmt.template else None
        create_db_mode = stmt.branch_type
    elif isinstance(stmt, qlast.AlterDatabase):
        for cmd in stmt.commands:
            if isinstance(cmd, qlast.Rename):
                drop_db = stmt.name.name
                create_db = cmd.new_name.name
                drop_db_reset_connections = stmt.force

    if debug.flags.delta_execute_ddl:
        debug.header('Delta Script (DDL Only)')
        # The schema updates are always the last statement, so grab
        # everything but
        code = '\n\n'.join(block.get_statements()[:-1])
        debug.dump_code(code, lexer='sql')
    if debug.flags.delta_execute:
        debug.header('Delta Script')
        debug.dump_code(sql + b"\n".join(db_op_trailer), lexer='sql')

    new_user_schema = current_tx.get_user_schema_if_updated()
    query = dbstate.DDLQuery(
        sql=sql,
        is_transactional=is_transactional,
        create_db=create_db,
        drop_db=drop_db,
        drop_db_reset_connections=drop_db_reset_connections,
        create_db_template=create_db_template,
        create_db_mode=create_db_mode,
        db_op_trailer=db_op_trailer,
        ddl_stmt_id=ddl_stmt_id,
        user_schema=new_user_schema,
        cached_reflection=current_tx.get_cached_reflection_if_updated(),
        global_schema=current_tx.get_global_schema_if_updated(),
        config_ops=config_ops,
        warnings=tuple(delta.warnings),
        feature_used_metrics=(
            produce_feature_used_metrics(ctx.compiler_state, new_user_schema)
            if new_user_schema else None
        ),
    )

    return query, block


def _new_delta_context(
    ctx: compiler.CompileContext, args: Any = None
) -> s_delta.CommandContext:
    return s_delta.CommandContext(
        backend_runtime_params=ctx.compiler_state.backend_runtime_params,
        internal_schema_mode=ctx.internal_schema_mode,
        **(_get_delta_context_args(ctx) if args is None else args),
    )


def _get_delta_context_args(ctx: compiler.CompileContext) -> dict[str, Any]:
    """Get the args needed for delta_and_schema_from_ddl"""
    return dict(
        stdmode=ctx.bootstrap_mode,
        testmode=ctx.is_testmode(),
        store_migration_sdl=(
            compiler._get_config_val(ctx, 'store_migration_sdl')
        ) == 'AlwaysStore',
        schema_object_ids=ctx.schema_object_ids,
        compat_ver=ctx.compat_ver,
    )


def _process_delta(
    ctx: compiler.CompileContext, delta: s_delta.DeltaRoot
) -> tuple[pg_dbops.SQLBlock, FrozenSet[str], Any]:
    """Adapt and process the delta command."""

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    pgdelta = pg_delta.CommandMeta.adapt(delta)
    assert isinstance(pgdelta, pg_delta.DeltaRoot)
    context = _new_delta_context(ctx)
    schema = pgdelta.apply(schema, context)
    current_tx.update_schema(schema)

    if debug.flags.delta_pgsql_plan:
        debug.header('PgSQL Delta Plan')
        debug.dump(pgdelta, schema=schema)

    db_cmd = any(
        isinstance(c, s_db.BranchCommand) for c in pgdelta.get_subcommands()
    )

    if db_cmd:
        block = pg_dbops.SQLBlock()
        new_types: FrozenSet[str] = frozenset()
    else:
        block = pg_dbops.PLTopBlock()
        new_types = frozenset(str(tid) for tid in pgdelta.new_types)

    # Generate SQL DDL for the delta.
    pgdelta.generate(block)  # type: ignore
    # XXX: We would prefer for there to not be trampolines ever after bootstrap
    pgdelta.create_trampolines.generate(block)  # type: ignore

    # Generate schema storage SQL (DML into schema storage tables).
    subblock = block.add_block()
    compiler.compile_schema_storage_in_delta(
        ctx, pgdelta, subblock, context=context
    )

    # Performance hack; we really want trivial migration commands
    # (that only mutate the migration log) to not trigger a pg_catalog
    # view refresh, since many get issued as part of MIGRATION
    # REWRITEs.
    all_migration_tweaks = all(
        isinstance(
            cmd, (s_ver.AlterSchemaVersion, s_migrations.MigrationCommand)
        )
        and not cmd.get_subcommands(type=s_delta.ObjectCommand)
        for cmd in delta.get_subcommands()
    )

    if not ctx.bootstrap_mode and not all_migration_tweaks:
        from edb.pgsql import metaschema
        refresh = metaschema.generate_sql_information_schema_refresh(
            ctx.compiler_state.backend_runtime_params.instance_params.version
        )
        refresh.generate(subblock)

    return block, new_types, pgdelta.config_ops


def compile_dispatch_ql_migration(
    ctx: compiler.CompileContext,
    ql: qlast.MigrationCommand,
    *,
    in_script: bool,
) -> dbstate.BaseQuery:
    if ctx.expect_rollback and not isinstance(
        ql, (qlast.AbortMigration, qlast.AbortMigrationRewrite)
    ):
        # Only allow ABORT MIGRATION to pass when expecting a rollback
        if ctx.state.current_tx().get_migration_state() is None:
            raise errors.TransactionError(
                'expected a ROLLBACK or ROLLBACK TO SAVEPOINT command'
            )
        else:
            raise errors.TransactionError(
                'expected a ROLLBACK or ABORT MIGRATION command'
            )

    match ql:
        case qlast.CreateMigration():
            ctx._assert_not_in_migration_block(ql)

            return compile_and_apply_ddl_stmt(ctx, ql)

        case qlast.StartMigration():
            return _start_migration(ctx, ql, in_script)

        case qlast.PopulateMigration():
            return _populate_migration(ctx, ql)

        case qlast.DescribeCurrentMigration():
            return _describe_current_migration(ctx, ql)

        case qlast.AlterCurrentMigrationRejectProposed():
            return _alter_current_migration_reject_proposed(ctx, ql)

        case qlast.CommitMigration():
            return _commit_migration(ctx, ql)

        case qlast.AbortMigration():
            return _abort_migration(ctx, ql)

        case qlast.DropMigration():
            ctx._assert_not_in_migration_block(ql)

            return compile_and_apply_ddl_stmt(ctx, ql)

        case qlast.StartMigrationRewrite():
            return _start_migration_rewrite(ctx, ql, in_script)

        case qlast.CommitMigrationRewrite():
            return _commit_migration_rewrite(ctx, ql)

        case qlast.AbortMigrationRewrite():
            return _abort_migration_rewrite(ctx, ql)

        case qlast.ResetSchema():
            return _reset_schema(ctx, ql)

        case _:
            raise AssertionError(f'unexpected migration command: {ql}')


def _start_migration(
    ctx: compiler.CompileContext,
    ql: qlast.StartMigration,
    in_script: bool,
) -> dbstate.BaseQuery:
    ctx._assert_not_in_migration_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    if current_tx.is_implicit() and not in_script:
        savepoint_name = None
        tx_cmd = qlast.StartTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        query = dbstate.MigrationControlQuery(
            sql=tx_query.sql,
            action=dbstate.MigrationAction.START,
            tx_action=tx_query.action,
            cacheable=False,
            modaliases=None,
        )
    else:
        savepoint_name = current_tx.start_migration()
        query = dbstate.MigrationControlQuery(
            sql=NIL_QUERY,
            action=dbstate.MigrationAction.START,
            tx_action=None,
            cacheable=False,
            modaliases=None,
        )

    if isinstance(ql.target, qlast.CommittedSchema):
        mrstate = ctx._assert_in_migration_rewrite_block(ql)
        target_schema = mrstate.target_schema

    else:
        assert ctx.compiler_state.std_schema is not None
        base_schema = s_schema.ChainedSchema(
            ctx.compiler_state.std_schema,
            s_schema.EMPTY_SCHEMA,
            current_tx.get_global_schema(),
        )
        target_schema, warnings = s_ddl.apply_sdl(
            ql.target,
            base_schema=base_schema,
            current_schema=schema,
            testmode=ctx.is_testmode(),
        )
        query = dataclasses.replace(query, warnings=tuple(warnings))

    current_tx.update_migration_state(
        dbstate.MigrationState(
            parent_migration=schema.get_last_migration(),
            initial_schema=schema,
            initial_savepoint=savepoint_name,
            guidance=s_obj.DeltaGuidance(),
            target_schema=target_schema,
            accepted_cmds=tuple(),
            last_proposed=None,
        ),
    )
    return query


def _populate_migration(
    ctx: compiler.CompileContext,
    ql: qlast.PopulateMigration,
) -> dbstate.BaseQuery:
    mstate = ctx._assert_in_migration_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    diff = s_ddl.delta_schemas(
        schema,
        mstate.target_schema,
        guidance=mstate.guidance,
    )
    if debug.flags.delta_plan:
        debug.header('Populate Migration Diff')
        debug.dump(diff, schema=schema)

    new_ddl: Tuple[qlast.DDLCommand, ...] = tuple(
        s_ddl.ddlast_from_delta(  # type: ignore
            schema,
            mstate.target_schema,
            diff,
            testmode=ctx.is_testmode(),
        ),
    )
    all_ddl = mstate.accepted_cmds + new_ddl
    mstate = mstate._replace(
        accepted_cmds=all_ddl,
        last_proposed=None,
    )
    if debug.flags.delta_plan:
        debug.header('Populate Migration DDL AST')
        text = []
        for cmd in new_ddl:
            debug.dump(cmd)
            text.append(qlcodegen.generate_source(cmd, pretty=True))
        debug.header('Populate Migration DDL Text')
        debug.dump_code(';\n'.join(text) + ';')
    current_tx.update_migration_state(mstate)

    delta_context = _new_delta_context(ctx)

    # We want to make *certain* that the DDL we generate
    # produces the correct schema when applied, so we reload
    # the diff from the AST instead of just relying on the
    # delta tree. We do this check because it is *very
    # important* that we not emit DDL that moves the schema
    # into the wrong state.
    #
    # The actual check for whether the schema matches is done
    # by DESCRIBE CURRENT MIGRATION AS JSON, to populate the
    # 'complete' flag.
    if debug.flags.delta_plan:
        debug.header('Populate Migration Applied Diff')
    for cmd in new_ddl:
        reloaded_diff = s_ddl.delta_from_ddl(
            cmd,
            schema=schema,
            modaliases=current_tx.get_modaliases(),
            **_get_delta_context_args(ctx),
        )
        schema = reloaded_diff.apply(schema, delta_context)
        if debug.flags.delta_plan:
            debug.dump(reloaded_diff, schema=schema)

    current_tx.update_schema(schema)

    return dbstate.MigrationControlQuery(
        sql=NIL_QUERY,
        tx_action=None,
        action=dbstate.MigrationAction.POPULATE,
        cacheable=False,
        modaliases=None,
    )


def _describe_current_migration(
    ctx: compiler.CompileContext,
    ql: qlast.DescribeCurrentMigration,
) -> dbstate.BaseQuery:
    mstate = ctx._assert_in_migration_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    if ql.language is qltypes.DescribeLanguage.DDL:
        text = []
        for stmt in mstate.accepted_cmds:
            # Generate uppercase DDL commands for backwards
            # compatibility with older migration text.
            text.append(
                qlcodegen.generate_source(stmt, pretty=True, uppercase=True)
            )

        if text:
            description = ';\n'.join(text) + ';'
        else:
            description = ''

        desc_ql = edgeql.parse_query(
            f'SELECT {qlquote.quote_literal(description)}')
        return compiler._compile_ql_query(
            ctx,
            desc_ql,
            cacheable=False,
            migration_block_query=True,
        )

    if ql.language is qltypes.DescribeLanguage.JSON:
        confirmed = []
        for stmt in mstate.accepted_cmds:
            confirmed.append(
                # Add a terminating semicolon to match
                # "proposed", which is created by
                # s_ddl.statements_from_delta.
                #
                # Also generate uppercase DDL commands for
                # backwards compatibility with older migration
                # text.
                qlcodegen.generate_source(stmt, pretty=True, uppercase=True)
                + ';',
            )

        if mstate.last_proposed is None:
            guided_diff = s_ddl.delta_schemas(
                schema,
                mstate.target_schema,
                generate_prompts=True,
                guidance=mstate.guidance,
            )
            if debug.flags.delta_plan:
                debug.header('DESCRIBE CURRENT MIGRATION AS JSON delta')
                debug.dump(guided_diff)

            proposed_ddl = s_ddl.statements_from_delta(
                schema, mstate.target_schema, guided_diff, uppercase=True
            )
            proposed_steps = []

            if proposed_ddl:
                for ddl_text, ddl_ast, top_op in proposed_ddl:
                    assert isinstance(top_op, s_delta.ObjectCommand)

                    # get_ast has a lot of logic for figuring
                    # out when an op is implicit in a parent
                    # op. get_user_prompt does not have any of
                    # that sort of logic, which makes it
                    # susceptible to producing overly broad
                    # messages. To avoid duplicating that sort
                    # of logic, we recreate the delta from the
                    # AST, and extract a user prompt from
                    # *that*.
                    # This is stupid, and it is slow.
                    top_op2 = s_ddl.cmd_from_ddl(
                        ddl_ast,
                        schema=schema,
                        modaliases=current_tx.get_modaliases(),
                    )
                    assert isinstance(top_op2, s_delta.ObjectCommand)
                    prompt_key2, prompt_text = top_op2.get_user_prompt()

                    # Similarly, some placeholders may not have made
                    # it into the actual query, so filter them out.
                    used_placeholders = {
                        p.name
                        for p in ast.find_children(ddl_ast, qlast.Placeholder)
                    }
                    required_user_input = tuple(
                        inp
                        for inp in top_op.get_required_user_input()
                        if inp['placeholder'] in used_placeholders
                    )

                    # The prompt_id still needs to come from
                    # the original op, though, since
                    # orig_cmd_class is lost in ddl.
                    prompt_key, _ = top_op.get_user_prompt()
                    prompt_id = s_delta.get_object_command_id(prompt_key)
                    confidence = top_op.get_annotation('confidence')
                    assert confidence is not None

                    step = dbstate.ProposedMigrationStep(
                        statements=(ddl_text,),
                        confidence=confidence,
                        prompt=prompt_text,
                        prompt_id=prompt_id,
                        data_safe=top_op.is_data_safe(),
                        required_user_input=required_user_input,
                        operation_key=prompt_key2,
                    )
                    proposed_steps.append(step)

                proposed_desc = proposed_steps[0].to_json()
            else:
                proposed_desc = None

            mstate = mstate._replace(
                last_proposed=tuple(proposed_steps),
            )

            current_tx.update_migration_state(mstate)
        else:
            if mstate.last_proposed:
                proposed_desc = mstate.last_proposed[0].to_json()
            else:
                proposed_desc = None

        extra = {}

        complete = False
        if proposed_desc is None:
            diff = s_ddl.delta_schemas(schema, mstate.target_schema)
            complete = not bool(diff.get_subcommands())
            if debug.flags.delta_plan and not complete:
                debug.header('DESCRIBE CURRENT MIGRATION AS JSON mismatch')
                debug.dump(diff)
            if not complete:
                extra['debug_diff'] = debug.dumps(diff)

        desc = (
            json.dumps(
                {
                    'parent': (
                        str(mstate.parent_migration.get_name(schema))
                        if mstate.parent_migration is not None
                        else 'initial'
                    ),
                    'complete': complete,
                    'confirmed': confirmed,
                    'proposed': proposed_desc,
                    **extra,
                }
            )
        )

        desc_ql = edgeql.parse_query(
            f'SELECT to_json({qlquote.quote_literal(desc)})'
        )
        return compiler._compile_ql_query(
            ctx,
            desc_ql,
            cacheable=False,
            migration_block_query=True,
        )

    raise AssertionError(
        f'DESCRIBE CURRENT MIGRATION AS {ql.language}' f' is not implemented'
    )


def _alter_current_migration_reject_proposed(
    ctx: compiler.CompileContext,
    ql: qlast.AlterCurrentMigrationRejectProposed,
) -> dbstate.BaseQuery:
    mstate = ctx._assert_in_migration_block(ql)

    current_tx = ctx.state.current_tx()

    if not mstate.last_proposed:
        # XXX: Or should we compute what the proposal would be?
        new_guidance = mstate.guidance
    else:
        last = mstate.last_proposed[0]
        cmdclass_name, mcls, classname, new_name = last.operation_key
        if new_name is None:
            new_name = classname

        if cmdclass_name.startswith('Create'):
            new_guidance = mstate.guidance._replace(
                banned_creations=mstate.guidance.banned_creations
                | {
                    (mcls, classname),
                }
            )
        elif cmdclass_name.startswith('Delete'):
            new_guidance = mstate.guidance._replace(
                banned_deletions=mstate.guidance.banned_deletions
                | {
                    (mcls, classname),
                }
            )
        else:
            new_guidance = mstate.guidance._replace(
                banned_alters=mstate.guidance.banned_alters
                | {
                    (mcls, (classname, new_name)),
                }
            )

    mstate = mstate._replace(
        guidance=new_guidance,
        last_proposed=None,
    )
    current_tx.update_migration_state(mstate)

    return dbstate.MigrationControlQuery(
        sql=NIL_QUERY,
        tx_action=None,
        action=dbstate.MigrationAction.REJECT_PROPOSED,
        cacheable=False,
        modaliases=None,
    )


def _commit_migration(
    ctx: compiler.CompileContext,
    ql: qlast.CommitMigration,
) -> dbstate.BaseQuery:
    mstate = ctx._assert_in_migration_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    diff = s_ddl.delta_schemas(schema, mstate.target_schema)
    if list(diff.get_subcommands()):
        raise errors.QueryError(
            'cannot commit incomplete migration',
            hint=(
                'Please finish the migration by specifying the'
                ' remaining DDL operations or run POPULATE MIGRATION'
                ' to let the system populate the outstanding DDL'
                ' automatically.'
            ),
            span=ql.span,
        )

    if debug.flags.delta_plan:
        debug.header('Commit Migration DDL AST')
        text = []
        for cmd in mstate.accepted_cmds:
            debug.dump(cmd)
            text.append(qlcodegen.generate_source(cmd, pretty=True))
        debug.header('Commit Migration DDL Text')
        debug.dump_code(';\n'.join(text) + ';')

    last_migration = schema.get_last_migration()
    if last_migration:
        last_migration_ref = s_utils.name_to_ast_ref(
            last_migration.get_name(schema),
        )
    else:
        last_migration_ref = None

    target_sdl: Optional[str] = None
    store_migration_sdl = compiler._get_config_val(ctx, 'store_migration_sdl')
    if store_migration_sdl == 'AlwaysStore':
        target_sdl = s_ddl.sdl_text_from_schema(schema)

    create_migration = qlast.CreateMigration(  # type: ignore
        body=qlast.NestedQLBlock(
            commands=mstate.accepted_cmds  # type: ignore
        ),
        parent=last_migration_ref,
        target_sdl=target_sdl,
    )

    current_tx.update_schema(mstate.initial_schema)
    current_tx.update_migration_state(None)

    # If we are in a migration rewrite, don't actually apply
    # the change, just record it.
    if mrstate := current_tx.get_migration_rewrite_state():
        current_tx.update_schema(mstate.target_schema)
        mrstate = mrstate._replace(
            accepted_migrations=(
                mrstate.accepted_migrations + (create_migration,)
            )
        )
        current_tx.update_migration_rewrite_state(mrstate)

        return dbstate.MigrationControlQuery(
            sql=NIL_QUERY,
            action=dbstate.MigrationAction.COMMIT,
            tx_action=None,
            cacheable=False,
            modaliases=None,
        )

    current_tx.update_schema(mstate.initial_schema)
    current_tx.update_migration_state(None)

    ddl_query = compile_and_apply_ddl_stmt(
        ctx,
        create_migration,
    )

    if mstate.initial_savepoint:
        current_tx.commit_migration(mstate.initial_savepoint)
        tx_action = None
    else:
        tx_action = dbstate.TxAction.COMMIT

    return dbstate.MigrationControlQuery(
        sql=ddl_query.sql,
        ddl_stmt_id=ddl_query.ddl_stmt_id,
        action=dbstate.MigrationAction.COMMIT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
        user_schema=ctx.state.current_tx().get_user_schema(),
        cached_reflection=(current_tx.get_cached_reflection_if_updated()),
    )


def _abort_migration(
    ctx: compiler.CompileContext,
    ql: qlast.AbortMigration,
) -> dbstate.BaseQuery:
    mstate = ctx._assert_in_migration_block(ql)

    current_tx = ctx.state.current_tx()

    if mstate.initial_savepoint:
        current_tx.abort_migration(mstate.initial_savepoint)
        sql = NIL_QUERY
        tx_action = None
    else:
        tx_cmd = qlast.RollbackTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        sql = tx_query.sql
        tx_action = tx_query.action

    current_tx.update_migration_state(None)
    return dbstate.MigrationControlQuery(
        sql=sql,
        action=dbstate.MigrationAction.ABORT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
    )


def _start_migration_rewrite(
    ctx: compiler.CompileContext,
    ql: qlast.StartMigrationRewrite,
    in_script: bool,
) -> dbstate.BaseQuery:
    ctx._assert_not_in_migration_block(ql)
    ctx._assert_not_in_migration_rewrite_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    # Start a transaction if we aren't in one already
    if current_tx.is_implicit() and not in_script:
        savepoint_name = None
        tx_cmd = qlast.StartTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        query = dbstate.MigrationControlQuery(
            sql=tx_query.sql,
            action=dbstate.MigrationAction.START,
            tx_action=tx_query.action,
            cacheable=False,
            modaliases=None,
        )
    else:
        savepoint_name = current_tx.start_migration()
        query = dbstate.MigrationControlQuery(
            sql=NIL_QUERY,
            action=dbstate.MigrationAction.START,
            tx_action=None,
            cacheable=False,
            modaliases=None,
        )

        # Start from an empty schema except for `module default`
    base_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.EMPTY_SCHEMA,
        current_tx.get_global_schema(),
    )
    new_base_schema, _ = s_ddl.apply_sdl(
        qlast.Schema(
            declarations=[
                qlast.ModuleDeclaration(
                    name=qlast.ObjectRef(name='default'),
                    declarations=[],
                )
            ]
        ),
        base_schema=base_schema,
        current_schema=base_schema,
    )

    # Set our current schema to be the empty one
    current_tx.update_schema(new_base_schema)
    current_tx.update_migration_rewrite_state(
        dbstate.MigrationRewriteState(
            target_schema=schema,
            initial_savepoint=savepoint_name,
            accepted_migrations=tuple(),
        ),
    )

    return query


def _commit_migration_rewrite(
    ctx: compiler.CompileContext,
    ql: qlast.CommitMigrationRewrite,
) -> dbstate.BaseQuery:
    ctx._assert_not_in_migration_block(ql)
    mrstate = ctx._assert_in_migration_rewrite_block(ql)

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    diff = s_ddl.delta_schemas(schema, mrstate.target_schema)
    if list(diff.get_subcommands()):
        if debug.flags.delta_plan:
            debug.header("COMMIT MIGRATION REWRITE mismatch")
            diff.dump()
        raise errors.QueryError(
            'cannot commit migration rewrite: schema resulting '
            'from rewrite does not match committed schema',
            span=ql.span,
        )

    schema = mrstate.target_schema
    current_tx.update_schema(schema)
    current_tx.update_migration_rewrite_state(None)

    cmds: List[qlast.DDLCommand] = []
    # Now we find all the migrations...
    migrations = s_migrations.get_ordered_migrations(schema)
    for mig in reversed(migrations):
        cmds.append(
            qlast.DropMigration(
                name=qlast.ObjectRef(name=mig.get_name(schema).name)
            )
        )
    for acc_cmd in mrstate.accepted_migrations:
        acc_cmd.metadata_only = True
        cmds.append(acc_cmd)

    if debug.flags.delta_plan:
        debug.header('COMMIT MIGRATION REWRITE DDL text')
        for cm in cmds:
            cm.dump_edgeql()

    block = pg_dbops.PLTopBlock()
    for cmd in cmds:
        _, ddl_block = _compile_and_apply_ddl_stmt(ctx, cmd)
        assert isinstance(ddl_block, pg_dbops.PLBlock)
        # We know nothing serious can be in that query
        # except for the SQL, so it's fine to just discard
        # it all.
        for stmt in ddl_block.get_statements():
            block.add_command(stmt)

    if mrstate.initial_savepoint:
        current_tx.commit_migration(mrstate.initial_savepoint)
        tx_action = None
    else:
        tx_action = dbstate.TxAction.COMMIT

    return dbstate.MigrationControlQuery(
        sql=block.to_string().encode("utf-8"),
        action=dbstate.MigrationAction.COMMIT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
        user_schema=ctx.state.current_tx().get_user_schema(),
        cached_reflection=(current_tx.get_cached_reflection_if_updated()),
    )


def _abort_migration_rewrite(
    ctx: compiler.CompileContext,
    ql: qlast.AbortMigrationRewrite,
) -> dbstate.BaseQuery:
    mrstate = ctx._assert_in_migration_rewrite_block(ql)

    current_tx = ctx.state.current_tx()

    if mrstate.initial_savepoint:
        current_tx.abort_migration(mrstate.initial_savepoint)
        sql = NIL_QUERY
        tx_action = None
    else:
        tx_cmd = qlast.RollbackTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        sql = tx_query.sql
        tx_action = tx_query.action

    current_tx.update_migration_state(None)
    current_tx.update_migration_rewrite_state(None)
    query = dbstate.MigrationControlQuery(
        sql=sql,
        action=dbstate.MigrationAction.ABORT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
    )

    return query


def _reset_schema(
    ctx: compiler.CompileContext,
    ql: qlast.ResetSchema,
) -> dbstate.BaseQuery:
    ctx._assert_not_in_migration_block(ql)
    ctx._assert_not_in_migration_rewrite_block(ql)

    if ql.target.name != 'initial':
        raise errors.QueryError(
            f'Unknown schema version "{ql.target.name}". '
            'Currently, only revision supported is "initial"',
            span=ql.target.span,
        )

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    empty_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.EMPTY_SCHEMA,
        current_tx.get_global_schema(),
    )
    empty_schema, _ = s_ddl.apply_sdl(  # type: ignore
        qlast.Schema(
            declarations=[
                qlast.ModuleDeclaration(
                    name=qlast.ObjectRef(name='default'),
                    declarations=[],
                )
            ]
        ),
        base_schema=empty_schema,
        current_schema=empty_schema,
    )

    # diff and create migration that drops all objects
    diff = s_ddl.delta_schemas(schema, empty_schema)
    new_ddl: Tuple[qlast.DDLCommand, ...] = tuple(
        s_ddl.ddlast_from_delta(schema, empty_schema, diff),  # type: ignore
    )
    create_mig = qlast.CreateMigration(  # type: ignore
        body=qlast.NestedQLBlock(commands=tuple(new_ddl)),  # type: ignore
    )
    ddl_query, ddl_block = _compile_and_apply_ddl_stmt(ctx, create_mig)
    assert ddl_block is not None

    # delete all migrations
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    migrations = s_delta.sort_by_cross_refs(
        schema,
        schema.get_objects(type=s_migrations.Migration),
    )
    for mig in migrations:
        drop_mig = qlast.DropMigration(
            name=qlast.ObjectRef(name=mig.get_name(schema).name),
        )
        _, mig_block = _compile_and_apply_ddl_stmt(ctx, drop_mig)
        assert isinstance(mig_block, pg_dbops.PLBlock)
        for stmt in mig_block.get_statements():
            ddl_block.add_command(stmt)

    return dbstate.MigrationControlQuery(
        sql=ddl_block.to_string().encode("utf-8"),
        ddl_stmt_id=ddl_query.ddl_stmt_id,
        action=dbstate.MigrationAction.COMMIT,
        tx_action=None,
        cacheable=False,
        modaliases=None,
        user_schema=current_tx.get_user_schema(),
        cached_reflection=(current_tx.get_cached_reflection_if_updated()),
    )


_FEATURE_NAMES: dict[type[s_obj.Object], str] = {
    s_annos.AnnotationValue: 'annotation',
    s_policies.AccessPolicy: 'policy',
    s_triggers.Trigger: 'trigger',
    s_rewrites.Rewrite: 'rewrite',
    s_globals.Global: 'global',
    s_expraliases.Alias: 'alias',
    s_func.Function: 'function',
    s_indexes.Index: 'index',
    s_scalars.ScalarType: 'scalar',
}


def produce_feature_used_metrics(
    compiler_state: compiler.CompilerState,
    user_schema: s_schema.Schema,
) -> dict[str, float]:
    schema = s_schema.ChainedSchema(
        compiler_state.std_schema,
        user_schema,
        # Skipping global schema is a little dodgy but not that bad
        s_schema.EMPTY_SCHEMA,
    )

    features: dict[str, float] = {}

    def _track(key: str) -> None:
        features[key] = 1

    # TODO(perf): Should we optimize peeking into the innards directly
    # so we can skip creating the proxies?
    for obj in user_schema.get_objects(
        type=s_obj.Object, exclude_extensions=True,
    ):
        typ = type(obj)
        if (key := _FEATURE_NAMES.get(typ)):
            _track(key)

        if isinstance(obj, s_globals.Global) and obj.get_expr(user_schema):
            _track('computed_global')
        elif (
            isinstance(obj, s_properties.Property)
        ):
            if obj.get_expr(user_schema):
                _track('computed_property')
            elif obj.get_cardinality(schema).is_multi():
                _track('multi_property')

            if (
                obj.is_link_property(schema)
                and not obj.is_special_pointer(schema)
            ):
                _track('link_property')
        elif (
            isinstance(obj, s_links.Link)
            and obj.get_expr(user_schema)
        ):
            _track('computed_link')
        elif (
            isinstance(obj, s_indexes.Index)
            and s_indexes.is_fts_index(schema, obj)
        ):
            _track('fts')
        elif (
            isinstance(obj, s_constraints.Constraint)
            and not (
                (subject := obj.get_subject(schema))
                and isinstance(subject, s_properties.Property)
                and subject.is_special_pointer(schema)
            )
        ):
            _track('constraint')
            exclusive_constr = schema.get(
                'std::exclusive', type=s_constraints.Constraint
            )
            if not obj.issubclass(schema, exclusive_constr):
                _track('constraint_expr')
        elif (
            isinstance(obj, s_objtypes.ObjectType)
            and len(obj.get_bases(schema).objects(schema)) > 1
        ):
            _track('multiple_inheritance')
        elif (
            isinstance(obj, s_scalars.ScalarType)
            and obj.is_enum(schema)
        ):
            _track('enum')

    return features


def repair_schema(
    ctx: compiler.CompileContext,
) -> Optional[tuple[bytes, s_schema.Schema, Any]]:
    """Repair inconsistencies in the schema caused by bug fixes

    Works by comparing the actual current schema to the schema we get
    from reloading the DDL description of the schema and then directly
    applying the diff.
    """
    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    empty_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.EMPTY_SCHEMA,
        current_tx.get_global_schema(),
    )

    context_args = _get_delta_context_args(ctx)
    context_args.update(dict(
        testmode=True,
    ))

    text = s_ddl.ddl_text_from_schema(schema)
    reloaded_schema, _ = s_ddl.apply_ddl_script_ex(
        text,
        schema=empty_schema,
        **context_args,
    )

    delta = s_ddl.delta_schemas(
        schema,
        reloaded_schema,
    )
    mismatch = bool(delta.get_subcommands())
    if not mismatch:
        return None

    if debug.flags.delta_plan:
        debug.header('Repair Delta')
        debug.dump(delta)

    if not delta.is_data_safe():
        raise AssertionError(
            'Repair script for version upgrade is not data safe'
        )

    # Update the schema version also
    context = _new_delta_context(ctx, context_args)
    ver = schema.get_global(
        s_ver.SchemaVersion, '__schema_version__')
    reloaded_schema = ver.set_field_value(
        reloaded_schema, 'version', ver.get_version(schema))
    ver_cmd = ver.init_delta_command(schema, s_delta.AlterObject)
    ver_cmd.set_attribute_value('version', uuidgen.uuid1mc())
    reloaded_schema = ver_cmd.apply(reloaded_schema, context)
    delta.add(ver_cmd)

    # Apply and adapt delta, build native delta plan, which
    # will also update the schema.
    block, new_types, config_ops = _process_delta(ctx, delta)
    is_transactional = block.is_transactional()
    assert not new_types
    assert is_transactional
    sql = block.to_string().encode('utf-8')

    if debug.flags.delta_execute:
        debug.header('Repair Delta Script')
        debug.dump_code(sql, lexer='sql')

    return sql, reloaded_schema, config_ops


def administer_repair_schema(
    ctx: compiler.CompileContext,
    ql: qlast.AdministerStmt,
) -> dbstate.BaseQuery:
    if ql.expr.args or ql.expr.kwargs:
        raise errors.QueryError(
            'repair_schema() does not take arguments',
            span=ql.expr.span,
        )

    current_tx = ctx.state.current_tx()

    res = repair_schema(ctx)
    if not res:
        return dbstate.MaintenanceQuery(sql=b"")
    sql, new_schema, config_ops = res

    current_tx.update_schema(new_schema)

    return dbstate.DDLQuery(
        sql=sql,
        user_schema=current_tx.get_user_schema_if_updated(),  # type: ignore
        global_schema=current_tx.get_global_schema_if_updated(),
        config_ops=config_ops,
    )


def administer_reindex(
    ctx: compiler.CompileContext,
    ql: qlast.AdministerStmt,
) -> dbstate.BaseQuery:
    from edb.ir import ast as irast
    from edb.ir import typeutils as irtypeutils

    from edb.schema import objtypes as s_objtypes
    from edb.schema import constraints as s_constraints
    from edb.schema import indexes as s_indexes

    if len(ql.expr.args) != 1 or ql.expr.kwargs:
        raise errors.QueryError(
            'reindex() takes exactly one position argument',
            span=ql.expr.span,
        )

    arg = ql.expr.args[0]
    match arg:
        case qlast.Path(
            steps=[qlast.ObjectRef()],
            partial=False,
        ):
            ptr = False
        case qlast.Path(
            steps=[qlast.ObjectRef(), qlast.Ptr()],
            partial=False,
        ):
            ptr = True
        case _:
            raise errors.QueryError(
                'argument to reindex() must be an object type',
                span=arg.span,
            )

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)
    modaliases = current_tx.get_modaliases()

    ir: irast.Statement = qlcompiler.compile_ast_to_ir(
        arg,
        schema=schema,
        options=qlcompiler.CompilerOptions(
            modaliases=modaliases
        ),
    )
    expr = ir.expr
    if ptr:
        if (
            not expr.expr
            or not isinstance(expr.expr, irast.SelectStmt)
            or not isinstance(expr.expr.result.expr, irast.Pointer)
        ):
            raise errors.QueryError(
                'invalid pointer argument to reindex()',
                span=arg.span,
            )
        rptr = expr.expr.result.expr
        source = rptr.source
    else:
        rptr = None
        source = expr
    schema, obj = irtypeutils.ir_typeref_to_type(schema, source.typeref)

    if (
        not isinstance(obj, s_objtypes.ObjectType)
        or not obj.is_material_object_type(schema)
    ):
        raise errors.QueryError(
            'argument to reindex() must be a regular object type',
            span=arg.span,
        )

    tables: set[s_pointers.Pointer | s_objtypes.ObjectType] = set()
    pindexes: set[
        s_constraints.Constraint | s_indexes.Index | s_pointers.Pointer
    ] = set()

    commands = []
    if not rptr:
        # On a type, we just reindex the type and its descendants
        tables.update({obj} | {
            desc for desc in obj.descendants(schema)
            if desc.is_material_object_type(schema)
        })
    else:
        # On a pointer, we reindex any indexes and constraints, as well as
        # any link indexes (which might be table indexes on a link table)
        if not isinstance(rptr.ptrref, irast.PointerRef):
            raise errors.QueryError(
                'invalid pointer argument to reindex()',
                span=arg.span,
            )
        schema, ptrcls = irtypeutils.ptrcls_from_ptrref(
            rptr.ptrref, schema=schema)

        indexes = set(schema.get_referrers(ptrcls, scls_type=s_indexes.Index))

        exclusive = schema.get('std::exclusive', type=s_constraints.Constraint)
        constrs = {
            c for c in
            schema.get_referrers(ptrcls, scls_type=s_constraints.Constraint)
            if c.issubclass(schema, exclusive)
        }

        pindexes.update(indexes | constrs)
        pindexes.update({
            desc for pindex in pindexes for desc in pindex.descendants(schema)
        })

        # For links, collect any single link indexes and any link table indexes
        if not ptrcls.is_property(schema):
            ptrclses = {ptrcls} | {
                desc for desc in ptrcls.descendants(schema)
                if isinstance(
                    (src := desc.get_source(schema)), s_objtypes.ObjectType)
                and src.is_material_object_type(schema)
            }

            card = ptrcls.get_cardinality(schema)
            if card.is_single():
                pindexes.update(ptrclses)
            if card.is_multi() or ptrcls.has_user_defined_properties(schema):
                tables.update(ptrclses)

    commands = [
        f'REINDEX TABLE '
        f'{pg_common.get_backend_name(schema, table)};'
        for table in tables
    ] + [
        f'REINDEX INDEX '
        f'{pg_common.get_backend_name(schema, pindex, aspect="index")};'
        for pindex in pindexes
    ]

    block = pg_dbops.PLTopBlock()
    for command in commands:
        block.add_command(command)

    return dbstate.MaintenanceQuery(sql=block.to_string().encode("utf-8"))


def administer_vacuum(
    ctx: compiler.CompileContext,
    ql: qlast.AdministerStmt,
) -> dbstate.BaseQuery:
    from edb.ir import ast as irast
    from edb.ir import typeutils as irtypeutils
    from edb.schema import objtypes as s_objtypes

    # check that the kwargs are valid
    kwargs: Dict[str, str] = {}
    for name, val in ql.expr.kwargs.items():
        if name != 'full':
            raise errors.QueryError(
                f'unrecognized keyword argument {name!r} for vacuum()',
                span=val.span,
            )
        elif (
            not isinstance(val, qlast.Constant)
            or val.kind != qlast.ConstantKind.BOOLEAN
        ):
            raise errors.QueryError(
                f'argument {name!r} for vacuum() must be a boolean literal',
                span=val.span,
            )
        kwargs[name] = val.value

    # Next go over the args (if any) and convert paths to tables/columns
    args: List[Tuple[irast.Pointer | None, s_objtypes.ObjectType]] = []
    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)
    modaliases = current_tx.get_modaliases()

    for arg in ql.expr.args:
        match arg:
            case qlast.Path(
                steps=[qlast.ObjectRef()],
                partial=False,
            ):
                ptr = False
            case qlast.Path(
                steps=[qlast.ObjectRef(), qlast.Ptr()],
                partial=False,
            ):
                ptr = True
            case _:
                raise errors.QueryError(
                    'argument to vacuum() must be an object type '
                    'or a link or property reference',
                    span=arg.span,
                )

        ir: irast.Statement = qlcompiler.compile_ast_to_ir(
            arg,
            schema=schema,
            options=qlcompiler.CompilerOptions(
                modaliases=modaliases
            ),
        )
        expr = ir.expr
        if ptr:
            if (
                not expr.expr
                or not isinstance(expr.expr, irast.SelectStmt)
                or not isinstance(expr.expr.result.expr, irast.Pointer)
            ):
                raise errors.QueryError(
                    'invalid pointer argument to vacuum()',
                    span=arg.span,
                )
            rptr = expr.expr.result.expr
            source = rptr.source
        else:
            rptr = None
            source = expr
        schema, obj = irtypeutils.ir_typeref_to_type(schema, source.typeref)

        if (
            not isinstance(obj, s_objtypes.ObjectType)
            or not obj.is_material_object_type(schema)
        ):
            raise errors.QueryError(
                'argument to vacuum() must be an object type '
                'or a link or property reference',
                span=arg.span,
            )
        args.append((rptr, obj))

    tables: set[s_pointers.Pointer | s_objtypes.ObjectType] = set()

    for arg, (rptr, obj) in zip(ql.expr.args, args):
        if not rptr:
            # On a type, we just vacuum the type and its descendants
            tables.update({obj} | {
                desc for desc in obj.descendants(schema)
                if desc.is_material_object_type(schema)
            })
        else:
            # On a pointer, we must go over the pointer and its descendants
            # so that we may retrieve any link talbes if necessary.
            if not isinstance(rptr.ptrref, irast.PointerRef):
                raise errors.QueryError(
                    'invalid pointer argument to vacuum()',
                    span=arg.span,
                )
            schema, ptrcls = irtypeutils.ptrcls_from_ptrref(
                rptr.ptrref, schema=schema)

            card = ptrcls.get_cardinality(schema)
            if not (
                card.is_multi() or ptrcls.has_user_defined_properties(schema)
            ):
                vn = ptrcls.get_verbosename(schema, with_parent=True)
                if ptrcls.is_property(schema):
                    raise errors.QueryError(
                        f'{vn} is not a valid argument to vacuum() '
                        f'because it is not a multi property',
                        span=arg.span,
                    )
                else:
                    raise errors.QueryError(
                        f'{vn} is not a valid argument to vacuum() '
                        f'because it is neither a multi link nor '
                        f'does it have link properties',
                        span=arg.span,
                    )

            ptrclses = {ptrcls} | {
                desc for desc in ptrcls.descendants(schema)
                if isinstance(
                    (src := desc.get_source(schema)), s_objtypes.ObjectType)
                and src.is_material_object_type(schema)
            }
            tables.update(ptrclses)

    tables_and_columns = [
        pg_common.get_backend_name(schema, table)
        for table in tables
    ]

    if kwargs.get('full', '').lower() == 'true':
        options = 'FULL'
    else:
        options = ''

    command = f'VACUUM {options} ' + ', '.join(tables_and_columns)

    return dbstate.MaintenanceQuery(
        sql=command.encode('utf-8'),
        is_transactional=False,
    )


def administer_prepare_upgrade(
    ctx: compiler.CompileContext,
    ql: qlast.AdministerStmt,
) -> dbstate.BaseQuery:

    user_schema = ctx.state.current_tx().get_user_schema()
    global_schema = ctx.state.current_tx().get_global_schema()

    schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        user_schema,
        global_schema
    )

    schema_ddl = s_ddl.ddl_text_from_schema(
        schema, include_migrations=True)
    ids, _ = compiler.get_obj_ids(schema, include_extras=True)
    json_ids = [(name, cls, str(id)) for name, cls, id in ids]

    obj = dict(
        ddl=schema_ddl, ids=json_ids
    )

    desc_ql = edgeql.parse_query(
        f'SELECT to_json({qlquote.quote_literal(json.dumps(obj))})'
    )
    return compiler._compile_ql_query(
        ctx,
        desc_ql,
        cacheable=False,
        migration_block_query=True,
    )


def validate_schema_equivalence(
    state: compiler.CompilerState,
    schema_a: s_schema.FlatSchema,
    schema_b: s_schema.FlatSchema,
    global_schema: s_schema.FlatSchema,
) -> None:
    schema_a_full = s_schema.ChainedSchema(
        state.std_schema,
        schema_a,
        global_schema,
    )
    schema_b_full = s_schema.ChainedSchema(
        state.std_schema,
        schema_b,
        global_schema,
    )

    diff = s_ddl.delta_schemas(schema_a_full, schema_b_full)
    complete = not bool(diff.get_subcommands())
    if not complete:
        if debug.flags.delta_plan:
            debug.header('COMPARE SCHEMAS MISMATCH')
            debug.dump(diff)
        raise AssertionError(
            f'schemas did not match after introspection:\n{debug.dumps(diff)}'
        )
