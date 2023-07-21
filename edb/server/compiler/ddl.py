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
from typing import *

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

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as s_delta
from edb.schema import migrations as s_migrations
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema
from edb.schema import utils as s_utils
from edb.schema import version as s_ver

from edb.pgsql import common as pg_common
from edb.pgsql import delta as pg_delta
from edb.pgsql import dbops as pg_dbops

from . import dbstate
from . import compiler


def compile_and_apply_ddl_stmt(
    ctx: compiler.CompileContext,
    stmt: qlast.DDLOperation,
    source: Optional[edgeql.Source] = None,
) -> dbstate.DDLQuery:
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
                "bare DDL statements are not allowed in this database",
                hint="Use the migration commands instead.",
                details=(
                    f"The `allow_bare_ddl` configuration variable "
                    f"is set to {str(allow_bare_ddl)!r}.  The "
                    f"`edgedb migrate` command normally sets this "
                    f"to avoid accidental schema changes outside of "
                    f"the migration flow."
                ),
                context=stmt.context,
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
                            qlast.Ptr(
                                ptr=qlast.ObjectRef(name='DDLStatement')
                            ),
                        ]
                    ),
                )
            ],
        )
        return compile_and_apply_ddl_stmt(ctx, cm)

    assert isinstance(stmt, qlast.DDLCommand)
    delta = s_ddl.delta_from_ddl(
        stmt,
        schema=schema,
        modaliases=current_tx.get_modaliases(),
        **_get_delta_context_args(ctx),
    )

    if debug.flags.delta_plan:
        debug.header('Delta Plan Input')
        debug.dump(delta)

    if mstate := current_tx.get_migration_state():
        mstate = mstate._replace(
            accepted_cmds=mstate.accepted_cmds + (stmt,),
        )

        context = _new_delta_context(ctx)
        schema = delta.apply(schema, context=context)

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
        current_tx.update_schema(schema)

        return dbstate.DDLQuery(
            sql=(b'SELECT LIMIT 0',),
            user_schema=current_tx.get_user_schema(),
            is_transactional=True,
            single_unit=False,
        )

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

        context = _new_delta_context(ctx)
        schema = delta.apply(schema, context=context)

        current_tx.update_schema(schema)

        return dbstate.DDLQuery(
            sql=(b'SELECT LIMIT 0',),
            user_schema=current_tx.get_user_schema(),
            is_transactional=True,
            single_unit=False,
        )

    # Do a dry-run on test_schema to canonicalize
    # the schema delta-commands.
    test_schema = current_tx.get_schema(ctx.compiler_state.std_schema)
    context = _new_delta_context(ctx)
    delta.apply(test_schema, context=context)
    delta.canonical = True

    # Apply and adapt delta, build native delta plan, which
    # will also update the schema.
    block, new_types, config_ops = _process_delta(ctx, delta)

    ddl_stmt_id: Optional[str] = None

    is_transactional = block.is_transactional()
    if not is_transactional:
        sql = tuple(stmt.encode('utf-8') for stmt in block.get_statements())
    else:
        sql = (block.to_string().encode('utf-8'),)

        if new_types:
            # Inject a query returning backend OIDs for the newly
            # created types.
            ddl_stmt_id = str(uuidgen.uuid1mc())
            new_type_ids = [
                f'{pg_common.quote_literal(tid)}::uuid' for tid in new_types
            ]
            sql = sql + (
                textwrap.dedent(
                    f'''\
                SELECT
                    json_build_object(
                        'ddl_stmt_id',
                        {pg_common.quote_literal(ddl_stmt_id)},
                        'new_types',
                        (SELECT
                            json_object_agg(
                                "id"::text,
                                "backend_id"
                            )
                            FROM
                            edgedb."_SchemaType"
                            WHERE
                                "id" = any(ARRAY[
                                    {', '.join(new_type_ids)}
                                ])
                        )
                    )::text;
            '''
                ).encode('utf-8'),
            )

    create_db = None
    drop_db = None
    create_db_template = None
    create_ext = None
    drop_ext = None
    if isinstance(stmt, qlast.DropDatabase):
        drop_db = stmt.name.name
    elif isinstance(stmt, qlast.CreateDatabase):
        create_db = stmt.name.name
        create_db_template = stmt.template.name if stmt.template else None
    elif isinstance(stmt, qlast.CreateExtension):
        create_ext = stmt.name.name
    elif isinstance(stmt, qlast.DropExtension):
        drop_ext = stmt.name.name

    if debug.flags.delta_execute:
        debug.header('Delta Script')
        debug.dump_code(b'\n'.join(sql), lexer='sql')

    return dbstate.DDLQuery(
        sql=sql,
        is_transactional=is_transactional,
        single_unit=bool(
            (not is_transactional)
            or (drop_db is not None)
            or (create_db is not None)
            or new_types
        ),
        create_db=create_db,
        drop_db=drop_db,
        create_db_template=create_db_template,
        create_ext=create_ext,
        drop_ext=drop_ext,
        has_role_ddl=isinstance(stmt, qlast.RoleCommand),
        ddl_stmt_id=ddl_stmt_id,
        user_schema=current_tx.get_user_schema_if_updated(),  # type: ignore
        cached_reflection=current_tx.get_cached_reflection_if_updated(),
        global_schema=current_tx.get_global_schema_if_updated(),
        config_ops=config_ops,
    )


def _new_delta_context(
    ctx: compiler.CompileContext, args: Any=None
) -> s_delta.CommandContext:
    return s_delta.CommandContext(
        backend_runtime_params=ctx.compiler_state.backend_runtime_params,
        stdmode=ctx.bootstrap_mode,
        internal_schema_mode=ctx.internal_schema_mode,
        **(_get_delta_context_args(ctx) if args is None else args),
    )


def _get_delta_context_args(ctx: compiler.CompileContext) -> dict[str, Any]:
    """Get the args need from delta_from_ddl"""
    return dict(
        testmode=compiler._get_config_val(ctx, '__internal_testmode'),
        allow_dml_in_functions=(
            compiler._get_config_val(ctx, 'allow_dml_in_functions')
        ),
        schema_object_ids=ctx.schema_object_ids,
        compat_ver=ctx.compat_ver,
    )


def _process_delta(
    ctx: compiler.CompileContext, delta: s_delta.DeltaRoot
) -> tuple[pg_dbops.SQLBlock, FrozenSet[str], Any]:
    """Adapt and process the delta command."""

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    if debug.flags.delta_plan:
        debug.header('Canonical Delta Plan')
        debug.dump(delta, schema=schema)

    pgdelta = pg_delta.CommandMeta.adapt(delta)
    assert isinstance(pgdelta, pg_delta.DeltaRoot)
    context = _new_delta_context(ctx)
    schema = pgdelta.apply(schema, context)
    current_tx.update_schema(schema)

    if debug.flags.delta_pgsql_plan:
        debug.header('PgSQL Delta Plan')
        debug.dump(pgdelta, schema=schema)

    db_cmd = any(
        isinstance(c, s_db.DatabaseCommand) for c in pgdelta.get_subcommands()
    )

    if db_cmd:
        block = pg_dbops.SQLBlock()
        new_types: FrozenSet[str] = frozenset()
    else:
        block = pg_dbops.PLTopBlock()
        new_types = frozenset(str(tid) for tid in pgdelta.new_types)

    # Generate SQL DDL for the delta.
    pgdelta.generate(block)  # type: ignore

    # Generate schema storage SQL (DML into schema storage tables).
    subblock = block.add_block()
    compiler.compile_schema_storage_in_delta(
        ctx, pgdelta, subblock, context=context
    )

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
            single_unit=tx_query.single_unit,
        )
    else:
        savepoint_name = current_tx.start_migration()
        query = dbstate.MigrationControlQuery(
            sql=(b'SELECT LIMIT 0',),
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
            s_schema.FlatSchema(),
            current_tx.get_global_schema(),
        )
        target_schema = s_ddl.apply_sdl(
            ql.target,
            base_schema=base_schema,
            current_schema=schema,
            testmode=(compiler._get_config_val(ctx, '__internal_testmode')),
            allow_dml_in_functions=(
                compiler._get_config_val(ctx, 'allow_dml_in_functions')
            ),
        )

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
            testmode=compiler._get_config_val(ctx, '__internal_testmode'),
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
        sql=(b'SELECT LIMIT 0',),
        tx_action=None,
        action=dbstate.MigrationAction.POPULATE,
        cacheable=False,
        modaliases=None,
        single_unit=False,
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

        complete = False
        if proposed_desc is None:
            diff = s_ddl.delta_schemas(schema, mstate.target_schema)
            complete = not bool(diff.get_subcommands())
            if debug.flags.delta_plan and not complete:
                debug.header('DESCRIBE CURRENT MIGRATION AS JSON mismatch')
                debug.dump(diff)

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
                }
            )
            .encode('unicode_escape')
            .decode('utf-8')
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
        sql=(b'SELECT LIMIT 0',),
        tx_action=None,
        action=dbstate.MigrationAction.REJECT_PROPOSED,
        cacheable=False,
        modaliases=None,
        single_unit=False,
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
            context=ql.context,
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

    create_migration = qlast.CreateMigration(  # type: ignore
        body=qlast.NestedQLBlock(
            commands=mstate.accepted_cmds  # type: ignore
        ),
        parent=last_migration_ref,
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
            sql=(b'SELECT LIMIT 0',),
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
        sql = ddl_query.sql
        tx_action = None
    else:
        tx_cmd = qlast.CommitTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        sql = ddl_query.sql + tx_query.sql
        tx_action = tx_query.action

    return dbstate.MigrationControlQuery(
        sql=sql,
        ddl_stmt_id=ddl_query.ddl_stmt_id,
        action=dbstate.MigrationAction.COMMIT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
        single_unit=True,
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
        sql: Tuple[bytes, ...] = (b'SELECT LIMIT 0',)
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
        single_unit=True,
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
            single_unit=tx_query.single_unit,
        )
    else:
        savepoint_name = current_tx.start_migration()
        query = dbstate.MigrationControlQuery(
            sql=(b'SELECT LIMIT 0',),
            action=dbstate.MigrationAction.START,
            tx_action=None,
            cacheable=False,
            modaliases=None,
        )

        # Start from an empty schema except for `module default`
    base_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.FlatSchema(),
        current_tx.get_global_schema(),
    )
    base_schema = s_ddl.apply_sdl(  # type: ignore
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
    current_tx.update_schema(base_schema)
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
            context=ql.context,
        )

    schema = mrstate.target_schema
    current_tx.update_schema(schema)
    current_tx.update_migration_rewrite_state(None)

    cmds: List[qlast.DDLCommand] = []
    # Now we find all the migrations...
    migrations = s_delta.sort_by_cross_refs(
        schema,
        schema.get_objects(type=s_migrations.Migration),
    )
    for mig in migrations:
        cmds.append(
            qlast.DropMigration(  # type: ignore
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

    sqls: List[bytes] = []
    for cmd in cmds:
        ddl_query = compile_and_apply_ddl_stmt(ctx, cmd)
        # We know nothing serious can be in that query
        # except for the SQL, so it's fine to just discard
        # it all.
        sqls.extend(ddl_query.sql)

    if mrstate.initial_savepoint:
        current_tx.commit_migration(mrstate.initial_savepoint)
        tx_action = None
    else:
        tx_cmd = qlast.CommitTransaction()
        tx_query = compiler._compile_ql_transaction(ctx, tx_cmd)
        sqls.extend(tx_query.sql)
        tx_action = tx_query.action

    return dbstate.MigrationControlQuery(
        sql=tuple(sqls),
        action=dbstate.MigrationAction.COMMIT,
        tx_action=tx_action,
        cacheable=False,
        modaliases=None,
        single_unit=True,
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
        sql: Tuple[bytes, ...] = (b'SELECT LIMIT 0',)
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
        single_unit=True,
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
            context=ql.target.context,
        )

    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    empty_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.FlatSchema(),
        current_tx.get_global_schema(),
    )
    empty_schema = s_ddl.apply_sdl(  # type: ignore
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

    sqls: List[bytes] = []

    # diff and create migration that drops all objects
    diff = s_ddl.delta_schemas(schema, empty_schema)
    new_ddl: Tuple[qlast.DDLCommand, ...] = tuple(
        s_ddl.ddlast_from_delta(schema, empty_schema, diff),  # type: ignore
    )
    create_mig = qlast.CreateMigration(  # type: ignore
        body=qlast.NestedQLBlock(commands=tuple(new_ddl)),  # type: ignore
    )
    ddl_query = compile_and_apply_ddl_stmt(ctx, create_mig)
    sqls.extend(ddl_query.sql)

    # delete all migrations
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    migrations = s_delta.sort_by_cross_refs(
        schema,
        schema.get_objects(type=s_migrations.Migration),
    )
    for mig in migrations:
        drop_mig = qlast.DropMigration(  # type: ignore
            name=qlast.ObjectRef(name=mig.get_name(schema).name),
        )
        ddl_query = compile_and_apply_ddl_stmt(ctx, drop_mig)
        sqls.extend(ddl_query.sql)

    return dbstate.MigrationControlQuery(
        sql=tuple(sqls),
        ddl_stmt_id=ddl_query.ddl_stmt_id,
        action=dbstate.MigrationAction.COMMIT,
        tx_action=None,
        cacheable=False,
        modaliases=None,
        single_unit=True,
        user_schema=current_tx.get_user_schema(),
        cached_reflection=(current_tx.get_cached_reflection_if_updated()),
    )


def repair_schema(
    ctx: compiler.CompileContext,
) -> Optional[tuple[tuple[bytes, ...], s_schema.Schema, Any]]:
    """Repair inconsistencies in the schema caused by bug fixes

    Works by comparing the actual current schema to the schema we get
    from reloading the DDL description of the schema and then directly
    applying the diff.
    """
    current_tx = ctx.state.current_tx()
    schema = current_tx.get_schema(ctx.compiler_state.std_schema)

    empty_schema = s_schema.ChainedSchema(
        ctx.compiler_state.std_schema,
        s_schema.FlatSchema(),
        current_tx.get_global_schema(),
    )

    context_args = _get_delta_context_args(ctx)
    context_args.update(dict(
        testmode=True,
        allow_dml_in_functions=True,
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
    sql = (block.to_string().encode('utf-8'),)

    if debug.flags.delta_execute:
        debug.header('Repair Delta Script')
        debug.dump_code(b'\n'.join(sql), lexer='sql')

    return sql, reloaded_schema, config_ops


def administer_repair_schema(
    ctx: compiler.CompileContext,
    ql: qlast.AdministerStmt,
) -> dbstate.BaseQuery:
    if ql.expr.args or ql.expr.kwargs:
        raise errors.QueryError(
            'repair_schema() does not take arguments',
            context=ql.expr.context,
        )

    current_tx = ctx.state.current_tx()

    res = repair_schema(ctx)
    if not res:
        return dbstate.MaintenanceQuery(sql=(b'',))
    sql, new_schema, config_ops = res

    current_tx.update_schema(new_schema)

    return dbstate.DDLQuery(
        sql=sql,
        single_unit=False,
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
    from edb.schema import pointers as s_pointers

    if len(ql.expr.args) != 1 or ql.expr.kwargs:
        raise errors.QueryError(
            'reindex() takes exactly one position argument',
            context=ql.expr.context,
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
                context=arg.context,
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
            or not expr.expr.result.rptr
        ):
            raise errors.QueryError(
                'invalid pointer argument to reindex()',
                context=arg.context,
            )
        rptr = expr.expr.result.rptr
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
            context=arg.context,
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
                context=arg.context,
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

    return dbstate.MaintenanceQuery(
        sql=tuple(q.encode('utf-8') for q in commands)
    )
