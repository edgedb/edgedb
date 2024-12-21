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
from typing import (
    Any,
    Optional,
    Sequence,
)

import dataclasses
import json
import logging
import os


from edb import edgeql
from edb import errors
from edb.edgeql import ast as qlast

from edb.common import debug
from edb.common import uuidgen

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import functions as s_func
from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema

from edb.server import args as edbargs
from edb.server import bootstrap
from edb.server import config
from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import instdata
from edb.server import pgcluster
from edb.server import pgcon

from edb.pgsql import common as pg_common
from edb.pgsql import dbops
from edb.pgsql import metaschema
from edb.pgsql import trampoline


logger = logging.getLogger('edb.server')

PGCon = bootstrap.PGConnectionProxy | pgcon.PGConnection


async def _load_schema(
    ctx: bootstrap.BootstrapContext, state: edbcompiler.CompilerState
) -> s_schema.ChainedSchema:
    assert state.global_intro_query
    json_data = await ctx.conn.sql_fetch_val(
        state.global_intro_query.encode('utf-8'))
    global_schema = s_refl.parse_into(
        base_schema=state.std_schema,
        schema=s_schema.EMPTY_SCHEMA,
        data=json_data,
        schema_class_layout=state.schema_class_layout,
    )

    return s_schema.ChainedSchema(
        state.std_schema,
        s_schema.EMPTY_SCHEMA,
        global_schema,
    )


def _is_stdlib_target(
    t: s_objtypes.ObjectType,
    schema: s_schema.Schema,
) -> bool:
    if intersection := t.get_intersection_of(schema):
        return any((_is_stdlib_target(it, schema)
                    for it in intersection.objects(schema)))
    elif union := t.get_union_of(schema):
        return any((_is_stdlib_target(ut, schema)
                    for ut in union.objects(schema)))

    name = t.get_name(schema)

    if name == sn.QualName('std', 'Object'):
        return False
    return t.get_name(schema).get_module_name() in s_schema.STD_MODULES


def _compile_schema_fixup(
    ctx: bootstrap.BootstrapContext,
    schema: s_schema.ChainedSchema,
    keys: dict[str, Any],
) -> str:
    """Compile any schema-specific fixes that need to be applied."""
    current_block = dbops.PLTopBlock()
    backend_params = ctx.cluster.get_runtime_params()

    # Recompile functions that reference stdlib types (like
    # std::BaseObject or schema::Object), since new subtypes may have
    # been added.
    to_recompile = schema._top_schema.get_objects(type=s_func.Function)
    for func in to_recompile:
        if func.get_name(schema).get_root_module_name() == s_schema.EXT_MODULE:
            continue
        # If none of the types referenced in the function are standard
        # library types, we don't need to recompile.
        if not (
            (expr := func.get_nativecode(schema))
            and expr.refs
            and any(
                isinstance(dep, s_objtypes.ObjectType)
                and _is_stdlib_target(dep, schema)
                for dep in expr.refs.objects(schema)
            )
        ):
            continue

        alter_func = func.init_delta_command(
            schema, sd.AlterObject
        )
        alter_func.set_attribute_value(
            'nativecode', func.get_nativecode(schema)
        )
        alter_func.canonical = True

        # N.B: We are ignoring the schema changes, since we aren't
        # updating the schema version.
        _, plan, _ = bootstrap._process_delta_params(
            sd.DeltaRoot.from_commands(alter_func),
            schema,
            backend_params,
            stdmode=False,
            **keys,
        )
        plan.generate(current_block)

    # Regenerate on_target_delete triggers for any links targeting a
    # stdlib type.
    links = schema._top_schema.get_objects(type=s_links.Link)
    for link in links:
        if link.get_name(schema).get_root_module_name() == s_schema.EXT_MODULE:
            continue
        source = link.get_source(schema)
        if (
            not source
            or not source.is_material_object_type(schema)
            or link.get_computable(schema)
            or link.get_shortname(schema).name == '__type__'
            or not _is_stdlib_target(link.get_target(schema), schema)
        ):
            continue

        pol = link.get_on_target_delete(schema)
        # HACK: Set the policy in a temporary in-memory schema to be
        # something else, so that we can set it back to the real value
        # and pgdelta will generate code for it.
        fake_pol = (
            s_links.LinkTargetDeleteAction.Allow
            if pol == s_links.LinkTargetDeleteAction.Restrict
            else s_links.LinkTargetDeleteAction.Restrict
        )
        fake_schema = link.set_field_value(schema, 'on_target_delete', fake_pol)

        alter_delta, alter_link, _ = link.init_delta_branch(
            schema, sd.CommandContext(), sd.AlterObject
        )
        alter_link.set_attribute_value('on_target_delete', pol)

        # N.B: We are ignoring the schema changes, since we aren't
        # updating the schema version.
        _, plan, _ = bootstrap._process_delta_params(
            sd.DeltaRoot.from_commands(alter_delta),
            fake_schema,
            backend_params,
            stdmode=False,
            **keys,
        )
        plan.generate(current_block)

    return current_block.to_string()


async def _upgrade_one(
    ctx: bootstrap.BootstrapContext,
    state: edbcompiler.CompilerState,
    upgrade_data: Optional[Any],
) -> None:
    if not upgrade_data:
        return

    backend_params = ctx.cluster.get_runtime_params()
    assert backend_params.has_create_database

    ddl = upgrade_data['ddl']
    # ids:
    schema_object_ids = {
        (
            sn.name_from_string(name), qltype if qltype else None
        ): uuidgen.UUID(objid)
        for name, qltype, objid in upgrade_data['ids']
    }

    # Load the schemas
    schema = await _load_schema(ctx, state)

    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=state,
        user_schema=schema.get_top_schema(),
        bootstrap_mode=False,  # MAYBE?
    )

    keys: dict[str, Any] = dict(
        testmode=True,
    )

    # Apply the DDL, but *only* execute the schema storage part!!
    for ddl_cmd in edgeql.parse_block(ddl):
        current_block = dbops.PLTopBlock()

        if debug.flags.sdl_loading:
            ddl_cmd.dump_edgeql()

        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema,
            schema_object_ids=schema_object_ids,
            **keys,
        )
        schema, plan, _ = bootstrap._process_delta_params(
            delta_command,
            schema,
            backend_params,
            stdmode=False,
            **keys,
        )

        compilerctx.state.current_tx().update_schema(schema)

        context = sd.CommandContext(**keys)
        edbcompiler.compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=plan,
            block=current_block,
            context=context,
        )

        # TODO: Should we batch them all up?
        patch = current_block.to_string()

        if debug.flags.delta_execute:
            debug.header('Patch Script')
            debug.dump_code(patch, lexer='sql')

        try:
            await ctx.conn.sql_execute(patch.encode('utf-8'))
        except Exception:
            raise

    # Refresh the pg_catalog materialized views
    current_block = dbops.PLTopBlock()
    refresh = metaschema.generate_sql_information_schema_refresh(
        backend_params.instance_params.version
    )
    refresh.generate(current_block)
    patch = current_block.to_string()
    await ctx.conn.sql_execute(patch.encode('utf-8'))

    new_local_spec = config.load_spec_from_schema(
        schema,
        only_exts=True,
        # suppress validation because we might be in an intermediate state
        validate=False,
    )
    spec_json = config.spec_to_json(new_local_spec)
    await ctx.conn.sql_execute(trampoline.fixup_query(f'''\
        UPDATE
            edgedbinstdata_VER.instdata
        SET
            json = {pg_common.quote_literal(spec_json)}
        WHERE
            key = 'configspec_ext';
    ''').encode('utf-8'))

    # Compile the fixup script for the schema and stash it away
    schema_fixup = _compile_schema_fixup(ctx, schema, keys)
    await bootstrap._store_static_text_cache(
        ctx,
        f'schema_fixup_query',
        schema_fixup,
    )


DEP_CHECK_QUERY = r'''
with
-- Fetch all the object types we care about.
all_objs AS (
  select objs.oid, ns.nspname as nspname, objs.name, objs.typ
  from (
    select
      oid as oid, relname as name,
      (case when relkind = 'v' then 'view' else 'table' end) as typ,
      relnamespace as namespace
    from pg_catalog.pg_class
    union all
    select
      oid as oid, typname as name, 'type' as typ, typnamespace as namespace
    from pg_catalog.pg_type
    union all
    select
      oid as oid, proname as name, 'function' as typ, pronamespace as namespace
    from pg_catalog.pg_proc
  ) as objs
  inner join pg_catalog.pg_namespace ns on objs.namespace = ns.oid
),
-- Fetch pg_depend along with some special handling of internal deps.
cdeps AS (
  select dep.objid, dep.refobjid, dep.deptype
  from pg_catalog.pg_depend dep
  union
  -- if there is an incoming 'i' dep to an obj A from B, treat all
  -- other outgoing deps from B as outgoing from A. We do this because
  -- the actual query in a view is stored in a pg_rewrite that *depends on*
  -- the view. (Seems backward.)
  select i.refobjid, c.refobjid, c.deptype
  from pg_catalog.pg_depend i
  inner join pg_catalog.pg_depend c
  on i.objid = c.objid
  where i.refobjid != c.refobjid and i.deptype = 'i'
)
-- Get any dependencies from outside our namespaces into them.
select src.typ, src.nspname, src.name, tgt.typ, tgt.nspname, tgt.name
from all_objs src
inner join cdeps dep on src.oid = dep.objid
inner join all_objs tgt on tgt.oid = dep.refobjid
where true
and NOT src.nspname = ANY ({namespaces})
and tgt.nspname = ANY ({namespaces})
and dep.deptype != 'i'
'''


async def _delete_schemas(
    conn: PGCon,
    to_delete: Sequence[str]
) -> None:
    # To add a bit more safety, check whether there are any
    # dependencies on the modules we want to delete from outside those
    # modules since the only way to delete non-empty schemas in
    # postgres is CASCADE.
    namespaces = (
        f'ARRAY[{", ".join(pg_common.quote_literal(k) for k in to_delete)}]'
    )
    qry = DEP_CHECK_QUERY.format(namespaces=namespaces)
    existing_deps = await conn.sql_fetch(qry.encode('utf-8'))
    if existing_deps:
        # All of the fields are text, so decode them all
        sdeps = [
            tuple(x.decode('utf-8') for x in row)
            for row in existing_deps
        ]

        messages = [
            f'{st} {pg_common.qname(ss, sn)} depends on '
            f'{tt} {pg_common.qname(ts, tn)}\n'
            for st, ss, sn, tt, ts, tn in sdeps
        ]

        raise AssertionError(
            'Dependencies to old schemas still exist: \n%s'
            % ''.join(messages)
        )

    # It is *really* dumb the way that CASCADE works in postgres.
    await conn.sql_execute(f"""
        drop schema {', '.join(to_delete)} cascade
    """.encode('utf-8'))


async def _get_namespaces(
    conn: PGCon,
) -> list[str]:
    return json.loads(await conn.sql_fetch_val("""
        select json_agg(nspname) from pg_namespace
        where nspname like 'edgedb%\\_v%'
    """.encode('utf-8')))


async def _finalize_one(
    ctx: bootstrap.BootstrapContext,
) -> None:
    conn = ctx.conn

    # If the upgrade is already finalized, skip it. This lets us be
    # resilient to crashes during the finalization process, which may
    # leave some databases upgraded but not all.
    if (await instdata.get_instdata(conn, 'upgrade_finalized', 'text')) == b'1':
        logger.info(f"Database upgrade already finalized")
        return

    trampoline_query = await instdata.get_instdata(
        conn, 'trampoline_pivot_query', 'text')
    fixup_query = await instdata.get_instdata(
        conn, 'schema_fixup_query', 'text')

    await conn.sql_execute(trampoline_query)
    if fixup_query:
        await conn.sql_execute(fixup_query)

    namespaces = await _get_namespaces(ctx.conn)

    cur_suffix = pg_common.versioned_schema("")
    to_delete = [x for x in namespaces if not x.endswith(cur_suffix)]

    await _delete_schemas(conn, to_delete)

    await bootstrap._store_static_text_cache(
        ctx,
        f'upgrade_finalized',
        '1',
    )


async def _get_databases(
    ctx: bootstrap.BootstrapContext,
) -> list[str]:
    cluster = ctx.cluster

    tpl_db = cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB)
    conn = await cluster.connect(
        source_description="inplace upgrade",
        database=tpl_db
    )

    # FIXME: Use the sys query instead?
    try:
        databases = json.loads(await conn.sql_fetch_val(
            trampoline.fixup_query("""
                SELECT json_agg(name) FROM edgedb_VER."_SysBranch";
            """).encode('utf-8'),
        ))
    finally:
        conn.terminate()

    # DEBUG VELOCITY HACK: You can add a failing database to EARLY
    # when trying to upgrade the whole suite.
    #
    # Note: We put template last, since when deleting, we need it to
    # stay around so we can query all branches.
    EARLY: tuple[str, ...] = ()
    databases.sort(
        key=lambda k: (k == edbdef.EDGEDB_TEMPLATE_DB, k not in EARLY, k)
    )

    return databases


async def _rollback_one(
    ctx: bootstrap.BootstrapContext,
) -> None:
    conn = ctx.conn

    namespaces = await _get_namespaces(conn)
    if pg_common.versioned_schema("edgedb") not in namespaces:
        logger.info(f"Database already rolled back or not prepared; skipping")
        return

    if (await instdata.get_instdata(conn, 'upgrade_finalized', 'text')) == b'1':
        logger.info(f"Database upgrade already finalized")
        raise errors.ConfigurationError(
            f"attempting to rollback database that has already begun "
            f"finalization: retry finalize instead"
        )

    cur_suffix = pg_common.versioned_schema("")
    to_delete = [x for x in namespaces if x.endswith(cur_suffix)]

    await _delete_schemas(conn, to_delete)


async def _rollback_all(
    ctx: bootstrap.BootstrapContext,
) -> None:
    cluster = ctx.cluster
    databases = await _get_databases(ctx)

    for database in databases:
        if database == os.environ.get(
            'EDGEDB_UPGRADE_ROLLBACK_ERROR_INJECTION'
        ):
            raise AssertionError(f'failure injected on {database}')

        conn = bootstrap.PGConnectionProxy(
            cluster,
            source_description='inplace upgrade: rollback all',
            dbname=cluster.get_db_name(database),
        )
        try:
            subctx = dataclasses.replace(ctx, conn=conn)

            logger.info(f"Rolling back preparation of database '{database}'")
            await _rollback_one(ctx=subctx)
        finally:
            conn.terminate()


async def _upgrade_all(
    ctx: bootstrap.BootstrapContext,
) -> None:
    cluster = ctx.cluster

    state = (await bootstrap._bootstrap(ctx)).state
    databases = await _get_databases(ctx)

    assert ctx.args.inplace_upgrade_prepare
    with open(ctx.args.inplace_upgrade_prepare) as f:
        upgrade_data = json.load(f)

    for database in databases:
        if database == edbdef.EDGEDB_TEMPLATE_DB:
            continue

        conn = bootstrap.PGConnectionProxy(
            cluster,
            source_description="inplace upgrade: upgrade all",
            dbname=cluster.get_db_name(database)
        )
        try:
            subctx = dataclasses.replace(ctx, conn=conn)

            logger.info(f"Upgrading database '{database}'")
            await bootstrap._bootstrap(ctx=subctx, no_template=True)

            logger.info(f"Populating schema tables for '{database}'")
            await _upgrade_one(
                ctx=subctx,
                state=state,
                upgrade_data=upgrade_data.get(database),
            )
        finally:
            conn.terminate()


async def _finalize_all(
    ctx: bootstrap.BootstrapContext,
) -> None:
    cluster = ctx.cluster
    databases = await _get_databases(ctx)

    async def go(
        message: str,
        finish_message: Optional[str],
        final_command: bytes,
        inject_failure_on: Optional[str]=None,
    ) -> None:
        for database in databases:
            conn = await cluster.connect(
                source_description="inplace upgrade: finish",
                database=cluster.get_db_name(database)
            )
            try:
                subctx = dataclasses.replace(ctx, conn=conn)

                logger.info(f"{message} database '{database}'")
                await conn.sql_execute(b'START TRANSACTION')
                # DEBUG HOOK: Inject a failure if specified
                if database == inject_failure_on:
                    raise AssertionError(f'failure injected on {database}')

                await _finalize_one(subctx)
                await conn.sql_execute(final_command)
                if finish_message:
                    logger.info(f"{finish_message} database '{database}'")
            finally:
                conn.terminate()

    inject_failure = os.environ.get('EDGEDB_UPGRADE_FINALIZE_ERROR_INJECTION')

    # Test all of the pivots in transactions we rollback, to make sure
    # that they work. This ensures that if there is a bug in the pivot
    # scripts on some database, we fail before any irreversible
    # changes are made to any database.
    #
    # *Then*, apply them all for real. They may fail
    # when applying for real, but that should be due to a crash or
    # some such, and so the user should be able to retry.
    #
    # We wanted to apply them all inside transactions and then commit
    # the transactions, but that requires holding open potentially too
    # many connections.
    await go("Testing pivot of", None, b'ROLLBACK')
    await go("Pivoting", "Finished pivoting", b'COMMIT', inject_failure)


async def inplace_upgrade(
    cluster: pgcluster.BaseCluster,
    args: edbargs.ServerConfig,
) -> None:
    """Perform some or all of the inplace upgrade operations"""
    pgconn = bootstrap.PGConnectionProxy(
        cluster,
        source_description="inplace_upgrade"
    )
    ctx = bootstrap.BootstrapContext(cluster=cluster, conn=pgconn, args=args)

    try:
        # XXX: Do we need to do this?
        mode = await bootstrap._get_cluster_mode(ctx)
        ctx = dataclasses.replace(ctx, mode=mode)

        if args.inplace_upgrade_rollback:
            await _rollback_all(ctx)

        if args.inplace_upgrade_prepare:
            await _upgrade_all(ctx)

        if args.inplace_upgrade_finalize:
            await _finalize_all(ctx)

    finally:
        pgconn.terminate()
