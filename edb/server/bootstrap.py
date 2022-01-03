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

import textwrap
from typing import *

import enum
import json
import logging
import os
import pathlib
import pickle
import re

from edb import buildmeta
from edb import errors

from edb import edgeql
from edb import _edgeql_rust
from edb.ir import statypes
from edb.edgeql import ast as qlast

from edb.common import context as parser_context
from edb.common import debug
from edb.common import devmode
from edb.common import exceptions
from edb.common import uuidgen

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import std as s_std

from edb.server import args as edbargs
from edb.server import config
from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import pgcluster

from edb.pgsql import common as pg_common
from edb.pgsql import dbops
from edb.pgsql import delta as delta_cmds
from edb.pgsql import metaschema
from edb.pgsql import params
from edb.pgsql.common import quote_ident as qi
from edb.pgsql.common import quote_literal as ql

from edgedb import scram

if TYPE_CHECKING:
    import uuid

    from asyncpg import connection as asyncpg_con


logger = logging.getLogger('edb.server')


class ClusterMode(enum.IntEnum):
    pristine = 0
    regular = 1
    single_role = 2
    single_database = 3


class BootstrapContext(NamedTuple):

    cluster: pgcluster.BaseCluster
    conn: asyncpg_con.Connection
    args: edbargs.ServerConfig
    mode: Optional[ClusterMode] = None


async def _execute(conn, query):
    return await metaschema._execute_sql_script(conn, query)


async def _execute_block(conn, block: dbops.SQLBlock) -> None:

    if not block.is_transactional():
        stmts = block.get_statements()
    else:
        stmts = [block.to_string()]

    for stmt in stmts:
        await _execute(conn, stmt)


async def _execute_edgeql_ddl(
    schema: s_schema.Schema_T,
    ddltext: str,
    stdmode: bool = True,
) -> s_schema.Schema_T:
    context = sd.CommandContext(stdmode=stdmode)

    for ddl_cmd in edgeql.parse_block(ddltext):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=stdmode)

        schema = delta_command.apply(schema, context)  # type: ignore

    return schema


async def _ensure_edgedb_supergroup(
    ctx: BootstrapContext,
    role_name: str,
    *,
    member_of: Iterable[str] = (),
    members: Iterable[str] = (),
) -> None:
    member_of = set(member_of)
    backend_params = ctx.cluster.get_runtime_params()
    superuser_role = backend_params.instance_params.base_superuser
    if superuser_role:
        # If the cluster is exposing an explicit superuser role,
        # become a member of that instead of creating a superuser
        # role directly.
        member_of.add(superuser_role)

    pg_role_name = ctx.cluster.get_role_name(role_name)

    role = dbops.Role(
        name=pg_role_name,
        superuser=backend_params.has_superuser_access,
        allow_login=False,
        allow_createdb=True,
        allow_createrole=True,
        membership=member_of,
        members=members,
    )

    create_role = dbops.CreateRole(
        role,
        neg_conditions=[dbops.RoleExists(pg_role_name)],
    )

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(ctx.conn, block)


async def _ensure_edgedb_role(
    ctx: BootstrapContext,
    role_name: str,
    *,
    superuser: bool = False,
    builtin: bool = False,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    member_of = set()
    if superuser:
        member_of.add(edbdef.EDGEDB_SUPERGROUP)

    if objid is None:
        objid = uuidgen.uuid1mc()

    members = set()
    login_role = ctx.cluster.get_connection_params().user
    sup_role = ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER)
    if login_role != sup_role:
        members.add(login_role)

    backend_params = ctx.cluster.get_runtime_params()
    pg_role_name = ctx.cluster.get_role_name(role_name)
    role = dbops.Role(
        name=pg_role_name,
        superuser=superuser and backend_params.has_superuser_access,
        allow_login=True,
        allow_createdb=True,
        allow_createrole=True,
        membership=[ctx.cluster.get_role_name(m) for m in member_of],
        members=members,
        metadata=dict(
            id=str(objid),
            name=role_name,
            tenant_id=backend_params.tenant_id,
            builtin=builtin,
        ),
    )

    create_role = dbops.CreateRole(
        role,
        neg_conditions=[dbops.RoleExists(pg_role_name)],
    )

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(ctx.conn, block)

    return objid


async def _get_cluster_mode(ctx: BootstrapContext) -> ClusterMode:
    backend_params = ctx.cluster.get_runtime_params()
    tenant_id = backend_params.tenant_id

    # First, check the existence of EDGEDB_SUPERGROUP - the role which is
    # usually created at the beginning of bootstrap.
    is_default_tenant = tenant_id == buildmeta.get_default_tenant_id()
    if is_default_tenant:
        result = await ctx.conn.fetch('''
            SELECT
                r.rolname
            FROM
                pg_catalog.pg_roles AS r
            WHERE
                r.rolname LIKE ('%' || $1)
        ''', edbdef.EDGEDB_SUPERGROUP)
    else:
        result = await ctx.conn.fetch('''
            SELECT
                r.rolname
            FROM
                pg_catalog.pg_roles AS r
            WHERE
                r.rolname = $1
        ''', ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERGROUP))

    if result:
        if not is_default_tenant or not ctx.args.ignore_other_tenants:
            return ClusterMode.regular

        for row in result:
            rolname = row['rolname']
            other_tenant_id = rolname[: -(len(edbdef.EDGEDB_SUPERGROUP) + 1)]
            if other_tenant_id == tenant_id:
                return ClusterMode.regular

    # Then, check if the current database was bootstrapped in single-db mode.
    has_instdata = await ctx.conn.fetch('''
        SELECT
            tablename
        FROM
            pg_catalog.pg_tables
        WHERE
            schemaname = 'edgedbinstdata'
            AND tablename = 'instdata'
    ''')
    if has_instdata:
        return ClusterMode.single_database

    # At last, check for single-role-bootstrapped instance by trying to find
    # the EdgeDB System DB with the assumption that we are not running in
    # single-db mode. If not found, this is a pristine backend cluster.
    sys_db = await _find_system_db(ctx)
    if sys_db:
        return ClusterMode.single_role
    else:
        return ClusterMode.pristine


async def _create_edgedb_template_database(
    ctx: BootstrapContext,
) -> uuid.UUID:
    backend_params = ctx.cluster.get_runtime_params()
    have_c_utf8 = backend_params.has_c_utf8_locale

    logger.info('Creating template database...')
    block = dbops.SQLBlock()
    dbid = uuidgen.uuid1mc()
    db = dbops.Database(
        ctx.cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB),
        owner=ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER),
        is_template=True,
        lc_collate='C',
        lc_ctype='C.UTF-8' if have_c_utf8 else 'en_US.UTF-8',
        encoding='UTF8',
        metadata=dict(
            id=str(dbid),
            tenant_id=backend_params.tenant_id,
            name=edbdef.EDGEDB_TEMPLATE_DB,
            builtin=True,
        ),
    )

    dbops.CreateDatabase(db, template='template0').generate(block)
    await _execute_block(ctx.conn, block)
    return dbid


async def _store_static_bin_cache(
    ctx: BootstrapContext,
    key: str,
    data: bytes,
) -> None:

    text = f"""\
        INSERT INTO edgedbinstdata.instdata (key, bin)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_bytea_literal(data)}::bytea
        )
    """

    await _execute(ctx.conn, text)


async def _store_static_text_cache(
    ctx: BootstrapContext,
    key: str,
    data: str,
) -> None:

    text = f"""\
        INSERT INTO edgedbinstdata.instdata (key, text)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_literal(data)}::text
        )
    """

    await _execute(ctx.conn, text)


async def _store_static_json_cache(
    ctx: BootstrapContext,
    key: str,
    data: str,
) -> None:

    text = f"""\
        INSERT INTO edgedbinstdata.instdata (key, json)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_literal(data)}::jsonb
        )
    """

    await _execute(ctx.conn, text)


def _process_delta(ctx, delta, schema):
    """Adapt and process the delta command."""

    if debug.flags.delta_plan:
        debug.header('Delta Plan')
        debug.dump(delta, schema=schema)

    context = sd.CommandContext()
    context.stdmode = True

    if not delta.canonical:
        # Canonicalize
        sd.apply(delta, schema=schema)

    delta = delta_cmds.CommandMeta.adapt(delta)
    context = sd.CommandContext(
        stdmode=True,
        backend_runtime_params=ctx.cluster.get_runtime_params(),
    )
    schema = sd.apply(delta, schema=schema, context=context)

    if debug.flags.delta_pgsql_plan:
        debug.header('PgSQL Delta Plan')
        debug.dump(delta, schema=schema)

    return schema, delta


def compile_bootstrap_script(
    compiler: edbcompiler.Compiler,
    schema: s_schema.Schema,
    eql: str,
    *,
    single_statement: bool = False,
    expected_cardinality_one: bool = False,
    output_format: edbcompiler.IoFormat = edbcompiler.IoFormat.JSON,
) -> Tuple[s_schema.Schema, str]:

    ctx = edbcompiler.new_compiler_context(
        user_schema=schema,
        single_statement=single_statement,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=True,
        output_format=output_format,
        bootstrap_mode=True,
    )

    return edbcompiler.compile_edgeql_script(compiler, ctx, eql)


def compile_single_query(
    eql: str,
    compiler: edbcompiler.Compiler,
    compilerctx: edbcompiler.CompileContext,
) -> str:
    ql_source = edgeql.Source.from_string(eql)
    units = compiler._compile(ctx=compilerctx, source=ql_source)
    assert len(units) == 1 and len(units[0].sql) == 1
    return units[0].sql[0].decode()


class StdlibBits(NamedTuple):

    #: User-visible std.
    stdschema: s_schema.Schema
    #: Shadow extended schema for reflection..
    reflschema: s_schema.Schema
    #: Standard portion of the global schema
    global_schema: s_schema.Schema
    #: SQL text of the procedure to initialize `std` in Postgres.
    sqltext: str
    #: A set of ids of all types in std.
    types: Set[uuid.UUID]
    #: Schema class reflection layout.
    classlayout: Dict[Type[s_obj.Object], s_refl.SchemaTypeLayout]
    #: Schema introspection SQL query.
    local_intro_query: str
    #: Global object introspection SQL query.
    global_intro_query: str


async def _make_stdlib(
    ctx: BootstrapContext,
    testmode: bool,
    global_ids: Mapping[str, uuid.UUID],
) -> StdlibBits:
    schema = s_schema.ChainedSchema(
        s_schema.FlatSchema(),
        s_schema.FlatSchema(),
        s_schema.FlatSchema(),
    )
    schema, _ = s_mod.Module.create_in_schema(
        schema,
        name=sn.UnqualName('__derived__'),
    )

    current_block = dbops.PLTopBlock()

    std_texts = []
    for modname in s_schema.STD_SOURCES:
        std_texts.append(s_std.get_std_module_text(modname))

    if testmode:
        std_texts.append(s_std.get_std_module_text(sn.UnqualName('_testmode')))

    ddl_text = '\n'.join(std_texts)
    types: Set[uuid.UUID] = set()
    std_plans: List[sd.Command] = []

    for ddl_cmd in edgeql.parse_block(ddl_text):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta_command)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = _process_delta(ctx, delta_command, schema)
        std_plans.append(delta_command)

        types.update(plan.new_types)
        plan.generate(current_block)

    _, schema_version = s_std.make_schema_version(schema)
    schema, plan = _process_delta(ctx, schema_version, schema)
    std_plans.append(schema_version)
    plan.generate(current_block)

    stdglobals = '\n'.join([
        f'''CREATE SUPERUSER ROLE {edbdef.EDGEDB_SUPERUSER} {{
            SET id := <uuid>'{global_ids[edbdef.EDGEDB_SUPERUSER]}'
        }};''',
    ])

    schema = await _execute_edgeql_ddl(schema, stdglobals)

    _, global_schema_version = s_std.make_global_schema_version(schema)
    schema, plan = _process_delta(ctx, global_schema_version, schema)
    std_plans.append(global_schema_version)
    plan.generate(current_block)

    reflection = s_refl.generate_structure(schema)
    reflschema, reflplan = _process_delta(
        ctx, reflection.intro_schema_delta, schema)

    assert current_block is not None
    reflplan.generate(current_block)
    subblock = current_block.add_block()

    compiler = edbcompiler.new_compiler(
        std_schema=schema.get_top_schema(),
        reflection_schema=reflschema.get_top_schema(),
        schema_class_layout=reflection.class_layout,  # type: ignore
    )

    compilerctx = edbcompiler.new_compiler_context(
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        bootstrap_mode=True,
    )

    for std_plan in std_plans:
        compiler._compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=std_plan,
            block=subblock,
        )

    compilerctx = edbcompiler.new_compiler_context(
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        bootstrap_mode=True,
        internal_schema_mode=True,
    )
    compiler._compile_schema_storage_in_delta(
        ctx=compilerctx,
        delta=reflection.intro_schema_delta,
        block=subblock,
    )

    sqltext = current_block.to_string()

    compilerctx = edbcompiler.new_compiler_context(
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        schema_reflection_mode=True,
        output_format=edbcompiler.IoFormat.JSON_ELEMENTS,
    )

    # The introspection query bits are returned in chunks
    # because it's a large UNION and we currently generate SQL
    # that is much harder for Postgres to plan as opposed to a
    # straight flat UNION.
    sql_intro_local_parts = []
    sql_intro_global_parts = []
    for intropart in reflection.local_intro_parts:
        sql_intro_local_parts.append(
            compile_single_query(
                intropart,
                compiler=compiler,
                compilerctx=compilerctx,
            ),
        )

    for intropart in reflection.global_intro_parts:
        sql_intro_global_parts.append(
            compile_single_query(
                intropart,
                compiler=compiler,
                compilerctx=compilerctx,
            ),
        )

    local_intro_sql = ' UNION ALL '.join(sql_intro_local_parts)
    local_intro_sql = f'''
        WITH intro(c) AS ({local_intro_sql})
        SELECT json_agg(intro.c) FROM intro
    '''

    global_intro_sql = ' UNION ALL '.join(sql_intro_global_parts)
    global_intro_sql = f'''
        WITH intro(c) AS ({global_intro_sql})
        SELECT json_agg(intro.c) FROM intro
    '''

    return StdlibBits(
        stdschema=schema.get_top_schema(),
        reflschema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        sqltext=sqltext,
        types=types,
        classlayout=reflection.class_layout,
        local_intro_query=local_intro_sql,
        global_intro_query=global_intro_sql,
    )


async def _amend_stdlib(
    ctx: BootstrapContext,
    ddl_text: str,
    stdlib: StdlibBits,
) -> Tuple[StdlibBits, str]:
    schema = stdlib.stdschema
    reflschema = stdlib.reflschema

    topblock = dbops.PLTopBlock()
    plans = []

    context = sd.CommandContext()
    context.stdmode = True

    for ddl_cmd in edgeql.parse_block(ddl_text):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta_command)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = _process_delta(ctx, delta_command, schema)
        reflschema = delta_command.apply(reflschema, context)
        plan.generate(topblock)
        plans.append(plan)

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=reflschema,
        schema_class_layout=stdlib.classlayout,  # type: ignore
    )

    compilerctx = edbcompiler.new_compiler_context(
        user_schema=schema
    )
    for plan in plans:
        compiler._compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=plan,
            block=topblock,
        )

    sqltext = topblock.to_string()

    return stdlib._replace(stdschema=schema, reflschema=reflschema), sqltext


async def _init_stdlib(
    ctx: BootstrapContext,
    testmode: bool,
    global_ids: Mapping[str, uuid.UUID],
) -> Tuple[StdlibBits, config.Spec, edbcompiler.Compiler]:
    in_dev_mode = devmode.is_in_dev_mode()
    conn = ctx.conn
    cluster = ctx.cluster

    specified_cache_dir = os.environ.get('_EDGEDB_WRITE_DATA_CACHE_TO')
    if not specified_cache_dir:
        cache_dir = None
    else:
        cache_dir = pathlib.Path(specified_cache_dir)

    stdlib_cache = f'backend-stdlib.pickle'
    tpldbdump_cache = f'backend-tpldbdump.sql'

    src_hash = buildmeta.hash_dirs(
        buildmeta.get_cache_src_dirs(), extra_files=[__file__],
    )

    stdlib = buildmeta.read_data_cache(
        src_hash, stdlib_cache, source_dir=cache_dir)
    tpldbdump = buildmeta.read_data_cache(
        src_hash, tpldbdump_cache, source_dir=cache_dir, pickled=False)

    if stdlib is None:
        logger.info('Compiling the standard library...')
        stdlib = await _make_stdlib(ctx, in_dev_mode or testmode, global_ids)

    logger.info('Creating the necessary PostgreSQL extensions...')
    await metaschema.create_pg_extensions(conn)

    config_spec = config.load_spec_from_schema(stdlib.stdschema)
    config.set_settings(config_spec)

    if tpldbdump is None:
        logger.info('Populating internal SQL structures...')
        await metaschema.bootstrap(conn, config_spec)
        logger.info('Executing the standard library...')
        await _execute_ddl(conn, stdlib.sqltext)

        if in_dev_mode or specified_cache_dir:
            tpl_db_name = edbdef.EDGEDB_TEMPLATE_DB
            tpl_pg_db_name = cluster.get_db_name(tpl_db_name)
            tpl_pg_db_name_dyn = (
                f"edgedb.get_database_backend_name({ql(tpl_db_name)})")
            tpldbdump = await cluster.dump_database(
                tpl_pg_db_name,
                exclude_schemas=['edgedbinstdata', 'edgedbext'],
                dump_object_owners=False,
            )

            # Excluding the "edgedbext" schema above apparently
            # doesn't apply to extensions created in that schema,
            # so we have to resort to commenting out extension
            # statements in the dump.
            tpldbdump = re.sub(
                rb'^(CREATE|COMMENT ON) EXTENSION.*$',
                rb'-- \g<0>',
                tpldbdump,
                flags=re.MULTILINE,
            )

            global_metadata = await conn.fetchval(
                f'SELECT edgedb.get_database_metadata({ql(tpl_db_name)})',
            )
            global_metadata = json.loads(global_metadata)

            pl_block = dbops.PLTopBlock()

            set_metadata_text = dbops.SetMetadata(
                dbops.Database(name='__dummy_placeholder_database__'),
                global_metadata,
            ).code(pl_block)
            set_metadata_text = set_metadata_text.replace(
                '__dummy_placeholder_database__',
                f"' || quote_ident({tpl_pg_db_name_dyn}) || '",
            )

            set_single_db_metadata_text = dbops.SetSingleDBMetadata(
                edbdef.EDGEDB_TEMPLATE_DB, global_metadata
            ).code(pl_block)

            pl_block.add_command(textwrap.dedent(f"""\
                IF (edgedb.get_backend_capabilities()
                    & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
                THEN
                {textwrap.indent(set_metadata_text, '    ')}
                ELSE
                {textwrap.indent(set_single_db_metadata_text, '    ')}
                END IF
                """))

            text = pl_block.to_string()

            tpldbdump += b'\n' + text.encode('utf-8')

            buildmeta.write_data_cache(
                tpldbdump,
                src_hash,
                tpldbdump_cache,
                pickled=False,
                target_dir=cache_dir,
            )

            buildmeta.write_data_cache(
                stdlib,
                src_hash,
                stdlib_cache,
                target_dir=cache_dir,
            )
    else:
        logger.info('Initializing the standard library...')
        await metaschema._execute_sql_script(conn, tpldbdump.decode('utf-8'))
        # Restore the search_path as the dump might have altered it.
        await conn.execute(
            "SELECT pg_catalog.set_config('search_path', 'edgedb', false)")

    if not in_dev_mode and testmode:
        # Running tests on a production build.
        stdlib, testmode_sql = await _amend_stdlib(
            ctx,
            s_std.get_std_module_text(sn.UnqualName('_testmode')),
            stdlib,
        )
        await conn.execute(testmode_sql)
        # _testmode includes extra config settings, so make sure
        # those are picked up.
        config_spec = config.load_spec_from_schema(stdlib.stdschema)
        config.set_settings(config_spec)

    # Make sure that schema backend_id properties are in sync with
    # the database.

    compiler = edbcompiler.new_compiler(
        std_schema=stdlib.stdschema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
    )
    _, sql = compile_bootstrap_script(
        compiler,
        stdlib.reflschema,
        '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .builtin AND NOT (.abstract ?? False);
        ''',
        expected_cardinality_one=False,
        single_statement=True,
    )
    schema = stdlib.stdschema
    typemap = await conn.fetchval(sql)
    for entry in json.loads(typemap):
        t = schema.get_by_id(uuidgen.UUID(entry['id']))
        schema = t.set_field_value(
            schema, 'backend_id', entry['backend_id'])

    stdlib = stdlib._replace(stdschema=schema)

    await _store_static_bin_cache(
        ctx,
        'stdschema',
        pickle.dumps(schema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        ctx,
        'reflschema',
        pickle.dumps(stdlib.reflschema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        ctx,
        'global_schema',
        pickle.dumps(stdlib.global_schema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        ctx,
        'classlayout',
        pickle.dumps(stdlib.classlayout, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_text_cache(
        ctx,
        'local_intro_query',
        stdlib.local_intro_query,
    )

    await _store_static_text_cache(
        ctx,
        'global_intro_query',
        stdlib.global_intro_query,
    )

    await metaschema.generate_support_views(
        conn, stdlib.reflschema, cluster.get_runtime_params()
    )
    await metaschema.generate_support_functions(conn, stdlib.reflschema)

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
    )

    await metaschema.generate_more_support_functions(
        conn, compiler, stdlib.reflschema, testmode)

    if tpldbdump is not None:
        # When we restore a database from a dump, OIDs for non-system
        # Postgres types might get skewed as they are not part of the dump.
        # A good example of that is `std::bigint` which is implemented as
        # a custom domain type. The OIDs are stored under
        # `schema::Object.backend_id` property and are injected into
        # array query arguments.
        #
        # The code below re-syncs backend_id properties of EdgeDB builtin
        # types with the actual OIDs in the DB.

        compiler = edbcompiler.new_compiler(
            std_schema=stdlib.stdschema,
            reflection_schema=stdlib.reflschema,
            schema_class_layout=stdlib.classlayout,
        )
        _, sql = compile_bootstrap_script(
            compiler,
            stdlib.reflschema,
            '''
            UPDATE schema::Type
            FILTER
                .builtin
                AND NOT (.abstract ?? False)
                AND schema::Type IS schema::ScalarType | schema::Tuple
            SET {
                backend_id := sys::_get_pg_type_for_edgedb_type(
                    .id,
                    <uuid>{}
                )
            }
            ''',
            expected_cardinality_one=False,
            single_statement=True,
        )
        await conn.execute(sql)

        _, sql = compile_bootstrap_script(
            compiler,
            stdlib.reflschema,
            '''
            UPDATE schema::Array
            FILTER
                .builtin
                AND NOT (.abstract ?? False)
            SET {
                backend_id := sys::_get_pg_type_for_edgedb_type(
                    .id,
                    .element_type.id,
                )
            }
            ''',
            expected_cardinality_one=False,
            single_statement=True,
        )
        await conn.execute(sql)

    await _store_static_json_cache(
        ctx,
        'configspec',
        config.spec_to_json(config_spec),
    )

    return stdlib, config_spec, compiler


async def _execute_ddl(conn, sql_text):
    try:
        if debug.flags.bootstrap:
            debug.header('Delta Script')
            debug.dump_code(sql_text, lexer='sql')

        await conn.execute(sql_text)

    except Exception as e:
        position = getattr(e, 'position', None)
        internal_position = getattr(e, 'internal_position', None)
        context = getattr(e, 'context', '')
        if context:
            pl_func_line = re.search(
                r'^PL/pgSQL function inline_code_block line (\d+).*',
                context, re.M)

            if pl_func_line:
                pl_func_line = int(pl_func_line.group(1))
        else:
            pl_func_line = None
        point = None

        if position is not None:
            point = int(position)
            text = e.query
            if text is None:
                # Parse errors
                text = sql_text

        elif internal_position is not None:
            point = int(internal_position)
            text = e.internal_query

        elif pl_func_line:
            point = _edgeql_rust.offset_of_line(sql_text, pl_func_line)
            text = sql_text

        if point is not None:
            context = parser_context.ParserContext(
                'query', text, start=point, end=point, context_lines=30)
            exceptions.replace_context(e, context)

        raise


async def _init_defaults(schema, compiler, conn):
    script = '''
        CREATE MODULE default;
    '''

    schema, sql = compile_bootstrap_script(compiler, schema, script)
    await _execute_ddl(conn, sql)
    return schema


async def _populate_data(schema, compiler, conn):
    script = '''
        INSERT std::FreeObject;
    '''

    schema, sql = compile_bootstrap_script(compiler, schema, script)
    await _execute_ddl(conn, sql)
    return schema


async def _configure(
    ctx: BootstrapContext,
    config_spec: config.Spec,
    schema: s_schema.Schema,
    compiler: edbcompiler.Compiler,
) -> None:
    settings: Mapping[str, config.SettingValue] = {}

    config_json = config.to_json(config_spec, settings, include_source=False)
    block = dbops.PLTopBlock()
    metadata = {'sysconfig': json.loads(config_json)}
    if ctx.cluster.get_runtime_params().has_create_database:
        dbops.UpdateMetadata(
            dbops.Database(
                name=ctx.cluster.get_db_name(edbdef.EDGEDB_SYSTEM_DB)
            ),
            metadata,
        ).generate(block)
    else:
        dbops.UpdateSingleDBMetadata(
            edbdef.EDGEDB_SYSTEM_DB, metadata,
        ).generate(block)

    await _execute_block(ctx.conn, block)

    backend_params = ctx.cluster.get_runtime_params()
    for setname in config_spec:
        setting = config_spec[setname]
        if (
            setting.backend_setting
            and setting.default is not None
            and (
                # Do not attempt to run CONFIGURE INSTANCE on
                # backends that don't support it.
                # TODO: this should be replaced by instance-wide
                #       emulation at backend connection time.
                backend_params.has_configfile_access
            )
        ):
            if isinstance(setting.default, statypes.Duration):
                val = f'<std::duration>"{setting.default.to_iso8601()}"'
            else:
                val = repr(setting.default)
            script = f'''
                CONFIGURE INSTANCE SET {setting.name} := {val};
            '''
            schema, sql = compile_bootstrap_script(compiler, schema, script)
            await _execute_ddl(ctx.conn, sql)


async def _compile_sys_queries(
    ctx: BootstrapContext,
    schema: s_schema.Schema,
    compiler: edbcompiler.Compiler,
    config_spec: config.Spec,
) -> None:
    queries = {}

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        'SELECT cfg::get_config_json()',
        expected_cardinality_one=True,
        single_statement=True,
    )

    queries['config'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        "SELECT cfg::get_config_json(sources := ['database'])",
        expected_cardinality_one=True,
        single_statement=True,
    )

    queries['dbconfig'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        "SELECT cfg::get_config_json(max_source := 'system override')",
        expected_cardinality_one=True,
        single_statement=True,
    )

    queries['sysconfig'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        'SELECT (SELECT sys::Database FILTER NOT .builtin).name',
        expected_cardinality_one=False,
        single_statement=True,
    )

    queries['listdbs'] = sql

    role_query = '''
        SELECT sys::Role {
            name,
            superuser,
            password,
        };
    '''
    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        role_query,
        expected_cardinality_one=False,
        single_statement=True,
    )
    queries['roles'] = sql

    tids_query = '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .id IN <uuid>json_array_unpack(<json>$ids);
    '''
    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        tids_query,
        expected_cardinality_one=False,
        single_statement=True,
    )

    queries['backend_tids'] = sql

    report_settings: list[str] = []
    for setname in config_spec:
        setting = config_spec[setname]
        if setting.report:
            report_settings.append(setname)

    report_configs_query = f'''
        SELECT assert_single(cfg::Config {{
            {', '.join(report_settings)}
        }});
    '''

    units = compiler._compile(
        ctx=edbcompiler.new_compiler_context(
            user_schema=schema,
            single_statement=True,
            expected_cardinality_one=True,
            json_parameters=False,
            output_format=edbcompiler.IoFormat.BINARY,
            bootstrap_mode=True,
        ),
        source=edgeql.Source.from_string(report_configs_query))
    assert len(units) == 1 and len(units[0].sql) == 1

    report_configs_typedesc = units[0].out_type_id + units[0].out_type_data
    queries['report_configs'] = units[0].sql[0].decode()

    await _store_static_json_cache(
        ctx,
        'sysqueries',
        json.dumps(queries),
    )

    await _store_static_bin_cache(
        ctx,
        'report_configs_typedesc',
        report_configs_typedesc,
    )


async def _populate_misc_instance_data(
    ctx: BootstrapContext,
) -> Dict[str, Any]:

    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.CreateSchema(name='edgedbinstdata'),
        dbops.CreateTable(dbops.Table(
            name=('edgedbinstdata', 'instdata'),
            columns=[
                dbops.Column(
                    name='key',
                    type='text',
                ),
                dbops.Column(
                    name='bin',
                    type='bytea',
                ),
                dbops.Column(
                    name='text',
                    type='text',
                ),
                dbops.Column(
                    name='json',
                    type='jsonb',
                ),
            ],
            constraints=[
                dbops.PrimaryKey(
                    table_name=('edgedbinstdata', 'instdata'),
                    columns=['key'],
                ),
            ],
        ))
    ])

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(ctx.conn, block)

    mock_auth_nonce = scram.generate_nonce()
    json_instance_data = {
        'version': dict(buildmeta.get_version_dict()),
        'catver': edbdef.EDGEDB_CATALOG_VERSION,
        'mock_auth_nonce': mock_auth_nonce,
    }

    await _store_static_json_cache(
        ctx,
        'instancedata',
        json.dumps(json_instance_data),
    )

    backend_params = ctx.cluster.get_runtime_params()
    instance_params = backend_params.instance_params
    await _store_static_json_cache(
        ctx,
        'backend_instance_params',
        json.dumps(instance_params._asdict()),
    )

    if not backend_params.has_create_role:
        json_single_role_metadata = {
            'id': str(uuidgen.uuid1mc()),
            'name': edbdef.EDGEDB_SUPERUSER,
            'tenant_id': backend_params.tenant_id,
            'builtin': False,
        }
        await _store_static_json_cache(
            ctx,
            'single_role_metadata',
            json.dumps(json_single_role_metadata),
        )

    if not backend_params.has_create_database:
        await _store_static_json_cache(
            ctx,
            f'{edbdef.EDGEDB_TEMPLATE_DB}metadata',
            json.dumps({}),
        )
        await _store_static_json_cache(
            ctx,
            f'{edbdef.EDGEDB_SYSTEM_DB}metadata',
            json.dumps({}),
        )
    return json_instance_data


async def _create_edgedb_database(
    ctx: BootstrapContext,
    database: str,
    owner: str,
    *,
    builtin: bool = False,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    logger.info(f'Creating database: {database}')
    block = dbops.SQLBlock()
    if objid is None:
        objid = uuidgen.uuid1mc()
    instance_params = ctx.cluster.get_runtime_params().instance_params
    db = dbops.Database(
        ctx.cluster.get_db_name(database),
        owner=ctx.cluster.get_role_name(owner),
        metadata=dict(
            id=str(objid),
            tenant_id=instance_params.tenant_id,
            name=database,
            builtin=builtin,
        ),
    )
    tpl_db = ctx.cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB)
    dbops.CreateDatabase(db, template=tpl_db).generate(block)
    await _execute_block(ctx.conn, block)
    return objid


async def _set_edgedb_database_metadata(
    ctx: BootstrapContext,
    database: str,
    *,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    logger.info(f'Configuring database: {database}')
    block = dbops.SQLBlock()
    if objid is None:
        objid = uuidgen.uuid1mc()
    instance_params = ctx.cluster.get_runtime_params().instance_params
    db = dbops.Database(ctx.cluster.get_db_name(database))
    metadata = dict(
        id=str(objid),
        tenant_id=instance_params.tenant_id,
        name=database,
        builtin=False,
    )
    dbops.SetMetadata(db, metadata).generate(block)
    await _execute_block(ctx.conn, block)
    return objid


def _pg_log_listener(conn, msg):
    if msg.severity_en == 'WARNING':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    logger.log(level, msg.message)


async def _get_instance_data(conn: Any) -> Dict[str, Any]:

    data = await conn.fetchval(
        "SELECT json FROM edgedbinstdata.instdata WHERE key = 'instancedata'"
    )

    return json.loads(data)


async def _find_system_db(ctx: BootstrapContext) -> Optional[str]:
    tenant_id = ctx.cluster.get_runtime_params().tenant_id
    is_default_tenant = tenant_id == buildmeta.get_default_tenant_id()

    if is_default_tenant:
        sys_db = await ctx.conn.fetchval(f'''
            SELECT datname
            FROM pg_database
            WHERE datname LIKE '%' || $1
            ORDER BY
                datname = $1,
                datname DESC
            LIMIT 1
        ''', edbdef.EDGEDB_SYSTEM_DB)
    else:
        sys_db = await ctx.conn.fetchval(f'''
            SELECT datname
            FROM pg_database
            WHERE datname = $1
        ''', ctx.cluster.get_db_name(edbdef.EDGEDB_SYSTEM_DB))

    return sys_db


async def _check_catalog_compatibility(
    ctx: BootstrapContext,
) -> asyncpg_con.Connection:
    if ctx.mode == ClusterMode.single_database:
        tenant_id = ctx.cluster.get_runtime_params().tenant_id
        sys_db = await ctx.conn.fetchval(f'''
            SELECT current_database()
            FROM edgedbinstdata.instdata
            WHERE key = '{edbdef.EDGEDB_TEMPLATE_DB}metadata'
            AND json->>'tenant_id' = '{tenant_id}'
        ''')
    else:
        sys_db = await _find_system_db(ctx)

    if not sys_db:
        raise errors.ConfigurationError(
            'database instance is corrupt',
            details=(
                f'The database instance does not appear to have been fully '
                f'initialized or has been corrupted.'
            )
        )

    conn = await ctx.cluster.connect(database=sys_db)

    try:
        instancedata = await _get_instance_data(conn)
        datadir_version = instancedata.get('version')
        if datadir_version:
            datadir_major = datadir_version.get('major')

        expected_ver = buildmeta.get_version()
        datadir_catver = instancedata.get('catver')
        expected_catver = edbdef.EDGEDB_CATALOG_VERSION

        status = dict(
            data_catalog_version=datadir_catver,
            expected_catalog_version=expected_catver,
        )

        if datadir_major != expected_ver.major:
            for status_sink in ctx.args.status_sinks:
                status_sink(f'INCOMPATIBLE={json.dumps(status)}')
            raise errors.ConfigurationError(
                'database instance incompatible with this version of EdgeDB',
                details=(
                    f'The database instance was initialized with '
                    f'EdgeDB version {datadir_major}, '
                    f'which is incompatible with this version '
                    f'{expected_ver.major}'
                ),
                hint=(
                    f'You need to recreate the instance and upgrade '
                    f'using dump/restore.'
                )
            )

        if datadir_catver != expected_catver:
            for status_sink in ctx.args.status_sinks:
                status_sink(f'INCOMPATIBLE={json.dumps(status)}')
            raise errors.ConfigurationError(
                'database instance incompatible with this version of EdgeDB',
                details=(
                    f'The database instance was initialized with '
                    f'EdgeDB format version {datadir_catver}, '
                    f'but this version of the server expects '
                    f'format version {expected_catver}'
                ),
                hint=(
                    f'You need to recreate the instance and upgrade '
                    f'using dump/restore.'
                )
            )
    except Exception:
        await conn.close()
        raise

    return conn


def _check_capabilities(ctx: BootstrapContext) -> None:
    caps = ctx.cluster.get_runtime_params().instance_params.capabilities
    for cap in ctx.args.backend_capability_sets.must_be_present:
        if not caps & cap:
            raise errors.ConfigurationError(
                f"the backend doesn't have necessary capability: "
                f"{cap.name}"
            )
    for cap in ctx.args.backend_capability_sets.must_be_absent:
        if caps & cap:
            raise errors.ConfigurationError(
                f"the backend was already bootstrapped with capability: "
                f"{cap.name}"
            )


async def _start(ctx: BootstrapContext) -> None:
    conn = await _check_catalog_compatibility(ctx)

    try:
        caps = await conn.fetchval("SELECT edgedb.get_backend_capabilities()")
        ctx.cluster.overwrite_capabilities(caps)
        _check_capabilities(ctx)

        compiler = edbcompiler.Compiler()
        await compiler.initialize_from_pg(conn)
        std_schema = compiler.get_std_schema()
        config_spec = config.load_spec_from_schema(std_schema)

        # Initialize global config
        config.set_settings(config_spec)

    finally:
        await conn.close()


async def _bootstrap_edgedb_super_roles(ctx: BootstrapContext) -> uuid.UUID:
    cluster = ctx.cluster

    await _ensure_edgedb_supergroup(
        ctx,
        edbdef.EDGEDB_SUPERGROUP,
    )

    superuser_uid = await _ensure_edgedb_role(
        ctx,
        edbdef.EDGEDB_SUPERUSER,
        superuser=True,
        builtin=True,
    )

    superuser = ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER)

    await _execute(ctx.conn, f'SET ROLE {qi(superuser)}')
    cluster.set_default_session_authorization(superuser)
    return superuser_uid


async def _bootstrap(ctx: BootstrapContext) -> None:
    args = ctx.args
    cluster = ctx.cluster

    if args.backend_capability_sets.must_be_absent:
        caps = cluster.get_runtime_params().instance_params.capabilities
        disabled = []
        for cap in args.backend_capability_sets.must_be_absent:
            if caps & cap:
                caps &= ~cap
                disabled.append(cap)
        if disabled:
            logger.info(f"the following backend capabilities are disabled: "
                        f"{', '.join(str(cap.name) for cap in disabled)}")
            cluster.overwrite_capabilities(caps)
    _check_capabilities(ctx)
    backend_params = cluster.get_runtime_params()

    if backend_params.has_create_role:
        superuser_uid = await _bootstrap_edgedb_super_roles(ctx)
    else:
        superuser_uid = uuidgen.uuid1mc()

    in_dev_mode = devmode.is_in_dev_mode()
    # Protect against multiple EdgeDB tenants from trying to bootstrap
    # on the same cluster in devmode, as that is both a waste of resources
    # and might result in broken stdlib cache.
    if in_dev_mode:
        bootstrap_lock = 0xEDB00001
        await ctx.conn.execute('SELECT pg_advisory_lock($1)', bootstrap_lock)

    if backend_params.has_create_database:
        new_template_db_id = await _create_edgedb_template_database(ctx)
        tpl_db = cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB)
        conn = await cluster.connect(database=tpl_db)
    else:
        new_template_db_id = uuidgen.uuid1mc()

    try:
        if backend_params.has_create_database:
            tpl_ctx = ctx._replace(conn=conn)
            conn.add_log_listener(_pg_log_listener)
        else:
            tpl_ctx = ctx

        await _populate_misc_instance_data(tpl_ctx)

        stdlib, config_spec, compiler = await _init_stdlib(
            tpl_ctx,
            testmode=args.testmode,
            global_ids={
                edbdef.EDGEDB_SUPERUSER: superuser_uid,
                edbdef.EDGEDB_TEMPLATE_DB: new_template_db_id,
            }
        )
        await _compile_sys_queries(
            tpl_ctx,
            stdlib.reflschema,
            compiler,
            config_spec,
        )

        schema = s_schema.FlatSchema()
        schema = await _init_defaults(schema, compiler, tpl_ctx.conn)
        schema = await _populate_data(schema, compiler, tpl_ctx.conn)
    finally:
        if in_dev_mode:
            await ctx.conn.execute(
                'SELECT pg_advisory_unlock($1)',
                bootstrap_lock,
            )

        if backend_params.has_create_database:
            await conn.close()

    if backend_params.has_create_database:
        await _create_edgedb_database(
            ctx,
            edbdef.EDGEDB_SYSTEM_DB,
            edbdef.EDGEDB_SUPERUSER,
            builtin=True,
        )

        conn = await cluster.connect(
            database=cluster.get_db_name(edbdef.EDGEDB_SYSTEM_DB))

        try:
            conn.add_log_listener(_pg_log_listener)
            await _configure(
                ctx._replace(conn=conn),
                config_spec=config_spec,
                schema=schema,
                compiler=compiler,
            )
        finally:
            await conn.close()
    else:
        await _configure(
            ctx,
            config_spec=config_spec,
            schema=schema,
            compiler=compiler,
        )

    if backend_params.has_create_database:
        await _create_edgedb_database(
            ctx,
            edbdef.EDGEDB_SUPERUSER_DB,
            edbdef.EDGEDB_SUPERUSER,
        )
    else:
        await _set_edgedb_database_metadata(
            ctx,
            edbdef.EDGEDB_SUPERUSER_DB,
        )

    if (
        backend_params.has_create_role
        and args.default_database_user
        and args.default_database_user != edbdef.EDGEDB_SUPERUSER
    ):
        await _ensure_edgedb_role(
            ctx,
            args.default_database_user,
            superuser=True,
        )

        def_role = ctx.cluster.get_role_name(args.default_database_user)
        await _execute(ctx.conn, f"SET ROLE {qi(def_role)}")

    if (
        backend_params.has_create_database
        and args.default_database
        and args.default_database != edbdef.EDGEDB_SUPERUSER_DB
    ):
        await _create_edgedb_database(
            ctx,
            args.default_database,
            args.default_database_user or edbdef.EDGEDB_SUPERUSER,
        )


async def ensure_bootstrapped(
    cluster: pgcluster.BaseCluster,
    args: edbargs.ServerConfig,
) -> bool:
    pgconn = await cluster.connect()
    pgconn.add_log_listener(_pg_log_listener)

    ctx = BootstrapContext(cluster=cluster, conn=pgconn, args=args)

    try:
        mode = await _get_cluster_mode(ctx)
        ctx = ctx._replace(mode=mode)
        if mode == ClusterMode.pristine:
            await _bootstrap(ctx)
            return True
        else:
            await _start(ctx)
            return False
    finally:
        await pgconn.close()
