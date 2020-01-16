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
from typing import *  # NoQA

import json
import logging
import pathlib
import pickle
import re

import immutables
import psutil

from edb import errors

from edb import edgeql

from edb.common import context as parser_context
from edb.common import debug
from edb.common import devmode
from edb.common import exceptions
from edb.common import uuidgen

from edb.schema import database as s_db
from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import modules as s_mod
from edb.schema import schema as s_schema
from edb.schema import std as s_std

from edb.server import buildmeta
from edb.server import config
from edb.server import compiler
from edb.server import defines as edbdef

from edb.pgsql import common as pg_common
from edb.pgsql import dbops
from edb.pgsql import delta as delta_cmds
from edb.pgsql import metaschema

from edgedb import scram

if TYPE_CHECKING:
    import uuid

    from . import pgcluster
    from asyncpg import connection as asyncpg_con


CACHE_SRC_DIRS = s_std.CACHE_SRC_DIRS + (
    (pathlib.Path(metaschema.__file__).parent, '.py'),
)


logger = logging.getLogger('edb.server')


async def _statement(conn, query, *args, method):
    logger.debug('query: %s, args: %s', query, args)
    return await getattr(conn, method)(query, *args)


async def _execute(conn, query, *args):
    return await _statement(conn, query, *args, method='execute')


async def _execute_block(conn, block: dbops.PLBlock) -> None:

    if not block.is_transactional():
        stmts = block.get_statements()
    else:
        stmts = [block.to_string()]
    if debug.flags.bootstrap:
        debug.header('Bootstrap')
        debug.dump_code(';\n'.join(stmts), lexer='sql')

    for stmt in stmts:
        await _execute(conn, stmt)


async def _ensure_edgedb_role(
    cluster,
    conn,
    username,
    *,
    membership=(),
    is_superuser=False,
) -> None:
    membership = set(membership)
    if is_superuser:
        superuser_role = cluster.get_superuser_role()
        if superuser_role:
            # If the cluster is exposing an explicit superuser role,
            # become a member of that instead of creating a superuser
            # role directly,
            membership.add(superuser_role)
            superuser_flag = False
        else:
            superuser_flag = True
    else:
        superuser_flag = False

    role = dbops.Role(
        name=username,
        is_superuser=superuser_flag,
        allow_login=True,
        allow_createdb=True,
        allow_createrole=True,
        membership=membership,
        metadata=dict(
            id=str(uuidgen.uuid1mc()),
        ),
    )

    create_role = dbops.CreateRole(
        role=role, neg_conditions=[dbops.RoleExists(username)])

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(conn, block)


async def _get_db_info(conn, dbname):
    result = await conn.fetchrow('''
        SELECT
            r.rolname,
            datistemplate,
            datallowconn
        FROM
            pg_catalog.pg_database d
            INNER JOIN pg_catalog.pg_roles r
                ON (d.datdba = r.oid)
        WHERE
            d.datname = $1
    ''', dbname)

    return result


async def _ensure_edgedb_template_database(cluster, conn):
    result = await _get_db_info(conn, edbdef.EDGEDB_TEMPLATE_DB)

    if not result:
        logger.info('Creating template database...')
        block = dbops.SQLBlock()
        db = dbops.Database(
            edbdef.EDGEDB_TEMPLATE_DB,
            owner=edbdef.EDGEDB_SUPERUSER,
            is_template=True,
            template='template0',
            lc_collate='C',
            lc_ctype=('C.UTF-8' if cluster.supports_c_utf8_locale()
                      else 'en_US.UTF-8'),
            encoding='UTF8',
            metadata=dict(
                id=str(uuidgen.uuid1mc()),
            ),
        )
        dbops.CreateDatabase(db).generate(block)
        await _execute_block(conn, block)

        return True
    else:
        alter = []
        alter_owner = False

        if not result['datistemplate']:
            alter.append('IS_TEMPLATE = true')

        if result['rolname'] != edbdef.EDGEDB_SUPERUSER:
            alter_owner = True

        if alter or alter_owner:
            logger.info('Altering template database parameters...')
            if alter:
                await _execute(
                    conn,
                    'ALTER DATABASE {} WITH {}'.format(
                        edbdef.EDGEDB_TEMPLATE_DB,
                        ' '.join(alter)))

            if alter_owner:
                await _execute(
                    conn,
                    'ALTER DATABASE {} OWNER TO {}'.format(
                        edbdef.EDGEDB_TEMPLATE_DB,
                        edbdef.EDGEDB_SUPERUSER))

        return False


async def _ensure_edgedb_template_not_connectable(conn):
    result = await _get_db_info(conn, edbdef.EDGEDB_TEMPLATE_DB)
    if result['datallowconn']:
        await _execute(
            conn,
            f'''ALTER DATABASE {edbdef.EDGEDB_TEMPLATE_DB}
                WITH ALLOW_CONNECTIONS = false
            '''
        )


async def _store_static_bin_cache(cluster, key: str, data: bytes) -> None:

    text = f"""\
        CREATE OR REPLACE FUNCTION edgedb.__syscache_{key} () RETURNS bytea
        AS $$
            SELECT {pg_common.quote_bytea_literal(data)};
        $$ LANGUAGE SQL IMMUTABLE;
    """

    dbconn = await cluster.connect(
        database=edbdef.EDGEDB_TEMPLATE_DB,
    )

    try:
        await _execute(dbconn, text)
    finally:
        await dbconn.close()


async def _store_static_json_cache(cluster, key: str, data: str) -> None:

    text = f"""\
        CREATE OR REPLACE FUNCTION edgedb.__syscache_{key} () RETURNS jsonb
        AS $$
            SELECT {pg_common.quote_literal(data)}::jsonb;
        $$ LANGUAGE SQL IMMUTABLE;
    """

    dbconn = await cluster.connect(
        database=edbdef.EDGEDB_TEMPLATE_DB,
    )

    try:
        await _execute(dbconn, text)
    finally:
        await dbconn.close()


async def _ensure_meta_schema(conn):
    logger.info('Bootstrapping meta schema...')
    await metaschema.bootstrap(conn)


def _process_delta(delta, schema):
    """Adapt and process the delta command."""

    if debug.flags.delta_plan:
        debug.header('Delta Plan')
        debug.dump(delta, schema=schema)

    delta = delta_cmds.CommandMeta.adapt(delta)

    context = sd.CommandContext()
    context.stdmode = True

    schema, _ = delta.apply(schema, context)

    if debug.flags.delta_pgsql_plan:
        debug.header('PgSQL Delta Plan')
        debug.dump(delta, schema=schema)

    return schema, delta


async def _make_stdlib(
    testmode: bool
) -> Tuple[s_schema.Schema, str, Set[uuid.UUID]]:
    schema = s_schema.Schema()
    schema, _ = s_mod.Module.create_in_schema(schema, name='__derived__')

    current_block = None

    std_texts = []
    for modname in s_schema.STD_LIB + ('stdgraphql',):
        std_texts.append(s_std.get_std_module_text(modname))

    if testmode:
        std_texts.append(s_std.get_std_module_text('_testmode'))

    ddl_text = '\n'.join(std_texts)
    new_types: Set[uuid.UUID] = set()

    for ddl_cmd in edgeql.parse_block(ddl_text):
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta_command)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = _process_delta(delta_command, schema)

        if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
            if (current_block is not None and
                    not isinstance(current_block, dbops.SQLBlock)):
                raise errors.QueryError(
                    'cannot mix DATABASE commands with regular DDL '
                    'commands in a single block')
            if current_block is None:
                current_block = dbops.SQLBlock()

        else:
            new_types.update(plan.new_types)
            if (current_block is not None and
                    not isinstance(current_block, dbops.PLTopBlock)):
                raise errors.QueryError(
                    'cannot mix DATABASE commands with regular DDL '
                    'commands in a single block')
            if current_block is None:
                current_block = dbops.PLTopBlock()

        plan.generate(current_block)

    assert current_block is not None
    sql_text = current_block.to_string()

    mods = {
        mod.get_name(schema)
        for mod in schema.get_modules()
        if mod.get_builtin(schema)
    }
    if mods != s_schema.STD_MODULES:
        raise errors.SchemaError(
            f'modules {s_schema.STD_MODULES - mods} are not marked as builtin')

    return schema, sql_text, new_types


async def _init_stdlib(cluster, conn, testmode):
    in_dev_mode = devmode.is_in_dev_mode()

    cache_hit = False
    sql_text = None

    if in_dev_mode:
        schema_cache = 'backend-stdschema.pickle'
        script_cache = 'backend-stdinitsql.pickle'
        testmode_flag = 'backend-stdtestmode.pickle'

        src_hash = devmode.hash_dirs(CACHE_SRC_DIRS)

        cached_testmode = devmode.read_dev_mode_cache(src_hash, testmode_flag)

        if cached_testmode is not None and cached_testmode == testmode:
            sql_text = devmode.read_dev_mode_cache(src_hash, script_cache)

        if sql_text is not None:
            schema = devmode.read_dev_mode_cache(src_hash, schema_cache)

    if sql_text is None or schema is None:
        schema, sql_text, new_types = await _make_stdlib(testmode)
    else:
        cache_hit = True

    await _execute_ddl(conn, sql_text)

    if not cache_hit:
        typemap = await conn.fetch('''
            SELECT id, backend_id FROM edgedb.type WHERE id = any($1::uuid[])
        ''', new_types)
        for tid, backend_tid in typemap:
            t = schema.get_by_id(tid)
            schema = t.set_field_value(schema, 'backend_id', backend_tid)

    if not cache_hit and in_dev_mode:
        devmode.write_dev_mode_cache(schema, src_hash, schema_cache)
        devmode.write_dev_mode_cache(sql_text, src_hash, script_cache)
        devmode.write_dev_mode_cache(testmode, src_hash, testmode_flag)

    await _store_static_bin_cache(
        cluster,
        'stdschema',
        pickle.dumps(schema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await metaschema.generate_views(conn, schema)
    await metaschema.generate_support_views(conn, schema)

    return schema


async def _execute_ddl(conn, sql_text):
    try:
        if debug.flags.delta_execute:
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
            position = int(position)
            point = parser_context.SourcePoint(
                None, None, position)
            text = e.query
            if text is None:
                # Parse errors
                text = sql_text

        elif internal_position is not None:
            internal_position = int(internal_position)
            point = parser_context.SourcePoint(
                None, None, internal_position)
            text = e.internal_query

        elif pl_func_line:
            point = parser_context.SourcePoint(
                pl_func_line, None, None
            )
            text = sql_text

        if point is not None:
            context = parser_context.ParserContext(
                'query', text, start=point, end=point)
            exceptions.replace_context(e, context)

        raise


async def _init_defaults(std_schema, schema, conn):
    script = '''
        CREATE MODULE default;
    '''

    schema, sql = compiler.compile_bootstrap_script(std_schema, schema, script)
    await conn.execute(sql)
    return schema


async def _populate_data(std_schema, schema, conn):
    script = '''
        INSERT stdgraphql::Query;
        INSERT stdgraphql::Mutation;
    '''

    schema, sql = compiler.compile_bootstrap_script(std_schema, schema, script)
    await _execute_ddl(conn, sql)
    return schema


async def _configure(
    schema: s_schema.Schema,
    conn: asyncpg_con.Connection,
    cluster: pgcluster.BaseCluster,
    *,
    insecure: bool = False,
    testmode: bool = False,
) -> None:
    scripts = []

    if cluster.is_managed() and not testmode:
        memory_kb = psutil.virtual_memory().total // 1024
        settings: Mapping[str, str] = {
            'shared_buffers': f'"{int(memory_kb * 0.2)}kB"',
            'effective_cache_size': f'"{int(memory_kb * 0.5)}kB"',
            'query_work_mem': f'"{6 * (2 ** 10)}kB"',
        }

        for setting, value in settings.items():
            scripts.append(f'''
                CONFIGURE SYSTEM SET {setting} := {value};
            ''')
    else:
        settings = {}

    if insecure:
        scripts.append('''
            CONFIGURE SYSTEM INSERT Auth {
                priority := 0,
                method := (INSERT Trust),
            };
        ''')

    config_spec = config.get_settings()

    for script in scripts:
        _, sql = compiler.compile_bootstrap_script(
            schema, schema, script, single_statement=True)

        if debug.flags.bootstrap:
            debug.header('Bootstrap')
            debug.dump_code(sql, lexer='sql')

        config_op_data = await conn.fetchval(sql)
        if config_op_data is not None and isinstance(config_op_data, str):
            config_op = config.Operation.from_json(config_op_data)
            storage: Mapping[str, str] = immutables.Map()
            settings = config_op.apply(config_spec, storage)

    config_json = config.to_json(config_spec, settings)
    block = dbops.PLTopBlock()
    dbops.UpdateMetadata(
        dbops.Database(name=edbdef.EDGEDB_TEMPLATE_DB),
        {'sysconfig': json.loads(config_json)},
    ).generate(block)

    await _execute_block(conn, block)


async def _compile_sys_queries(schema, cluster):
    queries = {}

    cfg_query = config.generate_config_query(schema)

    schema, sql = compiler.compile_bootstrap_script(
        schema, schema, cfg_query,
        expected_cardinality_one=True,
        single_statement=True)

    queries['config'] = sql

    role_query = '''
        SELECT sys::Role {
            name,
            is_superuser,
            password,
        } FILTER .name = <str>$name;
    '''
    schema, sql = compiler.compile_bootstrap_script(
        schema, schema, role_query,
        expected_cardinality_one=True,
        single_statement=True)

    queries['role'] = sql

    await _store_static_json_cache(
        cluster,
        'sysqueries',
        json.dumps(queries),
    )


async def _populate_misc_instance_data(cluster):

    mock_auth_nonce = scram.generate_nonce()
    json_instance_data = {
        'version': dict(buildmeta.get_version_dict()),
        'catver': edbdef.EDGEDB_CATALOG_VERSION,
        'mock_auth_nonce': mock_auth_nonce,
    }

    await _store_static_json_cache(
        cluster,
        'instancedata',
        json.dumps(json_instance_data),
    )

    return json_instance_data


async def _ensure_edgedb_database(conn, database, owner, *, cluster):
    result = await _get_db_info(conn, database)
    if not result:
        logger.info(
            f'Creating database: '
            f'{database}')

        block = dbops.SQLBlock()
        db = dbops.Database(
            database,
            owner=owner,
            metadata=dict(
                id=str(uuidgen.uuid1mc()),
            ),
        )
        dbops.CreateDatabase(db).generate(block)
        await _execute_block(conn, block)


async def _bootstrap_config_spec(schema, cluster):
    config_spec = config.load_spec_from_schema(schema)
    config.set_settings(config_spec)

    await _store_static_json_cache(
        cluster,
        'configspec',
        config.spec_to_json(config_spec),
    )


def _pg_log_listener(conn, msg):
    if msg.severity_en == 'WARNING':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    logger.log(level, msg.message)


async def _get_instance_data(conn: Any) -> Dict[str, Any]:

    data = await conn.fetchval(
        'SELECT edgedb.__syscache_instancedata()'
    )

    return json.loads(data)


async def bootstrap(cluster, args) -> bool:
    pgconn = await cluster.connect()
    pgconn.add_log_listener(_pg_log_listener)
    std_schema = None

    try:
        membership = set()
        session_user = cluster.get_connection_params().user
        if session_user != edbdef.EDGEDB_SUPERUSER:
            membership.add(session_user)

        await _ensure_edgedb_role(
            cluster,
            pgconn,
            edbdef.EDGEDB_SUPERUSER,
            membership=membership,
            is_superuser=True,
        )

        if session_user != edbdef.EDGEDB_SUPERUSER:
            await _execute(
                pgconn,
                f'SET ROLE {edbdef.EDGEDB_SUPERUSER};',
            )
            cluster.set_default_session_authorization(edbdef.EDGEDB_SUPERUSER)

        need_meta_bootstrap = await _ensure_edgedb_template_database(
            cluster, pgconn)

        if need_meta_bootstrap:
            conn = await cluster.connect(database=edbdef.EDGEDB_TEMPLATE_DB)
            conn.add_log_listener(_pg_log_listener)

            await _execute(
                conn,
                f'ALTER SCHEMA public OWNER TO {edbdef.EDGEDB_SUPERUSER}',
            )

            try:
                conn.add_log_listener(_pg_log_listener)

                await _ensure_meta_schema(conn)
                instancedata = await _populate_misc_instance_data(cluster)

                std_schema = await _init_stdlib(
                    cluster, conn, testmode=args['testmode'])
                await _bootstrap_config_spec(std_schema, cluster)
                await _compile_sys_queries(std_schema, cluster)
                schema = await _init_defaults(std_schema, std_schema, conn)
                schema = await _populate_data(std_schema, schema, conn)
                await _configure(std_schema, conn, cluster,
                                 insecure=args['insecure'],
                                 testmode=args['testmode'])
            finally:
                await conn.close()

            await _ensure_edgedb_database(
                pgconn,
                edbdef.EDGEDB_SUPERUSER_DB,
                edbdef.EDGEDB_SUPERUSER,
                cluster=cluster)

        else:
            conn = await cluster.connect(database=edbdef.EDGEDB_SUPERUSER_DB)

            try:
                std_schema = await compiler.load_std_schema(conn)
                config_spec = config.load_spec_from_schema(std_schema)
                config.set_settings(config_spec)
                instancedata = await _get_instance_data(conn)
            finally:
                await conn.close()

        datadir_version = instancedata.get('version')
        if datadir_version:
            datadir_major = datadir_version.get('major')

        expected_ver = buildmeta.get_version()

        if datadir_major != expected_ver.major:
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

        datadir_catver = instancedata.get('catver')
        expected_catver = edbdef.EDGEDB_CATALOG_VERSION

        if datadir_catver != expected_catver:
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

        await _ensure_edgedb_template_not_connectable(pgconn)

        await _ensure_edgedb_role(
            cluster,
            pgconn,
            args['default_database_user'],
            membership=membership,
            is_superuser=True,
        )

        await _execute(
            pgconn,
            f"SET ROLE {args['default_database_user']};",
        )

        await _ensure_edgedb_database(
            pgconn,
            args['default_database'],
            args['default_database_user'],
            cluster=cluster,
        )

    finally:
        await pgconn.close()

    return need_meta_bootstrap
