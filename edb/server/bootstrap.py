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
import logging
import os
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
from edb.schema import objects as s_obj
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import std as s_std

from edb.server import buildmeta
from edb.server import config
from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import tokenizer  # type: ignore

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


async def _execute_block(conn, block: dbops.SQLBlock) -> None:

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
    builtin=False,
    objid=None,
) -> None:
    membership = set(membership)
    if is_superuser:
        superuser_role = cluster.get_superuser_role()
        if superuser_role:
            # If the cluster is exposing an explicit superuser role,
            # become a member of that instead of creating a superuser
            # role directly.
            membership.add(superuser_role)
            superuser_flag = False
        else:
            superuser_flag = True
    else:
        superuser_flag = False

    if objid is None:
        objid = uuidgen.uuid1mc()

    role = dbops.Role(
        name=username,
        is_superuser=superuser_flag,
        allow_login=True,
        allow_createdb=True,
        allow_createrole=True,
        membership=membership,
        metadata=dict(
            id=str(objid),
            builtin=builtin,
        ),
    )

    create_role = dbops.CreateRole(
        role,
        neg_conditions=[dbops.RoleExists(username)],
    )

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(conn, block)

    return objid


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
        dbid = uuidgen.uuid1mc()
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
                id=str(dbid),
                builtin=True,
            ),
        )
        dbops.CreateDatabase(db).generate(block)
        await _execute_block(conn, block)

        return dbid
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

        return None


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
        CREATE OR REPLACE FUNCTION edgedbinstdata.__syscache_{key} ()
        RETURNS bytea
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
        CREATE OR REPLACE FUNCTION edgedbinstdata.__syscache_{key} ()
        RETURNS jsonb
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

    schema = delta.apply(schema, context)

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
        schema=schema,
        single_statement=single_statement,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=True,
        output_format=output_format,
    )

    return edbcompiler.compile_edgeql_script(compiler, ctx, eql)


class StdlibBits(NamedTuple):

    #: User-visible std.
    stdschema: s_schema.Schema
    #: Shadow extended schema for reflection..
    reflschema: s_schema.Schema
    #: SQL text of the procedure to initialize `std` in Postgres.
    sqltext: str
    #: A set of ids of all types in std.
    types: Set[uuid.UUID]
    #: Schema class reflection layout.
    classlayout: Dict[Type[s_obj.Object], s_refl.SchemaTypeLayout]
    #: Schema introspection query (SQL).
    introquery: str


async def _make_stdlib(testmode: bool, global_ids) -> StdlibBits:
    schema = s_schema.Schema()
    schema, _ = s_mod.Module.create_in_schema(schema, name='__derived__')

    current_block = dbops.PLTopBlock()

    std_texts = []
    for modname in s_schema.STD_LIB + ('stdgraphql',):
        std_texts.append(s_std.get_std_module_text(modname))

    if testmode:
        std_texts.append(s_std.get_std_module_text('_testmode'))

    ddl_text = '\n'.join(std_texts)
    types: Set[uuid.UUID] = set()
    std_plans: List[sd.Command] = []

    for ddl_cmd in edgeql.parse_block(ddl_text):
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta_command)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = _process_delta(delta_command, schema)
        std_plans.append(delta_command)

        types.update(plan.new_types)
        plan.generate(current_block)

    stdglobals = '\n'.join([
        f'''CREATE SUPERUSER ROLE {edbdef.EDGEDB_SUPERUSER} {{
            SET id := <uuid>'{global_ids[edbdef.EDGEDB_SUPERUSER]}'
        }};''',
        f'''CREATE DATABASE {edbdef.EDGEDB_TEMPLATE_DB} {{
            SET id := <uuid>'{global_ids[edbdef.EDGEDB_TEMPLATE_DB]}'
        }};''',
        f'CREATE DATABASE {edbdef.EDGEDB_SUPERUSER_DB};',
    ])

    context = sd.CommandContext(stdmode=True)

    for ddl_cmd in edgeql.parse_block(stdglobals):
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        schema = delta_command.apply(schema, context)

    refldelta, classlayout, introparts = s_refl.generate_structure(schema)
    reflschema, reflplan = _process_delta(refldelta, schema)

    std_plans.append(refldelta)

    assert current_block is not None
    reflplan.generate(current_block)
    subblock = current_block.add_block()

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=reflschema,
        schema_class_layout=classlayout,
        bootstrap_mode=True,
    )

    compilerctx = edbcompiler.new_compiler_context(reflschema)

    for std_plan in std_plans:
        compiler._compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=std_plan,
            block=subblock,
            is_internal_reflection=std_plan is refldelta,
            stdmode=True,
        )

    sqltext = current_block.to_string()

    compilerctx = edbcompiler.new_compiler_context(
        reflschema,
        schema_reflection_mode=True,
        output_format=edbcompiler.IoFormat.JSON_ELEMENTS,
    )

    # The introspection query bits are returned in chunks
    # because it's a large UNION and we currently generate SQL
    # that is much harder for Posgres to plan as opposed to a
    # straight flat UNION.
    sql_introparts = []

    for intropart in introparts:
        introtokens = tokenizer.tokenize(intropart.encode())
        units = compiler._compile(ctx=compilerctx, tokens=introtokens)
        assert len(units) == 1 and len(units[0].sql) == 1
        sql_intropart = units[0].sql[0].decode()
        sql_introparts.append(sql_intropart)

    introsql = ' UNION ALL '.join(sql_introparts)

    return StdlibBits(
        stdschema=schema,
        reflschema=reflschema,
        sqltext=sqltext,
        types=types,
        classlayout=classlayout,
        introquery=introsql,
    )


async def _amend_stdlib(
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
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(delta_command)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = _process_delta(delta_command, schema)
        reflschema = delta_command.apply(reflschema, context)
        plan.generate(topblock)
        plans.append(plan)

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=reflschema,
        schema_class_layout=stdlib.classlayout,
        bootstrap_mode=True,
    )

    compilerctx = edbcompiler.new_compiler_context(schema)

    for plan in plans:
        compiler._compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=plan,
            block=topblock,
            stdmode=True,
        )

    sqltext = topblock.to_string()

    return stdlib._replace(stdschema=schema, reflschema=reflschema), sqltext


async def _init_stdlib(cluster, conn, testmode, global_ids):
    in_dev_mode = devmode.is_in_dev_mode()

    specified_cache_dir = os.environ.get('_EDGEDB_WRITE_DATA_CACHE_TO')
    if specified_cache_dir:
        cache_dir = pathlib.Path(specified_cache_dir)
    else:
        cache_dir = None

    stdlib_cache = 'backend-stdlib.pickle'
    tpldbdump_cache = 'backend-tpldbdump.sql'
    src_hash = buildmeta.hash_dirs(CACHE_SRC_DIRS)
    stdlib = buildmeta.read_data_cache(
        src_hash, stdlib_cache, source_dir=cache_dir)
    tpldbdump = buildmeta.read_data_cache(
        src_hash, tpldbdump_cache, source_dir=cache_dir, pickled=False)

    if stdlib is None:
        stdlib = await _make_stdlib(in_dev_mode or testmode, global_ids)
        cache_hit = False
    else:
        cache_hit = True

    if tpldbdump is None:
        await _ensure_meta_schema(conn)
        await _execute_ddl(conn, stdlib.sqltext)

        if in_dev_mode or specified_cache_dir:
            tpldbdump = cluster.dump_database(
                edbdef.EDGEDB_TEMPLATE_DB, exclude_schema='edgedbinstdata')
            buildmeta.write_data_cache(
                tpldbdump,
                src_hash,
                tpldbdump_cache,
                pickled=False,
                target_dir=cache_dir,
            )
    else:
        cluster.restore_database(edbdef.EDGEDB_TEMPLATE_DB, tpldbdump)

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
            bootstrap_mode=True,
        )
        _, sql = compile_bootstrap_script(
            compiler,
            stdlib.reflschema,
            '''
            UPDATE schema::ScalarType
            FILTER .builtin AND NOT .is_abstract
            SET {
                backend_id := sys::_get_pg_type_for_scalar_type(.id)
            }
            ''',
            expected_cardinality_one=False,
            single_statement=True,
        )
        await conn.execute(sql)

    if not in_dev_mode and testmode:
        # Running tests on a production build.
        stdlib, testmode_sql = await _amend_stdlib(
            s_std.get_std_module_text('_testmode'),
            stdlib,
        )
        await conn.execute(testmode_sql)
        await metaschema.generate_support_views(
            cluster,
            conn,
            stdlib.reflschema,
        )

    # Make sure that schema backend_id properties are in sync with
    # the database.

    compiler = edbcompiler.new_compiler(
        std_schema=stdlib.stdschema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
        bootstrap_mode=True,
    )
    _, sql = compile_bootstrap_script(
        compiler,
        stdlib.reflschema,
        '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .builtin AND NOT .is_abstract;
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

    if not cache_hit and (in_dev_mode or specified_cache_dir):
        buildmeta.write_data_cache(
            stdlib,
            src_hash,
            stdlib_cache,
            target_dir=cache_dir,
        )

    await _store_static_bin_cache(
        cluster,
        'stdschema',
        pickle.dumps(schema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        cluster,
        'reflschema',
        pickle.dumps(stdlib.reflschema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        cluster,
        'classlayout',
        pickle.dumps(stdlib.classlayout, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_json_cache(
        cluster,
        'introquery',
        json.dumps(stdlib.introquery),
    )

    await metaschema.generate_support_views(cluster, conn, stdlib.reflschema)
    await metaschema.generate_support_functions(conn, stdlib.reflschema)

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
        bootstrap_mode=True,
    )

    await metaschema.generate_more_support_functions(
        conn, compiler, stdlib.reflschema)

    return schema, stdlib.reflschema, compiler


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


async def _init_defaults(schema, compiler, conn):
    script = '''
        CREATE MODULE default;
    '''

    schema, sql = compile_bootstrap_script(compiler, schema, script)
    await _execute_ddl(conn, sql)
    return schema


async def _populate_data(schema, compiler, conn):
    script = '''
        INSERT stdgraphql::Query;
        INSERT stdgraphql::Mutation;
    '''

    schema, sql = compile_bootstrap_script(compiler, schema, script)
    await _execute_ddl(conn, sql)
    return schema


async def _configure(
    schema: s_schema.Schema,
    compiler: edbcompiler.Compiler,
    conn: asyncpg_con.Connection,
    cluster: pgcluster.BaseCluster,
    *,
    insecure: bool = False,
) -> None:
    scripts = []

    if cluster.is_managed() and not devmode.is_in_dev_mode():
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
        _, sql = compile_bootstrap_script(
            compiler,
            schema,
            script,
            single_statement=True,
        )

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


async def _compile_sys_queries(schema, compiler, cluster):
    queries = {}

    cfg_query = config.generate_config_query(schema)

    schema, sql = compile_bootstrap_script(
        compiler,
        schema,
        cfg_query,
        expected_cardinality_one=True,
        single_statement=True,
    )

    queries['config'] = sql

    role_query = '''
        SELECT sys::Role {
            name,
            is_superuser,
            password,
        } FILTER .name = <str>$name;
    '''
    schema, sql = compile_bootstrap_script(
        compiler,
        schema,
        role_query,
        expected_cardinality_one=True,
        single_statement=True,
    )

    queries['role'] = sql

    tids_query = '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .id IN <uuid>json_array_unpack(<json>$ids);
    '''
    schema, sql = compile_bootstrap_script(
        compiler,
        schema,
        tids_query,
        expected_cardinality_one=False,
        single_statement=True,
    )

    queries['backend_tids'] = sql

    await _store_static_json_cache(
        cluster,
        'sysqueries',
        json.dumps(queries),
    )


async def _populate_misc_instance_data(cluster, conn):

    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.CreateSchema(name='edgedbinstdata'),
    ])

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)

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


async def _ensure_edgedb_database(
    conn,
    database,
    owner,
    *,
    cluster,
    builtin: bool = False,
    objid: Optional[uuid.UUID] = None,
):
    result = await _get_db_info(conn, database)
    if not result:
        logger.info(
            f'Creating database: '
            f'{database}')

        block = dbops.SQLBlock()
        if objid is None:
            objid = uuidgen.uuid1mc()
        db = dbops.Database(
            database,
            owner=owner,
            metadata=dict(
                id=str(objid),
                builtin=builtin,
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
        'SELECT edgedbinstdata.__syscache_instancedata()'
    )

    return json.loads(data)


async def _check_data_dir_compatibility(conn):
    instancedata = await _get_instance_data(conn)
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


async def bootstrap(cluster, args) -> bool:
    pgconn = await cluster.connect()
    pgconn.add_log_listener(_pg_log_listener)
    std_schema = None

    try:
        membership = set()
        session_user = cluster.get_connection_params().user
        if session_user != edbdef.EDGEDB_SUPERUSER:
            membership.add(session_user)

        superuser_uid = await _ensure_edgedb_role(
            cluster,
            pgconn,
            edbdef.EDGEDB_SUPERUSER,
            membership=membership,
            is_superuser=True,
            builtin=True,
        )

        if session_user != edbdef.EDGEDB_SUPERUSER:
            await _execute(
                pgconn,
                f'SET ROLE {edbdef.EDGEDB_SUPERUSER};',
            )
            cluster.set_default_session_authorization(edbdef.EDGEDB_SUPERUSER)

        new_template_db_id = await _ensure_edgedb_template_database(
            cluster, pgconn)

        if new_template_db_id:
            conn = await cluster.connect(database=edbdef.EDGEDB_TEMPLATE_DB)
            conn.add_log_listener(_pg_log_listener)

            await _execute(
                conn,
                f'ALTER SCHEMA public OWNER TO {edbdef.EDGEDB_SUPERUSER}',
            )

            try:
                conn.add_log_listener(_pg_log_listener)

                await _populate_misc_instance_data(cluster, conn)

                std_schema, refl_schema, compiler = await _init_stdlib(
                    cluster,
                    conn,
                    testmode=args['testmode'],
                    global_ids={
                        edbdef.EDGEDB_SUPERUSER: superuser_uid,
                        edbdef.EDGEDB_TEMPLATE_DB: new_template_db_id,
                    }
                )
                await _bootstrap_config_spec(std_schema, cluster)
                await _compile_sys_queries(refl_schema, compiler, cluster)
                schema = await _init_defaults(std_schema, compiler, conn)
                schema = await _populate_data(std_schema, compiler, conn)
                await _configure(schema, compiler, conn, cluster,
                                 insecure=args['insecure'])
            finally:
                await conn.close()

            superuser_db = schema.get_global(
                s_db.Database, edbdef.EDGEDB_SUPERUSER_DB)

            await _ensure_edgedb_database(
                pgconn,
                edbdef.EDGEDB_SUPERUSER_DB,
                edbdef.EDGEDB_SUPERUSER,
                cluster=cluster,
                builtin=True,
                objid=superuser_db.id,
            )

        else:
            conn = await cluster.connect(database=edbdef.EDGEDB_SUPERUSER_DB)

            try:
                await _check_data_dir_compatibility(conn)
                compiler = edbcompiler.Compiler({})
                await compiler.ensure_initialized(conn)
                std_schema = compiler.get_std_schema()
                config_spec = config.load_spec_from_schema(std_schema)
                config.set_settings(config_spec)
            finally:
                await conn.close()

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

    return new_template_db_id is not None
