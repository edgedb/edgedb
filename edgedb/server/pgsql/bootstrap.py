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


import logging
import os.path

from edgedb.lang import edgeql
from edgedb.lang import schema as edgedb_schema
from edgedb.lang.schema import ddl as s_ddl

from edgedb.server import defines as edgedb_defines
from edgedb.server import protocol as edgedb_protocol

from . import backend
from . import dbops
from . import delta
from . import metaschema


logger = logging.getLogger('edgedb.server')


async def _statement(conn, query, *args, method):
    logger.debug('query: %s, args: %s', query, args)
    return await getattr(conn, method)(query, *args)


async def _execute(conn, query, *args):
    return await _statement(conn, query, *args, method='execute')


async def _ensure_edgedb_user(conn, username, *, is_superuser=False):
    result = await conn.fetchrow('''
        SELECT
            rolsuper,
            rolcanlogin
        FROM
            pg_catalog.pg_roles
        WHERE
            rolname = $1
    ''', username)

    if not result:
        logger.info(f'Creating {username} role...')
        if is_superuser:
            extra = 'SUPERUSER'
        else:
            extra = 'CREATEDB'
        await _execute(
            conn,
            'CREATE ROLE {} WITH LOGIN {}'.format(username, extra))
    else:
        alter = []

        if not result['rolsuper'] and is_superuser:
            alter.append('SUPERUSER')

        if not result['rolcanlogin']:
            alter.append('LOGIN')

        if alter:
            logger.info('Altering superuser role privileges...')
            await _execute(
                conn,
                'ALTER ROLE {} WITH {}'.format(username, ' '.join(alter)))


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


async def _ensure_edgedb_template_database(conn):
    result = await _get_db_info(conn, edgedb_defines.EDGEDB_TEMPLATE_DB)

    if not result:
        logger.info('Creating template database...')
        await _execute(
            conn,
            'CREATE DATABASE {} WITH OWNER = {} IS_TEMPLATE = TRUE'.format(
                edgedb_defines.EDGEDB_TEMPLATE_DB,
                edgedb_defines.EDGEDB_SUPERUSER))

        return True
    else:
        alter = []
        alter_owner = False

        if not result['datistemplate']:
            alter.append('IS_TEMPLATE')

        if result['rolname'] != edgedb_defines.EDGEDB_SUPERUSER:
            alter_owner = True

        if alter or alter_owner:
            logger.info('Altering template database parameters...')
            if alter:
                await _execute(
                    conn,
                    'ALTER DATABASE {} WITH {}'.format(
                        edgedb_defines.EDGEDB_TEMPLATE_DB,
                        ' '.join(alter)))

            if alter_owner:
                await _execute(
                    conn,
                    'ALTER DATABASE {} OWNER TO {}'.format(
                        edgedb_defines.EDGEDB_TEMPLATE_DB,
                        edgedb_defines.EDGEDB_SUPERUSER))

        return False


async def _ensure_edgedb_template_not_connectable(conn):
    result = await _get_db_info(conn, edgedb_defines.EDGEDB_TEMPLATE_DB)
    if result['datallowconn']:
        await _execute(
            conn,
            f'''ALTER DATABASE {edgedb_defines.EDGEDB_TEMPLATE_DB}
                WITH ALLOW_CONNECTIONS = false
            '''
        )


async def _ensure_meta_schema(conn):
    logger.info('Bootstrapping meta schema...')
    await metaschema.bootstrap(conn)


async def _init_std_schema(conn):
    logger.info('Bootstrapping std module...')

    stdschema = os.path.join(
        os.path.dirname(edgedb_schema.__file__), '_std.eql')
    with open(stdschema, 'r') as f:
        stdschema_script = f.read()

    statements = edgeql.parse_block(stdschema_script)

    bk = await backend.open_database(conn)

    for statement in statements:
        cmd = s_ddl.delta_from_ddl(
            statement, schema=bk.schema, modaliases={None: 'std'})
        await bk.run_ddl_command(cmd)

    await metaschema.generate_views(conn, bk.schema)


async def _run_script(script, conn, cluster, loop):
    protocol = edgedb_protocol.Protocol(cluster, loop=loop)
    protocol.backend = await backend.open_database(conn)
    await protocol._run_script(script)


async def _init_graphql_schema(conn, cluster, loop):
    logger.info('Bootstrapping graphql module...')

    with open(os.path.join(os.path.dirname(edgedb_schema.__file__),
                           '_graphql.eschema'), 'r') as f:
        schema = f.read()

    script = f'''
        CREATE MODULE graphql;
        CREATE MIGRATION graphql::d0 TO eschema $${schema}$$;
        COMMIT MIGRATION graphql::d0;
    '''
    with open(os.path.join(os.path.dirname(edgedb_schema.__file__),
                           '_graphql.eql'), 'r') as f:
        script += f.read()

    await _run_script(script, conn, cluster, loop)


async def _init_defaults(conn, cluster, loop):
    script = f'''
        CREATE MODULE default;
    '''

    await _run_script(script, conn, cluster, loop)


async def _ensure_edgedb_database(conn, database, owner, *, cluster, loop):
    result = await _get_db_info(conn, database)
    if not result:
        logger.info(
            f'Creating database: '
            f'{database}')

        ctx = delta.CommandContext(conn)
        db = dbops.Database(database, owner=owner)
        await dbops.CreateDatabase(db).execute(ctx)

        if owner != edgedb_defines.EDGEDB_SUPERUSER:
            dbconn = await cluster.connect(
                loop=loop, database=database,
                user=edgedb_defines.EDGEDB_SUPERUSER
            )

            dbconnctx = delta.CommandContext(dbconn)

            try:
                reassign = dbops.ReassignOwned(
                    edgedb_defines.EDGEDB_SUPERUSER, owner)
                await reassign.execute(dbconnctx)
            finally:
                await dbconn.close()


async def bootstrap(cluster, args, loop=None):
    pgconn = await cluster.connect(loop=loop)

    try:
        await _ensure_edgedb_user(pgconn, edgedb_defines.EDGEDB_SUPERUSER,
                                  is_superuser=True)
        need_meta_bootstrap = await _ensure_edgedb_template_database(pgconn)

        if need_meta_bootstrap:
            conn = await cluster.connect(
                loop=loop, database=edgedb_defines.EDGEDB_TEMPLATE_DB,
                user=edgedb_defines.EDGEDB_SUPERUSER)

            try:
                await _ensure_meta_schema(conn)
                await _init_std_schema(conn)
                await _init_graphql_schema(conn, cluster, loop)
                await _init_defaults(conn, cluster, loop)
            finally:
                await conn.close()

        await _ensure_edgedb_database(
            pgconn, edgedb_defines.EDGEDB_SUPERUSER_DB,
            edgedb_defines.EDGEDB_SUPERUSER,
            cluster=cluster, loop=loop)
        await _ensure_edgedb_template_not_connectable(pgconn)

        await _ensure_edgedb_user(
            pgconn, args['default_database_user'], is_superuser=True)

        await _ensure_edgedb_database(
            pgconn, args['default_database'], args['default_database_user'],
            cluster=cluster, loop=loop)

    finally:
        await pgconn.close()
