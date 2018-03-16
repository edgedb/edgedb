##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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


async def _ensure_edgedb_superuser(conn):
    result = await conn.fetchrow('''
        SELECT
            rolsuper,
            rolcanlogin
        FROM
            pg_catalog.pg_roles
        WHERE
            rolname = $1
    ''', edgedb_defines.EDGEDB_SUPERUSER)

    if not result:
        logger.info('Creating superuser role...')
        await _execute(
            conn,
            'CREATE ROLE {} WITH LOGIN SUPERUSER'.format(
                edgedb_defines.EDGEDB_SUPERUSER))
    else:
        alter = []

        if not result['rolsuper']:
            alter.append('SUPERUSER')

        if not result['rolcanlogin']:
            alter.append('LOGIN')

        if alter:
            logger.info('Altering superuser role privileges...')
            await _execute(
                conn,
                'ALTER ROLE {} WITH {}'.format(
                    edgedb_defines.EDGEDB_SUPERUSER,
                    ' '.join(alter)))


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
        cmd = s_ddl.delta_from_ddl(statement, schema=bk.schema)
        await bk.run_ddl_command(cmd)

    await metaschema.generate_views(conn, bk.schema)


async def _init_graphql_schema(conn, cluster, loop):
    logger.info('Bootstrapping graphql module...')

    protocol = edgedb_protocol.Protocol(cluster, loop=loop)
    protocol.backend = await backend.open_database(conn)

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

    await protocol._run_script(script)


async def _ensure_edgedb_default_database(conn):
    result = await _get_db_info(conn, edgedb_defines.EDGEDB_DEFAULT_DB)
    if not result:
        logger.info(
            f'Creating default database: {edgedb_defines.EDGEDB_DEFAULT_DB}')

        ctx = delta.CommandContext(conn)
        db = dbops.Database(edgedb_defines.EDGEDB_DEFAULT_DB)
        await dbops.CreateDatabase(db).execute(ctx)


async def bootstrap(cluster, loop=None):
    pgconn = await cluster.connect(loop=loop)

    try:
        await _ensure_edgedb_superuser(pgconn)
        need_meta_bootstrap = await _ensure_edgedb_template_database(pgconn)

        if need_meta_bootstrap:
            conn = await cluster.connect(
                loop=loop, database=edgedb_defines.EDGEDB_TEMPLATE_DB,
                user=edgedb_defines.EDGEDB_SUPERUSER)

            try:
                await _ensure_meta_schema(conn)
                await _init_std_schema(conn)
                await _init_graphql_schema(conn, cluster, loop)
            finally:
                await conn.close()

        await _ensure_edgedb_default_database(pgconn)
        await _ensure_edgedb_template_not_connectable(pgconn)

    finally:
        await pgconn.close()
