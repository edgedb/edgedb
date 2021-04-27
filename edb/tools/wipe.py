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

import asyncio
import json
import pathlib
import sys

import asyncpg
import click

from edb.schema import schema as s_schema

from edb.common import topological
from edb.tools.edb import edbcommands

from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import pgcluster
from edb.server import pgconnparams

from edb.pgsql import common as pgcommon
from edb.pgsql.common import quote_ident as qi


class AbsPath(click.Path):
    name = 'path'

    def convert(self, value, param, ctx):
        return pathlib.Path(super().convert(value, param, ctx)).absolute()


@edbcommands.command('wipe')
@click.option(
    '--postgres-dsn',
    type=str,
    help='DSN of the remote Postgres instance to wipe EdgeDB from')
@click.option(
    '-D',
    '--data-dir',
    type=AbsPath(),
    help='database cluster directory')
@click.option(
    '--tenant-id',
    type=str,
    multiple=True,
    help='The tenant ID of an EdgeDB server to wipe.  May be specified'
         ' multiple times.  If not specified, all tenants are wiped.')
@click.option(
    '-y',
    'yes',
    is_flag=True,
    help='assume Yes response to all questions')
@click.option(
    '--dry-run',
    is_flag=True,
    help='give a summary of wipe operations without performing them')
@click.option(
    '--list-tenants',
    is_flag=True,
    help='list cluster tenants instead of performing a wipe')
def wipe(
    *,
    postgres_dsn,
    data_dir,
    tenant_id,
    yes,
    dry_run,
    list_tenants,
):
    if postgres_dsn:
        cluster = pgcluster.get_remote_pg_cluster(
            postgres_dsn,
            tenant_id='<unknown>',
        )
    elif data_dir:
        cluster = pgcluster.get_local_pg_cluster(
            data_dir,
            tenant_id='<unknown>',
        )
        cluster.set_connection_params(
            pgconnparams.ConnectionParameters(
                user='postgres',
                database='template1',
            ),
        )
    else:
        raise click.UsageError(
            'either --postgres-dsn or --data-dir is required'
        )

    if not yes and not dry_run and not list_tenants and not click.confirm(
            'This will DELETE all EdgeDB data from the target '
            'PostgreSQL instance.  ARE YOU SURE?'):
        click.echo('OK. Not proceeding.')
        return

    status = cluster.get_status()
    cluster_started_by_us = False
    if status != 'running':
        if isinstance(cluster, pgcluster.RemoteCluster):
            click.secho(f'Remote cluster is not running', fg='red')
            sys.exit(1)
        else:
            cluster.start()
            cluster_started_by_us = True

    try:
        asyncio.run(
            do_wipe(cluster, tenant_id, dry_run, list_tenants),
        )
    finally:
        if cluster_started_by_us:
            cluster.stop()


async def do_wipe(
    cluster: pgcluster.BaseCluster,
    tenants: List[str],
    dry_run: bool,
    list_tenants: bool,
) -> None:

    conn = await cluster.connect()

    try:
        if not tenants:
            tenants = await _get_all_tenants(conn)
            if list_tenants:
                print('\n'.join(t if t else '(none)' for t in tenants))
                return

        for tenant in tenants:
            await wipe_tenant(cluster, conn, tenant, dry_run)
    finally:
        await conn.close()


def get_database_backend_name(name: str, tenant_id: str) -> str:
    if not tenant_id:
        return name
    else:
        return pgcommon.get_database_backend_name(name, tenant_id=tenant_id)


def get_role_backend_name(name: str, tenant_id: str) -> str:
    if not tenant_id:
        return name
    else:
        return pgcommon.get_role_backend_name(name, tenant_id=tenant_id)


async def wipe_tenant(
    cluster: pgcluster.BaseCluster,
    pgconn: asyncpg.Connection,
    tenant: str,
    dry_run: bool,
) -> None:

    tpl_db = get_database_backend_name(
        edbdef.EDGEDB_TEMPLATE_DB,
        tenant_id=tenant,
    )

    sup_role = get_role_backend_name(
        edbdef.EDGEDB_SUPERUSER,
        tenant_id=tenant,
    )

    try:
        tpl_conn = await cluster.connect(database=tpl_db)
    except asyncpg.InvalidCatalogNameError:
        click.secho(
            f'Instance tenant {tenant!r} does not have the '
            f'{edbdef.EDGEDB_TEMPLATE_DB!r} database. Is it already clean?'
        )
        return

    try:
        databases, roles = await _get_dbs_and_roles(tpl_conn)
    finally:
        await tpl_conn.close()

    stmts = [
        f'SET ROLE {qi(sup_role)}',
    ]

    for db in databases:
        pg_db = get_database_backend_name(db, tenant_id=tenant)
        owner = await pgconn.fetchval("""
            SELECT
                rolname
            FROM
                pg_database d
                INNER JOIN pg_roles r
                    ON (d.datdba = r.oid)
            WHERE
                d.datname = $1
        """, pg_db)

        if owner:
            stmts.append(f'SET ROLE {qi(owner)}')

        if pg_db == tpl_db:
            stmts.append(f'ALTER DATABASE {qi(pg_db)} IS_TEMPLATE = false')

        stmts.append(f'DROP DATABASE {qi(pg_db)}')

    stmts.append('RESET ROLE;')

    for role in roles:
        pg_role = get_role_backend_name(role, tenant_id=tenant)

        members = await pgconn.fetchval("""
            SELECT
                array_agg(member::regrole::text)
            FROM
                pg_auth_members
            WHERE
                roleid = (SELECT oid FROM pg_roles WHERE rolname = $1)
        """, pg_role)

        for member in members:
            stmts.append(f'REVOKE {qi(pg_role)} FROM {qi(member)}')

        stmts.append(f'DROP ROLE {qi(pg_role)}')

    super_group = get_role_backend_name(
        edbdef.EDGEDB_SUPERGROUP, tenant_id=tenant)
    stmts.append(f'DROP ROLE {qi(super_group)}')

    for stmt in stmts:
        click.echo(stmt + (';' if not stmt.endswith(';') else ''))
        if not dry_run:
            await pgconn.execute(stmt)


async def _get_all_tenants(
    conn: asyncpg.Connection,
) -> List[str]:
    dbs = await conn.fetch(
        """
            SELECT datname
            FROM pg_database
            WHERE datname LIKE $1
        """,
        f"%{edbdef.EDGEDB_TEMPLATE_DB}",
    )

    tenants = []
    for db in dbs:
        if db['datname'] == edbdef.EDGEDB_TEMPLATE_DB:
            t = ""
        else:
            t, _, _ = db['datname'].partition('_')
        tenants.append(t)

    return tenants


async def _get_dbs_and_roles(
    pgconn: asyncpg.Connection,
) -> Tuple[List[str], List[str]]:
    compiler = edbcompiler.Compiler()
    await compiler.initialize_from_pg(pgconn)
    compilerctx = edbcompiler.new_compiler_context(
        user_schema=s_schema.FlatSchema(),
        global_schema=s_schema.FlatSchema(),
        expected_cardinality_one=False,
        single_statement=True,
        output_format=edbcompiler.IoFormat.JSON,
        bootstrap_mode=True,
    )

    _, get_databases_sql = edbcompiler.compile_edgeql_script(
        compiler,
        compilerctx,
        'SELECT sys::Database.name',
    )

    databases = list(sorted(
        json.loads(await pgconn.fetchval(get_databases_sql)),
        key=lambda dname: edbdef.EDGEDB_TEMPLATE_DB in dname,
    ))

    _, get_roles_sql = edbcompiler.compile_edgeql_script(
        compiler,
        compilerctx,
        '''SELECT sys::Role {
            name,
            parents := .member_of.name,
        }''',
    )

    roles = json.loads(await pgconn.fetchval(get_roles_sql))
    sorted_roles = list(topological.sort({
        r['name']: topological.DepGraphEntry(
            item=r['name'],
            deps=r['parents'],
            extra=False,
        ) for r in roles
    }))

    return databases, sorted_roles
