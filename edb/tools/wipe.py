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

from edb.server import cluster as edbcluster
from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import pgcluster
from edb.server import pgconnparams

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
    '-y',
    'yes',
    is_flag=True,
    help='assume Yes response to all questions')
@click.option(
    '--dry-run',
    is_flag=True,
    help='give a summary of wipe operations without performing them')
def wipe(*, postgres_dsn, data_dir, yes, dry_run):
    if postgres_dsn:
        cluster = pgcluster.get_remote_pg_cluster(postgres_dsn)
    elif data_dir:
        cluster = pgcluster.get_local_pg_cluster(data_dir)
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

    if not yes and not click.confirm(
            'This will DELETE all EdgeDB data from the target '
            'PostgreSQL instance.  ARE YOU SURE?'):
        click.echo('OK. Not proceeding.')

    status = cluster.get_status()
    cluster_started_by_us = False
    if status != 'running':
        if isinstance(cluster, pgcluster.RemoteCluster):
            click.secho(f'Remote cluster is not running', fg='red')
            sys.exit(1)
        else:
            cluster.start(port=edbcluster.find_available_port())
            cluster_started_by_us = True

    try:
        asyncio.run(do_wipe(cluster, dry_run))
    finally:
        if cluster_started_by_us:
            cluster.stop()


async def do_wipe(
    cluster: pgcluster.RemoteCluster,
    dry_run: bool,
) -> None:

    try:
        pgconn = await cluster.connect(database=edbdef.EDGEDB_TEMPLATE_DB)
    except asyncpg.InvalidCatalogNameError:
        click.secho(
            f'Instance does not have the {edbdef.EDGEDB_TEMPLATE_DB!r} '
            f'database. Is it already clean?'
        )
        return

    try:
        databases, roles = await _get_dbs_and_roles(pgconn)
    finally:
        await pgconn.close()

    stmts = [
        f'SET ROLE {qi(edbdef.EDGEDB_SUPERUSER)}',
    ]

    pgconn = await cluster.connect()

    try:
        for db in databases:
            owner = await pgconn.fetchval("""
                SELECT
                    rolname
                FROM
                    pg_database d
                    INNER JOIN pg_roles r
                        ON (d.datdba = r.oid)
                WHERE
                    d.datname = $1
            """, db)

            stmts.append(f'SET ROLE {qi(owner)}')

            if db == edbdef.EDGEDB_TEMPLATE_DB:
                stmts.append(f'ALTER DATABASE {qi(db)} IS_TEMPLATE = false')

            stmts.append(f'DROP DATABASE {qi(db)}')

        stmts.append('RESET ROLE;')

        for role in roles:
            stmts.append(f'DROP ROLE {qi(role)}')

        for stmt in stmts:
            click.echo(stmt)
            if not dry_run:
                await pgconn.execute(stmt)
    finally:
        await pgconn.close()


async def _get_dbs_and_roles(pgconn) -> Tuple[List[str], List[str]]:
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
        key=lambda dname: dname == edbdef.EDGEDB_TEMPLATE_DB,
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
