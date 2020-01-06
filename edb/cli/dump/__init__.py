#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import click
import edgedb

from edb.cli import cli
from edb.cli import utils

from . import dump as dumpmod
from . import restore as restoremod


@cli.command(help="Create a database backup")
@utils.connect_command
@click.pass_context
@click.argument('file', type=click.Path(exists=False, dir_okay=False,
                                        resolve_path=True))
def dump(ctx, file: str) -> None:
    cargs = ctx.obj['connargs']
    conn = cargs.new_connection()
    try:
        dumper = dumpmod.DumpImpl(conn)
        dumper.dump(file)
    finally:
        conn.close()


def is_empty_db(conn: edgedb.BlockingIOConnection) -> bool:
    ret = conn.fetchone('''
        SELECT (
            mods := array_agg((
                WITH x := (SELECT schema::Module {name} FILTER NOT .builtin)
                SELECT x.name
            )),
            cnt := (
                SELECT count(schema::Object FILTER .name LIKE "default::%")
            )
        );
    ''')

    return ret.mods == ['default'] and ret.cnt == 0


@cli.command(help="Restore the database from a backup")
@utils.connect_command
@click.pass_context
@click.option('--allow-nonempty', is_flag=True)
@click.argument('file', type=click.Path(exists=True, dir_okay=False,
                                        resolve_path=True))
def restore(ctx, file: str, allow_nonempty: bool) -> None:
    cargs = ctx.obj['connargs']
    conn = cargs.new_connection()
    dbname = conn.dbname

    try:
        if not is_empty_db(conn) and not allow_nonempty:
            raise click.ClickException(
                f'cannot restore into the {dbname!r} database: '
                f'the database is not empty; '
                f'consider using the --allow-nonempty option'
            )

        restorer = restoremod.RestoreImpl()
        restorer.restore(conn, file)
    finally:
        conn.close()
