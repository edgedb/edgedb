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


import sys

import click

from edb.common import devmode as dm
from edb.server import defines as edgedb_defines

from edb import repl


@click.group(
    invoke_without_command=True,
    context_settings=dict(help_option_names=['-?', '--help']))
@click.pass_context
@click.option('-h', '--host')
@click.option('-p', '--port', type=int)
@click.option('-u', '--user')
@click.option('-d', '--database')
@click.option('--admin', is_flag=True)
@click.option('--password/--no-password', default=None)
@click.option('--password-from-stdin', is_flag=True)
def cli(ctx, host, port, user, database, admin, password, password_from_stdin):
    ctx.ensure_object(dict)

    if admin:
        if not user:
            user = edgedb_defines.EDGEDB_SUPERUSER
        if not database:
            database = edgedb_defines.EDGEDB_SUPERUSER_DB

    if password is None and password_from_stdin:
        password = True

    password_prompt = None

    if password:
        if password_from_stdin:
            password = sys.stdin.readline().strip('\r\n')
        else:
            password = _password_prompt()
    elif password is None:
        password_prompt = _password_prompt
    else:
        password = None

    if ctx.invoked_subcommand is None:
        repl.main(
            host=host, port=port, user=user,
            database=database, password=password,
            password_prompt=password_prompt,
            admin=admin,
        )
    else:
        ctx.obj['host'] = host
        ctx.obj['port'] = port
        ctx.obj['user'] = user
        ctx.obj['database'] = database
        ctx.obj['password'] = password
        ctx.obj['password_prompt'] = password_prompt
        ctx.obj['admin'] = admin


def _password_prompt():
    if sys.stdin.isatty():
        password = click.prompt(
            'Password', hide_input=True)
    else:
        raise click.UsageError(
            'password required and input is not a TTY, please '
            'use --password-from-stdin to provide the password value'
        )

    return password


def cli_dev():
    dm.enable_dev_mode()
    cli()


# Import subcommands to register them

from . import mng  # noqa
