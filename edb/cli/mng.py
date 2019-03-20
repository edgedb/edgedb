#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.cli import cli
from edb.edgeql.quote import quote_literal as ql, quote_ident as qi


def connect(ctx):
    try:
        conn = edgedb.connect(
            host=ctx.obj['host'], port=ctx.obj['port'],
            user=ctx.obj['user'], database=ctx.obj['database'],
            admin=ctx.obj['admin'], password=ctx.obj['password'],
        )
    except edgedb.AuthenticationError:
        if (ctx.obj['password'] is None
                and ctx.obj['password_prompt'] is not None):
            password = ctx.obj['password_prompt']

            conn = edgedb.connect(
                host=ctx.obj['host'], port=ctx.obj['port'],
                user=ctx.obj['user'], database=ctx.obj['database'],
                admin=ctx.obj['admin'], password=password,
            )
        else:
            raise

    ctx.obj['conn'] = conn
    ctx.call_on_close(lambda: conn.close())


@cli.group()
@click.pass_context
def create(ctx):
    connect(ctx)


@cli.group()
@click.pass_context
def alter(ctx):
    connect(ctx)


@cli.group()
@click.pass_context
def drop(ctx):
    connect(ctx)


def options(options):
    def _decorator(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _decorator


_role_options = [
    click.option('--password/--no-password', default=None),
    click.option('--password-from-stdin', is_flag=True, default=False),
    click.option('--allow-login/--no-allow-login', default=None),
]


def _process_role_options(ctx, password, password_from_stdin, allow_login):
    if password is None and password_from_stdin:
        password = True

    if password is not None:
        if password:
            if password_from_stdin:
                password_value = ql(sys.stdin.readline().strip('\r\n'))
            elif sys.stdin.isatty():
                password_value = ql(click.prompt(
                    'Password',
                    hide_input=True,
                    confirmation_prompt=True,
                    type=str,
                ))
            else:
                raise click.UsageError(
                    'input is not a TTY, please use --password-from-stdin '
                    'to provide the password value'
                )
        else:
            password_value = '{}'
    else:
        password_value = None

    if allow_login is not None:
        allow_login_value = 'true' if allow_login else 'false'
    else:
        allow_login_value = None

    alters = []
    if password_value is not None:
        alters.append(f'SET password := {password_value}')
    if allow_login_value is not None:
        alters.append(f'SET allow_login := {allow_login_value}')

    if not alters:
        raise click.UsageError(
            'please specify an attribute to alter', ctx=ctx,
        )

    return alters


@create.command(name='role')
@click.argument('role-name', type=str)
@options(_role_options)
@click.pass_context
def create_role(ctx, role_name, **kwargs):
    attrs = ";\n".join(_process_role_options(ctx, **kwargs))

    qry = f'''
        CREATE ROLE {qi(role_name)} {{
            {attrs}
        }}
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e


@alter.command(name='role')
@click.argument('role-name', type=str)
@options(_role_options)
@click.pass_context
def alter_role(ctx, role_name, **kwargs):

    attrs = ";\n".join(_process_role_options(ctx, **kwargs))

    qry = f'''
        ALTER ROLE {qi(role_name)} {{
            {attrs}
        }}
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e


@drop.command(name='role')
@click.argument('role-name', type=str)
@click.pass_context
def drop_role(ctx, role_name, **kwargs):
    qry = f'''
        DROP ROLE {qi(role_name)};
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e
