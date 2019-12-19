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

import dataclasses
import functools
import sys

import click
import edgedb

from edb.server import defines as edgedb_defines


@dataclasses.dataclass
class ConnectionArgs:

    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    database: Optional[str] = None
    admin: bool = False
    allow_password_request: bool = False
    password: Optional[str] = None

    def get_password(self) -> Optional[str]:
        if self.password is not None:
            return self.password
        if self.allow_password_request and sys.stdin.isatty():
            self.password = password_prompt()
            return self.password
        return None

    def new_connection(
        self,
        *,
        database: Optional[str] = None,
        **extra_con_args: Any,
    ) -> edgedb.BlockingIOConnection:

        try:
            return edgedb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                database=self.database if database is None else database,
                admin=self.admin,
                password=self.password,
                **extra_con_args
            )
        except edgedb.AuthenticationError as ex:
            if self.password is None and self.allow_password_request:
                password = self.get_password()
                if password is None:
                    raise click.ClickException(str(ex))
                return self.new_connection(
                    database=database,
                    **extra_con_args
                )
            else:
                raise click.ClickException(str(ex))
        except Exception as ex:
            raise click.ClickException(str(ex))


_connect_params = [
    click.option('-h', '--host'),
    click.option('-p', '--port', type=int),
    click.option('-u', '--user'),
    click.option('-d', '--database'),
    click.option('--admin', is_flag=True),
    click.option('--password/--no-password', default=None),
    click.option('--password-from-stdin', is_flag=True),
]


def connect_command(func):

    @functools.wraps(func)
    def wrapper(
        *,
        host,
        port,
        user,
        database,
        admin,
        password,
        password_from_stdin,
        **kwargs
    ):
        ctx = click.get_current_context()
        ctx.ensure_object(dict)

        if password is False and password_from_stdin:
            raise click.UsageError(
                '--no-password and --password-from-stdin cannot be specified '
                'at the same time'
            )

        if 'connargs' not in ctx.obj:
            # First time this wrapper is run.
            # The wrapper can be run multiple times since it's used
            # for two levels, e.g.:
            #
            #    $ edgedb --password dump --no-password
            #
            cargs = ctx.obj['connargs'] = ConnectionArgs()

            if admin:
                if not user:
                    user = edgedb_defines.EDGEDB_SUPERUSER
                if not database:
                    database = edgedb_defines.EDGEDB_SUPERUSER_DB

            cargs.host = host
            cargs.port = port
            cargs.user = user
            cargs.database = database
            cargs.admin = admin
            cargs.allow_password_request = bool(password)
        else:
            cargs = ctx.obj['connargs']

        if password is False:
            cargs.allow_password_request = False

        if password or password_from_stdin:
            if password_from_stdin:
                cargs.password = sys.stdin.readline().strip('\r\n')
            else:
                cargs.password = password_prompt()

        if host is not None:
            cargs.host = host
        if port is not None:
            cargs.port = port
        if user is not None:
            cargs.user = user
        if database is not None:
            cargs.database = database
        if admin:
            cargs.admin = True

        return func(ctx, **kwargs)

    for option in reversed(_connect_params):
        wrapper = option(wrapper)

    return wrapper


def connect(
    ctx: click.Context,
) -> edgedb.BlockingIOConnection:
    conn = ctx.obj['connargs'].new_connection()
    ctx.obj['conn'] = conn
    ctx.call_on_close(lambda: conn.close())


def password_prompt():
    if sys.stdin.isatty():
        return click.prompt('Password', hide_input=True)

    raise click.UsageError(
        'password required and input is not a TTY, please '
        'use --password-from-stdin to provide the password value'
    )
