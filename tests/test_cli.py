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


import click.testing

import edgedb

from edb import cli

from edb.testbase import server as tb


class TestCLI(tb.ConnectedTestCase):

    ISOLATED_METHODS = False

    def _run_cli(self, *args, input=None):
        conn_args = self.get_connect_args()

        cmd_args = (
            '--host', conn_args['host'],
            '--port', conn_args['port'],
            '--user', conn_args['user'],
        ) + args

        if conn_args['password']:
            cmd_args = ('--password-from-stdin',) + cmd_args
            if input is not None:
                input = f"{conn_args['password']}\n{input}"
            else:
                input = f"{conn_args['password']}\n"

        runner = click.testing.CliRunner()
        return runner.invoke(
            cli.cli, args=cmd_args, input=input,
            catch_exceptions=False)

    async def test_cli_role(self):
        self._run_cli('create', 'role', 'foo', '--allow-login',
                      '--password-from-stdin',
                      input='foo-pass\n')

        conn = await self.connect(
            user='foo',
            password='foo-pass',
        )
        await conn.close()

        self._run_cli('alter', 'role', 'foo', '--no-allow-login')

        # good password, but allow_login is False
        with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed'):
            await self.connect(
                user='foo',
                password='foo-pass',
            )

        self._run_cli('alter', 'role', 'foo', '--allow-login',
                      '--password-from-stdin',
                      input='foo-new-pass\n')

        conn = await self.connect(
            user='foo',
            password='foo-new-pass',
        )
        await conn.close()

        self._run_cli('drop', 'role', 'foo')

        with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed'):
            await self.connect(
                user='foo',
                password='foo-new-pass',
            )

        result = self._run_cli('create', 'role', 'foo', '--allow-login',
                               '--password', input='foo-pass\n')
        self.assertIn(b'input is not a TTY', result.stdout_bytes)
