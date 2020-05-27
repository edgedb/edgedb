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


import edgedb

from edb.testbase import server as tb


class TestCLI(tb.ConnectedTestCase, tb.OldCLITestCaseMixin):

    ISOLATED_METHODS = False
    SERIALIZED = True

    async def test_cli_role(self):
        self.run_cli('create-superuser-role', 'foo', '--password-from-stdin',
                     input='foo-pass\n')

        conn = await self.connect(
            user='foo',
            password='foo-pass',
        )
        await conn.aclose()

        self.run_cli('alter-role', 'foo', '--password-from-stdin',
                     input='foo-new-pass\n')

        conn = await self.connect(
            user='foo',
            password='foo-new-pass',
        )
        await conn.aclose()

        self.run_cli('drop-role', 'foo')

        with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed'):
            await self.connect(
                user='foo',
                password='foo-new-pass',
            )

        result = self.run_cli('create-superuser-role', 'foo',
                              '--password', input='foo-pass\n')
        self.assertIn('input is not a TTY', result.output)

        result = self.run_cli(
            'create-superuser-role',
            'create-role-empty-options'
        )

        self.assertEqual(result.exit_code, 0)

    async def test_cli_repl_script(self):
        result = self.run_cli(input='SELECT 1 + 1')
        self.assertEqual(list(result.output.split('\n'))[0], '{2}')
