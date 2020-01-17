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


class TestServerAuth(tb.ConnectedTestCase):

    ISOLATED_METHODS = False

    async def test_server_auth_01(self):
        await self.con.fetchall('''
            CREATE SUPERUSER ROLE foo {
                SET password := 'foo-pass';
            }
        ''')

        # bad password
        with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed'):
            await self.connect(
                user='foo',
                password='wrong',
            )

        # good password
        conn = await self.connect(
            user='foo',
            password='foo-pass',
        )
        await conn.aclose()

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT Auth {
                comment := 'test',
                priority := 0,
                method := (INSERT Trust),
            }
        ''')

        try:
            # bad password, but the trust method doesn't care
            conn = await self.connect(
                user='foo',
                password='wrong',
            )
            await conn.aclose()

            # insert password auth with a higher priority
            await self.con.fetchall('''
                CONFIGURE SYSTEM INSERT Auth {
                    comment := 'test-2',
                    priority := -1,
                    method := (INSERT SCRAM),
                }
            ''')

            # bad password is bad again
            with self.assertRaisesRegex(
                    edgedb.AuthenticationError,
                    'authentication failed'):
                await self.connect(
                    user='foo',
                    password='wrong',
                )
        finally:
            await self.con.fetchall('''
                CONFIGURE SYSTEM RESET Auth FILTER .comment = 'test'
            ''')

            await self.con.fetchall('''
                CONFIGURE SYSTEM RESET Auth FILTER .comment = 'test-2'
            ''')

            await self.con.fetchall('''
                DROP ROLE foo;
            ''')
