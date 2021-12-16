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

from edb.schema import defines as s_def
from edb.testbase import server as tb


class TestServerAuth(tb.ConnectedTestCase):

    PARALLELISM_GRANULARITY = 'system'
    TRANSACTION_ISOLATION = False

    async def test_server_auth_01(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        await self.con.query('''
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

        await self.con.query('''
            CONFIGURE INSTANCE INSERT Auth {
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
            await self.con.query('''
                CONFIGURE INSTANCE INSERT Auth {
                    comment := 'test-2',
                    priority := -1,
                    method := (INSERT SCRAM),
                }
            ''')

            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed',
            ):
                # bad password is bad again
                await self.connect(
                    user='foo',
                    password='wrong',
                )

        finally:
            await self.con.query('''
                CONFIGURE INSTANCE RESET Auth FILTER .comment = 'test'
            ''')

            await self.con.query('''
                CONFIGURE INSTANCE RESET Auth FILTER .comment = 'test-2'
            ''')

            await self.con.query('''
                DROP ROLE foo;
            ''')

        # Basically the second test, but we can't run it concurrently
        # because disabling Auth above conflicts with the following test

        await self.con.query('''
            CREATE SUPERUSER ROLE bar {
                SET password_hash := 'SCRAM-SHA-256$4096:SHzNmIppMwXnPSWgY2yMvg==$5zmnXMm9+mn2nseKPF1NTKvuoBPVSWgxHrnptxpQgcU=:/c1vJV+MmS7v9vv6CDVo56OyOJkNd3F+m3JIBB1U7ho=';
            }
        ''')  # noqa

        try:
            conn = await self.connect(
                user='bar',
                password='bar-pass',
            )
            await conn.aclose()

            await self.con.query('''
                ALTER ROLE bar {
                    SET password_hash := 'SCRAM-SHA-256$4096:mWDBY53yzQ4aDet5erBmbg==$ZboQEMuUhC6+1SChp2bx1qSRBZGAnyV4I8T/iK+qeEs=:B7yF2k10tTH2RHayOg3rw4Q6wqf+Fj5CuXR/9CyZ8n8=';
                }
            ''')  # noqa

            conn = await self.connect(
                user='bar',
                password='bar-pass-2',
            )
            await conn.aclose()

            # bad (old) password
            with self.assertRaisesRegex(
                    edgedb.AuthenticationError,
                    'authentication failed'):
                await self.connect(
                    user='bar',
                    password='bar-pass',
                )

            with self.assertRaisesRegex(
                    edgedb.EdgeQLSyntaxError,
                    'cannot specify both `password` and `password_hash`'
                    ' in the same statement'):
                await self.con.query('''
                    CREATE SUPERUSER ROLE bar1 {
                        SET password := 'hello';
                        SET password_hash := 'SCRAM-SHA-256$4096:SHzNmIppMwXnPSWgY2yMvg==$5zmnXMm9+mn2nseKPF1NTKvuoBPVSWgxHrnptxpQgcU=:/c1vJV+MmS7v9vv6CDVo56OyOJkNd3F+m3JIBB1U7ho=';
                    }
                ''')  # noqa

            with self.assertRaisesRegex(
                    edgedb.InvalidValueError,
                    'invalid SCRAM verifier'):
                await self.con.query('''
                    CREATE SUPERUSER ROLE bar2 {
                        SET password_hash := 'SCRAM-BLAKE2B$4096:SHzNmIppMwXnPSWgY2yMvg==$5zmnXMm9+mn2nseKPF1NTKvuoBPVSWgxHrnptxpQgcU=:/c1vJV+MmS7v9vv6CDVo56OyOJkNd3F+m3JIBB1U7ho=';
                    }
                ''')  # noqa

        finally:
            await self.con.query("DROP ROLE bar")

    async def test_server_auth_02(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        try:
            await self.con.query('''
                CREATE SUPERUSER ROLE foo {
                    SET password := 'foo-pass';
                }
            ''')

            await self.con.query('''
                CREATE SUPERUSER ROLE bar {
                    SET password := 'bar-pass';
                }
            ''')

            await self.con.query('''
                CONFIGURE INSTANCE INSERT Auth {
                    comment := 'test-02',
                    priority := 0,
                    method := (INSERT SCRAM),
                    user := 'foo',
                }
            ''')

            # good password with configured Auth
            conn = await self.connect(
                user='foo',
                password='foo-pass',
            )
            await conn.aclose()

            # good password but Auth is not configured
            # (should default to SCRAM and succeed)
            conn2 = await self.connect(
                user='bar',
                password='bar-pass'
            )
            await conn2.aclose()
        finally:
            await self.con.query('''
                CONFIGURE INSTANCE RESET Auth FILTER .comment = 'test-02'
            ''')

            await self.con.query('''
                DROP ROLE foo;
            ''')

            await self.con.query('''
                DROP ROLE bar;
            ''')

    async def test_long_role_name(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'Role names longer than \d+ '
                r'characters are not supported'):
            await self.con.execute(
                f'CREATE SUPERUSER ROLE myrole_{"x" * s_def.MAX_NAME_LENGTH};')
