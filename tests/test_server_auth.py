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

import asyncio
import os
import pathlib
import signal
import tempfile
import unittest
import urllib.error
import urllib.request

import jwcrypto.jwk

import edgedb

from edb.common import secretkey
from edb.server import args
from edb.schema import defines as s_def
from edb.testbase import server as tb


class TestServerAuth(tb.ConnectedTestCase):

    PARALLELISM_GRANULARITY = 'system'
    TRANSACTION_ISOLATION = False

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_auth_01(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        await self.con.query('''
            CREATE EXTENSION edgeql_http;
        ''')
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

        # Test wrong password on http basic auth
        body, code = await self._basic_http_request(None, 'foo', 'wrong')
        self.assertEqual(code, 401, f"Wrong result: {body}")

        # good password
        conn = await self.connect(
            user='foo',
            password='foo-pass',
        )
        await conn.aclose()
        body, code = await self._basic_http_request(None, 'foo', 'foo-pass')
        self.assertEqual(code, 200, f"Wrong result: {body}")

        # Force foo to use a JWT so auth fails
        await self.con.query('''
            CONFIGURE INSTANCE INSERT Auth {
                comment := 'foo-jwt',
                priority := -1,
                user := 'foo',
                method := (INSERT JWT {
                    transports := "SIMPLE_HTTP",
                }),
            }
        ''')

        # Should fail now
        body, code = await self._basic_http_request(None, 'foo', 'foo-pass')
        self.assertEqual(code, 401, f"Wrong result: {body}")

        # But *edgedb* should still work
        body, code = await self._basic_http_request(None, 'edgedb', None)
        self.assertEqual(code, 200, f"Wrong result: {body}")

        await self.con.query('''
            CONFIGURE INSTANCE RESET Auth
            filter .comment = 'foo-jwt'
        ''')

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

    async def _basic_http_request(
        self, server, user, password, db='edgedb',
    ):
        url = f'{self.http_addr}/db/{db}/edgeql'
        params = dict(query='select 1')
        password = password or self.get_connect_args()['password']

        # Do the elaborate dance to let urllib do basic auth
        # Most tests we just construct the header ourselves because that
        # was very easy to integrate and also less confusing, but it's
        # worth making sure that we interoperate with *something*.
        https_handler = urllib.request.HTTPSHandler(context=self.tls_context)

        passman = urllib.request.HTTPPasswordMgr()
        passman.add_password('edgedb', url, user, password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(passman)
        opener = urllib.request.build_opener(https_handler, auth_handler)

        request = urllib.request.Request(
            f'{url}/?{urllib.parse.urlencode(params)}',
        )
        try:
            resp = opener.open(request)
        except urllib.error.HTTPError as e:
            resp = e.fp
        resp_body = resp.read()
        resp_status = resp.status
        return resp_body, resp_status

    async def _jwt_http_request(
        self, server, sk, username='edgedb', db='edgedb', proto='edgeql'
    ):
        with self.http_con(server, keep_alive=False) as con:
            return self.http_con_request(
                con,
                path=f'/db/{db}/{proto}',
                # ... the graphql ones will produce an error, but that's
                # still a 200
                params=dict(query='select 1'),
                headers={
                    'Authorization': f'bearer {sk}',
                    'X-EdgeDB-User': username,
                },
            )

    def _jwt_gql_request(self, server, sk):
        return self._jwt_http_request(server, sk, proto='graphql')

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_auth_jwt_1(self):
        jwk_fd, jwk_file = tempfile.mkstemp()

        key = jwcrypto.jwk.JWK(generate='EC')
        with open(jwk_fd, "wb") as f:
            f.write(key.export_to_pem(private_key=True, password=None))
        jwk = secretkey.load_secret_key(pathlib.Path(jwk_file))
        async with tb.start_edgedb_server(
            jws_key_file=pathlib.Path(jwk_file),
            default_auth_method=args.ServerAuthMethod.JWT,
            extra_args=["--instance-name=localtest"],
        ) as sd:
            base_sk = secretkey.generate_secret_key(jwk)
            conn = await sd.connect(secret_key=base_sk)
            await conn.execute('''
                CREATE SUPERUSER ROLE foo {
                    SET password := 'foo-pass';
                }
            ''')
            # Force foo to use passwords for simple auth so auth fails
            await self.con.query('''
                CONFIGURE INSTANCE INSERT Auth {
                    comment := 'foo-jwt',
                    priority := -1,
                    user := 'foo',
                    method := (INSERT Password {
                        transports := "SIMPLE_HTTP",
                    }),
                }
            ''')
            await conn.execute('''
                CREATE EXTENSION edgeql_http;
                CREATE EXTENSION graphql;
            ''')
            await conn.aclose()

            # bad secret keys
            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: malformed JWT',
            ):
                await sd.connect(secret_key='wrong')

            sk = secretkey.generate_secret_key(jwk)
            corrupt_sk = sk[:50] + "0" + sk[51:]

            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: Verification failed',
            ):
                await sd.connect(secret_key=corrupt_sk)

            body, _, code = await self._jwt_http_request(sd, corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(sd, corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")

            # Try to mess up the *signature* part of it
            wrong_sk = sk[:-20] + ("1" if sk[-20] == "0" else "0") + sk[-20:]
            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: Verification failed',
            ):
                await sd.connect(secret_key=wrong_sk)

            body, _, code = await self._jwt_http_request(
                sd, corrupt_sk, db='non_existant')
            self.assertEqual(code, 401, f"Wrong result: {body}")

            # Good key (control check, mostly)
            body, _, code = await self._jwt_http_request(sd, base_sk)
            self.assertEqual(code, 200, f"Wrong result: {body}")
            # Good key but nonexistant user
            body, _, code = await self._jwt_http_request(
                sd, base_sk, username='elonmusk')
            self.assertEqual(code, 401, f"Wrong result: {body}")
            # Good key but user needs password auth
            body, _, code = await self._jwt_http_request(
                sd, base_sk, username='elonmusk')
            self.assertEqual(code, 401, f"Wrong result: {body}")

            good_keys = [
                [],
                [("roles", ["edgedb"])],
                [("databases", ["edgedb"])],
                [("instances", ["localtest"])],
            ]

            for params in good_keys:
                params_dict = dict(params)
                with self.subTest(**params_dict):
                    sk = secretkey.generate_secret_key(jwk, **params_dict)
                    conn = await sd.connect(secret_key=sk)
                    await conn.aclose()

                    body, _, code = await self._jwt_http_request(sd, sk)
                    self.assertEqual(code, 200, f"Wrong result: {body}")
                    body, _, code = await self._jwt_gql_request(sd, sk)
                    self.assertEqual(code, 200, f"Wrong result: {body}")

            bad_keys = {
                (("roles", ("bad-role",)),):
                    'secret key does not authorize access '
                    + 'in role "edgedb"',
                (("databases", ("bad-database",)),):
                    'secret key does not authorize access '
                    + 'to database "edgedb"',
                (("instances", ("bad-instance",)),):
                    'secret key does not authorize access '
                    + 'to this instance',
            }

            for params, msg in bad_keys.items():
                params_dict = dict(params)
                with self.subTest(**params_dict):
                    sk = secretkey.generate_secret_key(jwk, **params_dict)
                    with self.assertRaisesRegex(
                        edgedb.AuthenticationError,
                        "authentication failed: " + msg,
                    ):
                        await sd.connect(secret_key=sk)

                    body, _, code = await self._jwt_http_request(sd, sk)
                    self.assertEqual(code, 401, f"Wrong result: {body}")
                    body, _, code = await self._jwt_gql_request(sd, sk)
                    self.assertEqual(code, 401, f"Wrong result: {body}")

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_auth_jwt_2(self):
        jwk_fd, jwk_file = tempfile.mkstemp()

        key = jwcrypto.jwk.JWK(generate='EC')
        with open(jwk_fd, "wb") as f:
            f.write(key.export_to_pem(private_key=True, password=None))

        allowlist_fd, allowlist_file = tempfile.mkstemp()
        os.close(allowlist_fd)

        revokelist_fd, revokelist_file = tempfile.mkstemp()
        os.close(revokelist_fd)

        subject = "test"
        key_id = "foobar"

        async with tb.start_edgedb_server(
            jws_key_file=jwk_file,
            jwt_sub_allowlist_file=allowlist_file,
            jwt_revocation_list_file=revokelist_file,
        ) as sd:

            jwk = secretkey.load_secret_key(pathlib.Path(jwk_file))

            # enable JWT
            conn = await sd.connect()
            await conn.query("""
                CONFIGURE INSTANCE INSERT Auth {
                    comment := 'test',
                    priority := 0,
                    method := (INSERT JWT {
                        transports := cfg::ConnectionTransport.TCP
                    }),
                }
            """)
            await conn.aclose()

            # Try connecting with "test" not being in the allowlist.
            sk = secretkey.generate_secret_key(
                jwk,
                subject=subject,
                key_id=key_id,
            )
            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: unauthorized subject',
            ):
                await sd.connect(secret_key=sk)

            # Now add it to the allowlist.
            with open(allowlist_file, "w") as f:
                f.write(subject)
            os.kill(sd.pid, signal.SIGHUP)

            await asyncio.sleep(1)

            conn = await sd.connect(secret_key=sk)
            await conn.aclose()

            # Now revoke the key
            with open(revokelist_file, "w") as f:
                f.write(key_id)
            os.kill(sd.pid, signal.SIGHUP)

            await asyncio.sleep(1)

            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: revoked key',
            ):
                await sd.connect(secret_key=sk)

    async def test_server_auth_in_transaction(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        async with self.con.transaction():
            await self.con.query('''
                CREATE SUPERUSER ROLE foo {
                    SET password := 'foo-pass';
                };
            ''')

        try:
            conn = await self.connect(
                user='foo',
                password='foo-pass',
            )
            await conn.aclose()
        finally:
            await self.con.query('''
                DROP ROLE foo;
            ''')
