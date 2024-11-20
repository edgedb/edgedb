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
import platform
import signal
import ssl
import tempfile
import unittest
import urllib.error
import urllib.request

import jwcrypto.jwk

import edgedb

from edb import errors
from edb import protocol
from edb.common import secretkey
from edb.server import args
from edb.server import cluster as edbcluster
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

    async def _http_request(
        self,
        server,
        *,
        sk=None,
        username='edgedb',
        password=None,
        db='edgedb',
        proto='edgeql',
        client_cert_file=None,
        client_key_file=None,
    ):
        with self.http_con(
            server,
            keep_alive=False,
            client_cert_file=client_cert_file,
            client_key_file=client_key_file,
        ) as con:
            headers = {'X-EdgeDB-User': username}
            if sk is not None:
                headers['Authorization'] = f'bearer {sk}'
            elif password is not None:
                headers['Authorization'] = self.make_auth_header(
                    username, password)
            return self.http_con_request(
                con,
                path=f'/db/{db}/{proto}',
                # ... the graphql ones will produce an error, but that's
                # still a 200
                params=dict(query='select 1'),
                headers=headers,
            )

    async def _jwt_http_request(
        self,
        server,
        *,
        sk=None,
        username='edgedb',
        password=None,
        db='edgedb',
        proto='edgeql',
    ):
        return await self._http_request(
            server,
            sk=sk,
            username=username,
            password=password,
            db=db,
            proto=proto,
        )

    def _jwt_gql_request(self, server, *, sk=None, password=None):
        return self._jwt_http_request(
            server,
            sk=sk,
            password=password,
            proto='graphql',
        )

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
        ) as sd:
            base_sk = secretkey.generate_secret_key(jwk)
            conn = await sd.connect(secret_key=base_sk)
            await conn.execute('''
                CREATE SUPERUSER ROLE foo {
                    SET password := 'foo-pass';
                }
            ''')
            # Force foo to use passwords for simple auth so auth fails
            await conn.query('''
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

            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: no authorization data provided',
            ):
                await sd.connect()

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

            body, _, code = await self._jwt_http_request(sd, sk=corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(sd, sk=corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")

            # Try to mess up the *signature* part of it
            wrong_sk = sk[:-20] + ("1" if sk[-20] == "0" else "0") + sk[-20:]
            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'authentication failed: Verification failed',
            ):
                await sd.connect(secret_key=wrong_sk)

            body, _, code = await self._jwt_http_request(
                sd, sk=corrupt_sk, db='non_existant')
            self.assertEqual(code, 401, f"Wrong result: {body}")

            # Good key (control check, mostly)
            body, _, code = await self._jwt_http_request(sd, sk=base_sk)
            self.assertEqual(code, 200, f"Wrong result: {body}")
            # Good key but nonexistant user
            body, _, code = await self._jwt_http_request(
                sd, sk=base_sk, username='elonmusk')
            self.assertEqual(code, 401, f"Wrong result: {body}")
            # Good key but user needs password auth
            body, _, code = await self._jwt_http_request(
                sd, sk=base_sk, username='foo')
            self.assertEqual(code, 401, f"Wrong result: {body}")

            good_keys = [
                [],
                [("roles", ["admin"])],
                [("databases", ["main"])],
                [("instances", ["localtest"])],
            ]

            for params in good_keys:
                params_dict = dict(params)
                with self.subTest(**params_dict):
                    sk = secretkey.generate_secret_key(jwk, **params_dict)
                    conn = await sd.connect(secret_key=sk)
                    await conn.aclose()

                    body, _, code = await self._jwt_http_request(sd, sk=sk)
                    self.assertEqual(code, 200, f"Wrong result: {body}")
                    body, _, code = await self._jwt_gql_request(sd, sk=sk)
                    self.assertEqual(code, 200, f"Wrong result: {body}")

            bad_keys = {
                (("roles", ("bad-role",)),):
                    'secret key does not authorize access '
                    + 'in role "admin"',
                (("databases", ("bad-database",)),):
                    'secret key does not authorize access '
                    + 'to database "main"',
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

                    body, _, code = await self._jwt_http_request(sd, sk=sk)
                    self.assertEqual(code, 401, f"Wrong result: {body}")
                    body, _, code = await self._jwt_gql_request(sd, sk=sk)
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

        with self.assertRaisesRegex(
            edbcluster.ClusterError, "cannot load JWT"
        ):
            async with tb.start_edgedb_server(
                jws_key_file=jwk_file,
                jwt_sub_allowlist_file='/tmp/non_existant',
                jwt_revocation_list_file='/tmp/non_existant',
            ):
                pass

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

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_auth_multiple_methods(self):
        jwk_fd, jwk_file = tempfile.mkstemp()

        key = jwcrypto.jwk.JWK(generate='EC')
        with open(jwk_fd, "wb") as f:
            f.write(key.export_to_pem(private_key=True, password=None))
        jwk = secretkey.load_secret_key(pathlib.Path(jwk_file))
        async with tb.start_edgedb_server(
            jws_key_file=pathlib.Path(jwk_file),
            default_auth_method=args.ServerAuthMethods({
                args.ServerConnTransport.TCP: [
                    args.ServerAuthMethod.JWT,
                    args.ServerAuthMethod.Scram,
                ],
                args.ServerConnTransport.SIMPLE_HTTP: [
                    args.ServerAuthMethod.Password,
                    args.ServerAuthMethod.JWT,
                ],
            }),
        ) as sd:
            base_sk = secretkey.generate_secret_key(jwk)
            conn = await sd.connect(secret_key=base_sk)
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
                await sd.connect(secret_key='wrong', password=None)

            # But connecting with the default password should still work
            # because we are defaulting to Scram/JWT
            c1 = await sd.connect(secret_key='wrong')
            await c1.aclose()

            sk = secretkey.generate_secret_key(jwk)

            body, _, code = await self._jwt_http_request(sd, sk=sk)
            self.assertEqual(code, 200, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(sd, sk=sk)
            self.assertEqual(code, 200, f"Wrong result: {body}")

            corrupt_sk = sk[:50] + "0" + sk[51:]
            body, _, code = await self._jwt_http_request(sd, sk=corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(sd, sk=corrupt_sk)
            self.assertEqual(code, 401, f"Wrong result: {body}")

            body, _, code = await self._jwt_http_request(
                sd, password=sd.password)
            self.assertEqual(code, 200, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(
                sd, password=sd.password)
            self.assertEqual(code, 200, f"Wrong result: {body}")

            body, _, code = await self._jwt_http_request(
                sd, password="wrong password")
            self.assertEqual(code, 401, f"Wrong result: {body}")
            body, _, code = await self._jwt_gql_request(
                sd, password="wrong password")
            self.assertEqual(code, 401, f"Wrong result: {body}")

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

    @unittest.skipIf(
        platform.system() == "Darwin" and platform.machine() == 'x86_64',
        "Postgres is not getting getting enough shared memory on macos-14 "
        "GitHub runner by default"
    )
    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_auth_mtls(self):
        if not self.has_create_role:
            self.skipTest('create role is not supported by the backend')

        certs = pathlib.Path(__file__).parent / 'certs'
        client_ca_cert_file = certs / 'client_ca.cert.pem'
        client_ssl_cert_file = certs / 'client.cert.pem'
        client_ssl_key_file = certs / 'client.key.pem'
        async with tb.start_edgedb_server(
            tls_client_ca_file=client_ca_cert_file,
            security=args.ServerSecurityMode.Strict,
        ) as sd:
            # Setup mTLS and extensions
            conn = await sd.connect()
            try:
                await conn.query("CREATE SUPERUSER ROLE ssl_user;")
                await conn.query("CREATE EXTENSION edgeql_http;")
                await self._test_mtls(
                    sd, client_ssl_cert_file, client_ssl_key_file, False)
                await conn.query("""
                    CONFIGURE INSTANCE INSERT Auth {
                        comment := 'test',
                        priority := 0,
                        method := (INSERT mTLS {
                            transports := {
                                cfg::ConnectionTransport.TCP,
                                cfg::ConnectionTransport.TCP_PG,
                                cfg::ConnectionTransport.HTTP,
                                cfg::ConnectionTransport.SIMPLE_HTTP,
                            },
                        }),
                    }
                """)
                await self._test_mtls(
                    sd, client_ssl_cert_file, client_ssl_key_file, True)
            finally:
                await conn.aclose()

    async def _test_mtls(
        self, sd, client_ssl_cert_file, client_ssl_key_file, granted
    ):
        # Verifies mTLS authentication on edgeql_http
        if granted:
            body, _, code = await self._http_request(sd, username="ssl_user")
            self.assertEqual(code, 401, f"Wrong result: {body}")
        body, _, code = await self._http_request(
            sd,
            username="ssl_user",
            client_cert_file=client_ssl_cert_file,
            client_key_file=client_ssl_key_file,
        )
        if granted:
            self.assertEqual(code, 200, f"Wrong result: {body}")
        else:
            self.assertEqual(code, 401, f"Wrong result: {body}")

        # Verifies mTLS authentication on the binary protocol
        if granted:
            with self.assertRaisesRegex(
                edgedb.AuthenticationError,
                'client certificate required',
            ):
                await sd.connect()
        # FIXME: add mTLS support in edgedb-python

        # Verifies mTLS authentication on binary protocol over HTTP
        if granted:
            with self.http_con(
                sd,
                keep_alive=False,
            ) as con:
                msgs, _, status = self.http_con_binary_request(
                    con, "select 42", user="ssl_user")
            self.assertEqual(status, 200)
            self.assertIsInstance(msgs[0], protocol.ErrorResponse)
            self.assertEqual(
                msgs[0].error_code, errors.AuthenticationError.get_code())
        with self.http_con(
            sd,
            keep_alive=False,
            client_cert_file=client_ssl_cert_file,
            client_key_file=client_ssl_key_file,
        ) as con:
            msgs, _, status = self.http_con_binary_request(
                con, "select 42", user="ssl_user")
        if granted:
            self.assertEqual(status, 200)
            self.assertIsInstance(msgs[0], protocol.CommandDataDescription)
            self.assertIsInstance(msgs[1], protocol.Data)
            self.assertEqual(bytes(msgs[1].data[0].data), b"42")
            self.assertIsInstance(msgs[2], protocol.CommandComplete)
            self.assertEqual(msgs[2].status, "SELECT")
            self.assertIsInstance(msgs[3], protocol.ReadyForCommand)
        else:
            self.assertEqual(status, 200)
            self.assertIsInstance(msgs[0], protocol.ErrorResponse)
            self.assertEqual(
                msgs[0].error_code, errors.AuthenticationError.get_code())

        # Verifies mTLS authentication on emulated Postgres protocol
        try:
            import asyncpg
        except ImportError:
            # don't run pg-ext test if asyncpg is not installed
            pass
        else:
            conargs = sd.get_connect_args()
            tls_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=conargs["tls_ca_file"],
            )
            tls_context.check_hostname = False
            conargs = dict(
                host=conargs['host'],
                port=conargs['port'],
                user="ssl_user",
                database=conargs.get('database', 'main'),
                ssl=tls_context,
            )
            if granted:
                with self.assertRaisesRegex(
                    asyncpg.InvalidAuthorizationSpecificationError,
                    'client certificate required',
                ):
                    await asyncpg.connect(**conargs)
            tls_context.load_cert_chain(
                client_ssl_cert_file, client_ssl_key_file)
            if granted:
                conn = await asyncpg.connect(**conargs)
                self.assertEqual(await conn.fetchval("select 42"), 42)
                await conn.close()
            else:
                with self.assertRaisesRegex(
                    asyncpg.InvalidAuthorizationSpecificationError,
                    'authentication failed',
                ):
                    await asyncpg.connect(**conargs)
