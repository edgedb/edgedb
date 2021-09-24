#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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
import warnings
from typing import *

import asyncio
import contextlib
import dataclasses
import ipaddress
import os
import pathlib
import pickle
import shutil
import ssl
import stat
import sys
import tempfile
import textwrap
import unittest
import unittest.mock
import urllib.parse

import click
from click.testing import CliRunner

from edb.server import args as edb_args
from edb.server import bootstrap
from edb.server import pgcluster
from edb.server import pgconnparams
from edb.server.pgconnparams import SSLMode
from edb.server import pgcon
from edb.server.pgcon import errors
from edb.testbase import server as tb
from edb.tools import test


CERTS = os.path.join(os.path.dirname(__file__), 'certs')
SSL_CA_CERT_FILE = os.path.join(CERTS, 'ca.cert.pem')
SSL_CA_CRL_FILE = os.path.join(CERTS, 'ca.crl.pem')
SSL_CERT_FILE = os.path.join(CERTS, 'server.cert.pem')
SSL_KEY_FILE = os.path.join(CERTS, 'server.key.pem')
CLIENT_CA_CERT_FILE = os.path.join(CERTS, 'client_ca.cert.pem')
CLIENT_SSL_CERT_FILE = os.path.join(CERTS, 'client.cert.pem')
CLIENT_SSL_KEY_FILE = os.path.join(CERTS, 'client.key.pem')
CLIENT_SSL_PROTECTED_KEY_FILE = os.path.join(CERTS, 'client.key.protected.pem')


@contextlib.contextmanager
def mock_dot_postgresql(*, ca=True, crl=False, client=False, protected=False):
    with tempfile.TemporaryDirectory() as temp_dir:
        home = pathlib.Path(temp_dir)
        pg_home = home / '.postgresql'
        pg_home.mkdir()
        if ca:
            shutil.copyfile(SSL_CA_CERT_FILE, pg_home / 'root.crt')
        if crl:
            shutil.copyfile(SSL_CA_CRL_FILE, pg_home / 'root.crl')
        if client:
            shutil.copyfile(CLIENT_SSL_CERT_FILE, pg_home / 'postgresql.crt')
            if protected:
                shutil.copyfile(
                    CLIENT_SSL_PROTECTED_KEY_FILE, pg_home / 'postgresql.key'
                )
            else:
                shutil.copyfile(
                    CLIENT_SSL_KEY_FILE, pg_home / 'postgresql.key'
                )
        with unittest.mock.patch(
            'pathlib.Path.home', unittest.mock.Mock(return_value=home)
        ):
            yield


def _get_initdb_options(initdb_options=None):
    if not initdb_options:
        initdb_options = {}
    else:
        initdb_options = dict(initdb_options)

    # Make the default superuser name stable.
    if 'username' not in initdb_options:
        initdb_options['username'] = 'postgres'

    return initdb_options


@click.command()
@edb_args.server_options
def get_default_args(version, **kwargs):
    pickle.dump(kwargs, sys.stdout.buffer)


class TempCluster(pgcluster.Cluster):
    def __init__(self, *,
                 data_dir_suffix=None, data_dir_prefix=None,
                 data_dir_parent=None):
        self._data_dir = tempfile.mkdtemp(suffix=data_dir_suffix,
                                          prefix=data_dir_prefix,
                                          dir=data_dir_parent)
        super().__init__(self._data_dir)


class ClusterTestCase(tb.TestCase):
    cluster: Optional[TempCluster]
    tenant_id: Optional[str]
    loop: asyncio.AbstractEventLoop

    @classmethod
    def get_server_settings(cls):
        return {
            'log_connections': 'on',
            # JITting messes up timing tests, and
            # is not essential for testing.
            'jit': 'off',
            'listen_addresses': '127.0.0.1',
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls.init_temp_cluster())

    @classmethod
    async def init_temp_cluster(cls):
        cluster = cls.cluster = TempCluster()
        await cluster.lookup_postgres()
        cluster.set_connection_params(
            pgconnparams.ConnectionParameters(
                user='postgres',
                database='postgres',
            ),
        )
        await cluster.init(**_get_initdb_options({}))
        await cluster.trust_local_connections()
        port = tb.find_available_port()
        await cluster.start(
            port=port, server_settings=cls.get_server_settings())
        result = CliRunner().invoke(get_default_args, [])
        arg_input = pickle.loads(result.stdout_bytes)
        arg_input["data_dir"] = pathlib.Path(cluster.get_data_dir())
        arg_input["generate_self_signed_cert"] = True
        cls.tenant_id = cluster.get_runtime_params().instance_params.tenant_id
        cls.dbname = cluster.get_db_name('edgedb')
        args = edb_args.parse_args(**arg_input)
        await bootstrap.ensure_bootstrapped(cluster, args)

    @classmethod
    def tearDownClass(cls):
        try:
            cluster, cls.cluster = cls.cluster, None
            if cluster is not None:
                try:
                    cls.loop.run_until_complete(cluster.stop())
                finally:
                    cluster.destroy()
        finally:
            super().tearDownClass()

    @classmethod
    def get_connection_spec(cls, kwargs=None):
        if not kwargs:
            kwargs = {}
        conn_spec = cls.cluster.get_connection_spec()
        conn_spec['host'] = 'localhost'
        if kwargs.get('dsn'):
            addrs, params = pgconnparams.parse_dsn(kwargs['dsn'])
            for k in (
                'user',
                'password',
                'database',
                'ssl',
                'sslmode',
                'server_settings',
            ):
                v = getattr(params, k)
                if v is not None:
                    conn_spec[k] = v
        conn_spec.update(kwargs)
        if not os.environ.get('PGHOST') and not kwargs.get('dsn'):
            if 'database' not in conn_spec:
                conn_spec['database'] = 'postgres'
            if 'user' not in conn_spec:
                conn_spec['user'] = 'postgres'
        return conn_spec

    @classmethod
    def connect(cls, **kwargs):
        conn_spec = cls.get_connection_spec(kwargs)
        return pgcon.connect(conn_spec, cls.dbname, cls.tenant_id)

    def setUp(self):
        super().setUp()

        self.con = self.loop.run_until_complete(self.connect())

    def tearDown(self):
        try:
            self.con.terminate()
            self.con = None
        finally:
            super().tearDown()

    async def assertConnected(self, con):
        self.assertEqual(
            await con.simple_query(b'SELECT 42', ignore_data=False),
            [[b'42']],
        )


class TestAuthentication(ClusterTestCase):
    def setUp(self):
        super().setUp()

        methods = [
            ('trust', None),
            ('reject', None),
            ('scram-sha-256', 'correctpassword'),
            ('md5', 'correctpassword'),
            ('password', 'correctpassword'),
        ]

        self.cluster.reset_hba()

        create_script = []
        for method, password in methods:
            username = method.replace('-', '_')

            # if this is a SCRAM password, we need to set the encryption method
            # to "scram-sha-256" in order to properly hash the password
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'scram-sha-256';"
                )

            create_script.append(
                'CREATE ROLE {}_user WITH LOGIN{};'.format(
                    username,
                    ' PASSWORD {!r}'.format(password) if password else ''
                )
            )
            create_script.append(
                f'GRANT postgres TO {username}_user;'
            )

            # to be courteous to the MD5 test, revert back to MD5 after the
            # scram-sha-256 password is set
            if method == 'scram-sha-256':
                create_script.append(
                    "SET password_encryption = 'md5';"
                )

            self.cluster.add_hba_entry(
                type='local',
                database=self.dbname, user='{}_user'.format(username),
                auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('127.0.0.0/24'),
                database=self.dbname, user='{}_user'.format(username),
                auth_method=method)

            self.cluster.add_hba_entry(
                type='host', address=ipaddress.ip_network('::1/128'),
                database=self.dbname, user='{}_user'.format(username),
                auth_method=method)

        # Put hba changes into effect
        self.loop.run_until_complete(self.cluster.reload())

        create_script = '\n'.join(create_script).encode()
        self.loop.run_until_complete(
            self.con.simple_query(create_script, ignore_data=True)
        )

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.loop.run_until_complete(self.cluster.trust_local_connections())

        methods = [
            'trust',
            'reject',
            'scram-sha-256',
            'md5',
            'password',
        ]

        drop_script = []
        for method in methods:
            username = method.replace('-', '_')

            drop_script.append('DROP ROLE {}_user;'.format(username))

        drop_script = '\n'.join(drop_script).encode()
        self.loop.run_until_complete(
            self.con.simple_query(drop_script, ignore_data=True)
        )

        super().tearDown()

    async def test_auth_bad_user(self):
        with self.assertRaises(errors.BackendError) as cm:
            await self.connect(user='__nonexistent__')
        self.assertTrue(
            cm.exception.code_is(
                errors.ERROR_INVALID_AUTHORIZATION_SPECIFICATION
            )
        )

    async def test_auth_trust(self):
        conn = await self.connect(user='trust_user')
        conn.terminate()

    async def test_auth_reject(self):
        with self.assertRaisesRegex(
            errors.BackendError,
            'pg_hba.conf rejects connection'
        ):
            await self.connect(user='reject_user')

    async def test_auth_password_cleartext(self):
        with self.assertRaisesRegex(RuntimeError, 'unsupported auth method'):
            await self.connect(
                user='password_user',
                password='correctpassword')

    async def test_auth_password_md5(self):
        conn = await self.connect(
            user='md5_user', password='correctpassword')
        conn.terminate()

        with self.assertRaisesRegex(
            errors.BackendError,
            'password authentication failed for user "md5_user"'
        ):
            await self.connect(
                user='md5_user', password='wrongpassword')

    @test.not_implemented("SCRAM SHA-256 auth method")
    async def test_auth_password_scram_sha_256(self):
        conn = await self.connect(
            user='scram_sha_256_user', password='correctpassword')
        conn.terminate()

        with self.assertRaisesRegex(
            errors.BackendError,
            'password authentication failed for user "scram_sha_256_user"'
        ):
            await self.connect(
                user='scram_sha_256_user', password='wrongpassword')

        # various SASL prep tests
        # first ensure that password are being hashed for SCRAM-SHA-256
        await self.con.execute("SET password_encryption = 'scram-sha-256';")
        alter_password = "ALTER ROLE scram_sha_256_user PASSWORD E{!r};"
        passwords = [
            'nonascii\u1680space',  # C.1.2
            'common\u1806nothing',  # B.1
            'ab\ufb01c',            # normalization
            'ab\u007fc',            # C.2.1
            'ab\u206ac',            # C.2.2, C.6
            'ab\ue000c',            # C.3, C.5
            'ab\ufdd0c',            # C.4
            'ab\u2ff0c',            # C.7
            'ab\u2000c',            # C.8
            'ab\ue0001',            # C.9
        ]

        # ensure the passwords that go through SASLprep work
        for password in passwords:
            # update the password
            await self.con.execute(alter_password.format(password))
            # test to see that passwords are properly SASL prepped
            conn = await self.connect(
                user='scram_sha_256_user', password=password)
            conn.terminate()

        alter_password = \
            "ALTER ROLE scram_sha_256_user PASSWORD 'correctpassword';"
        await self.con.execute(alter_password)
        await self.con.execute("SET password_encryption = 'md5';")

    async def test_auth_unsupported(self):
        pass


class TestConnectParams(tb.TestCase):

    TESTS = [
        {
            'name': 'all_env_default_ssl',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },
            'result': ([('host', 123)], {
                'user': 'user',
                'password': 'passw',
                'database': 'testdb',
                'ssl': True,
                'sslmode': SSLMode.prefer})
        },

        {
            'name': 'dsn_override_env',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123'
            },

            'dsn': 'postgres://user2:passw2@host2:456/db2',

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2'})
        },

        {
            'name': 'dsn_override_env_ssl',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'allow'
            },

            'dsn': 'postgres://user2:passw2@host2:456/db2?sslmode=disable',

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2',
                'sslmode': SSLMode.disable,
                'ssl': None})
        },

        {
            'name': 'dsn_overrides_env_partially',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'allow'
            },

            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',

            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef',
                'ssl': True,
                'sslmode': SSLMode.allow})
        },

        {
            'name': 'dsn_override_env_ssl_prefer',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'prefer'
            },

            'dsn': 'postgres://user2:passw2@host2:456/db2?sslmode=disable',

            'result': ([('host2', 456)], {
                'user': 'user2',
                'password': 'passw2',
                'database': 'db2',
                'sslmode': SSLMode.disable,
                'ssl': None})
        },

        {
            'name': 'dsn_overrides_env_partially_ssl_prefer',
            'env': {
                'PGUSER': 'user',
                'PGDATABASE': 'testdb',
                'PGPASSWORD': 'passw',
                'PGHOST': 'host',
                'PGPORT': '123',
                'PGSSLMODE': 'prefer'
            },

            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',

            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef',
                'ssl': True,
                'sslmode': SSLMode.prefer})
        },

        {
            'name': 'dsn_only',
            'dsn': 'postgres://user3:123123@localhost:5555/abcdef',
            'result': ([('localhost', 5555)], {
                'user': 'user3',
                'password': '123123',
                'database': 'abcdef'})
        },

        {
            'name': 'dsn_only_multi_host',
            'dsn': 'postgresql://user@host1,host2/db',
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'user',
            })
        },

        {
            'name': 'dsn_only_multi_host_and_port',
            'dsn': 'postgresql://user@host1:1111,host2:2222/db',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'user',
            })
        },

        {
            'name': 'dsn_combines_env_multi_host',
            'env': {
                'PGHOST': 'host1:1111,host2:2222',
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'dsn_multi_host_combines_env',
            'env': {
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql:///db?host=host1:1111,host2:2222',
            'result': ([('host1', 1111), ('host2', 2222)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'params_multi_host_dsn_env_mix',
            'env': {
                'PGUSER': 'foo',
            },
            'dsn': 'postgresql://host1,host2/db',
            'result': ([('host1', 5432), ('host2', 5432)], {
                'database': 'db',
                'user': 'foo',
            })
        },

        {
            'name': 'dsn_settings_override_and_ssl',
            'dsn': 'postgresql://me:ask@127.0.0.1:888/'
                   'db?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb&sslmode=require',
            'result': ([('127.0.0.1', 888)], {
                'server_settings': {'param': '123'},
                'user': 'me',
                'password': 'ask',
                'database': 'db',
                'ssl': True,
                'sslmode': SSLMode.require})
        },

        {
            'name': 'multiple_settings',
            'dsn': 'postgresql://me:ask@127.0.0.1:888/'
                   'db?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb&sslmode=verify_full'
                   '&aa=bb',
            'result': ([('127.0.0.1', 888)], {
                'server_settings': {'aa': 'bb', 'param': '123'},
                'user': 'me',
                'password': 'ask',
                'database': 'db',
                'sslmode': SSLMode.verify_full,
                'ssl': True})
        },

        {
            'name': 'dsn_only_unix',
            'dsn': 'postgresql:///dbname?host=/unix_sock/test&user=spam',
            'result': ([('/unix_sock/test', 5432)], {
                'user': 'spam',
                'database': 'dbname'})
        },

        {
            'name': 'dsn_only_quoted',
            'dsn': 'postgresql://us%40r:p%40ss@h%40st1,h%40st2:543%33/d%62',
            'result': (
                [('h@st1', 5432), ('h@st2', 5433)],
                {
                    'user': 'us@r',
                    'password': 'p@ss',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_unquoted_host',
            'dsn': 'postgresql://user:p@ss@host/db',
            'result': (
                [('ss@host', 5432)],
                {
                    'user': 'user',
                    'password': 'p',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_quoted_params',
            'dsn': 'postgresql:///d%62?user=us%40r&host=h%40st&port=543%33',
            'result': (
                [('h@st', 5433)],
                {
                    'user': 'us@r',
                    'database': 'db',
                }
            )
        },

        {
            'name': 'dsn_only_illegal_protocol',
            'dsn': 'pq:///dbname?host=/unix_sock/test&user=spam',
            'error': (ValueError, 'invalid DSN')
        },
        {
            'name': 'env_ports_mismatch_dsn_multi_hosts',
            'dsn': 'postgresql://host1,host2,host3/db',
            'env': {'PGPORT': '111,222'},
            'error': (
                ValueError,
                'could not match 2 port numbers to 3 hosts'
            )
        },
        {
            'name': 'dsn_only_quoted_unix_host_port_in_params',
            'dsn': 'postgres://user@?port=56226&host=%2Ftmp',
            'result': (
                [('/tmp', 56226)],
                {
                    'user': 'user',
                    'database': 'user',
                    'sslmode': SSLMode.disable,
                    'ssl': None
                }
            )
        },
        {
            'name': 'dsn_only_cloudsql',
            'dsn': 'postgres:///db?host=/cloudsql/'
                   'project:region:instance-name&user=spam',
            'result': (
                [(
                    '/cloudsql/project:region:instance-name',
                    5432,
                )], {
                    'user': 'spam',
                    'database': 'db'
                }
            )
        },
        {
            'name': 'dsn_only_cloudsql_unix_and_tcp',
            'dsn': 'postgres:///db?host=127.0.0.1:5432,/cloudsql/'
                   'project:region:instance-name,localhost:5433&user=spam',
            'result': (
                [
                    ('127.0.0.1', 5432),
                    (
                        '/cloudsql/project:region:instance-name',
                        5432,
                    ),
                    ('localhost', 5433)
                ], {
                    'user': 'spam',
                    'database': 'db',
                    'ssl': True,
                    'sslmode': SSLMode.prefer,
                }
            )
        },
    ]

    @contextlib.contextmanager
    def environ(self, **kwargs):
        old_vals = {}
        for key in kwargs:
            if key in os.environ:
                old_vals[key] = os.environ[key]

        for key, val in kwargs.items():
            if val is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = val

        try:
            yield
        finally:
            for key in kwargs:
                if key in os.environ:
                    del os.environ[key]
            for key, val in old_vals.items():
                os.environ[key] = val

    def run_testcase(self, testcase):
        env = testcase.get('env', {})
        test_env = {'PGHOST': None, 'PGPORT': None,
                    'PGUSER': None, 'PGPASSWORD': None,
                    'PGDATABASE': None, 'PGSSLMODE': None}
        test_env.update(env)

        dsn = testcase.get('dsn', 'postgres://')

        expected = testcase.get('result')
        expected_error = testcase.get('error')
        if expected is None and expected_error is None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified')
        if expected is not None and expected_error is not None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified, got both')

        with contextlib.ExitStack() as es:
            es.enter_context(self.subTest(dsn=dsn, env=env))
            es.enter_context(self.environ(**test_env))

            if expected_error:
                es.enter_context(self.assertRaisesRegex(*expected_error))

            addrs, conn_params = pgconnparams.parse_dsn(dsn=dsn)

            params = {}
            for k in dataclasses.fields(conn_params):
                k = k.name
                v = getattr(conn_params, k)
                if v or (expected is not None and k in expected[1]):
                    params[k] = v

            if isinstance(params.get('ssl'), ssl.SSLContext):
                params['ssl'] = True

            result = (list(addrs), params)

        if expected is not None:
            if 'ssl' not in expected[1]:
                # Avoid the hassle of specifying the default SSL mode
                # unless explicitly tested for.
                params.pop('ssl', None)
                params.pop('sslmode', None)

            self.assertEqual(
                expected,
                result,
                'Testcase: {}'.format(testcase.get('name', testcase))
            )

    def test_test_connect_params_environ(self):
        self.assertNotIn('AAAAAAAAAA123', os.environ)
        self.assertNotIn('AAAAAAAAAA456', os.environ)
        self.assertNotIn('AAAAAAAAAA789', os.environ)

        try:

            os.environ['AAAAAAAAAA456'] = '123'
            os.environ['AAAAAAAAAA789'] = '123'

            with self.environ(AAAAAAAAAA123='1',
                              AAAAAAAAAA456='2',
                              AAAAAAAAAA789=None):

                self.assertEqual(os.environ['AAAAAAAAAA123'], '1')
                self.assertEqual(os.environ['AAAAAAAAAA456'], '2')
                self.assertNotIn('AAAAAAAAAA789', os.environ)

            self.assertNotIn('AAAAAAAAAA123', os.environ)
            self.assertEqual(os.environ['AAAAAAAAAA456'], '123')
            self.assertEqual(os.environ['AAAAAAAAAA789'], '123')

        finally:
            for key in {'AAAAAAAAAA123', 'AAAAAAAAAA456', 'AAAAAAAAAA789'}:
                if key in os.environ:
                    del os.environ[key]

    def test_test_connect_params_run_testcase(self):
        with self.environ(PGPORT='777'):
            self.run_testcase({
                'env': {
                    'PGUSER': '__test__'
                },
                'dsn': 'postgres://abc',
                'result': (
                    [('abc', 5432)],
                    {'user': '__test__', 'database': '__test__'}
                )
            })

    def test_connect_params(self):
        with mock_dot_postgresql():
            for testcase in self.TESTS:
                self.run_testcase(testcase)

    def test_connect_pgpass_regular(self):
        passfile = tempfile.NamedTemporaryFile('w+t', delete=False)
        passfile.write(textwrap.dedent(R'''
            abc:*:*:user:password from pgpass for user@abc
            localhost:*:*:*:password from pgpass for localhost
            cde:5433:*:*:password from pgpass for cde:5433

            *:*:*:testuser:password from pgpass for testuser
            *:*:testdb:*:password from pgpass for testdb
            # comment
            *:*:test\:db:test\\:password from pgpass with escapes
        '''))
        passfile.close()
        os.chmod(passfile.name, stat.S_IWUSR | stat.S_IRUSR)

        try:
            # passfile path in env
            self.run_testcase({
                'env': {
                    'PGPASSFILE': passfile.name
                },
                'dsn': 'postgres://user@abc/db',
                'result': (
                    [('abc', 5432)],
                    {
                        'password': 'password from pgpass for user@abc',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # passfile path in dsn
            self.run_testcase({
                'dsn': 'postgres://user@abc/db?passfile={}'.format(
                    passfile.name),
                'result': (
                    [('abc', 5432)],
                    {
                        'password': 'password from pgpass for user@abc',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            self.run_testcase({
                'dsn': 'postgres://user@localhost/db?passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('localhost', 5432)],
                    {
                        'password': 'password from pgpass for localhost',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # unix socket gets normalized as localhost
            self.run_testcase({
                'dsn': 'postgres:///db?user=user&host=/tmp&passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('/tmp', 5432)],
                    {
                        'password': 'password from pgpass for localhost',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # port matching (also tests that `:` can be part of password)
            self.run_testcase({
                'dsn': 'postgres://user@cde:5433/db?passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('cde', 5433)],
                    {
                        'password': 'password from pgpass for cde:5433',
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

            # user matching
            self.run_testcase({
                'dsn': 'postgres://testuser@def/db?passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('def', 5432)],
                    {
                        'password': 'password from pgpass for testuser',
                        'user': 'testuser',
                        'database': 'db',
                    }
                )
            })

            # database matching
            self.run_testcase({
                'dsn': 'postgres://user@efg/testdb?passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('efg', 5432)],
                    {
                        'password': 'password from pgpass for testdb',
                        'user': 'user',
                        'database': 'testdb',
                    }
                )
            })

            # test escaping
            self.run_testcase({
                'dsn': 'postgres://{}@fgh/{}?passfile={}'.format(
                    R'test\\', R'test\:db', passfile.name
                ),
                'result': (
                    [('fgh', 5432)],
                    {
                        'password': 'password from pgpass with escapes',
                        'user': R'test\\',
                        'database': R'test\:db',
                    }
                )
            })

        finally:
            os.unlink(passfile.name)

    def test_connect_pgpass_badness_mode(self):
        # Verify that .pgpass permissions are checked
        with tempfile.NamedTemporaryFile('w+t') as passfile:
            os.chmod(passfile.name,
                     stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP)

            with self.assertWarnsRegex(
                    UserWarning,
                    'password file .* has group or world access'):
                self.run_testcase({
                    'dsn': 'postgres://user@abc/db?passfile={}'.format(
                        passfile.name
                    ),
                    'result': (
                        [('abc', 5432)],
                        {
                            'user': 'user',
                            'database': 'db',
                        }
                    )
                })

    def test_connect_pgpass_badness_non_file(self):
        # Verify warnings when .pgpass is not a file
        with tempfile.TemporaryDirectory() as passfile:
            with self.assertWarnsRegex(
                    UserWarning,
                    'password file .* is not a plain file'):
                self.run_testcase({
                    'dsn': 'postgres://user@abc/db?passfile={}'.format(
                        passfile
                    ),
                    'result': (
                        [('abc', 5432)],
                        {
                            'user': 'user',
                            'database': 'db',
                        }
                    )
                })

    def test_connect_pgpass_nonexistent(self):
        # nonexistent passfile is OK
        self.run_testcase({
            'dsn': 'postgres://user@abc/db?passfile=totally+nonexistent',
            'result': (
                [('abc', 5432)],
                {
                    'user': 'user',
                    'database': 'db',
                }
            )
        })

    def test_connect_pgpass_inaccessible_file(self):
        with tempfile.NamedTemporaryFile('w+t') as passfile:
            os.chmod(passfile.name, stat.S_IWUSR)

            # nonexistent passfile is OK
            self.run_testcase({
                'dsn': 'postgres://user@abc/db?passfile={}'.format(
                    passfile.name
                ),
                'result': (
                    [('abc', 5432)],
                    {
                        'user': 'user',
                        'database': 'db',
                    }
                )
            })

    def test_connect_pgpass_inaccessible_directory(self):
        with tempfile.TemporaryDirectory() as passdir:
            with tempfile.NamedTemporaryFile('w+t', dir=passdir) as passfile:
                os.chmod(passdir, stat.S_IWUSR)

                try:
                    # nonexistent passfile is OK
                    self.run_testcase({
                        'dsn': 'postgres://user@abc/db?passfile={}'.format(
                            passfile.name
                        ),
                        'result': (
                            [('abc', 5432)],
                            {
                                'user': 'user',
                                'database': 'db',
                            }
                        )
                    })
                finally:
                    os.chmod(passdir, stat.S_IRWXU)


class TestConnection(ClusterTestCase):

    async def test_connection_isinstance(self):
        self.assertTrue(isinstance(self.con, pgcon.PGConnection))
        self.assertTrue(isinstance(self.con, object))
        self.assertFalse(isinstance(self.con, list))

    async def test_connection_use_after_close(self):
        def check():
            return self.assertRaisesRegex(RuntimeError,
                                          'not connected')

        self.con.terminate()

        with check():
            await self.con.simple_query(b'SELECT 1', ignore_data=False)

        with check():
            await self.con.parse_execute_json(None, None, None, None, None)

        with check():
            await self.con.parse_execute_notebook(None, None)

        with check():
            await self.con.parse_execute(None, None, None, False, None, 0)

        with check():
            await self.con.run_ddl(None)

        with check():
            await self.con.dump(None, None, None)

        with check():
            await self.con.restore(None, b'', {})

    async def test_connection_ssl_to_no_ssl_server(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        with self.assertRaisesRegex(ConnectionError, 'rejected SSL'):
            await self.connect(
                host='localhost',
                sslmode=SSLMode.require,
                ssl=ssl_context)

    async def test_connection_sslmode_no_ssl_server(self):
        async def verify_works(sslmode):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/?sslmode=' + sslmode,
                    user='postgres',
                    database='postgres',
                    host='localhost')
                await self.assertConnected(con)
                self.assertFalse(con.is_ssl)
            finally:
                if con:
                    con.terminate()

        async def verify_fails(sslmode):
            con = None
            try:
                with self.assertRaises(ConnectionError):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        user='postgres',
                        database='postgres',
                        host='localhost')
                    await self.assertConnected(con)
            finally:
                if con:
                    con.terminate()

        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require')
        with mock_dot_postgresql():
            await verify_fails('require')
            await verify_fails('verify-ca')
            await verify_fails('verify-full')


class BaseTestSSLConnection(ClusterTestCase):
    @classmethod
    def get_server_settings(cls):
        conf = super().get_server_settings()
        conf.update({
            'ssl': 'on',
            'ssl_cert_file': SSL_CERT_FILE,
            'ssl_key_file': SSL_KEY_FILE,
            'ssl_ca_file': CLIENT_CA_CERT_FILE,
            'ssl_min_protocol_version': 'TLSv1.2',
            'ssl_max_protocol_version': 'TLSv1.2',
        })
        return conf

    def setUp(self):
        super().setUp()

        self.cluster.reset_hba()

        create_script = []
        create_script.append('CREATE ROLE ssl_user WITH LOGIN;')
        create_script.append('GRANT postgres TO ssl_user;')

        self._add_hba_entry()

        # Put hba changes into effect
        self.loop.run_until_complete(self.cluster.reload())

        create_script = '\n'.join(create_script).encode()
        self.loop.run_until_complete(
            self.con.simple_query(create_script, ignore_data=True)
        )

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.loop.run_until_complete(self.cluster.trust_local_connections())

        drop_script = []
        drop_script.append('DROP ROLE ssl_user;')
        drop_script = '\n'.join(drop_script).encode()
        self.loop.run_until_complete(
            self.con.simple_query(drop_script, ignore_data=True)
        )

        super().tearDown()

    def _add_hba_entry(self):
        raise NotImplementedError()


@unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
class TestSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('127.0.0.0/24'),
            database=self.dbname, user='ssl_user',
            auth_method='trust')

        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('::1/128'),
            database=self.dbname, user='ssl_user',
            auth_method='trust')

    async def test_ssl_connection_custom_context(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(SSL_CA_CERT_FILE)

        con = await self.connect(
            host='localhost',
            user='ssl_user',
            sslmode=SSLMode.require,
            ssl=ssl_context)

        try:
            await self.assertConnected(con)
            self.assertTrue(con.is_ssl)
        finally:
            con.terminate()

    async def test_ssl_connection_sslmode(self):
        async def verify_works(sslmode, *, host='localhost'):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/postgres?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                await self.assertConnected(con)
                self.assertTrue(con.is_ssl)
            finally:
                if con:
                    con.terminate()

        async def verify_fails(sslmode, *, host='localhost', exn_type):
            # XXX: uvloop artifact
            old_handler = self.loop.get_exception_handler()
            con = None
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaises(exn_type):
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host=host,
                        user='ssl_user')
                    await self.assertConnected(con)
            finally:
                if con:
                    con.terminate()
                self.loop.set_exception_handler(old_handler)

        invalid_auth_err = errors.BackendError
        await verify_fails('disable', exn_type=invalid_auth_err)
        await verify_works('allow')
        await verify_works('prefer')
        await verify_works('require')
        await verify_fails('verify-ca', exn_type=ValueError)
        await verify_fails('verify-full', exn_type=ValueError)

        with mock_dot_postgresql():
            await verify_works('require')
            await verify_works('verify-ca')
            await verify_works('verify-ca', host='127.0.0.1')
            await verify_works('verify-full')
            await verify_fails('verify-full', host='127.0.0.1',
                               exn_type=ssl.CertificateError)

        with mock_dot_postgresql(crl=True):
            await verify_fails('disable', exn_type=invalid_auth_err)
            await verify_works('allow')
            await verify_works('prefer')
            await verify_fails('require',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-ca',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-ca', host='127.0.0.1',
                               exn_type=ssl.SSLError)
            await verify_fails('verify-full',
                               exn_type=ssl.SSLError)

    async def test_ssl_connection_default_context(self):
        # XXX: uvloop artifact
        old_handler = self.loop.get_exception_handler()
        try:
            self.loop.set_exception_handler(lambda *args: None)
            with self.assertRaisesRegex(ssl.SSLError, 'verify failed'):
                await self.connect(
                    host='localhost',
                    user='ssl_user',
                    sslmode=SSLMode.verify_full,
                    ssl=ssl.create_default_context()
                )
        finally:
            self.loop.set_exception_handler(old_handler)

    async def test_tls_version(self):
        # XXX: uvloop artifact
        old_handler = self.loop.get_exception_handler()
        try:
            self.loop.set_exception_handler(lambda *args: None)
            with self.assertRaisesRegex(ssl.SSLError, 'protocol version'):
                await self.connect(
                    dsn=f'postgresql://ssl_user@localhost/{self.dbname}'
                        '?sslmode=require&ssl_min_protocol_version=TLSv1.3'
                )
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', DeprecationWarning)
                with self.assertRaises(ssl.SSLError):
                    await self.connect(
                        dsn=f'postgresql://ssl_user@localhost/{self.dbname}'
                            '?sslmode=require'
                            '&ssl_min_protocol_version=TLSv1.1'
                            '&ssl_max_protocol_version=TLSv1.1'
                    )
                with self.assertRaisesRegex(ssl.SSLError, 'no protocols'):
                    await self.connect(
                        dsn=f'postgresql://ssl_user@localhost/{self.dbname}'
                            '?sslmode=require'
                            '&ssl_min_protocol_version=TLSv1.2'
                            '&ssl_max_protocol_version=TLSv1.1'
                    )
            con = await self.connect(
                dsn=f'postgresql://ssl_user@localhost/{self.dbname}'
                    '?sslmode=require'
                    '&ssl_min_protocol_version=TLSv1.2'
                    '&ssl_max_protocol_version=TLSv1.2'
            )
            try:
                await self.assertConnected(con)
            finally:
                con.terminate()
        finally:
            self.loop.set_exception_handler(old_handler)


class TestClientSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('127.0.0.0/24'),
            database=self.dbname, user='ssl_user',
            auth_method='cert')

        self.cluster.add_hba_entry(
            type='hostssl', address=ipaddress.ip_network('::1/128'),
            database=self.dbname, user='ssl_user',
            auth_method='cert')

    async def test_ssl_connection_client_auth_fails_with_wrong_setup(self):
        ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=SSL_CA_CERT_FILE,
        )

        with self.assertRaisesRegex(
            errors.BackendError,
            "requires a valid client certificate",
        ):
            await self.connect(
                host='localhost',
                user='ssl_user',
                sslmode=SSLMode.require,
                ssl=ssl_context,
            )

    async def _test_works(self, **conn_args):
        con = await self.connect(**conn_args)

        try:
            await self.assertConnected(con)
        finally:
            con.terminate()

    async def test_ssl_connection_client_auth_custom_context(self):
        for key_file in (CLIENT_SSL_KEY_FILE, CLIENT_SSL_PROTECTED_KEY_FILE):
            ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=SSL_CA_CERT_FILE,
            )
            ssl_context.load_cert_chain(
                CLIENT_SSL_CERT_FILE,
                keyfile=key_file,
                password='secRet',
            )
            await self._test_works(
                host='localhost',
                user='ssl_user',
                sslmode=SSLMode.require,
                ssl=ssl_context,
            )

    async def test_ssl_connection_client_auth_dsn(self):
        params = {
            'sslrootcert': SSL_CA_CERT_FILE,
            'sslcert': CLIENT_SSL_CERT_FILE,
            'sslkey': CLIENT_SSL_KEY_FILE,
            'sslmode': 'verify-full',
        }
        params_str = urllib.parse.urlencode(params)
        dsn = 'postgres://ssl_user@localhost/postgres?' + params_str
        await self._test_works(dsn=dsn)

        params['sslkey'] = CLIENT_SSL_PROTECTED_KEY_FILE
        params['sslpassword'] = 'secRet'
        params_str = urllib.parse.urlencode(params)
        dsn = 'postgres://ssl_user@localhost/postgres?' + params_str
        await self._test_works(dsn=dsn)

    async def test_ssl_connection_client_auth_env(self):
        env = {
            'PGSSLROOTCERT': SSL_CA_CERT_FILE,
            'PGSSLCERT': CLIENT_SSL_CERT_FILE,
            'PGSSLKEY': CLIENT_SSL_KEY_FILE,
        }
        dsn = 'postgres://ssl_user@localhost/postgres?sslmode=verify-full'
        with unittest.mock.patch.dict('os.environ', env):
            await self._test_works(dsn=dsn)

        env['PGSSLKEY'] = CLIENT_SSL_PROTECTED_KEY_FILE
        with unittest.mock.patch.dict('os.environ', env):
            await self._test_works(dsn=dsn + '&sslpassword=secRet')

    async def test_ssl_connection_client_auth_dot_postgresql(self):
        dsn = 'postgres://ssl_user@localhost/postgres?sslmode=verify-full'
        with mock_dot_postgresql(client=True):
            await self._test_works(dsn=dsn)
        with mock_dot_postgresql(client=True, protected=True):
            await self._test_works(dsn=dsn + '&sslpassword=secRet')


class TestNoSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostnossl', address=ipaddress.ip_network('127.0.0.0/24'),
            database=self.dbname, user='ssl_user',
            auth_method='trust')

        self.cluster.add_hba_entry(
            type='hostnossl', address=ipaddress.ip_network('::1/128'),
            database=self.dbname, user='ssl_user',
            auth_method='trust')

    async def test_nossl_connection_sslmode(self):
        async def verify_works(sslmode, *, host='localhost'):
            con = None
            try:
                con = await self.connect(
                    dsn='postgresql://foo/postgres?sslmode=' + sslmode,
                    host=host,
                    user='ssl_user')
                await self.assertConnected(con)
                self.assertFalse(con.is_ssl)
            finally:
                if con:
                    con.terminate()

        async def verify_fails(sslmode, *, host='localhost'):
            # XXX: uvloop artifact
            old_handler = self.loop.get_exception_handler()
            con = None
            try:
                self.loop.set_exception_handler(lambda *args: None)
                with self.assertRaises(
                    errors.BackendError
                ) as cm:
                    con = await self.connect(
                        dsn='postgresql://foo/?sslmode=' + sslmode,
                        host=host,
                        user='ssl_user')
                    await self.assertConnected(con)
                self.assertTrue(
                    cm.exception.code_is(
                        errors.ERROR_INVALID_AUTHORIZATION_SPECIFICATION
                    )
                )
            finally:
                if con:
                    con.terminate()
                self.loop.set_exception_handler(old_handler)

        await verify_works('disable')
        await verify_works('allow')
        await verify_works('prefer')
        await verify_fails('require')
        with mock_dot_postgresql():
            await verify_fails('require')
            await verify_fails('verify-ca')
            await verify_fails('verify-full')
