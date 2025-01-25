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
from typing import Optional, Unpack

import asyncio
import contextlib
import ipaddress
import os
import pathlib
import pickle
import platform
import shutil
import socket
import ssl
import stat
import sys
import tempfile
import unittest
import unittest.mock
import urllib.parse

import click
from click.testing import CliRunner

from edb.pgsql import params as pg_params
from edb.server import args as edb_args
from edb.server import bootstrap
from edb.server import pgcluster
from edb.server import pgconnparams
from edb.server.pgconnparams import SSLMode
from edb.server import pgcon
from edb.server.pgcon import errors
from edb.testbase import server as tb


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
        cluster.update_connection_params(
            user='postgres',
        )
        await cluster.init(**_get_initdb_options({}))
        await cluster.trust_local_connections()
        port = tb.find_available_port()
        await cluster.start(
            port=port,
            server_settings=cls.get_server_settings(),
            wait=120,
        )
        result = CliRunner().invoke(get_default_args, [])
        arg_input = pickle.loads(result.stdout_bytes)
        arg_input["data_dir"] = pathlib.Path(cluster.get_data_dir())
        arg_input["multitenant_config_file"] = ""
        arg_input["tls_cert_mode"] = "generate_self_signed"
        arg_input["jose_key_mode"] = "generate"
        cls.dbname = cluster.get_db_name('main')  # XXX
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
    async def connect(cls, **kwargs: Unpack[pgconnparams.CreateParamsKwargs]):
        import inspect
        assert cls.cluster is not None
        source_description = ("ClusterTestCase: "
                              f"{inspect.currentframe().f_back.f_code.co_name}")  # type: ignore
        kwargs['database'] = cls.dbname
        return await cls.cluster.connect(
            source_description=source_description,
            **kwargs
        )

    def setUp(self):
        super().setUp()

        self.con = self.loop.run_until_complete(self.connect())

    def tearDown(self):
        try:
            if self.con:
                self.con.terminate()
            self.con = None
        finally:
            super().tearDown()

    async def assertConnected(self, con):
        self.assertEqual(await con.sql_fetch_val(b"SELECT 'OK'"), b'OK')


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
        self.loop.run_until_complete(self.con.sql_execute(create_script))

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
        self.loop.run_until_complete(self.con.sql_execute(drop_script))

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
        conn = await self.connect(
            user='password_user',
            password='correctpassword')
        conn.terminate()

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
        await self.con.sql_execute(
            b"SET password_encryption = 'scram-sha-256';",
        )
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
            await self.con.sql_execute(
                alter_password.format(password).encode(),
            )
            # test to see that passwords are properly SASL prepped
            conn = await self.connect(
                user='scram_sha_256_user', password=password)
            conn.terminate()

        alter_password = \
            b"ALTER ROLE scram_sha_256_user PASSWORD 'correctpassword';"
        await self.con.sql_execute(alter_password)
        await self.con.sql_execute(b"SET password_encryption = 'md5';")

    async def test_auth_unsupported(self):
        pass


class TestConnectParams(tb.TestCase):

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

            conn_params = pgconnparams.ConnectionParams(dsn=dsn)
            conn_params = conn_params.resolve()

            to_dict = conn_params.__dict__
            host = to_dict.pop('host', None).split(',')
            port = map(int, to_dict.pop('port', None).split(','))
            to_dict.pop('sslmode', None)
            result = (list(zip(host, port)), to_dict)

        if expected is not None:
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

    def test_connect_pgpass_badness_mode(self):
        # Verify that .pgpass permissions are checked
        with tempfile.NamedTemporaryFile('w+t') as passfile:
            os.chmod(passfile.name,
                     stat.S_IWUSR | stat.S_IRUSR | stat.S_IWGRP | stat.S_IRGRP)

            with self.assertWarnsRegex(
                    UserWarning,
                    'Password file .* has group or world access'):
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
                    'Password file .* is not a plain file'):
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
        with self.assertWarnsRegex(
            UserWarning,
            'Password file .* does not exist',
        ):
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
            with self.assertWarnsRegex(
                UserWarning,
                'Password file .* is not accessible'):
                # inaccessible passfile is OK
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
                    with self.assertWarnsRegex(
                        UserWarning,
                        'Password file .* is not accessible'):
                        # inaccessible passfile is OK
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

    async def test_connection_connect_timeout(self):
        server = socket.socket()
        gc = []
        try:
            server.bind(('localhost', 0))
            if platform.system() != "Darwin":
                # The backlog on macOS is different from Linux
                server.listen(0)
            host, port = server.getsockname()
            conn_spec = pgconnparams.ConnectionParams(
                hosts=[(host, port)],
                user='foo',
                connect_timeout=2,
            )

            async def placeholder():
                async with asyncio.timeout(2):
                    _, w = await asyncio.open_connection(host, port)
                gc.append(w)

            # Fill up the TCP server backlog so that our future pgcon.connect
            # could time out reliably
            i = 0
            while i < 8:
                i += 1
                try:
                    async with asyncio.TaskGroup() as tg:
                        for _ in range(4):
                            tg.create_task(placeholder())
                except* TimeoutError:
                    i = 10
            if i < 10:
                self.fail("Couldn't fill TCP server backlog within 32 tries")

            with self.assertRaises(errors.BackendConnectionError):
                async with asyncio.timeout(4):  # failsafe
                    await pgcon.pg_connect(
                        conn_spec,
                        source_description="test_connection_connect_timeout",
                        backend_params=pg_params.get_default_runtime_params(),
                    )

        finally:
            server.close()
            if gc:
                for writer in gc:
                    writer.close()
                await asyncio.wait(
                    [asyncio.create_task(w.wait_closed()) for w in gc]
                )


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
            await self.con.sql_fetch_val(b'SELECT 1')

        with check():
            await self.con.sql_fetch_col(b'SELECT 1')

        with check():
            await self.con.sql_fetch(b'SELECT 1')

        with check():
            await self.con.sql_execute(b'SELECT 1')

        with check():
            await self.con.parse_execute(query=None, bind_data=None)

        with check():
            await self.con.dump(None, None, None)

        with check():
            await self.con.restore(None, b'', {})

    async def test_connection_ssl_to_no_ssl_server(self):
        with self.assertRaisesRegex(ConnectionError, 'rejected SSL'):
            await self.connect(
                host='localhost',
                sslmode=SSLMode.require)

    async def test_connection_sslmode_no_ssl_server(self):
        async def verify_works(sslmode):
            con = None
            try:
                con = await self.connect(
                    sslmode=SSLMode.parse(sslmode),
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
                        sslmode=SSLMode.parse(sslmode),
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
            self.con.sql_execute(create_script)
        )

    def tearDown(self):
        # Reset cluster's pg_hba.conf since we've meddled with it
        self.loop.run_until_complete(self.cluster.trust_local_connections())

        drop_script = []
        drop_script.append('DROP ROLE ssl_user;')
        drop_script = '\n'.join(drop_script).encode()
        self.loop.run_until_complete(self.con.sql_execute(drop_script))

        super().tearDown()

    def _add_hba_entry(self):
        raise NotImplementedError()


@unittest.skipIf(os.environ.get('PGHOST'), 'unmanaged cluster')
class TestSSLConnection(BaseTestSSLConnection):
    def _add_hba_entry(self):
        self.cluster.add_hba_entry(
            type='hostssl', address="all",
            database=self.dbname, user='ssl_user',
            auth_method='trust')

    async def test_ssl_connection_custom_context(self):
        con = await self.connect(
            host='localhost',
            user='ssl_user',
            sslmode=SSLMode.require,
            sslrootcert=SSL_CA_CERT_FILE)

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
                    sslmode=SSLMode.parse(sslmode),
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
                with self.assertRaises(exn_type, msg=f"{sslmode} {host}"):
                    con = await self.connect(
                        sslmode=SSLMode.parse(sslmode),
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
                    # This won't validate
                    sslrootcert=CLIENT_CA_CERT_FILE
                )
        finally:
            self.loop.set_exception_handler(old_handler)

    async def test_tls_version_bad(self):
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

    async def test_tls_version_ok(self):
        con = await self.connect(
            dsn=f'postgresql://ssl_user@localhost/{self.dbname}'
                '?sslmode=require'
        )
        try:
            await self.assertConnected(con)
        finally:
            con.terminate()


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
        with self.assertRaisesRegex(
            errors.BackendError,
            "requires a valid client certificate",
        ):
            await self.connect(
                host='localhost',
                user='ssl_user',
                sslmode=SSLMode.require,
                sslrootcert=SSL_CA_CERT_FILE,
            )

    async def _test_works(self, **conn_args):
        con = await self.connect(**conn_args)

        try:
            await self.assertConnected(con)
        finally:
            con.terminate()

    async def test_ssl_connection_client_auth_custom_context(self):
        for key_file in (CLIENT_SSL_KEY_FILE, CLIENT_SSL_PROTECTED_KEY_FILE):
            await self._test_works(
                host='localhost',
                user='ssl_user',
                sslmode=SSLMode.require,
                sslcert=CLIENT_SSL_CERT_FILE,
                sslrootcert=SSL_CA_CERT_FILE,
                sslpassword='secret1234',
                sslkey=key_file
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
        params['sslpassword'] = 'secret1234'
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
            await self._test_works(dsn=dsn + '&sslpassword=secret1234')

    async def test_ssl_connection_client_auth_dot_postgresql(self):
        dsn = 'postgres://ssl_user@localhost/postgres?sslmode=verify-full'
        with mock_dot_postgresql(client=True):
            await self._test_works(dsn=dsn)
        with mock_dot_postgresql(client=True, protected=True):
            await self._test_works(dsn=dsn + '&sslpassword=secret1234')


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
                    sslmode=SSLMode.parse(sslmode),
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
                        sslmode=SSLMode.parse(sslmode),
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

        # We no longer retry without SSL if SSL is presented but
        # fails to authenticate.
        await verify_works('disable')
        await verify_fails('allow')
        await verify_fails('prefer')
        await verify_fails('require')
        with mock_dot_postgresql():
            await verify_fails('require')
            await verify_fails('verify-ca')
            await verify_fails('verify-full')
