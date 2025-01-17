# mypy: ignore-errors

#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    Union,
    Iterable,
    Literal,
    Sequence,
    Dict,
    List,
    NamedTuple,
    TYPE_CHECKING,
)
import typing

import asyncio
import atexit
import base64
import contextlib
import dataclasses
import functools
import heapq
import http
import http.client
import inspect
import json
import os
import pathlib
import random
import re
import secrets
import shlex
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import urllib

import edgedb

from edb.edgeql import quote as qlquote
from edb.server import args as edgedb_args
from edb.server import cluster as edgedb_cluster
from edb.server import pgcluster
from edb.server import defines as edgedb_defines
from edb.server.pgconnparams import ConnectionParams

from edb.common import assert_data_shape
from edb.common import devmode
from edb.common import debug
from edb.common import retryloop
from edb.common import secretkey

from edb import buildmeta
from edb import protocol
from edb.protocol import protocol as test_protocol
from edb.testbase import serutils

from edb.testbase import connection as tconn


if TYPE_CHECKING:
    import asyncpg
    DatabaseName = str
    SetupScript = str


def _add_test(result, test):
    # test is a tuple of the same test method that may zREPEAT
    cls = type(test[0])
    try:
        methods, repeat_methods = result[cls]
    except KeyError:
        # put zREPEAT tests in a separate list
        methods = []
        repeat_methods = []
        result[cls] = methods, repeat_methods

    methods.append(test[0])
    if len(test) > 1:
        repeat_methods.extend(test[1:])


def _merge_results(result):
    # make sure all the zREPEAT tests comes in the end
    return {k: v[0] + v[1] for k, v in result.items()}


def _get_test_cases(tests):
    result = {}

    for test in tests:
        if isinstance(test, unittest.TestSuite):
            result.update(_get_test_cases(test._tests))
        elif not getattr(test, '__unittest_skip__', False):
            _add_test(result, (test,))

    return result


def get_test_cases(tests):
    return _merge_results(_get_test_cases(tests))


bag = assert_data_shape.bag

generate_jwk = secretkey.generate_jwk
generate_tls_cert = secretkey.generate_tls_cert


class CustomSNI_HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args, server_hostname=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_hostname = server_hostname

    def connect(self):
        super(http.client.HTTPSConnection, self).connect()

        if self._tunnel_host:
            server_hostname = self._tunnel_host
        elif self.server_hostname is not None:
            server_hostname = self.server_hostname
        else:
            server_hostname = self.host

        self.sock = self._context.wrap_socket(self.sock,
                                              server_hostname=server_hostname)

    def true_close(self):
        self.close()


class StubbornHttpConnection(CustomSNI_HTTPSConnection):

    def close(self):
        # Don't actually close the connection.  This allows us to
        # test keep-alive and "Connection: close" headers.
        pass

    def true_close(self):
        http.client.HTTPConnection.close(self)


class TestCaseMeta(type(unittest.TestCase)):
    _database_names = set()

    @staticmethod
    def _iter_methods(bases, ns):
        for base in bases:
            for methname in dir(base):
                if not methname.startswith('test_'):
                    continue

                meth = getattr(base, methname)
                if not inspect.iscoroutinefunction(meth):
                    continue

                yield methname, meth

        for methname, meth in ns.items():
            if not methname.startswith('test_'):
                continue

            if not inspect.iscoroutinefunction(meth):
                continue

            yield methname, meth

    @classmethod
    def wrap(mcls, meth, is_repeat=False):
        @functools.wraps(meth)
        def wrapper(self, *args, __meth__=meth, **kwargs):
            try_no = 1

            if is_repeat and not getattr(self, 'TRANSACTION_ISOLATION', False):
                raise unittest.SkipTest()

            self.is_repeat = is_repeat
            while True:
                try:
                    # There might be unobvious serializability
                    # anomalies across the test suite, so, rather
                    # than hunting them down every time, simply
                    # retry the test.
                    self.loop.run_until_complete(
                        __meth__(self, *args, **kwargs))
                except (edgedb.TransactionSerializationError,
                        edgedb.TransactionDeadlockError):
                    if (
                        try_no == 10
                        # Only do a retry loop when we have a transaction
                        or not getattr(self, 'TRANSACTION_ISOLATION', False)
                    ):
                        raise
                    else:
                        self.loop.run_until_complete(self.xact.rollback())
                        self.loop.run_until_complete(asyncio.sleep(
                            min((2 ** try_no) * 0.1, 10)
                            + random.randrange(100) * 0.001
                        ))
                        self.xact = self.con.transaction()
                        self.loop.run_until_complete(self.xact.start())

                        try_no += 1
                else:
                    break

        return wrapper

    @classmethod
    def add_method(mcls, methname, ns, meth):
        ns[methname] = mcls.wrap(meth)

        # If EDGEDB_TEST_REPEATS is set, duplicate all the tests.
        # This is valuable because it should exercise the function
        # cache.
        if (
            os.environ.get('EDGEDB_TEST_REPEATS', None)
            and methname.startswith('test_')
        ):
            new = methname.replace('test_', 'test_zREPEAT_', 1)
            ns[new] = mcls.wrap(meth, is_repeat=True)

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns.copy()):
            if methname in ns:
                del ns[methname]
            mcls.add_method(methname, ns, meth)

        cls = super().__new__(mcls, name, bases, ns)
        if not ns.get('BASE_TEST_CLASS') and hasattr(cls, 'get_database_name'):
            dbname = cls.get_database_name()

            if name in mcls._database_names:
                raise TypeError(
                    f'{name} wants duplicate database name: {dbname}')

            mcls._database_names.add(name)

        return cls


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    is_repeat: bool = False

    @classmethod
    def setUpClass(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)

    @classmethod
    def uses_server(cls) -> bool:
        return True

    def add_fail_notes(self, **kwargs):
        if getattr(self, 'fail_notes', None) is None:
            self.fail_notes = {}
        self.fail_notes.update(kwargs)

    @contextlib.contextmanager
    def annotate(self, **kwargs):
        # Annotate the test in case the nested block of code fails.
        try:
            yield
        except Exception:
            self.add_fail_notes(**kwargs)
            raise

    @contextlib.contextmanager
    def assertRaisesRegex(self, exception, regex, msg=None, **kwargs):
        with super().assertRaisesRegex(exception, regex, msg=msg):
            try:
                yield
            except BaseException as e:
                if isinstance(e, exception):
                    for attr_name, expected_val in kwargs.items():
                        val = getattr(e, attr_name)
                        if val != expected_val:
                            raise self.failureException(
                                f'{exception.__name__} context attribute '
                                f'{attr_name!r} is {val} (expected '
                                f'{expected_val!r})') from e
                raise

    @staticmethod
    def try_until_succeeds(
        *,
        ignore: Union[Type[Exception], Tuple[Type[Exception]]] | None = None,
        ignore_regexp: str | None = None,
        delay: float=0.5,
        timeout: float=5
    ):
        """Retry a block of code a few times ignoring the specified errors.

        Example:

            async for tr in self.try_until_succeeds(
                    ignore=edgedb.AuthenticationError):
                async with tr:
                    await edgedb.connect(...)

        """
        if ignore is None and ignore_regexp is None:
            raise ValueError('Expect at least one of ignore or ignore_regexp')
        return retryloop.RetryLoop(
            backoff=retryloop.const_backoff(delay),
            timeout=timeout,
            ignore=ignore,
            ignore_regexp=ignore_regexp,
        )

    @staticmethod
    def try_until_fails(
        *,
        wait_for: Union[Type[Exception], Tuple[Type[Exception]]] | None = None,
        wait_for_regexp: str | None = None,
        delay: float=0.5,
        timeout: float=5
    ):
        """Retry a block of code a few times until the specified error happens.

        Example:

            async for tr in self.try_until_fails(
                    wait_for=edgedb.AuthenticationError):
                async with tr:
                    await edgedb.connect(...)

        """
        if wait_for is None and wait_for_regexp is None:
            raise ValueError(
                'Expect at least one of wait_for or wait_for_regexp'
            )
        return retryloop.RetryLoop(
            backoff=retryloop.const_backoff(delay),
            timeout=timeout,
            wait_for=wait_for,
            wait_for_regexp=wait_for_regexp,
        )

    def addCleanup(self, func, *args, **kwargs):
        @functools.wraps(func)
        def cleanup():
            res = func(*args, **kwargs)
            if inspect.isawaitable(res):
                self.loop.run_until_complete(res)
        super().addCleanup(cleanup)

    def __getstate__(self):
        # TestCases get pickled when run in in separate OS processes
        # via `edb test -jN`. If they reference any unpickleable objects,
        # the test engine crashes with no indication why and on what test.
        # That said, most of the TestCases' guts are not needed for the
        # test results renderer, so we only keep the essential attributes
        # here.

        outcome = self._outcome
        if outcome is not None and getattr(outcome, "errors", []):
            # We don't use `test._outcome` to render errors in
            # our renderers.
            outcome.errors = []

        return {
            '_testMethodName': self._testMethodName,
            '_outcome': outcome,
            '_testMethodDoc': self._testMethodDoc,
            '_subtest': self._subtest,
            '_cleanups': [],
            '_type_equality_funcs': self._type_equality_funcs,
            'fail_notes': getattr(self, 'fail_notes', None),
        }

    @contextlib.contextmanager
    def assertChange(
        self, measure_fn: typing.Callable[[], int | float],
        expected_change: int | float
    ):
        before = measure_fn()
        try:
            yield
        finally:
            after = measure_fn()
            change = after - before
            self.assertEqual(expected_change, change)


class RollbackException(Exception):
    pass


class RollbackChanges:
    def __init__(self, test):
        self._conn = test.con

    async def __aenter__(self):
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx.rollback()


class TestCaseWithHttpClient(TestCase):
    @classmethod
    def get_api_prefix(cls):
        return ''

    @classmethod
    @contextlib.contextmanager
    def http_con(
        cls,
        server,
        keep_alive=True,
        server_hostname=None,
        client_cert_file=None,
        client_key_file=None,
    ):
        conn_args = server.get_connect_args()
        tls_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=conn_args["tls_ca_file"],
        )
        tls_context.check_hostname = False
        if any((client_cert_file, client_key_file)):
            tls_context.load_cert_chain(client_cert_file, client_key_file)
        if keep_alive:
            ConCls = StubbornHttpConnection
        else:
            ConCls = CustomSNI_HTTPSConnection

        con = ConCls(
            conn_args["host"],
            conn_args["port"],
            server_hostname=server_hostname,
            context=tls_context,
        )
        con.connect()
        try:
            yield con
        finally:
            con.true_close()

    @classmethod
    def http_con_send_request(
        cls,
        http_con: http.client.HTTPConnection,
        params: Optional[dict[str, str]] = None,
        *,
        prefix: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        method: str = "GET",
        body: bytes = b"",
        path: str = "",
    ):
        url = f'https://{http_con.host}:{http_con.port}'
        if prefix is None:
            prefix = cls.get_api_prefix()
        if prefix:
            url = f'{url}{prefix}'
        if path:
            url = f'{url}/{path}'
        if params is not None:
            url = f'{url}?{urllib.parse.urlencode(params)}'
        if headers is None:
            headers = {}
        http_con.request(method, url, body=body, headers=headers)

    @classmethod
    def http_con_read_response(
        cls,
        http_con: http.client.HTTPConnection,
    ) -> tuple[bytes, dict[str, str], int]:
        resp = http_con.getresponse()
        resp_body = resp.read()
        resp_headers = {k.lower(): v for k, v in resp.getheaders()}
        return resp_body, resp_headers, resp.status

    @classmethod
    def http_con_request(
        cls,
        http_con: http.client.HTTPConnection,
        params: Optional[dict[str, str]] = None,
        *,
        prefix: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        method: str = "GET",
        body: bytes = b"",
        path: str = "",
    ) -> tuple[bytes, dict[str, str], int]:
        cls.http_con_send_request(
            http_con,
            params,
            prefix=prefix,
            headers=headers,
            method=method,
            body=body,
            path=path,
        )
        return cls.http_con_read_response(http_con)

    @classmethod
    def http_con_json_request(
        cls,
        http_con: http.client.HTTPConnection,
        params: Optional[dict[str, str]] = None,
        *,
        prefix: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        body: Any,
        path: str = "",
    ):
        response, headers, status = cls.http_con_request(
            http_con,
            params,
            method="POST",
            body=json.dumps(body).encode(),
            prefix=prefix,
            headers={
                "Content-Type": "application/json",
                **(headers or {}),
            },
            path=path,
        )

        if status == http.HTTPStatus.OK:
            result = json.loads(response)
        else:
            result = None

        return result, headers, status

    @classmethod
    def http_con_binary_request(
        cls,
        http_con: http.client.HTTPConnection,
        query: str,
        proto_ver=edgedb_defines.CURRENT_PROTOCOL,
        bearer_token: Optional[str] = None,
        user: str = "edgedb",
        database: str = "main",
    ):
        proto_ver_str = f"v_{proto_ver[0]}_{proto_ver[1]}"
        mime_type = f"application/x.edgedb.{proto_ver_str}.binary"
        headers = {"Content-Type": mime_type, "X-EdgeDB-User": user}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        content, headers, status = cls.http_con_request(
            http_con,
            method="POST",
            path=f"db/{database}",
            prefix="",
            body=protocol.Execute(
                annotations=[],
                allowed_capabilities=protocol.Capability.ALL,
                compilation_flags=protocol.CompilationFlag(0),
                implicit_limit=0,
                command_text=query,
                input_language=protocol.InputLanguage.EDGEQL,
                output_format=protocol.OutputFormat.JSON,
                expected_cardinality=protocol.Cardinality.AT_MOST_ONE,
                input_typedesc_id=b"\0" * 16,
                output_typedesc_id=b"\0" * 16,
                state_typedesc_id=b"\0" * 16,
                arguments=b"",
                state_data=b"",
            ).dump() + protocol.Sync().dump(),
            headers=headers,
        )
        content = memoryview(content)
        uint32_unpack = struct.Struct("!L").unpack
        msgs = []
        while content:
            mtype = content[0]
            (msize,) = uint32_unpack(content[1:5])
            msg = protocol.ServerMessage.parse(mtype, content[5: msize + 1])
            msgs.append(msg)
            content = content[msize + 1:]
        return msgs, headers, status


_default_cluster = None


async def init_cluster(
    data_dir=None,
    backend_dsn=None,
    *,
    cleanup_atexit=True,
    init_settings=None,
    security=edgedb_args.ServerSecurityMode.Strict,
    http_endpoint_security=edgedb_args.ServerEndpointSecurityMode.Optional,
    compiler_pool_mode=edgedb_args.CompilerPoolMode.Fixed,
) -> edgedb_cluster.BaseCluster:
    if data_dir is not None and backend_dsn is not None:
        raise ValueError(
            "data_dir and backend_dsn cannot be set at the same time")
    if init_settings is None:
        init_settings = {}

    log_level = 's' if not debug.flags.server else 'd'

    if backend_dsn:
        cluster = edgedb_cluster.TempClusterWithRemotePg(
            backend_dsn,
            testmode=True,
            log_level=log_level,
            data_dir_prefix='edb-test-',
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )
        destroy = True
    elif data_dir is None:
        cluster = edgedb_cluster.TempCluster(
            testmode=True,
            log_level=log_level,
            data_dir_prefix='edb-test-',
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )
        destroy = True
    else:
        cluster = edgedb_cluster.Cluster(
            testmode=True,
            data_dir=data_dir,
            log_level=log_level,
            security=security,
            http_endpoint_security=http_endpoint_security,
            compiler_pool_mode=compiler_pool_mode,
        )
        destroy = False

    pg_cluster = await cluster._get_pg_cluster()
    if await pg_cluster.get_status() == 'not-initialized':
        await cluster.init(server_settings=init_settings)

    await cluster.start(port=0)
    await cluster.set_test_config()
    await cluster.set_superuser_password('test')

    if cleanup_atexit:
        atexit.register(_shutdown_cluster, cluster, destroy=destroy)

    return cluster


def _start_cluster(
    *,
    loop: asyncio.AbstractEventLoop,
    cleanup_atexit=True,
    http_endpoint_security=None,
):
    global _default_cluster

    if _default_cluster is None:
        cluster_addr = os.environ.get('EDGEDB_TEST_CLUSTER_ADDR')
        if cluster_addr:
            conn_spec = json.loads(cluster_addr)
            _default_cluster = edgedb_cluster.RunningCluster(**conn_spec)
        else:
            # This branch is not usually used - `edb test` will call
            # init_cluster() separately and set EDGEDB_TEST_CLUSTER_ADDR
            data_dir = os.environ.get('EDGEDB_TEST_DATA_DIR')
            backend_dsn = os.environ.get('EDGEDB_TEST_BACKEND_DSN')
            _default_cluster = loop.run_until_complete(
                init_cluster(
                    data_dir=data_dir,
                    backend_dsn=backend_dsn,
                    cleanup_atexit=cleanup_atexit,
                    http_endpoint_security=http_endpoint_security,
                )
            )

    return _default_cluster


def _shutdown_cluster(cluster, *, destroy=True):
    global _default_cluster
    _default_cluster = None
    if cluster is not None:
        cluster.stop()
        if destroy:
            cluster.destroy()


def _fetch_metrics(host: str, port: int, sslctx=None) -> str:
    return _call_system_api(
        host, port, '/metrics', return_json=False, sslctx=sslctx
    )


def _fetch_server_info(host: str, port: int) -> dict[str, Any]:
    return _call_system_api(host, port, '/server-info')


def _call_system_api(
    host: str, port: int, path: str, return_json=True, sslctx=None
):
    if sslctx is None:
        con = http.client.HTTPConnection(host, port)
    else:
        con = http.client.HTTPSConnection(host, port, context=sslctx)
    con.connect()
    try:
        con.request(
            'GET',
            f'http://{host}:{port}{path}'
        )
        resp = con.getresponse()
        if resp.status != 200:
            err = resp.read().decode()
            raise AssertionError(
                f'{path} returned non 200 HTTP status: {resp.status}\n\t{err}'
            )
        rv = resp.read().decode()
        if return_json:
            rv = json.loads(rv)
        return rv
    finally:
        con.close()


def parse_metrics(metrics: str) -> dict[str, float]:
    res = {}
    for line in metrics.splitlines():
        if line.startswith('#') or ' ' not in line:
            continue
        key, _, val = line.partition(' ')
        res[key] = float(val)
    return res


def _extract_background_errors(metrics: str) -> str | None:
    non_zero = []

    for label, total in parse_metrics(metrics).items():
        if label.startswith('edgedb_server_background_errors_total'):
            if total:
                non_zero.append(
                    f'non-zero {label!r} metric: {total}'
                )

    if non_zero:
        return '\n'.join(non_zero)
    else:
        return None


async def drop_db(conn, dbname):
    await conn.execute(f'DROP BRANCH {dbname}')


class ClusterTestCase(TestCaseWithHttpClient):

    BASE_TEST_CLASS = True
    backend_dsn: Optional[str] = None

    # Some tests may want to manage transactions manually,
    # or affect non-transactional state, in which case
    # TRANSACTION_ISOLATION must be set to False
    TRANSACTION_ISOLATION = True

    # By default, tests from the same testsuite may be ran in parallel in
    # several test worker processes.  However, certain cases might exhibit
    # pathological locking behavior, or are parallel-unsafe altogether, in
    # which case PARALLELISM_GRANULARITY must be set to 'database', 'suite',
    # or 'system'.  The 'database' granularity signals that no two runners
    # may execute tests on the same database in parallel, although the tests
    # may still run on copies of the test database.  The 'suite' granularity
    # means that only one test worker is allowed to execute tests from this
    # suite.  Finally, the 'system' granularity means that the test suite
    # is not parallelizable at all and must run sequentially with respect
    # to *all other* suites with 'system' granularity.
    PARALLELISM_GRANULARITY = 'default'

    # Turns on "Gel developer" mode which allows using restricted
    # syntax like USING SQL and similar. It allows modifying standard
    # library (e.g. declaring casts).
    INTERNAL_TESTMODE = True

    # Turns off query cache recompilation on DDL
    ENABLE_RECOMPILATION = False

    # Setup and teardown commands that run per test
    PER_TEST_SETUP: Sequence[str] = ()
    PER_TEST_TEARDOWN: Sequence[str] = ()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cluster = _start_cluster(
            loop=cls.loop,
            cleanup_atexit=True,
            http_endpoint_security=(
                edgedb_args.ServerEndpointSecurityMode.Optional),
        )
        cls.has_create_database = cls.cluster.has_create_database()
        cls.has_create_role = cls.cluster.has_create_role()
        cls.is_superuser = cls.has_create_database and cls.has_create_role
        cls.backend_dsn = os.environ.get('EDGEDB_TEST_BACKEND_DSN')
        if getattr(cls, 'BACKEND_SUPERUSER', False):
            if not cls.is_superuser:
                raise unittest.SkipTest('skipped due to lack of superuser')

    @classmethod
    async def tearDownSingleDB(cls):
        await cls.con.execute("RESET SCHEMA TO initial;")

    @classmethod
    def fetch_metrics(cls) -> str:
        assert cls.cluster is not None
        conargs = cls.cluster.get_connect_args()
        host, port = conargs['host'], conargs['port']
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(conargs['tls_ca_file'])
        return _fetch_metrics(host, port, sslctx=ctx)

    @classmethod
    def get_connect_args(
        cls,
        *,
        cluster=None,
        database=edgedb_defines.EDGEDB_SUPERUSER_DB,
        user=edgedb_defines.EDGEDB_SUPERUSER,
        password=None,
        secret_key=None,
    ):
        if password is None and secret_key is None:
            password = "test"
        if cluster is None:
            cluster = cls.cluster
        conargs = cluster.get_connect_args().copy()
        conargs.update(dict(user=user,
                            password=password,
                            database=database,
                            secret_key=secret_key))
        return conargs

    @classmethod
    def make_auth_header(
        cls, user=edgedb_defines.EDGEDB_SUPERUSER, password=None
    ):
        # urllib *does* have actual support for basic auth but it is so much
        # more annoying than just doing it yourself...
        conargs = cls.get_connect_args(user=user, password=password)
        username = conargs.get('user')
        password = conargs.get('password')
        key = f'{username}:{password}'.encode('ascii')
        basic_header = f'Basic {base64.b64encode(key).decode("ascii")}'

        return basic_header

    @classmethod
    def get_parallelism_granularity(cls):
        if cls.PARALLELISM_GRANULARITY == 'default':
            if cls.TRANSACTION_ISOLATION:
                return 'default'
            else:
                return 'database'
        else:
            return cls.PARALLELISM_GRANULARITY

    @classmethod
    def uses_database_copies(cls):
        return (
            os.environ.get('EDGEDB_TEST_PARALLEL')
            and cls.get_parallelism_granularity() == 'database'
        )

    def ensure_no_background_server_errors(self):
        metrics = self.fetch_metrics()
        errors = _extract_background_errors(metrics)
        if errors:
            raise AssertionError(
                f'{self._testMethodName!r}:\n\n{errors}'
            )

    @contextlib.asynccontextmanager
    async def assertRaisesRegexTx(self, exception, regex, msg=None, **kwargs):
        """A version of assertRaisesRegex with automatic transaction recovery
        """

        with super().assertRaisesRegex(exception, regex, msg=msg, **kwargs):
            try:
                tx = self.con.transaction()
                await tx.start()
                yield
            finally:
                await tx.rollback()

    @classmethod
    @contextlib.contextmanager
    def http_con(
        cls,
        server=None,
        keep_alive=True,
        server_hostname=None,
        client_cert_file=None,
        client_key_file=None,
    ):
        if server is None:
            server = cls
        with super().http_con(
            server,
            keep_alive=keep_alive,
            server_hostname=server_hostname,
            client_cert_file=client_cert_file,
            client_key_file=client_key_file,
        ) as http_con:
            yield http_con

    @property
    def http_addr(self) -> str:
        conn_args = self.get_connect_args()
        url = f'https://{conn_args["host"]}:{conn_args["port"]}'
        prefix = self.get_api_prefix()
        if prefix:
            url = f'{url}{prefix}'
        return url

    @property
    def tls_context(self) -> ssl.SSLContext:
        conn_args = self.get_connect_args()
        tls_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=conn_args["tls_ca_file"],
        )
        tls_context.check_hostname = False
        return tls_context


def ignore_warnings(warning_message=None):
    def w(f):
        async def wf(self, *args, **kwargs):
            with self.ignore_warnings(warning_message):
                return await f(self, *args, **kwargs)

        return wf

    return w


class ConnectedTestCase(ClusterTestCase):

    BASE_TEST_CLASS = True
    NO_FACTOR = False
    WARN_FACTOR = False

    con: tconn.Connection

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls.setup_and_connect())

    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.teardown_and_disconnect())
        finally:
            super().tearDownClass()

    @contextlib.contextmanager
    def ignore_warnings(self, warning_message=None):
        old = self.con._capture_warnings
        warnings = []
        self.con._capture_warnings = warnings
        try:
            yield
        finally:
            self.con._capture_warnings = old

        if warning_message is not None:
            for warning in warnings:
                with self.assertRaisesRegex(Exception, warning_message):
                    raise warning

    @classmethod
    async def setup_and_connect(cls):
        cls.con = await cls.connect()

    @classmethod
    async def teardown_and_disconnect(cls):
        await cls.con.aclose()
        # Give event loop another iteration so that connection
        # transport has a chance to properly close.
        await asyncio.sleep(0)
        cls.con = None

    def setUp(self):
        if self.INTERNAL_TESTMODE:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET __internal_testmode := true;'))

        if not self.ENABLE_RECOMPILATION:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET auto_rebuild_query_cache := false;'
                )
            )

        if self.NO_FACTOR:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET simple_scoping := true;'))

        if self.WARN_FACTOR:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET warn_old_scoping := true;'))

        if self.TRANSACTION_ISOLATION:
            self.xact = self.con.transaction()
            self.loop.run_until_complete(self.xact.start())

        for cmd in self.PER_TEST_SETUP:
            self.loop.run_until_complete(self.con.execute(cmd))

        super().setUp()

    def tearDown(self):
        try:
            self.ensure_no_background_server_errors()

            for cmd in self.PER_TEST_TEARDOWN:
                self.loop.run_until_complete(self.con.execute(cmd))
        finally:
            try:
                if self.TRANSACTION_ISOLATION:
                    self.loop.run_until_complete(self.xact.rollback())
                    del self.xact

                if self.con.is_in_transaction():
                    self.loop.run_until_complete(
                        self.con.query('ROLLBACK'))
                    raise AssertionError(
                        'test connection is still in transaction '
                        '*after* the test')

                if not self.TRANSACTION_ISOLATION:
                    self.loop.run_until_complete(
                        self.con.execute('RESET ALIAS *;'))

            finally:
                super().tearDown()

    @classmethod
    async def connect(
        cls,
        *,
        cluster=None,
        database=edgedb_defines.EDGEDB_SUPERUSER_DB,
        user=edgedb_defines.EDGEDB_SUPERUSER,
        password=None,
        secret_key=None,
    ) -> tconn.Connection:
        conargs = cls.get_connect_args(
            cluster=cluster,
            database=database,
            user=user,
            password=password,
            secret_key=secret_key,
        )
        return await tconn.async_connect_test_client(**conargs)

    def repl(self):
        """Open interactive EdgeQL REPL right in the test.

        This is obviously only for debugging purposes.  Just add
        `self.repl()` at any point in your test.
        """

        conargs = self.get_connect_args()

        cmd = [
            'python', '-m', 'edb.cli',
            '--database', self.con.dbname,
            '--user', conargs['user'],
            '--tls-ca-file', conargs['tls_ca_file'],
        ]

        env = os.environ.copy()
        env['EDGEDB_HOST'] = conargs['host']
        env['EDGEDB_PORT'] = str(conargs['port'])
        if password := conargs.get('password'):
            env['EDGEDB_PASSWORD'] = password
        if secret_key := conargs.get('secret_key'):
            env['EDGEDB_SECRET_KEY'] = secret_key

        proc = subprocess.Popen(
            cmd, stdin=sys.stdin, stdout=sys.stdout, env=env)
        while proc.returncode is None:
            try:
                proc.wait()
            except KeyboardInterrupt:
                pass

    def _run_and_rollback(self):
        return RollbackChanges(self)

    async def _run_and_rollback_retrying(self):
        @contextlib.asynccontextmanager
        async def cm(tx):
            try:
                async with tx:
                    await tx._ensure_transaction()
                    yield tx
                    raise RollbackException
            except RollbackException:
                pass

        async for tx in self.con.retrying_transaction():
            yield cm(tx)

    def assert_data_shape(self, data, shape,
                          message=None, rel_tol=None, abs_tol=None):
        assert_data_shape.assert_data_shape(
            data, shape, self.fail,
            message=message, rel_tol=rel_tol, abs_tol=abs_tol,
        )

    async def assert_query_result(
        self,
        query,
        exp_result_json,
        exp_result_binary=...,
        *,
        always_typenames=False,
        msg=None,
        sort=None,
        implicit_limit=0,
        variables=None,
        json_only=False,
        binary_only=False,
        rel_tol=None,
        abs_tol=None,
        language: Literal["sql", "edgeql"] = "edgeql",
    ):
        fetch_args = variables if isinstance(variables, tuple) else ()
        fetch_kw = variables if isinstance(variables, dict) else {}

        if not binary_only and language != "sql":
            try:
                tx = self.con.transaction()
                await tx.start()
                try:
                    res = await self.con._fetchall_json(
                        query,
                        *fetch_args,
                        __limit__=implicit_limit,
                        **fetch_kw)
                finally:
                    await tx.rollback()

                res = json.loads(res)
                if sort is not None:
                    assert_data_shape.sort_results(res, sort)
                assert_data_shape.assert_data_shape(
                    res, exp_result_json, self.fail,
                    message=msg, rel_tol=rel_tol, abs_tol=abs_tol,
                )
            except Exception:
                self.add_fail_notes(serialization='json')
                if msg:
                    self.add_fail_notes(msg=msg)
                raise

        if json_only:
            return

        if exp_result_binary is ...:
            # The expected result is the same
            exp_result_binary = exp_result_json

        typenames = random.choice([True, False]) or always_typenames
        typeids = random.choice([True, False])

        try:
            res = await self.con._fetchall(
                query,
                *fetch_args,
                __typenames__=typenames,
                __typeids__=typeids,
                __limit__=implicit_limit,
                __language__=(
                    tconn.InputLanguage.SQL if language == "sql"
                    else tconn.InputLanguage.EDGEQL
                ),
                **fetch_kw
            )
            res = serutils.serialize(res)
            if sort is not None:
                assert_data_shape.sort_results(res, sort)
            assert_data_shape.assert_data_shape(
                res,
                exp_result_binary,
                self.fail,
                message=msg,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
            )
        except Exception:
            self.add_fail_notes(
                serialization='binary',
                __typenames__=typenames,
                __typeids__=typeids)
            if msg:
                self.add_fail_notes(msg=msg)
            raise

    async def assert_sql_query_result(
        self,
        query,
        exp_result,
        *,
        implicit_limit=0,
        msg=None,
        sort=None,
        variables=None,
        rel_tol=None,
        abs_tol=None,
        apply_access_policies=True,
    ):
        if not apply_access_policies:
            ctx = self.without_access_policies()
        else:
            ctx = contextlib.nullcontext()
        async with ctx:
            await self.assert_query_result(
                query,
                exp_result,
                implicit_limit=implicit_limit,
                msg=msg,
                sort=sort,
                variables=variables,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
                language="sql",
            )

    async def assert_index_use(self, query, *args, index_type):
        def look(obj):
            if (
                isinstance(obj, dict)
                and "IndexScan" in obj.get('plan_type', '')
            ):
                return any(
                    prop['title'] == 'index_name'
                    and index_type in prop['value']
                    for prop in obj.get('properties', [])
                )

            if isinstance(obj, dict):
                return any([look(v) for v in obj.values()])
            elif isinstance(obj, list):
                return any(look(v) for v in obj)
            else:
                return False

        plan = await self.con.query_json(f'analyze {query}', *args)
        if not look(json.loads(plan)):
            raise AssertionError(f"query did not use the {index_type!r} index")

    @classmethod
    def get_backend_sql_dsn(cls, dbname=None):
        settings = cls.con.get_settings()
        pgdsn = settings.get('pgdsn')
        if pgdsn is None:
            raise unittest.SkipTest('raw SQL test skipped: not in devmode')
        params = ConnectionParams(dsn=pgdsn.decode('utf8'))
        if dbname:
            params.update(database=dbname)
        params.clear_server_settings()
        return params.to_dsn()

    @classmethod
    async def get_backend_sql_connection(cls, dbname=None):
        """Get a raw connection to the underlying SQL server, if possible

        This is useful when we want to do things like querying the pg_catalog
        of the underlying database.
        """
        try:
            import asyncpg
        except ImportError:
            raise unittest.SkipTest(
                'SQL test skipped: asyncpg not installed')

        pgdsn = cls.get_backend_sql_dsn(dbname=dbname)
        return await asyncpg.connect(pgdsn)

    @classmethod
    @contextlib.asynccontextmanager
    async def with_backend_sql_connection(cls, dbname=None):
        con = await cls.get_backend_sql_connection(dbname=dbname)
        try:
            yield con
        finally:
            await con.close()

    @contextlib.asynccontextmanager
    async def without_access_policies(self):
        await self.con.execute(
            'CONFIGURE SESSION SET apply_access_policies := false'
        )
        raised_an_execption = False
        try:
            yield
        except BaseException:
            raised_an_execption = True
            raise
        finally:
            if not (raised_an_execption and self.con.is_in_transaction()):
                await self.con.execute(
                    'CONFIGURE SESSION RESET apply_access_policies'
                )

    @classmethod
    def get_sql_proto_dsn(cls, dbname=None):
        dbname = dbname or cls.con.dbname
        conargs = cls.get_connect_args()
        return (
            f"postgres://{conargs['user']}:{conargs['password']}@"
            f"{conargs['host']}:{conargs['port']}/{cls.con.dbname}?"
            f"sslrootcert={conargs['tls_ca_file']}"
        )


class DatabaseTestCase(ConnectedTestCase):

    SETUP: Optional[str | pathlib.Path | list[str] | list[pathlib.Path]] = None
    TEARDOWN: Optional[str] = None
    SCHEMA: Optional[str | pathlib.Path] = None
    DEFAULT_MODULE: str = 'default'
    EXTENSIONS: List[str] = []

    BASE_TEST_CLASS = True

    con: Any  # XXX: the real type?

    @classmethod
    async def setup_and_connect(cls):
        dbname = cls.get_database_name()

        cls.con = None

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP', 'run')

        # Only open an extra admin connection if necessary.
        if class_set_up == 'run':
            script = f'CREATE DATABASE {dbname};'
            admin_conn = await cls.connect()
            await admin_conn.execute(script)
            await admin_conn.aclose()

        elif class_set_up == 'inplace':
            dbname = edgedb_defines.EDGEDB_SUPERUSER_DB

        elif cls.uses_database_copies():
            admin_conn = await cls.connect()

            base_db_name, _, _ = dbname.rpartition('_')

            if cls.get_setup_script():
                await admin_conn.execute('''
                    configure session set __internal_testmode := true;
                ''')

                create_command = (
                    f'CREATE TEMPLATE BRANCH {qlquote.quote_ident(dbname)}'
                    f' FROM {qlquote.quote_ident(base_db_name)};'
                )
            else:
                create_command = (
                    f'CREATE EMPTY BRANCH {qlquote.quote_ident(dbname)}')

            # The retry here allows the test to survive a concurrent testing
            # Gel server (e.g. async with tb.start_edgedb_server()) whose
            # introspection holds a lock on the base_db here
            async for tr in cls.try_until_succeeds(
                ignore=edgedb.ExecutionError,
                timeout=30,
            ):
                async with tr:
                    await admin_conn.execute(create_command)

            await admin_conn.aclose()

        cls.con = await cls.connect(database=dbname)

        if class_set_up != 'skip':
            script = cls.get_setup_script()
            if script:
                await cls.con.execute(script)

    @staticmethod
    def get_set_up():
        return os.environ.get('EDGEDB_TEST_CASES_SET_UP', 'run')

    @classmethod
    async def teardown_and_disconnect(cls):
        script = ''

        class_set_up = cls.get_set_up()

        if cls.TEARDOWN and class_set_up != 'skip':
            script = cls.TEARDOWN.strip()

        try:
            if script:
                await cls.con.execute(script)
            if class_set_up == 'inplace':
                await cls.tearDownSingleDB()
        finally:
            await cls.con.aclose()

            if class_set_up == 'inplace':
                pass

            elif class_set_up == 'run' or cls.uses_database_copies():
                dbname = qlquote.quote_ident(cls.get_database_name())
                admin_conn = await cls.connect()
                try:
                    await drop_db(admin_conn, dbname)
                finally:
                    await admin_conn.aclose()

    @classmethod
    def get_database_name(cls):
        if not getattr(cls, 'has_create_database', True):
            return edgedb_defines.EDGEDB_SUPERUSER_DB

        if cls.__name__.startswith('TestEdgeQL'):
            dbname = cls.__name__[len('TestEdgeQL'):]
        elif cls.__name__.startswith('Test'):
            dbname = cls.__name__[len('Test'):]
        else:
            dbname = cls.__name__

        if cls.uses_database_copies():
            return f'{dbname.lower()}_{os.getpid()}'
        else:
            return dbname.lower()

    @classmethod
    def get_api_prefix(cls):
        return f'/db/{cls.get_database_name()}'

    @classmethod
    def get_setup_script(cls):
        script = ''
        has_nontrivial_script = False

        # allow the setup script to also run in test mode and no recompilation
        if cls.INTERNAL_TESTMODE:
            script += '\nCONFIGURE SESSION SET __internal_testmode := true;'
        if not cls.ENABLE_RECOMPILATION:
            script += (
                '\nCONFIGURE SESSION SET auto_rebuild_query_cache := false;'
            )

        if getattr(cls, 'BACKEND_SUPERUSER', False):
            is_superuser = getattr(cls, 'is_superuser', True)
            if not is_superuser:
                raise unittest.SkipTest('skipped due to lack of superuser')

        schema = []
        # Incude the extensions before adding schemas.
        for ext in cls.EXTENSIONS:
            schema.append(f'using extension {ext};')

        # Look at all SCHEMA entries and potentially create multiple
        # modules, but always create the test module, if not `default`.
        if cls.DEFAULT_MODULE != 'default':
            schema.append(f'\nmodule {cls.DEFAULT_MODULE} {{}}')
        for name in dir(cls):
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (
                    (m.group(1) or cls.DEFAULT_MODULE)
                    .lower().replace('_', '::')
                )

                schema_fn = getattr(cls, name)
                if schema_fn is not None:
                    with open(schema_fn, 'r') as sf:
                        module = sf.read()

                    schema.append(f'\nmodule {module_name} {{ {module} }}')

        if schema:
            has_nontrivial_script = True

            script += f'\nSTART MIGRATION'
            script += f' TO {{ {"".join(schema)} }};'
            script += f'\nPOPULATE MIGRATION;'
            script += f'\nCOMMIT MIGRATION;'

        if cls.SETUP:
            if not isinstance(cls.SETUP, (list, tuple)):
                scripts = [cls.SETUP]
            else:
                scripts = cls.SETUP

            for scr in scripts:
                has_nontrivial_script = True

                is_path = (
                    isinstance(scr, pathlib.Path)
                    or '\n' not in scr and os.path.exists(scr)
                )

                if is_path:
                    with open(scr, 'rt') as f:
                        setup_text = f.read()
                else:
                    assert isinstance(scr, str)
                    setup_text = scr

                script += '\n' + setup_text

            # If the SETUP script did a SET MODULE, make sure it is cleared
            # (since in some modes we keep using the same connection)
            script += '\nRESET MODULE;'

        # allow the setup script to also run in test mode
        if cls.INTERNAL_TESTMODE:
            script += '\nCONFIGURE SESSION SET __internal_testmode := false;'
        if not cls.ENABLE_RECOMPILATION:
            script += '\nCONFIGURE SESSION RESET auto_rebuild_query_cache;'

        return script.strip(' \n') if has_nontrivial_script else ''

    async def migrate(self, migration, *, module: str = 'default'):
        async with self.con.transaction():
            await self.con.execute(f"""
                START MIGRATION TO {{
                    module {module} {{
                        {migration}
                    }}
                }};
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)


class Error:
    def __init__(self, cls, message, shape):
        self._message = message
        self._class = cls
        self._shape = shape

    @property
    def message(self):
        return self._message

    @property
    def cls(self):
        return self._class

    @property
    def shape(self):
        return self._shape


class BaseQueryTestCase(DatabaseTestCase):

    BASE_TEST_CLASS = True


class DDLTestCase(BaseQueryTestCase):
    # DDL test cases generally need to be serialized
    # to avoid deadlocks in parallel execution.
    PARALLELISM_GRANULARITY = 'database'
    BASE_TEST_CLASS = True


class QueryTestCase(BaseQueryTestCase):

    BASE_TEST_CLASS = True


class SQLQueryTestCase(BaseQueryTestCase):

    BASE_TEST_CLASS = True

    scon: asyncpg.Connection

    @classmethod
    def setUpClass(cls):
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            raise unittest.SkipTest('SQL tests skipped: asyncpg not installed')

        super().setUpClass()
        cls.scon = cls.loop.run_until_complete(
            cls.create_sql_connection()
        )

    @classmethod
    def create_sql_connection(cls) -> asyncio.Future[asyncpg.Connection]:
        import asyncpg
        conargs = cls.get_connect_args()

        tls_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=conargs["tls_ca_file"],
        )
        tls_context.check_hostname = False

        return asyncpg.connect(
            host=conargs['host'],
            port=conargs['port'],
            user=conargs['user'],
            password=conargs['password'],
            database=cls.con.dbname,
            ssl=tls_context,
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.scon.close())
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            cls.loop.run_until_complete(asyncio.sleep(0))
            cls.scon = None
        finally:
            super().tearDownClass()

    def setUp(self):
        if self.TRANSACTION_ISOLATION:
            self.stran = self.scon.transaction()
            self.loop.run_until_complete(self.stran.start())
        super().setUp()

    def tearDown(self):
        try:
            if self.TRANSACTION_ISOLATION:
                self.loop.run_until_complete(self.stran.rollback())
            self.loop.run_until_complete(self.scon.execute('RESET ALL'))
        finally:
            super().tearDown()

    async def squery_values(self, query, *args):
        res = await self.scon.fetch(query, *args)
        return [list(r.values()) for r in res]

    def assert_shape(self, res: Any, rows: int, columns: int | List[str]):
        """
        Fail if query result does not confront the specified shape, defined in
        terms of:
        - number of rows,
        - number of columns (not checked if there are not rows)
        - column names.
        """

        self.assertEqual(len(res), rows)

        if isinstance(columns, int):
            if rows > 0:
                self.assertEqual(len(res[0]), columns)
        elif isinstance(columns, list):
            self.assertListEqual(columns, list(res[0].keys()))


class CLITestCaseMixin:

    def run_cli(self, *args, input: Optional[str] = None) -> None:
        conn_args = self.get_connect_args()
        self.run_cli_on_connection(conn_args, *args, input=input)

    @classmethod
    def run_cli_on_connection(
        cls, conn_args: Dict[str, Any], *args, input: Optional[str] = None
    ) -> None:
        cmd_args = [
            '--host', conn_args['host'],
            '--port', str(conn_args['port']),
            '--tls-ca-file', conn_args['tls_ca_file']
        ]
        if conn_args.get('user'):
            cmd_args += ['--user', conn_args['user']]
        if conn_args.get('password'):
            cmd_args += ['--password-from-stdin']
            if input is not None:
                input = f"{conn_args['password']}\n{input}"
            else:
                input = f"{conn_args['password']}\n"
        cmd_args += args
        cmd = ['edgedb'] + cmd_args
        try:
            subprocess.run(
                cmd,
                input=input.encode() if input else None,
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            output = '\n'.join(getattr(out, 'decode', out.__str__)()
                               for out in [e.output, e.stderr] if out)
            raise AssertionError(
                f'command {cmd} returned non-zero exit status {e.returncode}'
                f'\n{output}'
            ) from e


class DumpCompatTestCaseMeta(TestCaseMeta):

    def __new__(
        mcls,
        name,
        bases,
        ns,
        *,
        dump_subdir=None,
        check_method=None,
    ):
        if not name.startswith('Test'):
            return super().__new__(mcls, name, bases, ns)

        if dump_subdir is None:
            raise TypeError(
                f'{name}: missing required "dump_subdir" class argument')

        if check_method is None:
            raise TypeError(
                f'{name}: missing required "check_method" class argument')

        mod = sys.modules[ns['__module__']]
        dumps_dir = pathlib.Path(mod.__file__).parent / 'dumps' / dump_subdir

        async def check_dump_restore_compat_single_db(self, *, dumpfn):
            dbname = edgedb_defines.EDGEDB_SUPERUSER_DB
            self.run_cli('-d', dbname, 'restore', str(dumpfn))
            try:
                await check_method(self)
            finally:
                await self.tearDownSingleDB()

        async def check_dump_restore_compat(self, *, dumpfn: pathlib.Path):
            if not self.has_create_database:
                return await check_dump_restore_compat_single_db(
                    self, dumpfn=dumpfn
                )

            dbname = f"{type(self).__name__}_{dumpfn.stem}"
            qdbname = qlquote.quote_ident(dbname)
            await self.con.execute(f'CREATE DATABASE {qdbname}')
            try:
                self.run_cli('-d', dbname, 'restore', str(dumpfn))
                con2 = await self.connect(database=dbname)
            except Exception:
                await drop_db(self.con, qdbname)
                raise

            oldcon = self.__class__.con
            self.__class__.con = con2
            try:
                await check_method(self)
            finally:
                self.__class__.con = oldcon
                await con2.aclose()

                await drop_db(self.con, qdbname)

        for entry in dumps_dir.iterdir():
            if not entry.is_file() or not entry.name.endswith(".dump"):
                continue

            mcls.add_method(
                f'test_{dump_subdir}_restore_compatibility_{entry.stem}',
                ns,
                functools.partial(check_dump_restore_compat, dumpfn=entry),
            )

        return super().__new__(mcls, name, bases, ns)


class DumpCompatTestCase(
    ConnectedTestCase,
    CLITestCaseMixin,
    metaclass=DumpCompatTestCaseMeta,
):
    BASE_TEST_CLASS = True
    TRANSACTION_ISOLATION = False


class StableDumpTestCase(QueryTestCase, CLITestCaseMixin):

    BASE_TEST_CLASS = True
    STABLE_DUMP = True
    TRANSACTION_ISOLATION = False
    PARALLELISM_GRANULARITY = 'suite'

    async def check_dump_restore_single_db(self, check_method):
        with tempfile.TemporaryDirectory() as f:
            fname = os.path.join(f, 'dump')
            dbname = edgedb_defines.EDGEDB_SUPERUSER_DB
            await asyncio.to_thread(self.run_cli, '-d', dbname, 'dump', fname)
            await self.tearDownSingleDB()
            await asyncio.to_thread(
                self.run_cli, '-d', dbname, 'restore', fname
            )

        # Cycle the connection to avoid state mismatches
        await self.con.aclose()
        self.con = await self.connect(database=dbname)

        await check_method(self)

    async def check_dump_restore(
        self, check_method, include_secrets: bool=False
    ):
        if not self.has_create_database:
            return await self.check_dump_restore_single_db(check_method)

        src_dbname = self.get_database_name()
        tgt_dbname = f'{src_dbname}_restored'
        q_tgt_dbname = qlquote.quote_ident(tgt_dbname)
        with tempfile.TemporaryDirectory() as f:
            fname = os.path.join(f, 'dump')
            extra = ['--include-secrets'] if include_secrets else []
            await asyncio.to_thread(
                self.run_cli, '-d', src_dbname, 'dump', fname, *extra
            )

            await self.con.execute(f'CREATE DATABASE {q_tgt_dbname}')
            try:
                await asyncio.to_thread(
                    self.run_cli, '-d', tgt_dbname, 'restore', fname
                )
                con2 = await self.connect(database=tgt_dbname)
            except Exception:
                await drop_db(self.con, q_tgt_dbname)
                raise

        oldcon = self.con
        self.__class__.con = con2
        try:
            await check_method(self)
        finally:
            self.__class__.con = oldcon
            await con2.aclose()
            await drop_db(self.con, q_tgt_dbname)

    async def check_branching(self, include_data=False, *, check_method):
        if not self.has_create_database:
            self.skipTest("create branch is not supported by the backend")

        orig_branch = self.get_database_name()
        new_branch = f'new_{orig_branch}'
        # record the original schema
        orig_schema = await self.con.query_single('describe schema as sdl')

        # connect to a default branch so we can create a new branch
        branch_type = 'data' if include_data else 'schema'
        await self.con.execute(
            f'create {branch_type} branch {new_branch} '
            f'from {orig_branch}'
        )

        try:
            con2 = await self.connect(database=new_branch)
        except Exception:
            await drop_db(self.con, new_branch)
            raise

        oldcon = self.con
        self.__class__.con = con2
        try:
            # We cannot compare the SDL text of the new branch schema to the
            # original because the order in which it renders all the
            # components is not guaranteed. Instead we will use migrations to
            # compare the new branch schema to the original. We expect there
            # to be no difference and therefore a new migration to the
            # original schema should have the "complete" status right away.
            await self.con.execute(f'start migration to {{ {orig_schema} }}')
            mig_status = json.loads(
                await self.con.query_single_json(
                    'describe current migration as json'
                )
            )
            self.assertTrue(mig_status.get('complete'))
            await self.con.execute('abort migration')

            # run the check_method on the copied branch
            if include_data:
                await check_method(self)
            else:
                await check_method(self, include_data=include_data)
        finally:
            self.__class__.con = oldcon
            await con2.aclose()
            await drop_db(self.con, new_branch)


class StablePGDumpTestCase(BaseQueryTestCase):

    BASE_TEST_CLASS = True
    TRANSACTION_ISOLATION = False

    def run_pg_dump(self, *args, input: Optional[str] = None) -> None:
        conargs = self.get_connect_args()
        self.run_pg_dump_on_connection(conargs, *args, input=input)

    @classmethod
    def run_pg_dump_on_connection(
        cls, dsn: str, *args, input: Optional[str] = None
    ) -> None:
        cmd = [cls._pg_bin_dir / 'pg_dump', '--dbname', dsn]
        cmd += args
        try:
            subprocess.run(
                cmd,
                input=input.encode() if input else None,
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            output = '\n'.join(getattr(out, 'decode', out.__str__)()
                               for out in [e.output, e.stderr] if out)
            raise AssertionError(
                f'command {cmd} returned non-zero exit status {e.returncode}'
                f'\n{output}'
            ) from e

    @classmethod
    def setUpClass(cls):
        try:
            import asyncpg
        except ImportError:
            raise unittest.SkipTest('SQL tests skipped: asyncpg not installed')

        if cls.get_set_up() == 'inplace':
            raise unittest.SkipTest('SQL dump tests skipped in single db mode')

        super().setUpClass()
        frontend_dsn = cls.get_sql_proto_dsn()
        src_dbname = cls.con.dbname
        tgt_dbname = f'restored_{src_dbname}'
        try:
            newdsn = cls.get_backend_sql_dsn(dbname=tgt_dbname)
        except Exception:
            super().tearDownClass()
            raise

        cls._pg_bin_dir = cls.loop.run_until_complete(
            pgcluster.get_pg_bin_dir())

        cls.backend = cls.loop.run_until_complete(
            cls.get_backend_sql_connection())

        # Run pg_dump to create the dump data for an existing Gel database.
        with tempfile.NamedTemporaryFile() as f:
            cls.run_pg_dump_on_connection(frontend_dsn, '-f', f.name)

            # Skip the restore part of the test if the database
            # backend is older than our pg_dump, since it won't work.
            pg_ver_str = cls.loop.run_until_complete(
                cls.backend.fetch('select version()')
            )[0][0]
            pg_ver = buildmeta.parse_pg_version(pg_ver_str)
            bundled_ver = buildmeta.get_pg_version()
            if pg_ver.major < bundled_ver.major:
                raise unittest.SkipTest('pg_dump newer than backend')

            # Create a new Postgres database to be used for dump tests.
            db_exists = cls.loop.run_until_complete(
                cls.backend.fetch(f'''
                    SELECT oid
                    FROM pg_database
                    WHERE datname = {tgt_dbname!r}
                ''')
            )
            if list(db_exists):
                cls.loop.run_until_complete(
                    cls.backend.execute(f'drop database {tgt_dbname}')
                )
            cls.loop.run_until_complete(
                cls.backend.execute(f'create database {tgt_dbname}')
            )

            # Populate the new database using the dump
            cmd = [
                cls._pg_bin_dir / 'psql',
                '-a',
                '--dbname', newdsn,
                '-f', f.name,
                '-v', 'ON_ERROR_STOP=on',
            ]
            try:
                subprocess.run(
                    cmd,
                    input=None,
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError as e:
                output = '\n'.join(getattr(out, 'decode', out.__str__)()
                                   for out in [e.output, e.stderr] if out)
                raise AssertionError(
                    f'command {cmd} returned non-zero exit status '
                    f'{e.returncode}\n{output}'
                ) from e

        # Connect to the newly created database.
        cls.scon = cls.loop.run_until_complete(
            asyncpg.connect(newdsn))

    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.scon.close())
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            cls.loop.run_until_complete(asyncio.sleep(0))
            cls.scon = None

            tgt_dbname = f'restored_{cls.con.dbname}'
            cls.loop.run_until_complete(
                cls.backend.execute(f'drop database {tgt_dbname}')
            )
            cls.loop.run_until_complete(cls.backend.close())
            cls.loop.run_until_complete(asyncio.sleep(0))
        finally:
            super().tearDownClass()

    def assert_shape(
        self,
        sqlres: Iterable[Any],
        eqlres: Iterable[asyncpg.Record],
    ) -> None:
        """
        Compare the shape of results produced by a SQL query and an EdgeQL
        query.
        """

        assert_data_shape.assert_data_shape(
            list(sqlres),
            [dataclasses.asdict(r) for r in eqlres],
            self.fail,
            from_sql=True,
        )

    def multi_prop_subquery(self, source: str, prop: str) -> str:
        "Propduce a subquery fetching a multi prop as an array."

        return (
            f'(SELECT array_agg(target) FROM "{source}.{prop}"'
            f' WHERE source = "{source}".id) AS {prop}'
        )

    def single_link_subquery(
        self,
        source: str,
        link: str,
        target: str,
        link_props: Optional[Iterable[str]] = None
    ) -> str:
        """Propduce a subquery fetching a single link as a record.

        If no link properties are specified then the array of records will be
        made up of target types.

        If the link properties are specified then the array of records will be
        made up of link records.
        """

        if link_props:
            return (
                f'(SELECT x FROM "{target}"'
                f' JOIN "{source}.{link}" x ON x.target = "{target}".id'
                f' WHERE x.source = "{source}".id) AS _{link}'
            )

        else:
            return (
                f'(SELECT "{target}" FROM "{target}"'
                f' WHERE "{target}".id = "{source}".{link}_id) AS {link}'
            )

    def multi_link_subquery(
        self,
        source: str,
        link: str,
        target: str,
        link_props: Optional[Iterable[str]] = None
    ) -> str:
        """Propduce a subquery fetching a multi link as an array or records.

        If no link properties are specified then the array of records will be
        made up of target types.

        If the link properties are specified then the array of records will be
        made up of link records.
        """

        if link_props:
            return (
                f'(SELECT array_agg(x) FROM "{target}"'
                f' JOIN "{source}.{link}" x ON x.target = "{target}".id'
                f' WHERE x.source = "{source}".id) AS _{link}'
            )

        else:
            return (
                f'(SELECT array_agg("{target}") FROM "{target}"'
                f' JOIN "{source}.{link}" x ON x.target = "{target}".id'
                f' WHERE x.source = "{source}".id) AS {link}'
            )


def get_test_cases_setup(
    cases: Iterable[unittest.TestCase]
) -> List[Tuple[unittest.TestCase, DatabaseName, SetupScript]]:
    result: List[Tuple[unittest.TestCase, DatabaseName, SetupScript]] = []

    for case in cases:
        if not hasattr(case, 'get_setup_script'):
            continue

        try:
            setup_script = case.get_setup_script()
        except unittest.SkipTest:
            continue

        dbname = case.get_database_name()
        result.append((case, dbname, setup_script))

    return result


def test_cases_use_server(cases: Iterable[unittest.TestCase]) -> bool:
    for case in cases:
        if not hasattr(case, 'uses_server'):
            continue

        if case.uses_server():
            return True


async def setup_test_cases(
    cases,
    conn,
    num_jobs,
    try_cached_db=False,
    skip_empty_databases=False,
    verbose=False,
):
    setup = get_test_cases_setup(cases)

    stats = []
    if num_jobs == 1:
        # Special case for --jobs=1
        for _case, dbname, setup_script in setup:
            if skip_empty_databases and not setup_script:
                continue
            await _setup_database(
                dbname, setup_script, conn, stats, try_cached_db)
            if verbose:
                print(f' -> {dbname}: OK', flush=True)
    else:
        async with asyncio.TaskGroup() as g:
            # Use a semaphore to limit the concurrency of bootstrap
            # tasks to the number of jobs (bootstrap is heavy, having
            # more tasks than `--jobs` won't necessarily make
            # things faster.)
            sem = asyncio.BoundedSemaphore(num_jobs)

            async def controller(coro, dbname, *args):
                async with sem:
                    await coro(dbname, *args)
                    if verbose:
                        print(f' -> {dbname}: OK', flush=True)

            for _case, dbname, setup_script in setup:
                if skip_empty_databases and not setup_script:
                    continue

                g.create_task(controller(
                    _setup_database, dbname, setup_script, conn, stats,
                    try_cached_db))
    return stats


async def _setup_database(
        dbname, setup_script, conn_args, stats, try_cached_db):
    start_time = time.monotonic()
    default_args = {
        'user': edgedb_defines.EDGEDB_SUPERUSER,
        'password': 'test',
    }

    default_args.update(conn_args)

    try:
        admin_conn = await tconn.async_connect_test_client(
            database=edgedb_defines.EDGEDB_SUPERUSER_DB,
            **default_args)
    except Exception as ex:
        raise RuntimeError(
            f'exception during creation of {dbname!r} test DB; '
            f'could not connect to the {edgedb_defines.EDGEDB_SUPERUSER_DB} '
            f'db; {type(ex).__name__}({ex})'
        ) from ex

    try:
        await admin_conn.execute(
            f'CREATE DATABASE {qlquote.quote_ident(dbname)};'
        )
    except edgedb.DuplicateDatabaseDefinitionError:
        # Eh, that's fine
        # And, if we are trying to use a cache of the database, assume
        # the db is populated and return.
        if try_cached_db:
            elapsed = time.monotonic() - start_time
            stats.append(
                ('setup::' + dbname,
                 {'running-time': elapsed, 'cached': True}))
            return
    except Exception as ex:
        raise RuntimeError(
            f'exception during creation of {dbname!r} test DB: '
            f'{type(ex).__name__}({ex})'
        ) from ex
    finally:
        await admin_conn.aclose()

    dbconn = await tconn.async_connect_test_client(
        database=dbname, **default_args
    )
    try:
        if setup_script:
            async for tx in dbconn.retrying_transaction():
                async with tx:
                    await dbconn.execute(setup_script)
    except Exception as ex:
        raise RuntimeError(
            f'exception during initialization of {dbname!r} test DB: '
            f'{type(ex).__name__}({ex})'
        ) from ex
    finally:
        await dbconn.aclose()

    elapsed = time.monotonic() - start_time
    stats.append(
        ('setup::' + dbname, {'running-time': elapsed, 'cached': False}))


_lock_cnt = 0


def gen_lock_key():
    global _lock_cnt
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt


class _EdgeDBServerData(NamedTuple):

    host: str
    port: int
    password: str
    server_data: Any
    tls_cert_file: str
    pid: int

    def get_connect_args(self, **kwargs) -> dict[str, str | int]:
        conn_args = dict(
            user='edgedb',
            password=self.password,
            host=self.host,
            port=self.port,
            tls_ca_file=self.tls_cert_file,
        )

        conn_args.update(kwargs)
        return conn_args

    def fetch_metrics(self) -> str:
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(self.tls_cert_file)
        return _fetch_metrics(self.host, self.port, sslctx=ctx)

    def fetch_server_info(self) -> dict[str, Any]:
        return _fetch_server_info(self.host, self.port)

    def call_system_api(self, path: str):
        return _call_system_api(self.host, self.port, path)

    async def connect(self, **kwargs: Any) -> tconn.Connection:
        conn_args = self.get_connect_args(**kwargs)
        return await tconn.async_connect_test_client(**conn_args)

    async def connect_pg(self, **kwargs: Any) -> asyncpg.Connection:
        import asyncpg

        conn_args = self.get_connect_args(**kwargs)
        return await asyncpg.connect(
            host=conn_args['host'],
            port=conn_args['port'],
            user=conn_args['user'],
            password=conn_args['password'],
            ssl='require'
        )

    async def connect_test_protocol(self, **kwargs):
        conn_args = self.get_connect_args(**kwargs)
        conn = await test_protocol.new_connection(**conn_args)
        await conn.connect()
        return conn


class _EdgeDBServer:

    proc: Optional[asyncio.Process]

    def __init__(
        self,
        *,
        bind_addrs: Tuple[str, ...] = ('localhost',),
        bootstrap_command: Optional[str],
        auto_shutdown_after: Optional[int],
        adjacent_to: Optional[tconn.Connection],
        max_allowed_connections: Optional[int],
        compiler_pool_size: int,
        compiler_pool_mode: Optional[edgedb_args.CompilerPoolMode] = None,
        debug: bool,
        backend_dsn: Optional[str] = None,
        data_dir: Optional[str] = None,
        runstate_dir: Optional[str] = None,
        reset_auth: Optional[bool] = None,
        tenant_id: Optional[str] = None,
        security: edgedb_args.ServerSecurityMode,
        default_auth_method: Optional[
            edgedb_args.ServerAuthMethod | edgedb_args.ServerAuthMethods
        ] = None,
        binary_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode] = None,  # see __aexit__
        enable_backend_adaptive_ha: bool = False,
        ignore_other_tenants: bool = False,
        readiness_state_file: Optional[str] = None,
        tls_cert_file: Optional[os.PathLike] = None,
        tls_key_file: Optional[os.PathLike] = None,
        tls_cert_mode: edgedb_args.ServerTlsCertMode = (
            edgedb_args.ServerTlsCertMode.SelfSigned),
        tls_client_ca_file: Optional[os.PathLike] = None,
        jws_key_file: Optional[os.PathLike] = None,
        jwt_sub_allowlist_file: Optional[os.PathLike] = None,
        jwt_revocation_list_file: Optional[os.PathLike] = None,
        multitenant_config: Optional[str] = None,
        config_file: Optional[os.PathLike] = None,
        default_branch: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        extra_args: Optional[List[str]] = None,
        net_worker_mode: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.bind_addrs = bind_addrs
        self.auto_shutdown_after = auto_shutdown_after
        self.bootstrap_command = bootstrap_command
        self.adjacent_to = adjacent_to
        self.max_allowed_connections = max_allowed_connections
        self.compiler_pool_size = compiler_pool_size
        self.compiler_pool_mode = compiler_pool_mode
        self.debug = debug
        self.backend_dsn = backend_dsn
        self.data_dir = data_dir
        self.runstate_dir = runstate_dir
        self.reset_auth = reset_auth
        self.tenant_id = tenant_id
        self.proc = None
        self.data = None
        self.security = security
        self.default_auth_method = default_auth_method
        self.binary_endpoint_security = binary_endpoint_security
        self.http_endpoint_security = http_endpoint_security
        self.enable_backend_adaptive_ha = enable_backend_adaptive_ha
        self.ignore_other_tenants = ignore_other_tenants
        self.readiness_state_file = readiness_state_file
        self.tls_cert_file = tls_cert_file
        self.tls_key_file = tls_key_file
        self.tls_cert_mode = tls_cert_mode
        self.tls_client_ca_file = tls_client_ca_file
        self.jws_key_file = jws_key_file
        self.jwt_sub_allowlist_file = jwt_sub_allowlist_file
        self.jwt_revocation_list_file = jwt_revocation_list_file
        self.multitenant_config = multitenant_config
        self.config_file = config_file
        self.default_branch = default_branch
        self.env = env
        self.extra_args = extra_args
        self.net_worker_mode = net_worker_mode
        self.password = password

    async def wait_for_server_readiness(self, stream: asyncio.StreamReader):
        while True:
            line = await stream.readline()
            if self.debug:
                print(line.decode())
            if not line:
                raise RuntimeError("Gel server terminated")
            if line.startswith(b'READY='):
                break

        _, _, dataline = line.decode().partition('=')
        return json.loads(dataline)

    async def kill_process(self, proc: asyncio.Process):
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=60)
        except TimeoutError:
            proc.kill()

    async def _shutdown(self, exc: Optional[Exception] = None):
        if self.proc is None:
            return

        if self.proc.returncode is None:
            if self.auto_shutdown_after is not None and exc is None:
                try:
                    await asyncio.wait_for(self.proc.wait(), timeout=60 * 5)
                except TimeoutError:
                    self.proc.kill()
                    raise AssertionError(
                        'server did not auto-shutdown in 5 minutes')
            else:
                await self.kill_process(self.proc)

        # asyncio, hello?
        # Workaround SubprocessProtocol.__del__ weirdly
        # complaining that loop is closed.
        self.proc._transport.close()

        self.proc = None

    async def __aenter__(self):
        status_r, status_w = socket.socketpair()

        cmd = [
            sys.executable, '-m', 'edb.server.main',
            '--port', 'auto',
            '--testmode',
            '--emit-server-status', f'fd://{status_w.fileno()}',
            '--compiler-pool-size', str(self.compiler_pool_size),
            '--tls-cert-mode', str(self.tls_cert_mode),
            '--jose-key-mode', 'generate',
        ]

        if self.compiler_pool_mode is not None:
            cmd.extend(('--compiler-pool-mode', self.compiler_pool_mode.value))

        for addr in self.bind_addrs:
            cmd.extend(('--bind-address', addr))

        reset_auth = self.reset_auth

        cmd.extend(['--log-level', 'd' if self.debug else 's'])
        if self.max_allowed_connections is not None:
            cmd.extend([
                '--max-backend-connections', str(self.max_allowed_connections),
            ])
        if self.backend_dsn is not None:
            cmd.extend([
                '--backend-dsn', self.backend_dsn,
            ])
        elif self.adjacent_to is not None:
            settings = self.adjacent_to.get_settings()
            pgdsn = settings.get('pgdsn')
            if pgdsn is None:
                raise RuntimeError('test requires devmode to access pgdsn')
            cmd += [
                '--backend-dsn', pgdsn.decode('utf-8')
            ]
        elif self.multitenant_config:
            cmd += ['--multitenant-config-file', self.multitenant_config]
        elif self.data_dir:
            cmd += ['--data-dir', self.data_dir]
        else:
            cmd += ['--temp-dir']

            if reset_auth is None:
                reset_auth = True

        if not reset_auth:
            password = self.password
            bootstrap_command = ''
        else:
            password = secrets.token_urlsafe()
            bootstrap_command = f"""\
                ALTER ROLE admin {{
                    SET password := '{password}';
                }};
                """

        if self.bootstrap_command is not None:
            bootstrap_command += self.bootstrap_command

        if bootstrap_command:
            cmd += ['--bootstrap-command', bootstrap_command]

        if self.default_branch is not None:
            cmd += ['--default-branch', self.default_branch]

        if self.auto_shutdown_after is not None:
            cmd += ['--auto-shutdown-after', str(self.auto_shutdown_after)]

        if self.runstate_dir:
            cmd += ['--runstate-dir', self.runstate_dir]

        if self.tenant_id:
            cmd += ['--tenant-id', self.tenant_id]

        if self.security:
            cmd += ['--security', str(self.security)]

        if self.default_auth_method:
            cmd += ['--default-auth-method', str(self.default_auth_method)]

        if self.binary_endpoint_security:
            cmd += ['--binary-endpoint-security',
                    str(self.binary_endpoint_security)]

        if self.http_endpoint_security:
            cmd += ['--http-endpoint-security',
                    str(self.http_endpoint_security)]

        if self.enable_backend_adaptive_ha:
            cmd += ['--enable-backend-adaptive-ha']

        if self.ignore_other_tenants:
            cmd += ['--ignore-other-tenants']

        if self.tls_cert_file:
            cmd += ['--tls-cert-file', self.tls_cert_file]

        if self.tls_key_file:
            cmd += ['--tls-key-file', self.tls_key_file]

        if self.tls_client_ca_file:
            cmd += ['--tls-client-ca-file', str(self.tls_client_ca_file)]

        if self.readiness_state_file:
            cmd += ['--readiness-state-file', self.readiness_state_file]

        if self.jws_key_file:
            cmd += ['--jws-key-file', str(self.jws_key_file)]

        if self.jwt_sub_allowlist_file:
            cmd += ['--jwt-sub-allowlist-file', self.jwt_sub_allowlist_file]

        if self.jwt_revocation_list_file:
            cmd += ['--jwt-revocation-list-file',
                    self.jwt_revocation_list_file]

        if self.config_file:
            cmd += ['--config-file', self.config_file]

        if not self.multitenant_config:
            cmd += ['--instance-name=localtest']

        if self.net_worker_mode:
            cmd += ['--net-worker-mode', self.net_worker_mode]

        if self.extra_args:
            cmd.extend(self.extra_args)

        if self.debug:
            print(
                f'Starting Gel cluster with the following params:\n'
                f'{" ".join(shlex.quote(c) for c in cmd)}'
            )

        env = os.environ.copy()
        if self.env:
            env.update(self.env)
        env.pop("EDGEDB_SERVER_MULTITENANT_CONFIG_FILE", None)

        stat_reader, stat_writer = await asyncio.open_connection(sock=status_r)

        self.proc: asyncio.Process = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            pass_fds=(status_w.fileno(),),
        )

        status_task = asyncio.create_task(
            asyncio.wait_for(
                self.wait_for_server_readiness(stat_reader),
                timeout=240,
            ),
        )

        output = b''

        async def read_stdout():
            nonlocal output
            # Tee the log temporarily to a tempfile that exists as long as the
            # test is running. This helps debug hanging tests.
            with tempfile.NamedTemporaryFile(
                mode='w+t',
                prefix='edgedb-test-log-') as temp_file:
                if self.debug:
                    print(f"Logging to {temp_file.name}")
                while True:
                    line = await self.proc.stdout.readline()
                    if not line:
                        break
                    output += line
                    temp_file.write(line.decode(errors='ignore'))
                    if self.debug:
                        print(line.decode(errors='ignore'), end='')

        stdout_task = asyncio.create_task(read_stdout())

        try:
            _, pending = await asyncio.wait(
                [
                    status_task,
                    asyncio.create_task(self.proc.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
        except (Exception, asyncio.CancelledError):
            try:
                await self._shutdown()
            finally:
                raise
        finally:
            stat_writer.close()
            status_w.close()

        if pending:
            for task in pending:
                if not task.done():
                    task.cancel()

            await asyncio.wait(pending, timeout=10)

        if self.proc.returncode is not None:
            await stdout_task
            raise edgedb_cluster.ClusterError(output.decode(errors='ignore'))
        else:
            assert status_task.done()
            data = status_task.result()

        return _EdgeDBServerData(
            host='localhost',
            port=data['port'],
            password=password,
            server_data=data,
            tls_cert_file=data['tls_cert_file'],
            pid=self.proc.pid,
        )

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if (
                (
                    self.http_endpoint_security
                    is edgedb_args.ServerEndpointSecurityMode.Optional
                )
                and self.data is not None
                and self.auto_shutdown_after is None
            ):
                # It's a good idea to test most of the ad-hoc test clusters
                # for any errors in background tasks, as such tests usually
                # test the functionality that involves notifications and
                # other async events.
                metrics = _fetch_metrics('127.0.0.1', self.data['port'])
                errors = _extract_background_errors(metrics)
                if errors:
                    raise AssertionError(
                        'server terminated with unexpected ' +
                        'background errors\n\n' +
                        errors
                    )
        finally:
            await self._shutdown(exc)


def start_edgedb_server(
    *,
    bind_addrs: tuple[str, ...] = ('localhost',),
    auto_shutdown_after: Optional[int]=None,
    bootstrap_command: Optional[str]=None,
    max_allowed_connections: Optional[int]=10,
    compiler_pool_size: int=2,
    compiler_pool_mode: Optional[edgedb_args.CompilerPoolMode] = None,
    adjacent_to: Optional[tconn.Connection]=None,
    debug: bool=debug.flags.server,
    backend_dsn: Optional[str] = None,
    runstate_dir: Optional[str] = None,
    data_dir: Optional[str] = None,
    reset_auth: Optional[bool] = None,
    tenant_id: Optional[str] = None,
    security: edgedb_args.ServerSecurityMode = (
        edgedb_args.ServerSecurityMode.Strict),
    default_auth_method: Optional[
        edgedb_args.ServerAuthMethod | edgedb_args.ServerAuthMethods
    ] = None,
    binary_endpoint_security: Optional[
        edgedb_args.ServerEndpointSecurityMode] = None,
    http_endpoint_security: Optional[
        edgedb_args.ServerEndpointSecurityMode] = None,
    enable_backend_adaptive_ha: bool = False,
    ignore_other_tenants: bool = False,
    readiness_state_file: Optional[str] = None,
    tls_cert_file: Optional[os.PathLike] = None,
    tls_key_file: Optional[os.PathLike] = None,
    tls_cert_mode: edgedb_args.ServerTlsCertMode = (
        edgedb_args.ServerTlsCertMode.SelfSigned),
    tls_client_ca_file: Optional[os.PathLike] = None,
    jws_key_file: Optional[os.PathLike] = None,
    jwt_sub_allowlist_file: Optional[os.PathLike] = None,
    jwt_revocation_list_file: Optional[os.PathLike] = None,
    multitenant_config: Optional[str] = None,
    config_file: Optional[os.PathLike] = None,
    env: Optional[Dict[str, str]] = None,
    extra_args: Optional[List[str]] = None,
    default_branch: Optional[str] = None,
    net_worker_mode: Optional[str] = None,
    force_new: bool = False,  # True for ignoring multitenant config env
):
    if (not devmode.is_in_dev_mode() or adjacent_to) and not runstate_dir:
        if backend_dsn or adjacent_to:
            import traceback
            # We don't want to implicitly "fix the issue" for the test author
            print('WARNING: starting an Gel server with the default '
                  'runstate_dir; the test is likely to fail or hang. '
                  'Consider specifying the runstate_dir parameter.')
            print('\n'.join(traceback.format_stack(limit=5)))

    password = None
    if mt_conf := os.environ.get("EDGEDB_SERVER_MULTITENANT_CONFIG_FILE"):
        if multitenant_config is None and max_allowed_connections == 10:
            if not any(
                (
                    adjacent_to,
                    data_dir,
                    backend_dsn,
                    compiler_pool_mode,
                    default_branch,
                    force_new,
                )
            ):
                multitenant_config = mt_conf
                max_allowed_connections = None
                password = 'test'  # set in init_cluster() by test/runner.py

    params = locals()
    exclusives = [
        name
        for name in [
            "adjacent_to",
            "data_dir",
            "backend_dsn",
            "multitenant_config",
        ]
        if params[name]
    ]
    if len(exclusives) > 1:
        raise RuntimeError(
            " and ".join(exclusives) + " options are mutually exclusive"
        )

    if not runstate_dir and data_dir:
        runstate_dir = data_dir

    return _EdgeDBServer(
        bind_addrs=bind_addrs,
        auto_shutdown_after=auto_shutdown_after,
        bootstrap_command=bootstrap_command,
        max_allowed_connections=max_allowed_connections,
        adjacent_to=adjacent_to,
        compiler_pool_size=compiler_pool_size,
        compiler_pool_mode=compiler_pool_mode,
        debug=debug,
        backend_dsn=backend_dsn,
        tenant_id=tenant_id,
        data_dir=data_dir,
        runstate_dir=runstate_dir,
        reset_auth=reset_auth,
        security=security,
        default_auth_method=default_auth_method,
        binary_endpoint_security=binary_endpoint_security,
        http_endpoint_security=http_endpoint_security,
        enable_backend_adaptive_ha=enable_backend_adaptive_ha,
        ignore_other_tenants=ignore_other_tenants,
        readiness_state_file=readiness_state_file,
        tls_cert_file=tls_cert_file,
        tls_key_file=tls_key_file,
        tls_cert_mode=tls_cert_mode,
        tls_client_ca_file=tls_client_ca_file,
        jws_key_file=jws_key_file,
        jwt_sub_allowlist_file=jwt_sub_allowlist_file,
        jwt_revocation_list_file=jwt_revocation_list_file,
        multitenant_config=multitenant_config,
        config_file=config_file,
        env=env,
        extra_args=extra_args,
        default_branch=default_branch,
        net_worker_mode=net_worker_mode,
        password=password,
    )


def get_cases_by_shard(cases, selected_shard, total_shards, verbosity, stats):
    if total_shards <= 1:
        return cases

    selected_shard -= 1  # starting from 0
    new_test_est = 0.1  # default estimate if test is not found in stats
    new_setup_est = 1  # default estimate if setup is not found in stats

    # For logging
    total_tests = 0
    selected_tests = 0
    total_est = 0
    selected_est = 0

    # Priority queue of tests grouped by setup script ordered by estimated
    # running time of the groups. Order of tests within cases is preserved.
    tests_by_setup = []

    # Priority queue of individual tests ordered by estimated running time.
    tests_with_est = []

    # Prepare the source heaps
    setup_count = 0
    for case, tests in cases.items():
        # Extract zREPEAT tests and attach them to their first runs
        combined = {}
        for test in tests:
            test_name = str(test)
            orig_name = test_name.replace('test_zREPEAT', 'test')
            if orig_name == test_name:
                if test_name in combined:
                    combined[test_name] = (test, *combined[test_name])
                else:
                    combined[test_name] = (test,)
            else:
                if orig_name in combined:
                    combined[orig_name] = (*combined[orig_name], test)
                else:
                    combined[orig_name] = (test,)

        setup_script_getter = getattr(case, 'get_setup_script', None)
        if setup_script_getter and combined:
            tests_per_setup = []
            est_per_setup = setup_est = stats.get(
                'setup::' + case.get_database_name(), (new_setup_est, 0),
            )[0]
            for test_name, test in combined.items():
                total_tests += len(test)
                est = stats.get(test_name, (new_test_est, 0))[0] * len(test)
                est_per_setup += est
                tests_per_setup.append((est, test))
            heapq.heappush(
                tests_by_setup,
                (-est_per_setup, setup_count, setup_est, tests_per_setup),
            )
            setup_count += 1
            total_est += est_per_setup
        else:
            for test_name, test in combined.items():
                total_tests += len(test)
                est = stats.get(test_name, (new_test_est, 0))[0] * len(test)
                total_est += est
                heapq.heappush(tests_with_est, (-est, total_tests, test))

    target_est = total_est / total_shards  # target running time of one shard
    shards_est = [(0, shard, set()) for shard in range(total_shards)]
    cases = {}  # output
    setup_to_alloc = set(range(setup_count))  # tracks first run of each setup

    # Assign per-setup tests first
    while tests_by_setup:
        remaining_est, setup_id, setup_est, tests = heapq.heappop(
            tests_by_setup,
        )
        est_acc, current, setups = heapq.heappop(shards_est)

        # Add setup time
        if setup_id not in setups:
            setups.add(setup_id)
            est_acc += setup_est
            if current == selected_shard:
                selected_est += setup_est
            if setup_id in setup_to_alloc:
                setup_to_alloc.remove(setup_id)
            else:
                # This means one more setup for the overall test run
                target_est += setup_est / total_shards

        # Add as much tests from this group to current shard as possible
        while tests:
            est, test = tests.pop(0)
            est_acc += est  # est is a positive number
            remaining_est += est  # remaining_est is a negative number

            if current == selected_shard:
                # Add the test to the result
                _add_test(cases, test)
                selected_tests += len(test)
                selected_est += est

            if est_acc >= target_est and -remaining_est > setup_est * 2:
                # Current shard is full and the remaining tests would take more
                # time than their setup, then add the tests back to the heap so
                # that we could add them to another shard
                heapq.heappush(
                    tests_by_setup,
                    (remaining_est, setup_id, setup_est, tests),
                )
                break

        heapq.heappush(shards_est, (est_acc, current, setups))

    # Assign all non-setup tests, but leave the last shard for everything else
    setups = set()
    while tests_with_est and len(shards_est) > 1:
        est, _, test = heapq.heappop(tests_with_est)  # est is negative
        est_acc, current, setups = heapq.heappop(shards_est)
        est_acc -= est

        if current == selected_shard:
            # Add the test to the result
            _add_test(cases, test)
            selected_tests += len(test)
            selected_est -= est

        if est_acc >= target_est:
            # The current shard is full
            if current == selected_shard:
                # End early if the selected shard is full
                break
        else:
            # Only add the current shard back to the heap if it's not full
            heapq.heappush(shards_est, (est_acc, current, setups))

    else:
        # Add all the remaining tests to the first remaining shard if any
        while shards_est:
            est_acc, current, setups = heapq.heappop(shards_est)
            if current == selected_shard:
                for est, _, test in tests_with_est:
                    _add_test(cases, test)
                    selected_tests += len(test)
                    selected_est -= est
                break
            tests_with_est.clear()  # should always be empty already here

    if verbosity >= 1:
        print(f'Running {selected_tests}/{total_tests} tests for shard '
              f'#{selected_shard + 1} out of {total_shards} shards, '
              f'estimate: {int(selected_est / 60)}m {int(selected_est % 60)}s'
              f' / {int(total_est / 60)}m {int(total_est % 60)}s, '
              f'{len(setups)}/{setup_count} databases to setup.')
    return _merge_results(cases)


def find_available_port(max_value=None) -> int:
    if max_value is None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("localhost", 0))
            return sock.getsockname()[1]
    elif max_value > 1024:
        port = max_value
        while port > 1024:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.bind(("localhost", port))
                    return port
            except IOError:
                port -= 1
        raise RuntimeError("cannot find an available port")
    else:
        raise ValueError("max_value must be greater than 1024")


def _needs_factoring(weakly):
    def decorator(f):
        async def g(self, *args, **kwargs):
            if self.NO_FACTOR and not weakly:
                with self.assertRaisesRegex(Exception, ''):
                    await f(self, *args, **kwargs)
            elif self.WARN_FACTOR:
                with self.assertRaisesRegex(
                    edgedb.InvalidReferenceError, 'attempting to factor out'
                ):
                    await f(self, *args, **kwargs)

            else:
                await f(self, *args, **kwargs)

        return g
    return decorator


@contextlib.asynccontextmanager
async def temp_file_with(data: bytes):
    with tempfile.NamedTemporaryFile() as f:
        f.write(data)
        f.flush()
        yield f


needs_factoring = _needs_factoring(weakly=False)
needs_factoring_weakly = _needs_factoring(weakly=True)
