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
from typing import *

import asyncio
import atexit
import contextlib
import functools
import heapq
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
import subprocess
import sys
import tempfile
import time
import unittest

import edgedb

from edb.edgeql import quote as qlquote
from edb.server import args as edgedb_args
from edb.server import cluster as edgedb_cluster
from edb.server import defines as edgedb_defines

from edb.common import assert_data_shape
from edb.common import devmode
from edb.common import taskgroup

from edb.protocol import protocol as test_protocol
from edb.testbase import serutils

from edb.testbase import connection as tconn


if TYPE_CHECKING:
    DatabaseName = str
    SetupScript = str


def _add_test(result, test):
    cls = type(test)
    try:
        methods = result[cls]
    except KeyError:
        methods = result[cls] = []

    methods.append(test)


def get_test_cases(tests):
    result = {}

    for test in tests:
        if isinstance(test, unittest.TestSuite):
            result.update(get_test_cases(test._tests))
        elif not getattr(test, '__unittest_skip__', False):
            _add_test(result, test)

    return result


bag = assert_data_shape.bag


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
    def wrap(mcls, meth):
        @functools.wraps(meth)
        def wrapper(self, *args, __meth__=meth, **kwargs):
            try_no = 1

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
                        try_no == 6
                        # Only do a retry loop when we have a transaction
                        or not getattr(self, 'TRANSACTION_ISOLATION', False)
                    ):
                        raise
                    else:
                        self.loop.run_until_complete(self.xact.rollback())
                        self.loop.run_until_complete(asyncio.sleep(
                            (2 ** try_no) * 0.1 + random.randrange(100) * 0.001
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

    @classmethod
    def setUpClass(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)

    def add_fail_notes(self, **kwargs):
        if not hasattr(self, 'fail_notes'):
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
        ignore: Union[Type[Exception], Tuple[Type[Exception]]],
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
        return _TryFewTimes(
            delay=delay,
            timeout=timeout,
            ignore=ignore,
        )

    @staticmethod
    def try_until_fails(
        *,
        wait_for: Union[Type[Exception], Tuple[Type[Exception]]],
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
        return _TryFewTimes(
            delay=delay,
            timeout=timeout,
            wait_for=wait_for,
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
        if outcome is not None and outcome.errors:
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
        }


class _TryFewTimes:

    def __init__(
        self,
        *,
        delay: float,
        timeout: float,
        ignore: Optional[Union[Type[Exception],
                               Tuple[Type[Exception]]]] = None,
        wait_for: Optional[Union[Type[Exception],
                                 Tuple[Type[Exception]]]]=None,
    ) -> None:
        self._delay = delay
        self._timeout = timeout
        self._ignore = ignore
        self._wait_for = wait_for
        self._started_at = 0
        self._stop_request = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._stop_request:
            raise StopAsyncIteration

        if self._started_at == 0:
            # First run
            self._started_at = time.monotonic()
        else:
            # Second or greater run -- delay before yielding
            await asyncio.sleep(self._delay)

        return _TryRunner(self)


class _TryRunner:

    def __init__(self, controller: _TryFewTimes):
        self._controller = controller

    async def __aenter__(self):
        return self

    async def __aexit__(self, et, e, tb):
        elapsed = time.monotonic() - self._controller._started_at

        if self._controller._ignore is not None:
            # Mode 1: Try until we don't get errors matching `ignore`

            if et is None:
                self._controller._stop_request = True
                return

            if not isinstance(e, self._controller._ignore):
                # Propagate, it's not the error we expected.
                return

            if elapsed > self._controller._timeout:
                # Propagate -- we've run it enough times.
                return

            # Ignore the exception until next run.
            return True

        else:
            # Mode 2: Try until we fail with an error matching `wait_for`

            assert self._controller._wait_for is not None

            if et is not None:
                if isinstance(e, self._controller._wait_for):
                    # We're done, we've got what we waited for.
                    self._controller._stop_request = True
                    return True
                else:
                    # Propagate, it's not the error we expected.
                    return

            if elapsed > self._controller._timeout:
                raise AssertionError(
                    f'exception matching {self._controller._wait_for!r} '
                    f'has not happen in {self._controller._timeout} seconds')

            # Ignore the exception until next run.
            return True


_default_cluster = None


async def init_cluster(
    data_dir=None,
    backend_dsn=None,
    *,
    cleanup_atexit=True,
    init_settings=None,
    security=edgedb_args.ServerSecurityMode.Strict,
    http_endpoint_security=edgedb_args.ServerEndpointSecurityMode.Optional,
) -> edgedb_cluster.BaseCluster:
    if data_dir is not None and backend_dsn is not None:
        raise ValueError(
            "data_dir and backend_dsn cannot be set at the same time")
    if init_settings is None:
        init_settings = {}

    if backend_dsn:
        cluster = edgedb_cluster.TempClusterWithRemotePg(
            backend_dsn, testmode=True, log_level='s',
            data_dir_prefix='edb-test-',
            security=security,
            http_endpoint_security=http_endpoint_security)
        destroy = True
    elif data_dir is None:
        cluster = edgedb_cluster.TempCluster(
            testmode=True, log_level='s', data_dir_prefix='edb-test-',
            security=security,
            http_endpoint_security=http_endpoint_security)
        destroy = True
    else:
        cluster = edgedb_cluster.Cluster(
            data_dir=data_dir, log_level='s',
            security=security,
            http_endpoint_security=http_endpoint_security)
        destroy = False

    if await cluster.get_status() == 'not-initialized':
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


def _fetch_metrics(host: str, port: int) -> str:
    return _call_system_api(host, port, '/metrics', return_json=False)


def _fetch_server_info(host: str, port: int) -> dict[str, Any]:
    return _call_system_api(host, port, '/server-info')


def _call_system_api(host: str, port: int, path: str, return_json=True):
    con = http.client.HTTPConnection(host, port)
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


def _extract_background_errors(metrics: str) -> str | None:
    non_zero = []

    for line in metrics.splitlines():
        if line.startswith('edgedb_server_background_errors_total'):
            label, _, total = line.rpartition(' ')
            total = float(total)
            if total:
                non_zero.append(
                    f'non-zero {label!r} metric: {total}'
                )

    if non_zero:
        return '\n'.join(non_zero)
    else:
        return None


class ClusterTestCase(TestCase):

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

    # Turns on "EdgeDB developer" mode which allows using restricted
    # syntax like USING SQL and similar. It allows modifying standard
    # library (e.g. declaring casts).
    INTERNAL_TESTMODE = True

    SETUP_METHOD: Optional[str] = None
    TEARDOWN_METHOD: Optional[str] = None

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
        cls.backend_dsn = os.environ.get('EDGEDB_TEST_BACKEND_DSN')

    @classmethod
    async def tearDownSingleDB(cls):
        await cls.con.execute(
            'START MIGRATION TO {};\n'
            'POPULATE MIGRATION;\n'
            'COMMIT MIGRATION;'
        )
        while m := await cls.con.query_single(
            "SELECT schema::Migration { name } "
            "FILTER NOT EXISTS .<parents LIMIT 1"
        ):
            await cls.con.execute(f"DROP MIGRATION {m.name}")

    @classmethod
    def fetch_metrics(cls) -> str:
        assert cls.cluster is not None
        conargs = cls.cluster.get_connect_args()
        host, port = conargs['host'], conargs['port']
        return _fetch_metrics(host, port)

    @classmethod
    def get_connect_args(cls, *,
                         cluster=None,
                         database=edgedb_defines.EDGEDB_SUPERUSER_DB,
                         user=edgedb_defines.EDGEDB_SUPERUSER,
                         password='test'):
        if cluster is None:
            cluster = cls.cluster
        conargs = cluster.get_connect_args().copy()
        conargs.update(dict(user=user,
                            password=password,
                            database=database))
        return conargs

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

    def setUp(self):
        if self.INTERNAL_TESTMODE:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET __internal_testmode := true;'))

        if self.TRANSACTION_ISOLATION:
            self.xact = self.con.transaction()
            self.loop.run_until_complete(self.xact.start())

        if self.SETUP_METHOD:
            self.loop.run_until_complete(
                self.con.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self):
        try:
            self.ensure_no_background_server_errors()

            if self.TEARDOWN_METHOD:
                self.loop.run_until_complete(
                    self.con.execute(self.TEARDOWN_METHOD))
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

    @contextlib.asynccontextmanager
    async def assertRaisesRegexTx(self, exception, regex, msg=None, **kwargs):
        """A version of assertRaisesRegex with automatic transaction recovery
        """

        with super().assertRaisesRegex(exception, regex, msg=msg):
            try:
                tx = self.con.transaction()
                await tx.start()
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
            finally:
                await tx.rollback()


class RollbackChanges:
    def __init__(self, test):
        self._conn = test.con

    async def __aenter__(self):
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx.rollback()


class ConnectedTestCaseMixin:

    @classmethod
    async def connect(cls, *,
                      cluster=None,
                      database=edgedb_defines.EDGEDB_SUPERUSER_DB,
                      user=edgedb_defines.EDGEDB_SUPERUSER,
                      password='test'):
        conargs = cls.get_connect_args(
            cluster=cluster, database=database, user=user, password=password)
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

        proc = subprocess.Popen(
            cmd, stdin=sys.stdin, stdout=sys.stdout, env=env)
        while proc.returncode is None:
            try:
                proc.wait()
            except KeyboardInterrupt:
                pass

    def _run_and_rollback(self):
        return RollbackChanges(self)

    async def assert_query_result(self, query,
                                  exp_result_json,
                                  exp_result_binary=...,
                                  *,
                                  always_typenames=False,
                                  msg=None, sort=None, implicit_limit=0,
                                  variables=None):
        fetch_args = variables if isinstance(variables, tuple) else ()
        fetch_kw = variables if isinstance(variables, dict) else {}
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
                res, exp_result_json, self.fail, message=msg)
        except Exception:
            self.add_fail_notes(serialization='json')
            raise

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
                **fetch_kw
            )
            res = serutils.serialize(res)
            if sort is not None:
                assert_data_shape.sort_results(res, sort)
            assert_data_shape.assert_data_shape(
                res, exp_result_binary, self.fail, message=msg)
        except Exception:
            self.add_fail_notes(
                serialization='binary',
                __typenames__=typenames,
                __typeids__=typeids)
            raise


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


class ConnectedTestCase(ClusterTestCase, ConnectedTestCaseMixin):

    BASE_TEST_CLASS = True

    con: Any  # XXX: the real type?

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.con = cls.loop.run_until_complete(cls.connect())

    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.con.aclose())
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            cls.loop.run_until_complete(asyncio.sleep(0))
            cls.con = None
        finally:
            super().tearDownClass()


class DatabaseTestCase(ClusterTestCase, ConnectedTestCaseMixin):

    SETUP: Optional[Union[str, List[str]]] = None
    TEARDOWN: Optional[str] = None
    SCHEMA: Optional[Union[str, pathlib.Path]] = None
    DEFAULT_MODULE: str = 'default'

    BASE_TEST_CLASS = True

    con: Any  # XXX: the real type?

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dbname = cls.get_database_name()

        cls.admin_conn = None
        cls.con = None

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP', 'run')

        # Only open an extra admin connection if necessary.
        if class_set_up == 'run':
            script = f'CREATE DATABASE {dbname};'
            cls.admin_conn = cls.loop.run_until_complete(cls.connect())
            cls.loop.run_until_complete(cls.admin_conn.execute(script))

        elif class_set_up == 'inplace':
            dbname = edgedb_defines.EDGEDB_SUPERUSER_DB

        elif cls.uses_database_copies():
            cls.admin_conn = cls.loop.run_until_complete(cls.connect())

            orig_testmode = cls.loop.run_until_complete(
                cls.admin_conn.query(
                    'SELECT cfg::Config.__internal_testmode',
                ),
            )
            if not orig_testmode:
                orig_testmode = False
            else:
                orig_testmode = orig_testmode[0]

            # Enable testmode to unblock the template database syntax below.
            if not orig_testmode:
                cls.loop.run_until_complete(
                    cls.admin_conn.execute(
                        'CONFIGURE SESSION SET __internal_testmode := true;',
                    ),
                )

            base_db_name, _, _ = dbname.rpartition('_')

            # The retry here allows the test to survive a concurrent testing
            # EdgeDB server (e.g. async with tb.start_edgedb_server()) whose
            # introspection holds a lock on the base_db here
            async def create_db():
                async for tr in cls.try_until_succeeds(
                    ignore=edgedb.ExecutionError,
                    timeout=30,
                ):
                    async with tr:
                        await cls.admin_conn.execute(
                            f'''
                                CREATE DATABASE {qlquote.quote_ident(dbname)}
                                FROM {qlquote.quote_ident(base_db_name)}
                            ''',
                        )
            cls.loop.run_until_complete(create_db())

            if not orig_testmode:
                cls.loop.run_until_complete(
                    cls.admin_conn.execute(
                        'CONFIGURE SESSION SET __internal_testmode := false;',
                    ),
                )

        cls.con = cls.loop.run_until_complete(cls.connect(database=dbname))

        if class_set_up != 'skip':
            script = cls.get_setup_script()
            if script:
                cls.loop.run_until_complete(cls.con.execute(script))

    @classmethod
    def tearDownClass(cls):
        script = ''

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP', 'run')

        if cls.TEARDOWN and class_set_up != 'skip':
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.loop.run_until_complete(
                    cls.con.execute(script))
            if class_set_up == 'inplace':
                cls.loop.run_until_complete(cls.tearDownSingleDB())
        finally:
            try:
                cls.loop.run_until_complete(cls.con.aclose())

                if class_set_up == 'inplace':
                    pass

                elif class_set_up == 'run' or cls.uses_database_copies():
                    dbname = qlquote.quote_ident(cls.get_database_name())

                    # The retry loop below masks connection abort races.
                    # The current implementation of edgedb-python aborts
                    # connections when it gets a CancelledError.  This creates
                    # a situation in which the server might still have not
                    # realized that the connection went away, and will raise
                    # an ExecutionError on DROP DATABASE below complaining
                    # about the database still being in use.
                    #
                    # A better fix would be for edgedb-python to learn to
                    # aclose() gracefully or implement protocol-level
                    # cancellation that guarantees consensus on the server
                    # connection state.
                    async def drop_db():
                        async for tr in cls.try_until_succeeds(
                            ignore=edgedb.ExecutionError,
                            timeout=30
                        ):
                            async with tr:
                                await cls.admin_conn.execute(
                                    f'DROP DATABASE {dbname};'
                                )

                    cls.loop.run_until_complete(drop_db())

            finally:
                try:
                    if cls.admin_conn is not None:
                        cls.loop.run_until_complete(
                            cls.admin_conn.aclose())
                finally:
                    super().tearDownClass()

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
    def get_setup_script(cls):
        script = ''

        # allow the setup script to also run in test mode
        if cls.INTERNAL_TESTMODE:
            script += '\nCONFIGURE SESSION SET __internal_testmode := true;'

        # Look at all SCHEMA entries and potentially create multiple
        # modules, but always create the test module, if not `default`.
        if cls.DEFAULT_MODULE != 'default':
            schema = [f'\nmodule {cls.DEFAULT_MODULE} {{}}']
        else:
            schema = []
        for name in dir(cls):
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (
                    (m.group(1) or cls.DEFAULT_MODULE)
                    .lower().replace('__', '.')
                )

                schema_fn = getattr(cls, name)
                if schema_fn is not None:
                    with open(schema_fn, 'r') as sf:
                        module = sf.read()

                    schema.append(f'\nmodule {module_name} {{ {module} }}')

        if schema:
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
                if '\n' not in scr and os.path.exists(scr):
                    with open(scr, 'rt') as f:
                        setup = f.read()
                else:
                    setup = scr

                script += '\n' + setup

        # allow the setup script to also run in test mode
        if cls.INTERNAL_TESTMODE:
            script += '\nCONFIGURE SESSION SET __internal_testmode := false;'

        return script.strip(' \n')

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
                await self.con.execute(f'DROP DATABASE {qdbname}')
                raise

            oldcon = self.__class__.con
            self.__class__.con = con2
            try:
                await check_method(self)
            finally:
                self.__class__.con = oldcon
                await con2.aclose()
                await self.con.execute(f'DROP DATABASE {qdbname}')

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
    ISOLATED_METHODS = False
    STABLE_DUMP = True
    TRANSACTION_ISOLATION = False

    async def check_dump_restore_single_db(self, check_method):
        with tempfile.NamedTemporaryFile() as f:
            dbname = edgedb_defines.EDGEDB_SUPERUSER_DB
            self.run_cli('-d', dbname, 'dump', f.name)
            await self.tearDownSingleDB()
            self.run_cli('-d', dbname, 'restore', f.name)
        await check_method(self)

    async def check_dump_restore(self, check_method):
        if not self.has_create_database:
            return await self.check_dump_restore_single_db(check_method)

        src_dbname = self.get_database_name()
        tgt_dbname = f'{src_dbname}_restored'
        q_tgt_dbname = qlquote.quote_ident(tgt_dbname)
        with tempfile.NamedTemporaryFile() as f:
            self.run_cli('-d', src_dbname, 'dump', f.name)

            await self.con.execute(f'CREATE DATABASE {q_tgt_dbname}')
            try:
                self.run_cli('-d', tgt_dbname, 'restore', f.name)
                con2 = await self.connect(database=tgt_dbname)
            except Exception:
                await self.con.execute(f'DROP DATABASE {q_tgt_dbname}')
                raise

        oldcon = self.con
        self.__class__.con = con2
        try:
            await check_method(self)
        finally:
            self.__class__.con = oldcon
            await con2.aclose()
            await self.con.execute(f'DROP DATABASE {q_tgt_dbname}')


def get_test_cases_setup(
    cases: Iterable[unittest.TestCase]
) -> List[Tuple[unittest.TestCase, DatabaseName, SetupScript]]:
    result: List[Tuple[unittest.TestCase, DatabaseName, SetupScript]] = []

    for case in cases:
        if not hasattr(case, 'get_setup_script'):
            continue

        setup_script = case.get_setup_script()
        if not setup_script:
            continue

        dbname = case.get_database_name()
        result.append((case, dbname, setup_script))

    return result


async def setup_test_cases(cases, conn, num_jobs, verbose=False):
    setup = get_test_cases_setup(cases)

    stats = []
    if num_jobs == 1:
        # Special case for --jobs=1
        for _case, dbname, setup_script in setup:
            await _setup_database(dbname, setup_script, conn, stats)
            if verbose:
                print(f' -> {dbname}: OK', flush=True)
    else:
        async with taskgroup.TaskGroup(name='setup test cases') as g:
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
                g.create_task(controller(
                    _setup_database, dbname, setup_script, conn, stats))
    return stats


async def _setup_database(dbname, setup_script, conn_args, stats):
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
    stats.append(('setup::' + dbname, {'running-time': elapsed}))
    return dbname


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
        return _fetch_metrics(self.host, self.port)

    def fetch_server_info(self) -> dict[str, Any]:
        return _fetch_server_info(self.host, self.port)

    def call_system_api(self, path: str):
        return _call_system_api(self.host, self.port, path)

    async def connect(self, **kwargs: Any) -> tconn.Connection:
        conn_args = self.get_connect_args(**kwargs)
        return await tconn.async_connect_test_client(**conn_args)

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
        auto_shutdown: bool,
        adjacent_to: Optional[tconn.Connection],
        max_allowed_connections: Optional[int],
        compiler_pool_size: int,
        debug: bool,
        backend_dsn: Optional[str] = None,
        data_dir: Optional[str] = None,
        runstate_dir: Optional[str] = None,
        reset_auth: Optional[bool] = None,
        tenant_id: Optional[str] = None,
        security: Optional[edgedb_args.ServerSecurityMode] = None,
        default_auth_method: Optional[edgedb_args.ServerAuthMethod] = None,
        binary_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode] = None,
        http_endpoint_security: Optional[
            edgedb_args.ServerEndpointSecurityMode] = None,  # see __aexit__
        enable_backend_adaptive_ha: bool = False,
        ignore_other_tenants: bool = False,
        tls_cert_file: Optional[os.PathLike] = None,
        tls_key_file: Optional[os.PathLike] = None,
        tls_cert_mode: edgedb_args.ServerTlsCertMode = (
            edgedb_args.ServerTlsCertMode.SelfSigned),
        env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.bind_addrs = bind_addrs
        self.auto_shutdown = auto_shutdown
        self.bootstrap_command = bootstrap_command
        self.adjacent_to = adjacent_to
        self.max_allowed_connections = max_allowed_connections
        self.compiler_pool_size = compiler_pool_size
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
        self.tls_cert_file = tls_cert_file
        self.tls_key_file = tls_key_file
        self.tls_cert_mode = tls_cert_mode
        self.env = env

    async def wait_for_server_readiness(self, stream: asyncio.StreamReader):
        while True:
            line = await stream.readline()
            if self.debug:
                print(line.decode())
            if not line:
                raise RuntimeError("EdgeDB server terminated")
            if line.startswith(b'READY='):
                break

        _, _, dataline = line.decode().partition('=')
        return json.loads(dataline)

    async def kill_process(self, proc: asyncio.Process):
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=20)
        except TimeoutError:
            proc.kill()

    async def _shutdown(self, exc: Optional[Exception] = None):
        if self.proc is None:
            return

        if self.proc.returncode is None:
            if self.auto_shutdown and exc is None:
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
        ]

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
            pgaddr = settings.get('pgaddr')
            if pgaddr is None:
                raise RuntimeError('test requires devmode')
            pgaddr = json.loads(pgaddr)
            pgdsn = (
                f'postgres:///?user={pgaddr["user"]}&port={pgaddr["port"]}'
                f'&host={pgaddr["host"]}'
            )
            cmd += [
                '--backend-dsn', pgdsn
            ]
        elif self.data_dir:
            cmd += ['--data-dir', self.data_dir]
        else:
            cmd += ['--temp-dir']

            if reset_auth is None:
                reset_auth = True

        if not reset_auth:
            password = None
            bootstrap_command = ''
        else:
            password = secrets.token_urlsafe()
            bootstrap_command = f"""\
                ALTER ROLE edgedb {{
                    SET password := '{password}';
                }};
                """

        if self.bootstrap_command is not None:
            bootstrap_command += self.bootstrap_command

        if bootstrap_command:
            cmd += ['--bootstrap-command', bootstrap_command]

        if self.auto_shutdown:
            cmd += ['--auto-shutdown-after', '0']

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

        if self.debug:
            print(
                f'Starting EdgeDB cluster with the following params:\n'
                f'{" ".join(shlex.quote(c) for c in cmd)}'
            )

        env = os.environ.copy()
        if self.env:
            env.update(self.env)

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
            output = (await self.proc.stdout.read()).decode().strip()
            raise edgedb_cluster.ClusterError(output)
        else:
            assert status_task.done()
            data = status_task.result()

        return _EdgeDBServerData(
            host='127.0.0.1',
            port=data['port'],
            password=password,
            server_data=data,
            tls_cert_file=data['tls_cert_file'],
        )

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if (
                (
                    self.http_endpoint_security
                    is edgedb_args.ServerEndpointSecurityMode.Optional
                )
                and
                self.data is not None
                and not self.auto_shutdown
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
    auto_shutdown: bool=False,
    bootstrap_command: Optional[str]=None,
    max_allowed_connections: Optional[int]=10,
    compiler_pool_size: int=2,
    adjacent_to: Optional[tconn.Connection]=None,
    debug: bool=False,
    backend_dsn: Optional[str] = None,
    runstate_dir: Optional[str] = None,
    data_dir: Optional[str] = None,
    reset_auth: Optional[bool] = None,
    tenant_id: Optional[str] = None,
    security: Optional[edgedb_args.ServerSecurityMode] = None,
    default_auth_method: Optional[edgedb_args.ServerAuthMethod] = None,
    binary_endpoint_security: Optional[
        edgedb_args.ServerEndpointSecurityMode] = None,
    http_endpoint_security: Optional[
        edgedb_args.ServerEndpointSecurityMode] = None,
    enable_backend_adaptive_ha: bool = False,
    ignore_other_tenants: bool = False,
    tls_cert_file: Optional[os.PathLike] = None,
    tls_key_file: Optional[os.PathLike] = None,
    tls_cert_mode: edgedb_args.ServerTlsCertMode = (
        edgedb_args.ServerTlsCertMode.SelfSigned),
    env: Optional[Dict[str, str]] = None,
):
    if not devmode.is_in_dev_mode() and not runstate_dir:
        if backend_dsn or adjacent_to:
            # We don't want to implicitly "fix the issue" for the test author
            print('WARNING: starting an EdgeDB server with the default '
                  'runstate_dir; the test is likely to fail or hang. '
                  'Consider specifying the runstate_dir parameter.')

    if adjacent_to and data_dir:
        raise RuntimeError(
            'adjacent_to and data_dir options are mutually exclusive')
    if backend_dsn and data_dir:
        raise RuntimeError(
            'backend_dsn and data_dir options are mutually exclusive')
    if backend_dsn and adjacent_to:
        raise RuntimeError(
            'backend_dsn and adjacent_to options are mutually exclusive')

    if not runstate_dir and data_dir:
        runstate_dir = data_dir

    return _EdgeDBServer(
        bind_addrs=bind_addrs,
        auto_shutdown=auto_shutdown,
        bootstrap_command=bootstrap_command,
        max_allowed_connections=max_allowed_connections,
        adjacent_to=adjacent_to,
        compiler_pool_size=compiler_pool_size,
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
        tls_cert_file=tls_cert_file,
        tls_key_file=tls_key_file,
        tls_cert_mode=tls_cert_mode,
        env=env,
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
        setup_script = getattr(case, 'get_setup_script', lambda: None)()
        if setup_script and tests:
            tests_per_setup = []
            est_per_setup = setup_est = stats.get(
                'setup::' + case.get_database_name(), (new_setup_est, 0),
            )[0]
            for test in tests:
                total_tests += 1
                est = stats.get(str(test), (new_test_est, 0))[0]
                est_per_setup += est
                tests_per_setup.append((est, test))
            heapq.heappush(
                tests_by_setup,
                (-est_per_setup, setup_count, setup_est, tests_per_setup),
            )
            setup_count += 1
            total_est += est_per_setup
        else:
            for test in tests:
                total_tests += 1
                est = stats.get(str(test), (new_test_est, 0))[0]
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
                selected_tests += 1
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
            selected_tests += 1
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
                    selected_tests += 1
                    selected_est -= est
                break
            tests_with_est.clear()  # should always be empty already here

    if verbosity >= 1:
        print(f'Running {selected_tests}/{total_tests} tests for shard '
              f'#{selected_shard + 1} out of {total_shards} shards, '
              f'estimate: {int(selected_est / 60)}m {int(selected_est % 60)}s'
              f' / {int(total_est / 60)}m {int(total_est % 60)}s, '
              f'{len(setups)}/{setup_count} databases to setup.')
    return cases


def find_available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]
