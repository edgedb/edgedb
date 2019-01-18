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


import asyncio
import atexit
import collections
import contextlib
import functools
import inspect
import os
import pprint
import re
import textwrap
import unittest

import edgedb

from edb.server import cluster as edgedb_cluster
from edb.server import defines as edgedb_defines

from edb.common import taskgroup


def get_test_cases(tests):
    result = collections.OrderedDict()

    for test in tests:
        if isinstance(test, unittest.TestSuite):
            result.update(get_test_cases(test._tests))
        else:
            cls = type(test)
            try:
                methods = result[cls]
            except KeyError:
                methods = result[cls] = []

            methods.append(test)

    return result


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
            self.loop.run_until_complete(__meth__(self, *args, **kwargs))

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


_default_cluster = None


def _init_cluster(data_dir=None, *, pg_cluster=None,
                  cleanup_atexit=True, init_settings={}):
    if (not os.environ.get('EDGEDB_DEBUG_SERVER') and
            not os.environ.get('EDGEDB_LOG_LEVEL')):
        _env = {'EDGEDB_LOG_LEVEL': 'silent'}
    else:
        _env = {}

    if data_dir is None:
        cluster = edgedb_cluster.TempCluster(env=_env)
        destroy = True
    else:
        cluster = edgedb_cluster.Cluster(
            data_dir=data_dir, postgres_cluster=pg_cluster, env=_env)
        destroy = False

    if cluster.get_status() == 'not-initialized':
        cluster.init(server_settings=init_settings)

    cluster.start(port='dynamic', timezone='UTC')

    if cleanup_atexit:
        atexit.register(_shutdown_cluster, cluster, destroy=destroy)

    return cluster


def _set_default_cluster(cluster):
    global _default_cluster
    _default_cluster = cluster


def _start_cluster(*, cleanup_atexit=True):
    global _default_cluster

    if _default_cluster is None:
        data_dir = os.environ.get('EDGEDB_TEST_DATA_DIR')
        _default_cluster = _init_cluster(
            data_dir=data_dir, cleanup_atexit=cleanup_atexit)

    return _default_cluster


def _shutdown_cluster(cluster, *, destroy=True):
    cluster.stop()
    if destroy:
        cluster.destroy()


class ClusterTestCase(TestCase):

    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cluster = _start_cluster(cleanup_atexit=True)


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
    def connect(cls, loop, cluster, database=None):
        conargs = cluster.get_connect_args().copy()
        conargs.update(dict(user='edgedb', database=database))
        return loop.run_until_complete(edgedb.connect(**conargs))

    def _run_and_rollback(self):
        return RollbackChanges(self)


class ConnectedTestCase(ClusterTestCase, ConnectedTestCaseMixin):

    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.con = cls.connect(cls.loop, cls.cluster)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.loop.run_until_complete(cls.con.close())
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            cls.loop.run_until_complete(asyncio.sleep(0))
            cls.con = None
        finally:
            super().tearDownClass()


class DatabaseTestCase(ClusterTestCase, ConnectedTestCaseMixin):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None

    SETUP_METHOD = None
    TEARDOWN_METHOD = None

    # Some tests may want to manage transactions manually,
    # in which case ISOLATED_METHODS will be False.
    ISOLATED_METHODS = True
    # Turns on "EdgeDB developer" mode which allows using restricted
    # syntax like FROM SQL and similar. It allows modifying standard
    # library (e.g. declaring casts).
    INTERNAL_TESTMODE = True

    BASE_TEST_CLASS = True

    def setUp(self):
        if self.INTERNAL_TESTMODE:
            self.loop.run_until_complete(
                self.con.execute('SET CONFIG __internal_testmode := true;'))

        if self.ISOLATED_METHODS:
            self.xact = self.con.transaction()
            self.loop.run_until_complete(self.xact.start())

        if self.SETUP_METHOD:
            self.loop.run_until_complete(
                self.con.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self):
        try:
            if self.TEARDOWN_METHOD:
                self.loop.run_until_complete(
                    self.con.execute(self.TEARDOWN_METHOD))
        finally:
            try:
                if self.ISOLATED_METHODS:
                    self.loop.run_until_complete(self.xact.rollback())
                    del self.xact
                else:
                    self.loop.run_until_complete(
                        self.con.execute('RESET ALIAS *;'))
            finally:
                super().tearDown()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dbname = cls.get_database_name()

        cls.admin_conn = None
        cls.con = None

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP')

        # Only open an extra admin connection if necessary.
        if not class_set_up:
            script = f'CREATE DATABASE {dbname};'
            cls.admin_conn = cls.connect(cls.loop, cls.cluster)
            cls.loop.run_until_complete(cls.admin_conn.execute(script))

        cls.con = cls.connect(cls.loop, cls.cluster, database=dbname)

        if not class_set_up:
            script = cls.get_setup_script()
            if script:
                cls.loop.run_until_complete(cls.con.execute(script))

    @classmethod
    def get_database_name(cls):
        if cls.__name__.startswith('TestEdgeQL'):
            dbname = cls.__name__[len('TestEdgeQL'):]
        elif cls.__name__.startswith('Test'):
            dbname = cls.__name__[len('Test'):]
        else:
            dbname = cls.__name__

        return dbname.lower()

    @classmethod
    def get_setup_script(cls):
        # Always create the test module.
        script = 'CREATE MODULE test;'

        # look at all SCHEMA entries and potentially create multiple modules
        #
        for name, val in cls.__dict__.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (m.group(1) or 'test').lower().replace(
                    '__', '.')

                with open(val, 'r') as sf:
                    schema = sf.read()

                if (module_name != 'test' and
                        module_name != edgedb_defines.DEFAULT_MODULE_ALIAS):
                    script += f'\nCREATE MODULE {module_name};'

                script += f'\nCREATE MIGRATION {module_name}::d1'
                script += f' TO eschema $${schema}$$;'
                script += f'\nCOMMIT MIGRATION {module_name}::d1;'

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

        return script.strip(' \n')

    @classmethod
    def tearDownClass(cls):
        script = ''

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP')

        if cls.TEARDOWN and not class_set_up:
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.loop.run_until_complete(
                    cls.con.execute(script))
        finally:
            try:
                cls.loop.run_until_complete(cls.con.close())

                if not class_set_up:
                    dbname = cls.get_database_name()
                    script = f'DROP DATABASE {dbname};'

                    cls.loop.run_until_complete(
                        cls.admin_conn.execute(script))

            finally:
                if cls.admin_conn is not None:
                    cls.loop.run_until_complete(
                        cls.admin_conn.close())


class nullable:
    def __init__(self, value):
        self.value = value


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

    async def query(self, query):
        query = textwrap.dedent(query)
        return await self.con._legacy_execute(query)

    async def graphql_query(self, query):
        query = textwrap.dedent(query)
        return await self.con._legacy_execute(query, graphql=True)

    async def assert_query_result(self, query, result, *, msg=None):
        res = await self.con._legacy_execute(query)
        self.assert_data_shape(res, result, message=msg)
        return res

    async def assert_sorted_query_result(self, query, key, result, *,
                                         msg=None):
        res = await self.con._legacy_execute(query)
        # sort the query result by using the supplied key
        for r in res:
            # don't bother sorting empty things
            if r:
                r.sort(key=key)
        self.assert_data_shape(res, result, message=msg)
        return res

    @contextlib.contextmanager
    def assertRaisesRegex(self, exception, regex, msg=None,
                          **kwargs):
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

    def assert_data_shape(self, data, shape, message=None):
        _void = object()

        def _assert_type_shape(data, shape):
            if shape in (int, float):
                if not isinstance(data, shape):
                    self.fail(
                        '{}: expected {}, got {!r}'.format(
                            message, shape, data))
            else:
                try:
                    shape(data)
                except (ValueError, TypeError):
                    self.fail(
                        '{}: expected {}, got {!r}'.format(
                            message, shape, data))

        def _assert_dict_shape(data, shape):
            for sk, sv in shape.items():
                if not data or sk not in data:
                    self.fail(
                        '{}: key {!r} is missing\n{}'.format(
                            message, sk, pprint.pformat(data)))

                _assert_data_shape(data[sk], sv)

        def _list_shape_iter(shape):
            last_shape = _void

            for item in shape:
                if item is Ellipsis:
                    if last_shape is _void:
                        raise ValueError(
                            'invalid shape spec: Ellipsis cannot be the'
                            'first element')

                    while True:
                        yield last_shape

                last_shape = item

                yield item

        def _assert_list_shape(data, shape):
            if not isinstance(data, list):
                self.fail('{}: expected list'.format(message))

            if not data and shape:
                self.fail('{}: expected non-empty list'.format(message))

            shape_iter = _list_shape_iter(shape)

            i = 0
            for i, el in enumerate(data):
                try:
                    el_shape = next(shape_iter)
                except StopIteration:
                    self.fail(
                        '{}: unexpected trailing elements in list'.format(
                            message))

                _assert_data_shape(el, el_shape)

            if len(shape) > i + 1:
                if shape[i + 1] is not Ellipsis:
                    self.fail(
                        '{}: expecting more elements in list'.format(
                            message))

        def _assert_set_shape(data, shape):
            if not isinstance(data, (list, set)):
                self.fail('{}: expected list or set'.format(message))

            if not data and shape:
                self.fail('{}: expected non-empty set'.format(message))

            shape_iter = _list_shape_iter(sorted(shape))

            i = 0
            for i, el in enumerate(sorted(data)):
                try:
                    el_shape = next(shape_iter)
                except StopIteration:
                    self.fail(
                        '{}: unexpected trailing elements in set'.format(
                            message))

                _assert_data_shape(el, el_shape)

            if len(shape) > i + 1:
                if Ellipsis not in shape:
                    self.fail(
                        '{}: expecting more elements in set'.format(
                            message))

        def _assert_data_shape(data, shape):
            if isinstance(shape, nullable):
                if data is None:
                    return
                else:
                    shape = shape.value

            if isinstance(shape, list):
                return _assert_list_shape(data, shape)
            elif isinstance(shape, set):
                return _assert_set_shape(data, shape)
            elif isinstance(shape, dict):
                return _assert_dict_shape(data, shape)
            elif isinstance(shape, type):
                return _assert_type_shape(data, shape)
            elif isinstance(shape, (str, int, float)):
                if data != shape:
                    self.fail('{}: {} != {}'.format(message, data, shape))
            elif shape is None:
                if data is not None:
                    self.fail(
                        '{}: {!r} is expected to be None'.format(
                            message, data))
            else:
                raise ValueError('unsupported shape type {}'.format(shape))

        message = message or 'data shape differs'
        return _assert_data_shape(data, shape)


class DDLTestCase(BaseQueryTestCase):
    # DDL test cases generally need to be serialized
    # to avoid deadlocks in parallel execution.
    SERIALIZED = True


class NonIsolatedDDLTestCase(DDLTestCase):
    ISOLATED_METHODS = False

    BASE_TEST_CLASS = True


class QueryTestCase(BaseQueryTestCase):

    BASE_TEST_CLASS = True


def get_test_cases_setup(cases):
    result = []

    for case in cases:
        if not hasattr(case, 'get_setup_script'):
            continue

        setup_script = case.get_setup_script()
        if not setup_script:
            continue

        dbname = case.get_database_name()
        result.append((case, dbname, setup_script))

    return result


def setup_test_cases(cases, conn, num_jobs):
    setup = get_test_cases_setup(cases)

    async def _run():
        if num_jobs == 1:
            # Special case for --jobs=1
            for case, dbname, setup_script in setup:
                await _setup_database(dbname, setup_script, conn)
        else:
            async with taskgroup.TaskGroup(name='setup test cases') as g:
                # Use a semaphore to limit the concurrency of bootstrap
                # tasks to the number of jobs (bootstrap is heavy, having
                # more tasks than `--jobs` won't necessarily make
                # things faster.)
                sem = asyncio.BoundedSemaphore(num_jobs)

                async def controller(coro, *args):
                    async with sem:
                        await coro(*args)

                for case, dbname, setup_script in setup:
                    g.create_task(controller(
                        _setup_database, dbname, setup_script, conn))

    return asyncio.run(_run())


async def _setup_database(dbname, setup_script, conn_args):
    admin_conn = await edgedb.connect(
        database=edgedb_defines.EDGEDB_SUPERUSER_DB, **conn_args)

    try:
        await admin_conn.execute(f'CREATE DATABASE {dbname};')
    finally:
        await admin_conn.close()

    dbconn = await edgedb.connect(database=dbname, **conn_args)
    try:
        await dbconn.execute(setup_script)
    finally:
        await dbconn.close()

    return dbname


_lock_cnt = 0


def gen_lock_key():
    global _lock_cnt
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt
