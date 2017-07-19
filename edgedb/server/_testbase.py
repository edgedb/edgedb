##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncio
import atexit
import collections
import contextlib
import functools
import inspect
import multiprocessing
import os
import pprint
import re
import sys
import textwrap
import unittest

import pytest

from edgedb import client as edgedb_client
from edgedb.server import cluster as edgedb_cluster


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
        if hasattr(cls, 'get_database_name'):
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
        asyncio.set_event_loop(None)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)


_default_cluster = None


def _start_cluster(cleanup_atexit=True):
    global _default_cluster

    if _default_cluster is None:
        test_cluster_addr = os.environ.get('EDGEDB_TEST_CLUSTER')
        if test_cluster_addr:
            m = re.match(r'^(\w+):(\d+)', test_cluster_addr)
            if not m:
                raise ValueError(
                    f'invalid value in EDGEDB_TEST_CLUSTER '
                    f'environment variable: {test_cluster_addr}')

            host = m.group(1)
            port = m.group(2)

            _default_cluster = edgedb_cluster.RunningCluster(
                host=host, port=port)
        else:
            if (not os.environ.get('EDGEDB_DEBUG_SERVER') and
                    not os.environ.get('EDGEDB_LOG_LEVEL')):
                _env = {'EDGEDB_LOG_LEVEL': 'silent'}
            else:
                _env = {}

            _default_cluster = edgedb_cluster.TempCluster(env=_env)
            _default_cluster.init()
            _default_cluster.start(port='dynamic', timezone='UTC')
            if cleanup_atexit:
                atexit.register(_shutdown_cluster, _default_cluster)

    return _default_cluster


def _shutdown_cluster(cluster):
    cluster.stop()
    cluster.destroy()


@pytest.mark.usefixtures('cluster')
class ClusterTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if getattr(sys, '_in_pytest', None):
            # Under pytest we start the cluster via a session fixture.
            # This dance is necessary to make sure the destruction of
            # the cluster is done _before_ pytest plugin deinit, so
            # pytest-cov et al work as expected.
            #
            cls.cluster = _start_cluster(False)
        else:
            cls.cluster = _start_cluster(True)


class RollbackChanges:
    def __init__(self, test):
        self._conn = test.con

    async def __aenter__(self):
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx.rollback()


class ConnectedTestCase(ClusterTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.con = cls.loop.run_until_complete(
            cls.cluster.connect(
                database='edgedb0', user='edgedb', loop=cls.loop))

    @classmethod
    def tearDownClass(cls):
        try:
            cls.con.close()
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            cls.loop.run_until_complete(asyncio.sleep(0, loop=cls.loop))
            cls.con = None
        finally:
            super().tearDownClass()

    def _run_and_rollback(self):
        return RollbackChanges(self)


class DatabaseTestCase(ConnectedTestCase):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None

    SETUP_METHOD = None
    TEARDOWN_METHOD = None

    def setUp(self):
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
            super().tearDown()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.admin_conn = cls.con
        script = 'CREATE DATABASE edgedb_test;'

        cls.loop.run_until_complete(cls.admin_conn.execute(script))

        cls.con = cls.loop.run_until_complete(
            cls.cluster.connect(
                database='edgedb_test', user='edgedb', loop=cls.loop))

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

                if module_name != 'test':
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

        if cls.TEARDOWN:
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.loop.run_until_complete(cls.con.execute(script))
        finally:
            cls.con.close()
            cls.con = cls.admin_conn

            script = 'DROP DATABASE edgedb_test;'

            try:
                cls.loop.run_until_complete(cls.admin_conn.execute(script))
            finally:
                super().tearDownClass()


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
    async def query(self, query):
        query = textwrap.dedent(query)
        return await self.con.execute(query)

    async def assert_query_result(self, query, result):
        res = await self.con.execute(query)
        self.assert_data_shape(res, result)
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
                        '{}: unexpected trailing elements in list'.format(
                            message))

                _assert_data_shape(el, el_shape)

            if len(shape) > i + 1:
                if shape[i + 1] is not Ellipsis:
                    self.fail(
                        '{}: expecting more elements in list'.format(
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
    pass


class QueryTestCaseMeta(TestCaseMeta):

    @classmethod
    def wrap_opt(mcls, meth):
        @functools.wraps(meth)
        def wrapper(self, *args, **kwargs):
            old_opt = self.con.get_optimize()
            self.con.set_optimize(True)
            try:
                self.loop.run_until_complete(meth(self, *args, **kwargs))
            finally:
                self.con.set_optimize(old_opt)

        return wrapper

    @classmethod
    def add_method(mcls, methname, ns, meth):
        wrapper = mcls.wrap(meth)
        if getattr(meth, '_expected_no_optimizer_failure', False):
            wrapper = unittest.expectedFailure(wrapper)
        wrapper.__name__ = methname + '_no_opt'
        ns[methname + '_no_opt'] = wrapper

        wrapped = mcls.wrap_opt(meth)
        if getattr(meth, '_expected_optimizer_failure', False):
            wrapped = unittest.expectedFailure(wrapped)
        wrapped.__name__ = methname + '_opt'
        ns[methname + '_opt'] = wrapped


class QueryTestCase(BaseQueryTestCase, metaclass=QueryTestCaseMeta):
    pass


def expected_optimizer_failure(obj):
    obj._expected_optimizer_failure = True
    return obj


def expected_no_optimizer_failure(obj):
    obj._expected_no_optimizer_failure = True
    return obj


async def setup_test_cases(cluster, cases, *, jobs=None):
    setup = {}

    conn_args = dict(cluster.get_connect_args())
    conn_args['user'] = 'edgedb'

    admin_conn = await edgedb_client.connect(database='edgedb0', **conn_args)
    try:
        await admin_conn.execute(f'CREATE DATABASE edgedb1;')
    finally:
        admin_conn.close()

    for case in cases:
        if not hasattr(case, 'get_setup_script'):
            continue

        setup_script = case.get_setup_script()
        if not setup_script:
            continue

        dbname = case.get_database_name()
        setup[dbname] = setup_script

    if jobs is None:
        jobs = multiprocessing.cpu_count()

    loop = asyncio.get_event_loop()
    tasks = []

    for dbname, setup_script in setup.items():
        task = loop.create_task(
            _setup_database(dbname, setup_script, conn_args))
        tasks.append(task)

    await asyncio.gather(*tasks)


async def _setup_database(dbname, setup_script, conn_args):
    admin_conn = await edgedb_client.connect(database='edgedb1', **conn_args)

    try:
        await admin_conn.execute(f'CREATE DATABASE {dbname};')
    finally:
        admin_conn.close()

    dbconn = await edgedb_client.connect(database=dbname, **conn_args)
    try:
        await dbconn.execute(setup_script)
    finally:
        dbconn.close()
