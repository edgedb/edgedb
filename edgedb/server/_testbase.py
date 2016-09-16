import asyncio
import atexit
import functools
import inspect
import pprint
import textwrap
import unittest

from edgedb.server import cluster as edgedb_cluster
from edgedb.client import exceptions as edgeclient_exc


class TestCaseMeta(type(unittest.TestCase)):
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

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns):
            wrapper = mcls.wrap(meth)
            ns[methname] = wrapper

        return super().__new__(mcls, name, bases, ns)


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop = loop

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)


_default_cluster = None


def _start_cluster():
    global _default_cluster

    if _default_cluster is None:
        _default_cluster = edgedb_cluster.TempCluster()
        _default_cluster.init()
        _default_cluster.start()
        atexit.register(_shutdown_cluster, _default_cluster)

    return _default_cluster


def _shutdown_cluster(cluster):
    cluster.stop()
    cluster.destroy()


class ClusterTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.cluster = _start_cluster()


class RollbackChanges:
    def __init__(self, test):
        self._conn = test.con

    async def __aenter__(self):
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def __aexit__(self, exc_type, exc, tb):
        await self._tx.rollback()


class ConnectedTestCase(ClusterTestCase):
    def setUp(self):
        super().setUp()
        self.con = self.loop.run_until_complete(
            self.cluster.connect(
                database='edgedb0', user='edgedb', loop=self.loop))

    def tearDown(self):
        try:
            self.con.close()
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            self.loop.run_until_complete(asyncio.sleep(0, loop=self.loop))
            self.con = None
        finally:
            super().tearDown()

    def _run_and_rollback(self):
        return RollbackChanges(self)


class DatabaseTestCase(ConnectedTestCase):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None

    def setUp(self):
        super().setUp()
        self.admin_conn = self.con
        script = 'CREATE DATABASE edgedb_test;'

        self.loop.run_until_complete(self.admin_conn.execute(script))

        self.con = self.loop.run_until_complete(
            self.cluster.connect(
                database='edgedb_test', user='edgedb', loop=self.loop))

        script = '\nCREATE MODULE test;'

        if self.SCHEMA:
            with open(self.SCHEMA, 'r') as sf:
                schema = sf.read()

            script += '\nCREATE DELTA test::d1 TO $${schema}$$;'.format(
                schema=schema)
            script += '\nCOMMIT DELTA test::d1;'

        if self.SETUP:
            script += '\n' + self.SETUP

        self.loop.run_until_complete(self.con.execute(script))

    def tearDown(self):
        script = ''

        if self.TEARDOWN:
            script = self.TEARDOWN.strip()

        try:
            if script:
                self.loop.run_until_complete(self.con.execute(script))
        finally:
            self.con.close()
            self.con = self.admin_conn

            script = 'DROP DATABASE edgedb_test;'

            try:
                self.loop.run_until_complete(self.admin_conn.execute(script))
            finally:
                super().tearDown()


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


class QueryTestCaseMeta(TestCaseMeta):
    @classmethod
    def wrap(mcls, meth):
        sig = inspect.signature(meth)

        if len(sig.parameters) <= 1:
            # No input parameter, run directly
            return super().wrap(meth)

        if 'input' not in sig.parameters:
            raise TypeError(
                'missing expected "input" param in {!r}'.format(meth))

        queries = sig.parameters['input'].default

        if not queries or not isinstance(queries, str) or not queries.strip():
            raise TypeError(
                'missing expected string default in "input" param in '
                '{!r}'.format(meth))

        queries = textwrap.dedent(queries)

        output = sig.return_annotation
        if output is inspect.Signature.empty:
            raise TypeError(
                'missing expected return annotation in '
                '{!r}'.format(meth))

        if isinstance(output, Error):
            expected_shape = output.shape
        else:
            expected_shape = output

        @functools.wraps(meth)
        async def wrapper(self):
            try:
                res = await self.con.execute(queries)
            except edgeclient_exc.EdgeDBError as e:
                if not isinstance(output, Error):
                    raise
                else:
                    with self.assertRaisesRegex(output.cls, output.message):
                        raise
                    res = vars(e)

            self.assert_data_shape(res, expected_shape)

        return super().wrap(wrapper)


class QueryTestCase(DatabaseTestCase, metaclass=QueryTestCaseMeta):
    async def assert_query_result(self, query, result):
        res = await self.con.execute(query)
        self.assert_data_shape(res, result)

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

            for el in data:
                try:
                    el_shape = next(shape_iter)
                except StopIteration:
                    self.fail(
                        '{}: unexpected trailing elements in list'.format(
                            message))

                _assert_data_shape(el, el_shape)

        def _assert_data_shape(data, shape):
            if isinstance(shape, nullable):
                if data is None:
                    return
                else:
                    shape = shape.value

            if isinstance(shape, list):
                return _assert_list_shape(data, shape)
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
