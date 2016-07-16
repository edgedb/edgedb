import asyncio
import atexit
import functools
import inspect
import json
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


class ConnectedTestCase(ClusterTestCase):

    def setUp(self):
        super().setUp()
        self.con = self.loop.run_until_complete(
            self.cluster.connect(database='edgedb0', user='edgedb',
                                 loop=self.loop))

    def tearDown(self):
        try:
            self.con.close()
            # Give event loop another iteration so that connection
            # transport has a chance to properly close.
            self.loop.run_until_complete(asyncio.sleep(0, loop=self.loop))
            self.con = None
        finally:
            super().tearDown()


class DatabaseTestCase(ConnectedTestCase):
    def setUp(self):
        super().setUp()
        script = 'CREATE DATABASE edgedb_test'
        script += '\nCREATE MODULE test'
        if self.SETUP:
            script += '\n' + self.SETUP

        self.loop.run_until_complete(
            self.con.execute(script))

    def tearDown(self):
        script = ''

        if self.TEARDOWN:
            script = self.TEARDOWN

        script += '\n' + 'DROP DATABASE edgedb_test'

        try:
            self.loop.run_until_complete(
                self.con.execute(script))
        finally:
            super().tearDown()


class QueryTestCaseMeta(TestCaseMeta):
    @classmethod
    def wrap(mcls, meth):
        doc = meth.__doc__

        if not doc:
            # No docstring, run directly
            return meth

        doc = textwrap.dedent(doc)

        output = error = None

        query, _, output = doc.partition('\n% OK %')

        if not output:
            query, _, error = doc.partition('\n% ERROR %')

            if not error:
                raise TypeError('missing expected output in {!r}'.format(meth))
            else:
                output = json.loads(error)
        else:
            output = json.loads(output)

        @functools.wraps(meth)
        async def wrapper(self):
            try:
                res = await self.con.execute(query)
            except edgeclient_exc.Error as e:
                if error is None:
                    raise
                else:
                    res = {
                        'code': e.code
                    }

            self.assertEqual(res, output)

        return super().wrap(wrapper)


class QueryTestCase(DatabaseTestCase, metaclass=QueryTestCaseMeta):
    pass
