import collections.abc
import io
import itertools
import multiprocessing
import os
import queue as std_queue
import random
import threading
import time
import unittest.runner

from edgedb.server import _testbase as tb
from edgedb.server import cluster as edgedb_cluster


cache = {}
result = None


def init_worker(param_queue, result_queue, descriptions, verbosity):
    global result

    # Make sure the generator is re-seeded, as we have inherited
    # the seed from the parent process.
    random.seed()

    result = ChannelingTestResult(result_queue, descriptions, verbosity)
    server_addr = param_queue.get()

    cluster = edgedb_cluster.RunningCluster(**server_addr)
    tb._set_default_cluster(cluster)


class StreamingTestSuite(unittest.TestSuite):
    _cleanup = False

    def run(self, test, result):
        self._tearDownPreviousClass(test, result)
        self._handleModuleFixture(test, result)
        self._handleClassSetUp(test, result)
        result._previousTestClass = test.__class__

        if (getattr(test.__class__, '_classSetupFailed', False) or
                getattr(result, '_moduleSetUpFailed', False)):
            return

        start = time.monotonic()
        test.run(result)
        elapsed = time.monotonic() - start

        result.record_test_stats(test, {'running-time': elapsed})

        return result


def _run_test(case):
    case_class = type(case)

    try:
        suite = cache[case_class]
    except KeyError:
        suite = cache[case_class] = StreamingTestSuite()

    if isinstance(case, collections.abc.Iterable):
        # Got a test suite
        for test in case:
            suite.run(test, result)
    else:
        suite.run(case, result)


class ChannelingTestResultMeta(type):
    @staticmethod
    def get_wrapper(meth):
        def _wrapper(self, *args, **kwargs):
            args = list(args)

            if (args and isinstance(args[-1], tuple) and
                    len(args[-1]) == 3 and
                    issubclass(args[-1][0], Exception)):
                # exc_info triple
                error_text = self._exc_info_to_string(args[-1], args[0])
                args[-1] = error_text

            self._queue.put_nowait((meth, args, kwargs))
        return _wrapper

    def __new__(mcls, name, bases, dct):
        for meth in {'startTest', 'addSuccess', 'addError', 'addFailure',
                     'addSkip', 'addExpectedFailure', 'addUnexpectedSuccess',
                     'addSubTest', 'record_test_stats'}:
            dct[meth] = mcls.get_wrapper(meth)

        return super().__new__(mcls, name, bases, dct)


class ChannelingTestResult(unittest.runner.TextTestResult,
                           metaclass=ChannelingTestResultMeta):
    def __init__(self, queue, descriptions, verbosity):
        super().__init__(io.StringIO(), descriptions, verbosity)
        self._queue = queue

    def _setupStdout(self):
        pass

    def _restoreStdout(self):
        pass

    def printErrors(self):
        pass

    def printErrorList(self, flavour, errors):
        pass

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_queue')
        state.pop('_original_stdout')
        state.pop('_original_stderr')
        return state


stop_monitor = threading.Event()


def monitor_thread(queue, result):
    while not stop_monitor.is_set():
        try:
            methname, args, kwargs = queue.get(timeout=1)
        except std_queue.Empty:
            continue

        method = result
        for part in methname.split('.'):
            method = getattr(method, part)
        method(*args, **kwargs)


class ParallelTestSuite(unittest.TestSuite):
    def __init__(self, tests, server_conns):
        self.tests = tests
        self.server_conns = server_conns

    def run(self, result):
        result_queue = multiprocessing.Queue()
        worker_param_queue = multiprocessing.Queue()

        # Prepopulate the worker param queue with server connection
        # information.
        for server_conn in self.server_conns:
            worker_param_queue.put_nowait(server_conn)

        result_thread = threading.Thread(
            name='test-monitor', target=monitor_thread,
            args=(result_queue, result), daemon=True)
        result_thread.start()

        descriptions = result.descriptions
        verbosity = 1 if result.dots else 2

        initargs = (worker_param_queue, result_queue,
                    descriptions, verbosity)

        pool = multiprocessing.Pool(
            len(self.server_conns), initializer=init_worker,
            initargs=initargs)

        with pool:
            pool.map(_run_test, self.tests, chunksize=1)

        stop_monitor.set()
        result_thread.join()

        return result


class ParallelTextTestResult(unittest.runner.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_stats = {}

    def record_test_stats(self, test, stats):
        self.test_stats[test] = stats

    def _exc_info_to_string(self, err, test):
        # Errors are serialized in the worker.
        return err


class ParallelTextTestRunner(unittest.runner.TextTestRunner):
    resultclass = ParallelTextTestResult

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num_workers = os.cpu_count()

    def run(self, test):
        cases = tb.get_test_cases([test])
        setup = tb.get_test_cases_setup(cases)
        servers = []
        conns = []

        try:
            if setup:
                cluster = tb._init_cluster(init_settings={
                    # Make sure the server accomodates the possible
                    # number of connections from all test classes.
                    'max_connections': self.num_workers * len(setup) * 2
                }, cleanup_atexit=False)

                servers, conns = tb.start_worker_servers(
                    cluster, self.num_workers)

                tb.setup_test_cases(cases, conns)

                os.environ.update({
                    'EDGEDB_TEST_CASES_SET_UP': "1"
                })

            tests = self.sort_tests(cases)
            suite = ParallelTestSuite(tests, conns)

            result = super().run(suite)

        except KeyboardInterrupt:
            print('SHUTTING DOWN SERVERS')
            raise

        finally:
            tb.shutdown_worker_servers(servers)

        return result

    def sort_tests(self, cases):
        serialized_suites = {
            casecls: unittest.TestSuite(tests)
            for casecls, tests in cases.items()
            if getattr(casecls, 'SERIALIZED', False)
        }

        tests = itertools.chain(
            serialized_suites.values(),
            itertools.chain.from_iterable(
                tests for casecls, tests in cases.items()
                if casecls not in serialized_suites
            )
        )

        return tests
