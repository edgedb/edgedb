##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections.abc
import enum
import io
import itertools
import multiprocessing
import multiprocessing.reduction
import os
import random
import re
import sys
import threading
import time
import types
import unittest.result
import unittest.runner
import unittest.signals
import warnings

import click

from edgedb.server import _testbase as tb
from edgedb.server import cluster as edgedb_cluster

from . import styles


cache = {}
result = None


def init_worker(param_queue, result_queue):
    global result

    # Make sure the generator is re-seeded, as we have inherited
    # the seed from the parent process.
    random.seed()

    result = ChannelingTestResult(result_queue)
    if not param_queue.empty():
        server_addr = param_queue.get()

        cluster = edgedb_cluster.RunningCluster(**server_addr)
        tb._set_default_cluster(cluster)


class StreamingTestSuite(unittest.TestSuite):
    _cleanup = False

    def run(self, test, result):
        with warnings.catch_warnings(record=True) as ww:
            warnings.resetwarnings()
            warnings.simplefilter('default')

            self._run(test, result)

            if ww:
                for wmsg in ww:
                    if wmsg.source is not None:
                        wmsg.source = str(wmsg.source)
                    result.addWarning(test, wmsg)

    def _run(self, test, result):
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

            self._queue.put((meth, args, kwargs))
        return _wrapper

    def __new__(mcls, name, bases, dct):
        for meth in {'startTest', 'addSuccess', 'addError', 'addFailure',
                     'addSkip', 'addExpectedFailure', 'addUnexpectedSuccess',
                     'addSubTest', 'addWarning', 'record_test_stats'}:
            dct[meth] = mcls.get_wrapper(meth)

        return super().__new__(mcls, name, bases, dct)


class ChannelingTestResult(unittest.result.TestResult,
                           metaclass=ChannelingTestResultMeta):
    def __init__(self, queue):
        super().__init__(io.StringIO(), False, 1)
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


def monitor_thread(queue, result):
    while True:
        methname, args, kwargs = queue.get()
        if methname is None and args is None and kwargs is None:
            # This must be the last message in the queue, injected
            # when all tests are completed and the pool is about
            # to be closed.
            break

        method = result
        for part in methname.split('.'):
            method = getattr(method, part)
        method(*args, **kwargs)


class ParallelTestSuite(unittest.TestSuite):
    def __init__(self, tests, server_conns, num_workers):
        self.tests = tests
        self.server_conns = server_conns
        self.num_workers = num_workers

    def run(self, result):
        # We use SimpleQueues because they are more predictable.
        # The do the necessary IO directly, without using a
        # helper thread.
        result_queue = multiprocessing.SimpleQueue()
        worker_param_queue = multiprocessing.SimpleQueue()

        # Prepopulate the worker param queue with server connection
        # information.
        for server_conn in self.server_conns:
            worker_param_queue.put(server_conn)

        result_thread = threading.Thread(
            name='test-monitor', target=monitor_thread,
            args=(result_queue, result), daemon=True)
        result_thread.start()

        initargs = (worker_param_queue, result_queue)

        pool = multiprocessing.Pool(
            self.num_workers, initializer=init_worker,
            initargs=initargs)

        with pool:
            pool.map(_run_test, self.tests, chunksize=1)

            # Post the terminal message to the queue so that
            # test-monitor can stop.
            result_queue.put((None, None, None))

            # Give the test-monitor thread some time to
            # process the queue messages.  If something
            # goes wrong, the thread will be forcibly
            # joined by a timeout.
            result_thread.join(timeout=3)

        return result


class Markers(enum.Enum):
    passed = '.'
    errored = 'E'
    skipped = 's'
    failed = 'F'
    xfailed = 'x'  # expected fail
    upassed = 'U'  # unexpected success


class BaseRenderer:
    def __init__(self, *, tests, stream):
        self.stream = stream
        self.styles_map = {
            marker.value: getattr(styles, f'marker_{marker.name}')
            for marker in Markers}

    def report(self, test, marker):
        raise NotImplementedError


class SimpleRenderer(BaseRenderer):
    def report(self, test, marker):
        click.echo(self.styles_map[marker.value](marker.value),
                   nl=False, file=self.stream)


class VerboseRenderer(BaseRenderer):
    fullnames = {
        Markers.passed: 'OK',
        Markers.errored: 'ERROR',
        Markers.skipped: 'SKIPPED',
        Markers.failed: 'FAILED',
        Markers.xfailed: 'expected failure',
        Markers.upassed: 'unexpected success',
    }

    def _render_test(self, test, marker):
        return f'{test}: {self.fullnames[marker]}'

    def report(self, test, marker):
        style = self.styles_map[marker.value]
        click.echo(style(self._render_test(test, marker)),
                   file=self.stream)


class MultiLineRenderer(BaseRenderer):
    def __init__(self, *, tests, stream):
        super().__init__(tests=tests, stream=stream)

        self.total_tests = len(tests)
        self.completed_tests = 0

        test_modules = {test.__class__.__module__ for test in tests}
        max_test_module_len = max(len(self._render_modname(name))
                                  for name in test_modules)
        self.first_col_width = max_test_module_len + 1  # 1 == len(' ')

        self.buffer = collections.defaultdict(str)
        self.last_lines = -1

    def report(self, test, marker):
        self.buffer[test.__class__.__module__] += marker.value
        self.completed_tests += 1
        self._render()

    def _render_modname(self, name):
        return name.replace('.', '/') + '.py'

    def _render(self):
        cols = click.get_terminal_size()[0]
        second_col_width = cols - self.first_col_width

        clear_cmd = ''
        if self.last_lines > 0:
            clear_cmd = '\033[A' * self.last_lines + '\r'

        lines = []
        for mod, progress in self.buffer.items():
            line = self._render_modname(mod).ljust(self.first_col_width, ' ')
            while progress:
                second_col = progress[:second_col_width]
                progress = progress[second_col_width:]

                # Apply styles *after* the slicing the string
                # (otherwise ANSI codes could be sliced in half).
                second_col = re.sub(
                    r'.',
                    lambda x: self.styles_map[x[0]](x[0]),
                    second_col)

                second_col = second_col.ljust(second_col_width, ' ')
                lines.append(f'{line}{second_col}')

                if line[0] != ' ':
                    line = ' ' * self.first_col_width

        lines.append(' ' * cols)
        lines.append(
            f'Progress: {self.completed_tests}/{self.total_tests} tests.')

        # Hide cursor.
        print('\033[?25l', end='', flush=True, file=self.stream)
        try:
            # Use `print` (not `click.echo`) because we want to
            # precisely control when the output is flushed.
            print(clear_cmd + '\n'.join(lines), flush=False, file=self.stream)
        finally:
            # Show cursor.
            print('\033[?25h', end='', flush=True, file=self.stream)

        self.last_lines = len(lines)


class ParallelTextTestResult(unittest.result.TestResult):
    def __init__(self, *, stream, verbosity, warnings, tests, failfast=False):
        super().__init__(stream, False, verbosity)
        self.verbosity = verbosity
        self.catch_warnings = warnings
        self.failfast = failfast
        self.test_stats = {}
        self.warnings = []
        # An index of all seen warnings to keep track
        # of repeated warnings.
        self._warnings = {}

        if self.verbosity > 1:
            self.ren = VerboseRenderer(tests=tests, stream=stream)
        elif (stream.isatty() and
                click.get_terminal_size()[0] > 60 and
                os.name != 'nt'):
            self.ren = MultiLineRenderer(tests=tests, stream=stream)
        else:
            self.ren = SimpleRenderer(tests=tests, stream=stream)

    def record_test_stats(self, test, stats):
        self.test_stats[test] = stats

    def _exc_info_to_string(self, err, test):
        # Errors are serialized in the worker.
        return err

    def getDescription(self, test):
        return str(test)

    def addSuccess(self, test):
        super().addSuccess(test)
        self.ren.report(test, Markers.passed)

    def addError(self, test, err):
        super().addError(test, err)
        self.ren.report(test, Markers.errored)

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.ren.report(test, Markers.failed)

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.ren.report(test, Markers.skipped)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self.ren.report(test, Markers.xfailed)

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.ren.report(test, Markers.upassed)

    def addWarning(self, test, wmsg):
        if not self.catch_warnings:
            return

        key = str(wmsg.message), wmsg.filename, wmsg.lineno

        if key not in self._warnings:
            self._warnings[key] = wmsg
            self.warnings.append((test, warnings.formatwarning(
                wmsg.message, wmsg.category, wmsg.filename, wmsg.lineno,
                wmsg.line
            )))


class ParallelTextTestRunner:
    def __init__(self, *, stream=None, num_workers=1, verbosity=1,
                 warnings=True, failfast=False):
        self.stream = stream if stream is not None else sys.stderr
        self.num_workers = num_workers
        self.verbosity = verbosity
        self.warnings = warnings
        self.failfast = failfast

    def run(self, test):
        session_start = time.monotonic()
        cases = tb.get_test_cases([test])
        setup = tb.get_test_cases_setup(cases)
        servers = []
        conns = []
        bootstrap_time_taken = 0
        tests_time_taken = 0
        result = None

        try:
            if setup:
                self._echo('Populating test databases... ',
                           fg='white', nl=False)

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

                bootstrap_time_taken = time.monotonic() - session_start
                self._echo('OK.')

            start = time.monotonic()

            all_tests = list(itertools.chain.from_iterable(
                tests for tests in cases.values()))

            suite = ParallelTestSuite(
                self._sort_tests(cases),
                conns,
                self.num_workers)

            result = ParallelTextTestResult(
                stream=self.stream, verbosity=self.verbosity,
                warnings=self.warnings, failfast=self.failfast,
                tests=all_tests)
            unittest.signals.registerResult(result)

            self._echo()
            suite.run(result)

            tests_time_taken = time.monotonic() - start

        except KeyboardInterrupt:
            raise

        finally:
            if self.verbosity == 1:
                self._echo()

            if setup:
                self._echo()
                self._echo('Shutting down test cluster... ', nl=False)
                tb.shutdown_worker_servers(servers)
                self._echo('OK.')

        if result is not None:
            self._render_result(
                result, bootstrap_time_taken, tests_time_taken)

        return result

    def _get_term_width(self):
        return click.get_terminal_size()[0] or 70

    def _echo(self, s='', **kwargs):
        if self.verbosity > 0:
            click.secho(s, file=self.stream, **kwargs)

    def _fill(self, char, **kwargs):
        self._echo(char * self._get_term_width(), **kwargs)

    def _format_time(self, seconds):
        hours = int(seconds // 3600)
        seconds %= 3600
        minutes = int(seconds // 60)
        seconds %= 60

        return f'{hours:02d}:{minutes:02d}:{seconds:04.1f}'

    def _print_errors(self, result):
        for kind, fg, errors in zip(('WARNING', 'ERROR', 'FAIL'),
                                    ('yellow', 'red', 'red'),
                                    (result.warnings, result.errors,
                                     result.failures)):
            for test, err in errors:
                self._fill('=', fg=fg)
                self._echo(f'{kind}: {result.getDescription(test)}',
                           fg=fg, bold=True)
                self._fill('-', fg=fg)
                self._echo(err)

    def _render_result(self, result, boot_time_taken, tests_time_taken):
        self._echo()

        if self.verbosity > 0:
            self._print_errors(result)

        if result.wasSuccessful():
            fg = 'green'
            outcome = 'SUCCESS'
        else:
            fg = 'red'
            outcome = 'FAILURE'

        if self.verbosity > 1:
            self._fill('=', fg=fg)
        self._echo(outcome, fg=fg, bold=True)

        counts = [('tests ran', result.testsRun)]

        for bit in ['failures', 'errors', 'expectedFailures',
                    'unexpectedSuccesses', 'skipped']:
            count = len(getattr(result, bit))
            if count:
                counts.append((bit, count))

        for bit, count in counts:
            self._echo(f'  {bit}: ', nl=False)
            self._echo(f'{count}', bold=True)

        self._echo()
        self._echo(f'Running times: ')
        if boot_time_taken:
            self._echo('  bootstrap: ', nl=False)
            self._echo(self._format_time(boot_time_taken), bold=True)

        self._echo('  tests: ', nl=False)
        self._echo(self._format_time(tests_time_taken), bold=True)

        if boot_time_taken:
            self._echo('  total: ', nl=False)
            self._echo(self._format_time(boot_time_taken + tests_time_taken),
                       bold=True)

        self._echo()

        return result

    def _sort_tests(self, cases):
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

        return list(tests)


# Disable pickling of traceback objects in multiprocessing.
# Test errors' tracebacks are serialized manually by
# `TestReesult._exc_info_to_string()`.  Therefore we need
# to make sure that some random __traceback__ attribute
# doesn't crash the test results queue.
multiprocessing.reduction.ForkingPickler.register(
    types.TracebackType,
    lambda o: (_restore_Traceback, ()))


def _restore_Traceback():
    return None
