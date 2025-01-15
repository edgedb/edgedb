#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Callable, Optional, Dict, TYPE_CHECKING

import asyncio
import collections
import collections.abc
import csv
import dataclasses
import enum
import faulthandler
import io
import itertools
import json
import multiprocessing
import multiprocessing.reduction
import multiprocessing.util
import os
import pathlib
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest.case
import unittest.result
import unittest.runner
import unittest.signals
import warnings

import click

import edgedb

from edb.common import devmode
from edb.testbase import server as tb

from . import cpython_state
from . import mproc_fixes
from . import styles
from . import results

if TYPE_CHECKING:
    import edb.server.cluster as edb_cluster

result: Optional[unittest.result.TestResult] = None
coverage_run: Optional[Any] = None
py_hash_secret: bytes = cpython_state.get_py_hash_secret()
py_random_seed: bytes = random.SystemRandom().randbytes(8)

faulthandler.enable(file=sys.stderr, all_threads=True)


def teardown_suite() -> None:
    # The TestSuite methods are mutating the *result* object,
    # and the suite itself does not hold any state whatsoever,
    # and, in our case specifically, it doesn't even hold
    # references to tests being run, so we can think of
    # its methods as static.
    suite = StreamingTestSuite()
    suite._tearDownPreviousClass(None, result)  # type: ignore[attr-defined]
    suite._handleModuleTearDown(result)  # type: ignore[attr-defined]


def init_worker(
    status_queue: multiprocessing.SimpleQueue,
    param_queue: multiprocessing.SimpleQueue,
    result_queue: multiprocessing.SimpleQueue,
    additional_init: Optional[Callable]
) -> None:
    global result
    global coverage_run
    global py_hash_secret
    global py_random_seed

    faulthandler.enable(file=sys.stderr, all_threads=True)

    if additional_init:
        additional_init()

    # Make sure the generator is re-seeded, as we have inherited
    # the seed from the parent process.
    py_random_seed = random.SystemRandom().randbytes(8)
    random.seed(py_random_seed)

    result = ChannelingTestResult(result_queue)
    if not param_queue.empty():
        server_addr, backend_dsn = param_queue.get()

        if server_addr is not None:
            os.environ['EDGEDB_TEST_CLUSTER_ADDR'] = json.dumps(server_addr)
        if backend_dsn:
            os.environ['EDGEDB_TEST_BACKEND_DSN'] = backend_dsn

    os.environ['EDGEDB_TEST_PARALLEL'] = '1'
    coverage_run = devmode.CoverageConfig.start_coverage_if_requested()
    py_hash_secret = cpython_state.get_py_hash_secret()
    status_queue.put(True)


def shutdown_worker() -> None:
    global coverage_run

    teardown_suite()
    if coverage_run is not None:
        coverage_run.stop()
        coverage_run.save()


class StreamingTestSuite(unittest.TestSuite):
    _cleanup = False

    def run(self, test, result):
        with warnings.catch_warnings(record=True) as ww:
            warnings.resetwarnings()
            warnings.simplefilter('default')

            # This is temporary, until we implement `subtransaction`
            # functionality of RFC1004
            warnings.filterwarnings(
                'ignore',
                message=r'The "transaction\(\)" method is deprecated'
                        r' and is scheduled to be removed',
                category=DeprecationWarning)

            self._run(test, result)

            if ww:
                for wmsg in ww:
                    if wmsg.source is not None:
                        wmsg.source = str(wmsg.source)
                    result.addWarning(test, wmsg)

    def _run(self, test, result):
        result._testRunEntered = True
        self._tearDownPreviousClass(test, result)
        self._handleModuleFixture(test, result)
        self._handleClassSetUp(test, result)
        result._previousTestClass = test.__class__

        if (getattr(test.__class__, '_classSetupFailed', False) or
                getattr(result, '_moduleSetUpFailed', False)):
            return

        result.annotate_test(test, {
            'py-hash-secret': py_hash_secret,
            'py-random-seed': py_random_seed,
            'runner-pid': os.getpid(),
        })

        start = time.monotonic()
        test.run(result)
        elapsed = time.monotonic() - start

        result.record_test_stats(test, {'running-time': elapsed})

        result._testRunEntered = False
        return result


def _run_test(workload):
    suite = StreamingTestSuite()

    if isinstance(workload, collections.abc.Iterable):
        # Got a test suite
        for test in workload:
            suite.run(test, result)
    else:
        suite.run(workload, result)


def _is_exc_info(args):
    return (
        isinstance(args, tuple) and
        len(args) == 3 and
        issubclass(args[0], BaseException)
    )


def _is_assert_failure(args):
    if _is_exc_info(args):
        return issubclass(args[0], AssertionError)
    elif isinstance(args, str):
        # HACK: If we serialized the error on the client side... just
        # detect it in the string.
        return "\nAssertionError" in args
    else:
        return False


@dataclasses.dataclass
class SerializedServerError:
    test_error: str
    server_error: str


class ChannelingTestResultMeta(type):
    @staticmethod
    def get_wrapper(meth):
        def _wrapper(self, *args, **kwargs):
            args = list(args)

            if args and _is_exc_info(args[-1]):
                exc_info = args[-1]
                err = self._exc_info_to_string(exc_info, args[0])
                if isinstance(exc_info[1], edgedb.EdgeDBError):
                    srv_tb = exc_info[1].get_server_context()
                    if srv_tb:
                        err = SerializedServerError(err, srv_tb)
                args[-1] = err

            try:
                self._queue.put((meth, args, kwargs))
            except Exception:
                print(
                    f'!!! Test worker child process: '
                    f'failed to serialize arguments for {meth}: '
                    f'*args={args} **kwargs={kwargs} !!!')
                raise
        return _wrapper

    def __new__(mcls, name, bases, dct):
        for meth in {'startTest', 'addSuccess', 'addError', 'addFailure',
                     'addSkip', 'addExpectedFailure', 'addUnexpectedSuccess',
                     'addSubTest', 'addWarning', 'record_test_stats',
                     'annotate_test'}:
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


def status_thread_func(
    result: ParallelTextTestResult,
    stop_event: threading.Event,
) -> None:
    while True:
        result.report_still_running()
        time.sleep(1)
        if stop_event.is_set():
            break


class ParallelTestSuite(unittest.TestSuite):
    def __init__(
        self, tests, server_conn, num_workers, backend_dsn, init_worker
    ):
        self.tests = tests
        self.server_conn = server_conn
        self.num_workers = num_workers
        self.stop_requested = False
        self.backend_dsn = backend_dsn
        self.init_worker = init_worker

    def run(self, result):
        # We use SimpleQueues because they are more predictable.
        # They do the necessary IO directly, without using a
        # helper thread.
        result_queue = multiprocessing.SimpleQueue()
        status_queue = multiprocessing.SimpleQueue()
        worker_param_queue = multiprocessing.SimpleQueue()

        # Prepopulate the worker param queue with server connection
        # information.
        for _ in range(self.num_workers):
            worker_param_queue.put((self.server_conn, self.backend_dsn))

        result_thread = threading.Thread(
            name='test-monitor',
            target=monitor_thread,
            args=(result_queue, result),
            daemon=True,
        )
        result_thread.start()

        status_thread_stop_event = threading.Event()
        status_thread = threading.Thread(
            name='test-status',
            target=status_thread_func,
            args=(result, status_thread_stop_event),
            daemon=True,
        )
        status_thread.start()

        initargs = (
            status_queue, worker_param_queue, result_queue, self.init_worker
        )

        pool = multiprocessing.Pool(
            self.num_workers,
            initializer=mproc_fixes.WorkerScope(init_worker, shutdown_worker),
            initargs=initargs)

        # Wait for all workers to initialize.
        for _ in range(self.num_workers):
            status_queue.get()

        with pool:
            for is_repeat in (False, True):
                if self.stop_requested:
                    break
                ar = pool.map_async(
                    _run_test,
                    filter(
                        lambda t: ('test_zREPEAT' in str(t)) == is_repeat,
                        self.tests,
                    ),
                    chunksize=1,
                )

                while True:
                    try:
                        ar.get(timeout=0.1)
                    except multiprocessing.TimeoutError:
                        # multiprocessing doesn't handle processes
                        # crashing very well, so we check ourselves
                        # (having disabled its own child pruning in
                        # mproc_fixes)
                        #
                        # TODO: Should we look into using
                        # concurrent.futures.ProcessPoolExecutor
                        # instead?
                        for p in pool._pool:
                            if p.exitcode:
                                tmsg = ''
                                if isinstance(result, ParallelTextTestResult):
                                    test = result.current_pids.get(p.pid)
                                    tmsg = f' while running {test}'
                                print(
                                    f"ERROR: Test worker {p.pid} crashed with "
                                    f"exit code {p.exitcode}{tmsg}",
                                    file=sys.stderr,
                                )
                                sys.stderr.flush()
                                os._exit(1)

                        if self.stop_requested:
                            break
                        else:
                            continue
                    else:
                        break

        # Wait for pool to shutdown, this includes test teardowns.
        pool.join()

        # Post the terminal message to the queue so that
        # test-monitor can stop.
        result_queue.put((None, None, None))
        status_thread_stop_event.set()

        # Give the test-monitor and test-status threads some time to process the
        # queue messages.  If something goes wrong, the thread will be forcibly
        # joined by a timeout.
        result_thread.join(timeout=3)
        status_thread.join(timeout=3)

        return result


class SequentialTestSuite(unittest.TestSuite):

    def __init__(self, tests, server_conn, backend_dsn, worker_init):
        self.tests = tests
        self.server_conn = server_conn
        self.stop_requested = False
        self.backend_dsn = backend_dsn
        self.worker_init = worker_init

    def run(self, result_):
        global result
        result = result_

        if self.server_conn:
            os.environ['EDGEDB_TEST_CLUSTER_ADDR'] = \
                json.dumps(self.server_conn)
        if self.backend_dsn:
            os.environ['EDGEDB_TEST_BACKEND_DSN'] = self.backend_dsn

        if self.worker_init:
            self.worker_init()

        random.seed(py_random_seed)

        for test in self.tests:
            _run_test(test)
            if self.stop_requested:
                break

        # Make sure the class and the module teardown methods are
        # executed for the trailing test, _run_test() does not do
        # this for us.
        teardown_suite()

        return result


class Markers(enum.Enum):
    passed = '.'
    errored = 'E'
    skipped = 's'
    failed = 'F'
    xfailed = 'x'  # expected fail
    not_implemented = '-'
    upassed = 'U'  # unexpected success


class OutputFormat(str, enum.Enum):
    auto = 'auto'
    simple = 'simple'
    stacked = 'stacked'
    verbose = 'verbose'


class BaseRenderer:
    def __init__(self, *, tests, stream):
        self.stream = stream
        self.styles_map = {
            marker.value: getattr(styles, f'marker_{marker.name}')
            for marker in Markers}

    def format_test(self, test):
        if isinstance(test, unittest.case._SubTest):
            if test.params:
                params = ', '.join(
                    f'{k}={v!r}' for k, v in test.params.items())
            else:
                params = '<subtest>'
            return f'{test.test_case} {{{params}}}'
        else:
            if hasattr(test, 'fail_notes') and test.fail_notes:
                fail_notes = ', '.join(
                    f'{k}={v!r}' for k, v in test.fail_notes.items())
                return f'{test} {{{fail_notes}}}'
            else:
                return str(test)

    def report(self, test, marker, description=None, *, currently_running):
        raise NotImplementedError

    def report_start(self, test, *, currently_running):
        return

    def report_still_running(self, still_running: dict[str, float]):
        return


class SimpleRenderer(BaseRenderer):
    def report(self, test, marker, description=None, *, currently_running):
        click.echo(self.styles_map[marker.value](marker.value),
                   nl=False, file=self.stream)


class VerboseRenderer(BaseRenderer):
    fullnames = {
        Markers.passed: 'OK',
        Markers.errored: 'ERROR',
        Markers.skipped: 'SKIPPED',
        Markers.failed: 'FAILED',
        Markers.xfailed: 'expected failure',
        Markers.not_implemented: 'not implemented',
        Markers.upassed: 'unexpected success',
    }

    def _render_test(self, test, marker, description):
        test_title = self.format_test(test)
        if description:
            return f'{test_title}: {self.fullnames[marker]}: {description}'
        else:
            return f'{test_title}: {self.fullnames[marker]}'

    def report(self, test, marker, description=None, *, currently_running):
        style = self.styles_map[marker.value]
        click.echo(style(self._render_test(test, marker, description)),
                   file=self.stream)

    def report_still_running(self, still_running: dict[str, float]) -> None:
        items = [f"{t} for {d:.02f}s" for t, d in still_running.items()]
        click.echo(f"still running:\n  {'\n   '.join(items)}")


class MultiLineRenderer(BaseRenderer):

    FT_LABEL = 'First few failed: '
    FT_MAX_LINES = 6

    R_LABEL = 'Running: '
    R_MAX_LINES = 6

    def __init__(self, *, tests, stream):
        super().__init__(tests=tests, stream=stream)

        self.total_tests = len(tests)
        self.completed_tests = 0

        test_modules = {test.__class__.__module__ for test in tests}
        max_test_module_len = max((len(self._render_modname(name))
                                   for name in test_modules), default=0)
        self.first_col_width = max_test_module_len + 1  # 1 == len(' ')

        self.failed_tests = set()

        self.buffer = collections.defaultdict(str)
        self.last_lines = -1
        self.max_lines = 0
        self.max_label_lines_rendered = collections.defaultdict(int)

    def report(self, test, marker, description=None, *, currently_running):
        if marker in {Markers.failed, Markers.errored}:
            test_name = test.id().rpartition('.')[2]
            if ' ' in test_name:
                test_name = test_name.split(' ')[0]
            self.failed_tests.add(test_name)

        self.buffer[test.__class__.__module__] += marker.value
        self.completed_tests += 1
        self._render(currently_running)

    def report_start(self, test, *, currently_running):
        self._render(currently_running)

    def report_still_running(self, still_running: dict[str, float]):
        # Still-running tests are already reported in normal repert
        return

    def _render_modname(self, name):
        return name.replace('.', '/') + '.py'

    def _color_second_column(self, line, style):
        return line[:self.first_col_width] + style(line[self.first_col_width:])

    def _render(self, currently_running):

        def print_line(line):
            if len(line) < cols:
                line += ' ' * (cols - len(line))
            lines.append(line)

        def print_empty_line():
            print_line(' ')

        last_render = self.completed_tests == self.total_tests
        cols, rows = shutil.get_terminal_size()
        second_col_width = cols - self.first_col_width

        def _render_test_list(label, max_lines, tests, style):

            if (
                len(label) > self.first_col_width
                or cols - self.first_col_width <= 40
            ):
                return

            print_empty_line()

            line = f'{label}{" " * (self.first_col_width - len(label))}'
            tests_lines = 1
            for testi, test in enumerate(tests, 1):
                last = testi == len(tests)

                if not last:
                    test += ', '

                test_name_len = len(test)

                if len(line) + test_name_len < cols:
                    line += test

                else:
                    if tests_lines == max_lines:
                        if len(line) + 3 < cols:
                            line += '...'
                        break

                    else:
                        line += (cols - len(line)) * ' '
                        line = self._color_second_column(line, style)
                        lines.append(line)

                        tests_lines += 1
                        line = self.first_col_width * ' '

                        if len(line) + test_name_len > cols:
                            continue

                        line += test

            line += (cols - len(line)) * ' '
            line = self._color_second_column(line, style)
            lines.append(line)

            # Prevent the rendered output from "jumping" up/down when we
            # render 2 lines worth of running tests just after we rendered
            # 3 lines.
            lkey = label.split(':')[0]
            # ^- We can't just use `label`, as we append extra information
            # to the "Running: (..)" label, so strip that
            for _ in range(self.max_label_lines_rendered[lkey] - tests_lines):
                lines.append(' ' * cols)
            self.max_label_lines_rendered[lkey] = max(
                self.max_label_lines_rendered[lkey],
                tests_lines
            )

        clear_cmd = ''
        if self.last_lines > 0:
            # Move cursor up `last_lines` times.
            clear_cmd = f'\r\033[{self.last_lines}A'

        lines = []
        for mod, progress in self.buffer.items():
            line = self._render_modname(mod).ljust(self.first_col_width, ' ')
            while progress:
                second_col = progress[:second_col_width]
                second_col = second_col.ljust(second_col_width, ' ')

                progress = progress[second_col_width:]

                # Apply styles *after* slicing and padding the string
                # (otherwise ANSI codes could be sliced in half).
                second_col = re.sub(
                    r'\S',
                    lambda x: self.styles_map[x[0]](x[0]),
                    second_col)

                lines.append(f'{line}{second_col}')

                if line[0] != ' ':
                    line = ' ' * self.first_col_width

        if not last_render:
            if self.failed_tests:
                _render_test_list(
                    self.FT_LABEL,
                    self.FT_MAX_LINES,
                    self.failed_tests,
                    styles.marker_errored,
                )

            running_tests = []
            for test in currently_running:
                test_name = test.id().rpartition('.')[2]
                if ' ' in test_name:
                    test_name = test_name.split(' ')[0]
                running_tests.append(test_name)

            if not running_tests:
                running_tests.append('...')

            _render_test_list(
                self.R_LABEL + f'({len(currently_running)})',
                self.R_MAX_LINES,
                running_tests,
                styles.marker_passed
            )

        print_empty_line()
        print_line(
            f'Progress: {self.completed_tests}/{self.total_tests} tests.'
        )

        if self.max_lines > len(lines):
            for _ in range(self.max_lines - len(lines)):
                lines.insert(0, ' ' * cols)

        if not last_render:
            # If it's not the last test, check if our render buffer
            # requires more rows than currently visible.
            if len(lines) + 1 > rows:
                # Scroll the render buffer to the bottom and
                # cut the lines from the beginning, so that it
                # will fit the screen.
                #
                # We need to do this because we can't move the
                # cursor past the visible screen area, so if we
                # render more data than the screen can fit, we
                # will have lot's of garbage output.
                lines = lines[len(lines) + 1 - rows:]
                lines[0] = '^' * cols

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
        self.max_lines = max(self.last_lines, self.max_lines)


class ParallelTextTestResult(unittest.result.TestResult):
    def __init__(self, *, stream, verbosity, warnings, tests,
                 output_format=OutputFormat.auto, failfast=False, suite):
        super().__init__(stream, False, verbosity)
        self.verbosity = verbosity
        self.catch_warnings = warnings
        self.failfast = failfast
        self.test_stats = []
        self.test_annotations = collections.defaultdict(dict)
        self.warnings = []
        self.notImplemented = []
        self.currently_running = {}
        self.current_pids = {}
        # An index of all seen warnings to keep track
        # of repeated warnings.
        self._warnings = {}
        self.suite = suite

        if (output_format is OutputFormat.verbose or
                (output_format is OutputFormat.auto and self.verbosity > 1)):
            self.ren = VerboseRenderer(tests=tests, stream=stream)
        elif (output_format is OutputFormat.stacked or
                (output_format is OutputFormat.auto and stream.isatty() and
                 shutil.get_terminal_size()[0] > 60 and
                 os.name != 'nt')):
            self.ren = MultiLineRenderer(tests=tests, stream=stream)
        else:
            self.ren = SimpleRenderer(tests=tests, stream=stream)

    def report_progress(self, test, marker, description=None):
        self.currently_running.pop(test, None)
        self.ren.report(
            test,
            marker,
            description,
            currently_running=list(self.currently_running),
        )

    def report_still_running(self):
        now = time.monotonic()
        still_running = {}
        for test, start in self.currently_running.items():
            running_for = now - start
            if running_for > 5.0:
                key = str(test)
                if (
                    test in self.test_annotations
                    and (pid := self.test_annotations[test].get('runner-pid'))
                ):
                    key = f'{key} (pid={pid})'

                still_running[key] = running_for
        if still_running:
            self.ren.report_still_running(still_running)

    def record_test_stats(self, test, stats):
        self.test_stats.append((test, stats))

    def annotate_test(self, test, annotations: Dict[str, Any]) -> None:
        self.test_annotations[test].update(annotations)

    def get_test_annotations(self, test) -> Optional[Dict[str, Any]]:
        return self.test_annotations.get(test)

    def _exc_info_to_string(self, err, test):
        # Errors are serialized in the worker.
        return err

    def getDescription(self, test):
        return self.ren.format_test(test)

    def startTest(self, test):
        super().startTest(test)
        self.currently_running[test] = time.monotonic()
        self.ren.report_start(
            test, currently_running=list(self.currently_running))
        if (
            test in self.test_annotations
            and (pid := self.test_annotations[test].get('runner-pid'))
        ):
            self.current_pids[pid] = test

    def addSuccess(self, test):
        super().addSuccess(test)
        self.report_progress(test, Markers.passed)

    def addError(self, test, err):
        super().addError(test, err)
        self.report_progress(test, Markers.errored)
        if self.failfast:
            self.suite.stop_requested = True

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.report_progress(test, Markers.failed)
        if self.failfast:
            self.suite.stop_requested = True

    def addSubTest(self, test, subtest, err):
        if err is not None:
            self.errors.append((subtest, self._exc_info_to_string(err, test)))
            self._mirrorOutput = True

            self.ren.report(
                subtest,
                Markers.errored,
                currently_running=list(self.currently_running))
            if self.failfast:
                self.suite.stop_requested = True

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.report_progress(test, Markers.skipped)

    def addExpectedFailure(self, test, err):
        method = getattr(test, test._testMethodName)
        try:
            reason = method.__et_xfail_reason__
            not_impl = getattr(method, '__et_xfail_not_implemented__', False)
            allow_fail = getattr(method, '__et_xfail_allow_failure__', False)
            allow_error = getattr(method, '__et_xfail_allow_error__', False)
        except AttributeError:
            # Maybe the whole test case class is decorated?
            reason = getattr(test, '__et_xfail_reason__', None)
            not_impl = getattr(test, '__et_xfail_not_implemented__', False)
            allow_fail = getattr(test, '__et_xfail_allow_failure__', False)
            allow_error = getattr(test, '__et_xfail_allow_error__', False)

        marker = Markers.not_implemented if not_impl else Markers.xfailed
        if not_impl:
            self.notImplemented.append(
                (test, self._exc_info_to_string(err, test)))
        else:
            is_fail = _is_assert_failure(err)
            if (allow_fail and is_fail) or (allow_error and not is_fail):
                super().addExpectedFailure(test, err)
            else:
                if is_fail:
                    super().addFailure(test, err)
                else:
                    super().addError(test, err)

        self.report_progress(test, marker, reason)

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.report_progress(test, Markers.upassed)

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

    def wasSuccessful(self):
        # Overload TestResult.wasSuccessful to ignore unexpected successes
        return (len(self.failures) == len(self.errors) == 0)


class ParallelTextTestRunner:

    def __init__(self, *, stream=None, num_workers=1, verbosity=1,
                 output_format=OutputFormat.auto, warnings=True,
                 failfast=False, shuffle=False, backend_dsn=None,
                 data_dir=None, try_cached_db=False, use_data_dir_dbs=False):
        self.stream = stream if stream is not None else sys.stderr
        self.num_workers = num_workers
        self.verbosity = verbosity
        self.warnings = warnings
        self.failfast = failfast
        self.shuffle = shuffle
        self.output_format = output_format
        self.backend_dsn = backend_dsn
        self.data_dir = data_dir
        self.use_data_dir_dbs = use_data_dir_dbs
        self.try_cached_db = try_cached_db

    def run(
        self,
        test: Any,
        selected_shard: int,
        total_shards: int,
        running_times_log_file: Optional[Any],
    ) -> results.TestResult:
        session_start = time.monotonic()
        cases = tb.get_test_cases([test])
        stats = {}
        if running_times_log_file:
            running_times_log_file.seek(0)
            stats = {
                k: (float(v), int(c))
                for k, v, c in csv.reader(running_times_log_file)
            }
        cases = tb.get_cases_by_shard(
            cases, selected_shard, total_shards, self.verbosity, stats,
        )
        setup = tb.get_test_cases_setup(cases)
        server_used = tb.test_cases_use_server(cases)
        worker_init = None
        bootstrap_time_taken = 0.0
        tests_time_taken = 0.0
        result: Optional[ParallelTextTestResult] = None
        cluster: Optional[edb_cluster.BaseCluster] = None
        conn = None
        tempdir = None
        setup_stats = []

        if server_used:
            tempdir = tempfile.TemporaryDirectory(prefix="edb-test-")

            if (
                not os.environ.get("EDGEDB_SERVER_TLS_CERT_FILE")
                and not os.environ.get("EDGEDB_SERVER_TLS_KEY_FILE")
                and not os.environ.get("GEL_SERVER_TLS_CERT_FILE")
                and not os.environ.get("GEL_SERVER_TLS_KEY_FILE")
            ):
                if self.verbosity >= 1:
                    self._echo(
                        'Generating TLS key and certificate...',
                        fg='white',
                    )
                cert_file = pathlib.Path(tempdir.name) / "tlscert.pem"
                key_file = pathlib.Path(tempdir.name) / "tlskey.pem"
                tb.generate_tls_cert(cert_file, key_file, ["localhost"])

                os.environ["GEL_SERVER_TLS_CERT_FILE"] = str(cert_file)
                os.environ["GEL_SERVER_TLS_KEY_FILE"] = str(key_file)

            if (
                not os.environ.get("EDGEDB_SERVER_JWS_KEY_FILE")
                and not os.environ.get("GEL_SERVER_JWS_KEY_FILE")
            ):
                jwk_file = pathlib.Path(tempdir.name) / "jwk.pem"
                if self.verbosity >= 1:
                    self._echo(
                        'Generating JSON Web Key...',
                        fg='white',
                    )
                tb.generate_jwk(jwk_file)

                os.environ["GEL_SERVER_JWS_KEY_FILE"] = str(jwk_file)

        try:
            if setup:
                if self.verbosity >= 1:
                    self._echo(
                        'Populating test databases... ',
                        fg='white',
                        nl=False,
                    )

                if self.verbosity > 1:
                    self._echo(
                        '\n -> Bootstrapping Gel instance...',
                        fg='white',
                        nl=False,
                    )

                async def _setup():
                    nonlocal cluster
                    nonlocal conn

                    data_dir = self.data_dir

                    if (
                        self.try_cached_db
                        and (cache_file := (
                            devmode.get_dev_mode_cache_dir() / 'test_dbs.tar')
                        ).is_file()
                    ):
                        if self.verbosity >= 1:
                            self._echo(
                                f'(using DB cache from {cache_file}) ',
                                fg='white',
                                nl=False,
                            )

                        data_dir = tempfile.mkdtemp(prefix="edb-test-c-")

                        # We shell out to tar with subprocess instead of using
                        # tarfile because it is quite a bit faster.
                        subprocess.check_call(
                            ('tar', 'xf', cache_file, '--strip-components=1'),
                            cwd=data_dir,
                        )

                    cluster = await tb.init_cluster(
                        backend_dsn=self.backend_dsn,
                        cleanup_atexit=False,
                        data_dir=data_dir,
                    )

                    if self.verbosity > 1:
                        self._echo(' OK')

                    conn = cluster.get_connect_args()

                    if not cluster.has_create_database():
                        return []

                    if not cluster.has_create_role():
                        for case in cases:
                            case.is_superuser = False

                    stats = await tb.setup_test_cases(
                        cases,
                        conn,
                        self.num_workers,
                        verbose=self.verbosity > 1,
                        try_cached_db=(
                            self.try_cached_db or self.use_data_dir_dbs
                        ),
                    )
                    if self.try_cached_db and any(
                        not x[1]['cached'] for x in stats
                    ):
                        # We stop the cluster before making a cache of
                        # the data directory. This isn't strictly
                        # necessary, but it speeds up startup when
                        # restoring a cached directory, since postgres
                        # needs to go through recovery if the shutdown
                        # wasn't clean.
                        cluster.stop()
                        if self.verbosity > 1:
                            self._echo(
                                f'\n -> Writing DB cache to {cache_file} ...',
                                fg='white',
                                nl=False,
                            )
                        subprocess.check_output(
                            ('tar', 'cf', cache_file, '.'),
                            cwd=cluster._data_dir,
                            stderr=subprocess.STDOUT,
                        )
                        await cluster.start(port=conn['port'])

                    return stats

                setup_stats = asyncio.run(_setup())

                assert cluster
                if cluster.has_create_database():
                    os.environ.update({
                        'EDGEDB_TEST_CASES_SET_UP': "skip"
                    })
                else:
                    os.environ.update({
                        'EDGEDB_TEST_CASES_SET_UP': "inplace"
                    })
                os.environ.update({
                    'EDGEDB_TEST_HAS_CREATE_ROLE': str(
                        cluster.has_create_role()
                    )
                })

                bootstrap_time_taken = time.monotonic() - session_start

                if self.verbosity >= 1:
                    self._echo('OK')

            start = time.monotonic()

            all_tests = list(itertools.chain.from_iterable(
                tests for tests in cases.values()))

            suite: unittest.TestSuite
            if self.num_workers > 1:
                suite = ParallelTestSuite(
                    self._sort_tests(cases),
                    conn,
                    self.num_workers,
                    self.backend_dsn,
                    worker_init,
                )
            else:
                suite = SequentialTestSuite(
                    self._sort_tests(cases),
                    conn,
                    self.backend_dsn,
                    worker_init,
                )

            result = ParallelTextTestResult(
                stream=self.stream, verbosity=self.verbosity,
                warnings=self.warnings, failfast=self.failfast,
                output_format=self.output_format,
                tests=all_tests, suite=suite)
            unittest.signals.registerResult(result)

            self._echo()
            suite.run(result)

            if running_times_log_file:
                for test, stat in result.test_stats + setup_stats:
                    name = str(test)
                    t = stat['running-time']
                    at, c = stats.get(name, (0, 0))
                    stats[name] = (at + (t - at) / (c + 1), c + 1)
                running_times_log_file.seek(0)
                running_times_log_file.truncate()
                writer = csv.writer(running_times_log_file)
                for k, v in stats.items():
                    writer.writerow((k, ) + v)
            tests_time_taken = time.monotonic() - start

        except KeyboardInterrupt:
            raise

        finally:
            if self.verbosity == 1:
                self._echo()

            if tempdir is not None:
                tempdir.cleanup()

            if setup:
                self._echo()
                self._echo('Shutting down test cluster... ', nl=False)
                tb._shutdown_cluster(cluster, destroy=self.data_dir is None)
                self._echo('OK.')

        if result is not None:
            return results.collect_result_data(
                result, bootstrap_time_taken, tests_time_taken
            )
        else:
            return None

    def _echo(self, s: str = '', **kwargs):
        if self.verbosity > 0:
            click.secho(s, file=self.stream, **kwargs)

    def _sort_tests(self, cases):
        serialized_suites = {}
        exclusive_suites = set()
        exclusive_tests = []

        for casecls, tests in cases.items():
            gg = getattr(casecls, 'get_parallelism_granularity', None)
            granularity = gg() if gg is not None else 'default'

            if granularity == 'suite':
                serialized_suites[casecls] = unittest.TestSuite(tests)
            elif granularity == 'system':
                exclusive_tests.extend(tests)
                exclusive_suites.add(casecls)

        tests = itertools.chain(
            serialized_suites.values(),
            itertools.chain.from_iterable(
                tests for casecls, tests in cases.items()
                if (
                    casecls not in serialized_suites
                    and casecls not in exclusive_suites
                )
            ),
            [unittest.TestSuite(exclusive_tests)],
        )

        test_list = list(tests)
        if self.shuffle:
            random.shuffle(test_list)

        return test_list


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
