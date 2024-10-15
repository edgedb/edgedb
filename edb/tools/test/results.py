#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

# Warning: this file is ran in GHA tests/test-conclusion, with (almost) no
# dependencies installed.

from __future__ import annotations

import binascii
import dataclasses
import datetime
import functools
import json
import pathlib
import sys
import traceback
import typing
import unittest
import glob
import shutil
from unittest.result import STDERR_LINE, STDOUT_LINE

import click

if typing.TYPE_CHECKING:
    from . import runner


@dataclasses.dataclass()
class TestCase:
    id: str
    description: str

    py_HashSecret: typing.Optional[str]
    py_random_seed: typing.Optional[str]

    error_message: typing.Optional[str]
    server_traceback: typing.Optional[str]


def _collect_case_data(
    result: runner.ParallelTextTestResult,
    test: unittest.TestCase,
    err: typing.Any,
) -> TestCase:
    from . import runner
    import edgedb

    py_HashSecret = None
    py_random_seed = None
    if annos := result.get_test_annotations(test):
        if phs := annos.get('py-hash-secret'):
            py_HashSecret = binascii.hexlify(phs).decode()
        if prs := annos.get('py-random-seed'):
            py_random_seed = binascii.hexlify(prs).decode()

    error_message = None
    server_traceback = None
    if runner._is_exc_info(err):
        if isinstance(err[1], edgedb.EdgeDBError):
            server_traceback = err[1].get_server_context()
        error_message = _exc_info_to_string(result, err, test)
    elif isinstance(err, runner.SerializedServerError):
        error_message, server_traceback = err.test_error, err.server_error
    elif isinstance(err, str):
        error_message = err

    return TestCase(
        id=test.id(),
        description=result.getDescription(test),
        py_HashSecret=py_HashSecret,
        py_random_seed=py_random_seed,
        error_message=error_message,
        server_traceback=server_traceback,
    )


def _exc_info_to_string(
    result: runner.ParallelTextTestResult,
    err: typing.Any,
    test: unittest.TestCase,
):
    """Converts a sys.exc_info()-style tuple of values into a string."""
    from edb.common import traceback as edb_traceback

    # Copied from unittest.TestResult._exc_info_to_string

    exctype, value, tb = err
    tb = result._clean_tracebacks(exctype, value, tb, test)  # type: ignore
    tb_e = traceback.TracebackException(
        exctype, value, tb, capture_locals=result.tb_locals, compact=True
    )
    tb_e.stack = edb_traceback.StandardStackSummary(tb_e.stack)
    msgLines = list(tb_e.format())

    if result.buffer:
        output = sys.stdout.getvalue()  # type: ignore
        error = sys.stderr.getvalue()  # type: ignore
        if output:
            if not output.endswith('\n'):
                output += '\n'
            msgLines.append(STDOUT_LINE % output)
        if error:
            if not error.endswith('\n'):
                error += '\n'
            msgLines.append(STDERR_LINE % error)
    return ''.join(msgLines)


@dataclasses.dataclass()
class TestResult:
    was_successful: bool

    testsRun: int
    boot_time_taken: float
    tests_time_taken: float

    # negative
    failures: typing.List[TestCase]
    errors: typing.List[TestCase]
    unexpected_successes: typing.List[TestCase]

    # positive
    warnings: typing.List[TestCase]
    skipped: typing.List[TestCase]
    not_implemented: typing.List[TestCase]
    expected_failures: typing.List[TestCase]


def _combine_test_results(a: TestResult, b: TestResult) -> TestResult:
    return TestResult(
        was_successful=a.was_successful and b.was_successful,
        testsRun=a.testsRun + b.testsRun,
        # this assumes each result comes from a parallel run
        boot_time_taken=max(a.boot_time_taken, b.boot_time_taken),
        # this assumes each result comes from a parallel run
        tests_time_taken=max(a.tests_time_taken, b.tests_time_taken),
        # negative
        failures=a.failures + b.failures,
        errors=a.errors + b.errors,
        unexpected_successes=a.unexpected_successes + b.unexpected_successes,
        # positive
        warnings=a.warnings + b.warnings,
        skipped=a.skipped + b.skipped,
        not_implemented=a.not_implemented + b.not_implemented,
        expected_failures=a.expected_failures + b.expected_failures,
    )


def collect_result_data(
    r: runner.ParallelTextTestResult,
    boot_time_taken: float,
    tests_time_taken: float,
):
    return TestResult(
        was_successful=r.wasSuccessful(),
        testsRun=r.testsRun,
        boot_time_taken=boot_time_taken,
        tests_time_taken=tests_time_taken,
        failures=[_collect_case_data(r, t, e) for t, e in r.failures],
        errors=[_collect_case_data(r, t, e) for t, e in r.errors],
        unexpected_successes=[
            _collect_case_data(r, t, None) for t in r.unexpectedSuccesses
        ],
        warnings=[_collect_case_data(r, t, e) for t, e in r.warnings],
        skipped=[_collect_case_data(r, t, e) for t, e in r.skipped],
        not_implemented=[
            _collect_case_data(r, t, e) for t, e in r.notImplemented
        ],
        expected_failures=[
            _collect_case_data(r, t, e) for t, e in r.expectedFailures
        ],
    )


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def _get_term_width():
    return shutil.get_terminal_size()[0] or 70


def _echo(file: typing.IO, s: str = '', **kwargs):
    click.secho(s, file=file, **kwargs)


def _fill(file: typing.IO, char, **kwargs):
    _echo(file, char * _get_term_width(), **kwargs)


def _format_time(seconds: float):
    hours = int(seconds // 3600)
    seconds %= 3600
    minutes = int(seconds // 60)
    seconds %= 60

    return f'{hours:02d}:{minutes:02d}:{seconds:04.1f}'


def _print_case_result(file: typing.IO, case: TestCase, kind: str, fg: str):
    _fill(file, '=', fg=fg)
    _echo(file, f'{kind}: {case.description}', fg=fg, bold=True)
    _fill(file, '-', fg=fg)

    if case.py_HashSecret or case.py_random_seed:
        if case.py_HashSecret:
            _echo(file, f'Py_HashSecret: {case.py_HashSecret}')
        if case.py_random_seed:
            _echo(file, f'random.seed(): {case.py_random_seed}')
        _fill(file, '-', fg=fg)

    if case.server_traceback:
        _echo(file, 'Server Traceback:', fg='red', bold=True)
        _echo(file, case.server_traceback)
    if case.error_message:
        if case.server_traceback:
            _echo(file, 'Test Traceback:', fg='red', bold=True)
        _echo(file, case.error_message)


def render_result(
    file: typing.IO,
    result: TestResult,
) -> None:
    _echo(file)

    # cases
    for case in sorted(result.warnings, key=lambda c: c.id):
        _print_case_result(file, case, 'WARNING', 'yellow')
    for case in sorted(result.errors, key=lambda c: c.id):
        _print_case_result(file, case, 'ERROR', 'red')
    for case in sorted(result.failures, key=lambda c: c.id):
        _print_case_result(file, case, 'FAIL', 'red')
    for case in sorted(result.unexpected_successes, key=lambda c: c.id):
        _print_case_result(file, case, 'UNEXPECTED SUCCESS', 'red')

    # outcome
    if result.was_successful:
        _echo(file, 'SUCCESS', fg='green', bold=True)
    else:
        _echo(file, 'FAILURE', fg='red', bold=True)

    # counts
    counts = [
        ('tests ran', result.testsRun, None),
        ('failures', len(result.failures), 'red'),
        ('errors', len(result.errors), 'red'),
        ('unexpected successes', len(result.unexpected_successes), 'red'),
        ('not implemented', len(result.not_implemented), 'yellow'),
        ('skipped', len(result.skipped), 'yellow'),
        ('expected failures', len(result.expected_failures), None),
    ]
    for name, count, fg in counts:
        if not count:
            continue
        _echo(file, f'  {name}: ', nl=False, fg=fg)
        _echo(file, f'{count}', bold=True)

    # running times
    _echo(file)
    _echo(file, f'Running times: ')
    if result.boot_time_taken > 0.0:
        _echo(file, '  bootstrap: ', nl=False)
        _echo(file, _format_time(result.boot_time_taken), bold=True)

    _echo(file, '  tests: ', nl=False)
    _echo(file, _format_time(result.tests_time_taken), bold=True)

    if result.boot_time_taken > 0.0:
        _echo(file, '  total: ', nl=False)
        _echo(
            file,
            _format_time(result.boot_time_taken + result.tests_time_taken),
            bold=True,
        )

    _echo(file)


def _result_log_path(path_template: str) -> typing.Optional[pathlib.Path]:
    now = str(datetime.datetime.now()).replace(' ', '_')
    path = pathlib.Path(path_template.replace('%TIMESTAMP%', now))

    dir = path.parent
    try:
        dir.mkdir(parents=True, exist_ok=True)
        return path
    except OSError:
        # this might happen when the process is running in readonly mode
        return None


def write_result(path_template: str, res: TestResult):
    path = _result_log_path(path_template)
    if not path:
        return None
    log_file = open(path, 'w')

    json.dump(dataclasses.asdict(res), log_file, indent=4)


def read_unsuccessful(path_template: str) -> typing.List[str]:
    log_path = _result_log_path(path_template)
    if not log_path:
        return []
    results = list(log_path.parent.iterdir())
    if not results:
        return []

    results.sort()
    last = results[-1]

    try:
        result_dict = json.load(open(last, 'r'))
    except Exception:
        return []
    result: TestResult = _dataclass_from_dict(TestResult, result_dict)
    return [
        case.id.split('.')[-1]
        for case in result.failures
        + result.errors
        + result.unexpected_successes
    ]


def _dataclass_from_dict(cls: typing.Type | None, data: typing.Any):
    if not cls:
        return data

    if hasattr(cls, '__origin__') and cls.__origin__ is list:
        args = cls.__args__
        return [_dataclass_from_dict(args[0], e) for e in data]

    if not dataclasses.is_dataclass(cls):
        return data
    if not isinstance(data, dict):
        raise ValueError(f'expected a dict of a dataclass, found {type(data)}')

    field_types: typing.Mapping[str, typing.Type] = typing.get_type_hints(cls)
    return cls(
        **{
            k: _dataclass_from_dict(field_types.get(k), v)
            for k, v in data.items()
        }
    )


# if this file is invoked directly
if __name__ == '__main__':
    # read result JSON files, concat them into a single result and render
    result_path_glob = sys.argv[1]

    results: typing.List[TestResult] = []
    for new_file in glob.glob(result_path_glob):
        with open(new_file) as f:
            result_dict = json.load(f)
            results.append(_dataclass_from_dict(TestResult, result_dict))

    result = functools.reduce(
        lambda acc, r: _combine_test_results(acc, r) if acc else r,
        results,
        typing.cast(typing.Optional[TestResult], None),
    )
    if not result:
        raise ValueError(
            f'no result files were found at glob {result_path_glob}'
        )

    render_result(sys.stdout, result)
    sys.exit(0 if result.was_successful else 1)
