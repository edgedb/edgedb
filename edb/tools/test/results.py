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


from __future__ import annotations

import typing
import unittest
import dataclasses
import json
import datetime
import pathlib

import shutil
import click
import binascii

import edgedb

if typing.TYPE_CHECKING:
    from . import runner


@dataclasses.dataclass()
class TestCase:
    name: str
    description: str

    py_HashSecret: bytes
    py_random_seed: bytes

    error_message: typing.Optional[str]
    server_traceback: typing.Optional[str]


def _collect_case_data(
    result: runner.ParallelTextTestResult,
    test: unittest.TestCase,
    err: typing.Tuple[typing.Type, Exception, typing.Any],
) -> str:
    from . import runner

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
        error_message = unittest.result.TestResult._exc_info_to_string(
            result, err, test
        )
    elif isinstance(err, runner.SerializedServerError):
        error_message, server_traceback = err.test_error, err.server_error
    elif isinstance(err, str):
        error_message = err

    return TestCase(
        name=test._testMethodName,
        description=result.getDescription(test),
        py_HashSecret=py_HashSecret,
        py_random_seed=py_random_seed,
        error_message=error_message,
        server_traceback=server_traceback,
    )


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
    for case in result.warnings:
        _print_case_result(file, case, 'WARNING', 'yellow')
    for case in result.errors:
        _print_case_result(file, case, 'ERROR', 'red')
    for case in result.failures:
        _print_case_result(file, case, 'FAIL', 'red')
    for case in result.unexpected_successes:
        _print_case_result(file, case, 'UNEXPECTED SUCCESS', 'red')

    # outcome
    if result.was_successful:
        _echo(file, 'SUCCESS', fg='green', bold=True)
    else:
        _echo(file, 'FAILURE', fg='red', bold=True)

    # counts
    counts = [
        ('tests ran', result.testsRun),
        ('failures', len(result.failures)),
        ('errors', len(result.errors)),
        ('expected failures', len(result.expected_failures)),
        ('not implemented', len(result.not_implemented)),
        ('unexpected successes', len(result.unexpected_successes)),
        ('skipped', len(result.skipped)),
    ]
    for bit, count in counts:
        if not count:
            continue
        _echo(file, f'  {bit}: ', nl=False)
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


def _result_log_dir() -> pathlib.Path:
    dir = pathlib.Path('.') / 'build' / 'test-results'
    dir.mkdir(exist_ok=True)
    return dir


def _result_log_file() -> typing.IO:
    dir = _result_log_dir()
    now = str(datetime.datetime.now()).replace(' ', '_')
    return open(dir / f'{now}.json', 'w')


def write_result(res: TestResult):
    log_file = _result_log_file()

    json.dump(dataclasses.asdict(res), log_file, indent=4)


def read_unsuccessful() -> typing.List[str]:
    dir = _result_log_dir()
    results = list(dir.iterdir())
    if not results:
        return []

    results.sort()
    last = results[-1]

    try:
        result_dict = json.load(open(last, 'r'))
        result = _dataclass_from_dict(TestResult, result_dict)
    except Exception:
        return []
    return [
        case.name
        for case in result.failures
        + result.errors
        + result.unexpected_successes
    ]


def _dataclass_from_dict(klass: typing.Type, d: typing.Dict):
    # JSON -> dataclasses is surprisingly hard.
    # Maybe we could use a library here.

    # HACK: hardcoded List[TestCase]
    if isinstance(d, list):
        return [_dataclass_from_dict(TestCase, e) for e in d]

    try:
        field_types = {f.name: f.type for f in dataclasses.fields(klass)}
    except:
        return d  # Not a dataclass field

    return klass(**{f: _dataclass_from_dict(field_types[f], d[f]) for f in d})
