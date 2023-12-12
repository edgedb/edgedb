import typing
import unittest
import dataclasses
import json
import datetime
import pathlib


def _result_log_dir() -> pathlib.Path:
    dir = pathlib.Path('.') / 'build' / 'test-results'
    dir.mkdir(exist_ok=True)
    return dir


def _result_log_file() -> typing.IO:
    dir = _result_log_dir()
    now = str(datetime.datetime.now()).replace(' ', '_')
    return open(dir / f'{now}.json', 'w')


def log_result(ut_result: unittest.result.TestResult):
    log_file = _result_log_file()

    result = TestResult(
        testsRun=ut_result.testsRun,
        failures=[_map_case(t, r) for t, r in ut_result.failures],
        errors=[_map_case(t, r) for t, r in ut_result.errors],
        unexpectedSuccesses=[
            _map_case(t, r) for t, r in ut_result.unexpectedSuccesses
        ],
        skipped=[_map_case(t, r) for t, r in ut_result.skipped],
        expectedFailures=[
            _map_case(t, r) for t, r in ut_result.expectedFailures
        ],
    )

    json.dump(dataclasses.asdict(result), log_file, indent=4)


def read_unsuccessful() -> typing.List[str]:
    dir = _result_log_dir()
    results = list(dir.iterdir())
    if not results:
        return []

    results.sort()
    last = results[-1]

    try:
        result_dict = json.load(open(last, 'r'))
        result = TestResult(**result_dict)
    except Exception:
        return []
    return result.failures + result.errors + result.unexpectedSuccesses


def _map_case(
    test: unittest.TestCase,
    _result: typing.Tuple[typing.Type, Exception, typing.Any],
) -> str:
    return test._testMethodName


@dataclasses.dataclass()
class TestResult:
    failures: typing.List[str]
    errors: typing.List[str]
    testsRun: int
    skipped: int
    expectedFailures: typing.List[str]
    unexpectedSuccesses: typing.List[str]


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)
