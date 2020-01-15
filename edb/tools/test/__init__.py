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


from __future__ import annotations

import contextlib
import functools
import os
import pathlib
import shutil
import sys
import tempfile
import unittest

import click

import edb
from edb.common import devmode

from edb.tools.edb import edbcommands

from .decorators import not_implemented
from .decorators import xfail
from .decorators import skip

from . import loader
from . import mproc_fixes
from . import runner
from . import styles


__all__ = ('not_implemented', 'xfail', 'skip')


@edbcommands.command()
@click.argument('files', nargs=-1, metavar='[file or directory]...')
@click.option('-v', '--verbose', is_flag=True,
              help='increase verbosity')
@click.option('-q', '--quiet', is_flag=True,
              help='decrease verbosity')
@click.option('--debug', is_flag=True,
              help='output internal debug logs')
@click.option('--output-format',
              type=click.Choice(runner.OutputFormat.__members__),
              help='test progress output style',
              default=runner.OutputFormat.auto.value)
@click.option('--warnings/--no-warnings',
              help='enable or disable warnings (enabled by default)',
              default=True)
@click.option('-j', '--jobs', type=int,
              default=lambda: os.cpu_count() // 2,
              help='number of parallel processes to use')
@click.option('-k', '--include', type=str, multiple=True, metavar='REGEXP',
              help='only run tests which match the given regular expression')
@click.option('-e', '--exclude', type=str, multiple=True, metavar='REGEXP',
              help='do not run tests which match the given regular expression')
@click.option('-x', '--failfast', is_flag=True,
              help='stop tests after a first failure/error')
@click.option('--repeat', type=int, default=1,
              help='repeat tests N times or until first unsuccessful run')
@click.option('--cov', type=str, multiple=True,
              help='package name to measure code coverage for, '
                   'can be specified multiple times '
                   '(e.g --cov edb.common --cov edb.server)')
def test(*, files, jobs, include, exclude, verbose, quiet, debug,
         output_format, warnings, failfast, cov, repeat):
    """Run EdgeDB test suite.

    Discovers and runs tests in the specified files or directories.
    If no files or directories are specified, current directory is assumed.
    """
    if quiet:
        if verbose:
            click.secho(
                'Warning: both --quiet and --verbose are '
                'specified, assuming --quiet.', fg='yellow')
        verbosity = 0
    elif verbose:
        verbosity = 2
    else:
        verbosity = 1

    mproc_fixes.patch_multiprocessing(debug=debug)

    output_format = runner.OutputFormat(output_format)
    if verbosity > 1 and output_format is runner.OutputFormat.stacked:
        click.secho(
            'Error: cannot use stacked output format in verbose mode.',
            fg='red')
        sys.exit(1)

    if repeat < 1:
        click.secho(
            'Error: --repeat must be a positive non-zero number.', fg='red')
        sys.exit(1)

    if not files:
        cwd = os.path.abspath(os.getcwd())
        if os.path.exists(os.path.join(cwd, 'tests')):
            files = ('tests',)
        else:
            click.secho(
                'Error: no test path specified and no "tests" directory found',
                fg='red')
            sys.exit(1)

    for file in files:
        if not os.path.exists(file):
            click.secho(
                f'Error: test path {file!r} does not exist', fg='red')
            sys.exit(1)

    run = functools.partial(
        _run,
        include=include,
        exclude=exclude,
        verbosity=verbosity,
        files=files,
        jobs=jobs,
        output_format=output_format,
        warnings=warnings,
        failfast=failfast,
        repeat=repeat,
    )

    if cov:
        for pkg in cov:
            if '\\' in pkg or '/' in pkg or pkg.endswith('.py'):
                click.secho(
                    f'Error: --cov argument {pkg!r} looks like a path, '
                    f'expected a Python package name', fg='red')
                sys.exit(1)

        with _coverage_wrapper(cov):
            result = run()
    else:
        result = run()

    sys.exit(result)


@contextlib.contextmanager
def _coverage_wrapper(paths):
    try:
        import coverage  # NoQA
    except ImportError:
        click.secho(
            'Error: "coverage" package is missing, cannot run tests '
            'with --cov')
        sys.exit(1)

    for path in edb.__path__:
        cov_rc = pathlib.Path(path).parent / '.coveragerc'
        if cov_rc.exists():
            break
    else:
        raise RuntimeError('cannot locate the .coveragerc file')

    with tempfile.TemporaryDirectory() as td:
        cov_config = devmode.CoverageConfig(
            paths=paths,
            config=str(cov_rc),
            datadir=td)
        cov_config.save_to_environ()

        main_cov = cov_config.new_coverage_object()
        main_cov.start()

        try:
            yield
        finally:
            main_cov.stop()
            main_cov.save()

            data = coverage.CoverageData()

            with os.scandir(td) as it:
                for entry in it:
                    new_data = coverage.CoverageData()
                    new_data.read_file(entry.path)
                    data.update(new_data)

            covfile = str(pathlib.Path(td) / '.coverage')
            data.write_file(covfile)
            report_cov = cov_config.new_custom_coverage_object(
                config_file=str(cov_rc),
                data_file=covfile,
            )
            report_cov.load()
            click.secho('Coverage:')
            report_cov.report()
            # store the coverage file in cwd, so it can be used to produce
            # additional reports with coverage cli
            shutil.copy(covfile, '.')


def _run(*, include, exclude, verbosity, files, jobs, output_format,
         warnings, failfast, repeat):
    suite = unittest.TestSuite()

    total = 0
    total_unfiltered = 0

    if verbosity > 0:
        def _update_progress(n, unfiltered_n):
            nonlocal total, total_unfiltered
            total += n
            total_unfiltered += unfiltered_n
            click.echo(styles.status(
                f'Collected {total}/{total_unfiltered} tests.\r'), nl=False)
    else:
        _update_progress = None

    test_loader = loader.TestLoader(
        verbosity=verbosity, exclude=exclude, include=include,
        progress_cb=_update_progress)

    for file in files:
        if not os.path.exists(file) and verbosity > 0:
            click.echo(styles.warning(
                f'Warning: {file}: no such file or directory.'))

        if os.path.isdir(file):
            tests = test_loader.discover(file)
        else:
            tests = test_loader.discover(
                os.path.dirname(file),
                pattern=os.path.basename(file))

        suite.addTest(tests)

    jobs = max(min(total, jobs), 1)

    if verbosity > 0:
        click.echo()
        if jobs > 1:
            click.echo(styles.status(
                f'Using up to {jobs} processes to run tests.'))

    for rnum in range(repeat):
        if repeat > 1:
            click.echo(styles.status(
                f'Repeat #{rnum + 1} out of {repeat}.'))

        test_runner = runner.ParallelTextTestRunner(
            verbosity=verbosity, output_format=output_format,
            warnings=warnings, num_workers=jobs, failfast=failfast)

        result = test_runner.run(suite)

        if not result.wasSuccessful():
            return 1

    return 0
