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


import multiprocessing
import os
import platform
import sys
import unittest

import click

from edb.tools import edbcommands

from . import loader
from . import runner
from . import styles


@edbcommands.command()
@click.argument('files', nargs=-1, metavar='[file or directory]...')
@click.option('-v', '--verbose', is_flag=True,
              help='increase verbosity')
@click.option('-q', '--quiet', is_flag=True,
              help='decrease verbosity')
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
def test(*, files, jobs, include, exclude, verbose, quiet, output_format,
         warnings, failfast):
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

    if platform.system().lower() == 'darwin':
        # A "fork" without "exec" is broken on macOS since 10.14:
        # https://www.wefearchange.org/2018/11/forkmacos.rst.html
        multiprocessing.set_start_method('spawn')

    output_format = runner.OutputFormat(output_format)
    if verbosity > 1 and output_format is runner.OutputFormat.stacked:
        click.secho(
            'Error: cannot use stacked output format in verbose mode.',
            fg='red')
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
        click.echo(styles.status(
            f'Using up to {jobs} processes to run tests.'))

    test_runner = runner.ParallelTextTestRunner(
        verbosity=verbosity, output_format=output_format,
        warnings=warnings, num_workers=jobs, failfast=failfast)
    result = test_runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)
